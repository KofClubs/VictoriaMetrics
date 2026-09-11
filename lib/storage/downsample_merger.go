package storage

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

// 31 天在 5m 分辨率下最多 8928 个 bucket，池最多保留两倍容量。
// 这仅控制缓存回收；每个 TSID 的有效槽数由其 block 时间范围决定。
const downsampleMaxPooledBuckets = 8928 * 2

// downsampleMerger 按 TSID 的完整时间范围归并重叠 block，所有源贡献只计入所属 bucket 一次。
type downsampleMerger struct {
	// currentResolutionReaders 持有当前分辨率的全部源 reader，包括已出堆及初始化失败的实例，保证统一释放。
	currentResolutionReaders []*downsampleReader
	// currentResolutionReaderHeap 按 TSID 排序尚未读完索引的源 reader，每个分辨率建一次堆。
	currentResolutionReaderHeap downsampleReaderHeap
	// currentTSIDReaders 借用当前 TSID 涉及的源 reader；每次收集前清空，归还 reader 前移除借用引用。
	currentTSIDReaders []*downsampleReader
	// currentTSIDBucketSamples 保存当前分辨率、当前 TSID 的完整 bucket 范围；按 header 时间范围精确开槽，空槽精度为 0。
	currentTSIDBucketSamples []downsampleSample
	// currentSourceBlock 复用当前源、当前分辨率的多特征解码缓冲；Merge 返回前归还，不保留整个源的数据。
	currentSourceBlock *downsampleDecodedResolutionFeaturesBlock
	// partWriter 借用本次 merge 的目标 writer；直接接收 bucket 样本，Finish/Abort 由调用方统一负责。
	partWriter *downsampleWriter
	// stopCh 在本次 Merge 的索引扫描、数据聚合和写出期间检查取消；返回前清除引用。
	stopCh <-chan struct{}
	// mergeStats 累计整次 merge 的源行统计；跨分辨率保留，reset 才清零，避免 raw 行重复计数。
	mergeStats downsampleMergeStats
}

// downsampleMergeStats 统计实际处理的源物理行，不重复累计原始源的两个分辨率。
type downsampleMergeStats struct {
	// rowsMerged 记录保留的源物理行；raw 仅在首个分辨率累计，降采样行按五个特征换算。
	rowsMerged uint64
	// rowsDeleted 记录删除或完全过期的源物理行，避免 raw 在两个分辨率中被重复累计。
	rowsDeleted uint64
}

type downsampleReaderHeap []*downsampleReader

// Merge 不关闭 writer；调用方在成功后 Finish，失败或取消时 Abort。
// 所有 reader 和解码对象均在返回前归还；统计按值返回，清理错误与归并错误一起返回。
// sourceParts 的引用由调用方持有，必须覆盖整个归并和目标发布过程。
func (m *downsampleMerger) Merge(sourceParts []*partWrapper, partWriter *downsampleWriter, stopCh <-chan struct{}, deletedMetricIDs *uint64set.Set, retentionDeadline int64) (_ downsampleMergeStats, err error) {
	if err := m.reset(); err != nil {
		return m.mergeStats, err
	}
	defer func() {
		closeErr := m.reset()
		if closeErr != nil {
			// 取消本身可静默退出，但同时发生的 reader 关闭失败不能被隐藏。
			downsampleMergeLogger.Warnf("[downsampling] cannot release merge resources: %s", closeErr)
		}
		err = errors.Join(err, closeErr)
	}()
	m.partWriter = partWriter
	m.stopCh = stopCh
	m.currentSourceBlock = getDownsampleDecodedResolutionFeaturesBlock()
	var sourceRows uint64
	for _, sourcePart := range sourceParts {
		if sourcePart == nil || sourcePart.p == nil {
			return m.mergeStats, fmt.Errorf("[downsampling] source part is nil")
		}
		if math.MaxUint64-sourceRows < sourcePart.p.ph.RowsCount {
			return m.mergeStats, fmt.Errorf("[downsampling] source row count overflows")
		}
		sourceRows += sourcePart.p.ph.RowsCount
	}
	for _, currentResolution := range downsampleResolutions {
		if err := m.initSources(sourceParts, currentResolution); err != nil {
			return m.mergeStats, err
		}
		for len(m.currentResolutionReaderHeap) > 0 {
			if err := m.checkStopped(); err != nil {
				return m.mergeStats, err
			}
			currentTSID := m.currentResolutionReaderHeap[0].Header().TSID
			deleted := deletedMetricIDs != nil && deletedMetricIDs.Has(currentTSID.MetricID)
			currentTSIDTimeRange, err := m.collectSources(&currentTSID, currentResolution, deleted)
			if err != nil {
				return m.mergeStats, err
			}
			if deleted {
				continue
			}
			if err := m.mergeTSID(&currentTSID, currentResolution, currentTSIDTimeRange, retentionDeadline); err != nil {
				return m.mergeStats, fmt.Errorf("[downsampling] cannot merge TSID %+v at resolution %d: %w", currentTSID, currentResolution, err)
			}
		}
	}
	return m.mergeStats, m.checkStopped()
}

// reset 归还本次归并持有的 reader 和解码对象，清空工作状态；普通切片容量留待复用。
func (m *downsampleMerger) reset() error {
	err := m.closeResolutionReaders()
	if m.currentSourceBlock != nil {
		block := m.currentSourceBlock
		m.currentSourceBlock = nil
		putDownsampleDecodedResolutionFeaturesBlock(block)
	}
	if cap(m.currentTSIDBucketSamples) > downsampleMaxPooledBuckets {
		m.currentTSIDBucketSamples = nil
	} else {
		clear(m.currentTSIDBucketSamples)
		m.currentTSIDBucketSamples = m.currentTSIDBucketSamples[:0]
	}
	m.partWriter = nil
	m.stopCh = nil
	m.mergeStats = downsampleMergeStats{}
	return err
}

// closeResolutionReaders 统一归还当前分辨率各源的 reader，每个源的索引与数据共用同一个 reader。
func (m *downsampleMerger) closeResolutionReaders() error {
	// 先移除借用引用，再归还 reader，避免保留已回到对象池的实例。
	clear(m.currentTSIDReaders)
	m.currentTSIDReaders = m.currentTSIDReaders[:0]
	clear(m.currentResolutionReaderHeap)
	m.currentResolutionReaderHeap = m.currentResolutionReaderHeap[:0]
	var err error
	for i, currentResolutionReader := range m.currentResolutionReaders {
		m.currentResolutionReaders[i] = nil
		if currentResolutionReader != nil {
			err = errors.Join(err, putDownsampleReader(currentResolutionReader))
		}
	}
	m.currentResolutionReaders = m.currentResolutionReaders[:0]
	return err
}

// initSources 为当前分辨率的每个源建立一个 reader，并使用各源首条 header 构建 TSID 堆。
func (m *downsampleMerger) initSources(sourceParts []*partWrapper, currentResolution int64) error {
	if err := m.closeResolutionReaders(); err != nil {
		return err
	}
	if cap(m.currentResolutionReaders) < len(sourceParts) {
		m.currentResolutionReaders = make([]*downsampleReader, len(sourceParts))
	} else {
		m.currentResolutionReaders = m.currentResolutionReaders[:len(sourceParts)]
	}
	for i, sourcePart := range sourceParts {
		if err := m.checkStopped(); err != nil {
			return err
		}
		currentResolutionReader := getDownsampleReader()
		m.currentResolutionReaders[i] = currentResolutionReader
		if err := currentResolutionReader.Init(sourcePart.p, currentResolution); err != nil {
			return err
		}
		if currentResolutionReader.NextHeader() {
			m.currentResolutionReaderHeap = append(m.currentResolutionReaderHeap, currentResolutionReader)
		} else if err := currentResolutionReader.Error(); err != nil {
			return err
		}
	}
	heap.Init(&m.currentResolutionReaderHeap)
	return nil
}

// collectSources 首次顺序扫描当前 TSID 的全部 header，按值保存并收集完整时间范围，再将各源游标推进到下一个 TSID。
// 已删除 TSID 只累计统计，不缓存 header 或读取时间戳和值负载。
func (m *downsampleMerger) collectSources(currentTSID *TSID, currentResolution int64, deleted bool) (TimeRange, error) {
	clear(m.currentTSIDReaders)
	m.currentTSIDReaders = m.currentTSIDReaders[:0]
	currentTSIDTimeRange := TimeRange{MinTimestamp: math.MaxInt64, MaxTimestamp: math.MinInt64}
	for len(m.currentResolutionReaderHeap) > 0 && m.currentResolutionReaderHeap[0].Header().TSID == *currentTSID {
		currentTSIDReader := m.currentResolutionReaderHeap[0]
		currentTSIDReader.currentTSIDBlockHeaders = currentTSIDReader.currentTSIDBlockHeaders[:0]
		m.currentTSIDReaders = append(m.currentTSIDReaders, currentTSIDReader)
		for {
			if err := m.checkStopped(); err != nil {
				return TimeRange{}, err
			}
			currentBlockHeader := currentTSIDReader.Header()
			currentTSIDTimeRange.MinTimestamp = min(currentTSIDTimeRange.MinTimestamp, currentBlockHeader.MinTimestamp)
			currentTSIDTimeRange.MaxTimestamp = max(currentTSIDTimeRange.MaxTimestamp, currentBlockHeader.MaxTimestamp)
			if !deleted {
				currentTSIDReader.currentTSIDBlockHeaders = append(currentTSIDReader.currentTSIDBlockHeaders, *currentBlockHeader)
			}
			if deleted && (currentTSIDReader.currentSourcePart.dsMetadata != nil || currentResolution == downsampleResolution5m) {
				m.mergeStats.rowsDeleted += uint64(currentBlockHeader.RowsCount) * downsampleSourceRowWidth(currentTSIDReader.currentSourcePart)
			}
			if !currentTSIDReader.NextHeader() {
				if err := currentTSIDReader.Error(); err != nil {
					return TimeRange{}, err
				}
				heap.Pop(&m.currentResolutionReaderHeap)
				break
			}
			if currentTSIDReader.Header().TSID != *currentTSID {
				heap.Fix(&m.currentResolutionReaderHeap, 0)
				break
			}
		}
	}
	return currentTSIDTimeRange, nil
}

// mergeTSID 根据已收集的完整时间范围分配槽位，依次消费各源保存的 header，再逐特征写出聚合样本。
func (m *downsampleMerger) mergeTSID(currentTSID *TSID, currentResolution int64, currentTSIDTimeRange TimeRange, retentionDeadline int64) error {
	if err := m.checkStopped(); err != nil {
		return err
	}
	if currentTSIDTimeRange.MinTimestamp > currentTSIDTimeRange.MaxTimestamp {
		return fmt.Errorf("[downsampling] invalid TSID time range [%d, %d]", currentTSIDTimeRange.MinTimestamp, currentTSIDTimeRange.MaxTimestamp)
	}
	firstBucketID, err := downsampleBucketID(currentTSIDTimeRange.MinTimestamp, currentResolution)
	if err != nil {
		return err
	}
	lastBucketID, err := downsampleBucketID(currentTSIDTimeRange.MaxTimestamp, currentResolution)
	if err != nil {
		return err
	}
	// 两端已通过时间域和分辨率校验；合法 bucket 总数可安全转换为 int。
	bucketCount := int(lastBucketID - firstBucketID + 1)
	if cap(m.currentTSIDBucketSamples) < bucketCount {
		m.currentTSIDBucketSamples = make([]downsampleSample, bucketCount)
	} else {
		m.currentTSIDBucketSamples = m.currentTSIDBucketSamples[:bucketCount]
		clear(m.currentTSIDBucketSamples)
	}
	for _, currentTSIDReader := range m.currentTSIDReaders {
		if err := m.readSource(currentTSIDReader, currentResolution, currentTSIDTimeRange, firstBucketID, retentionDeadline); err != nil {
			return err
		}
	}
	// writer 直接借用 bucket 样本并逐列编码，不再构造另一份五列输出 Block。
	return m.partWriter.WriteSamples(currentTSID, currentResolution, m.currentTSIDBucketSamples, m.stopCh)
}

// readSource 使用首次扫描保存的 header 顺序读取当前源的 TSID 数据，不重新定位或解码首列索引。
// 读取沿用当前源 reader 的自有句柄；返回时清空已消费的 header，容量仅在当前分辨率内复用。
func (m *downsampleMerger) readSource(currentTSIDReader *downsampleReader, currentResolution int64, currentTSIDTimeRange TimeRange, firstBucketID, retentionDeadline int64) error {
	defer func() {
		currentTSIDReader.currentTSIDBlockHeaders = currentTSIDReader.currentTSIDBlockHeaders[:0]
	}()
	currentSourcePart := currentTSIDReader.currentSourcePart
	for currentBlockIndex := range currentTSIDReader.currentTSIDBlockHeaders {
		if err := m.checkStopped(); err != nil {
			return err
		}
		currentBlockHeader := &currentTSIDReader.currentTSIDBlockHeaders[currentBlockIndex]
		if err := currentTSIDReader.ReadBlock(m.currentSourceBlock, currentBlockHeader); err != nil {
			return err
		}
		currentSourceBlock := m.currentSourceBlock
		for currentRow, timestamp := range currentSourceBlock.timestamps {
			bucketID, err := downsampleBucketID(timestamp, currentResolution)
			if err != nil {
				return err
			}
			currentBucketSlot := bucketID - firstBucketID
			if currentBucketSlot < 0 || currentBucketSlot >= int64(len(m.currentTSIDBucketSamples)) {
				return fmt.Errorf("[downsampling] timestamp %d is outside the TSID bucket range for [%d, %d]", timestamp, currentTSIDTimeRange.MinTimestamp, currentTSIDTimeRange.MaxTimestamp)
			}
			bucketEnd, err := downsampleBucketEnd(timestamp, currentResolution)
			if err != nil {
				return err
			}
			if currentSourcePart.dsMetadata != nil || currentResolution == downsampleResolution5m {
				// 原始行只有两个目标区间均过期才计为删除；第二次读取不重复统计。
				expired := bucketEnd <= retentionDeadline
				if currentSourcePart.dsMetadata == nil && expired {
					coarseEnd, err := downsampleBucketEnd(timestamp, downsampleResolution1h)
					if err != nil {
						return err
					}
					expired = coarseEnd <= retentionDeadline
				}
				if expired {
					m.mergeStats.rowsDeleted += downsampleSourceRowWidth(currentSourcePart)
				} else {
					m.mergeStats.rowsMerged += downsampleSourceRowWidth(currentSourcePart)
				}
			}
			if bucketEnd <= retentionDeadline {
				continue
			}
			currentBucketSample := &m.currentTSIDBucketSamples[currentBucketSlot]
			if !currentBucketSample.isEmpty() && currentBucketSample.precisionBits != currentSourceBlock.precisionBits {
				return fmt.Errorf("[downsampling] source precision differs within a bucket: %d vs %d", currentBucketSample.precisionBits, currentSourceBlock.precisionBits)
			}
			currentSourceSample := downsampleSample{timestamp: timestamp, precisionBits: currentSourceBlock.precisionBits}
			for currentFeature := range currentSourceSample.values {
				currentSourceSample.values[currentFeature] = currentSourceBlock.values[currentFeature][currentRow]
			}
			currentBucketSample.Merge(&currentSourceSample)
		}
	}
	return currentTSIDReader.Error()
}

// checkStopped 在索引扫描、数据聚合和写出之间检查取消信号。
func (m *downsampleMerger) checkStopped() error {
	select {
	case <-m.stopCh:
		return fmt.Errorf("[downsampling] merge stopped: %w", errForciblyStopped)
	default:
		return nil
	}
}

// downsampleSourceRowWidth 将聚合批次的一行换算为单值 Block 的物理行数。
// 原始样本只有一个值，摘要在同一分辨率下分别保存五个特征 Block。
func downsampleSourceRowWidth(p *part) uint64 {
	if p.dsMetadata != nil {
		return countOfDownsampleFeatures
	}
	return 1
}

func (h downsampleReaderHeap) Len() int { return len(h) }

func (h downsampleReaderHeap) Less(i, j int) bool {
	// 堆只用于按 TSID 收集全部源及其 block 时间范围，不要求同一 TSID 的 block 跨源按时间排序。
	// 样本按 timestamp 累加到对应 bucket，last 显式比较 timestamp；最终按 bucket 顺序输出。
	return h[i].Header().TSID.Less(&h[j].Header().TSID)
}

func (h downsampleReaderHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *downsampleReaderHeap) Push(any) { panic("[downsampling] BUG: unexpected reader heap push") }

func (h *downsampleReaderHeap) Pop() any {
	a := *h
	v := a[len(a)-1]
	a[len(a)-1] = nil
	*h = a[:len(a)-1]
	return v
}

func getDownsampleMerger() *downsampleMerger {
	if v := downsampleMergerPool.Get(); v != nil {
		return v.(*downsampleMerger)
	}
	return &downsampleMerger{}
}

func putDownsampleMerger(m *downsampleMerger) error {
	err := m.reset()
	downsampleMergerPool.Put(m)
	return err
}

var downsampleMergerPool sync.Pool
