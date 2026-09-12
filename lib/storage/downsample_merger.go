package storage

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

// 预算覆盖并行归并中随 TSID 规模增长的缓冲；不足时立即退出当前归并，不等待其他任务释放。
var downsampleMergeMemoryLimiter = sync.OnceValue(func() *memory.Limiter {
	return &memory.Limiter{MaxSize: min(uint64(memory.Allowed()/16), 256<<20)}
})

// 稀疏索引每项预留键值、哈希表空槽及扩容时旧表与新表共存的空间。
const (
	downsampleSparseBucketIndexBytes = 256
	// 空表和首次槽组也占用内存，不能仅按已插入项目计费。
	downsampleSparseBucketIndexBaseBytes = 1024
)

// downsampleMerger 按 TSID 的完整时间范围归并重叠 block，所有源贡献只计入所属 bucket 一次。
type downsampleMerger struct {
	// baseResolutionReaders 持有本次归并各源的 base reader，包括已出堆及初始化失败的实例，保证统一释放。
	baseResolutionReaders []*downsampleReader
	// baseResolutionReaderHeap 按 TSID 排序尚未读完 base 索引的源 reader，整次归并只建一次堆。
	baseResolutionReaderHeap downsampleReaderHeap
	// currentTSIDReaders 借用当前 TSID 涉及的源 reader；每次收集前清空，归还 reader 前移除借用引用。
	currentTSIDReaders []*downsampleReader
	// currentTSIDBaseBucketSamples 累计当前 TSID 的 base 样本；密集范围按 bucket 开槽，空槽精度为 0。
	currentTSIDBaseBucketSamples []downsampleSample
	// currentTSIDBaseBucketIndexes 仅在时间范围比源行数更稀疏时使用，避免为缺失时间分配巨大空槽数组。
	currentTSIDBaseBucketIndexes map[int64]int
	// currentTSIDResolutionSamples 逐分辨率复用，由 base 样本推导当前租户的额外分辨率。
	currentTSIDResolutionSamples []downsampleSample
	// mergeMemoryLimiter 借用全部归并共享的非等待预算；测试可提供更小的预算验证失败清理。
	mergeMemoryLimiter *memory.Limiter
	// mergeMemoryBytes 记录本次归并实际持有的样本、header 容量及稀疏索引预留额。
	mergeMemoryBytes uint64
	// currentTSIDBaseSamplesMemory 与 currentTSIDResolutionSamplesMemory 分别记录两个样本数组的已预留容量。
	currentTSIDBaseSamplesMemory, currentTSIDResolutionSamplesMemory uint64
	// currentTSIDBucketIndexesMemory 记录当前稀疏索引的保守预留额，丢弃索引后立即归还。
	currentTSIDBucketIndexesMemory uint64
	// currentSourceBlock 复用当前源基础分辨率的多特征解码缓冲；Merge 返回前归还，不保留整个源的数据。
	currentSourceBlock *downsampleDecodedResolutionFeaturesBlock
	// partWriter 借用本次 merge 的目标 writer；直接接收 bucket 样本，Finish/Abort 由调用方统一负责。
	partWriter *downsampleWriter
	// stopCh 在本次 Merge 的索引扫描、数据聚合和写出期间检查取消；返回前清除引用。
	stopCh <-chan struct{}
	// mergeStats 累计整次 merge 的源行统计；只计算实际消费的 base 输入，避免派生表示重复计数。
	mergeStats downsampleMergeStats
}

// downsampleMergeStats 统计实际读取的 base 输入物理行，旧派生列由 base 重建，不重复计数。
type downsampleMergeStats struct {
	// rowsMerged 记录参与 base 计算且保留的源物理行；降采样 base 行按五个特征换算。
	rowsMerged uint64
	// rowsDeleted 记录 base 输入中删除或完全过期的物理行，不重复统计派生分辨率。
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
	if partWriter == nil || partWriter.downsamplingConfig == nil {
		return m.mergeStats, fmt.Errorf("[downsampling] merge writer is not initialized")
	}
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
	baseResolution := partWriter.downsamplingConfig.BaseResolutionMs()
	if err := m.initSources(sourceParts, baseResolution); err != nil {
		return m.mergeStats, err
	}
	for len(m.baseResolutionReaderHeap) > 0 {
		if err := checkDownsampleStopped(m.stopCh); err != nil {
			return m.mergeStats, err
		}
		currentTSID := m.baseResolutionReaderHeap[0].Header().TSID
		deleted := deletedMetricIDs != nil && deletedMetricIDs.Has(currentTSID.MetricID)
		currentTSIDTimeRange, err := m.collectSources(&currentTSID, deleted)
		if err != nil {
			return m.mergeStats, err
		}
		if deleted {
			continue
		}
		if err := m.mergeTSID(&currentTSID, baseResolution, currentTSIDTimeRange, retentionDeadline); err != nil {
			return m.mergeStats, fmt.Errorf("[downsampling] cannot merge TSID %+v at base resolution %d: %w", currentTSID, baseResolution, err)
		}
	}
	return m.mergeStats, checkDownsampleStopped(m.stopCh)
}

// reset 归还本次归并持有的 reader 和解码对象，清空工作状态；普通切片容量留待复用。
func (m *downsampleMerger) reset() error {
	err := m.closeBaseReaders()
	if m.currentSourceBlock != nil {
		block := m.currentSourceBlock
		m.currentSourceBlock = nil
		putDownsampleDecodedResolutionFeaturesBlock(block)
	}
	m.currentTSIDBaseBucketSamples = nil
	m.currentTSIDBaseBucketIndexes = nil
	m.currentTSIDResolutionSamples = nil
	m.releaseMergeMemory(m.mergeMemoryBytes)
	m.currentTSIDBaseSamplesMemory = 0
	m.currentTSIDResolutionSamplesMemory = 0
	m.currentTSIDBucketIndexesMemory = 0
	m.partWriter = nil
	m.stopCh = nil
	m.mergeStats = downsampleMergeStats{}
	return err
}

// closeBaseReaders 统一归还本次归并各源的 base reader，每个源的索引与数据共用同一个 reader。
func (m *downsampleMerger) closeBaseReaders() error {
	// 先移除借用引用，再归还 reader，避免保留已回到对象池的实例。
	clear(m.currentTSIDReaders)
	m.currentTSIDReaders = m.currentTSIDReaders[:0]
	clear(m.baseResolutionReaderHeap)
	m.baseResolutionReaderHeap = m.baseResolutionReaderHeap[:0]
	var err error
	for i, baseResolutionReader := range m.baseResolutionReaders {
		m.baseResolutionReaders[i] = nil
		if baseResolutionReader != nil {
			// 先移除大数组，再归还额度；reader.Close 只负责其有界解码缓冲和文件。
			baseResolutionReader.currentTSIDBlockHeaders = nil
			m.releaseMergeMemory(baseResolutionReader.currentTSIDBlockHeadersMemory)
			baseResolutionReader.currentTSIDBlockHeadersMemory = 0
			err = errors.Join(err, putDownsampleReader(baseResolutionReader))
		}
	}
	m.baseResolutionReaders = m.baseResolutionReaders[:0]
	return err
}

// initSources 为每个源建立一个 base reader，并使用各源首条 header 构建 TSID 堆。
func (m *downsampleMerger) initSources(sourceParts []*partWrapper, baseResolution int64) error {
	if err := m.closeBaseReaders(); err != nil {
		return err
	}
	if cap(m.baseResolutionReaders) < len(sourceParts) {
		m.baseResolutionReaders = make([]*downsampleReader, len(sourceParts))
	} else {
		m.baseResolutionReaders = m.baseResolutionReaders[:len(sourceParts)]
	}
	for i, sourcePart := range sourceParts {
		if sourcePart.p.dsMetadata != nil && sourcePart.p.dsMetadata.DownsamplingConfig.BaseResolutionMs() != baseResolution {
			return fmt.Errorf("[downsampling] source part base resolution differs from the merge configuration")
		}
		if err := checkDownsampleStopped(m.stopCh); err != nil {
			return err
		}
		baseResolutionReader := getDownsampleReader()
		m.baseResolutionReaders[i] = baseResolutionReader
		if err := baseResolutionReader.Init(sourcePart.p, baseResolution); err != nil {
			return err
		}
		baseResolutionReader.stopCh = m.stopCh
		if baseResolutionReader.NextHeader() {
			m.baseResolutionReaderHeap = append(m.baseResolutionReaderHeap, baseResolutionReader)
		} else if err := baseResolutionReader.Error(); err != nil {
			return err
		}
	}
	heap.Init(&m.baseResolutionReaderHeap)
	return nil
}

// collectSources 首次顺序扫描当前 TSID 的全部 header，按值保存并收集完整时间范围，再将各源游标推进到下一个 TSID。
// 已删除 TSID 只累计统计，不缓存 header 或读取时间戳和值负载。
func (m *downsampleMerger) collectSources(currentTSID *TSID, deleted bool) (TimeRange, error) {
	clear(m.currentTSIDReaders)
	m.currentTSIDReaders = m.currentTSIDReaders[:0]
	currentTSIDTimeRange := TimeRange{MinTimestamp: math.MaxInt64, MaxTimestamp: math.MinInt64}
	for len(m.baseResolutionReaderHeap) > 0 && m.baseResolutionReaderHeap[0].Header().TSID == *currentTSID {
		currentTSIDReader := m.baseResolutionReaderHeap[0]
		currentTSIDReader.currentTSIDBlockHeaders = currentTSIDReader.currentTSIDBlockHeaders[:0]
		m.currentTSIDReaders = append(m.currentTSIDReaders, currentTSIDReader)
		for {
			if err := checkDownsampleStopped(m.stopCh); err != nil {
				return TimeRange{}, err
			}
			currentBlockHeader := currentTSIDReader.Header()
			currentTSIDTimeRange.MinTimestamp = min(currentTSIDTimeRange.MinTimestamp, currentBlockHeader.MinTimestamp)
			currentTSIDTimeRange.MaxTimestamp = max(currentTSIDTimeRange.MaxTimestamp, currentBlockHeader.MaxTimestamp)
			if !deleted {
				headers := currentTSIDReader.currentTSIDBlockHeaders
				var err error
				headers, err = growDownsampleMergeSlice(m, headers, len(headers)+1, &currentTSIDReader.currentTSIDBlockHeadersMemory)
				if err != nil {
					return TimeRange{}, err
				}
				headers[len(headers)-1] = *currentBlockHeader
				currentTSIDReader.currentTSIDBlockHeaders = headers
			}
			if deleted {
				m.mergeStats.rowsDeleted += uint64(currentBlockHeader.RowsCount) * downsampleSourceRowWidth(currentTSIDReader.currentSourcePart)
			}
			if !currentTSIDReader.NextHeader() {
				if err := currentTSIDReader.Error(); err != nil {
					return TimeRange{}, err
				}
				heap.Pop(&m.baseResolutionReaderHeap)
				break
			}
			if currentTSIDReader.Header().TSID != *currentTSID {
				heap.Fix(&m.baseResolutionReaderHeap, 0)
				break
			}
		}
	}
	return currentTSIDTimeRange, nil
}

// mergeTSID 根据已收集的完整时间范围分配槽位，依次消费各源保存的 header，再逐特征写出聚合样本。
func (m *downsampleMerger) mergeTSID(currentTSID *TSID, baseResolution int64, currentTSIDTimeRange TimeRange, retentionDeadline int64) error {
	if err := checkDownsampleStopped(m.stopCh); err != nil {
		return err
	}
	if currentTSIDTimeRange.MinTimestamp > currentTSIDTimeRange.MaxTimestamp {
		return fmt.Errorf("[downsampling] invalid TSID time range [%d, %d]", currentTSIDTimeRange.MinTimestamp, currentTSIDTimeRange.MaxTimestamp)
	}
	firstBucketID, err := downsampleBucketID(currentTSIDTimeRange.MinTimestamp, baseResolution)
	if err != nil {
		return err
	}
	lastBucketID, err := downsampleBucketID(currentTSIDTimeRange.MaxTimestamp, baseResolution)
	if err != nil {
		return err
	}
	// 源行数是实际 bucket 数量的上界；稀疏长时间范围只给出现过的 bucket 开槽。
	var sourceRows uint64
	for _, reader := range m.currentTSIDReaders {
		for _, header := range reader.currentTSIDBlockHeaders {
			sourceRows = addDownsampleSpace(sourceRows, uint64(header.RowsCount))
		}
	}
	bucketCount := lastBucketID - firstBucketID + 1
	m.currentTSIDBaseBucketIndexes = nil
	m.releaseMergeMemory(m.currentTSIDBucketIndexesMemory)
	m.currentTSIDBucketIndexesMemory = 0
	if uint64(bucketCount) > sourceRows {
		if err := m.reserveMergeMemory(downsampleSparseBucketIndexBaseBytes); err != nil {
			return err
		}
		m.currentTSIDBucketIndexesMemory = downsampleSparseBucketIndexBaseBytes
		m.currentTSIDBaseBucketSamples = m.currentTSIDBaseBucketSamples[:0]
		m.currentTSIDBaseBucketIndexes = make(map[int64]int)
	} else {
		if uint64(bucketCount) > uint64(int(^uint(0)>>1)) {
			return fmt.Errorf("[downsampling] base bucket range is too large")
		}
		m.currentTSIDBaseBucketSamples, err = growDownsampleMergeSlice(m, m.currentTSIDBaseBucketSamples, int(bucketCount), &m.currentTSIDBaseSamplesMemory)
		if err != nil {
			return err
		}
		clear(m.currentTSIDBaseBucketSamples)
	}
	for _, currentTSIDReader := range m.currentTSIDReaders {
		if err := m.readSource(currentTSIDReader, baseResolution, currentTSIDTimeRange, firstBucketID, retentionDeadline); err != nil {
			return err
		}
	}
	if m.currentTSIDBaseBucketIndexes != nil {
		sort.Slice(m.currentTSIDBaseBucketSamples, func(i, j int) bool {
			return m.currentTSIDBaseBucketSamples[i].timestamp < m.currentTSIDBaseBucketSamples[j].timestamp
		})
		m.currentTSIDBaseBucketIndexes = nil
		m.releaseMergeMemory(m.currentTSIDBucketIndexesMemory)
		m.currentTSIDBucketIndexesMemory = 0
	}
	if err := m.partWriter.WriteSamples(currentTSID, baseResolution, m.currentTSIDBaseBucketSamples, m.stopCh); err != nil {
		return err
	}
	resolutions := m.partWriter.downsamplingConfig.resolutionsForTenant(currentTSID.AccountID, currentTSID.ProjectID)
	for _, resolution := range resolutions[1:] {
		if err := checkDownsampleStopped(m.stopCh); err != nil {
			return err
		}
		var err error
		m.currentTSIDResolutionSamples, err = m.aggregateSamples(m.currentTSIDResolutionSamples, m.currentTSIDBaseBucketSamples, resolution, m.stopCh)
		if err != nil {
			return err
		}
		if err := m.partWriter.WriteSamples(currentTSID, resolution, m.currentTSIDResolutionSamples, m.stopCh); err != nil {
			return err
		}
	}
	return nil
}

// readSource 使用首次扫描保存的 header 顺序读取当前源的 TSID 数据，不重新定位或解码首列索引。
// 读取沿用当前源 reader 的自有句柄；返回时清空已消费的 header，容量仅在本次归并内复用。
func (m *downsampleMerger) readSource(currentTSIDReader *downsampleReader, baseResolution int64, currentTSIDTimeRange TimeRange, firstBucketID, retentionDeadline int64) error {
	defer func() {
		currentTSIDReader.currentTSIDBlockHeaders = currentTSIDReader.currentTSIDBlockHeaders[:0]
	}()
	currentSourcePart := currentTSIDReader.currentSourcePart
	retentionStart := downsampleBaseRetentionStart(m.partWriter.downsamplingConfig, currentSourcePart.dsMetadata, currentTSIDReader.currentTSIDBlockHeaders, retentionDeadline)
	for currentBlockIndex := range currentTSIDReader.currentTSIDBlockHeaders {
		if err := checkDownsampleStopped(m.stopCh); err != nil {
			return err
		}
		currentBlockHeader := &currentTSIDReader.currentTSIDBlockHeaders[currentBlockIndex]
		if err := currentTSIDReader.ReadBlock(m.currentSourceBlock, currentBlockHeader); err != nil {
			return err
		}
		currentSourceBlock := m.currentSourceBlock
		for currentRow, timestamp := range currentSourceBlock.timestamps {
			if err := checkDownsampleStopped(m.stopCh); err != nil {
				return err
			}
			bucketID, err := downsampleBucketID(timestamp, baseResolution)
			if err != nil {
				return err
			}
			if timestamp < retentionStart {
				m.mergeStats.rowsDeleted += downsampleSourceRowWidth(currentSourcePart)
				continue
			}
			m.mergeStats.rowsMerged += downsampleSourceRowWidth(currentSourcePart)
			currentBucketSlot := bucketID - firstBucketID
			if m.currentTSIDBaseBucketIndexes != nil {
				slot, ok := m.currentTSIDBaseBucketIndexes[bucketID]
				if !ok {
					slot = len(m.currentTSIDBaseBucketSamples)
					if err := m.reserveMergeMemory(downsampleSparseBucketIndexBytes); err != nil {
						return err
					}
					m.currentTSIDBucketIndexesMemory += downsampleSparseBucketIndexBytes
					m.currentTSIDBaseBucketSamples, err = growDownsampleMergeSlice(m, m.currentTSIDBaseBucketSamples, slot+1, &m.currentTSIDBaseSamplesMemory)
					if err != nil {
						return err
					}
					m.currentTSIDBaseBucketSamples[slot] = downsampleSample{}
					m.currentTSIDBaseBucketIndexes[bucketID] = slot
				}
				currentBucketSlot = int64(slot)
			}
			if currentBucketSlot < 0 || currentBucketSlot >= int64(len(m.currentTSIDBaseBucketSamples)) {
				return fmt.Errorf("[downsampling] timestamp %d is outside the TSID bucket range for [%d, %d]", timestamp, currentTSIDTimeRange.MinTimestamp, currentTSIDTimeRange.MaxTimestamp)
			}
			currentBucketSample := &m.currentTSIDBaseBucketSamples[currentBucketSlot]
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

// checkDownsampleStopped 在有界文件读取之间及聚合循环中检查取消，避免长任务阻塞归并停机。
func checkDownsampleStopped(stopCh <-chan struct{}) error {
	select {
	case <-stopCh:
		return fmt.Errorf("[downsampling] operation stopped: %w", errForciblyStopped)
	default:
		return nil
	}
}

// aggregateSamples 顺序消费已完成的 base 样本，空槽跳过，同目标 bucket 直接合并。
// retention 已在读取源时处理；派生列必须完整表达保留的 base，供更粗查询选择任意可整除的源列。
// dst 仅保存非空目标 bucket，容量随实际数据增长，与时间范围中的空白长度无关。
func (m *downsampleMerger) aggregateSamples(dst, baseSamples []downsampleSample, resolution int64, stopCh <-chan struct{}) ([]downsampleSample, error) {
	dst = dst[:0]
	var previousBucket int64 = -1
	for i := range baseSamples {
		if err := checkDownsampleStopped(stopCh); err != nil {
			return dst, err
		}
		sample := &baseSamples[i]
		if sample.isEmpty() {
			continue
		}
		bucket, err := downsampleBucketID(sample.timestamp, resolution)
		if err != nil {
			return dst, err
		}
		if bucket < previousBucket {
			return dst, fmt.Errorf("[downsampling] base samples are out of bucket order")
		}
		if len(dst) == 0 || bucket != previousBucket {
			dst, err = growDownsampleMergeSlice(m, dst, len(dst)+1, &m.currentTSIDResolutionSamplesMemory)
			if err != nil {
				return dst, err
			}
			dst[len(dst)-1] = downsampleSample{}
		}
		current := &dst[len(dst)-1]
		if !current.isEmpty() && current.precisionBits != sample.precisionBits {
			return dst, fmt.Errorf("[downsampling] source precision differs within a derived bucket: %d vs %d", current.precisionBits, sample.precisionBits)
		}
		current.Merge(sample)
		previousBucket = bucket
	}
	return dst, nil
}

// reserveMergeMemory 只尝试领取额度，不持有锁等待，也不阻塞其他归并释放内存。
func (m *downsampleMerger) reserveMergeMemory(size uint64) error {
	if m.mergeMemoryLimiter == nil {
		m.mergeMemoryLimiter = downsampleMergeMemoryLimiter()
	}
	if !m.mergeMemoryLimiter.Get(size) {
		return fmt.Errorf("[downsampling] merge memory budget exceeded: requested %d bytes, limit %d bytes", size, m.mergeMemoryLimiter.MaxSize)
	}
	m.mergeMemoryBytes += size
	return nil
}

func (m *downsampleMerger) releaseMergeMemory(size uint64) {
	if size > 0 {
		m.mergeMemoryLimiter.Put(size)
		m.mergeMemoryBytes -= size
	}
}

// growDownsampleMergeSlice 在分配前计入新数组的完整容量，复制结束后才归还旧数组额度。
// 因而扩容期间的新旧数组也受同一预算约束；拒绝时保持原数组及其额度不变。
func growDownsampleMergeSlice[T any](m *downsampleMerger, src []T, size int, reserved *uint64) ([]T, error) {
	if size < 0 {
		return src, fmt.Errorf("[downsampling] merge buffer length overflows")
	}
	if size <= cap(src) {
		return src[:size], nil
	}
	capacity := size
	if cap(src) <= int(^uint(0)>>1)/2 {
		capacity = max(capacity, 2*cap(src))
	}
	var item T
	itemSize := uint64(unsafe.Sizeof(item))
	if uint64(capacity) > math.MaxUint64/itemSize {
		return src, fmt.Errorf("[downsampling] merge buffer size overflows")
	}
	bytes := uint64(capacity) * itemSize
	if err := m.reserveMergeMemory(bytes); err != nil {
		return src, err
	}
	dst := make([]T, size, capacity)
	copy(dst, src)
	m.releaseMergeMemory(*reserved)
	*reserved = bytes
	return dst, nil
}

// downsampleBaseRetentionStart 保留所有仍有效目标 bucket 所需的 base 前缀。
// 旧 part 与本次配置都参与计算；每个源/TSID 只计算一次，逐行判断复用此边界。
func downsampleBaseRetentionStart(config *DownsamplingConfig, metadata *downsamplePartMetadata, headers []blockHeader, deadline int64) int64 {
	if deadline <= 0 || len(headers) == 0 {
		return deadline
	}
	tsid := &headers[0].TSID
	configs := []*DownsamplingConfig{config}
	if metadata != nil {
		configs = append(configs, metadata.DownsamplingConfig)
	}
	start := deadline
	for _, currentConfig := range configs {
		for _, resolution := range currentConfig.resolutionsForTenant(tsid.AccountID, tsid.ProjectID) {
			start = min(start, deadline-deadline%resolution)
		}
	}
	return start
}

// downsampleSourceRowWidth 将聚合批次的一行换算为单值 Block 的物理行数。
// 原始样本只有一个值，降采样源的基础分辨率分别保存五个特征 Block。
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
