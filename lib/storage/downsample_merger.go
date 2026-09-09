package storage

import (
	"container/heap"
	"fmt"
	"math"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

const (
	downsampleWindowBuckets   = 1024
	downsampleMaxMergeSources = 1024
)

// downsampleMergeStats 统计实际处理的源物理行，不重复累计原始源的两个分辨率。
type downsampleMergeStats struct {
	rowsMerged  uint64
	rowsDeleted uint64
}

// downsampleWindowState 保存一个完整区间的特征及源数据的共享精度。
type downsampleWindowState struct {
	acc           downsampleAccumulator
	precisionBits uint8
}

// downsampleMergeCursor 仅保留一个源的索引游标，不持有该源的全部数据 block。
type downsampleMergeCursor struct {
	p      *part
	reader *downsampleReader
}

type downsampleCursorHeap []*downsampleMergeCursor

func (h downsampleCursorHeap) Len() int { return len(h) }
func (h downsampleCursorHeap) Less(i, j int) bool {
	return h[i].reader.Header().TSID.Less(&h[j].reader.Header().TSID)
}
func (h downsampleCursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *downsampleCursorHeap) Push(any)     { panic("BUG: unexpected downsample heap push") }
func (h *downsampleCursorHeap) Pop() any {
	a := *h
	v := a[len(a)-1]
	a[len(a)-1] = nil
	*h = a[:len(a)-1]
	return v
}

// downsampleMerger 以完整区间窗口归并重叠 block，所有源贡献只计入所属窗口一次。
type downsampleMerger struct {
	cursors       []downsampleMergeCursor
	heap          downsampleCursorHeap
	activeSources []*part
	states        []downsampleWindowState
	reader        *downsampleReader
	input         *downsampleBatch
	output        *downsampleBatch
	writer        *downsampleWriter
	stopCh        <-chan struct{}
	stats         downsampleMergeStats
}

// Reset 释放借用对象与源引用；正常工作缓冲可供下一次作业复用。
func (m *downsampleMerger) Reset() {
	m.resetCursors()
	if m.reader != nil {
		putDownsampleReader(m.reader)
		m.reader = nil
	}
	if m.input != nil {
		putDownsampleBatch(m.input)
		m.input = nil
	}
	if m.output != nil {
		putDownsampleBatch(m.output)
		m.output = nil
	}
	clear(m.activeSources)
	m.activeSources = m.activeSources[:0]
	clear(m.states)
	m.states = m.states[:0]
	m.writer = nil
	m.stopCh = nil
	m.stats = downsampleMergeStats{}
	// 强制 merge 可能临时涉及大量 part，池不长期保留这种峰值容量。
	if cap(m.cursors) > 1024 {
		m.cursors = nil
	}
	if cap(m.heap) > 1024 {
		m.heap = nil
	}
	if cap(m.activeSources) > 1024 {
		m.activeSources = nil
	}
	if cap(m.states) > downsampleWindowBuckets {
		m.states = nil
	}
}

func (m *downsampleMerger) resetCursors() {
	for i := range m.cursors {
		c := &m.cursors[i]
		if c.reader != nil {
			putDownsampleReader(c.reader)
		}
		*c = downsampleMergeCursor{}
	}
	m.cursors = m.cursors[:0]
	clear(m.heap)
	m.heap = m.heap[:0]
}

// Merge 不关闭 writer；调用方在成功后 Finish，失败或取消时 Abort。
// pws 的引用由调用方持有，必须覆盖整个归并和目标发布过程。
func (m *downsampleMerger) Merge(pws []*partWrapper, w *downsampleWriter, stopCh <-chan struct{}, dmis *uint64set.Set, retentionDeadline int64, windowBuckets int) (downsampleMergeStats, error) {
	m.Reset()
	if len(pws) > downsampleMaxMergeSources {
		return m.stats, fmt.Errorf("downsampling merge has %d sources; maximum is %d", len(pws), downsampleMaxMergeSources)
	}
	if windowBuckets < 1 || windowBuckets > downsampleWindowBuckets {
		return m.stats, fmt.Errorf("downsampling window must contain 1..%d buckets; got %d", downsampleWindowBuckets, windowBuckets)
	}
	m.writer = w
	m.stopCh = stopCh
	m.reader = getDownsampleReader()
	m.input = getDownsampleBatch()
	m.output = getDownsampleBatch()
	if cap(m.states) < windowBuckets {
		m.states = make([]downsampleWindowState, windowBuckets)
	} else {
		m.states = m.states[:windowBuckets]
	}
	var sourceRows uint64
	for _, pw := range pws {
		if pw == nil || pw.p == nil {
			return m.stats, fmt.Errorf("nil downsampling source part")
		}
		if math.MaxUint64-sourceRows < pw.p.ph.RowsCount {
			return m.stats, fmt.Errorf("downsampling source row count overflows")
		}
		sourceRows += pw.p.ph.RowsCount
	}
	for _, resolution := range downsampleResolutions {
		if err := m.initSources(pws, resolution); err != nil {
			return m.stats, err
		}
		for len(m.heap) > 0 {
			if err := m.checkStopped(); err != nil {
				return m.stats, err
			}
			tsid := m.heap[0].reader.Header().TSID
			deleted := dmis != nil && dmis.Has(tsid.MetricID)
			minTimestamp, err := m.collectSources(&tsid, resolution, deleted)
			if err != nil {
				return m.stats, err
			}
			if deleted {
				continue
			}
			if err := m.mergeTSID(&tsid, resolution, minTimestamp, retentionDeadline); err != nil {
				return m.stats, fmt.Errorf("cannot downsample TSID %+v at resolution %d: %w", tsid, resolution, err)
			}
		}
	}
	return m.stats, m.checkStopped()
}

func (m *downsampleMerger) checkStopped() error {
	select {
	case <-m.stopCh:
		return errForciblyStopped
	default:
		return nil
	}
}

func (m *downsampleMerger) initSources(pws []*partWrapper, resolution int64) error {
	m.resetCursors()
	if cap(m.cursors) < len(pws) {
		m.cursors = make([]downsampleMergeCursor, len(pws))
	} else {
		m.cursors = m.cursors[:len(pws)]
	}
	for i, pw := range pws {
		if err := m.checkStopped(); err != nil {
			return err
		}
		c := &m.cursors[i]
		c.p = pw.p
		c.reader = getDownsampleReader()
		if err := c.reader.Init(c.p, resolution); err != nil {
			return err
		}
		if c.reader.NextHeader() {
			m.heap = append(m.heap, c)
		} else if err := c.reader.Error(); err != nil {
			return err
		}
	}
	heap.Init(&m.heap)
	return nil
}

// collectSources 消费当前 TSID 的所有索引项，并将各源游标推进到下一个 TSID。
func (m *downsampleMerger) collectSources(tsid *TSID, resolution int64, deleted bool) (int64, error) {
	clear(m.activeSources)
	m.activeSources = m.activeSources[:0]
	minTimestamp := int64(math.MaxInt64)
	for len(m.heap) > 0 && m.heap[0].reader.Header().TSID == *tsid {
		c := m.heap[0]
		m.activeSources = append(m.activeSources, c.p)
		for {
			if err := m.checkStopped(); err != nil {
				return 0, err
			}
			h := c.reader.Header()
			minTimestamp = min(minTimestamp, h.MinTimestamp)
			if deleted && (c.p.dsMetadata != nil || resolution == downsampleResolution5m) {
				m.stats.rowsDeleted += uint64(h.RowsCount) * downsampleSourceRowWidth(c.p)
			}
			if !c.reader.NextHeader() {
				if err := c.reader.Error(); err != nil {
					return 0, err
				}
				heap.Pop(&m.heap)
				break
			}
			if c.reader.Header().TSID != *tsid {
				heap.Fix(&m.heap, 0)
				break
			}
		}
	}
	return minTimestamp, nil
}

func (m *downsampleMerger) resetOutput(tsid *TSID, resolution int64) {
	m.output.Reset()
	m.output.tsid = *tsid
	m.output.resolution = resolution
}

func (m *downsampleMerger) flushOutput(tsid *TSID, resolution int64) error {
	if len(m.output.timestamps) == 0 {
		return nil
	}
	if err := m.checkStopped(); err != nil {
		return err
	}
	if err := m.writer.WriteBlock(m.output); err != nil {
		return err
	}
	m.resetOutput(tsid, resolution)
	return nil
}

func (m *downsampleMerger) mergeTSID(tsid *TSID, resolution, minTimestamp, retentionDeadline int64) error {
	firstBucket, err := downsampleBucketID(minTimestamp, resolution)
	if err != nil {
		return err
	}
	m.resetOutput(tsid, resolution)
	for firstBucket != math.MaxInt64 {
		if err := m.checkStopped(); err != nil {
			return err
		}
		clear(m.states)
		lastBucket := min(firstBucket+int64(len(m.states))-1, maxUnixMilli/resolution)
		nextBucket := int64(math.MaxInt64)
		for _, p := range m.activeSources {
			if err := m.readWindow(p, tsid, resolution, firstBucket, lastBucket, retentionDeadline, &nextBucket); err != nil {
				return err
			}
		}
		for i := range m.states {
			s := &m.states[i]
			if !s.acc.initialized {
				continue
			}
			b := m.output
			if len(b.timestamps) > 0 && b.precisionBits != s.precisionBits {
				if err := m.flushOutput(tsid, resolution); err != nil {
					return err
				}
			}
			b.precisionBits = s.precisionBits
			b.timestamps = append(b.timestamps, s.acc.sample.timestamp)
			for feature, value := range s.acc.sample.values {
				b.values[feature] = append(b.values[feature], value)
			}
			if len(b.timestamps) == maxRowsPerBlock {
				if err := m.flushOutput(tsid, resolution); err != nil {
					return err
				}
			}
		}
		firstBucket = nextBucket
	}
	return m.flushOutput(tsid, resolution)
}

// readWindow 仅读取与窗口相交的 block；未来数据仅用于跳过空区间。
func (m *downsampleMerger) readWindow(p *part, tsid *TSID, resolution, firstBucket, lastBucket, retentionDeadline int64, nextBucket *int64) error {
	if err := m.reader.Init(p, resolution); err != nil {
		return err
	}
	m.reader.SetFilter(tsid, firstBucket*resolution, maxUnixMilli)
	for m.reader.NextHeader() {
		if err := m.checkStopped(); err != nil {
			return err
		}
		h := m.reader.Header()
		if h.MinTimestamp/resolution > lastBucket {
			*nextBucket = min(*nextBucket, h.MinTimestamp/resolution)
			break
		}
		if err := m.reader.ReadBlock(m.input); err != nil {
			return err
		}
		b := m.input
		for i, timestamp := range b.timestamps {
			bucketID, err := downsampleBucketID(timestamp, resolution)
			if err != nil {
				return err
			}
			if bucketID < firstBucket {
				continue
			}
			if bucketID > lastBucket {
				*nextBucket = min(*nextBucket, bucketID)
				continue
			}
			bucketEnd, err := downsampleBucketEnd(timestamp, resolution)
			if err != nil {
				return err
			}
			if p.dsMetadata != nil || resolution == downsampleResolution5m {
				// 原始行只有两个目标区间均过期才计为删除；第二次读取不重复统计。
				expired := bucketEnd <= retentionDeadline
				if p.dsMetadata == nil && expired {
					coarseEnd, err := downsampleBucketEnd(timestamp, downsampleResolution1h)
					if err != nil {
						return err
					}
					expired = coarseEnd <= retentionDeadline
				}
				if expired {
					m.stats.rowsDeleted += downsampleSourceRowWidth(p)
				} else {
					m.stats.rowsMerged += downsampleSourceRowWidth(p)
				}
			}
			if bucketEnd <= retentionDeadline {
				continue
			}
			s := &m.states[bucketID-firstBucket]
			if !s.acc.initialized {
				s.precisionBits = b.precisionBits
			} else if s.precisionBits != b.precisionBits {
				return fmt.Errorf("同一降采样 bucket 的源精度不一致: %d vs %d", s.precisionBits, b.precisionBits)
			}
			point := downsampleSample{timestamp: timestamp}
			for feature := range point.values {
				point.values[feature] = b.values[feature][i]
			}
			s.acc.AddSummary(&point)
		}
	}
	return m.reader.Error()
}

func getDownsampleMerger() *downsampleMerger {
	if v := downsampleMergerPool.Get(); v != nil {
		return v.(*downsampleMerger)
	}
	return &downsampleMerger{}
}

// downsampleSourceRowWidth 将聚合批次的一行换算为单值 Block 的物理行数。
// 原始样本只有一个值，摘要在同一分辨率下分别保存五个特征 Block。
func downsampleSourceRowWidth(p *part) uint64 {
	if p.dsMetadata != nil {
		return countOfDownsampleFeatures
	}
	return 1
}

func putDownsampleMerger(m *downsampleMerger) {
	m.Reset()
	downsampleMergerPool.Put(m)
}

var downsampleMergerPool sync.Pool
