package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/chunkedbuffer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// downsampleReader 每次只解码一个 index block，按索引定位窗口而不载入全部 header。
// 调用方必须在 reader 存活期间持有 part 引用；返回的 header 在下次迭代时失效。
type downsampleReader struct {
	p            *part
	resolution   int64
	files        [3]*os.File
	fileSizes    [3]uint64
	ownFiles     bool
	filterTSID   TSID
	hasFilter    bool
	minTimestamp int64
	maxTimestamp int64
	metaPos      int
	metaEnd      int
	indexPos     int
	indexData    []byte
	compressed   []byte
	decompressed []byte
	block        Block
	current      downsampleBlockHeader
	previous     downsampleBlockHeader
	hasPrevious  bool
	// 仅比较已经读取且在 index.bin 中相邻的 index，过滤跳过的区间不补读。
	previousIndexEnd     uint64
	previousTimestampEnd uint64
	previousValuesEnd    uint64
	hasPreviousIndex     bool
	err                  error
}

func (r *downsampleReader) Init(p *part, resolution int64) error {
	if p == nil || !validDownsampleResolution(resolution) {
		r.reset()
		return fmt.Errorf("无效降采样源或分辨率")
	}
	// part 不可变，同一源的窗口切换复用文件句柄和工作缓冲。
	if r.p == p {
		r.resolution = resolution
		r.SetFilter(nil, minUnixMilli, maxUnixMilli)
		return nil
	}
	r.reset()
	r.p = p
	r.resolution = resolution
	if p.dsMetadata != nil {
		r.files = p.dsFiles
		r.fileSizes = p.dsFileSizes
	} else if p.path != "" {
		r.ownFiles = true
		for i, name := range []string{timestampsFilename, valuesFilename, indexFilename} {
			f, err := os.Open(filepath.Join(p.path, name))
			if err != nil {
				r.reset()
				return err
			}
			r.files[i] = f
			st, err := f.Stat()
			if err != nil {
				r.reset()
				return err
			}
			r.fileSizes[i] = uint64(st.Size())
		}
	} else {
		for i, f := range []fs.MustReadAtCloser{p.timestampsFile, p.valuesFile, p.indexFile} {
			b, ok := f.(*chunkedbuffer.Buffer)
			if !ok {
				r.reset()
				return fmt.Errorf("不支持的 inmemory 原始缓冲")
			}
			r.fileSizes[i] = uint64(b.SizeBytes())
		}
	}
	r.SetFilter(nil, minUnixMilli, maxUnixMilli)
	return nil
}

// SetFilter 重置索引游标；筛选相交 block，不提前过滤 block 内的样本贡献。
func (r *downsampleReader) SetFilter(tsid *TSID, minTimestamp, maxTimestamp int64) {
	r.hasFilter = tsid != nil
	r.filterTSID = TSID{}
	if tsid != nil {
		r.filterTSID = *tsid
	}
	r.minTimestamp = minTimestamp
	r.maxTimestamp = maxTimestamp
	r.metaPos = 0
	r.metaEnd = 0
	r.indexPos = 0
	r.indexData = r.indexData[:0]
	r.current = downsampleBlockHeader{}
	r.previous = downsampleBlockHeader{}
	r.hasPrevious = false
	r.previousIndexEnd = 0
	r.previousTimestampEnd = 0
	r.previousValuesEnd = 0
	r.hasPreviousIndex = false
	r.err = nil
	if minTimestamp > maxTimestamp {
		r.err = fmt.Errorf("无效降采样读取窗口")
		return
	}
	if r.p == nil {
		return
	}
	if r.p.dsMetadata != nil {
		rows := r.p.dsMetaindex
		start := sort.Search(len(rows), func(i int) bool { return rows[i].ResolutionMs >= r.resolution })
		end := start + sort.Search(len(rows)-start, func(i int) bool { return rows[start+i].ResolutionMs > r.resolution })
		if r.hasFilter {
			// 同一分辨率下 LastTSID 单调，保留所有可能包含目标的边界 index。
			start += sort.Search(end-start, func(i int) bool { return !rows[start+i].LastTSID.Less(&r.filterTSID) })
		}
		r.metaPos = start
		r.metaEnd = end
		return
	}
	rows := r.p.metaindex
	r.metaEnd = len(rows)
	if r.hasFilter {
		// 原始 metaindex 只有首 TSID，相等边界前一块也可能包含目标。
		start := sort.Search(len(rows), func(i int) bool { return !rows[i].TSID.Less(&r.filterTSID) })
		if start > 0 {
			start--
		}
		r.metaPos = start
	}
}

func (r *downsampleReader) Header() *downsampleBlockHeader {
	return &r.current
}

func (r *downsampleReader) Error() error {
	return r.err
}

func (r *downsampleReader) NextHeader() bool {
	if r.err != nil || r.p == nil {
		return false
	}
	for {
		if r.indexPos >= len(r.indexData) {
			ok, err := r.nextIndex()
			if err != nil {
				r.err = fmt.Errorf("读取 part %q 的 index: %w", r.p.path, err)
				return false
			}
			if !ok {
				return false
			}
		}
		var h downsampleBlockHeader
		if r.p.dsMetadata != nil {
			if err := h.unmarshal(r.indexData[r.indexPos : r.indexPos+downsampleBlockHeaderSize]); err != nil {
				r.err = err
				return false
			}
			r.indexPos += downsampleBlockHeaderSize
		} else {
			var bh blockHeader
			if _, err := bh.Unmarshal(r.indexData[r.indexPos : r.indexPos+marshaledBlockHeaderSize]); err != nil {
				r.err = err
				return false
			}
			r.indexPos += marshaledBlockHeaderSize
			h = downsampleBlockHeader{TSID: bh.TSID, ResolutionMs: r.resolution, RowsCount: bh.RowsCount, MinTimestamp: bh.MinTimestamp, MaxTimestamp: bh.MaxTimestamp, raw: true, rawHeader: bh}
		}
		if r.hasPrevious && (h.less(&r.previous) || (!h.raw && !r.previous.less(&h))) {
			r.err = fmt.Errorf("part %q 的 block header 排序错误", r.p.path)
			return false
		}
		r.previous = h
		r.hasPrevious = true
		if r.hasFilter && r.filterTSID.Less(&h.TSID) {
			r.metaPos = r.metaEnd
			r.indexPos = len(r.indexData)
			return false
		}
		if h.ResolutionMs != r.resolution || (r.hasFilter && h.TSID != r.filterTSID) || h.MaxTimestamp < r.minTimestamp || h.MinTimestamp > r.maxTimestamp {
			continue
		}
		r.current = h
		return true
	}
}

func (r *downsampleReader) nextIndex() (bool, error) {
	for {
		var off uint64
		var size, count uint32
		var dm *downsampleMetaindexRow
		if r.p.dsMetadata != nil {
			if r.metaPos >= r.metaEnd {
				return false, nil
			}
			m := &r.p.dsMetaindex[r.metaPos]
			r.metaPos++
			if r.hasFilter && r.filterTSID.Less(&m.TSID) {
				r.metaPos = r.metaEnd
				return false, nil
			}
			if m.MaxTimestamp < r.minTimestamp || m.MinTimestamp > r.maxTimestamp || (r.hasFilter && m.LastTSID.Less(&r.filterTSID)) {
				continue
			}
			dm = m
			off = m.IndexBlockOffset
			size = m.IndexBlockSize
			count = m.BlockHeadersCount
		} else {
			if r.metaPos >= r.metaEnd {
				return false, nil
			}
			pos := r.metaPos
			m := &r.p.metaindex[pos]
			r.metaPos++
			if r.hasFilter && r.filterTSID.Less(&m.TSID) {
				r.metaPos = r.metaEnd
				return false, nil
			}
			if m.MaxTimestamp < r.minTimestamp || m.MinTimestamp > r.maxTimestamp {
				continue
			}
			if r.hasFilter && pos+1 < r.metaEnd && r.p.metaindex[pos+1].TSID.Less(&r.filterTSID) {
				continue
			}
			off = m.IndexBlockOffset
			size = m.IndexBlockSize
			count = m.BlockHeadersCount
		}
		if size > downsampleMaxIndexSize || count == 0 {
			return false, fmt.Errorf("index block 大小或条目数无效")
		}
		var err error
		r.compressed, err = r.readAt(r.compressed[:0], 2, off, size)
		if err != nil {
			return false, err
		}
		data := r.compressed
		headerSize := marshaledBlockHeaderSize
		if dm != nil {
			if len(data) < len(downsampleIndexMagic) || string(data[:len(downsampleIndexMagic)]) != downsampleIndexMagic {
				return false, fmt.Errorf("降采样 index 标识错误")
			}
			data = data[len(downsampleIndexMagic):]
			headerSize = downsampleFieldHeaderSize
		}
		if uint64(count)*uint64(headerSize) > maxBlockSize {
			return false, fmt.Errorf("index 解码长度超过上限")
		}
		r.indexData, err = encoding.DecompressZSTDLimited(r.indexData[:0], data, maxBlockSize)
		if err != nil {
			return false, err
		}
		if len(r.indexData) != int(count)*headerSize {
			return false, fmt.Errorf("index 长度与 header 数量矛盾")
		}
		if dm != nil {
			if err := r.validateIndex(dm); err != nil {
				return false, err
			}
		}
		r.indexPos = 0
		return true, nil
	}
}

func (r *downsampleReader) validateIndex(m *downsampleMetaindexRow) error {
	var rows uint64
	var minTime, maxTime int64
	var first, last TSID
	var previous downsampleBlockHeader
	for pos := 0; pos < len(r.indexData); pos += downsampleBlockHeaderSize {
		var h downsampleBlockHeader
		if err := h.unmarshal(r.indexData[pos : pos+downsampleBlockHeaderSize]); err != nil {
			return err
		}
		if h.ResolutionMs != m.ResolutionMs {
			return fmt.Errorf("index block 混合分辨率")
		}
		if pos == 0 {
			if r.hasPreviousIndex && m.IndexBlockOffset == r.previousIndexEnd && (h.Timestamps.Offset != r.previousTimestampEnd || h.Columns[0].Offset != r.previousValuesEnd) {
				return fmt.Errorf("相邻 index block 的降采样负载不连续")
			}
			first = h.TSID
			minTime = h.MinTimestamp
			maxTime = h.MaxTimestamp
		} else if !previous.less(&h) {
			return fmt.Errorf("index block 排序错误")
		}
		if pos > 0 && (h.Timestamps.Offset != previous.Timestamps.Offset+uint64(previous.Timestamps.Size) || h.Columns[0].Offset != previous.Columns[downsampleFeaturesCount-1].Offset+uint64(previous.Columns[downsampleFeaturesCount-1].Size)) {
			return fmt.Errorf("相邻降采样 block 负载不连续")
		}
		if pos == 0 && m.IndexBlockOffset == 0 && (h.Timestamps.Offset != 0 || h.Columns[0].Offset != 0) {
			return fmt.Errorf("降采样负载首偏移不是零")
		}
		last = h.TSID
		previous = h
		rows += uint64(h.RowsCount) * downsampleFeaturesCount
		minTime = min(minTime, h.MinTimestamp)
		maxTime = max(maxTime, h.MaxTimestamp)
		if err := checkDownsampleExtent(h.Timestamps.Offset, h.Timestamps.Size, r.fileSizes[0]); err != nil {
			return err
		}
		for i := range h.Columns {
			if err := checkDownsampleExtent(h.Columns[i].Offset, h.Columns[i].Size, r.fileSizes[1]); err != nil {
				return err
			}
		}
	}
	if m.IndexBlockOffset+uint64(m.IndexBlockSize) == r.fileSizes[2] && (previous.Timestamps.Offset+uint64(previous.Timestamps.Size) != r.fileSizes[0] || previous.Columns[downsampleFeaturesCount-1].Offset+uint64(previous.Columns[downsampleFeaturesCount-1].Size) != r.fileSizes[1]) {
		return fmt.Errorf("降采样负载存在截断或未引用尾部")
	}
	if rows != m.RowsCount || first != m.TSID || last != m.LastTSID || minTime != m.MinTimestamp || maxTime != m.MaxTimestamp {
		return fmt.Errorf("index 与 metaindex 统计矛盾")
	}
	r.previousIndexEnd = m.IndexBlockOffset + uint64(m.IndexBlockSize)
	r.previousTimestampEnd = previous.Timestamps.Offset + uint64(previous.Timestamps.Size)
	r.previousValuesEnd = previous.Columns[downsampleFeaturesCount-1].Offset + uint64(previous.Columns[downsampleFeaturesCount-1].Size)
	r.hasPreviousIndex = true
	return nil
}

// prepareBlockPayload 仅约束压缩帧展开大小；实际数值解码和状态转换均由 Block 完成。
func (r *downsampleReader) prepareBlockPayload(src []byte, mt encoding.MarshalType, rows uint32) ([]byte, encoding.MarshalType, error) {
	if rows < 1 || rows > downsampleMaxRawRows {
		return nil, mt, fmt.Errorf("无效 Block 行数 %d", rows)
	}
	if err := validateDownsampleRowCodec(mt, rows); err != nil {
		return nil, mt, err
	}
	if mt != encoding.MarshalTypeZSTDNearestDelta && mt != encoding.MarshalTypeZSTDNearestDelta2 {
		return src, mt, nil
	}
	var err error
	r.decompressed, err = encoding.DecompressZSTDLimited(r.decompressed[:0], src, int(rows)*10)
	if err != nil {
		return nil, mt, err
	}
	src = append(src[:0], r.decompressed...)
	if mt == encoding.MarshalTypeZSTDNearestDelta {
		mt = encoding.MarshalTypeNearestDelta
	} else {
		mt = encoding.MarshalTypeNearestDelta2
	}
	return src, mt, nil
}

func (r *downsampleReader) readAt(dst []byte, file int, off uint64, size uint32) ([]byte, error) {
	if err := checkDownsampleExtent(off, size, r.fileSizes[file]); err != nil {
		return dst, err
	}
	if cap(dst) < int(size) {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	if size == 0 {
		return dst, nil
	}
	if r.files[file] != nil {
		_, err := r.files[file].ReadAt(dst, int64(off))
		return dst, err
	}
	f := []fs.MustReadAtCloser{r.p.timestampsFile, r.p.valuesFile, r.p.indexFile}[file]
	f.MustReadAt(dst, int64(off))
	return dst, nil
}

// FieldHeader 返回单个特征的原生 header，供现有 BlockRef 读取链路使用。
func (r *downsampleReader) FieldHeader(feature uint8) (blockHeader, error) {
	if r.p == nil || r.current.RowsCount == 0 || feature >= downsampleFeaturesCount {
		return blockHeader{}, fmt.Errorf("降采样 reader 未定位 Block 或特征无效")
	}
	if r.current.raw {
		return r.current.rawHeader, nil
	}
	fh := r.current.fieldHeader(int(feature))
	return fh.BlockHeader, nil
}

// ReadFieldBlock 读取一个分辨率、一个特征对应的原生 Block，并完成其解码状态转换。
func (r *downsampleReader) ReadFieldBlock(dst *Block, feature uint8) error {
	if r.p == nil || r.current.RowsCount == 0 || feature >= downsampleFeaturesCount {
		return fmt.Errorf("降采样 reader 未定位 Block 或特征无效")
	}
	if r.current.raw {
		return r.readNativeBlock(dst, &r.current.rawHeader)
	}
	fh := r.current.fieldHeader(int(feature))
	return r.readNativeBlock(dst, &fh.BlockHeader)
}

func (r *downsampleReader) readNativeBlock(b *Block, h *blockHeader) error {
	b.Reset()
	if err := h.validate(); err != nil {
		return err
	}
	b.bh = *h
	var err error
	b.timestampsData, err = r.readAt(b.timestampsData[:0], 0, h.TimestampsBlockOffset, h.TimestampsBlockSize)
	if err != nil {
		return err
	}
	b.valuesData, err = r.readAt(b.valuesData[:0], 1, h.ValuesBlockOffset, h.ValuesBlockSize)
	if err != nil {
		return err
	}
	b.timestampsData, b.bh.TimestampsMarshalType, err = r.prepareBlockPayload(b.timestampsData, b.bh.TimestampsMarshalType, b.bh.RowsCount)
	if err != nil {
		return err
	}
	b.valuesData, b.bh.ValuesMarshalType, err = r.prepareBlockPayload(b.valuesData, b.bh.ValuesMarshalType, b.bh.RowsCount)
	if err != nil {
		return err
	}
	b.bh.TimestampsBlockSize = uint32(len(b.timestampsData))
	b.bh.ValuesBlockSize = uint32(len(b.valuesData))
	return b.UnmarshalData()
}

func (r *downsampleReader) ReadBlock(b *downsampleBatch) error {
	if r.p == nil || r.current.RowsCount == 0 {
		return fmt.Errorf("降采样 reader 未定位批次")
	}
	b.Reset()
	h := &r.current
	b.tsid, b.resolution = h.TSID, r.resolution
	if h.raw {
		return r.readRawBlock(b, &h.rawHeader)
	}
	b.precisionBits = h.Timestamps.PrecisionBits
	for i := range b.values {
		if err := r.ReadFieldBlock(&r.block, uint8(i)); err != nil {
			return err
		}
		if i == 0 {
			b.timestamps = append(b.timestamps[:0], r.block.timestamps...)
		}
		b.values[i] = decimal.AppendDecimalToFloat(b.values[i][:0], r.block.values, r.block.bh.Scale)
		for j, v := range b.values[i] {
			if math.IsNaN(v) {
				b.values[i][j] = decimal.StaleNaN
			}
		}
	}
	return nil
}

func (r *downsampleReader) readRawBlock(b *downsampleBatch, h *blockHeader) error {
	if err := r.readNativeBlock(&r.block, h); err != nil {
		return err
	}
	b.timestamps = append(b.timestamps[:0], r.block.timestamps...)
	b.values[0] = decimal.AppendDecimalToFloat(b.values[0][:0], r.block.values, r.block.bh.Scale)
	for j, v := range b.values[0] {
		if math.IsNaN(v) {
			v = decimal.StaleNaN
			b.values[0][j] = v
		}
		for i := 1; i < downsampleFeaturesCount; i++ {
			x := v
			if i == downsampleFeatureCount {
				x = 1
			}
			b.values[i] = append(b.values[i], x)
		}
	}
	b.precisionBits = h.PrecisionBits
	return nil
}

func (r *downsampleReader) reset() {
	if r.ownFiles {
		for _, f := range r.files {
			if f != nil {
				_ = f.Close()
			}
		}
	}
	r.p = nil
	r.files = [3]*os.File{}
	r.fileSizes = [3]uint64{}
	r.ownFiles = false
	r.resolution = 0
	r.SetFilter(nil, 0, 0)
	for _, p := range []*[]byte{&r.indexData, &r.compressed} {
		if cap(*p) > downsampleMaxIndexSize {
			*p = nil
		} else {
			*p = (*p)[:0]
		}
	}
	if cap(r.decompressed) > downsampleMaxPooledRows*10 {
		r.decompressed = nil
	} else {
		r.decompressed = r.decompressed[:0]
	}
	r.block.Reset()
}

func getDownsampleReader() *downsampleReader {
	if v := downsampleReaderPool.Get(); v != nil {
		return v.(*downsampleReader)
	}
	return &downsampleReader{}
}
func putDownsampleReader(r *downsampleReader) {
	r.reset()
	downsampleReaderPool.Put(r)
}

var downsampleReaderPool sync.Pool
