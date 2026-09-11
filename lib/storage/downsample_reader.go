package storage

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/chunkedbuffer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// downsampleReader 仅供降采样合并读取 raw inmemory、raw 磁盘及降采样磁盘源。
// 调用方必须在 reader 存活期间持有 part 引用；返回的 header 在下次迭代时失效。
type downsampleReader struct {
	p                *part                                            // 当前源，由调用方持有引用；切换源或 Close 时清除。
	resolution       int64                                            // 当前合并分辨率；Init 重建此分辨率的索引扫描范围。
	timestampsReader filestream.ReadAtCloser                          // timestamps.bin；磁盘源由当前 reader 独立打开和关闭，inmemory 为 nil。
	valuesReader     filestream.ReadAtCloser                          // values.bin；所有权及生命周期与 timestampsReader 一致。
	indexReader      filestream.ReadAtCloser                          // index.bin；当前 reader 自有，五个特征游标共用此句柄。
	timestampsSize   uint64                                           // 当前源时间戳文件或内存缓冲大小，用于读取范围校验。
	valuesSize       uint64                                           // 当前源值文件或内存缓冲大小，用于读取范围校验。
	indexSize        uint64                                           // 当前源索引文件或内存缓冲大小，用于读取范围校验。
	featureIndexes   [countOfDownsampleFeatures]downsampleIndexCursor // last 驱动遍历，其余游标仅对齐当前 block 的特征列；raw 只使用 last。
	decompressed     []byte                                           // 单个时间戳或值 payload 的限长解压缓冲，不缓存整个文件。
	block            Block                                            // 当前列的原生解码工作区；一次 ReadBlock 的五列共用其时间戳。
	err              error                                            // 当前索引读取错误；Init 或 SeekTSID 重新定位时清除。
}

// downsampleIndexCursor 仅保存一个特征当前 index block 的迭代和校验状态。
// 游标直接内嵌在 reader 中，不持有文件、part 引用或需要归还的对象。
type downsampleIndexCursor struct {
	metaPos      int           // 下一条待读取的 metaindex 位置。
	metaEnd      int           // 当前扫描范围的 metaindex 结束位置，不包含此位置。
	indexPos     int           // 当前 indexHeaders 中下一条 header 的位置。
	indexData    []byte        // 当前单个 index block 的解压工作区，跨索引读取复用容量。
	indexHeaders []blockHeader // 当前 index block 的原生 header，不缓存整个 TSID 的 header。
	compressed   []byte        // 单个 index block 的压缩读取缓冲。
	current      blockHeader   // 当前 header，下次推进或重新定位时失效。
	previous     blockHeader   // 上一条已遍历 header，用于检查 raw 和降采样源的顺序。
	hasPrevious  bool          // previous 是否有效。

	// 只校验当前连续扫描的 index；SeekTSID 定位前的区间不补读。
	previousIndexEnd     uint64      // 上一 index 的结束偏移，用于识别物理相邻索引。
	previousTimestampEnd uint64      // 上一 index 最后一个时间戳 payload 的结束偏移。
	previousValuesEnd    uint64      // 上一 index 最后一个值 payload 的结束偏移。
	previousIndexHeader  blockHeader // 上一 index 的末尾 header，用于跨 index 顺序校验。
	hasPreviousIndex     bool        // 跨 index 校验状态是否有效。
}

func (r *downsampleReader) Init(p *part, resolution int64) (err error) {
	if p == nil || !validDownsampleResolution(resolution) {
		return errors.Join(fmt.Errorf("[downsampling] invalid reader source or resolution"), r.Close())
	}
	// part 不可变，同一源切换分辨率或 TSID 时复用自有文件和工作缓冲。
	if r.p != p {
		if err := r.Close(); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				err = errors.Join(err, r.Close())
			}
		}()
		r.p = p
		if p.path != "" {
			if err := openDownsamplePartDataFile(p.path, timestampsFilename, &r.timestampsReader, &r.timestampsSize); err != nil {
				return err
			}
			if err := openDownsamplePartDataFile(p.path, valuesFilename, &r.valuesReader, &r.valuesSize); err != nil {
				return err
			}
			if err := openDownsamplePartDataFile(p.path, indexFilename, &r.indexReader, &r.indexSize); err != nil {
				return err
			}
		} else {
			if r.timestampsSize, err = downsampleInmemoryReaderSize(p.timestampsFile); err != nil {
				return err
			}
			if r.valuesSize, err = downsampleInmemoryReaderSize(p.valuesFile); err != nil {
				return err
			}
			if r.indexSize, err = downsampleInmemoryReaderSize(p.indexFile); err != nil {
				return err
			}
		}
	}
	r.resolution = resolution
	r.initIndexCursors()
	return nil
}

// Close 关闭当前 reader 的全部自有磁盘句柄，不操作 part 持有的查询读取对象。
// 即使关闭失败也尝试其余文件；清除句柄后，重复调用不会再次关闭。
func (r *downsampleReader) Close() error {
	var closeErr error
	for _, reader := range []*filestream.ReadAtCloser{&r.timestampsReader, &r.valuesReader, &r.indexReader} {
		f := *reader
		*reader = nil
		if f != nil {
			if err := f.Close(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("[downsampling] cannot close reader file %q: %w", f.Path(), err))
			}
		}
	}
	for i := range r.featureIndexes {
		c := &r.featureIndexes[i]
		for _, b := range []*[]byte{&c.indexData, &c.compressed} {
			if cap(*b) > downsampleMaxIndexSize {
				*b = nil
			} else {
				*b = (*b)[:0]
			}
		}
		if cap(c.indexHeaders) > maxBlockSize/marshaledBlockHeaderSize {
			c.indexHeaders = nil
		} else {
			c.indexHeaders = c.indexHeaders[:0]
		}
		*c = downsampleIndexCursor{indexData: c.indexData, indexHeaders: c.indexHeaders, compressed: c.compressed}
	}
	if cap(r.decompressed) > downsampleMaxPooledRows*10 {
		r.decompressed = nil
	} else {
		r.decompressed = r.decompressed[:0]
	}
	r.block.Reset()
	*r = downsampleReader{featureIndexes: r.featureIndexes, decompressed: r.decompressed, block: r.block}
	return closeErr
}

// SeekTSID 将全部特征游标定位到可能包含当前合并 TSID 的索引区间，并读出首条匹配 header。
// 不保存筛选条件；调用方逐个消费 block，并在 Header().TSID 改变时停止当前 TSID。
func (r *downsampleReader) SeekTSID(tsid TSID) bool {
	r.initIndexCursors()
	if r.p == nil {
		return false
	}
	for feature := range r.featureIndexes {
		c := &r.featureIndexes[feature]
		if r.p.dsMetadata != nil {
			rows := r.p.dsMetaindex
			c.metaPos += sort.Search(c.metaEnd-c.metaPos, func(i int) bool { return !rows[c.metaPos+i].LastTSID.Less(&tsid) })
			c.metaEnd = c.metaPos + sort.Search(c.metaEnd-c.metaPos, func(i int) bool { return tsid.Less(&rows[c.metaPos+i].TSID) })
		} else if feature == downsampleFeatureLast {
			rows := r.p.metaindex
			// raw metaindex 只有首 TSID；相等边界前一块也可能包含目标。
			c.metaPos = sort.Search(len(rows), func(i int) bool { return !rows[i].TSID.Less(&tsid) })
			if c.metaPos > 0 {
				c.metaPos--
			}
			c.metaEnd = sort.Search(len(rows), func(i int) bool { return tsid.Less(&rows[i].TSID) })
		}
	}
	for r.NextHeader() {
		h := r.Header()
		if !h.TSID.Less(&tsid) {
			return h.TSID == tsid
		}
	}
	return false
}

func (r *downsampleReader) NextHeader() bool {
	return r.featureIndexes[downsampleFeatureLast].nextHeader(r)
}

func (r *downsampleReader) Header() *blockHeader {
	return &r.featureIndexes[downsampleFeatureLast].current
}

func (r *downsampleReader) ReadBlock(b *downsampleDecodedResolutionFeaturesBlock) error {
	if r.err != nil {
		return r.err
	}
	h := r.Header()
	if r.p == nil || h.RowsCount == 0 {
		return fmt.Errorf("[downsampling] reader is not positioned at a block")
	}
	b.Reset()
	b.tsid, b.resolution = h.TSID, r.resolution
	if r.p.dsMetadata == nil {
		return r.readRawBlock(b, h)
	}
	b.precisionBits = h.PrecisionBits
	for feature := range b.values {
		if feature == downsampleFeatureLast {
			// 首列完整解码时间戳；其余四列只更换原生 Block 的值列。
			if err := r.readNativeBlock(&r.block, h); err != nil {
				return err
			}
			b.timestamps = append(b.timestamps[:0], r.block.timestamps...)
		} else {
			column, err := r.readFeatureHeader(uint8(feature))
			if err != nil {
				return err
			}
			if err := r.readNativeValues(&r.block, &column, h); err != nil {
				return err
			}
		}
		b.values[feature] = decimal.AppendDecimalToFloat(b.values[feature][:0], r.block.values, r.block.bh.Scale)
	}
	return nil
}

func (r *downsampleReader) Error() error {
	return r.err
}

// initIndexCursors 清除上次遍历状态，按当前分辨率建立五列各自的完整索引区间。
// Init 和 SeekTSID 共用此处；仅复用缓冲，不打开或关闭文件。
func (r *downsampleReader) initIndexCursors() {
	r.err = nil
	for feature := range r.featureIndexes {
		c := &r.featureIndexes[feature]
		*c = downsampleIndexCursor{indexData: c.indexData[:0], indexHeaders: c.indexHeaders[:0], compressed: c.compressed[:0]}
		if r.p == nil {
			continue
		}
		if r.p.dsMetadata == nil {
			if feature == downsampleFeatureLast {
				c.metaEnd = len(r.p.metaindex)
			}
			continue
		}
		rows := r.p.dsMetaindex
		start := sort.Search(len(rows), func(i int) bool {
			return rows[i].ResolutionMs > r.resolution || (rows[i].ResolutionMs == r.resolution && rows[i].feature >= uint8(feature))
		})
		end := start + sort.Search(len(rows)-start, func(i int) bool {
			return rows[start+i].ResolutionMs > r.resolution || rows[start+i].feature > uint8(feature)
		})
		c.metaPos, c.metaEnd = start, end
	}
}

// readFeatureHeader 只为当前 last block 对齐其余特征，验证五列共用的时间戳描述。
func (r *downsampleReader) readFeatureHeader(feature uint8) (blockHeader, error) {
	if r.err != nil {
		return blockHeader{}, r.err
	}
	h := r.Header()
	if r.p == nil || h.RowsCount == 0 || feature >= countOfDownsampleFeatures {
		return blockHeader{}, fmt.Errorf("[downsampling] reader is not positioned at a block or the feature is invalid")
	}
	if r.p.dsMetadata == nil || feature == downsampleFeatureLast {
		return *h, nil
	}
	c := &r.featureIndexes[feature]
	for c.current.RowsCount == 0 || downsampleHeaderLess(&c.current, h) {
		if !c.nextHeader(r) {
			if r.err != nil {
				return blockHeader{}, r.err
			}
			return blockHeader{}, fmt.Errorf("[downsampling] missing feature block")
		}
	}
	if !sameDownsampleTimestamps(h, &c.current) {
		return blockHeader{}, fmt.Errorf("[downsampling] feature blocks in the same block group have different timestamp descriptors")
	}
	return c.current, nil
}

func downsampleInmemoryReaderSize(f fs.MustReadAtCloser) (uint64, error) {
	b, ok := f.(*chunkedbuffer.Buffer)
	if !ok {
		return 0, fmt.Errorf("[downsampling] unsupported raw inmemory buffer")
	}
	return uint64(b.SizeBytes()), nil
}

func (c *downsampleIndexCursor) nextHeader(r *downsampleReader) bool {
	c.current = blockHeader{}
	if r.err != nil || r.p == nil {
		return false
	}
	if c.indexPos >= len(c.indexHeaders) {
		ok, err := c.nextIndex(r)
		if err != nil {
			r.err = fmt.Errorf("[downsampling] cannot read index for part %q: %w", r.p.path, err)
			return false
		}
		if !ok {
			return false
		}
	}
	h := c.indexHeaders[c.indexPos]
	c.indexPos++
	if c.hasPrevious && (downsampleHeaderLess(&h, &c.previous) || (r.p.dsMetadata != nil && !downsampleHeadersOrdered(&c.previous, &h))) {
		r.err = fmt.Errorf("[downsampling] block headers are out of order in part %q", r.p.path)
		return false
	}
	c.previous = h
	c.hasPrevious = true
	c.current = h
	return true
}

func (c *downsampleIndexCursor) nextIndex(r *downsampleReader) (bool, error) {
	if c.metaPos >= c.metaEnd {
		return false, nil
	}
	var off uint64
	var size, count uint32
	var dm *downsampleMetaindexRow
	if r.p.dsMetadata != nil {
		dm = &r.p.dsMetaindex[c.metaPos]
		off, size, count = dm.IndexBlockOffset, dm.IndexBlockSize, dm.BlockHeadersCount
	} else {
		m := &r.p.metaindex[c.metaPos]
		off, size, count = m.IndexBlockOffset, m.IndexBlockSize, m.BlockHeadersCount
	}
	c.metaPos++
	if size > downsampleMaxIndexSize || count == 0 {
		return false, fmt.Errorf("[downsampling] invalid index block size or header count")
	}
	var err error
	c.compressed, err = r.readAt(c.compressed[:0], r.indexReader, r.p.indexFile, r.indexSize, off, size)
	if err != nil {
		return false, err
	}
	data := c.compressed
	if dm != nil {
		if len(data) < len(downsampleIndexMagic) || string(data[:len(downsampleIndexMagic)]) != downsampleIndexMagic {
			return false, fmt.Errorf("[downsampling] invalid index magic")
		}
		data = data[len(downsampleIndexMagic):]
	}
	if uint64(count)*uint64(marshaledBlockHeaderSize) > maxBlockSize {
		return false, fmt.Errorf("[downsampling] decoded index size exceeds the limit")
	}
	c.indexData, err = encoding.DecompressZSTDLimited(c.indexData[:0], data, maxBlockSize)
	if err != nil {
		return false, err
	}
	if len(c.indexData) != int(count)*marshaledBlockHeaderSize {
		return false, fmt.Errorf("[downsampling] index size does not match the header count")
	}
	if dm != nil {
		c.indexHeaders, err = unmarshalDownsampleIndexBlock(c.indexHeaders[:0], c.indexData, dm, r.timestampsSize, r.valuesSize, r.indexSize)
		if err != nil {
			return false, err
		}
		first := &c.indexHeaders[0]
		if c.hasPreviousIndex && !downsampleHeadersOrdered(&c.previousIndexHeader, first) {
			return false, fmt.Errorf("[downsampling] index blocks are out of order or have overlapping time ranges")
		}
		if c.hasPreviousIndex && dm.IndexBlockOffset == c.previousIndexEnd && (first.TimestampsBlockOffset != c.previousTimestampEnd || first.ValuesBlockOffset != c.previousValuesEnd) {
			return false, fmt.Errorf("[downsampling] payloads are not contiguous across adjacent index blocks")
		}
		last := &c.indexHeaders[len(c.indexHeaders)-1]
		c.previousIndexEnd = dm.IndexBlockOffset + uint64(dm.IndexBlockSize)
		c.previousTimestampEnd = last.TimestampsBlockOffset + uint64(last.TimestampsBlockSize)
		c.previousValuesEnd = last.ValuesBlockOffset + uint64(last.ValuesBlockSize)
		c.previousIndexHeader = *last
		c.hasPreviousIndex = true
	} else {
		c.indexHeaders, err = unmarshalBlockHeaders(c.indexHeaders[:0], c.indexData, int(count))
		if err != nil {
			return false, err
		}
	}
	c.indexPos = 0
	return true, nil
}

func (r *downsampleReader) readRawBlock(b *downsampleDecodedResolutionFeaturesBlock, h *blockHeader) error {
	if err := r.readNativeBlock(&r.block, h); err != nil {
		return err
	}
	b.timestamps = append(b.timestamps[:0], r.block.timestamps...)
	b.values[0] = decimal.AppendDecimalToFloat(b.values[0][:0], r.block.values, r.block.bh.Scale)
	for _, v := range b.values[0] {
		for i := 1; i < countOfDownsampleFeatures; i++ {
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

func (r *downsampleReader) readNativeBlock(b *Block, h *blockHeader) error {
	b.Reset()
	if err := h.validate(); err != nil {
		return fmt.Errorf("[downsampling] invalid native block header: %w", err)
	}
	if h.MinTimestamp > h.MaxTimestamp || h.MinTimestamp < minUnixMilli || h.MaxTimestamp > maxUnixMilli || (h.RowsCount == 1 && h.MinTimestamp != h.MaxTimestamp) {
		return fmt.Errorf("[downsampling] invalid block time range")
	}
	b.bh = *h
	var err error
	b.timestampsData, err = r.readAt(b.timestampsData[:0], r.timestampsReader, r.p.timestampsFile, r.timestampsSize, h.TimestampsBlockOffset, h.TimestampsBlockSize)
	if err != nil {
		return err
	}
	b.valuesData, err = r.readAt(b.valuesData[:0], r.valuesReader, r.p.valuesFile, r.valuesSize, h.ValuesBlockOffset, h.ValuesBlockSize)
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
	if err := b.UnmarshalData(); err != nil {
		return fmt.Errorf("[downsampling] cannot decode native block: %w", err)
	}
	// 原生 const/delta-const 不检查边界；有损编码则先沿用原生顺序修复。
	if len(b.timestamps) != int(h.RowsCount) || b.timestamps[0] != h.MinTimestamp || b.timestamps[len(b.timestamps)-1] != h.MaxTimestamp {
		return fmt.Errorf("[downsampling] decoded timestamp bounds or row count do not match the block header")
	}
	if err := checkTimestampsBounds(b.timestamps, h.MinTimestamp, h.MaxTimestamp); err != nil {
		return fmt.Errorf("[downsampling] invalid decoded timestamps: %w", err)
	}
	return nil
}

// readNativeValues 只更换原生 Block 的值列，复用本次 ReadBlock 已解码并校验的时间戳。
// shared 是首列的磁盘 header，不能使用 prepareBlockPayload 已转换过 codec 的 b.bh 替代。
func (r *downsampleReader) readNativeValues(b *Block, h, shared *blockHeader) error {
	if err := h.validate(); err != nil {
		return fmt.Errorf("[downsampling] invalid native block header: %w", err)
	}
	if !sameDownsampleTimestamps(shared, h) {
		return fmt.Errorf("[downsampling] feature blocks at the same resolution have different timestamp descriptors")
	}
	if len(b.timestamps) != int(h.RowsCount) || b.timestamps[0] != h.MinTimestamp || b.timestamps[len(b.timestamps)-1] != h.MaxTimestamp {
		return fmt.Errorf("[downsampling] shared decoded timestamp bounds or row count do not match the block header")
	}
	// 时间戳仍处于首列的原生解码状态；只清除上一列的值及相关编码状态。
	timestampsMarshalType, timestampsBlockSize := b.bh.TimestampsMarshalType, b.bh.TimestampsBlockSize
	b.bh = *h
	b.bh.TimestampsMarshalType, b.bh.TimestampsBlockSize = timestampsMarshalType, timestampsBlockSize
	b.nextIdx = 0
	b.headerData = b.headerData[:0]
	b.values = b.values[:0]
	var err error
	b.valuesData, err = r.readAt(b.valuesData[:0], r.valuesReader, r.p.valuesFile, r.valuesSize, h.ValuesBlockOffset, h.ValuesBlockSize)
	if err != nil {
		return err
	}
	b.valuesData, b.bh.ValuesMarshalType, err = r.prepareBlockPayload(b.valuesData, b.bh.ValuesMarshalType, b.bh.RowsCount)
	if err != nil {
		return err
	}
	b.bh.ValuesBlockSize = uint32(len(b.valuesData))
	// 直接复用原生值列解码器，保留已解码的时间戳，不扩展共享 Block 的解码入口。
	b.values, err = encoding.UnmarshalValues(b.values[:0], b.valuesData, b.bh.ValuesMarshalType, b.bh.FirstValue, int(b.bh.RowsCount))
	if err != nil {
		return fmt.Errorf("[downsampling] cannot decode native values: %w", err)
	}
	b.valuesData = b.valuesData[:0]
	if len(b.timestamps) != len(b.values) {
		return fmt.Errorf("[downsampling] timestamps and values count mismatch; got %d vs %d", len(b.timestamps), len(b.values))
	}
	return nil
}

func (r *downsampleReader) readAt(dst []byte, reader filestream.ReadAtCloser, inmemorySource fs.MustReadAtCloser, fileSize, off uint64, size uint32) ([]byte, error) {
	if err := checkDownsampleExtent(off, size, fileSize); err != nil {
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
	if reader != nil {
		if _, err := reader.ReadAt(dst, int64(off)); err != nil {
			return dst, fmt.Errorf("[downsampling] cannot read %d bytes at offset %d from %q: %w", size, off, reader.Path(), err)
		}
		return dst, nil
	}
	if r.p == nil || r.p.path != "" || r.p.dsMetadata != nil {
		return dst, fmt.Errorf("[downsampling] merge file handle is unavailable")
	}
	if inmemorySource == nil {
		return dst, fmt.Errorf("[downsampling] raw file handle is unavailable")
	}
	inmemorySource.MustReadAt(dst, int64(off))
	return dst, nil
}

func validateDownsampleRowCodec(mt encoding.MarshalType, rows uint32) error {
	if rows < 2 && (mt == encoding.MarshalTypeNearestDelta2 || mt == encoding.MarshalTypeZSTDNearestDelta2) {
		return fmt.Errorf("[downsampling] delta-of-delta encoding requires at least two rows")
	}
	return nil
}

// prepareBlockPayload 仅约束压缩帧展开大小；实际数值解码和状态转换均由 Block 完成。
func (r *downsampleReader) prepareBlockPayload(src []byte, mt encoding.MarshalType, rows uint32) ([]byte, encoding.MarshalType, error) {
	if rows < 1 || rows > downsampleMaxRawRows {
		return nil, mt, fmt.Errorf("[downsampling] invalid block row count %d", rows)
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
		return nil, mt, fmt.Errorf("[downsampling] cannot decompress block payload: %w", err)
	}
	src = append(src[:0], r.decompressed...)
	if mt == encoding.MarshalTypeZSTDNearestDelta {
		mt = encoding.MarshalTypeNearestDelta
	} else {
		mt = encoding.MarshalTypeNearestDelta2
	}
	return src, mt, nil
}

func getDownsampleReader() *downsampleReader {
	if v := downsampleReaderPool.Get(); v != nil {
		return v.(*downsampleReader)
	}
	return &downsampleReader{}
}

func putDownsampleReader(r *downsampleReader) error {
	err := r.Close()
	downsampleReaderPool.Put(r)
	return err
}

var downsampleReaderPool sync.Pool
