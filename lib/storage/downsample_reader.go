package storage

import (
	"errors"
	"fmt"
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
	// currentSourcePart 借用当前源，调用方持有的引用必须覆盖读取期间；切换源或 Close 时清除。
	currentSourcePart *part
	// currentResolution 指定当前合并分辨率；Init 重建此分辨率的索引扫描范围。
	currentResolution int64
	// timestampsReader 独立打开和关闭当前磁盘源的 timestamps.bin；inmemory 源为 nil。
	timestampsReader filestream.ReadAtCloser
	// valuesReader 独立打开和关闭当前磁盘源的 values.bin；按各特征 header 的偏移读取。
	valuesReader filestream.ReadAtCloser
	// indexReader 独立打开和关闭当前磁盘源的 index.bin；当前 reader 的五个特征游标共用此句柄。
	indexReader filestream.ReadAtCloser
	// timestampsFileSize 保存 timestamps.bin 或对应内存缓冲的大小，用于读取范围校验。
	timestampsFileSize uint64
	// valuesFileSize 保存 values.bin 或对应内存缓冲的大小，用于读取范围校验。
	valuesFileSize uint64
	// indexFileSize 保存 index.bin 或对应内存缓冲的大小，用于读取范围校验。
	indexFileSize uint64
	// currentResolutionFeatureIndexes 保存当前分辨率各特征的索引进度；last 驱动遍历，其余特征对齐当前 block，raw 只使用 last。
	currentResolutionFeatureIndexes [countOfDownsampleFeatures]downsampleIndexCursor
	// currentTSIDBlockHeaders 按值保存首次顺序扫描得到的当前 TSID 首列 header，供确定完整时间范围后直接读取；消费后清空，Close 时释放容量。
	currentTSIDBlockHeaders []blockHeader
	// currentBlockDecompressedPayload 复用单个时间戳或值 payload 的限长解压缓冲，不缓存整个文件。
	currentBlockDecompressedPayload []byte
	// currentFeatureBlock 复用当前特征的原生解码工作区；一次 ReadBlock 的五列共用其时间戳。
	currentFeatureBlock Block
	// readErr 保存当前索引遍历错误，使后续迭代停止；Init 开始新的分辨率扫描时清除。
	readErr error
}

// downsampleIndexCursor 仅保存一个特征当前 index block 的迭代和校验状态。
// 游标直接内嵌在 reader 中，不持有文件、part 引用或需要归还的对象。
type downsampleIndexCursor struct {
	// nextMetaindexRow 指向当前特征下一条待读取的 metaindex row。
	nextMetaindexRow int
	// metaindexRowsEnd 限定当前特征的索引扫描区间，不包含此位置。
	metaindexRowsEnd int
	// nextBlockHeader 指向当前 index block 中下一条待返回的 header。
	nextBlockHeader int
	// currentIndexBlockData 复用当前单个 index block 的解压缓冲。
	currentIndexBlockData []byte
	// currentIndexBlockHeaders 保存当前 index block 的原生 header；末项同时供下一 index 的边界校验使用。
	currentIndexBlockHeaders []blockHeader
	// currentIndexBlockCompressedData 复用当前单个 index block 的压缩读取缓冲。
	currentIndexBlockCompressedData []byte
	// currentBlockHeader 保存当前特征已遍历到的 header；下次推进或 Init 时失效，结束或错误时清零。
	currentBlockHeader blockHeader
	// currentIndexBlockEndOffset 标记当前 index block 的结束偏移，用于下一 index 的连续 payload 校验。
	currentIndexBlockEndOffset uint64
}

// Init 打开当前源的自有读取句柄，并顺序扫描内存中的 metaindex，建立当前分辨率各特征的完整遍历区间。
func (r *downsampleReader) Init(p *part, resolution int64) (err error) {
	if p == nil || !validDownsampleResolution(resolution) {
		return errors.Join(fmt.Errorf("[downsampling] invalid reader source or resolution"), r.Close())
	}
	// part 不可变，同一源切换分辨率时复用自有文件和工作缓冲。
	sourceChanged := r.currentSourcePart != p
	if sourceChanged {
		if err := r.Close(); err != nil {
			return err
		}
	}
	// 初始化新源或重新校验同一源失败时，都只在此处统一释放当前持有的句柄和缓存。
	defer func() {
		if err != nil {
			err = errors.Join(err, r.Close())
		}
	}()
	if sourceChanged {
		r.currentSourcePart = p
		if p.path != "" {
			if err := openDownsamplePartDataFile(p.path, timestampsFilename, &r.timestampsReader, &r.timestampsFileSize); err != nil {
				return err
			}
			if err := openDownsamplePartDataFile(p.path, valuesFilename, &r.valuesReader, &r.valuesFileSize); err != nil {
				return err
			}
			if err := openDownsamplePartDataFile(p.path, indexFilename, &r.indexReader, &r.indexFileSize); err != nil {
				return err
			}
		} else {
			if r.timestampsFileSize, err = downsampleInmemoryReaderSize(p.timestampsFile); err != nil {
				return err
			}
			if r.valuesFileSize, err = downsampleInmemoryReaderSize(p.valuesFile); err != nil {
				return err
			}
			if r.indexFileSize, err = downsampleInmemoryReaderSize(p.indexFile); err != nil {
				return err
			}
		}
	}
	r.currentResolution = resolution
	r.readErr = nil
	r.currentTSIDBlockHeaders = r.currentTSIDBlockHeaders[:0]
	for currentFeature := range r.currentResolutionFeatureIndexes {
		currentFeatureIndex := &r.currentResolutionFeatureIndexes[currentFeature]
		*currentFeatureIndex = downsampleIndexCursor{
			currentIndexBlockData:           currentFeatureIndex.currentIndexBlockData[:0],
			currentIndexBlockHeaders:        currentFeatureIndex.currentIndexBlockHeaders[:0],
			currentIndexBlockCompressedData: currentFeatureIndex.currentIndexBlockCompressedData[:0],
		}
	}
	if p.dsMetadata == nil {
		r.currentResolutionFeatureIndexes[downsampleFeatureLast].metaindexRowsEnd = len(p.metaindex)
		return nil
	}
	for currentMetaindexRow := range p.dsMetaindex {
		row := &p.dsMetaindex[currentMetaindexRow]
		if row.ResolutionMs < resolution {
			continue
		}
		if row.ResolutionMs > resolution {
			break
		}
		if row.feature >= countOfDownsampleFeatures {
			return fmt.Errorf("[downsampling] invalid feature %d in metaindex", row.feature)
		}
		currentFeatureIndex := &r.currentResolutionFeatureIndexes[row.feature]
		if currentFeatureIndex.metaindexRowsEnd == 0 {
			currentFeatureIndex.nextMetaindexRow = currentMetaindexRow
		}
		currentFeatureIndex.metaindexRowsEnd = currentMetaindexRow + 1
	}
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
	for i := range r.currentResolutionFeatureIndexes {
		c := &r.currentResolutionFeatureIndexes[i]
		for _, b := range []*[]byte{&c.currentIndexBlockData, &c.currentIndexBlockCompressedData} {
			if cap(*b) > downsampleMaxIndexSize {
				*b = nil
			} else {
				*b = (*b)[:0]
			}
		}
		if cap(c.currentIndexBlockHeaders) > maxBlockSize/marshaledBlockHeaderSize {
			c.currentIndexBlockHeaders = nil
		} else {
			c.currentIndexBlockHeaders = c.currentIndexBlockHeaders[:0]
		}
		*c = downsampleIndexCursor{currentIndexBlockData: c.currentIndexBlockData, currentIndexBlockHeaders: c.currentIndexBlockHeaders, currentIndexBlockCompressedData: c.currentIndexBlockCompressedData}
	}
	if cap(r.currentBlockDecompressedPayload) > downsampleMaxPooledRows*10 {
		r.currentBlockDecompressedPayload = nil
	} else {
		r.currentBlockDecompressedPayload = r.currentBlockDecompressedPayload[:0]
	}
	r.currentFeatureBlock.Reset()
	*r = downsampleReader{currentResolutionFeatureIndexes: r.currentResolutionFeatureIndexes, currentBlockDecompressedPayload: r.currentBlockDecompressedPayload, currentFeatureBlock: r.currentFeatureBlock}
	return closeErr
}

// NextHeader 顺序推进 last 或 raw 首列索引，不读取时间戳和值负载；合并堆只使用此遍历位置。
func (r *downsampleReader) NextHeader() bool {
	return r.currentResolutionFeatureIndexes[downsampleFeatureLast].nextHeader(r)
}

// Header 返回首列当前 header；调用方如需推进后读取该 block，必须先按值保存。
func (r *downsampleReader) Header() *blockHeader {
	return &r.currentResolutionFeatureIndexes[downsampleFeatureLast].currentBlockHeader
}

// ReadBlock 按首次扫描保存的首列 header 读取一个多特征 block，不改变合并堆使用的首列遍历位置。
// 调用方按同一源的 TSID、时间顺序传入 header；其余特征游标只向前推进，时间戳只解码一次。
func (r *downsampleReader) ReadBlock(b *downsampleDecodedResolutionFeaturesBlock, currentBlockHeader *blockHeader) error {
	if r.readErr != nil {
		return r.readErr
	}
	h := currentBlockHeader
	if r.currentSourcePart == nil || h == nil || h.RowsCount == 0 {
		return fmt.Errorf("[downsampling] reader source or block header is unavailable")
	}
	b.Reset()
	b.tsid, b.resolution = h.TSID, r.currentResolution
	// raw 与降采样首列共用一次原生解码，后续特征只处理值列。
	if err := r.readNativeBlock(&r.currentFeatureBlock, h); err != nil {
		return err
	}
	b.timestamps = append(b.timestamps, r.currentFeatureBlock.timestamps...)
	b.values[downsampleFeatureLast] = decimal.AppendDecimalToFloat(b.values[downsampleFeatureLast], r.currentFeatureBlock.values, r.currentFeatureBlock.bh.Scale)
	b.precisionBits = h.PrecisionBits
	for currentFeature := uint8(downsampleFeatureLast + 1); currentFeature < countOfDownsampleFeatures; currentFeature++ {
		if r.currentSourcePart.dsMetadata == nil {
			// 每个 raw 样本贡献一次计数，其余特征沿用原始值；实际聚合由 merger 完成。
			if currentFeature == downsampleFeatureCount {
				for range b.timestamps {
					b.values[currentFeature] = append(b.values[currentFeature], 1)
				}
			} else {
				b.values[currentFeature] = append(b.values[currentFeature], b.values[downsampleFeatureLast]...)
			}
			continue
		}
		currentFeatureHeader, err := r.readFeatureHeader(h, currentFeature)
		if err != nil {
			return err
		}
		if err := r.readNativeValues(&r.currentFeatureBlock, &currentFeatureHeader, h); err != nil {
			return err
		}
		b.values[currentFeature] = decimal.AppendDecimalToFloat(b.values[currentFeature], r.currentFeatureBlock.values, r.currentFeatureBlock.bh.Scale)
	}
	return nil
}

// Error 返回当前索引扫描错误；错误发生后不再推进游标。
func (r *downsampleReader) Error() error {
	return r.readErr
}

// readFeatureHeader 顺序对齐传入的首列 header，验证同一 block 的其他特征共用相同时间戳描述。
func (r *downsampleReader) readFeatureHeader(currentBlockHeader *blockHeader, feature uint8) (blockHeader, error) {
	if r.readErr != nil {
		return blockHeader{}, r.readErr
	}
	h := currentBlockHeader
	if r.currentSourcePart == nil || h == nil || h.RowsCount == 0 || feature >= countOfDownsampleFeatures {
		return blockHeader{}, fmt.Errorf("[downsampling] reader source, block header or feature is invalid")
	}
	if r.currentSourcePart.dsMetadata == nil || feature == downsampleFeatureLast {
		return *h, nil
	}
	c := &r.currentResolutionFeatureIndexes[feature]
	for c.currentBlockHeader.RowsCount == 0 || downsampleHeaderLess(&c.currentBlockHeader, h) {
		if !c.nextHeader(r) {
			if r.readErr != nil {
				return blockHeader{}, r.readErr
			}
			return blockHeader{}, fmt.Errorf("[downsampling] missing feature block")
		}
	}
	if !sameDownsampleTimestamps(h, &c.currentBlockHeader) {
		return blockHeader{}, fmt.Errorf("[downsampling] feature blocks in the same block group have different timestamp descriptors")
	}
	return c.currentBlockHeader, nil
}

// downsampleInmemoryReaderSize 只接受原生 inmemory 缓冲，避免内存源误借用磁盘查询句柄。
func downsampleInmemoryReaderSize(f fs.MustReadAtCloser) (uint64, error) {
	b, ok := f.(*chunkedbuffer.Buffer)
	if !ok {
		return 0, fmt.Errorf("[downsampling] unsupported raw inmemory buffer")
	}
	return uint64(b.SizeBytes()), nil
}

// nextHeader 顺序返回当前特征的下一条 header，并在索引块用尽后继续读取下一块。
func (c *downsampleIndexCursor) nextHeader(r *downsampleReader) bool {
	previousBlockHeader := c.currentBlockHeader
	c.currentBlockHeader = blockHeader{}
	if r.readErr != nil || r.currentSourcePart == nil {
		return false
	}
	if c.nextBlockHeader >= len(c.currentIndexBlockHeaders) {
		ok, err := c.nextIndex(r)
		if err != nil {
			r.readErr = fmt.Errorf("[downsampling] cannot read index for part %q: %w", r.currentSourcePart.path, err)
			return false
		}
		if !ok {
			return false
		}
	}
	h := c.currentIndexBlockHeaders[c.nextBlockHeader]
	c.nextBlockHeader++
	// 降采样索引在解码时已检查块内及跨块顺序；raw 仍需检查同 TSID 的时间顺序，允许相等和重叠。
	if r.currentSourcePart.dsMetadata == nil && previousBlockHeader.RowsCount != 0 && downsampleHeaderLess(&h, &previousBlockHeader) {
		r.readErr = fmt.Errorf("[downsampling] block headers are out of order in part %q", r.currentSourcePart.path)
		return false
	}
	c.currentBlockHeader = h
	return true
}

// nextIndex 顺序读取并校验当前特征的下一 index block，复用单块缓冲并检查相邻索引边界。
func (c *downsampleIndexCursor) nextIndex(r *downsampleReader) (bool, error) {
	if c.nextMetaindexRow >= c.metaindexRowsEnd {
		return false, nil
	}
	var off uint64
	var size, count uint32
	var dm *downsampleMetaindexRow
	if r.currentSourcePart.dsMetadata != nil {
		dm = &r.currentSourcePart.dsMetaindex[c.nextMetaindexRow]
		off, size, count = dm.IndexBlockOffset, dm.IndexBlockSize, dm.BlockHeadersCount
	} else {
		m := &r.currentSourcePart.metaindex[c.nextMetaindexRow]
		off, size, count = m.IndexBlockOffset, m.IndexBlockSize, m.BlockHeadersCount
	}
	c.nextMetaindexRow++
	if size > downsampleMaxIndexSize || count == 0 {
		return false, fmt.Errorf("[downsampling] invalid index block size or header count")
	}
	var err error
	c.currentIndexBlockCompressedData, err = r.readAt(c.currentIndexBlockCompressedData[:0], r.indexReader, r.currentSourcePart.indexFile, r.indexFileSize, off, size)
	if err != nil {
		return false, err
	}
	data := c.currentIndexBlockCompressedData
	if dm != nil {
		if len(data) < len(downsampleIndexMagic) || string(data[:len(downsampleIndexMagic)]) != downsampleIndexMagic {
			return false, fmt.Errorf("[downsampling] invalid index magic")
		}
		data = data[len(downsampleIndexMagic):]
	}
	if uint64(count)*uint64(marshaledBlockHeaderSize) > maxBlockSize {
		return false, fmt.Errorf("[downsampling] decoded index size exceeds the limit")
	}
	c.currentIndexBlockData, err = encoding.DecompressZSTDLimited(c.currentIndexBlockData[:0], data, maxBlockSize)
	if err != nil {
		return false, err
	}
	if len(c.currentIndexBlockData) != int(count)*marshaledBlockHeaderSize {
		return false, fmt.Errorf("[downsampling] index size does not match the header count")
	}
	if dm != nil {
		// 解码会复用并覆盖 header 切片，只在此处保存上一 index 的末项，无需长期重复保存边界状态。
		var previousIndexLastHeader blockHeader
		if len(c.currentIndexBlockHeaders) > 0 {
			previousIndexLastHeader = c.currentIndexBlockHeaders[len(c.currentIndexBlockHeaders)-1]
		}
		c.currentIndexBlockHeaders, err = unmarshalDownsampleIndexBlock(c.currentIndexBlockHeaders[:0], c.currentIndexBlockData, dm, r.timestampsFileSize, r.valuesFileSize, r.indexFileSize)
		if err != nil {
			return false, err
		}
		first := &c.currentIndexBlockHeaders[0]
		if previousIndexLastHeader.RowsCount != 0 && !downsampleHeadersOrdered(&previousIndexLastHeader, first) {
			return false, fmt.Errorf("[downsampling] index blocks are out of order or have overlapping time ranges")
		}
		previousTimestampsEnd := previousIndexLastHeader.TimestampsBlockOffset + uint64(previousIndexLastHeader.TimestampsBlockSize)
		previousValuesEnd := previousIndexLastHeader.ValuesBlockOffset + uint64(previousIndexLastHeader.ValuesBlockSize)
		if previousIndexLastHeader.RowsCount != 0 && dm.IndexBlockOffset == c.currentIndexBlockEndOffset && (first.TimestampsBlockOffset != previousTimestampsEnd || first.ValuesBlockOffset != previousValuesEnd) {
			return false, fmt.Errorf("[downsampling] payloads are not contiguous across adjacent index blocks")
		}
		c.currentIndexBlockEndOffset = dm.IndexBlockOffset + uint64(dm.IndexBlockSize)
	} else {
		c.currentIndexBlockHeaders, err = unmarshalBlockHeaders(c.currentIndexBlockHeaders[:0], c.currentIndexBlockData, int(count))
		if err != nil {
			return false, err
		}
	}
	c.nextBlockHeader = 0
	return true, nil
}

// readNativeBlock 读取并完整解码首列原生 Block，校验编码、行数和时间戳边界。
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
	b.timestampsData, err = r.readAt(b.timestampsData[:0], r.timestampsReader, r.currentSourcePart.timestampsFile, r.timestampsFileSize, h.TimestampsBlockOffset, h.TimestampsBlockSize)
	if err != nil {
		return err
	}
	b.valuesData, err = r.readAt(b.valuesData[:0], r.valuesReader, r.currentSourcePart.valuesFile, r.valuesFileSize, h.ValuesBlockOffset, h.ValuesBlockSize)
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
	b.valuesData, err = r.readAt(b.valuesData[:0], r.valuesReader, r.currentSourcePart.valuesFile, r.valuesFileSize, h.ValuesBlockOffset, h.ValuesBlockSize)
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

// readAt 在文件或内存缓冲的有效范围内读取单个负载；磁盘源只使用当前 reader 的自有句柄。
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
	if r.currentSourcePart == nil || r.currentSourcePart.path != "" || r.currentSourcePart.dsMetadata != nil {
		return dst, fmt.Errorf("[downsampling] merge file handle is unavailable")
	}
	if inmemorySource == nil {
		return dst, fmt.Errorf("[downsampling] raw file handle is unavailable")
	}
	inmemorySource.MustReadAt(dst, int64(off))
	return dst, nil
}

// prepareBlockPayload 校验行数与编码约束，并限制压缩帧展开大小；实际数值仍由原生解码器解码。
func (r *downsampleReader) prepareBlockPayload(src []byte, mt encoding.MarshalType, rows uint32) ([]byte, encoding.MarshalType, error) {
	if rows < 1 || rows > downsampleMaxRawRows {
		return nil, mt, fmt.Errorf("[downsampling] invalid block row count %d", rows)
	}
	if rows < 2 && (mt == encoding.MarshalTypeNearestDelta2 || mt == encoding.MarshalTypeZSTDNearestDelta2) {
		return nil, mt, fmt.Errorf("[downsampling] delta-of-delta encoding requires at least two rows")
	}
	if mt != encoding.MarshalTypeZSTDNearestDelta && mt != encoding.MarshalTypeZSTDNearestDelta2 {
		return src, mt, nil
	}
	var err error
	r.currentBlockDecompressedPayload, err = encoding.DecompressZSTDLimited(r.currentBlockDecompressedPayload[:0], src, int(rows)*10)
	if err != nil {
		return nil, mt, fmt.Errorf("[downsampling] cannot decompress block payload: %w", err)
	}
	src = append(src[:0], r.currentBlockDecompressedPayload...)
	if mt == encoding.MarshalTypeZSTDNearestDelta {
		mt = encoding.MarshalTypeNearestDelta
	} else {
		mt = encoding.MarshalTypeNearestDelta2
	}
	return src, mt, nil
}

// getDownsampleReader 取得一个已清理的合并 reader。
func getDownsampleReader() *downsampleReader {
	if v := downsampleReaderPool.Get(); v != nil {
		return v.(*downsampleReader)
	}
	return &downsampleReader{}
}

// putDownsampleReader 关闭自有句柄并释放当前 TSID header 缓存，再将有限工作缓冲归还对象池。
func putDownsampleReader(r *downsampleReader) error {
	err := r.Close()
	downsampleReaderPool.Put(r)
	return err
}

var downsampleReaderPool sync.Pool
