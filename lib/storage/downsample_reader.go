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
// 每次只解码一个 index block，按索引定位当前 TSID，而不载入整个 part 的 header。
// 调用方必须在 reader 存活期间持有 part 引用；返回的 header 在下次迭代时失效。
type downsampleReader struct {
	p                *part                   // 当前源，由调用方持有引用；切换源或 Close 时清除。
	resolution       int64                   // Init 指定的分辨率，同一源切换分辨率时重新定位索引。
	timestampsReader filestream.ReadAtCloser // timestamps.bin；raw 磁盘源自有，降采样源借用 part，inmemory 为 nil。
	valuesReader     filestream.ReadAtCloser // values.bin；所有权及生命周期与 timestampsReader 一致。
	indexReader      filestream.ReadAtCloser // index.bin；所有权及生命周期与 timestampsReader 一致。
	timestampsSize   uint64                  // 当前源时间戳文件或内存缓冲大小，用于每次读取的范围校验。
	valuesSize       uint64                  // 当前源值文件或内存缓冲大小，用于每次读取的范围校验。
	indexSize        uint64                  // 当前源索引文件或内存缓冲大小，用于每次读取的范围校验。
	ownFiles         bool                    // 仅 raw 磁盘源为 true；Close 只关闭自有文件。
	filterTSID       TSID                    // SetFilter 指定的 TSID，hasFilter 为 false 时不启用。
	hasFilter        bool                    // 当前是否按一个 TSID 筛选；SetFilter 重设。
	minTimestamp     int64                   // 当前过滤范围下界，只筛选相交 block，不裁剪其中样本。
	maxTimestamp     int64                   // 当前过滤范围上界，与 minTimestamp 一同由 SetFilter 重设。
	metaPos          int                     // 下一条待读取的 metaindex 位置，SetFilter 重新定位。
	metaEnd          int                     // 当前分辨率、特征的 metaindex 结束位置，不包含此位置。
	indexPos         int                     // indexHeaders 中下一条 header 的位置。
	indexData        []byte                  // 当前单个 index block 的解压工作区，跨索引读取复用容量。
	indexHeaders     []blockHeader           // 当前 index block 的原生 header，供合并逐个迭代。
	compressed       []byte                  // 单个 index block 的压缩读取缓冲，Close 保留正常容量。
	decompressed     []byte                  // 单个时间戳或值 payload 的限长解压缓冲，不缓存整个文件。
	block            Block                   // 当前列的原生解码工作区；一次 ReadBlock 的五列共用其时间戳。
	current          blockHeader             // NextHeader 定位的 header，下次迭代或 SetFilter 时失效。
	previous         blockHeader             // 当前索引扫描中上一条 header，用于验证顺序。
	feature          uint8                   // 主游标固定为 last；其它特征仅由内部 peer 扫描。

	peers [countOfDownsampleFeatures]*downsampleReader // 其它特征的索引游标；按需借用，SetFilter/Close 归还。

	hasPrevious bool // previous 是否有效；SetFilter 清除。
	// 仅比较已经读取且在 index.bin 中相邻的 index，过滤跳过的区间不补读。
	previousIndexEnd     uint64      // 上一已读 index 在文件中的结束偏移，用于识别物理相邻索引。
	previousTimestampEnd uint64      // 上一已读 index 最后一条时间戳 payload 的结束偏移。
	previousValuesEnd    uint64      // 上一已读 index 最后一条值 payload 的结束偏移。
	previousIndexHeader  blockHeader // 上一已读 index 的末尾 header，用于跨 index 排序校验。
	hasPreviousIndex     bool        // 跨 index 校验状态是否有效；SetFilter 清除。
	err                  error       // 当前迭代或过滤器清理错误，后续读取返回此错误，SetFilter/Close 重设。
}

func (r *downsampleReader) Init(p *part, resolution int64) (err error) {
	if p == nil || !validDownsampleResolution(resolution) {
		return errors.Join(fmt.Errorf("[downsampling] invalid reader source or resolution"), r.Close())
	}
	feature := uint8(downsampleFeatureLast)
	// part 不可变，同一源的窗口切换复用文件句柄和工作缓冲。
	if r.p == p {
		r.resolution, r.feature = resolution, feature
		r.SetFilter(nil, minUnixMilli, maxUnixMilli)
		return r.err
	}
	if err := r.Close(); err != nil {
		return err
	}
	// 新源一经接管，任何初始化失败都由这一处释放已打开的文件。
	defer func() {
		if err != nil {
			err = errors.Join(err, r.Close())
		}
	}()
	r.p = p
	r.resolution, r.feature = resolution, feature
	if p.dsMetadata != nil {
		r.timestampsReader, r.timestampsSize = p.dsTimestampsFile, p.dsTimestampsSize
		r.valuesReader, r.valuesSize = p.dsValuesFile, p.dsValuesSize
		r.indexReader, r.indexSize = p.dsIndexFile, p.dsIndexSize
	} else if p.path != "" {
		r.ownFiles = true
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
		var err error
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
	r.SetFilter(nil, minUnixMilli, maxUnixMilli)
	return r.err
}

// Close 释放自有原始文件和 feature reader；借用的降采样文件仍由 part 管理。
// 即使关闭失败也尝试其余资源，并清除所有权；重复调用不会再次关闭或归还。
func (r *downsampleReader) Close() error {
	closeErr := r.closePeers()
	if r.ownFiles {
		for _, f := range []filestream.ReadAtCloser{r.timestampsReader, r.valuesReader, r.indexReader} {
			if f != nil {
				if err := f.Close(); err != nil {
					closeErr = errors.Join(closeErr, fmt.Errorf("[downsampling] cannot close reader file %q: %w", f.Path(), err))
				}
			}
		}
	}
	for _, b := range []*[]byte{&r.indexData, &r.compressed} {
		if cap(*b) > downsampleMaxIndexSize {
			*b = nil
		} else {
			*b = (*b)[:0]
		}
	}
	if cap(r.decompressed) > downsampleMaxPooledRows*10 {
		r.decompressed = nil
	} else {
		r.decompressed = r.decompressed[:0]
	}
	if cap(r.indexHeaders) > maxBlockSize/marshaledBlockHeaderSize {
		r.indexHeaders = nil
	} else {
		r.indexHeaders = r.indexHeaders[:0]
	}
	r.block.Reset()
	// 清除迭代和过滤状态，无需调用会再次处理 peers 的 SetFilter。
	*r = downsampleReader{
		indexData:    r.indexData,
		indexHeaders: r.indexHeaders,
		compressed:   r.compressed,
		decompressed: r.decompressed,
		block:        r.block,
	}
	return closeErr
}

// SetFilter 重置索引游标；筛选相交 block，不提前过滤 block 内的样本贡献。
func (r *downsampleReader) SetFilter(tsid *TSID, minTimestamp, maxTimestamp int64) {
	closeErr := r.closePeers()
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
	r.indexHeaders = r.indexHeaders[:0]
	r.current = blockHeader{}
	r.previous = blockHeader{}
	r.hasPrevious = false
	r.previousIndexEnd = 0
	r.previousTimestampEnd = 0
	r.previousValuesEnd = 0
	r.previousIndexHeader = blockHeader{}
	r.hasPreviousIndex = false
	r.err = closeErr
	if minTimestamp > maxTimestamp {
		r.err = errors.Join(r.err, fmt.Errorf("[downsampling] invalid reader time range"))
		return
	}
	if r.p == nil {
		return
	}
	if r.p.dsMetadata != nil {
		rows := r.p.dsMetaindex
		start := sort.Search(len(rows), func(i int) bool {
			return rows[i].ResolutionMs > r.resolution || (rows[i].ResolutionMs == r.resolution && rows[i].feature >= r.feature)
		})
		end := start + sort.Search(len(rows)-start, func(i int) bool {
			return rows[start+i].ResolutionMs > r.resolution || rows[start+i].feature > r.feature
		})
		if r.hasFilter {
			// 同一分辨率、特征下 LastTSID 单调，保留所有可能包含目标的边界 index。
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

func (r *downsampleReader) NextHeader() bool {
	r.current = blockHeader{}
	if r.err != nil || r.p == nil {
		return false
	}
	for {
		if r.indexPos >= len(r.indexHeaders) {
			ok, err := r.nextIndex()
			if err != nil {
				r.err = fmt.Errorf("[downsampling] cannot read index for part %q: %w", r.p.path, err)
				return false
			}
			if !ok {
				return false
			}
		}
		h := r.indexHeaders[r.indexPos]
		r.indexPos++
		if r.hasPrevious && (downsampleHeaderLess(&h, &r.previous) || (r.p.dsMetadata != nil && !downsampleHeadersOrdered(&r.previous, &h))) {
			r.err = fmt.Errorf("[downsampling] block headers are out of order in part %q", r.p.path)
			return false
		}
		r.previous = h
		r.hasPrevious = true
		if r.hasFilter && r.filterTSID.Less(&h.TSID) {
			r.metaPos = r.metaEnd
			r.indexPos = len(r.indexHeaders)
			return false
		}
		if (r.hasFilter && h.TSID != r.filterTSID) || h.MaxTimestamp < r.minTimestamp || h.MinTimestamp > r.maxTimestamp {
			continue
		}
		r.current = h
		return true
	}
}

func (r *downsampleReader) Header() *blockHeader {
	return &r.current
}

func (r *downsampleReader) ReadBlock(b *downsampleDecodedResolutionFeaturesBlock) error {
	if r.err != nil {
		return r.err
	}
	if r.p == nil || r.current.RowsCount == 0 {
		return fmt.Errorf("[downsampling] reader is not positioned at a batch")
	}
	b.Reset()
	h := &r.current
	b.tsid, b.resolution = h.TSID, r.resolution
	if r.p.dsMetadata == nil {
		return r.readRawBlock(b, h)
	}
	b.precisionBits = h.PrecisionBits
	for i := range b.values {
		if i == 0 {
			// 每次调用都完整读取首列；时间戳只在本次五列解码中复用。
			if err := r.readNativeBlock(&r.block, h); err != nil {
				return err
			}
			b.timestamps = append(b.timestamps[:0], r.block.timestamps...)
		} else {
			column, err := r.featureHeader(uint8(i))
			if err != nil {
				return err
			}
			if err := r.readNativeValues(&r.block, &column, h); err != nil {
				return err
			}
		}
		b.values[i] = decimal.AppendDecimalToFloat(b.values[i][:0], r.block.values, r.block.bh.Scale)
	}
	return nil
}

func (r *downsampleReader) Error() error {
	return r.err
}

// featureHeader 为当前合并 block 定位同一分辨率的特征列，验证五列共用的时间戳描述。
func (r *downsampleReader) featureHeader(feature uint8) (blockHeader, error) {
	if r.err != nil {
		return blockHeader{}, r.err
	}
	if r.p == nil || r.current.RowsCount == 0 || feature >= countOfDownsampleFeatures {
		return blockHeader{}, fmt.Errorf("[downsampling] reader is not positioned at a block or the feature is invalid")
	}
	if r.p.dsMetadata == nil || feature == r.feature {
		return r.current, nil
	}
	peer := r.peers[feature]
	if peer == nil {
		peer = getDownsampleReader()
		r.peers[feature] = peer
		if err := peer.Init(r.p, r.resolution); err != nil {
			return blockHeader{}, err
		}
		peer.feature = feature
		var tsid *TSID
		if r.hasFilter {
			tsid = &r.filterTSID
		}
		peer.SetFilter(tsid, r.minTimestamp, r.maxTimestamp)
	}
	for peer.current.RowsCount == 0 || downsampleHeaderLess(&peer.current, &r.current) {
		if !peer.NextHeader() {
			if err := peer.Error(); err != nil {
				return blockHeader{}, err
			}
			return blockHeader{}, fmt.Errorf("[downsampling] missing feature block")
		}
	}
	if !sameDownsampleTimestamps(&r.current, &peer.current) {
		return blockHeader{}, fmt.Errorf("[downsampling] feature blocks in the same batch have different timestamp descriptors")
	}
	return peer.current, nil
}

func downsampleInmemoryReaderSize(f fs.MustReadAtCloser) (uint64, error) {
	b, ok := f.(*chunkedbuffer.Buffer)
	if !ok {
		return 0, fmt.Errorf("[downsampling] unsupported raw inmemory buffer")
	}
	return uint64(b.SizeBytes()), nil
}

// closePeers 先移除借用引用，再将各特征 reader 归还对象池。
// 切换过滤条件和关闭 reader 共用此处，避免关闭后重复归还已复用的 peer。
func (r *downsampleReader) closePeers() error {
	var err error
	for i, peer := range r.peers {
		if peer != nil {
			r.peers[i] = nil
			err = errors.Join(err, putDownsampleReader(peer))
		}
	}
	return err
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
			return false, fmt.Errorf("[downsampling] invalid index block size or header count")
		}
		var err error
		r.compressed, err = r.readAt(r.compressed[:0], r.indexReader, r.p.indexFile, r.indexSize, off, size)
		if err != nil {
			return false, err
		}
		data := r.compressed
		headerSize := marshaledBlockHeaderSize
		if dm != nil {
			if len(data) < len(downsampleIndexMagic) || string(data[:len(downsampleIndexMagic)]) != downsampleIndexMagic {
				return false, fmt.Errorf("[downsampling] invalid index magic")
			}
			data = data[len(downsampleIndexMagic):]
		}
		if uint64(count)*uint64(headerSize) > maxBlockSize {
			return false, fmt.Errorf("[downsampling] decoded index size exceeds the limit")
		}
		r.indexData, err = encoding.DecompressZSTDLimited(r.indexData[:0], data, maxBlockSize)
		if err != nil {
			return false, err
		}
		if len(r.indexData) != int(count)*headerSize {
			return false, fmt.Errorf("[downsampling] index size does not match the header count")
		}
		if dm != nil {
			r.indexHeaders, err = unmarshalDownsampleIndexBlock(r.indexHeaders[:0], r.indexData, dm, r.timestampsSize, r.valuesSize, r.indexSize)
			if err != nil {
				return false, err
			}
			first := &r.indexHeaders[0]
			if r.hasPreviousIndex && !downsampleHeadersOrdered(&r.previousIndexHeader, first) {
				return false, fmt.Errorf("[downsampling] index blocks are out of order or have overlapping time ranges")
			}
			if r.hasPreviousIndex && dm.IndexBlockOffset == r.previousIndexEnd && (first.TimestampsBlockOffset != r.previousTimestampEnd || first.ValuesBlockOffset != r.previousValuesEnd) {
				return false, fmt.Errorf("[downsampling] payloads are not contiguous across adjacent index blocks")
			}
			last := &r.indexHeaders[len(r.indexHeaders)-1]
			r.previousIndexEnd = dm.IndexBlockOffset + uint64(dm.IndexBlockSize)
			r.previousTimestampEnd = last.TimestampsBlockOffset + uint64(last.TimestampsBlockSize)
			r.previousValuesEnd = last.ValuesBlockOffset + uint64(last.ValuesBlockSize)
			r.previousIndexHeader = *last
			r.hasPreviousIndex = true
		} else {
			r.indexHeaders, err = unmarshalBlockHeaders(r.indexHeaders[:0], r.indexData, int(count))
			if err != nil {
				return false, err
			}
		}
		r.indexPos = 0
		return true, nil
	}
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
	if r.p == nil || r.p.dsMetadata != nil {
		return dst, fmt.Errorf("[downsampling] downsampled file handle is unavailable")
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
