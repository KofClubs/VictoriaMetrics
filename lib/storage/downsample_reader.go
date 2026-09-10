package storage

import (
	"errors"
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
	p                *part       // 当前源，由调用方持有引用；切换源或 Close 时清除。
	resolution       int64       // Init 指定的分辨率，同一源切换分辨率时重新定位索引。
	timestampsReader *os.File    // timestamps.bin；raw 磁盘源自有，摘要源借用 part，inmemory 为 nil。
	valuesReader     *os.File    // values.bin；所有权及生命周期与 timestampsReader 一致。
	indexReader      *os.File    // index.bin；所有权及生命周期与 timestampsReader 一致。
	timestampsSize   uint64      // 当前源时间戳文件或内存缓冲大小，用于每次读取的范围校验。
	valuesSize       uint64      // 当前源值文件或内存缓冲大小，用于每次读取的范围校验。
	indexSize        uint64      // 当前源索引文件或内存缓冲大小，用于每次读取的范围校验。
	ownFiles         bool        // 仅 raw 磁盘源为 true；reset 只关闭自有文件。
	filterTSID       TSID        // SetFilter 指定的 TSID，hasFilter 为 false 时不启用。
	hasFilter        bool        // 当前是否按一个 TSID 筛选；SetFilter 重设。
	minTimestamp     int64       // 当前过滤范围下界，只筛选相交 block，不裁剪其中样本。
	maxTimestamp     int64       // 当前过滤范围上界，与 minTimestamp 一同由 SetFilter 重设。
	metaPos          int         // 下一条待读取的 metaindex 位置，SetFilter 重新定位。
	metaEnd          int         // 当前分辨率、特征的 metaindex 结束位置，不包含此位置。
	indexPos         int         // indexData 中下一条 header 的字节偏移。
	indexData        []byte      // 当前已解压的单个 index block，跨索引读取复用容量。
	compressed       []byte      // 单个 index block 的压缩读取缓冲，reset 保留正常容量。
	decompressed     []byte      // 单个时间戳或值 payload 的限长解压缓冲，不缓存整个文件。
	block            Block       // 当前列的原生解码工作区；一次 ReadBlock 的五列共用其时间戳。
	current          blockHeader // NextHeader 定位的 header，下次迭代或 SetFilter 时失效。
	previous         blockHeader // 当前索引扫描中上一条 header，用于验证顺序。
	feature          uint8       // 当前索引扫描的特征，由 Init 指定。

	peers [countOfDownsampleFeatures]*downsampleReader // 其它特征的索引游标；按需借用，SetFilter/reset 归还。

	hasPrevious bool // previous 是否有效；SetFilter 清除。
	// 仅比较已经读取且在 index.bin 中相邻的 index，过滤跳过的区间不补读。
	previousIndexEnd     uint64      // 上一已读 index 在文件中的结束偏移，用于识别物理相邻索引。
	previousTimestampEnd uint64      // 上一已读 index 最后一条时间戳 payload 的结束偏移。
	previousValuesEnd    uint64      // 上一已读 index 最后一条值 payload 的结束偏移。
	previousIndexHeader  blockHeader // 上一已读 index 的末尾 header，用于跨 index 排序校验。
	hasPreviousIndex     bool        // 跨 index 校验状态是否有效；SetFilter 清除。
	err                  error       // 当前迭代或过滤器清理错误，后续读取返回此错误，SetFilter/reset 重设。
}

func (r *downsampleReader) Init(p *part, resolution int64, features ...uint8) error {
	feature := uint8(0)
	if len(features) > 0 {
		feature = features[0]
	}
	if p == nil || !validDownsampleResolution(resolution) || feature >= countOfDownsampleFeatures || len(features) > 1 {
		return errors.Join(fmt.Errorf("无效降采样源或分辨率"), r.reset())
	}
	// part 不可变，同一源的窗口切换复用文件句柄和工作缓冲。
	if r.p == p {
		r.resolution, r.feature = resolution, feature
		r.SetFilter(nil, minUnixMilli, maxUnixMilli)
		return r.err
	}
	if err := r.reset(); err != nil {
		return err
	}
	r.p = p
	r.resolution, r.feature = resolution, feature
	if p.dsMetadata != nil {
		r.timestampsReader, r.timestampsSize = p.dsTimestampsFile, p.dsTimestampsSize
		r.valuesReader, r.valuesSize = p.dsValuesFile, p.dsValuesSize
		r.indexReader, r.indexSize = p.dsIndexFile, p.dsIndexSize
	} else if p.path != "" {
		r.ownFiles = true
		if err := openDownsampleReaderFile(filepath.Join(p.path, timestampsFilename), &r.timestampsReader, &r.timestampsSize); err != nil {
			return errors.Join(err, r.reset())
		}
		if err := openDownsampleReaderFile(filepath.Join(p.path, valuesFilename), &r.valuesReader, &r.valuesSize); err != nil {
			return errors.Join(err, r.reset())
		}
		if err := openDownsampleReaderFile(filepath.Join(p.path, indexFilename), &r.indexReader, &r.indexSize); err != nil {
			return errors.Join(err, r.reset())
		}
	} else {
		var err error
		if r.timestampsSize, err = downsampleInmemoryReaderSize(p.timestampsFile); err != nil {
			return errors.Join(err, r.reset())
		}
		if r.valuesSize, err = downsampleInmemoryReaderSize(p.valuesFile); err != nil {
			return errors.Join(err, r.reset())
		}
		if r.indexSize, err = downsampleInmemoryReaderSize(p.indexFile); err != nil {
			return errors.Join(err, r.reset())
		}
	}
	r.SetFilter(nil, minUnixMilli, maxUnixMilli)
	return r.err
}

// 文件一经打开即交给调用方持有，即使 Stat 失败也由 reader.reset 统一关闭。
func openDownsampleReaderFile(path string, dst **os.File, size *uint64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	*dst = f
	st, err := f.Stat()
	if err != nil {
		return err
	}
	*size = uint64(st.Size())
	return nil
}

func downsampleInmemoryReaderSize(f fs.MustReadAtCloser) (uint64, error) {
	b, ok := f.(*chunkedbuffer.Buffer)
	if !ok {
		return 0, fmt.Errorf("不支持的 inmemory 原始缓冲")
	}
	return uint64(b.SizeBytes()), nil
}

// SetFilter 重置索引游标；筛选相交 block，不提前过滤 block 内的样本贡献。
func (r *downsampleReader) SetFilter(tsid *TSID, minTimestamp, maxTimestamp int64) {
	var closeErr error
	for i, peer := range r.peers {
		if peer != nil {
			closeErr = errors.Join(closeErr, putDownsampleReader(peer))
			r.peers[i] = nil
		}
	}
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
		r.err = errors.Join(r.err, fmt.Errorf("无效降采样读取窗口"))
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

func (r *downsampleReader) Header() *blockHeader {
	return &r.current
}

func (r *downsampleReader) Error() error {
	return r.err
}

func (r *downsampleReader) NextHeader() bool {
	r.current = blockHeader{}
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
		var h blockHeader
		if _, err := h.Unmarshal(r.indexData[r.indexPos : r.indexPos+marshaledBlockHeaderSize]); err != nil {
			r.err = err
			return false
		}
		r.indexPos += marshaledBlockHeaderSize
		if r.hasPrevious && (downsampleHeaderLess(&h, &r.previous) || (r.p.dsMetadata != nil && !downsampleHeadersOrdered(&r.previous, &h))) {
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
		if (r.hasFilter && h.TSID != r.filterTSID) || h.MaxTimestamp < r.minTimestamp || h.MinTimestamp > r.maxTimestamp {
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
		r.compressed, err = r.readAt(r.compressed[:0], r.indexReader, r.p.indexFile, r.indexSize, off, size)
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
	var previous blockHeader
	for pos := 0; pos < len(r.indexData); pos += marshaledBlockHeaderSize {
		var h blockHeader
		if _, err := h.Unmarshal(r.indexData[pos : pos+marshaledBlockHeaderSize]); err != nil {
			return err
		}
		if err := validateDownsampleHeader(&h); err != nil {
			return err
		}
		if !sameDownsampleTenant(&h.TSID, &m.TSID) || h.TSID.Less(&m.TSID) || m.LastTSID.Less(&h.TSID) {
			return fmt.Errorf("index block 租户或 TSID 范围错误")
		}
		if pos == 0 {
			if r.hasPreviousIndex && !downsampleHeadersOrdered(&r.previousIndexHeader, &h) {
				return fmt.Errorf("跨 index block 排序错误或时间范围重叠")
			}
			if r.hasPreviousIndex && m.IndexBlockOffset == r.previousIndexEnd && (h.TimestampsBlockOffset != r.previousTimestampEnd || h.ValuesBlockOffset != r.previousValuesEnd) {
				return fmt.Errorf("相邻 index block 的降采样负载不连续")
			}
			first, minTime, maxTime = h.TSID, h.MinTimestamp, h.MaxTimestamp
		} else {
			if !downsampleHeadersOrdered(&previous, &h) {
				return fmt.Errorf("index block 排序错误或时间范围重叠")
			}
			if h.TimestampsBlockOffset != previous.TimestampsBlockOffset+uint64(previous.TimestampsBlockSize) || h.ValuesBlockOffset != previous.ValuesBlockOffset+uint64(previous.ValuesBlockSize) {
				return fmt.Errorf("相邻降采样 block 负载不连续")
			}
		}
		if pos == 0 && m.IndexBlockOffset == 0 && (h.TimestampsBlockOffset != 0 || h.ValuesBlockOffset != 0) {
			return fmt.Errorf("降采样负载首偏移不是零")
		}
		last, previous = h.TSID, h
		rows += uint64(h.RowsCount)
		minTime, maxTime = min(minTime, h.MinTimestamp), max(maxTime, h.MaxTimestamp)
		if err := checkDownsampleExtent(h.TimestampsBlockOffset, h.TimestampsBlockSize, r.timestampsSize); err != nil {
			return err
		}
		if err := checkDownsampleExtent(h.ValuesBlockOffset, h.ValuesBlockSize, r.valuesSize); err != nil {
			return err
		}
	}
	if m.IndexBlockOffset+uint64(m.IndexBlockSize) == r.indexSize && (previous.TimestampsBlockOffset+uint64(previous.TimestampsBlockSize) != r.timestampsSize || previous.ValuesBlockOffset+uint64(previous.ValuesBlockSize) != r.valuesSize) {
		return fmt.Errorf("降采样负载存在截断或未引用尾部")
	}
	if rows != m.RowsCount || first != m.TSID || last != m.LastTSID || minTime != m.MinTimestamp || maxTime != m.MaxTimestamp {
		return fmt.Errorf("index 与 metaindex 统计矛盾")
	}
	r.previousIndexEnd = m.IndexBlockOffset + uint64(m.IndexBlockSize)
	r.previousTimestampEnd = previous.TimestampsBlockOffset + uint64(previous.TimestampsBlockSize)
	r.previousValuesEnd = previous.ValuesBlockOffset + uint64(previous.ValuesBlockSize)
	r.previousIndexHeader = previous
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

func (r *downsampleReader) readAt(dst []byte, reader *os.File, inmemorySource fs.MustReadAtCloser, fileSize, off uint64, size uint32) ([]byte, error) {
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
		_, err := reader.ReadAt(dst, int64(off))
		return dst, err
	}
	if r.p == nil || r.p.dsMetadata != nil {
		return dst, fmt.Errorf("降采样文件句柄不可用")
	}
	if inmemorySource == nil {
		return dst, fmt.Errorf("原始文件句柄不可用")
	}
	inmemorySource.MustReadAt(dst, int64(off))
	return dst, nil
}

// FieldHeader 返回单个特征的原生 header，供现有 BlockRef 读取链路使用。
func (r *downsampleReader) FieldHeader(feature uint8) (blockHeader, error) {
	if r.err != nil {
		return blockHeader{}, r.err
	}
	if r.p == nil || r.current.RowsCount == 0 || feature >= countOfDownsampleFeatures {
		return blockHeader{}, fmt.Errorf("降采样 reader 未定位 Block 或特征无效")
	}
	if r.p.dsMetadata == nil || feature == r.feature {
		return r.current, nil
	}
	peer := r.peers[feature]
	if peer == nil {
		peer = getDownsampleReader()
		r.peers[feature] = peer
		if err := peer.Init(r.p, r.resolution, feature); err != nil {
			return blockHeader{}, err
		}
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
			return blockHeader{}, fmt.Errorf("降采样特征缺失")
		}
	}
	if !sameDownsampleTimestamps(&r.current, &peer.current) {
		return blockHeader{}, fmt.Errorf("同一批次的单特征 Block 未共享一致的时间戳描述")
	}
	return peer.current, nil
}

// ReadFieldBlock 读取一个分辨率、一个特征对应的原生 Block，并完成其解码状态转换。
func (r *downsampleReader) ReadFieldBlock(dst *Block, feature uint8) error {
	h, err := r.FieldHeader(feature)
	if err != nil {
		return err
	}
	return r.readNativeBlock(dst, &h)
}

func (r *downsampleReader) readNativeBlock(b *Block, h *blockHeader) error {
	b.Reset()
	if err := h.validate(); err != nil {
		return err
	}
	if h.MinTimestamp > h.MaxTimestamp || h.MinTimestamp < minUnixMilli || h.MaxTimestamp > maxUnixMilli || (h.RowsCount == 1 && h.MinTimestamp != h.MaxTimestamp) {
		return fmt.Errorf("Block 时间范围无效")
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
		return err
	}
	// 原生 const/delta-const 不检查边界；有损编码则先沿用原生顺序修复。
	if len(b.timestamps) != int(h.RowsCount) || b.timestamps[0] != h.MinTimestamp || b.timestamps[len(b.timestamps)-1] != h.MaxTimestamp {
		return fmt.Errorf("Block 时间戳首尾或行数与 header 矛盾")
	}
	return checkTimestampsBounds(b.timestamps, h.MinTimestamp, h.MaxTimestamp)
}

// readNativeValues 只更换原生 Block 的值列，复用本次 ReadBlock 已解码并校验的时间戳。
// shared 是首列的磁盘 header，不能使用 prepareBlockPayload 已转换过 codec 的 b.bh 替代。
func (r *downsampleReader) readNativeValues(b *Block, h, shared *blockHeader) error {
	if err := h.validate(); err != nil {
		return err
	}
	if !sameDownsampleTimestamps(shared, h) {
		return fmt.Errorf("同一分辨率多特征 Block 未共享一致的时间戳描述")
	}
	if len(b.timestamps) != int(h.RowsCount) || b.timestamps[0] != h.MinTimestamp || b.timestamps[len(b.timestamps)-1] != h.MaxTimestamp {
		return fmt.Errorf("已解码时间戳首尾或行数与 header 矛盾")
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
	return b.unmarshalValues()
}

func (r *downsampleReader) ReadBlock(b *downsampleDecodedResolutionFeaturesBlock) error {
	if r.err != nil {
		return r.err
	}
	if r.p == nil || r.current.RowsCount == 0 {
		return fmt.Errorf("降采样 reader 未定位批次")
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
			if err := r.ReadFieldBlock(&r.block, uint8(i)); err != nil {
				return err
			}
			b.timestamps = append(b.timestamps[:0], r.block.timestamps...)
		} else {
			column, err := r.FieldHeader(uint8(i))
			if err != nil {
				return err
			}
			if err := r.readNativeValues(&r.block, &column, h); err != nil {
				return err
			}
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

func (r *downsampleReader) readRawBlock(b *downsampleDecodedResolutionFeaturesBlock, h *blockHeader) error {
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

// Close 释放 reader 自己打开的原始源文件；下采样 part 的文件由引用持有者管理。
// 即使一个文件关闭失败，也尝试其余文件，并清除句柄以避免重复关闭。
func (r *downsampleReader) Close() error {
	return r.reset()
}

func (r *downsampleReader) reset() error {
	var closeErr error
	if r.ownFiles {
		for _, f := range []*os.File{r.timestampsReader, r.valuesReader, r.indexReader} {
			if f != nil {
				closeErr = errors.Join(closeErr, f.Close())
			}
		}
	}
	r.p = nil
	r.timestampsReader, r.valuesReader, r.indexReader = nil, nil, nil
	r.timestampsSize, r.valuesSize, r.indexSize = 0, 0, 0
	r.ownFiles = false
	r.resolution = 0
	r.SetFilter(nil, 0, 0)
	closeErr = errors.Join(closeErr, r.err)
	r.err = nil
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
	return closeErr
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
