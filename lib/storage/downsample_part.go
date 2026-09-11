package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
)

const (
	downsampleVersion          = 2
	downsampleMaxIndexSize     = 2 * maxBlockSize
	downsampleMaxMetaindexSize = 64 << 20
	downsampleMaxMetadataSize  = 64 << 10

	// 文件前缀不属于 ZSTD 帧，原始格式 reader 会拒绝降采样文件。
	downsampleMetaindexMagic = "VMDSMI\x00\x02"
	downsampleIndexMagic     = "VMDSIX\x00\x02"
)

// downsamplePartMetadata 保留原有统计字段，并显式声明降采样的计算语义。
type downsamplePartMetadata struct {
	partHeader
	FormatVersion    uint32
	SemanticsVersion uint32
	Mode             string
	Resolutions      []int64
	BucketOrigin     int64
	NumericCodec     string
	Retention        string
}

func newDownsamplePartMetadata(ph partHeader) downsamplePartMetadata {
	return downsamplePartMetadata{partHeader: ph, FormatVersion: downsampleVersion, SemanticsVersion: downsampleVersion, Mode: "downsampling", Resolutions: []int64{300000, 3600000}, NumericCodec: "decimal-values", Retention: "bucket-end"}
}

func (m *downsamplePartMetadata) validate() error {
	if m.FormatVersion != downsampleVersion || m.SemanticsVersion != downsampleVersion || m.Mode != "downsampling" || len(m.Resolutions) != 2 || m.Resolutions[0] != 300000 || m.Resolutions[1] != 3600000 || m.BucketOrigin != 0 || m.NumericCodec != "decimal-values" || m.Retention != "bucket-end" || m.MinDedupInterval != 0 {
		return fmt.Errorf("[downsampling] unsupported or inconsistent format, resolution, or semantics metadata")
	}
	if m.RowsCount%countOfDownsampleFeatures != 0 || m.BlocksCount%countOfDownsampleFeatures != 0 {
		return fmt.Errorf("[downsampling] physical row and block counts must be grouped by five features")
	}
	if m.MinTimestamp < minUnixMilli || m.MaxTimestamp > maxUnixMilli {
		return fmt.Errorf("[downsampling] part time range is outside the supported range")
	}
	return validateDownsamplePartHeader(&m.partHeader)
}

func validateDownsamplePartHeader(ph *partHeader) error {
	if ph.RowsCount == 0 || ph.BlocksCount == 0 || ph.BlocksCount > ph.RowsCount || ph.MinTimestamp > ph.MaxTimestamp {
		return fmt.Errorf("[downsampling] invalid part statistics or time range")
	}
	return nil
}

func readDownsampleLimitedFile(path string, limit int64) (_ []byte, err error) {
	if limit < 0 || limit == math.MaxInt64 {
		return nil, fmt.Errorf("[downsampling] invalid file size limit %d", limit)
	}
	f, err := filestream.OpenReadAt(path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if f.Size() > uint64(limit) {
		return nil, fmt.Errorf("[downsampling] file %q exceeds the size limit %d", path, limit)
	}
	b, err := io.ReadAll(io.NewSectionReader(f, 0, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("[downsampling] file %q exceeds the size limit %d", path, limit)
	}
	return b, nil
}

// readDownsampleMetadata 不把损坏的降采样格式解释为 raw 格式。
func readDownsampleMetadata(path string) (*downsamplePartMetadata, error) {
	b, err := readDownsampleLimitedFile(filepath.Join(path, metadataFilename), downsampleMaxMetadataSize)
	if errors.Is(err, os.ErrNotExist) {
		var ph partHeader
		if err := ph.ParseFromPath(path); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, err
	}
	if _, ok := fields["FormatVersion"]; !ok {
		var ph partHeader
		if err := json.Unmarshal(b, &ph); err != nil {
			return nil, err
		}
		if err := validateDownsamplePartHeader(&ph); err != nil {
			return nil, err
		}
		// 缺少版本但含降采样语义字段时不能按 raw 格式解释。
		for _, key := range []string{"SemanticsVersion", "Mode", "Resolutions", "BucketOrigin", "NumericCodec", "Retention"} {
			if _, ok := fields[key]; ok {
				return nil, fmt.Errorf("[downsampling] metadata is missing FormatVersion")
			}
		}
		return nil, nil
	}
	for _, key := range []string{"FormatVersion", "SemanticsVersion", "Mode", "Resolutions", "BucketOrigin", "NumericCodec", "Retention", "RowsCount", "BlocksCount", "MinTimestamp", "MaxTimestamp", "MinDedupInterval"} {
		if v, ok := fields[key]; !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return nil, fmt.Errorf("[downsampling] metadata is missing field %s", key)
		}
	}
	var m downsamplePartMetadata
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

// detectDownsampleFormat 仅根据完整元数据识别格式；数据文件由正式打开过程校验。
func detectDownsampleFormat(path string) (bool, error) {
	m, err := readDownsampleMetadata(path)
	if err != nil {
		return false, fmt.Errorf("[downsampling] cannot inspect part %q: %w", path, err)
	}
	return m != nil, nil
}

// openDownsamplePart 在正常错误路径上关闭已打开文件，不改变原始 inmemory 打开过程。
func openDownsamplePart(path string) (_ *part, err error) {
	m, err := readDownsampleMetadata(path)
	if err != nil {
		return nil, fmt.Errorf("[downsampling] cannot read part metadata %q: %w", path, err)
	}
	if m == nil {
		return nil, fmt.Errorf("[downsampling] part %q is missing downsampling metadata", path)
	}
	b, err := readDownsampleLimitedFile(filepath.Join(path, metaindexFilename), downsampleMaxMetaindexSize)
	if err != nil {
		return nil, err
	}
	if len(b) < len(downsampleMetaindexMagic) || string(b[:len(downsampleMetaindexMagic)]) != downsampleMetaindexMagic {
		return nil, fmt.Errorf("[downsampling] invalid metaindex marker")
	}
	data, err := encoding.DecompressZSTDLimited(nil, b[len(downsampleMetaindexMagic):], downsampleMaxMetaindexSize)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data)%downsampleMetaindexRowSize != 0 {
		return nil, fmt.Errorf("[downsampling] invalid metaindex length")
	}
	p := &part{path: path, ph: m.partHeader, dsMetadata: m}
	defer func() {
		if err != nil {
			for _, f := range []filestream.ReadAtCloser{p.dsTimestampsFile, p.dsValuesFile, p.dsIndexFile} {
				if f != nil {
					err = errors.Join(err, f.Close())
				}
			}
		}
	}()
	if err := openDownsamplePartDataFile(path, timestampsFilename, &p.dsTimestampsFile, &p.dsTimestampsSize); err != nil {
		return nil, err
	}
	if err := openDownsamplePartDataFile(path, valuesFilename, &p.dsValuesFile, &p.dsValuesSize); err != nil {
		return nil, err
	}
	if err := openDownsamplePartDataFile(path, indexFilename, &p.dsIndexFile, &p.dsIndexSize); err != nil {
		return nil, err
	}
	p.size = p.dsTimestampsSize + p.dsValuesSize + p.dsIndexSize + uint64(len(b))
	var rows, blocks, nextOffset uint64
	var minTime, maxTime int64
	for len(data) > 0 {
		var mr downsampleMetaindexRow
		if err := mr.unmarshal(data[:downsampleMetaindexRowSize]); err != nil {
			return nil, err
		}
		if err := checkDownsampleExtent(mr.IndexBlockOffset, mr.IndexBlockSize, p.dsIndexSize); err != nil {
			return nil, err
		}
		if mr.IndexBlockOffset != nextOffset {
			return nil, fmt.Errorf("[downsampling] index offsets are not contiguous")
		}
		nextOffset += uint64(mr.IndexBlockSize)
		if len(p.dsMetaindex) > 0 {
			prev := &p.dsMetaindex[len(p.dsMetaindex)-1]
			if mr.ResolutionMs < prev.ResolutionMs || (mr.ResolutionMs == prev.ResolutionMs && (mr.feature < prev.feature || (mr.feature == prev.feature && mr.TSID.Less(&prev.LastTSID)))) {
				return nil, fmt.Errorf("[downsampling] invalid metaindex order")
			}
		} else {
			minTime = mr.MinTimestamp
			maxTime = mr.MaxTimestamp
		}
		minTime = min(minTime, mr.MinTimestamp)
		maxTime = max(maxTime, mr.MaxTimestamp)
		if ^uint64(0)-rows < mr.RowsCount || ^uint64(0)-blocks < uint64(mr.BlockHeadersCount) {
			return nil, fmt.Errorf("[downsampling] part statistics overflow")
		}
		rows += mr.RowsCount
		blocks += uint64(mr.BlockHeadersCount)
		p.dsMetaindex = append(p.dsMetaindex, mr)
		data = data[downsampleMetaindexRowSize:]
	}
	if rows != p.ph.RowsCount || blocks != p.ph.BlocksCount || minTime != p.ph.MinTimestamp || maxTime != p.ph.MaxTimestamp || nextOffset != p.dsIndexSize {
		return nil, fmt.Errorf("[downsampling] metaindex statistics do not match part statistics")
	}
	if err := validateDownsamplePartIndexes(p); err != nil {
		return nil, fmt.Errorf("[downsampling] cannot validate part %q: %w", path, err)
	}
	p.metaindexSizeBytes = uint64(cap(p.dsMetaindex)) * uint64(unsafe.Sizeof(downsampleMetaindexRow{}))
	p.timestampsFile = p.dsTimestampsFile
	p.valuesFile = p.dsValuesFile
	p.indexFile = p.dsIndexFile
	return p, nil
}

func checkDownsampleExtent(offset uint64, size uint32, fileSize uint64) error {
	if offset > uint64(^uint64(0)>>1)-uint64(size) || offset > fileSize || uint64(size) > fileSize-offset {
		return fmt.Errorf("[downsampling] payload exceeds file bounds: offset=%d size=%d fileSize=%d", offset, size, fileSize)
	}
	return nil
}

// 普通文件校验与打开失败清理由 filestream 负责；成功后交给 part 或 reader 持有。
func openDownsamplePartDataFile(path, name string, dst *filestream.ReadAtCloser, size *uint64) error {
	filePath := filepath.Join(path, name)
	f, err := filestream.OpenReadAt(filePath)
	if err != nil {
		return fmt.Errorf("[downsampling] cannot open reader file %q: %w", filePath, err)
	}
	*dst = f
	*size = f.Size()
	return nil
}

// 五路只保留各自当前 index block，不建立随 part 大小增长的 header/offset 集合。
// 打开校验仅读取索引，不借用合并 reader，也不解码 timestamps.bin 或 values.bin。
func validateDownsamplePartIndexes(p *part) error {
	var cursors [countOfDownsampleFeatures]downsamplePartIndexCursor
	var timestampEnd, valuesEnd uint64
	metaPos := 0
	for _, resolution := range p.dsMetadata.Resolutions {
		var firstValues, lastValues [countOfDownsampleFeatures]uint64
		var previousHeaders [countOfDownsampleFeatures]blockHeader
		var seen bool
		for feature := range cursors {
			start := metaPos
			for metaPos < len(p.dsMetaindex) && p.dsMetaindex[metaPos].ResolutionMs == resolution && p.dsMetaindex[metaPos].feature == uint8(feature) {
				metaPos++
			}
			cursors[feature].rows = p.dsMetaindex[start:metaPos]
			cursors[feature].headers = cursors[feature].headers[:0]
			cursors[feature].headerPos = 0
		}
		for {
			var headers [countOfDownsampleFeatures]*blockHeader
			for feature := range cursors {
				h, err := cursors[feature].nextHeader(p)
				if err != nil {
					return err
				}
				headers[feature] = h
			}
			for feature := 1; feature < countOfDownsampleFeatures; feature++ {
				if (headers[feature] != nil) != (headers[0] != nil) {
					return fmt.Errorf("[downsampling] missing feature columns or extra blocks at resolution %d", resolution)
				}
			}
			h := headers[0]
			if h == nil {
				break
			}
			if h.TimestampsBlockOffset != timestampEnd {
				return fmt.Errorf("[downsampling] shared timestamp payloads are not contiguous")
			}
			timestampEnd += uint64(h.TimestampsBlockSize)
			for feature, column := range headers {
				if !sameDownsampleTimestamps(h, column) {
					return fmt.Errorf("[downsampling] the five feature columns have inconsistent timestamp descriptors at resolution %d", resolution)
				}
				if !seen {
					firstValues[feature] = column.ValuesBlockOffset
				} else {
					if !downsampleHeadersOrdered(&previousHeaders[feature], column) {
						return fmt.Errorf("[downsampling] index blocks are out of order or have overlapping time ranges")
					}
					if column.ValuesBlockOffset != lastValues[feature] {
						return fmt.Errorf("[downsampling] feature column values payloads are not contiguous")
					}
				}
				lastValues[feature] = column.ValuesBlockOffset + uint64(column.ValuesBlockSize)
				previousHeaders[feature] = *column
			}
			seen = true
		}
		if seen {
			for feature := range cursors {
				if firstValues[feature] != valuesEnd {
					return fmt.Errorf("[downsampling] values payloads are not contiguous across resolution/feature groups")
				}
				valuesEnd = lastValues[feature]
			}
		}
	}
	if metaPos != len(p.dsMetaindex) || timestampEnd != p.dsTimestampsSize || valuesEnd != p.dsValuesSize {
		return fmt.Errorf("[downsampling] payload is truncated or contains an unreferenced tail")
	}
	return nil
}

// downsamplePartIndexCursor 只用于打开校验；各特征独立推进，允许其 index 分块边界不同。
// 文件由 part 持有，游标只管理当前 index 的解码工作区，没有文件或对象池所有权。
type downsamplePartIndexCursor struct {
	rows       []downsampleMetaindexRow // 当前分辨率、特征尚未读取的索引行。
	headers    []blockHeader            // 当前 index 的原生 header，下一次读取时复用容量。
	headerPos  int                      // 当前 index 中下一条 header 的位置。
	compressed []byte                   // 单个 index 的压缩读取缓冲。
	indexData  []byte                   // 单个 index 的限长解压缓冲。
}

func (c *downsamplePartIndexCursor) nextHeader(p *part) (*blockHeader, error) {
	if c.headerPos == len(c.headers) {
		if len(c.rows) == 0 {
			return nil, nil
		}
		m := &c.rows[0]
		c.rows = c.rows[1:]
		if m.IndexBlockSize > downsampleMaxIndexSize || m.BlockHeadersCount == 0 {
			return nil, fmt.Errorf("[downsampling] invalid index block size or header count")
		}
		if err := checkDownsampleExtent(m.IndexBlockOffset, m.IndexBlockSize, p.dsIndexSize); err != nil {
			return nil, err
		}
		if cap(c.compressed) < int(m.IndexBlockSize) {
			c.compressed = make([]byte, m.IndexBlockSize)
		} else {
			c.compressed = c.compressed[:m.IndexBlockSize]
		}
		n, err := p.dsIndexFile.ReadAt(c.compressed, int64(m.IndexBlockOffset))
		if err != nil {
			return nil, fmt.Errorf("[downsampling] cannot read index block: %w", err)
		}
		if n != len(c.compressed) {
			return nil, fmt.Errorf("[downsampling] cannot read complete index block: %w", io.ErrUnexpectedEOF)
		}
		if len(c.compressed) < len(downsampleIndexMagic) || string(c.compressed[:len(downsampleIndexMagic)]) != downsampleIndexMagic {
			return nil, fmt.Errorf("[downsampling] invalid index magic")
		}
		c.indexData, err = encoding.DecompressZSTDLimited(c.indexData[:0], c.compressed[len(downsampleIndexMagic):], maxBlockSize)
		if err != nil {
			return nil, fmt.Errorf("[downsampling] cannot decompress index block: %w", err)
		}
		c.headers, err = unmarshalDownsampleIndexBlock(c.headers[:0], c.indexData, m, p.dsTimestampsSize, p.dsValuesSize, p.dsIndexSize)
		if err != nil {
			return nil, err
		}
		c.headerPos = 0
	}
	h := &c.headers[c.headerPos]
	c.headerPos++
	return h, nil
}

// unmarshalDownsampleIndexBlock 将已解压的单个 index block 解码为原生 header。
// part 打开校验、合并和查询共用此无状态校验；跨 index 的完整性由各自的遍历负责。
func unmarshalDownsampleIndexBlock(dst []blockHeader, data []byte, m *downsampleMetaindexRow, timestampsSize, valuesSize, indexSize uint64) ([]blockHeader, error) {
	if m == nil || m.BlockHeadersCount == 0 || uint64(m.BlockHeadersCount)*uint64(marshaledBlockHeaderSize) > maxBlockSize || len(data) != int(m.BlockHeadersCount)*marshaledBlockHeaderSize {
		return dst, fmt.Errorf("[downsampling] invalid decoded index size or header count")
	}
	if m.IndexBlockSize > downsampleMaxIndexSize {
		return dst, fmt.Errorf("[downsampling] invalid index block size")
	}
	if err := checkDownsampleExtent(m.IndexBlockOffset, m.IndexBlockSize, indexSize); err != nil {
		return dst, err
	}
	start := len(dst)
	var err error
	dst, err = unmarshalBlockHeaders(dst, data, int(m.BlockHeadersCount))
	if err != nil {
		return dst[:start], fmt.Errorf("[downsampling] cannot decode index block headers: %w", err)
	}
	headers := dst[start:]
	var rows uint64
	first, last := &headers[0], &headers[len(headers)-1]
	minTime, maxTime := first.MinTimestamp, first.MaxTimestamp
	for i := range headers {
		h := &headers[i]
		if err := validateDownsampleHeader(h); err != nil {
			return dst[:start], err
		}
		if !sameDownsampleTenant(&h.TSID, &m.TSID) || h.TSID.Less(&m.TSID) || m.LastTSID.Less(&h.TSID) {
			return dst[:start], fmt.Errorf("[downsampling] index block has an invalid tenant or TSID range")
		}
		if i > 0 {
			previous := &headers[i-1]
			if !downsampleHeadersOrdered(previous, h) {
				return dst[:start], fmt.Errorf("[downsampling] block headers are out of order or have overlapping time ranges")
			}
			if h.TimestampsBlockOffset != previous.TimestampsBlockOffset+uint64(previous.TimestampsBlockSize) || h.ValuesBlockOffset != previous.ValuesBlockOffset+uint64(previous.ValuesBlockSize) {
				return dst[:start], fmt.Errorf("[downsampling] adjacent block payloads are not contiguous")
			}
		}
		if err := checkDownsampleExtent(h.TimestampsBlockOffset, h.TimestampsBlockSize, timestampsSize); err != nil {
			return dst[:start], err
		}
		if err := checkDownsampleExtent(h.ValuesBlockOffset, h.ValuesBlockSize, valuesSize); err != nil {
			return dst[:start], err
		}
		rows += uint64(h.RowsCount)
		minTime, maxTime = min(minTime, h.MinTimestamp), max(maxTime, h.MaxTimestamp)
	}
	if m.IndexBlockOffset == 0 && (first.TimestampsBlockOffset != 0 || first.ValuesBlockOffset != 0) {
		return dst[:start], fmt.Errorf("[downsampling] the first payload offset is not zero")
	}
	if m.IndexBlockOffset+uint64(m.IndexBlockSize) == indexSize && (last.TimestampsBlockOffset+uint64(last.TimestampsBlockSize) != timestampsSize || last.ValuesBlockOffset+uint64(last.ValuesBlockSize) != valuesSize) {
		return dst[:start], fmt.Errorf("[downsampling] payloads are truncated or have an unreferenced tail")
	}
	if rows != m.RowsCount || first.TSID != m.TSID || last.TSID != m.LastTSID || minTime != m.MinTimestamp || maxTime != m.MaxTimestamp {
		return dst[:start], fmt.Errorf("[downsampling] index statistics do not match the metaindex row")
	}
	return dst, nil
}

// estimateDownsamplePartSize 估算整个目标 part 的编码上界，不假设源 part 已被删除。
// 估算以五个特征的聚合批次为单位；磁盘统计的五个单值 Block 行数须先换算。
func estimateDownsamplePartSize(pws []*partWrapper) uint64 {
	var rows uint64
	for _, pw := range pws {
		if pw == nil || pw.p == nil {
			return math.MaxUint64
		}
		n := pw.p.ph.RowsCount
		if pw.p.dsMetadata == nil {
			n = multiplyDownsampleSpace(n, uint64(len(downsampleResolutions)))
		} else {
			// 完整摘要每行对应五个单值 Block 行；非整除统计保守向上取整。
			physicalRows := n
			n /= countOfDownsampleFeatures
			if physicalRows%countOfDownsampleFeatures != 0 {
				n++
			}
		}
		rows = addDownsampleSpace(rows, n)
	}
	if rows == 0 {
		return 0
	}
	// 不知道 TSID 分布时，每一行都可能单独占据一个 block 和一个 index block。
	return estimateDownsampleOutputSize(rows, rows)
}
