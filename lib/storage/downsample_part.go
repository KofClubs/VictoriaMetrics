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

// detectDownsampleFormat 只读取元数据与标识，供初始化预检查和正式打开共用。
func detectDownsampleFormat(path string) (_ bool, err error) {
	m, err := readDownsampleMetadata(path)
	if err != nil {
		return false, fmt.Errorf("[downsampling] cannot inspect part %q: %w", path, err)
	}
	f, err := filestream.OpenReadAt(filepath.Join(path, metaindexFilename))
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	var magic [8]byte
	n, err := f.ReadAt(magic[:], 0)
	if err != nil {
		return false, fmt.Errorf("[downsampling] part %q has a truncated metaindex marker: %w", path, err)
	}
	if m != nil {
		if string(magic[:n]) != downsampleMetaindexMagic {
			return false, fmt.Errorf("[downsampling] part %q metadata does not match its metaindex marker", path)
		}
	} else if !bytes.Equal(magic[:4], []byte{0x28, 0xb5, 0x2f, 0xfd}) {
		return false, fmt.Errorf("[downsampling] part %q has no valid raw format or has an unknown metaindex marker", path)
	}
	for _, name := range []string{timestampsFilename, valuesFilename, indexFilename} {
		st, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			return false, err
		}
		if !st.Mode().IsRegular() {
			return false, fmt.Errorf("[downsampling] part %q file %s is not a regular file", path, name)
		}
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
// 打开时验证全部索引，避免过滤查询跳过缺失列、跨组空洞或未引用尾部。
func validateDownsamplePartIndexes(p *part) error {
	var readers [countOfDownsampleFeatures]*downsampleReader
	defer func() {
		for _, r := range readers {
			if r != nil {
				putDownsampleReader(r)
			}
		}
	}()
	var timestampEnd, valuesEnd uint64
	for _, resolution := range p.dsMetadata.Resolutions {
		var firstValues, lastValues [countOfDownsampleFeatures]uint64
		var seen bool
		for feature := range readers {
			if readers[feature] == nil {
				readers[feature] = getDownsampleReader()
			}
			if err := readers[feature].Init(p, resolution, uint8(feature)); err != nil {
				return err
			}
		}
		for {
			var present [countOfDownsampleFeatures]bool
			for feature, r := range readers {
				present[feature] = r.NextHeader()
				if err := r.Error(); err != nil {
					return err
				}
			}
			for feature := 1; feature < countOfDownsampleFeatures; feature++ {
				if present[feature] != present[0] {
					return fmt.Errorf("[downsampling] missing feature columns or extra blocks at resolution %d", resolution)
				}
			}
			if !present[0] {
				break
			}
			h := readers[0].Header()
			if h.TimestampsBlockOffset != timestampEnd {
				return fmt.Errorf("[downsampling] shared timestamp payloads are not contiguous")
			}
			timestampEnd += uint64(h.TimestampsBlockSize)
			for feature, r := range readers {
				column := r.Header()
				if !sameDownsampleTimestamps(h, column) {
					return fmt.Errorf("[downsampling] the five feature columns have inconsistent timestamp descriptors at resolution %d", resolution)
				}
				if !seen {
					firstValues[feature] = column.ValuesBlockOffset
				} else if column.ValuesBlockOffset != lastValues[feature] {
					return fmt.Errorf("[downsampling] feature column values payloads are not contiguous")
				}
				lastValues[feature] = column.ValuesBlockOffset + uint64(column.ValuesBlockSize)
			}
			seen = true
		}
		if seen {
			for feature := range readers {
				if firstValues[feature] != valuesEnd {
					return fmt.Errorf("[downsampling] values payloads are not contiguous across resolution/feature groups")
				}
				valuesEnd = lastValues[feature]
			}
		}
	}
	if timestampEnd != p.dsTimestampsSize || valuesEnd != p.dsValuesSize {
		return fmt.Errorf("[downsampling] payload is truncated or contains an unreferenced tail")
	}
	return nil
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
