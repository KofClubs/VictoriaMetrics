package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// downsamplePartMetadata 保留原有统计字段，并显式声明 v3 的计算语义。
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
	return downsamplePartMetadata{partHeader: ph, FormatVersion: downsampleFormatVersion, SemanticsVersion: downsampleSemanticsVersion, Mode: "downsampling", Resolutions: []int64{300000, 3600000}, NumericCodec: "decimal-values-v1", Retention: "bucket-end"}
}

func (m *downsamplePartMetadata) validate() error {
	if m.FormatVersion != downsampleFormatVersion || m.SemanticsVersion != downsampleSemanticsVersion || m.Mode != "downsampling" || len(m.Resolutions) != 2 || m.Resolutions[0] != 300000 || m.Resolutions[1] != 3600000 || m.BucketOrigin != 0 || m.NumericCodec != "decimal-values-v1" || m.Retention != "bucket-end" || m.MinDedupInterval != 0 {
		return fmt.Errorf("不支持或矛盾的降采样格式、分辨率或语义元数据")
	}
	if m.RowsCount%downsampleFeaturesCount != 0 || m.BlocksCount%downsampleFeaturesCount != 0 {
		return fmt.Errorf("降采样物理行数和 Block 数量必须按五个特征成组")
	}
	if m.MinTimestamp < minUnixMilli || m.MaxTimestamp > maxUnixMilli {
		return fmt.Errorf("降采样 part 时间范围超出支持域")
	}
	return validateDownsamplePartHeader(&m.partHeader)
}

func validateDownsamplePartHeader(ph *partHeader) error {
	if ph.RowsCount == 0 || ph.BlocksCount == 0 || ph.BlocksCount > ph.RowsCount || ph.MinTimestamp > ph.MaxTimestamp {
		return fmt.Errorf("无效 part 统计或时间范围")
	}
	return nil
}

func readDownsampleLimitedFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("文件 %q 超过大小上限 %d", path, limit)
	}
	return b, nil
}

// readDownsampleMetadata 不把损坏的新格式解释为原始格式。
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
		// 缺少版本但含新格式语义字段时不能按旧格式解释。
		for _, key := range []string{"SemanticsVersion", "Mode", "Resolutions", "BucketOrigin", "NumericCodec", "Retention"} {
			if _, ok := fields[key]; ok {
				return nil, fmt.Errorf("元数据缺少 FormatVersion")
			}
		}
		return nil, nil
	}
	for _, key := range []string{"FormatVersion", "SemanticsVersion", "Mode", "Resolutions", "BucketOrigin", "NumericCodec", "Retention", "RowsCount", "BlocksCount", "MinTimestamp", "MaxTimestamp", "MinDedupInterval"} {
		if v, ok := fields[key]; !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return nil, fmt.Errorf("降采样元数据缺少字段 %s", key)
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
func detectDownsampleFormat(path string) (bool, error) {
	m, err := readDownsampleMetadata(path)
	if err != nil {
		return false, fmt.Errorf("检查 part %q: %w", path, err)
	}
	f, err := os.Open(filepath.Join(path, metaindexFilename))
	if err != nil {
		return false, err
	}
	defer f.Close()
	var magic [8]byte
	n, err := io.ReadFull(f, magic[:])
	if err != nil {
		return false, fmt.Errorf("part %q 的 metaindex 标识被截断: %w", path, err)
	}
	if m != nil {
		if string(magic[:n]) != downsampleMetaindexMagic {
			return false, fmt.Errorf("part %q 的 v3 元数据与 metaindex 标识矛盾", path)
		}
	} else if !bytes.Equal(magic[:4], []byte{0x28, 0xb5, 0x2f, 0xfd}) {
		return false, fmt.Errorf("part %q 缺少合法原始格式或具有未知 metaindex 标识", path)
	}
	for _, name := range []string{timestampsFilename, valuesFilename, indexFilename} {
		st, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			return false, err
		}
		if !st.Mode().IsRegular() {
			return false, fmt.Errorf("part %q 的 %s 不是普通文件", path, name)
		}
	}
	return m != nil, nil
}

// openDownsamplePart 在正常错误路径上关闭已打开文件，不改变原始 inmemory 打开过程。
func openDownsamplePart(path string) (_ *part, err error) {
	m, err := readDownsampleMetadata(path)
	if err != nil || m == nil {
		return nil, fmt.Errorf("读取 v3 元数据 %q: %v", path, err)
	}
	b, err := readDownsampleLimitedFile(filepath.Join(path, metaindexFilename), downsampleMaxMetaindexSize)
	if err != nil {
		return nil, err
	}
	if len(b) < len(downsampleMetaindexMagic) || string(b[:len(downsampleMetaindexMagic)]) != downsampleMetaindexMagic {
		return nil, fmt.Errorf("v3 metaindex 标识错误")
	}
	data, err := encoding.DecompressZSTDLimited(nil, b[len(downsampleMetaindexMagic):], downsampleMaxMetaindexSize)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data)%downsampleMetaindexRowSize != 0 {
		return nil, fmt.Errorf("v3 metaindex 长度错误")
	}
	p := &part{path: path, ph: m.partHeader, dsMetadata: m}
	defer func() {
		if err != nil {
			for _, f := range p.dsFiles {
				if f != nil {
					_ = f.Close()
				}
			}
		}
	}()
	for i, name := range []string{timestampsFilename, valuesFilename, indexFilename} {
		p.dsFiles[i], err = os.Open(filepath.Join(path, name))
		if err != nil {
			return nil, err
		}
		st, e := p.dsFiles[i].Stat()
		if e != nil {
			return nil, e
		}
		p.dsFileSizes[i] = uint64(st.Size())
		p.size += uint64(st.Size())
	}
	p.size += uint64(len(b))
	var rows, blocks, nextOffset uint64
	var minTime, maxTime int64
	for len(data) > 0 {
		var mr downsampleMetaindexRow
		if err := mr.unmarshal(data[:downsampleMetaindexRowSize]); err != nil {
			return nil, err
		}
		if err := checkDownsampleExtent(mr.IndexBlockOffset, mr.IndexBlockSize, p.dsFileSizes[2]); err != nil {
			return nil, err
		}
		if mr.IndexBlockOffset != nextOffset {
			return nil, fmt.Errorf("v3 index 偏移不连续")
		}
		nextOffset += uint64(mr.IndexBlockSize)
		if len(p.dsMetaindex) > 0 {
			prev := &p.dsMetaindex[len(p.dsMetaindex)-1]
			if mr.ResolutionMs < prev.ResolutionMs || (mr.ResolutionMs == prev.ResolutionMs && mr.TSID.Less(&prev.LastTSID)) {
				return nil, fmt.Errorf("v3 metaindex 排序错误")
			}
		} else {
			minTime = mr.MinTimestamp
			maxTime = mr.MaxTimestamp
		}
		minTime = min(minTime, mr.MinTimestamp)
		maxTime = max(maxTime, mr.MaxTimestamp)
		if ^uint64(0)-rows < mr.RowsCount || ^uint64(0)-blocks < uint64(mr.BlockHeadersCount) {
			return nil, fmt.Errorf("v3 统计溢出")
		}
		rows += mr.RowsCount
		blocks += uint64(mr.BlockHeadersCount)
		p.dsMetaindex = append(p.dsMetaindex, mr)
		data = data[downsampleMetaindexRowSize:]
	}
	if rows != p.ph.RowsCount || blocks != p.ph.BlocksCount || minTime != p.ph.MinTimestamp || maxTime != p.ph.MaxTimestamp || nextOffset != p.dsFileSizes[2] {
		return nil, fmt.Errorf("v3 metaindex 与 part 统计矛盾")
	}
	p.metaindexSizeBytes = uint64(cap(p.dsMetaindex)) * uint64(unsafe.Sizeof(downsampleMetaindexRow{}))
	p.timestampsFile = &downsamplePartFile{f: p.dsFiles[0]}
	p.valuesFile = &downsamplePartFile{f: p.dsFiles[1]}
	p.indexFile = &downsamplePartFile{f: p.dsFiles[2]}
	return p, nil
}

// 此适配器只为原有 part 生命周期提供 Must 接口，新 reader 使用可返回错误的 ReadAt。
type downsamplePartFile struct{ f *os.File }

func (f *downsamplePartFile) Path() string { return f.f.Name() }
func (f *downsamplePartFile) MustReadAt(b []byte, off int64) {
	if _, err := f.f.ReadAt(b, off); err != nil {
		logger.Panicf("FATAL: 读取 %q: %s", f.f.Name(), err)
	}
}
func (f *downsamplePartFile) MustClose() {
	if err := f.f.Close(); err != nil {
		logger.Panicf("FATAL: 关闭 %q: %s", f.f.Name(), err)
	}
}
