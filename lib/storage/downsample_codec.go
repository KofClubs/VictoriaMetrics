package storage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

const (
	downsampleVersion       = 2
	downsampleMaxColumnSize = 2 * maxBlockSize
	// 原始输入沿用已有双倍行数上限。
	downsampleMaxRawRows = 2 * maxRowsPerBlock
	// Go append 的正常容量增长也属于可复用容量，不只按逻辑行数判断峰值。
	downsampleMaxPooledRows    = 2 * downsampleMaxRawRows
	downsampleMaxIndexSize     = 2 * maxBlockSize
	downsampleMaxMetaindexSize = 64 << 20
	downsampleMaxMetadataSize  = 64 << 10
	downsampleTimestampFeature = 255
)

// 下列尺寸依赖 TSID/blockHeader 的序列化大小，而集群版 TSID 含 AccountID/ProjectID，
// 比单机版多 8 字节，因此不能硬编码，必须运行时计算。
var downsampleFieldHeaderSize = func() int {
	var h downsampleFieldHeader
	return len(h.marshal(nil))
}()

var downsampleBlockHeaderSize = downsampleFeaturesCount * downsampleFieldHeaderSize

var downsampleMetaindexRowSize = func() int {
	var m downsampleMetaindexRow
	return len(m.marshal(nil))
}()

// 前缀不属于 ZSTD 帧，raw reader 会在读取 metaindex 时明确拒绝。
const downsampleMetaindexMagic = "VMDSMI\x00\x02"
const downsampleIndexMagic = "VMDSIX\x00\x02"

type downsampleColumnHeader struct {
	Feature       uint8
	Offset        uint64
	Size          uint32
	FirstValue    int64
	Scale         int16
	PrecisionBits uint8
	MarshalType   encoding.MarshalType
}

type downsampleBlockHeader struct {
	TSID         TSID
	ResolutionMs int64
	RowsCount    uint32
	MinTimestamp int64
	MaxTimestamp int64
	Timestamps   downsampleColumnHeader
	Columns      [downsampleFeaturesCount]downsampleColumnHeader
	// raw 格式适配只在内存中使用，不进入文件序列化。
	raw       bool
	rawHeader blockHeader
}

type downsampleMetaindexRow struct {
	ResolutionMs      int64
	TSID              TSID
	LastTSID          TSID
	MinTimestamp      int64
	MaxTimestamp      int64
	BlockHeadersCount uint32
	IndexBlockOffset  uint64
	IndexBlockSize    uint32
	RowsCount         uint64
}

func validDownsampleResolution(r int64) bool { return r == 300000 || r == 3600000 }

// downsampleFieldHeader 包装一个原生 blockHeader；每个条目只描述一个分辨率与一个特征值。
type downsampleFieldHeader struct {
	ResolutionMs           int64
	Feature                uint8
	TimestampPrecisionBits uint8
	BlockHeader            blockHeader
}

func (h *downsampleFieldHeader) marshal(dst []byte) []byte {
	dst = encoding.MarshalInt64(dst, h.ResolutionMs)
	dst = append(dst, h.Feature, h.TimestampPrecisionBits)
	return h.BlockHeader.Marshal(dst)
}

func (h *downsampleFieldHeader) unmarshal(src []byte) error {
	if len(src) != downsampleFieldHeaderSize {
		return fmt.Errorf("单特征 block header 长度错误: %d", len(src))
	}
	h.ResolutionMs = encoding.UnmarshalInt64(src)
	h.Feature = src[8]
	h.TimestampPrecisionBits = src[9]
	if !validDownsampleResolution(h.ResolutionMs) || h.Feature < 1 || h.Feature > downsampleFeaturesCount || h.TimestampPrecisionBits < 1 || h.TimestampPrecisionBits > 64 {
		return fmt.Errorf("单特征 block 标识或时间戳精度无效")
	}
	if _, err := h.BlockHeader.Unmarshal(src[10:]); err != nil {
		return err
	}
	bh := &h.BlockHeader
	// 保留现有磁盘字段，但时间戳和 values 必须共用原生 Block 的精度。
	if h.TimestampPrecisionBits != bh.PrecisionBits {
		return fmt.Errorf("单特征 block 时间戳与 values 精度不一致")
	}
	if bh.RowsCount > maxRowsPerBlock || bh.MinTimestamp > bh.MaxTimestamp || bh.MinTimestamp < minUnixMilli || bh.MaxTimestamp > maxUnixMilli {
		return fmt.Errorf("单特征 block 行数或时间范围无效")
	}
	if bh.TimestampsBlockOffset > uint64(^uint64(0)>>1) || bh.ValuesBlockOffset > uint64(^uint64(0)>>1) {
		return fmt.Errorf("单特征 block 偏移量溢出")
	}
	return nil
}

// fieldHeader 生成对应特征的原生 Block 描述；五个条目引用同一时间戳负载。
func (h *downsampleBlockHeader) fieldHeader(feature int) downsampleFieldHeader {
	c := &h.Columns[feature]
	return downsampleFieldHeader{ResolutionMs: h.ResolutionMs, Feature: c.Feature, TimestampPrecisionBits: h.Timestamps.PrecisionBits, BlockHeader: blockHeader{
		TSID: h.TSID, RowsCount: h.RowsCount, MinTimestamp: h.MinTimestamp, MaxTimestamp: h.MaxTimestamp,
		TimestampsBlockOffset: h.Timestamps.Offset, TimestampsBlockSize: h.Timestamps.Size, TimestampsMarshalType: h.Timestamps.MarshalType,
		ValuesBlockOffset: c.Offset, ValuesBlockSize: c.Size, ValuesMarshalType: c.MarshalType,
		FirstValue: c.FirstValue, Scale: c.Scale, PrecisionBits: c.PrecisionBits,
	}}
}

// marshal 将批次中的五个原生 Block header 依次写入索引，批次自身没有磁盘 header。
func (h *downsampleBlockHeader) marshal(dst []byte) []byte {
	for i := range h.Columns {
		fh := h.fieldHeader(i)
		dst = fh.marshal(dst)
	}
	return dst
}

func (h *downsampleBlockHeader) unmarshal(src []byte) error {
	if len(src) != downsampleBlockHeaderSize {
		return fmt.Errorf("降采样批次索引长度错误: %d", len(src))
	}
	*h = downsampleBlockHeader{}
	for i := range h.Columns {
		var fh downsampleFieldHeader
		if err := fh.unmarshal(src[i*downsampleFieldHeaderSize : (i+1)*downsampleFieldHeaderSize]); err != nil {
			return err
		}
		bh := &fh.BlockHeader
		if fh.Feature != uint8(i+1) {
			return fmt.Errorf("降采样特征缺失、重复或顺序错误")
		}
		if i == 0 {
			h.TSID, h.ResolutionMs, h.RowsCount, h.MinTimestamp, h.MaxTimestamp = bh.TSID, fh.ResolutionMs, bh.RowsCount, bh.MinTimestamp, bh.MaxTimestamp
			h.Timestamps = downsampleColumnHeader{Feature: downsampleTimestampFeature, Offset: bh.TimestampsBlockOffset, Size: bh.TimestampsBlockSize, FirstValue: bh.MinTimestamp, PrecisionBits: fh.TimestampPrecisionBits, MarshalType: bh.TimestampsMarshalType}
		} else if bh.TSID != h.TSID || fh.ResolutionMs != h.ResolutionMs || bh.RowsCount != h.RowsCount || bh.MinTimestamp != h.MinTimestamp || bh.MaxTimestamp != h.MaxTimestamp || bh.TimestampsBlockOffset != h.Timestamps.Offset || bh.TimestampsBlockSize != h.Timestamps.Size || bh.TimestampsMarshalType != h.Timestamps.MarshalType || fh.TimestampPrecisionBits != h.Timestamps.PrecisionBits {
			return fmt.Errorf("同一批次的单特征 Block 未共享一致的时间戳描述")
		}
		h.Columns[i] = downsampleColumnHeader{Feature: fh.Feature, Offset: bh.ValuesBlockOffset, Size: bh.ValuesBlockSize, FirstValue: bh.FirstValue, Scale: bh.Scale, PrecisionBits: bh.PrecisionBits, MarshalType: bh.ValuesMarshalType}
		if i > 0 && bh.ValuesBlockOffset != h.Columns[i-1].Offset+uint64(h.Columns[i-1].Size) {
			return fmt.Errorf("降采样 values 列偏移不连续")
		}
	}
	return nil
}

func validateDownsampleRowCodec(mt encoding.MarshalType, rows uint32) error {
	if rows < 2 && (mt == encoding.MarshalTypeNearestDelta2 || mt == encoding.MarshalTypeZSTDNearestDelta2) {
		return fmt.Errorf("二阶差分编码要求至少两行")
	}
	return nil
}

func (h *downsampleBlockHeader) less(other *downsampleBlockHeader) bool {
	if h.ResolutionMs != other.ResolutionMs {
		return h.ResolutionMs < other.ResolutionMs
	}
	if h.TSID != other.TSID {
		return h.TSID.Less(&other.TSID)
	}
	return h.MinTimestamp < other.MinTimestamp
}

func (m *downsampleMetaindexRow) marshal(dst []byte) []byte {
	dst = encoding.MarshalInt64(dst, m.ResolutionMs)
	dst = m.TSID.Marshal(dst)
	dst = m.LastTSID.Marshal(dst)
	dst = encoding.MarshalInt64(dst, m.MinTimestamp)
	dst = encoding.MarshalInt64(dst, m.MaxTimestamp)
	dst = encoding.MarshalUint32(dst, m.BlockHeadersCount)
	dst = encoding.MarshalUint64(dst, m.IndexBlockOffset)
	dst = encoding.MarshalUint32(dst, m.IndexBlockSize)
	return encoding.MarshalUint64(dst, m.RowsCount)
}

func (m *downsampleMetaindexRow) unmarshal(src []byte) error {
	if len(src) != downsampleMetaindexRowSize {
		return fmt.Errorf("降采样 metaindex 行长度错误")
	}
	m.ResolutionMs = encoding.UnmarshalInt64(src)
	rest := src[8:]
	tail, err := m.TSID.Unmarshal(rest)
	if err != nil {
		return err
	}
	tail, err = m.LastTSID.Unmarshal(tail)
	if err != nil {
		return err
	}
	m.MinTimestamp = encoding.UnmarshalInt64(tail)
	m.MaxTimestamp = encoding.UnmarshalInt64(tail[8:])
	m.BlockHeadersCount = encoding.UnmarshalUint32(tail[16:])
	m.IndexBlockOffset = encoding.UnmarshalUint64(tail[20:])
	m.IndexBlockSize = encoding.UnmarshalUint32(tail[28:])
	m.RowsCount = encoding.UnmarshalUint64(tail[32:])
	if !validDownsampleResolution(m.ResolutionMs) || m.LastTSID.Less(&m.TSID) || m.MinTimestamp > m.MaxTimestamp || m.MinTimestamp < minUnixMilli || m.MaxTimestamp > maxUnixMilli || m.BlockHeadersCount == 0 || m.BlockHeadersCount%downsampleFeaturesCount != 0 || m.RowsCount < uint64(m.BlockHeadersCount) || m.RowsCount > uint64(m.BlockHeadersCount)*maxRowsPerBlock || uint64(m.BlockHeadersCount) > uint64(maxBlockSize/downsampleBlockHeaderSize*downsampleFeaturesCount) || m.IndexBlockSize < uint32(len(downsampleIndexMagic)) || m.IndexBlockSize > downsampleMaxIndexSize {
		return fmt.Errorf("无效降采样 metaindex 行")
	}
	return nil
}

func checkDownsampleExtent(offset uint64, size uint32, fileSize uint64) error {
	if offset > fileSize || uint64(size) > fileSize-offset {
		return fmt.Errorf("降采样负载越过文件边界: offset=%d size=%d fileSize=%d", offset, size, fileSize)
	}
	return nil
}
