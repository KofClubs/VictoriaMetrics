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
)

// 尺寸随集群版/单机版 TSID 大小计算。

var downsampleMetaindexRowSize = func() int {
	var m downsampleMetaindexRow
	return len(m.marshal(nil))
}()

// 前缀不属于 ZSTD 帧，raw reader 会在读取 metaindex 时明确拒绝。
const downsampleMetaindexMagic = "VMDSMI\x00\x02"
const downsampleIndexMagic = "VMDSIX\x00\x02"

type downsampleMetaindexRow struct {
	metaindexRow
	feature      uint8 // 特征编号，与 downsampleFeature* 的 iota 值一致（0..4）。
	ResolutionMs int64
	LastTSID     TSID
	RowsCount    uint64
}

func validDownsampleResolution(r int64) bool { return r == 300000 || r == 3600000 }

func validateDownsampleHeader(h *blockHeader) error {
	if err := h.validate(); err != nil {
		return err
	}
	if h.RowsCount > maxRowsPerBlock || h.MinTimestamp > h.MaxTimestamp || h.MinTimestamp < minUnixMilli || h.MaxTimestamp > maxUnixMilli || (h.RowsCount == 1 && h.MinTimestamp != h.MaxTimestamp) {
		return fmt.Errorf("单特征 block 行数或时间范围无效")
	}
	if h.TimestampsBlockOffset > uint64(^uint64(0)>>1)-uint64(h.TimestampsBlockSize) || h.ValuesBlockOffset > uint64(^uint64(0)>>1)-uint64(h.ValuesBlockSize) {
		return fmt.Errorf("单特征 block 偏移量溢出")
	}
	for _, column := range []struct {
		mt   encoding.MarshalType
		size uint32
	}{
		{h.TimestampsMarshalType, h.TimestampsBlockSize},
		{h.ValuesMarshalType, h.ValuesBlockSize},
	} {
		if column.mt == 0 || (column.mt == encoding.MarshalTypeConst && column.size != 0) || (column.mt != encoding.MarshalTypeConst && column.size == 0) || (column.mt == encoding.MarshalTypeDeltaConst && column.size > 10) {
			return fmt.Errorf("单特征 block 编码类型与负载大小矛盾")
		}
	}
	if h.TimestampsMarshalType == encoding.MarshalTypeConst && h.MinTimestamp != h.MaxTimestamp {
		return fmt.Errorf("常量时间戳与 header 时间范围矛盾")
	}
	return nil
}

// 编码前 bucket 唯一；有损时间戳可能移动 bucket，磁盘排序只要求批次时间范围不重叠。
func downsampleHeadersOrdered(a, b *blockHeader) bool {
	return downsampleHeaderLess(a, b) && (a.TSID != b.TSID || a.MaxTimestamp < b.MinTimestamp)
}

func downsampleHeaderLess(a, b *blockHeader) bool {
	if a.TSID != b.TSID {
		return a.TSID.Less(&b.TSID)
	}
	return a.MinTimestamp < b.MinTimestamp
}

func sameDownsampleTimestamps(a, b *blockHeader) bool {
	return a.TSID == b.TSID && a.RowsCount == b.RowsCount && a.MinTimestamp == b.MinTimestamp && a.MaxTimestamp == b.MaxTimestamp && a.PrecisionBits == b.PrecisionBits && a.TimestampsBlockOffset == b.TimestampsBlockOffset && a.TimestampsBlockSize == b.TimestampsBlockSize && a.TimestampsMarshalType == b.TimestampsMarshalType
}

func sameDownsampleTenant(a, b *TSID) bool {
	return a.AccountID == b.AccountID && a.ProjectID == b.ProjectID
}

func validateDownsampleRowCodec(mt encoding.MarshalType, rows uint32) error {
	if rows < 2 && (mt == encoding.MarshalTypeNearestDelta2 || mt == encoding.MarshalTypeZSTDNearestDelta2) {
		return fmt.Errorf("二阶差分编码要求至少两行")
	}
	return nil
}

func (m *downsampleMetaindexRow) marshal(dst []byte) []byte {
	dst = m.metaindexRow.Marshal(dst)
	dst = append(dst, m.feature)
	dst = encoding.MarshalInt64(dst, m.ResolutionMs)
	dst = m.LastTSID.Marshal(dst)
	return encoding.MarshalUint64(dst, m.RowsCount)
}

func (m *downsampleMetaindexRow) unmarshal(src []byte) error {
	if len(src) != downsampleMetaindexRowSize {
		return fmt.Errorf("降采样 metaindex 行长度错误")
	}
	tail, err := m.metaindexRow.Unmarshal(src)
	if err != nil {
		return err
	}
	m.feature = tail[0]
	m.ResolutionMs = encoding.UnmarshalInt64(tail[1:])
	tail, err = m.LastTSID.Unmarshal(tail[9:])
	if err != nil {
		return err
	}
	m.RowsCount = encoding.UnmarshalUint64(tail)
	if !validDownsampleResolution(m.ResolutionMs) || m.feature >= countOfDownsampleFeatures || !sameDownsampleTenant(&m.TSID, &m.LastTSID) || m.LastTSID.Less(&m.TSID) || m.MinTimestamp > m.MaxTimestamp || m.MinTimestamp < minUnixMilli || m.MaxTimestamp > maxUnixMilli || m.RowsCount < uint64(m.BlockHeadersCount) || m.RowsCount > uint64(m.BlockHeadersCount)*maxRowsPerBlock || uint64(m.BlockHeadersCount) > uint64(maxBlockSize/marshaledBlockHeaderSize) || m.IndexBlockSize < uint32(len(downsampleIndexMagic)) || m.IndexBlockOffset > uint64(^uint64(0)>>1)-uint64(m.IndexBlockSize) {
		return fmt.Errorf("无效降采样 metaindex 行")
	}
	return nil
}

func checkDownsampleExtent(offset uint64, size uint32, fileSize uint64) error {
	if offset > uint64(^uint64(0)>>1)-uint64(size) || offset > fileSize || uint64(size) > fileSize-offset {
		return fmt.Errorf("降采样负载越过文件边界: offset=%d size=%d fileSize=%d", offset, size, fileSize)
	}
	return nil
}
