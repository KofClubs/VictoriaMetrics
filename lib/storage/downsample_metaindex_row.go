package storage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

// downsampleMetaindexRow 描述同一分辨率、特征和租户的一组 block header，指向一个 index block。
type downsampleMetaindexRow struct {
	metaindexRow        // 复用原始索引行的首个 TSID、时间范围及 index block 位置。
	feature      uint8  // 特征编号，与 downsampleFeature* 的 iota 值一致（0..4）。
	ResolutionMs int64  // 此索引行对应的分辨率，单位为毫秒。
	LastTSID     TSID   // 此索引行覆盖的最后一个 TSID，用于定位范围。
	RowsCount    uint64 // 此特征下全部 block 的物理行数之和。
}

// downsampleMetaindexRowSize 随集群版或单机版的 TSID 编码长度计算。
var downsampleMetaindexRowSize = func() int {
	var m downsampleMetaindexRow
	return len(m.marshal(nil))
}()

func (m *downsampleMetaindexRow) marshal(dst []byte) []byte {
	dst = m.metaindexRow.Marshal(dst)
	dst = append(dst, m.feature)
	dst = encoding.MarshalInt64(dst, m.ResolutionMs)
	dst = m.LastTSID.Marshal(dst)
	return encoding.MarshalUint64(dst, m.RowsCount)
}

func (m *downsampleMetaindexRow) unmarshal(src []byte) error {
	if len(src) != downsampleMetaindexRowSize {
		return fmt.Errorf("[downsampling] invalid metaindex row length")
	}
	tail, err := m.metaindexRow.Unmarshal(src)
	if err != nil {
		return fmt.Errorf("[downsampling] cannot decode metaindex row: %w", err)
	}
	m.feature = tail[0]
	m.ResolutionMs = encoding.UnmarshalInt64(tail[1:])
	tail, err = m.LastTSID.Unmarshal(tail[9:])
	if err != nil {
		return fmt.Errorf("[downsampling] cannot decode metaindex last TSID: %w", err)
	}
	m.RowsCount = encoding.UnmarshalUint64(tail)
	if !validDownsampleResolution(m.ResolutionMs) || m.feature >= countOfDownsampleFeatures || !sameDownsampleTenant(&m.TSID, &m.LastTSID) || m.LastTSID.Less(&m.TSID) || m.MinTimestamp > m.MaxTimestamp || m.MinTimestamp < minUnixMilli || m.MaxTimestamp > maxUnixMilli || m.RowsCount < uint64(m.BlockHeadersCount) || m.RowsCount > uint64(m.BlockHeadersCount)*maxRowsPerBlock || uint64(m.BlockHeadersCount) > uint64(maxBlockSize/marshaledBlockHeaderSize) || m.IndexBlockSize < uint32(len(downsampleIndexMagic)) || m.IndexBlockOffset > uint64(^uint64(0)>>1)-uint64(m.IndexBlockSize) {
		return fmt.Errorf("[downsampling] invalid metaindex row")
	}
	return nil
}
