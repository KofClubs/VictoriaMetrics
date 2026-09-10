package storage

import "testing"

func TestDownsampleMetaindexTimeBounds(t *testing.T) {
	for _, timestamp := range []int64{minUnixMilli - 1, maxUnixMilli + 1} {
		mr := downsampleMetaindexRow{metaindexRow: metaindexRow{TSID: TSID{MetricID: 1}, MinTimestamp: timestamp, MaxTimestamp: timestamp, BlockHeadersCount: 1, IndexBlockSize: 16}, ResolutionMs: 300000, feature: 0, LastTSID: TSID{MetricID: 1}, RowsCount: 1}
		var got downsampleMetaindexRow
		if err := got.unmarshal(mr.marshal(nil)); err == nil {
			t.Fatal("非法 metaindex 时间范围未拒绝")
		}
	}
}
