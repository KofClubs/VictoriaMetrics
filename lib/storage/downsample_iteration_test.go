package storage

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
)

const (
	downsampleIterationSeries = 180
	// fixture 将每个 feature 的 index 限为 132 个原生 header，使长序列跨 index。
	downsampleIterationWideSeries = 131
	downsampleIterationWideRows   = 8192 + 37
)

type downsampleIterationSample struct {
	tsid      TSID
	timestamp int64
	value     float64
}

func downsampleIterationTSID(series int) TSID {
	// 高位分组字段递增；每跨一个 InstanceID，MetricID 反向跳变。
	// 整体仅按 TSID.Less 有序，不能用 MetricID 大小替代完整比较。
	return TSID{
		MetricGroupID: uint64(100 + series/60),
		JobID:         uint32(200 + (series%60)/20),
		InstanceID:    uint32(300 + (series%20)/10),
		MetricID:      uint64(100000 - (series/10)*1000 + (series%10)*10),
	}
}

func downsampleIterationRows(series int) int {
	if series == downsampleIterationWideSeries {
		return downsampleIterationWideRows
	}
	return 3 + series%5
}

func downsampleIterationTimestamp(series, row int, resolution int64) int64 {
	return minUnixMilli + int64(row)*resolution + int64(series%97+1+(row%7)*13)
}

func downsampleIterationValue(series, row int, resolution int64, feature uint8) float64 {
	// 各维度占用互不重叠的数位，四分之一的小数可由 float64 精确表示。
	slot := int64(1)
	if resolution == downsampleResolution1h {
		slot = 2
	}
	return float64(slot*1e12+int64(series)*1e6+int64(feature)*1e5+int64(row)*4) + 0.25
}

func newDownsampleIterationPart(t *testing.T) *part {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	w.indexLimit = (downsampleIterationWideSeries + 1) * marshaledBlockHeaderSize
	b := getDownsampleBatch()
	defer putDownsampleBatch(b)
	var expectedRows uint64
	for _, resolution := range []int64{300000, 3600000} {
		for series := 0; series < downsampleIterationSeries; series++ {
			rows := downsampleIterationRows(series)
			expectedRows += uint64(rows) * 5
			for start := 0; start < rows; start += 8192 {
				b.Reset()
				b.tsid = downsampleIterationTSID(series)
				b.resolution = resolution
				b.precisionBits = 64
				for row := start; row < min(start+8192, rows); row++ {
					b.timestamps = append(b.timestamps, downsampleIterationTimestamp(series, row, resolution))
					for feature := range b.values {
						b.values[feature] = append(b.values[feature], downsampleIterationValue(series, row, resolution, uint8(feature)))
					}
				}
				if err := w.WriteBlock(b); err != nil {
					t.Fatalf("写入分辨率 %d、序列 %d、起始行 %d: %v", resolution, series, start, err)
				}
			}
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	p, err := openDownsamplePart(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.MustClose)
	if p.ph.RowsCount != expectedRows || p.ph.BlocksCount != 181*2*5 {
		t.Fatalf("fixture 物理统计错误: %+v", p.ph)
	}
	if len(p.dsMetaindex) != 2*5*2 {
		t.Fatalf("fixture 未形成两个分辨率、五个 feature 各两个 index: %d", len(p.dsMetaindex))
	}
	wide := downsampleIterationTSID(downsampleIterationWideSeries)
	for i, resolution := range []int64{300000, 3600000} {
		for feature := 0; feature < 5; feature++ {
			pos := (i*5 + feature) * 2
			left, right := &p.dsMetaindex[pos], &p.dsMetaindex[pos+1]
			if left.ResolutionMs != resolution || right.ResolutionMs != resolution || left.feature != uint8(feature+1) || right.feature != uint8(feature+1) || left.LastTSID != wide || right.TSID != wide || left.BlockHeadersCount != 132 || right.BlockHeadersCount != 49 {
				t.Fatalf("分辨率 %d 特征 %d 的长序列未跨 index: 左=%+v，右=%+v", resolution, feature, left, right)
			}
		}
	}
	return p
}

func downsampleIterationExpected(tsids []TSID, resolution int64, feature uint8, tr TimeRange) []downsampleIterationSample {
	var samples []downsampleIterationSample
	for series := 0; series < downsampleIterationSeries; series++ {
		tsid := downsampleIterationTSID(series)
		if tsids != nil {
			i := sort.Search(len(tsids), func(i int) bool { return !tsids[i].Less(&tsid) })
			if i == len(tsids) || tsids[i] != tsid {
				continue
			}
		}
		for row := 0; row < downsampleIterationRows(series); row++ {
			timestamp := downsampleIterationTimestamp(series, row, resolution)
			if timestamp >= tr.MinTimestamp && timestamp <= tr.MaxTimestamp {
				samples = append(samples, downsampleIterationSample{tsid: tsid, timestamp: timestamp, value: downsampleIterationValue(series, row, resolution, feature)})
			}
		}
	}
	return samples
}

func checkDownsampleIterationBlock(t *testing.T, br *BlockRef, tr TimeRange, expected []downsampleIterationSample, offset *int) {
	t.Helper()
	// 查询将引用暂存后，再通过原有单值 Block 完成读取、解码及闭区间过滤。
	// 复刻单机版 BlockRef.Init(PartRef, data) 语义：仅保留 part 指针 + header 序列化往返。
	var restored BlockRef
	restored.p = br.p
	tail, err := restored.bh.Unmarshal(br.bh.Marshal(nil))
	if err != nil {
		t.Fatalf("cannot unmarshal block header: %v", err)
	}
	if len(tail) > 0 {
		t.Fatalf("unexpected non-empty tail after unmarshaling block header: len(tail)=%d", len(tail))
	}
	var b Block
	restored.MustReadBlock(&b)
	if err := b.UnmarshalData(); err != nil {
		t.Fatal(err)
	}
	timestamps, values := b.AppendRowsWithTimeRangeFilter(nil, nil, tr)
	if len(timestamps) != len(values) {
		t.Fatalf("时间戳与值长度不一致: %d/%d", len(timestamps), len(values))
	}
	for i, timestamp := range timestamps {
		got := downsampleIterationSample{tsid: b.bh.TSID, timestamp: timestamp, value: values[i]}
		if *offset >= len(expected) {
			t.Fatalf("返回了多余样本 %d: %+v", *offset, got)
		}
		if want := expected[*offset]; got != want {
			t.Fatalf("样本 %d 丢失、重复或错串: got=%+v want=%+v", *offset, got, want)
		}
		*offset += 1
	}
}

func checkDownsampleIterationSearch(t *testing.T, ps *partSearch, p *part, tsids []TSID, q DownsampleQueryField, tr TimeRange) {
	t.Helper()
	expected := downsampleIterationExpected(tsids, q.ResolutionMs, q.Feature, tr)
	ps.initWithDownsampleField(p, tsids, tr, &q)
	count := 0
	for ps.NextBlock() {
		checkDownsampleIterationBlock(t, &ps.BlockRef, tr, expected, &count)
	}
	if err := ps.Error(); err != nil {
		t.Fatal(err)
	}
	if count != len(expected) {
		t.Fatalf("遗漏样本: got=%d want=%d", count, len(expected))
	}
	if ps.NextBlock() || ps.Error() != nil {
		t.Fatalf("迭代结束后再次产生结果或错误: %v", ps.Error())
	}
}

func TestDownsampleIterationMultiTSID(t *testing.T) {
	p := newDownsampleIterationPart(t)
	var all []TSID
	for series := 0; series < downsampleIterationSeries; series++ {
		all = append(all, downsampleIterationTSID(series))
	}
	for i := 1; i < len(all); i++ {
		if !all[i-1].Less(&all[i]) {
			t.Fatalf("fixture 未按完整 TSID 排序: %+v %+v", all[i-1], all[i])
		}
		if i%10 == 0 && all[i-1].MetricID <= all[i].MetricID {
			t.Fatal("fixture 未覆盖 MetricID 在高位字段切换后的逆序")
		}
	}
	var ps partSearch
	defer ps.reset()
	full := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
	for _, resolution := range []int64{300000, 3600000} {
		for feature := uint8(0); feature < 5; feature++ {
			q := DownsampleQueryField{ResolutionMs: resolution, Feature: feature}
			t.Run(fmt.Sprintf("全部序列/%d/%d", resolution, feature), func(t *testing.T) {
				checkDownsampleIterationSearch(t, &ps, p, all, q, full)
			})
			t.Run(fmt.Sprintf("非连续及缺失序列/%d/%d", resolution, feature), func(t *testing.T) {
				selected := []TSID{all[0], all[5], all[9], all[10], all[19], all[20], all[59], all[60], all[130], all[131], all[132], all[179]}
				beforeFirst := all[0]
				beforeFirst.MetricID -= 5
				selected = append(selected, beforeFirst)
				for _, series := range []int{5, 9, 19, 59, 130, 131, 179} {
					missing := all[series]
					missing.MetricID += 5
					selected = append(selected, missing)
				}
				sort.Slice(selected, func(i, j int) bool { return selected[i].Less(&selected[j]) })
				checkDownsampleIterationSearch(t, &ps, p, selected, q, full)
			})
			before := downsampleIterationTimestamp(downsampleIterationWideSeries, 8191, resolution)
			after := downsampleIterationTimestamp(downsampleIterationWideSeries, 8192, resolution)
			for _, tc := range []struct {
				name string
				tr   TimeRange
			}{
				{"前一 index 末点", TimeRange{MinTimestamp: before, MaxTimestamp: before}},
				{"后一 index 首点", TimeRange{MinTimestamp: after, MaxTimestamp: after}},
				{"跨 index 闭区间", TimeRange{MinTimestamp: before, MaxTimestamp: after}},
				{"区间相交但未覆盖点", TimeRange{MinTimestamp: before + 1, MaxTimestamp: after - 1}},
				{"序列末点", TimeRange{MinTimestamp: downsampleIterationTimestamp(downsampleIterationWideSeries, downsampleIterationWideRows-1, resolution), MaxTimestamp: downsampleIterationTimestamp(downsampleIterationWideSeries, downsampleIterationWideRows-1, resolution)}},
			} {
				t.Run(fmt.Sprintf("%s/%d/%d", tc.name, resolution, feature), func(t *testing.T) {
					checkDownsampleIterationSearch(t, &ps, p, []TSID{all[130], all[131], all[132]}, q, tc.tr)
				})
			}
		}
	}
}

func checkDownsampleIterationReader(t *testing.T, r *downsampleReader, p *part, resolution int64, feature uint8, filter *TSID, tr TimeRange) {
	t.Helper()
	var tsids []TSID
	if filter != nil {
		tsids = []TSID{*filter}
	}
	expected := downsampleIterationExpected(tsids, resolution, feature, tr)
	r.SetFilter(filter, tr.MinTimestamp, tr.MaxTimestamp)
	count := 0
	for r.NextHeader() {
		h, err := r.FieldHeader(feature)
		if err != nil {
			t.Fatal(err)
		}
		if r.resolution != resolution {
			t.Fatalf("读取了其他分辨率: %d", r.resolution)
		}
		if h.TimestampsBlockOffset != r.Header().TimestampsBlockOffset || h.TimestampsBlockSize != r.Header().TimestampsBlockSize || h.PrecisionBits != r.Header().PrecisionBits {
			t.Fatal("跨 feature 读取未共享时间戳定位或精度")
		}
		var br BlockRef
		br.init(p, &h)
		checkDownsampleIterationBlock(t, &br, tr, expected, &count)
	}
	if err := r.Error(); err != nil {
		t.Fatal(err)
	}
	if count != len(expected) {
		t.Fatalf("reader 遗漏样本: got=%d want=%d", count, len(expected))
	}
	if r.NextHeader() || r.Error() != nil {
		t.Fatalf("reader 结束后再次产生结果或错误: %v", r.Error())
	}
}

func TestDownsampleIterationReaderReuse(t *testing.T) {
	p := newDownsampleIterationPart(t)
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	full := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
	// 先后选择 index 边界、缺失序列、末尾和开头，防止游标沿用前一次的位置。
	for pass, resolution := range []int64{300000, 3600000, 300000} {
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		for feature := uint8(0); feature < 5; feature++ {
			wide := downsampleIterationTSID(131)
			missingAfterWide := wide
			missingAfterWide.MetricID += 5
			last := downsampleIterationTSID(179)
			missingAfterLast := last
			missingAfterLast.MetricID += 5
			first := downsampleIterationTSID(0)
			missingBeforeFirst := first
			missingBeforeFirst.MetricID -= 5
			filters := []TSID{wide, missingAfterWide, last, missingAfterLast, missingBeforeFirst, first, downsampleIterationTSID(132), downsampleIterationTSID(9), downsampleIterationTSID(10), downsampleIterationTSID(19), downsampleIterationTSID(20), downsampleIterationTSID(59), downsampleIterationTSID(60)}
			for _, filter := range filters {
				t.Run(fmt.Sprintf("筛选复用/%d/%d/%d", pass, feature, filter.MetricID), func(t *testing.T) {
					checkDownsampleIterationReader(t, r, p, resolution, feature, &filter, full)
				})
			}
			t.Run(fmt.Sprintf("移除筛选/%d/%d", pass, feature), func(t *testing.T) {
				checkDownsampleIterationReader(t, r, p, resolution, feature, nil, full)
			})
		}
		// 错误状态必须能由后续 SetFilter 和同一 part 的 Init 清除。
		r.SetFilter(nil, 2, 1)
		if r.Error() == nil || r.NextHeader() {
			t.Fatal("无效时间范围未进入错误状态")
		}
		first := downsampleIterationTSID(0)
		checkDownsampleIterationReader(t, r, p, resolution, 2, &first, full)
		r.reset()
		if r.p != nil || r.NextHeader() {
			t.Fatal("reset 后仍保留 part 或可读取结果")
		}
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		last := downsampleIterationTSID(downsampleIterationSeries - 1)
		checkDownsampleIterationReader(t, r, p, resolution, 4, &last, full)
	}
}
