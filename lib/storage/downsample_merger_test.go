package storage

import (
	"errors"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestDownsampleMergerMoreThan1024Sources(t *testing.T) {
	const (
		base        int64 = 1704067200000
		sourceCount       = 1025 // 超过已移除的单次源数量限制。
	)
	tsid := TSID{AccountID: 11, ProjectID: 17, MetricID: 42}
	sources := make([]*partWrapper, sourceCount)
	for i := range sources {
		// 每个源都拥有独立的真实内存 part，无须为大量磁盘源打开文件。
		sources[i] = newDownsampleTestRawPart(t, []rawRow{{
			TSID: tsid, Timestamp: base + int64(i+1), Value: float64(i + 1), PrecisionBits: 64,
		}})
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	output, stats := runDownsampleTestMerge(t, m, sources, nil, 0)
	if stats.rowsMerged != sourceCount || stats.rowsDeleted != 0 {
		t.Fatalf("unexpected source statistics: %+v; want %d merged rows", stats, sourceCount)
	}
	// 所有源落在同一个 bucket，两个分辨率都必须包含全部贡献。
	want := make(map[downsampleTestKey]downsampleSample)
	for _, resolution := range downsampleResolutions {
		want[downsampleTestKey{tsid: tsid, resolution: resolution, bucket: base / resolution}] = downsampleSample{
			timestamp:     base + sourceCount,
			precisionBits: 64,
			values: [countOfDownsampleFeatures]float64{
				sourceCount, sourceCount * (sourceCount + 1) / 2, sourceCount, 1, sourceCount,
			},
		}
	}
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), want)
}

func TestDownsampleMergerRepeatedMerge(t *testing.T) {
	const base int64 = 1704067200000
	ts := TSID{MetricID: 42}
	rows := []rawRow{
		{TSID: ts, Timestamp: base + 60000, Value: 2, PrecisionBits: 64},
		{TSID: ts, Timestamp: base + 240000, Value: 8, PrecisionBits: 64},
		{TSID: ts, Timestamp: base + 240000, Value: 3, PrecisionBits: 64},
	}
	late := []rawRow{
		{TSID: ts, Timestamp: base + 60000, Value: 5, PrecisionBits: 64},
		{TSID: ts, Timestamp: base + 180000, Value: 4, PrecisionBits: 64},
		{TSID: ts, Timestamp: base + 60000, Value: 5, PrecisionBits: 64},
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	raw := newDownsampleTestRawPart(t, rows)
	first, stats := runDownsampleTestMerge(t, m, []*partWrapper{raw}, nil, 0)
	if stats.rowsMerged != uint64(len(rows)) {
		t.Fatalf("raw rows counted by resolution; got %d; want %d", stats.rowsMerged, len(rows))
	}
	assertDownsampleTestRows(t, readDownsampleTestPart(t, first.p), referenceDownsampleTestRows(rows, nil, 0))

	second, stats := runDownsampleTestMerge(t, m, []*partWrapper{first, newDownsampleTestRawPart(t, late)}, nil, 0)
	if stats.rowsMerged != first.p.ph.RowsCount+uint64(len(late)) || stats.rowsDeleted != 0 {
		t.Fatalf("mixed source physical row statistics changed: %+v", stats)
	}
	all := append(append([]rawRow(nil), rows...), late...)
	want := referenceDownsampleTestRows(all, nil, 0)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, second.p), want)
	// 每轮只使用每个源的目标分辨率，不能把同源的两份表示重复统计。
	third, stats := runDownsampleTestMerge(t, m, []*partWrapper{second}, nil, 0)
	if stats.rowsMerged != second.p.ph.RowsCount || stats.rowsDeleted != 0 {
		t.Fatalf("summary rewrite must count each feature Block row: %+v", stats)
	}
	assertDownsampleTestRows(t, readDownsampleTestPart(t, third.p), want)
	// 重用同一个 merger 处理不同序列，检查窗口、索引游标和输出状态已清空。
	other := []rawRow{{TSID: TSID{MetricID: 100}, Timestamp: base + 1, Value: -7, PrecisionBits: 64}}
	fourth, _ := runDownsampleTestMerge(t, m, []*partWrapper{newDownsampleTestRawPart(t, other)}, nil, 0)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, fourth.p), referenceDownsampleTestRows(other, nil, 0))
	// raw 提升、混合归并、纯摘要重写及 merger 复用均须保留默认共享精度64。
	for _, stage := range []struct {
		name string
		p    *part
	}{
		{name: "raw", p: raw.p},
		{name: "raw_to_summary", p: first.p},
		{name: "mixed_to_summary", p: second.p},
		{name: "summary_to_summary", p: third.p},
		{name: "reused_merger", p: fourth.p},
	} {
		t.Run(stage.name, func(t *testing.T) {
			assertDownsampleTestPrecision64(t, stage.p)
		})
	}
}

func TestDownsampleMergerSourcePrecision(t *testing.T) {
	const base int64 = 1704067200000
	for _, tc := range []struct {
		name       string
		precisions []uint8
		offset     int64
		wantError  bool
	}{
		{name: "shared_low_precision", precisions: []uint8{8, 8}, offset: 1},
		{name: "split_precision_blocks", precisions: []uint8{8, 64, 8}, offset: downsampleResolution1h},
		{name: "reject_mixed_bucket", precisions: []uint8{8, 64}, offset: 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sources []*partWrapper
			var rows []rawRow
			for i, precision := range tc.precisions {
				row := rawRow{
					TSID: TSID{MetricID: 42}, Timestamp: base + int64(i)*tc.offset,
					Value: float64(i + 1), PrecisionBits: precision,
				}
				rows = append(rows, row)
				sources = append(sources, newDownsampleTestRawPart(t, []rawRow{row}))
			}
			m := getDownsampleMerger()
			defer putDownsampleMerger(m)
			if tc.wantError {
				w := getDownsampleWriter()
				defer putDownsampleWriter(w)
				if err := w.Init(filepath.Join(t.TempDir(), "mixed"), -5); err != nil {
					t.Fatal(err)
				}
				defer w.Abort()
				if _, err := m.Merge(sources, w, nil, nil, 0); err == nil {
					t.Fatal("accepted different source precisions in the same bucket")
				}
				return
			}
			wantRows := referenceDownsampleTestRows(rows, nil, 0)
			wantMerged := uint64(len(rows))
			for round := 0; round < 2; round++ {
				output, stats := runDownsampleTestMerge(t, m, sources, nil, 0)
				if stats.rowsMerged != wantMerged || stats.rowsDeleted != 0 {
					t.Fatalf("round %d changed source statistics: %+v; want %d merged rows", round, stats, wantMerged)
				}
				assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), wantRows)
				r := getDownsampleReader()
				defer putDownsampleReader(r)
				for _, resolution := range downsampleResolutions {
					if err := r.Init(output.p, resolution); err != nil {
						t.Fatal(err)
					}
					blocks := 0
					for r.NextHeader() {
						var b downsampleDecodedResolutionFeaturesBlock
						if err := r.ReadBlock(&b); err != nil {
							t.Fatal(err)
						}
						want := tc.precisions[0]
						if tc.offset == downsampleResolution1h {
							want = tc.precisions[(b.timestamps[0]-base)/tc.offset]
						}
						if b.precisionBits != want {
							t.Fatalf("round %d lost source precision %d: %+v", round, want, b)
						}
						assertDownsampleTestHeaderPrecision(t, r, want)
						blocks++
					}
					if err := r.Error(); err != nil {
						t.Fatal(err)
					}
					wantBlocks := 1
					if tc.offset == downsampleResolution1h {
						wantBlocks = len(tc.precisions)
					}
					if blocks != wantBlocks {
						t.Fatalf("got %d blocks; want %d", blocks, wantBlocks)
					}
				}
				sources = []*partWrapper{output}
				wantMerged = output.p.ph.RowsCount
			}
		})
	}
}

func TestDownsampleMergerDynamicBucketRange(t *testing.T) {
	const (
		base int64 = 1704067200000
		day  int64 = 24 * 60 * 60 * 1000
	)
	shortBefore, long, shortAfter := TSID{MetricID: 1}, TSID{MetricID: 2}, TSID{MetricID: 3}
	rows := []rawRow{
		// 相邻点恰好跨越 5m 边界，但仍属于同一小时。
		{TSID: shortBefore, Timestamp: base + 299999, Value: 2, PrecisionBits: 64},
		{TSID: shortBefore, Timestamp: base + 300000, Value: 3, PrecisionBits: 64},
		{TSID: long, Timestamp: base + 31*day + 3000, Value: 8, PrecisionBits: 64},
		// 相邻点同时跨越 5m 和 1h 边界。
		{TSID: shortAfter, Timestamp: base + 3599999, Value: -2, PrecisionBits: 64},
		{TSID: shortAfter, Timestamp: base + 3600000, Value: 7, PrecisionBits: 64},
	}
	// 直接调用 merger 的合成跨月测试；生产源来自同一个月分区。
	// 两个 block 的时间范围重叠，最大时间在第二个 block，最小时间在另一个源。
	blocks := [][]rawRow{
		{
			{TSID: long, Timestamp: base + 1000, Value: 2, PrecisionBits: 64},
			{TSID: long, Timestamp: base + 31*day + 1000, Value: 3, PrecisionBits: 64},
			{TSID: long, Timestamp: base + 62*day + 299999, Value: 4, PrecisionBits: 64},
		},
		{
			{TSID: long, Timestamp: base + 2*day + 1000, Value: 5, PrecisionBits: 64},
			{TSID: long, Timestamp: base + 31*day + 2000, Value: 6, PrecisionBits: 64},
			{TSID: long, Timestamp: base + 63*day + 1000, Value: 7, PrecisionBits: 64},
		},
	}
	first := []rawRow{{TSID: long, Timestamp: base + 500, Value: 11, PrecisionBits: 64}}
	sources := []*partWrapper{
		newDownsampleTestRawPart(t, rows),
		newDownsampleTestOverlappingPart(t, blocks),
		newDownsampleTestRawPart(t, first),
	}
	all := append(append([]rawRow(nil), rows...), first...)
	for _, block := range blocks {
		all = append(all, block...)
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	type observedRange struct {
		metricID   uint64
		resolution int64
		buckets    int
	}
	// 63 天含首尾 bucket：5m 为 18145，1h 为 1513；短序列不受 part 范围影响。
	wantRanges := []observedRange{
		{1, downsampleResolution5m, 2}, {2, downsampleResolution5m, 18145}, {3, downsampleResolution5m, 2},
		{1, downsampleResolution1h, 1}, {2, downsampleResolution1h, 1513}, {3, downsampleResolution1h, 2},
	}
	wantMerged := uint64(len(all))
	// raw 首次聚合和 summary 重写都按当前分辨率的 header 范围开槽。
	for round := 0; round < 2; round++ {
		w := getDownsampleWriter()
		defer putDownsampleWriter(w)
		path := filepath.Join(t.TempDir(), "part")
		if err := w.Init(path, -5); err != nil {
			t.Fatal(err)
		}
		defer w.Abort()
		var observed []observedRange
		w.timestampsWriter = &downsampleStateObserverWriter{WriteCloser: w.timestampsWriter, observe: func() {
			observed = append(observed, observedRange{w.blocks[0].bh.TSID.MetricID, w.resolution, len(m.currentTSIDBucketSamples)})
		}}
		stats, err := m.Merge(sources, w, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if stats.rowsMerged != wantMerged || stats.rowsDeleted != 0 {
			t.Fatalf("round %d: unexpected source statistics: %+v; want %d merged rows", round, stats, wantMerged)
		}
		if len(observed) != len(wantRanges) {
			t.Fatalf("round %d: unexpected output ranges: got %+v; want %+v", round, observed, wantRanges)
		}
		for i, want := range wantRanges {
			if observed[i] != want {
				t.Fatalf("round %d output %d used the wrong TSID bucket range: got %+v; want %+v", round, i, observed[i], want)
			}
		}
		if _, err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		p, err := openDownsamplePart(path)
		if err != nil {
			t.Fatal(err)
		}
		defer p.MustClose()
		assertDownsampleTestRows(t, readDownsampleTestPart(t, p), referenceDownsampleTestRows(all, nil, 0))
		sources = []*partWrapper{{p: p}}
		wantMerged = p.ph.RowsCount
	}

	// 同一 merger 再处理长序列的一个点，前一作业的桶和统计不能带入新结果。
	reusedRows := []rawRow{{TSID: long, Timestamp: base + 4000, Value: -17, PrecisionBits: 64}}
	reused, stats := runDownsampleTestMerge(t, m, []*partWrapper{newDownsampleTestRawPart(t, reusedRows)}, nil, 0)
	if stats.rowsMerged != 1 || stats.rowsDeleted != 0 {
		t.Fatalf("merger reuse retained old statistics: %+v", stats)
	}
	assertDownsampleTestRows(t, readDownsampleTestPart(t, reused.p), referenceDownsampleTestRows(reusedRows, nil, 0))
}

func TestDownsampleMergerRetentionAndDeletedMetricID(t *testing.T) {
	const base int64 = 1704067200000
	rows := []rawRow{
		{TSID: TSID{MetricID: 7}, Timestamp: base + 60000, Value: 2, PrecisionBits: 64},
		{TSID: TSID{MetricID: 7}, Timestamp: base + 360000, Value: 8, PrecisionBits: 64},
		{TSID: TSID{MetricID: 9}, Timestamp: base + 240000, Value: 100, PrecisionBits: 64},
	}
	var deleted uint64set.Set
	deleted.Add(9)
	deadline := base + downsampleResolution5m
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	raw := newDownsampleTestRawPart(t, rows)
	output, stats := runDownsampleTestMerge(t, m, []*partWrapper{raw}, &deleted, deadline)
	want := referenceDownsampleTestRows(rows, &deleted, deadline)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), want)
	if stats.rowsDeleted != 1 || stats.rowsMerged != 2 {
		t.Fatalf("unexpected physical source statistics: merged=%d deleted=%d", stats.rowsMerged, stats.rowsDeleted)
	}
	// 5m 的早期贡献已经移除，后续 merge 仍须从完整 1h 表示保留该贡献。
	merged, _ := runDownsampleTestMerge(t, m, []*partWrapper{output}, &deleted, deadline)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, merged.p), want)
}

func TestDownsampleMergerEmptyTSIDBeforeRetainedTSID(t *testing.T) {
	const base int64 = 1704067200000
	rows := []rawRow{
		{TSID: TSID{MetricID: 1}, Timestamp: base + 1, Value: 100, PrecisionBits: 8},
		{TSID: TSID{MetricID: 1}, Timestamp: base + downsampleResolution5m + 1, Value: 200, PrecisionBits: 8},
		{TSID: TSID{MetricID: 2}, Timestamp: base + downsampleResolution1h + 1, Value: 3, PrecisionBits: 64},
	}
	deadline := base + downsampleResolution1h
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	output, stats := runDownsampleTestMerge(t, m, []*partWrapper{newDownsampleTestRawPart(t, rows)}, nil, deadline)
	if stats.rowsDeleted != 2 || stats.rowsMerged != 1 {
		t.Fatalf("unexpected empty TSID statistics: %+v", stats)
	}
	if output.p.ph.BlocksCount != 2*countOfDownsampleFeatures || output.p.ph.RowsCount != 2*countOfDownsampleFeatures {
		t.Fatalf("empty TSID produced output blocks or rows: %+v", output.p.ph)
	}
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), referenceDownsampleTestRows(rows, nil, deadline))
}

func TestDownsampleMergerSpecialValues(t *testing.T) {
	const base int64 = 1704067200000
	rows := []rawRow{
		{TSID: TSID{MetricID: 7}, Timestamp: base + 60000, Value: math.Inf(1), PrecisionBits: 64},
		{TSID: TSID{MetricID: 7}, Timestamp: base + 120000, Value: math.Inf(-1), PrecisionBits: 64},
		{TSID: TSID{MetricID: 8}, Timestamp: base + 60000, Value: decimal.StaleNaN, PrecisionBits: 64},
		{TSID: TSID{MetricID: 8}, Timestamp: base + 120000, Value: 5, PrecisionBits: 64},
	}
	want := make(map[downsampleTestKey]downsampleSample)
	for _, resolution := range downsampleResolutions {
		want[downsampleTestKey{TSID{MetricID: 7}, resolution, base / resolution}] = downsampleSample{
			timestamp: base + 120000, values: [5]float64{math.Inf(-1), decimal.StaleNaN, 2, math.Inf(-1), math.Inf(1)}, precisionBits: 64,
		}
		want[downsampleTestKey{TSID{MetricID: 8}, resolution, base / resolution}] = downsampleSample{
			timestamp: base + 120000, values: [5]float64{5, decimal.StaleNaN, 2, decimal.StaleNaN, decimal.StaleNaN}, precisionBits: 64,
		}
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	output, _ := runDownsampleTestMerge(t, m, []*partWrapper{
		newDownsampleTestRawPart(t, []rawRow{rows[0], rows[2]}),
		newDownsampleTestRawPart(t, []rawRow{rows[1], rows[3]}),
	}, nil, 0)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), want)
	repeated, _ := runDownsampleTestMerge(t, m, []*partWrapper{output}, nil, 0)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, repeated.p), want)
}

func TestDownsampleMergerSummaryPhysicalStatistics(t *testing.T) {
	const base int64 = 1704067200000
	rows := []rawRow{
		{TSID: TSID{MetricID: 7}, Timestamp: base + 60000, Value: 2, PrecisionBits: 64},
		{TSID: TSID{MetricID: 9}, Timestamp: base + 120000, Value: 8, PrecisionBits: 64},
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	summary, _ := runDownsampleTestMerge(t, m, []*partWrapper{newDownsampleTestRawPart(t, rows)}, nil, 0)
	// 两个 TSID、两个分辨率、五个特征；每个真实 Block 各包含一行。
	if summary.p.ph.RowsCount != 20 || summary.p.ph.BlocksCount != 20 {
		t.Fatalf("unexpected single-feature Block statistics: %+v", summary.p.ph)
	}
	var deleted uint64set.Set
	deleted.Add(9)
	output, stats := runDownsampleTestMerge(t, m, []*partWrapper{summary}, &deleted, 0)
	if stats.rowsMerged != 10 || stats.rowsDeleted != 10 || output.p.ph.RowsCount != 10 || output.p.ph.BlocksCount != 10 {
		t.Fatalf("unexpected retained/deleted physical rows: stats=%+v, part=%+v", stats, output.p.ph)
	}
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), referenceDownsampleTestRows(rows, &deleted, 0))
}

func TestDownsampleMergerOutputBlockBoundary(t *testing.T) {
	const base int64 = 1704067200000
	for _, tc := range []struct {
		name       string
		bucketStep int64
	}{
		{name: "dense", bucketStep: 1},
		{name: "sparse", bucketStep: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]rawRow, maxRowsPerBlock+1)
			for i := range rows {
				rows[i] = rawRow{
					TSID: TSID{MetricID: 7}, Timestamp: base + int64(i)*tc.bucketStep*downsampleResolution5m,
					Value: float64(i % 5), PrecisionBits: 64,
				}
			}
			m := getDownsampleMerger()
			defer putDownsampleMerger(m)
			output, _ := runDownsampleTestMerge(t, m, []*partWrapper{newDownsampleTestRawPart(t, rows)}, nil, 0)
			if output.p.ph.BlocksCount != 3*countOfDownsampleFeatures {
				t.Fatalf("expected split 5m blocks and a 1h block; got %d", output.p.ph.BlocksCount)
			}
			assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), referenceDownsampleTestRows(rows, nil, 0))
		})
	}
}

func TestDownsampleMergerCancellation(t *testing.T) {
	rows := []rawRow{{TSID: TSID{MetricID: 1}, Timestamp: 1704067200000, Value: 7, PrecisionBits: 64}}
	raw := newDownsampleTestRawPart(t, rows)
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	path := filepath.Join(t.TempDir(), "cancelled")
	if err := w.Init(path, -5); err != nil {
		t.Fatal(err)
	}
	stopCh := make(chan struct{})
	close(stopCh)
	stats, err := m.Merge([]*partWrapper{raw}, w, stopCh, nil, 0)
	if !errors.Is(err, errForciblyStopped) {
		t.Fatalf("unexpected cancellation result: %v", err)
	}
	if stats.rowsMerged != 0 || stats.rowsDeleted != 0 {
		t.Fatalf("cancelled merge counted unread source rows: %+v", stats)
	}
	// 取消后的未发布文件由调用者负责清理，不影响仍有效的源。
	w.Abort()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled output wasn't removed: %v", err)
	}
	output, _ := runDownsampleTestMerge(t, m, []*partWrapper{raw}, nil, 0)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), referenceDownsampleTestRows(rows, nil, 0))
}

func TestDownsampleMergerSharedPrecisionAndColumnScales(t *testing.T) {
	const base int64 = 1704067200000
	var sources []*partWrapper
	var decoded []*downsampleDecodedResolutionFeaturesBlock
	for source := 0; source < 2; source++ {
		block := &downsampleDecodedResolutionFeaturesBlock{
			tsid: TSID{MetricID: 42}, resolution: downsampleResolution5m,
			precisionBits: 64,
		}
		for i := 0; i < 12; i++ {
			block.timestamps = append(block.timestamps, base+int64(i)*downsampleResolution5m+10000+int64(i*i*137+source*10000))
			row := [5]float64{
				float64(i)/8 - 0.5, float64(i*i+3) * 12345.6789,
				0.375 + float64(i)/4 + float64(source)/8, -float64(i+1) * 1e-8, float64(i+1)*2e6 + 0.5,
			}
			for feature, v := range row {
				block.values[feature] = append(block.values[feature], v)
			}
		}
		path := filepath.Join(t.TempDir(), "source")
		w := getDownsampleWriter()
		if err := w.Init(path, -5); err != nil {
			putDownsampleWriter(w)
			t.Fatal(err)
		}
		if err := writeDownsampleTestBlock(w, block); err != nil {
			putDownsampleWriter(w)
			t.Fatal(err)
		}
		if _, err := w.Finish(); err != nil {
			putDownsampleWriter(w)
			t.Fatal(err)
		}
		putDownsampleWriter(w)
		p, err := openDownsamplePart(path)
		if err != nil {
			t.Fatal(err)
		}
		pw := &partWrapper{p: p}
		pw.incRef()
		t.Cleanup(pw.decRef)
		sources = append(sources, pw)
		reference := roundTripDownsampleTestReferenceBlock(t, block)
		decoded = append(decoded, reference)
		assertDownsampleTestRows(t, readDownsampleTestPart(t, p), downsampleTestBlockRows(reference))
		func() {
			r := getDownsampleReader()
			defer putDownsampleReader(r)
			if err := r.Init(p, downsampleResolution5m); err != nil || !r.NextHeader() {
				t.Fatalf("cannot read source header: %v", err)
			}
			assertDownsampleTestHeaderPrecision(t, r, 64)
			scales := make(map[int16]bool)
			for feature := range block.values {
				_, scale := decimal.AppendFloatToDecimal(nil, block.values[feature])
				column, err := r.FieldHeader(uint8(feature))
				if err != nil {
					t.Fatal(err)
				}
				if column.Scale != scale || column.PrecisionBits != block.precisionBits {
					t.Fatalf("source column %d lost scale or shared precision64", feature)
				}
				scales[scale] = true
			}
			if len(scales) < 3 {
				t.Fatal("test must cover distinct column scales")
			}
		}()
	}
	// 参考结果以原 codec 解码后的输入为基准，再独立执行摘要运算和输出编码。
	points := make(map[int64]downsampleSample)
	for _, block := range decoded {
		for row, timestamp := range block.timestamps {
			bucket := timestamp / block.resolution
			point, ok := points[bucket]
			if !ok {
				point.timestamp = timestamp
				point.precisionBits = block.precisionBits
				for feature := range point.values {
					point.values[feature] = block.values[feature][row]
				}
			} else {
				if point.precisionBits != block.precisionBits {
					t.Fatal("reference summary inputs have different precisions in one bucket")
				}
				if timestamp > point.timestamp || timestamp == point.timestamp && block.values[0][row] > point.values[0] {
					point.timestamp, point.values[0] = timestamp, block.values[0][row]
				}
				point.values[1] += block.values[1][row]
				point.values[2] += block.values[2][row]
				point.values[3] = math.Min(point.values[3], block.values[3][row])
				point.values[4] = math.Max(point.values[4], block.values[4][row])
			}
			points[bucket] = point
		}
	}
	buckets := make([]int64, 0, len(points))
	for bucket := range points {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	expected := &downsampleDecodedResolutionFeaturesBlock{
		tsid: TSID{MetricID: 42}, resolution: downsampleResolution5m,
		precisionBits: 64,
	}
	for _, bucket := range buckets {
		point := points[bucket]
		expected.timestamps = append(expected.timestamps, point.timestamp)
		for feature, v := range point.values {
			expected.values[feature] = append(expected.values[feature], v)
		}
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	for round := 0; round < 2; round++ {
		output, _ := runDownsampleTestMerge(t, m, sources, nil, 0)
		func() {
			r := getDownsampleReader()
			defer putDownsampleReader(r)
			if err := r.Init(output.p, downsampleResolution5m); err != nil || !r.NextHeader() {
				t.Fatalf("cannot read output header: %v", err)
			}
			blocks := 0
			for {
				assertDownsampleTestHeaderPrecision(t, r, 64)
				var batch downsampleDecodedResolutionFeaturesBlock
				if err := r.ReadBlock(&batch); err != nil {
					t.Fatal(err)
				}
				if batch.precisionBits != 64 {
					t.Fatalf("merge round %d decoded batch lost precision64", round)
				}
				blocks++
				if !r.NextHeader() {
					break
				}
			}
			if err := r.Error(); err != nil {
				t.Fatal(err)
			}
			if blocks == 0 {
				t.Fatal("no merged blocks checked")
			}
		}()
		expected = roundTripDownsampleTestReferenceBlock(t, expected)
		assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), downsampleTestBlockRows(expected))
		sources = []*partWrapper{output}
	}
}

func TestDownsampleMergerResetClosesAllReaders(t *testing.T) {
	for _, operation := range []string{"reset", "init_sources", "merge"} {
		t.Run(operation, func(t *testing.T) {
			first := newDownsampleCloseTestReader(t)
			second := newDownsampleCloseTestReader(t)
			window := newDownsampleCloseTestReader(t)
			files := []filestream.ReadAtCloser{
				first.timestampsReader, first.valuesReader, first.indexReader,
				second.timestampsReader, second.valuesReader, second.indexReader,
				window.timestampsReader, window.valuesReader, window.indexReader,
			}
			first.timestampsReader.(*downsampleCloseTestFile).closeErr = os.ErrClosed
			// first 已出堆，但仍由 readers 持有；清理不能只遍历 heap。
			activeReaders := []*downsampleReader{first, second}
			m := downsampleMerger{
				currentResolutionReaders:    []*downsampleReader{first, second},
				currentTSIDReaders:          activeReaders,
				currentResolutionReaderHeap: downsampleReaderHeap{second},
				currentSourceReader:         window,
			}
			var err error
			switch operation {
			case "reset":
				err = m.reset()
			case "init_sources":
				err = m.initSources(nil, downsampleResolution1h)
				// Switching columns closes source readers; the independent data reader remains usable.
				if n, readErr := window.timestampsReader.ReadAt(make([]byte, 1), 0); readErr != nil || n != 1 {
					t.Fatalf("initSources closed the independent window currentSourceReader: n=%d, err=%v", n, readErr)
				}
				if closeErr := m.reset(); closeErr != nil {
					t.Fatal(closeErr)
				}
			case "merge":
				_, err = m.Merge(nil, nil, nil, nil, 0)
			}
			if !errors.Is(err, os.ErrClosed) {
				t.Fatalf("%s lost reader close error: %v", operation, err)
			}
			assertDownsampleFilesClosed(t, files)
			if len(m.currentResolutionReaders) != 0 || len(m.currentTSIDReaders) != 0 || len(m.currentResolutionReaderHeap) != 0 || m.currentSourceReader != nil {
				t.Fatal("merger retained closed readers")
			}
			for _, r := range activeReaders {
				if r != nil {
					t.Fatal("active readers backing array retained a returned reader")
				}
			}
			if err := m.reset(); err != nil {
				t.Fatalf("reset retried closed readers: %v", err)
			}
			assertDownsampleFilesClosed(t, files)
		})
	}
}

func TestDownsampleMergerClosesBeforeReturning(t *testing.T) {
	for _, scenario := range []string{"success", "close_error", "write_and_close_error", "cancel_and_close_error"} {
		t.Run(scenario, func(t *testing.T) {
			const base int64 = 1704067200000
			mp := getInmemoryPart()
			mp.InitFromRows([]rawRow{{TSID: TSID{MetricID: 1}, Timestamp: base + 1, Value: 2, PrecisionBits: 64}})
			sourcePath := filepath.Join(t.TempDir(), "raw")
			mp.MustStoreToDisk(sourcePath)
			putInmemoryPart(mp)
			p := mustOpenFilePart(sourcePath)
			defer p.MustClose()
			var m downsampleMerger
			defer m.reset()
			var w downsampleWriter
			targetPath := filepath.Join(t.TempDir(), "target")
			if err := w.Init(targetPath, 1); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			stopCh := make(chan struct{})
			writeErr := errors.New("injected output write failure")
			var files []filestream.ReadAtCloser
			var closeNames []string
			var activeReaders []*downsampleReader
			w.timestampsWriter = &downsampleCloseTestWriter{WriteCloser: w.timestampsWriter, beforeWrite: func() error {
				if w.resolution != downsampleResolution1h || files != nil {
					return nil
				}
				// The last output batch is already read. Wrap the actual source files
				// to inject cleanup errors and count every Close without changing reads.
				if len(m.currentResolutionReaderHeap) != 0 || len(m.currentResolutionReaders) != 1 || m.currentResolutionReaders[0].p != p {
					t.Fatal("exhausted source reader must leave the heap but remain owned until cleanup")
				}
				if len(m.currentTSIDReaders) != 1 || m.currentTSIDReaders[0] != m.currentResolutionReaders[0] {
					t.Fatal("active reader must borrow the same instance held by the complete readers list")
				}
				activeReaders = m.currentTSIDReaders
				readers := []*downsampleReader{m.currentResolutionReaders[0], m.currentSourceReader}
				for _, r := range readers {
					timestamps := &downsampleCloseTestFile{ReadAtCloser: r.timestampsReader}
					r.timestampsReader = timestamps
					r.valuesReader = &downsampleCloseTestFile{ReadAtCloser: r.valuesReader}
					r.indexReader = &downsampleCloseTestFile{ReadAtCloser: r.indexReader}
					files = append(files, r.timestampsReader, r.valuesReader, r.indexReader)
					if scenario != "success" {
						closeNames = append(closeNames, timestamps.Path())
						timestamps.closeErr = os.ErrClosed
					}
				}
				if scenario == "write_and_close_error" {
					return writeErr
				}
				if scenario == "cancel_and_close_error" {
					close(stopCh)
				}
				return nil
			}}
			stats, err := m.Merge([]*partWrapper{{p: p}}, &w, stopCh, nil, 0)
			if len(files) != 6 {
				t.Fatal("test did not reach the final batch with both source readers")
			}
			assertDownsampleFilesClosed(t, files)
			if len(m.currentResolutionReaders) != 0 || len(m.currentTSIDReaders) != 0 || len(m.currentResolutionReaderHeap) != 0 || m.currentSourceReader != nil {
				t.Fatal("Merge returned with live readers")
			}
			for _, r := range activeReaders {
				if r != nil {
					t.Fatal("Merge left a returned reader in the active readers backing array")
				}
			}
			if stats.rowsMerged != 1 || m.mergeStats != (downsampleMergeStats{}) || m.stopCh != nil || m.currentSourceBlock != nil || m.partWriter != nil {
				t.Fatalf("Merge lost returned statistics or retained task state: returned=%+v; retained=%+v", stats, m.mergeStats)
			}
			if scenario == "success" {
				if err != nil {
					t.Fatal(err)
				}
				// The caller still owns cancellation; Merge must not close its channel.
				close(stopCh)
			} else {
				if !errors.Is(err, os.ErrClosed) {
					t.Fatalf("Merge succeeded or lost cleanup error: %v", err)
				}
				for _, name := range closeNames {
					if !strings.Contains(err.Error(), name) {
						t.Fatalf("lost one reader's close failure: %v", err)
					}
				}
				if scenario == "write_and_close_error" && !errors.Is(err, writeErr) {
					t.Fatalf("cleanup hid output failure: %v", err)
				}
				if scenario == "cancel_and_close_error" && !errors.Is(err, errForciblyStopped) {
					t.Fatalf("cleanup hid cancellation: %v", err)
				}
			}
			// The owner aborts any unpublished output once Merge reports failure.
			if err := w.Abort(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Abort left the unpublished target: %v", err)
			}
		})
	}
}

// 文件基准包括 writer 的创建、编码、写入及同步关闭；目录统计和删除不计时。
// 每次生成独立目标并立即删除，源 part 和工作对象在循环间复用。
func BenchmarkDownsampleMerge(b *testing.B) {
	for _, workload := range []string{"Dense", "Sparse", "HighCardinality"} {
		for _, mode := range []string{"Raw", "Summary", "SummaryRewrite"} {
			b.Run(workload+"/"+mode, func(b *testing.B) {
				root := b.TempDir()
				sources := newDownsampleBenchmarkSources(b, workload)
				if mode != "Raw" {
					for i, source := range sources {
						sources[i] = newDownsampleBenchmarkFile(b, []*partWrapper{source}, filepath.Join(root, "source-"+strconv.Itoa(i)))
					}
				}
				if mode == "SummaryRewrite" {
					// 先合并成一个完整摘要 part，测量已经归并过的摘要再次落盘的成本。
					sources = []*partWrapper{newDownsampleBenchmarkFile(b, sources, filepath.Join(root, "merged-source"))}
				}
				var sourceRows uint64
				for _, source := range sources {
					sourceRows += source.p.ph.RowsCount
				}
				m := getDownsampleMerger()
				defer putDownsampleMerger(m)
				w := getDownsampleWriter()
				defer putDownsampleWriter(w)
				var outputBytes uint64
				b.ReportAllocs()
				b.ResetTimer()
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					path := filepath.Join(root, "output-"+strconv.Itoa(i))
					b.StartTimer()
					if err := w.Init(path, -5); err != nil {
						b.Fatal(err)
					}
					if _, err := m.Merge(sources, w, nil, nil, 0); err != nil {
						b.Fatal(err)
					}
					if _, err := w.Finish(); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					outputBytes += downsampleBenchmarkFileSize(b, path)
					if err := os.RemoveAll(path); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(outputBytes)/float64(b.N), "file-bytes/op")
				reportDownsampleBenchmarkRows(b, sourceRows)
			})
		}
	}
}
