package storage

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unsafe"
)

func TestDownsampleMergerTenantResolutions(t *testing.T) {
	config, err := ParseDownsamplingConfig([]byte(`{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":["1h"]},{"tenant":"2:0","resolutions":["30m","2h"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := ParseDownsamplingConfig([]byte(`{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":["30m","2h"]},{"tenant":"3:0","resolutions":["1h"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	const base int64 = 1704067200000
	var rows, late []rawRow
	for tenant := uint32(1); tenant <= 3; tenant++ {
		for metricID := uint64(1); metricID <= 2; metricID++ {
			tsid := TSID{AccountID: tenant, MetricID: uint64(tenant)*10 + metricID}
			for i, offset := range []int64{1, 299999, 300001, 1799000, 1800500, 3599999, 3600001, 7199999, 7200001} {
				rows = append(rows, rawRow{TSID: tsid, Timestamp: base + offset, Value: float64(i*3) - 7, PrecisionBits: 64})
			}
			late = append(late, rawRow{TSID: tsid, Timestamp: base + 310000, Value: 50, PrecisionBits: 64})
		}
	}
	// 同一轮同时覆盖原始内存源、原始磁盘源；所有源都只在 base 上读取一次。
	memorySource := newDownsampleTestRawPart(t, rows[:len(rows)/2])
	diskSource := newDownsampleTestRawPart(t, rows[len(rows)/2:])
	path := filepath.Join(t.TempDir(), "raw")
	diskSource.mp.MustStoreToDisk(path)
	diskPart := mustOpenFilePart(path)
	defer diskPart.MustClose()
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	first, stats := runDownsampleTestMerge(t, m, []*partWrapper{memorySource, {p: diskPart}}, nil, 0, config)
	if stats.rowsMerged != uint64(len(rows)) || stats.rowsDeleted != 0 {
		t.Fatalf("raw source contributions were counted more than once: %+v", stats)
	}
	assertDownsampleTestRows(t, readDownsampleTestPart(t, first.p), referenceDownsampleTestRows(rows, nil, 0, config))
	if got := fmt.Sprint(downsamplePartResolutionsForTest(first.p)); got != "[300000 1800000 3600000 7200000]" {
		t.Fatalf("unexpected resolution sections: %s", got)
	}
	// 更新配置后新增、删除和替换租户分辨率；旧 part 的粗列不得参与下一轮求和。
	latePart, _ := runDownsampleTestMerge(t, m, []*partWrapper{newDownsampleTestRawPart(t, late)}, nil, 0, updated)
	second, stats := runDownsampleTestMerge(t, m, []*partWrapper{first, latePart}, nil, 0, updated)
	wantInput := downsampleBasePhysicalRowsForTest(first.p) + downsampleBasePhysicalRowsForTest(latePart.p)
	if stats.rowsMerged != wantInput {
		t.Fatalf("read derived columns while merging old and new configurations: got %d; want %d", stats.rowsMerged, wantInput)
	}
	all := append(append([]rawRow(nil), rows...), late...)
	want := referenceDownsampleTestRows(all, nil, 0, updated)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, second.p), want)
	third, _ := runDownsampleTestMerge(t, m, []*partWrapper{second}, nil, 0, updated)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, third.p), want)
	// 更新和重写没有改变仍被持有的旧 part 的配置或数据。
	assertDownsampleTestRows(t, readDownsampleTestPart(t, first.p), referenceDownsampleTestRows(rows, nil, 0, config))
}

func TestDownsampleMergerArbitraryBaseAndSparseRange(t *testing.T) {
	for _, base := range []string{"1ms", "10m"} {
		t.Run(base, func(t *testing.T) {
			config, err := ParseDownsamplingConfig([]byte(fmt.Sprintf(`{"base_resolution":%q,"tenant_resolutions":[{"tenant":"1:0","resolutions":["30m","2h"]}]}`, base)))
			if err != nil {
				t.Fatal(err)
			}
			rows := []rawRow{
				{TSID: TSID{AccountID: 1, MetricID: 1}, Timestamp: minUnixMilli, Value: 2, PrecisionBits: 64},
				{TSID: TSID{AccountID: 1, MetricID: 1}, Timestamp: minUnixMilli + config.BaseResolutionMs(), Value: 3, PrecisionBits: 64},
				{TSID: TSID{AccountID: 1, MetricID: 1}, Timestamp: maxUnixMilli, Value: 7, PrecisionBits: 64},
			}
			m := &downsampleMerger{}
			defer m.reset()
			output, _ := runDownsampleTestMerge(t, m, []*partWrapper{newDownsampleTestRawPart(t, rows)}, nil, 0, config)
			assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), referenceDownsampleTestRows(rows, nil, 0, config))
			if cap(m.currentTSIDBaseBucketSamples) > 16 {
				t.Fatalf("allocated empty time range for three samples: %d", cap(m.currentTSIDBaseBucketSamples))
			}
		})
	}
}

func TestDownsampleMergerDerivedCancellationAndPrecision(t *testing.T) {
	var m downsampleMerger
	defer m.reset()
	samples := []downsampleSample{
		{timestamp: minUnixMilli + 1, precisionBits: 8, values: [countOfDownsampleFeatures]float64{2, 2, 1, 2, 2}},
		{timestamp: minUnixMilli + 300001, precisionBits: 64, values: [countOfDownsampleFeatures]float64{3, 3, 1, 3, 3}},
	}
	if _, err := m.aggregateSamples(nil, samples, downsampleResolution1h, nil); err == nil {
		t.Fatal("accepted different precisions in one derived bucket")
	}
	stopCh := make(chan struct{})
	close(stopCh)
	if _, err := m.aggregateSamples(nil, samples, downsampleResolution1h, stopCh); !errors.Is(err, errForciblyStopped) {
		t.Fatalf("derived aggregation ignored cancellation: %v", err)
	}
	if _, err := m.Merge(nil, nil, nil, nil, 0); err == nil {
		t.Fatal("accepted an uninitialized writer")
	}
}

func TestDownsampleMergerMemoryBudget(t *testing.T) {
	const base int64 = 1704067200000
	sampleBytes := uint64(unsafe.Sizeof(downsampleSample{}))
	headerBytes := uint64(unsafe.Sizeof(blockHeader{}))
	for _, tc := range []struct {
		name    string
		limit   uint64
		offsets []int64
		extra   bool
	}{
		{"headers", headerBytes - 1, []int64{0}, false},
		{"dense", headerBytes + 3*sampleBytes, []int64{0, 1, 2, 3}, false},
		{"sparse", headerBytes + downsampleSparseBucketIndexBaseBytes + downsampleSparseBucketIndexBytes + sampleBytes, []int64{0, 1000}, false},
		{"derived", headerBytes + 5*sampleBytes, []int64{0, 1, 2, 3}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configJSON := `{"base_resolution":"1ms"}`
			if tc.extra {
				configJSON = `{"base_resolution":"1ms","tenant_resolutions":[{"tenant":"1:0","resolutions":["2ms"]}]}`
			}
			config, err := ParseDownsamplingConfig([]byte(configJSON))
			if err != nil {
				t.Fatal(err)
			}
			var rows []rawRow
			for _, offset := range tc.offsets {
				rows = append(rows, rawRow{TSID: TSID{AccountID: 1, MetricID: 1}, Timestamp: base + offset, Value: 2, PrecisionBits: 64})
			}
			source := newDownsampleTestRawPart(t, rows)
			limiter := &memory.Limiter{MaxSize: tc.limit}
			m := downsampleMerger{mergeMemoryLimiter: limiter}
			var w downsampleWriter
			path := filepath.Join(t.TempDir(), "target")
			if err := w.Init(path, 1, config); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if _, err := m.Merge([]*partWrapper{source}, &w, nil, nil, 0); err == nil || !strings.Contains(err.Error(), "merge memory budget exceeded") {
				t.Fatalf("expected a bounded-memory failure, got %v", err)
			}
			if m.mergeMemoryBytes != 0 || cap(m.currentTSIDBaseBucketSamples) != 0 || cap(m.currentTSIDResolutionSamples) != 0 || m.currentTSIDBaseBucketIndexes != nil {
				t.Fatal("failed merge retained charged allocations")
			}
			if !limiter.Get(tc.limit) {
				t.Fatal("failed merge leaked memory budget")
			}
			limiter.Put(tc.limit)
			if err := w.Abort(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed output remains: %v", err)
			}
		})
	}
}

func TestDownsampleMergerMemoryGrowthAndSharedBudget(t *testing.T) {
	sampleBytes := uint64(unsafe.Sizeof(downsampleSample{}))
	limiter := &memory.Limiter{MaxSize: 3 * sampleBytes}
	a := downsampleMerger{mergeMemoryLimiter: limiter}
	b := downsampleMerger{mergeMemoryLimiter: limiter}
	defer a.reset()
	defer b.reset()
	var err error
	a.currentTSIDBaseBucketSamples, err = growDownsampleMergeSlice[downsampleSample](&a, nil, 1, &a.currentTSIDBaseSamplesMemory)
	if err != nil {
		t.Fatal(err)
	}
	a.currentTSIDBaseBucketSamples[0].timestamp = 123
	b.currentTSIDBaseBucketSamples, err = growDownsampleMergeSlice[downsampleSample](&b, nil, 1, &b.currentTSIDBaseSamplesMemory)
	if err != nil {
		t.Fatal(err)
	}
	// 新容量虽只有两项，扩容时另一个任务和当前旧数组仍持有额度。
	if _, err := growDownsampleMergeSlice(&a, a.currentTSIDBaseBucketSamples, 2, &a.currentTSIDBaseSamplesMemory); err == nil {
		t.Fatal("growth ignored another merge or the old allocation")
	}
	if a.currentTSIDBaseBucketSamples[0].timestamp != 123 || a.mergeMemoryBytes != sampleBytes {
		t.Fatal("failed growth changed existing state")
	}
	if err := b.reset(); err != nil {
		t.Fatal(err)
	}
	a.currentTSIDBaseBucketSamples, err = growDownsampleMergeSlice(&a, a.currentTSIDBaseBucketSamples, 2, &a.currentTSIDBaseSamplesMemory)
	if err != nil || a.currentTSIDBaseBucketSamples[0].timestamp != 123 {
		t.Fatalf("growth after release failed: %v", err)
	}
	if a.mergeMemoryBytes != 2*sampleBytes {
		t.Fatal("growth did not release old allocation")
	}
	if err := a.reset(); err != nil {
		t.Fatal(err)
	}
	if !limiter.Get(limiter.MaxSize) {
		t.Fatal("reset leaked a shared reservation")
	}
	limiter.Put(limiter.MaxSize)
}

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
	if stats.rowsMerged != downsampleBasePhysicalRowsForTest(first.p)+uint64(len(late)) || stats.rowsDeleted != 0 {
		t.Fatalf("mixed source physical row statistics changed: %+v", stats)
	}
	all := append(append([]rawRow(nil), rows...), late...)
	want := referenceDownsampleTestRows(all, nil, 0)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, second.p), want)
	// 每轮只使用每个源的目标分辨率，不能把同源的两份表示重复统计。
	third, stats := runDownsampleTestMerge(t, m, []*partWrapper{second}, nil, 0)
	if stats.rowsMerged != downsampleBasePhysicalRowsForTest(second.p) || stats.rowsDeleted != 0 {
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
				if err := w.Init(filepath.Join(t.TempDir(), "mixed"), -5, downsampleTestConfig(t)); err != nil {
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
						if err := r.ReadBlock(&b, r.Header()); err != nil {
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
				wantMerged = downsampleBasePhysicalRowsForTest(output.p)
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
	wantMerged := uint64(len(all))
	// raw 首次聚合和 summary 重写都按当前分辨率的 header 范围开槽。
	for round := 0; round < 2; round++ {
		w := getDownsampleWriter()
		defer putDownsampleWriter(w)
		path := filepath.Join(t.TempDir(), "part")
		if err := w.Init(path, -5, downsampleTestConfig(t)); err != nil {
			t.Fatal(err)
		}
		defer w.Abort()
		m.partWriter = w
		m.currentSourceBlock = getDownsampleDecodedResolutionFeaturesBlock()
		if err := m.initSources(sources, downsampleResolution5m); err != nil {
			t.Fatal(err)
		}
		for len(m.baseResolutionReaderHeap) > 0 {
			tsid := m.baseResolutionReaderHeap[0].Header().TSID
			tr, err := m.collectSources(&tsid, false)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.mergeTSID(&tsid, downsampleResolution5m, tr, 0); err != nil {
				t.Fatal(err)
			}
			wantBuckets := 2
			if tsid == long {
				wantBuckets = 5 // 63 天中只有五个实际 base bucket，空白不分配槽。
			}
			if len(m.currentTSIDBaseBucketSamples) != wantBuckets {
				t.Fatalf("TSID %d: got %d base slots; want %d", tsid.MetricID, len(m.currentTSIDBaseBucketSamples), wantBuckets)
			}
		}
		stats := m.mergeStats
		if err := m.reset(); err != nil {
			t.Fatal(err)
		}
		if stats.rowsMerged != wantMerged || stats.rowsDeleted != 0 {
			t.Fatalf("round %d: unexpected source statistics: %+v; want %d merged rows", round, stats, wantMerged)
		}
		if _, err := w.Finish(nil); err != nil {
			t.Fatal(err)
		}
		p, err := openDownsamplePart(path, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer p.MustClose()
		assertDownsampleTestRows(t, readDownsampleTestPart(t, p), referenceDownsampleTestRows(all, nil, 0))
		sources = []*partWrapper{{p: p}}
		wantMerged = downsampleBasePhysicalRowsForTest(p)
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
	want := referenceDownsampleTestRows(rows, &deleted, 0) // base 前缀仍为未过期的 1h bucket 提供贡献。
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), want)
	if stats.rowsDeleted != 1 || stats.rowsMerged != 2 {
		t.Fatalf("unexpected physical source statistics: merged=%d deleted=%d", stats.rowsMerged, stats.rowsDeleted)
	}
	// base 保留完整的 1h 前缀，后续 merge 只读取 base 仍能重建全部贡献。
	merged, _ := runDownsampleTestMerge(t, m, []*partWrapper{output}, &deleted, deadline)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, merged.p), want)
}

func TestDownsampleMergerRetainedPrefixThroughQueryDivisor(t *testing.T) {
	previous, err := ParseDownsamplingConfig([]byte(`{"base_resolution":"5m","tenant_resolutions":[{"tenant":"7:11","resolutions":["2h"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	current, err := ParseDownsamplingConfig([]byte(`{"base_resolution":"5m","tenant_resolutions":[{"tenant":"7:11","resolutions":["1h"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	const base int64 = 1704067200000
	tsid := TSID{AccountID: 7, ProjectID: 11, MetricID: 1}
	rows := []rawRow{
		{TSID: tsid, Timestamp: base + 60000, Value: 2, PrecisionBits: 64},
		{TSID: tsid, Timestamp: base + 61*60000, Value: 3, PrecisionBits: 64},
		{TSID: tsid, Timestamp: base + 91*60000, Value: 7, PrecisionBits: 64},
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	source, _ := runDownsampleTestMerge(t, m, []*partWrapper{newDownsampleTestRawPart(t, rows)}, nil, 0, previous)
	deadline := base + 90*60000
	output, _ := runDownsampleTestMerge(t, m, []*partWrapper{source}, nil, deadline, current)
	// 旧 2h 配置保留第一小时的 BASE 前缀。新 1h 列也必须完整表达它，
	// 否则查询 2h 选择最大的已存储因子 1h 时，会绕过仍存在的 BASE 贡献。
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), referenceDownsampleTestRows(rows, nil, 0, current))
	wantValues := []float64{7, 12, 3, 2, 7}
	for feature := uint8(0); feature < countOfDownsampleFeatures; feature++ {
		query := DownsampleQuery{ResolutionMs: 2 * downsampleResolution1h, Feature: feature}
		tr := TimeRange{deadline, base + 2*downsampleResolution1h - 1}
		ts := newDownsampleQueryTableFixture(t, []*part{output.p}, []TSID{tsid}, query, tr)
		var timestamps []int64
		var values []float64
		for ts.NextBlock() {
			var b Block
			ts.BlockRef.MustReadBlock(&b)
			if err := b.UnmarshalData(); err != nil {
				t.Fatal(err)
			}
			timestamps, values = b.AppendRowsWithTimeRangeFilter(timestamps, values, tr)
		}
		if err := ts.Error(); err != nil {
			t.Fatal(err)
		}
		ts.reset()
		if len(timestamps) != 1 || timestamps[0] != rows[2].Timestamp || len(values) != 1 || values[0] != wantValues[feature] {
			t.Fatalf("retained prefix lost through query divisor: feature=%d timestamps=%v values=%v", feature, timestamps, values)
		}
	}
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
	if stats.rowsMerged != 5 || stats.rowsDeleted != 5 || output.p.ph.RowsCount != 10 || output.p.ph.BlocksCount != 10 {
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
	if err := w.Init(path, -5, downsampleTestConfig(t)); err != nil {
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
		if err := w.Init(path, -5, downsampleTestConfig(t)); err != nil {
			putDownsampleWriter(w)
			t.Fatal(err)
		}
		if err := writeDownsampleTestBlock(w, block); err != nil {
			putDownsampleWriter(w)
			t.Fatal(err)
		}
		if _, err := w.Finish(nil); err != nil {
			putDownsampleWriter(w)
			t.Fatal(err)
		}
		putDownsampleWriter(w)
		p, err := openDownsamplePart(path, nil)
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
				column, err := r.readFeatureHeader(r.Header(), uint8(feature))
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
				if err := r.ReadBlock(&batch, r.Header()); err != nil {
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
		got := readDownsampleTestPart(t, output.p)
		for key := range got {
			if key.resolution != expected.resolution {
				delete(got, key)
			}
		}
		assertDownsampleTestRows(t, got, downsampleTestBlockRows(expected))
		sources = []*partWrapper{output}
	}
}

func TestDownsampleMergerResetClosesAllReaders(t *testing.T) {
	for _, operation := range []string{"reset", "init_sources", "merge"} {
		t.Run(operation, func(t *testing.T) {
			first := newDownsampleCloseTestReader(t)
			second := newDownsampleCloseTestReader(t)
			files := []filestream.ReadAtCloser{
				first.timestampsReader, first.valuesReader, first.indexReader,
				second.timestampsReader, second.valuesReader, second.indexReader,
			}
			first.timestampsReader.(*downsampleCloseTestFile).closeErr = os.ErrClosed
			// first 已出堆，但仍由 readers 持有；清理不能只遍历 heap。
			activeReaders := []*downsampleReader{first, second}
			m := downsampleMerger{
				baseResolutionReaders:    []*downsampleReader{first, second},
				currentTSIDReaders:       activeReaders,
				baseResolutionReaderHeap: downsampleReaderHeap{second},
			}
			var err error
			switch operation {
			case "reset":
				err = m.reset()
			case "init_sources":
				err = m.initSources(nil, downsampleResolution1h)
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
			if len(m.baseResolutionReaders) != 0 || len(m.currentTSIDReaders) != 0 || len(m.baseResolutionReaderHeap) != 0 {
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

func TestDownsampleMergerReadsIndexesOnce(t *testing.T) {
	// 降采样源的每列包含两个 index，长 TSID 横跨这两个 index。
	downsamplePart := newDownsampleIterationPart(t)
	// 已存储的 1h fixture 特意与 base 内容不同；归并必须忽略它并从 base 重建。
	want := readDownsampleTestPart(t, downsamplePart)
	for key := range want {
		if key.resolution != downsampleResolution5m {
			delete(want, key)
		}
	}
	var baseKeys []downsampleTestKey
	for key := range want {
		baseKeys = append(baseKeys, key)
	}
	sort.Slice(baseKeys, func(i, j int) bool {
		if baseKeys[i].tsid != baseKeys[j].tsid {
			return baseKeys[i].tsid.Less(&baseKeys[j].tsid)
		}
		return baseKeys[i].bucket < baseKeys[j].bucket
	})
	for _, key := range baseKeys {
		point := want[key]
		coarseKey := downsampleTestKey{key.tsid, downsampleResolution1h, point.timestamp / downsampleResolution1h}
		previous, exists := want[coarseKey]
		if exists {
			point.values[downsampleFeatureSum] += previous.values[downsampleFeatureSum]
			point.values[downsampleFeatureCount] += previous.values[downsampleFeatureCount]
			point.values[downsampleFeatureMin] = math.Min(point.values[downsampleFeatureMin], previous.values[downsampleFeatureMin])
			point.values[downsampleFeatureMax] = math.Max(point.values[downsampleFeatureMax], previous.values[downsampleFeatureMax])
		}
		want[coarseKey] = point
	}
	const base int64 = 1704067200000
	var rawRows []rawRow
	for metricID := uint64(1); metricID <= 3; metricID++ {
		for row := 0; row < 6; row++ {
			rawRows = append(rawRows, rawRow{TSID: TSID{MetricID: metricID}, Timestamp: base + int64(row)*3600000 + 1, Value: float64(row + 1), PrecisionBits: 64})
		}
	}
	rawSource := newDownsampleTestRawPart(t, rawRows)
	rawPath := filepath.Join(t.TempDir(), "raw")
	rawSource.mp.MustStoreToDisk(rawPath)
	rawPart := mustOpenFilePart(rawPath)
	t.Cleanup(rawPart.MustClose)
	lateRows := []rawRow{{TSID: TSID{MetricID: 1}, Timestamp: base + 100, Value: 9, PrecisionBits: 64}}
	sources := []*partWrapper{{p: downsamplePart}, {p: rawPart}, newDownsampleTestRawPart(t, lateRows)}
	var deleted uint64set.Set
	deleted.Add(2)
	deleted.Add(downsampleIterationTSID(5).MetricID)
	for key := range want {
		if deleted.Has(key.tsid.MetricID) {
			delete(want, key)
		}
	}
	for key, point := range referenceDownsampleTestRows(append(rawRows, lateRows...), &deleted, 0) {
		want[key] = point
	}
	var w downsampleWriter
	path := filepath.Join(t.TempDir(), "output")
	if err := w.Init(path, 1, downsampleTestConfig(t)); err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	// 使用与 Merge 相同的收集和聚合入口，在初始化后观察各源的真实 index 读取。
	m := downsampleMerger{partWriter: &w, currentSourceBlock: getDownsampleDecodedResolutionFeaturesBlock()}
	defer m.reset()
	for _, resolution := range []int64{downsampleResolution5m} {
		if err := m.initSources(sources, resolution); err != nil {
			t.Fatal(err)
		}
		var indexFiles []*downsampleIndexReadTestFile
		var expectedOffsets [][]int64
		for _, reader := range m.baseResolutionReaders {
			p := reader.currentSourcePart
			if p.path == "" {
				continue
			}
			var offsets []int64
			if p.dsMetadata != nil {
				for _, row := range p.dsMetaindex {
					if row.ResolutionMs == resolution {
						offsets = append(offsets, int64(row.IndexBlockOffset))
					}
				}
			} else {
				for _, row := range p.metaindex {
					offsets = append(offsets, int64(row.IndexBlockOffset))
				}
			}
			// initSources 为建堆已读过首列的第一个 index；记录该次读取，之后均由包装器观察。
			file := &downsampleIndexReadTestFile{ReadAtCloser: reader.indexReader, offsets: []int64{offsets[0]}}
			reader.indexReader = file
			indexFiles = append(indexFiles, file)
			expectedOffsets = append(expectedOffsets, offsets)
		}
		for len(m.baseResolutionReaderHeap) > 0 {
			tsid := m.baseResolutionReaderHeap[0].Header().TSID
			isDeleted := deleted.Has(tsid.MetricID)
			tr, err := m.collectSources(&tsid, isDeleted)
			if err != nil {
				t.Fatal(err)
			}
			if isDeleted {
				for _, reader := range m.currentTSIDReaders {
					if len(reader.currentTSIDBlockHeaders) != 0 {
						t.Fatal("deleted TSID retained block headers")
					}
				}
				continue
			}
			var nextHeaders []blockHeader
			for _, reader := range m.currentTSIDReaders {
				nextHeaders = append(nextHeaders, *reader.Header())
				if len(reader.currentTSIDBlockHeaders) == 0 {
					t.Fatal("source did not retain the current TSID headers")
				}
				for _, header := range reader.currentTSIDBlockHeaders {
					if header.TSID != tsid {
						t.Fatal("source retained headers from another TSID")
					}
				}
			}
			if err := m.mergeTSID(&tsid, resolution, tr, 0); err != nil {
				t.Fatal(err)
			}
			for i, reader := range m.currentTSIDReaders {
				if *reader.Header() != nextHeaders[i] || len(reader.currentTSIDBlockHeaders) != 0 {
					t.Fatal("payload reads changed the heap header or retained consumed headers")
				}
			}
		}
		for i, file := range indexFiles {
			counts := make(map[int64]int)
			for _, offset := range file.offsets {
				counts[offset]++
			}
			if len(file.offsets) != len(expectedOffsets[i]) {
				t.Fatalf("resolution %d index %q read %d blocks; want %d", resolution, file.Path(), len(file.offsets), len(expectedOffsets[i]))
			}
			for _, offset := range expectedOffsets[i] {
				if counts[offset] != 1 {
					t.Fatalf("resolution %d index %q offset %d read %d times; want once", resolution, file.Path(), offset, counts[offset])
				}
			}
		}
	}
	var sourceRows uint64
	for _, source := range sources {
		sourceRows += downsampleBasePhysicalRowsForTest(source.p)
	}
	if m.mergeStats.rowsMerged+m.mergeStats.rowsDeleted != sourceRows {
		t.Fatalf("source rows were lost or counted twice: %+v; want %d", m.mergeStats, sourceRows)
	}
	if err := m.reset(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}
	output, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer output.MustClose()
	got := readDownsampleTestPart(t, output)
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for key, expected := range want {
		actual, exists := got[key]
		if !exists || actual.timestamp != expected.timestamp || actual.precisionBits != expected.precisionBits {
			t.Fatalf("unexpected sample at %+v", key)
		}
		for feature, value := range expected.values {
			// 大数经原生 decimal 编解码会有末位舍入；本测试检查索引只读一次及正确选源。
			tolerance := 4 * math.Abs(math.Nextafter(value, math.Inf(1))-value)
			if math.Abs(actual.values[feature]-value) > tolerance {
				t.Fatalf("wrong feature %d at %+v: got %g; want %g", feature, key, actual.values[feature], value)
			}
		}
	}
}

func TestDownsampleMergerClosesBeforeReturning(t *testing.T) {
	for _, scenario := range []string{"success", "read_failure", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			const base int64 = 1704067200000
			mp := getInmemoryPart()
			mp.InitFromRows([]rawRow{{TSID: TSID{MetricID: 1}, Timestamp: base + 1, Value: 2, PrecisionBits: 64}})
			sourcePath := filepath.Join(t.TempDir(), "raw")
			mp.MustStoreToDisk(sourcePath)
			putInmemoryPart(mp)
			p := mustOpenFilePart(sourcePath)
			defer p.MustClose()
			if scenario == "read_failure" {
				if err := os.WriteFile(filepath.Join(sourcePath, indexFilename), []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var m downsampleMerger
			var w downsampleWriter
			if err := w.Init(filepath.Join(t.TempDir(), "target"), 1, downsampleTestConfig(t)); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			stopCh := make(chan struct{})
			if scenario == "cancel" {
				close(stopCh)
			}
			stats, err := m.Merge([]*partWrapper{{p: p}}, &w, stopCh, nil, 0)
			if (err == nil) != (scenario == "success") {
				t.Fatalf("unexpected merge result: %v", err)
			}
			if scenario == "cancel" && !errors.Is(err, errForciblyStopped) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if len(m.baseResolutionReaders) != 0 || len(m.baseResolutionReaderHeap) != 0 || len(m.currentTSIDReaders) != 0 || m.currentSourceBlock != nil || m.partWriter != nil || m.stopCh != nil {
				t.Fatal("Merge returned with retained reader or task state")
			}
			if scenario == "success" {
				if stats.rowsMerged != 1 {
					t.Fatalf("lost merge statistics: %+v", stats)
				}
				if _, err := w.Finish(nil); err != nil {
					t.Fatal(err)
				}
				close(stopCh)
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
					sourceRows += downsampleBasePhysicalRowsForTest(source.p)
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
					if err := w.Init(path, -5, downsampleTestConfig(b)); err != nil {
						b.Fatal(err)
					}
					if _, err := m.Merge(sources, w, nil, nil, 0); err != nil {
						b.Fatal(err)
					}
					if _, err := w.Finish(nil); err != nil {
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
