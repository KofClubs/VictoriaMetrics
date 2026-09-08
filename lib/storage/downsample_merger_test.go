package storage

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

type downsampleTestKey struct {
	tsid       TSID
	resolution int64
	bucket     int64
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
}

func TestDownsampleMergerOverlappingBlocksAndWindows(t *testing.T) {
	const base int64 = 1704067200000
	ts := TSID{MetricID: 42}
	rows := make([]rawRow, maxRowsPerBlock+257)
	for i := range rows {
		rows[i] = rawRow{TSID: ts, Timestamp: base + int64(i)*1000, Value: float64(i%7 - 3), PrecisionBits: 64}
	}
	// 手工构造同一 part 中时间范围重叠的两个 block，均跨越多个双区间窗口。
	var blocks [][]rawRow
	for offset := 0; offset < 2; offset++ {
		var block []rawRow
		for i := 0; i < 130; i++ {
			block = append(block, rawRow{TSID: ts, Timestamp: base + int64(i+offset)*60000, Value: float64(offset + 2), PrecisionBits: 64})
		}
		blocks = append(blocks, block)
	}
	all := append([]rawRow(nil), rows...)
	for _, block := range blocks {
		all = append(all, block...)
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	output, stats := runDownsampleTestMerge(t, m, []*partWrapper{
		newDownsampleTestRawPart(t, rows), newDownsampleTestOverlappingPart(t, blocks),
	}, nil, 0)
	if stats.rowsMerged != uint64(len(all)) {
		t.Fatalf("unexpected source rows; got %d; want %d", stats.rowsMerged, len(all))
	}
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), referenceDownsampleTestRows(all, nil, 0))
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

func TestDownsampleMergerSpecialValues(t *testing.T) {
	const base int64 = 1704067200000
	rows := []rawRow{
		{TSID: TSID{MetricID: 7}, Timestamp: base + 60000, Value: math.Inf(1), PrecisionBits: 64},
		{TSID: TSID{MetricID: 7}, Timestamp: base + 120000, Value: math.Inf(-1), PrecisionBits: 64},
		{TSID: TSID{MetricID: 8}, Timestamp: base + 60000, Value: decimal.StaleNaN, PrecisionBits: 64},
		{TSID: TSID{MetricID: 8}, Timestamp: base + 120000, Value: 5, PrecisionBits: 64},
	}
	want := make(map[downsampleTestKey]downsamplePoint)
	for _, resolution := range downsampleResolutions {
		want[downsampleTestKey{TSID{MetricID: 7}, resolution, base / resolution}] = downsamplePoint{
			base + 120000, [5]float64{math.Inf(-1), decimal.StaleNaN, 2, math.Inf(-1), math.Inf(1)},
		}
		want[downsampleTestKey{TSID{MetricID: 8}, resolution, base / resolution}] = downsamplePoint{
			base + 120000, [5]float64{5, decimal.StaleNaN, 2, decimal.StaleNaN, decimal.StaleNaN},
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
	rows := make([]rawRow, maxRowsPerBlock+1)
	for i := range rows {
		rows[i] = rawRow{
			TSID: TSID{MetricID: 7}, Timestamp: base + int64(i)*downsampleResolution5m,
			Value: float64(i % 5), PrecisionBits: 64,
		}
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	output, _ := runDownsampleTestMergeWindow(t, m, []*partWrapper{newDownsampleTestRawPart(t, rows)}, nil, 0, downsampleWindowBuckets)
	if output.p.ph.BlocksCount < 3 {
		t.Fatalf("expected split 5m blocks and a 1h block; got %d", output.p.ph.BlocksCount)
	}
	assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), referenceDownsampleTestRows(rows, nil, 0))
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
	stats, err := m.Merge([]*partWrapper{raw}, w, stopCh, nil, 0, 2)
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

func TestDownsampleMergerPrecisionAndColumnScales(t *testing.T) {
	const base int64 = 1704067200000
	precisions := [2][5]uint8{{32, 16, 8, 40, 64}, {16, 32, 16, 8, 32}}
	timestampPrecisions := [2]uint8{8, 32}
	var sources []*partWrapper
	var decoded []*downsampleBatch
	for source := range precisions {
		block := &downsampleBatch{
			tsid: TSID{MetricID: 42}, resolution: downsampleResolution5m,
			precisionBits: precisions[source], timestampPrecisionBits: timestampPrecisions[source],
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
		if err := w.WriteBlock(block); err != nil {
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
			scales := make(map[int16]bool)
			for feature := range block.values {
				_, scale := decimal.AppendFloatToDecimal(nil, block.values[feature])
				column := r.Header().Columns[feature]
				if column.Scale != scale || column.PrecisionBits != block.precisionBits[feature] {
					t.Fatalf("source column %d lost scale or precision", feature)
				}
				scales[scale] = true
			}
			if len(scales) < 3 {
				t.Fatal("test must cover distinct column scales")
			}
		}()
	}
	// 参考结果以原 codec 解码后的输入为基准，再独立执行摘要运算和输出编码。
	points := make(map[int64]downsamplePoint)
	for _, block := range decoded {
		for row, timestamp := range block.timestamps {
			bucket := timestamp / block.resolution
			point, ok := points[bucket]
			if !ok {
				point.timestamp = timestamp
				for feature := range point.values {
					point.values[feature] = block.values[feature][row]
				}
			} else {
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
	expected := &downsampleBatch{
		tsid: TSID{MetricID: 42}, resolution: downsampleResolution5m,
		precisionBits: [5]uint8{16, 16, 8, 8, 32}, timestampPrecisionBits: 8,
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
			if r.Header().Timestamps.PrecisionBits != expected.timestampPrecisionBits {
				t.Fatal("merged timestamp precision wasn't inherited from its sources")
			}
			for feature, precision := range expected.precisionBits {
				if r.Header().Columns[feature].PrecisionBits != precision {
					t.Fatalf("merged column %d didn't retain minimum source precision", feature)
				}
			}
		}()
		expected = roundTripDownsampleTestReferenceBlock(t, expected)
		assertDownsampleTestRows(t, readDownsampleTestPart(t, output.p), downsampleTestBlockRows(expected))
		sources = []*partWrapper{output}
	}
}

func roundTripDownsampleTestReferenceBlock(t *testing.T, source *downsampleBatch) *downsampleBatch {
	t.Helper()
	result := &downsampleBatch{
		tsid: source.tsid, resolution: source.resolution,
		precisionBits: source.precisionBits, timestampPrecisionBits: source.timestampPrecisionBits,
	}
	data, marshalType, first := encoding.MarshalTimestamps(nil, source.timestamps, source.timestampPrecisionBits)
	var err error
	result.timestamps, err = encoding.UnmarshalTimestamps(nil, data, marshalType, first, len(source.timestamps))
	if err != nil {
		t.Fatal(err)
	}
	if source.timestampPrecisionBits < 64 {
		encoding.EnsureNonDecreasingSequence(result.timestamps, source.timestamps[0], source.timestamps[len(source.timestamps)-1])
	}
	for feature, values := range source.values {
		integers, scale := decimal.AppendFloatToDecimal(nil, values)
		data, marshalType, first := encoding.MarshalValues(nil, integers, source.precisionBits[feature])
		decoded, err := encoding.UnmarshalValues(nil, data, marshalType, first, len(values))
		if err != nil {
			t.Fatal(err)
		}
		result.values[feature] = decimal.AppendDecimalToFloat(nil, decoded, scale)
	}
	return result
}

func downsampleTestBlockRows(block *downsampleBatch) map[downsampleTestKey]downsamplePoint {
	rows := make(map[downsampleTestKey]downsamplePoint)
	for row, timestamp := range block.timestamps {
		point := downsamplePoint{timestamp: timestamp}
		for feature := range point.values {
			point.values[feature] = block.values[feature][row]
		}
		rows[downsampleTestKey{block.tsid, block.resolution, timestamp / block.resolution}] = point
	}
	return rows
}

func newDownsampleTestRawPart(t *testing.T, rows []rawRow) *partWrapper {
	t.Helper()
	mp := getInmemoryPart()
	mp.InitFromRows(append([]rawRow(nil), rows...))
	pw := newPartWrapperFromInmemoryPart(mp, time.Time{})
	t.Cleanup(pw.decRef)
	return pw
}

func newDownsampleTestOverlappingPart(t *testing.T, blocks [][]rawRow) *partWrapper {
	t.Helper()
	mp := getInmemoryPart()
	mp.Reset()
	bsw := getBlockStreamWriter()
	bsw.MustInitFromInmemoryPart(mp, -5)
	var merged uint64
	for _, rows := range blocks {
		var timestamps []int64
		var values []float64
		for _, row := range rows {
			timestamps = append(timestamps, row.Timestamp)
			values = append(values, row.Value)
		}
		encoded, scale := decimal.AppendFloatToDecimal(nil, values)
		var b Block
		b.Init(&rows[0].TSID, timestamps, encoded, scale, 64)
		bsw.WriteExternalBlock(&b, &mp.ph, &merged)
	}
	bsw.MustClose()
	putBlockStreamWriter(bsw)
	pw := newPartWrapperFromInmemoryPart(mp, time.Time{})
	t.Cleanup(pw.decRef)
	return pw
}

func runDownsampleTestMerge(t *testing.T, m *downsampleMerger, sources []*partWrapper, deleted *uint64set.Set, deadline int64) (*partWrapper, downsampleMergeStats) {
	t.Helper()
	return runDownsampleTestMergeWindow(t, m, sources, deleted, deadline, 2)
}

func runDownsampleTestMergeWindow(t *testing.T, m *downsampleMerger, sources []*partWrapper, deleted *uint64set.Set, deadline int64, windowBuckets int) (*partWrapper, downsampleMergeStats) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, -5); err != nil {
		t.Fatal(err)
	}
	stats, err := m.Merge(sources, w, nil, deleted, deadline, windowBuckets)
	if err != nil {
		w.Abort()
		t.Fatalf("cannot merge: %s", err)
	}
	if _, err := w.Finish(); err != nil {
		w.Abort()
		t.Fatalf("cannot finish merged part: %s", err)
	}
	p, err := openDownsamplePart(path)
	if err != nil {
		t.Fatalf("cannot open merged part: %s", err)
	}
	pw := &partWrapper{p: p}
	pw.incRef()
	t.Cleanup(pw.decRef)
	return pw, stats
}

func readDownsampleTestPart(t *testing.T, p *part) map[downsampleTestKey]downsamplePoint {
	t.Helper()
	result := make(map[downsampleTestKey]downsamplePoint)
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	b := getDownsampleBatch()
	defer putDownsampleBatch(b)
	for _, resolution := range downsampleResolutions {
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		for r.NextHeader() {
			if err := r.ReadBlock(b); err != nil {
				t.Fatal(err)
			}
			for i, timestamp := range b.timestamps {
				key := downsampleTestKey{b.tsid, resolution, timestamp / resolution}
				if _, ok := result[key]; ok {
					t.Fatalf("multiple rows for target bucket %+v", key)
				}
				point := downsamplePoint{timestamp: timestamp}
				for feature := range point.values {
					point.values[feature] = b.values[feature][i]
				}
				result[key] = point
			}
		}
		if err := r.Error(); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func referenceDownsampleTestRows(rows []rawRow, deleted *uint64set.Set, deadline int64) map[downsampleTestKey]downsamplePoint {
	groups := make(map[downsampleTestKey][]downsampleTestSample)
	for _, row := range rows {
		if deleted != nil && deleted.Has(row.TSID.MetricID) {
			continue
		}
		for _, resolution := range downsampleResolutions {
			bucket := row.Timestamp / resolution
			if (bucket+1)*resolution <= deadline {
				continue
			}
			key := downsampleTestKey{row.TSID, resolution, bucket}
			groups[key] = append(groups[key], downsampleTestSample{row.Timestamp, row.Value})
		}
	}
	result := make(map[downsampleTestKey]downsamplePoint)
	for key, samples := range groups {
		result[key] = referenceDownsampleTestPoint(samples)
	}
	return result
}

func assertDownsampleTestRows(t *testing.T, got, want map[downsampleTestKey]downsamplePoint) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("unexpected summary rows; got %d; want %d\ngot: %+v\nwant: %+v", len(got), len(want), got, want)
	}
	for key, expected := range want {
		actual, ok := got[key]
		if !ok {
			t.Fatalf("missing summary key %+v", key)
		}
		assertDownsamplePoint(t, &actual, &expected)
	}
}
