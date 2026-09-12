package storage

import (
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func TestDownsampleDecodedBlockCapacity(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows int
		keep bool
	}{
		{"normal", downsampleMaxRawRows, true},
		{"oversized", downsampleMaxPooledRows + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := downsampleDecodedResolutionFeaturesBlock{tsid: TSID{MetricID: 1}, resolution: downsampleResolution5m, precisionBits: 64}
			for i := 0; i < tc.rows; i++ {
				b.timestamps = append(b.timestamps, int64(i))
				for feature := range b.values {
					b.values[feature] = append(b.values[feature], float64(i+feature))
				}
			}
			timestampsCap := cap(b.timestamps)
			var valuesCaps [countOfDownsampleFeatures]int
			for feature := range b.values {
				valuesCaps[feature] = cap(b.values[feature])
			}
			b.Reset()
			if b.tsid != (TSID{}) || b.resolution != 0 || b.precisionBits != 0 {
				t.Fatal("decoded block retained its logical state after Reset")
			}
			if len(b.timestamps) != 0 || tc.keep && cap(b.timestamps) != timestampsCap || !tc.keep && b.timestamps != nil {
				t.Fatalf("unexpected timestamp capacity after Reset: got %d; previous %d; keep %v", cap(b.timestamps), timestampsCap, tc.keep)
			}
			for feature := range b.values {
				if len(b.values[feature]) != 0 || tc.keep && cap(b.values[feature]) != valuesCaps[feature] || !tc.keep && b.values[feature] != nil {
					t.Fatalf("unexpected feature %d capacity after Reset: got %d; previous %d; keep %v", feature, cap(b.values[feature]), valuesCaps[feature], tc.keep)
				}
			}
		})
	}
}

func TestDownsampleBucket(t *testing.T) {
	for _, resolution := range []int64{1, 5, 300, 60000, downsampleResolution5m, downsampleResolution1h, 7 * downsampleResolution1h} {
		for _, timestamp := range []int64{
			minUnixMilli, minUnixMilli + 1,
			minUnixMilli + resolution - 1, minUnixMilli + resolution,
			minUnixMilli + resolution + 1, maxUnixMilli,
		} {
			bucketID, err := downsampleBucketID(timestamp, resolution)
			if err != nil {
				t.Fatalf("unexpected bucket error for (%d, %d): %s", timestamp, resolution, err)
			}
			end, err := downsampleBucketEnd(timestamp, resolution)
			if err != nil {
				t.Fatalf("unexpected bucket end error for (%d, %d): %s", timestamp, resolution, err)
			}
			// 通过区间包含关系和整除性验证，不直接复写分桶公式。
			if end%resolution != 0 || timestamp < end-resolution || timestamp >= end {
				t.Fatalf("invalid interval for timestamp %d: [%d, %d)", timestamp, end-resolution, end)
			}
			if bucketID*resolution != end-resolution {
				t.Fatalf("bucket %d doesn't match interval [%d, %d)", bucketID, end-resolution, end)
			}
		}
		for _, timestamp := range []int64{math.MinInt64, -1, 0, minUnixMilli - 1, maxUnixMilli + 1, math.MaxInt64} {
			if _, err := downsampleBucketID(timestamp, resolution); err == nil {
				t.Fatalf("expecting error for timestamp %d", timestamp)
			}
			if _, err := downsampleBucketEnd(timestamp, resolution); err == nil {
				t.Fatalf("expecting bucket end error for timestamp %d", timestamp)
			}
		}
	}
	for _, resolution := range []int64{math.MinInt64, -1, 0, maxUnixMilli + 1, math.MaxInt64} {
		if _, err := downsampleBucketID(minUnixMilli, resolution); err == nil {
			t.Fatalf("expecting error for resolution %d", resolution)
		}
		if _, err := downsampleBucketEnd(minUnixMilli, resolution); err == nil {
			t.Fatalf("expecting bucket end error for resolution %d", resolution)
		}
	}
	// 最后一个区间的右端点不属于可写入时间域，但必须仍能用于 retention。
	end, err := downsampleBucketEnd(maxUnixMilli, downsampleResolution1h)
	if err != nil || end != maxUnixMilli+1 {
		t.Fatalf("unexpected final bucket end; got %d, %v; want %d", end, err, maxUnixMilli+1)
	}
}

func TestDownsampleSampleMergeNormalization(t *testing.T) {
	values := []float64{0, math.Copysign(0, -1), 1.25, -2.5, math.SmallestNonzeroFloat64, math.MaxFloat64, math.Inf(1), math.Inf(-1)}
	for _, bits := range []uint64{0x7ff0000000000001, 0x7ff8000000000001, 0xfff8000000001234, math.Float64bits(decimal.StaleNaN)} {
		values = append(values, math.Float64frombits(bits))
	}
	for _, value := range values {
		src := downsampleSample{
			timestamp:     minUnixMilli + 1,
			values:        [countOfDownsampleFeatures]float64{value, value, value, value, value},
			precisionBits: 32,
		}
		original := src
		var got downsampleSample
		got.Merge(&src)
		if got.isEmpty() || got.timestamp != src.timestamp || got.precisionBits != src.precisionBits {
			t.Fatalf("first sample lost its timestamp or precision: %+v", got)
		}
		wantBits := math.Float64bits(value)
		if math.IsNaN(value) {
			wantBits = math.Float64bits(decimal.StaleNaN)
		}
		for feature := range got.values {
			if bits := math.Float64bits(got.values[feature]); bits != wantBits {
				t.Fatalf("column %d first input %x became %x; want %x", feature, math.Float64bits(value), bits, wantBits)
			}
			if math.Float64bits(src.values[feature]) != math.Float64bits(original.values[feature]) {
				t.Fatalf("source column %d was modified", feature)
			}
		}
		if src.timestamp != original.timestamp || src.precisionBits != original.precisionBits {
			t.Fatal("source timestamp or precision was modified")
		}
	}
}

func TestDownsampleSampleMergeRaw(t *testing.T) {
	nan := math.Float64frombits(0xfff8000000001234)
	stale := decimal.StaleNaN
	for _, tc := range []struct {
		name    string
		samples []downsampleTestSample
		want    downsampleSample
	}{
		{
			name:    "single",
			samples: []downsampleTestSample{{1, -2.5}},
			want:    downsampleSample{timestamp: 1, values: [countOfDownsampleFeatures]float64{-2.5, -2.5, 1, -2.5, -2.5}, precisionBits: 64},
		},
		{
			name:    "duplicates",
			samples: []downsampleTestSample{{1, 8}, {1, 8}, {1, 3}, {2, 5}},
			want:    downsampleSample{timestamp: 2, values: [countOfDownsampleFeatures]float64{5, 24, 4, 3, 8}, precisionBits: 64},
		},
		{
			name:    "timestamp-before-value",
			samples: []downsampleTestSample{{4, 3}, {1, 100}, {4, 8}},
			want:    downsampleSample{timestamp: 4, values: [countOfDownsampleFeatures]float64{8, 111, 3, 3, 100}, precisionBits: 64},
		},
		{
			name:    "latest-stale",
			samples: []downsampleTestSample{{4, stale}, {1, 2}, {2, 3}},
			want:    downsampleSample{timestamp: 4, values: [countOfDownsampleFeatures]float64{stale, stale, 3, stale, stale}, precisionBits: 64},
		},
		{
			name:    "earlier-stale",
			samples: []downsampleTestSample{{1, stale}, {2, 3}},
			want:    downsampleSample{timestamp: 2, values: [countOfDownsampleFeatures]float64{3, stale, 2, stale, stale}, precisionBits: 64},
		},
		{
			name:    "same-timestamp-number-first",
			samples: []downsampleTestSample{{1, 2}, {1, stale}, {1, 3}},
			want:    downsampleSample{timestamp: 1, values: [countOfDownsampleFeatures]float64{3, stale, 3, stale, stale}, precisionBits: 64},
		},
		{
			name:    "same-timestamp-stale-first",
			samples: []downsampleTestSample{{1, nan}, {1, -2}, {1, stale}},
			want:    downsampleSample{timestamp: 1, values: [countOfDownsampleFeatures]float64{-2, stale, 3, stale, stale}, precisionBits: 64},
		},
		{
			name:    "all-stale",
			samples: []downsampleTestSample{{1, nan}, {2, stale}},
			want:    downsampleSample{timestamp: 2, values: [countOfDownsampleFeatures]float64{stale, stale, 2, stale, stale}, precisionBits: 64},
		},
		{
			name:    "same-timestamp-all-nan",
			samples: []downsampleTestSample{{1, nan}, {1, stale}},
			want:    downsampleSample{timestamp: 1, values: [countOfDownsampleFeatures]float64{stale, stale, 2, stale, stale}, precisionBits: 64},
		},
		{
			name:    "opposite-infinities",
			samples: []downsampleTestSample{{1, math.Inf(1)}, {2, math.Inf(-1)}, {1, 5}},
			want:    downsampleSample{timestamp: 2, values: [countOfDownsampleFeatures]float64{math.Inf(-1), stale, 3, math.Inf(-1), math.Inf(1)}, precisionBits: 64},
		},
		{
			name:    "infinity-and-stale-at-same-time",
			samples: []downsampleTestSample{{1, stale}, {1, math.Inf(-1)}, {1, math.Inf(1)}},
			want:    downsampleSample{timestamp: 1, values: [countOfDownsampleFeatures]float64{math.Inf(1), stale, 3, stale, stale}, precisionBits: 64},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var a downsampleSample
			for _, sample := range tc.samples {
				a.MergeRaw(minUnixMilli+sample.timestamp, sample.value, 64)
			}
			if a.isEmpty() {
				t.Fatal("sample is empty")
			}
			tc.want.timestamp += minUnixMilli
			assertDownsamplePoint(t, &a, &tc.want)
		})
	}
}

func TestDownsampleSampleMerge(t *testing.T) {
	for _, precisionBits := range []uint8{1, 32, 64} {
		t.Run(strconv.Itoa(int(precisionBits)), func(t *testing.T) {
			aPoint := downsampleSample{timestamp: minUnixMilli + 4, values: [countOfDownsampleFeatures]float64{8, 20.5, 1.25, -2, 10}, precisionBits: precisionBits}
			bPoint := downsampleSample{timestamp: minUnixMilli + 4, values: [countOfDownsampleFeatures]float64{3, 12, 2.5, -5, 20}, precisionBits: precisionBits}
			aOriginal, bOriginal := aPoint, bPoint
			var a downsampleSample
			a.Merge(&aPoint)
			assertDownsamplePoint(t, &a, &aPoint)
			a.Merge(&bPoint)
			want := downsampleSample{timestamp: minUnixMilli + 4, values: [countOfDownsampleFeatures]float64{8, 32.5, 3.75, -5, 20}, precisionBits: precisionBits}
			assertDownsamplePoint(t, &a, &want)

			// 同一份样本再次输入仍增加 sum/count；此层不识别或删除重复贡献。
			a.Merge(&aPoint)
			want.values[downsampleFeatureSum] = 53
			want.values[downsampleFeatureCount] = 5
			assertDownsamplePoint(t, &a, &want)
			assertDownsamplePoint(t, &aPoint, &aOriginal)
			assertDownsamplePoint(t, &bPoint, &bOriginal)

			// 输入可与当前 sample 共用存储，不得读到已更新的半行状态。
			a.Merge(&a)
			want.values[downsampleFeatureSum] = 106
			want.values[downsampleFeatureCount] = 10
			assertDownsamplePoint(t, &a, &want)
		})
	}
}

func TestDownsampleSampleNaNColumns(t *testing.T) {
	for feature := 0; feature < countOfDownsampleFeatures; feature++ {
		for _, markerFirst := range []bool{false, true} {
			// min(-Inf, NaN) 和 max(+Inf, NaN) 仍须传播标记，且不能依赖输入顺序。
			base := downsampleSample{timestamp: minUnixMilli + 1, values: [countOfDownsampleFeatures]float64{3, 10, 2, math.Inf(-1), math.Inf(1)}, precisionBits: 64}
			marked := downsampleSample{timestamp: minUnixMilli + 2, values: [countOfDownsampleFeatures]float64{4, 20, 3, 2, 12}, precisionBits: 64}
			marked.values[feature] = math.Float64frombits(0xfff8000000001234)
			originalBits := math.Float64bits(marked.values[feature])
			var a downsampleSample
			if markerFirst {
				a.Merge(&marked)
				a.Merge(&base)
			} else {
				a.Merge(&base)
				a.Merge(&marked)
			}
			want := downsampleSample{timestamp: minUnixMilli + 2, values: [countOfDownsampleFeatures]float64{4, 30, 5, math.Inf(-1), math.Inf(1)}, precisionBits: 64}
			want.values[feature] = decimal.StaleNaN
			assertDownsamplePoint(t, &a, &want)
			if math.Float64bits(marked.values[feature]) != originalBits {
				t.Fatalf("source column %d was modified", feature)
			}
		}
	}

	// sum/count 的相反无穷值相加产生 NaN；两个输入方向均须规范化，且不影响其他列。
	left := downsampleSample{timestamp: minUnixMilli, values: [countOfDownsampleFeatures]float64{2, math.Inf(1), math.Inf(-1), 2, 2}, precisionBits: 64}
	right := downsampleSample{timestamp: minUnixMilli + 1, values: [countOfDownsampleFeatures]float64{3, math.Inf(-1), math.Inf(1), 3, 3}, precisionBits: 64}
	want := downsampleSample{timestamp: minUnixMilli + 1, values: [countOfDownsampleFeatures]float64{3, decimal.StaleNaN, decimal.StaleNaN, 2, 3}, precisionBits: 64}
	for _, reverse := range []bool{false, true} {
		var a downsampleSample
		if reverse {
			a.Merge(&right)
			a.Merge(&left)
		} else {
			a.Merge(&left)
			a.Merge(&right)
		}
		assertDownsamplePoint(t, &a, &want)
		a.Merge(&a)
		assertDownsamplePoint(t, &a, &want)
	}
}

func TestDownsampleSampleReset(t *testing.T) {
	var a downsampleSample
	if !a.isEmpty() || a != (downsampleSample{}) {
		t.Fatal("zero sample must be empty")
	}
	a.MergeRaw(minUnixMilli, decimal.StaleNaN, 32)
	want := downsampleSample{timestamp: minUnixMilli, values: [countOfDownsampleFeatures]float64{decimal.StaleNaN, decimal.StaleNaN, 1, decimal.StaleNaN, decimal.StaleNaN}, precisionBits: 32}
	assertDownsamplePoint(t, &a, &want)
	a.Reset()
	a.Reset()
	if !a.isEmpty() || a != (downsampleSample{}) {
		t.Fatal("reset retained sample state")
	}
	a.MergeRaw(minUnixMilli+1, -3, 64)
	want = downsampleSample{timestamp: minUnixMilli + 1, values: [countOfDownsampleFeatures]float64{-3, -3, 1, -3, -3}, precisionBits: 64}
	assertDownsamplePoint(t, &a, &want)
}

func TestDownsampleSampleRandomizedReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const baseTimestamp int64 = 1704067200000
	for iteration := 0; iteration < 50; iteration++ {
		samples := make([]downsampleTestSample, 256)
		for i := range samples {
			samples[i] = downsampleTestSample{
				timestamp: baseTimestamp + int64(rng.Intn(3*60*60))*1000,
				value:     float64(rng.Intn(201) - 100),
			}
			if i > 0 && i%11 == 0 {
				samples[i] = samples[i-1]
			}
		}
		// 所有加数均为小整数，保证这些不同分组的加法在 float64 中精确。
		for _, resolution := range downsampleResolutions {
			rawBuckets := groupDownsampleTestSamples(samples, resolution)
			parts := make([]map[int64]*downsampleSample, 7)
			for i := range parts {
				parts[i] = make(map[int64]*downsampleSample)
			}
			for _, sample := range samples {
				part := parts[rng.Intn(len(parts))]
				bucket := sample.timestamp / resolution
				if part[bucket] == nil {
					part[bucket] = &downsampleSample{}
				}
				part[bucket].MergeRaw(sample.timestamp, sample.value, 64)
			}
			for bucket, raw := range rawBuckets {
				want := referenceDownsampleTestPoint(raw)
				var direct downsampleSample
				for _, sample := range raw {
					direct.MergeRaw(sample.timestamp, sample.value, 64)
				}
				assertDownsamplePoint(t, &direct, &want)
				var level1 [3]downsampleSample
				for _, i := range rng.Perm(len(parts)) {
					if partial := parts[i][bucket]; partial != nil {
						level1[rng.Intn(len(level1))].Merge(partial)
					}
				}
				var merged downsampleSample
				for _, i := range rng.Perm(len(level1)) {
					if !level1[i].isEmpty() {
						merged.Merge(&level1[i])
					}
				}
				assertDownsamplePoint(t, &merged, &want)
			}
		}

		// 完整 5m 五特征样本可归并为 1h；参考结果直接由原始输入计算。
		coarse := make(map[int64]*downsampleSample)
		for _, raw := range groupDownsampleTestSamples(samples, downsampleResolution5m) {
			var fine downsampleSample
			for _, sample := range raw {
				fine.MergeRaw(sample.timestamp, sample.value, 64)
			}
			bucket := fine.timestamp / downsampleResolution1h
			if coarse[bucket] == nil {
				coarse[bucket] = &downsampleSample{}
			}
			coarse[bucket].Merge(&fine)
		}
		for bucket, raw := range groupDownsampleTestSamples(samples, downsampleResolution1h) {
			want := referenceDownsampleTestPoint(raw)
			assertDownsamplePoint(t, coarse[bucket], &want)
		}
	}
}

func TestDownsampleHeaderValidation(t *testing.T) {
	path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	if !r.NextHeader() {
		t.Fatal(r.Error())
	}
	base := *r.Header()
	cases := map[string]func(*blockHeader){
		"rows":                        func(h *blockHeader) { h.RowsCount = 0 },
		"too_many_rows":               func(h *blockHeader) { h.RowsCount = maxRowsPerBlock + 1 },
		"precision":                   func(h *blockHeader) { h.PrecisionBits = 0 },
		"type":                        func(h *blockHeader) { h.ValuesMarshalType = 255 },
		"size":                        func(h *blockHeader) { h.ValuesBlockSize = math.MaxUint32 },
		"time_domain":                 func(h *blockHeader) { h.MinTimestamp = minUnixMilli - 1 },
		"column_offset":               func(h *blockHeader) { h.ValuesBlockOffset = math.MaxUint64 },
		"single_row_delta2":           func(h *blockHeader) { h.RowsCount = 1; h.ValuesMarshalType = encoding.MarshalTypeNearestDelta2 },
		"single_row_timestamp_delta2": func(h *blockHeader) { h.RowsCount = 1; h.TimestampsMarshalType = encoding.MarshalTypeZSTDNearestDelta2 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			h := base
			mutate(&h)
			var got blockHeader
			_, err := got.Unmarshal(h.Marshal(nil))
			if err == nil {
				err = validateDownsampleHeader(&got)
			}
			if err == nil {
				t.Fatal("未拒绝非法 header")
			}
		})
	}
	var got blockHeader
	if _, err := got.Unmarshal(base.Marshal(nil)[:marshaledBlockHeaderSize-1]); err == nil {
		t.Fatal("未拒绝截断 header")
	}
}

func BenchmarkDownsampleSample(b *testing.B) {
	const rowsCount = 8192
	for _, sampleInput := range []bool{false, true} {
		name := "Raw"
		if sampleInput {
			name = "Sample"
		}
		b.Run(name, func(b *testing.B) {
			points := make([]downsampleSample, rowsCount)
			for i := range points {
				v := float64(i%17 - 8)
				points[i] = downsampleSample{timestamp: minUnixMilli + int64(i), values: [countOfDownsampleFeatures]float64{v, v * 16, 16, v - 2, v + 2}, precisionBits: 64}
			}
			var a downsampleSample
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a.Reset()
				for j := range points {
					if sampleInput {
						a.Merge(&points[j])
					} else {
						a.MergeRaw(points[j].timestamp, points[j].values[downsampleFeatureLast], points[j].precisionBits)
					}
				}
			}
			b.StopTimer()
			downsampleBenchmarkPoint = a
			reportDownsampleBenchmarkRows(b, rowsCount)
		})
	}
}
