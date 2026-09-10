package storage

import (
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
)

func TestDownsampleBucket(t *testing.T) {
	for _, resolution := range downsampleResolutions {
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
	for _, resolution := range []int64{math.MinInt64, -1, 0, 5, 300, 60000, math.MaxInt64} {
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

func TestDownsampleNormalizeValue(t *testing.T) {
	for _, v := range []float64{0, math.Copysign(0, -1), 1.25, -2.5, math.SmallestNonzeroFloat64, math.MaxFloat64, math.Inf(1), math.Inf(-1)} {
		got := normalizeDownsampleValue(v)
		if math.Float64bits(got) != math.Float64bits(v) {
			t.Fatalf("normalization changed non-NaN value %v to %v", v, got)
		}
	}
	for _, bits := range []uint64{0x7ff0000000000001, 0x7ff8000000000001, 0xfff8000000001234, math.Float64bits(decimal.StaleNaN)} {
		got := normalizeDownsampleValue(math.Float64frombits(bits))
		if math.Float64bits(got) != math.Float64bits(decimal.StaleNaN) {
			t.Fatalf("NaN %x wasn't normalized; got %x", bits, math.Float64bits(got))
		}
	}
}

type downsampleTestSample struct {
	timestamp int64
	value     float64
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
			base := downsampleSample{timestamp: minUnixMilli + 1, values: [countOfDownsampleFeatures]float64{3, 10, 2, 1, 9}, precisionBits: 64}
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
			want := downsampleSample{timestamp: minUnixMilli + 2, values: [countOfDownsampleFeatures]float64{4, 30, 5, 1, 12}, precisionBits: 64}
			want.values[feature] = decimal.StaleNaN
			assertDownsamplePoint(t, &a, &want)
			if math.Float64bits(marked.values[feature]) != originalBits {
				t.Fatalf("source column %d was modified", feature)
			}
		}
	}

	// NaN 的算术来源不增加状态，也不影响其他列。
	left := downsampleSample{timestamp: minUnixMilli, values: [countOfDownsampleFeatures]float64{2, math.Inf(1), 1, 2, 2}, precisionBits: 64}
	right := downsampleSample{timestamp: minUnixMilli + 1, values: [countOfDownsampleFeatures]float64{3, math.Inf(-1), 1, 3, 3}, precisionBits: 64}
	var a downsampleSample
	a.Merge(&left)
	a.Merge(&right)
	want := downsampleSample{timestamp: minUnixMilli + 1, values: [countOfDownsampleFeatures]float64{3, decimal.StaleNaN, 2, 2, 3}, precisionBits: 64}
	assertDownsamplePoint(t, &a, &want)
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

func groupDownsampleTestSamples(samples []downsampleTestSample, resolution int64) map[int64][]downsampleTestSample {
	groups := make(map[int64][]downsampleTestSample)
	for _, sample := range samples {
		bucket := sample.timestamp / resolution
		groups[bucket] = append(groups[bucket], sample)
	}
	return groups
}

// referenceDownsampleTestPoint 分别扫描原始样本，独立计算有限小整数的参考统计。
func referenceDownsampleTestPoint(samples []downsampleTestSample) downsampleSample {
	p := downsampleSample{precisionBits: 64}
	for _, sample := range samples {
		if sample.timestamp > p.timestamp {
			p.timestamp = sample.timestamp
		}
	}
	p.values[downsampleFeatureLast] = math.Inf(-1)
	p.values[downsampleFeatureMin] = math.Inf(1)
	p.values[downsampleFeatureMax] = math.Inf(-1)
	p.values[downsampleFeatureCount] = float64(len(samples))
	for _, sample := range samples {
		p.values[downsampleFeatureSum] += sample.value
		p.values[downsampleFeatureMin] = math.Min(p.values[downsampleFeatureMin], sample.value)
		p.values[downsampleFeatureMax] = math.Max(p.values[downsampleFeatureMax], sample.value)
		if sample.timestamp == p.timestamp && sample.value > p.values[downsampleFeatureLast] {
			p.values[downsampleFeatureLast] = sample.value
		}
	}
	return p
}

func assertDownsamplePoint(t *testing.T, got, want *downsampleSample) {
	t.Helper()
	if got.precisionBits != want.precisionBits {
		t.Fatalf("unexpected precision bits; got %d; want %d", got.precisionBits, want.precisionBits)
	}
	if got.timestamp != want.timestamp {
		t.Fatalf("unexpected timestamp; got %d; want %d", got.timestamp, want.timestamp)
	}
	for feature, expected := range want.values {
		actual := got.values[feature]
		if math.IsNaN(expected) {
			if math.Float64bits(actual) != math.Float64bits(decimal.StaleNaN) {
				t.Fatalf("column %d wasn't normalized to StaleNaN; got %x", feature, math.Float64bits(actual))
			}
		} else if actual != expected {
			t.Fatalf("unexpected column %d; got %v; want %v", feature, actual, expected)
		}
	}
}
