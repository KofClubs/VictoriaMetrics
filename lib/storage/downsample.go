package storage

import (
	"fmt"
	"math"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
)

const (
	downsampleResolution5m int64 = 300000
	downsampleResolution1h int64 = 3600000
)

var downsampleResolutions = [2]int64{downsampleResolution5m, downsampleResolution1h}

const (
	downsampleFeatureLast = iota
	downsampleFeatureSum
	downsampleFeatureCount
	downsampleFeatureMin
	downsampleFeatureMax
	countOfDownsampleFeatures
)

// downsampleSample 保存一个 bucket 的样本，并可直接合并同 bucket 的其他样本。
// 时间戳和五个特征共用源精度；precisionBits 为 0 表示空槽，合法样本精度为 1..64。
type downsampleSample struct {
	// timestamp 是当前 bucket 中最新贡献的时间，用于选择 last，并作为写出时间戳。
	timestamp int64
	// values 保存当前 bucket 的五个特征，按 downsampleFeature* 编号直接累加，避免额外状态包装。
	values [countOfDownsampleFeatures]float64
	// precisionBits 继承源精度；0 标记尚无贡献的空 bucket，写出时跳过。
	precisionBits uint8
}

func (s *downsampleSample) isEmpty() bool {
	return s.precisionBits == 0
}

// Reset 清除全部样本状态，使当前槽位恢复为空。
func (s *downsampleSample) Reset() {
	*s = downsampleSample{}
}

// Merge 将另一个样本合入当前样本，不修改输入；sum 和 count 按输入值累加。
// 输入须携带 1..64 的源精度；调用方保证 TSID、分辨率、bucket 和精度一致，并处理选源及 retention。
func (s *downsampleSample) Merge(other *downsampleSample) {
	// 复制后再归并，保证输入与当前状态共用存储时仍只读取一份完整源贡献。
	src := *other
	for i, v := range src.values {
		src.values[i] = normalizeDownsampleValue(v)
	}
	if s.isEmpty() {
		*s = src
		return
	}

	dst := s
	if src.timestamp > dst.timestamp {
		dst.timestamp = src.timestamp
		dst.values[downsampleFeatureLast] = src.values[downsampleFeatureLast]
	} else if src.timestamp == dst.timestamp {
		last := dst.values[downsampleFeatureLast]
		next := src.values[downsampleFeatureLast]
		// 仅在相同时间戳下优先数值；较新标记不能回退到较早数值。
		if math.IsNaN(last) || (!math.IsNaN(next) && next > last) {
			dst.values[downsampleFeatureLast] = next
		}
	}

	for _, feature := range [...]int{downsampleFeatureSum, downsampleFeatureCount} {
		v := dst.values[feature]
		next := src.values[feature]
		if math.IsNaN(v) || math.IsNaN(next) {
			dst.values[feature] = decimal.StaleNaN
		} else {
			dst.values[feature] = normalizeDownsampleValue(v + next)
		}
	}
	for _, feature := range [...]int{downsampleFeatureMin, downsampleFeatureMax} {
		v := dst.values[feature]
		next := src.values[feature]
		if math.IsNaN(v) || math.IsNaN(next) {
			dst.values[feature] = decimal.StaleNaN
		} else if feature == downsampleFeatureMin {
			dst.values[feature] = math.Min(v, next)
		} else {
			dst.values[feature] = math.Max(v, next)
		}
	}
}

// normalizeDownsampleValue 将所有 NaN 统一为现有 decimal 编码支持的标记。
func normalizeDownsampleValue(v float64) float64 {
	if math.IsNaN(v) {
		return decimal.StaleNaN
	}
	return v
}

// downsampleBucketID 使用毫秒整数除法定位左闭右开的固定分辨率区间。
func downsampleBucketID(timestamp, resolution int64) (int64, error) {
	if resolution != downsampleResolution5m && resolution != downsampleResolution1h {
		return 0, fmt.Errorf("unsupported downsampling resolution %d", resolution)
	}
	if timestamp < minUnixMilli || timestamp > maxUnixMilli {
		return 0, fmt.Errorf("downsampling timestamp %d is outside [%d, %d]", timestamp, minUnixMilli, maxUnixMilli)
	}
	return timestamp / resolution, nil
}

// downsampleBucketEnd 返回区间右端点，供整区间 retention 判断使用。
func downsampleBucketEnd(timestamp, resolution int64) (int64, error) {
	bucketID, err := downsampleBucketID(timestamp, resolution)
	if err != nil {
		return 0, err
	}
	// 先检查商，避免加一或乘法溢出；右端点可以超出最后一个有效样本时间戳。
	if bucketID >= math.MaxInt64/resolution {
		return 0, fmt.Errorf("downsampling bucket end overflows for timestamp %d and resolution %d", timestamp, resolution)
	}
	return (bucketID + 1) * resolution, nil
}
