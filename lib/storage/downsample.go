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

// downsampleSample 保存一个区间的一份摘要；五个特征共享 timestamp。
type downsampleSample struct {
	timestamp int64
	values    [countOfDownsampleFeatures]float64
}

// downsampleAccumulator 归并同一 TSID、目标分辨率和区间的全部输入。
// 调用方负责分桶、选源和 retention；此处不删除重复输入。
type downsampleAccumulator struct {
	sample      downsampleSample
	initialized bool
}

// Reset 清除全部摘要状态，空区间不产生摘要行。
func (a *downsampleAccumulator) Reset() {
	*a = downsampleAccumulator{}
}

// AddSummary 合并一份源摘要，不改变输入，也不将摘要行重新计为一个样本。
func (a *downsampleAccumulator) AddSummary(s *downsampleSample) {
	// 复制后再归并，保证输入与当前状态共用存储时仍只读取一份完整源贡献。
	src := *s
	for i, v := range src.values {
		src.values[i] = normalizeDownsampleValue(v)
	}
	if !a.initialized {
		a.sample = src
		a.initialized = true
		return
	}

	dst := &a.sample
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
