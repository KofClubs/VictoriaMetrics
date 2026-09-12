package storage

import (
	"fmt"
	"math"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

const (
	downsampleFeatureLast = iota
	downsampleFeatureSum
	downsampleFeatureCount
	downsampleFeatureMin
	downsampleFeatureMax
	countOfDownsampleFeatures
)

// downsampleFeatureNames 与上面的 downsampleFeature* iota 逐项对齐，用于 spill 文件命名与诊断输出。
var downsampleFeatureNames = [countOfDownsampleFeatures]string{"last", "sum", "count", "min", "max"}

const (
	// 原始输入沿用已有双倍行数上限。
	downsampleMaxRawRows = 2 * maxRowsPerBlock
	// Go append 的正常容量增长也属于可复用容量，不只按逻辑行数判断峰值。
	downsampleMaxPooledRows = 2 * downsampleMaxRawRows
)

// downsampleSample 保存一个 bucket 的样本，并可直接合并同 bucket 的其他样本。
// 五个特征保留源值精度，时间戳写出时使用无损编码；precisionBits 为 0 表示空槽，合法值为 1..64。
type downsampleSample struct {
	// timestamp 是当前 bucket 中最新贡献的时间，用于选择 last，并作为写出时间戳。
	timestamp int64
	// values 保存当前 bucket 的五个特征；Merge 返回时全部 NaN 均为 decimal.StaleNaN，可直接编码。
	values [countOfDownsampleFeatures]float64
	// precisionBits 继承源精度；0 标记尚无贡献的空 bucket，写出时跳过。
	precisionBits uint8
}

// downsampleDecodedResolutionFeaturesBlock 保存一个 TSID 在指定分辨率下的已解码时间列和全部特征列。
// 文件中每个特征仍是独立的原生 Block；这里仅组织 reader 的解码结果，不负责聚合或持久化。
// raw 源读入时按目标分辨率标记、展开五个特征，实际分桶由 merger 完成。
type downsampleDecodedResolutionFeaturesBlock struct {
	// tsid 标识全部列所属的同一条序列，防止跨 TSID 合并数据。
	tsid TSID
	// resolution 是本次读取的目标分辨率（毫秒），raw 源尚未按此分辨率分桶。
	resolution int64
	// timestamps 保存当前 Block 批次唯一的已解码时间列，供五个特征按行共享。
	timestamps []int64
	// values 保存与 timestamps 逐行对齐的五个已解码浮点列；NaN 规范化由 downsampleSample.Merge 负责。
	values [countOfDownsampleFeatures][]float64
	// precisionBits 继承源 Block 的值精度，供五个特征共用；降采样时间列始终无损。
	precisionBits uint8
}

// Reset 清除全部样本状态，使当前槽位恢复为空。
func (s *downsampleSample) Reset() {
	*s = downsampleSample{}
}

// Merge 将另一个样本合入当前样本，不修改输入；sum 和 count 按输入值累加。
// 输入须携带 1..64 的源精度；调用方保证 TSID、分辨率、bucket 和精度一致，并处理选源及 retention。
// 输入标记和运算产生的 NaN 统一在结果中规范化，reader 与 writer 无需再次处理。
func (s *downsampleSample) Merge(other *downsampleSample) {
	// 复制后再归并，保证输入与当前状态共用存储时仍只读取一份完整源贡献。
	src := *other
	if s.isEmpty() {
		*s = src
	} else {
		if src.timestamp > s.timestamp {
			s.timestamp = src.timestamp
			s.values[downsampleFeatureLast] = src.values[downsampleFeatureLast]
		} else if src.timestamp == s.timestamp {
			last := s.values[downsampleFeatureLast]
			next := src.values[downsampleFeatureLast]
			// 仅在相同时间戳下优先数值；较新标记不能回退到较早数值。
			if math.IsNaN(last) || (!math.IsNaN(next) && next > last) {
				s.values[downsampleFeatureLast] = next
			}
		}

		for _, feature := range [...]int{downsampleFeatureSum, downsampleFeatureCount} {
			s.values[feature] += src.values[feature]
		}
		for _, feature := range [...]int{downsampleFeatureMin, downsampleFeatureMax} {
			v := s.values[feature]
			next := src.values[feature]
			// math.Min/Max 在负/正无穷与 NaN 同时出现时优先返回无穷，必须显式传播标记。
			if math.IsNaN(v) {
				continue
			}
			if math.IsNaN(next) {
				s.values[feature] = next
			} else if feature == downsampleFeatureMin {
				s.values[feature] = math.Min(v, next)
			} else {
				s.values[feature] = math.Max(v, next)
			}
		}
	}

	// 每次合并只在此处扫描结果，覆盖首次复制、输入 NaN 及正负无穷相加产生的 NaN。
	for feature, value := range s.values {
		if math.IsNaN(value) {
			s.values[feature] = decimal.StaleNaN
		}
	}
}

func (s *downsampleSample) isEmpty() bool {
	return s.precisionBits == 0
}

// Reset 清除逻辑状态，保留正常容量，并释放超出单 block 上限的缓冲。
func (b *downsampleDecodedResolutionFeaturesBlock) Reset() {
	b.tsid = TSID{}
	b.resolution = 0
	b.precisionBits = 0
	if cap(b.timestamps) > downsampleMaxPooledRows {
		b.timestamps = nil
	} else {
		b.timestamps = b.timestamps[:0]
	}
	for i := range b.values {
		if cap(b.values[i]) > downsampleMaxPooledRows {
			b.values[i] = nil
		} else {
			b.values[i] = b.values[i][:0]
		}
	}
}

// downsampleBucketID 使用毫秒整数除法定位左闭右开的固定分辨率区间。
func downsampleBucketID(timestamp, resolution int64) (int64, error) {
	if !validDownsampleResolution(resolution) {
		return 0, fmt.Errorf("[downsampling] unsupported downsampling resolution %d", resolution)
	}
	if timestamp < minUnixMilli || timestamp > maxUnixMilli {
		return 0, fmt.Errorf("[downsampling] downsampling timestamp %d is outside [%d, %d]", timestamp, minUnixMilli, maxUnixMilli)
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
		return 0, fmt.Errorf("[downsampling] downsampling bucket end overflows for timestamp %d and resolution %d", timestamp, resolution)
	}
	return (bucketID + 1) * resolution, nil
}

func validDownsampleResolution(r int64) bool { return r > 0 && r <= maxUnixMilli }

// marshalDownsampleBlock 复用原生 Block 的数值编码，并保证分桶时间戳无损。
// precisionBits 仍描述源数值精度；低精度时间戳不能移动 bucket 或改变最后贡献时间。
// timestamps 由调用方持有，不能借用 MarshalData 会清理的 Block 时间戳切片。
func marshalDownsampleBlock(b *Block, timestamps []int64, timestampsOffset, valuesOffset uint64) ([]byte, []byte, []byte) {
	b.MarshalData(timestampsOffset, valuesOffset)
	if b.bh.PrecisionBits < 64 {
		b.timestampsData, b.bh.TimestampsMarshalType, b.bh.MinTimestamp = encoding.MarshalTimestamps(b.timestampsData[:0], timestamps, 64)
		b.bh.MaxTimestamp = timestamps[len(timestamps)-1]
		b.bh.TimestampsBlockSize = uint32(len(b.timestampsData))
		b.headerData = b.bh.Marshal(b.headerData[:0])
	}
	return b.headerData, b.timestampsData, b.valuesData
}

func validateDownsampleHeader(h *blockHeader) error {
	if err := h.validate(); err != nil {
		return fmt.Errorf("[downsampling] invalid block header: %w", err)
	}
	if h.RowsCount > maxRowsPerBlock || h.MinTimestamp > h.MaxTimestamp || h.MinTimestamp < minUnixMilli || h.MaxTimestamp > maxUnixMilli || (h.RowsCount == 1 && h.MinTimestamp != h.MaxTimestamp) {
		return fmt.Errorf("[downsampling] invalid single-feature block row count or time range")
	}
	if h.TimestampsBlockOffset > uint64(^uint64(0)>>1)-uint64(h.TimestampsBlockSize) || h.ValuesBlockOffset > uint64(^uint64(0)>>1)-uint64(h.ValuesBlockSize) {
		return fmt.Errorf("[downsampling] single-feature block offset overflows")
	}
	for _, column := range []struct {
		mt   encoding.MarshalType
		size uint32
	}{
		{h.TimestampsMarshalType, h.TimestampsBlockSize},
		{h.ValuesMarshalType, h.ValuesBlockSize},
	} {
		if column.mt == 0 || (column.mt == encoding.MarshalTypeConst && column.size != 0) || (column.mt != encoding.MarshalTypeConst && column.size == 0) || (column.mt == encoding.MarshalTypeDeltaConst && column.size > 10) {
			return fmt.Errorf("[downsampling] single-feature block encoding type does not match payload size")
		}
	}
	if h.TimestampsMarshalType == encoding.MarshalTypeConst && h.MinTimestamp != h.MaxTimestamp {
		return fmt.Errorf("[downsampling] constant timestamp does not match the header time range")
	}
	return nil
}

// 编码前 bucket 唯一；有损时间戳可能移动 bucket，磁盘排序只要求批次时间范围不重叠。
func downsampleHeadersOrdered(a, b *blockHeader) bool {
	return downsampleHeaderLess(a, b) && (a.TSID != b.TSID || a.MaxTimestamp < b.MinTimestamp)
}

func downsampleHeaderLess(a, b *blockHeader) bool {
	if a.TSID != b.TSID {
		return a.TSID.Less(&b.TSID)
	}
	return a.MinTimestamp < b.MinTimestamp
}

func sameDownsampleTimestamps(a, b *blockHeader) bool {
	return a.TSID == b.TSID && a.RowsCount == b.RowsCount && a.MinTimestamp == b.MinTimestamp && a.MaxTimestamp == b.MaxTimestamp && a.PrecisionBits == b.PrecisionBits && a.TimestampsBlockOffset == b.TimestampsBlockOffset && a.TimestampsBlockSize == b.TimestampsBlockSize && a.TimestampsMarshalType == b.TimestampsMarshalType
}

func sameDownsampleTenant(a, b *TSID) bool {
	return a.AccountID == b.AccountID && a.ProjectID == b.ProjectID
}

func getDownsampleDecodedResolutionFeaturesBlock() *downsampleDecodedResolutionFeaturesBlock {
	if v := downsampleDecodedResolutionFeaturesBlockPool.Get(); v != nil {
		return v.(*downsampleDecodedResolutionFeaturesBlock)
	}
	return &downsampleDecodedResolutionFeaturesBlock{}
}

func putDownsampleDecodedResolutionFeaturesBlock(b *downsampleDecodedResolutionFeaturesBlock) {
	b.Reset()
	downsampleDecodedResolutionFeaturesBlockPool.Put(b)
}

var downsampleDecodedResolutionFeaturesBlockPool sync.Pool
