package storage

import "sync"

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
	// values 按 downsampleFeature* 编号保存五个已解码浮点列，与 timestamps 逐行对齐。
	values [countOfDownsampleFeatures][]float64
	// precisionBits 继承源 Block 的精度，时间列和五个特征共用，避免重写时改变精度。
	precisionBits uint8
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
