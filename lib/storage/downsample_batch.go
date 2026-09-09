package storage

import "sync"

// downsampleBatch 是归并计算的临时批次；文件中的每个特征由独立的原生 Block 存储。
type downsampleBatch struct {
	tsid       TSID
	resolution int64
	timestamps []int64
	values     [countOfDownsampleFeatures][]float64
	// 时间戳和五个 value 列共用 raw 的精度，默认 64。
	precisionBits uint8
}

// Reset 清除逻辑状态，保留正常容量，并释放超出单 block 上限的缓冲。
func (b *downsampleBatch) Reset() {
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

func getDownsampleBatch() *downsampleBatch {
	if v := downsampleBatchPool.Get(); v != nil {
		return v.(*downsampleBatch)
	}
	return &downsampleBatch{}
}

func putDownsampleBatch(b *downsampleBatch) { b.Reset(); downsampleBatchPool.Put(b) }

var downsampleBatchPool sync.Pool
