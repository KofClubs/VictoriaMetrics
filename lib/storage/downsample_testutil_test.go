package storage

import "fmt"

// writeDownsampleTestBlock 将已有的列式测试数据转成 writer 的样本输入。
// 该转置仅供测试构造文件；生产 merger 直接传入自己的 bucket 样本。
func writeDownsampleTestBlock(w *downsampleWriter, block *downsampleDecodedResolutionFeaturesBlock) error {
	if block == nil {
		return w.WriteSamples(nil, 0, nil, nil)
	}
	samples, err := downsampleTestBlockSamples(block)
	if err != nil {
		return err
	}
	return w.WriteSamples(&block.tsid, block.resolution, samples, nil)
}

func downsampleTestBlockSamples(block *downsampleDecodedResolutionFeaturesBlock) ([]downsampleSample, error) {
	for feature, values := range block.values {
		if len(values) != len(block.timestamps) {
			return nil, fmt.Errorf("invalid test feature %d row count: %d vs %d", feature, len(values), len(block.timestamps))
		}
	}
	samples := make([]downsampleSample, len(block.timestamps))
	for row, timestamp := range block.timestamps {
		samples[row].timestamp = timestamp
		samples[row].precisionBits = block.precisionBits
		for feature := range block.values {
			samples[row].values[feature] = block.values[feature][row]
		}
	}
	return samples, nil
}

// MergeRaw 将一条原始样本展开为五特征后合并；标记同样贡献一次 count。
// 仅用于测试注入原始样本，生产路径由 downsampleReader.readRawBlock 在读取时批量展开。
func (a *downsampleSample) MergeRaw(timestamp int64, value float64, precisionBits uint8) {
	value = normalizeDownsampleValue(value)
	p := downsampleSample{
		timestamp:     timestamp,
		precisionBits: precisionBits,
		values:        [countOfDownsampleFeatures]float64{value, value, 1, value, value},
	}
	a.Merge(&p)
}
