package storage

// AddRawRow 将一条原始样本提升为摘要；标记同样贡献一次 count。
// 仅用于测试注入原始样本，生产路径由 downsampleReader.readRawBlock 在读取时批量展开。
func (a *downsampleAccumulator) AddRawRow(timestamp int64, value float64) {
	value = normalizeDownsampleValue(value)
	p := downsampleSample{
		timestamp: timestamp,
		values:    [countOfDownsampleFeatures]float64{value, value, 1, value, value},
	}
	a.AddSummary(&p)
}
