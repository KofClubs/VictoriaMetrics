package storage

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

var downsampleBenchmarkPoint downsampleSample

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

// 文件基准包括 writer 的创建、编码、写入及同步关闭；目录统计和删除不计时。
// 每次生成独立目标并立即删除，源 part 和工作对象在循环间复用。
func BenchmarkDownsampleMerge(b *testing.B) {
	for _, workload := range []string{"Dense", "Sparse", "HighCardinality"} {
		for _, mode := range []string{"Raw", "Summary", "SummaryRewrite"} {
			b.Run(workload+"/"+mode, func(b *testing.B) {
				root := b.TempDir()
				sources := newDownsampleBenchmarkSources(b, workload)
				if mode != "Raw" {
					for i, source := range sources {
						sources[i] = newDownsampleBenchmarkFile(b, []*partWrapper{source}, filepath.Join(root, "source-"+strconv.Itoa(i)))
					}
				}
				if mode == "SummaryRewrite" {
					// 先合并成一个完整摘要 part，测量已经归并过的摘要再次落盘的成本。
					sources = []*partWrapper{newDownsampleBenchmarkFile(b, sources, filepath.Join(root, "merged-source"))}
				}
				var sourceRows uint64
				for _, source := range sources {
					sourceRows += source.p.ph.RowsCount
				}
				m := getDownsampleMerger()
				defer putDownsampleMerger(m)
				w := getDownsampleWriter()
				defer putDownsampleWriter(w)
				var outputBytes uint64
				b.ReportAllocs()
				b.ResetTimer()
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					path := filepath.Join(root, "output-"+strconv.Itoa(i))
					b.StartTimer()
					if err := w.Init(path, -5); err != nil {
						b.Fatal(err)
					}
					if _, err := m.Merge(sources, w, nil, nil, 0); err != nil {
						b.Fatal(err)
					}
					if _, err := w.Finish(); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					outputBytes += downsampleBenchmarkFileSize(b, path)
					if err := os.RemoveAll(path); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(outputBytes)/float64(b.N), "file-bytes/op")
				reportDownsampleBenchmarkRows(b, sourceRows)
			})
		}
	}
}

func BenchmarkDownsampleFileRead(b *testing.B) {
	for _, workload := range []string{"Dense", "Sparse", "HighCardinality"} {
		b.Run(workload, func(b *testing.B) {
			p := newDownsampleBenchmarkFile(b, newDownsampleBenchmarkSources(b, workload), filepath.Join(b.TempDir(), "source")).p
			fileBytes := downsampleBenchmarkFileSize(b, p.path)
			r := getDownsampleReader()
			defer putDownsampleReader(r)
			block := getDownsampleDecodedResolutionFeaturesBlock()
			defer putDownsampleDecodedResolutionFeaturesBlock(block)
			var count float64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, resolution := range downsampleResolutions {
					if err := r.Init(p, resolution); err != nil {
						b.Fatal(err)
					}
					for r.NextHeader() {
						if err := r.ReadBlock(block); err != nil {
							b.Fatal(err)
						}
						for _, v := range block.values[downsampleFeatureCount] {
							count += v
						}
					}
					if err := r.Error(); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			downsampleBenchmarkPoint.values[downsampleFeatureCount] = count
			b.ReportMetric(float64(fileBytes), "source-file-bytes")
			reportDownsampleBenchmarkRows(b, p.ph.RowsCount)
		})
	}
}

func BenchmarkDownsampleFileWrite(b *testing.B) {
	for _, workload := range []string{"Dense", "Sparse", "HighCardinality"} {
		b.Run(workload, func(b *testing.B) {
			root := b.TempDir()
			p := newDownsampleBenchmarkFile(b, newDownsampleBenchmarkSources(b, workload), filepath.Join(root, "source")).p
			blocks := readDownsampleBenchmarkBlocks(b, p)
			// 测试数据转置不计入 writer 基准；生产直接接收 merger 的样本切片。
			blockSamples := make([][]downsampleSample, len(blocks))
			for i, block := range blocks {
				var err error
				blockSamples[i], err = downsampleTestBlockSamples(block)
				if err != nil {
					b.Fatal(err)
				}
			}
			w := getDownsampleWriter()
			defer putDownsampleWriter(w)
			var outputBytes uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				path := filepath.Join(root, "output-"+strconv.Itoa(i))
				b.StartTimer()
				if err := w.Init(path, -5); err != nil {
					b.Fatal(err)
				}
				for blockIndex, block := range blocks {
					if err := w.WriteSamples(&block.tsid, block.resolution, blockSamples[blockIndex], nil); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := w.Finish(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				outputBytes += downsampleBenchmarkFileSize(b, path)
				if err := os.RemoveAll(path); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(outputBytes)/float64(b.N), "file-bytes/op")
			reportDownsampleBenchmarkRows(b, p.ph.RowsCount)
		})
	}
}

func newDownsampleBenchmarkSources(b *testing.B, workload string) []*partWrapper {
	b.Helper()
	const base int64 = 1704067200000
	rowsCount := 8192
	if workload == "Sparse" {
		rowsCount = 1024
	}
	var rows [4][]rawRow
	for i := 0; i < rowsCount; i++ {
		tsid := TSID{MetricID: 1}
		timestamp := base + int64(i)*1000
		switch workload {
		case "Dense":
		case "Sparse":
			timestamp = base + int64(i)*2*downsampleResolution5m
		case "HighCardinality":
			tsid.MetricID = uint64(i/64 + 1)
			timestamp = base + int64(i%64)*1000
		default:
			b.Fatalf("unknown workload %q", workload)
		}
		rows[i%len(rows)] = append(rows[i%len(rows)], rawRow{TSID: tsid, Timestamp: timestamp, Value: float64(i%17 - 8), PrecisionBits: 64})
	}
	sources := make([]*partWrapper, 0, len(rows))
	for _, raw := range rows {
		mp := getInmemoryPart()
		mp.InitFromRows(raw)
		pw := newPartWrapperFromInmemoryPart(mp, time.Time{})
		b.Cleanup(pw.decRef)
		sources = append(sources, pw)
	}
	return sources
}

func newDownsampleBenchmarkFile(b *testing.B, sources []*partWrapper, path string) *partWrapper {
	b.Helper()
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, -5); err != nil {
		b.Fatal(err)
	}
	if _, err := m.Merge(sources, w, nil, nil, 0); err != nil {
		b.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		b.Fatal(err)
	}
	p, err := openDownsamplePart(path)
	if err != nil {
		b.Fatal(err)
	}
	pw := &partWrapper{p: p}
	pw.incRef()
	b.Cleanup(pw.decRef)
	return pw
}

func readDownsampleBenchmarkBlocks(b *testing.B, p *part) []*downsampleDecodedResolutionFeaturesBlock {
	b.Helper()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	var blocks []*downsampleDecodedResolutionFeaturesBlock
	for _, resolution := range downsampleResolutions {
		if err := r.Init(p, resolution); err != nil {
			b.Fatal(err)
		}
		for r.NextHeader() {
			block := getDownsampleDecodedResolutionFeaturesBlock()
			b.Cleanup(func() { putDownsampleDecodedResolutionFeaturesBlock(block) })
			if err := r.ReadBlock(block); err != nil {
				b.Fatal(err)
			}
			blocks = append(blocks, block)
		}
		if err := r.Error(); err != nil {
			b.Fatal(err)
		}
	}
	return blocks
}

func downsampleBenchmarkFileSize(b *testing.B, path string) uint64 {
	b.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		b.Fatal(err)
	}
	var size uint64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			b.Fatal(err)
		}
		if info.Mode().IsRegular() {
			size += uint64(info.Size())
		}
	}
	return size
}

func reportDownsampleBenchmarkRows(b *testing.B, rowsPerOperation uint64) {
	b.Helper()
	b.ReportMetric(float64(rowsPerOperation), "input-rows/op")
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(rowsPerOperation)*float64(b.N)/elapsed, "rows/s")
	}
}
