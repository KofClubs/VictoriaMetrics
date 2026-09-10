package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func TestDownsampleReaderCrossIndexOffsets(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		shift int64
	}{
		{name: "continuous"},
		{name: "timestamp_gap", field: "timestamp", shift: 1},
		{name: "timestamp_overlap", field: "timestamp", shift: -1},
		{name: "values_gap", field: "values", shift: 1},
		{name: "values_overlap", field: "values", shift: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeDownsampleCrossIndexPart(t)
			if tc.field != "" {
				// 改动中间 index 的完整 block 偏移，保持列间连续及文件边界合法。
				// 若只校验单个 index 内部，四种损坏均不会在读取 header 时报错。
				rewriteDownsampleCrossIndexFile(t, path, 1, func(data []byte) {
					pos := 56 // 原生 blockHeader.TimestampsBlockOffset
					if tc.field == "values" {
						pos = 64 // 原生 blockHeader.ValuesBlockOffset
					}
					offset := binary.BigEndian.Uint64(data[pos : pos+8])
					binary.BigEndian.PutUint64(data[pos:pos+8], uint64(int64(offset)+tc.shift))
				})
			}
			if tc.field != "" {
				assertDownsampleLayoutOpenRejected(t, path, "相邻 index block 的降采样负载不连续")
			}
			p := openDownsampleLayoutReaderFixture(t, path)
			r := getDownsampleReader()
			defer putDownsampleReader(r)
			if err := r.Init(p, 300000); err != nil {
				t.Fatal(err)
			}
			count := 0
			for r.NextHeader() {
				count++
			}
			if tc.field == "" {
				if count != 4 || r.Error() != nil {
					t.Fatalf("正常跨 index 扫描失败: count=%d, err=%v", count, r.Error())
				}
			} else if count != 1 || r.Error() == nil || !strings.Contains(r.Error().Error(), "相邻 index block 的降采样负载不连续") {
				t.Fatalf("跨 index 偏移损坏未被结构校验拒绝: count=%d, err=%v", count, r.Error())
			}
		})
	}
}

func TestDownsampleReaderCrossIndexFilterAndReset(t *testing.T) {
	path := writeDownsampleCrossIndexPart(t)
	p, err := openDownsamplePart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	for _, resolution := range []int64{300000, 3600000} {
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		checkDownsampleCrossIndexStateCleared(t, r)
		checkDownsampleCrossIndexRows(t, r, []uint64{1, 2, 3, 4})
		if !r.hasPreviousIndex {
			t.Fatal("扫描后没有保存 index 边界")
		}
		// 第二个 index 的时间范围在窗口外；第三、第四个仍应正常读取。
		r.SetFilter(nil, minUnixMilli, minUnixMilli+4*resolution)
		checkDownsampleCrossIndexStateCleared(t, r)
		checkDownsampleCrossIndexRows(t, r, []uint64{1, 3, 4})
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		checkDownsampleCrossIndexStateCleared(t, r)
		tsid := TSID{MetricID: 3}
		r.SetFilter(&tsid, minUnixMilli, maxUnixMilli)
		checkDownsampleCrossIndexStateCleared(t, r)
		wantMetaPos := 2
		if resolution == 3600000 {
			wantMetaPos += 4 * countOfDownsampleFeatures
		}
		if r.metaPos != wantMetaPos {
			t.Fatalf("二分定位未跳过先前 index: got=%d, want=%d", r.metaPos, wantMetaPos)
		}
		checkDownsampleCrossIndexRows(t, r, []uint64{3})
	}
	r.reset()
	checkDownsampleCrossIndexStateCleared(t, r)
}

func TestDownsampleReaderCrossIndexSkippedCorruption(t *testing.T) {
	path := writeDownsampleCrossIndexPart(t)
	// feature 已上移到 metaindex；在不相交的原生 header 中制造非法精度。
	// 窗口读取不应补读跳过的 index，全量扫描则必须拒绝它。
	rewriteDownsampleCrossIndexFile(t, path, 1, func(data []byte) {
		data[88] = 0
	})
	assertDownsampleLayoutOpenRejected(t, path, "precisionBits")
	p := openDownsampleLayoutReaderFixture(t, path)
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	r.SetFilter(nil, minUnixMilli, minUnixMilli+4*300000)
	checkDownsampleCrossIndexRows(t, r, []uint64{1, 3, 4})
	r.SetFilter(nil, minUnixMilli, maxUnixMilli)
	count := 0
	for r.NextHeader() {
		count++
	}
	if count != 1 || r.Error() == nil {
		t.Fatalf("全量扫描未读取并拒绝损坏的 index: count=%d, err=%v", count, r.Error())
	}
}

func TestDownsampleLayoutMetaindexIdentityCorruption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"feature_out_of_range", func(m []byte) { m[64] = 5 }},
		{"feature_out_of_range_high", func(m []byte) { m[64] = 255 }},
		{"resolution", func(m []byte) { copy(m[65:73], encoding.MarshalInt64(nil, 1)) }},
		{"last_account", func(m []byte) { binary.BigEndian.PutUint32(m[73:77], 1) }},
		{"last_project", func(m []byte) { binary.BigEndian.PutUint32(m[77:81], 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeDownsampleCrossIndexPart(t)
			filename := filepath.Join(path, metaindexFilename)
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			meta := decodeDownsampleLayoutFrame(t, data, "VMDSMI")
			tc.mutate(meta[:clusterDownsampleMetaindexBytes])
			data = encoding.CompressZSTDLevel(append([]byte(nil), data[:8]...), meta, 1)
			if err := os.WriteFile(filename, data, 0644); err != nil {
				t.Fatal(err)
			}
			p, err := openDownsamplePart(path)
			if err == nil {
				p.MustClose()
				t.Fatal("接受了损坏的 metaindex feature/resolution/租户")
			}
		})
	}
}

func TestDownsampleLayoutIndexTenantCorruption(t *testing.T) {
	for _, offset := range []int{0, 4} {
		name := "account"
		if offset == 4 {
			name = "project"
		}
		t.Run(name, func(t *testing.T) {
			path := writeDownsampleCrossIndexPart(t)
			rewriteDownsampleCrossIndexFile(t, path, 1, func(data []byte) {
				binary.BigEndian.PutUint32(data[offset:offset+4], 1)
			})
			assertDownsampleLayoutOpenRejected(t, path, "租户")
			p := openDownsampleLayoutReaderFixture(t, path)
			r := getDownsampleReader()
			defer putDownsampleReader(r)
			if err := r.Init(p, 300000); err != nil {
				t.Fatal(err)
			}
			if !r.NextHeader() || r.NextHeader() || r.Error() == nil || !strings.Contains(r.Error().Error(), "租户") {
				t.Fatalf("未拒绝 index 内的跨租户 header: %v", r.Error())
			}
		})
	}
}

func assertDownsampleLayoutOpenRejected(t *testing.T, path, message string) {
	t.Helper()
	p, err := openDownsamplePart(path)
	if err == nil {
		p.MustClose()
		t.Fatal("打开时未拒绝损坏的 index")
	}
	if !strings.Contains(err.Error(), message) {
		t.Fatalf("打开时未报告预期损坏 %q: %v", message, err)
	}
}

// 仅为 reader 单元测试装配文件和 metaindex，不执行 open 的全量 index 校验。
// 损坏文件的生产打开路径由 assertDownsampleLayoutOpenRejected 单独覆盖。
func openDownsampleLayoutReaderFixture(t *testing.T, path string) *part {
	t.Helper()
	metadata, err := readDownsampleMetadata(path)
	if err != nil || metadata == nil {
		t.Fatalf("读取 fixture metadata: %v", err)
	}
	p := &part{path: path, ph: metadata.partHeader, dsMetadata: metadata}
	t.Cleanup(func() {
		for _, f := range []*os.File{p.dsTimestampsFile, p.dsValuesFile, p.dsIndexFile} {
			if f != nil {
				if err := f.Close(); err != nil {
					t.Error(err)
				}
			}
		}
	})
	if err := openDownsamplePartDataFile(path, timestampsFilename, &p.dsTimestampsFile, &p.dsTimestampsSize); err != nil {
		t.Fatal(err)
	}
	if err := openDownsamplePartDataFile(path, valuesFilename, &p.dsValuesFile, &p.dsValuesSize); err != nil {
		t.Fatal(err)
	}
	if err := openDownsamplePartDataFile(path, indexFilename, &p.dsIndexFile, &p.dsIndexSize); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(path, metaindexFilename))
	if err != nil {
		t.Fatal(err)
	}
	meta := decodeDownsampleLayoutFrame(t, data, "VMDSMI")
	if len(meta) == 0 || len(meta)%clusterDownsampleMetaindexBytes != 0 {
		t.Fatalf("fixture metaindex 长度错误: %d", len(meta))
	}
	for pos := 0; pos < len(meta); pos += clusterDownsampleMetaindexBytes {
		var mr downsampleMetaindexRow
		if err := mr.unmarshal(meta[pos : pos+clusterDownsampleMetaindexBytes]); err != nil {
			t.Fatal(err)
		}
		p.dsMetaindex = append(p.dsMetaindex, mr)
	}
	return p
}

func checkDownsampleCrossIndexStateCleared(t *testing.T, r *downsampleReader) {
	t.Helper()
	if r.hasPreviousIndex || r.previousIndexEnd != 0 || r.previousTimestampEnd != 0 || r.previousValuesEnd != 0 {
		t.Fatal("reader 重置后仍保留先前 index 边界")
	}
}

func checkDownsampleCrossIndexRows(t *testing.T, r *downsampleReader, ids []uint64) {
	t.Helper()
	b := getDownsampleDecodedResolutionFeaturesBlock()
	defer putDownsampleDecodedResolutionFeaturesBlock(b)
	i := 0
	for r.NextHeader() {
		if i >= len(ids) || r.Header().TSID.MetricID != ids[i] {
			t.Fatalf("读取顺序或筛选结果错误: position=%d, TSID=%+v", i, r.Header().TSID)
		}
		if err := r.ReadBlock(b); err != nil {
			t.Fatal(err)
		}
		if len(b.timestamps) != 4 {
			t.Fatalf("读取行数错误: %d", len(b.timestamps))
		}
		i++
	}
	if i != len(ids) || r.Error() != nil {
		t.Fatalf("跨 index 读取失败: count=%d, want=%d, err=%v", i, len(ids), r.Error())
	}
}

func writeDownsampleCrossIndexPart(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	// WriteSamples 暂存各 feature，实际切 index 发生在 flushResolution。
	w.indexLimit = clusterDownsampleFieldHeaderBytes
	for _, resolution := range []int64{300000, 3600000} {
		for id := uint64(1); id <= 4; id++ {
			b := &downsampleDecodedResolutionFeaturesBlock{tsid: TSID{MetricID: id}, resolution: resolution, precisionBits: 64}
			base := int64(minUnixMilli)
			if id == 2 {
				base += 50 * resolution
			}
			for row := 0; row < 4; row++ {
				b.timestamps = append(b.timestamps, base+int64(row)*resolution+int64(row*row+1))
				for col := range b.values {
					b.values[col] = append(b.values[col], float64(int(id)*10+col*4+row*row*row))
				}
			}
			if err := writeDownsampleTestBlock(w, b); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return path
}

// 直接修改磁盘 header 字节并重建压缩帧及 metaindex 的物理 offset/size。
// 不使用生产 header/metaindex decoder，损坏范围限定在指定 index 的字段中。
func rewriteDownsampleCrossIndexFile(t *testing.T, path string, target int, mutate func([]byte)) {
	t.Helper()
	metaFile, err := os.ReadFile(filepath.Join(path, metaindexFilename))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := encoding.DecompressZSTDLimited(nil, metaFile[8:], 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	indexFile, err := os.ReadFile(filepath.Join(path, indexFilename))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) != 40*clusterDownsampleMetaindexBytes || target < 0 || target >= 40 {
		t.Fatalf("测试需要四十个单特征单 block index，实际 metaindex 字节数=%d，target=%d", len(meta), target)
	}
	var rewritten []byte
	for pos := 0; pos < len(meta); pos += clusterDownsampleMetaindexBytes {
		mr := meta[pos : pos+clusterDownsampleMetaindexBytes]
		offset := binary.BigEndian.Uint64(mr[52:60])
		size := binary.BigEndian.Uint32(mr[60:64])
		frame := indexFile[offset : offset+uint64(size)]
		data, err := encoding.DecompressZSTDLimited(nil, frame[8:], maxBlockSize)
		if err != nil || len(data) != clusterDownsampleFieldHeaderBytes || binary.BigEndian.Uint32(mr[32:36]) != 1 {
			t.Fatalf("测试 index 不是一个 89 字节原生 header: size=%d, err=%v", len(data), err)
		}
		if pos/clusterDownsampleMetaindexBytes == target {
			mutate(data)
		}
		newFrame := encoding.CompressZSTDLevel(append([]byte(nil), frame[:8]...), data, 1)
		binary.BigEndian.PutUint64(mr[52:60], uint64(len(rewritten)))
		binary.BigEndian.PutUint32(mr[60:64], uint32(len(newFrame)))
		rewritten = append(rewritten, newFrame...)
	}
	if err := os.WriteFile(filepath.Join(path, indexFilename), rewritten, 0644); err != nil {
		t.Fatal(err)
	}
	metaFile = encoding.CompressZSTDLevel(append([]byte(nil), metaFile[:8]...), meta, 1)
	if err := os.WriteFile(filepath.Join(path, metaindexFilename), metaFile, 0644); err != nil {
		t.Fatal(err)
	}
}

// TestDownsampleReaderDuplicateBatchKey 拒绝同一 resolution/feature 内重复的 TSID/起始时间戳。
// 不同 feature 共享批次键合法，但同一 feature 内及跨 index 的键必须严格递增。
func TestDownsampleReaderDuplicateBatchKey(t *testing.T) {
	for _, separateIndexes := range []bool{false, true} {
		name := "within_index"
		if separateIndexes {
			name = "across_indexes"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part")
			w := getDownsampleWriter()
			defer putDownsampleWriter(w)
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			if separateIndexes {
				w.indexLimit = clusterDownsampleFieldHeaderBytes
			}
			for i := 0; i < 2; i++ {
				b := fileTestDownsampleBlock(1, 300000)
				for j := range b.timestamps {
					b.timestamps[j] += int64(i) * 2 * 300000
				}
				if err := writeDownsampleTestBlock(w, b); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			metaFile, err := os.ReadFile(filepath.Join(path, metaindexFilename))
			if err != nil {
				t.Fatal(err)
			}
			meta, err := encoding.DecompressZSTDLimited(nil, metaFile[8:], 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			indexFile, err := os.ReadFile(filepath.Join(path, indexFilename))
			if err != nil {
				t.Fatal(err)
			}
			wantMetaRows := 5
			if separateIndexes {
				wantMetaRows = 10
			}
			if len(meta) != wantMetaRows*clusterDownsampleMetaindexBytes {
				t.Fatalf("重复键 fixture 的 metaindex 行数错误: %d", len(meta))
			}
			var rewritten []byte
			for pos := 0; pos < len(meta); pos += clusterDownsampleMetaindexBytes {
				mr := meta[pos : pos+clusterDownsampleMetaindexBytes]
				offset := binary.BigEndian.Uint64(mr[52:60])
				size := binary.BigEndian.Uint32(mr[60:64])
				frame := indexFile[offset : offset+uint64(size)]
				data, err := encoding.DecompressZSTDLimited(nil, frame[8:], maxBlockSize)
				wantHeaders := 2
				if separateIndexes {
					wantHeaders = 1
				}
				if err != nil || len(data) != wantHeaders*clusterDownsampleFieldHeaderBytes {
					t.Fatalf("重复键 fixture 的 index 长度错误: %d, err=%v", len(data), err)
				}
				if !separateIndexes || pos/clusterDownsampleMetaindexBytes%2 == 1 {
					// 每个 feature 只替换第二个批次的 MinTimestamp，保留负载和其他统计。
					batchOffset := clusterDownsampleFieldHeaderBytes
					if separateIndexes {
						batchOffset = 0
					}
					first := encoding.MarshalInt64(nil, minUnixMilli+1)
					copy(data[batchOffset+32:batchOffset+40], first)
					copy(mr[36:44], first)
				}
				newFrame := encoding.CompressZSTDLevel(append([]byte(nil), frame[:8]...), data, 1)
				binary.BigEndian.PutUint64(mr[52:60], uint64(len(rewritten)))
				binary.BigEndian.PutUint32(mr[60:64], uint32(len(newFrame)))
				rewritten = append(rewritten, newFrame...)
			}
			if err := os.WriteFile(filepath.Join(path, indexFilename), rewritten, 0644); err != nil {
				t.Fatal(err)
			}
			metaFile = encoding.CompressZSTDLevel(append([]byte(nil), metaFile[:8]...), meta, 1)
			if err := os.WriteFile(filepath.Join(path, metaindexFilename), metaFile, 0644); err != nil {
				t.Fatal(err)
			}
			assertDownsampleLayoutOpenRejected(t, path, "排序错误")
			p := openDownsampleLayoutReaderFixture(t, path)
			r := getDownsampleReader()
			defer putDownsampleReader(r)
			if err := r.Init(p, 300000); err != nil {
				t.Fatal(err)
			}
			count := 0
			for r.NextHeader() {
				count++
			}
			wantCount := 0
			if separateIndexes {
				wantCount = 1
			}
			if count != wantCount || r.Error() == nil || !strings.Contains(r.Error().Error(), "排序错误") {
				t.Fatalf("重复批次键未在读取 header 时被拒绝: count=%d, want=%d, error=%v", count, wantCount, r.Error())
			}
		})
	}
}

func TestDownsampleReaderRawDuplicateBoundaryAllowed(t *testing.T) {
	mp := getInmemoryPart()
	defer putInmemoryPart(mp)
	var writer blockStreamWriter
	writer.MustInitFromInmemoryPart(mp, 1)
	mp.ph.Reset()
	var merged uint64
	for i := int64(0); i < 2; i++ {
		var b Block
		b.Init(&TSID{MetricID: 1}, []int64{minUnixMilli + 1, minUnixMilli + 2}, []int64{i + 10, i + 20}, 0, 64)
		writer.WriteExternalBlock(&b, &mp.ph, &merged)
	}
	writer.MustClose()
	p := mp.NewPart()
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	var batch downsampleDecodedResolutionFeaturesBlock
	count := 0
	for r.NextHeader() {
		if err := r.ReadBlock(&batch); err != nil {
			t.Fatal(err)
		}
		if len(batch.timestamps) != 2 || batch.values[downsampleFeatureLast][0] != float64(count+10) {
			t.Fatal("原始相同边界 Block 的样本未完整保留")
		}
		count++
	}
	if count != 2 || r.Error() != nil {
		t.Fatalf("原始相同边界 Block 被拒绝: count=%d, error=%v", count, r.Error())
	}
}
