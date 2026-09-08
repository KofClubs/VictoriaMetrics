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
					positions := []int{66, 165, 264, 363, 462}
					if tc.field == "values" {
						positions = []int{74, 173, 272, 371, 470}
					}
					for _, pos := range positions {
						offset := binary.BigEndian.Uint64(data[pos : pos+8])
						binary.BigEndian.PutUint64(data[pos:pos+8], uint64(int64(offset)+tc.shift))
					}
				})
			}
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
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
			wantMetaPos += 4
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
	// 在不相交的 index 中制造非法 feature，证明窗口读取不会补读跳过的 index。
	rewriteDownsampleCrossIndexFile(t, path, 1, func(data []byte) {
		data[8] = 99
	})
	p, err := openDownsamplePart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
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

func checkDownsampleCrossIndexStateCleared(t *testing.T, r *downsampleReader) {
	t.Helper()
	if r.hasPreviousIndex || r.previousIndexEnd != 0 || r.previousTimestampEnd != 0 || r.previousValuesEnd != 0 {
		t.Fatal("reader 重置后仍保留先前 index 边界")
	}
}

func checkDownsampleCrossIndexRows(t *testing.T, r *downsampleReader, ids []uint64) {
	t.Helper()
	b := getDownsampleBatch()
	defer putDownsampleBatch(b)
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
	for _, resolution := range []int64{300000, 3600000} {
		for id := uint64(1); id <= 4; id++ {
			b := &downsampleBatch{tsid: TSID{MetricID: id}, resolution: resolution, timestampPrecisionBits: 64, precisionBits: [5]uint8{64, 64, 64, 64, 64}}
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
			if err := w.WriteBlock(b); err != nil {
				t.Fatal(err)
			}
			if err := w.flushIndex(); err != nil {
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
	if len(meta) != 8*downsampleMetaindexRowSize {
		t.Fatalf("测试需要八个单 block index，实际 metaindex 字节数=%d", len(meta))
	}
	var rewritten []byte
	for pos := 0; pos < len(meta); pos += downsampleMetaindexRowSize {
		mr := meta[pos : pos+downsampleMetaindexRowSize]
		offset := binary.BigEndian.Uint64(mr[92:100])
		size := binary.BigEndian.Uint32(mr[100:104])
		frame := indexFile[offset : offset+uint64(size)]
		data, err := encoding.DecompressZSTDLimited(nil, frame[8:], maxBlockSize)
		if err != nil || len(data) != downsampleBlockHeaderSize {
			t.Fatalf("测试 index 不是五个 %d 字节单特征 header: size=%d, err=%v", downsampleFieldHeaderSize, len(data), err)
		}
		if pos/downsampleMetaindexRowSize == target {
			mutate(data)
		}
		newFrame := encoding.CompressZSTDLevel(append([]byte(nil), frame[:8]...), data, 1)
		binary.BigEndian.PutUint64(mr[92:100], uint64(len(rewritten)))
		binary.BigEndian.PutUint32(mr[100:104], uint32(len(newFrame)))
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

// TestDownsampleReaderDuplicateBatchKey 拒绝相同分辨率、TSID 和起始时间戳的重复批次。
// 原生字段顺序要求每个批次的键严格递增，不能在相同键下由 feature 5 再回到 feature 1。
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
			for i := 0; i < 2; i++ {
				b := fileTestDownsampleBlock(1, 300000)
				for j := range b.timestamps {
					b.timestamps[j] += int64(i) * 2 * 300000
				}
				if err := w.WriteBlock(b); err != nil {
					t.Fatal(err)
				}
				if separateIndexes {
					if err := w.flushIndex(); err != nil {
						t.Fatal(err)
					}
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
			var rewritten []byte
			for pos := 0; pos < len(meta); pos += downsampleMetaindexRowSize {
				mr := meta[pos : pos+downsampleMetaindexRowSize]
				offset := binary.BigEndian.Uint64(mr[92:100])
				size := binary.BigEndian.Uint32(mr[100:104])
				frame := indexFile[offset : offset+uint64(size)]
				data, err := encoding.DecompressZSTDLimited(nil, frame[8:], maxBlockSize)
				if err != nil {
					t.Fatal(err)
				}
				if !separateIndexes || pos > 0 {
					// 只替换第二个批次的最小时间戳，保留负载 offset/size 和其他统计。
					batchOffset := downsampleBlockHeaderSize
					if separateIndexes {
						batchOffset = 0
					}
					first := encoding.MarshalInt64(nil, minUnixMilli+1)
					for feature := 0; feature < 5; feature++ {
						copy(data[batchOffset+feature*downsampleFieldHeaderSize+42:], first)
					}
					copy(mr[72:80], first)
				}
				newFrame := encoding.CompressZSTDLevel(append([]byte(nil), frame[:8]...), data, 1)
				binary.BigEndian.PutUint64(mr[92:100], uint64(len(rewritten)))
				binary.BigEndian.PutUint32(mr[100:104], uint32(len(newFrame)))
				rewritten = append(rewritten, newFrame...)
			}
			if err := os.WriteFile(filepath.Join(path, indexFilename), rewritten, 0644); err != nil {
				t.Fatal(err)
			}
			metaFile = encoding.CompressZSTDLevel(append([]byte(nil), metaFile[:8]...), meta, 1)
			if err := os.WriteFile(filepath.Join(path, metaindexFilename), metaFile, 0644); err != nil {
				t.Fatal(err)
			}
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
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
	var batch downsampleBatch
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
