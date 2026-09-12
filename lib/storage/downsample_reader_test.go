package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestDownsampleFileRoundtrip(t *testing.T) {
	blocks := []*downsampleDecodedResolutionFeaturesBlock{fileTestDownsampleBlock(1, 300000), fileTestDownsampleBlock(2, 300000), fileTestDownsampleBlock(1, 3600000)}
	blocks[1].values[downsampleFeatureSum][0] = decimal.StaleNaN
	path := writeFileTestDownsamplePart(t, blocks...)
	if ds, err := detectDownsampleFormat(path); err != nil || !ds {
		t.Fatalf("格式识别: %v %v", ds, err)
	}
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	if p.ph.RowsCount != 30 || p.ph.BlocksCount != 15 || len(p.dsMetaindex) != 2*countOfDownsampleFeatures {
		t.Fatalf("错误 part 统计: %+v; metaindex rows=%d", p.ph, len(p.dsMetaindex))
	}
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	b := getDownsampleDecodedResolutionFeaturesBlock()
	defer putDownsampleDecodedResolutionFeaturesBlock(b)
	i := 0
	for _, res := range []int64{300000, 3600000} {
		if err := r.Init(p, res); err != nil {
			t.Fatal(err)
		}
		for r.NextHeader() {
			if err := r.ReadBlock(b, r.Header()); err != nil {
				t.Fatal(err)
			}
			want := blocks[i]
			if !reflect.DeepEqual(b.timestamps, want.timestamps) || b.tsid != want.tsid || b.resolution != res {
				t.Fatalf("时间戳或标识往返失败: %+v", b)
			}
			for col := range b.values {
				for row, v := range b.values[col] {
					expected := want.values[col][row]
					if v != expected && !(math.IsNaN(v) && math.IsNaN(expected)) {
						t.Fatalf("列 %d 行 %d: %v != %v", col, row, v, expected)
					}
				}
			}
			i++
		}
		if err := r.Error(); err != nil {
			t.Fatal(err)
		}
	}
	if i != len(blocks) {
		t.Fatalf("读取 block 数错误 %d", i)
	}
	data, err := os.ReadFile(filepath.Join(path, metaindexFilename))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unmarshalMetaindexRows(nil, bytes.NewReader(data)); err == nil {
		t.Fatal("raw decoder 接受了降采样格式")
	}
}

func TestDownsampleFileConstantAndEmptyResolution(t *testing.T) {
	b := fileTestDownsampleBlock(1, 300000)
	path := writeFileTestDownsamplePart(t, b)
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	if !r.NextHeader() || r.Header().TSID != b.tsid {
		t.Fatalf("丢失目标 TSID 的首个 block: %v", r.Error())
	}
	h, err := r.readFeatureHeader(r.Header(), downsampleFeatureCount)
	if err != nil {
		t.Fatal(err)
	}
	if h.ValuesBlockSize != 0 || h.ValuesMarshalType != encoding.MarshalTypeConst {
		t.Fatalf("常量列未使用零负载: %+v", h)
	}
	var got downsampleDecodedResolutionFeaturesBlock
	if err := r.ReadBlock(&got, r.Header()); err != nil {
		t.Fatal(err)
	}
	if len(got.timestamps) != 2 {
		t.Fatal("未完整读取目标 TSID 的首个 block")
	}
	if r.NextHeader() || r.Error() != nil {
		t.Fatal("已读完的分辨率返回了额外数据")
	}
	if err := r.Init(p, 3600000); err != nil {
		t.Fatal(err)
	}
	if r.NextHeader() || r.Error() != nil {
		t.Fatal("合法零行分辨率读取失败")
	}
}

func TestDownsampleReaderRawInmemory(t *testing.T) {
	mp := getInmemoryPart()
	defer putInmemoryPart(mp)
	rows := []rawRow{{TSID: TSID{MetricID: 1}, Timestamp: minUnixMilli + 1, Value: 2, PrecisionBits: 64}, {TSID: TSID{MetricID: 1}, Timestamp: minUnixMilli + 2, Value: decimal.StaleNaN, PrecisionBits: 64}}
	mp.InitFromRows(rows)
	p := mp.NewPart()
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	if !r.NextHeader() {
		t.Fatal(r.Error())
	}
	var b downsampleDecodedResolutionFeaturesBlock
	if err := r.ReadBlock(&b, r.Header()); err != nil {
		t.Fatal(err)
	}
	if len(b.timestamps) != 2 || b.values[downsampleFeatureCount][0] != 1 || b.values[downsampleFeatureCount][1] != 1 || !decimal.IsStaleNaN(b.values[downsampleFeatureMin][1]) {
		t.Fatalf("原始样本提升错误: %+v", b)
	}
}

func TestDownsampleFileAllConstantPayloads(t *testing.T) {
	b := fileTestDownsampleBlock(1, 300000)
	b.timestamps = b.timestamps[:1]
	for i := range b.values {
		b.values[i] = b.values[i][:1]
	}
	path := writeFileTestDownsamplePart(t, b)
	for _, name := range []string{timestampsFilename, valuesFilename} {
		st, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() != 0 {
			t.Fatalf("常量文件 %s 不为空", name)
		}
	}
	if isDS, err := detectDownsampleFormat(path); err != nil || !isDS {
		t.Fatalf("空负载格式识别失败: %v", err)
	}
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	if !r.NextHeader() {
		t.Fatal(r.Error())
	}
	var got downsampleDecodedResolutionFeaturesBlock
	if err := r.ReadBlock(&got, r.Header()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.values, b.values) {
		t.Fatal("零负载列恢复失败")
	}
}

func TestDownsampleReaderReadErrors(t *testing.T) {
	path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	if !r.NextHeader() {
		t.Fatal(r.Error())
	}
	var got downsampleDecodedResolutionFeaturesBlock
	for _, header := range []*blockHeader{nil, {}} {
		if err := r.ReadBlock(&got, header); err == nil {
			t.Fatal("缺失的缓存 header 未报告读取错误")
		}
	}
	if err := os.Truncate(filepath.Join(path, valuesFilename), 0); err != nil {
		t.Fatal(err)
	}
	if err := r.ReadBlock(&got, r.Header()); err == nil {
		t.Fatal("截断数据未报告读取错误")
	}
}

func TestDownsampleCodecRowCountAndDecodeLimit(t *testing.T) {
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	for _, mt := range []encoding.MarshalType{encoding.MarshalTypeNearestDelta2, encoding.MarshalTypeZSTDNearestDelta2} {
		if _, _, err := r.prepareBlockPayload(nil, mt, 1); err == nil {
			t.Fatal("单行二阶差分未被拒绝")
		}
	}
	payload := encoding.CompressZSTDLevel(nil, bytes.Repeat([]byte{0}, 1000), 1)
	if _, _, err := r.prepareBlockPayload(payload, encoding.MarshalTypeZSTDNearestDelta, 2); err == nil {
		t.Fatal("列解压超过行数上限未被拒绝")
	}
	if err := checkDownsampleExtent(math.MaxUint64, 1, 10); err == nil {
		t.Fatal("偏移溢出未拒绝")
	}
}

func TestDownsampleReaderLargeRawBlock(t *testing.T) {
	mp := getInmemoryPart()
	defer putInmemoryPart(mp)
	ts := make([]int64, downsampleMaxRawRows)
	vs := make([]int64, len(ts))
	for i := range ts {
		ts[i] = minUnixMilli + int64(i)
		vs[i] = 2
	}
	var raw Block
	raw.Init(&TSID{MetricID: 1}, ts, vs, 0, 64)
	var bw blockStreamWriter
	bw.MustInitFromInmemoryPart(mp, 1)
	var merged uint64
	mp.ph.Reset()
	bw.WriteExternalBlock(&raw, &mp.ph, &merged)
	bw.MustClose()
	p := mp.NewPart()
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	if !r.NextHeader() {
		t.Fatal(r.Error())
	}
	var b downsampleDecodedResolutionFeaturesBlock
	if err := r.ReadBlock(&b, r.Header()); err != nil {
		t.Fatal(err)
	}
	if len(b.timestamps) != downsampleMaxRawRows || b.values[downsampleFeatureCount][len(ts)-1] != 1 {
		t.Fatal("合法原始双倍行数 block 未完整读取")
	}
}

func TestDownsampleReaderResolutionAndSharedTSID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1, downsampleTestConfig(t)); err != nil {
		t.Fatal(err)
	}
	w.maxIndexBlockSize = 2 * marshaledBlockHeaderSize
	ids := []uint64{1, 10, 10, 10, 20, 30}
	buckets := []int64{30, 1, 2, 3, 1, 1}
	for _, res := range []int64{300000, 3600000} {
		for i, id := range ids {
			b := fileTestDownsampleBlock(id, res)
			b.timestamps = []int64{minUnixMilli + buckets[i]*res + 1}
			for col := range b.values {
				b.values[col] = b.values[col][:1]
			}
			if err := writeDownsampleTestBlock(w, b); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	for resIdx, res := range []int64{300000, 3600000} {
		if err := r.Init(p, res); err != nil {
			t.Fatal(err)
		}
		if r.currentResolutionFeatureIndexes[downsampleFeatureLast].nextMetaindexRow != resIdx*15 || r.currentResolutionFeatureIndexes[downsampleFeatureLast].metaindexRowsEnd != resIdx*15+3 {
			t.Fatalf("分辨率索引范围错误: %d..%d", r.currentResolutionFeatureIndexes[downsampleFeatureLast].nextMetaindexRow, r.currentResolutionFeatureIndexes[downsampleFeatureLast].metaindexRowsEnd)
		}
		count := 0
		var decoded downsampleDecodedResolutionFeaturesBlock
		for hasHeader := r.NextHeader(); hasHeader; {
			currentTSID := r.Header().TSID
			r.currentTSIDBlockHeaders = r.currentTSIDBlockHeaders[:0]
			for hasHeader && r.Header().TSID == currentTSID {
				r.currentTSIDBlockHeaders = append(r.currentTSIDBlockHeaders, *r.Header())
				hasHeader = r.NextHeader()
			}
			// 主游标已经推进到下一 TSID 或 EOF；直接使用缓存 header，不能重新定位或改变主游标。
			nextHeader := *r.Header()
			for i := range r.currentTSIDBlockHeaders {
				currentBlockHeader := &r.currentTSIDBlockHeaders[i]
				if count >= len(ids) || currentBlockHeader.TSID.MetricID != ids[count] {
					t.Fatalf("跨 index 的 TSID 顺序错误: block=%d header=%+v", count, currentBlockHeader)
				}
				if err := r.ReadBlock(&decoded, currentBlockHeader); err != nil {
					t.Fatal(err)
				}
				if *r.Header() != nextHeader || decoded.tsid != currentTSID {
					t.Fatalf("缓存 header 解码改变了主游标或读取了其他 TSID: block=%d header=%+v", count, r.Header())
				}
				if len(decoded.timestamps) != 1 || decoded.timestamps[0] != minUnixMilli+buckets[count]*res+1 {
					t.Fatalf("跨 index 的 TSID 数据错误: block=%d timestamps=%v", count, decoded.timestamps)
				}
				for feature, values := range decoded.values {
					if len(values) != 1 || values[0] != float64(feature+1) {
						t.Fatalf("缓存 header 的特征对齐错误: block=%d feature=%d values=%v", count, feature, values)
					}
				}
				count++
			}
			r.currentTSIDBlockHeaders = r.currentTSIDBlockHeaders[:0]
		}
		if err := r.Error(); err != nil {
			t.Fatal(err)
		}
		if count != len(ids) {
			t.Fatalf("跨 index 的相同 TSID 丢失: %d", count)
		}
	}
}

func TestDownsampleReaderRawEqualBoundaryAndReuse(t *testing.T) {
	mp := getInmemoryPart()
	defer putInmemoryPart(mp)
	var bw blockStreamWriter
	bw.MustInitFromInmemoryPart(mp, 1)
	mp.ph.Reset()
	ids := []uint64{1, 10, 10, 10, 20, 30}
	times := [][2]int64{{1, 2}, {10, 100}, {20, 80}, {30, 60}, {1, 2}, {1, 2}}
	var merged uint64
	for i, id := range ids {
		var b Block
		b.Init(&TSID{MetricID: id}, []int64{minUnixMilli + times[i][0], minUnixMilli + times[i][1]}, []int64{2, 3}, 0, 64)
		bw.WriteExternalBlock(&b, &mp.ph, &merged)
		if i%2 == 1 {
			bw.flushIndexData()
		}
	}
	bw.MustClose()
	path := filepath.Join(t.TempDir(), "raw")
	mp.MustStoreToDisk(path)
	p := mustOpenFilePart(path)
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	timestampsReader, valuesReader, indexReader := r.timestampsReader, r.valuesReader, r.indexReader
	for _, resolution := range downsampleResolutions {
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		if r.timestampsReader != timestampsReader || r.valuesReader != valuesReader || r.indexReader != indexReader {
			t.Fatal("同一原始源重复 Init 重开了文件")
		}
		count := 0
		var decoded downsampleDecodedResolutionFeaturesBlock
		for hasHeader := r.NextHeader(); hasHeader; {
			if count >= len(ids) || r.Header().TSID.MetricID != ids[count] {
				t.Fatalf("原始源跨 index 的 TSID 顺序错误: block=%d header=%+v", count, r.Header())
			}
			currentBlockHeader := *r.Header()
			hasHeader = r.NextHeader()
			nextHeader := *r.Header()
			if err := r.ReadBlock(&decoded, &currentBlockHeader); err != nil {
				t.Fatal(err)
			}
			if *r.Header() != nextHeader || decoded.tsid != currentBlockHeader.TSID {
				t.Fatal("原始 block 按缓存 header 解码改变了主游标或 TSID")
			}
			want := []int64{minUnixMilli + times[count][0], minUnixMilli + times[count][1]}
			if !reflect.DeepEqual(decoded.timestamps, want) {
				t.Fatalf("原始源重叠 block 被遗漏或改写: block=%d got=%v want=%v", count, decoded.timestamps, want)
			}
			count++
		}
		if count != len(ids) || r.Error() != nil {
			t.Fatalf("原始源跨 index 读取不完整: %d %v", count, r.Error())
		}
	}
}

func TestDownsampleIterationReaderReuse(t *testing.T) {
	p := newDownsampleIterationPart(t)
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	// 每轮 Init 从当前分辨率起点顺序扫描，单次解码同时验证五个特征。
	for pass, resolution := range []int64{300000, 3600000, 300000} {
		t.Run(fmt.Sprintf("顺序复用/%d", pass), func(t *testing.T) {
			checkDownsampleIterationReader(t, r, p, resolution)
		})
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.currentSourcePart != nil || r.NextHeader() {
		t.Fatal("Close 后仍保留 part 或可读取结果")
	}
	checkDownsampleIterationReader(t, r, p, downsampleResolution5m)
}

func TestDownsampleReaderCrossIndexOffsets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		shift   int64
	}{
		{name: "continuous"},
		{name: "timestamp_gap", payload: "timestamp", shift: 1},
		{name: "timestamp_overlap", payload: "timestamp", shift: -1},
		{name: "values_gap", payload: "values", shift: 1},
		{name: "values_overlap", payload: "values", shift: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeDownsampleCrossIndexPart(t)
			if tc.payload != "" {
				// 改动中间 index 的完整 block 偏移，保持列间连续及文件边界合法。
				// 若只校验单个 index 内部，四种损坏均不会在读取 header 时报错。
				rewriteDownsampleCrossIndexFile(t, path, 1, func(data []byte) {
					pos := 56 // 原生 blockHeader.TimestampsBlockOffset
					if tc.payload == "values" {
						pos = 64 // 原生 blockHeader.ValuesBlockOffset
					}
					offset := binary.BigEndian.Uint64(data[pos : pos+8])
					binary.BigEndian.PutUint64(data[pos:pos+8], uint64(int64(offset)+tc.shift))
				})
			}
			if tc.payload != "" {
				assertDownsampleLayoutOpenRejected(t, path, "payloads are not contiguous")
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
			if tc.payload == "" {
				if count != 4 || r.Error() != nil {
					t.Fatalf("正常跨 index 扫描失败: count=%d, err=%v", count, r.Error())
				}
			} else if count != 1 || r.Error() == nil || !strings.Contains(r.Error().Error(), "payloads are not contiguous across adjacent index blocks") {
				t.Fatalf("跨 index 偏移损坏未被结构校验拒绝: count=%d, err=%v", count, r.Error())
			}
		})
	}
}

func TestDownsampleReaderCrossIndexInitAndReset(t *testing.T) {
	path := writeDownsampleCrossIndexPart(t)
	p, err := openDownsamplePart(path, nil)
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
		if len(r.currentResolutionFeatureIndexes[downsampleFeatureLast].currentIndexBlockHeaders) == 0 {
			t.Fatal("扫描后没有保存 index 边界")
		}
		// 重新 Init 必须清除上一轮的跨 index 校验状态。
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		checkDownsampleCrossIndexStateCleared(t, r)
		checkDownsampleCrossIndexRows(t, r, []uint64{1, 2, 3, 4})
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	checkDownsampleCrossIndexStateCleared(t, r)
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
			assertDownsampleLayoutOpenRejected(t, path, "index block has an invalid tenant or TSID range")
			p := openDownsampleLayoutReaderFixture(t, path)
			r := getDownsampleReader()
			defer putDownsampleReader(r)
			if err := r.Init(p, 300000); err != nil {
				t.Fatal(err)
			}
			if !r.NextHeader() || r.NextHeader() || r.Error() == nil || !strings.Contains(r.Error().Error(), "index block has an invalid tenant or TSID range") {
				t.Fatalf("未拒绝 index 内的跨租户 header: %v", r.Error())
			}
		})
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
			if err := w.Init(path, 1, downsampleTestConfig(t)); err != nil {
				t.Fatal(err)
			}
			if separateIndexes {
				w.maxIndexBlockSize = clusterDownsampleHeaderBytes
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
			if _, err := w.Finish(nil); err != nil {
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
				if err != nil || len(data) != wantHeaders*clusterDownsampleHeaderBytes {
					t.Fatalf("重复键 fixture 的 index 长度错误: %d, err=%v", len(data), err)
				}
				if !separateIndexes || pos/clusterDownsampleMetaindexBytes%2 == 1 {
					// 每个 feature 只替换第二个批次的 MinTimestamp，保留负载和其他统计。
					batchOffset := clusterDownsampleHeaderBytes
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
			assertDownsampleLayoutOpenRejected(t, path, "out of order")
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
			if count != wantCount || r.Error() == nil || !strings.Contains(r.Error().Error(), "out of order") {
				t.Fatalf("重复批次键未在读取 header 时被拒绝: count=%d, want=%d, error=%v", count, wantCount, r.Error())
			}
		})
	}
}

func TestDownsampleReaderRawBlockOrder(t *testing.T) {
	for _, separateIndexes := range []bool{false, true} {
		for _, tc := range []struct {
			name         string
			secondMin    int64
			secondMax    int64
			wantRejected bool
		}{
			{name: "equal_bounds", secondMin: 10, secondMax: 30},
			{name: "overlapping_bounds", secondMin: 20, secondMax: 40},
			{name: "decreasing_minimum", secondMin: 9, secondMax: 40, wantRejected: true},
		} {
			t.Run(fmt.Sprintf("%s/separate_indexes=%t", tc.name, separateIndexes), func(t *testing.T) {
				mp := getInmemoryPart()
				defer putInmemoryPart(mp)
				var writer blockStreamWriter
				writer.MustInitFromInmemoryPart(mp, 1)
				mp.ph.Reset()
				var merged uint64
				for i, bounds := range [][2]int64{{10, 30}, {tc.secondMin, tc.secondMax}} {
					var b Block
					b.Init(&TSID{MetricID: 1}, []int64{minUnixMilli + bounds[0], minUnixMilli + bounds[1]}, []int64{int64(i + 10), int64(i + 20)}, 0, 64)
					writer.WriteExternalBlock(&b, &mp.ph, &merged)
					if separateIndexes {
						writer.flushIndexData()
					}
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
					if err := r.ReadBlock(&batch, r.Header()); err != nil {
						t.Fatal(err)
					}
					if len(batch.timestamps) != 2 || batch.values[downsampleFeatureLast][0] != float64(count+10) {
						t.Fatal("raw block samples were not retained")
					}
					count++
				}
				if tc.wantRejected {
					if count != 1 || r.Error() == nil || !strings.Contains(r.Error().Error(), "block headers are out of order") {
						t.Fatalf("decreasing raw timestamps were not rejected: count=%d, error=%v", count, r.Error())
					}
				} else if count != 2 || r.Error() != nil {
					t.Fatalf("valid raw boundaries were rejected: count=%d, error=%v", count, r.Error())
				}
				readErr := r.Error()
				if r.Header().RowsCount != 0 || r.NextHeader() || r.Error() != readErr {
					t.Fatal("stopped reader retained a header or changed its error")
				}
			})
		}
	}
}

func TestDownsampleReaderCloseFiles(t *testing.T) {
	r := newDownsampleCloseTestReader(t)
	files := []filestream.ReadAtCloser{r.timestampsReader, r.valuesReader, r.indexReader}
	failedFiles := []filestream.ReadAtCloser{r.timestampsReader, r.indexReader}
	for _, f := range failedFiles {
		f.(*downsampleCloseTestFile).closeErr = os.ErrClosed
	}
	err := r.Close()
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("lost actual file close error: %v", err)
	}
	for _, f := range failedFiles {
		if !strings.Contains(err.Error(), "[downsampling] cannot close reader file \""+f.Path()+"\"") {
			t.Fatalf("lost prefixed close error for file %q: %v", f.Path(), err)
		}
	}
	assertDownsampleFilesClosed(t, files)
	if r.currentSourcePart != nil || r.timestampsReader != nil || r.valuesReader != nil || r.indexReader != nil {
		t.Fatal("Close retained part or file handles")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close attempted the same files again: %v", err)
	}
	assertDownsampleFilesClosed(t, files)
}

func TestDownsampleReaderFilesIndependentFromQueries(t *testing.T) {
	path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, downsampleResolution5m))
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	var r downsampleReader
	defer r.Close()
	if err := r.Init(p, downsampleResolution5m); err != nil {
		t.Fatal(err)
	}
	var files []filestream.ReadAtCloser
	for _, reader := range []*filestream.ReadAtCloser{&r.timestampsReader, &r.valuesReader, &r.indexReader} {
		*reader = &downsampleCloseTestFile{ReadAtCloser: *reader}
		files = append(files, *reader)
	}
	if !r.NextHeader() {
		t.Fatalf("missing header: %v", r.Error())
	}
	if _, err := r.readFeatureHeader(r.Header(), downsampleFeatureSum); err != nil {
		t.Fatal(err)
	}
	// 同一源重新 Init 仅重置顺序扫描状态，复用本次 merge 自己打开的文件。
	if err := r.Init(p, downsampleResolution5m); err != nil {
		t.Fatal(err)
	}
	if !r.NextHeader() {
		t.Fatalf("cannot restart merge source: %v", r.Error())
	}
	for _, f := range files {
		if f.(*downsampleCloseTestFile).closes != 0 {
			t.Fatalf("reader Init closed merge file %q", f.Path())
		}
	}
	var decoded downsampleDecodedResolutionFeaturesBlock
	if err := r.ReadBlock(&decoded, r.Header()); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	assertDownsampleFilesClosed(t, files)
	for _, f := range []fs.MustReadAtCloser{p.timestampsFile, p.valuesFile, p.indexFile} {
		if _, ok := f.(*fs.ReaderAt); !ok {
			t.Fatalf("query file %q has reader type %T; want *fs.ReaderAt", f.Path(), f)
		}
		want, err := os.ReadFile(f.Path())
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		f.MustReadAt(got, 0)
		if !bytes.Equal(got, want) {
			t.Fatalf("merge reader cleanup affected query file %q", f.Path())
		}
	}

}

func TestDownsampleReaderInitFailureReleasesPartialSource(t *testing.T) {
	path := t.TempDir()
	timestampsPath := filepath.Join(path, timestampsFilename)
	if err := os.WriteFile(timestampsPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	p := &part{path: path}
	var r downsampleReader
	defer r.Close()
	err := r.Init(p, downsampleResolution5m)
	if !errors.Is(err, os.ErrNotExist) || !strings.HasPrefix(err.Error(), "[downsampling] cannot open reader file ") {
		t.Fatalf("Init lost the missing values file error: %v", err)
	}
	if r.currentSourcePart != nil || r.timestampsReader != nil || r.valuesReader != nil || r.indexReader != nil {
		t.Fatal("failed Init retained partial source ownership")
	}
	// The same reader can initialize a complete source after the failed attempt.
	for _, name := range []string{valuesFilename, indexFilename} {
		if err := os.WriteFile(filepath.Join(path, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Init(p, downsampleResolution1h); err != nil {
		t.Fatal(err)
	}
	files := []filestream.ReadAtCloser{r.timestampsReader, r.valuesReader, r.indexReader}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	assertDownsampleFilesClosed(t, files)
	if r.currentResolution != 0 {
		t.Fatal("Close retained source selection state")
	}
}

func TestDownsampleReaderInitCloseFailure(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		name := "switch_source"
		if invalid {
			name = "invalid_source"
		}
		t.Run(name, func(t *testing.T) {
			r := newDownsampleCloseTestReader(t)
			files := []filestream.ReadAtCloser{r.timestampsReader, r.valuesReader, r.indexReader}
			r.timestampsReader.(*downsampleCloseTestFile).closeErr = os.ErrClosed
			next := &part{path: filepath.Join(t.TempDir(), "must-not-open")}
			if invalid {
				next = nil
			}
			err := r.Init(next, downsampleResolution5m)
			if !errors.Is(err, os.ErrClosed) {
				t.Fatalf("Init lost previous source close error: %v", err)
			}
			if invalid && !strings.Contains(err.Error(), "[downsampling] invalid reader source") {
				t.Fatalf("Init lost validation error: %v", err)
			}
			if errors.Is(err, os.ErrNotExist) || r.currentSourcePart != nil {
				t.Fatalf("Init continued opening the next source after close failed: %v", err)
			}
			assertDownsampleFilesClosed(t, files)
		})
	}
	t.Run("same_source_invalid_feature", func(t *testing.T) {
		r := newDownsampleCloseTestReader(t)
		p := r.currentSourcePart
		p.dsMetadata = &downsamplePartMetadata{}
		p.dsMetaindex = []downsampleMetaindexRow{{ResolutionMs: downsampleResolution1h, feature: countOfDownsampleFeatures}}
		if err := r.Init(p, downsampleResolution5m); err != nil {
			t.Fatalf("cannot initialize first resolution: %v", err)
		}
		r.currentTSIDBlockHeaders = []blockHeader{{RowsCount: 1}}
		files := []filestream.ReadAtCloser{r.timestampsReader, r.valuesReader, r.indexReader}
		r.valuesReader.(*downsampleCloseTestFile).closeErr = os.ErrClosed
		err := r.Init(p, downsampleResolution1h)
		if err == nil || !strings.Contains(err.Error(), "[downsampling] invalid feature") || !errors.Is(err, os.ErrClosed) {
			t.Fatalf("Init lost feature validation or cleanup error: %v", err)
		}
		if r.currentSourcePart != nil || r.timestampsReader != nil || r.valuesReader != nil || r.indexReader != nil || cap(r.currentTSIDBlockHeaders) != 0 {
			t.Fatal("failed same-source Init retained file ownership or TSID headers")
		}
		assertDownsampleFilesClosed(t, files)
		if err := r.Close(); err != nil {
			t.Fatalf("Close attempted cleanup twice: %v", err)
		}
		assertDownsampleFilesClosed(t, files)
	})
}

func TestDownsampleReaderCancellation(t *testing.T) {
	input := sharedTimestampsTestBlock(10, downsampleResolution5m, minUnixMilli+1, 4, 64)
	path := writeFileTestDownsamplePart(t, input)
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	var r downsampleReader
	defer r.Close()
	if err := r.Init(p, input.resolution); err != nil || !r.NextHeader() {
		t.Fatalf("cannot initialize reader: %v / %v", err, r.Error())
	}
	header := *r.Header()
	stopCh := make(chan struct{})
	r.stopCh = stopCh
	close(stopCh)
	if _, err := r.readFeatureHeader(&header, downsampleFeatureSum); !errors.Is(err, errForciblyStopped) {
		t.Fatalf("feature scan ignored cancellation: %v", err)
	}
	if r.NextHeader() || !errors.Is(r.Error(), errForciblyStopped) {
		t.Fatalf("index scan ignored cancellation: %v", r.Error())
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.stopCh != nil {
		t.Fatal("reader retained previous merge cancellation")
	}
}

// 首列之后关闭时间戳文件，使任何重复读取立即失败；原生 Block 中仅保留已解码时间戳。
func TestDownsampleReaderSharedTimestampsValuesOnly(t *testing.T) {
	for _, precision := range []uint8{64, 8} {
		t.Run(fmt.Sprintf("precision_%d", precision), func(t *testing.T) {
			input := sharedTimestampsTestBlock(10, downsampleResolution5m, minUnixMilli+1, 512, precision)
			path := writeFileTestDownsamplePart(t, input)
			p, err := openDownsamplePart(path, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			var r downsampleReader
			defer r.Close()
			if err := r.Init(p, input.resolution); err != nil || !r.NextHeader() {
				t.Fatalf("cannot position reader: %v / %v", err, r.Error())
			}
			shared := *r.Header()
			if shared.TimestampsBlockSize == 0 || shared.TimestampsMarshalType == encoding.MarshalTypeConst {
				t.Fatal("fixture must require timestamp payload decoding")
			}
			var columns [countOfDownsampleFeatures]blockHeader
			var expected [countOfDownsampleFeatures]Block
			for feature := range columns {
				columns[feature], err = r.readFeatureHeader(r.Header(), uint8(feature))
				if err != nil {
					t.Fatal(err)
				}
				if err := readDownsampleFeatureBlockForTest(&r, &expected[feature], uint8(feature)); err != nil {
					t.Fatal(err)
				}
			}
			// 主 reader 自有句柄，关闭后不影响 part 查询和其它 reader。
			f := r.timestampsReader
			var got Block
			if err := readDownsampleFeatureBlockForTest(&r, &got, downsampleFeatureLast); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if len(got.timestampsData) != 0 {
				t.Fatal("native Block did not finish timestamp decoding")
			}
			firstTimestamp := &got.timestamps[0]
			for feature := 1; feature < countOfDownsampleFeatures; feature++ {
				if err := r.readNativeValues(&got, &columns[feature], &shared); err != nil {
					t.Fatalf("feature %d tried to read or decode timestamps again: %v", feature, err)
				}
				want := &expected[feature]
				if &got.timestamps[0] != firstTimestamp || !reflect.DeepEqual(got.timestamps, want.timestamps) || !reflect.DeepEqual(got.values, want.values) {
					t.Fatalf("feature %d did not reuse timestamps with the correct native values", feature)
				}
				if got.bh.Scale != want.bh.Scale || got.bh.PrecisionBits != precision || got.bh.TimestampsMarshalType != want.bh.TimestampsMarshalType || got.bh.TimestampsBlockSize != want.bh.TimestampsBlockSize || len(got.timestampsData) != 0 || len(got.valuesData) != 0 || got.nextIdx != 0 {
					t.Fatalf("feature %d lost the native decoded Block state", feature)
				}
			}
			// 查询的单列读取仍完整读取时间戳，不借用另一次操作留下的缓存。
			if err := readDownsampleFeatureBlockForTest(&r, &got, downsampleFeatureMax); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("single-feature reference read unexpectedly reused timestamps: %v", err)
			}
			// 下一次多特征读取也必须重新读取首列，不能跨 ReadBlock 缓存。
			var next downsampleDecodedResolutionFeaturesBlock
			if err := r.ReadBlock(&next, r.Header()); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("ReadBlock unexpectedly reused a previous block's timestamps: %v", err)
			}
		})
	}
}

func TestDownsampleReaderSharedTimestampsSwitches(t *testing.T) {
	var parts []*part
	for source, precision := range []uint8{64, 8} {
		base := minUnixMilli + 1 + int64(source)*5*24*3600000
		var inputs []*downsampleDecodedResolutionFeaturesBlock
		for _, resolution := range downsampleResolutions {
			inputs = append(inputs,
				sharedTimestampsTestBlock(1, resolution, base, 19, precision),
				sharedTimestampsTestBlock(1, resolution, base+25*resolution, 7, precision),
				sharedTimestampsTestBlock(2, resolution, base+3*resolution, 1, precision),
			)
		}
		p, err := openDownsamplePart(writeFileTestDownsamplePart(t, inputs...), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer p.MustClose()
		parts = append(parts, p)
	}
	var r, reference downsampleReader
	defer r.Close()
	defer reference.Close()
	var got downsampleDecodedResolutionFeaturesBlock
	// 同源切换分辨率、跨源切换，最后重新读取第一个源，均复用同一组工作缓冲。
	for _, step := range []struct {
		source     int
		resolution int64
	}{{0, downsampleResolution5m}, {0, downsampleResolution1h}, {1, downsampleResolution5m}, {1, downsampleResolution1h}, {0, downsampleResolution5m}} {
		p := parts[step.source]
		if err := r.Init(p, step.resolution); err != nil {
			t.Fatal(err)
		}
		if err := reference.Init(p, step.resolution); err != nil {
			t.Fatal(err)
		}
		blocks := 0
		for r.NextHeader() {
			if !reference.NextHeader() {
				t.Fatalf("reference reader lost block: %v", reference.Error())
			}
			if err := r.ReadBlock(&got, r.Header()); err != nil {
				t.Fatal(err)
			}
			h := reference.Header()
			if got.tsid != h.TSID || got.resolution != step.resolution || got.precisionBits != h.PrecisionBits {
				t.Fatal("block identity was reused across TSID, source or resolution")
			}
			for feature := range got.values {
				var want Block
				if err := readDownsampleFeatureBlockForTest(&reference, &want, uint8(feature)); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got.timestamps, want.timestamps) {
					t.Fatalf("block %d feature %d reused different timestamps", blocks, feature)
				}
				values := decimal.AppendDecimalToFloat(nil, want.values, want.bh.Scale)
				if len(got.values[feature]) != len(values) {
					t.Fatal("decoded values count changed")
				}
				for row, v := range values {
					if actual := got.values[feature][row]; actual != v && !(math.IsNaN(actual) && math.IsNaN(v)) {
						t.Fatalf("block %d feature %d row %d: got %v; want %v", blocks, feature, row, actual, v)
					}
				}
			}
			blocks++
		}
		if err := r.Error(); err != nil {
			t.Fatal(err)
		}
		if reference.NextHeader() || reference.Error() != nil || blocks != 3 {
			t.Fatalf("lost blocks after switching: got %d; reference reader error %v", blocks, reference.Error())
		}
	}
}

func TestDownsampleReaderSharedTimestampsValidation(t *testing.T) {
	input := sharedTimestampsTestBlock(1, downsampleResolution5m, minUnixMilli+1, 8, 64)
	p, err := openDownsamplePart(writeFileTestDownsamplePart(t, input), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	var r downsampleReader
	defer r.Close()
	if err := r.Init(p, input.resolution); err != nil || !r.NextHeader() {
		t.Fatalf("cannot position reader: %v / %v", err, r.Error())
	}
	shared := *r.Header()
	column, err := r.readFeatureHeader(r.Header(), downsampleFeatureSum)
	if err != nil {
		t.Fatal(err)
	}
	assertInvalidValues := func(t *testing.T, h *blockHeader) {
		t.Helper()
		var b Block
		if err := readDownsampleFeatureBlockForTest(&r, &b, downsampleFeatureLast); err != nil {
			t.Fatal(err)
		}
		if err := r.readNativeValues(&b, h, &shared); err == nil {
			t.Fatal("values-only path accepted an invalid header or payload")
		}
	}
	sharedMutations := map[string]func(*blockHeader){
		"timestamp_offset": func(h *blockHeader) { h.TimestampsBlockOffset++ },
		"timestamp_size":   func(h *blockHeader) { h.TimestampsBlockSize++ },
		"timestamp_codec":  func(h *blockHeader) { h.TimestampsMarshalType = encoding.MarshalTypeConst },
		"timestamp_minimum": func(h *blockHeader) {
			h.MinTimestamp++
		},
		"timestamp_maximum": func(h *blockHeader) { h.MaxTimestamp++ },
		"rows":              func(h *blockHeader) { h.RowsCount++ },
		"tsid":              func(h *blockHeader) { h.TSID.MetricID++ },
		"account_id":        func(h *blockHeader) { h.TSID.AccountID++ },
		"project_id":        func(h *blockHeader) { h.TSID.ProjectID++ },
		"precision":         func(h *blockHeader) { h.PrecisionBits = 8 },
	}
	for name, mutate := range sharedMutations {
		t.Run(name, func(t *testing.T) {
			h := column
			mutate(&h)
			assertInvalidValues(t, &h)
			// readFeatureHeader validates the current feature cursor before decoding values.
			cursor := &r.currentResolutionFeatureIndexes[downsampleFeatureSum]
			cursor.currentBlockHeader = h
			defer func() { cursor.currentBlockHeader = column }()
			if _, err := r.readFeatureHeader(r.Header(), downsampleFeatureSum); err == nil {
				t.Fatal("readFeatureHeader accepted a cursor with different shared timestamps")
			}
		})
	}
	cases := map[string]func(*blockHeader){
		"invalid_precision": func(h *blockHeader) {
			h.PrecisionBits = 0
		},
		"values_codec": func(h *blockHeader) { h.ValuesMarshalType = 255 },
		"values_extent": func(h *blockHeader) {
			h.ValuesBlockOffset = r.valuesFileSize
			h.ValuesBlockSize = 1
		},
		"values_payload": func(h *blockHeader) {
			h.ValuesMarshalType = encoding.MarshalTypeNearestDelta
			h.ValuesBlockSize = 0
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			h := column
			mutate(&h)
			assertInvalidValues(t, &h)
		})
	}
	// 完整多特征读取仍检查每一列的共享描述，而不是只检查第一列。
	r.currentResolutionFeatureIndexes[downsampleFeatureSum].currentBlockHeader.TimestampsBlockOffset++
	var got downsampleDecodedResolutionFeaturesBlock
	if err := r.ReadBlock(&got, r.Header()); err == nil {
		t.Fatal("ReadBlock accepted a later feature with different shared timestamps")
	}
}

func TestDownsampleClusterTenantIndexBoundaries(t *testing.T) {
	batches := makeDownsampleLayoutBlocks(330, false)
	path := writeFileTestDownsamplePart(t, batches...)
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	if len(p.dsMetaindex) != 50 {
		t.Fatalf("未覆盖两个分辨率、五列各五个租户 index: %d", len(p.dsMetaindex))
	}
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	var got downsampleDecodedResolutionFeaturesBlock
	for resolutionIndex, resolution := range downsampleResolutions {
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		// 顺序读取全部租户，覆盖 account/project 与 index 边界，检查同一 TSID 的两个 block 均完整返回。
		count := 0
		for r.NextHeader() {
			if count >= 330 {
				t.Fatal("租户顺序扫描返回了额外 block")
			}
			want := batches[resolutionIndex*330+count]
			if err := r.ReadBlock(&got, r.Header()); err != nil {
				t.Fatal(err)
			}
			if got.tsid != want.tsid || got.resolution != want.resolution || !reflect.DeepEqual(got.timestamps, want.timestamps) || !reflect.DeepEqual(got.values, want.values) {
				t.Fatalf("租户跨 index 顺序或数据错误: resolution=%d block=%d", resolution, count)
			}
			count++
		}
		if count != 330 || r.Error() != nil {
			t.Fatalf("租户跨 index 读取不完整: resolution=%d count=%d error=%v", resolution, count, r.Error())
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
						if err := r.ReadBlock(block, r.Header()); err != nil {
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
