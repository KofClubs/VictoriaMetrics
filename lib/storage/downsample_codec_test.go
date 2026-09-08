package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func fileTestDownsampleBlock(tsid uint64, resolution int64) *downsampleBatch {
	b := &downsampleBatch{tsid: TSID{MetricID: tsid}, resolution: resolution, timestamps: []int64{minUnixMilli + 1, minUnixMilli + resolution + 1}, timestampPrecisionBits: 64}
	for i := range b.values {
		b.values[i] = []float64{float64(i + 1), float64(i + 6)}
		b.precisionBits[i] = 64
	}
	b.values[downsampleFeatureCount] = []float64{3, 3}
	return b
}

func writeFileTestDownsamplePart(t *testing.T, blocks ...*downsampleBatch) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	for _, b := range blocks {
		if err := w.WriteBlock(b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDownsampleFileRoundtrip(t *testing.T) {
	blocks := []*downsampleBatch{fileTestDownsampleBlock(1, 300000), fileTestDownsampleBlock(2, 300000), fileTestDownsampleBlock(1, 3600000)}
	blocks[1].values[downsampleFeatureSum][0] = decimal.StaleNaN
	path := writeFileTestDownsamplePart(t, blocks...)
	if ds, err := detectDownsampleFormat(path); err != nil || !ds {
		t.Fatalf("格式识别: %v %v", ds, err)
	}
	p, err := openDownsamplePart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	if p.ph.RowsCount != 30 || p.ph.BlocksCount != 15 || len(p.dsMetaindex) != 2 {
		t.Fatalf("错误 part 统计: %+v", p.ph)
	}
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	b := getDownsampleBatch()
	defer putDownsampleBatch(b)
	i := 0
	for _, res := range []int64{300000, 3600000} {
		if err := r.Init(p, res); err != nil {
			t.Fatal(err)
		}
		for r.NextHeader() {
			if err := r.ReadBlock(b); err != nil {
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
		t.Fatal("旧 decoder 接受了 v3")
	}
}

func TestDownsampleFileConstantAndFilter(t *testing.T) {
	b := fileTestDownsampleBlock(1, 300000)
	path := writeFileTestDownsamplePart(t, b)
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
	r.SetFilter(&b.tsid, b.timestamps[1], b.timestamps[1])
	if !r.NextHeader() {
		t.Fatalf("丢失相交 block: %v", r.Error())
	}
	h := r.Header()
	if h.Columns[downsampleFeatureCount].Size != 0 || h.Columns[downsampleFeatureCount].MarshalType != encoding.MarshalTypeConst {
		t.Fatalf("常量列未使用零负载: %+v", h.Columns[downsampleFeatureCount])
	}
	var got downsampleBatch
	if err := r.ReadBlock(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.timestamps) != 2 {
		t.Fatal("reader 提前裁剪了 block")
	}
	other := TSID{MetricID: 2}
	r.SetFilter(&other, minUnixMilli, maxUnixMilli)
	if r.NextHeader() || r.Error() != nil {
		t.Fatal("错误 TSID 过滤")
	}
	if err := r.Init(p, 3600000); err != nil {
		t.Fatal(err)
	}
	if r.NextHeader() || r.Error() != nil {
		t.Fatal("合法零行分辨率读取失败")
	}
}

func TestDownsampleHeaderValidation(t *testing.T) {
	path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
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
	if !r.NextHeader() {
		t.Fatal(r.Error())
	}
	base := *r.Header()
	cases := map[string]func(*downsampleBlockHeader){
		"resolution":          func(h *downsampleBlockHeader) { h.ResolutionMs = 1 },
		"rows":                func(h *downsampleBlockHeader) { h.RowsCount = 0 },
		"feature":             func(h *downsampleBlockHeader) { h.Columns[1].Feature = 1 },
		"precision":           func(h *downsampleBlockHeader) { h.Columns[0].PrecisionBits = 0 },
		"type":                func(h *downsampleBlockHeader) { h.Columns[0].MarshalType = 255 },
		"size":                func(h *downsampleBlockHeader) { h.Columns[0].Size = math.MaxUint32 },
		"timestamp_precision": func(h *downsampleBlockHeader) { h.Timestamps.PrecisionBits = 0 },
		"time_domain": func(h *downsampleBlockHeader) {
			h.MinTimestamp = minUnixMilli - 1
			h.Timestamps.FirstValue = h.MinTimestamp
		},
		"column_offset": func(h *downsampleBlockHeader) { h.Columns[1].Offset++ },
		"single_row_delta2": func(h *downsampleBlockHeader) {
			h.RowsCount = 1
			h.Columns[0].MarshalType = encoding.MarshalTypeNearestDelta2
		},
		"single_row_timestamp_delta2": func(h *downsampleBlockHeader) {
			h.RowsCount = 1
			h.Timestamps.MarshalType = encoding.MarshalTypeZSTDNearestDelta2
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			h := base
			mutate(&h)
			var got downsampleBlockHeader
			if err := got.unmarshal(h.marshal(nil)); err == nil {
				t.Fatal("未拒绝非法 header")
			}
		})
	}
	var got downsampleBlockHeader
	if err := got.unmarshal(base.marshal(nil)[:downsampleBlockHeaderSize-1]); err == nil {
		t.Fatal("未拒绝截断 header")
	}
	if err := checkDownsampleExtent(math.MaxUint64, 1, 10); err == nil {
		t.Fatal("偏移溢出未拒绝")
	}
}

func TestDownsampleMetadataValidation(t *testing.T) {
	for _, key := range []string{"FormatVersion", "SemanticsVersion", "Mode", "Resolutions", "BucketOrigin", "NumericCodec", "Retention", "RowsCount", "BlocksCount", "MinTimestamp", "MaxTimestamp", "MinDedupInterval"} {
		t.Run(key, func(t *testing.T) {
			path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
			metaPath := filepath.Join(path, metadataFilename)
			data, err := os.ReadFile(metaPath)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			delete(fields, key)
			data, err = json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(metaPath, data, 0644); err != nil {
				t.Fatal(err)
			}
			if _, err := detectDownsampleFormat(path); err == nil {
				t.Fatal("未拒绝缺少元数据字段")
			}
		})
	}
}

func TestDownsampleWriterOrderAbortAndPool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBlock(fileTestDownsampleBlock(1, 3600000)); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBlock(fileTestDownsampleBlock(1, 300000)); err == nil {
		t.Fatal("未拒绝分辨率逆序")
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("取消未清理目标")
	}
	putDownsampleWriter(w)
	b := getDownsampleBatch()
	b.timestamps = make([]int64, downsampleMaxPooledRows+1)
	b.values[0] = make([]float64, downsampleMaxPooledRows+1)
	b.Reset()
	if b.timestamps != nil || b.values[0] != nil || b.resolution != 0 {
		t.Fatal("block 未释放异常容量")
	}
	putDownsampleBatch(b)
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
	var b downsampleBatch
	if err := r.ReadBlock(&b); err != nil {
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
	if !r.NextHeader() {
		t.Fatal(r.Error())
	}
	var got downsampleBatch
	if err := r.ReadBlock(&got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.values, b.values) {
		t.Fatal("零负载列恢复失败")
	}
}

func TestDownsampleReaderReadErrors(t *testing.T) {
	path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
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
	if !r.NextHeader() {
		t.Fatal(r.Error())
	}
	if err := os.Truncate(filepath.Join(path, valuesFilename), 0); err != nil {
		t.Fatal(err)
	}
	var got downsampleBatch
	if err := r.ReadBlock(&got); err == nil {
		t.Fatal("截断数据未报告读取错误")
	}
}

func TestDownsampleWriterExistingDirectory(t *testing.T) {
	path := t.TempDir()
	sentinel := filepath.Join(path, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err == nil {
		t.Fatal("writer 接受了已有目录")
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatal("取消删除了非本 writer 创建的文件")
	}
}

func TestDownsampleWriterAbortAfterFinish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBlock(fileTestDownsampleBlock(1, 300000)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("Finish 后取消未删除未发布目标")
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
}

func TestDownsampleV3TimeBounds(t *testing.T) {
	for _, timestamp := range []int64{minUnixMilli - 1, maxUnixMilli + 1} {
		m := newDownsamplePartMetadata(partHeader{RowsCount: 5, BlocksCount: 5, MinTimestamp: timestamp, MaxTimestamp: timestamp})
		if err := m.validate(); err == nil {
			t.Fatal("非法 part 时间范围未拒绝")
		}
		mr := downsampleMetaindexRow{ResolutionMs: 300000, TSID: TSID{MetricID: 1}, LastTSID: TSID{MetricID: 1}, MinTimestamp: timestamp, MaxTimestamp: timestamp, BlockHeadersCount: 5, RowsCount: 5, IndexBlockSize: 16}
		var got downsampleMetaindexRow
		if err := got.unmarshal(mr.marshal(nil)); err == nil {
			t.Fatal("非法 metaindex 时间范围未拒绝")
		}
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
	var b downsampleBatch
	if err := r.ReadBlock(&b); err != nil {
		t.Fatal(err)
	}
	if len(b.timestamps) != downsampleMaxRawRows || b.values[downsampleFeatureCount][len(ts)-1] != 1 {
		t.Fatal("合法原始双倍行数 block 未完整读取")
	}
}

func TestDownsamplePoolNormalAndLargeCapacity(t *testing.T) {
	var b downsampleBatch
	for i := 0; i < downsampleMaxRawRows; i++ {
		b.timestamps = append(b.timestamps, int64(i))
		b.values[0] = append(b.values[0], float64(i))
	}
	timestampsCap, valuesCap := cap(b.timestamps), cap(b.values[0])
	b.Reset()
	if cap(b.timestamps) != timestampsCap || cap(b.values[0]) != valuesCap || len(b.timestamps) != 0 || len(b.values[0]) != 0 {
		t.Fatal("合法原始 block 的正常增长容量未保留")
	}
	var w downsampleWriter
	for i := 0; i < maxRowsPerBlock; i++ {
		w.integers = append(w.integers, int64(i))
		w.normalized = append(w.normalized, float64(i))
	}
	intsCap, floatsCap := cap(w.integers), cap(w.normalized)
	w.reset()
	if cap(w.integers) != intsCap || cap(w.normalized) != floatsCap {
		t.Fatal("writer 正常输出容量未保留")
	}
	w.normalized = make([]float64, downsampleMaxPooledRows+1)
	w.integers = make([]int64, downsampleMaxPooledRows+1)
	w.reset()
	if w.normalized != nil || w.integers != nil {
		t.Fatal("writer 异常容量未释放")
	}
}

func TestDownsampleFileMultiIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 400; id++ {
		if err := w.WriteBlock(fileTestDownsampleBlock(id, 300000)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	p, err := openDownsamplePart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	if len(p.dsMetaindex) < 2 {
		t.Fatal("未跨越 index block 上限")
	}
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil {
		t.Fatal(err)
	}
	tsid := TSID{MetricID: 380}
	r.SetFilter(&tsid, minUnixMilli, maxUnixMilli)
	if !r.NextHeader() || r.Header().TSID != tsid {
		t.Fatalf("跨 index block 定位失败: %v", r.Error())
	}
	var b downsampleBatch
	if err := r.ReadBlock(&b); err != nil {
		t.Fatal(err)
	}
	if r.NextHeader() || r.Error() != nil {
		t.Fatal("TSID 过滤遗漏或重复")
	}
}

func TestDownsampleReaderSeekResolutionAndSharedTSID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	ids := []uint64{1, 10, 10, 10, 20, 30}
	buckets := []int64{30, 1, 2, 3, 1, 1}
	for _, res := range []int64{300000, 3600000} {
		for i, id := range ids {
			b := fileTestDownsampleBlock(id, res)
			b.timestamps = []int64{minUnixMilli + buckets[i]*res + 1}
			for col := range b.values {
				b.values[col] = b.values[col][:1]
			}
			if err := w.WriteBlock(b); err != nil {
				t.Fatal(err)
			}
			if i%2 == 1 {
				if err := w.flushIndex(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	p, err := openDownsamplePart(path)
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
		if r.metaPos != resIdx*3 || r.metaEnd != (resIdx+1)*3 {
			t.Fatalf("未直接定位分辨率范围: %d..%d", r.metaPos, r.metaEnd)
		}
		tsid := TSID{MetricID: 10}
		r.SetFilter(&tsid, minUnixMilli, maxUnixMilli)
		if r.metaPos != resIdx*3 {
			t.Fatal("遗漏含目标 TSID 的首个 index")
		}
		count := 0
		for r.NextHeader() {
			if r.Header().TSID != tsid {
				t.Fatal("返回其他 TSID")
			}
			count++
		}
		if err := r.Error(); err != nil {
			t.Fatal(err)
		}
		if count != 3 {
			t.Fatalf("跨 index 的相同 TSID 丢失: %d", count)
		}
		r.SetFilter(&tsid, minUnixMilli+3*res, minUnixMilli+3*res+10)
		count = 0
		for r.NextHeader() {
			count++
		}
		if count != 1 || r.Error() != nil {
			t.Fatalf("重叠 metaindex 时间范围过滤失败: %d %v", count, r.Error())
		}
		for _, missing := range []uint64{0, 15, 40} {
			tsid.MetricID = missing
			r.SetFilter(&tsid, minUnixMilli, maxUnixMilli)
			if missing == 15 && r.metaPos != resIdx*3+2 {
				t.Fatal("未二分跳过较小 LastTSID")
			}
			if r.NextHeader() || r.Error() != nil {
				t.Fatalf("稀疏 TSID 过滤失败: %d %v", missing, r.Error())
			}
			if len(r.indexData) != 0 {
				t.Fatal("不存在的 TSID 不应解码候选范围外的 index")
			}
		}
	}
}

func TestDownsampleReaderRawSeekEqualBoundaryAndReuse(t *testing.T) {
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
	files := r.files
	tsid := TSID{MetricID: 10}
	r.SetFilter(&tsid, minUnixMilli+50, minUnixMilli+55)
	if r.metaPos != 0 {
		t.Fatal("原始相等边界没有保留前一个 index")
	}
	count := 0
	for r.NextHeader() {
		count++
	}
	if count != 3 || r.Error() != nil {
		t.Fatalf("原始跨 index 重叠 block 丢失: %d %v", count, r.Error())
	}
	if err := r.Init(p, 3600000); err != nil {
		t.Fatal(err)
	}
	if r.files != files {
		t.Fatal("同一原始源重复 Init 重开了文件")
	}
	tsid.MetricID = 20
	r.SetFilter(&tsid, minUnixMilli, maxUnixMilli)
	if r.metaPos != 1 {
		t.Fatalf("原始 lower-bound-1 位置错误: %d", r.metaPos)
	}
	if !r.NextHeader() || r.Header().TSID != tsid || r.NextHeader() || r.Error() != nil {
		t.Fatal("原始 TSID 二分后读取失败")
	}
}

func TestDownsampleReaderSeekHighCardinality(t *testing.T) {
	const rows = 10000
	p := &part{dsMetadata: &downsamplePartMetadata{}, dsMetaindex: make([]downsampleMetaindexRow, rows*2)}
	for resIdx, res := range []int64{300000, 3600000} {
		for i := 0; i < rows; i++ {
			tsid := TSID{MetricID: uint64(i*10 + 10)}
			p.dsMetaindex[resIdx*rows+i] = downsampleMetaindexRow{ResolutionMs: res, TSID: tsid, LastTSID: tsid}
		}
	}
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 3600000); err != nil {
		t.Fatal(err)
	}
	tsid := TSID{MetricID: 99990}
	r.SetFilter(&tsid, minUnixMilli, maxUnixMilli)
	if r.metaPos != rows+9998 || r.metaEnd != 2*rows {
		t.Fatalf("高基数定位未跳过无关 metaindex: %d..%d", r.metaPos, r.metaEnd)
	}
}

// TestDownsampleNativeBlockReuse 验证文件 writer 的每个特征使用独立 Block，且 reader 返回标准解码状态。
func TestDownsampleNativeBlockReuse(t *testing.T) {
	for _, timestampPrecision := range []uint8{64, 8} {
		t.Run(fmt.Sprintf("timestamps_precision_%d", timestampPrecision), func(t *testing.T) {
			batch := &downsampleBatch{tsid: TSID{MetricID: 10}, resolution: 300000, timestampPrecisionBits: timestampPrecision, precisionBits: [5]uint8{64, 32, 64, 16, 52}}
			for row := 0; row < 1024; row++ {
				batch.timestamps = append(batch.timestamps, minUnixMilli+int64(row)*300000+int64((row*row*7919)%299999))
				for feature := range batch.values {
					batch.values[feature] = append(batch.values[feature], float64((row*row*17+feature*101)%104729)/float64((feature+1)*128))
				}
			}
			path := filepath.Join(t.TempDir(), "part")
			w := getDownsampleWriter()
			defer putDownsampleWriter(w)
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			if err := w.WriteBlock(batch); err != nil {
				t.Fatal(err)
			}
			var stored [5]Block
			for feature := range w.blocks {
				fb := &w.blocks[feature]
				if fb.bh.TSID != batch.tsid || fb.bh.RowsCount != 1024 || fb.nextIdx != 0 || len(fb.values) != 0 || len(fb.timestamps) != 0 || len(fb.headerData) != marshaledBlockHeaderSize || fb.bh.PrecisionBits != batch.precisionBits[feature] {
					t.Fatalf("特征 %d 未保持原生 Block 的编码状态", feature)
				}
				var originalHeader blockHeader
				if tail, err := originalHeader.Unmarshal(fb.headerData); err != nil || len(tail) != 0 || originalHeader != fb.bh {
					t.Fatalf("特征 %d 的 header 不是原生 Block header: %v", feature, err)
				}
				stored[feature].CopyFrom(fb)
				if feature > 0 && (!bytes.Equal(fb.timestampsData, w.blocks[0].timestampsData) || fb.bh.TimestampsBlockOffset != w.blocks[0].bh.TimestampsBlockOffset) {
					t.Fatal("五个独立 Block 未引用同一时间戳负载")
				}
			}
			if _, err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			r := getDownsampleReader()
			defer putDownsampleReader(r)
			if err := r.Init(p, 300000); err != nil || !r.NextHeader() {
				t.Fatalf("无法定位批次: %v / %v", err, r.Error())
			}
			for feature := range stored {
				want := &stored[feature]
				if err := want.unmarshalDataWithTimestampPrecision(timestampPrecision); err != nil {
					t.Fatal(err)
				}
				var got Block
				if err := r.ReadFieldBlock(&got, uint8(feature)); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got.timestamps, want.timestamps) || !reflect.DeepEqual(got.values, want.values) || got.bh.Scale != want.bh.Scale || got.bh.PrecisionBits != batch.precisionBits[feature] || len(got.timestampsData) != 0 || len(got.valuesData) != 0 || got.nextIdx != 0 {
					t.Fatalf("特征 %d 未通过原生 Block 完成解码", feature)
				}
				// 查询用 header 直接进入既有 BlockRef，验证扩展格式不需要专用查询 decoder。
				bh, err := r.FieldHeader(uint8(feature))
				if err != nil || bh.PrecisionBits != min(timestampPrecision, batch.precisionBits[feature]) {
					t.Fatalf("查询用精度提示错误: %+v / %v", bh, err)
				}
				var br BlockRef
				br.init(p, &bh)
				var standard Block
				br.MustReadBlock(&standard)
				if err := standard.UnmarshalData(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(standard.timestamps, got.timestamps) || !reflect.DeepEqual(standard.values, got.values) {
					t.Fatal("标准 BlockRef 读取结果与单特征 reader 不一致")
				}
				// 验证返回对象可继续沿原有 Block 的筛选、复制和再次编码链路使用。
				ts, values := got.AppendRowsWithTimeRangeFilter(nil, nil, TimeRange{MinTimestamp: got.timestamps[10], MaxTimestamp: got.timestamps[20]})
				if len(ts) == 0 || len(ts) != len(values) {
					t.Fatal("标准 Block 时间范围筛选失败")
				}
				var copied Block
				copied.CopyFrom(&got)
				copied.MarshalData(12, 34)
				if copied.bh.TimestampsBlockOffset != 12 || copied.bh.ValuesBlockOffset != 34 || len(copied.values) != 0 || len(copied.timestamps) != 0 {
					t.Fatal("标准 Block 重新编码状态错误")
				}
				copied.MarshalData(56, 78)
				if err := copied.UnmarshalData(); err != nil {
					t.Fatal(err)
				}
				copied.Reset()
				if copied.bh != (blockHeader{}) || copied.nextIdx != 0 || len(copied.timestamps) != 0 || len(copied.values) != 0 || len(copied.headerData) != 0 || len(copied.timestampsData) != 0 || len(copied.valuesData) != 0 {
					t.Fatal("标准 Block Reset 未清除完整逻辑状态")
				}
			}
			w.reset()
			for feature := range w.blocks {
				if w.blocks[feature].bh != (blockHeader{}) || len(w.blocks[feature].timestampsData) != 0 || len(w.blocks[feature].valuesData) != 0 {
					t.Fatal("writer Reset 遗留字段 Block 状态")
				}
			}
			r.reset()
			if r.block.bh != (blockHeader{}) || len(r.block.values) != 0 || len(r.block.timestamps) != 0 {
				t.Fatal("reader Reset 遗留 Block 状态")
			}
		})
	}
}

func TestDownsamplePreviousFormatRejected(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("metadata_version_%d", version), func(t *testing.T) {
			path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
			metadataPath := filepath.Join(path, metadataFilename)
			data, err := os.ReadFile(metadataPath)
			if err != nil {
				t.Fatal(err)
			}
			var metadata map[string]any
			if err := json.Unmarshal(data, &metadata); err != nil {
				t.Fatal(err)
			}
			metadata["FormatVersion"] = version
			data, err = json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(metadataPath, data, 0644); err != nil {
				t.Fatal(err)
			}
			metaindexPath := filepath.Join(path, metaindexFilename)
			data, err = os.ReadFile(metaindexPath)
			if err != nil {
				t.Fatal(err)
			}
			data[7] = 2
			if err := os.WriteFile(metaindexPath, data, 0644); err != nil {
				t.Fatal(err)
			}
			if _, err := detectDownsampleFormat(path); err == nil {
				t.Fatal("未拒绝未发布的旧降采样格式")
			}
		})
	}
}

func TestDownsampleFieldHeaderSharedTimestamps(t *testing.T) {
	path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
	p, err := openDownsamplePart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	if err := r.Init(p, 300000); err != nil || !r.NextHeader() {
		t.Fatalf("无法定位批次: %v / %v", err, r.Error())
	}
	base := r.Header().marshal(nil)
	if len(base) != 5*(10+marshaledBlockHeaderSize) || downsampleFieldHeaderSize != 10+marshaledBlockHeaderSize {
		t.Fatal("文件条目长度未与原生 blockHeader 保持一致")
	}
	mutations := map[string]func(*downsampleFieldHeader){
		"resolution":          func(h *downsampleFieldHeader) { h.ResolutionMs = 3600000 },
		"feature":             func(h *downsampleFieldHeader) { h.Feature = 1 },
		"timestamp_precision": func(h *downsampleFieldHeader) { h.TimestampPrecisionBits = 8 },
		"tsid":                func(h *downsampleFieldHeader) { h.BlockHeader.TSID.MetricID++ },
		"rows":                func(h *downsampleFieldHeader) { h.BlockHeader.RowsCount++ },
		"minimum":             func(h *downsampleFieldHeader) { h.BlockHeader.MinTimestamp++ },
		"maximum":             func(h *downsampleFieldHeader) { h.BlockHeader.MaxTimestamp++ },
		"timestamp_offset":    func(h *downsampleFieldHeader) { h.BlockHeader.TimestampsBlockOffset++ },
		"timestamp_size":      func(h *downsampleFieldHeader) { h.BlockHeader.TimestampsBlockSize++ },
		"timestamp_codec": func(h *downsampleFieldHeader) {
			h.BlockHeader.TimestampsMarshalType = encoding.MarshalTypeNearestDelta2
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			data := append([]byte(nil), base...)
			var h downsampleFieldHeader
			if err := h.unmarshal(data[downsampleFieldHeaderSize : 2*downsampleFieldHeaderSize]); err != nil {
				t.Fatal(err)
			}
			mutate(&h)
			copy(data[downsampleFieldHeaderSize:], h.marshal(nil))
			var batch downsampleBlockHeader
			if err := batch.unmarshal(data); err == nil {
				t.Fatal("未拒绝五个单特征 Block 之间不一致的共享时间戳描述")
			}
		})
	}
}
