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
)

func TestDownsampleFileRoundtrip(t *testing.T) {
	blocks := []*downsampleDecodedResolutionFeaturesBlock{fileTestDownsampleBlock(1, 300000), fileTestDownsampleBlock(2, 300000), fileTestDownsampleBlock(1, 3600000)}
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
		t.Fatal("raw decoder 接受了降采样格式")
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
	h, err := r.FieldHeader(downsampleFeatureCount)
	if err != nil {
		t.Fatal(err)
	}
	if h.ValuesBlockSize != 0 || h.ValuesMarshalType != encoding.MarshalTypeConst {
		t.Fatalf("常量列未使用零负载: %+v", h)
	}
	var got downsampleDecodedResolutionFeaturesBlock
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
	var got downsampleDecodedResolutionFeaturesBlock
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
	var got downsampleDecodedResolutionFeaturesBlock
	if err := r.ReadBlock(&got); err == nil {
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
	if err := r.ReadBlock(&b); err != nil {
		t.Fatal(err)
	}
	if len(b.timestamps) != downsampleMaxRawRows || b.values[downsampleFeatureCount][len(ts)-1] != 1 {
		t.Fatal("合法原始双倍行数 block 未完整读取")
	}
}

func TestDownsampleReaderSeekResolutionAndSharedTSID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	w.indexLimit = 2 * marshaledBlockHeaderSize
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
		if r.metaPos != resIdx*15 || r.metaEnd != resIdx*15+3 {
			t.Fatalf("未直接定位分辨率范围: %d..%d", r.metaPos, r.metaEnd)
		}
		tsid := TSID{MetricID: 10}
		r.SetFilter(&tsid, minUnixMilli, maxUnixMilli)
		if r.metaPos != resIdx*15 {
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
			if missing == 15 && r.metaPos != resIdx*15+2 {
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
	timestampsReader, valuesReader, indexReader := r.timestampsReader, r.valuesReader, r.indexReader
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
	if r.timestampsReader != timestampsReader || r.valuesReader != valuesReader || r.indexReader != indexReader {
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

func TestDownsampleIterationReaderReuse(t *testing.T) {
	p := newDownsampleIterationPart(t)
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	full := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
	// 先后选择 index 边界、缺失序列、末尾和开头，防止游标沿用前一次的位置。
	for pass, resolution := range []int64{300000, 3600000, 300000} {
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		for feature := uint8(0); feature < 5; feature++ {
			wide := downsampleIterationTSID(131)
			missingAfterWide := wide
			missingAfterWide.MetricID += 5
			last := downsampleIterationTSID(179)
			missingAfterLast := last
			missingAfterLast.MetricID += 5
			first := downsampleIterationTSID(0)
			missingBeforeFirst := first
			missingBeforeFirst.MetricID -= 5
			filters := []TSID{wide, missingAfterWide, last, missingAfterLast, missingBeforeFirst, first, downsampleIterationTSID(132), downsampleIterationTSID(9), downsampleIterationTSID(10), downsampleIterationTSID(19), downsampleIterationTSID(20), downsampleIterationTSID(59), downsampleIterationTSID(60)}
			for _, filter := range filters {
				t.Run(fmt.Sprintf("筛选复用/%d/%d/%d", pass, feature, filter.MetricID), func(t *testing.T) {
					checkDownsampleIterationReader(t, r, p, resolution, feature, &filter, full)
				})
			}
			t.Run(fmt.Sprintf("移除筛选/%d/%d", pass, feature), func(t *testing.T) {
				checkDownsampleIterationReader(t, r, p, resolution, feature, nil, full)
			})
		}
		// 错误状态必须能由后续 SetFilter 和同一 part 的 Init 清除。
		r.SetFilter(nil, 2, 1)
		if r.Error() == nil || r.NextHeader() {
			t.Fatal("无效时间范围未进入错误状态")
		}
		first := downsampleIterationTSID(0)
		checkDownsampleIterationReader(t, r, p, resolution, 2, &first, full)
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if r.p != nil || r.NextHeader() {
			t.Fatal("Close 后仍保留 part 或可读取结果")
		}
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		last := downsampleIterationTSID(downsampleIterationSeries - 1)
		checkDownsampleIterationReader(t, r, p, resolution, 4, &last, full)
	}
}

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
				assertDownsampleLayoutOpenRejected(t, path, "payloads are not contiguous across adjacent index blocks")
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
			} else if count != 1 || r.Error() == nil || !strings.Contains(r.Error().Error(), "payloads are not contiguous across adjacent index blocks") {
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
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
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

func TestDownsampleReaderCloseOwnFiles(t *testing.T) {
	r := newDownsampleCloseTestReader(t)
	peer := newDownsampleCloseTestReader(t)
	r.peers[downsampleFeatureSum] = peer
	files := []*os.File{r.timestampsReader, r.valuesReader, r.indexReader, peer.timestampsReader, peer.valuesReader, peer.indexReader}
	failedFiles := []*os.File{r.timestampsReader, peer.indexReader}
	for _, f := range failedFiles {
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	err := r.Close()
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("lost actual file close error: %v", err)
	}
	for _, f := range failedFiles {
		if !strings.Contains(err.Error(), "[downsampling] cannot close reader file \""+f.Name()+"\"") {
			t.Fatalf("lost prefixed close error for file %q: %v", f.Name(), err)
		}
	}
	assertDownsampleFilesClosed(t, files)
	if r.p != nil || r.ownFiles || r.timestampsReader != nil || r.valuesReader != nil || r.indexReader != nil {
		t.Fatal("Close retained part or file ownership")
	}
	for _, peer := range r.peers {
		if peer != nil {
			t.Fatal("Close retained a returned peer")
		}
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close attempted the same files or peers again: %v", err)
	}
}

func TestDownsampleReaderCloseBorrowedFiles(t *testing.T) {
	path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, downsampleResolution5m))
	p, err := openDownsamplePart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	var r downsampleReader
	if err := r.Init(p, downsampleResolution5m); err != nil {
		t.Fatal(err)
	}
	if !r.NextHeader() {
		t.Fatalf("missing header: %v", r.Error())
	}
	if _, err := r.FieldHeader(downsampleFeatureSum); err != nil {
		t.Fatal(err)
	}
	if r.peers[downsampleFeatureSum] == nil {
		t.Fatal("test did not create a feature reader")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []*os.File{p.dsTimestampsFile, p.dsValuesFile, p.dsIndexFile} {
		if _, err := f.Stat(); err != nil {
			t.Fatalf("reader closed borrowed file %q: %v", f.Name(), err)
		}
	}
	for _, peer := range r.peers {
		if peer != nil {
			t.Fatal("Close retained a feature reader")
		}
	}
}

// A filter change returns feature readers without closing the parent's source.
// A failed peer close remains observable until the next filter is installed.
func TestDownsampleReaderSetFilterReleasesPeers(t *testing.T) {
	r := newDownsampleCloseTestReader(t)
	defer r.Close()
	parentFiles := []*os.File{r.timestampsReader, r.valuesReader, r.indexReader}
	var peerFiles []*os.File
	for feature := uint8(1); feature < countOfDownsampleFeatures; feature++ {
		peer := newDownsampleCloseTestReader(t)
		r.peers[feature] = peer
		peerFiles = append(peerFiles, peer.timestampsReader, peer.valuesReader, peer.indexReader)
	}
	if err := peerFiles[0].Close(); err != nil {
		t.Fatal(err)
	}
	tsid := TSID{MetricID: 7}
	r.SetFilter(&tsid, minUnixMilli, maxUnixMilli)
	if !errors.Is(r.Error(), os.ErrClosed) || r.NextHeader() {
		t.Fatalf("filter change hid peer cleanup failure: %v", r.Error())
	}
	assertDownsampleFilesClosed(t, peerFiles)
	for _, peer := range r.peers {
		if peer != nil {
			t.Fatal("filter change retained a returned peer")
		}
	}
	for _, f := range parentFiles {
		if _, err := f.Stat(); err != nil {
			t.Fatalf("filter change closed parent file %q: %v", f.Name(), err)
		}
	}
	r.SetFilter(nil, minUnixMilli, maxUnixMilli)
	if r.Error() != nil || r.hasFilter || r.filterTSID != (TSID{}) {
		t.Fatalf("the next filter retried peer cleanup or retained stale state: %v", r.Error())
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close tried to close already returned peers: %v", err)
	}
	assertDownsampleFilesClosed(t, parentFiles)
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
	if r.p != nil || r.ownFiles || r.timestampsReader != nil || r.valuesReader != nil || r.indexReader != nil {
		t.Fatal("failed Init retained partial source ownership")
	}
	// The same reader can initialize a complete source after the failed attempt.
	for _, name := range []string{valuesFilename, indexFilename} {
		if err := os.WriteFile(filepath.Join(path, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Init(p, downsampleResolution1h, downsampleFeatureMax); err != nil {
		t.Fatal(err)
	}
	files := []*os.File{r.timestampsReader, r.valuesReader, r.indexReader}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	assertDownsampleFilesClosed(t, files)
	if r.feature != 0 || r.resolution != 0 {
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
			files := []*os.File{r.timestampsReader, r.valuesReader, r.indexReader}
			if err := r.timestampsReader.Close(); err != nil {
				t.Fatal(err)
			}
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
			if errors.Is(err, os.ErrNotExist) || r.p != nil {
				t.Fatalf("Init continued opening the next source after close failed: %v", err)
			}
			assertDownsampleFilesClosed(t, files)
		})
	}
}

// 首列之后关闭时间戳文件，使任何重复读取立即失败；原生 Block 中仅保留已解码时间戳。
func TestDownsampleReaderSharedTimestampsValuesOnly(t *testing.T) {
	for _, precision := range []uint8{64, 8} {
		t.Run(fmt.Sprintf("precision_%d", precision), func(t *testing.T) {
			input := sharedTimestampsTestBlock(10, downsampleResolution5m, minUnixMilli+1, 512, precision)
			path := writeFileTestDownsamplePart(t, input)
			p, err := openDownsamplePart(path)
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
				columns[feature], err = r.FieldHeader(uint8(feature))
				if err != nil {
					t.Fatal(err)
				}
				if err := r.readFieldBlock(&expected[feature], uint8(feature)); err != nil {
					t.Fatal(err)
				}
			}
			// 使用独立句柄，关闭后不影响 part 的引用生命周期和其它 reader。
			f, err := os.Open(filepath.Join(path, timestampsFilename))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			r.timestampsReader = f
			var got Block
			if err := r.readFieldBlock(&got, downsampleFeatureLast); err != nil {
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
			if err := r.readFieldBlock(&got, downsampleFeatureMax); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("readFieldBlock unexpectedly reused timestamps: %v", err)
			}
			// 下一次多特征读取也必须重新读取首列，不能跨 ReadBlock 缓存。
			var next downsampleDecodedResolutionFeaturesBlock
			if err := r.ReadBlock(&next); !errors.Is(err, os.ErrClosed) {
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
		p, err := openDownsamplePart(writeFileTestDownsamplePart(t, inputs...))
		if err != nil {
			t.Fatal(err)
		}
		defer p.MustClose()
		parts = append(parts, p)
	}
	var r, query downsampleReader
	defer r.Close()
	defer query.Close()
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
		if err := query.Init(p, step.resolution); err != nil {
			t.Fatal(err)
		}
		blocks := 0
		for r.NextHeader() {
			if !query.NextHeader() {
				t.Fatalf("query lost block: %v", query.Error())
			}
			if err := r.ReadBlock(&got); err != nil {
				t.Fatal(err)
			}
			h := query.Header()
			if got.tsid != h.TSID || got.resolution != step.resolution || got.precisionBits != h.PrecisionBits {
				t.Fatal("block identity was reused across TSID, source or resolution")
			}
			for feature := range got.values {
				var want Block
				if err := query.readFieldBlock(&want, uint8(feature)); err != nil {
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
		if query.NextHeader() || query.Error() != nil || blocks != 3 {
			t.Fatalf("lost blocks after switching: got %d; query error %v", blocks, query.Error())
		}
	}
}

func TestDownsampleReaderSharedTimestampsValidation(t *testing.T) {
	input := sharedTimestampsTestBlock(1, downsampleResolution5m, minUnixMilli+1, 8, 64)
	p, err := openDownsamplePart(writeFileTestDownsamplePart(t, input))
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
	column, err := r.FieldHeader(downsampleFeatureSum)
	if err != nil {
		t.Fatal(err)
	}
	assertInvalidValues := func(t *testing.T, h *blockHeader) {
		t.Helper()
		var b Block
		if err := r.readFieldBlock(&b, downsampleFeatureLast); err != nil {
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
			// FieldHeader validates cached peers before returning a query header;
			// this path must reject mismatches without relying on values decoding.
			peer := r.peers[downsampleFeatureSum]
			peer.current = h
			defer func() { peer.current = column }()
			if _, err := r.FieldHeader(downsampleFeatureSum); err == nil {
				t.Fatal("FieldHeader accepted a peer with different shared timestamps")
			}
		})
	}
	cases := map[string]func(*blockHeader){
		"invalid_precision": func(h *blockHeader) {
			h.PrecisionBits = 0
		},
		"values_codec": func(h *blockHeader) { h.ValuesMarshalType = 255 },
		"values_extent": func(h *blockHeader) {
			h.ValuesBlockOffset = r.valuesSize
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
	r.peers[downsampleFeatureSum].current.TimestampsBlockOffset++
	var got downsampleDecodedResolutionFeaturesBlock
	if err := r.ReadBlock(&got); err == nil {
		t.Fatal("ReadBlock accepted a later feature with different shared timestamps")
	}
}

func TestDownsampleClusterTenantIndexBoundaries(t *testing.T) {
	batches := makeDownsampleLayoutBlocks(330, false)
	path := writeFileTestDownsamplePart(t, batches...)
	p, err := openDownsamplePart(path)
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
		// 覆盖 account、project 的首末边界和相邻 index 首末项，后续查询复用同一个 reader。
		for _, offset := range []int{0, 64, 66, 130, 132, 196, 198, 262, 264, 328} {
			first := batches[resolutionIndex*330+offset]
			r.SetFilter(&first.tsid, minUnixMilli, maxUnixMilli)
			count := 0
			for r.NextHeader() {
				if count >= 2 {
					t.Fatal("租户边界筛选返回其他序列的 Block")
				}
				want := batches[resolutionIndex*330+offset+count]
				if err := r.ReadBlock(&got); err != nil {
					t.Fatal(err)
				}
				if got.tsid != want.tsid || got.resolution != want.resolution || !reflect.DeepEqual(got.timestamps, want.timestamps) || !reflect.DeepEqual(got.values, want.values) {
					t.Fatalf("租户跨 index 定位或数据错误: resolution=%d offset=%d block=%d", resolution, offset, count)
				}
				count++
			}
			if count != 2 || r.Error() != nil {
				t.Fatalf("租户跨 index 读取不完整: resolution=%d offset=%d count=%d error=%v", resolution, offset, count, r.Error())
			}
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
