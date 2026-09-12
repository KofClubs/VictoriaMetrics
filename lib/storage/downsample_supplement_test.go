package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	vmfs "github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
	"io/fs"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	downsampleResolution5m int64 = 300000
	downsampleResolution1h int64 = 3600000
)

var downsampleResolutions = [2]int64{downsampleResolution5m, downsampleResolution1h}

// downsampleTestConfig 显式保留旧测试的 5m/1h 输入，生产默认配置仅包含 base。
func downsampleTestConfig(t testing.TB, tenants ...TenantToken) *DownsamplingConfig {
	t.Helper()
	wire := downsamplingConfigJSON{BaseResolution: "5m", TenantResolutions: []downsamplingTenantConfigJSON{}}
	seen := make(map[TenantToken]bool)
	for _, tenant := range append([]TenantToken{{}}, tenants...) {
		if !seen[tenant] {
			seen[tenant] = true
			wire.TenantResolutions = append(wire.TenantResolutions, downsamplingTenantConfigJSON{Tenant: fmt.Sprintf("%d:%d", tenant.AccountID, tenant.ProjectID), Resolutions: []string{"1h"}})
		}
	}
	data, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	config, err := ParseDownsamplingConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func downsampleTestConfigForParts(t testing.TB, parts []*partWrapper) *DownsamplingConfig {
	t.Helper()
	var tenants []TenantToken
	for _, source := range parts {
		reader := getDownsampleReader()
		if err := reader.Init(source.p, downsampleResolution5m); err != nil {
			t.Fatal(err)
		}
		for reader.NextHeader() {
			tsid := reader.Header().TSID
			tenants = append(tenants, TenantToken{AccountID: tsid.AccountID, ProjectID: tsid.ProjectID})
		}
		err := reader.Error()
		err = errors.Join(err, putDownsampleReader(reader))
		if err != nil {
			t.Fatal(err)
		}
	}
	return downsampleTestConfig(t, tenants...)
}

func TestDownsampleClusterTenantIsolation(t *testing.T) {
	const base int64 = 1704067200000
	tsids := []TSID{
		{AccountID: 1, ProjectID: 7, MetricGroupID: 100, JobID: 200, InstanceID: 300, MetricID: 10},
		{AccountID: 1, ProjectID: 8, MetricGroupID: 100, JobID: 200, InstanceID: 300, MetricID: 20},
		{AccountID: 2, ProjectID: 7, MetricGroupID: 100, JobID: 200, InstanceID: 300, MetricID: 30},
		{AccountID: math.MaxUint32, ProjectID: math.MaxUint32 - 1, MetricGroupID: 100, JobID: 200, InstanceID: 300, MetricID: 40},
	}
	var rows, late []rawRow
	for tenant, tsid := range tsids {
		rows = append(rows,
			rawRow{TSID: tsid, Timestamp: base + 1, Value: float64(tenant*100 + 2), PrecisionBits: 64},
			rawRow{TSID: tsid, Timestamp: base + 2, Value: float64(tenant*100 + 8), PrecisionBits: 64})
		late = append(late, rawRow{TSID: tsid, Timestamp: base + 3, Value: float64(tenant*100 + 5), PrecisionBits: 64})
	}
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	first, _ := runDownsampleTestMerge(t, m, []*partWrapper{newDownsampleTestRawPart(t, rows)}, nil, 0)
	second, _ := runDownsampleTestMerge(t, m, []*partWrapper{first, newDownsampleTestRawPart(t, late)}, nil, 0)
	final, _ := runDownsampleTestMerge(t, m, []*partWrapper{second}, nil, 0)
	all := append(append([]rawRow(nil), rows...), late...)
	assertDownsampleTestRows(t, readDownsampleTestPart(t, final.p), referenceDownsampleTestRows(all, nil, 0))
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	for _, resolution := range downsampleResolutions {
		if err := r.Init(final.p, resolution); err != nil {
			t.Fatal(err)
		}
		for tenant, tsid := range tsids {
			if !r.NextHeader() || r.Header().TSID != tsid {
				t.Fatalf("租户 %d 定位失败: %v", tenant, r.Error())
			}
			want := [5]float64{float64(tenant*100 + 5), float64(tenant*300 + 15), 3, float64(tenant*100 + 2), float64(tenant*100 + 8)}
			for feature := range want {
				bh, err := r.readFeatureHeader(r.Header(), uint8(feature))
				if err != nil || bh.TSID != tsid {
					t.Fatalf("特征 header 丢失租户身份: %+v / %v", bh.TSID, err)
				}
				var block Block
				if err := readDownsampleFeatureBlockForTest(r, &block, uint8(feature)); err != nil {
					t.Fatal(err)
				}
				_, values := block.AppendRowsWithTimeRangeFilter(nil, nil, TimeRange{MinTimestamp: base, MaxTimestamp: base + resolution - 1})
				if block.bh.TSID != tsid || len(values) != 1 || values[0] != want[feature] {
					t.Fatalf("租户 %d 特征 %d 出现其他租户贡献: TSID=%+v values=%v", tenant, feature, block.bh.TSID, values)
				}
			}

		}
		if r.NextHeader() || r.Error() != nil {
			t.Fatalf("租户顺序扫描返回额外 block 或错误: %v", r.Error())
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if *r.Header() != (blockHeader{}) || r.currentSourcePart != nil || r.currentFeatureBlock.bh.TSID.AccountID != 0 || r.currentFeatureBlock.bh.TSID.ProjectID != 0 {
		t.Fatal("reader Close 遗留租户身份")
	}
}

func TestDownsampleStorageLifecycle(t *testing.T) {
	path := t.TempDir()
	opts := OpenOptions{Retention: 48 * time.Hour}
	s := MustOpenStorage(path, opts)
	defer func() {
		if s != nil {
			s.MustClose()
		}
	}()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
	var mn MetricName
	mn.MetricGroup = []byte("downsample_lifecycle")
	metricName := mn.marshalRaw(nil)
	mrs := []MetricRow{
		{MetricNameRaw: metricName, Timestamp: base + 60000, Value: 2},
		{MetricNameRaw: metricName, Timestamp: base + 240000, Value: 8},
	}
	s.AddRows(mrs, 64)
	s.DebugFlush()
	tsid := func() TSID {
		ptws := s.tb.GetAllPartitions(nil)
		defer s.tb.PutPartitions(ptws)
		if len(ptws) != 1 {
			t.Fatalf("unexpected partitions: %d", len(ptws))
		}
		ptws[0].pt.flushInmemoryRowsToFiles()
		parts := ptws[0].pt.GetParts(nil, true)
		defer ptws[0].pt.PutParts(parts)
		if len(parts) == 0 {
			t.Fatal("raw storage didn't create a file part")
		}
		var result TSID
		for _, pw := range parts {
			if pw.mp != nil || pw.p.dsMetadata != nil {
				t.Fatal("disabled downsampling must retain the raw file format")
			}
			func() {
				r := getBlockStreamReader()
				r.MustInitFromFilePart(pw.p.path)
				defer putBlockStreamReader(r)
				if !r.NextBlock() {
					t.Fatalf("raw part has no block: %v", r.Error())
				}
				result = r.Block.bh.TSID
			}()
		}
		return result
	}()
	s.MustClose()
	s = nil

	// 启用后读取合法 raw 格式；尚未参与 merge 的 raw 文件无需预先迁移。
	opts.DownsamplingEnabled = true
	opts.DownsamplingConfig = downsampleTestConfig(t)
	s = MustOpenStorage(path, opts)
	assertDownsampleTestStorageFormats(t, s, true, false)
	late := []MetricRow{
		{MetricNameRaw: metricName, Timestamp: base + 60000, Value: 5},
		{MetricNameRaw: metricName, Timestamp: base + 180000, Value: 4},
		{MetricNameRaw: metricName, Timestamp: base + 60000, Value: 5},
	}
	s.AddRows(late, 64)
	s.DebugFlush()
	mrs = append(mrs, late...)
	want := referenceDownsampleTestRows(downsampleTestMetricRows(mrs, tsid), nil, 0)

	// snapshot 必须先把待落盘样本写为降采样格式；快照可以同时包含合法 raw 源。
	snapshotName := s.MustCreateSnapshot()
	assertDownsampleTestStorageFormats(t, s, false, true)
	func() {
		snapshot := MustOpenStorage(filepath.Join(path, snapshotsDirname, snapshotName), opts)
		defer snapshot.MustClose()
		if err := snapshot.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot merge snapshot: %s", err)
		}
		assertDownsampleTestStorageRows(t, snapshot, want)
	}()

	if err := s.ForceMergePartitions(""); err != nil {
		t.Fatal(err)
	}
	assertDownsampleTestStorageRows(t, s, want)
	s.MustClose()
	s = nil
	s = MustOpenStorage(path, opts)
	assertDownsampleTestStorageRows(t, s, want)

	// 关闭操作也必须把新 inmemory 输出为摘要，重开后可继续归并。
	closing := MetricRow{MetricNameRaw: metricName, Timestamp: base + 240000, Value: 3}
	s.AddRows([]MetricRow{closing}, 64)
	mrs = append(mrs, closing)
	s.MustClose()
	s = nil
	s = MustOpenStorage(path, opts)
	if err := s.ForceMergePartitions(""); err != nil {
		t.Fatal(err)
	}
	want = referenceDownsampleTestRows(downsampleTestMetricRows(mrs, tsid), nil, 0)
	assertDownsampleTestStorageRows(t, s, want)
}

func fileTestDownsampleBlock(tsid uint64, resolution int64) *downsampleDecodedResolutionFeaturesBlock {
	b := &downsampleDecodedResolutionFeaturesBlock{tsid: TSID{MetricID: tsid}, resolution: resolution, timestamps: []int64{minUnixMilli + 1, minUnixMilli + resolution + 1}, precisionBits: 64}
	for i := range b.values {
		b.values[i] = []float64{float64(i + 1), float64(i + 6)}
	}
	b.values[downsampleFeatureCount] = []float64{3, 3}
	return b
}

func writeFileTestDownsamplePart(t *testing.T, blocks ...*downsampleDecodedResolutionFeaturesBlock) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	var tenants []TenantToken
	for _, block := range blocks {
		tenants = append(tenants, TenantToken{AccountID: block.tsid.AccountID, ProjectID: block.tsid.ProjectID})
	}
	if err := w.Init(path, 1, downsampleTestConfig(t, tenants...)); err != nil {
		t.Fatal(err)
	}
	for _, b := range blocks {
		if err := writeDownsampleTestBlock(w, b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}
	return path
}

// 使用真实 Storage/IndexDB 和源文件，但作业集合独立于后台 partition。
func newDownsampleFailurePartition(t *testing.T) (*partition, int64) {
	t.Helper()
	s := MustOpenStorage(t.TempDir(), OpenOptions{DownsamplingEnabled: true, DownsamplingConfig: downsampleTestConfig(t, TenantToken{AccountID: 11, ProjectID: 17})})
	t.Cleanup(s.MustClose)
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
	ptw := s.tb.MustGetPartition(base)
	t.Cleanup(func() { s.tb.PutPartition(ptw) })
	owner := ptw.pt
	pt := &partition{s: s, idb: owner.idb, name: owner.name, tr: owner.tr,
		smallPartsPath: owner.smallPartsPath, bigPartsPath: owner.bigPartsPath,
		indexDBPartsPath: owner.indexDBPartsPath, stopCh: make(chan struct{})}
	pt.mergeIdx.Store(0x10000)
	t.Cleanup(func() {
		close(pt.stopCh)
		pt.wg.Wait()
		for _, group := range [][]*partWrapper{pt.inmemoryParts, pt.smallParts, pt.bigParts} {
			for _, pw := range group {
				pw.decRef()
			}
		}
	})
	return pt, base
}

func addDownsampleFailureSources(t *testing.T, pt *partition, base int64, count int, kind partType) []*partWrapper {
	t.Helper()
	var result []*partWrapper
	for i := 0; i < count; i++ {
		rows := []rawRow{{TSID: TSID{AccountID: 11, ProjectID: 17, MetricID: uint64(i + 1)},
			Timestamp: base + 60000, Value: float64(i + 1), PrecisionBits: 64}}
		if kind == partInmemory {
			result = append(result, registerDownsampleTestInmemoryPart(pt, rows))
			continue
		}
		mp := getInmemoryPart()
		mp.InitFromRows(rows)
		path := pt.getDstPartPath(kind, uint64(i+1))
		mp.MustStoreToDisk(path)
		putInmemoryPart(mp)
		pw := &partWrapper{p: mustOpenFilePart(path), isInMerge: true}
		pw.incRef()
		if kind == partSmall {
			pt.smallParts = append(pt.smallParts, pw)
		} else {
			pt.bigParts = append(pt.bigParts, pw)
		}
		result = append(result, pw)
	}
	mustWritePartNames(pt.smallParts, pt.bigParts, pt.smallPartsPath)
	return result
}

func failureManifest(t *testing.T, pt *partition) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(pt.smallPartsPath, partsFilename))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertDownsampleFailurePreserved(t *testing.T, pt *partition, sources []*partWrapper, before []byte, targets []string) {
	t.Helper()
	if !bytes.Equal(before, failureManifest(t, pt)) {
		t.Fatal("失败作业修改了活动 manifest")
	}
	for _, source := range sources {
		if source.isInMerge || source.mustDrop.Load() || source.p == nil || source.refCount.Load() != 1 {
			t.Fatal("失败作业没有保留源或释放 isInMerge")
		}
		if source.p.path != "" {
			if _, err := os.Stat(source.p.path); err != nil {
				t.Fatal("失败作业删除了源文件", err)
			}
		}
	}
	for _, target := range targets {
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatalf("失败作业遗留未发布目标 %q: %v", target, err)
		}
	}
	files, err := filepath.Glob(filepath.Join(pt.smallPartsPath, partsFilename+".tmp.*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("失败作业遗留临时 manifest: %v / %v", files, err)
	}
}

const (
	downsampleIterationSeries = 180
	// fixture 将每个 feature 的 index 限为 132 个原生 header，使长序列跨 index。
	downsampleIterationWideSeries = 131
	downsampleIterationWideRows   = 8192 + 37
)

type downsampleIterationSample struct {
	tsid      TSID
	timestamp int64
	value     float64
}

func downsampleIterationTSID(series int) TSID {
	// 高位分组字段递增；每跨一个 InstanceID，MetricID 反向跳变。
	// 整体仅按 TSID.Less 有序，不能用 MetricID 大小替代完整比较。
	return TSID{
		MetricGroupID: uint64(100 + series/60),
		JobID:         uint32(200 + (series%60)/20),
		InstanceID:    uint32(300 + (series%20)/10),
		MetricID:      uint64(100000 - (series/10)*1000 + (series%10)*10),
	}
}

func downsampleIterationRows(series int) int {
	if series == downsampleIterationWideSeries {
		return downsampleIterationWideRows
	}
	return 3 + series%5
}

func downsampleIterationTimestamp(series, row int, resolution int64) int64 {
	return minUnixMilli + int64(row)*resolution + int64(series%97+1+(row%7)*13)
}

func downsampleIterationValue(series, row int, resolution int64, feature uint8) float64 {
	// 各维度占用互不重叠的数位，四分之一的小数可由 float64 精确表示。
	slot := int64(1)
	if resolution == downsampleResolution1h {
		slot = 2
	}
	return float64(slot*1e12+int64(series)*1e6+int64(feature)*1e5+int64(row)*4) + 0.25
}

func newDownsampleIterationPart(t *testing.T) *part {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	var tenants []TenantToken
	for series := 0; series < downsampleIterationSeries; series++ {
		tsid := downsampleIterationTSID(series)
		tenants = append(tenants, TenantToken{AccountID: tsid.AccountID, ProjectID: tsid.ProjectID})
	}
	if err := w.Init(path, 1, downsampleTestConfig(t, tenants...)); err != nil {
		t.Fatal(err)
	}
	w.maxIndexBlockSize = (downsampleIterationWideSeries + 1) * marshaledBlockHeaderSize
	b := getDownsampleDecodedResolutionFeaturesBlock()
	defer putDownsampleDecodedResolutionFeaturesBlock(b)
	var expectedRows uint64
	for _, resolution := range []int64{300000, 3600000} {
		for series := 0; series < downsampleIterationSeries; series++ {
			rows := downsampleIterationRows(series)
			expectedRows += uint64(rows) * 5
			for start := 0; start < rows; start += 8192 {
				b.Reset()
				b.tsid = downsampleIterationTSID(series)
				b.resolution = resolution
				b.precisionBits = 64
				for row := start; row < min(start+8192, rows); row++ {
					b.timestamps = append(b.timestamps, downsampleIterationTimestamp(series, row, resolution))
					for feature := range b.values {
						b.values[feature] = append(b.values[feature], downsampleIterationValue(series, row, resolution, uint8(feature)))
					}
				}
				if err := writeDownsampleTestBlock(w, b); err != nil {
					t.Fatalf("写入分辨率 %d、序列 %d、起始行 %d: %v", resolution, series, start, err)
				}
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
	t.Cleanup(p.MustClose)
	if p.ph.RowsCount != expectedRows || p.ph.BlocksCount != 181*2*5 {
		t.Fatalf("fixture 物理统计错误: %+v", p.ph)
	}
	if len(p.dsMetaindex) != 2*5*2 {
		t.Fatalf("fixture 未形成两个分辨率、五个 feature 各两个 index: %d", len(p.dsMetaindex))
	}
	wide := downsampleIterationTSID(downsampleIterationWideSeries)
	for i, resolution := range []int64{300000, 3600000} {
		for feature := 0; feature < 5; feature++ {
			pos := (i*5 + feature) * 2
			left, right := &p.dsMetaindex[pos], &p.dsMetaindex[pos+1]
			if left.ResolutionMs != resolution || right.ResolutionMs != resolution || left.feature != uint8(feature) || right.feature != uint8(feature) || left.LastTSID != wide || right.TSID != wide || left.BlockHeadersCount != 132 || right.BlockHeadersCount != 49 {
				t.Fatalf("分辨率 %d 特征 %d 的长序列未跨 index: 左=%+v，右=%+v", resolution, feature, left, right)
			}
		}
	}
	return p
}

func downsampleIterationExpected(tsids []TSID, resolution int64, feature uint8, tr TimeRange) []downsampleIterationSample {
	var samples []downsampleIterationSample
	for series := 0; series < downsampleIterationSeries; series++ {
		tsid := downsampleIterationTSID(series)
		if tsids != nil {
			i := sort.Search(len(tsids), func(i int) bool { return !tsids[i].Less(&tsid) })
			if i == len(tsids) || tsids[i] != tsid {
				continue
			}
		}
		for row := 0; row < downsampleIterationRows(series); row++ {
			timestamp := downsampleIterationTimestamp(series, row, resolution)
			if timestamp >= tr.MinTimestamp && timestamp <= tr.MaxTimestamp {
				samples = append(samples, downsampleIterationSample{tsid: tsid, timestamp: timestamp, value: downsampleIterationValue(series, row, resolution, feature)})
			}
		}
	}
	return samples
}

func checkDownsampleIterationBlock(t *testing.T, br *BlockRef, tr TimeRange, expected []downsampleIterationSample, offset *int) {
	t.Helper()
	// 查询将引用暂存后，再通过原有单值 Block 完成读取、解码及闭区间过滤。
	// 复刻单机版 BlockRef.Init(PartRef, data) 语义：仅保留 part 指针 + header 序列化往返。
	var restored BlockRef
	restored.p = br.p
	tail, err := restored.bh.Unmarshal(br.bh.Marshal(nil))
	if err != nil {
		t.Fatalf("cannot unmarshal block header: %v", err)
	}
	if len(tail) > 0 {
		t.Fatalf("unexpected non-empty tail after unmarshaling block header: len(tail)=%d", len(tail))
	}
	var b Block
	restored.MustReadBlock(&b)
	if err := b.UnmarshalData(); err != nil {
		t.Fatal(err)
	}
	timestamps, values := b.AppendRowsWithTimeRangeFilter(nil, nil, tr)
	if len(timestamps) != len(values) {
		t.Fatalf("时间戳与值长度不一致: %d/%d", len(timestamps), len(values))
	}
	for i, timestamp := range timestamps {
		got := downsampleIterationSample{tsid: b.bh.TSID, timestamp: timestamp, value: values[i]}
		if *offset >= len(expected) {
			t.Fatalf("返回了多余样本 %d: %+v", *offset, got)
		}
		if want := expected[*offset]; got != want {
			t.Fatalf("样本 %d 丢失、重复或错串: got=%+v want=%+v", *offset, got, want)
		}
		*offset += 1
	}
}

func checkDownsampleIterationSearch(t *testing.T, ps *partSearch, p *part, tsids []TSID, q DownsampleQuery, tr TimeRange) {
	t.Helper()
	expected := downsampleIterationExpected(tsids, q.ResolutionMs, q.Feature, tr)
	ps.Init(p, tsids, tr, &q)
	count := 0
	for ps.NextBlock() {
		checkDownsampleIterationBlock(t, &ps.BlockRef, tr, expected, &count)
	}
	if err := ps.Error(); err != nil {
		t.Fatal(err)
	}
	if count != len(expected) {
		t.Fatalf("遗漏样本: got=%d want=%d", count, len(expected))
	}
	if ps.NextBlock() || ps.Error() != nil {
		t.Fatalf("迭代结束后再次产生结果或错误: %v", ps.Error())
	}
}

func checkDownsampleIterationReader(t *testing.T, r *downsampleReader, p *part, resolution int64) {
	t.Helper()
	tr := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
	var expected [countOfDownsampleFeatures][]downsampleIterationSample
	for feature := range expected {
		expected[feature] = downsampleIterationExpected(nil, resolution, uint8(feature), tr)
	}
	if err := r.Init(p, resolution); err != nil {
		t.Fatal(err)
	}
	count := 0
	var b downsampleDecodedResolutionFeaturesBlock
	for r.NextHeader() {
		if err := r.ReadBlock(&b, r.Header()); err != nil {
			t.Fatal(err)
		}
		if b.resolution != resolution {
			t.Fatalf("读取了其他分辨率: %d", b.resolution)
		}
		for i, timestamp := range b.timestamps {
			for feature := range expected {
				got := downsampleIterationSample{tsid: b.tsid, timestamp: timestamp, value: b.values[feature][i]}
				if count >= len(expected[feature]) || got != expected[feature][count] {
					t.Fatalf("reader sample %d feature %d is missing, duplicated or belongs to another TSID: %+v", count, feature, got)
				}
			}
			count++
		}
	}
	if err := r.Error(); err != nil {
		t.Fatal(err)
	}
	if count != len(expected[downsampleFeatureLast]) {
		t.Fatalf("reader 遗漏样本: got=%d want=%d", count, len(expected[downsampleFeatureLast]))
	}
	if r.NextHeader() || r.Error() != nil {
		t.Fatalf("reader 结束后再次产生结果或错误: %v", r.Error())
	}
}

func assertDownsampleLayoutOpenRejected(t *testing.T, path, message string) {
	t.Helper()
	p, err := openDownsamplePart(path, nil)
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
	p.timestampsFile = vmfs.MustOpenReaderAt(filepath.Join(path, timestampsFilename))
	p.valuesFile = vmfs.MustOpenReaderAt(filepath.Join(path, valuesFilename))
	p.indexFile = vmfs.MustOpenReaderAt(filepath.Join(path, indexFilename))
	t.Cleanup(p.MustClose)
	p.dsTimestampsSize = vmfs.MustFileSize(filepath.Join(path, timestampsFilename))
	p.dsValuesSize = vmfs.MustFileSize(filepath.Join(path, valuesFilename))
	p.dsIndexSize = vmfs.MustFileSize(filepath.Join(path, indexFilename))
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
	if len(r.currentTSIDBlockHeaders) != 0 {
		t.Fatal("reader 重置后仍保留先前 TSID 的 header")
	}
	for _, index := range r.currentResolutionFeatureIndexes {
		if index.currentIndexBlockEndOffset != 0 || len(index.currentIndexBlockHeaders) != 0 {
			t.Fatal("reader 重置后仍保留先前 index 边界")
		}
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
		if err := r.ReadBlock(b, r.Header()); err != nil {
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
	if err := w.Init(path, 1, downsampleTestConfig(t)); err != nil {
		t.Fatal(err)
	}
	// WriteSamples 暂存各 feature，实际切 index 发生在 flushResolution。
	w.maxIndexBlockSize = clusterDownsampleHeaderBytes
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
	if _, err := w.Finish(nil); err != nil {
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
		if err != nil || len(data) != clusterDownsampleHeaderBytes || binary.BigEndian.Uint32(mr[32:36]) != 1 {
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

const (
	clusterDownsampleHeaderBytes    = 89
	clusterDownsampleMetaindexBytes = 113
)

func makeDownsampleLayoutBlocks(blocksPerResolution int, singleRow bool) []*downsampleDecodedResolutionFeaturesBlock {
	var blocks []*downsampleDecodedResolutionFeaturesBlock
	for _, resolution := range []int64{300000, 3600000} {
		for i := 0; i < blocksPerResolution; i++ {
			b := &downsampleDecodedResolutionFeaturesBlock{
				tsid:       TSID{AccountID: 0x11223344 + uint32(i/132), ProjectID: 0x55667788 + uint32(i%132/66), MetricGroupID: 0x123456789abcdef0, JobID: 0x23456789, InstanceID: 0x3456789a, MetricID: uint64(i/2 + 1)},
				resolution: resolution, precisionBits: 64,
			}
			n := 4 + i%3
			if singleRow {
				n = 1
			}
			for j := 0; j < n; j++ {
				b.timestamps = append(b.timestamps, 1704067200000+int64(i%2*16+j)*resolution+int64((j+1)*(j+1)*101))
				values := [5]float64{
					float64(i*16+j*j+1) / 8, float64(i*64+j*j*j+3) * 2,
					float64(j+1) / 4, -float64(i*16+j*j*j+7) / 2, float64(i*32+j*j*3+11) / 4,
				}
				if i%2 == 0 {
					values[2] = 3.25
				}
				for col, value := range values {
					b.values[col] = append(b.values[col], value)
				}
			}
			blocks = append(blocks, b)
		}
	}
	return blocks
}

func decodeDownsampleLayoutFrame(t *testing.T, data []byte, prefix string) []byte {
	t.Helper()
	if len(data) < 8 || string(data[:6]) != prefix || data[6] != 0 || data[7] != 2 {
		t.Fatalf("%s 格式前缀错误", prefix)
	}
	decoded, err := encoding.DecompressZSTDLimited(nil, data[8:], 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func checkDownsampleLayoutPayload(t *testing.T, offsetData, sizeData []byte, offset uint64, payload, file []byte) {
	t.Helper()
	gotOffset := binary.BigEndian.Uint64(offsetData)
	gotSize := binary.BigEndian.Uint32(sizeData)
	if gotOffset != offset || uint64(gotSize) != uint64(len(payload)) {
		t.Fatalf("负载定位错误: offset=%d/%d, size=%d/%d", gotOffset, offset, gotSize, len(payload))
	}
	end := gotOffset + uint64(gotSize)
	if end > uint64(len(file)) || !bytes.Equal(file[gotOffset:end], payload) {
		t.Fatalf("实际字节未形成完整连续列: offset=%d, size=%d", gotOffset, gotSize)
	}
}

func readDownsampleLayoutInt64(data []byte) int64 {
	u := binary.BigEndian.Uint64(data)
	return int64(u>>1) ^ -int64(u&1)
}

func readDownsampleLayoutTSID(data []byte) TSID {
	return TSID{
		AccountID: binary.BigEndian.Uint32(data[0:4]), ProjectID: binary.BigEndian.Uint32(data[4:8]),
		MetricGroupID: binary.BigEndian.Uint64(data[8:16]), JobID: binary.BigEndian.Uint32(data[16:20]),
		InstanceID: binary.BigEndian.Uint32(data[20:24]), MetricID: binary.BigEndian.Uint64(data[24:32]),
	}
}

type downsampleTestKey struct {
	tsid       TSID
	resolution int64
	bucket     int64
}

func assertDownsampleTestPrecision64(t *testing.T, p *part) {
	t.Helper()
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	b := getDownsampleDecodedResolutionFeaturesBlock()
	defer putDownsampleDecodedResolutionFeaturesBlock(b)
	for _, resolution := range downsampleResolutions {
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		blocks := 0
		for r.NextHeader() {
			assertDownsampleTestHeaderPrecision(t, r, 64)
			if err := r.ReadBlock(b, r.Header()); err != nil {
				t.Fatal(err)
			}
			if b.precisionBits != 64 {
				t.Fatalf("resolution %d decoded batch lost precision64: %d", resolution, b.precisionBits)
			}
			blocks++
		}
		if err := r.Error(); err != nil {
			t.Fatal(err)
		}
		if blocks == 0 {
			t.Fatalf("resolution %d has no blocks to check", resolution)
		}
	}
}

func assertDownsampleTestHeaderPrecision(t *testing.T, r *downsampleReader, want uint8) {
	t.Helper()
	h := *r.Header()
	if h.PrecisionBits != want {
		t.Fatalf("resolution %d lost native precision: got=%d want=%d", r.currentResolution, h.PrecisionBits, want)
	}
	for feature := uint8(0); feature < countOfDownsampleFeatures; feature++ {
		column, err := r.readFeatureHeader(r.Header(), feature)
		if err != nil {
			t.Fatal(err)
		}
		if column.PrecisionBits != want || column.TSID != h.TSID || column.RowsCount != h.RowsCount || column.MinTimestamp != h.MinTimestamp || column.MaxTimestamp != h.MaxTimestamp || column.TimestampsBlockOffset != h.TimestampsBlockOffset || column.TimestampsBlockSize != h.TimestampsBlockSize || column.TimestampsMarshalType != h.TimestampsMarshalType {
			t.Fatalf("resolution %d feature %d lost shared timestamps or precision %d: %+v", r.currentResolution, feature, want, column)
		}
	}
}

func roundTripDownsampleTestReferenceBlock(t *testing.T, source *downsampleDecodedResolutionFeaturesBlock) *downsampleDecodedResolutionFeaturesBlock {
	t.Helper()
	result := &downsampleDecodedResolutionFeaturesBlock{
		tsid: source.tsid, resolution: source.resolution,
		precisionBits: source.precisionBits,
	}
	data, marshalType, first := encoding.MarshalTimestamps(nil, source.timestamps, 64)
	var err error
	result.timestamps, err = encoding.UnmarshalTimestamps(nil, data, marshalType, first, len(source.timestamps))
	if err != nil {
		t.Fatal(err)
	}
	if source.precisionBits < 64 {
		encoding.EnsureNonDecreasingSequence(result.timestamps, source.timestamps[0], source.timestamps[len(source.timestamps)-1])
	}
	for feature, values := range source.values {
		integers, scale := decimal.AppendFloatToDecimal(nil, values)
		data, marshalType, first := encoding.MarshalValues(nil, integers, source.precisionBits)
		decoded, err := encoding.UnmarshalValues(nil, data, marshalType, first, len(values))
		if err != nil {
			t.Fatal(err)
		}
		result.values[feature] = decimal.AppendDecimalToFloat(nil, decoded, scale)
	}
	return result
}

func downsampleTestBlockRows(block *downsampleDecodedResolutionFeaturesBlock) map[downsampleTestKey]downsampleSample {
	rows := make(map[downsampleTestKey]downsampleSample)
	for row, timestamp := range block.timestamps {
		point := downsampleSample{timestamp: timestamp, precisionBits: block.precisionBits}
		for feature := range point.values {
			point.values[feature] = block.values[feature][row]
		}
		rows[downsampleTestKey{block.tsid, block.resolution, timestamp / block.resolution}] = point
	}
	return rows
}

func newDownsampleTestRawPart(t *testing.T, rows []rawRow) *partWrapper {
	t.Helper()
	mp := getInmemoryPart()
	mp.InitFromRows(append([]rawRow(nil), rows...))
	pw := newPartWrapperFromInmemoryPart(mp, time.Time{})
	t.Cleanup(pw.decRef)
	return pw
}

func newDownsampleTestOverlappingPart(t *testing.T, blocks [][]rawRow) *partWrapper {
	t.Helper()
	mp := getInmemoryPart()
	mp.Reset()
	bsw := getBlockStreamWriter()
	bsw.MustInitFromInmemoryPart(mp, -5)
	var merged uint64
	for _, rows := range blocks {
		var timestamps []int64
		var values []float64
		for _, row := range rows {
			timestamps = append(timestamps, row.Timestamp)
			values = append(values, row.Value)
		}
		encoded, scale := decimal.AppendFloatToDecimal(nil, values)
		var b Block
		b.Init(&rows[0].TSID, timestamps, encoded, scale, 64)
		bsw.WriteExternalBlock(&b, &mp.ph, &merged)
	}
	bsw.MustClose()
	putBlockStreamWriter(bsw)
	pw := newPartWrapperFromInmemoryPart(mp, time.Time{})
	t.Cleanup(pw.decRef)
	return pw
}

func runDownsampleTestMerge(t *testing.T, m *downsampleMerger, sources []*partWrapper, deleted *uint64set.Set, deadline int64, configs ...*DownsamplingConfig) (*partWrapper, downsampleMergeStats) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	var config *DownsamplingConfig
	if len(configs) == 0 {
		config = downsampleTestConfigForParts(t, sources)
	} else {
		config = configs[0]
	}
	if err := w.Init(path, -5, config); err != nil {
		t.Fatal(err)
	}
	stats, err := m.Merge(sources, w, nil, deleted, deadline)
	if err != nil {
		w.Abort()
		t.Fatalf("cannot merge: %s", err)
	}
	if _, err := w.Finish(nil); err != nil {
		w.Abort()
		t.Fatalf("cannot finish merged part: %s", err)
	}
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatalf("cannot open merged part: %s", err)
	}
	pw := &partWrapper{p: p}
	pw.incRef()
	t.Cleanup(pw.decRef)
	return pw, stats
}

// downsamplePartResolutionsForTest 从有序索引提取实际分辨率，供文件内容断言和测试读回使用。
func downsamplePartResolutionsForTest(p *part) []int64 {
	var resolutions []int64
	for _, row := range p.dsMetaindex {
		if len(resolutions) == 0 || resolutions[len(resolutions)-1] != row.ResolutionMs {
			resolutions = append(resolutions, row.ResolutionMs)
		}
	}
	return resolutions
}

func readDownsampleTestPart(t *testing.T, p *part) map[downsampleTestKey]downsampleSample {
	t.Helper()
	result := make(map[downsampleTestKey]downsampleSample)
	r := getDownsampleReader()
	defer putDownsampleReader(r)
	b := getDownsampleDecodedResolutionFeaturesBlock()
	defer putDownsampleDecodedResolutionFeaturesBlock(b)
	for _, resolution := range downsamplePartResolutionsForTest(p) {
		if err := r.Init(p, resolution); err != nil {
			t.Fatal(err)
		}
		for r.NextHeader() {
			if err := r.ReadBlock(b, r.Header()); err != nil {
				t.Fatal(err)
			}
			for i, timestamp := range b.timestamps {
				key := downsampleTestKey{b.tsid, resolution, timestamp / resolution}
				if _, ok := result[key]; ok {
					t.Fatalf("multiple rows for target bucket %+v", key)
				}
				point := downsampleSample{timestamp: timestamp, precisionBits: b.precisionBits}
				for feature := range point.values {
					point.values[feature] = b.values[feature][i]
				}
				result[key] = point
			}
		}
		if err := r.Error(); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func referenceDownsampleTestRows(rows []rawRow, deleted *uint64set.Set, deadline int64, configs ...*DownsamplingConfig) map[downsampleTestKey]downsampleSample {
	groups := make(map[downsampleTestKey][]downsampleTestSample)
	precisions := make(map[downsampleTestKey]uint8)
	for _, row := range rows {
		if deleted != nil && deleted.Has(row.TSID.MetricID) {
			continue
		}
		resolutions := downsampleResolutions[:]
		if len(configs) != 0 {
			resolutions = configs[0].ResolutionsForTenant(row.TSID.AccountID, row.TSID.ProjectID)
		}
		for _, resolution := range resolutions {
			bucket := row.Timestamp / resolution
			if (bucket+1)*resolution <= deadline {
				continue
			}
			key := downsampleTestKey{row.TSID, resolution, bucket}
			groups[key] = append(groups[key], downsampleTestSample{row.Timestamp, row.Value})
			precisions[key] = row.PrecisionBits
		}
	}
	result := make(map[downsampleTestKey]downsampleSample)
	for key, samples := range groups {
		point := referenceDownsampleTestPoint(samples)
		point.precisionBits = precisions[key]
		result[key] = point
	}
	return result
}

func assertDownsampleTestRows(t *testing.T, got, want map[downsampleTestKey]downsampleSample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("unexpected downsample rows; got %d; want %d", len(got), len(want))
	}
	for key, expected := range want {
		actual, ok := got[key]
		if !ok {
			t.Fatalf("missing summary key %+v", key)
		}
		assertDownsamplePoint(t, &actual, &expected)
	}
}

func setDownsampleOpenTestDedup(t *testing.T, interval time.Duration) {
	t.Helper()
	previous := globalDedupInterval
	SetDedupInterval(interval)
	t.Cleanup(func() { globalDedupInterval = previous })
}

func createDownsampleOpenTestRaw(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	mp := getInmemoryPart()
	defer putInmemoryPart(mp)
	mp.InitFromRows([]rawRow{{
		TSID: TSID{MetricID: 1}, Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), Value: 1, PrecisionBits: 64,
	}})
	mp.MustStoreToDisk(path)
}

func createDownsampleOpenTestSummary(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1, downsampleTestConfig(t)); err != nil {
		t.Fatal(err)
	}
	for _, resolution := range downsampleResolutions {
		b := downsampleDecodedResolutionFeaturesBlock{
			tsid: TSID{MetricID: 1}, resolution: resolution,
			timestamps: []int64{time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()}, precisionBits: 64,
		}
		for i := range b.values {
			b.values[i] = []float64{1}
		}
		if err := writeDownsampleTestBlock(w, &b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}
}

func writeDownsampleOpenTestManifest(t *testing.T, path, partition string, small, big []string) {
	t.Helper()
	data, err := json.Marshal(partNamesJSON{Small: small, Big: big})
	if err != nil {
		t.Fatal(err)
	}
	writeDownsampleOpenTestFile(t, filepath.Join(path, dataDirname, smallDirname, partition, partsFilename), data)
}

func writeDownsampleOpenTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func snapshotDownsampleOpenTestFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	files := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if os.IsNotExist(err) && path == root {
			return nil
		}
		if err != nil {
			return err
		}
		info, err := de.Info()
		if err != nil {
			return err
		}
		data := []byte(nil)
		if !de.IsDir() {
			data, err = os.ReadFile(path)
			if err != nil {
				return err
			}
		}
		files[path] = fmt.Sprintf("%s:%d:%x", info.Mode(), info.ModTime().UnixNano(), data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func registerDownsampleTestInmemoryPart(pt *partition, rows []rawRow) *partWrapper {
	mp := getInmemoryPart()
	mp.InitFromRows(append([]rawRow(nil), rows...))
	pw := newPartWrapperFromInmemoryPart(mp, time.Now().Add(time.Hour))
	pw.isInMerge = true
	pt.partsLock.Lock()
	pt.inmemoryParts = append(pt.inmemoryParts, pw)
	pt.partsLock.Unlock()
	return pw
}

func assertDownsampleTestRawPart(t *testing.T, pw *partWrapper, rows []rawRow) {
	t.Helper()
	r := getBlockStreamReader()
	defer putBlockStreamReader(r)
	r.MustInitFromInmemoryPart(pw.mp)
	var got []rawRow
	for r.NextBlock() {
		if err := r.Block.UnmarshalData(); err != nil {
			t.Fatal(err)
		}
		values := decimal.AppendDecimalToFloat(nil, r.Block.values, r.Block.bh.Scale)
		for i, timestamp := range r.Block.timestamps {
			got = append(got, rawRow{TSID: r.Block.bh.TSID, Timestamp: timestamp, Value: values[i], PrecisionBits: r.Block.bh.PrecisionBits})
		}
	}
	if err := r.Error(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(rows) {
		t.Fatalf("inmemory rows changed; got %d; want %d", len(got), len(rows))
	}
	for i := range rows {
		if got[i] != rows[i] {
			t.Fatalf("inmemory row %d changed; got %+v; want %+v", i, got[i], rows[i])
		}
	}
}

func downsampleTestMetricRows(rows []MetricRow, tsid TSID) []rawRow {
	result := make([]rawRow, len(rows))
	for i, row := range rows {
		result[i] = rawRow{TSID: tsid, Timestamp: row.Timestamp, Value: row.Value, PrecisionBits: 64}
	}
	return result
}

func assertDownsampleTestStorageFormats(t *testing.T, s *Storage, requireRaw, requireSummary bool) {
	t.Helper()
	ptws := s.tb.GetAllPartitions(nil)
	defer s.tb.PutPartitions(ptws)
	var raw, summary int
	for _, ptw := range ptws {
		parts := ptw.pt.GetParts(nil, false)
		for _, pw := range parts {
			if pw.p.dsMetadata == nil {
				raw++
			} else {
				summary++
			}
		}
		ptw.pt.PutParts(parts)
	}
	if requireRaw && raw == 0 || requireSummary && summary == 0 {
		t.Fatalf("unexpected active formats: raw=%d, summary=%d", raw, summary)
	}
}

func assertDownsampleTestStorageRows(t *testing.T, s *Storage, want map[downsampleTestKey]downsampleSample) {
	t.Helper()
	ptws := s.tb.GetAllPartitions(nil)
	defer s.tb.PutPartitions(ptws)
	got := make(map[downsampleTestKey]downsampleSample)
	for _, ptw := range ptws {
		func() {
			parts := ptw.pt.GetParts(nil, true)
			defer ptw.pt.PutParts(parts)
			for _, pw := range parts {
				if pw.mp != nil || pw.p.dsMetadata == nil {
					t.Fatal("expected only persisted downsample parts after merge or reopen")
				}
				for key, point := range readDownsampleTestPart(t, pw.p) {
					if _, ok := got[key]; ok {
						t.Fatalf("duplicate target bucket after force merge: %+v", key)
					}
					got[key] = point
				}
			}
		}()
	}
	assertDownsampleTestRows(t, got, want)
}

func newDownsampleCloseTestReader(t *testing.T) *downsampleReader {
	t.Helper()
	r := &downsampleReader{currentSourcePart: &part{}}
	dir := t.TempDir()
	newFile := func(name string) filestream.ReadAtCloser {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("reader fixture"), 0600); err != nil {
			t.Fatal(err)
		}
		f, err := filestream.OpenReadAt(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		return &downsampleCloseTestFile{ReadAtCloser: f}
	}
	r.timestampsReader = newFile(timestampsFilename)
	r.valuesReader = newFile(valuesFilename)
	r.indexReader = newFile(indexFilename)
	return r
}

type downsampleCloseTestFile struct {
	filestream.ReadAtCloser
	closeErr error
	closes   int
}

type downsampleQueryIndexTestFile struct {
	vmfs.MustReadAtCloser
	read func([]byte, int64)
}

func (f *downsampleQueryIndexTestFile) MustReadAt(dst []byte, offset int64) {
	f.read(dst, offset)
}

func (f *downsampleCloseTestFile) Close() error {
	f.closes++
	return errors.Join(f.ReadAtCloser.Close(), f.closeErr)
}

func assertDownsampleFilesClosed(t *testing.T, files []filestream.ReadAtCloser) {
	t.Helper()
	buf := make([]byte, 1)
	for _, f := range files {
		if _, err := f.ReadAt(buf, 0); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("file %q remains open: %v", f.Path(), err)
		}
		if f, ok := f.(*downsampleCloseTestFile); ok && f.closes != 1 {
			t.Fatalf("file %q closed %d times; want 1", f.Path(), f.closes)
		}
	}
}

func sharedTimestampsTestBlock(tsid uint64, resolution, base int64, rows int, precision uint8) *downsampleDecodedResolutionFeaturesBlock {
	b := &downsampleDecodedResolutionFeaturesBlock{
		tsid: TSID{MetricID: tsid}, resolution: resolution, precisionBits: precision,
	}
	for i := 0; i < rows; i++ {
		b.timestamps = append(b.timestamps, base+int64(i)*resolution+int64(i*i*7919)%(resolution-1))
		for feature := range b.values {
			v := float64((i*i*17+feature*101)%104729) / float64((feature+1)*128)
			if feature == downsampleFeatureCount {
				v = 3 // 同时覆盖无需读取 values payload 的常量列。
			}
			b.values[feature] = append(b.values[feature], v)
		}
	}
	return b
}

// Use independent cluster-format constants and arbitrary precision so the oracle
// cannot repeat a missing feature multiplier or uint64 wrap in production code.
func downsampleSpaceBoundReference(rows, blocks uint64) uint64 {
	// Final payload: 60 bytes/row; spill values: 50 bytes/row.
	// Each batch has five independent index frames, metaindex rows and spill headers.
	const bytesPerBatch = 5 * ((2*89 + 256 + 8) + (2*113 + 256 + 8) + 89)
	total := new(big.Int).Mul(new(big.Int).SetUint64(rows), big.NewInt(120))
	total.Add(total, new(big.Int).Mul(new(big.Int).SetUint64(blocks), big.NewInt(bytesPerBatch)))
	total.Add(total, big.NewInt(64<<10))
	if !total.IsUint64() {
		return math.MaxUint64
	}
	return total.Uint64()
}

type downsampleTestSample struct {
	timestamp int64
	value     float64
}

func groupDownsampleTestSamples(samples []downsampleTestSample, resolution int64) map[int64][]downsampleTestSample {
	groups := make(map[int64][]downsampleTestSample)
	for _, sample := range samples {
		bucket := sample.timestamp / resolution
		groups[bucket] = append(groups[bucket], sample)
	}
	return groups
}

// referenceDownsampleTestPoint 分别扫描原始样本，独立计算有限小整数的参考统计。
func referenceDownsampleTestPoint(samples []downsampleTestSample) downsampleSample {
	p := downsampleSample{precisionBits: 64}
	for _, sample := range samples {
		if sample.timestamp > p.timestamp {
			p.timestamp = sample.timestamp
		}
	}
	p.values[downsampleFeatureLast] = math.Inf(-1)
	p.values[downsampleFeatureMin] = math.Inf(1)
	p.values[downsampleFeatureMax] = math.Inf(-1)
	p.values[downsampleFeatureCount] = float64(len(samples))
	for _, sample := range samples {
		p.values[downsampleFeatureSum] += sample.value
		p.values[downsampleFeatureMin] = math.Min(p.values[downsampleFeatureMin], sample.value)
		p.values[downsampleFeatureMax] = math.Max(p.values[downsampleFeatureMax], sample.value)
		if sample.timestamp == p.timestamp && sample.value > p.values[downsampleFeatureLast] {
			p.values[downsampleFeatureLast] = sample.value
		}
	}
	return p
}

func assertDownsamplePoint(t *testing.T, got, want *downsampleSample) {
	t.Helper()
	if got.precisionBits != want.precisionBits {
		t.Fatalf("unexpected precision bits; got %d; want %d", got.precisionBits, want.precisionBits)
	}
	if got.timestamp != want.timestamp {
		t.Fatalf("unexpected timestamp; got %d; want %d", got.timestamp, want.timestamp)
	}
	for feature, expected := range want.values {
		actual := got.values[feature]
		if math.IsNaN(expected) {
			if math.Float64bits(actual) != math.Float64bits(decimal.StaleNaN) {
				t.Fatalf("column %d wasn't normalized to StaleNaN; got %x", feature, math.Float64bits(actual))
			}
		} else if actual != expected {
			t.Fatalf("unexpected column %d; got %v; want %v", feature, actual, expected)
		}
	}
}

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
		src := downsampleSample{timestamp: timestamp, precisionBits: block.precisionBits}
		for feature := range block.values {
			src.values[feature] = block.values[feature][row]
		}
		samples[row].Merge(&src)
	}
	return samples, nil
}

// MergeRaw 将一条原始样本展开为五特征后合并；标记同样贡献一次 count。
// 仅用于测试注入原始样本，生产路径由 downsampleReader.ReadBlock 在读取时批量展开。
func (a *downsampleSample) MergeRaw(timestamp int64, value float64, precisionBits uint8) {
	p := downsampleSample{
		timestamp:     timestamp,
		precisionBits: precisionBits,
		values:        [countOfDownsampleFeatures]float64{value, value, 1, value, value},
	}
	a.Merge(&p)
}

var downsampleBenchmarkPoint downsampleSample

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
	if err := w.Init(path, -5, downsampleTestConfigForParts(b, sources)); err != nil {
		b.Fatal(err)
	}
	if _, err := m.Merge(sources, w, nil, nil, 0); err != nil {
		b.Fatal(err)
	}
	if _, err := w.Finish(nil); err != nil {
		b.Fatal(err)
	}
	p, err := openDownsamplePart(path, nil)
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
			if err := r.ReadBlock(block, r.Header()); err != nil {
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

// Faults belong to this writer instance; no process-wide I/O hooks are changed.
type failingDownsampleFile struct {
	filestream.WriteCloser
	writeErr   error
	shortWrite bool
	closes     int
	afterWrite func() // 在成功写入后触发实例级观察或取消。
	afterClose func() // 观察真实文件关闭后的状态，或模拟该时刻进程中断。
}

func (f *failingDownsampleFile) Write(b []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.shortWrite && len(b) > 0 {
		return len(b) - 1, nil
	}
	n, err := f.WriteCloser.Write(b)
	if err == nil && f.afterWrite != nil {
		f.afterWrite()
	}
	return n, err
}

func (f *failingDownsampleFile) MustClose() {
	f.closes++
	f.WriteCloser.MustClose()
	if f.afterClose != nil {
		f.afterClose()
	}
}

func assertDownsampleWriterAborted(t *testing.T, w *downsampleWriter, path string, cause error) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed writer left its target or spills at %q: %v", path, err)
	}
	if w.partPath != "" || w.isFinished {
		t.Fatalf("failed target is still publishable: path=%q; finished=%v", w.partPath, w.isFinished)
	}
	if w.timestampsWriter != nil || w.valuesWriter != nil || w.indexWriter != nil || w.metaindexWriter != nil {
		t.Fatal("failed writer retained a final-file handle")
	}
	for _, resolution := range w.partResolutionSpills {
		for _, spill := range resolution.featureSpills {
			if spill != nil {
				t.Fatal("failed writer retained a spill")
			}
		}
	}
	if _, err := w.Finish(nil); !errors.Is(err, cause) {
		t.Fatalf("Finish lost the failure or accepted partial output: %v", err)
	}
	if err := writeDownsampleTestBlock(w, fileTestDownsampleBlock(2, 300000)); !errors.Is(err, cause) {
		t.Fatalf("WriteSamples lost the failure or accepted more output: %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("cleanup must be idempotent: %v", err)
	}
}

func trackDownsampleWriterFiles(w *downsampleWriter) []*failingDownsampleFile {
	var files []*failingDownsampleFile
	for _, slot := range []*filestream.WriteCloser{&w.timestampsWriter, &w.valuesWriter, &w.indexWriter, &w.metaindexWriter} {
		f := &failingDownsampleFile{WriteCloser: *slot}
		*slot = f
		files = append(files, f)
	}
	return files
}

// readDownsampleFeatureBlockForTest 为测试单独解码一列，作为五特征共享时间戳读取的对照。
// 生产合并始终通过 ReadBlock 读取完整五列，不需要这个单列入口。
func readDownsampleFeatureBlockForTest(r *downsampleReader, dst *Block, feature uint8) error {
	h, err := r.readFeatureHeader(r.Header(), feature)
	if err != nil {
		return err
	}
	return r.readNativeBlock(dst, &h)
}

// downsampleIndexReadTestFile 记录顺序合并实际读取的索引偏移，不改变读取结果。
type downsampleIndexReadTestFile struct {
	filestream.ReadAtCloser
	offsets []int64
}

func (f *downsampleIndexReadTestFile) ReadAt(dst []byte, offset int64) (int, error) {
	f.offsets = append(f.offsets, offset)
	return f.ReadAtCloser.ReadAt(dst, offset)
}

// 源派生列不参加新计算；统计只覆盖实际消费的 base 五列。
func downsampleBasePhysicalRowsForTest(p *part) uint64 {
	if p.dsMetadata == nil {
		return p.ph.RowsCount
	}
	var rows uint64
	for _, row := range p.dsMetaindex {
		if row.ResolutionMs == p.dsMetadata.DownsamplingConfig.BaseResolutionMs() {
			rows += row.RowsCount
		}
	}
	return rows
}
