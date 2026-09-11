package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func TestDownsampleIterationMultiTSID(t *testing.T) {
	p := newDownsampleIterationPart(t)
	var all []TSID
	for series := 0; series < downsampleIterationSeries; series++ {
		all = append(all, downsampleIterationTSID(series))
	}
	for i := 1; i < len(all); i++ {
		if !all[i-1].Less(&all[i]) {
			t.Fatalf("fixture 未按完整 TSID 排序: %+v %+v", all[i-1], all[i])
		}
		if i%10 == 0 && all[i-1].MetricID <= all[i].MetricID {
			t.Fatal("fixture 未覆盖 MetricID 在高位分组字段切换后的逆序")
		}
	}
	var ps partSearch
	defer ps.reset()
	full := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
	for _, resolution := range []int64{300000, 3600000} {
		for feature := uint8(0); feature < 5; feature++ {
			q := DownsampleQuery{ResolutionMs: resolution, Feature: feature}
			t.Run(fmt.Sprintf("全部序列/%d/%d", resolution, feature), func(t *testing.T) {
				checkDownsampleIterationSearch(t, &ps, p, all, q, full)
			})
			t.Run(fmt.Sprintf("非连续及缺失序列/%d/%d", resolution, feature), func(t *testing.T) {
				selected := []TSID{all[0], all[5], all[9], all[10], all[19], all[20], all[59], all[60], all[130], all[131], all[132], all[179]}
				beforeFirst := all[0]
				beforeFirst.MetricID -= 5
				selected = append(selected, beforeFirst)
				for _, series := range []int{5, 9, 19, 59, 130, 131, 179} {
					missing := all[series]
					missing.MetricID += 5
					selected = append(selected, missing)
				}
				sort.Slice(selected, func(i, j int) bool { return selected[i].Less(&selected[j]) })
				checkDownsampleIterationSearch(t, &ps, p, selected, q, full)
			})
			before := downsampleIterationTimestamp(downsampleIterationWideSeries, 8191, resolution)
			after := downsampleIterationTimestamp(downsampleIterationWideSeries, 8192, resolution)
			for _, tc := range []struct {
				name string
				tr   TimeRange
			}{
				{"前一 index 末点", TimeRange{MinTimestamp: before, MaxTimestamp: before}},
				{"后一 index 首点", TimeRange{MinTimestamp: after, MaxTimestamp: after}},
				{"跨 index 闭区间", TimeRange{MinTimestamp: before, MaxTimestamp: after}},
				{"区间相交但未覆盖点", TimeRange{MinTimestamp: before + 1, MaxTimestamp: after - 1}},
				{"序列末点", TimeRange{MinTimestamp: downsampleIterationTimestamp(downsampleIterationWideSeries, downsampleIterationWideRows-1, resolution), MaxTimestamp: downsampleIterationTimestamp(downsampleIterationWideSeries, downsampleIterationWideRows-1, resolution)}},
			} {
				t.Run(fmt.Sprintf("%s/%d/%d", tc.name, resolution, feature), func(t *testing.T) {
					checkDownsampleIterationSearch(t, &ps, p, []TSID{all[130], all[131], all[132]}, q, tc.tr)
				})
			}
		}
	}
}

func TestDownsampleQueryParse(t *testing.T) {
	for _, resolution := range []struct {
		name string
		ms   int64
	}{{"5m", 300000}, {"1h", 3600000}} {
		for i, name := range []string{"last", "sum", "count", "min", "max"} {
			q, err := ParseDownsampleQuery(resolution.name, name)
			if err != nil || q.ResolutionMs != resolution.ms || q.Feature != uint8(i) {
				t.Fatalf("错误特征选择: %v %v", q, err)
			}
		}
	}
	for _, invalid := range [][2]string{
		{"5m", ""}, {"", "sum"}, {"1m", "sum"}, {"300000", "sum"},
		{"5m", "avg"}, {"5m", "sum:count"}, {"5m", "SUM"}, {" 5m", "sum"},
	} {
		if q, err := ParseDownsampleQuery(invalid[0], invalid[1]); q != nil || err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") {
			t.Fatalf("invalid resolution/feature accepted: %q: %v", invalid, err)
		}
	}
	if q, err := ParseDownsampleQuery("", ""); q != nil || err != nil {
		t.Fatalf("empty resolution/feature did not select raw data: %v %v", q, err)
	}
}

func TestDownsampleQueryBlockRef(t *testing.T) {
	var blocks []*downsampleDecodedResolutionFeaturesBlock
	for _, resolution := range downsampleResolutions {
		for _, id := range []uint64{10, 20, 30} {
			b := fileTestDownsampleBlock(id, resolution)
			for feature := range b.values {
				for row := range b.timestamps {
					b.values[feature][row] = float64(resolution/1000) + float64(id*100) + float64(feature*10+row)
				}
			}
			blocks = append(blocks, b)
		}
	}
	// MetricID 相同但完整 TSID 不同，既覆盖同一 index 内的分组边界，也覆盖租户边界。
	sharedMetricTSIDs := []TSID{
		{AccountID: 1, ProjectID: 2, JobID: 1, MetricID: 10},
		{AccountID: 1, ProjectID: 2, JobID: 3, MetricID: 10},
		{AccountID: 2, ProjectID: 2, JobID: 1, MetricID: 10},
	}
	var sharedMetricBlocks []*downsampleDecodedResolutionFeaturesBlock
	for _, resolution := range downsampleResolutions {
		for i, tsid := range sharedMetricTSIDs {
			b := fileTestDownsampleBlock(tsid.MetricID, resolution)
			b.tsid = tsid
			for feature := range b.values {
				for row := range b.timestamps {
					b.values[feature][row] = float64(i*1000 + feature*10 + row)
				}
			}
			sharedMetricBlocks = append(sharedMetricBlocks, b)
		}
	}
	b := fileTestDownsampleBlock(10, 300000)
	b.precisionBits = 64
	b.timestamps = b.timestamps[:0]
	for i := 0; i < 40; i++ {
		b.timestamps = append(b.timestamps, minUnixMilli+int64(i)*300000+int64(i*i+1))
	}
	for feature := range b.values {
		b.values[feature] = b.values[feature][:0]
		for row := range b.timestamps {
			b.values[feature] = append(b.values[feature], float64(feature*100+row)+0.125)
		}
	}
	for _, tc := range []struct {
		name        string
		blocks      []*downsampleDecodedResolutionFeaturesBlock
		resolutions []int64
		tsids       []TSID
		wantTSIDs   []TSID
	}{
		{"multiple-series", blocks, downsampleResolutions[:], []TSID{{MetricID: 1}, {MetricID: 10}, {MetricID: 25}, {MetricID: 30}, {MetricID: 40}}, []TSID{{MetricID: 10}, {MetricID: 30}}},
		{"shared-precision", []*downsampleDecodedResolutionFeaturesBlock{b}, []int64{300000}, []TSID{b.tsid}, []TSID{b.tsid}},
		{"same-metric-id", sharedMetricBlocks, downsampleResolutions[:], sharedMetricTSIDs, sharedMetricTSIDs},
		{"same-metric-id-selected-group", sharedMetricBlocks, downsampleResolutions[:], sharedMetricTSIDs[1:2], sharedMetricTSIDs[1:2]},
		{"same-metric-id-missing-group", sharedMetricBlocks, downsampleResolutions[:], []TSID{{AccountID: 1, ProjectID: 2, JobID: 2, MetricID: 10}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := openDownsamplePart(writeFileTestDownsamplePart(t, tc.blocks...))
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			tr := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
			var ps partSearch
			defer ps.reset()
			for _, resolution := range tc.resolutions {
				for feature := uint8(0); feature < countOfDownsampleFeatures; feature++ {
					t.Run(fmt.Sprintf("%d/%d", resolution, feature), func(t *testing.T) {
						q := DownsampleQuery{ResolutionMs: resolution, Feature: feature}
						ps.Init(p, tc.tsids, tr, &q)
						var tsids []TSID
						for ps.NextBlock() {
							br := &ps.BlockRef
							tsid := br.bh.TSID
							tsids = append(tsids, tsid)
							var want *downsampleDecodedResolutionFeaturesBlock
							for _, source := range tc.blocks {
								if source.resolution == resolution && source.tsid == tsid {
									want = source
									break
								}
							}
							if want == nil || br.bh.PrecisionBits != want.precisionBits {
								t.Fatalf("返回未知序列或丢失共享精度: %+v", br.bh)
							}
							// 模拟查询暂存：仅保留 part 指针和序列化后的原生 header，再通过 BlockRef 解码。
							var restored BlockRef
							restored.p = br.p
							tail, err := restored.bh.Unmarshal(br.bh.Marshal(nil))
							if err != nil || len(tail) != 0 {
								t.Fatalf("原生 header 往返失败: tail=%d err=%v", len(tail), err)
							}
							var got Block
							restored.MustReadBlock(&got)
							if err := got.UnmarshalData(); err != nil {
								t.Fatal(err)
							}
							timestamps, values := got.AppendRowsWithTimeRangeFilter(nil, nil, tr)
							if got.bh.PrecisionBits != want.precisionBits || !reflect.DeepEqual(timestamps, want.timestamps) || !reflect.DeepEqual(values, want.values[feature]) {
								t.Fatalf("特征、共享时间戳或精度往返错误: header=%+v timestamps=%v values=%v", got.bh, timestamps, values)
							}
						}
						if ps.Error() != nil || !reflect.DeepEqual(tsids, tc.wantTSIDs) {
							t.Fatalf("错误 TSID 选择: got=%v want=%v err=%v", tsids, tc.wantTSIDs, ps.Error())
						}
					})
				}
			}
			ps.Init(p, []TSID{{MetricID: 10}}, tr, nil)
			if ps.NextBlock() || ps.Error() != nil {
				t.Fatalf("未指定特征时读到了摘要: %v", ps.Error())
			}
			q := &DownsampleQuery{ResolutionMs: 300000, Feature: downsampleFeatureSum}
			ps.Init(p, []TSID{{MetricID: 10}}, TimeRange{MinTimestamp: maxUnixMilli - 1, MaxTimestamp: maxUnixMilli}, q)
			if ps.NextBlock() || ps.Error() != nil {
				t.Fatalf("返回了时间范围之外的摘要: %v", ps.Error())
			}
		})
	}
}

func TestDownsampleQueryRawIsolationAndReset(t *testing.T) {
	p := newTestPart([]rawRow{{TSID: TSID{MetricID: 10}, Timestamp: 100, Value: 42, PrecisionBits: 64}})
	defer p.MustClose()
	var ps partSearch
	defer ps.reset()
	tsids := []TSID{{MetricID: 10}}
	tr := TimeRange{MinTimestamp: 0, MaxTimestamp: 200}
	for feature := uint8(0); feature < countOfDownsampleFeatures; feature++ {
		ps.Init(p, tsids, tr, &DownsampleQuery{ResolutionMs: 300000, Feature: feature})
		if ps.NextBlock() || ps.Error() != nil {
			t.Fatalf("原始数据冒充特征 %d: %v", feature, ps.Error())
		}
	}
	ps.Init(p, tsids, tr, nil)
	if !ps.NextBlock() || ps.Error() != nil {
		t.Fatalf("复用对象后原始查询失效: %v", ps.Error())
	}
}

func TestDownsampleSearchProtocolValidation(t *testing.T) {
	tenant := TenantToken{AccountID: 123, ProjectID: 456}
	sq := NewSearchQuery(123, 456, 100000, 200000, nil, 37)
	legacy := sq.MarshalWithoutTenant(tenant.Marshal(nil))
	downsampleQuery := &DownsampleQuery{ResolutionMs: 300000, Feature: downsampleFeatureCount}
	sq.DownsampleQuery = downsampleQuery
	if !bytes.Equal(legacy, sq.MarshalWithoutTenant(tenant.Marshal(nil))) {
		t.Fatal("降采样参数改变了现有集群协议")
	}
	wire, err := sq.MarshalDownsampleWithoutTenant(tenant.Marshal(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire[:len(legacy)], legacy) || len(wire) != len(legacy)+9 {
		t.Fatal("降采样查询改写了原生查询前缀")
	}
	var received SearchQuery
	if tail, err := received.UnmarshalDownsample(wire); err != nil || len(tail) != 0 || !reflect.DeepEqual(received.DownsampleQuery, downsampleQuery) {
		t.Fatalf("降采样查询编解码往返失败: tail=%d err=%v downsampleQuery=%+v", len(tail), err, received.DownsampleQuery)
	}
	// 原有 decoder 必须保留扩展尾部，让旧 RPC 的严格尾部校验明确拒绝该负载。
	if tail, err := received.Unmarshal(wire); err != nil || len(tail) != 9 || received.DownsampleQuery != nil {
		t.Fatalf("旧 decoder 静默接受特征或残留状态: tail=%d err=%v", len(tail), err)
	}
	received.DownsampleQuery = downsampleQuery
	if tail, err := received.Unmarshal(legacy); err != nil || len(tail) != 0 || received.DownsampleQuery != nil {
		t.Fatalf("原生查询复用对象解码失败: tail=%d err=%v", len(tail), err)
	}
	for size := 0; size < len(wire); size++ {
		received.DownsampleQuery = downsampleQuery
		if _, err := received.UnmarshalDownsample(wire[:size]); err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") || received.DownsampleQuery != nil {
			t.Fatalf("截断长度 %d 未拒绝或特征未清除: %v", size, err)
		}
	}
	for _, invalid := range []*DownsampleQuery{nil, {ResolutionMs: 0, Feature: 0}, {ResolutionMs: 60000, Feature: 2}, {ResolutionMs: 300000, Feature: 5}, {ResolutionMs: 3600000, Feature: 255}} {
		sq.DownsampleQuery = invalid
		if _, err := sq.MarshalDownsampleWithoutTenant(nil); err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") {
			t.Fatalf("编码无效分辨率或特征未返回降采样错误: %+v: %v", invalid, err)
		}
		if invalid == nil {
			continue
		}
		bad := append([]byte(nil), legacy...)
		bad = encoding.MarshalInt64(bad, invalid.ResolutionMs)
		bad = append(bad, invalid.Feature)
		received.DownsampleQuery = downsampleQuery
		if _, err := received.UnmarshalDownsample(bad); err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") || received.DownsampleQuery != nil {
			t.Fatalf("解码接受无效分辨率或特征或残留状态: %+v err=%v", invalid, err)
		}
	}
}

func TestDownsampleQueryIndexAccess(t *testing.T) {
	path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(10, downsampleResolution5m), fileTestDownsampleBlock(10, downsampleResolution1h))
	tr := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
	for _, resolution := range downsampleResolutions {
		for feature := uint8(0); feature < countOfDownsampleFeatures; feature++ {
			t.Run(fmt.Sprintf("%d/%d", resolution, feature), func(t *testing.T) {
				p, err := openDownsamplePart(path)
				if err != nil {
					t.Fatal(err)
				}
				defer p.MustClose()
				indexFile := p.dsIndexFile
				reads := 0
				p.dsIndexFile = &downsampleQueryIndexTestFile{ReadAtCloser: indexFile, read: func(dst []byte, offset int64) (int, error) {
					reads++
					for _, mr := range p.dsMetaindex {
						if int64(mr.IndexBlockOffset) == offset && int(mr.IndexBlockSize) == len(dst) && mr.ResolutionMs == resolution && mr.feature == feature {
							return indexFile.ReadAt(dst, offset)
						}
					}
					t.Fatalf("query read an index outside its resolution/feature: offset=%d size=%d", offset, len(dst))
					return 0, io.ErrUnexpectedEOF
				}}
				var ps partSearch
				ps.Init(p, []TSID{{MetricID: 10}}, tr, &DownsampleQuery{ResolutionMs: resolution, Feature: feature})
				defer ps.reset()
				if !ps.NextBlock() || ps.Error() != nil || ps.BlockRef.bh.TSID.MetricID != 10 {
					t.Fatalf("cannot locate the selected native block: %v", ps.Error())
				}
				if ps.NextBlock() || ps.Error() != nil || reads != 1 {
					t.Fatalf("unexpected extra block or index reads: reads=%d err=%v", reads, ps.Error())
				}
			})
		}
	}

	readErr := errors.New("injected index read failure")
	for _, scenario := range []string{"read_error", "short_read", "bad_magic"} {
		t.Run(scenario, func(t *testing.T) {
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			indexFile := p.dsIndexFile
			reads := 0
			p.dsIndexFile = &downsampleQueryIndexTestFile{ReadAtCloser: indexFile, read: func(dst []byte, offset int64) (int, error) {
				reads++
				switch scenario {
				case "read_error":
					return 0, readErr
				case "short_read":
					return len(dst) - 1, nil
				default:
					clear(dst)
					return len(dst), nil
				}
			}}
			var ps partSearch
			ps.Init(p, []TSID{{MetricID: 10}}, tr, &DownsampleQuery{ResolutionMs: downsampleResolution5m, Feature: downsampleFeatureSum})
			defer ps.reset()
			if ps.NextBlock() || ps.Error() == nil || !strings.HasPrefix(ps.Error().Error(), "[downsampling] ") {
				t.Fatalf("invalid index read succeeded: %v", ps.Error())
			}
			if scenario == "read_error" && !errors.Is(ps.Error(), readErr) || scenario == "short_read" && !errors.Is(ps.Error(), io.ErrUnexpectedEOF) {
				t.Fatalf("index read lost its error cause: %v", ps.Error())
			}
			if ps.NextBlock() || reads != 1 {
				t.Fatal("query continued reading after an index failure")
			}
		})
	}
}

func TestDownsampleQuerySearchPropagation(t *testing.T) {
	s := MustOpenStorage(t.TempDir(), OpenOptions{Retention: 48 * time.Hour, DownsamplingEnabled: true})
	defer s.MustClose()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
	mn := MetricName{AccountID: 17, ProjectID: 23, MetricGroup: []byte("downsample_query_propagation")}
	metricName := mn.marshalRaw(nil)
	s.AddRows([]MetricRow{
		{MetricNameRaw: metricName, Timestamp: base + 60000, Value: 2},
		{MetricNameRaw: metricName, Timestamp: base + 240000, Value: 8},
		{MetricNameRaw: metricName, Timestamp: base + 360000, Value: 6},
	}, 64)
	s.DebugFlush()
	if err := s.ForceMergePartitions(""); err != nil {
		t.Fatal(err)
	}
	tfs := NewTagFilters(17, 23)
	if err := tfs.Add(nil, mn.MetricGroup, false, false); err != nil {
		t.Fatal(err)
	}
	tr := TimeRange{MinTimestamp: base, MaxTimestamp: base + downsampleResolution1h - 1}
	var search Search
	for _, tc := range []struct {
		resolution int64
		timestamps []int64
		values     [][]float64
	}{
		{downsampleResolution5m, []int64{base + 240000, base + 360000}, [][]float64{{8, 6}, {10, 6}, {2, 1}, {2, 6}, {8, 6}}},
		{downsampleResolution1h, []int64{base + 360000}, [][]float64{{6}, {16}, {3}, {2}, {8}}},
	} {
		for feature := range tc.values {
			t.Run(fmt.Sprintf("%d/%d", tc.resolution, feature), func(t *testing.T) {
				query := &DownsampleQuery{ResolutionMs: tc.resolution, Feature: uint8(feature)}
				search.Init(nil, s, []*TagFilters{tfs}, tr, 100, noDeadline, query)
				defer search.MustClose()
				var timestamps []int64
				var values []float64
				for search.NextMetricBlock() {
					var block Block
					search.MetricBlockRef.BlockRef.MustReadBlock(&block)
					if err := block.UnmarshalData(); err != nil {
						t.Fatal(err)
					}
					timestamps, values = block.AppendRowsWithTimeRangeFilter(timestamps, values, tr)
				}
				if search.Error() != nil || !reflect.DeepEqual(timestamps, tc.timestamps) || !reflect.DeepEqual(values, tc.values[feature]) {
					t.Fatalf("resolution/feature changed through Search: timestamps=%v values=%v err=%v", timestamps, values, search.Error())
				}
			})
		}
	}
	// 同一 Search 对象归还后再次查询，不能保留前一次的降采样选择。
	search.Init(nil, s, []*TagFilters{tfs}, tr, 100, noDeadline, nil)
	defer search.MustClose()
	if search.NextMetricBlock() || search.Error() != nil {
		t.Fatalf("raw query reused a previous resolution/feature: %v", search.Error())
	}
}
