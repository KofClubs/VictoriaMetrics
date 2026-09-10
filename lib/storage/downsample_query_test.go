package storage

import (
	"bytes"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"reflect"
	"sort"
	"strings"
	"testing"
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
			t.Fatal("fixture 未覆盖 MetricID 在高位字段切换后的逆序")
		}
	}
	var ps partSearch
	defer ps.reset()
	full := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
	for _, resolution := range []int64{300000, 3600000} {
		for feature := uint8(0); feature < 5; feature++ {
			q := DownsampleQueryField{ResolutionMs: resolution, Feature: feature}
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

func TestDownsampleQueryFieldParse(t *testing.T) {
	for _, resolution := range []struct {
		name string
		ms   int64
	}{{"5m", 300000}, {"1h", 3600000}} {
		for i, name := range []string{"last", "sum", "count", "min", "max"} {
			q, err := ParseDownsampleQueryField(resolution.name + ":" + name)
			if err != nil || q.ResolutionMs != resolution.ms || q.Feature != uint8(i) {
				t.Fatalf("错误字段选择: %v %v", q, err)
			}
		}
	}
	for _, s := range []string{"last", "5m", "5m:", ":sum", "1m:sum", "300000:sum", "5m:avg", "5m:sum:count", "5m:SUM", " 5m:sum"} {
		if q, err := ParseDownsampleQueryField(s); q != nil || err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") {
			t.Fatalf("无效字段未返回降采样错误: %q: %v", s, err)
		}
	}
	if q, err := ParseDownsampleQueryField(""); q != nil || err != nil {
		t.Fatalf("空字段未恢复原始路径: %v %v", q, err)
	}
}

func TestDownsampleQueryFieldBlockRef(t *testing.T) {
	var blocks []*downsampleDecodedResolutionFeaturesBlock
	for _, resolution := range downsampleResolutions {
		for _, id := range []uint64{10, 20, 30} {
			b := fileTestDownsampleBlock(id, resolution)
			for field := range b.values {
				for row := range b.timestamps {
					b.values[field][row] = float64(resolution/1000) + float64(id*100) + float64(field*10+row)
				}
			}
			blocks = append(blocks, b)
		}
	}
	b := fileTestDownsampleBlock(10, 300000)
	b.precisionBits = 64
	b.timestamps = b.timestamps[:0]
	for i := 0; i < 40; i++ {
		b.timestamps = append(b.timestamps, minUnixMilli+int64(i)*300000+int64(i*i+1))
	}
	for field := range b.values {
		b.values[field] = b.values[field][:0]
		for row := range b.timestamps {
			b.values[field] = append(b.values[field], float64(field*100+row)+0.125)
		}
	}
	for _, tc := range []struct {
		name        string
		blocks      []*downsampleDecodedResolutionFeaturesBlock
		resolutions []int64
		tsids       []TSID
		wantIDs     []uint64
	}{
		{"multiple-series", blocks, downsampleResolutions[:], []TSID{{MetricID: 1}, {MetricID: 10}, {MetricID: 25}, {MetricID: 30}, {MetricID: 40}}, []uint64{10, 30}},
		{"shared-precision", []*downsampleDecodedResolutionFeaturesBlock{b}, []int64{300000}, []TSID{b.tsid}, []uint64{10}},
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
						q := DownsampleQueryField{ResolutionMs: resolution, Feature: feature}
						ps.initWithDownsampleField(p, tc.tsids, tr, &q)
						var ids []uint64
						for ps.NextBlock() {
							br := &ps.BlockRef
							id := br.bh.TSID.MetricID
							ids = append(ids, id)
							var want *downsampleDecodedResolutionFeaturesBlock
							for _, source := range tc.blocks {
								if source.resolution == resolution && source.tsid.MetricID == id {
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
								t.Fatalf("字段、共享时间戳或精度往返错误: header=%+v timestamps=%v values=%v", got.bh, timestamps, values)
							}
						}
						if ps.Error() != nil || !reflect.DeepEqual(ids, tc.wantIDs) {
							t.Fatalf("错误 TSID 选择: %v %v", ids, ps.Error())
						}
					})
				}
			}
			ps.Init(p, []TSID{{MetricID: 10}}, tr)
			if ps.NextBlock() || ps.Error() != nil {
				t.Fatalf("未指定字段时读到了摘要: %v", ps.Error())
			}
			q := &DownsampleQueryField{ResolutionMs: 300000, Feature: downsampleFeatureSum}
			ps.initWithDownsampleField(p, []TSID{{MetricID: 10}}, TimeRange{MinTimestamp: maxUnixMilli - 1, MaxTimestamp: maxUnixMilli}, q)
			if ps.NextBlock() || ps.Error() != nil {
				t.Fatalf("返回了时间范围之外的摘要: %v", ps.Error())
			}
		})
	}
}

func TestDownsampleQueryFieldRawIsolationAndReset(t *testing.T) {
	p := newTestPart([]rawRow{{TSID: TSID{MetricID: 10}, Timestamp: 100, Value: 42, PrecisionBits: 64}})
	defer p.MustClose()
	var ps partSearch
	defer ps.reset()
	tsids := []TSID{{MetricID: 10}}
	tr := TimeRange{MinTimestamp: 0, MaxTimestamp: 200}
	for feature := uint8(0); feature < countOfDownsampleFeatures; feature++ {
		ps.initWithDownsampleField(p, tsids, tr, &DownsampleQueryField{ResolutionMs: 300000, Feature: feature})
		if ps.NextBlock() || ps.Error() != nil {
			t.Fatalf("原始数据冒充字段 %d: %v", feature, ps.Error())
		}
	}
	ps.Init(p, tsids, tr)
	if !ps.NextBlock() || ps.Error() != nil {
		t.Fatalf("复用对象后原始查询失效: %v", ps.Error())
	}
}

func TestDownsampleSearchProtocolValidation(t *testing.T) {
	tenant := TenantToken{AccountID: 123, ProjectID: 456}
	sq := NewSearchQuery(123, 456, 100000, 200000, nil, 37)
	legacy := sq.MarshalWithoutTenant(tenant.Marshal(nil))
	field := &DownsampleQueryField{ResolutionMs: 300000, Feature: downsampleFeatureCount}
	sq.DownsampleField = field
	if !bytes.Equal(legacy, sq.MarshalWithoutTenant(tenant.Marshal(nil))) {
		t.Fatal("测试字段改变了现有集群协议")
	}
	wire, err := sq.MarshalDownsampleWithoutTenant(tenant.Marshal(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire[:len(legacy)], legacy) || len(wire) != len(legacy)+9 {
		t.Fatal("字段查询改写了原生查询前缀")
	}
	var received SearchQuery
	if tail, err := received.UnmarshalDownsample(wire); err != nil || len(tail) != 0 || !reflect.DeepEqual(received.DownsampleField, field) {
		t.Fatalf("字段查询编解码往返失败: tail=%d err=%v field=%+v", len(tail), err, received.DownsampleField)
	}
	// 原有 decoder 必须保留扩展尾部，让旧 RPC 的严格尾部校验明确拒绝该负载。
	if tail, err := received.Unmarshal(wire); err != nil || len(tail) != 9 || received.DownsampleField != nil {
		t.Fatalf("旧 decoder 静默接受字段或残留状态: tail=%d err=%v", len(tail), err)
	}
	received.DownsampleField = field
	if tail, err := received.Unmarshal(legacy); err != nil || len(tail) != 0 || received.DownsampleField != nil {
		t.Fatalf("原生查询复用对象解码失败: tail=%d err=%v", len(tail), err)
	}
	for size := 0; size < len(wire); size++ {
		received.DownsampleField = field
		if _, err := received.UnmarshalDownsample(wire[:size]); err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") || received.DownsampleField != nil {
			t.Fatalf("截断长度 %d 未拒绝或字段未清除: %v", size, err)
		}
	}
	for _, invalid := range []*DownsampleQueryField{nil, {ResolutionMs: 0, Feature: 0}, {ResolutionMs: 60000, Feature: 2}, {ResolutionMs: 300000, Feature: 5}, {ResolutionMs: 3600000, Feature: 255}} {
		sq.DownsampleField = invalid
		if _, err := sq.MarshalDownsampleWithoutTenant(nil); err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") {
			t.Fatalf("编码无效字段未返回降采样错误: %+v: %v", invalid, err)
		}
		if invalid == nil {
			continue
		}
		bad := append([]byte(nil), legacy...)
		bad = encoding.MarshalInt64(bad, invalid.ResolutionMs)
		bad = append(bad, invalid.Feature)
		received.DownsampleField = field
		if _, err := received.UnmarshalDownsample(bad); err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") || received.DownsampleField != nil {
			t.Fatalf("解码接受无效字段或残留状态: %+v err=%v", invalid, err)
		}
	}
}
