package storage

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

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
		if q, err := ParseDownsampleQueryField(s); q != nil || err == nil {
			t.Fatalf("接受了无效字段 %q", s)
		}
	}
	if q, err := ParseDownsampleQueryField(""); q != nil || err != nil {
		t.Fatalf("空字段未恢复原始路径: %v %v", q, err)
	}
}

func TestDownsampleQueryFieldBlockRef(t *testing.T) {
	var blocks []*downsampleBatch
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
	p, err := openDownsamplePart(writeFileTestDownsamplePart(t, blocks...))
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	tr := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
	var ps partSearch
	defer ps.reset()
	for _, resolution := range downsampleResolutions {
		for feature := uint8(0); feature < downsampleFeaturesCount; feature++ {
			t.Run(fmt.Sprintf("%d/%d", resolution, feature), func(t *testing.T) {
				q := DownsampleQueryField{ResolutionMs: resolution, Feature: feature}
				ps.initWithDownsampleField(p, []TSID{{MetricID: 1}, {MetricID: 10}, {MetricID: 25}, {MetricID: 30}, {MetricID: 40}}, tr, &q)
				var ids []uint64
				for ps.NextBlock() {
					br := &ps.BlockRef
					id := br.bh.TSID.MetricID
					ids = append(ids, id)
					// 模拟查询暂存：序列化后只保留标准 BlockRef，再使用原有读取与解码链路。
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
					wantTimestamps := []int64{minUnixMilli + 1, minUnixMilli + resolution + 1}
					wantValue := float64(resolution/1000) + float64(id*100) + float64(feature*10)
					if !reflect.DeepEqual(timestamps, wantTimestamps) || !reflect.DeepEqual(values, []float64{wantValue, wantValue + 1}) {
						t.Fatalf("字段引用改变或混入其他字段: timestamps=%v values=%v", timestamps, values)
					}
				}
				if ps.Error() != nil || !reflect.DeepEqual(ids, []uint64{10, 30}) {
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
}

func TestDownsampleQueryFieldRawIsolationAndReset(t *testing.T) {
	p := newTestPart([]rawRow{{TSID: TSID{MetricID: 10}, Timestamp: 100, Value: 42, PrecisionBits: 64}})
	defer p.MustClose()
	var ps partSearch
	defer ps.reset()
	tsids := []TSID{{MetricID: 10}}
	tr := TimeRange{MinTimestamp: 0, MaxTimestamp: 200}
	for feature := uint8(0); feature < downsampleFeaturesCount; feature++ {
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

func TestDownsampleQueryFieldIndependentPrecision(t *testing.T) {
	b := fileTestDownsampleBlock(10, 300000)
	b.timestampPrecisionBits = 2
	b.timestamps = b.timestamps[:0]
	for i := 0; i < 40; i++ {
		b.timestamps = append(b.timestamps, minUnixMilli+int64(i)*300000+int64(i*i+1))
	}
	for field := range b.values {
		b.values[field] = b.values[field][:0]
		b.precisionBits[field] = 64
		for row := range b.timestamps {
			b.values[field] = append(b.values[field], float64(field*100+row)+0.125)
		}
	}
	p, err := openDownsamplePart(writeFileTestDownsamplePart(t, b))
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	var ps partSearch
	defer ps.reset()
	q := DownsampleQueryField{ResolutionMs: 300000, Feature: downsampleFeatureSum}
	tr := TimeRange{MinTimestamp: minUnixMilli, MaxTimestamp: maxUnixMilli}
	ps.initWithDownsampleField(p, []TSID{b.tsid}, tr, &q)
	if !ps.NextBlock() {
		t.Fatalf("未找到低精度 block: %v", ps.Error())
	}
	if ps.BlockRef.bh.PrecisionBits != 2 {
		t.Fatalf("丢失时间戳修复所需的精度提示: %d", ps.BlockRef.bh.PrecisionBits)
	}
	// 复刻单机版 BlockRef.Init(PartRef, data) 语义：仅保留 part 指针 + header 序列化往返。
	var restored BlockRef
	restored.p = ps.BlockRef.p
	tail, err := restored.bh.Unmarshal(ps.BlockRef.bh.Marshal(nil))
	if err != nil {
		t.Fatalf("cannot unmarshal block header: %v", err)
	}
	if len(tail) > 0 {
		t.Fatalf("unexpected non-empty tail after unmarshaling block header: len(tail)=%d", len(tail))
	}
	var got Block
	restored.MustReadBlock(&got)
	if err := got.UnmarshalData(); err != nil {
		t.Fatalf("原始 Block 解码失败: %v", err)
	}
	timestamps, values := got.AppendRowsWithTimeRangeFilter(nil, nil, tr)
	if !reflect.DeepEqual(values, b.values[downsampleFeatureSum]) {
		t.Fatalf("时间戳精度提示降低了值精度: %v", values)
	}
	if len(timestamps) != len(b.timestamps) || timestamps[0] != b.timestamps[0] || timestamps[len(timestamps)-1] != b.timestamps[len(b.timestamps)-1] {
		t.Fatalf("时间戳边界错误: %v", timestamps)
	}
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] < timestamps[i-1] {
			t.Fatalf("时间戳未执行顺序修复: %v", timestamps)
		}
	}
}

func TestDownsampleQueryFieldDoesNotChangeSearchProtocol(t *testing.T) {
	sq := NewSearchQuery(0, 0, 100, 200, nil, 3)
	before := sq.MarshalWithoutTenant(nil)
	sq.DownsampleField = &DownsampleQueryField{ResolutionMs: 300000, Feature: downsampleFeatureCount}
	if !bytes.Equal(before, sq.MarshalWithoutTenant(nil)) {
		t.Fatal("测试字段改变了现有集群协议")
	}
	wire := encoding.MarshalUint32(nil, 0)
	wire = encoding.MarshalUint32(wire, 0)
	wire = append(wire, before...)
	if tail, err := sq.Unmarshal(wire); err != nil || len(tail) != 0 || sq.DownsampleField != nil {
		t.Fatalf("反序列化后残留测试字段: %v %v", sq.DownsampleField, err)
	}
}
