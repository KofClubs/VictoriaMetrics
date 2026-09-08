package storage

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func TestDownsampleSearchProtocolValidation(t *testing.T) {
	tenant := TenantToken{AccountID: 123, ProjectID: 456}
	sq := NewSearchQuery(123, 456, 100000, 200000, nil, 37)
	legacy := sq.MarshalWithoutTenant(tenant.Marshal(nil))
	field := &DownsampleQueryField{ResolutionMs: 300000, Feature: downsampleFeatureCount}
	sq.DownsampleField = field
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
	if tail, err := received.Unmarshal(legacy); err != nil || len(tail) != 0 || received.DownsampleField != nil {
		t.Fatalf("原生查询复用对象解码失败: tail=%d err=%v", len(tail), err)
	}
	for size := 0; size < len(wire); size++ {
		received.DownsampleField = field
		if _, err := received.UnmarshalDownsample(wire[:size]); err == nil || received.DownsampleField != nil {
			t.Fatalf("截断长度 %d 未拒绝或字段未清除: %v", size, err)
		}
	}
	for _, invalid := range []*DownsampleQueryField{nil, {ResolutionMs: 0, Feature: 0}, {ResolutionMs: 60000, Feature: 2}, {ResolutionMs: 300000, Feature: 5}, {ResolutionMs: 3600000, Feature: 255}} {
		sq.DownsampleField = invalid
		if _, err := sq.MarshalDownsampleWithoutTenant(nil); err == nil {
			t.Fatalf("编码接受无效字段: %+v", invalid)
		}
		if invalid == nil {
			continue
		}
		bad := append([]byte(nil), legacy...)
		bad = encoding.MarshalInt64(bad, invalid.ResolutionMs)
		bad = append(bad, invalid.Feature)
		received.DownsampleField = field
		if _, err := received.UnmarshalDownsample(bad); err == nil || received.DownsampleField != nil {
			t.Fatalf("解码接受无效字段或残留状态: %+v err=%v", invalid, err)
		}
	}
}
