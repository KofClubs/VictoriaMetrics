package promql

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestDownsampleQueryFieldCacheAndCopy(t *testing.T) {
	field, err := storage.ParseDownsampleQueryField("5m:sum")
	if err != nil {
		t.Fatal(err)
	}
	ec := &EvalConfig{Start: 1000, End: 2000, Step: 1000, DownsampleField: field}
	if ec.MayCache() {
		t.Fatal("摘要查询使用了未区分特征值的结果缓存")
	}
	copy := copyEvalConfig(ec)
	if copy.DownsampleField != field || copy.MayCache() {
		t.Fatal("子查询复制丢失字段选择或重新启用了结果缓存")
	}
	ec.Start = ec.End
	if ec.MayCache() {
		t.Fatal("瞬时摘要查询未禁用结果缓存")
	}
}
