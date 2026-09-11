package promql

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestDownsampleQueryCacheAndCopy(t *testing.T) {
	downsampleQuery, err := storage.ParseDownsampleQuery("5m", "sum")
	if err != nil {
		t.Fatal(err)
	}
	ec := &EvalConfig{Start: 1000, End: 2000, Step: 1000, DownsampleQuery: downsampleQuery}
	if ec.MayCache() {
		t.Fatal("降采样查询使用了未区分特征值的结果缓存")
	}
	copy := copyEvalConfig(ec)
	if copy.DownsampleQuery != downsampleQuery || copy.MayCache() {
		t.Fatal("子查询复制丢失分辨率与特征选择或重新启用了结果缓存")
	}
	ec.Start = ec.End
	if ec.MayCache() {
		t.Fatal("瞬时降采样查询未禁用结果缓存")
	}
}
