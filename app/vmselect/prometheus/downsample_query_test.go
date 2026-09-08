package prometheus

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestDownsampleQueryFieldHTTPParameter(t *testing.T) {
	for _, field := range []string{"5m:last", "5m:sum", "5m:count", "5m:min", "5m:max", "1h:last", "1h:sum", "1h:count", "1h:min", "1h:max"} {
		r := httptest.NewRequest("GET", "/api/v1/query?query=m&query.field="+url.QueryEscape(field), nil)
		if q, err := getDownsampleQueryField(r); q == nil || err != nil {
			t.Fatalf("合法参数 %q 被拒绝: %v", field, err)
		}
	}
	if q, err := getDownsampleQueryField(httptest.NewRequest("GET", "/api/v1/query?query=m", nil)); q != nil || err != nil {
		t.Fatalf("未指定字段时改变原始路径: %v %v", q, err)
	}
	for _, args := range []string{"query.field=", "query.field=5m%3Asum&query.field=1h%3Asum", "query.field=1m%3Asum", "query.field=5m%3Aavg", "query.field=%zz"} {
		for _, handler := range []string{"instant", "range"} {
			r := httptest.NewRequest("GET", "/api/v1/query?query=m&"+args, nil)
			w := httptest.NewRecorder()
			var err error
			if handler == "instant" {
				err = QueryHandler(nil, time.Now(), w, r)
			} else {
				err = QueryRangeHandler(nil, time.Now(), w, r)
			}
			if err == nil {
				t.Fatalf("%s 接受了无效字段: %s", handler, args)
			}
		}
	}
}
