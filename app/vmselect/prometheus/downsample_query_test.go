package prometheus

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestDownsampleQueryHTTPParameter(t *testing.T) {
	for _, resolution := range []struct {
		name         string
		milliseconds int64
	}{{"5m", 300000}, {"1h", 3600000}} {
		for featureID, feature := range []string{"last", "sum", "count", "min", "max"} {
			args := url.Values{"query.resolution": {resolution.name}, "query.feature": {feature}}.Encode()
			for _, method := range []string{"GET", "POST"} {
				r := httptest.NewRequest(method, "/api/v1/query?query=m&"+args, nil)
				if method == "POST" {
					r = httptest.NewRequest(method, "/api/v1/query?query=m", strings.NewReader(args))
					r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				}
				q, err := getDownsampleQuery(r)
				if err != nil || q == nil || q.ResolutionMs != resolution.milliseconds || q.Feature != uint8(featureID) {
					t.Fatalf("合法参数解析错误: method=%s args=%s query=%+v err=%v", method, args, q, err)
				}
			}
		}
	}
	if q, err := getDownsampleQuery(httptest.NewRequest("GET", "/api/v1/query?query=m", nil)); q != nil || err != nil {
		t.Fatalf("未指定降采样参数时改变原始路径: %v %v", q, err)
	}
	r := httptest.NewRequest("POST", "/api/v1/query?query.resolution=5m", strings.NewReader("query.resolution=5m&query.feature=sum"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if q, err := getDownsampleQuery(r); q != nil || err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") {
		t.Fatalf("请求 URL 与正文中的重复参数未被拒绝: query=%+v err=%v", q, err)
	}
	for _, args := range []string{
		"query.resolution=", "query.feature=", "query.resolution=5m", "query.feature=sum",
		"query.resolution=&query.feature=sum", "query.resolution=5m&query.feature=",
		"query.resolution=5m&query.resolution=1h&query.feature=sum",
		"query.resolution=5m&query.feature=sum&query.feature=max",
		"query.resolution=1m&query.feature=sum", "query.resolution=5m&query.feature=avg",
		"query.field=", "query.field=5m%3Asum",
		"query.field=5m%3Asum&query.resolution=5m&query.feature=sum",
		"query.resolution=%zz&query.feature=sum",
	} {
		for _, handler := range []string{"instant", "range"} {
			r := httptest.NewRequest("GET", "/api/v1/query?query=m&"+args, nil)
			w := httptest.NewRecorder()
			var err error
			if handler == "instant" {
				err = QueryHandler(nil, time.Now(), nil, w, r)
			} else {
				err = QueryRangeHandler(nil, time.Now(), nil, w, r)
			}
			if err == nil {
				t.Fatalf("%s 接受了无效降采样参数: %s", handler, args)
			}
			if args != "query.resolution=%zz&query.feature=sum" && !strings.HasPrefix(err.Error(), "[downsampling] ") {
				t.Fatalf("%s 无效降采样参数错误缺少降采样前缀: %s: %v", handler, args, err)
			}
		}
	}
}
