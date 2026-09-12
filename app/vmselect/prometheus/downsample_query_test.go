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
	}{{"1m", 60000}, {"5m", 300000}, {"45m", 2700000}, {"1h", 3600000}, {"2h", 7200000}} {
		for featureID, feature := range []string{"last", "sum", "count", "min", "max"} {
			args := url.Values{"resolution": {resolution.name}, "feature": {feature}}.Encode()
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
	r := httptest.NewRequest("POST", "/api/v1/query?resolution=5m", strings.NewReader("resolution=5m&feature=sum"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if q, err := getDownsampleQuery(r); q != nil || err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") {
		t.Fatalf("请求 URL 与正文中的重复参数未被拒绝: query=%+v err=%v", q, err)
	}
	for _, args := range []string{
		"resolution=", "feature=", "resolution=5m", "feature=sum",
		"resolution=&feature=sum", "resolution=5m&feature=",
		"resolution=5m&resolution=1h&feature=sum",
		"resolution=5m&feature=sum&feature=max",
		"resolution=0m&feature=sum", "resolution=5m&feature=avg",
		"query.field=", "query.field=5m%3Asum",
		"query.field=5m%3Asum&resolution=5m&feature=sum",
		"resolution=%zz&feature=sum",
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
			if args != "resolution=%zz&feature=sum" && !strings.HasPrefix(err.Error(), "[downsampling] ") {
				t.Fatalf("%s 无效降采样参数错误缺少降采样前缀: %s: %v", handler, args, err)
			}
		}
	}
}
