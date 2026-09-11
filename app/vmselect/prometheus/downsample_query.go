package prometheus

import (
	"fmt"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// getDownsampleQuery 只在请求入口解析分辨率与特征；后续查询链路透传解析结果。
func getDownsampleQuery(r *http.Request) (*storage.DownsampleQuery, error) {
	if err := r.ParseForm(); err != nil {
		return nil, httpserver.InvalidParamError(err)
	}
	if _, ok := r.Form["query.field"]; ok {
		return nil, httpserver.InvalidParamError(fmt.Errorf("[downsampling] query.field is unsupported; use query.resolution and query.feature"))
	}
	resolutions := r.Form["query.resolution"]
	features := r.Form["query.feature"]
	if len(resolutions) == 0 && len(features) == 0 {
		return nil, nil
	}
	if len(resolutions) != 1 || resolutions[0] == "" || len(features) != 1 || features[0] == "" {
		return nil, httpserver.InvalidParamError(fmt.Errorf("[downsampling] query.resolution and query.feature must each contain exactly one non-empty value"))
	}
	downsampleQuery, err := storage.ParseDownsampleQuery(resolutions[0], features[0])
	if err != nil {
		return nil, httpserver.InvalidParamError(err)
	}
	return downsampleQuery, nil
}
