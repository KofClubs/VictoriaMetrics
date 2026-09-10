package prometheus

import (
	"fmt"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func getDownsampleQueryField(r *http.Request) (*storage.DownsampleQueryField, error) {
	if err := r.ParseForm(); err != nil {
		return nil, httpserver.InvalidParamError(err)
	}
	values := r.Form["query.field"]
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) != 1 || values[0] == "" {
		return nil, httpserver.InvalidParamError(fmt.Errorf("[downsampling] query.field must contain exactly one non-empty resolution:feature selector"))
	}
	field, err := storage.ParseDownsampleQueryField(values[0])
	if err != nil {
		return nil, httpserver.InvalidParamError(err)
	}
	return field, nil
}
