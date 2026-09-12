package main

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestDownsamplingConfigHTTP(t *testing.T) {
	oldDedup := storage.GetDedupInterval()
	storage.SetDedupInterval(0)
	defer storage.SetDedupInterval(time.Duration(oldDedup) * time.Millisecond)
	s := storage.MustOpenStorage(t.TempDir(), storage.OpenOptions{DownsamplingEnabled: true})
	defer s.MustClose()
	vms := &VMStorage{s: s}
	oldKey := downsamplingConfigAuthKey.Get()
	if err := downsamplingConfigAuthKey.Set("config-secret"); err != nil {
		t.Fatal(err)
	}
	defer downsamplingConfigAuthKey.Set(oldKey)
	request := func(method, query, body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, "/internal/downsampling/config"+query, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		if !vms.requestHandler(w, r) {
			t.Fatal("configuration route wasn't handled")
		}
		return w
	}
	if w := request(http.MethodGet, "", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated config read: %d", w.Code)
	}
	key := "?authKey=config-secret"
	if w := request(http.MethodPost, key, ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("unsupported method accepted: %d", w.Code)
	}
	update := `{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":["1h"]},{"tenant":"2:0","resolutions":["30m","2h"]}]}`
	if w := request(http.MethodPut, key, update); w.Code != http.StatusOK {
		t.Fatalf("update failed: %d %s", w.Code, w.Body.String())
	}
	var oversizedCanonical strings.Builder
	oversizedCanonical.WriteString(`{"base_resolution":"5m","tenant_resolutions":[`)
	for i := 0; i < 1480; i++ {
		if i > 0 {
			oversizedCanonical.WriteByte(',')
		}
		oversizedCanonical.WriteString(`{"tenant":"` + strconv.Itoa(i) + `:0","resolutions":["1h"]}`)
	}
	oversizedCanonical.WriteString(`]}`)
	for _, body := range []string{`{"base_resolution":"1m"}`, `{"base_resolution":"5m","unknown":1}`, `{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":["7m"]}]}`, strings.Repeat(" ", (64<<10)+1), oversizedCanonical.String()} {
		if w := request(http.MethodPut, key, body); w.Code != http.StatusBadRequest {
			t.Fatalf("invalid config accepted: %d %s", w.Code, w.Body.String())
		}
	}
	w := request(http.MethodGet, key, "")
	var response struct {
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	config, err := storage.ParseDownsamplingConfig(response.Data)
	if err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || response.Status != "success" || config.BaseResolutionMs() != 300000 || len(config.ResolutionsForTenant(1, 0)) != 2 || len(config.ResolutionsForTenant(2, 0)) != 3 || len(config.ResolutionsForTenant(2, 1)) != 1 {
		t.Fatalf("GET or rejected PUT changed config: %s", w.Body.String())
	}
}

func TestCalculateMaxMetricsLimitByResource(t *testing.T) {
	f := func(maxConcurrentRequest, remainingMemory, expect int) {
		t.Helper()
		maxMetricsLimit := calculateMaxUniqueTimeseries(maxConcurrentRequest, remainingMemory)
		if maxMetricsLimit != expect {
			t.Fatalf("unexpected max metrics limit: got %d, want %d", maxMetricsLimit, expect)
		}
	}

	// 64-bit architectures support memory sizes > 4GB.
	if strconv.IntSize == 64 {
		// 8 CPU & 32 GiB
		f(16, int(math.Round(32*1024*1024*1024*0.4)), 4294967)
		// 4 CPU & 32 GiB
		f(8, int(math.Round(32*1024*1024*1024*0.4)), 8589934)
	}

	// 2 CPU & 4 GiB
	f(4, int(math.Round(4*1024*1024*1024*0.4)), 2147483)

	// other edge cases
	f(0, int(math.Round(4*1024*1024*1024*0.4)), 2e9)
	f(4, 0, 0)

}

func TestGetMaxMetrics(t *testing.T) {
	originalMaxUniqueTimeSeries := *maxUniqueTimeseries
	defer func() {
		*maxUniqueTimeseries = originalMaxUniqueTimeSeries
		fs.MustRemoveDir(t.Name())
	}()

	maxConcurrentRequests := 2 * cgroup.AvailableCPUs()
	f := func(searchQueryLimit, storageMaxUniqueTimeseries, expect int) {
		t.Helper()
		*maxUniqueTimeseries = storageMaxUniqueTimeseries
		s := storage.MustOpenStorage(t.Name(), storage.OpenOptions{})
		vms := newVMStorage(s, maxConcurrentRequests)
		defer vms.Stop()
		maxMetrics := vms.getMaxMetrics(searchQueryLimit)
		if maxMetrics != expect {
			t.Fatalf("unexpected max metrics: got %d, want %d", maxMetrics, expect)
		}
	}

	f(0, 1e6, 1e6)
	f(2e6, 0, 2e6)
	f(2e6, 1e6, 1e6)
}
