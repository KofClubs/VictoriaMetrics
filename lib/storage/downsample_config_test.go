package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func parseDownsamplingConfigForTest(t *testing.T, data string) *DownsamplingConfig {
	t.Helper()
	c, err := ParseDownsamplingConfig([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDownsamplingConfigJSON(t *testing.T) {
	c := parseDownsamplingConfigForTest(t, `{"base_resolution":"30s","tenant_resolutions":[{"tenant":"2:0","resolutions":["2h","30m"]},{"tenant":"1:0","resolutions":["1h"]}]}`)
	if c.BaseResolutionMs() != 30000 || c.MaxResolutionsPerTenant() != 3 {
		t.Fatalf("invalid config: %+v", c)
	}
	for tenant, want := range map[TenantToken][]int64{{1, 0}: {30000, 3600000}, {2, 0}: {30000, 1800000, 7200000}, {1, 1}: {30000}} {
		if got := c.ResolutionsForTenant(tenant.AccountID, tenant.ProjectID); !reflect.DeepEqual(got, want) {
			t.Fatalf("tenant %v: got %v; want %v", tenant, got, want)
		}
	}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	copy := parseDownsamplingConfigForTest(t, string(data))
	if !reflect.DeepEqual(c, copy) {
		t.Fatalf("roundtrip changed config: %s", data)
	}
	subset := c.forTenants([]TenantToken{{1, 0}, {9, 0}})
	if subset.MaxResolutionsPerTenant() != 2 || !reflect.DeepEqual(subset.ResolutionsForTenant(2, 0), []int64{30000}) {
		t.Fatal("metadata subset kept an unrelated tenant")
	}
	got := c.ResolutionsForTenant(2, 0)
	got[0] = 1
	if c.BaseResolutionMs() != 30000 || c.resolutionsForTenant(2, 0)[0] != 30000 {
		t.Fatal("public resolution slice mutated config")
	}
	for _, value := range []string{"1ms", "1.5s", "1m30s", "1d", "1w"} {
		if _, err := ParseDownsamplingConfig([]byte(`{"base_resolution":"` + value + `"}`)); err != nil {
			t.Fatalf("valid duration %q rejected: %v", value, err)
		}
	}
}

func TestDownsamplingConfigRejectsInvalidJSON(t *testing.T) {
	for _, data := range []string{
		``, `null`, `{}`, `[]`, `{"base_resolution":null}`, `{"base_resolution":"0s"}`, `{"base_resolution":"-1s"}`, `{"base_resolution":"1.5ms"}`, `{"base_resolution":"999999999999999999h"}`,
		`{"base_resolution":"5m","unknown":1}`, `{"base_resolution":"5m","base_resolution":"1h"}`, `{"base_resolution":"5m"} {}`,
		`{"base_resolution":"5m","tenant_resolutions":null}`, `{"base_resolution":"5m","tenant_resolutions":[null]}`,
		`{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0"}]}`,
		`{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":[],"resolutions":[]}]}`,
		`{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":[],"extra":1}]}`,
		`{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":[]},{"tenant":"1:0","resolutions":["1h"]}]}`,
	} {
		t.Run(data, func(t *testing.T) {
			if _, err := ParseDownsamplingConfig([]byte(data)); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	for _, tenant := range []string{"1", "1:", ":0", "01:0", "1:+0", "-1:0", "1:4294967296", " 1:0", "1:0:0"} {
		data := `{"base_resolution":"5m","tenant_resolutions":[{"tenant":"` + tenant + `","resolutions":[]}]}`
		if _, err := ParseDownsamplingConfig([]byte(data)); err == nil {
			t.Fatalf("invalid tenant %q accepted", tenant)
		}
	}
	for _, resolutions := range []string{`["5m"]`, `["1m"]`, `["7m"]`, `["1h","60m"]`, `null`, `[1]`} {
		data := `{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":` + resolutions + `}]}`
		if _, err := ParseDownsamplingConfig([]byte(data)); err == nil {
			t.Fatalf("invalid extras %s accepted", resolutions)
		}
	}
	if _, err := ParseDownsamplingConfig(bytes.Repeat([]byte(" "), downsampleMaxMetadataSize+1)); err == nil {
		t.Fatal("unbounded configuration accepted")
	}
}

func TestDownsamplingConfigMetadataSize(t *testing.T) {
	makeJSON := func(tenants int) []byte {
		wire := downsamplingConfigJSON{BaseResolution: "5m", TenantResolutions: []downsamplingTenantConfigJSON{}}
		for i := 0; i < tenants; i++ {
			wire.TenantResolutions = append(wire.TenantResolutions, downsamplingTenantConfigJSON{
				Tenant: strconv.Itoa(i) + ":0", Resolutions: []string{"1h"},
			})
		}
		data, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	// 请求本身合法且不足 64KiB，duration 规范化和 metadata 外壳会使输出超限。
	oversized := makeJSON(1480)
	if len(oversized) >= downsampleMaxMetadataSize {
		t.Fatal("fixture must fit the request byte limit")
	}
	if _, err := ParseDownsamplingConfig(oversized); err == nil || !strings.Contains(err.Error(), "metadata bytes") {
		t.Fatalf("configuration that cannot fit metadata was accepted: %v", err)
	}
	// 找到这一实际配置族在格式上限附近的最后一个可接受值，再直接编码生产 metadata。
	low, high := 0, 1480
	for low+1 < high {
		middle := (low + high) / 2
		if _, err := ParseDownsamplingConfig(makeJSON(middle)); err == nil {
			low = middle
		} else {
			high = middle
		}
	}
	config, err := ParseDownsamplingConfig(makeJSON(low))
	if err != nil {
		t.Fatal(err)
	}
	metadata := newDownsamplePartMetadata(partHeader{RowsCount: math.MaxUint64, BlocksCount: math.MaxUint64,
		MinTimestamp: maxUnixMilli, MaxTimestamp: maxUnixMilli}, config)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > downsampleMaxMetadataSize || downsampleMaxMetadataSize-len(encoded) > 64 {
		t.Fatalf("fixture didn't reach the metadata size boundary: %d bytes, %d tenants", len(encoded), low)
	}
	if _, err := ParseDownsamplingConfig(makeJSON(high)); err == nil {
		t.Fatal("next tenant beyond metadata limit was accepted")
	}
	// 重新读取可接受 metadata 会再次解析嵌套配置，必须终止且保持配置不变。
	var roundTrip downsamplePartMetadata
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if len(roundTrip.DownsamplingConfig.tenantResolutions) != low {
		t.Fatal("configuration round trip changed tenant count")
	}
}

func TestDownsamplingConfigSnapshots(t *testing.T) {
	initial := parseDownsamplingConfigForTest(t, `{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":["1h"]}]}`)
	updated := parseDownsamplingConfigForTest(t, `{"base_resolution":"5m","tenant_resolutions":[{"tenant":"2:0","resolutions":["30m","2h"]}]}`)
	s := &Storage{downsamplingEnabled: true}
	s.downsamplingConfig.Store(initial.clone())
	old := s.getDownsamplingConfig()
	if err := s.UpdateDownsamplingConfig(updated); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(old.ResolutionsForTenant(1, 0), []int64{300000, 3600000}) {
		t.Fatal("update changed an existing job snapshot")
	}
	public := s.GetDownsamplingConfig()
	if err := json.Unmarshal([]byte(`{"base_resolution":"1s"}`), public); err != nil {
		t.Fatal(err)
	}
	if s.GetDownsamplingConfig().BaseResolutionMs() != 300000 {
		t.Fatal("public config mutation changed storage")
	}
	if err := s.UpdateDownsamplingConfig(public); !errors.Is(err, errDownsamplingBaseChanged) {
		t.Fatalf("base change accepted: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Go(func() {
			for j := 0; j < 20; j++ {
				if err := s.UpdateDownsamplingConfig(initial); err != nil {
					t.Error(err)
				}
				snapshot := s.GetDownsamplingConfig()
				if snapshot.BaseResolutionMs() != 300000 || snapshot.MaxResolutionsPerTenant() < 1 {
					t.Error("invalid atomic snapshot")
				}
				if err := s.UpdateDownsamplingConfig(updated); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
}

func TestDownsamplingBasePersistsWithoutParts(t *testing.T) {
	path := t.TempDir()
	initial := parseDownsamplingConfigForTest(t, `{"base_resolution":"30s","tenant_resolutions":[{"tenant":"1:0","resolutions":["1h"]}]}`)
	if err := checkDownsamplingBase(path, initial); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(path); err != nil || len(entries) != 0 {
		t.Fatalf("base precheck modified storage: %v %v", entries, err)
	}
	oldDedup := GetDedupInterval()
	SetDedupInterval(0)
	defer SetDedupInterval(time.Duration(oldDedup) * time.Millisecond)
	s := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true, DownsamplingConfig: initial})
	defer func() {
		if s != nil {
			s.MustClose()
		}
	}()
	snapshot := s.MustCreateSnapshot()
	if err := checkDownsamplingBase(filepath.Join(path, snapshotsDirname, snapshot), initial); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(path, metadataDirname, downsamplingBaseFilename)
	before, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	updated := parseDownsamplingConfigForTest(t, `{"base_resolution":"30s","tenant_resolutions":[{"tenant":"2:0","resolutions":["2h"]}]}`)
	if err := s.UpdateDownsamplingConfig(updated); err != nil {
		t.Fatal(err)
	}
	s.MustClose()
	s = nil
	if err := checkDownsamplingBase(path, defaultDownsamplingConfig()); !errors.Is(err, errDownsamplingBaseChanged) {
		t.Fatalf("empty storage forgot base: %v", err)
	}
	if err := checkDownsamplingOpen(path, OpenOptions{DownsamplingEnabled: true}); !errors.Is(err, errDownsamplingBaseChanged) {
		t.Fatalf("storage precheck accepted a different base without parts: %v", err)
	}
	s = MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true, DownsamplingConfig: initial})
	if !reflect.DeepEqual(s.GetDownsamplingConfig().ResolutionsForTenant(1, 0), []int64{30000, 3600000}) || len(s.GetDownsamplingConfig().ResolutionsForTenant(2, 0)) != 1 {
		t.Fatal("restart did not restore startup tenant config")
	}
	s.MustClose()
	s = nil
	after, err := os.ReadFile(marker)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("mutable tenant updates changed persisted base: %v", err)
	}
}
