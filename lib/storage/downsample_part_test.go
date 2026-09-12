package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
)

func TestDownsamplePartOpenCancellation(t *testing.T) {
	stopCh := make(chan struct{})
	close(stopCh)
	if _, err := openDownsamplePart(filepath.Join(t.TempDir(), "missing"), stopCh); !errors.Is(err, errForciblyStopped) {
		t.Fatalf("open ignored cancellation before accessing files: %v", err)
	}
	input := sharedTimestampsTestBlock(10, downsampleResolution5m, minUnixMilli+1, 4, 64)
	path := writeFileTestDownsamplePart(t, input)
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	f, err := filestream.OpenReadAt(filepath.Join(path, indexFilename))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	stopCh = make(chan struct{})
	reader := &downsampleCancelIndexTestReader{ReadAtCloser: f, stopCh: stopCh}
	if err := validateDownsamplePartIndexes(p, reader, stopCh); !errors.Is(err, errForciblyStopped) {
		t.Fatalf("index validation ignored cancellation during IO: %v", err)
	}
	if reader.reads > countOfDownsampleFeatures {
		t.Fatalf("validation continued scanning indexes after cancellation: %d reads", reader.reads)
	}
}

type downsampleCancelIndexTestReader struct {
	filestream.ReadAtCloser
	stopCh chan struct{}
	reads  int
}

func (r *downsampleCancelIndexTestReader) ReadAt(dst []byte, offset int64) (int, error) {
	n, err := r.ReadAtCloser.ReadAt(dst, offset)
	r.reads++
	if r.reads == 1 {
		close(r.stopCh)
	}
	return n, err
}

func TestDownsampleFormatDetection(t *testing.T) {
	ph := partHeader{RowsCount: 5, BlocksCount: 5, MinTimestamp: 1735689600000, MaxTimestamp: 1735689600000}
	downsampleMetadata, err := json.Marshal(newDownsamplePartMetadata(ph, defaultDownsamplingConfig()))
	if err != nil {
		t.Fatal(err)
	}
	rawMetadata, err := json.Marshal(ph)
	if err != nil {
		t.Fatal(err)
	}
	withConfig := func(config string) []byte {
		return []byte(string(rawMetadata[:len(rawMetadata)-1]) + `,"downsampling_config":` + config + `}`)
	}
	const legacyPartName = "1_1_20250101000000.000_20250101000000.000_1"
	for _, tc := range []struct {
		name           string
		partName       string
		metadata       []byte
		wantDownsample bool
		wantErr        bool
	}{
		{name: "downsample_metadata_only", metadata: downsampleMetadata, wantDownsample: true},
		{name: "raw_metadata_only", metadata: rawMetadata},
		{name: "raw_null_config", metadata: withConfig("null")},
		{name: "empty_config", metadata: withConfig("{}"), wantErr: true},
		{name: "invalid_config_type", metadata: withConfig(`"5m"`), wantErr: true},
		{name: "legacy_raw_without_metadata", partName: legacyPartName},
		{name: "missing_metadata", wantErr: true},
		{name: "empty_metadata", metadata: []byte{}, wantErr: true},
		{name: "truncated_json", metadata: downsampleMetadata[:len(downsampleMetadata)-1], wantErr: true},
		{name: "invalid_json", metadata: []byte("not json"), wantErr: true},
		{name: "null_metadata", metadata: []byte("null"), wantErr: true},
		{name: "empty_object", metadata: []byte("{}"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			partName := tc.partName
			if partName == "" {
				partName = "part"
			}
			path := filepath.Join(t.TempDir(), partName)
			if err := os.Mkdir(path, 0755); err != nil {
				t.Fatal(err)
			}
			if tc.metadata != nil {
				if err := os.WriteFile(filepath.Join(path, metadataFilename), tc.metadata, 0644); err != nil {
					t.Fatal(err)
				}
			}
			isDownsample, err := detectDownsampleFormat(path)
			if isDownsample != tc.wantDownsample || (err != nil) != tc.wantErr {
				t.Fatalf("unexpected format detection: got (%t, %v); want (%t, error=%t)", isDownsample, err, tc.wantDownsample, tc.wantErr)
			}
			if err != nil && !strings.HasPrefix(err.Error(), "[downsampling] ") {
				t.Fatalf("missing downsampling error prefix: %v", err)
			}
			if isDownsample {
				if p, err := openDownsamplePart(path, nil); err == nil {
					p.MustClose()
					t.Fatal("opening a downsample part accepted missing data files")
				}
			}
		})
	}
}

func TestDownsampleMetadataValidation(t *testing.T) {
	t.Run("time_bounds", func(t *testing.T) {
		for _, timestamp := range []int64{minUnixMilli - 1, maxUnixMilli + 1} {
			m := newDownsamplePartMetadata(partHeader{RowsCount: 5, BlocksCount: 5, MinTimestamp: timestamp, MaxTimestamp: timestamp}, defaultDownsamplingConfig())
			if err := m.validate(); err == nil {
				t.Fatal("part metadata accepted timestamps outside the supported range")
			}
		}
	})
	for _, key := range []string{"RowsCount", "BlocksCount", "MinTimestamp", "MaxTimestamp", "MinDedupInterval"} {
		for _, missing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/missing=%t", key, missing), func(t *testing.T) {
				path := t.TempDir()
				m := newDownsamplePartMetadata(partHeader{RowsCount: 5, BlocksCount: 5}, defaultDownsamplingConfig())
				data, err := json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(data, &fields); err != nil {
					t.Fatal(err)
				}
				if missing {
					delete(fields, key)
				} else {
					fields[key] = json.RawMessage("null")
				}
				data, err = json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, metadataFilename), data, 0644); err != nil {
					t.Fatal(err)
				}
				if _, err := detectDownsampleFormat(path); err == nil {
					t.Fatal("format detection accepted incomplete metadata")
				}
			})
		}
	}
}

func TestDownsamplePartTenantResolutions(t *testing.T) {
	config := parseDownsamplingConfigForTest(t, `{"base_resolution":"30s","tenant_resolutions":[{"tenant":"1:0","resolutions":["1m","3m"]},{"tenant":"2:0","resolutions":["2m"]},{"tenant":"9:0","resolutions":["6m"]}]}`)
	for _, scenario := range []string{"sparse_valid", "missing_base_tsid", "unconfigured_extra", "missing_base_resolution"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part")
			var w downsampleWriter
			if err := w.Init(path, 1, config); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			write := func(tenant uint32, resolution int64, metricID uint64) {
				b := fileTestDownsampleBlock(metricID, resolution)
				b.tsid.AccountID = tenant
				b.timestamps = []int64{1735689600001}
				for feature := range b.values {
					b.values[feature] = b.values[feature][:1]
				}
				if err := writeDownsampleTestBlock(&w, b); err != nil {
					t.Fatal(err)
				}
			}
			baseMetric := uint64(1)
			if scenario == "missing_base_tsid" {
				baseMetric = 99
			}
			if scenario != "missing_base_resolution" {
				write(1, 30000, baseMetric)
				write(2, 30000, 1)
				write(3, 30000, 1)
			}
			write(1, 60000, 1)
			write(1, 180000, 1)
			if _, err := w.Finish(nil); err != nil {
				t.Fatal(err)
			}
			if scenario == "unconfigured_extra" {
				metadata, err := readDownsampleMetadata(path)
				if err != nil {
					t.Fatal(err)
				}
				metadata.DownsamplingConfig = config.forTenants(nil)
				data, err := json.Marshal(metadata)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, metadataFilename), data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			p, err := openDownsamplePart(path, nil)
			if scenario != "sparse_valid" {
				if err == nil {
					p.MustClose()
					t.Fatal("invalid resolution ownership or base coverage was accepted")
				}
				if scenario == "missing_base_tsid" && !strings.Contains(err.Error(), "no base data") {
					t.Fatalf("didn't reach base TSID validation: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			if !reflect.DeepEqual(downsamplePartResolutionsForTest(p), []int64{30000, 60000, 180000}) {
				t.Fatalf("metaindex does not list actual resolutions: %v", downsamplePartResolutionsForTest(p))
			}
			if len(p.dsMetadata.DownsamplingConfig.ResolutionsForTenant(9, 0)) != 1 {
				t.Fatal("part retained an unrelated tenant config")
			}
			if len(p.dsMetadata.DownsamplingConfig.ResolutionsForTenant(2, 0)) != 2 {
				t.Fatal("configured but empty extra was lost")
			}
			if got := p.downsampleResolutionsForTenant(1, 0); !reflect.DeepEqual(got, []int64{30000, 60000, 180000}) {
				t.Fatalf("stored tenant resolutions: %v", got)
			}
			if got := p.downsampleResolutionsForTenant(2, 0); !reflect.DeepEqual(got, []int64{30000}) {
				t.Fatalf("configured extra was mistaken for stored data: %v", got)
			}
			if got := p.downsampleResolutionsForTenant(9, 0); len(got) != 0 {
				t.Fatalf("missing tenant has stored resolutions: %v", got)
			}
		})
	}
}

func TestDownsampleRejectsInvalidMarker(t *testing.T) {
	for _, name := range []string{metaindexFilename, indexFilename} {
		t.Run(name, func(t *testing.T) {
			path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
			filePath := filepath.Join(path, name)
			data, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatal(err)
			}
			data[0] ^= 0xff
			if err := os.WriteFile(filePath, data, 0644); err != nil {
				t.Fatal(err)
			}
			isDownsample, err := detectDownsampleFormat(path)
			if err != nil || !isDownsample {
				t.Fatalf("format detection inspected the data files: got (%t, %v)", isDownsample, err)
			}
			if p, err := openDownsamplePart(path, nil); err == nil {
				p.MustClose()
				t.Fatal("opening a downsample part accepted an invalid file marker")
			}
		})
	}
}

func TestDownsampleLayoutMetaindexIdentityCorruption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"feature_out_of_range", func(m []byte) { m[64] = 5 }},
		{"feature_out_of_range_high", func(m []byte) { m[64] = 255 }},
		{"resolution", func(m []byte) { copy(m[65:73], encoding.MarshalInt64(nil, 1)) }},
		{"last_account", func(m []byte) { binary.BigEndian.PutUint32(m[73:77], 1) }},
		{"last_project", func(m []byte) { binary.BigEndian.PutUint32(m[77:81], 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeDownsampleCrossIndexPart(t)
			filename := filepath.Join(path, metaindexFilename)
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			meta := decodeDownsampleLayoutFrame(t, data, "VMDSMI")
			tc.mutate(meta[:clusterDownsampleMetaindexBytes])
			data = encoding.CompressZSTDLevel(append([]byte(nil), data[:8]...), meta, 1)
			if err := os.WriteFile(filename, data, 0644); err != nil {
				t.Fatal(err)
			}
			p, err := openDownsamplePart(path, nil)
			if err == nil {
				p.MustClose()
				t.Fatal("接受了损坏的 metaindex feature/resolution/租户")
			}
		})
	}
}

func TestEstimateDownsamplePartSize(t *testing.T) {
	config := parseDownsamplingConfigForTest(t, `{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":["30m","2h"]}]}`)
	raw := func(rows uint64) *partWrapper {
		return &partWrapper{p: &part{ph: partHeader{RowsCount: rows}, size: 1}}
	}
	summary := func(rows uint64) *partWrapper {
		return &partWrapper{p: &part{ph: partHeader{RowsCount: rows}, size: 1, dsMetadata: &downsamplePartMetadata{DownsamplingConfig: defaultDownsamplingConfig()}}}
	}
	if got := estimateDownsamplePartSize(nil, config); got != 0 {
		t.Fatalf("empty input reserves space: %d", got)
	}
	if got, want := estimateDownsamplePartSize([]*partWrapper{raw(10)}, config), downsampleSpaceBoundReference(30, 30); got != want {
		t.Fatalf("raw bound lost new tenant resolutions: got %d; want %d", got, want)
	}
	if got, want := estimateDownsamplePartSize([]*partWrapper{summary(6)}, config), downsampleSpaceBoundReference(6, 6); got != want {
		t.Fatalf("unknown summary rows were not conservatively rounded and expanded: got %d; want %d", got, want)
	}
	// The source has 100 physical rows in base and old extras, but only 10 base samples contribute to each new target resolution.
	stored := summary(100)
	stored.p.dsMetaindex = []downsampleMetaindexRow{{ResolutionMs: 300000, feature: downsampleFeatureLast, RowsCount: 10}, {ResolutionMs: 3600000, feature: downsampleFeatureLast, RowsCount: 10}}
	if got, want := estimateDownsamplePartSize([]*partWrapper{stored}, config), downsampleSpaceBoundReference(30, 30); got != want {
		t.Fatalf("source extras were counted as additional base rows: got %d; want %d", got, want)
	}
	mixed := []*partWrapper{raw(10), stored}
	if got, want := estimateDownsamplePartSize(mixed, config), downsampleSpaceBoundReference(60, 60); got != want {
		t.Fatalf("mixed source bound: got %d; want %d", got, want)
	}
	before := estimateDownsamplePartSize(mixed, config)
	for _, pw := range mixed {
		pw.p.size = math.MaxUint64
	}
	if after := estimateDownsamplePartSize(mixed, config); after != before {
		t.Fatalf("bound depends on compressed bytes: %d vs %d", before, after)
	}
	if got := estimateDownsamplePartSize([]*partWrapper{raw(0), summary(0)}, config); got != 0 {
		t.Fatalf("empty sources reserve metadata: %d", got)
	}
	for _, pws := range [][]*partWrapper{{raw(math.MaxUint64)}, {summary(math.MaxUint64)}, {summary(math.MaxUint64 / 2), summary(math.MaxUint64/2 + 2)}, {nil}, {{}}} {
		if got := estimateDownsamplePartSize(pws, config); got != math.MaxUint64 {
			t.Fatalf("invalid or overflowing input didn't saturate: %d", got)
		}
	}
}

// 打开校验按原生 header 对齐五列，不能假设不同 feature 的 index 分块边界相同。
func TestDownsamplePartIndependentFeatureIndexes(t *testing.T) {
	path := writeDownsampleCrossIndexPart(t)
	p, err := openDownsamplePart(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := append([]downsampleMetaindexRow(nil), p.dsMetaindex...)
	p.MustClose()
	indexFile, err := os.ReadFile(filepath.Join(path, indexFilename))
	if err != nil {
		t.Fatal(err)
	}
	var rewrittenIndex, rewrittenMeta []byte
	for i := 0; i < len(rows); i++ {
		m := rows[i]
		frame := indexFile[m.IndexBlockOffset : m.IndexBlockOffset+uint64(m.IndexBlockSize)]
		data := decodeDownsampleLayoutFrame(t, frame, "VMDSIX")
		if m.feature == downsampleFeatureSum && m.TSID.MetricID == 1 {
			// 仅 sum 合并前两个 index，其余四个 feature 仍是一条 header 一个 index。
			i++
			next := &rows[i]
			frame := indexFile[next.IndexBlockOffset : next.IndexBlockOffset+uint64(next.IndexBlockSize)]
			data = append(data, decodeDownsampleLayoutFrame(t, frame, "VMDSIX")...)
			m.LastTSID = next.LastTSID
			m.RowsCount += next.RowsCount
			m.BlockHeadersCount += next.BlockHeadersCount
			m.MinTimestamp = min(m.MinTimestamp, next.MinTimestamp)
			m.MaxTimestamp = max(m.MaxTimestamp, next.MaxTimestamp)
		}
		frame = encoding.CompressZSTDLevel([]byte(downsampleIndexMagic), data, 1)
		m.IndexBlockOffset = uint64(len(rewrittenIndex))
		m.IndexBlockSize = uint32(len(frame))
		rewrittenIndex = append(rewrittenIndex, frame...)
		rewrittenMeta = m.marshal(rewrittenMeta)
	}
	if err := os.WriteFile(filepath.Join(path, indexFilename), rewrittenIndex, 0644); err != nil {
		t.Fatal(err)
	}
	meta := encoding.CompressZSTDLevel([]byte(downsampleMetaindexMagic), rewrittenMeta, 1)
	if err := os.WriteFile(filepath.Join(path, metaindexFilename), meta, 0644); err != nil {
		t.Fatal(err)
	}
	p, err = openDownsamplePart(path, nil)
	if err != nil {
		t.Fatalf("valid feature columns with independent index boundaries were rejected: %v", err)
	}
	defer p.MustClose()
	if len(p.dsMetaindex) != len(rows)-len(downsampleResolutions) {
		t.Fatal("fixture did not change the sum feature index boundaries")
	}
}

// 查询缓存及打开校验共用原生 header 解码；失败不能把半个 index 暴露给调用方。
func TestUnmarshalDownsampleIndexBlock(t *testing.T) {
	first := blockHeader{
		TSID:         TSID{AccountID: 1, ProjectID: 2, MetricID: 3},
		MinTimestamp: minUnixMilli + 1, MaxTimestamp: minUnixMilli + 1,
		RowsCount: 1, PrecisionBits: 64,
		TimestampsMarshalType: encoding.MarshalTypeConst,
		ValuesMarshalType:     encoding.MarshalTypeConst,
	}
	last := first
	last.TSID.MetricID++
	last.MinTimestamp++
	last.MaxTimestamp++
	m := downsampleMetaindexRow{
		metaindexRow: metaindexRow{TSID: first.TSID, MinTimestamp: first.MinTimestamp, MaxTimestamp: last.MaxTimestamp, BlockHeadersCount: 2, IndexBlockSize: 16},
		ResolutionMs: downsampleResolution5m, LastTSID: last.TSID, RowsCount: 2,
	}
	prefix := blockHeader{TSID: TSID{MetricID: 99}}
	data := last.Marshal(first.Marshal(nil))
	got, err := unmarshalDownsampleIndexBlock([]blockHeader{prefix}, data, &m, 0, 0, 16)
	if err != nil || !reflect.DeepEqual(got, []blockHeader{prefix, first, last}) {
		t.Fatalf("index headers changed when appended to an existing cache buffer: %+v; %v", got, err)
	}
	for _, name := range []string{"truncated", "invalid_last_header", "statistics_mismatch"} {
		t.Run(name, func(t *testing.T) {
			corrupt := append([]byte(nil), data...)
			metadata := m
			switch name {
			case "truncated":
				corrupt = corrupt[:len(corrupt)-1]
			case "invalid_last_header":
				// 保留前一条合法 header，令后一条的精度非法。
				corrupt[len(corrupt)-1] = 0
			case "statistics_mismatch":
				metadata.RowsCount++
			}
			dst := make([]blockHeader, 1, 3)
			dst[0] = prefix
			got, err := unmarshalDownsampleIndexBlock(dst, corrupt, &metadata, 0, 0, 16)
			if err == nil || !reflect.DeepEqual(got, []blockHeader{prefix}) {
				t.Fatalf("invalid index exposed partial headers or changed the existing cache prefix: %+v; %v", got, err)
			}
		})
	}
}
