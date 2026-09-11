package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func TestDownsampleFormatDetection(t *testing.T) {
	ph := partHeader{RowsCount: 5, BlocksCount: 5, MinTimestamp: 1735689600000, MaxTimestamp: 1735689600000}
	downsampleMetadata, err := json.Marshal(newDownsamplePartMetadata(ph))
	if err != nil {
		t.Fatal(err)
	}
	rawMetadata, err := json.Marshal(ph)
	if err != nil {
		t.Fatal(err)
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
				if p, err := openDownsamplePart(path); err == nil {
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
			m := newDownsamplePartMetadata(partHeader{RowsCount: 5, BlocksCount: 5, MinTimestamp: timestamp, MaxTimestamp: timestamp})
			if err := m.validate(); err == nil {
				t.Fatal("part metadata accepted timestamps outside the supported range")
			}
		}
	})
	for _, key := range []string{"FormatVersion", "SemanticsVersion", "Mode", "Resolutions", "BucketOrigin", "NumericCodec", "Retention", "RowsCount", "BlocksCount", "MinTimestamp", "MaxTimestamp", "MinDedupInterval"} {
		t.Run(key, func(t *testing.T) {
			path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
			metaPath := filepath.Join(path, metadataFilename)
			data, err := os.ReadFile(metaPath)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			delete(fields, key)
			data, err = json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(metaPath, data, 0644); err != nil {
				t.Fatal(err)
			}
			if _, err := detectDownsampleFormat(path); err == nil {
				t.Fatal("未拒绝缺少元数据字段")
			}
		})
	}
}

func TestDownsampleRejectsUnknownVersionAndMarker(t *testing.T) {
	for _, field := range []string{"FormatVersion", "SemanticsVersion", metaindexFilename, indexFilename} {
		for _, version := range []int{0, 255} {
			t.Run(fmt.Sprintf("%s_%d", field, version), func(t *testing.T) {
				path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, 300000))
				name := field
				if field == "FormatVersion" || field == "SemanticsVersion" {
					name = metadataFilename
				}
				filePath := filepath.Join(path, name)
				data, err := os.ReadFile(filePath)
				if err != nil {
					t.Fatal(err)
				}
				if name == metadataFilename {
					var metadata map[string]any
					if err := json.Unmarshal(data, &metadata); err != nil {
						t.Fatal(err)
					}
					metadata[field] = version
					data, err = json.Marshal(metadata)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					data[7] = byte(version)
				}
				if err := os.WriteFile(filePath, data, 0644); err != nil {
					t.Fatal(err)
				}
				isDownsample, err := detectDownsampleFormat(path)
				if name == metadataFilename {
					if err == nil {
						t.Fatal("format detection accepted an unknown metadata version")
					}
				} else if err != nil || !isDownsample {
					t.Fatalf("format detection inspected the data files: got (%t, %v)", isDownsample, err)
				}
				if p, err := openDownsamplePart(path); err == nil {
					p.MustClose()
					t.Fatal("opening a downsample part accepted an unknown version or format marker")
				}
			})
		}
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
			p, err := openDownsamplePart(path)
			if err == nil {
				p.MustClose()
				t.Fatal("接受了损坏的 metaindex feature/resolution/租户")
			}
		})
	}
}

func TestEstimateDownsamplePartSize(t *testing.T) {
	raw := func(rows uint64) *partWrapper {
		return &partWrapper{p: &part{ph: partHeader{RowsCount: rows}, size: 1}}
	}
	summary := func(rows uint64) *partWrapper {
		return &partWrapper{p: &part{ph: partHeader{RowsCount: rows}, size: 1, dsMetadata: &downsamplePartMetadata{}}}
	}
	if got := estimateDownsamplePartSize(nil); got != 0 {
		t.Fatalf("empty input must not reserve output space; got %d", got)
	}
	if got := estimateDownsamplePartSize([]*partWrapper{raw(10)}); got != estimateDownsamplePartSize([]*partWrapper{summary(100)}) {
		t.Fatalf("raw rows must reserve both resolutions and five feature Blocks; got %d", got)
	}
	if got, want := estimateDownsamplePartSize([]*partWrapper{raw(10)}), downsampleSpaceBoundReference(20, 20); got != want {
		t.Fatalf("raw input must reserve two resolutions including spills: got %d; want %d", got, want)
	}
	if got, want := estimateDownsamplePartSize([]*partWrapper{summary(6)}), downsampleSpaceBoundReference(2, 2); got != want {
		t.Fatalf("partial physical row groups must round up: got %d; want %d", got, want)
	}
	if got := estimateDownsamplePartSize([]*partWrapper{raw(0), summary(0)}); got != 0 {
		t.Fatalf("zero-row sources must not reserve metadata: got %d", got)
	}
	mixed := []*partWrapper{raw(10), summary(100)}
	if got, want := estimateDownsamplePartSize(mixed), estimateDownsamplePartSize([]*partWrapper{summary(200)}); got != want {
		t.Fatalf("mixed input row bounds were not combined: got %d; want %d", got, want)
	}
	before := estimateDownsamplePartSize(mixed)
	for _, pw := range mixed {
		pw.p.size = math.MaxUint64
	}
	if after := estimateDownsamplePartSize(mixed); after != before {
		t.Fatalf("the output bound depends on compressed source bytes: before=%d; after=%d", before, after)
	}
	for _, pws := range [][]*partWrapper{
		{raw(math.MaxUint64)},
		{summary(math.MaxUint64)},
		{summary(math.MaxUint64 / 2), summary(math.MaxUint64/2 + 2)},
		{nil},
		{{}},
	} {
		if got := estimateDownsamplePartSize(pws); got != math.MaxUint64 {
			t.Fatalf("overflow or invalid input must saturate the estimate: got %d", got)
		}
	}
}

// 打开校验按原生 header 对齐五列，不能假设不同 feature 的 index 分块边界相同。
func TestDownsamplePartIndependentFeatureIndexes(t *testing.T) {
	path := writeDownsampleCrossIndexPart(t)
	p, err := openDownsamplePart(path)
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
	p, err = openDownsamplePart(path)
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
