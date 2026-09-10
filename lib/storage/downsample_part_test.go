package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"math"
	"os"
	"path/filepath"
	"testing"
)

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
				if name != indexFilename {
					if _, err := detectDownsampleFormat(path); err == nil {
						t.Fatal("预检查未拒绝未知版本或格式标识")
					}
					if p, err := openDownsamplePart(path); err == nil {
						p.MustClose()
						t.Fatal("打开 part 时未拒绝未知版本或格式标识")
					}
					return
				}
				if p, err := openDownsamplePart(path); err == nil {
					p.MustClose()
					t.Fatal("打开 part 时未拒绝未知 index 格式标识")
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
