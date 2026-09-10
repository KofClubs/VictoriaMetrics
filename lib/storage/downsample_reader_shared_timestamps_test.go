package storage

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func sharedTimestampsTestBlock(tsid uint64, resolution, base int64, rows int, precision uint8) *downsampleDecodedResolutionFeaturesBlock {
	b := &downsampleDecodedResolutionFeaturesBlock{
		tsid: TSID{MetricID: tsid}, resolution: resolution, precisionBits: precision,
	}
	for i := 0; i < rows; i++ {
		b.timestamps = append(b.timestamps, base+int64(i)*resolution+int64(i*i*7919)%(resolution-1))
		for feature := range b.values {
			v := float64((i*i*17+feature*101)%104729) / float64((feature+1)*128)
			if feature == downsampleFeatureCount {
				v = 3 // 同时覆盖无需读取 values payload 的常量列。
			}
			b.values[feature] = append(b.values[feature], v)
		}
	}
	return b
}

// 首列之后关闭时间戳文件，使任何重复读取立即失败；原生 Block 中仅保留已解码时间戳。
func TestDownsampleReaderSharedTimestampsValuesOnly(t *testing.T) {
	for _, precision := range []uint8{64, 8} {
		t.Run(fmt.Sprintf("precision_%d", precision), func(t *testing.T) {
			input := sharedTimestampsTestBlock(10, downsampleResolution5m, minUnixMilli+1, 512, precision)
			path := writeFileTestDownsamplePart(t, input)
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			var r downsampleReader
			defer r.Close()
			if err := r.Init(p, input.resolution); err != nil || !r.NextHeader() {
				t.Fatalf("cannot position reader: %v / %v", err, r.Error())
			}
			shared := *r.Header()
			if shared.TimestampsBlockSize == 0 || shared.TimestampsMarshalType == encoding.MarshalTypeConst {
				t.Fatal("fixture must require timestamp payload decoding")
			}
			var columns [countOfDownsampleFeatures]blockHeader
			var expected [countOfDownsampleFeatures]Block
			for feature := range columns {
				columns[feature], err = r.FieldHeader(uint8(feature))
				if err != nil {
					t.Fatal(err)
				}
				if err := r.ReadFieldBlock(&expected[feature], uint8(feature)); err != nil {
					t.Fatal(err)
				}
			}
			// 使用独立句柄，关闭后不影响 part 的引用生命周期和其它 reader。
			f, err := os.Open(filepath.Join(path, timestampsFilename))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			r.timestampsReader = f
			var got Block
			if err := r.ReadFieldBlock(&got, downsampleFeatureLast); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if len(got.timestampsData) != 0 {
				t.Fatal("native Block did not finish timestamp decoding")
			}
			firstTimestamp := &got.timestamps[0]
			for feature := 1; feature < countOfDownsampleFeatures; feature++ {
				if err := r.readNativeValues(&got, &columns[feature], &shared); err != nil {
					t.Fatalf("feature %d tried to read or decode timestamps again: %v", feature, err)
				}
				want := &expected[feature]
				if &got.timestamps[0] != firstTimestamp || !reflect.DeepEqual(got.timestamps, want.timestamps) || !reflect.DeepEqual(got.values, want.values) {
					t.Fatalf("feature %d did not reuse timestamps with the correct native values", feature)
				}
				if got.bh.Scale != want.bh.Scale || got.bh.PrecisionBits != precision || got.bh.TimestampsMarshalType != want.bh.TimestampsMarshalType || got.bh.TimestampsBlockSize != want.bh.TimestampsBlockSize || len(got.timestampsData) != 0 || len(got.valuesData) != 0 || got.nextIdx != 0 {
					t.Fatalf("feature %d lost the native decoded Block state", feature)
				}
			}
			// 查询的单列读取仍完整读取时间戳，不借用另一次操作留下的缓存。
			if err := r.ReadFieldBlock(&got, downsampleFeatureMax); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("ReadFieldBlock unexpectedly reused timestamps: %v", err)
			}
			// 下一次多特征读取也必须重新读取首列，不能跨 ReadBlock 缓存。
			var next downsampleDecodedResolutionFeaturesBlock
			if err := r.ReadBlock(&next); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("ReadBlock unexpectedly reused a previous block's timestamps: %v", err)
			}
		})
	}
}

func TestDownsampleReaderSharedTimestampsSwitches(t *testing.T) {
	var parts []*part
	for source, precision := range []uint8{64, 8} {
		base := minUnixMilli + 1 + int64(source)*5*24*3600000
		var inputs []*downsampleDecodedResolutionFeaturesBlock
		for _, resolution := range downsampleResolutions {
			inputs = append(inputs,
				sharedTimestampsTestBlock(1, resolution, base, 19, precision),
				sharedTimestampsTestBlock(1, resolution, base+25*resolution, 7, precision),
				sharedTimestampsTestBlock(2, resolution, base+3*resolution, 1, precision),
			)
		}
		p, err := openDownsamplePart(writeFileTestDownsamplePart(t, inputs...))
		if err != nil {
			t.Fatal(err)
		}
		defer p.MustClose()
		parts = append(parts, p)
	}
	var r, query downsampleReader
	defer r.Close()
	defer query.Close()
	var got downsampleDecodedResolutionFeaturesBlock
	// 同源切换分辨率、跨源切换，最后重新读取第一个源，均复用同一组工作缓冲。
	for _, step := range []struct {
		source     int
		resolution int64
	}{{0, downsampleResolution5m}, {0, downsampleResolution1h}, {1, downsampleResolution5m}, {1, downsampleResolution1h}, {0, downsampleResolution5m}} {
		p := parts[step.source]
		if err := r.Init(p, step.resolution); err != nil {
			t.Fatal(err)
		}
		if err := query.Init(p, step.resolution); err != nil {
			t.Fatal(err)
		}
		blocks := 0
		for r.NextHeader() {
			if !query.NextHeader() {
				t.Fatalf("query lost block: %v", query.Error())
			}
			if err := r.ReadBlock(&got); err != nil {
				t.Fatal(err)
			}
			h := query.Header()
			if got.tsid != h.TSID || got.resolution != step.resolution || got.precisionBits != h.PrecisionBits {
				t.Fatal("block identity was reused across TSID, source or resolution")
			}
			for feature := range got.values {
				var want Block
				if err := query.ReadFieldBlock(&want, uint8(feature)); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got.timestamps, want.timestamps) {
					t.Fatalf("block %d feature %d reused different timestamps", blocks, feature)
				}
				values := decimal.AppendDecimalToFloat(nil, want.values, want.bh.Scale)
				if len(got.values[feature]) != len(values) {
					t.Fatal("decoded values count changed")
				}
				for row, v := range values {
					if actual := got.values[feature][row]; actual != v && !(math.IsNaN(actual) && math.IsNaN(v)) {
						t.Fatalf("block %d feature %d row %d: got %v; want %v", blocks, feature, row, actual, v)
					}
				}
			}
			blocks++
		}
		if err := r.Error(); err != nil {
			t.Fatal(err)
		}
		if query.NextHeader() || query.Error() != nil || blocks != 3 {
			t.Fatalf("lost blocks after switching: got %d; query error %v", blocks, query.Error())
		}
	}
}

func TestDownsampleReaderSharedTimestampsValidation(t *testing.T) {
	input := sharedTimestampsTestBlock(1, downsampleResolution5m, minUnixMilli+1, 8, 64)
	p, err := openDownsamplePart(writeFileTestDownsamplePart(t, input))
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	var r downsampleReader
	defer r.Close()
	if err := r.Init(p, input.resolution); err != nil || !r.NextHeader() {
		t.Fatalf("cannot position reader: %v / %v", err, r.Error())
	}
	shared := *r.Header()
	column, err := r.FieldHeader(downsampleFeatureSum)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*blockHeader){
		"timestamp_offset": func(h *blockHeader) { h.TimestampsBlockOffset++ },
		"timestamp_size":   func(h *blockHeader) { h.TimestampsBlockSize++ },
		"timestamp_codec":  func(h *blockHeader) { h.TimestampsMarshalType = encoding.MarshalTypeConst },
		"timestamp_bounds": func(h *blockHeader) { h.MaxTimestamp++ },
		"rows":             func(h *blockHeader) { h.RowsCount++ },
		"tsid":             func(h *blockHeader) { h.TSID.MetricID++ },
		"precision":        func(h *blockHeader) { h.PrecisionBits = 8 },
		"invalid_precision": func(h *blockHeader) {
			h.PrecisionBits = 0
		},
		"values_codec": func(h *blockHeader) { h.ValuesMarshalType = 255 },
		"values_extent": func(h *blockHeader) {
			h.ValuesBlockOffset = r.valuesSize
			h.ValuesBlockSize = 1
		},
		"values_payload": func(h *blockHeader) {
			h.ValuesMarshalType = encoding.MarshalTypeNearestDelta
			h.ValuesBlockSize = 0
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			var b Block
			if err := r.ReadFieldBlock(&b, downsampleFeatureLast); err != nil {
				t.Fatal(err)
			}
			h := column
			mutate(&h)
			if err := r.readNativeValues(&b, &h, &shared); err == nil {
				t.Fatal("values-only path accepted an invalid header or payload")
			}
		})
	}
	// 完整多特征读取仍检查每一列的共享描述，而不是只检查第一列。
	r.peers[downsampleFeatureSum].current.TimestampsBlockOffset++
	var got downsampleDecodedResolutionFeaturesBlock
	if err := r.ReadBlock(&got); err == nil {
		t.Fatal("ReadBlock accepted a later feature with different shared timestamps")
	}
}
