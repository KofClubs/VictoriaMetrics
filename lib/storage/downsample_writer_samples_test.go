package storage

import (
	"errors"
	"math"
	"path/filepath"
	"strconv"
	"testing"
)

func TestDownsampleWriterSamplesPreserveInput(t *testing.T) {
	const base int64 = 1704067200000
	for _, precision := range []uint8{8, 64} {
		t.Run(strconv.Itoa(int(precision)), func(t *testing.T) {
			tsid := TSID{MetricID: 7}
			decoded := &downsampleDecodedResolutionFeaturesBlock{tsid: tsid, resolution: downsampleResolution5m, precisionBits: precision}
			samples := make([]downsampleSample, 2*24+3)
			for row := 0; row < 24; row++ {
				sample := &samples[2*row+2]
				sample.timestamp = base + int64(2*row)*downsampleResolution5m + int64(row*row*137+1)
				sample.precisionBits = precision
				decoded.timestamps = append(decoded.timestamps, sample.timestamp)
				for feature := range sample.values {
					value := float64((row*row*17+feature*101)%104729) / float64((feature+1)*128)
					if row == 3 && feature == downsampleFeatureSum {
						value = math.Float64frombits(0xfff8000000001234)
					}
					sample.values[feature] = value
					decoded.values[feature] = append(decoded.values[feature], normalizeDownsampleValue(value))
				}
			}
			original := append([]downsampleSample(nil), samples...)
			want := downsampleTestBlockRows(roundTripDownsampleTestReferenceBlock(t, decoded))
			var w downsampleWriter
			path := filepath.Join(t.TempDir(), "part")
			if err := w.Init(path, -5); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			// 没有有效样本时不产生 block，也不能影响后续同一 writer 的输出。
			for _, empty := range [][]downsampleSample{nil, make([]downsampleSample, 3)} {
				if err := w.WriteSamples(&tsid, downsampleResolution5m, empty, nil); err != nil {
					t.Fatal(err)
				}
			}
			if w.ph.RowsCount != 0 || w.ph.BlocksCount != 0 || w.hasPrevious {
				t.Fatal("empty samples changed the output")
			}
			if err := w.WriteSamples(&tsid, downsampleResolution5m, samples, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			for i, sample := range samples {
				before := &original[i]
				if sample.timestamp != before.timestamp || sample.precisionBits != before.precisionBits {
					t.Fatalf("sample %d timestamp or precision was modified", i)
				}
				for feature, value := range sample.values {
					if math.Float64bits(value) != math.Float64bits(before.values[feature]) {
						t.Fatalf("sample %d feature %d was modified", i, feature)
					}
				}
			}
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			assertDownsampleTestRows(t, readDownsampleTestPart(t, p), want)
		})
	}
}

func TestDownsampleWriterSamplesLaterBlockFailure(t *testing.T) {
	const base int64 = 1704067200000
	for _, scenario := range []string{"write", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			tsid := TSID{MetricID: 7}
			samples := make([]downsampleSample, 3)
			for i, precision := range []uint8{64, 8, 64} {
				samples[i] = downsampleSample{
					timestamp: base + int64(i)*downsampleResolution5m + 1,
					values:    [countOfDownsampleFeatures]float64{2, 2, 1, 2, 2}, precisionBits: precision,
				}
			}
			var w downsampleWriter
			path := filepath.Join(t.TempDir(), "part")
			if err := w.Init(path, -5); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			stopCh := make(chan struct{})
			cause := errors.New("injected later block write failure")
			writes := 0
			w.timestampsWriter = &downsampleCloseTestWriter{downsampleFileWriter: w.timestampsWriter, beforeWrite: func() error {
				writes++
				if writes != 2 {
					return nil
				}
				if w.ph.BlocksCount != countOfDownsampleFeatures {
					t.Fatal("failure did not occur after a completed output block")
				}
				if scenario == "cancel" {
					close(stopCh)
					return nil
				}
				return cause
			}}
			err := w.WriteSamples(&tsid, downsampleResolution5m, samples, stopCh)
			if scenario == "cancel" {
				cause = errForciblyStopped
			}
			if !errors.Is(err, cause) || writes != 2 {
				t.Fatalf("later block failure was missed: writes=%d; err=%v", writes, err)
			}
			assertDownsampleWriterAborted(t, &w, path, cause)
		})
	}
}
