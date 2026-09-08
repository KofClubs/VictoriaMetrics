package storage

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

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

func TestDownsampleSpaceBoundCoversEncodedParts(t *testing.T) {
	for _, fragmented := range []bool{false, true} {
		name := "full_block"
		if fragmented {
			name = "one_row_per_series"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part")
			w := getDownsampleWriter()
			defer putDownsampleWriter(w)
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			blockCount, rowsPerBlock := 1, maxRowsPerBlock
			if fragmented {
				blockCount, rowsPerBlock = 100, 1
			}
			for i := 0; i < blockCount; i++ {
				b := downsampleBatch{tsid: TSID{MetricID: uint64(i + 1)}, resolution: downsampleResolution5m, timestampPrecisionBits: 64}
				for j := 0; j < rowsPerBlock; j++ {
					b.timestamps = append(b.timestamps, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()+int64(j)*downsampleResolution5m)
					for feature := range b.values {
						value := float64((uint64(j+feature+1) * 0x9e3779b97f4a7c15) >> 12)
						if j%2 == 0 {
							value = -value
						}
						b.values[feature] = append(b.values[feature], value)
						b.precisionBits[feature] = 64
					}
				}
				if err := w.WriteBlock(&b); err != nil {
					t.Fatal(err)
				}
			}
			ph, err := w.Finish()
			if err != nil {
				t.Fatal(err)
			}
			var encodedSize uint64
			entries, err := os.ReadDir(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				info, err := entry.Info()
				if err != nil {
					t.Fatal(err)
				}
				encodedSize += uint64(info.Size())
			}
			bound := estimateDownsamplePartSize([]*partWrapper{{p: &part{ph: ph, dsMetadata: &downsamplePartMetadata{}}}})
			if encodedSize > bound {
				t.Fatalf("encoded part exceeds the row-derived space bound: got %d; bound %d", encodedSize, bound)
			}
		})
	}
}

func TestReserveDownsampleSpaceConcurrentAndCachedFreeSpace(t *testing.T) {
	var budget downsampleSpaceBudget
	now := time.Unix(100, 0)
	available := uint64(1000)
	getFree := func(string) uint64 { return available }
	getNow := func() time.Time { return now }
	results := make(chan func(), 32)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			release, err := reserveDownsampleSpaceWithBudget(&budget, "same-filesystem", 100, 0, getFree, getNow)
			if err != nil {
				if !errors.Is(err, errDownsampleNoSpace) {
					t.Errorf("unexpected reservation failure: %s", err)
				}
				return
			}
			results <- release
		})
	}
	wg.Wait()
	close(results)
	if len(results) != 10 {
		t.Fatalf("concurrent reservations oversubscribed or underused the same free-space sample: got %d; want 10", len(results))
	}
	for release := range results {
		wg.Go(release)
		wg.Go(release)
	}
	wg.Wait()
	if budget.reserved != 0 || budget.retiredBytes != 1000 {
		t.Fatalf("reservation release was not idempotent: active=%d; awaiting cache refresh=%d", budget.reserved, budget.retiredBytes)
	}
	if _, err := reserveDownsampleSpaceWithBudget(&budget, "another-directory", 1, 0, getFree, getNow); !errors.Is(err, errDownsampleNoSpace) {
		t.Fatalf("released budget was reused before the cached free-space sample could refresh: %v", err)
	}
	now = now.Add(downsampleSpaceCacheLifetime - time.Nanosecond)
	if _, err := reserveDownsampleSpaceWithBudget(&budget, "another-directory", 1, 0, getFree, getNow); !errors.Is(err, errDownsampleNoSpace) {
		t.Fatalf("released budget expired before the full free-space cache interval: %v", err)
	}
	now = now.Add(time.Nanosecond)
	// 新读数包含旧源和刚写入的目标占用；释放预算并不意味着删除这些文件。
	available = 550
	if _, err := reserveDownsampleSpaceWithBudget(&budget, "another-directory", 551, 0, getFree, getNow); !errors.Is(err, errDownsampleNoSpace) {
		t.Fatalf("reservation ignored the refreshed free-space value: %v", err)
	}
	release, err := reserveDownsampleSpaceWithBudget(&budget, "another-directory", 550, 0, getFree, getNow)
	if err != nil {
		t.Fatalf("expired cache debt did not release the remaining capacity: %s", err)
	}
	release()
}

func TestDownsampleAvailableSpaceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		available, held, requested, minimumFree uint64
		ok                                      bool
	}{
		{100, 40, 50, 10, true},
		{100, 40, 51, 10, false},
		{99, 0, 0, 100, false},
		{100, 101, 0, 0, false},
		{math.MaxUint64, math.MaxUint64, 1, 0, false},
		{math.MaxUint64, 0, 1, math.MaxUint64, false},
		{math.MaxUint64, 0, math.MaxUint64, 0, true},
	} {
		err := checkDownsampleAvailableSpace(tc.available, tc.held, tc.requested, tc.minimumFree)
		if tc.ok && err != nil {
			t.Fatalf("valid space budget was rejected: %+v: %s", tc, err)
		}
		if !tc.ok && !errors.Is(err, errDownsampleNoSpace) {
			t.Fatalf("invalid space budget did not return the space sentinel: %+v: %v", tc, err)
		}
	}
	for _, rows := range []int{-1, maxRowsPerBlock + 1} {
		if err := checkDownsampleWriteSpace("unused", rows); err == nil {
			t.Fatalf("invalid block row count %d was accepted", rows)
		}
	}
}
