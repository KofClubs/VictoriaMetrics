package storage

import (
	"errors"
	"math"
	"math/big"
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

// Use independent cluster-format constants and arbitrary precision so the oracle
// cannot repeat a missing feature multiplier or uint64 wrap in production code.
func downsampleSpaceBoundReference(rows, blocks uint64) uint64 {
	// Final payload: 60 bytes/row; spill values: 50 bytes/row.
	// Each batch has five independent index frames, metaindex rows and spill headers.
	const bytesPerBatch = 5 * ((2*89 + 256 + 8) + (2*113 + 256 + 8) + 89)
	total := new(big.Int).Mul(new(big.Int).SetUint64(rows), big.NewInt(110))
	total.Add(total, new(big.Int).Mul(new(big.Int).SetUint64(blocks), big.NewInt(bytesPerBatch)))
	total.Add(total, big.NewInt(64<<10))
	if !total.IsUint64() {
		return math.MaxUint64
	}
	return total.Uint64()
}

func TestEstimateDownsampleOutputSize(t *testing.T) {
	if marshaledBlockHeaderSize != 89 || downsampleMetaindexRowSize != 113 || countOfDownsampleFeatures != 5 {
		t.Fatal("space oracle requires the current cluster 89/113-byte five-column layout")
	}
	for _, tc := range []struct {
		name         string
		rows, blocks uint64
	}{
		{"metadata_only", 0, 0},
		{"one_batch_five_indexes", 1, 1},
		{"full_batch", maxRowsPerBlock, 1},
		{"fragmented_batches", 100, 100},
		{"multiple_indexes", 738 * maxRowsPerBlock, 738},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := estimateDownsampleOutputSize(tc.rows, tc.blocks), downsampleSpaceBoundReference(tc.rows, tc.blocks); got != want {
				t.Fatalf("five-column output plus spill bound: got %d; want %d", got, want)
			}
		})
	}
	// One extra batch needs five index/metaindex pairs and five spill headers,
	// even when the logical row count is unchanged.
	if got := estimateDownsampleOutputSize(100, 2) - estimateDownsampleOutputSize(100, 1); got != 5105 {
		t.Fatalf("independent per-column index/metaindex and spill headers: got %d; want 5105", got)
	}
	if got := estimateDownsampleOutputSize(101, 1) - estimateDownsampleOutputSize(100, 1); got != 110 {
		t.Fatalf("one shared timestamp plus final/spilled values: got %d; want 110", got)
	}
}

func TestDownsampleSpaceEstimateOverflow(t *testing.T) {
	const bytesPerBatch = 5105
	rowLimit := (uint64(math.MaxUint64) - (64 << 10)) / 110
	blockLimit := (uint64(math.MaxUint64) - (64 << 10)) / bytesPerBatch
	for _, tc := range []struct {
		name         string
		rows, blocks uint64
	}{
		{"rows_before_saturation", rowLimit, 0},
		{"rows_after_saturation", rowLimit + 1, 0},
		{"blocks_before_saturation", 0, blockLimit},
		{"blocks_after_saturation", 0, blockLimit + 1},
		{"combined_addition", rowLimit, 1},
		{"payload_multiplication", math.MaxUint64/60 + 1, 0},
		{"physical_blocks_multiplication", 0, math.MaxUint64/5 + 1},
		{"maximum_inputs", math.MaxUint64, math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := estimateDownsampleOutputSize(tc.rows, tc.blocks), downsampleSpaceBoundReference(tc.rows, tc.blocks); got != want {
				t.Fatalf("space estimate wrapped or saturated early: got %d; want %d", got, want)
			}
		})
	}
	for _, tc := range []struct {
		a, b, sum, product uint64
	}{
		{0, math.MaxUint64, math.MaxUint64, 0},
		{math.MaxUint64, 0, math.MaxUint64, 0},
		{math.MaxUint64 - 1, 1, math.MaxUint64, math.MaxUint64 - 1},
		{math.MaxUint64, 1, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64/2 + 1, 2, math.MaxUint64/2 + 3, math.MaxUint64},
	} {
		if got := addDownsampleSpace(tc.a, tc.b); got != tc.sum {
			t.Fatalf("saturating add(%d, %d): got %d; want %d", tc.a, tc.b, got, tc.sum)
		}
		if got := multiplyDownsampleSpace(tc.a, tc.b); got != tc.product {
			t.Fatalf("saturating multiply(%d, %d): got %d; want %d", tc.a, tc.b, got, tc.product)
		}
	}
}

func TestDownsampleSpaceBoundCoversEncodedParts(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		blockCount, rowsPerBlock int
		indexLimit               int
	}{
		{"full_block", 1, maxRowsPerBlock, 0},
		{"one_row_per_series", 100, 1, 0},
		{"five_independent_indexes", 7, 17, marshaledBlockHeaderSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part")
			w := getDownsampleWriter()
			defer putDownsampleWriter(w)
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			w.indexLimit = tc.indexLimit
			var timestampBytes, valuesBytes uint64
			for i := 0; i < tc.blockCount; i++ {
				b := downsampleBatch{tsid: TSID{MetricID: uint64(i + 1)}, resolution: downsampleResolution5m, precisionBits: 64}
				for j := 0; j < tc.rowsPerBlock; j++ {
					b.timestamps = append(b.timestamps, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()+int64(j)*downsampleResolution5m)
					for feature := range b.values {
						value := float64((uint64(j+feature+1) * 0x9e3779b97f4a7c15) >> 12)
						if j%2 == 0 {
							value = -value
						}
						b.values[feature] = append(b.values[feature], value)
					}
				}
				if err := w.WriteBlock(&b); err != nil {
					t.Fatal(err)
				}
				timestampBytes += uint64(len(w.blocks[0].timestampsData))
				for feature := range w.blocks {
					valuesBytes += uint64(len(w.blocks[feature].valuesData))
				}
			}
			var spillBytes uint64
			for feature, f := range w.spills {
				if f == nil {
					t.Fatalf("missing spill for feature %d", feature)
				}
				info, err := f.Stat()
				if err != nil {
					t.Fatal(err)
				}
				spillBytes += uint64(info.Size())
			}
			wantSpill := valuesBytes + uint64(tc.blockCount)*5*89
			if spillBytes != wantSpill {
				t.Fatalf("spill must contain five headers/values, no timestamps: got %d; want %d", spillBytes, wantSpill)
			}
			if w.offsets[0] != timestampBytes || w.offsets[1] != 0 || w.offsets[2] != 0 {
				t.Fatalf("before resolution flush only shared timestamps may reach final files: offsets=%v; timestamps=%d", w.offsets, timestampBytes)
			}
			ph, err := w.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if w.offsets[0] != timestampBytes || w.offsets[1] != valuesBytes {
				t.Fatalf("flush duplicated timestamps or lost values: offsets=%v; timestamps=%d; values=%d", w.offsets, timestampBytes, valuesBytes)
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
			if len(entries) != 5 {
				t.Fatalf("finished part must contain only five final files, no spills: got %d entries", len(entries))
			}
			for feature, f := range w.spills {
				if f != nil {
					t.Fatalf("finished writer retained spill %d", feature)
				}
			}
			// Final output plus all pre-flush spills is a conservative envelope,
			// not a sampled peak: production deletes each spill after its column.
			peakEnvelope := encodedSize + spillBytes
			rows, blocks := uint64(tc.blockCount*tc.rowsPerBlock), uint64(tc.blockCount)
			bound := estimateDownsampleOutputSize(rows, blocks)
			if peakEnvelope > bound {
				t.Fatalf("output plus spill exceeds batch-derived bound: output=%d; spill=%d; bound=%d", encodedSize, spillBytes, bound)
			}
			partBound := estimateDownsamplePartSize([]*partWrapper{{p: &part{ph: ph, dsMetadata: &downsamplePartMetadata{}}}})
			if partBound < bound || peakEnvelope > partBound {
				t.Fatalf("row-derived bound does not cover output/spill: peak=%d; batch bound=%d; part bound=%d", peakEnvelope, bound, partBound)
			}
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			var indexCounts [5]int
			var blockCounts, rowCounts [5]uint64
			for _, mr := range p.dsMetaindex {
				feature := int(mr.feature) - 1
				if feature < 0 || feature >= len(indexCounts) || mr.ResolutionMs != downsampleResolution5m {
					t.Fatalf("unexpected metaindex identity: %+v", mr)
				}
				indexCounts[feature]++
				blockCounts[feature] += uint64(mr.BlockHeadersCount)
				rowCounts[feature] += mr.RowsCount
			}
			wantIndexes := 1
			if tc.indexLimit != 0 {
				wantIndexes = tc.blockCount
			}
			for feature := range indexCounts {
				if indexCounts[feature] != wantIndexes || blockCounts[feature] != blocks || rowCounts[feature] != rows {
					t.Fatalf("feature %d independent index/metaindex: indexes=%d blocks=%d rows=%d; want %d/%d/%d", feature, indexCounts[feature], blockCounts[feature], rowCounts[feature], wantIndexes, blocks, rows)
				}
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
