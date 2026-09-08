package storage

import "testing"

func TestDownsampleMergeBatchPreservesSources(t *testing.T) {
	for _, n := range []int{0, 1, downsampleMaxMergeSources, downsampleMaxMergeSources + 1, 2*downsampleMaxMergeSources + 3} {
		sources := make([]*partWrapper, n)
		for i := range sources {
			sources[i] = &partWrapper{isInMerge: true}
		}
		alreadyWaiting := &partWrapper{isInMerge: true}
		batch, remaining := splitDownsampleMergeBatch(sources, []*partWrapper{alreadyWaiting})
		seen := make(map[*partWrapper]int)
		for {
			if len(batch) > downsampleMaxMergeSources {
				t.Fatalf("source limit exceeded: %d", len(batch))
			}
			for _, pw := range batch {
				seen[pw]++
				if !pw.isInMerge {
					t.Fatal("batching unexpectedly released source ownership")
				}
			}
			if len(remaining) == 0 {
				break
			}
			batch, remaining = splitDownsampleMergeBatch(remaining, nil)
		}
		if len(seen) != n+1 {
			t.Fatalf("source lost while batching %d sources: got %d", n, len(seen))
		}
		for _, pw := range append(sources, alreadyWaiting) {
			if seen[pw] != 1 {
				t.Fatalf("source processed %d times", seen[pw])
			}
		}
	}
}
