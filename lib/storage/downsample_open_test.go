package storage

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	vmfs "github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestCheckDownsamplingOpen(t *testing.T) {
	setDownsampleOpenTestDedup(t, 0)
	for _, tc := range []struct {
		name    string
		enabled bool
		setup   func(*testing.T, string)
		wantErr string
	}{
		{name: "empty"},
		{name: "enabled_empty", enabled: true},
		{
			name: "raw_manifest",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"raw"}, nil)
			},
		},
		{
			name: "raw_without_manifest_or_metadata",
			setup: func(t *testing.T, path string) {
				partPath := filepath.Join(path, "data/big/2025_01/1_1_20250101000000.000_20250101000000.000_1")
				createDownsampleOpenTestRaw(t, partPath)
				if err := os.Remove(filepath.Join(partPath, metadataFilename)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "enabled_mixed_raw_summary",
			enabled: true,
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/big/2025_01/summary"))
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"raw"}, []string{"summary"})
			},
		},
		{
			name: "disabled_active_summary_in_later_partition",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2024_12/raw"))
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/big/2025_01/summary"))
				writeDownsampleOpenTestManifest(t, path, "2025_01", nil, []string{"summary"})
			},
			wantErr: "enable -storage.downsampling.enabled",
		},
		{
			name: "summary_only_in_snapshots",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestSummary(t, filepath.Join(path, "snapshots/snapshot/data/small/2025_01/summary"))
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/snapshots/snapshot/2025_01/summary"))
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/big/snapshots/snapshot/2025_01/summary"))
			},
		},
		{
			name: "manifest_excludes_unpublished_summary_and_special_directories",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
				for _, name := range []string{"unpublished", "tmp", "txn", "snapshots"} {
					createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/2025_01", name))
				}
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"raw"}, nil)
			},
		},
		{
			name: "historical_special_directories",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
				for _, name := range []string{"tmp", "txn", "snapshots"} {
					createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/2025_01", name))
				}
			},
		},
		{
			name: "partially_removed_partition",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/2025_01/summary"))
				writeDownsampleOpenTestFile(t, filepath.Join(path, "data/small/2025_01/.delete-this-dir"), nil)
			},
		},
		{
			name: "partially_removed_small_keeps_active_big",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/2025_01/summary"))
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/big/2025_01/raw"))
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"summary"}, nil)
				writeDownsampleOpenTestFile(t, filepath.Join(path, "data/small/2025_01/.delete-this-dir"), nil)
			},
		},
		{
			name: "indexdb_partition_without_data",
			setup: func(t *testing.T, path string) {
				writeDownsampleOpenTestFile(t, filepath.Join(path, "data/indexdb/2025_01/index.bin"), []byte("not decoded by the precheck"))
			},
		},
		{
			name: "missing_active_part",
			setup: func(t *testing.T, path string) {
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"missing"}, nil)
			},
			wantErr: "missing",
		},
		{
			name: "corrupt_active_metadata",
			setup: func(t *testing.T, path string) {
				partPath := filepath.Join(path, "data/small/2025_01/corrupt")
				createDownsampleOpenTestRaw(t, partPath)
				writeDownsampleOpenTestFile(t, filepath.Join(partPath, metadataFilename), []byte("{"))
			},
			wantErr: "corrupt",
		},
		{
			name: "indexdb_invalid_partition_name",
			setup: func(t *testing.T, path string) {
				writeDownsampleOpenTestFile(t, filepath.Join(path, "data/indexdb/invalid/index.bin"), []byte("not decoded by the precheck"))
			},
			wantErr: "invalid partition directory",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "storage")
			if tc.setup != nil {
				tc.setup(t, path)
			}
			before := snapshotDownsampleOpenTestFiles(t, path)
			err := checkDownsamplingOpen(path, OpenOptions{DownsamplingEnabled: tc.enabled})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected precheck error: %s", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("unexpected precheck error: got %v; want substring %q", err, tc.wantErr)
			}
			after := snapshotDownsampleOpenTestFiles(t, path)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("precheck modified storage files: before=%v; after=%v", before, after)
			}
		})
	}
}

func TestCheckDownsamplingOpenInvalidManifests(t *testing.T) {
	setDownsampleOpenTestDedup(t, 0)
	for _, tc := range []struct {
		data    string
		wantErr string
	}{
		{`{`, "cannot parse"},
		{`null`, "expected a JSON object"},
		{`[]`, "expected a JSON object"},
		{`{"Small":[],"Unknown":[]}`, "unknown field"},
		{`{"Small":[],"small":["other"]}`, "duplicate field"},
		{`{"Small":["raw","raw"]}`, "duplicate active part"},
		{`{"Small":["../raw"]}`, "invalid active part name"},
		{`{"Small":["tmp"]}`, "invalid active part name"},
		{`{"Small":[""]}`, "invalid active part name"},
		{`{"Small":[1]}`, "invalid \"Small\" list"},
		{`{"Small":[]} {}`, "unexpected trailing"},
		{`{"Big":["raw"]}`, "partition directory is missing"},
	} {
		t.Run(tc.data, func(t *testing.T) {
			path := t.TempDir()
			manifest := filepath.Join(path, "data/small/2025_01", partsFilename)
			writeDownsampleOpenTestFile(t, manifest, []byte(tc.data))
			before := snapshotDownsampleOpenTestFiles(t, path)
			err := checkDownsamplingOpen(path, OpenOptions{DownsamplingEnabled: true})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), manifest) {
				t.Fatalf("unexpected manifest error: got %v; want %q and %q", err, tc.wantErr, manifest)
			}
			if after := snapshotDownsampleOpenTestFiles(t, path); !reflect.DeepEqual(before, after) {
				t.Fatalf("invalid manifest was modified during precheck")
			}
		})
	}
}

func TestMustOpenStorageDownsamplingRejectsDedup(t *testing.T) {
	for _, interval := range []time.Duration{time.Millisecond, -time.Millisecond} {
		t.Run(interval.String(), func(t *testing.T) {
			setDownsampleOpenTestDedup(t, interval)
			path := t.TempDir()
			func() {
				defer func() {
					r := recover()
					if r == nil || !strings.Contains(fmt.Sprint(r), "requires -dedup.minScrapeInterval=0") {
						t.Fatalf("unexpected storage API result: got %v; want a dedup configuration panic", r)
					}
				}()
				s := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true})
				s.MustClose()
			}()
			for _, name := range []string{dataDirname, indexdbDirname, snapshotsDirname, cacheDirname, metadataDirname} {
				if _, err := os.Stat(filepath.Join(path, name)); !os.IsNotExist(err) {
					t.Fatalf("storage initialized %q before rejecting dedup: %v", name, err)
				}
			}
			// 错误返回后必须释放 flock，允许修正配置后再次打开目录。
			f := vmfs.MustCreateFlockFile(path)
			vmfs.MustClose(f)
		})
	}
}

func TestMustOpenStorageDownsamplingRejectsDisabledModeBeforeBackgroundWork(t *testing.T) {
	setDownsampleOpenTestDedup(t, 0)
	path := t.TempDir()
	createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2024_12/raw"))
	createDownsampleOpenTestSummary(t, filepath.Join(path, "data/big/2025_01/summary"))
	writeDownsampleOpenTestManifest(t, path, "2025_01", nil, []string{"summary"})
	before := snapshotDownsampleOpenTestFiles(t, filepath.Join(path, dataDirname))
	func() {
		defer func() {
			r := recover()
			if r == nil || !strings.Contains(fmt.Sprint(r), "enable -storage.downsampling.enabled") {
				t.Fatalf("unexpected storage API result: got %v; want a downsampling configuration panic", r)
			}
		}()
		s := MustOpenStorage(path, OpenOptions{})
		s.MustClose()
	}()
	if after := snapshotDownsampleOpenTestFiles(t, filepath.Join(path, dataDirname)); !reflect.DeepEqual(before, after) {
		t.Fatalf("storage modified source parts or manifests before rejecting active summary")
	}
	for _, name := range []string{indexdbDirname, snapshotsDirname, cacheDirname, metadataDirname} {
		if _, err := os.Stat(filepath.Join(path, name)); !os.IsNotExist(err) {
			t.Fatalf("storage initialized %q before rejecting active summary: %v", name, err)
		}
	}
	f := vmfs.MustCreateFlockFile(path)
	vmfs.MustClose(f)
}

func TestCheckDownsamplingOpenDisabledPreservesDedup(t *testing.T) {
	setDownsampleOpenTestDedup(t, time.Minute)
	path := t.TempDir()
	createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
	before := snapshotDownsampleOpenTestFiles(t, path)
	if err := checkDownsamplingOpen(path, OpenOptions{}); err != nil {
		t.Fatalf("disabled downsampling must retain the existing dedup configuration: %s", err)
	}
	if GetDedupInterval() != time.Minute.Milliseconds() {
		t.Fatalf("precheck changed the dedup interval: got %d", GetDedupInterval())
	}
	if after := snapshotDownsampleOpenTestFiles(t, path); !reflect.DeepEqual(before, after) {
		t.Fatalf("precheck modified raw source files")
	}
}

func setDownsampleOpenTestDedup(t *testing.T, interval time.Duration) {
	t.Helper()
	previous := globalDedupInterval
	SetDedupInterval(interval)
	t.Cleanup(func() { globalDedupInterval = previous })
}

func createDownsampleOpenTestRaw(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	mp := getInmemoryPart()
	defer putInmemoryPart(mp)
	mp.InitFromRows([]rawRow{{
		TSID: TSID{MetricID: 1}, Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), Value: 1, PrecisionBits: 64,
	}})
	mp.MustStoreToDisk(path)
}

func createDownsampleOpenTestSummary(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	for _, resolution := range downsampleResolutions {
		b := downsampleBatch{
			tsid: TSID{MetricID: 1}, resolution: resolution,
			timestamps: []int64{time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()}, precisionBits: 64,
		}
		for i := range b.values {
			b.values[i] = []float64{1}
		}
		if err := w.WriteBlock(&b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
}

func writeDownsampleOpenTestManifest(t *testing.T, path, partition string, small, big []string) {
	t.Helper()
	data, err := json.Marshal(partNamesJSON{Small: small, Big: big})
	if err != nil {
		t.Fatal(err)
	}
	writeDownsampleOpenTestFile(t, filepath.Join(path, dataDirname, smallDirname, partition, partsFilename), data)
}

func writeDownsampleOpenTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func snapshotDownsampleOpenTestFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	files := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if os.IsNotExist(err) && path == root {
			return nil
		}
		if err != nil {
			return err
		}
		info, err := de.Info()
		if err != nil {
			return err
		}
		data := []byte(nil)
		if !de.IsDir() {
			data, err = os.ReadFile(path)
			if err != nil {
				return err
			}
		}
		files[path] = fmt.Sprintf("%s:%d:%x", info.Mode(), info.ModTime().UnixNano(), data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}
