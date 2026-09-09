package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newDownsampleCloseTestReader(t *testing.T) *downsampleReader {
	t.Helper()
	r := &downsampleReader{p: &part{}, ownFiles: true}
	for i := range r.files {
		f, err := os.CreateTemp(t.TempDir(), "reader-")
		if err != nil {
			t.Fatal(err)
		}
		r.files[i] = f
		t.Cleanup(func() { _ = f.Close() })
	}
	return r
}

func assertDownsampleFilesClosed(t *testing.T, files []*os.File) {
	t.Helper()
	for _, f := range files {
		if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("file %q remains open: %v", f.Name(), err)
		}
	}
}

func TestDownsampleReaderCloseOwnFiles(t *testing.T) {
	r := newDownsampleCloseTestReader(t)
	files := r.files
	for _, i := range []int{0, 2} {
		if err := files[i].Close(); err != nil {
			t.Fatal(err)
		}
	}
	err := r.Close()
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("lost actual file close error: %v", err)
	}
	for _, i := range []int{0, 2} {
		if !strings.Contains(err.Error(), files[i].Name()) {
			t.Fatalf("lost close error for file %d: %v", i, err)
		}
	}
	assertDownsampleFilesClosed(t, files[:])
	if r.p != nil || r.ownFiles || r.files != [3]*os.File{} {
		t.Fatal("Close retained part or file ownership")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close attempted the same files again: %v", err)
	}
}

func TestDownsampleReaderCloseBorrowedFiles(t *testing.T) {
	path := writeFileTestDownsamplePart(t, fileTestDownsampleBlock(1, downsampleResolution5m))
	p, err := openDownsamplePart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.MustClose()
	var r downsampleReader
	if err := r.Init(p, downsampleResolution5m); err != nil {
		t.Fatal(err)
	}
	if !r.NextHeader() {
		t.Fatalf("missing header: %v", r.Error())
	}
	if _, err := r.FieldHeader(downsampleFeatureSum); err != nil {
		t.Fatal(err)
	}
	if r.peers[downsampleFeatureSum] == nil {
		t.Fatal("test did not create a feature reader")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	for _, f := range p.dsFiles {
		if _, err := f.Stat(); err != nil {
			t.Fatalf("reader closed borrowed file %q: %v", f.Name(), err)
		}
	}
	for _, peer := range r.peers {
		if peer != nil {
			t.Fatal("Close retained a feature reader")
		}
	}
}

func TestDownsampleReaderInitCloseFailure(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		name := "switch_source"
		if invalid {
			name = "invalid_source"
		}
		t.Run(name, func(t *testing.T) {
			r := newDownsampleCloseTestReader(t)
			files := r.files
			if err := files[0].Close(); err != nil {
				t.Fatal(err)
			}
			next := &part{path: filepath.Join(t.TempDir(), "must-not-open")}
			if invalid {
				next = nil
			}
			err := r.Init(next, downsampleResolution5m)
			if !errors.Is(err, os.ErrClosed) {
				t.Fatalf("Init lost previous source close error: %v", err)
			}
			if invalid && !strings.Contains(err.Error(), "无效降采样源") {
				t.Fatalf("Init lost validation error: %v", err)
			}
			if errors.Is(err, os.ErrNotExist) || r.p != nil {
				t.Fatalf("Init continued opening the next source after close failed: %v", err)
			}
			assertDownsampleFilesClosed(t, files[:])
		})
	}
}

func TestDownsampleMergerResetClosesAllReaders(t *testing.T) {
	for _, operation := range []string{"reset", "init_sources", "merge"} {
		t.Run(operation, func(t *testing.T) {
			first := newDownsampleCloseTestReader(t)
			second := newDownsampleCloseTestReader(t)
			window := newDownsampleCloseTestReader(t)
			files := append(append(append([]*os.File{}, first.files[:]...), second.files[:]...), window.files[:]...)
			if err := first.files[0].Close(); err != nil {
				t.Fatal(err)
			}
			m := downsampleMerger{cursors: []downsampleMergeCursor{{reader: first}, {reader: second}}, reader: window}
			var err error
			switch operation {
			case "reset":
				err = m.Reset()
			case "init_sources":
				err = m.initSources(nil, downsampleResolution1h)
				// Switching columns closes cursors; the independent window reader remains usable.
				if _, statErr := window.files[0].Stat(); statErr != nil {
					t.Fatalf("initSources closed the independent window reader: %v", statErr)
				}
				if closeErr := m.closeReaders(); closeErr != nil {
					t.Fatal(closeErr)
				}
			case "merge":
				_, err = m.Merge(nil, nil, nil, nil, 0, 1)
			}
			if !errors.Is(err, os.ErrClosed) {
				t.Fatalf("%s lost reader close error: %v", operation, err)
			}
			assertDownsampleFilesClosed(t, files)
			if len(m.cursors) != 0 || len(m.heap) != 0 || m.reader != nil {
				t.Fatal("merger retained closed readers")
			}
		})
	}
}

type downsampleCloseTestWriter struct {
	downsampleFileWriter
	beforeWrite func() error
}

func (w *downsampleCloseTestWriter) Write(b []byte) (int, error) {
	if err := w.beforeWrite(); err != nil {
		return 0, err
	}
	return w.downsampleFileWriter.Write(b)
}

func TestDownsampleMergerClosesBeforeReturning(t *testing.T) {
	for _, scenario := range []string{"success", "close_error", "write_and_close_error", "cancel_and_close_error"} {
		t.Run(scenario, func(t *testing.T) {
			const base int64 = 1704067200000
			mp := getInmemoryPart()
			mp.InitFromRows([]rawRow{{TSID: TSID{MetricID: 1}, Timestamp: base + 1, Value: 2, PrecisionBits: 64}})
			sourcePath := filepath.Join(t.TempDir(), "raw")
			mp.MustStoreToDisk(sourcePath)
			putInmemoryPart(mp)
			p := mustOpenFilePart(sourcePath)
			defer p.MustClose()
			var m downsampleMerger
			defer m.Reset()
			var w downsampleWriter
			targetPath := filepath.Join(t.TempDir(), "target")
			if err := w.Init(targetPath, 1); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			stopCh := make(chan struct{})
			writeErr := errors.New("injected output write failure")
			var files []*os.File
			var closeNames []string
			w.files[0] = &downsampleCloseTestWriter{downsampleFileWriter: w.files[0], beforeWrite: func() error {
				if w.resolution != downsampleResolution1h || files != nil {
					return nil
				}
				// The last output batch is already read. Close actual source handles to
				// make only cleanup fail, without adding production test hooks.
				readers := []*downsampleReader{m.cursors[0].reader, m.reader}
				for _, r := range readers {
					files = append(files, r.files[:]...)
					if scenario != "success" {
						closeNames = append(closeNames, r.files[0].Name())
						if err := r.files[0].Close(); err != nil {
							t.Fatal(err)
						}
					}
				}
				if scenario == "write_and_close_error" {
					return writeErr
				}
				if scenario == "cancel_and_close_error" {
					close(stopCh)
				}
				return nil
			}}
			stats, err := m.Merge([]*partWrapper{{p: p}}, &w, stopCh, nil, 0, 1)
			if len(files) != 6 {
				t.Fatal("test did not reach the final batch with both source readers")
			}
			assertDownsampleFilesClosed(t, files)
			if len(m.cursors) != 0 || m.reader != nil {
				t.Fatal("Merge returned with live readers")
			}
			if stats.rowsMerged != 1 || m.stats != stats || m.stopCh != stopCh {
				t.Fatalf("closing readers lost statistics or cancellation: returned=%+v; retained=%+v", stats, m.stats)
			}
			if scenario == "success" {
				if err != nil {
					t.Fatal(err)
				}
				close(stopCh)
				if !errors.Is(m.checkStopped(), errForciblyStopped) {
					t.Fatal("cancellation after Merge is no longer observed before publication")
				}
			} else {
				if !errors.Is(err, os.ErrClosed) {
					t.Fatalf("Merge succeeded or lost cleanup error: %v", err)
				}
				for _, name := range closeNames {
					if !strings.Contains(err.Error(), name) {
						t.Fatalf("lost one reader's close failure: %v", err)
					}
				}
				if scenario == "write_and_close_error" && !errors.Is(err, writeErr) {
					t.Fatalf("cleanup hid output failure: %v", err)
				}
				if scenario == "cancel_and_close_error" && !errors.Is(err, errForciblyStopped) {
					t.Fatalf("cleanup hid cancellation: %v", err)
				}
			}
			// The owner aborts any unpublished output once Merge reports failure.
			if err := w.Abort(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Abort left the unpublished target: %v", err)
			}
		})
	}
}
