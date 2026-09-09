package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
)

// Faults belong to this writer instance; no process-wide I/O hooks are changed.
type failingDownsampleFile struct {
	downsampleFileWriter
	writeErr, closeErr, abortErr error
	shortWrite                   bool
	closes, aborts               int
}

func (f *failingDownsampleFile) Write(b []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.shortWrite && len(b) > 0 {
		return len(b) - 1, nil
	}
	return f.downsampleFileWriter.Write(b)
}

func (f *failingDownsampleFile) Close() error {
	f.closes++
	return errors.Join(f.downsampleFileWriter.Close(), f.closeErr)
}

func (f *failingDownsampleFile) Abort() error {
	f.aborts++
	return errors.Join(f.downsampleFileWriter.Abort(), f.abortErr)
}

func assertDownsampleWriterAborted(t *testing.T, w *downsampleWriter, path string, cause error) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed writer left its target or spills at %q: %v", path, err)
	}
	if w.path != "" || w.finished {
		t.Fatalf("failed target is still publishable: path=%q; finished=%v", w.path, w.finished)
	}
	for _, f := range w.files {
		if f != nil {
			t.Fatal("failed writer retained a final-file handle")
		}
	}
	for _, spill := range w.spills {
		if spill != nil {
			t.Fatal("failed writer retained a spill")
		}
	}
	if _, err := w.Finish(); !errors.Is(err, cause) {
		t.Fatalf("Finish lost the failure or accepted partial output: %v", err)
	}
	if err := w.WriteBlock(fileTestDownsampleBlock(2, 300000)); !errors.Is(err, cause) {
		t.Fatalf("WriteBlock lost the failure or accepted more output: %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("cleanup must be idempotent: %v", err)
	}
}

func TestDownsampleWriterFinalFileFailures(t *testing.T) {
	for i, name := range []string{timestampsFilename, valuesFilename, indexFilename, metaindexFilename} {
		for _, operation := range []string{"write", "short_write", "close", "abort"} {
			t.Run(name+"/"+operation, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "part")
				var w downsampleWriter
				if err := w.Init(path, 1); err != nil {
					t.Fatal(err)
				}
				defer w.Abort()
				var files [4]*failingDownsampleFile
				for j, f := range w.files {
					files[j] = &failingDownsampleFile{downsampleFileWriter: f}
					w.files[j] = files[j]
				}
				cause := errors.New("injected " + operation)
				switch operation {
				case "write":
					files[i].writeErr = cause
				case "short_write":
					files[i].shortWrite = true
					cause = io.ErrShortWrite
				case "close":
					files[i].closeErr = cause
				case "abort":
					files[i].writeErr = io.ErrUnexpectedEOF
					files[i].abortErr = cause
				}
				err := w.WriteBlock(fileTestDownsampleBlock(1, 300000))
				if err == nil {
					_, err = w.Finish()
				}
				if !errors.Is(err, cause) {
					t.Fatalf("lost %s error: %v", operation, err)
				}
				assertDownsampleWriterAborted(t, &w, path, cause)
				for j, f := range files {
					if f.closes+f.aborts != 1 {
						t.Fatalf("file %d must be released once even if another fails: close=%d; abort=%d", j, f.closes, f.aborts)
					}
					if _, err := f.downsampleFileWriter.Write([]byte("closed")); !errors.Is(err, os.ErrClosed) {
						t.Fatalf("file %d is still open: %v", j, err)
					}
				}
			})
		}
	}
}

func TestDownsampleWriterSpillAndValidationFailures(t *testing.T) {
	for _, scenario := range []string{"spill_create", "spill_truncated_header", "metadata_create", "invalid_next_batch"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part")
			var w downsampleWriter
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if scenario == "spill_create" {
				// Earlier columns already have buffered data when the third fails.
				w.spills[2] = filestream.NewSpillWriter(filepath.Join(path, "missing"))
			}
			err := w.WriteBlock(fileTestDownsampleBlock(1, 300000))
			if scenario != "spill_create" && err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "spill_truncated_header":
				if _, err := w.spills[2].Write([]byte{1}); err != nil {
					t.Fatal(err)
				}
				_, err = w.Finish()
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("truncated spill header must fail: %v", err)
				}
			case "metadata_create":
				if err := os.Mkdir(filepath.Join(path, metadataFilename), 0755); err != nil {
					t.Fatal(err)
				}
				_, err = w.Finish()
			case "invalid_next_batch":
				err = w.WriteBlock(nil)
			}
			if err == nil {
				t.Fatal("expected failure")
			}
			assertDownsampleWriterAborted(t, &w, path, err)
			// The same instance must be reusable after all failed output is gone.
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			if err := w.WriteBlock(fileTestDownsampleBlock(1, 300000)); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Finish(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDownsampleWriterFinalFilePermissions(t *testing.T) {
	dir := t.TempDir()
	control, err := filestream.Create(filepath.Join(dir, "control"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(control.Path())
	if err != nil {
		t.Fatal(err)
	}
	var w downsampleWriter
	path := filepath.Join(dir, "part")
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := w.WriteBlock(fileTestDownsampleBlock(1, 300000)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{timestampsFilename, valuesFilename, indexFilename, metaindexFilename, metadataFilename} {
		got, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			t.Fatal(err)
		}
		if got.Mode().Perm() != info.Mode().Perm() {
			t.Fatalf("%s permissions=%o; filestream permissions=%o", name, got.Mode().Perm(), info.Mode().Perm())
		}
	}
}
