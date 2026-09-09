package filestream

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs/fsutil"
)

// faultyWriterFile injects failures on a single real file, without package-level hooks.
// Close always closes that file, even when it also reports an injected close error.
type faultyWriterFile struct {
	*os.File
	writeErr, syncErr, closeErr error
	shortWrite                  bool
	writes, syncs, closes       int
}

func (f *faultyWriterFile) Write(p []byte) (int, error) {
	f.writes++
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.shortWrite {
		return f.File.Write(p[:len(p)/2])
	}
	return f.File.Write(p)
}

func (f *faultyWriterFile) Sync() error {
	f.syncs++
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.File.Sync()
}

func (f *faultyWriterFile) Close() error {
	f.closes++
	return errors.Join(f.File.Close(), f.closeErr)
}

func newFaultyWriter(t *testing.T) (*Writer, *faultyWriterFile, uint64) {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	backend := &faultyWriterFile{File: f}
	before := writersCount.Get()
	w := newWriter(backend, false)
	t.Cleanup(func() { _ = w.Abort() })
	if got := writersCount.Get(); got != before+1 {
		t.Fatalf("writer counter after create: got %d; want %d", got, before+1)
	}
	return w, backend, before
}

func checkWriterReleased(t *testing.T, w *Writer, f *faultyWriterFile, before uint64, closeErr error) {
	t.Helper()
	if w.f != nil || w.bw != nil || w.st != (streamTracker{}) {
		t.Fatal("closed writer retained file, buffer or tracker")
	}
	if f.closes != 1 || writersCount.Get() != before {
		t.Fatalf("resources not released exactly once: closes=%d, writers=%d; want 1, %d", f.closes, writersCount.Get(), before)
	}
	if _, err := f.File.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("underlying file wasn't closed: %v", err)
	}
	if w.Path() != f.Name() {
		t.Fatal("path must remain available after close")
	}
	for _, closeAgain := range []func() error{w.Close, w.Abort, w.Close} {
		if err := closeAgain(); err != closeErr {
			t.Fatalf("repeated close changed the result: got %v; want %v", err, closeErr)
		}
	}
	if f.closes != 1 || writersCount.Get() != before {
		t.Fatal("repeated Close or Abort released resources twice")
	}
	if n, err := w.Write([]byte("after close")); n != 0 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("write after close: n=%d, err=%v", n, err)
	}
}

func TestWriterCreateAndCreateExclusive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	for _, data := range []string{"original long content", "short"} {
		w, err := Create(path, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(data)); err != nil {
			_ = w.Abort()
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil || string(got) != data {
			t.Fatalf("Create didn't truncate/write file: got=%q, err=%v", got, err)
		}
	}
	if w, err := CreateExclusive(path, false); w != nil || !errors.Is(err, os.ErrExist) {
		t.Fatalf("CreateExclusive must reject existing file: writer=%v, err=%v", w, err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "short" {
		t.Fatalf("exclusive create altered existing contents: got=%q, err=%v", got, err)
	}
	control, err := os.Create(filepath.Join(dir, "control"))
	if err != nil {
		t.Fatal(err)
	}
	controlInfo, err := control.Stat()
	if closeErr := control.Close(); err != nil || closeErr != nil {
		t.Fatalf("control file: stat=%v, close=%v", err, closeErr)
	}
	for i, create := range []func(string, bool) (*Writer, error){Create, CreateExclusive} {
		name := filepath.Join(dir, []string{"ordinary", "exclusive"}[i])
		w, err := create(name, true)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(name)
		if err != nil || info.Mode().Perm() != controlInfo.Mode().Perm() {
			t.Fatalf("permissions must match os.Create: info=%v, err=%v", info, err)
		}
		if failed, err := create(filepath.Join(dir, "missing", "file"), false); failed != nil || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("create failure should return an error: writer=%v, err=%v", failed, err)
		}
	}
}

func TestWriterCloseReleasesAfterFailures(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		writeErr, syncErr, closeErr error
		shortWrite                  bool
	}{
		{name: "flush", writeErr: errors.New("injected buffered write failure")},
		{name: "short_flush", shortWrite: true},
		{name: "close", closeErr: errors.New("injected close failure")},
		{name: "combined", writeErr: errors.New("injected flush failure"), syncErr: errors.New("injected sync failure"), closeErr: errors.New("injected close failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, f, before := newFaultyWriter(t)
			f.writeErr, f.syncErr, f.closeErr, f.shortWrite = tc.writeErr, tc.syncErr, tc.closeErr, tc.shortWrite
			if _, err := w.Write([]byte("buffered payload")); err != nil {
				t.Fatalf("small write should still be buffered: %v", err)
			}
			if f.writes != 0 || w.bw.Buffered() == 0 {
				t.Fatal("test didn't exercise buffered flush on Close")
			}
			err := w.Close()
			for _, expected := range []error{tc.writeErr, tc.closeErr} {
				if expected != nil && !errors.Is(err, expected) {
					t.Fatalf("close lost operation error %v: %v", expected, err)
				}
			}
			if tc.shortWrite && !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("close didn't report short buffered flush: %v", err)
			}
			if !fsutil.IsFsyncDisabled() {
				if f.syncs != 1 || (tc.syncErr != nil && !errors.Is(err, tc.syncErr)) {
					t.Fatalf("sync wasn't attempted/preserved after flush error: syncs=%d, err=%v", f.syncs, err)
				}
			} else if f.syncs != 0 {
				t.Fatal("Close ignored fsync configuration")
			}
			checkWriterReleased(t, w, f, before, err)
		})
	}
}

func TestWriterClosePreservesFailedWrite(t *testing.T) {
	w, f, before := newFaultyWriter(t)
	failure := errors.New("injected direct write failure")
	f.writeErr = failure
	if n, err := w.Write(bytes.Repeat([]byte("x"), w.bw.Size()+1)); n != 0 || !errors.Is(err, failure) {
		t.Fatalf("direct Write error: n=%d err=%v", n, err)
	}
	calls := f.writes
	f.writeErr = nil // The first failure still makes the writer unusable.
	if n, err := w.Write([]byte("retry")); n != 0 || !errors.Is(err, failure) || f.writes != calls {
		t.Fatalf("writer retried after failure: n=%d err=%v calls=%d/%d", n, err, f.writes, calls)
	}
	err := w.Close()
	if !errors.Is(err, failure) {
		t.Fatalf("Close lost first write error: %v", err)
	}
	checkWriterReleased(t, w, f, before, err)
}

func TestWriterAbortDiscardsBufferedData(t *testing.T) {
	w, f, before := newFaultyWriter(t)
	if _, err := w.Write([]byte("must not reach the file")); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected close failure")
	f.writeErr, f.syncErr, f.closeErr = errors.New("unexpected Write"), errors.New("unexpected Sync"), failure
	err := w.Abort()
	if !errors.Is(err, failure) || f.writes != 0 || f.syncs != 0 {
		t.Fatalf("Abort flushed, synced or lost close error: writes=%d syncs=%d err=%v", f.writes, f.syncs, err)
	}
	if got, err := os.ReadFile(w.Path()); err != nil || len(got) != 0 {
		t.Fatalf("Abort didn't discard buffered data: got=%q err=%v", got, err)
	}
	checkWriterReleased(t, w, f, before, err)
}

func TestWriterCloseRealReadOnlyFileFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readonly")
	if err := os.WriteFile(path, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w := newWriter(f, false)
	if _, err := w.Write([]byte("buffered")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err == nil || !strings.Contains(err.Error(), "flush") {
		t.Fatalf("real buffered flush to read-only file must fail: %v", err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("file not released after actual flush failure: %v", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "unchanged" {
		t.Fatalf("read-only file changed: got=%q err=%v", got, err)
	}
}

func TestWriterSyncFailureWithFsyncEnabled(t *testing.T) {
	if fsutil.IsFsyncDisabled() {
		// IsFsyncDisabled is initialized at process startup. Exercise the actual
		// Close path with fsync enabled in a subprocess instead of a global hook.
		cmd := exec.Command(os.Args[0], "-test.run=^TestWriterSyncFailureWithFsyncEnabled$", "-test.v")
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "DISABLE_FSYNC_FOR_TESTING=") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		cmd.Env = append(cmd.Env, "DISABLE_FSYNC_FOR_TESTING=false")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fsync-enabled test failed: %v\n%s", err, output)
		}
		return
	}
	w, f, before := newFaultyWriter(t)
	failure := errors.New("injected fsync failure")
	f.syncErr = failure
	if _, err := w.Write([]byte("written before sync")); err != nil {
		t.Fatal(err)
	}
	err := w.Close()
	if !errors.Is(err, failure) || f.writes != 1 || f.syncs != 1 {
		t.Fatalf("sync failure wasn't reported after flush: writes=%d syncs=%d err=%v", f.writes, f.syncs, err)
	}
	checkWriterReleased(t, w, f, before, err)
}

func TestWriterMustFlushAndMustCloseSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "must")
	w := MustCreate(path, false)
	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	w.MustFlush(true)
	if got, err := os.ReadFile(path); err != nil || string(got) != "first" {
		t.Fatalf("MustFlush didn't flush: got=%q err=%v", got, err)
	}
	if _, err := w.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	w.MustClose()
	w.MustClose()
	if got, err := os.ReadFile(path); err != nil || string(got) != "firstsecond" {
		t.Fatalf("MustClose didn't flush: got=%q err=%v", got, err)
	}
}
