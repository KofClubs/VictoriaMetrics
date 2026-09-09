package filestream

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestSpillWriterRoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, getWriteBufferSize() - 1, getWriteBufferSize(), 3*getWriteBufferSize() + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			dir := t.TempDir()
			w := NewSpillWriter(dir)
			data := bytes.Repeat([]byte("aBc012"), size/6+1)[:size]
			for pos := 0; pos < len(data); {
				end := min(pos+373, len(data))
				n, err := w.Write(data[pos:end])
				if err != nil || n != end-pos {
					t.Fatalf("Write: got (%d, %v); want (%d, nil)", n, err, end-pos)
				}
				pos = end
				if w.Size() != uint64(pos) {
					t.Fatalf("Size: got %d; want %d", w.Size(), pos)
				}
			}
			var saved io.Reader
			if err := w.ReadAll(func(r io.Reader) error {
				saved = r
				got, err := io.ReadAll(r)
				if !bytes.Equal(got, data) {
					t.Fatalf("read data differs: got %d bytes; want %d", len(got), len(data))
				}
				return err
			}); err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if w.Size() != uint64(size) {
				t.Fatalf("Size changed after ReadAll: %d", w.Size())
			}
			if _, err := saved.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("escaped reader: got %v; want ErrClosed", err)
			}
			assertSpillRemoved(t, w, dir)
			if err := w.Close(); err != nil {
				t.Fatalf("repeated Close: %v", err)
			}
			if _, err := w.Write(nil); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("Write after ReadAll: got %v; want ErrClosed", err)
			}
			if err := w.ReadAll(func(io.Reader) error {
				t.Fatal("ReadAll called consumer twice")
				return nil
			}); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("repeated ReadAll: got %v; want ErrClosed", err)
			}
		})
	}
}

func TestSpillWriterLazyCreationAndDiscard(t *testing.T) {
	dir := t.TempDir()
	w := NewSpillWriter(dir)
	if n, err := w.Write(nil); n != 0 || err != nil {
		t.Fatalf("empty Write: (%d, %v)", n, err)
	}
	assertSpillRemoved(t, w, dir)
	if _, err := w.Write([]byte("buffered data")); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(w.path)
	if err != nil || fi.Size() != 0 {
		t.Fatalf("expected data to be buffered; stat=%v, err=%v", fi, err)
	}
	f := &spillFaultFile{File: w.f.(*os.File)}
	f.write = func([]byte) (int, error) {
		t.Fatal("Close must discard buffered bytes without writing")
		return 0, io.ErrClosedPipe
	}
	w.f = f
	w.bw.Reset(spillFileWriter{f})
	if _, err := w.Write([]byte("more buffered data")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Close(); err != nil || f.closeCalls != 1 {
		t.Fatalf("Close repeated: err=%v, closes=%d", err, f.closeCalls)
	}
	assertSpillRemoved(t, w, dir)
}

func TestSpillWriterPermissions(t *testing.T) {
	dir := t.TempDir()
	control, err := os.Create(filepath.Join(dir, "control"))
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	controlStat, err := control.Stat()
	if err != nil {
		t.Fatal(err)
	}
	w := NewSpillWriter(dir)
	defer w.Close()
	if _, err := w.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	spillStat, err := os.Stat(w.path)
	if err != nil {
		t.Fatal(err)
	}
	if spillStat.Mode().Perm() != controlStat.Mode().Perm() {
		t.Fatalf("spill permissions=%o; os.Create permissions=%o", spillStat.Mode().Perm(), controlStat.Mode().Perm())
	}
	dirStat, err := os.Stat(w.tempDir)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && dirStat.Mode().Perm()&0077 != 0 {
		t.Fatalf("spill directory is accessible to others: %o", dirStat.Mode().Perm())
	}
}

func TestSpillWriterCreateFailure(t *testing.T) {
	dir := t.TempDir()
	w := NewSpillWriter(filepath.Join(dir, "missing", "directory"))
	if n, err := w.Write([]byte("data")); n != 0 || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Write with missing parent: (%d, %v)", n, err)
	}
	if _, err := w.Write([]byte("retry")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Write after creation failure: %v", err)
	}
	if err := w.Close(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Close must retain creation error: %v", err)
	}
	assertSpillRemoved(t, w, dir)
}

func TestSpillWriterWriteFailures(t *testing.T) {
	writeErr := errors.New("injected write failure")
	for _, tc := range []struct {
		name  string
		write func([]byte) (int, error)
		want  error
	}{
		{"error", func([]byte) (int, error) { return 0, writeErr }, writeErr},
		{"partial_error", func(p []byte) (int, error) { return len(p) / 2, writeErr }, writeErr},
		{"short_write", func(p []byte) (int, error) { return len(p) / 2, nil }, io.ErrShortWrite},
		{"no_progress", func([]byte) (int, error) { return 0, nil }, io.ErrShortWrite},
		{"negative_count", func([]byte) (int, error) { return -1, nil }, nil},
		{"excess_count", func(p []byte) (int, error) { return len(p) + 1, nil }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, f, dir := newFaultSpill(t)
			f.write = tc.write
			n, err := w.Write(make([]byte, 2*getWriteBufferSize()))
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("Write error: got %v; want %v", err, tc.want)
			}
			if w.Size() != uint64(n) {
				t.Fatalf("Size=%d; accepted=%d", w.Size(), n)
			}
			if n, err := w.Write([]byte("retry")); n != 0 || err == nil {
				t.Fatalf("Write after failure: (%d, %v)", n, err)
			}
			if err := w.ReadAll(func(io.Reader) error {
				t.Fatal("consumer called after failure")
				return nil
			}); err == nil {
				t.Fatal("ReadAll succeeded after Write failed")
			}
			if f.closeCalls != 1 {
				t.Fatalf("file closed %d times; want 1", f.closeCalls)
			}
			assertSpillRemoved(t, w, dir)
		})
	}
}

func TestSpillWriterFlushAndSeekFailures(t *testing.T) {
	injected := errors.New("injected failure")
	for _, tc := range []struct {
		name  string
		setup func(*spillFaultFile)
		want  error
	}{
		{"flush", func(f *spillFaultFile) { f.write = func([]byte) (int, error) { return 0, injected } }, injected},
		{"flush_short_write", func(f *spillFaultFile) { f.write = func(p []byte) (int, error) { return len(p) / 2, nil } }, io.ErrShortWrite},
		{"seek", func(f *spillFaultFile) { f.seek = func(int64, int) (int64, error) { return 0, injected } }, injected},
		{"wrong_seek_offset", func(f *spillFaultFile) { f.seek = func(int64, int) (int64, error) { return 1, nil } }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, f, dir := newFaultSpill(t)
			tc.setup(f)
			if _, err := w.Write([]byte("buffered")); err != nil {
				t.Fatal(err)
			}
			err := w.ReadAll(func(io.Reader) error {
				t.Fatal("consumer called after flush or seek failure")
				return nil
			})
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("ReadAll: got %v; want %v", err, tc.want)
			}
			assertSpillRemoved(t, w, dir)
		})
	}
}

func TestSpillWriterReadFailures(t *testing.T) {
	readErr := errors.New("injected read failure")
	for _, tc := range []struct {
		name string
		read func(*os.File, []byte) (int, error)
		want error
	}{
		{"error", func(*os.File, []byte) (int, error) { return 0, readErr }, readErr},
		{"error_with_all_data", func(f *os.File, p []byte) (int, error) { n, _ := f.Read(p); return n, readErr }, readErr},
		{"truncated", func(*os.File, []byte) (int, error) { return 0, io.EOF }, io.ErrUnexpectedEOF},
		{"no_progress", func(*os.File, []byte) (int, error) { return 0, nil }, io.ErrNoProgress},
		{"invalid_count", func(_ *os.File, p []byte) (int, error) { return len(p) + 1, nil }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, f, dir := newFaultSpill(t)
			data := []byte("buffered data")
			if _, err := w.Write(data); err != nil {
				t.Fatal(err)
			}
			f.read = func(p []byte) (int, error) { return tc.read(f.File, p) }
			err := w.ReadAll(func(r io.Reader) error {
				// Deliberately ignore the reader's error. ReadAll must still fail,
				// including when bufio hides an error delivered with the last bytes.
				_, _ = io.ReadFull(r, make([]byte, len(data)))
				return nil
			})
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("ReadAll: got %v; want %v", err, tc.want)
			}
			assertSpillRemoved(t, w, dir)
		})
	}
}

func TestSpillWriterConsumerFailures(t *testing.T) {
	consumerErr := errors.New("injected consumer failure")
	for _, tc := range []struct {
		name    string
		consume func(io.Reader) error
		want    error
	}{
		{"nil", nil, nil},
		{"none_consumed", func(io.Reader) error { return nil }, io.ErrUnexpectedEOF},
		{"partial", func(r io.Reader) error { _, err := io.ReadFull(r, make([]byte, 1)); return err }, io.ErrUnexpectedEOF},
		{"callback_error", func(io.Reader) error { return consumerErr }, consumerErr},
		{"error_after_read", func(r io.Reader) error { _, _ = io.Copy(io.Discard, r); return consumerErr }, consumerErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _, dir := newFaultSpill(t)
			if _, err := w.Write([]byte("data")); err != nil {
				t.Fatal(err)
			}
			err := w.ReadAll(tc.consume)
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("ReadAll: got %v; want %v", err, tc.want)
			}
			if _, err := w.Write([]byte("retry")); err == nil {
				t.Fatal("Write succeeded after consumer failure")
			}
			assertSpillRemoved(t, w, dir)
		})
	}
}

func TestSpillWriterExactReadAndSealedCallback(t *testing.T) {
	w, _, dir := newFaultSpill(t)
	data := []byte("exact data")
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.ReadAll(func(r io.Reader) error {
		if _, err := w.Write([]byte("late")); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("Write during ReadAll: %v", err)
		}
		if err := w.ReadAll(func(io.Reader) error { return nil }); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("nested ReadAll: %v", err)
		}
		got := make([]byte, len(data))
		_, err := io.ReadFull(r, got)
		if !bytes.Equal(got, data) {
			t.Fatal("data mismatch")
		}
		return err
	}); err != nil {
		t.Fatalf("exact read without extra EOF read: %v", err)
	}
	assertSpillRemoved(t, w, dir)
}

func TestSpillWriterCleanupRetry(t *testing.T) {
	removeErr := errors.New("injected remove failure")
	for _, failDirectory := range []bool{false, true} {
		name := "file"
		if failDirectory {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			w, f, dir := newFaultSpill(t)
			if _, err := w.Write([]byte("data")); err != nil {
				t.Fatal(err)
			}
			failPath := w.path
			if failDirectory {
				failPath = w.tempDir
			}
			w.remove = func(path string) error {
				if path == failPath {
					return removeErr
				}
				return os.Remove(path)
			}
			err := w.ReadAll(func(r io.Reader) error { _, err := io.Copy(io.Discard, r); return err })
			if !errors.Is(err, removeErr) {
				t.Fatalf("ReadAll removal: got %v; want injected error", err)
			}
			if _, err := os.Stat(failPath); err != nil {
				t.Fatalf("failed removal lost its path: %v", err)
			}
			if w.tempDir == "" || !failDirectory && w.path == "" {
				t.Fatal("failed cleanup discarded retry state")
			}
			if err := w.Close(); !errors.Is(err, removeErr) {
				t.Fatalf("retry while removal is failing: %v", err)
			}
			w.remove = os.Remove
			if err := w.Close(); err != nil {
				t.Fatalf("retry after removal becomes available: %v", err)
			}
			if f.closeCalls != 1 {
				t.Fatalf("file closed %d times; want 1", f.closeCalls)
			}
			assertSpillRemoved(t, w, dir)
		})
	}
}

func TestSpillWriterCloseErrorStillRemoves(t *testing.T) {
	w, f, dir := newFaultSpill(t)
	closeErr := errors.New("injected close failure")
	f.closeErr = closeErr
	if err := w.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close error: %v", err)
	}
	assertSpillRemoved(t, w, dir)
	if err := w.Close(); !errors.Is(err, closeErr) || f.closeCalls != 1 {
		t.Fatalf("repeated Close: err=%v, closes=%d", err, f.closeCalls)
	}
}

func TestSpillWriterWriteErrorCleanupRetry(t *testing.T) {
	w, f, dir := newFaultSpill(t)
	writeErr := errors.New("injected write failure")
	removeErr := errors.New("injected remove failure")
	f.write = func([]byte) (int, error) { return 0, writeErr }
	w.remove = func(string) error { return removeErr }
	_, err := w.Write(make([]byte, 2*getWriteBufferSize()))
	if !errors.Is(err, writeErr) || !errors.Is(err, removeErr) {
		t.Fatalf("Write lost a write or cleanup error: %v", err)
	}
	if w.path == "" || w.tempDir == "" {
		t.Fatal("failed cleanup discarded retry state")
	}
	w.remove = os.Remove
	if err := w.Close(); !errors.Is(err, writeErr) || errors.Is(err, removeErr) {
		t.Fatalf("Close retry: got %v; want only the original write failure", err)
	}
	assertSpillRemoved(t, w, dir)
	if f.closeCalls != 1 {
		t.Fatalf("file closed %d times; want 1", f.closeCalls)
	}
}

func TestSpillWriterBoundedBuffers(t *testing.T) {
	w := NewSpillWriter(t.TempDir())
	defer w.Close()
	chunk := bytes.Repeat([]byte("abcdefgh"), 8*1024)
	want := sha256.New()
	const chunks = 256 // 16 MiB; neither writes nor reads retain the stream.
	for range chunks {
		if _, err := w.Write(chunk); err != nil {
			t.Fatal(err)
		}
		_, _ = want.Write(chunk)
		if w.bw.Size() != getWriteBufferSize() {
			t.Fatalf("write buffer grew with spill size: %d", w.bw.Size())
		}
	}
	if w.Size() != chunks*uint64(len(chunk)) {
		t.Fatalf("Size: %d", w.Size())
	}
	got := sha256.New()
	if err := w.ReadAll(func(r io.Reader) error {
		br := r.(*spillReader).r.(*bufio.Reader)
		if br.Size() != getReadBufferSize() {
			t.Fatalf("read buffer size: %d", br.Size())
		}
		n, err := io.CopyBuffer(got, r, make([]byte, 8192))
		if uint64(n) != w.Size() {
			t.Fatalf("read %d bytes; want %d", n, w.Size())
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
		t.Fatal("stream checksum mismatch")
	}
}

func TestSpillWriterIndependentInstances(t *testing.T) {
	dir := t.TempDir()
	const n = 12
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := NewSpillWriter(dir)
			defer w.Close()
			data := strings.Repeat(string(rune('a'+i)), 10_000)
			if _, err := w.Write([]byte(data)); err != nil {
				t.Error(err)
				return
			}
			if err := w.ReadAll(func(r io.Reader) error {
				got, err := io.ReadAll(r)
				if string(got) != data {
					t.Error("independent spill data mismatch")
				}
				return err
			}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	assertSpillRemoved(t, NewSpillWriter(dir), dir)
}

func newFaultSpill(t *testing.T) (*SpillWriter, *spillFaultFile, string) {
	t.Helper()
	dir := t.TempDir()
	w := NewSpillWriter(dir)
	if err := w.create(); err != nil {
		t.Fatal(err)
	}
	f := &spillFaultFile{File: w.f.(*os.File)}
	w.f = f
	w.bw.Reset(spillFileWriter{f})
	t.Cleanup(func() { _ = w.Close() })
	return w, f, dir
}

type spillFaultFile struct {
	*os.File
	write      func([]byte) (int, error)
	read       func([]byte) (int, error)
	seek       func(int64, int) (int64, error)
	closeErr   error
	closeCalls int
}

func (f *spillFaultFile) Write(p []byte) (int, error) {
	if f.write != nil {
		return f.write(p)
	}
	return f.File.Write(p)
}

func (f *spillFaultFile) Read(p []byte) (int, error) {
	if f.read != nil {
		return f.read(p)
	}
	return f.File.Read(p)
}

func (f *spillFaultFile) Seek(offset int64, whence int) (int64, error) {
	if f.seek != nil {
		return f.seek(offset, whence)
	}
	return f.File.Seek(offset, whence)
}

func (f *spillFaultFile) Close() error {
	f.closeCalls++
	return errors.Join(f.File.Close(), f.closeErr)
}

func assertSpillRemoved(t *testing.T, w *SpillWriter, dir string) {
	t.Helper()
	if w.f != nil || w.bw != nil || w.path != "" || w.tempDir != "" {
		t.Fatalf("spill retained resources: file=%v, buffer=%v, path=%q, directory=%q", w.f, w.bw, w.path, w.tempDir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("spill left files: entries=%v, err=%v", entries, err)
	}
}
