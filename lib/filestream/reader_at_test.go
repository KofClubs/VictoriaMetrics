package filestream

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type faultyReaderAtFile struct {
	*os.File
	statErr, readErr, closeErr error
	readResult                 *int
	info                       os.FileInfo
	stats, reads, closes       int
}

func (f *faultyReaderAtFile) Stat() (os.FileInfo, error) {
	f.stats++
	if f.statErr != nil {
		return nil, f.statErr
	}
	if f.info != nil {
		return f.info, nil
	}
	return f.File.Stat()
}

func (f *faultyReaderAtFile) ReadAt(p []byte, off int64) (int, error) {
	f.reads++
	if f.readResult != nil {
		n := *f.readResult
		if n > 0 && n <= len(p) {
			copy(p[:n], bytes.Repeat([]byte("x"), n))
		}
		return n, f.readErr
	}
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.File.ReadAt(p, off)
}

func (f *faultyReaderAtFile) Close() error {
	f.closes++
	return errors.Join(f.File.Close(), f.closeErr)
}

type readerAtSizeInfo struct {
	os.FileInfo
	size int64
}

func (i readerAtSizeInfo) Size() int64 { return i.size }

func createReaderAtTestFile(t *testing.T, data []byte) (string, *os.File) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return path, f
}

func TestReaderAtOffsetsAndCursor(t *testing.T) {
	data := []byte("abcdefghijklmnopqrstuvwxyz")
	path, f := createReaderAtTestFile(t, data)
	r, err := newReaderAt(path, f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := f.Seek(7, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	for _, off := range []int64{19, 0, 11, 19, 2} {
		buf := make([]byte, 4)
		if n, err := r.ReadAt(buf, off); n != len(buf) || err != nil || !bytes.Equal(buf, data[off:off+4]) {
			t.Fatalf("ReadAt(%d): n=%d data=%q err=%v", off, n, buf, err)
		}
	}
	buf := make([]byte, 1)
	if n, err := f.Read(buf); n != 1 || err != nil || buf[0] != data[7] {
		t.Fatalf("ReadAt changed the file cursor: n=%d data=%q err=%v", n, buf, err)
	}
	for _, off := range []int64{int64(len(data)), int64(len(data) + 10)} {
		if n, err := r.ReadAt(buf, off); n != 0 || err != io.EOF {
			t.Fatalf("ReadAt beyond EOF: n=%d err=%v", n, err)
		}
		if n, err := r.ReadAt(nil, off); n != 0 || err != nil {
			t.Fatalf("empty ReadAt beyond EOF: n=%d err=%v", n, err)
		}
	}
	if n, err := r.ReadAt(buf, 0); n != 1 || err != nil || buf[0] != data[0] {
		t.Fatalf("EOF prevented later reads: n=%d data=%q err=%v", n, buf, err)
	}
	r.MustReadAt(buf, 3)
	if buf[0] != data[3] {
		t.Fatal("MustReadAt did not use the requested offset")
	}
	r.MustClose()
	r.MustClose()
}

func TestReaderAtConcurrentReads(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 251)
	}
	path, _ := createReaderAtTestFile(t, data)
	before := readersCount.Get()
	r, err := OpenReadAt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if readersCount.Get() != before+1 || r.Path() != path || r.Size() != uint64(len(data)) {
		t.Fatal("open did not capture path, size or active reader count")
	}
	realCalls, realBytes := readCallsReal.Get(), readBytesReal.Get()
	bufferedCalls, bufferedBytes := readCallsBuffered.Get(), readBytesBuffered.Get()
	const workers, iterations, readSize = 16, 64, 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			buf := make([]byte, readSize)
			for i := 0; i < iterations; i++ {
				off := (worker*173 + i*37) % (len(data) - readSize)
				if n, err := r.ReadAt(buf, int64(off)); n != len(buf) || err != nil || !bytes.Equal(buf, data[off:off+readSize]) {
					errs <- fmt.Errorf("worker=%d offset=%d: n=%d err=%v", worker, off, n, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if readCallsReal.Get() != realCalls+workers*iterations || readBytesReal.Get() != realBytes+workers*iterations*readSize {
		t.Fatal("concurrent reads did not count each real operation and byte exactly once")
	}
	if readCallsBuffered.Get() != bufferedCalls || readBytesBuffered.Get() != bufferedBytes {
		t.Fatal("unbuffered ReadAt changed buffered read statistics")
	}
	if err := r.Close(); err != nil || readersCount.Get() != before {
		t.Fatalf("close did not release the active reader: %v", err)
	}
}

func TestReaderAtTruncatedFile(t *testing.T) {
	path, _ := createReaderAtTestFile(t, []byte("abcdefghij"))
	r, err := OpenReadAt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := os.Truncate(path, 5); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if n, err := r.ReadAt(buf, 3); n != 2 || err != io.EOF || string(buf[:n]) != "de" {
		t.Fatalf("truncated ReadAt: n=%d data=%q err=%v", n, buf, err)
	}
	if r.Size() != 10 {
		t.Fatal("Size must remain the size captured from the opened file")
	}
	if n, err := r.ReadAt(buf, 0); n != 4 || err != nil || string(buf) != "abcd" {
		t.Fatalf("partial EOF prevented a later complete read: n=%d data=%q err=%v", n, buf, err)
	}
}

func TestReaderAtOpenFailures(t *testing.T) {
	dir := t.TempDir()
	before := readersCount.Get()
	for _, tc := range []struct {
		path string
		err  error
	}{
		{filepath.Join(dir, "missing"), os.ErrNotExist},
		{dir, os.ErrInvalid},
	} {
		if r, err := OpenReadAt(tc.path); r != nil || !errors.Is(err, tc.err) || !strings.HasPrefix(err.Error(), "[downsampling]") {
			t.Fatalf("OpenReadAt(%q): reader=%v err=%v", tc.path, r, err)
		}
	}
	if readersCount.Get() != before {
		t.Fatal("rejected opens changed the active reader count")
	}
}

func TestReaderAtRejectedFileCleanup(t *testing.T) {
	statErr := errors.New("injected stat failure")
	closeErr := errors.New("injected rejected file close failure")
	for _, kind := range []string{"stat", "directory", "negative_size"} {
		t.Run(kind, func(t *testing.T) {
			path, file := createReaderAtTestFile(t, []byte("data"))
			f := &faultyReaderAtFile{File: file, closeErr: closeErr}
			wantErr := error(os.ErrInvalid)
			switch kind {
			case "stat":
				f.statErr = statErr
				wantErr = statErr
			case "directory":
				info, err := os.Stat(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				f.info = info
			case "negative_size":
				info, err := file.Stat()
				if err != nil {
					t.Fatal(err)
				}
				f.info = readerAtSizeInfo{FileInfo: info, size: -1}
			}
			before := readersCount.Get()
			r, err := newReaderAt(path, f)
			if r != nil || !errors.Is(err, wantErr) || !errors.Is(err, closeErr) {
				t.Fatalf("rejected file lost validation or cleanup error: reader=%v err=%v", r, err)
			}
			if f.stats != 1 || f.closes != 1 || readersCount.Get() != before {
				t.Fatalf("rejected file cleanup: stats=%d closes=%d readers=%d/%d", f.stats, f.closes, readersCount.Get(), before)
			}
			if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("rejected file remained open: %v", err)
			}
		})
	}
}

func TestReaderAtReadFailuresAndCounts(t *testing.T) {
	readErr := errors.New("injected read failure")
	for _, tc := range []struct {
		name    string
		n       int
		err     error
		wantN   int
		wantErr error
	}{
		{"read_error", 0, readErr, 0, readErr},
		{"partial_error", 2, readErr, 2, readErr},
		{"short_nil", 2, nil, 2, io.ErrUnexpectedEOF},
		{"zero_nil", 0, nil, 0, io.ErrUnexpectedEOF},
		{"partial_eof", 2, io.EOF, 2, io.EOF},
		{"full_eof", 4, io.EOF, 4, io.EOF},
		{"negative_count", -1, nil, 0, nil},
		{"oversized_count", 5, nil, 0, nil},
		{"invalid_count_with_error", -1, readErr, 0, readErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, file := createReaderAtTestFile(t, []byte("abcdefgh"))
			f := &faultyReaderAtFile{File: file, readResult: &tc.n, readErr: tc.err}
			r, err := newReaderAt(path, f)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			calls, readBytes := readCallsReal.Get(), readBytesReal.Get()
			buf := make([]byte, 4)
			n, err := r.ReadAt(buf, 1)
			if n != tc.wantN || err == nil || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
				t.Fatalf("ReadAt result: n=%d err=%v; want n=%d err=%v", n, err, tc.wantN, tc.wantErr)
			}
			if tc.wantErr == io.EOF {
				if err != io.EOF {
					t.Fatalf("ReadAt must preserve the io.EOF sentinel: %v", err)
				}
			} else if !strings.HasPrefix(err.Error(), "[downsampling]") {
				t.Fatalf("read error is missing context: %v", err)
			}
			if f.reads != 1 || readCallsReal.Get() != calls+1 || readBytesReal.Get() != readBytes+uint64(tc.wantN) {
				t.Fatal("failed read was retried or its invalid byte count entered statistics")
			}
			f.readResult, f.readErr = nil, nil
			if n, err := r.ReadAt(buf, 0); n != len(buf) || err != nil || string(buf) != "abcd" {
				t.Fatalf("read error prevented a later read: n=%d data=%q err=%v", n, buf, err)
			}
		})
	}
}

func TestReaderAtInvalidOffsets(t *testing.T) {
	path, file := createReaderAtTestFile(t, []byte("data"))
	f := &faultyReaderAtFile{File: file}
	r, err := newReaderAt(path, f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	calls := readCallsReal.Get()
	for _, tc := range []struct {
		off  int64
		size int
	}{
		{-1, 1}, {-1, 0}, {math.MaxInt64, 1}, {math.MaxInt64 - 1, 2},
	} {
		if n, err := r.ReadAt(make([]byte, tc.size), tc.off); n != 0 || !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("invalid extent offset=%d size=%d: n=%d err=%v", tc.off, tc.size, n, err)
		}
	}
	if n, err := r.ReadAt(nil, math.MaxInt64); n != 0 || err != nil {
		t.Fatalf("empty read at a valid offset: n=%d err=%v", n, err)
	}
	if f.reads != 0 || readCallsReal.Get() != calls {
		t.Fatal("invalid or empty reads reached the backend")
	}
}

func TestReaderAtCloseOnce(t *testing.T) {
	for _, closeErr := range []error{nil, errors.New("injected close failure")} {
		name := "success"
		if closeErr != nil {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			path, file := createReaderAtTestFile(t, []byte("data"))
			f := &faultyReaderAtFile{File: file, closeErr: closeErr}
			before := readersCount.Get()
			r, err := newReaderAt(path, f)
			if err != nil {
				t.Fatal(err)
			}
			firstErr := r.Close()
			if !errors.Is(firstErr, closeErr) || (closeErr == nil && firstErr != nil) {
				t.Fatalf("Close: got %v; want %v", firstErr, closeErr)
			}
			if r.f != nil || r.Path() != path || r.Size() != 4 || f.closes != 1 || readersCount.Get() != before {
				t.Fatal("Close lost metadata, retained the file, or released its count incorrectly")
			}
			if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("real file was not closed: %v", err)
			}
			for i := 0; i < 3; i++ {
				if err := r.Close(); err != firstErr || f.closes != 1 || readersCount.Get() != before {
					t.Fatalf("repeated Close changed its result or released resources again: %v", err)
				}
			}
			calls := readCallsReal.Get()
			for _, buf := range [][]byte{nil, make([]byte, 1)} {
				if n, err := r.ReadAt(buf, 0); n != 0 || !errors.Is(err, os.ErrClosed) {
					t.Fatalf("read after Close: n=%d err=%v", n, err)
				}
			}
			if f.reads != 0 || readCallsReal.Get() != calls {
				t.Fatal("read after Close accessed the backend")
			}
		})
	}
}
