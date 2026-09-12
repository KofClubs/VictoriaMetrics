package filestream

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
)

func TestSpillWriterInvalidMemoryLimit(t *testing.T) {
	old := spillMaxMemorySize.N
	t.Cleanup(func() { spillMaxMemorySize.N = old })
	for _, limit := range []int64{0, -1} {
		t.Run(strconv.FormatInt(limit, 10), func(t *testing.T) {
			spillMaxMemorySize.N = limit
			dir := t.TempDir()
			w := NewSpillWriter(dir, "invalid-limit")
			defer w.Close()
			n, err := w.Write([]byte("data"))
			if n != 0 || err == nil || !strings.Contains(err.Error(), "must be positive") || !w.closed {
				t.Fatalf("invalid limit did not fail immediately: n=%d err=%v closed=%t", n, err, w.closed)
			}
			assertSpillRemoved(t, w, dir)
		})
	}
}

func TestSpillWriterSharedMemoryBudget(t *testing.T) {
	budget := &memory.Limiter{MaxSize: 64}
	dir := t.TempDir()
	newWriter := func(name string) *SpillWriter {
		w := NewSpillWriter(dir, name)
		w.memoryLimit, w.memoryBudget = 64, budget
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	a, b := newWriter("a"), newWriter("b")
	first := bytes.Repeat([]byte("a"), 48)
	second := bytes.Repeat([]byte("b"), 48)
	if _, err := a.Write(first); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(second); err != nil {
		t.Fatal(err)
	}
	if a.f != nil || cap(a.memory) != 48 || b.fileSize != 48 || cap(b.memory) != 0 {
		t.Fatal("shared budget did not keep the first spill in memory and write the second to disk")
	}
	// Growing a must account for both the old and new buffer; fall back without waiting.
	if _, err := a.Write(second); err != nil {
		t.Fatal(err)
	}
	if cap(a.memory) != 0 || a.fileSize != 96 {
		t.Fatal("growth exceeded the combined allocation budget")
	}
	for _, tc := range []struct {
		w    *SpillWriter
		want []byte
	}{{a, append(append([]byte(nil), first...), second...)}, {b, second}} {
		if err := tc.w.Read(func(r io.Reader) error {
			got, err := io.ReadAll(r)
			if !bytes.Equal(got, tc.want) {
				t.Fatal("budget fallback changed the stream")
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	c := newWriter("c")
	if _, err := c.Write(make([]byte, 64)); err != nil || cap(c.memory) != 64 {
		t.Fatalf("released budget could not be reused: %v", err)
	}
	for _, w := range []*SpillWriter{a, b, c} {
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !budget.Get(budget.MaxSize) {
		t.Fatal("Close leaked the shared memory reservation")
	}
	budget.Put(budget.MaxSize)
	assertSpillRemoved(t, a, dir)
}

func TestSpillWriterBudgetFallbackFailure(t *testing.T) {
	for _, failDump := range []bool{false, true} {
		t.Run(strconv.FormatBool(failDump), func(t *testing.T) {
			w, f, dir := newFaultSpill(t)
			budget := &memory.Limiter{MaxSize: 16}
			w.memoryBudget = budget
			if failDump {
				if _, err := w.Write(make([]byte, 16)); err != nil {
					t.Fatal(err)
				}
			}
			f.write = func(p []byte) (int, error) { return len(p) / 2, nil }
			n, err := w.Write(make([]byte, 32))
			wantN := 16
			if failDump {
				wantN = 0
			}
			if n != wantN || !errors.Is(err, io.ErrShortWrite) || !w.closed || f.closeCalls != 1 {
				t.Fatalf("budget fallback failure: n=%d err=%v close calls=%d", n, err, f.closeCalls)
			}
			if !budget.Get(budget.MaxSize) {
				t.Fatal("failed spill retained its memory reservation")
			}
			budget.Put(budget.MaxSize)
			assertSpillRemoved(t, w, dir)
		})
	}
}

func TestSpillWriterAppendAfterIncompleteRead(t *testing.T) {
	w := NewSpillWriter(t.TempDir(), "append")
	w.memoryBudget = &memory.Limiter{} // Exercise direct writes with no available buffer budget.
	defer w.Close()
	data := bytes.Repeat([]byte("original"), 32768)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Read(func(r io.Reader) error {
		_, err := io.ReadFull(r, make([]byte, 1))
		return err
	}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("incomplete Read did not fail: %v", err)
	}
	if _, err := w.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := w.Read(func(r io.Reader) error {
		got, err := io.ReadAll(r)
		if !bytes.Equal(got, append(data, []byte("tail")...)) {
			t.Fatal("append after an incomplete Read overwrote the file prefix")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSpillWriterRoundTrip(t *testing.T) {
	const limit = 1024
	for _, size := range []int{0, 1, limit - 1, limit, limit + 1, 3 * limit, 3*limit + 1, 20*limit + 17} {
		for _, chunkSize := range []int{1, 373, 32 * limit} {
			t.Run(strconv.Itoa(size)+"/chunk_"+strconv.Itoa(chunkSize), func(t *testing.T) {
				dir := t.TempDir()
				w := NewSpillWriter(dir, "spill")
				w.memoryLimit = limit
				defer w.Close()
				data := bytes.Repeat([]byte("aBc012"), size/6+1)[:size]
				var file spillFile
				var path string
				for pos := 0; pos < len(data); {
					end := min(pos+chunkSize, len(data))
					n, err := w.Write(data[pos:end])
					if err != nil || n != end-pos {
						t.Fatalf("Write: got (%d, %v); want (%d, nil)", n, err, end-pos)
					}
					pos = end
					if w.Size() != uint64(pos) {
						t.Fatalf("Size: got %d; want %d", w.Size(), pos)
					}
					wantFileSize := uint64((pos - 1) / limit * limit)
					if w.fileSize != wantFileSize || len(w.memory) != pos-int(wantFileSize) || cap(w.memory) > limit {
						t.Fatalf("prefix/tail boundary: file=%d memory=%d cap=%d; total=%d", w.fileSize, len(w.memory), cap(w.memory), pos)
					}
					if wantFileSize == 0 {
						if w.f != nil {
							t.Fatal("a stream within the memory limit created a file")
						}
					} else {
						if file == nil {
							file, path = w.f, w.path
						}
						if w.f != file || w.path != path {
							t.Fatal("a later dump replaced the original spill file")
						}
						info, err := os.Stat(w.path)
						if err != nil || uint64(info.Size()) != wantFileSize {
							t.Fatalf("dump size: info=%v err=%v; want %d", info, err, wantFileSize)
						}
					}
				}
				var saved io.Reader
				if err := w.Read(func(r io.Reader) error {
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
				if r := saved.(*countingReader); r.r != nil {
					t.Fatal("escaped reader retained its input after ReadAll")
				}
				// Read 是只读的：不释放资源也不删除文件，Close 是最后的显式调用。
				if err := w.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				assertSpillRemoved(t, w, dir)
				if err := w.Close(); err != nil {
					t.Fatalf("repeated Close: %v", err)
				}
				if _, err := w.Write(nil); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("Write after Close: got %v; want ErrClosed", err)
				}
				if err := w.Read(func(io.Reader) error {
					t.Fatal("ReadAll called consumer after Close")
					return nil
				}); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("Read after Close: got %v; want ErrClosed", err)
				}
			})
		}
	}
}

func TestSpillWriterLazyCreationAndDiscard(t *testing.T) {
	limit := spillMaxMemorySize.IntN()
	for _, size := range []int{0, 1, limit - 1, limit} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			dir := t.TempDir()
			missing := filepath.Join(dir, "missing", "directory")
			w := NewSpillWriter(missing, "spill")
			w.remove = func(string) error {
				t.Fatal("memory-only cleanup must not attempt filesystem removal")
				return nil
			}
			if len(w.memory) != 0 || cap(w.memory) != 0 {
				t.Fatal("new spill eagerly allocated memory")
			}
			if n, err := w.Write(nil); n != 0 || err != nil {
				t.Fatalf("empty Write: (%d, %v)", n, err)
			}
			data := bytes.Repeat([]byte("x"), size)
			if n, err := w.Write(data); n != size || err != nil {
				t.Fatalf("memory-only Write with missing directory: (%d, %v)", n, err)
			}
			if w.f != nil || w.fileSize != 0 || cap(w.memory) > limit {
				t.Fatal("memory-only write created a file or exceeded the capacity limit")
			}
			if size == 1 && cap(w.memory) >= limit {
				t.Fatal("a small write eagerly allocated the full production limit")
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("repeated Close: %v", err)
			}
			if w.Size() != uint64(size) {
				t.Fatal("Close changed the accepted size")
			}
			assertSpillRemoved(t, w, dir)
		})
	}
	dir := t.TempDir()
	w := NewSpillWriter(filepath.Join(dir, "missing"), "spill")
	w.memoryLimit = 32
	data := bytes.Repeat([]byte("x"), w.memoryLimit)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Read(func(r io.Reader) error {
		got, err := io.ReadAll(r)
		if !bytes.Equal(got, data) {
			t.Fatal("memory-only ReadAll changed data")
		}
		return err
	}); err != nil {
		t.Fatalf("memory-only ReadAll with missing directory: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("memory-only Close with missing directory: %v", err)
	}
	assertSpillRemoved(t, w, dir)
}

func TestSpillWriterMetrics(t *testing.T) {
	for _, spilled := range []bool{false, true} {
		name := "memory"
		if spilled {
			name = "spilled"
		}
		t.Run(name, func(t *testing.T) {
			readers, writers := readersCount.Get(), writersCount.Get()
			bufferedReads, bufferedReadBytes := readCallsBuffered.Get(), readBytesBuffered.Get()
			bufferedWrites, bufferedWrittenBytes := writeCallsBuffered.Get(), writtenBytesBuffered.Get()
			realReads, realReadBytes := readCallsReal.Get(), readBytesReal.Get()
			realWrites, realWrittenBytes := writeCallsReal.Get(), writtenBytesReal.Get()
			w := NewSpillWriter(t.TempDir(), "spill")
			w.memoryLimit = 64
			defer w.Close()
			size := 13
			if spilled {
				size = w.memoryLimit + 13
			}
			data := bytes.Repeat([]byte("m"), size)
			if _, err := w.Write(data); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(nil); err != nil {
				t.Fatal(err)
			}
			wantFiles := uint64(0)
			wantFileBytes := uint64(0)
			if spilled {
				wantFiles = 1
				wantFileBytes = uint64(w.memoryLimit)
			}
			if writersCount.Get() != writers+wantFiles || readersCount.Get() != readers {
				t.Fatal("only a physical spill file may acquire an active writer")
			}
			if writeCallsBuffered.Get() != bufferedWrites+1 || writtenBytesBuffered.Get() != bufferedWrittenBytes+uint64(len(data)) {
				t.Fatal("logical spill write metrics do not match non-empty calls and accepted bytes")
			}
			if writeCallsReal.Get() != realWrites+wantFiles || writtenBytesReal.Get() != realWrittenBytes+wantFileBytes {
				t.Fatal("real write metrics include memory bytes or omit the file prefix")
			}
			actualFileReads := uint64(0)
			if spilled {
				f := &spillFaultFile{File: w.f.(*os.File)}
				f.read = func(p []byte) (int, error) {
					actualFileReads++
					return f.File.Read(p)
				}
				w.f = f
			}
			logicalReads := uint64(0)
			if err := w.Read(func(r io.Reader) error {
				if readersCount.Get() != readers+wantFiles || writersCount.Get() != writers+wantFiles {
					t.Fatal("ReadAll must keep the spill writer while temporarily adding a file reader")
				}
				got := make([]byte, len(data))
				for pos := 0; pos < len(got); {
					logicalReads++
					n, err := r.Read(got[pos:])
					pos += n
					if err != nil && !(err == io.EOF && pos == len(got)) {
						return err
					}
					if n == 0 {
						return io.ErrNoProgress
					}
				}
				if !bytes.Equal(got, data) {
					t.Fatal("read data differs")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if readersCount.Get() != readers || writersCount.Get() != writers+wantFiles {
				t.Fatal("ReadAll released the file reader but must keep the spill writer for the pending Close")
			}
			if readCallsBuffered.Get() != bufferedReads+logicalReads || readBytesBuffered.Get() != bufferedReadBytes+uint64(len(data)) {
				t.Fatal("logical read statistics do not match consumer reads")
			}
			if readCallsReal.Get() != realReads+actualFileReads || readBytesReal.Get() != realReadBytes+wantFileBytes {
				t.Fatal("real read statistics include memory tail bytes or omit the file prefix")
			}
			if spilled && actualFileReads == 0 {
				t.Fatal("spilled prefix was not read from its file")
			}
			if writeCallsReal.Get() != realWrites+wantFiles || writtenBytesReal.Get() != realWrittenBytes+wantFileBytes {
				t.Fatal("ReadAll flushed the memory tail")
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if readersCount.Get() != readers || writersCount.Get() != writers {
				t.Fatal("Close did not release the spill writer after Read")
			}
		})
	}
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
	w := NewSpillWriter(dir, "spill")
	w.memoryLimit = 2
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
}

func TestSpillWriterCreateFailure(t *testing.T) {
	dir := t.TempDir()
	w := NewSpillWriter(filepath.Join(dir, "missing", "directory"), "spill")
	w.memoryLimit = 4
	if n, err := w.Write([]byte("data")); n != 4 || err != nil {
		t.Fatalf("Write at limit must not touch missing directory: (%d, %v)", n, err)
	}
	if n, err := w.Write([]byte("x")); n != 0 || !errors.Is(err, os.ErrNotExist) || w.Size() != 4 {
		t.Fatalf("Write crossing limit with missing parent: n=%d size=%d err=%v", n, w.Size(), err)
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
		name        string
		write       func([]byte) (int, error)
		want        error
		halfWritten bool
	}{
		{"error", func([]byte) (int, error) { return 0, writeErr }, writeErr, false},
		{"partial_error", func(p []byte) (int, error) { return len(p) / 2, writeErr }, writeErr, true},
		{"short_write", func(p []byte) (int, error) { return len(p) / 2, nil }, io.ErrShortWrite, true},
		{"no_progress", func([]byte) (int, error) { return 0, nil }, io.ErrShortWrite, false},
		{"negative_count", func([]byte) (int, error) { return -1, nil }, nil, false},
		{"excess_count", func(p []byte) (int, error) { return len(p) + 1, nil }, nil, false},
	} {
		for _, prefilled := range []bool{false, true} {
			name := tc.name + "/same_call"
			if prefilled {
				name = tc.name + "/previous_call"
			}
			t.Run(name, func(t *testing.T) {
				w, f, dir := newFaultSpill(t)
				if prefilled {
					if _, err := w.Write(make([]byte, w.memoryLimit)); err != nil {
						t.Fatal(err)
					}
				}
				f.write = tc.write
				calls, written := writeCallsReal.Get(), writtenBytesReal.Get()
				logicalCalls, accepted := writeCallsBuffered.Get(), writtenBytesBuffered.Get()
				n, err := w.Write(make([]byte, 2*w.memoryLimit+1))
				if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
					t.Fatalf("Write: got %v; want %v", err, tc.want)
				}
				wantN := w.memoryLimit
				if prefilled {
					wantN = 0
				}
				if n != wantN || w.Size() != uint64(w.memoryLimit) {
					t.Fatalf("accepted n=%d Size=%d; want %d/%d", n, w.Size(), wantN, w.memoryLimit)
				}
				wantPhysical := uint64(0)
				if tc.halfWritten {
					wantPhysical = uint64(w.memoryLimit / 2)
				}
				if writeCallsReal.Get() != calls+1 || writtenBytesReal.Get() != written+wantPhysical {
					t.Fatal("dump statistics confused logical acceptance with physical partial write")
				}
				if writeCallsBuffered.Get() != logicalCalls+1 || writtenBytesBuffered.Get() != accepted+uint64(wantN) {
					t.Fatal("logical failure statistics lost accepted memory bytes")
				}
				if n, err := w.Write([]byte("retry")); n != 0 || err == nil {
					t.Fatalf("Write after failure: (%d,%v)", n, err)
				}
				if err := w.Read(func(io.Reader) error { t.Fatal("consumer called after failure"); return nil }); err == nil {
					t.Fatal("ReadAll succeeded after failed dump")
				}
				if f.closeCalls != 1 {
					t.Fatalf("file closed %d times; want 1", f.closeCalls)
				}
				assertSpillRemoved(t, w, dir)
			})
		}
	}
}

func TestSpillWriterSeekFailures(t *testing.T) {
	injected := errors.New("injected seek failure")
	for _, tc := range []struct {
		name        string
		whence      int
		wrongOffset bool
	}{
		{"end", io.SeekEnd, false}, {"start", io.SeekStart, false},
		{"wrong_end_offset", io.SeekEnd, true}, {"wrong_start_offset", io.SeekStart, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, f, dir := newFaultSpill(t)
			if _, err := w.Write(make([]byte, w.memoryLimit+1)); err != nil {
				t.Fatal(err)
			}
			f.seek = func(off int64, whence int) (int64, error) {
				if whence != tc.whence {
					return f.File.Seek(off, whence)
				}
				if !tc.wrongOffset {
					return 0, injected
				}
				n, err := f.File.Seek(off, whence)
				return n + 1, err
			}
			err := w.Read(func(io.Reader) error { t.Fatal("consumer called after seek validation failure"); return nil })
			if err == nil || !tc.wrongOffset && !errors.Is(err, injected) {
				t.Fatalf("ReadAll seek error: %v", err)
			}
			if f.closeCalls != 0 {
				t.Fatalf("read-only seek failure must not close the file: closes=%d", f.closeCalls)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close after seek failure: %v", err)
			}
			if f.closeCalls != 1 {
				t.Fatalf("Close after seek failure closed file %d times", f.closeCalls)
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
			data := bytes.Repeat([]byte("r"), w.memoryLimit+5)
			if _, err := w.Write(data); err != nil {
				t.Fatal(err)
			}
			f.read = func(p []byte) (int, error) { return tc.read(f.File, p) }
			err := w.Read(func(r io.Reader) error {
				// Deliberately ignore the reader's error. ReadAll must still fail,
				// including when bufio hides an error delivered with the last bytes.
				_, _ = io.ReadFull(r, make([]byte, len(data)))
				return nil
			})
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("ReadAll: got %v; want %v", err, tc.want)
			}
			if f.closeCalls != 0 {
				t.Fatalf("read-only Read closed the file after a read failure: closes=%d", f.closeCalls)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close after read failure: %v", err)
			}
			if f.closeCalls != 1 {
				t.Fatalf("Close after read failure closed file %d times", f.closeCalls)
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
		for _, limit := range []int{2, 8} {
			t.Run(tc.name+"/limit_"+strconv.Itoa(limit), func(t *testing.T) {
				dir := t.TempDir()
				w := NewSpillWriter(dir, "spill")
				w.memoryLimit = limit
				if _, err := w.Write([]byte("data")); err != nil {
					t.Fatal(err)
				}
				err := w.Read(tc.consume)
				if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
					t.Fatalf("ReadAll: got %v; want %v", err, tc.want)
				}
				// Read 是只读的，失败不会 seal writer；后续仍可写入，但须显式 Close。
				if _, err := w.Write([]byte("retry")); err != nil {
					t.Fatalf("Write after consumer failure: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("Close after consumer failure: %v", err)
				}
				assertSpillRemoved(t, w, dir)
			})
		}
	}
}

func TestSpillWriterReadIsReadOnlyAndRepeatable(t *testing.T) {
	w, f, dir := newFaultSpill(t)
	data := bytes.Repeat([]byte("e"), w.memoryLimit+9)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	// First read: exact consumption without an extra EOF read.
	if err := w.Read(func(r io.Reader) error {
		got := make([]byte, len(data))
		if _, err := io.ReadFull(r, got); err != nil {
			return err
		}
		if !bytes.Equal(got, data) {
			t.Fatal("data mismatch")
		}
		return nil
	}); err != nil {
		t.Fatalf("exact read without extra EOF read: %v", err)
	}
	// Read 是只读的：不 seal、不关闭文件、不删除文件。
	if f.closeCalls != 0 {
		t.Fatalf("read-only Read closed the file: closes=%d", f.closeCalls)
	}
	if _, err := os.Stat(w.path); err != nil {
		t.Fatalf("read-only Read removed or lost the file: %v", err)
	}
	// Read 可重复调用，每次都重新输出完整流。
	if err := w.Read(func(r io.Reader) error {
		got, err := io.ReadAll(r)
		if !bytes.Equal(got, data) {
			t.Fatal("repeated Read data mismatch")
		}
		return err
	}); err != nil {
		t.Fatalf("repeated Read: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if f.closeCalls != 1 {
		t.Fatalf("file closed %d times; want 1", f.closeCalls)
	}
	assertSpillRemoved(t, w, dir)
}

func TestSpillWriterCleanupRetry(t *testing.T) {
	removeErr := errors.New("injected remove failure")
	w, f, dir := newFaultSpill(t)
	if _, err := w.Write(make([]byte, w.memoryLimit+1)); err != nil {
		t.Fatal(err)
	}
	failPath := w.path
	w.remove = func(path string) error {
		if path == failPath {
			return removeErr
		}
		return os.Remove(path)
	}
	// Read 是只读的，不删除文件。
	if err := w.Read(func(r io.Reader) error { _, err := io.Copy(io.Discard, r); return err }); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, err := os.Stat(failPath); err != nil {
		t.Fatalf("read-only Read lost its path: %v", err)
	}
	if !w.created {
		t.Fatal("read-only Read discarded cleanup state")
	}
	if err := w.Close(); !errors.Is(err, removeErr) {
		t.Fatalf("Close with failing removal: got %v; want injected error", err)
	}
	if _, err := os.Stat(failPath); err != nil {
		t.Fatalf("failed removal lost its path: %v", err)
	}
	if !w.created {
		t.Fatal("failed cleanup discarded retry state")
	}
	w.remove = os.Remove
	if err := w.Close(); err != nil {
		t.Fatalf("retry after removal becomes available: %v", err)
	}
	if f.closeCalls != 1 {
		t.Fatalf("file closed %d times; want 1", f.closeCalls)
	}
	assertSpillRemoved(t, w, dir)
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
	_, err := w.Write(make([]byte, w.memoryLimit+1))
	if !errors.Is(err, writeErr) || !errors.Is(err, removeErr) {
		t.Fatalf("Write lost a write or cleanup error: %v", err)
	}
	if !w.created {
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
	dir := t.TempDir()
	w := NewSpillWriter(dir, "spill")
	w.memoryLimit = 64 * 1024
	defer w.Close()
	chunk := bytes.Repeat([]byte("abcdefgh"), 8*1024)
	want := sha256.New()
	const chunks = 256
	for range chunks {
		if _, err := w.Write(chunk); err != nil {
			t.Fatal(err)
		}
		_, _ = want.Write(chunk)
		if cap(w.memory) > w.memoryLimit || len(w.memory) > w.memoryLimit {
			t.Fatalf("memory grew beyond its limit: len=%d cap=%d", len(w.memory), cap(w.memory))
		}
		if w.fileSize+uint64(len(w.memory)) != w.Size() {
			t.Fatal("file and memory sizes do not cover accepted bytes")
		}
	}
	if w.Size() != chunks*uint64(len(chunk)) {
		t.Fatalf("Size: %d", w.Size())
	}
	got := sha256.New()
	if err := w.Read(func(r io.Reader) error {
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
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertSpillRemoved(t, w, dir)
}

func TestSpillWriterInputDoesNotAlias(t *testing.T) {
	for _, size := range []int{17, 3*32 + 7} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			dir := t.TempDir()
			w := NewSpillWriter(dir, "spill")
			w.memoryLimit = 32
			defer w.Close()
			input := bytes.Repeat([]byte("mutable-input"), size/13+1)[:size]
			want := append([]byte(nil), input...)
			if _, err := w.Write(input); err != nil {
				t.Fatal(err)
			}
			clear(input)
			if err := w.Read(func(r io.Reader) error {
				got, err := io.ReadAll(r)
				if !bytes.Equal(got, want) {
					t.Fatal("spill retained the caller's mutable buffer")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			assertSpillRemoved(t, w, dir)
		})
	}
}

func TestSpillWriterLaterDumpFailure(t *testing.T) {
	w, f, dir := newFaultSpill(t)
	writeErr := errors.New("injected second dump failure")
	calls := 0
	f.write = func(p []byte) (int, error) {
		calls++
		if calls == 2 {
			n, err := f.File.Write(p[:len(p)/2])
			return n, errors.Join(err, writeErr)
		}
		return f.File.Write(p)
	}
	physicalBefore := writtenBytesReal.Get()
	acceptedBefore := writtenBytesBuffered.Get()
	n, err := w.Write(make([]byte, 3*w.memoryLimit+1))
	if n != 2*w.memoryLimit || w.Size() != uint64(n) || !errors.Is(err, writeErr) {
		t.Fatalf("second dump failure: n=%d Size=%d err=%v", n, w.Size(), err)
	}
	wantPhysical := uint64(w.memoryLimit + w.memoryLimit/2)
	if calls != 2 || w.fileSize != wantPhysical || writtenBytesReal.Get() != physicalBefore+wantPhysical {
		t.Fatal("second dump lost the first prefix or confused physical and accepted sizes")
	}
	if writtenBytesBuffered.Get() != acceptedBefore+uint64(n) {
		t.Fatal("logical write statistics lost bytes accepted before the later failure")
	}
	if err := w.Close(); !errors.Is(err, writeErr) || f.closeCalls != 1 {
		t.Fatalf("later cleanup lost the write error or closed the file again: %v", err)
	}
	assertSpillRemoved(t, w, dir)
}

func TestSpillWriterFileExtentValidation(t *testing.T) {
	for _, delta := range []int64{-1, 1} {
		t.Run(strconv.FormatInt(delta, 10), func(t *testing.T) {
			w, f, dir := newFaultSpill(t)
			if _, err := w.Write(make([]byte, w.memoryLimit+5)); err != nil {
				t.Fatal(err)
			}
			if err := f.File.Truncate(int64(w.fileSize) + delta); err != nil {
				t.Fatal(err)
			}
			err := w.Read(func(io.Reader) error {
				t.Fatal("consumer ran with a truncated or extended file prefix")
				return nil
			})
			if err == nil || delta < 0 && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("invalid prefix size was accepted: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close after extent validation failure: %v", err)
			}
			assertSpillRemoved(t, w, dir)
		})
	}
}

func TestSpillWriterTruncatedPrefixCannotUseMemoryTail(t *testing.T) {
	w, f, dir := newFaultSpill(t)
	prefix := bytes.Repeat([]byte("f"), w.memoryLimit)
	tail := bytes.Repeat([]byte("m"), w.memoryLimit-2)
	if _, err := w.Write(append(prefix, tail...)); err != nil {
		t.Fatal(err)
	}
	// Change the file after ReadAll has checked its size. The source reader
	// itself must detect the missing prefix, before MultiReader reaches memory.
	f.seek = func(offset int64, whence int) (int64, error) {
		if whence == io.SeekStart {
			if err := f.File.Truncate(int64(w.fileSize) - 3); err != nil {
				return 0, err
			}
		}
		return f.File.Seek(offset, whence)
	}
	err := w.Read(func(r io.Reader) error {
		got, err := io.ReadAll(r)
		if !bytes.Equal(got, prefix[:len(prefix)-3]) {
			t.Fatalf("memory tail was used after an incomplete prefix: %q", got)
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("consumer did not see the incomplete file prefix: %v", err)
		}
		return nil // ReadAll must preserve the source error independently.
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadAll ignored a truncated prefix: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close after truncated prefix: %v", err)
	}
	assertSpillRemoved(t, w, dir)
}

func TestSpillWriterCloseDiscardsMemoryTail(t *testing.T) {
	w, f, dir := newFaultSpill(t)
	if _, err := w.Write(make([]byte, w.memoryLimit+5)); err != nil {
		t.Fatal(err)
	}
	f.write = func([]byte) (int, error) {
		t.Fatal("Close wrote the remaining memory tail")
		return 0, io.ErrClosedPipe
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil || f.closeCalls != 1 {
		t.Fatalf("repeated Close: err=%v closes=%d", err, f.closeCalls)
	}
	assertSpillRemoved(t, w, dir)
}

func BenchmarkSpillWriter(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"Memory", 1024 * 1024},
		{"Spilled", spillMaxMemorySize.IntN() + 1024*1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			data := bytes.Repeat([]byte("b"), tc.size)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := NewSpillWriter(dir, "spill")
				if _, err := w.Write(data); err != nil {
					_ = w.Close()
					b.Fatal(err)
				}
				if err := w.Read(func(r io.Reader) error {
					_, err := io.Copy(io.Discard, r)
					return err
				}); err != nil {
					_ = w.Close()
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
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
			w := NewSpillWriter(dir, strconv.Itoa(i))
			w.memoryLimit = 1024
			if i%2 == 0 {
				w.memoryLimit = 20_000
			}
			defer w.Close()
			data := strings.Repeat(string(rune('a'+i)), 10_000)
			if _, err := w.Write([]byte(data)); err != nil {
				t.Error(err)
				return
			}
			if err := w.Read(func(r io.Reader) error {
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
	assertSpillRemoved(t, NewSpillWriter(dir, "final"), dir)
}

func newFaultSpill(t *testing.T) (*SpillWriter, *spillFaultFile, string) {
	t.Helper()
	dir := t.TempDir()
	w := NewSpillWriter(dir, "spill")
	w.memoryLimit = 32
	if err := w.create(); err != nil {
		t.Fatal(err)
	}
	f := &spillFaultFile{File: w.f.(*os.File)}
	w.f = f
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
	if w.f != nil || w.memory != nil || w.created {
		t.Fatalf("spill retained resources: file=%v, buffer=%v, created=%v", w.f, w.memory, w.created)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("spill left files: entries=%v, err=%v", entries, err)
	}
}
