package filestream

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// spillMaxMemorySize bounds each spill's in-memory tail. A downsampling writer
// can hold five spills at once, so their buffers can occupy up to 80 MiB per job.
// Buffers grow on demand and are released on Close, without a large-buffer pool.
const spillMaxMemorySize = 16 * 1024 * 1024

// SpillWriter keeps a byte stream in memory until it exceeds spillMaxMemorySize.
// Each full buffer is then appended to one lazily created temporary file; the
// final tail remains in memory. It owns and removes the file and private directory.
//
// Write may be called repeatedly, followed by a single ReadAll. Close discards
// any unread data. Errors and ReadAll seal the writer against further writes.
// SpillWriter is not safe for concurrent use.
type SpillWriter struct {
	dir      string
	tempDir  string
	path     string
	f        spillFile
	memory   []byte // Pending tail; capacity never exceeds the memory threshold.
	fileSize uint64 // Bytes appended to the temporary file, preceding memory.
	size     uint64 // All accepted bytes, including memory; preserved after Close.
	state    spillState
	err      error

	// A positive value lets instance-level tests exercise spilling with small data.
	// Production leaves this zero and always uses spillMaxMemorySize.
	memoryLimit int

	// Per-instance removal allows testing cleanup failures without global hooks.
	remove func(string) error
}

type spillFile interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

type spillState uint8

const (
	spillWriting spillState = iota
	spillReading
	spillClosed
)

// NewSpillWriter returns a writer which only creates a temporary file under dir
// when its in-memory data exceeds spillMaxMemorySize. An empty dir uses the OS
// temporary directory. ReadAll consumes an in-memory tail without writing it out.
// The caller must eventually call ReadAll or Close, including on failure.
func NewSpillWriter(dir string) *SpillWriter {
	return &SpillWriter{
		dir:    dir,
		remove: os.Remove,
	}
}

// Write appends p to the spill. Size includes bytes accepted into its buffer.
// A failed Write seals the writer and immediately attempts to delete its files.
func (w *SpillWriter) Write(p []byte) (n int, err error) {
	if w.state != spillWriting {
		return 0, w.stateError()
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(len(p)) > math.MaxInt64-w.size {
		return 0, w.finish(fmt.Errorf("spill size exceeds the maximum file offset"))
	}
	writeCallsBuffered.Inc()
	defer func() { writtenBytesBuffered.Add(n) }()
	limit := w.memoryLimit
	if limit <= 0 {
		limit = spillMaxMemorySize
	}
	for len(p) > 0 {
		// Keep exactly one threshold in memory until another byte arrives.
		// This also splits large Writes without allocating a buffer for all of p.
		if len(w.memory) == limit {
			if err := w.dump(); err != nil {
				return n, w.finish(err)
			}
		}
		count := min(len(p), limit-len(w.memory))
		needed := len(w.memory) + count
		if needed > cap(w.memory) {
			// Clamp capacity as well as length; append's default growth could
			// retain more than the threshold even when length is bounded.
			capacity := min(limit, max(needed, 2*cap(w.memory)))
			memory := make([]byte, len(w.memory), capacity)
			copy(memory, w.memory)
			w.memory = memory
		}
		w.memory = append(w.memory, p[:count]...)
		w.size += uint64(count)
		n += count
		p = p[count:]
	}
	return n, nil
}

// dump appends a full memory buffer to the same file on each call. These large
// writes need no additional bufio.Writer; write errors surface in this Write.
func (w *SpillWriter) dump() error {
	if w.f == nil {
		if err := w.create(); err != nil {
			return err
		}
	}
	n, err := (spillFileWriter{w.f}).Write(w.memory)
	w.fileSize += uint64(n)
	if err != nil {
		return fmt.Errorf("cannot write spill file: %w", err)
	}
	w.memory = w.memory[:0]
	return nil
}

func (w *SpillWriter) create() error {
	dir, err := os.MkdirTemp(w.dir, ".spill-")
	if err != nil {
		return fmt.Errorf("cannot create spill directory in %q: %w", w.dir, err)
	}
	w.tempDir = dir
	w.path = filepath.Join(dir, "data")
	// Unlike os.CreateTemp's 0600, these permissions match os.Create used by
	// filestream.MustCreate. The private 0700 directory prevents external access.
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("cannot create spill file: %w", err)
	}
	w.f = f
	writersCount.Inc()
	return nil
}

// Size returns the total number of bytes accepted by Write, including buffered
// bytes. It remains available after reading or closing the spill.
func (w *SpillWriter) Size() uint64 {
	return w.size
}

// ReadAll streams the file prefix followed by the in-memory tail, without
// dumping the tail. The reader must consume exactly Size bytes and is only valid
// during consume. Reading to EOF is allowed; consuming fewer bytes is an error.
//
// ReadAll may be called only once. It always closes and removes the spill,
// including if seeking, reading, or consume fails. Cleanup errors are returned;
// Close can be called again to retry failed removal. Temporary files are never
// synced to storage.
func (w *SpillWriter) ReadAll(consume func(io.Reader) error) (err error) {
	if w.state != spillWriting {
		return w.stateError()
	}
	w.state = spillReading
	if w.f != nil {
		writersCount.Dec()
	}
	defer func() {
		err = w.finish(err)
	}()
	if consume == nil {
		return fmt.Errorf("nil spill consumer")
	}

	var source *spillReadSource
	r := &spillReader{
		owner:  w,
		r:      bytes.NewReader(w.memory),
		active: true,
	}
	if w.f != nil {
		// Check the complete file prefix independently of the memory tail.
		// A truncated prefix must not be silently filled by bytes from the tail.
		fileSize, err := w.f.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("cannot determine spill file size: %w", err)
		}
		if fileSize < int64(w.fileSize) {
			return fmt.Errorf("spill file has %d bytes; want %d: %w", fileSize, w.fileSize, io.ErrUnexpectedEOF)
		}
		if fileSize != int64(w.fileSize) {
			return fmt.Errorf("spill file has %d bytes; want %d", fileSize, w.fileSize)
		}
		offset, err := w.f.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("cannot rewind spill file: %w", err)
		}
		if offset != 0 {
			return fmt.Errorf("invalid rewind offset for spill file: got %d; want 0", offset)
		}
		source = &spillReadSource{r: w.f, remaining: w.fileSize}
		br := getSpillBufioReader(source)
		readersCount.Inc()
		r.r = io.MultiReader(br, r.r)
		defer func() {
			putSpillBufioReader(br)
			readersCount.Dec()
		}()
	}
	defer func() {
		r.active = false
		r.r = nil
		r.owner = nil
	}()
	err = consume(r)
	if source != nil && source.err != nil && !errors.Is(err, source.err) {
		err = errors.Join(err, fmt.Errorf("cannot read spill file: %w", source.err))
	}
	if r.err != nil && !errors.Is(err, r.err) {
		err = errors.Join(err, r.err)
	}
	if r.n < w.size {
		err = errors.Join(err, fmt.Errorf("spill consumer read %d bytes; want %d: %w", r.n, w.size, io.ErrUnexpectedEOF))
	} else if r.n > w.size {
		err = errors.Join(err, fmt.Errorf("spill consumer read %d bytes, exceeding the written size %d", r.n, w.size))
	}
	return err
}

// Close releases memory, closes the file, and removes its temporary file
// and directory. It is safe to call repeatedly. It returns earlier I/O errors as
// well as cleanup errors. Failed removal retains the path for the next Close.
func (w *SpillWriter) Close() error {
	wasWriting := w.state == spillWriting
	w.state = spillClosed
	w.memory = nil
	if w.f != nil {
		if wasWriting {
			writersCount.Dec()
		}
		if err := w.f.Close(); err != nil {
			w.err = errors.Join(w.err, fmt.Errorf("cannot close spill file: %w", err))
		}
		// os.File.Close closes the descriptor even when it reports an error.
		// Retrying Close could close an unrelated, reused descriptor.
		w.f = nil
	}
	var cleanupErr error
	if w.path != "" {
		if err := w.removePath(w.path); err != nil {
			cleanupErr = fmt.Errorf("cannot remove spill file %q: %w", w.path, err)
		} else {
			w.path = ""
		}
	}
	if w.tempDir != "" {
		if err := w.removePath(w.tempDir); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("cannot remove spill directory %q: %w", w.tempDir, err))
		} else {
			w.tempDir = ""
		}
	}
	return errors.Join(w.err, cleanupErr)
}

func (w *SpillWriter) removePath(path string) error {
	remove := w.remove
	if remove == nil {
		remove = os.Remove
	}
	err := remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (w *SpillWriter) finish(err error) error {
	if err != nil && !errors.Is(w.err, err) {
		w.err = errors.Join(w.err, err)
	}
	return w.Close()
}

func (w *SpillWriter) stateError() error {
	if w.err != nil {
		return w.err
	}
	return fmt.Errorf("spill is sealed: %w", os.ErrClosed)
}

// Count file writes and reject short or invalid counts without retrying them.
type spillFileWriter struct {
	w io.Writer
}

func (w spillFileWriter) Write(p []byte) (n int, err error) {
	startTime := time.Now()
	writeCallsReal.Inc()
	defer func() {
		writeDuration.Add(time.Since(startTime).Seconds())
		writtenBytesReal.Add(n)
	}()
	n, err = w.w.Write(p)
	if n < 0 || n > len(p) {
		return 0, errors.Join(fmt.Errorf("invalid spill write count %d for %d bytes", n, len(p)), err)
	}
	if n < len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

// Keep underlying read errors even if bufio.Reader or the consumer returns
// before surfacing an error that accompanied the last successful bytes.
type spillReadSource struct {
	r          io.Reader
	err        error
	emptyReads int
	remaining  uint64 // Only the recorded file prefix may precede the memory tail.
}

func (r *spillReadSource) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	startTime := time.Now()
	readCallsReal.Inc()
	defer func() {
		readDuration.Add(time.Since(startTime).Seconds())
		readBytesReal.Add(n)
	}()
	n, err = r.r.Read(p)
	if n < 0 || n > len(p) {
		n, err = 0, errors.Join(fmt.Errorf("invalid spill read count %d for %d bytes", n, len(p)), err)
	}
	r.remaining -= uint64(n)
	if errors.Is(err, io.EOF) && r.remaining > 0 {
		err = errors.Join(io.ErrUnexpectedEOF, err)
	}
	if n == 0 && err == nil && len(p) > 0 {
		r.emptyReads++
		// Match bufio's limit for readers that repeatedly make no progress.
		// bufio.Reader.Read itself doesn't enforce that limit on every path.
		if r.emptyReads >= 100 {
			err = io.ErrNoProgress
		}
	} else if n > 0 {
		r.emptyReads = 0
	}
	if err != nil && err != io.EOF && r.err == nil {
		r.err = err
	}
	return n, err
}

type spillReader struct {
	owner  *SpillWriter
	r      io.Reader
	active bool
	n      uint64
	err    error
}

func (r *spillReader) Read(p []byte) (int, error) {
	if !r.active || r.owner.state != spillReading {
		return 0, fmt.Errorf("spill reader is closed: %w", os.ErrClosed)
	}
	if r.err != nil {
		return 0, r.err
	}
	readCallsBuffered.Inc()
	n, err := r.r.Read(p)
	readBytesBuffered.Add(n)
	r.n += uint64(n)
	if err != nil && !errors.Is(err, io.EOF) && r.err == nil {
		r.err = err
	}
	return n, err
}

// Spill buffers have their own pools so their adapters and file references are
// released without changing the buffer ownership rules of regular filestreams.
func getSpillBufioReader(source *spillReadSource) *bufio.Reader {
	v := spillReaderPool.Get()
	if v == nil {
		return bufio.NewReaderSize(source, getReadBufferSize())
	}
	br := v.(*bufio.Reader)
	br.Reset(source)
	return br
}

func putSpillBufioReader(br *bufio.Reader) {
	br.Reset(nil)
	spillReaderPool.Put(br)
}

var spillReaderPool sync.Pool
