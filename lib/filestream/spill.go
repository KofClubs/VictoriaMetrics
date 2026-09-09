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
)

// SpillWriter temporarily stores a byte stream in a buffered file. It owns the
// temporary file and its private directory, including their removal.
//
// Write may be called repeatedly, followed by a single ReadAll. Close discards
// any unread data. Errors and ReadAll seal the writer against further writes.
// SpillWriter is not safe for concurrent use.
type SpillWriter struct {
	dir     string
	tempDir string
	path    string
	f       spillFile
	bw      *bufio.Writer
	size    uint64
	state   spillState
	err     error

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

// NewSpillWriter returns a writer whose temporary file is created lazily under
// dir on the first non-empty Write. An empty dir uses the OS temporary directory.
// The caller must eventually call ReadAll or Close, including on failure.
func NewSpillWriter(dir string) *SpillWriter {
	return &SpillWriter{
		dir:    dir,
		remove: os.Remove,
	}
}

// Write appends p to the spill. Size includes bytes accepted into its buffer.
// A failed Write seals the writer and immediately attempts to delete its files.
func (w *SpillWriter) Write(p []byte) (int, error) {
	if w.state != spillWriting {
		return 0, w.stateError()
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(len(p)) > math.MaxInt64-w.size {
		return 0, w.finish(fmt.Errorf("spill size exceeds the maximum file offset"))
	}
	if w.f == nil {
		if err := w.create(); err != nil {
			return 0, w.finish(err)
		}
	}
	writeCallsBuffered.Inc()
	n, err := w.bw.Write(p)
	writtenBytesBuffered.Add(n)
	w.size += uint64(n)
	if err != nil {
		return n, w.finish(fmt.Errorf("cannot write spill file: %w", err))
	}
	return n, nil
}

func (w *SpillWriter) create() error {
	dir, err := os.MkdirTemp(w.dir, ".spill-")
	if err != nil {
		return fmt.Errorf("cannot create spill directory in %q: %w", w.dir, err)
	}
	w.tempDir = dir
	w.path = filepath.Join(dir, "data")
	// Unlike os.CreateTemp's 0600, these permissions match os.Create used by
	// filestream.Create. The private 0700 directory prevents external access.
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("cannot create spill file: %w", err)
	}
	w.f = f
	w.bw = getBufioWriter(spillFileWriter{f})
	writersCount.Inc()
	return nil
}

// Size returns the total number of bytes accepted by Write, including buffered
// bytes. It remains available after reading or closing the spill.
func (w *SpillWriter) Size() uint64 {
	return w.size
}

// ReadAll flushes the spill and passes a streaming reader to consume exactly
// Size bytes. The reader is valid only for the duration of consume. Reading to
// EOF is allowed, but returning before consuming all bytes is an error.
//
// ReadAll may be called only once. It always closes and removes the spill,
// including if flushing, reading, or consume fails. Cleanup errors are returned;
// Close can be called again to retry failed removal. Temporary files are never
// synced to storage.
func (w *SpillWriter) ReadAll(consume func(io.Reader) error) (err error) {
	if w.state != spillWriting {
		return w.stateError()
	}
	w.state = spillReading
	defer func() {
		err = w.finish(err)
	}()
	if consume == nil {
		return fmt.Errorf("nil spill consumer")
	}

	var source *spillReadSource
	r := &spillReader{
		owner:  w,
		r:      bytes.NewReader(nil),
		active: true,
	}
	if w.f != nil {
		if err := w.bw.Flush(); err != nil {
			return fmt.Errorf("cannot flush spill file: %w", err)
		}
		w.releaseWriter()
		offset, err := w.f.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("cannot rewind spill file: %w", err)
		}
		if offset != 0 {
			return fmt.Errorf("invalid rewind offset for spill file: got %d; want 0", offset)
		}
		source = &spillReadSource{r: w.f}
		br := getBufioReader(source)
		readersCount.Inc()
		r.r = br
		defer func() {
			putBufioReader(br)
			readersCount.Dec()
		}()
	}
	defer func() {
		r.active = false
		r.r = nil
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

// Close discards buffered data, closes the file, and removes its temporary file
// and directory. It is safe to call repeatedly. It returns earlier I/O errors as
// well as cleanup errors. Failed removal retains the path for the next Close.
func (w *SpillWriter) Close() error {
	w.state = spillClosed
	w.releaseWriter()
	if w.f != nil {
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

func (w *SpillWriter) releaseWriter() {
	if w.bw != nil {
		putBufioWriter(w.bw)
		w.bw = nil
		writersCount.Dec()
	}
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

// bufio.Writer's direct-write path retries short writes with no error. Turn
// these into errors so a broken writer cannot silently lose data or loop.
type spillFileWriter struct {
	w io.Writer
}

func (w spillFileWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n < 0 || n > len(p) {
		return 0, fmt.Errorf("invalid spill write count %d for %d bytes", n, len(p))
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
}

func (r *spillReadSource) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.r.Read(p)
	if n < 0 || n > len(p) {
		n, err = 0, fmt.Errorf("invalid spill read count %d for %d bytes", n, len(p))
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
	if err != nil && !errors.Is(err, io.EOF) && r.err == nil {
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
