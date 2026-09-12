package filestream

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

// ReadAtCloser provides error-returning access to a regular file without a
// sequential cursor. It is used by downsampling merges and part validation;
// queries use fs.ReaderAt and its mmap support.
type ReadAtCloser interface {
	Path() string
	Size() uint64
	ReadAt(p []byte, off int64) (int, error)
	Close() error
}

// ReaderAt reads directly into the caller's buffer at the requested offset.
// It has no read buffer or sequential cursor. Concurrent ReadAt calls are safe;
// Close must not run concurrently with ReadAt or another Close call.
type ReaderAt struct {
	f        readerAtFile
	path     string
	size     uint64
	closeErr error
}

type readerAtFile interface {
	io.ReaderAt
	Stat() (os.FileInfo, error)
	Close() error
}

var _ ReadAtCloser = (*ReaderAt)(nil)

// OpenReadAt opens path for reading and checks the opened file is regular.
// Size is captured from that file descriptor; no file data is cached.
func OpenReadAt(path string) (*ReaderAt, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("[downsampling] cannot open input file %q: %w", path, err)
	}
	return newReaderAt(path, f)
}

// Path returns the opened path, including after Close.
func (r *ReaderAt) Path() string {
	return r.path
}

// Size returns the file size observed at open time, including after Close.
// Reads still use the actual file and report EOF if it has since been truncated.
func (r *ReaderAt) Size() uint64 {
	return r.size
}

// ReadAt reads p from off without changing a sequential cursor. It follows
// io.ReaderAt EOF semantics; a read error does not prevent subsequent reads.
func (r *ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if r.f == nil {
		return 0, fmt.Errorf("[downsampling] cannot read closed input file %q: %w", r.path, os.ErrClosed)
	}
	if off < 0 || int64(len(p)) > math.MaxInt64-off {
		return 0, fmt.Errorf("[downsampling] invalid input extent for %q: offset=%d, size=%d: %w", r.path, off, len(p), os.ErrInvalid)
	}
	if len(p) == 0 {
		return 0, nil
	}
	startTime := time.Now()
	readCallsReal.Inc()
	n, err := r.f.ReadAt(p, off)
	readDuration.Add(time.Since(startTime).Seconds())
	if n < 0 || n > len(p) {
		return 0, errors.Join(fmt.Errorf("[downsampling] invalid input read count %d for %d bytes at offset %d in %q", n, len(p), off, r.path), err)
	}
	readBytesReal.Add(n)
	if n < len(p) && err == nil {
		err = io.ErrUnexpectedEOF
	}
	if err == io.EOF {
		return n, io.EOF
	}
	if err != nil {
		return n, fmt.Errorf("[downsampling] cannot read %d bytes at offset %d from %q: %w", len(p), off, r.path, err)
	}
	return n, nil
}

// Close closes the file once and caches the result, including a close error.
// After Close, ReadAt returns os.ErrClosed and does not access the file again.
func (r *ReaderAt) Close() error {
	if r.f == nil {
		return r.closeErr
	}
	f := r.f
	r.f = nil
	if err := f.Close(); err != nil {
		r.closeErr = fmt.Errorf("[downsampling] cannot close input file %q: %w", r.path, err)
	}
	readersCount.Dec()
	return r.closeErr
}

func newReaderAt(path string, f readerAtFile) (_ *ReaderAt, err error) {
	defer func() {
		if err != nil {
			if closeErr := f.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("[downsampling] cannot close rejected input file %q: %w", path, closeErr))
			}
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("[downsampling] cannot stat input file %q: %w", path, err)
	}
	if info == nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("[downsampling] input file %q is not a regular file: %w", path, os.ErrInvalid)
	}
	if info.Size() < 0 {
		return nil, fmt.Errorf("[downsampling] input file %q has negative size %d: %w", path, info.Size(), os.ErrInvalid)
	}
	r := &ReaderAt{f: f, path: path, size: uint64(info.Size())}
	readersCount.Inc()
	return r, nil
}
