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

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
)

// spillMaxMemorySize bounds each spill's in-memory tail. All spills additionally
// share a process-wide budget, including buffers temporarily retained during growth.
var spillMaxMemorySize = flagutil.NewBytes("downsampling.spillMaxMemorySize", 16*1024*1024,
	"The maximum number of bytes to buffer in memory per downsampling spill before overflowing to a temporary file. "+
		"Must be positive. All spills share a memory budget of the smaller of 256 MiB and 10% of memory.allowedBytes or the memory.allowedPercent allowance; "+
		"when this budget is exhausted, writes spill to disk without waiting for memory")

// Bound the combined buffers across all resolutions and concurrent merges.
// This is an allocation budget, not a limit on the process RSS or filesystem cache.
const spillMaxTotalMemorySize = 256 << 20

var spillMemoryBudgetOnce sync.Once
var spillMemoryBudget memory.Limiter

func getSpillMemoryBudget() *memory.Limiter {
	spillMemoryBudgetOnce.Do(func() {
		spillMemoryBudget.MaxSize = uint64(min(spillMaxTotalMemorySize, memory.Allowed()/10))
	})
	return &spillMemoryBudget
}

// SpillWriter keeps a byte stream in memory while both memory budgets permit it.
// Each full buffer is then appended to one lazily created temporary file; the
// final tail remains in memory. It owns and removes that file.
//
// Write appends data, transparently spilling full buffers to disk. Read drains
// the whole stream (file prefix followed by the memory tail) as a read-only
// view; it neither seals the writer nor closes or removes the file. Close
// releases memory, closes the file, and removes the temporary file, and must be
// called explicitly after Read. A failed Write seals the writer and attempts
// cleanup immediately. SpillWriter is not safe for concurrent use.
type SpillWriter struct {
	path          string
	f             spillFile
	memory        []byte          // Pending tail; capacity never exceeds the memory threshold.
	memoryBudget  *memory.Limiter // Accounts for owned capacity; no reservation is held across a blocking wait.
	fileSize      uint64          // Bytes appended to the temporary file, preceding memory.
	size          uint64          // All accepted bytes, including memory; preserved after Close.
	state         spillState
	closed        bool // Sealed by Close (or a failed Write); further operations fail.
	created       bool // The temporary file exists and still needs removal.
	writerCounted bool // The open file currently accounts for an active writer.
	err           error

	// A positive value lets instance-level tests exercise spilling with small data.
	// Production captures spillMaxMemorySize at the first nonempty Write.
	memoryLimit int

	// Per-instance removal allows testing cleanup failures without global hooks.
	remove func(string) error
}

// spillFile 是 SpillWriter 持有文件的抽象，组合读、写、定位、关闭四个标准接口：
//   - io.Writer：dump 溢出数据时写入磁盘；
//   - io.Reader：Read 时读回文件前缀；
//   - io.Seeker：Read 时校验文件大小并回卷；
//   - io.Closer：Close 时关闭文件描述符。
//
// 用接口而非 *os.File，是为了让生产环境的真实文件与测试注入的
// spillFaultFile（可模拟写/读/seek/close 失败）互相替换，从而覆盖磁盘 I/O 故障路径。
type spillFile interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

type spillState uint8

const (
	spillMemory spillState = iota // All accepted bytes are still in memory; no file yet.
	spillDisk                     // Part of the stream has been written to the temporary file.
)

// NewSpillWriter returns a writer which creates its temporary file directly
// under dir when its per-spill threshold or shared allocation budget is reached. dir must already
// exist. name identifies this spill within dir, so each spill in a directory
// must use a distinct name; it also documents the spill's purpose in the filename.
// The caller must eventually call Close, including on failure.
func NewSpillWriter(dir, name string) *SpillWriter {
	return &SpillWriter{
		path:   filepath.Join(dir, ".spill-"+name),
		remove: os.Remove,
	}
}

// Write appends p to the spill. Size includes bytes accepted into its buffer.
// A failed Write seals the writer and immediately attempts to delete its files.
func (w *SpillWriter) Write(p []byte) (n int, err error) {
	if w.closed {
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
		limit = spillMaxMemorySize.IntN()
	}
	if limit <= 0 {
		return 0, w.finish(fmt.Errorf("downsampling.spillMaxMemorySize must be positive; got %d", limit))
	}
	w.memoryLimit = limit
	if w.memoryBudget == nil {
		w.memoryBudget = getSpillMemoryBudget()
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
			capacity := limit
			if cap(w.memory) <= limit/2 {
				capacity = min(limit, max(needed, 2*cap(w.memory)))
			}
			// Reserve the full new allocation while the old buffer is still live.
			// Budget pressure must never wait while another merge holds buffers.
			if !w.memoryBudget.Get(uint64(capacity)) {
				if len(w.memory) > 0 {
					if err := w.dump(); err != nil {
						return n, w.finish(err)
					}
				}
				w.memoryBudget.Put(uint64(cap(w.memory)))
				w.memory = nil
				written, err := w.appendFile(p)
				w.size += uint64(written)
				n += written
				if err != nil {
					return n, w.finish(err)
				}
				return n, nil
			}
			buf := make([]byte, len(w.memory), capacity)
			copy(buf, w.memory)
			w.memoryBudget.Put(uint64(cap(w.memory)))
			w.memory = buf
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
	if _, err := w.appendFile(w.memory); err != nil {
		return err
	}
	w.memory = w.memory[:0]
	return nil
}

func (w *SpillWriter) appendFile(p []byte) (int, error) {
	if w.f == nil {
		if err := w.create(); err != nil {
			return 0, err
		}
	}
	// A previous Read may have returned early, leaving the descriptor before EOF.
	offset, err := w.f.Seek(int64(w.fileSize), io.SeekStart)
	if err != nil || offset != int64(w.fileSize) {
		return 0, errors.Join(fmt.Errorf("cannot position spill append at offset %d; got %d", w.fileSize, offset), err)
	}
	n, err := (spillFileWriter{w.f}).Write(p)
	w.fileSize += uint64(n)
	if err != nil {
		return n, fmt.Errorf("cannot write spill file: %w", err)
	}
	w.state = spillDisk
	return n, nil
}

func (w *SpillWriter) create() error {
	// The filename is derived from the caller-supplied name and is fixed at
	// construction time.
	// Unlike os.CreateTemp's 0600, these permissions match os.Create used by
	// filestream.MustCreate.
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("cannot create spill file %q: %w", w.path, err)
	}
	w.f = f
	w.created = true
	w.writerCounted = true
	writersCount.Inc()
	return nil
}

// Size returns the total number of bytes accepted by Write, including buffered
// bytes. It remains available after reading or closing the spill.
func (w *SpillWriter) Size() uint64 {
	return w.size
}

// Read streams the file prefix followed by the in-memory tail, without dumping
// the tail. The reader must consume exactly Size bytes and is only valid during
// consume. Reading to EOF is allowed; consuming fewer bytes is an error.
//
// Read is a read-only operation: it never mutates memory or files, never closes
// or removes the spill, and never seals the writer. The caller must call Close
// afterward as the final step; Close releases memory, closes the file, and
// removes the temporary file. Temporary files are never synced to storage.
func (w *SpillWriter) Read(consume func(io.Reader) error) (err error) {
	if w.closed {
		return w.stateError()
	}
	if consume == nil {
		return fmt.Errorf("nil spill consumer")
	}

	var source *spillFileReader
	cr := &countingReader{r: bytes.NewReader(w.memory)}
	if w.state == spillDisk {
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
		source = &spillFileReader{r: w.f, remaining: w.fileSize}
		br := getSpillBufioReader(source)
		readersCount.Inc()
		cr.r = io.MultiReader(br, cr.r)
		defer func() {
			putSpillBufioReader(br)
			readersCount.Dec()
		}()
	}
	defer func() {
		cr.r = nil
	}()
	err = consume(cr)
	if source != nil && source.err != nil && !errors.Is(err, source.err) {
		err = errors.Join(err, fmt.Errorf("cannot read spill file: %w", source.err))
	}
	if cr.n < w.size {
		err = errors.Join(err, fmt.Errorf("spill consumer read %d bytes; want %d: %w", cr.n, w.size, io.ErrUnexpectedEOF))
	} else if cr.n > w.size {
		err = errors.Join(err, fmt.Errorf("spill consumer read %d bytes, exceeding the written size %d", cr.n, w.size))
	}
	return err
}

// Close releases memory, closes the file, and removes its temporary file.
// It is safe to call repeatedly. It must be called explicitly after Read as the
// final step. It returns earlier I/O errors as well as cleanup errors. Failed
// removal retains the created file for the next Close.
func (w *SpillWriter) Close() error {
	w.closed = true
	if cap(w.memory) > 0 {
		w.memoryBudget.Put(uint64(cap(w.memory)))
	}
	w.memory = nil
	if w.f != nil {
		if w.writerCounted {
			writersCount.Dec()
			w.writerCounted = false
		}
		if err := w.f.Close(); err != nil {
			w.err = errors.Join(w.err, fmt.Errorf("cannot close spill file: %w", err))
		}
		// os.File.Close closes the descriptor even when it reports an error.
		// Retrying Close could close an unrelated, reused descriptor.
		w.f = nil
	}
	var cleanupErr error
	if w.created {
		if err := w.removePath(w.path); err != nil {
			cleanupErr = fmt.Errorf("cannot remove spill file %q: %w", w.path, err)
		} else {
			w.created = false
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

// spillFileReader 读取 spill 文件前缀，与 spillFileWriter 对称。
// 它被限制在已记录的 fileSize 字节内，即使 bufio.Reader 或 consumer 在
// 最后一个成功字节携带的错误浮出之前就返回，也会保留底层读取错误，
// 并防护无进展的死循环读取。
type spillFileReader struct {
	r          io.Reader
	err        error
	emptyReads int
	remaining  uint64 // 只允许读取已记录的文件前缀，防止截断的前缀被内存尾巴掩盖。
}

func (r *spillFileReader) Read(p []byte) (n int, err error) {
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

// countingReader 统计 consumer 实际读取的字节数，用于校验恰好消费 Size 字节。
// Read 返回后其输入被置空，逃逸的 reader 再次读取会得到 ErrClosed。
type countingReader struct {
	r io.Reader
	n uint64
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.r == nil {
		return 0, fmt.Errorf("spill reader is closed: %w", os.ErrClosed)
	}
	readCallsBuffered.Inc()
	n, err := r.r.Read(p)
	readBytesBuffered.Add(n)
	r.n += uint64(n)
	return n, err
}

// Spill buffers have their own pools so their adapters and file references are
// released without changing the buffer ownership rules of regular filestreams.
func getSpillBufioReader(source *spillFileReader) *bufio.Reader {
	v := spillBufioPool.Get()
	if v == nil {
		return bufio.NewReaderSize(source, getReadBufferSize())
	}
	br := v.(*bufio.Reader)
	br.Reset(source)
	return br
}

func putSpillBufioReader(br *bufio.Reader) {
	br.Reset(nil)
	spillBufioPool.Put(br)
}

var spillBufioPool sync.Pool
