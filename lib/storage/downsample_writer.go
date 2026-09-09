package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
)

// 最终文件沿用 filestream 的写入接口，但可恢复的降采样失败不能调用 MustClose。
type downsampleFileWriter interface {
	filestream.WriteCloser
	Close() error
	Abort() error
}

// downsampleWriter 在全部列与索引同步完成后才返回可发布的 partHeader。
type downsampleWriter struct {
	path          string
	files         [4]downsampleFileWriter
	offsets       [3]uint64
	compressLevel int
	ph            partHeader
	previous      blockHeader
	resolution    int64
	spills        [countOfDownsampleFeatures]*filestream.SpillWriter
	spillData     []byte
	// indexLimit 可在测试中缩小，生产默认 maxBlockSize。
	indexLimit    int
	hasPrevious   bool
	mr            downsampleMetaindexRow
	indexData     []byte
	metaindexData []byte
	compressed    []byte
	blocks        [countOfDownsampleFeatures]Block
	integers      []int64
	normalized    []float64
	finished      bool
	err           error // 部分写入失败后禁止重试或发布；只能 Abort/reset。
}

func (w *downsampleWriter) Init(path string, compressLevel int) error {
	w.reset()
	if w.path != "" {
		return fmt.Errorf("无法清理先前降采样目标 %q: %w", w.path, w.err)
	}
	if err := os.Mkdir(path, 0755); err != nil {
		return err
	}
	w.path = path
	w.compressLevel = compressLevel
	w.ph.Reset()
	for i, name := range []string{timestampsFilename, valuesFilename, indexFilename, metaindexFilename} {
		f, err := filestream.CreateExclusive(filepath.Join(path, name), false)
		if err != nil {
			return w.fail(err)
		}
		w.files[i] = f
	}
	return nil
}

func (w *downsampleWriter) WriteBlock(b *downsampleBatch) (err error) {
	if w.err != nil {
		return w.err
	}
	if w.path == "" || w.finished {
		return fmt.Errorf("降采样 writer 尚未初始化或已关闭")
	}
	// 验证失败也取消整个未发布目标，不能把先前成功写入的批次单独发布。
	defer func() {
		if err != nil {
			err = w.fail(err)
		}
	}()
	if b == nil {
		return fmt.Errorf("降采样批次为空")
	}
	n := len(b.timestamps)
	if !validDownsampleResolution(b.resolution) || n == 0 || n > maxRowsPerBlock || b.precisionBits < 1 || b.precisionBits > 64 {
		return fmt.Errorf("无效降采样 block 分辨率、行数或精度")
	}
	for i := range b.values {
		if len(b.values[i]) != n {
			return fmt.Errorf("降采样列 %d 的行数错误", i)
		}
	}
	for i, t := range b.timestamps {
		if t < minUnixMilli || t > maxUnixMilli || (i > 0 && t <= b.timestamps[i-1]) || (i > 0 && t/b.resolution == b.timestamps[i-1]/b.resolution) {
			return fmt.Errorf("编码前的降采样时间戳或 bucket 顺序错误")
		}
	}
	h := blockHeader{TSID: b.tsid, RowsCount: uint32(n), MinTimestamp: b.timestamps[0], MaxTimestamp: b.timestamps[n-1]}
	if w.hasPrevious && (b.resolution < w.resolution || (b.resolution == w.resolution && (downsampleHeaderLess(&h, &w.previous) || (h.TSID == w.previous.TSID && h.MinTimestamp/b.resolution <= w.previous.MaxTimestamp/b.resolution)))) {
		return fmt.Errorf("降采样 block 排序错误或编码前 bucket 重复")
	}
	if ^uint64(0)-w.ph.RowsCount < uint64(n)*countOfDownsampleFeatures || ^uint64(0)-w.ph.BlocksCount < countOfDownsampleFeatures {
		return fmt.Errorf("降采样 part 行数溢出")
	}
	if err := checkDownsampleWriteSpace(filepath.Dir(w.path), n); err != nil {
		return err
	}
	if w.hasPrevious && b.resolution != w.resolution {
		if err := w.flushResolution(); err != nil {
			return err
		}
	}
	w.resolution = b.resolution
	sharedTimestampOffset := w.offsets[0]
	for i := range b.values {
		w.normalized = w.normalized[:0]
		for _, v := range b.values[i] {
			if math.IsNaN(v) {
				v = decimal.StaleNaN
			}
			w.normalized = append(w.normalized, v)
		}
		var scale int16
		w.integers, scale = decimal.AppendFloatToDecimal(w.integers[:0], w.normalized)
		// 每个特征拥有独立的原生 Block，时间戳和 values 均沿用 raw 的精度。
		fb := &w.blocks[i]
		fb.Init(&b.tsid, b.timestamps, w.integers, scale, b.precisionBits)
		_, timestampsData, _ := fb.MarshalData(sharedTimestampOffset, 0)
		if i > 0 && (!bytes.Equal(timestampsData, w.blocks[0].timestampsData) || !sameDownsampleTimestamps(&fb.bh, &w.blocks[0].bh)) {
			return fmt.Errorf("同一批次的 Block 时间戳编码不一致")
		}
		if err := validateDownsampleHeader(&fb.bh); err != nil {
			return err
		}
	}
	if err := w.writePayload(0, w.blocks[0].timestampsData); err != nil {
		return err
	}
	for i := range w.blocks {
		if w.spills[i] == nil {
			w.spills[i] = filestream.NewSpillWriter(w.path)
		}
		fb := &w.blocks[i]
		if err := writeDownsampleData(w.spills[i], fb.headerData); err != nil {
			return err
		}
		if err := writeDownsampleData(w.spills[i], fb.valuesData); err != nil {
			return err
		}
	}
	w.ph.RowsCount += uint64(n) * countOfDownsampleFeatures
	w.ph.BlocksCount += countOfDownsampleFeatures
	w.ph.MinTimestamp = min(w.ph.MinTimestamp, h.MinTimestamp)
	w.ph.MaxTimestamp = max(w.ph.MaxTimestamp, h.MaxTimestamp)
	w.previous = h
	w.hasPrevious = true
	return nil
}

// flushResolution 是有界外部转置：每个 spill 已按 TSID/MinTimestamp 排序，
// 依次消费五个文件即可保证整个 part 的 resolution/feature 全局顺序。
func (w *downsampleWriter) flushResolution() error {
	limit := w.indexLimit
	if limit == 0 {
		limit = maxBlockSize
	}
	if limit < marshaledBlockHeaderSize || limit > maxBlockSize {
		return fmt.Errorf("无效降采样 index 大小上限")
	}
	// spill 尚未删除时，最终 values/index/metaindex 仍需额外空间。
	var pending uint64
	var spillCount int
	for _, f := range w.spills {
		if f == nil {
			continue
		}
		spillCount++
		pending = addDownsampleSpace(pending, f.Size())
	}
	if spillCount == 0 {
		return nil
	}
	if spillCount != countOfDownsampleFeatures {
		return fmt.Errorf("降采样 spill 特征列缺失")
	}
	if err := checkDownsamplePathSpace(filepath.Dir(w.path), pending); err != nil {
		return err
	}
	header := make([]byte, marshaledBlockHeaderSize)
	var expectedBlocks, expectedRows uint64
	for feature, f := range w.spills {
		err := f.ReadAll(func(r io.Reader) error {
			var previous blockHeader
			var blocks, rows uint64
			for {
				n, err := io.ReadFull(r, header)
				if err == io.EOF && n == 0 {
					break
				}
				if err != nil {
					return err
				}
				var h blockHeader
				if _, err := h.Unmarshal(header); err != nil {
					return err
				}
				if err := validateDownsampleHeader(&h); err != nil {
					return err
				}
				if err := checkDownsampleExtent(h.TimestampsBlockOffset, h.TimestampsBlockSize, w.offsets[0]); err != nil {
					return err
				}
				if blocks > 0 && (!downsampleHeadersOrdered(&previous, &h) || h.TimestampsBlockOffset != previous.TimestampsBlockOffset+uint64(previous.TimestampsBlockSize)) {
					return fmt.Errorf("降采样 spill 排序或时间戳连续性错误")
				}
				previous = h
				blocks++
				rows += uint64(h.RowsCount)
				if len(w.indexData)+marshaledBlockHeaderSize > limit || (w.mr.BlockHeadersCount > 0 && !sameDownsampleTenant(&h.TSID, &w.mr.TSID)) {
					if err := w.flushIndex(); err != nil {
						return err
					}
				}
				if err := checkDownsampleWriteSpace(filepath.Dir(w.path), int(h.RowsCount)); err != nil {
					return err
				}
				if cap(w.spillData) < int(h.ValuesBlockSize) {
					w.spillData = make([]byte, h.ValuesBlockSize)
				}
				w.spillData = w.spillData[:h.ValuesBlockSize]
				if _, err := io.ReadFull(r, w.spillData); err != nil {
					return err
				}
				h.ValuesBlockOffset = w.offsets[1]
				if err := w.writePayload(1, w.spillData); err != nil {
					return err
				}
				w.indexData = h.Marshal(w.indexData)
				w.mr.ResolutionMs, w.mr.feature = w.resolution, uint8(feature)
				w.mr.RegisterBlockHeader(&h)
				w.mr.LastTSID = h.TSID
				w.mr.RowsCount += uint64(h.RowsCount)
			}
			if feature == 0 {
				expectedBlocks, expectedRows = blocks, rows
			} else if blocks != expectedBlocks || rows != expectedRows {
				return fmt.Errorf("降采样 spill 特征列统计不一致")
			}
			if blocks == 0 {
				return fmt.Errorf("降采样 spill 为空")
			}
			return w.flushIndex()
		})
		if err != nil {
			return err
		}
		w.spills[feature] = nil
	}
	return nil
}

func (w *downsampleWriter) writePayload(file int, b []byte) error {
	if len(b) > downsampleMaxColumnSize {
		return fmt.Errorf("降采样编码列超过大小上限")
	}
	if w.offsets[file] > uint64(math.MaxInt64) || uint64(len(b)) > uint64(math.MaxInt64)-w.offsets[file] {
		return fmt.Errorf("降采样文件偏移溢出")
	}
	if err := writeDownsampleData(w.files[file], b); err != nil {
		return err
	}
	w.offsets[file] += uint64(len(b))
	return nil
}

func writeDownsampleData(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}

func (w *downsampleWriter) flushIndex() error {
	if len(w.indexData) == 0 {
		return nil
	}
	if len(w.metaindexData)+downsampleMetaindexRowSize > downsampleMaxMetaindexSize {
		return fmt.Errorf("降采样 metaindex 超过内存上限")
	}
	if len(w.indexData) > maxBlockSize || len(w.indexData) != int(w.mr.BlockHeadersCount)*marshaledBlockHeaderSize {
		return fmt.Errorf("降采样 index 长度或条目数错误")
	}
	if err := checkDownsampleFinishSpace(filepath.Dir(w.path), len(w.indexData), len(w.metaindexData)); err != nil {
		return err
	}
	w.compressed = append(w.compressed[:0], downsampleIndexMagic...)
	w.compressed = encoding.CompressZSTDLevel(w.compressed, w.indexData, w.compressLevel)
	if len(w.compressed) > downsampleMaxIndexSize || len(w.compressed) > 2*len(w.indexData)+256+len(downsampleIndexMagic) {
		return fmt.Errorf("降采样 index block 超过大小上限")
	}
	w.mr.IndexBlockOffset = w.offsets[2]
	w.mr.IndexBlockSize = uint32(len(w.compressed))
	if err := w.writePayload(2, w.compressed); err != nil {
		return err
	}
	w.metaindexData = w.mr.marshal(w.metaindexData)
	w.indexData = w.indexData[:0]
	w.mr = downsampleMetaindexRow{}
	return nil
}

func (w *downsampleWriter) Finish() (_ partHeader, err error) {
	if w.err != nil {
		return partHeader{}, w.err
	}
	if w.path == "" || w.finished {
		return partHeader{}, fmt.Errorf("降采样 writer 尚未初始化或已完成")
	}
	defer func() {
		if err != nil {
			err = w.fail(err)
		}
	}()
	if w.ph.RowsCount > 0 {
		m := newDownsamplePartMetadata(w.ph)
		if err := m.validate(); err != nil {
			return partHeader{}, err
		}
	}
	if err := w.flushResolution(); err != nil {
		return partHeader{}, err
	}
	if err := checkDownsampleFinishSpace(filepath.Dir(w.path), len(w.indexData), len(w.metaindexData)); err != nil {
		return partHeader{}, err
	}
	if err := w.flushIndex(); err != nil {
		return partHeader{}, err
	}
	if w.ph.RowsCount > 0 {
		w.compressed = append(w.compressed[:0], downsampleMetaindexMagic...)
		w.compressed = encoding.CompressZSTDLevel(w.compressed, w.metaindexData, w.compressLevel)
		if len(w.compressed) > downsampleMaxMetaindexSize || len(w.compressed) > 2*len(w.metaindexData)+256+len(downsampleMetaindexMagic) {
			return partHeader{}, fmt.Errorf("压缩 metaindex 超过大小上限")
		}
		if err := writeDownsampleData(w.files[3], w.compressed); err != nil {
			return partHeader{}, err
		}
		m := newDownsamplePartMetadata(w.ph)
		b, err := json.Marshal(&m)
		if err != nil {
			return partHeader{}, err
		}
		f, err := filestream.CreateExclusive(filepath.Join(w.path, metadataFilename), false)
		if err != nil {
			return partHeader{}, err
		}
		if err := writeDownsampleData(f, b); err != nil {
			return partHeader{}, errors.Join(err, f.Abort())
		}
		if err := f.Close(); err != nil {
			return partHeader{}, err
		}
	}
	var errs []error
	for i, f := range w.files {
		if f != nil {
			errs = append(errs, f.Close())
			w.files[i] = nil
		}
	}
	if err := errors.Join(errs...); err != nil {
		return partHeader{}, err
	}
	if err := syncDownsampleDir(w.path); err != nil {
		return partHeader{}, err
	}
	if err := syncDownsampleDir(filepath.Dir(w.path)); err != nil {
		return partHeader{}, err
	}
	w.finished = true
	return w.ph, nil
}

func syncDownsampleDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (w *downsampleWriter) fail(err error) error {
	w.err = errors.Join(err, w.Abort())
	return w.err
}

// Abort 删除由本 writer 创建的未发布目标，包括 Finish 已完成但尚未发布的目标。
func (w *downsampleWriter) Abort() error {
	var errs []error
	for _, f := range w.spills {
		if f != nil {
			errs = append(errs, f.Close())
		}
	}
	for i, f := range w.files {
		if f != nil {
			errs = append(errs, f.Abort())
			w.files[i] = nil
		}
	}
	if w.path != "" {
		if err := os.RemoveAll(w.path); err != nil {
			errs = append(errs, err)
			w.err = errors.Join(errs...)
			w.finished = false
			return w.err // 保留路径，允许调用方再次清理。
		}
	}
	clear(w.spills[:])
	w.path = ""
	return errors.Join(errs...)
}

func (w *downsampleWriter) reset() {
	if !w.finished {
		if err := w.Abort(); err != nil && w.path != "" {
			return
		}
	} else {
		w.path = ""
	}
	w.offsets = [3]uint64{}
	w.compressLevel = 0
	w.ph.Reset()
	w.previous = blockHeader{}
	w.resolution = 0
	w.indexLimit = 0
	w.spillData = nil
	w.hasPrevious = false
	w.mr = downsampleMetaindexRow{}
	w.finished = false
	w.err = nil
	for i := range w.blocks {
		w.blocks[i].Reset()
	}
	for _, b := range []*[]byte{&w.indexData, &w.compressed} {
		if cap(*b) > downsampleMaxIndexSize {
			*b = nil
		} else {
			*b = (*b)[:0]
		}
	}
	if cap(w.metaindexData) > 1<<20 {
		w.metaindexData = nil
	} else {
		w.metaindexData = w.metaindexData[:0]
	}
	if cap(w.integers) > downsampleMaxPooledRows {
		w.integers = nil
	} else {
		w.integers = w.integers[:0]
	}
	if cap(w.normalized) > downsampleMaxPooledRows {
		w.normalized = nil
	} else {
		w.normalized = w.normalized[:0]
	}
}

func getDownsampleWriter() *downsampleWriter {
	if v := downsampleWriterPool.Get(); v != nil {
		return v.(*downsampleWriter)
	}
	return &downsampleWriter{}
}
func putDownsampleWriter(w *downsampleWriter) {
	w.reset()
	if w.path == "" {
		downsampleWriterPool.Put(w)
	}
}

var downsampleWriterPool sync.Pool
