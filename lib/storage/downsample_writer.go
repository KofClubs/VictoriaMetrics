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
	path                   string                                             // 本 writer 拥有的目标目录，发布前可由 Abort 删除。
	timestampsWriter       downsampleFileWriter                               // timestamps.bin
	valuesWriter           downsampleFileWriter                               // values.bin
	indexWriter            downsampleFileWriter                               // index.bin
	metaindexWriter        downsampleFileWriter                               // metaindex.bin
	timestampsOffset       uint64                                             // timestamps.bin 的下一写入位置。
	valuesOffset           uint64                                             // values.bin 的下一写入位置。
	indexOffset            uint64                                             // index.bin 的下一写入位置。
	compressLevel          int                                                // index 与 metaindex 的 ZSTD 压缩级别。
	ph                     partHeader                                         // 已成功编码批次的 part 统计。
	previous               blockHeader                                        // 上一批次编码前的时间范围，用于跨批次排序检查。
	resolution             int64                                              // 当前 spill 中的数据分辨率。
	spills                 [countOfDownsampleFeatures]*filestream.SpillWriter // 当前分辨率的五个特征 spill。
	spillData              []byte                                             // 从 spill 读取一个已编码 value block 的临时缓冲。
	indexLimit             int                                                // 测试可缩小的 index 上限；生产默认 maxBlockSize。
	hasPrevious            bool                                               // 是否已经成功写入至少一个批次。
	mr                     downsampleMetaindexRow                             // 当前 index block 对应的 metaindex 行。
	indexData              []byte                                             // 当前 index block 的未压缩 header 数据。
	metaindexData          []byte                                             // 当前 part 的全部未压缩 metaindex 行。
	compressed             []byte                                             // index/metaindex 共用的压缩缓冲。
	blocks                 [countOfDownsampleFeatures]Block                   // 当前批次的五个原生单特征 Block。
	currentBlockTimestamps []int64                                            // 当前输出 block 的有效时间戳，不含空槽。
	integers               []int64                                            // 当前特征转换成 decimal 后的整数缓冲。
	normalized             []float64                                          // 当前特征的浮点缓冲，NaN 已规范化。
	finished               bool                                               // 最终文件已关闭并同步，等待调用方发布。
	err                    error                                              // 首次失败后禁止重试或发布，只能 Abort/reset。
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
	f, err := filestream.CreateExclusive(filepath.Join(path, timestampsFilename), false)
	if err != nil {
		return w.fail(err)
	}
	w.timestampsWriter = f
	f, err = filestream.CreateExclusive(filepath.Join(path, valuesFilename), false)
	if err != nil {
		return w.fail(err)
	}
	w.valuesWriter = f
	f, err = filestream.CreateExclusive(filepath.Join(path, indexFilename), false)
	if err != nil {
		return w.fail(err)
	}
	w.indexWriter = f
	f, err = filestream.CreateExclusive(filepath.Join(path, metaindexFilename), false)
	if err != nil {
		return w.fail(err)
	}
	w.metaindexWriter = f
	return nil
}

// WriteSamples 借用当前 TSID 的 bucket 槽，按有效行数和精度拆成原生 Block。
// 空槽不输出；输入只读，写入结束后不保留其引用。
func (w *downsampleWriter) WriteSamples(tsid *TSID, resolution int64, bucketSamples []downsampleSample, stopCh <-chan struct{}) (err error) {
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
	if tsid == nil || !validDownsampleResolution(resolution) {
		return fmt.Errorf("无效降采样 TSID 或分辨率")
	}
	var start, rows int
	var precisionBits uint8
	for i := range bucketSamples {
		if err := checkDownsampleWriteStopped(stopCh); err != nil {
			return err
		}
		s := &bucketSamples[i]
		if s.isEmpty() {
			continue
		}
		if s.precisionBits > 64 || s.timestamp < minUnixMilli || s.timestamp > maxUnixMilli {
			return fmt.Errorf("无效降采样样本精度或时间戳")
		}
		if rows > 0 && precisionBits != s.precisionBits {
			if err := w.writeSamplesBlock(tsid, resolution, bucketSamples[start:i], precisionBits, stopCh); err != nil {
				return err
			}
			rows = 0
		}
		if rows == 0 {
			start = i
			precisionBits = s.precisionBits
		}
		rows++
		if rows == maxRowsPerBlock {
			if err := w.writeSamplesBlock(tsid, resolution, bucketSamples[start:i+1], precisionBits, stopCh); err != nil {
				return err
			}
			rows = 0
		}
	}
	if rows > 0 {
		if err := w.writeSamplesBlock(tsid, resolution, bucketSamples[start:], precisionBits, stopCh); err != nil {
			return err
		}
	}
	return checkDownsampleWriteStopped(stopCh)
}

func checkDownsampleWriteStopped(stopCh <-chan struct{}) error {
	select {
	case <-stopCh:
		return errForciblyStopped
	default:
		return nil
	}
}

// writeSamplesBlock 只提取一份时间戳和一个特征列，不构造完整的五列解码 block。
func (w *downsampleWriter) writeSamplesBlock(tsid *TSID, resolution int64, samples []downsampleSample, precisionBits uint8, stopCh <-chan struct{}) error {
	w.currentBlockTimestamps = w.currentBlockTimestamps[:0]
	for i := range samples {
		if err := checkDownsampleWriteStopped(stopCh); err != nil {
			return err
		}
		s := &samples[i]
		if s.isEmpty() {
			continue
		}
		t := s.timestamp
		n := len(w.currentBlockTimestamps)
		if n > 0 && (t <= w.currentBlockTimestamps[n-1] || t/resolution == w.currentBlockTimestamps[n-1]/resolution) {
			return fmt.Errorf("编码前的降采样时间戳或 bucket 顺序错误")
		}
		w.currentBlockTimestamps = append(w.currentBlockTimestamps, t)
	}
	n := len(w.currentBlockTimestamps)
	if n == 0 || n > maxRowsPerBlock {
		return fmt.Errorf("无效降采样 block 行数")
	}
	h := blockHeader{TSID: *tsid, RowsCount: uint32(n), MinTimestamp: w.currentBlockTimestamps[0], MaxTimestamp: w.currentBlockTimestamps[n-1]}
	if w.hasPrevious && (resolution < w.resolution || (resolution == w.resolution && (downsampleHeaderLess(&h, &w.previous) || (h.TSID == w.previous.TSID && h.MinTimestamp/resolution <= w.previous.MaxTimestamp/resolution)))) {
		return fmt.Errorf("降采样 block 排序错误或编码前 bucket 重复")
	}
	if ^uint64(0)-w.ph.RowsCount < uint64(n)*countOfDownsampleFeatures || ^uint64(0)-w.ph.BlocksCount < countOfDownsampleFeatures {
		return fmt.Errorf("降采样 part 行数溢出")
	}
	if err := checkDownsampleWriteSpace(filepath.Dir(w.path), n); err != nil {
		return err
	}
	if w.hasPrevious && resolution != w.resolution {
		if err := w.flushResolution(); err != nil {
			return err
		}
	}
	w.resolution = resolution
	sharedTimestampOffset := w.timestampsOffset
	for feature := range w.blocks {
		w.normalized = w.normalized[:0]
		for i := range samples {
			if err := checkDownsampleWriteStopped(stopCh); err != nil {
				return err
			}
			s := &samples[i]
			if s.isEmpty() {
				continue
			}
			v := s.values[feature]
			if math.IsNaN(v) {
				v = decimal.StaleNaN
			}
			w.normalized = append(w.normalized, v)
		}
		var scale int16
		w.integers, scale = decimal.AppendFloatToDecimal(w.integers[:0], w.normalized)
		// 每个特征拥有独立的原生 Block，时间戳和 values 均沿用 raw 的精度。
		fb := &w.blocks[feature]
		fb.Init(tsid, w.currentBlockTimestamps, w.integers, scale, precisionBits)
		_, timestampsData, _ := fb.MarshalData(sharedTimestampOffset, 0)
		if feature > 0 && (!bytes.Equal(timestampsData, w.blocks[0].timestampsData) || !sameDownsampleTimestamps(&fb.bh, &w.blocks[0].bh)) {
			return fmt.Errorf("同一批次的 Block 时间戳编码不一致")
		}
		if err := validateDownsampleHeader(&fb.bh); err != nil {
			return err
		}
	}
	if err := checkDownsampleWriteStopped(stopCh); err != nil {
		return err
	}
	if err := writeDownsamplePayload(w.timestampsWriter, &w.timestampsOffset, w.blocks[0].timestampsData); err != nil {
		return err
	}
	for i := range w.blocks {
		if err := checkDownsampleWriteStopped(stopCh); err != nil {
			return err
		}
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
				if err := checkDownsampleExtent(h.TimestampsBlockOffset, h.TimestampsBlockSize, w.timestampsOffset); err != nil {
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
				h.ValuesBlockOffset = w.valuesOffset
				if err := writeDownsamplePayload(w.valuesWriter, &w.valuesOffset, w.spillData); err != nil {
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

func writeDownsamplePayload(w io.Writer, offset *uint64, b []byte) error {
	if len(b) > downsampleMaxColumnSize {
		return fmt.Errorf("降采样编码列超过大小上限")
	}
	if *offset > uint64(math.MaxInt64) || uint64(len(b)) > uint64(math.MaxInt64)-*offset {
		return fmt.Errorf("降采样文件偏移溢出")
	}
	if err := writeDownsampleData(w, b); err != nil {
		return err
	}
	*offset += uint64(len(b))
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
	w.mr.IndexBlockOffset = w.indexOffset
	w.mr.IndexBlockSize = uint32(len(w.compressed))
	if err := writeDownsamplePayload(w.indexWriter, &w.indexOffset, w.compressed); err != nil {
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
		if err := writeDownsampleData(w.metaindexWriter, w.compressed); err != nil {
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
	if w.timestampsWriter != nil {
		errs = append(errs, w.timestampsWriter.Close())
		w.timestampsWriter = nil
	}
	if w.valuesWriter != nil {
		errs = append(errs, w.valuesWriter.Close())
		w.valuesWriter = nil
	}
	if w.indexWriter != nil {
		errs = append(errs, w.indexWriter.Close())
		w.indexWriter = nil
	}
	if w.metaindexWriter != nil {
		errs = append(errs, w.metaindexWriter.Close())
		w.metaindexWriter = nil
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
	if w.timestampsWriter != nil {
		errs = append(errs, w.timestampsWriter.Abort())
		w.timestampsWriter = nil
	}
	if w.valuesWriter != nil {
		errs = append(errs, w.valuesWriter.Abort())
		w.valuesWriter = nil
	}
	if w.indexWriter != nil {
		errs = append(errs, w.indexWriter.Abort())
		w.indexWriter = nil
	}
	if w.metaindexWriter != nil {
		errs = append(errs, w.metaindexWriter.Abort())
		w.metaindexWriter = nil
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
	w.timestampsOffset = 0
	w.valuesOffset = 0
	w.indexOffset = 0
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
	if cap(w.currentBlockTimestamps) > downsampleMaxPooledRows {
		w.currentBlockTimestamps = nil
	} else {
		w.currentBlockTimestamps = w.currentBlockTimestamps[:0]
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
