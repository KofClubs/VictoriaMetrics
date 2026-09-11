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
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// downsampleMaxColumnSize 限制每个已编码时间列或值列 block 的负载大小。
const downsampleMaxColumnSize = 2 * maxBlockSize

// downsampleWriter 先同步全部列与索引，最后写入 metadata.json，以完整内容标记 part 写入完成。
type downsampleWriter struct {
	// 本 writer 拥有的目标目录，发布前可由 Abort 删除。
	partPath string
	// timestampsWriter 写入 timestamps.bin；Finish 或 Abort 关闭并清除句柄。
	timestampsWriter filestream.WriteCloser
	// valuesWriter 写入 values.bin；Finish 或 Abort 关闭并清除句柄。
	valuesWriter filestream.WriteCloser
	// indexWriter 写入 index.bin；Finish 或 Abort 关闭并清除句柄。
	indexWriter filestream.WriteCloser
	// metaindexWriter 写入 metaindex.bin；Finish 或 Abort 关闭并清除句柄。
	metaindexWriter filestream.WriteCloser
	// timestamps.bin 中下一个时间戳 block 的写入位置。
	timestampsBlockOffset uint64
	// values.bin 中下一个特征值 block 的写入位置。
	valuesBlockOffset uint64
	// index.bin 中下一个索引 block 的写入位置。
	indexBlockOffset uint64
	// index 与 metaindex 的 ZSTD 压缩级别。
	compressLevel int
	// 已成功写入批次的 part 统计，BlocksCount 同时标识是否已有上一批次。
	partHeader partHeader
	// 上一批次编码前的时间范围，用于跨批次排序检查。
	previousBlockHeader blockHeader
	// 当前五个特征 spill 所属的分辨率，切换前须全部输出。
	currentResolution int64
	// 当前分辨率各特征的 header 与值列，按特征顺序输出到最终文件。
	currentResolutionFeatureSpills [countOfDownsampleFeatures]*filestream.SpillWriter
	// 当前 spill 中已编码值列的读取缓冲，只保留一个 block。
	currentFeatureValuesData []byte
	// 测试可缩小的 index block 上限；生产默认 maxBlockSize。
	maxIndexBlockSize int
	// 当前 index block 对应的 metaindex 行，写出后清空。
	currentIndexMetaindexRow downsampleMetaindexRow
	// 当前 index block 的未压缩 header 数据，按 TSID 和时间排列。
	currentIndexBlockData []byte
	// 当前 part 的全部未压缩 metaindex 行，Finish 时集中写出。
	partMetaindexData []byte
	// index 与 metaindex 共用的压缩缓冲，二者不会同时写出。
	compressedIndexData []byte
	// 当前分辨率、当前特征的原生 Block，写入 spill 后供下一个特征复用。
	currentFeatureBlock Block
	// 当前输出 block 的有效时间戳，不含空槽。
	currentBlockTimestamps []int64
	// 当前输出 block 的共享编码时间戳副本，用于核对后续特征的编码结果。
	currentBlockTimestampsData []byte
	// 当前特征转换成 decimal 后的整数缓冲，交由原生 Block 复制和编码。
	currentFeatureDecimalValues []int64
	// 当前输出特征列的浮点缓冲，直接提取 Merge 已规范化的样本值。
	currentFeatureValues []float64
	// 最终文件已关闭并同步，等待调用方发布。
	isFinished bool
	// 首次失败后禁止重试或发布，只能 Abort/reset。
	writeErr error
	// 仅供实例级故障测试替换目录删除；nil 时使用 os.RemoveAll。
	removeAll func(string) error
}

var errDownsampleNoSpace = errors.New("[downsampling] insufficient free space for downsampling")

func (w *downsampleWriter) Init(path string, compressLevel int) error {
	if !w.isFinished && w.partPath != "" {
		return errors.Join(fmt.Errorf("[downsampling] writer still owns target %q; abort or release it before reinitializing", w.partPath), w.writeErr)
	}
	w.reset()
	if err := os.Mkdir(path, 0755); err != nil {
		return fmt.Errorf("[downsampling] cannot create target directory %q: %w", path, err)
	}
	w.partPath = path
	w.compressLevel = compressLevel
	// 最终文件沿用共享 filestream 的创建、关闭及同步语义。
	w.timestampsWriter = filestream.MustCreate(filepath.Join(path, timestampsFilename), false)
	w.valuesWriter = filestream.MustCreate(filepath.Join(path, valuesFilename), false)
	w.indexWriter = filestream.MustCreate(filepath.Join(path, indexFilename), false)
	w.metaindexWriter = filestream.MustCreate(filepath.Join(path, metaindexFilename), false)
	return nil
}

// WriteSamples 借用当前 TSID 的 bucket 槽，按有效行数和精度拆成原生 Block。
// 空槽不输出；输入只读，写入结束后不保留其引用。
// 有效样本须经 downsampleSample.Merge 生成；数值规范化由该方法统一完成。
func (w *downsampleWriter) WriteSamples(tsid *TSID, resolution int64, bucketSamples []downsampleSample, stopCh <-chan struct{}) (err error) {
	if w.writeErr != nil {
		return w.writeErr
	}
	if w.partPath == "" || w.isFinished {
		return fmt.Errorf("[downsampling] writer is not initialized or is already finished")
	}
	// 验证失败也取消整个未发布目标，不能把先前成功写入的批次单独发布。
	defer func() {
		if err != nil {
			err = w.fail(err)
		}
	}()
	if tsid == nil || !validDownsampleResolution(resolution) {
		return fmt.Errorf("[downsampling] invalid TSID or resolution")
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
			return fmt.Errorf("[downsampling] invalid sample precision or timestamp")
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

func (w *downsampleWriter) Finish() (_ partHeader, err error) {
	if w.writeErr != nil {
		return partHeader{}, w.writeErr
	}
	if w.partPath == "" || w.isFinished {
		return partHeader{}, fmt.Errorf("[downsampling] writer is not initialized or is already finished")
	}
	defer func() {
		if err != nil {
			err = w.fail(err)
		}
	}()
	if w.partHeader.RowsCount > 0 {
		m := newDownsamplePartMetadata(w.partHeader)
		if err := m.validate(); err != nil {
			return partHeader{}, err
		}
	}
	if err := w.flushResolution(); err != nil {
		return partHeader{}, err
	}
	if err := checkDownsampleFinishSpace(filepath.Dir(w.partPath), len(w.currentIndexBlockData), len(w.partMetaindexData)); err != nil {
		return partHeader{}, err
	}
	if w.partHeader.RowsCount > 0 {
		w.compressedIndexData = append(w.compressedIndexData[:0], downsampleMetaindexMagic...)
		w.compressedIndexData = encoding.CompressZSTDLevel(w.compressedIndexData, w.partMetaindexData, w.compressLevel)
		if len(w.compressedIndexData) > downsampleMaxMetaindexSize || len(w.compressedIndexData) > 2*len(w.partMetaindexData)+256+len(downsampleMetaindexMagic) {
			return partHeader{}, fmt.Errorf("[downsampling] compressed metaindex exceeds the size limit")
		}
		if err := writeDownsampleData(w.metaindexWriter, w.compressedIndexData); err != nil {
			return partHeader{}, err
		}
	}
	for _, file := range []*filestream.WriteCloser{&w.timestampsWriter, &w.valuesWriter, &w.indexWriter, &w.metaindexWriter} {
		if *file == nil {
			continue
		}
		f := *file
		*file = nil
		f.MustClose()
	}
	// 先完成所有数据文件的缓冲刷出、fsync 和目录项同步，再创建完成标志。
	fs.MustSyncPath(w.partPath)
	if w.partHeader.RowsCount > 0 {
		m := newDownsamplePartMetadata(w.partHeader)
		b, err := json.Marshal(&m)
		if err != nil {
			return partHeader{}, fmt.Errorf("[downsampling] cannot encode part metadata: %w", err)
		}
		metadataPath := filepath.Join(w.partPath, metadataFilename)
		// 直接写入最终路径；格式检测校验 JSON 完整性，不能仅凭文件存在判断完成。
		var f filestream.WriteCloser = filestream.MustCreate(metadataPath, false)
		err = writeDownsampleData(f, b)
		f.MustClose()
		if err != nil {
			return partHeader{}, err
		}
		fs.MustSyncPath(w.partPath)
	}
	fs.MustSyncPath(filepath.Dir(w.partPath))
	w.isFinished = true
	return w.partHeader, nil
}

// Abort 删除由本 writer 创建的未发布目标，包括 Finish 已完成但尚未发布的目标。
func (w *downsampleWriter) Abort() error {
	var errs []error
	for feature, f := range w.currentResolutionFeatureSpills {
		if f != nil {
			if err := f.Close(); err != nil {
				errs = append(errs, fmt.Errorf("[downsampling] cannot close feature %d spill: %w", feature, err))
			}
			// Close 已释放句柄；所有 spill 都在 w.partPath 下，剩余删除责任由目录路径持有。
			w.currentResolutionFeatureSpills[feature] = nil
		}
	}
	for _, file := range []*filestream.WriteCloser{&w.timestampsWriter, &w.valuesWriter, &w.indexWriter, &w.metaindexWriter} {
		if *file == nil {
			continue
		}
		f := *file
		*file = nil
		f.MustClose()
	}
	w.isFinished = false
	if w.partPath != "" {
		removeAll := w.removeAll
		if removeAll == nil {
			removeAll = os.RemoveAll
		}
		if err := removeAll(w.partPath); err != nil {
			errs = append(errs, fmt.Errorf("[downsampling] cannot remove unpublished target %q: %w", w.partPath, err))
		} else {
			w.partPath = ""
		}
	}
	err := errors.Join(errs...)
	if err != nil {
		w.writeErr = errors.Join(w.writeErr, err)
	}
	return err
}

// reset 只清理逻辑状态和工作缓冲；调用方必须已完成或显式 Abort 所有文件资源。
func (w *downsampleWriter) reset() {
	w.partPath = ""
	w.timestampsBlockOffset = 0
	w.valuesBlockOffset = 0
	w.indexBlockOffset = 0
	w.compressLevel = 0
	w.partHeader.Reset()
	w.previousBlockHeader = blockHeader{}
	w.currentResolution = 0
	w.maxIndexBlockSize = 0
	w.currentFeatureValuesData = nil
	w.currentIndexMetaindexRow = downsampleMetaindexRow{}
	w.isFinished = false
	w.writeErr = nil
	w.removeAll = nil
	w.currentFeatureBlock.Reset()
	for _, b := range []*[]byte{&w.currentIndexBlockData, &w.compressedIndexData, &w.currentBlockTimestampsData} {
		if cap(*b) > downsampleMaxIndexSize {
			*b = nil
		} else {
			*b = (*b)[:0]
		}
	}
	if cap(w.partMetaindexData) > 1<<20 {
		w.partMetaindexData = nil
	} else {
		w.partMetaindexData = w.partMetaindexData[:0]
	}
	if cap(w.currentFeatureDecimalValues) > downsampleMaxPooledRows {
		w.currentFeatureDecimalValues = nil
	} else {
		w.currentFeatureDecimalValues = w.currentFeatureDecimalValues[:0]
	}
	if cap(w.currentFeatureValues) > downsampleMaxPooledRows {
		w.currentFeatureValues = nil
	} else {
		w.currentFeatureValues = w.currentFeatureValues[:0]
	}
	if cap(w.currentBlockTimestamps) > downsampleMaxPooledRows {
		w.currentBlockTimestamps = nil
	} else {
		w.currentBlockTimestamps = w.currentBlockTimestamps[:0]
	}
}

func (w *downsampleWriter) fail(err error) error {
	if !errors.Is(w.writeErr, err) {
		w.writeErr = errors.Join(w.writeErr, fmt.Errorf("[downsampling] cannot complete target %q: %w", w.partPath, err))
	}
	// 先保存工作错误，Abort 只追加清理错误，后续重试不能覆盖首次失败原因。
	_ = w.Abort()
	return w.writeErr
}

func checkDownsampleWriteStopped(stopCh <-chan struct{}) error {
	select {
	case <-stopCh:
		return fmt.Errorf("[downsampling] writing was canceled: %w", errForciblyStopped)
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
			return fmt.Errorf("[downsampling] timestamps or buckets are not strictly ordered before encoding")
		}
		w.currentBlockTimestamps = append(w.currentBlockTimestamps, t)
	}
	n := len(w.currentBlockTimestamps)
	if n == 0 || n > maxRowsPerBlock {
		return fmt.Errorf("[downsampling] invalid output block row count")
	}
	h := blockHeader{TSID: *tsid, RowsCount: uint32(n), MinTimestamp: w.currentBlockTimestamps[0], MaxTimestamp: w.currentBlockTimestamps[n-1]}
	if w.partHeader.BlocksCount > 0 && (resolution < w.currentResolution || (resolution == w.currentResolution && (downsampleHeaderLess(&h, &w.previousBlockHeader) || (h.TSID == w.previousBlockHeader.TSID && h.MinTimestamp/resolution <= w.previousBlockHeader.MaxTimestamp/resolution)))) {
		return fmt.Errorf("[downsampling] blocks are out of order or contain duplicate buckets before encoding")
	}
	if ^uint64(0)-w.partHeader.RowsCount < uint64(n)*countOfDownsampleFeatures || ^uint64(0)-w.partHeader.BlocksCount < countOfDownsampleFeatures {
		return fmt.Errorf("[downsampling] part row or block count overflows")
	}
	if err := checkDownsampleWriteSpace(filepath.Dir(w.partPath), n); err != nil {
		return err
	}
	if w.partHeader.BlocksCount > 0 && resolution != w.currentResolution {
		if err := w.flushResolution(); err != nil {
			return err
		}
	}
	w.currentResolution = resolution
	sharedTimestampOffset := w.timestampsBlockOffset
	var sharedTimestampHeader blockHeader
	for currentFeature := range countOfDownsampleFeatures {
		w.currentFeatureValues = w.currentFeatureValues[:0]
		for i := range samples {
			if err := checkDownsampleWriteStopped(stopCh); err != nil {
				return err
			}
			s := &samples[i]
			if s.isEmpty() {
				continue
			}
			w.currentFeatureValues = append(w.currentFeatureValues, s.values[currentFeature])
		}
		var scale int16
		w.currentFeatureDecimalValues, scale = decimal.AppendFloatToDecimal(w.currentFeatureDecimalValues[:0], w.currentFeatureValues)
		// 每个特征依次使用同一个原生 Block；Init 复制输入，编码仍沿用 raw 的精度。
		fb := &w.currentFeatureBlock
		fb.Init(tsid, w.currentBlockTimestamps, w.currentFeatureDecimalValues, scale, precisionBits)
		_, timestampsData, _ := fb.MarshalData(sharedTimestampOffset, 0)
		if currentFeature > 0 && (!bytes.Equal(timestampsData, w.currentBlockTimestampsData) || !sameDownsampleTimestamps(&fb.bh, &sharedTimestampHeader)) {
			return fmt.Errorf("[downsampling] feature blocks encode different shared timestamps")
		}
		if err := validateDownsampleHeader(&fb.bh); err != nil {
			return err
		}
		if err := checkDownsampleWriteStopped(stopCh); err != nil {
			return err
		}
		if currentFeature == downsampleFeatureLast {
			// 后续 Init/MarshalData 会覆盖 Block 缓冲，必须保存独立副本作为五列共享时间戳的基准。
			w.currentBlockTimestampsData = append(w.currentBlockTimestampsData[:0], timestampsData...)
			sharedTimestampHeader = fb.bh
			if err := writeDownsamplePayload(w.timestampsWriter, &w.timestampsBlockOffset, timestampsData); err != nil {
				return err
			}
		}
		if w.currentResolutionFeatureSpills[currentFeature] == nil {
			w.currentResolutionFeatureSpills[currentFeature] = filestream.NewSpillWriter(w.partPath, downsampleFeatureNames[currentFeature])
		}
		if err := writeDownsampleData(w.currentResolutionFeatureSpills[currentFeature], fb.headerData); err != nil {
			return err
		}
		if err := writeDownsampleData(w.currentResolutionFeatureSpills[currentFeature], fb.valuesData); err != nil {
			return err
		}
	}
	w.partHeader.RowsCount += uint64(n) * countOfDownsampleFeatures
	w.partHeader.BlocksCount += countOfDownsampleFeatures
	w.partHeader.MinTimestamp = min(w.partHeader.MinTimestamp, h.MinTimestamp)
	w.partHeader.MaxTimestamp = max(w.partHeader.MaxTimestamp, h.MaxTimestamp)
	w.previousBlockHeader = h
	return nil
}

// flushResolution 是有界外部转置：每个 spill 已按 TSID/MinTimestamp 排序，
// 依次消费五个文件即可保证整个 part 的 resolution/feature 全局顺序。
func (w *downsampleWriter) flushResolution() error {
	limit := w.maxIndexBlockSize
	if limit == 0 {
		limit = maxBlockSize
	}
	if limit < marshaledBlockHeaderSize || limit > maxBlockSize {
		return fmt.Errorf("[downsampling] invalid index block size limit")
	}
	// spill 尚未删除时，最终 values/index/metaindex 仍需额外空间。
	var pending uint64
	var spillCount int
	for _, f := range w.currentResolutionFeatureSpills {
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
		return fmt.Errorf("[downsampling] a feature spill is missing")
	}
	if err := checkDownsamplePathSpace(filepath.Dir(w.partPath), pending); err != nil {
		return err
	}
	header := make([]byte, marshaledBlockHeaderSize)
	var expectedBlocks, expectedRows uint64
	for feature, f := range w.currentResolutionFeatureSpills {
		err := f.Read(func(r io.Reader) error {
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
				if err := checkDownsampleExtent(h.TimestampsBlockOffset, h.TimestampsBlockSize, w.timestampsBlockOffset); err != nil {
					return err
				}
				if blocks > 0 && (!downsampleHeadersOrdered(&previous, &h) || h.TimestampsBlockOffset != previous.TimestampsBlockOffset+uint64(previous.TimestampsBlockSize)) {
					return fmt.Errorf("[downsampling] spill blocks are out of order or have non-contiguous timestamps")
				}
				previous = h
				blocks++
				rows += uint64(h.RowsCount)
				if len(w.currentIndexBlockData)+marshaledBlockHeaderSize > limit || (w.currentIndexMetaindexRow.BlockHeadersCount > 0 && !sameDownsampleTenant(&h.TSID, &w.currentIndexMetaindexRow.TSID)) {
					if err := w.flushIndex(); err != nil {
						return err
					}
				}
				if err := checkDownsampleWriteSpace(filepath.Dir(w.partPath), int(h.RowsCount)); err != nil {
					return err
				}
				if cap(w.currentFeatureValuesData) < int(h.ValuesBlockSize) {
					w.currentFeatureValuesData = make([]byte, h.ValuesBlockSize)
				}
				w.currentFeatureValuesData = w.currentFeatureValuesData[:h.ValuesBlockSize]
				if _, err := io.ReadFull(r, w.currentFeatureValuesData); err != nil {
					return err
				}
				h.ValuesBlockOffset = w.valuesBlockOffset
				if err := writeDownsamplePayload(w.valuesWriter, &w.valuesBlockOffset, w.currentFeatureValuesData); err != nil {
					return err
				}
				w.currentIndexBlockData = h.Marshal(w.currentIndexBlockData)
				w.currentIndexMetaindexRow.ResolutionMs, w.currentIndexMetaindexRow.feature = w.currentResolution, uint8(feature)
				w.currentIndexMetaindexRow.RegisterBlockHeader(&h)
				w.currentIndexMetaindexRow.LastTSID = h.TSID
				w.currentIndexMetaindexRow.RowsCount += uint64(h.RowsCount)
			}
			if feature == 0 {
				expectedBlocks, expectedRows = blocks, rows
			} else if blocks != expectedBlocks || rows != expectedRows {
				return fmt.Errorf("[downsampling] feature spills have inconsistent block or row counts")
			}
			if blocks == 0 {
				return fmt.Errorf("[downsampling] feature spill is empty")
			}
			return w.flushIndex()
		})
		// 无论 Read 是否成功，本 spill 都只关闭一次；先清除引用，残留文件由 Abort 删除目标目录。
		w.currentResolutionFeatureSpills[feature] = nil
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("[downsampling] cannot close feature %d spill: %w", feature, closeErr))
		}
		if err != nil {
			return fmt.Errorf("[downsampling] cannot consume feature %d spill: %w", feature, err)
		}
	}
	return nil
}

func (w *downsampleWriter) flushIndex() error {
	if len(w.currentIndexBlockData) == 0 {
		return nil
	}
	if len(w.partMetaindexData)+downsampleMetaindexRowSize > downsampleMaxMetaindexSize {
		return fmt.Errorf("[downsampling] metaindex exceeds the memory limit")
	}
	if len(w.currentIndexBlockData) > maxBlockSize || len(w.currentIndexBlockData) != int(w.currentIndexMetaindexRow.BlockHeadersCount)*marshaledBlockHeaderSize {
		return fmt.Errorf("[downsampling] invalid index block length or header count")
	}
	if err := checkDownsampleFinishSpace(filepath.Dir(w.partPath), len(w.currentIndexBlockData), len(w.partMetaindexData)); err != nil {
		return err
	}
	w.compressedIndexData = append(w.compressedIndexData[:0], downsampleIndexMagic...)
	w.compressedIndexData = encoding.CompressZSTDLevel(w.compressedIndexData, w.currentIndexBlockData, w.compressLevel)
	if len(w.compressedIndexData) > downsampleMaxIndexSize || len(w.compressedIndexData) > 2*len(w.currentIndexBlockData)+256+len(downsampleIndexMagic) {
		return fmt.Errorf("[downsampling] compressed index block exceeds the size limit")
	}
	w.currentIndexMetaindexRow.IndexBlockOffset = w.indexBlockOffset
	w.currentIndexMetaindexRow.IndexBlockSize = uint32(len(w.compressedIndexData))
	if err := writeDownsamplePayload(w.indexWriter, &w.indexBlockOffset, w.compressedIndexData); err != nil {
		return err
	}
	w.partMetaindexData = w.currentIndexMetaindexRow.marshal(w.partMetaindexData)
	w.currentIndexBlockData = w.currentIndexBlockData[:0]
	w.currentIndexMetaindexRow = downsampleMetaindexRow{}
	return nil
}

func writeDownsamplePayload(w io.Writer, offset *uint64, b []byte) error {
	if len(b) > downsampleMaxColumnSize {
		return fmt.Errorf("[downsampling] encoded column exceeds the size limit")
	}
	if *offset > uint64(math.MaxInt64) || uint64(len(b)) > uint64(math.MaxInt64)-*offset {
		return fmt.Errorf("[downsampling] file offset overflows")
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
		return fmt.Errorf("[downsampling] cannot write data: %w", err)
	}
	if n != len(b) {
		return fmt.Errorf("[downsampling] wrote %d of %d bytes: %w", n, len(b), io.ErrShortWrite)
	}
	return nil
}

// checkDownsampleWriteSpace 在写每个 block 前复查空间；作业的完整预算由 reserveDownsampleSpace 持有。
// 此处不重复扣除活动作业的完整预算，避免将当前 writer 自己的预留算作其他占用。
// 外部写入和缓存期间的空间变化仍可能引发实际 I/O 错误，writer 必须保留 Abort 路径。
func checkDownsampleWriteSpace(path string, rowsCount int) error {
	if rowsCount <= 0 || rowsCount > maxRowsPerBlock {
		return fmt.Errorf("[downsampling] invalid downsampling block rows count %d", rowsCount)
	}
	size := estimateDownsampleOutputSize(uint64(rowsCount), 1)
	return checkDownsamplePathSpace(path, size)
}

// checkDownsampleFinishSpace 根据 writer 尚未写入的索引长度检查最终输出，避免小 part 预留整个格式上限。
func checkDownsampleFinishSpace(path string, indexDataSize, metaindexDataSize int) error {
	if indexDataSize < 0 || indexDataSize > maxBlockSize || metaindexDataSize < 0 || metaindexDataSize > downsampleMaxMetaindexSize {
		return fmt.Errorf("[downsampling] invalid downsampling finish index sizes: index=%d, metaindex=%d", indexDataSize, metaindexDataSize)
	}
	var indexBytes uint64
	if indexDataSize > 0 {
		indexBytes = min(2*uint64(indexDataSize)+256+uint64(len(downsampleIndexMagic)), downsampleMaxIndexSize)
		metaindexDataSize += downsampleMetaindexRowSize
	}
	if metaindexDataSize > downsampleMaxMetaindexSize {
		return fmt.Errorf("[downsampling] downsampling metaindex exceeds the format size limit")
	}
	metaindexBytes := min(2*uint64(metaindexDataSize)+256+uint64(len(downsampleMetaindexMagic)), downsampleMaxMetaindexSize)
	return checkDownsamplePathSpace(path, indexBytes+metaindexBytes+downsampleMaxMetadataSize)
}

// estimateDownsampleOutputSize 以摘要行与五 Block 批次数计算共享时间戳、五值列及索引上界。
func estimateDownsampleOutputSize(rows, blocks uint64) uint64 {
	// 每个 int64 的 varint 最多十字节；MarshalValues/MarshalTimestamps 在压缩无效时退回原始 varint。
	payload := multiplyDownsampleSpace(rows, 10*uint64(countOfDownsampleFeatures+1))
	index := addDownsampleSpace(multiplyDownsampleSpace(uint64(marshaledBlockHeaderSize), 2), 256+uint64(len(downsampleIndexMagic)))
	metaindex := addDownsampleSpace(multiplyDownsampleSpace(uint64(downsampleMetaindexRowSize), 2), 256+uint64(len(downsampleMetaindexMagic)))
	physicalBlocks := multiplyDownsampleSpace(blocks, countOfDownsampleFeatures)
	indexBytes := multiplyDownsampleSpace(physicalBlocks, addDownsampleSpace(index, metaindex))
	// spill 与最终输出可能同时存在；每列暂存原生 header 和 values，时间戳不重复。
	spill := addDownsampleSpace(multiplyDownsampleSpace(rows, 10*countOfDownsampleFeatures), multiplyDownsampleSpace(physicalBlocks, uint64(marshaledBlockHeaderSize)))
	return addDownsampleSpace(addDownsampleSpace(addDownsampleSpace(payload, indexBytes), spill), downsampleMaxMetadataSize)
}

func addDownsampleSpace(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

func multiplyDownsampleSpace(a, b uint64) uint64 {
	if a != 0 && b > math.MaxUint64/a {
		return math.MaxUint64
	}
	return a * b
}

func checkDownsamplePathSpace(path string, size uint64) error {
	// 写入中的作业已经通过完整预算准入；此处只复查物理空间，避免重复扣除已反映在空闲读数中的输出。
	if err := checkDownsampleAvailableSpace(fs.MustGetFreeSpace(path), 0, size, freeDiskSpaceLimitBytes); err != nil {
		return fmt.Errorf("[downsampling] cannot write downsampling data at %q: %w", path, err)
	}
	return nil
}

// checkDownsampleAvailableSpace 使用逐次相减避免可用空间、预留和安全余量相加溢出。
func checkDownsampleAvailableSpace(available, held, requested, minimumFree uint64) error {
	if available < minimumFree {
		return fmt.Errorf("%w: available=%d, minimumFree=%d", errDownsampleNoSpace, available, minimumFree)
	}
	remaining := available - minimumFree
	if remaining < held || remaining-held < requested {
		return fmt.Errorf("%w: available=%d, held=%d, requested=%d, minimumFree=%d", errDownsampleNoSpace, available, held, requested, minimumFree)
	}
	return nil
}

func getDownsampleWriter() *downsampleWriter {
	if v := downsampleWriterPool.Get(); v != nil {
		return v.(*downsampleWriter)
	}
	return &downsampleWriter{}
}

func putDownsampleWriter(w *downsampleWriter) {
	if !w.isFinished && w.partPath != "" {
		if err := w.Abort(); err != nil && w.partPath != "" {
			return // 保留路径及错误，不把仍有待删除文件的对象放入池中。
		}
	}
	w.reset()
	downsampleWriterPool.Put(w)
}

var downsampleWriterPool sync.Pool
