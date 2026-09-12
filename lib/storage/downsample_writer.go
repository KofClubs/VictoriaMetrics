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
	"slices"
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
	// 已成功写入批次的 part 行数、block 数及时间范围。
	partHeader partHeader
	// downsamplingConfig 借用本次任务的不可变配置，验证每个租户允许写入的分辨率。
	downsamplingConfig *DownsamplingConfig
	// partResolutionSpills 保存实际写入的分辨率；允许按 TSID 交错写入，Finish 再按分辨率排序输出。
	partResolutionSpills map[int64]*downsampleResolutionSpills
	// partTenants 只记录实际写入的租户，metadata 不携带本 part 未涉及的租户配置。
	partTenants map[TenantToken]struct{}
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
	// 当前 block 的共享编码时间戳；写入时核对各特征，Finish 时复用为 spill 读取缓冲。
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

// downsampleResolutionSpills 保存一个分辨率的五列暂存及顺序校验状态，不持有独立的编码缓冲。
type downsampleResolutionSpills struct {
	// featureSpills 按特征暂存原生 header 和值；last 还保存唯一的共享时间列。
	featureSpills [countOfDownsampleFeatures]*filestream.SpillWriter
	// timestampsSize 是此分辨率的累计时间列大小，也是暂存 header 中下一个相对时间偏移。
	timestampsSize uint64
	// previousBlockHeader 保存此分辨率上一批次的输入时间范围，防止 TSID 逆序或 bucket 重复。
	previousBlockHeader blockHeader
}

var errDownsampleNoSpace = errors.New("[downsampling] insufficient free space for downsampling")

func (w *downsampleWriter) Init(path string, compressLevel int, config *DownsamplingConfig) error {
	if !w.isFinished && w.partPath != "" {
		return errors.Join(fmt.Errorf("[downsampling] writer still owns target %q; abort or release it before reinitializing", w.partPath), w.writeErr)
	}
	if config == nil || config.BaseResolutionMs() <= 0 {
		return fmt.Errorf("[downsampling] writer requires a valid downsampling configuration")
	}
	w.reset()
	if err := os.Mkdir(path, 0755); err != nil {
		return fmt.Errorf("[downsampling] cannot create target directory %q: %w", path, err)
	}
	w.partPath = path
	w.compressLevel = compressLevel
	w.downsamplingConfig = config
	w.partResolutionSpills = make(map[int64]*downsampleResolutionSpills)
	w.partTenants = make(map[TenantToken]struct{})
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
	if tsid == nil || !slices.Contains(w.downsamplingConfig.resolutionsForTenant(tsid.AccountID, tsid.ProjectID), resolution) {
		return fmt.Errorf("[downsampling] invalid TSID or resolution")
	}
	var start, rows int
	var precisionBits uint8
	for i := range bucketSamples {
		if err := checkDownsampleStopped(stopCh); err != nil {
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
	return checkDownsampleStopped(stopCh)
}

func (w *downsampleWriter) Finish(stopCh <-chan struct{}) (_ partHeader, err error) {
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
	if err := checkDownsampleStopped(stopCh); err != nil {
		return partHeader{}, err
	}
	resolutions := make([]int64, 0, len(w.partResolutionSpills))
	for resolution := range w.partResolutionSpills {
		resolutions = append(resolutions, resolution)
	}
	slices.Sort(resolutions)
	for _, resolution := range resolutions {
		if err := w.flushResolution(resolution, w.partResolutionSpills[resolution], stopCh); err != nil {
			return partHeader{}, err
		}
	}
	if err := checkDownsampleFinishSpace(filepath.Dir(w.partPath), len(w.currentIndexBlockData), len(w.partMetaindexData)); err != nil {
		return partHeader{}, err
	}
	var metadata downsamplePartMetadata
	if w.partHeader.RowsCount > 0 {
		tenants := make([]TenantToken, 0, len(w.partTenants))
		for tenant := range w.partTenants {
			tenants = append(tenants, tenant)
		}
		metadata = newDownsamplePartMetadata(w.partHeader, w.downsamplingConfig.forTenants(tenants))
		if err := metadata.validate(); err != nil {
			return partHeader{}, err
		}
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
	if err := checkDownsampleStopped(stopCh); err != nil {
		return partHeader{}, err
	}
	if w.partHeader.RowsCount > 0 {
		b, err := json.Marshal(&metadata)
		if err != nil {
			return partHeader{}, fmt.Errorf("[downsampling] cannot encode part metadata: %w", err)
		}
		if len(b) > downsampleMaxMetadataSize {
			return partHeader{}, fmt.Errorf("[downsampling] part metadata exceeds the size limit")
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
	for resolution, spills := range w.partResolutionSpills {
		for feature, f := range spills.featureSpills {
			if f != nil {
				spills.featureSpills[feature] = nil
				if err := f.Close(); err != nil {
					errs = append(errs, fmt.Errorf("[downsampling] cannot close resolution %d feature %d spill: %w", resolution, feature, err))
				}
			}
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
	w.downsamplingConfig = nil
	w.partResolutionSpills = nil
	w.partTenants = nil
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

// writeSamplesBlock 只提取一份时间戳和一个特征列，不构造完整的五列解码 block。
func (w *downsampleWriter) writeSamplesBlock(tsid *TSID, resolution int64, samples []downsampleSample, precisionBits uint8, stopCh <-chan struct{}) error {
	w.currentBlockTimestamps = w.currentBlockTimestamps[:0]
	for i := range samples {
		if err := checkDownsampleStopped(stopCh); err != nil {
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
	spills := w.partResolutionSpills[resolution]
	if spills == nil {
		spills = &downsampleResolutionSpills{}
		w.partResolutionSpills[resolution] = spills
	}
	previous := &spills.previousBlockHeader
	if previous.RowsCount > 0 && (downsampleHeaderLess(&h, previous) || (h.TSID == previous.TSID && h.MinTimestamp/resolution <= previous.MaxTimestamp/resolution)) {
		return fmt.Errorf("[downsampling] blocks are out of order or contain duplicate buckets before encoding")
	}
	if ^uint64(0)-w.partHeader.RowsCount < uint64(n)*countOfDownsampleFeatures || ^uint64(0)-w.partHeader.BlocksCount < countOfDownsampleFeatures {
		return fmt.Errorf("[downsampling] part row or block count overflows")
	}
	if err := checkDownsampleWriteSpace(filepath.Dir(w.partPath), n); err != nil {
		return err
	}
	sharedTimestampOffset := spills.timestampsSize
	var sharedTimestampHeader blockHeader
	for currentFeature := range countOfDownsampleFeatures {
		w.currentFeatureValues = w.currentFeatureValues[:0]
		for i := range samples {
			if err := checkDownsampleStopped(stopCh); err != nil {
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
		// 每个特征依次使用同一个原生 Block；值保留源精度，时间戳使用无损编码。
		fb := &w.currentFeatureBlock
		fb.Init(tsid, w.currentBlockTimestamps, w.currentFeatureDecimalValues, scale, precisionBits)
		_, timestampsData, _ := marshalDownsampleBlock(fb, w.currentBlockTimestamps, sharedTimestampOffset, 0)
		if currentFeature > 0 && (!bytes.Equal(timestampsData, w.currentBlockTimestampsData) || !sameDownsampleTimestamps(&fb.bh, &sharedTimestampHeader)) {
			return fmt.Errorf("[downsampling] feature blocks encode different shared timestamps")
		}
		if err := validateDownsampleHeader(&fb.bh); err != nil {
			return err
		}
		if err := checkDownsampleStopped(stopCh); err != nil {
			return err
		}
		if currentFeature == downsampleFeatureLast {
			// 后续 Init/MarshalData 会覆盖 Block 缓冲，必须保存独立副本作为五列共享时间戳的基准。
			w.currentBlockTimestampsData = append(w.currentBlockTimestampsData[:0], timestampsData...)
			sharedTimestampHeader = fb.bh
		}
		if spills.featureSpills[currentFeature] == nil {
			spills.featureSpills[currentFeature] = filestream.NewSpillWriter(w.partPath, fmt.Sprintf("%d-%s", resolution, downsampleFeatureNames[currentFeature]))
		}
		if err := writeDownsampleData(spills.featureSpills[currentFeature], fb.headerData); err != nil {
			return err
		}
		if currentFeature == downsampleFeatureLast {
			if err := writeDownsamplePayload(spills.featureSpills[currentFeature], &spills.timestampsSize, timestampsData); err != nil {
				return err
			}
		}
		if err := writeDownsampleData(spills.featureSpills[currentFeature], fb.valuesData); err != nil {
			return err
		}
	}
	w.partHeader.RowsCount += uint64(n) * countOfDownsampleFeatures
	w.partHeader.BlocksCount += countOfDownsampleFeatures
	w.partHeader.MinTimestamp = min(w.partHeader.MinTimestamp, h.MinTimestamp)
	w.partHeader.MaxTimestamp = max(w.partHeader.MaxTimestamp, h.MaxTimestamp)
	spills.previousBlockHeader = h
	w.partTenants[TenantToken{AccountID: tsid.AccountID, ProjectID: tsid.ProjectID}] = struct{}{}
	return nil
}

// flushResolution 按 feature 顺序消费此分辨率的五个 spill，并将相对时间偏移转换为最终文件偏移。
// 各 spill 已按 TSID/MinTimestamp 排序；Finish 按 resolution 顺序调用，形成最终文件的全局顺序。
func (w *downsampleWriter) flushResolution(resolution int64, spills *downsampleResolutionSpills, stopCh <-chan struct{}) error {
	limit := w.maxIndexBlockSize
	if limit == 0 {
		limit = maxBlockSize
	}
	if limit < marshaledBlockHeaderSize || limit > maxBlockSize {
		return fmt.Errorf("[downsampling] invalid index block size limit")
	}
	// spill 尚未删除时，最终时间列、值列和索引仍需额外空间。
	var pending uint64
	var spillCount int
	for _, f := range spills.featureSpills {
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
	timestampsStart := w.timestampsBlockOffset
	if timestampsStart > math.MaxInt64 || spills.timestampsSize > uint64(math.MaxInt64)-timestampsStart {
		return fmt.Errorf("[downsampling] resolution %d timestamp offsets overflow", resolution)
	}
	for feature, f := range spills.featureSpills {
		if err := checkDownsampleStopped(stopCh); err != nil {
			return err
		}
		err := f.Read(func(r io.Reader) error {
			var previous blockHeader
			var blocks, rows uint64
			for {
				if err := checkDownsampleStopped(stopCh); err != nil {
					return err
				}
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
				if err := checkDownsampleExtent(h.TimestampsBlockOffset, h.TimestampsBlockSize, spills.timestampsSize); err != nil {
					return err
				}
				if blocks == 0 && h.TimestampsBlockOffset != 0 {
					return fmt.Errorf("[downsampling] resolution %d feature %d timestamps do not start at zero", resolution, feature)
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
				if feature == downsampleFeatureLast {
					w.currentBlockTimestampsData = slices.Grow(w.currentBlockTimestampsData[:0], int(h.TimestampsBlockSize))[:h.TimestampsBlockSize]
					if _, err := io.ReadFull(r, w.currentBlockTimestampsData); err != nil {
						return err
					}
					if err := writeDownsamplePayload(w.timestampsWriter, &w.timestampsBlockOffset, w.currentBlockTimestampsData); err != nil {
						return err
					}
				}
				if cap(w.currentFeatureValuesData) < int(h.ValuesBlockSize) {
					w.currentFeatureValuesData = make([]byte, h.ValuesBlockSize)
				}
				w.currentFeatureValuesData = w.currentFeatureValuesData[:h.ValuesBlockSize]
				if _, err := io.ReadFull(r, w.currentFeatureValuesData); err != nil {
					return err
				}
				h.ValuesBlockOffset = w.valuesBlockOffset
				h.TimestampsBlockOffset += timestampsStart
				if err := writeDownsamplePayload(w.valuesWriter, &w.valuesBlockOffset, w.currentFeatureValuesData); err != nil {
					return err
				}
				w.currentIndexBlockData = h.Marshal(w.currentIndexBlockData)
				w.currentIndexMetaindexRow.ResolutionMs, w.currentIndexMetaindexRow.feature = resolution, uint8(feature)
				w.currentIndexMetaindexRow.RegisterBlockHeader(&h)
				w.currentIndexMetaindexRow.LastTSID = h.TSID
				w.currentIndexMetaindexRow.RowsCount += uint64(h.RowsCount)
			}
			if feature == 0 {
				expectedBlocks, expectedRows = blocks, rows
				if w.timestampsBlockOffset != timestampsStart+spills.timestampsSize {
					return fmt.Errorf("[downsampling] resolution %d timestamp size does not match its spill", resolution)
				}
			} else if blocks != expectedBlocks || rows != expectedRows {
				return fmt.Errorf("[downsampling] feature spills have inconsistent block or row counts")
			}
			if blocks == 0 {
				return fmt.Errorf("[downsampling] feature spill is empty")
			}
			if previous.TimestampsBlockOffset+uint64(previous.TimestampsBlockSize) != spills.timestampsSize {
				return fmt.Errorf("[downsampling] resolution %d feature %d timestamps do not cover the complete resolution", resolution, feature)
			}
			return w.flushIndex()
		})
		// 无论 Read 是否成功，本 spill 都只关闭一次；先清除引用，残留文件由 Abort 删除目标目录。
		spills.featureSpills[feature] = nil
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
	// spill 与最终输出可能同时存在；五列暂存原生 header 和 values，last 另存一份共享时间戳。
	spill := addDownsampleSpace(multiplyDownsampleSpace(rows, 10*uint64(countOfDownsampleFeatures+1)), multiplyDownsampleSpace(physicalBlocks, uint64(marshaledBlockHeaderSize)))
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
