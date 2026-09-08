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
)

// downsampleWriter 在全部列与索引同步完成后才返回可发布的 partHeader。
type downsampleWriter struct {
	path          string
	files         [4]*os.File
	offsets       [3]uint64
	compressLevel int
	ph            partHeader
	previous      downsampleBlockHeader
	hasPrevious   bool
	mr            downsampleMetaindexRow
	indexData     []byte
	metaindexData []byte
	compressed    []byte
	blocks        [downsampleFeaturesCount]Block
	integers      []int64
	normalized    []float64
	finished      bool
}

func (w *downsampleWriter) Init(path string, compressLevel int) error {
	w.reset()
	if err := os.Mkdir(path, 0755); err != nil {
		return err
	}
	w.path = path
	w.compressLevel = compressLevel
	w.ph.Reset()
	for i, name := range []string{timestampsFilename, valuesFilename, indexFilename, metaindexFilename} {
		f, err := os.OpenFile(filepath.Join(path, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			_ = w.Abort()
			return err
		}
		w.files[i] = f
	}
	return nil
}

func (w *downsampleWriter) WriteBlock(b *downsampleBatch) error {
	if w.path == "" || w.finished {
		return fmt.Errorf("降采样 writer 尚未初始化或已关闭")
	}
	n := len(b.timestamps)
	if err := checkDownsampleWriteSpace(filepath.Dir(w.path), n); err != nil {
		return err
	}
	if !validDownsampleResolution(b.resolution) || n == 0 || n > maxRowsPerBlock || b.timestampPrecisionBits < 1 || b.timestampPrecisionBits > 64 {
		return fmt.Errorf("无效降采样 block 分辨率、行数或时间戳精度")
	}
	for i := range b.values {
		if len(b.values[i]) != n || b.precisionBits[i] < 1 || b.precisionBits[i] > 64 {
			return fmt.Errorf("降采样列 %d 的行数或精度错误", i)
		}
	}
	for i, t := range b.timestamps {
		if t < minUnixMilli || t > maxUnixMilli || (i > 0 && t <= b.timestamps[i-1]) || (i > 0 && t/b.resolution == b.timestamps[i-1]/b.resolution) {
			return fmt.Errorf("编码前的降采样时间戳或 bucket 顺序错误")
		}
	}
	h := downsampleBlockHeader{TSID: b.tsid, ResolutionMs: b.resolution, RowsCount: uint32(n), MinTimestamp: b.timestamps[0], MaxTimestamp: b.timestamps[n-1]}
	if w.hasPrevious && (h.less(&w.previous) || (h.ResolutionMs == w.previous.ResolutionMs && h.TSID == w.previous.TSID && h.MinTimestamp/h.ResolutionMs <= w.previous.MaxTimestamp/h.ResolutionMs)) {
		return fmt.Errorf("降采样 block 排序错误或编码前 bucket 重复")
	}
	if w.hasPrevious && h.ResolutionMs != w.previous.ResolutionMs {
		if err := w.flushIndex(); err != nil {
			return err
		}
	}
	if len(w.indexData)+downsampleBlockHeaderSize > maxBlockSize {
		if err := w.flushIndex(); err != nil {
			return err
		}
	}
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
		// 每个特征拥有独立的原生 Block，完整复用初始化与编码状态转换。
		fb := &w.blocks[i]
		fb.Init(&b.tsid, b.timestamps, w.integers, scale, b.precisionBits[i])
		_, timestampsData, valuesData := fb.marshalDataWithTimestampPrecision(sharedTimestampOffset, w.offsets[1], b.timestampPrecisionBits)
		if i == 0 {
			// 时间戳负载只写入一次，后续特征引用相同的 offset 和 size。
			h.Timestamps = downsampleColumnHeader{Feature: downsampleTimestampFeature, Offset: w.offsets[0], Size: fb.bh.TimestampsBlockSize, FirstValue: fb.bh.MinTimestamp, PrecisionBits: b.timestampPrecisionBits, MarshalType: fb.bh.TimestampsMarshalType}
			if err := w.writePayload(0, timestampsData); err != nil {
				return err
			}
		} else if !bytes.Equal(timestampsData, w.blocks[0].timestampsData) {
			return fmt.Errorf("同一批次的 Block 时间戳编码不一致")
		}
		bh := &fb.bh
		h.Columns[i] = downsampleColumnHeader{Feature: uint8(i + 1), Offset: bh.ValuesBlockOffset, Size: bh.ValuesBlockSize, FirstValue: bh.FirstValue, Scale: bh.Scale, PrecisionBits: bh.PrecisionBits, MarshalType: bh.ValuesMarshalType}
		if err := w.writePayload(1, valuesData); err != nil {
			return err
		}
	}
	if ^uint64(0)-w.ph.RowsCount < uint64(n)*downsampleFeaturesCount || ^uint64(0)-w.ph.BlocksCount < downsampleFeaturesCount {
		return fmt.Errorf("降采样 part 行数溢出")
	}
	w.indexData = h.marshal(w.indexData)
	if w.mr.BlockHeadersCount == 0 {
		w.mr = downsampleMetaindexRow{ResolutionMs: h.ResolutionMs, TSID: h.TSID, MinTimestamp: h.MinTimestamp, MaxTimestamp: h.MaxTimestamp}
	}
	w.mr.LastTSID = h.TSID
	w.mr.MinTimestamp = min(w.mr.MinTimestamp, h.MinTimestamp)
	w.mr.MaxTimestamp = max(w.mr.MaxTimestamp, h.MaxTimestamp)
	w.mr.BlockHeadersCount += downsampleFeaturesCount
	w.mr.RowsCount += uint64(n) * downsampleFeaturesCount
	w.ph.RowsCount += uint64(n) * downsampleFeaturesCount
	w.ph.BlocksCount += downsampleFeaturesCount
	w.ph.MinTimestamp = min(w.ph.MinTimestamp, h.MinTimestamp)
	w.ph.MaxTimestamp = max(w.ph.MaxTimestamp, h.MaxTimestamp)
	w.previous = h
	w.hasPrevious = true
	return nil
}

func (w *downsampleWriter) writePayload(file int, b []byte) error {
	if len(b) > downsampleMaxColumnSize {
		return fmt.Errorf("降采样编码列超过大小上限")
	}
	if uint64(len(b)) > uint64(math.MaxInt64)-w.offsets[file] {
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
	w.compressed = append(w.compressed[:0], downsampleIndexMagic...)
	w.compressed = encoding.CompressZSTDLevel(w.compressed, w.indexData, w.compressLevel)
	if len(w.compressed) > downsampleMaxIndexSize || len(w.compressed) > 2*len(w.indexData)+256+len(downsampleIndexMagic) {
		return fmt.Errorf("降采样 index block 超过大小上限")
	}
	w.mr.IndexBlockOffset = w.offsets[2]
	w.mr.IndexBlockSize = uint32(len(w.compressed))
	if err := writeDownsampleData(w.files[2], w.compressed); err != nil {
		return err
	}
	w.offsets[2] += uint64(len(w.compressed))
	w.metaindexData = w.mr.marshal(w.metaindexData)
	w.indexData = w.indexData[:0]
	w.mr = downsampleMetaindexRow{}
	return nil
}

func (w *downsampleWriter) Finish() (partHeader, error) {
	if w.path == "" || w.finished {
		return partHeader{}, fmt.Errorf("降采样 writer 尚未初始化或已完成")
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
		f, err := os.OpenFile(filepath.Join(w.path, metadataFilename), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			return partHeader{}, err
		}
		err = writeDownsampleData(f, b)
		if err == nil {
			err = f.Sync()
		}
		err = errors.Join(err, f.Close())
		if err != nil {
			return partHeader{}, err
		}
	}
	var errs []error
	for i, f := range w.files {
		if f != nil {
			errs = append(errs, f.Sync(), f.Close())
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

// Abort 删除由本 writer 创建的未发布目标，包括 Finish 已完成但尚未发布的目标。
func (w *downsampleWriter) Abort() error {
	var errs []error
	for i, f := range w.files {
		if f != nil {
			errs = append(errs, f.Close())
			w.files[i] = nil
		}
	}
	if w.path != "" {
		errs = append(errs, os.RemoveAll(w.path))
	}
	w.path = ""
	return errors.Join(errs...)
}

func (w *downsampleWriter) reset() {
	if !w.finished {
		_ = w.Abort()
	} else {
		w.path = ""
	}
	w.offsets = [3]uint64{}
	w.compressLevel = 0
	w.ph.Reset()
	w.previous = downsampleBlockHeader{}
	w.hasPrevious = false
	w.mr = downsampleMetaindexRow{}
	w.finished = false
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
func putDownsampleWriter(w *downsampleWriter) { w.reset(); downsampleWriterPool.Put(w) }

var downsampleWriterPool sync.Pool
