package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDownsampleWriterOrderAbort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part")
	w := getDownsampleWriter()
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	if err := writeDownsampleTestBlock(w, fileTestDownsampleBlock(1, 3600000)); err != nil {
		t.Fatal(err)
	}
	if err := writeDownsampleTestBlock(w, fileTestDownsampleBlock(1, 300000)); err == nil {
		t.Fatal("未拒绝分辨率逆序")
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("取消未清理目标")
	}
	putDownsampleWriter(w)
}

func TestDownsampleWriterExistingDirectory(t *testing.T) {
	path := t.TempDir()
	sentinel := filepath.Join(path, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(path, 1); err == nil {
		t.Fatal("writer 接受了已有目录")
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatal("取消删除了非本 writer 创建的文件")
	}
}

func TestDownsampleWriterBufferCapacity(t *testing.T) {
	var w downsampleWriter
	for i := 0; i < maxRowsPerBlock; i++ {
		w.currentFeatureDecimalValues = append(w.currentFeatureDecimalValues, int64(i))
		w.currentFeatureValues = append(w.currentFeatureValues, float64(i))
		w.currentBlockTimestamps = append(w.currentBlockTimestamps, int64(i))
	}
	intsCap, floatsCap, timestampsCap := cap(w.currentFeatureDecimalValues), cap(w.currentFeatureValues), cap(w.currentBlockTimestamps)
	w.reset()
	if cap(w.currentFeatureDecimalValues) != intsCap || cap(w.currentFeatureValues) != floatsCap || cap(w.currentBlockTimestamps) != timestampsCap {
		t.Fatal("writer discarded normal output buffer capacities")
	}
	if len(w.currentFeatureDecimalValues) != 0 || len(w.currentFeatureValues) != 0 || len(w.currentBlockTimestamps) != 0 {
		t.Fatal("writer retained output buffer contents after reset")
	}
	w.currentFeatureValues = make([]float64, downsampleMaxPooledRows+1)
	w.currentFeatureDecimalValues = make([]int64, downsampleMaxPooledRows+1)
	w.currentBlockTimestamps = make([]int64, downsampleMaxPooledRows+1)
	w.reset()
	if w.currentFeatureValues != nil || w.currentFeatureDecimalValues != nil || w.currentBlockTimestamps != nil {
		t.Fatal("writer retained oversized output buffers")
	}
}

// TestDownsampleNativeBlockReuse 逐列核对原生 Block 编码、共享时间戳和标准解码状态。
func TestDownsampleNativeBlockReuse(t *testing.T) {
	for _, precision := range []uint8{64, 8} {
		t.Run(fmt.Sprintf("shared_precision_%d", precision), func(t *testing.T) {
			batch := &downsampleDecodedResolutionFeaturesBlock{tsid: TSID{MetricID: 10}, resolution: 300000, precisionBits: precision}
			for row := 0; row < 1024; row++ {
				batch.timestamps = append(batch.timestamps, minUnixMilli+int64(row)*300000+int64((row*row*7919)%299999))
				for feature := range batch.values {
					batch.values[feature] = append(batch.values[feature], float64((row*row*17+feature*101)%104729)/float64((feature+1)*128))
				}
			}
			path := filepath.Join(t.TempDir(), "part")
			w := getDownsampleWriter()
			defer putDownsampleWriter(w)
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			if err := writeDownsampleTestBlock(w, batch); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".spill-") {
					t.Fatalf("少量数据写入创建了 spill 临时目录: %q", entry.Name())
				}
			}
			var stored [countOfDownsampleFeatures]Block
			for feature := range stored {
				// 独立构造原生编码作为对照，不依赖 writer 复用缓冲中的上一特征内容。
				values, scale := decimal.AppendFloatToDecimal(nil, batch.values[feature])
				want := &stored[feature]
				want.Init(&batch.tsid, batch.timestamps, values, scale, precision)
				want.MarshalData(0, 0)
				if !bytes.Equal(w.currentBlockTimestampsData, want.timestampsData) || want.bh.TimestampsBlockOffset != 0 {
					t.Fatalf("特征 %d 未引用同一时间戳负载", feature)
				}
				if err := w.currentResolutionFeatureSpills[feature].Read(func(r io.Reader) error {
					got, err := io.ReadAll(r)
					if err != nil {
						return err
					}
					wantData := append(append([]byte(nil), want.headerData...), want.valuesData...)
					if !bytes.Equal(got, wantData) {
						return fmt.Errorf("feature %d spill differs from native Block encoding", feature)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			fb := &w.currentFeatureBlock
			if fb.bh != stored[downsampleFeatureMax].bh || fb.nextIdx != 0 || len(fb.values) != 0 || len(fb.timestamps) != 0 || !bytes.Equal(fb.headerData, stored[downsampleFeatureMax].headerData) || !bytes.Equal(fb.timestampsData, w.currentBlockTimestampsData) || !bytes.Equal(fb.valuesData, stored[downsampleFeatureMax].valuesData) {
				t.Fatal("writer 未保持最后一个特征的原生 Block 编码状态")
			}
			if _, err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			entries, err = os.ReadDir(path)
			if err != nil {
				t.Fatal(err)
			}
			var fileNames []string
			for _, entry := range entries {
				fileNames = append(fileNames, entry.Name())
			}
			wantFiles := []string{indexFilename, metadataFilename, metaindexFilename, timestampsFilename, valuesFilename}
			if !reflect.DeepEqual(fileNames, wantFiles) {
				t.Fatalf("完成写入后的文件集合错误: got=%v want=%v", fileNames, wantFiles)
			}
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			r := getDownsampleReader()
			defer putDownsampleReader(r)
			if err := r.Init(p, 300000); err != nil || !r.NextHeader() {
				t.Fatalf("无法定位批次: %v / %v", err, r.Error())
			}
			if r.Header().PrecisionBits != precision {
				t.Fatal("时间戳未保留共享精度")
			}
			for feature := range stored {
				want := &stored[feature]
				if err := want.UnmarshalData(); err != nil {
					t.Fatal(err)
				}
				var got Block
				if err := readDownsampleFeatureBlockForTest(r, &got, uint8(feature)); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got.timestamps, want.timestamps) || !reflect.DeepEqual(got.values, want.values) || got.bh.Scale != want.bh.Scale || got.bh.PrecisionBits != batch.precisionBits || len(got.timestampsData) != 0 || len(got.valuesData) != 0 || got.nextIdx != 0 {
					t.Fatalf("特征 %d 未通过原生 Block 完成解码", feature)
				}
				// 查询用 header 直接进入既有 BlockRef，验证扩展格式不需要专用查询 decoder。
				bh, err := r.readFeatureHeader(r.Header(), uint8(feature))
				if err != nil || bh.PrecisionBits != precision {
					t.Fatalf("查询未保留共享精度: %+v / %v", bh, err)
				}
				var br BlockRef
				br.init(p, &bh)
				var standard Block
				br.MustReadBlock(&standard)
				if err := standard.UnmarshalData(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(standard.timestamps, got.timestamps) || !reflect.DeepEqual(standard.values, got.values) {
					t.Fatal("标准 BlockRef 读取结果与单特征 reader 不一致")
				}
				// 验证返回对象可继续沿原有 Block 的筛选、复制和再次编码链路使用。
				ts, values := got.AppendRowsWithTimeRangeFilter(nil, nil, TimeRange{MinTimestamp: got.timestamps[10], MaxTimestamp: got.timestamps[20]})
				if len(ts) == 0 || len(ts) != len(values) {
					t.Fatal("标准 Block 时间范围筛选失败")
				}
				var copied Block
				copied.CopyFrom(&got)
				copied.MarshalData(12, 34)
				if copied.bh.TimestampsBlockOffset != 12 || copied.bh.ValuesBlockOffset != 34 || len(copied.values) != 0 || len(copied.timestamps) != 0 {
					t.Fatal("标准 Block 重新编码状态错误")
				}
				copied.MarshalData(56, 78)
				if err := copied.UnmarshalData(); err != nil {
					t.Fatal(err)
				}
				copied.Reset()
				if copied.bh != (blockHeader{}) || copied.nextIdx != 0 || len(copied.timestamps) != 0 || len(copied.values) != 0 || len(copied.headerData) != 0 || len(copied.timestampsData) != 0 || len(copied.valuesData) != 0 {
					t.Fatal("标准 Block Reset 未清除完整逻辑状态")
				}
			}
			w.reset()
			if w.currentFeatureBlock.bh != (blockHeader{}) || len(w.currentFeatureBlock.timestampsData) != 0 || len(w.currentFeatureBlock.valuesData) != 0 || len(w.currentBlockTimestampsData) != 0 {
				t.Fatal("writer Reset 遗留特征 Block 状态")
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if r.currentFeatureBlock.bh != (blockHeader{}) || len(r.currentFeatureBlock.values) != 0 || len(r.currentFeatureBlock.timestamps) != 0 {
				t.Fatal("reader Close 遗留 Block 状态")
			}
		})
	}
}

// TestDownsampleFilePhysicalLayout 按设计中的固定字节位置检查真实文件。
// 不调用降采样 reader 或 header decoder，避免写入端与读取端的相同错误互相抵消。
func TestDownsampleFilePhysicalLayout(t *testing.T) {
	if marshaledTSIDSize != 32 || marshaledBlockHeaderSize != clusterDownsampleHeaderBytes || downsampleMetaindexRowSize != clusterDownsampleMetaindexBytes {
		t.Fatal("集群磁盘格式尺寸与独立约定不一致")
	}
	for _, tc := range []struct {
		name         string
		blocksPerRes int
		singleRow    bool
		tenantCuts   bool
		wantMetaRows int
	}{
		{name: "multiple_blocks_and_resolutions", blocksPerRes: 3, wantMetaRows: 10},
		{name: "multiple_index_blocks", blocksPerRes: 738, wantMetaRows: 20},
		{name: "tenant_row_boundaries", blocksPerRes: 330, tenantCuts: true, wantMetaRows: 50},
		{name: "zero_payload_columns", blocksPerRes: 3, singleRow: true, wantMetaRows: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocks := makeDownsampleLayoutBlocks(tc.blocksPerRes, tc.singleRow)
			for i, b := range blocks {
				b.tsid.AccountID, b.tsid.ProjectID = 0x11223344, 0x55667788
				if tc.tenantCuts {
					// 当前集群 TSID.Less 先比较租户；分别覆盖 Project 和 Account 切换。
					group := (i % tc.blocksPerRes) / 66
					b.tsid.AccountID += uint32(group / 2)
					b.tsid.ProjectID += uint32(group % 2)
				}
			}
			path := writeFileTestDownsamplePart(t, blocks...)
			files := make(map[string][]byte)
			for _, name := range []string{"timestamps.bin", "values.bin", "index.bin", "metaindex.bin", "metadata.json"} {
				data, err := os.ReadFile(filepath.Join(path, name))
				if err != nil {
					t.Fatal(err)
				}
				files[name] = data
			}
			entries, err := os.ReadDir(path)
			if err != nil || len(entries) != len(files) {
				t.Fatalf("part 文件集合错误: entries=%d, err=%v", len(entries), err)
			}

			meta := decodeDownsampleLayoutFrame(t, files["metaindex.bin"], "VMDSMI")
			// 独立使用集群格式的 113/89 字节约定，避免复制生产尺寸计算中的错误。
			if len(meta) != tc.wantMetaRows*clusterDownsampleMetaindexBytes {
				t.Fatalf("metaindex 长度=%d，期望 %d 行，每行 %d 字节", len(meta), tc.wantMetaRows, clusterDownsampleMetaindexBytes)
			}
			var nextIndexOffset, totalRows uint64
			var expectedValues, expectedTimestamps []byte
			timestampOffsets := make([]uint64, len(blocks))
			for i, b := range blocks {
				timestampOffsets[i] = uint64(len(expectedTimestamps))
				payload, _, _ := encoding.MarshalTimestamps(nil, b.timestamps, b.precisionBits)
				expectedTimestamps = append(expectedTimestamps, payload...)
			}
			var zeroColumns, nonzeroColumns int
			nextFeatureBlock := 0
			minTimestamp, maxTimestamp := blocks[0].timestamps[0], blocks[0].timestamps[0]
			for pos := 0; pos < len(meta); pos += clusterDownsampleMetaindexBytes {
				mr := meta[pos : pos+clusterDownsampleMetaindexBytes]
				resolution := readDownsampleLayoutInt64(mr[65:73])
				feature := int(mr[64])
				count := binary.BigEndian.Uint32(mr[32:36])
				offset := binary.BigEndian.Uint64(mr[52:60])
				size := binary.BigEndian.Uint32(mr[60:64])
				resIndex := nextFeatureBlock / (tc.blocksPerRes * 5)
				col := nextFeatureBlock / tc.blocksPerRes % 5
				start := nextFeatureBlock % tc.blocksPerRes
				if resIndex >= 2 || feature != col || resolution != []int64{300000, 3600000}[resIndex] || count == 0 || count > 736 || start+int(count) > tc.blocksPerRes || offset != nextIndexOffset || offset+uint64(size) > uint64(len(files["index.bin"])) {
					t.Fatalf("metaindex 第 %d 行的 feature 分组、数量或 index offset/size 错误", pos/clusterDownsampleMetaindexBytes)
				}
				nextIndexOffset += uint64(size)
				index := decodeDownsampleLayoutFrame(t, files["index.bin"][offset:nextIndexOffset], "VMDSIX")
				if len(index) != int(count)*clusterDownsampleHeaderBytes {
					t.Fatalf("index 长度=%d，与 %d 个 89 字节原生 header 不一致", len(index), count)
				}
				blockStart := resIndex*tc.blocksPerRes + start
				group := blocks[blockStart : blockStart+int(count)]
				firstTSID := readDownsampleLayoutTSID(mr[0:32])
				if firstTSID != group[0].tsid || readDownsampleLayoutTSID(mr[73:105]) != group[len(group)-1].tsid {
					t.Fatal("metaindex 首末 TSID 错误")
				}
				groupMin, groupMax := group[0].timestamps[0], group[0].timestamps[0]
				var groupRows uint64
				for i, b := range group {
					n := len(b.timestamps)
					groupRows += uint64(n)
					groupMin = min(groupMin, b.timestamps[0])
					groupMax = max(groupMax, b.timestamps[n-1])
					if b.tsid.AccountID != firstTSID.AccountID || b.tsid.ProjectID != firstTSID.ProjectID {
						t.Fatal("同一 metaindex row 跨越租户")
					}
					timestampsPayload, timestampsType, firstTimestamp := encoding.MarshalTimestamps(nil, b.timestamps, b.precisionBits)
					decodedTimestamps, err := encoding.UnmarshalTimestamps(nil, timestampsPayload, timestampsType, firstTimestamp, n)
					if err != nil || !reflect.DeepEqual(decodedTimestamps, b.timestamps) {
						t.Fatalf("时间戳列解码错误: %v", err)
					}
					h := index[i*89 : (i+1)*89]
					integers, scale := decimal.AppendFloatToDecimal(nil, b.values[col])
					payload, mt, first := encoding.MarshalValues(nil, integers, b.precisionBits)
					if resolution != b.resolution || readDownsampleLayoutTSID(h[0:32]) != b.tsid || binary.BigEndian.Uint32(h[80:84]) != uint32(n) {
						t.Fatalf("批次 %d 特征 %d 的标识或 RowsCount 错误", blockStart+i, feature)
					}
					if readDownsampleLayoutInt64(h[32:40]) != b.timestamps[0] || readDownsampleLayoutInt64(h[40:48]) != b.timestamps[n-1] {
						t.Fatalf("批次 %d 特征 %d 的时间范围错误", blockStart+i, feature)
					}
					// 五个 feature 必须引用生成顺序中同一份 timestamps payload。
					checkDownsampleLayoutPayload(t, h[56:64], h[72:76], timestampOffsets[blockStart+i], timestampsPayload, files["timestamps.bin"])
					checkDownsampleLayoutPayload(t, h[64:72], h[76:80], uint64(len(expectedValues)), payload, files["values.bin"])
					u := binary.BigEndian.Uint16(h[84:86])
					if readDownsampleLayoutInt64(h[48:56]) != first || int16(u>>1)^-int16(u&1) != scale || h[86] != byte(timestampsType) || h[87] != byte(mt) || h[88] != b.precisionBits {
						t.Fatalf("批次 %d 特征 %d 的原生 Block 编码字段错误", blockStart+i, feature)
					}
					expectedValues = append(expectedValues, payload...)
					decoded, err := encoding.UnmarshalValues(nil, payload, mt, first, n)
					if err != nil || !reflect.DeepEqual(decimal.AppendDecimalToFloat(nil, decoded, scale), b.values[col]) {
						t.Fatalf("批次 %d 特征 %d 解码错误: %v", blockStart+i, feature, err)
					}
					if len(payload) == 0 {
						zeroColumns++
					} else {
						nonzeroColumns++
					}
				}
				if readDownsampleLayoutInt64(mr[36:44]) != groupMin || readDownsampleLayoutInt64(mr[44:52]) != groupMax || binary.BigEndian.Uint64(mr[105:113]) != groupRows {
					t.Fatal("metaindex 时间范围或物理行数错误")
				}
				minTimestamp = min(minTimestamp, groupMin)
				maxTimestamp = max(maxTimestamp, groupMax)
				totalRows += groupRows
				nextFeatureBlock += int(count)
			}
			if nextFeatureBlock != len(blocks)*5 || nextIndexOffset != uint64(len(files["index.bin"])) {
				t.Fatal("block 数量错误或 index 存在未引用字节")
			}
			if !bytes.Equal(files["values.bin"], expectedValues) || !bytes.Equal(files["timestamps.bin"], expectedTimestamps) {
				t.Fatal("数据文件存在交错排列、重复时间戳负载或未引用字节")
			}
			if zeroColumns == 0 || (!tc.singleRow && nonzeroColumns == 0) {
				t.Fatal("测试未覆盖预期的零负载或非零负载列")
			}
			wantMetadata := map[string]any{
				"FormatVersion": float64(2), "SemanticsVersion": float64(2), "Mode": "downsampling",
				"Resolutions": []any{float64(300000), float64(3600000)}, "BucketOrigin": float64(0),
				"NumericCodec": "decimal-values", "Retention": "bucket-end", "MinDedupInterval": float64(0),
				"RowsCount": float64(totalRows), "BlocksCount": float64(len(blocks) * 5),
				"MinTimestamp": float64(minTimestamp), "MaxTimestamp": float64(maxTimestamp),
			}
			var gotMetadata map[string]any
			if err := json.Unmarshal(files["metadata.json"], &gotMetadata); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotMetadata, wantMetadata) {
				t.Fatalf("metadata 字段或统计错误: got=%v; want=%v", gotMetadata, wantMetadata)
			}
			t.Logf("核验 %d 个原生单特征 Block、%d 个物理行、%d 个 metaindex 行；values=%d 字节，timestamps=%d 字节", len(blocks)*5, totalRows, tc.wantMetaRows, len(expectedValues), len(expectedTimestamps))
		})
	}
}

func TestEstimateDownsampleOutputSize(t *testing.T) {
	if marshaledBlockHeaderSize != 89 || downsampleMetaindexRowSize != 113 || countOfDownsampleFeatures != 5 {
		t.Fatal("space oracle requires the current cluster 89/113-byte five-column layout")
	}
	for _, tc := range []struct {
		name         string
		rows, blocks uint64
	}{
		{"metadata_only", 0, 0},
		{"one_batch_five_indexes", 1, 1},
		{"full_batch", maxRowsPerBlock, 1},
		{"fragmented_batches", 100, 100},
		{"multiple_indexes", 738 * maxRowsPerBlock, 738},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := estimateDownsampleOutputSize(tc.rows, tc.blocks), downsampleSpaceBoundReference(tc.rows, tc.blocks); got != want {
				t.Fatalf("five-column output plus spill bound: got %d; want %d", got, want)
			}
		})
	}
	// One extra batch needs five index/metaindex pairs and five spill headers,
	// even when the logical row count is unchanged.
	if got := estimateDownsampleOutputSize(100, 2) - estimateDownsampleOutputSize(100, 1); got != 5105 {
		t.Fatalf("independent per-column index/metaindex and spill headers: got %d; want 5105", got)
	}
	if got := estimateDownsampleOutputSize(101, 1) - estimateDownsampleOutputSize(100, 1); got != 110 {
		t.Fatalf("one shared timestamp plus final/spilled values: got %d; want 110", got)
	}
}

func TestDownsampleSpaceEstimateOverflow(t *testing.T) {
	const bytesPerBatch = 5105
	rowLimit := (uint64(math.MaxUint64) - (64 << 10)) / 110
	blockLimit := (uint64(math.MaxUint64) - (64 << 10)) / bytesPerBatch
	for _, tc := range []struct {
		name         string
		rows, blocks uint64
	}{
		{"rows_before_saturation", rowLimit, 0},
		{"rows_after_saturation", rowLimit + 1, 0},
		{"blocks_before_saturation", 0, blockLimit},
		{"blocks_after_saturation", 0, blockLimit + 1},
		{"combined_addition", rowLimit, 1},
		{"payload_multiplication", math.MaxUint64/60 + 1, 0},
		{"physical_blocks_multiplication", 0, math.MaxUint64/5 + 1},
		{"maximum_inputs", math.MaxUint64, math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := estimateDownsampleOutputSize(tc.rows, tc.blocks), downsampleSpaceBoundReference(tc.rows, tc.blocks); got != want {
				t.Fatalf("space estimate wrapped or saturated early: got %d; want %d", got, want)
			}
		})
	}
	for _, tc := range []struct {
		a, b, sum, product uint64
	}{
		{0, math.MaxUint64, math.MaxUint64, 0},
		{math.MaxUint64, 0, math.MaxUint64, 0},
		{math.MaxUint64 - 1, 1, math.MaxUint64, math.MaxUint64 - 1},
		{math.MaxUint64, 1, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64/2 + 1, 2, math.MaxUint64/2 + 3, math.MaxUint64},
	} {
		if got := addDownsampleSpace(tc.a, tc.b); got != tc.sum {
			t.Fatalf("saturating add(%d, %d): got %d; want %d", tc.a, tc.b, got, tc.sum)
		}
		if got := multiplyDownsampleSpace(tc.a, tc.b); got != tc.product {
			t.Fatalf("saturating multiply(%d, %d): got %d; want %d", tc.a, tc.b, got, tc.product)
		}
	}
}

func TestDownsampleSpaceBoundCoversEncodedParts(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		blockCount, rowsPerBlock int
		indexLimit               int
	}{
		{"full_block", 1, maxRowsPerBlock, 0},
		{"one_row_per_series", 100, 1, 0},
		{"five_independent_indexes", 7, 17, marshaledBlockHeaderSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part")
			w := getDownsampleWriter()
			defer putDownsampleWriter(w)
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			w.maxIndexBlockSize = tc.indexLimit
			var timestampBytes, valuesBytes uint64
			for i := 0; i < tc.blockCount; i++ {
				b := downsampleDecodedResolutionFeaturesBlock{tsid: TSID{MetricID: uint64(i + 1)}, resolution: downsampleResolution5m, precisionBits: 64}
				for j := 0; j < tc.rowsPerBlock; j++ {
					b.timestamps = append(b.timestamps, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()+int64(j)*downsampleResolution5m)
					for feature := range b.values {
						value := float64((uint64(j+feature+1) * 0x9e3779b97f4a7c15) >> 12)
						if j%2 == 0 {
							value = -value
						}
						b.values[feature] = append(b.values[feature], value)
					}
				}
				if err := writeDownsampleTestBlock(w, &b); err != nil {
					t.Fatal(err)
				}
				// 由独立的原生编码计算应写字节数，避免依赖 writer 已复用的特征缓冲。
				for feature := range countOfDownsampleFeatures {
					values, scale := decimal.AppendFloatToDecimal(nil, b.values[feature])
					var nativeBlock Block
					nativeBlock.Init(&b.tsid, b.timestamps, values, scale, b.precisionBits)
					_, timestampsData, valuesData := nativeBlock.MarshalData(0, 0)
					if feature == downsampleFeatureLast {
						timestampBytes += uint64(len(timestampsData))
					}
					valuesBytes += uint64(len(valuesData))
				}
			}
			var spillBytes uint64
			for feature, f := range w.currentResolutionFeatureSpills {
				if f == nil {
					t.Fatalf("missing spill for feature %d", feature)
				}
				spillBytes += f.Size()
			}
			wantSpill := valuesBytes + uint64(tc.blockCount)*5*89
			if spillBytes != wantSpill {
				t.Fatalf("spill must contain five headers/values, no timestamps: got %d; want %d", spillBytes, wantSpill)
			}
			if w.timestampsBlockOffset != timestampBytes || w.valuesBlockOffset != 0 || w.indexBlockOffset != 0 {
				t.Fatalf("before resolution flush only shared timestamps may reach final files: timestampsOffset=%d; valuesOffset=%d; indexOffset=%d; timestamps=%d", w.timestampsBlockOffset, w.valuesBlockOffset, w.indexBlockOffset, timestampBytes)
			}
			ph, err := w.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if w.timestampsBlockOffset != timestampBytes || w.valuesBlockOffset != valuesBytes {
				t.Fatalf("flush duplicated timestamps or lost values: timestampsOffset=%d; valuesOffset=%d; timestamps=%d; values=%d", w.timestampsBlockOffset, w.valuesBlockOffset, timestampBytes, valuesBytes)
			}
			var encodedSize uint64
			entries, err := os.ReadDir(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				info, err := entry.Info()
				if err != nil {
					t.Fatal(err)
				}
				encodedSize += uint64(info.Size())
			}
			if len(entries) != 5 {
				t.Fatalf("finished part must contain only five final files, no spills: got %d entries", len(entries))
			}
			for feature, f := range w.currentResolutionFeatureSpills {
				if f != nil {
					t.Fatalf("finished writer retained spill %d", feature)
				}
			}
			// Final output plus all pre-flush spills is a conservative envelope,
			// not a sampled peak: production deletes each spill after its column.
			peakEnvelope := encodedSize + spillBytes
			rows, blocks := uint64(tc.blockCount*tc.rowsPerBlock), uint64(tc.blockCount)
			bound := estimateDownsampleOutputSize(rows, blocks)
			if peakEnvelope > bound {
				t.Fatalf("output plus spill exceeds batch-derived bound: output=%d; spill=%d; bound=%d", encodedSize, spillBytes, bound)
			}
			partBound := estimateDownsamplePartSize([]*partWrapper{{p: &part{ph: ph, dsMetadata: &downsamplePartMetadata{}}}})
			if partBound < bound || peakEnvelope > partBound {
				t.Fatalf("row-derived bound does not cover output/spill: peak=%d; batch bound=%d; part bound=%d", peakEnvelope, bound, partBound)
			}
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			var indexCounts [5]int
			var blockCounts, rowCounts [5]uint64
			for _, mr := range p.dsMetaindex {
				feature := int(mr.feature)
				if feature < 0 || feature >= len(indexCounts) || mr.ResolutionMs != downsampleResolution5m {
					t.Fatalf("unexpected metaindex identity: %+v", mr)
				}
				indexCounts[feature]++
				blockCounts[feature] += uint64(mr.BlockHeadersCount)
				rowCounts[feature] += mr.RowsCount
			}
			wantIndexes := 1
			if tc.indexLimit != 0 {
				wantIndexes = tc.blockCount
			}
			for feature := range indexCounts {
				if indexCounts[feature] != wantIndexes || blockCounts[feature] != blocks || rowCounts[feature] != rows {
					t.Fatalf("feature %d independent index/metaindex: indexes=%d blocks=%d rows=%d; want %d/%d/%d", feature, indexCounts[feature], blockCounts[feature], rowCounts[feature], wantIndexes, blocks, rows)
				}
			}
		})
	}
}

func TestDownsampleAvailableSpaceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		available, held, requested, minimumFree uint64
		ok                                      bool
	}{
		{100, 40, 50, 10, true},
		{100, 40, 51, 10, false},
		{99, 0, 0, 100, false},
		{100, 101, 0, 0, false},
		{math.MaxUint64, math.MaxUint64, 1, 0, false},
		{math.MaxUint64, 0, 1, math.MaxUint64, false},
		{math.MaxUint64, 0, math.MaxUint64, 0, true},
	} {
		err := checkDownsampleAvailableSpace(tc.available, tc.held, tc.requested, tc.minimumFree)
		if tc.ok && err != nil {
			t.Fatalf("valid space budget was rejected: %+v: %s", tc, err)
		}
		if !tc.ok && !errors.Is(err, errDownsampleNoSpace) {
			t.Fatalf("invalid space budget did not return the space sentinel: %+v: %v", tc, err)
		}
	}
	for _, rows := range []int{-1, 0, maxRowsPerBlock + 1} {
		if err := checkDownsampleWriteSpace("unused", rows); err == nil {
			t.Fatalf("invalid block row count %d was accepted", rows)
		}
	}
}

func TestDownsampleWriterFinalFileFailures(t *testing.T) {
	for _, name := range []string{timestampsFilename, valuesFilename, indexFilename, metaindexFilename} {
		for _, operation := range []string{"write", "short_write"} {
			t.Run(name+"/"+operation, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "part")
				var w downsampleWriter
				if err := w.Init(path, 1); err != nil {
					t.Fatal(err)
				}
				defer w.Abort()
				files := make(map[string]*failingDownsampleFile)
				for _, file := range []struct {
					name   string
					writer *filestream.WriteCloser
				}{
					{timestampsFilename, &w.timestampsWriter},
					{valuesFilename, &w.valuesWriter},
					{indexFilename, &w.indexWriter},
					{metaindexFilename, &w.metaindexWriter},
				} {
					f := &failingDownsampleFile{WriteCloser: *file.writer}
					files[file.name] = f
					*file.writer = f
				}
				failingFile := files[name]
				cause := errors.New("injected " + operation)
				switch operation {
				case "write":
					failingFile.writeErr = cause
				case "short_write":
					failingFile.shortWrite = true
					cause = io.ErrShortWrite
				}
				err := writeDownsampleTestBlock(&w, fileTestDownsampleBlock(1, 300000))
				if err == nil {
					_, err = w.Finish()
				}
				if !errors.Is(err, cause) {
					t.Fatalf("lost %s error: %v", operation, err)
				}
				assertDownsampleWriterAborted(t, &w, path, cause)
				for name, f := range files {
					if f.closes != 1 {
						t.Fatalf("file %s must be released once even if another fails: closes=%d", name, f.closes)
					}
				}
			})
		}
	}
}

func TestDownsampleWriterMetadataCompletion(t *testing.T) {
	dataFiles := []string{timestampsFilename, valuesFilename, indexFilename, metaindexFilename}
	for _, scenario := range append([]string{"success", "empty"}, dataFiles...) {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part")
			var w downsampleWriter
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if scenario != "empty" {
				for _, resolution := range downsampleResolutions {
					if err := writeDownsampleTestBlock(&w, fileTestDownsampleBlock(1, resolution)); err != nil {
						t.Fatal(err)
					}
				}
			}
			metadataPath := filepath.Join(path, metadataFilename)
			stopped := errors.New("injected interruption after data file close")
			files := trackDownsampleWriterFiles(&w)
			for i, f := range files {
				name := dataFiles[i]
				f.afterClose = func() {
					// 包括最后一个 bin：每次真实关闭后，metadata 都尚未创建。
					if _, err := os.Stat(metadataPath); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("metadata appeared before all data files were closed: file=%s err=%v", name, err)
					}
					if scenario == name {
						panic(stopped)
					}
				}
			}
			var ph partHeader
			var finishErr error
			var interrupted bool
			func() {
				defer func() {
					if p := recover(); p != nil {
						if p != stopped {
							panic(p)
						}
						interrupted = true
					}
				}()
				ph, finishErr = w.Finish()
			}()
			switch scenario {
			case "success":
				if finishErr != nil || interrupted || !w.isFinished {
					t.Fatalf("Finish did not complete: err=%v interrupted=%v finished=%v", finishErr, interrupted, w.isFinished)
				}
				m, err := readDownsampleMetadata(path)
				if err != nil || m == nil || m.partHeader != ph {
					t.Fatalf("final metadata is incomplete or has wrong statistics: metadata=%+v err=%v", m, err)
				}
				p, err := openDownsamplePart(path)
				if err != nil {
					t.Fatalf("completion metadata refers to invalid data: %v", err)
				}
				p.MustClose()
			case "empty":
				if finishErr != nil || interrupted || ph.RowsCount != 0 {
					t.Fatalf("unexpected empty Finish result: header=%+v err=%v interrupted=%v", ph, finishErr, interrupted)
				}
				if _, err := os.Stat(metadataPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("empty output must not publish completion metadata: %v", err)
				}
			default:
				if !interrupted || w.isFinished {
					t.Fatalf("interrupted Finish published its output: interrupted=%v finished=%v", interrupted, w.isFinished)
				}
				if m, err := readDownsampleMetadata(path); err == nil || m != nil {
					t.Fatalf("interrupted data write has completion metadata: %+v / %v", m, err)
				}
				if err := w.Abort(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("interrupted output was not removed: %v", err)
				}
			}
			for i, f := range files {
				if f.closes != 1 {
					t.Fatalf("data file was not closed exactly once: file=%s closes=%d", dataFiles[i], f.closes)
				}
			}
		})
	}
}

func TestDownsampleWriterSpillAndValidationFailures(t *testing.T) {
	for _, scenario := range []string{"spill_truncated_header", "invalid_next_batch", "later_feature_write"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part")
			var w downsampleWriter
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			err := writeDownsampleTestBlock(&w, fileTestDownsampleBlock(1, 300000))
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "spill_truncated_header":
				if _, err := w.currentResolutionFeatureSpills[2].Write([]byte{1}); err != nil {
					t.Fatal(err)
				}
				_, err = w.Finish()
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("truncated spill header must fail: %v", err)
				}
			case "invalid_next_batch":
				err = writeDownsampleTestBlock(&w, nil)
			case "later_feature_write":
				// 第三个特征失败时，共享时间戳和前两个特征已写入，仍须撤销整个目标。
				spills := w.currentResolutionFeatureSpills
				lastBytes, sumBytes := spills[downsampleFeatureLast].Size(), spills[downsampleFeatureSum].Size()
				timestampBytes := w.timestampsBlockOffset
				if err := spills[downsampleFeatureCount].Close(); err != nil {
					t.Fatal(err)
				}
				err = writeDownsampleTestBlock(&w, fileTestDownsampleBlock(2, downsampleResolution5m))
				if !errors.Is(err, os.ErrClosed) {
					t.Fatalf("closed later feature spill must fail: %v", err)
				}
				if spills[downsampleFeatureLast].Size() <= lastBytes || spills[downsampleFeatureSum].Size() <= sumBytes || w.timestampsBlockOffset <= timestampBytes {
					t.Fatal("later feature failure did not follow partial batch output")
				}
			}
			if err == nil {
				t.Fatal("expected failure")
			}
			assertDownsampleWriterAborted(t, &w, path, err)
			// The same instance must be reusable after all failed output is gone.
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			if err := writeDownsampleTestBlock(&w, fileTestDownsampleBlock(1, 300000)); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Finish(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDownsampleWriterFinalFilePermissions(t *testing.T) {
	dir := t.TempDir()
	controlPath := filepath.Join(dir, "control")
	if err := os.WriteFile(controlPath, nil, 0666); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	var w downsampleWriter
	path := filepath.Join(dir, "part")
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := writeDownsampleTestBlock(&w, fileTestDownsampleBlock(1, 300000)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{timestampsFilename, valuesFilename, indexFilename, metaindexFilename, metadataFilename} {
		got, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			t.Fatal(err)
		}
		if got.Mode().Perm() != info.Mode().Perm() {
			t.Fatalf("%s permissions=%o; reference permissions=%o", name, got.Mode().Perm(), info.Mode().Perm())
		}
	}
}

func TestDownsampleWriterAbortRetriesOnlyDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part")
	var w downsampleWriter
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	files := trackDownsampleWriterFiles(&w)
	if err := writeDownsampleTestBlock(&w, fileTestDownsampleBlock(1, downsampleResolution5m)); err != nil {
		t.Fatal(err)
	}
	spills := w.currentResolutionFeatureSpills
	writeErr := errors.New("injected write failure")
	removeErr1 := errors.New("injected first removal failure")
	removeErr2 := errors.New("injected second removal failure")
	removes := 0
	w.removeAll = func(got string) error {
		if got != path {
			t.Fatalf("cleanup changed its target: got %q; want %q", got, path)
		}
		removes++
		switch removes {
		case 1:
			return removeErr1
		case 2:
			return removeErr2
		default:
			return os.RemoveAll(got)
		}
	}
	files[0].writeErr = writeErr
	err := writeDownsampleTestBlock(&w, fileTestDownsampleBlock(2, downsampleResolution5m))
	if !errors.Is(err, writeErr) || !errors.Is(err, removeErr1) || !strings.HasPrefix(err.Error(), "[downsampling]") {
		t.Fatalf("failure lost its original cause or context: %v", err)
	}
	if w.partPath != path || w.isFinished {
		t.Fatal("failed removal lost the target's cleanup ownership")
	}
	for feature, spill := range spills {
		if w.currentResolutionFeatureSpills[feature] != nil {
			t.Fatalf("released spill %d remains available for repeated cleanup", feature)
		}
		if _, err := spill.Write(nil); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("spill %d was not closed: %v", feature, err)
		}
	}
	for _, f := range files {
		if f.closes != 1 {
			t.Fatalf("final file cleanup count: closes=%d", f.closes)
		}
	}
	newPath := filepath.Join(t.TempDir(), "new-part")
	if err := w.Init(newPath, 1); err == nil || !errors.Is(err, writeErr) || !errors.Is(err, removeErr1) {
		t.Fatalf("Init discarded pending cleanup or its error: %v", err)
	}
	if w.partPath != path || removes != 1 {
		t.Fatal("Init changed or retried a pending target implicitly")
	}
	if err := w.Abort(); !errors.Is(err, removeErr2) {
		t.Fatalf("removal was not retried: %v", err)
	}
	for _, cause := range []error{writeErr, removeErr1, removeErr2} {
		if !errors.Is(w.writeErr, cause) {
			t.Fatalf("retry lost an earlier error %v: %v", cause, w.writeErr)
		}
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("successful cleanup reported an old operation error: %v", err)
	}
	if w.partPath != "" || removes != 3 {
		t.Fatal("successful removal did not release the directory")
	}
	if err := w.Abort(); err != nil || removes != 3 {
		t.Fatalf("completed cleanup was attempted again: removes=%d; err=%v", removes, err)
	}
	for _, f := range files {
		if f.closes != 1 {
			t.Fatal("directory retry repeated final file cleanup")
		}
	}
	if !errors.Is(w.writeErr, writeErr) || !errors.Is(w.writeErr, removeErr1) || !errors.Is(w.writeErr, removeErr2) {
		t.Fatalf("successful cleanup discarded the operation's error history: %v", w.writeErr)
	}
	if err := w.Init(newPath, 1); err != nil {
		t.Fatalf("fully cleaned writer cannot be reused: %v", err)
	}
	if w.writeErr != nil {
		t.Fatal("new operation inherited the previous failure")
	}
}

func TestDownsampleWriterFinishedTargetOwnership(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := "release_keeps_target"
		if abort {
			name = "abort_removes_target"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part")
			newPath := filepath.Join(t.TempDir(), "new-part")
			var w downsampleWriter
			if err := w.Init(path, 1); err != nil {
				t.Fatal(err)
			}
			oldFiles := trackDownsampleWriterFiles(&w)
			if err := writeDownsampleTestBlock(&w, fileTestDownsampleBlock(1, downsampleResolution5m)); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			if err := w.Init(newPath, 1); err != nil {
				t.Fatalf("finished writer cannot be reused: %v", err)
			}
			if w.partPath != newPath || w.isFinished {
				t.Fatal("Init did not start an independent target")
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("Init removed the previous completed target: %v", err)
			}
			files := trackDownsampleWriterFiles(&w)
			if err := writeDownsampleTestBlock(&w, fileTestDownsampleBlock(2, downsampleResolution5m)); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			if abort {
				if err := w.Abort(); err != nil {
					t.Fatal(err)
				}
			}
			putDownsampleWriter(&w)
			for _, f := range append(oldFiles, files...) {
				if f.closes != 1 {
					t.Fatalf("finished file was released again: closes=%d", f.closes)
				}
			}
			_, err := os.Stat(path)
			if err != nil {
				t.Fatalf("new target cleanup removed the previous completed target: %v", err)
			}
			_, err = os.Stat(newPath)
			if abort && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Abort kept the completed unpublished target: %v", err)
			}
			if !abort && err != nil {
				t.Fatalf("returning a finished writer deleted its target: %v", err)
			}
		})
	}
}

func TestDownsampleWriterPoolKeepsPendingCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part")
	var w downsampleWriter
	if err := w.Init(path, 1); err != nil {
		t.Fatal(err)
	}
	files := trackDownsampleWriterFiles(&w)
	cause := errors.New("injected persistent removal failure")
	w.removeAll = func(string) error { return cause }
	putDownsampleWriter(&w)
	if w.partPath != path || !errors.Is(w.writeErr, cause) {
		t.Fatal("pool release discarded pending cleanup ownership")
	}
	for _, f := range files {
		if f.closes != 1 {
			t.Fatal("pool release did not close the unfinished writer")
		}
	}
	w.removeAll = nil
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestDownsampleWriterSamplesPreserveInput(t *testing.T) {
	const base int64 = 1704067200000
	for _, precision := range []uint8{8, 64} {
		t.Run(strconv.Itoa(int(precision)), func(t *testing.T) {
			tsid := TSID{MetricID: 7}
			decoded := &downsampleDecodedResolutionFeaturesBlock{tsid: tsid, resolution: downsampleResolution5m, precisionBits: precision}
			samples := make([]downsampleSample, 2*24+3)
			for row := 0; row < 24; row++ {
				src := downsampleSample{
					timestamp:     base + int64(2*row)*downsampleResolution5m + int64(row*row*137+1),
					precisionBits: precision,
				}
				decoded.timestamps = append(decoded.timestamps, src.timestamp)
				for feature := range src.values {
					value := float64((row*row*17+feature*101)%104729) / float64((feature+1)*128)
					if row == 3 && feature == downsampleFeatureSum {
						value = math.Float64frombits(0xfff8000000001234)
					}
					src.values[feature] = value
					if math.IsNaN(value) {
						value = decimal.StaleNaN
					}
					decoded.values[feature] = append(decoded.values[feature], value)
				}
				samples[2*row+2].Merge(&src)
			}
			original := append([]downsampleSample(nil), samples...)
			want := downsampleTestBlockRows(roundTripDownsampleTestReferenceBlock(t, decoded))
			var w downsampleWriter
			path := filepath.Join(t.TempDir(), "part")
			if err := w.Init(path, -5); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			// 没有有效样本时不产生 block，也不能影响后续同一 writer 的输出。
			for _, empty := range [][]downsampleSample{nil, make([]downsampleSample, 3)} {
				if err := w.WriteSamples(&tsid, downsampleResolution5m, empty, nil); err != nil {
					t.Fatal(err)
				}
			}
			if w.partHeader.RowsCount != 0 || w.partHeader.BlocksCount != 0 {
				t.Fatal("empty samples changed the output")
			}
			if err := w.WriteSamples(&tsid, downsampleResolution5m, samples, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			for i, sample := range samples {
				before := &original[i]
				if sample.timestamp != before.timestamp || sample.precisionBits != before.precisionBits {
					t.Fatalf("sample %d timestamp or precision was modified", i)
				}
				for feature, value := range sample.values {
					if math.Float64bits(value) != math.Float64bits(before.values[feature]) {
						t.Fatalf("sample %d feature %d was modified", i, feature)
					}
				}
			}
			p, err := openDownsamplePart(path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.MustClose()
			assertDownsampleTestRows(t, readDownsampleTestPart(t, p), want)
		})
	}
}

func TestDownsampleWriterSamplesLaterBlockFailure(t *testing.T) {
	const base int64 = 1704067200000
	for _, scenario := range []string{"write", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			tsid := TSID{MetricID: 7}
			samples := make([]downsampleSample, 3)
			for i, precision := range []uint8{64, 8, 64} {
				samples[i] = downsampleSample{
					timestamp: base + int64(i)*downsampleResolution5m + 1,
					values:    [countOfDownsampleFeatures]float64{2, 2, 1, 2, 2}, precisionBits: precision,
				}
			}
			var w downsampleWriter
			path := filepath.Join(t.TempDir(), "part")
			if err := w.Init(path, -5); err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			stopCh := make(chan struct{})
			cause := errors.New("injected later block write failure")
			writes := 0
			w.timestampsWriter = &downsampleCloseTestWriter{WriteCloser: w.timestampsWriter, beforeWrite: func() error {
				writes++
				if writes != 2 {
					return nil
				}
				if w.partHeader.BlocksCount != countOfDownsampleFeatures {
					t.Fatal("failure did not occur after a completed output block")
				}
				if scenario == "cancel" {
					close(stopCh)
					return nil
				}
				return cause
			}}
			err := w.WriteSamples(&tsid, downsampleResolution5m, samples, stopCh)
			if scenario == "cancel" {
				cause = errForciblyStopped
			}
			if !errors.Is(err, cause) || writes != 2 {
				t.Fatalf("later block failure was missed: writes=%d; err=%v", writes, err)
			}
			assertDownsampleWriterAborted(t, &w, path, cause)
		})
	}
}

func BenchmarkDownsampleFileWrite(b *testing.B) {
	for _, workload := range []string{"Dense", "Sparse", "HighCardinality"} {
		b.Run(workload, func(b *testing.B) {
			root := b.TempDir()
			p := newDownsampleBenchmarkFile(b, newDownsampleBenchmarkSources(b, workload), filepath.Join(root, "source")).p
			blocks := readDownsampleBenchmarkBlocks(b, p)
			// 测试数据转置不计入 writer 基准；生产直接接收 merger 的样本切片。
			blockSamples := make([][]downsampleSample, len(blocks))
			for i, block := range blocks {
				var err error
				blockSamples[i], err = downsampleTestBlockSamples(block)
				if err != nil {
					b.Fatal(err)
				}
			}
			w := getDownsampleWriter()
			defer putDownsampleWriter(w)
			var outputBytes uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				path := filepath.Join(root, "output-"+strconv.Itoa(i))
				b.StartTimer()
				if err := w.Init(path, -5); err != nil {
					b.Fatal(err)
				}
				for blockIndex, block := range blocks {
					if err := w.WriteSamples(&block.tsid, block.resolution, blockSamples[blockIndex], nil); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := w.Finish(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				outputBytes += downsampleBenchmarkFileSize(b, path)
				if err := os.RemoveAll(path); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(outputBytes)/float64(b.N), "file-bytes/op")
			reportDownsampleBenchmarkRows(b, p.ph.RowsCount)
		})
	}
}
