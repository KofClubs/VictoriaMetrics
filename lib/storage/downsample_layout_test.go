package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

const (
	clusterDownsampleFieldHeaderBytes = 99
	clusterDownsampleMetaindexBytes   = 112
)

// TestDownsampleFilePhysicalLayout 按设计中的固定字节位置检查真实文件。
// 不调用降采样 reader 或 header decoder，避免写入端与读取端的相同错误互相抵消。
func TestDownsampleFilePhysicalLayout(t *testing.T) {
	if marshaledTSIDSize != 32 || marshaledBlockHeaderSize != 89 || downsampleFieldHeaderSize != clusterDownsampleFieldHeaderBytes || downsampleMetaindexRowSize != clusterDownsampleMetaindexBytes {
		t.Fatal("集群磁盘格式尺寸与独立约定不一致")
	}
	for _, tc := range []struct {
		name         string
		blocksPerRes int
		singleRow    bool
		wantMetaRows int
	}{
		{name: "multiple_blocks_and_resolutions", blocksPerRes: 3, wantMetaRows: 2},
		{name: "multiple_index_blocks", blocksPerRes: 330, wantMetaRows: 6},
		{name: "zero_payload_columns", blocksPerRes: 3, singleRow: true, wantMetaRows: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocks := makeDownsampleLayoutBlocks(tc.blocksPerRes, tc.singleRow)
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
			// 独立使用集群格式的 112/99 字节约定，避免复制生产尺寸计算中的错误。
			if len(meta) != tc.wantMetaRows*clusterDownsampleMetaindexBytes {
				t.Fatalf("metaindex 长度=%d，期望 %d 行，每行 %d 字节", len(meta), tc.wantMetaRows, clusterDownsampleMetaindexBytes)
			}
			var nextIndexOffset, totalRows uint64
			var expectedValues, expectedTimestamps []byte
			var zeroColumns, nonzeroColumns int
			nextBlock := 0
			minTimestamp, maxTimestamp := blocks[0].timestamps[0], blocks[0].timestamps[0]
			for pos := 0; pos < len(meta); pos += clusterDownsampleMetaindexBytes {
				mr := meta[pos : pos+clusterDownsampleMetaindexBytes]
				resolution := readDownsampleLayoutInt64(mr[0:8])
				count := binary.BigEndian.Uint32(mr[88:92])
				offset := binary.BigEndian.Uint64(mr[92:100])
				size := binary.BigEndian.Uint32(mr[100:104])
				if count == 0 || count%5 != 0 || uint64(nextBlock)+uint64(count/5) > uint64(len(blocks)) || offset != nextIndexOffset || offset+uint64(size) > uint64(len(files["index.bin"])) {
					t.Fatalf("metaindex 第 %d 行的数量或 index offset/size 错误", pos/clusterDownsampleMetaindexBytes)
				}
				nextIndexOffset += uint64(size)
				index := decodeDownsampleLayoutFrame(t, files["index.bin"][offset:nextIndexOffset], "VMDSIX")
				if len(index) != int(count)*clusterDownsampleFieldHeaderBytes {
					t.Fatalf("index 长度=%d，与 %d 个 %d 字节单特征 header 不一致", len(index), count, clusterDownsampleFieldHeaderBytes)
				}
				group := blocks[nextBlock : nextBlock+int(count/5)]
				if readDownsampleLayoutTSID(mr[8:40]) != group[0].tsid || readDownsampleLayoutTSID(mr[40:72]) != group[len(group)-1].tsid {
					t.Fatal("metaindex 首末 TSID 错误")
				}
				groupMin, groupMax := group[0].timestamps[0], group[0].timestamps[0]
				var groupRows uint64
				for i, b := range group {
					n := len(b.timestamps)
					groupRows += uint64(n) * 5
					groupMin = min(groupMin, b.timestamps[0])
					groupMax = max(groupMax, b.timestamps[n-1])
					timestampsOffset := uint64(len(expectedTimestamps))
					timestampsPayload, timestampsType, firstTimestamp := encoding.MarshalTimestamps(nil, b.timestamps, b.precisionBits)
					expectedTimestamps = append(expectedTimestamps, timestampsPayload...)
					decodedTimestamps, err := encoding.UnmarshalTimestamps(nil, timestampsPayload, timestampsType, firstTimestamp, n)
					if err != nil || !reflect.DeepEqual(decodedTimestamps, b.timestamps) {
						t.Fatalf("时间戳列解码错误: %v", err)
					}
					for col := 0; col < 5; col++ {
						h := index[(i*5+col)*clusterDownsampleFieldHeaderBytes : (i*5+col+1)*clusterDownsampleFieldHeaderBytes]
						integers, scale := decimal.AppendFloatToDecimal(nil, b.values[col])
						payload, mt, first := encoding.MarshalValues(nil, integers, b.precisionBits)
						if resolution != b.resolution || readDownsampleLayoutInt64(h[0:8]) != b.resolution || h[8] != byte(col+1) || h[9] != b.precisionBits || readDownsampleLayoutTSID(h[10:42]) != b.tsid || binary.BigEndian.Uint32(h[90:94]) != uint32(n) {
							t.Fatalf("批次 %d 特征 %d 的标识、精度或 RowsCount 错误", nextBlock+i, col+1)
						}
						if readDownsampleLayoutInt64(h[42:50]) != b.timestamps[0] || readDownsampleLayoutInt64(h[50:58]) != b.timestamps[n-1] {
							t.Fatalf("批次 %d 特征 %d 的时间范围错误", nextBlock+i, col+1)
						}
						checkDownsampleLayoutPayload(t, h[66:74], h[82:86], timestampsOffset, timestampsPayload, files["timestamps.bin"])
						checkDownsampleLayoutPayload(t, h[74:82], h[86:90], uint64(len(expectedValues)), payload, files["values.bin"])
						u := binary.BigEndian.Uint16(h[94:96])
						if readDownsampleLayoutInt64(h[58:66]) != first || int16(u>>1)^-int16(u&1) != scale || h[96] != byte(timestampsType) || h[97] != byte(mt) || h[98] != b.precisionBits {
							t.Fatalf("批次 %d 特征 %d 的原生 Block 编码字段错误", nextBlock+i, col+1)
						}
						expectedValues = append(expectedValues, payload...)
						decoded, err := encoding.UnmarshalValues(nil, payload, mt, first, n)
						if err != nil || !reflect.DeepEqual(decimal.AppendDecimalToFloat(nil, decoded, scale), b.values[col]) {
							t.Fatalf("批次 %d 特征 %d 解码错误: %v", nextBlock+i, col+1, err)
						}
						if len(payload) == 0 {
							zeroColumns++
						} else {
							nonzeroColumns++
						}
					}
				}

				if readDownsampleLayoutInt64(mr[72:80]) != groupMin || readDownsampleLayoutInt64(mr[80:88]) != groupMax || binary.BigEndian.Uint64(mr[104:112]) != groupRows {
					t.Fatal("metaindex 时间范围或物理行数错误")
				}
				minTimestamp = min(minTimestamp, groupMin)
				maxTimestamp = max(maxTimestamp, groupMax)
				totalRows += groupRows
				nextBlock += int(count / 5)
			}
			if nextBlock != len(blocks) || nextIndexOffset != uint64(len(files["index.bin"])) {
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

func makeDownsampleLayoutBlocks(blocksPerResolution int, singleRow bool) []*downsampleBatch {
	var blocks []*downsampleBatch
	for _, resolution := range []int64{300000, 3600000} {
		for i := 0; i < blocksPerResolution; i++ {
			b := &downsampleBatch{
				tsid:       TSID{AccountID: 0x11223344 + uint32(i/132), ProjectID: 0x55667788 + uint32(i%132/66), MetricGroupID: 0x123456789abcdef0, JobID: 0x23456789, InstanceID: 0x3456789a, MetricID: uint64(i/2 + 1)},
				resolution: resolution, precisionBits: 64,
			}
			n := 4 + i%3
			if singleRow {
				n = 1
			}
			for j := 0; j < n; j++ {
				b.timestamps = append(b.timestamps, 1704067200000+int64(i%2*16+j)*resolution+int64((j+1)*(j+1)*101))
				values := [5]float64{
					float64(i*16+j*j+1) / 8, float64(i*64+j*j*j+3) * 2,
					float64(j+1) / 4, -float64(i*16+j*j*j+7) / 2, float64(i*32+j*j*3+11) / 4,
				}
				if i%2 == 0 {
					values[2] = 3.25
				}
				for col, value := range values {
					b.values[col] = append(b.values[col], value)
				}
			}
			blocks = append(blocks, b)
		}
	}
	return blocks
}

func decodeDownsampleLayoutFrame(t *testing.T, data []byte, prefix string) []byte {
	t.Helper()
	if len(data) < 8 || string(data[:6]) != prefix || data[6] != 0 || data[7] != 2 {
		t.Fatalf("%s 格式前缀错误", prefix)
	}
	decoded, err := encoding.DecompressZSTDLimited(nil, data[8:], 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func checkDownsampleLayoutPayload(t *testing.T, offsetData, sizeData []byte, offset uint64, payload, file []byte) {
	t.Helper()
	gotOffset := binary.BigEndian.Uint64(offsetData)
	gotSize := binary.BigEndian.Uint32(sizeData)
	if gotOffset != offset || uint64(gotSize) != uint64(len(payload)) {
		t.Fatalf("负载定位错误: offset=%d/%d, size=%d/%d", gotOffset, offset, gotSize, len(payload))
	}
	end := gotOffset + uint64(gotSize)
	if end > uint64(len(file)) || !bytes.Equal(file[gotOffset:end], payload) {
		t.Fatalf("实际字节未形成完整连续列: offset=%d, size=%d", gotOffset, gotSize)
	}
}

func readDownsampleLayoutInt64(data []byte) int64 {
	u := binary.BigEndian.Uint64(data)
	return int64(u>>1) ^ -int64(u&1)
}

func readDownsampleLayoutTSID(data []byte) TSID {
	return TSID{
		AccountID: binary.BigEndian.Uint32(data[0:4]), ProjectID: binary.BigEndian.Uint32(data[4:8]),
		MetricGroupID: binary.BigEndian.Uint64(data[8:16]), JobID: binary.BigEndian.Uint32(data[16:20]),
		InstanceID: binary.BigEndian.Uint32(data[20:24]), MetricID: binary.BigEndian.Uint64(data[24:32]),
	}
}
