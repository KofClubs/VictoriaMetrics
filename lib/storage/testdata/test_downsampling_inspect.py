#!/usr/bin/env python3
"""用独立固定偏移 fixture 验证 89/113 字节布局、损坏拒绝和真实 coverage。"""

import copy
import ctypes
import json
import pathlib
import struct
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import downsampling_inspect as inspect


def uint(data, offset, width, value):
    data[offset:offset + width] = value.to_bytes(width, "big")


def sint(data, offset, width, value):
    uint(data, offset, width, (value << 1) ^ (value >> (width * 8 - 1)))


def read_sint(data, offset, width):
    value = int.from_bytes(data[offset:offset + width], "big")
    return (value >> 1) ^ -(value & 1)


def varint(value):
    value <<= 1
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return result


class Fixture:
    """只编码固定磁盘字段，不复用被测 decoder 或 VictoriaMetrics 序列化。"""

    def __init__(self, zstd, *, index_limit=2, zero=False, simple=False):
        self.zstd = zstd
        self.indexes = []
        self.timestamps = bytearray()
        self.values = bytearray()
        self.meta_mutations = {}
        self.metadata_changes = {}
        a = (7, 11, 10, 1, 1, 101)
        b = (7, 11, 20, 1, 1, 99)  # 高位 TSID 增加，MetricID 反向变化。
        c = (7, 12, 10, 1, 1, 102)
        d = (8, 11, 10, 1, 1, 103)
        identities = [(a, 0), (a, 4), (a, 8), (b, 0), (c, 0), (d, 0)]
        if simple:
            identities = [(a, 0)]
        for resolution in (300000, 3600000):
            timestamps = []
            for tsid, bucket in identities:
                rows = 1 if zero or tsid == d else 3
                minimum = 1704067200000 + bucket * resolution
                maximum = minimum + (rows - 1) * resolution
                payload = b"" if rows == 1 else varint(resolution)
                timestamps.append((tsid, rows, minimum, maximum, len(self.timestamps), len(payload)))
                self.timestamps.extend(payload)
            for feature in range(5):
                current = []
                for tsid, rows, minimum, maximum, offset, size in timestamps:
                    if current and (len(current) == index_limit or current[-1][:8] != struct.pack(">II", *tsid[:2])):
                        self.indexes.append([resolution, feature, current])
                        current = []
                    header = bytearray(89)
                    struct.pack_into(">IIQIIQ", header, 0, *tsid)
                    sint(header, 32, 8, minimum)
                    sint(header, 40, 8, maximum)
                    sint(header, 48, 8, -19 + feature)
                    uint(header, 56, 8, offset)
                    uint(header, 64, 8, len(self.values))
                    uint(header, 72, 4, size)
                    value_payload = b"" if zero or rows == 1 or feature == 2 else b"\x02"
                    uint(header, 76, 4, len(value_payload))
                    uint(header, 80, 4, rows)
                    sint(header, 84, 2, -3)
                    header[86:89] = bytes((3 if not size else 2, 3 if not value_payload else 2, 64))
                    self.values.extend(value_payload)
                    current.append(header)
                if current:
                    self.indexes.append([resolution, feature, current])

    def frame(self, payload, prefix):
        lib = self.zstd.lib
        lib.ZSTD_compressBound.argtypes = [ctypes.c_size_t]
        lib.ZSTD_compressBound.restype = ctypes.c_size_t
        lib.ZSTD_compress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p,
                                      ctypes.c_size_t, ctypes.c_int]
        lib.ZSTD_compress.restype = ctypes.c_size_t
        source = ctypes.create_string_buffer(bytes(payload))
        capacity = lib.ZSTD_compressBound(len(payload))
        destination = ctypes.create_string_buffer(capacity)
        count = lib.ZSTD_compress(destination, capacity, source, len(payload), 1)
        if lib.ZSTD_isError(count):
            raise RuntimeError(lib.ZSTD_getErrorName(count).decode())
        return prefix + b"\x00\x02" + destination.raw[:count]

    def write(self, path):
        path.mkdir(parents=True, exist_ok=True)
        meta, index = bytearray(), bytearray()
        physical_rows = physical_blocks = 0
        minimum, maximum = None, None
        for number, (resolution, feature, headers) in enumerate(self.indexes):
            frame = self.frame(b"".join(headers), b"VMDSIX")
            entry = bytearray(113)
            entry[:32] = headers[0][:32]
            uint(entry, 32, 4, len(headers))
            low = min(read_sint(header, 32, 8) for header in headers)
            high = max(read_sint(header, 40, 8) for header in headers)
            rows = sum(int.from_bytes(header[80:84], "big") for header in headers)
            sint(entry, 36, 8, low)
            sint(entry, 44, 8, high)
            uint(entry, 52, 8, len(index))
            uint(entry, 60, 4, len(frame))
            entry[64] = feature
            sint(entry, 65, 8, resolution)
            entry[73:105] = headers[-1][:32]
            uint(entry, 105, 8, rows)
            if number in self.meta_mutations:
                self.meta_mutations[number](entry)
            meta.extend(entry)
            index.extend(frame)
            physical_rows += rows
            physical_blocks += len(headers)
            minimum = low if minimum is None else min(minimum, low)
            maximum = high if maximum is None else max(maximum, high)
        metadata = {"RowsCount": physical_rows, "BlocksCount": physical_blocks,
                    "MinTimestamp": minimum, "MaxTimestamp": maximum, "MinDedupInterval": 0,
                    "FormatVersion": 2, "SemanticsVersion": 2, "Mode": "downsampling",
                    "Resolutions": [300000, 3600000], "BucketOrigin": 0,
                    "NumericCodec": "decimal-values", "Retention": "bucket-end"}
        metadata.update(self.metadata_changes)
        (path / "metadata.json").write_text(json.dumps(metadata))
        (path / "metaindex.bin").write_bytes(self.frame(meta, b"VMDSMI"))
        (path / "index.bin").write_bytes(index)
        (path / "timestamps.bin").write_bytes(self.timestamps)
        (path / "values.bin").write_bytes(self.values)
        return path


class InspectTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.zstd = inspect.Zstandard()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = pathlib.Path(self.temp.name) / "part"

    def parse(self, fixture):
        return inspect.inspect_part("2024_01", fixture.write(self.path), self.zstd)

    def reject(self, fixture, message):
        with self.assertRaisesRegex(ValueError, message):
            self.parse(fixture)

    def test_native_offsets_and_meta_identity(self):
        fixture = Fixture(self.zstd)
        fixture.write(self.path)
        meta = self.zstd.frame((self.path / "metaindex.bin").read_bytes(), b"VMDSMI", 65536)
        row = inspect.decode_meta(meta[:113])
        self.assertEqual((row["resolution_ms"], row["feature"], row["blocks"], row["rows"]), (300000, 0, 2, 6))
        header = inspect.decode_field(fixture.indexes[0][2][0], row)
        self.assertEqual(header["tsid"], (7, 11, 10, 1, 1, 101))
        self.assertEqual((header["first_value"], header["scale"], header["precision"]), (-19, -3, 64))
        self.assertEqual((header["timestamp_offset"], header["value_offset"]), (0, 0))
        other = inspect.decode_field(fixture.indexes[0][2][0], {**row, "resolution_ms": 3600000, "feature": 4})
        self.assertEqual((other["resolution_ms"], other["feature"]), (3600000, 4))
        for size in (88, 90, 99):
            with self.subTest(size=size), self.assertRaisesRegex(ValueError, "header 长度"):
                inspect.decode_field(bytes(size), row)

    def test_global_layout_shared_timestamps_and_tenants(self):
        report = self.parse(Fixture(self.zstd))
        self.assertEqual((report["physical_blocks"], report["physical_rows"], report["shared_timestamp_batches"]), (60, 160, 12))
        self.assertEqual(len(report["indexes"]), 40)
        self.assertTrue(report["physical_tsid_sets_equal_between_resolutions"])
        self.assertEqual([row["feature_id"] for row in report["indexes"]], [feature for _ in range(2) for feature in range(5) for _ in range(4)])
        summary = inspect.summarize([report])
        for key in ("same_tsid_across_adjacent_indexes", "different_tsids_across_adjacent_indexes",
                    "multiple_indexes_within_one_resolution"):
            self.assertTrue(summary["coverage"][key])
        self.assertEqual(len(summary["tenants"]), 3)
        for item in summary["coverage_evidence"]["same_tsid_across_indexes"]:
            self.assertEqual(item["previous_feature"], item["next_feature"])
            self.assertEqual(item["previous_resolution"], item["next_resolution"])

    def test_feature_switch_is_not_cross_index_coverage(self):
        for zero in (False, True):
            with self.subTest(zero=zero):
                report = self.parse(Fixture(self.zstd, simple=True, zero=zero))
                self.assertEqual(len(report["indexes"]), 10)
                self.assertEqual({item["kind"] for item in report["index_boundaries"]}, {"feature_switch", "resolution_switch"})
                coverage = inspect.summarize([report])["coverage"]
                self.assertFalse(coverage["same_tsid_across_adjacent_indexes"])
                self.assertFalse(coverage["different_tsids_across_adjacent_indexes"])
                self.assertFalse(coverage["multiple_indexes_within_one_resolution"])

    def test_all_zero_payloads_allow_equal_offsets(self):
        report = self.parse(Fixture(self.zstd, zero=True))
        self.assertEqual(report["file_sizes"]["timestamps.bin"], 0)
        self.assertEqual(report["file_sizes"]["values.bin"], 0)
        self.assertEqual((report["zero_timestamp_payloads"], report["zero_value_payloads"]), (12, 60))

    def test_feature_columns_can_have_different_index_boundaries(self):
        fixture = Fixture(self.zstd)
        # 只拆分 sum 第一行：同一批次仍应跨不同 index 分组精确对齐。
        resolution, feature, headers = fixture.indexes[4]
        fixture.indexes[4:5] = [[resolution, feature, [header]] for header in headers]
        self.assertEqual(len(self.parse(fixture)["indexes"]), 41)

    def test_old_and_unknown_feature_ids_rejected(self):
        fixture = Fixture(self.zstd)
        for index in fixture.indexes:
            index[1] += 1
        self.reject(fixture, "特征编号无效")
        fixture = Fixture(self.zstd)
        fixture.indexes[0][1] = 255
        self.reject(fixture, "特征编号无效")

    def test_meta_identity_statistics_and_order_rejected(self):
        mutations = {
            "resolution": lambda row: sint(row, 65, 8, 1),
            "last_tenant": lambda row: uint(row, 77, 4, 12),
            "time_range": lambda row: sint(row, 36, 8, 0),
            "rows": lambda row: uint(row, 105, 8, 1),
            "blocks": lambda row: uint(row, 32, 4, 737),
            "row_time_stats": lambda row: sint(row, 44, 8, read_sint(row, 44, 8) + 1),
            "feature_repeat": lambda row: uint(row, 64, 1, 0),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name):
                fixture = Fixture(self.zstd)
                fixture.meta_mutations[4 if name == "feature_repeat" else 0] = mutation
                self.reject(fixture, "metaindex|排序|index 与")

    def test_missing_column_and_extra_blocks_rejected(self):
        fixture = Fixture(self.zstd, simple=True)
        fixture.indexes = [entry for entry in fixture.indexes if entry[1] != 4]
        fixture.metadata_changes = {"RowsCount": 30, "BlocksCount": 10}
        self.reject(fixture, "缺列|多余 Block")
        fixture = Fixture(self.zstd)
        # max 列删掉末 TSID，修正 metadata 分组余数以使检查深入到跨列对齐。
        del fixture.indexes[19]
        fixture.metadata_changes = {"RowsCount": 160, "BlocksCount": 60}
        self.reject(fixture, "缺列|多余 Block")
        fixture = Fixture(self.zstd)
        extra = copy.deepcopy(fixture.indexes[19])
        header = extra[2][0]
        sint(header, 32, 8, read_sint(header, 32, 8) + 300000)
        sint(header, 40, 8, read_sint(header, 40, 8) + 300000)
        fixture.indexes.insert(20, extra)
        fixture.metadata_changes = {"RowsCount": 165, "BlocksCount": 65}
        self.reject(fixture, "缺列|多余 Block")

    def test_shared_timestamps_mismatch_rejected(self):
        for offset, width in ((56, 8), (80, 4), (88, 1)):
            with self.subTest(offset=offset):
                fixture = Fixture(self.zstd)
                header = fixture.indexes[4][2][0]
                value = int.from_bytes(header[offset:offset + width], "big")
                uint(header, offset, width, value - 1 if offset == 88 else value + 1)
                fixture.metadata_changes["RowsCount"] = 160
                self.reject(fixture, "共享相同|间隙或重叠")

    def test_tenant_inside_index_rejected(self):
        fixture = Fixture(self.zstd, index_limit=4)
        # 首末 TSID 仍属 7:11；仅修改中间 header 的 ProjectID。
        uint(fixture.indexes[0][2][1], 4, 4, 12)
        self.reject(fixture, "混入多个租户")

    def test_cross_index_time_overlap_rejected(self):
        fixture = Fixture(self.zstd)
        first = fixture.indexes[1][2][0]
        previous = fixture.indexes[0][2][-1]
        sint(first, 32, 8, read_sint(previous, 40, 8))
        self.reject(fixture, "同 TSID 时间范围重叠")

    def test_offset_gaps_overlaps_and_overflow_rejected(self):
        for kind, offset in (("timestamp", 56), ("value", 64)):
            for index_number, header_number in ((0, 1), (1, 0), (4, 0)):
                for delta in (-1, 1):
                    with self.subTest(kind=kind, index=index_number, header=header_number, delta=delta):
                        fixture = Fixture(self.zstd)
                        header = fixture.indexes[index_number][2][header_number]
                        original = int.from_bytes(header[offset:offset + 8], "big")
                        uint(header, offset, 8, original + delta if original + delta >= 0 else (1 << 64) - 1)
                        self.reject(fixture, "负载|间隙|共享相同|偏移")
            fixture = Fixture(self.zstd)
            uint(fixture.indexes[0][2][0], offset, 8, (1 << 63) - 1)
            self.reject(fixture, "偏移超出")
        for delta in (-1, 1):
            fixture = Fixture(self.zstd)
            fixture.meta_mutations[1] = lambda row, delta=delta: uint(row, 52, 8, int.from_bytes(row[52:60], "big") + delta)
            self.reject(fixture, "index.bin 存在间隙")

    def test_file_truncation_and_unreferenced_tail_rejected(self):
        for filename in ("timestamps.bin", "values.bin", "index.bin", "metaindex.bin"):
            for truncate in (False, True):
                with self.subTest(filename=filename, truncate=truncate):
                    Fixture(self.zstd).write(self.path)
                    path = self.path / filename
                    original = path.read_bytes()
                    path.write_bytes(original[:-1] if truncate else original + b"x")
                    with self.assertRaises(ValueError):
                        inspect.inspect_part("2024_01", self.path, self.zstd)

    def test_invalid_codec_precision_and_row_limits_rejected(self):
        for offset, width, value in ((80, 4, 0), (80, 4, 8193), (86, 1, 0), (87, 1, 7),
                                     (88, 1, 0), (88, 1, 65), (76, 4, 0), (76, 4, 11)):
            with self.subTest(offset=offset, value=value):
                fixture = Fixture(self.zstd)
                uint(fixture.indexes[0][2][0], offset, width, value)
                self.reject(fixture, "无效|超出|大小矛盾|上限|codec")

    def test_cli_report_and_failed_coverage(self):
        data_dir = pathlib.Path(self.temp.name) / "data"
        part = data_dir / "small" / "2024_01" / "ABCDEF"
        Fixture(self.zstd, simple=True).write(part)
        (part.parent / "parts.json").write_text(json.dumps({"Small": [part.name], "Big": []}))
        output = pathlib.Path(self.temp.name) / "report.json"
        command = [sys.executable, str(pathlib.Path(inspect.__file__)), "--data-dir", str(data_dir),
                   "--output", str(output), "--expected-series", "1", "--expected-tenant", "7:11"]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads(output.read_text())
        self.assertEqual(report["status"], "passed")
        self.assertIn("89/113", report["decoder"])
        result = subprocess.run(command + ["--require", "multiple_indexes_within_one_resolution"], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(json.loads(output.read_text())["status"], "failed")


if __name__ == "__main__":
    unittest.main()
