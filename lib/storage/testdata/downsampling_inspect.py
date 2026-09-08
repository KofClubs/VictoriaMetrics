#!/usr/bin/env python3
"""独立核验 E2E 产物中活动 v3 part 的磁盘索引、列负载覆盖及边界覆盖证据。"""

import argparse
import collections
import ctypes
import ctypes.util
import datetime
import json
import pathlib
import re
import struct
import sys


FIELDS = ("last", "sum", "count", "min", "max")
RESOLUTIONS = {300_000: "5m", 3_600_000: "1h"}
FIELD_HEADER_BYTES = 91
METAINDEX_ROW_BYTES = 96
MIN_TIMESTAMP = 86_400_000
MAX_TIMESTAMP = 9_222_422_399_999


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for name, value in pairs:
        require(name not in result, "JSON 出现重复字段：" + name)
        result[name] = value
    return result


def read_json(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_object)


def unsigned(data, offset, width):
    return int.from_bytes(data[offset:offset + width], "big")


def signed(data, offset, width):
    value = unsigned(data, offset, width)
    return (value >> 1) ^ -(value & 1)


def tsid_at(data, offset):
    return struct.unpack_from(">QIIQ", data, offset)


def tsid_text(tsid):
    return "%016x:%08x:%08x:%016x" % tsid


class Zstandard:
    """只调用系统 ZSTD 解压，不导入 VictoriaMetrics 代码或其 header decoder。"""

    def __init__(self):
        library = ctypes.util.find_library("zstd")
        require(library is not None, "未找到系统 libzstd；请安装 Zstandard 动态库")
        self.library = library
        self.lib = ctypes.CDLL(library)
        self.lib.ZSTD_getFrameContentSize.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        self.lib.ZSTD_getFrameContentSize.restype = ctypes.c_ulonglong
        self.lib.ZSTD_decompress.argtypes = [ctypes.c_void_p, ctypes.c_size_t,
                                           ctypes.c_void_p, ctypes.c_size_t]
        self.lib.ZSTD_decompress.restype = ctypes.c_size_t
        self.lib.ZSTD_isError.argtypes = [ctypes.c_size_t]
        self.lib.ZSTD_isError.restype = ctypes.c_uint
        self.lib.ZSTD_getErrorName.argtypes = [ctypes.c_size_t]
        self.lib.ZSTD_getErrorName.restype = ctypes.c_char_p

    def frame(self, data, prefix, limit):
        require(data[:8] == prefix + b"\x00\x03", "v3 压缩帧前缀错误")
        compressed = data[8:]
        require(compressed, "压缩帧为空")
        source = ctypes.create_string_buffer(compressed)
        size = self.lib.ZSTD_getFrameContentSize(source, len(compressed))
        require(size != (1 << 64) - 2, "无效 ZSTD 压缩帧")
        capacity = limit if size == (1 << 64) - 1 else size
        require(capacity <= limit, "压缩帧声明的解码长度超出限制")
        destination = ctypes.create_string_buffer(max(1, capacity))
        count = self.lib.ZSTD_decompress(destination, capacity, source, len(compressed))
        if self.lib.ZSTD_isError(count):
            raise ValueError("ZSTD 解压失败：" + self.lib.ZSTD_getErrorName(count).decode())
        require(count <= limit, "解压长度超出限制")
        return destination.raw[:count]


def decode_field(data):
    require(len(data) == FIELD_HEADER_BYTES, "单特征 header 长度错误")
    # 前 10 字节为扩展字段；后 81 字节按原生 blockHeader 的固定磁盘偏移读取。
    header = {
        "resolution_ms": signed(data, 0, 8), "feature": data[8],
        "timestamp_precision": data[9], "tsid": tsid_at(data, 10),
        "min_timestamp": signed(data, 34, 8), "max_timestamp": signed(data, 42, 8),
        "first_value": signed(data, 50, 8),
        "timestamp_offset": unsigned(data, 58, 8), "value_offset": unsigned(data, 66, 8),
        "timestamp_size": unsigned(data, 74, 4), "value_size": unsigned(data, 78, 4),
        "rows": unsigned(data, 82, 4), "scale": signed(data, 86, 2),
        "timestamp_codec": data[88], "value_codec": data[89], "precision": data[90],
    }
    require(header["resolution_ms"] in RESOLUTIONS, "非法分辨率")
    require(1 <= header["feature"] <= 5, "非法特征编号")
    require(1 <= header["rows"] <= 8192, "单特征 Block 行数超出限制")
    require(MIN_TIMESTAMP <= header["min_timestamp"] <= header["max_timestamp"] <= MAX_TIMESTAMP,
            "Block 时间范围无效")
    for key in ("timestamp_precision", "precision"):
        require(1 <= header[key] <= 64, "Block 精度字段无效")
    for kind in ("timestamp", "value"):
        codec = header[kind + "_codec"]
        size = header[kind + "_size"]
        require(1 <= codec <= 6, "Block codec 不属于现有编码种类")
        require(size <= 131072, "单列编码负载过大")
        require(codec not in (1, 5) or header["rows"] >= 2, "单行不能使用二阶差分编码")
        require(codec != 3 or size == 0, "常量列不应存储 values 或 timestamps 负载")
        require(header[kind + "_offset"] <= (1 << 63) - 1, "列偏移超出 int64 范围")
    return header


def decode_meta(data):
    require(len(data) == METAINDEX_ROW_BYTES, "metaindex 行长度错误")
    result = {
        "resolution_ms": signed(data, 0, 8),
        "first_tsid": tsid_at(data, 8), "last_tsid": tsid_at(data, 32),
        "min_timestamp": signed(data, 56, 8), "max_timestamp": signed(data, 64, 8),
        "blocks": unsigned(data, 72, 4), "offset": unsigned(data, 76, 8),
        "size": unsigned(data, 84, 4), "rows": unsigned(data, 88, 8),
    }
    require(result["resolution_ms"] in RESOLUTIONS, "metaindex 分辨率无效")
    require(result["first_tsid"] <= result["last_tsid"], "metaindex TSID 首末范围无效")
    require(0 < result["blocks"] <= (65536 // (5 * FIELD_HEADER_BYTES)) * 5
            and result["blocks"] % 5 == 0, "metaindex 单特征 Block 数量无效")
    require(result["blocks"] <= result["rows"] <= result["blocks"] * 8192,
            "metaindex 物理行数无效")
    require(8 < result["size"] <= 131072, "index 压缩负载大小无效")
    return result


def validate_metadata(metadata):
    require(set(metadata) == {
        "RowsCount", "BlocksCount", "MinTimestamp", "MaxTimestamp", "MinDedupInterval",
        "FormatVersion", "SemanticsVersion", "Mode", "Resolutions", "BucketOrigin",
        "NumericCodec", "Retention",
    }, "metadata 字段集合与 v3 约定不一致")
    require(metadata["FormatVersion"] == 3 and metadata["SemanticsVersion"] == 1
            and metadata["Mode"] == "downsampling", "活动 part 不是支持的 v3 降采样格式")
    require(metadata["Resolutions"] == [300000, 3600000] and metadata["BucketOrigin"] == 0,
            "metadata 分辨率或 bucket 原点错误")
    require(metadata["NumericCodec"] == "decimal-values-v1" and metadata["Retention"] == "bucket-end"
            and metadata["MinDedupInterval"] == 0, "metadata 计算语义字段错误")
    for key in ("RowsCount", "BlocksCount", "MinTimestamp", "MaxTimestamp", "MinDedupInterval",
                "FormatVersion", "SemanticsVersion", "BucketOrigin"):
        require(type(metadata[key]) is int, "metadata 的 " + key + " 不是整数")
    require(metadata["RowsCount"] >= metadata["BlocksCount"] > 0
            and metadata["RowsCount"] % 5 == 0 and metadata["BlocksCount"] % 5 == 0,
            "metadata 物理行数或 Block 数量无效")


def active_parts(data_dir):
    """parts.json 是唯一活动集合来源；不递归扫描目录，不读取 indexdb 或 snapshot。"""
    result = []
    seen = set()
    for manifest_path in sorted((data_dir / "small").glob("*/parts.json")):
        partition = manifest_path.parent.name
        require(re.fullmatch(r"\d{4}_\d{2}", partition) is not None, "partition 名称无效")
        manifest = read_json(manifest_path)
        require(set(manifest) == {"Small", "Big"}, "parts.json 字段集合无效")
        for kind in ("Small", "Big"):
            names = manifest[kind]
            require(names is None or isinstance(names, list), "parts.json 的 part 集合不是数组或 null")
            names = [] if names is None else names
            for name in names:
                require(isinstance(name, str) and re.fullmatch(r"[0-9A-Fa-f]+", name) is not None,
                        "parts.json 存在非法 part 名称")
                identity = (partition, kind, name)
                require(identity not in seen, "parts.json 重复引用活动 part")
                seen.add(identity)
                result.append((partition, data_dir / kind.lower() / partition / name))
    require(result, "没有找到活动 part；--data-dir 应指向同时包含 small 与 big 的 data 目录")
    return result


def shared_key(header):
    return tuple(header[name] for name in (
        "resolution_ms", "tsid", "min_timestamp", "max_timestamp", "rows",
        "timestamp_precision", "timestamp_offset", "timestamp_size", "timestamp_codec"))


def ordering_key(header):
    return (header["resolution_ms"], header["tsid"], header["min_timestamp"], header["feature"])


def inspect_part(partition, path, zstd):
    metadata = read_json(path / "metadata.json")
    validate_metadata(metadata)
    sizes = {name: (path / name).stat().st_size for name in
             ("timestamps.bin", "values.bin", "index.bin", "metaindex.bin", "metadata.json")}
    meta = zstd.frame((path / "metaindex.bin").read_bytes(), b"VMDSMI", 64 << 20)
    require(meta and len(meta) % METAINDEX_ROW_BYTES == 0, "metaindex 解码长度无效")
    reports, boundaries = [], []
    series = {}
    metric_ids = {}
    field_tsid_sets = collections.defaultdict(lambda: [set() for _ in FIELDS])
    next_index_offset = next_timestamp_offset = next_value_offset = 0
    physical_rows = physical_blocks = shared_timestamp_batches = 0
    zero_value_payloads = zero_timestamp_payloads = 0
    previous_header = None
    minimum = maximum = None
    with (path / "index.bin").open("rb") as index_file:
        for number, pos in enumerate(range(0, len(meta), METAINDEX_ROW_BYTES)):
            entry = decode_meta(meta[pos:pos + METAINDEX_ROW_BYTES])
            require(entry["offset"] == next_index_offset, "index.bin 存在间隙、重叠或起始偏移错误")
            require(entry["offset"] + entry["size"] <= sizes["index.bin"], "index 负载超出文件")
            index_file.seek(entry["offset"])
            frame = index_file.read(entry["size"])
            require(len(frame) == entry["size"], "index 文件被截断或测试期间发生变化")
            index = zstd.frame(frame, b"VMDSIX", 65536)
            require(len(index) == entry["blocks"] * FIELD_HEADER_BYTES, "index 长度与物理 Block 数量不一致")
            next_index_offset += entry["size"]
            headers = [decode_field(index[offset:offset + FIELD_HEADER_BYTES])
                       for offset in range(0, len(index), FIELD_HEADER_BYTES)]
            require(all(header["resolution_ms"] == entry["resolution_ms"] for header in headers),
                    "单个 index 混入多个分辨率")
            require(headers[0]["tsid"] == entry["first_tsid"] and headers[-1]["tsid"] == entry["last_tsid"],
                    "index 与 metaindex 的物理 TSID 首末标识不一致")
            require(sum(header["rows"] for header in headers) == entry["rows"]
                    and min(header["min_timestamp"] for header in headers) == entry["min_timestamp"]
                    and max(header["max_timestamp"] for header in headers) == entry["max_timestamp"],
                    "index 与 metaindex 的行数或时间范围不一致")
            if previous_header is not None:
                first = headers[0]
                if previous_header["resolution_ms"] != first["resolution_ms"]:
                    kind = "resolution_switch"
                elif previous_header["tsid"] == first["tsid"]:
                    kind = "same_tsid_continuation"
                else:
                    kind = "tsid_switch"
                boundaries.append({
                    "previous_index": number - 1, "next_index": number, "kind": kind,
                    "previous_resolution": RESOLUTIONS[previous_header["resolution_ms"]],
                    "next_resolution": RESOLUTIONS[first["resolution_ms"]],
                    "previous_tsid": tsid_text(previous_header["tsid"]), "next_tsid": tsid_text(first["tsid"]),
                    "previous_batch_min_timestamp": previous_header["min_timestamp"],
                    "next_batch_min_timestamp": first["min_timestamp"],
                })
            for batch_pos in range(0, len(headers), 5):
                batch = headers[batch_pos:batch_pos + 5]
                first = batch[0]
                require([header["feature"] for header in batch] == [1, 2, 3, 4, 5],
                        "批次的特征缺失、重复或顺序错误")
                require(all(shared_key(header) == shared_key(first) for header in batch),
                        "五个单特征 Block 未共享相同的时间戳和行数描述")
                require(first["timestamp_offset"] == next_timestamp_offset,
                        "timestamps.bin 重复存储、间隙、重叠或起始偏移错误")
                next_timestamp_offset += first["timestamp_size"]
                require(next_timestamp_offset <= sizes["timestamps.bin"], "时间戳负载超出文件")
                shared_timestamp_batches += 1
                zero_timestamp_payloads += first["timestamp_size"] == 0
                identity = (first["resolution_ms"], first["tsid"])
                if identity not in series:
                    series[identity] = {"resolution": RESOLUTIONS[identity[0]], "tsid": tsid_text(identity[1]),
                                        "blocks_by_field": dict.fromkeys(FIELDS, 0), "rows_by_field": dict.fromkeys(FIELDS, 0),
                                        "batch_rows": [], "index_numbers": set()}
                stat = series[identity]
                stat["batch_rows"].append(first["rows"])
                stat["index_numbers"].add(number)
                for header in batch:
                    if previous_header is not None:
                        require(ordering_key(previous_header) < ordering_key(header),
                                "完整物理排序键 (分辨率, TSID, MinTimestamp, Feature) 未严格递增")
                    require(header["value_offset"] == next_value_offset,
                            "values.bin 列顺序不连续，存在间隙或重叠")
                    next_value_offset += header["value_size"]
                    require(next_value_offset <= sizes["values.bin"], "values 负载超出文件")
                    field = FIELDS[header["feature"] - 1]
                    stat["blocks_by_field"][field] += 1
                    stat["rows_by_field"][field] += header["rows"]
                    field_tsid_sets[header["resolution_ms"]][header["feature"] - 1].add(header["tsid"])
                    metric_id = header["tsid"][3]
                    require(metric_id not in metric_ids or metric_ids[metric_id] == header["tsid"],
                            "同一 MetricID 对应多个物理 TSID")
                    metric_ids[metric_id] = header["tsid"]
                    zero_value_payloads += header["value_size"] == 0
                    physical_rows += header["rows"]
                    physical_blocks += 1
                    minimum = header["min_timestamp"] if minimum is None else min(minimum, header["min_timestamp"])
                    maximum = header["max_timestamp"] if maximum is None else max(maximum, header["max_timestamp"])
                    previous_header = header
            reports.append({
                "number": number, "resolution": RESOLUTIONS[entry["resolution_ms"]],
                "offset": entry["offset"], "size": entry["size"],
                "physical_blocks": entry["blocks"], "physical_rows": entry["rows"],
                "first_tsid": tsid_text(entry["first_tsid"]), "last_tsid": tsid_text(entry["last_tsid"]),
                "unique_tsids": len({header["tsid"] for header in headers}),
            })
    require(next_index_offset == sizes["index.bin"], "index.bin 存在未引用尾部")
    require(next_timestamp_offset == sizes["timestamps.bin"], "timestamps.bin 存在未引用尾部")
    require(next_value_offset == sizes["values.bin"], "values.bin 存在未引用尾部")
    require(physical_rows == metadata["RowsCount"] and physical_blocks == metadata["BlocksCount"]
            and minimum == metadata["MinTimestamp"] and maximum == metadata["MaxTimestamp"],
            "part 与实际物理 Block 的统计或时间范围不一致")
    physical_tsids = {}
    for resolution, sets in sorted(field_tsid_sets.items()):
        require(all(values == sets[0] for values in sets), "同分辨率下五个特征的物理 TSID 集合不一致")
        physical_tsids[RESOLUTIONS[resolution]] = [tsid_text(tsid) for tsid in sorted(sets[0])]
    for stat in series.values():
        require(len(set(stat["blocks_by_field"].values())) == 1 and len(set(stat["rows_by_field"].values())) == 1,
                "同一物理 TSID 五个特征的 Block 数量或行数不一致")
        stat["index_numbers"] = sorted(stat["index_numbers"])
    return {
        "partition": partition, "path": str(path), "metadata": metadata, "file_sizes": sizes,
        "physical_rows": physical_rows, "physical_blocks": physical_blocks,
        "shared_timestamp_batches": shared_timestamp_batches, "timestamp_reference_count": physical_blocks,
        "physical_tsid_sets_equal_between_resolutions": (set(physical_tsids) == {"5m", "1h"}
                                                        and physical_tsids["5m"] == physical_tsids["1h"]),
        "zero_value_payloads": zero_value_payloads, "zero_timestamp_payloads": zero_timestamp_payloads,
        "physical_tsids_by_resolution": physical_tsids, "indexes": reports, "index_boundaries": boundaries,
        "series": [series[key] for key in sorted(series)],
    }


def summarize(parts):
    partitions = collections.defaultdict(lambda: {"parts": [], "series": {}})
    identities = {}
    split_evidence, continuation_evidence, switch_evidence, multiple_indexes_evidence = [], [], [], []
    for part in parts:
        partition = partitions[part["partition"]]
        partition["parts"].append(part["path"])
        counts = collections.Counter(index["resolution"] for index in part["indexes"])
        for resolution, count in sorted(counts.items()):
            if count >= 2:
                multiple_indexes_evidence.append({"partition": part["partition"], "part": part["path"],
                                                  "resolution": resolution, "indexes": count})
        for stat in part["series"]:
            metric_id = stat["tsid"].rsplit(":", 1)[-1]
            require(metric_id not in identities or identities[metric_id] == stat["tsid"],
                    "不同活动 part 的同一 MetricID 对应多个物理 TSID")
            identities[metric_id] = stat["tsid"]
            key = (stat["resolution"], stat["tsid"])
            if key not in partition["series"]:
                partition["series"][key] = {"tsid": stat["tsid"], "resolution": stat["resolution"],
                                            "blocks_by_field": dict.fromkeys(FIELDS, 0),
                                            "rows_by_field": dict.fromkeys(FIELDS, 0), "part_count": 0}
            total = partition["series"][key]
            total["part_count"] += 1
            for field in FIELDS:
                total["blocks_by_field"][field] += stat["blocks_by_field"][field]
                total["rows_by_field"][field] += stat["rows_by_field"][field]
            if stat["resolution"] == "5m" and stat["rows_by_field"]["last"] > 8192 and stat["blocks_by_field"]["last"] > 1:
                split_evidence.append({"partition": part["partition"], "part": part["path"], **stat})
        for boundary in part["index_boundaries"]:
            evidence = {"partition": part["partition"], "part": part["path"], **boundary}
            if boundary["kind"] == "same_tsid_continuation":
                continuation_evidence.append(evidence)
            elif boundary["kind"] == "tsid_switch":
                switch_evidence.append(evidence)
    partition_reports = []
    for name, partition in sorted(partitions.items()):
        stats = [partition["series"][key] for key in sorted(partition["series"])]
        tsid_sets = {resolution: sorted({stat["tsid"] for stat in stats if stat["resolution"] == resolution})
                     for resolution in RESOLUTIONS.values()}
        partition_reports.append({"partition": name, "parts": partition["parts"],
                                  "unique_tsids": len({stat["tsid"] for stat in stats}),
                                  "physical_tsids_by_resolution": tsid_sets,
                                  "physical_tsid_sets_equal_between_resolutions": tsid_sets["5m"] == tsid_sets["1h"],
                                  "series": stats})
    coverage = {
        "same_tsid_5m_over_8192_rows_and_multiple_blocks": bool(split_evidence),
        "same_tsid_across_adjacent_indexes": bool(continuation_evidence),
        "multiple_indexes_within_one_resolution": bool(multiple_indexes_evidence),
        "different_tsids_across_adjacent_indexes": bool(switch_evidence),
        "multiple_partitions": len(partition_reports) > 1,
        "multiple_physical_tsids": len(identities) > 1,
    }
    return {
        "active_parts": len(parts), "partitions_count": len(partition_reports), "unique_tsids": len(identities),
        "physical_blocks": sum(part["physical_blocks"] for part in parts),
        "physical_rows": sum(part["physical_rows"] for part in parts),
        "index_blocks": sum(len(part["indexes"]) for part in parts),
        "partitions": partition_reports, "coverage": coverage,
        "coverage_evidence": {"same_tsid_multiple_blocks": split_evidence,
                              "same_tsid_across_indexes": continuation_evidence,
                              "different_tsids_across_indexes": switch_evidence,
                              "multiple_indexes": multiple_indexes_evidence},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=pathlib.Path, required=True,
                        help="候选实例下同时包含 small 与 big 的 data 目录")
    parser.add_argument("--output", type=pathlib.Path, required=True, help="独立核验 JSON 报告路径")
    parser.add_argument("--expected-series", type=int, help="核验全体活动 part 的物理 TSID 集合数量")
    parser.add_argument("--require", action="append", default=[], dest="required_coverage",
                        help="必须实际命中的 coverage 字段，可重复指定；未指定的未命中项仍如实报告 false")
    args = parser.parse_args()
    report = {"status": "running", "data_dir": str(args.data_dir.resolve()), "parts": [],
              "decoder": "Python 固定偏移解析 91/96 字节记录；系统 libzstd 仅解压",
              "scope": "仅 parts.json 引用的 small/big 活动 part；不读取 indexdb；不解码 values 数学内容"}
    try:
        zstd = Zstandard()
        report["zstd_library"] = zstd.library
        for partition, path in active_parts(args.data_dir.resolve()):
            report["parts"].append(inspect_part(partition, path, zstd))
        report.update(summarize(report["parts"]))
        if args.expected_series is not None:
            require(report["unique_tsids"] == args.expected_series,
                    "物理 TSID 集合数量不符合预期：%d != %d" % (report["unique_tsids"], args.expected_series))
        for key in args.required_coverage:
            require(key in report["coverage"], "未知 coverage 字段：" + key)
            require(report["coverage"][key], "E2E 产物未实际覆盖：" + key)
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
    report["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in
                      ("status", "active_parts", "partitions_count", "unique_tsids", "physical_blocks", "physical_rows",
                       "index_blocks", "coverage", "error") if key in report}, ensure_ascii=False))
    if report["status"] != "passed":
        sys.exit(1)


if __name__ == "__main__":
    main()
