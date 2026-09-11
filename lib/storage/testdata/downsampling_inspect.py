#!/usr/bin/env python3
"""独立核验 E2E 产物中活动 v2 part 的磁盘索引、列负载覆盖及边界覆盖证据。"""

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


FEATURES = ("last", "sum", "count", "min", "max")
RESOLUTIONS = {300_000: "5m", 3_600_000: "1h"}
BLOCK_HEADER_BYTES = 89
METAINDEX_ROW_BYTES = 113
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
    return struct.unpack_from(">IIQIIQ", data, offset)


def tsid_text(tsid):
    return "%08x:%08x:%016x:%08x:%08x:%016x" % tsid


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
        require(data[:8] == prefix + b"\x00\x02", "v2 压缩帧前缀错误")
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


def decode_block_header(data, entry):
    require(len(data) == BLOCK_HEADER_BYTES, "单特征 header 长度错误")
    # 原生集群 blockHeader 不含 resolution/feature，两者只能从所属 metaindex row 继承。
    header = {
        "resolution_ms": entry["resolution_ms"], "feature": entry["feature"],
        "tsid": tsid_at(data, 0),
        "min_timestamp": signed(data, 32, 8), "max_timestamp": signed(data, 40, 8),
        "first_value": signed(data, 48, 8),
        "timestamp_offset": unsigned(data, 56, 8), "value_offset": unsigned(data, 64, 8),
        "timestamp_size": unsigned(data, 72, 4), "value_size": unsigned(data, 76, 4),
        "rows": unsigned(data, 80, 4), "scale": signed(data, 84, 2),
        "timestamp_codec": data[86], "value_codec": data[87], "precision": data[88],
    }
    require(header["resolution_ms"] in RESOLUTIONS, "非法分辨率")
    require(0 <= header["feature"] < len(FEATURES), "非法特征编号")
    require(1 <= header["rows"] <= 8192, "单特征 Block 行数超出限制")
    require(MIN_TIMESTAMP <= header["min_timestamp"] <= header["max_timestamp"] <= MAX_TIMESTAMP,
            "Block 时间范围无效")
    require(header["rows"] != 1 or header["min_timestamp"] == header["max_timestamp"],
            "单行 Block 的首末时间不一致")
    require(1 <= header["precision"] <= 64, "Block 精度字段无效")
    for kind in ("timestamp", "value"):
        codec = header[kind + "_codec"]
        size = header[kind + "_size"]
        require(1 <= codec <= 6, "Block codec 不属于现有编码种类")
        require(size <= 131072, "单列编码负载过大")
        require(codec not in (1, 5) or header["rows"] >= 2, "单行不能使用二阶差分编码")
        require((codec == 3) == (size == 0), "常量与非常量列的负载大小矛盾")
        require(codec != 2 or size <= 10, "等差列的负载超过 varint 上限")
        require(header[kind + "_offset"] + size <= (1 << 63) - 1, "列偏移超出 int64 范围")
    require(header["timestamp_codec"] != 3 or header["min_timestamp"] == header["max_timestamp"],
            "常量时间戳与 header 时间范围矛盾")
    return header


def decode_meta(data):
    require(len(data) == METAINDEX_ROW_BYTES, "metaindex 行长度错误")
    result = {
        "first_tsid": tsid_at(data, 0), "blocks": unsigned(data, 32, 4),
        "min_timestamp": signed(data, 36, 8), "max_timestamp": signed(data, 44, 8),
        "offset": unsigned(data, 52, 8), "size": unsigned(data, 60, 4),
        "feature": data[64], "resolution_ms": signed(data, 65, 8),
        "last_tsid": tsid_at(data, 73), "rows": unsigned(data, 105, 8),
    }
    require(result["resolution_ms"] in RESOLUTIONS, "metaindex 分辨率无效")
    require(0 <= result["feature"] < len(FEATURES), "metaindex 特征编号无效（当前格式为 0..4）")
    require(result["first_tsid"] <= result["last_tsid"], "metaindex TSID 首末范围无效")
    require(result["first_tsid"][:2] == result["last_tsid"][:2], "metaindex 首末 TSID 不属于同一租户")
    require(MIN_TIMESTAMP <= result["min_timestamp"] <= result["max_timestamp"] <= MAX_TIMESTAMP,
            "metaindex 时间范围无效")
    require(0 < result["blocks"] <= 65536 // BLOCK_HEADER_BYTES, "metaindex 单特征 Block 数量无效")
    require(result["blocks"] <= result["rows"] <= result["blocks"] * 8192,
            "metaindex 物理行数无效")
    require(8 < result["size"] <= 131072, "index 压缩负载大小无效")
    require(result["offset"] + result["size"] <= (1 << 63) - 1, "index 偏移超出 int64 范围")
    return result


def validate_metadata(metadata):
    require(set(metadata) == {
        "RowsCount", "BlocksCount", "MinTimestamp", "MaxTimestamp", "MinDedupInterval",
        "FormatVersion", "SemanticsVersion", "Mode", "Resolutions", "BucketOrigin",
        "NumericCodec", "Retention",
    }, "metadata 字段集合与 v2 约定不一致")
    require(metadata["FormatVersion"] == 2 and metadata["SemanticsVersion"] == 2
            and metadata["Mode"] == "downsampling", "活动 part 不是支持的 v2 降采样格式")
    require(metadata["Resolutions"] == [300000, 3600000] and metadata["BucketOrigin"] == 0,
            "metadata 分辨率或 bucket 原点错误")
    require(metadata["NumericCodec"] == "decimal-values" and metadata["Retention"] == "bucket-end"
            and metadata["MinDedupInterval"] == 0, "metadata 计算语义字段错误")
    for key in ("RowsCount", "BlocksCount", "MinTimestamp", "MaxTimestamp", "MinDedupInterval",
                "FormatVersion", "SemanticsVersion", "BucketOrigin"):
        require(type(metadata[key]) is int, "metadata 的 " + key + " 不是整数")
    require(metadata["RowsCount"] >= metadata["BlocksCount"] > 0
            and metadata["RowsCount"] % 5 == 0 and metadata["BlocksCount"] % 5 == 0,
            "metadata 物理行数或 Block 数量无效")
    require(MIN_TIMESTAMP <= metadata["MinTimestamp"] <= metadata["MaxTimestamp"] <= MAX_TIMESTAMP,
            "metadata 时间范围无效")


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
        "precision", "timestamp_offset", "timestamp_size", "timestamp_codec"))


def ordering_key(header):
    return (header["resolution_ms"], header["feature"], header["tsid"], header["min_timestamp"])


def column_headers(entries, index_file, sizes, zstd, reports, endpoints):
    """遍历一个 (resolution, feature)；只保留当前解压 index 和前一个 header。"""
    previous = None
    for entry in entries:
        index_file.seek(entry["offset"])
        frame = index_file.read(entry["size"])
        require(len(frame) == entry["size"], "index 文件被截断或测试期间发生变化")
        index = zstd.frame(frame, b"VMDSIX", 65536)
        require(len(index) == entry["blocks"] * BLOCK_HEADER_BYTES,
                "index 长度与物理 Block 数量不一致")
        headers = [decode_block_header(index[offset:offset + BLOCK_HEADER_BYTES], entry)
                   for offset in range(0, len(index), BLOCK_HEADER_BYTES)]
        require(headers[0]["tsid"] == entry["first_tsid"] and headers[-1]["tsid"] == entry["last_tsid"],
                "index 与 metaindex 的物理 TSID 首末标识不一致")
        require(sum(header["rows"] for header in headers) == entry["rows"]
                and min(header["min_timestamp"] for header in headers) == entry["min_timestamp"]
                and max(header["max_timestamp"] for header in headers) == entry["max_timestamp"],
                "index 与 metaindex 的行数或时间范围不一致")
        reports.append({
            "number": entry["number"], "resolution": RESOLUTIONS[entry["resolution_ms"]],
            "feature": FEATURES[entry["feature"]], "feature_id": entry["feature"],
            "account_id": entry["first_tsid"][0], "project_id": entry["first_tsid"][1],
            "offset": entry["offset"], "size": entry["size"],
            "physical_blocks": entry["blocks"], "physical_rows": entry["rows"],
            "first_tsid": tsid_text(entry["first_tsid"]), "last_tsid": tsid_text(entry["last_tsid"]),
            "unique_tsids": len({header["tsid"] for header in headers}),
        })
        endpoints[entry["number"]] = (headers[0], headers[-1])
        for header in headers:
            require(header["tsid"][:2] == entry["first_tsid"][:2], "单个 metaindex row 混入多个租户")
            require(entry["first_tsid"] <= header["tsid"] <= entry["last_tsid"],
                    "header TSID 超出 metaindex 范围")
            if previous is not None:
                require(ordering_key(previous) < ordering_key(header),
                        "物理排序键 (分辨率, feature, TSID, MinTimestamp) 未严格递增")
                require(previous["tsid"] != header["tsid"]
                        or previous["max_timestamp"] < header["min_timestamp"],
                        "同列相邻 Block（含跨 index）的同 TSID 时间范围重叠")
                for kind in ("timestamp", "value"):
                    require(header[kind + "_offset"] == previous[kind + "_offset"] + previous[kind + "_size"],
                            "同列相邻 Block（含跨 index）的 " + kind + " 负载存在间隙或重叠")
            for kind, filename in (("timestamp", "timestamps.bin"), ("value", "values.bin")):
                require(header[kind + "_offset"] + header[kind + "_size"] <= sizes[filename],
                        filename + " 负载超出文件或被截断")
            header["index_number"] = entry["number"]
            previous = header
            yield header


def inspect_part(partition, path, zstd):
    sizes = {name: (path / name).stat().st_size for name in
             ("timestamps.bin", "values.bin", "index.bin", "metaindex.bin", "metadata.json")}
    require(sizes["metadata.json"] <= 64 << 10, "metadata 文件超过大小上限")
    require(sizes["metaindex.bin"] <= 64 << 20, "metaindex 文件超过大小上限")
    metadata = read_json(path / "metadata.json")
    validate_metadata(metadata)
    meta = zstd.frame((path / "metaindex.bin").read_bytes(), b"VMDSMI", 64 << 20)
    require(meta and len(meta) % METAINDEX_ROW_BYTES == 0, "metaindex 解码长度无效")
    groups = collections.defaultdict(list)
    next_index_offset = 0
    previous_entry = None
    for number, pos in enumerate(range(0, len(meta), METAINDEX_ROW_BYTES)):
        entry = decode_meta(meta[pos:pos + METAINDEX_ROW_BYTES])
        entry["number"] = number
        require(entry["offset"] == next_index_offset, "index.bin 存在间隙、重叠或起始偏移错误")
        next_index_offset += entry["size"]
        require(next_index_offset <= sizes["index.bin"], "index 负载超出文件或被截断")
        if previous_entry is not None:
            require((previous_entry["resolution_ms"], previous_entry["feature"], previous_entry["last_tsid"])
                    <= (entry["resolution_ms"], entry["feature"], entry["first_tsid"]),
                    "metaindex 的 (分辨率, feature, TSID) 排序错误")
        previous_entry = entry
        groups[(entry["resolution_ms"], entry["feature"])].append(entry)
    require(next_index_offset == sizes["index.bin"], "index.bin 存在未引用尾部")
    del meta

    reports, boundaries, endpoints = [], [], {}
    series, metric_ids = {}, {}
    feature_tsid_sets = collections.defaultdict(lambda: [set() for _ in FEATURES])
    next_timestamp_offset = next_value_offset = 0
    physical_rows = physical_blocks = shared_timestamp_batches = 0
    zero_value_payloads = zero_timestamp_payloads = 0
    minimum = maximum = None
    with (path / "index.bin").open("rb") as index_file:
        for resolution in sorted(RESOLUTIONS):
            readers = [column_headers(groups[(resolution, feature)], index_file, sizes, zstd, reports, endpoints)
                       for feature in range(len(FEATURES))]
            first_values = [None] * len(FEATURES)
            last_values = [None] * len(FEATURES)
            while True:
                batch = [next(reader, None) for reader in readers]
                require(all(header is None for header in batch) or all(header is not None for header in batch),
                        "同分辨率特征缺列或存在多余 Block")
                if batch[0] is None:
                    break
                first = batch[0]
                require(all(shared_key(header) == shared_key(first) for header in batch),
                        "五个单特征 Block 未共享相同的时间戳和行数描述")
                require(first["timestamp_offset"] == next_timestamp_offset,
                        "timestamps.bin 重复存储、间隙、重叠或起始偏移错误")
                next_timestamp_offset += first["timestamp_size"]
                shared_timestamp_batches += 1
                zero_timestamp_payloads += first["timestamp_size"] == 0
                identity = (resolution, first["tsid"])
                if identity not in series:
                    series[identity] = {"resolution": RESOLUTIONS[resolution], "tsid": tsid_text(identity[1]),
                                        "account_id": identity[1][0], "project_id": identity[1][1],
                                        "blocks_by_feature": dict.fromkeys(FEATURES, 0), "rows_by_feature": dict.fromkeys(FEATURES, 0),
                                        "batch_rows": [], "index_numbers": set(),
                                        "index_numbers_by_feature": {feature: set() for feature in FEATURES}}
                stat = series[identity]
                stat["batch_rows"].append(first["rows"])
                for feature_index, header in enumerate(batch):
                    if first_values[feature_index] is None:
                        first_values[feature_index] = header["value_offset"]
                    last_values[feature_index] = header["value_offset"] + header["value_size"]
                    feature = FEATURES[feature_index]
                    stat["blocks_by_feature"][feature] += 1
                    stat["rows_by_feature"][feature] += header["rows"]
                    stat["index_numbers"].add(header["index_number"])
                    stat["index_numbers_by_feature"][feature].add(header["index_number"])
                    feature_tsid_sets[resolution][feature_index].add(header["tsid"])
                    metric_id = header["tsid"][-1]
                    require(metric_id not in metric_ids or metric_ids[metric_id] == header["tsid"],
                            "同一 MetricID 对应多个物理 TSID")
                    metric_ids[metric_id] = header["tsid"]
                    zero_value_payloads += header["value_size"] == 0
                    physical_rows += header["rows"]
                    physical_blocks += 1
                    minimum = header["min_timestamp"] if minimum is None else min(minimum, header["min_timestamp"])
                    maximum = header["max_timestamp"] if maximum is None else max(maximum, header["max_timestamp"])
            if first_values[0] is not None:
                for first_value, last_value in zip(first_values, last_values):
                    require(first_value == next_value_offset,
                            "values.bin 跨分辨率/feature 的列顺序不连续，存在间隙或重叠")
                    next_value_offset = last_value
    require(next_timestamp_offset == sizes["timestamps.bin"], "timestamps.bin 存在未引用尾部")
    require(next_value_offset == sizes["values.bin"], "values.bin 存在未引用尾部")
    require(physical_rows == metadata["RowsCount"] and physical_blocks == metadata["BlocksCount"]
            and minimum == metadata["MinTimestamp"] and maximum == metadata["MaxTimestamp"],
            "part 与实际物理 Block 的统计或时间范围不一致")
    for number in range(1, len(endpoints)):
        previous = endpoints[number - 1][1]
        first = endpoints[number][0]
        if previous["resolution_ms"] != first["resolution_ms"]:
            kind = "resolution_switch"
        elif previous["feature"] != first["feature"]:
            kind = "feature_switch"
        elif previous["tsid"] == first["tsid"]:
            kind = "same_tsid_continuation"
        else:
            kind = "tsid_switch"
        boundaries.append({
            "previous_index": number - 1, "next_index": number, "kind": kind,
            "previous_resolution": RESOLUTIONS[previous["resolution_ms"]],
            "next_resolution": RESOLUTIONS[first["resolution_ms"]],
            "previous_feature": FEATURES[previous["feature"]], "next_feature": FEATURES[first["feature"]],
            "previous_tsid": tsid_text(previous["tsid"]), "next_tsid": tsid_text(first["tsid"]),
            "previous_batch_min_timestamp": previous["min_timestamp"],
            "next_batch_min_timestamp": first["min_timestamp"],
        })
    physical_tsids = {}
    for resolution, sets in sorted(feature_tsid_sets.items()):
        require(all(values == sets[0] for values in sets), "同分辨率下五个特征的物理 TSID 集合不一致")
        physical_tsids[RESOLUTIONS[resolution]] = [tsid_text(tsid) for tsid in sorted(sets[0])]
    for stat in series.values():
        require(len(set(stat["blocks_by_feature"].values())) == 1 and len(set(stat["rows_by_feature"].values())) == 1,
                "同一物理 TSID 五个特征的 Block 数量或行数不一致")
        stat["index_numbers"] = sorted(stat["index_numbers"])
        stat["index_numbers_by_feature"] = {feature: sorted(numbers)
                                           for feature, numbers in stat["index_numbers_by_feature"].items()}
    return {
        "partition": partition, "path": str(path), "metadata": metadata, "file_sizes": sizes,
        "physical_rows": physical_rows, "physical_blocks": physical_blocks,
        "shared_timestamp_batches": shared_timestamp_batches, "timestamp_reference_count": physical_blocks,
        "physical_tsid_sets_equal_between_resolutions": (set(physical_tsids) == {"5m", "1h"}
                                                        and physical_tsids["5m"] == physical_tsids["1h"]),
        "zero_value_payloads": zero_value_payloads, "zero_timestamp_payloads": zero_timestamp_payloads,
        "physical_tsids_by_resolution": physical_tsids,
        "indexes": sorted(reports, key=lambda report: report["number"]), "index_boundaries": boundaries,
        "series": [series[key] for key in sorted(series)],
    }


def summarize(parts):
    partitions = collections.defaultdict(lambda: {"parts": [], "series": {}})
    identities = {}
    split_evidence, continuation_evidence, switch_evidence, multiple_indexes_evidence = [], [], [], []
    for part in parts:
        partition = partitions[part["partition"]]
        partition["parts"].append(part["path"])
        counts = collections.Counter((index["resolution"], index["feature"]) for index in part["indexes"])
        for (resolution, feature), count in sorted(counts.items()):
            if count >= 2:
                multiple_indexes_evidence.append({"partition": part["partition"], "part": part["path"],
                                                  "resolution": resolution, "feature": feature, "indexes": count})
        for stat in part["series"]:
            metric_id = stat["tsid"].rsplit(":", 1)[-1]
            require(metric_id not in identities or identities[metric_id] == stat["tsid"],
                    "不同活动 part 的同一 MetricID 对应多个物理 TSID")
            identities[metric_id] = stat["tsid"]
            key = (stat["resolution"], stat["tsid"])
            if key not in partition["series"]:
                partition["series"][key] = {"tsid": stat["tsid"], "resolution": stat["resolution"],
                                            "account_id": stat["account_id"], "project_id": stat["project_id"],
                                            "blocks_by_feature": dict.fromkeys(FEATURES, 0),
                                            "rows_by_feature": dict.fromkeys(FEATURES, 0), "part_count": 0}
            total = partition["series"][key]
            total["part_count"] += 1
            for feature in FEATURES:
                total["blocks_by_feature"][feature] += stat["blocks_by_feature"][feature]
                total["rows_by_feature"][feature] += stat["rows_by_feature"][feature]
            if stat["resolution"] == "5m" and stat["rows_by_feature"]["last"] > 8192 and stat["blocks_by_feature"]["last"] > 1:
                split_evidence.append({"partition": part["partition"], "part": part["path"], **stat})
        for boundary in part["index_boundaries"]:
            evidence = {"partition": part["partition"], "part": part["path"], **boundary}
            if boundary["kind"] == "same_tsid_continuation":
                continuation_evidence.append(evidence)
            elif boundary["kind"] == "tsid_switch":
                switch_evidence.append(evidence)
    tenant_series = collections.defaultdict(set)
    for part in parts:
        for stat in part["series"]:
            tenant_series[(stat["account_id"], stat["project_id"])].add(stat["tsid"])
    tenants = [{"account_id": tenant[0], "project_id": tenant[1], "unique_tsids": len(tsids),
                "physical_tsids": sorted(tsids)} for tenant, tsids in sorted(tenant_series.items())]
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
        "tenants": tenants,
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
    parser.add_argument("--expected-tenant", action="append", default=[],
                        help="必须精确匹配的租户集合，格式 AccountID:ProjectID，可重复指定")
    parser.add_argument("--require", action="append", default=[], dest="required_coverage",
                        help="必须实际命中的 coverage 字段，可重复指定；未指定的未命中项仍如实报告 false")
    args = parser.parse_args()
    report = {"status": "running", "data_dir": str(args.data_dir.resolve()), "parts": [],
              "decoder": "Python 固定偏移解析集群 89/113 字节记录和 32 字节 TSID，feature=0..4；系统 libzstd 仅解压",
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
        if args.expected_tenant:
            expected_tenants = set()
            for value in args.expected_tenant:
                require(re.fullmatch(r"[0-9]+:[0-9]+", value) is not None, "租户参数必须为 AccountID:ProjectID")
                account, project = map(int, value.split(":"))
                require(account <= 0xffffffff and project <= 0xffffffff, "租户参数超出 uint32 范围")
                expected_tenants.add((account, project))
            observed_tenants = {(value["account_id"], value["project_id"]) for value in report["tenants"]}
            require(expected_tenants == observed_tenants, "物理 TSID 的租户集合不符合预期")
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
