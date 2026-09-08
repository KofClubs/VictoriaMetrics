#!/usr/bin/env python3
"""验证多时间线、多个 month partition 与长历史时间跨度的降采样查询。"""

import argparse
import bisect
import calendar
import datetime
import functools
import hashlib
import json
import math
import pathlib
import sys
import time
import urllib.parse
import urllib.request

sys.dont_write_bytecode = True

from downsampling_compare import FIELDS, RESOLUTIONS, Server, metric_value, request, write_json


DAY = 86_400_000
METRIC = "downsampling_multiseries_value"


def save_compact(path, value):
    with path.open("w") as output:
        json.dump(value, output, ensure_ascii=False, separators=(",", ":"))
        output.write("\n")


def labels_key(labels):
    return tuple(sorted(labels.items()))


class MultiServer(Server):
    def __init__(self, name, binary, root, downsampling):
        super().__init__(name, binary, root, downsampling)
        self.command = [argument for argument in self.command if not argument.startswith("-memory.allowedBytes=")]
        self.command.extend(["-memory.allowedBytes=512MiB", "-search.maxPointsPerTimeseries=1000000",
                             "-search.maxQueryDuration=2m"])
        write_json(self.root / "command.json", self.command)

    def part_metadata(self, stage):
        parts = []
        partitions = {}
        for manifest_path in sorted((self.storage / "data" / "small").glob("*/parts.json")):
            manifest = json.loads(manifest_path.read_text())
            partition = manifest_path.parent.name
            for kind in ("Small", "Big"):
                for name in manifest.get(kind) or []:
                    path = self.storage / "data" / kind.lower() / partition / name
                    metadata = json.loads((path / "metadata.json").read_text())
                    if self.downsampling:
                        assert metadata.get("Mode") == "downsampling", metadata
                        assert metadata.get("FormatVersion") == 3, metadata
                    else:
                        assert metadata.get("Mode") != "downsampling", metadata
                    partitions[partition] = partitions.get(partition, 0) + 1
                    parts.append({"partition": partition, "path": str(path), "metadata": metadata,
                                  "files": {item.name: item.stat().st_size for item in path.iterdir()
                                            if item.is_file()}})
        assert parts and all(count == 1 for count in partitions.values()), partitions
        write_json(self.root / (stage + "-parts.json"), parts)
        return parts

    def force_merge(self, stage):
        request(self.base, "/internal/force_flush")
        request(self.base, "/internal/force_merge")
        time.sleep(0.2)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            assert self.process.poll() is None, self.name + " 在归并过程中退出"
            metrics = request(self.base, "/metrics")
            active = metric_value(metrics, "vm_active_force_merges")
            inmemory = metric_value(metrics, 'vm_rows{type="storage/inmemory"}')
            merges = sum(metric_value(metrics, 'vm_active_merges{type="storage/' + kind + '"}')
                         for kind in ("inmemory", "small", "big"))
            if active == 0 and inmemory == 0 and merges == 0:
                try:
                    parts = self.part_metadata(stage)
                    file_parts = sum(metric_value(metrics, 'vm_parts{type="storage/' + kind + '"}')
                                     for kind in ("small", "big"))
                    assert file_parts == len(parts), ("metrics 与活动 manifest 的 part 数不一致", file_parts, len(parts))
                    (self.root / (stage + "-metrics.txt")).write_text(metrics)
                    return parts
                except AssertionError:
                    request(self.base, "/internal/force_merge")
            elif active == 0 and merges == 0:
                request(self.base, "/internal/force_merge")
            time.sleep(0.2)
        raise AssertionError(self.name + " 未能使每个 partition 仅保留一个 file part")

    def query_many(self, stage, case, selector, start, end, resolution=None, field=None, range_query=False):
        params = {"nocache": "1"}
        if range_query:
            path = "/api/v1/query_range"
            params.update({"query": selector, "start": start / 1000, "end": end / 1000,
                           "step": resolution, "max_lookback": resolution})
        else:
            path = "/api/v1/query"
            params.update({"query": selector + "[" + str(end - start + 1) + "ms]", "time": end / 1000})
        if field is not None:
            params["query.field"] = resolution + ":" + field
        name = "-".join(filter(None, (stage, case, resolution, field, "range" if range_query else "matrix")))
        write_json(self.root / (name + "-request.json"), {"path": path, "params": params})
        url = self.base + path + "?" + urllib.parse.urlencode(params)
        # 响应首先流式落盘，避免网络读取额外持有一份完整字符串。
        response_path = self.root / (name + "-response.json")
        with urllib.request.urlopen(url, timeout=120) as response, response_path.open("wb") as output:
            while chunk := response.read(256 * 1024):
                output.write(chunk)
        with response_path.open() as source:
            payload = json.load(source)
        assert payload.get("status") == "success", payload
        assert payload["data"]["resultType"] == "matrix", payload
        result = {}
        for series in payload["data"]["result"]:
            key = labels_key(series["metric"])
            assert key not in result, (name, "重复 series", key)
            points = [(round(float(timestamp) * 1000), float(value)) for timestamp, value in series["values"]]
            assert points, (name, "返回空 series", key)
            assert all(points[index][0] > points[index - 1][0] for index in range(1, len(points))), (name, "时间戳重复或未排序", key)
            result[key] = points
        return result, name


def make_labels(series, dense_series):
    labels = []
    for index in range(series):
        pattern = "dense" if index < dense_series else ("daily", "gapped", "early", "late")[index % 4]
        labels.append({"__name__": METRIC, "series_id": "s" + str(index).zfill(3),
                       "cohort": "dense" if index < dense_series else "sparse", "pattern": pattern,
                       "job": "job-" + str(index % 7), "instance": "instance-" + str(index % 13)})
    return labels


def active_day(pattern, day, days):
    if pattern == "gapped":
        return day % 7 not in (2, 5)
    if pattern == "early":
        return day < days * 2 // 3
    if pattern == "late":
        return day >= days // 4
    return True


def rows_for_series(index, labels, base, start_day, end_day, days, late=False):
    rows = []
    dense = labels["pattern"] == "dense"
    for day in range(start_day, end_day):
        if not active_day(labels["pattern"], day, days):
            continue
        day_start = base + day * DAY
        if dense:
            buckets = [(day * 13) % 288] if late else range(288)
            for bucket in buckets:
                bucket_start = day_start + bucket * 300_000
                value = index * 2 + (day % 11) * 0.5 + (bucket % 13) * 0.125
                if late:
                    rows.append((bucket_start + 299_997, -value - 0.25))
                else:
                    rows.extend(((bucket_start + 299_998, value), (bucket_start + 299_999, value + 0.375)))
        else:
            bucket_start = day_start + ((index * 7) % 24) * 3_600_000 + (index % 12) * 300_000
            value = index * 0.25 - (day % 19) * 0.125
            if late:
                rows.append((bucket_start + 180_000, -value - 0.5))
            else:
                rows.extend(((bucket_start + 60_000, value), (bucket_start + 240_000, value - 0.625)))
    return rows


def ingest_interval(servers, labels, inputs, fixture_file, base, first, last, days, late=False):
    pending = bytearray()
    row_count = 0
    for index, metric_labels in enumerate(labels):
        rows = rows_for_series(index, metric_labels, base, first, last, days, late)
        if not rows:
            continue
        inputs[labels_key(metric_labels)].extend(rows)
        row_count += len(rows)
        # 每条时间线倒序导入，完整记录实际发送的 JSON 行。
        rows.reverse()
        record = {"metric": metric_labels, "timestamps": [row[0] for row in rows], "values": [row[1] for row in rows]}
        line = (json.dumps(record, separators=(",", ":")) + "\n").encode()
        fixture_file.write(line)
        pending.extend(line)
        if len(pending) >= 256 * 1024:
            for server in servers:
                request(server.base, "/api/v1/import", data=bytes(pending))
            pending.clear()
    if pending:
        for server in servers:
            request(server.base, "/api/v1/import", data=bytes(pending))
    fixture_file.flush()
    return row_count


def aggregate_map(raw, resolution):
    result = {}
    for labels, points in raw.items():
        groups = {}
        for timestamp, value in points:
            groups.setdefault(timestamp // resolution, []).append((timestamp, value))
        aggregated = []
        for bucket in sorted(groups):
            group = groups[bucket]
            timestamp, last = max(group)
            values = [value for _, value in group]
            aggregated.append((timestamp, (last, math.fsum(values), float(len(values)), min(values), max(values))))
        result[labels] = aggregated
    return result


def filter_points(data, start, end, selected=None, field=None):
    result = {}
    for labels, rows in data.items():
        if selected is not None and dict(labels)["series_id"] not in selected:
            continue
        points = [(timestamp, values if field is None else values[field])
                  for timestamp, values in rows if start <= timestamp <= end]
        if points:
            result[labels] = points
    return result


def assert_maps(actual, expected, description, expected_path):
    save_compact(expected_path, [{"metric": dict(labels), "values": points} for labels, points in sorted(expected.items())])
    assert set(actual) == set(expected), (description, "series 集合不一致", "missing", list(set(expected) - set(actual))[:5],
                                          "unexpected", list(set(actual) - set(expected))[:5])
    max_error = 0.0
    rows = 0
    for labels, want in expected.items():
        got = actual[labels]
        assert len(got) == len(want), (description, labels, "样本数量", len(got), len(want))
        for index, (a, e) in enumerate(zip(got, want)):
            assert a[0] == e[0], (description, labels, index, "时间戳", a, e)
            assert math.isclose(a[1], e[1], rel_tol=1e-10, abs_tol=1e-9), (description, labels, index, "数值", a, e)
            max_error = max(max_error, abs(a[1] - e[1]))
        rows += len(got)
    return {"check": description, "series": len(actual), "rows": rows, "max_absolute_error": max_error}


def next_month(timestamp):
    date = datetime.datetime.fromtimestamp(timestamp / 1000, datetime.timezone.utc)
    year, month = date.year + (date.month == 12), date.month % 12 + 1
    return calendar.timegm((year, month, 1, 0, 0, 0)) * 1000


def range_expected(points, start, end, step):
    result = []
    timestamps = [timestamp for timestamp, _ in points]
    for timestamp in range(start, end + 1, step):
        index = bisect.bisect_right(timestamps, timestamp) - 1
        if index >= 0 and points[index][0] > timestamp - step:
            result.append((timestamp, points[index][1]))
    return result


@functools.lru_cache(maxsize=256)
def month_for_day(day):
    return datetime.datetime.fromtimestamp(day * DAY / 1000, datetime.timezone.utc).strftime("%Y_%m")


def physical_rows_by_month(data, multiplier):
    result = {}
    for points in data.values():
        for timestamp, _ in points:
            month = month_for_day(timestamp // DAY)
            result[month] = result.get(month, 0) + multiplier
    return result


def verify_part_generations(stage, previous_stage, servers, summary, changed):
    for server in servers:
        before = {part["partition"]: part["path"] for part in json.loads((server.root / (previous_stage + "-parts.json")).read_text())}
        after = {part["partition"]: part["path"] for part in json.loads((server.root / (stage + "-parts.json")).read_text())}
        assert before.keys() == after.keys(), (server.name, stage, "partition 集合改变", before, after)
        assert all((before[month] != after[month]) == changed for month in before), (
            server.name, stage, "part 路径未满足重写或重启要求", before, after)
        summary["checks"].append({"check": stage + ":" + server.name + ":part-generations",
                                  "changed": changed, "before": before, "after": after})


def verify_stage(stage, servers, inputs, base, active_days, root, summary):
    started = time.monotonic()
    end = base + active_days * DAY - 1
    for points in inputs.values():
        points.sort()
    expected_raw = filter_points(inputs, base, end)
    baseline, name = servers[0].query_many(stage, "full", METRIC, base, end)
    summary["checks"].append(assert_maps(baseline, expected_raw, "original:" + name, servers[0].root / (name + "-expected.json")))
    aggregate = {resolution: aggregate_map(baseline, duration) for resolution, duration in RESOLUTIONS.items()}
    # metadata 按物理单值 Block 行计数；分辨率之间不能重复读取相同贡献。
    physical_counts = {}
    for server in servers:
        parts = json.loads((server.root / (stage + "-parts.json")).read_text())
        expected_rows = sum(len(points) for points in baseline.values())
        if server.downsampling:
            expected_rows = sum(len(points) * len(FIELDS) for data in aggregate.values() for points in data.values())
        expected_months = physical_rows_by_month(baseline, 1)
        if server.downsampling:
            expected_months = {}
            for data in aggregate.values():
                for month, rows in physical_rows_by_month(data, len(FIELDS)).items():
                    expected_months[month] = expected_months.get(month, 0) + rows
        got_months = {part["partition"]: part["metadata"]["RowsCount"] for part in parts}
        assert got_months == expected_months, (stage, server.name, "partition 集合及物理行数", got_months, expected_months)
        got_rows = sum(part["metadata"]["RowsCount"] for part in parts)
        physical_counts[server.name] = got_rows
        assert got_rows == expected_rows, (stage, server.name, "物理 RowsCount", got_rows, expected_rows)
        summary["checks"].append({"check": stage + ":" + server.name + ":physical-rows", "rows": got_rows,
                                  "partitions": len(parts), "rows_by_partition": got_months,
                                  "blocks": sum(part["metadata"]["BlocksCount"] for part in parts)})
    all_ids = sorted(dict(labels)["series_id"] for labels in inputs)
    subset = {all_ids[index] for index in (0, 3, len(all_ids) // 7, len(all_ids) // 2, len(all_ids) - 1)}
    subset_selector = METRIC + '{series_id=~"' + "|".join(sorted(subset)) + '"}'
    middle = base + (active_days // 2) * DAY + 42 * 60_000 + 123
    boundary = next_month(base)
    # 跨月窗口位于已写入区间内；可配置短跨度时仍保留明确的时间裁剪测试。
    cross_start = max(base, boundary - 90 * 60_000)
    cross_end = min(end, boundary + 90 * 60_000)
    cases = [("full", METRIC, base, end, None),
             ("subset", subset_selector, base, end, subset),
             ("empty", METRIC + '{series_id="absent"}', base, end, set()),
             ("clipped", METRIC, middle, min(end, middle + 137 * 60_000 + 456), None)]
    if cross_start <= cross_end:
        cases.append(("cross-month", METRIC, cross_start, cross_end, None))
    for case, selector, start, finish, selected in cases:
        if case != "full":
            actual, name = servers[0].query_many(stage, case, selector, start, finish)
            expected = filter_points(baseline, start, finish, selected)
            summary["checks"].append(assert_maps(actual, expected, "original:" + name, servers[0].root / (name + "-expected.json")))
        for resolution in RESOLUTIONS:
            for field_index, field in enumerate(FIELDS):
                actual, name = servers[1].query_many(stage, case, selector, start, finish, resolution, field)
                expected = filter_points(aggregate[resolution], start, finish, selected, field_index)
                summary["checks"].append(assert_maps(actual, expected, "candidate:" + name, servers[1].root / (name + "-expected.json")))
        print(stage + ": " + case + " 的全部标签及十种分辨率/字段查询一致", flush=True)
        write_json(root / "summary.json", summary)
    # 裸线 query_range 使用独立的标准回看窗口参考值，在固定格子末点网格上求值。
    dense_id = all_ids[0]
    dense_labels = next(labels for labels in baseline if dict(labels)["series_id"] == dense_id)
    selector = METRIC + '{series_id="' + dense_id + '"}'
    for resolution, duration in RESOLUTIONS.items():
        start = end - 3 * 3_600_000 + duration
        actual, name = servers[0].query_many(stage, "bare-line", selector, start, end, resolution, range_query=True)
        expected = {dense_labels: range_expected(baseline[dense_labels], start, end, duration)}
        summary["checks"].append(assert_maps(actual, expected, "original:" + name, servers[0].root / (name + "-expected.json")))
        for field_index, field in enumerate(FIELDS):
            actual, name = servers[1].query_many(stage, "bare-line", selector, start, end, resolution, field, True)
            selected_points = [(timestamp, values[field_index]) for timestamp, values in aggregate[resolution][dense_labels]]
            expected = {dense_labels: range_expected(selected_points, start, end, duration)}
            summary["checks"].append(assert_maps(actual, expected, "candidate:" + name, servers[1].root / (name + "-expected.json")))
    # 全量多线 range 查询覆盖完整历史；子集查询使用末 48h，空窗口不返回空 series。
    for case, range_selector, selected in (("multi-range", METRIC, None), ("subset-range", subset_selector, subset)):
        for resolution, duration in RESOLUTIONS.items():
            start = base + duration - 1 if case == "multi-range" else max(base, end - 2 * DAY + duration)
            expected = {}
            for metric_labels, points in baseline.items():
                if selected is not None and dict(metric_labels)["series_id"] not in selected:
                    continue
                values = range_expected(points, start, end, duration)
                if values:
                    expected[metric_labels] = values
            actual, name = servers[0].query_many(stage, case, range_selector, start, end, resolution, range_query=True)
            summary["checks"].append(assert_maps(actual, expected, "original:" + name, servers[0].root / (name + "-expected.json")))
            for field_index, field in enumerate(FIELDS):
                expected = {}
                for metric_labels, points in aggregate[resolution].items():
                    if selected is not None and dict(metric_labels)["series_id"] not in selected:
                        continue
                    values = range_expected([(timestamp, features[field_index]) for timestamp, features in points], start, end, duration)
                    if values:
                        expected[metric_labels] = values
                actual, name = servers[1].query_many(stage, case, range_selector, start, end, resolution, field, True)
                summary["checks"].append(assert_maps(actual, expected, "candidate:" + name, servers[1].root / (name + "-expected.json")))
        print(stage + ": " + case + " 的全部标签及十种分辨率/字段查询一致", flush=True)
    summary["stages"].append({"name": stage, "days": active_days, "raw_series": len(baseline),
                              "raw_rows": sum(len(points) for points in baseline.values()),
                              "input_rows": sum(len(points) for points in inputs.values()),
                              "physical_rows": physical_counts,
                              "logical_rows": {resolution: sum(len(points) for points in data.values())
                                               for resolution, data in aggregate.items()},
                              "labels": [dict(labels) for labels in sorted(baseline)],
                              "cases": [case[0] for case in cases] + ["bare-line", "multi-range", "subset-range"],
                              "elapsed_seconds": round(time.monotonic() - started, 3)})
    write_json(root / "summary.json", summary)
    print(stage + ": 全部验证通过", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original", type=pathlib.Path, required=True)
    parser.add_argument("--candidate", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--days", type=int, default=93)
    parser.add_argument("--series", type=int, default=160)
    parser.add_argument("--dense-series", type=int, default=4)
    parser.add_argument("--base-ms", type=int)
    args = parser.parse_args()
    assert args.days >= 3 and args.series >= 8 and 1 <= args.dense_series <= args.series
    args.output.mkdir(parents=True, exist_ok=False)
    base = args.base_ms if args.base_ms is not None else (int(time.time() * 1000) // DAY - args.days) * DAY
    assert base % DAY == 0, "起始时间必须按 UTC 日边界对齐"
    labels = make_labels(args.series, args.dense_series)
    inputs = {labels_key(metric_labels): [] for metric_labels in labels}
    write_json(args.output / "fixture-config.json", {"base_ms": base, "days": args.days, "series": args.series,
                                                     "dense_series": args.dense_series, "labels": labels,
                                                     "meaning": "模拟历史数据跨度，不是连续运行指定天数"})
    summary = {"status": "running", "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "base_ms": base, "days": args.days, "series": args.series, "dense_series": args.dense_series,
               "checks": [], "stages": [], "batches": [], "binaries": {},
               "tolerance": {"relative": 1e-10, "absolute": 1e-9},
               "scope": "模拟历史跨度；每周写入及归并，三阶段递增区间、整个历史迟到追加、summary rewrite、正常重启"}
    servers = []
    try:
        for name, binary, downsampling in (("original", args.original, False), ("candidate", args.candidate, True)):
            summary["binaries"][name] = {"path": str(binary.resolve()), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
            server = MultiServer(name, binary, args.output, downsampling)
            servers.append(server)
            server.start()
        milestones = sorted({args.days // 3, args.days * 2 // 3, args.days})
        cursor = 0
        with (args.output / "input.ndjson").open("wb") as fixture_file:
            for milestone in milestones:
                while cursor < milestone:
                    finish = min(cursor + 7, milestone)
                    batch = "days-" + str(cursor + 1) + "-" + str(finish)
                    rows = ingest_interval(servers, labels, inputs, fixture_file, base, cursor, finish, args.days)
                    for server in servers:
                        server.force_merge(batch)
                    summary["batches"].append({"name": batch, "rows": rows})
                    write_json(args.output / "summary.json", summary)
                    print(batch + ": 写入并归并 " + str(rows) + " 行", flush=True)
                    cursor = finish
                stage = "day" + str(milestone)
                for server in servers:
                    server.part_metadata(stage)
                verify_stage(stage, servers, inputs, base, milestone, args.output, summary)
            rows = ingest_interval(servers, labels, inputs, fixture_file, base, 0, args.days, args.days, late=True)
            summary["batches"].append({"name": "late-history", "rows": rows})
        for server in servers:
            server.force_merge("late-history")
        verify_stage("late-history", servers, inputs, base, args.days, args.output, summary)
        for server in servers:
            server.force_merge("rewrite")
        verify_part_generations("rewrite", "late-history", servers, summary, changed=True)
        verify_stage("rewrite", servers, inputs, base, args.days, args.output, summary)
        for server in servers:
            server.stop()
            server.start()
            server.part_metadata("restart")
        verify_part_generations("restart", "rewrite", servers, summary, changed=False)
        verify_stage("restart", servers, inputs, base, args.days, args.output, summary)
        summary["status"] = "passed"
    except Exception as error:
        summary["status"] = "failed"
        summary["error"] = repr(error)
        raise
    finally:
        cleanup_errors = []
        for server in servers:
            try:
                server.stop()
            except Exception as error:
                cleanup_errors.append(repr(error))
        if cleanup_errors:
            summary["status"] = "failed"
            summary["cleanup_errors"] = cleanup_errors
        summary["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        summary["checks_count"] = len(summary["checks"])
        summary["max_absolute_error"] = max((check.get("max_absolute_error", 0) for check in summary["checks"]), default=0)
        write_json(args.output / "summary.json", summary)
    assert summary["status"] == "passed", summary
    print(json.dumps({"status": summary["status"], "checks": summary["checks_count"], "output": str(args.output)}))


if __name__ == "__main__":
    main()
