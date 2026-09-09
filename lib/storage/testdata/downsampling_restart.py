#!/usr/bin/env python3
"""验证降采样数据在集群三组件正常重启前后的查询结果严格一致。"""

import argparse
import datetime
import hashlib
import json
import math
import pathlib
import time

from downsampling_compare import (
    CONTROL_METRIC, FIELDS, INPUT_STEPS_MS, METRIC, RESOLUTIONS, Server,
    aggregate, assert_samples, binary_manifest, fixture, range_expected, write_json,
)


SELECTOR = '{__name__=~"downsampling_compare_(value|control)"}'


def normalize_response(response):
    """仅规范化时间线顺序，保留标签、时间戳及原始数值字符串。"""
    assert response.get("status") == "success", response
    assert not response.get("isPartial"), response
    data = response["data"]
    assert data["resultType"] == "matrix" and not data.get("isPartial"), response
    assert data["result"], "查询结果为空，不能作为持久化一致性的证据"
    result = []
    seen = set()
    for series in data["result"]:
        labels = series["metric"]
        assert labels and all(isinstance(key, str) and isinstance(value, str)
                              for key, value in labels.items()), series
        key = tuple(sorted(labels.items()))
        assert key not in seen, ("响应包含重复时间线", labels)
        seen.add(key)
        points = series["values"]
        assert points, ("时间线无样本", labels)
        previous = None
        for timestamp, value in points:
            assert type(timestamp) in (int, float) and math.isfinite(timestamp), (labels, timestamp)
            assert previous is None or timestamp > previous, ("时间戳未严格递增", labels, points)
            assert isinstance(value, str) and math.isfinite(float(value)), (labels, value)
            previous = timestamp
        result.append({"metric": dict(key), "values": points})
    return sorted(result, key=lambda series: tuple(series["metric"].items()))


def snapshot_digest(snapshot):
    return hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def assert_unchanged(before, after, description):
    """直接比较查询数据，不转换浮点数，不使用数值容差。"""
    assert len(before) == len(after), (description, "时间线数量变化", len(before), len(after))
    assert before, (description, "查询快照为空")
    for left, right in zip(before, after):
        assert left["metric"] == right["metric"], (description, "标签变化", left["metric"], right["metric"])
        assert left["values"] == right["values"], (description, left["metric"], "样本变化", left["values"], right["values"])
    return {"check": description, "exact_equal": True, "series": len(before),
            "rows": sum(len(series["values"]) for series in before),
            "before_sha256": snapshot_digest(before), "after_sha256": snapshot_digest(after)}


def capture(server, stage, samples, base, summary):
    end = base + 3 * 3_600_000 - 1
    snapshots = {}
    for resolution, milliseconds in RESOLUTIONS.items():
        expected = {metric: aggregate(samples, metric, milliseconds) for metric in (METRIC, CONTROL_METRIC)}
        for field in FIELDS:
            for query_type in ("matrix", "range"):
                params = {"nocache": "1", "query.field": resolution + ":" + field}
                start = base + milliseconds - 1
                if query_type == "matrix":
                    path = "/api/v1/query"
                    params.update(query=SELECTOR + "[" + str(end - base + 1) + "ms]", time=end / 1000)
                else:
                    path = "/api/v1/query_range"
                    params.update(query=SELECTOR, start=start / 1000, end=end / 1000,
                                  step=resolution, max_lookback=resolution)
                key = resolution + ":" + field + ":" + query_type
                server.cluster.assert_running()
                response = json.loads(server.request(path, params))
                write_json(server.root / (stage + "-" + key.replace(":", "-") + ".json"),
                           {"url": server.url(path), "params": params, "response": response})
                actual = normalize_response(response)
                assert [series["metric"] for series in actual] == [
                    {"__name__": metric} for metric in sorted(expected)], (stage, key, actual)
                for series in actual:
                    metric = series["metric"]["__name__"]
                    want = [(row["timestamp"], row[field]) for row in expected[metric]]
                    if query_type == "range":
                        want = range_expected(want, start, end, milliseconds)
                    points = [(round(timestamp * 1000), float(value)) for timestamp, value in series["values"]]
                    summary["checks"].append(assert_samples(points, want, stage + ":oracle:" + key + ":" + metric))
                snapshots[key] = actual
    server.cluster.assert_running()
    write_json(server.root / (stage + "-snapshot.json"), snapshots)
    return snapshots


def run(server, phases, base, summary):
    samples = [row for phase in phases for row in phase]
    server.start()
    for index, batch in enumerate(phases, 1):
        server.ingest(batch)
        server.force_merge("batch-" + str(index))
    before_parts = server.part_metadata("before-restart")
    expected_rows = sum(len(aggregate(samples, metric, resolution)) * len(FIELDS)
                        for metric in (METRIC, CONTROL_METRIC) for resolution in RESOLUTIONS.values())
    assert sum(part["metadata"]["RowsCount"] for part in before_parts) == expected_rows, before_parts
    summary["checks"].append({"check": "downsampling-physical-rows", "rows": expected_rows})
    before = capture(server, "before-restart", samples, base, summary)
    processes = dict(server.cluster.processes)
    before_processes = {name: process.pid for name, process in processes.items()}
    commands = {name: list(command) for name, command in server.cluster.commands.items()}
    write_json(server.root / "before-restart-processes.json", {"pids": before_processes, "commands": commands})
    server.stop()
    assert all(process.poll() == 0 for process in processes.values()), "旧集群组件未全部正常退出"

    # 沿用原数据目录和命令，重启后直接查询；禁止重写输入或再次强制归并。
    server.start()
    after_processes = {name: process.pid for name, process in server.cluster.processes.items()}
    assert set(before_processes) == set(after_processes) == {"vmstorage", "vminsert", "vmselect"}
    assert all(before_processes[name] != after_processes[name] for name in before_processes), "组件未重启"
    assert commands == server.cluster.commands, "重启前后启动参数变化"
    process_evidence = {"before_pids": before_processes, "after_pids": after_processes,
                        "old_exit_codes": {name: process.poll() for name, process in processes.items()},
                        "commands": commands, "storage": str(server.storage.resolve())}
    write_json(server.root / "restart-processes.json", process_evidence)
    summary["checks"].append({"check": "three-components-restarted", **process_evidence})
    after_parts = server.part_metadata("after-restart")
    assert before_parts == after_parts, ("重启前后活动 part 变化", before_parts, after_parts)
    summary["checks"].append({"check": "active-parts-unchanged", "parts": len(after_parts)})
    after = capture(server, "after-restart", samples, base, summary)
    assert before.keys() == after.keys(), "重启前后的字段查询集合变化"
    for key in before:
        summary["checks"].append(assert_unchanged(before[key], after[key], "restart-exact:" + key))
    summary["exact_comparisons"] = len(before)
    summary["exact_compared_points"] = sum(len(series["values"]) for snapshot in before.values() for series in snapshot)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=pathlib.Path, required=True, help="当前分支三个集群组件所在目录")
    parser.add_argument("--output", type=pathlib.Path, required=True, help="尚不存在的产物目录")
    parser.add_argument("--tenant", default="11:17", help="集群租户 account:project")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    # 使用 UTC 昨日零时，保证三小时输入处于过去且只包含一个 partition。
    base = (int(time.time() * 1000) // 86_400_000 - 1) * 86_400_000
    phases = fixture(base)
    write_json(args.output / "fixture.json", {"base_ms": base, "input_steps_ms": INPUT_STEPS_MS, "phases": phases})
    summary = {"status": "running", "mode": "cluster", "tenant": args.tenant, "base_ms": base,
               "input_rows": sum(map(len, phases)), "input_steps_ms": INPUT_STEPS_MS,
               "fields": [resolution + ":" + field for resolution in RESOLUTIONS for field in FIELDS],
               "restart_comparison": "标签、时间戳、数值字符串严格相等；仅规范化时间线顺序",
               "oracle_tolerance": {"relative": 1e-10, "absolute": 1e-9}, "checks": [],
               "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    server = None
    try:
        summary["binary"] = binary_manifest(args.candidate, "cluster")
        server = Server("candidate", args.candidate, args.output, True, mode="cluster", tenant=args.tenant)
        run(server, phases, base, summary)
        summary["status"] = "passed"
    except BaseException as error:
        summary.update(status="failed", error=repr(error))
        raise
    finally:
        try:
            if server is not None:
                server.stop()
        except BaseException as error:
            summary.update(status="failed", cleanup_error=repr(error))
            raise
        finally:
            summary["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            summary["checks_count"] = len(summary["checks"])
            summary["max_absolute_error"] = max((check.get("max_absolute_error", 0) for check in summary["checks"]), default=0)
            write_json(args.output / "summary.json", summary)
    print(json.dumps({"status": summary["status"], "checks": summary["checks_count"],
                      "exact_comparisons": summary["exact_comparisons"], "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
