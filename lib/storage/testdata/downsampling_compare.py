#!/usr/bin/env python3
"""使用独立数据目录，对照原始存储与降采样存储的 HTTP 查询结果。"""

import argparse
import datetime
import hashlib
import json
import math
import pathlib
import re
import signal
import socket
import subprocess
import time
import urllib.parse
import urllib.request


RESOLUTIONS = {"5m": 300_000, "1h": 3_600_000}
FIELDS = ("last", "sum", "count", "min", "max")
METRIC = "downsampling_compare_value"
DUPLICATE_METRIC = "downsampling_compare_duplicate"


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(base, path, params=None, data=None):
    url = base + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode()


def metric_value(metrics, name):
    match = re.search(r"^" + re.escape(name) + r" ([^\n]+)$", metrics, re.MULTILINE)
    if not match:
        raise AssertionError("未找到指标：" + name)
    return float(match.group(1))


class Server:
    def __init__(self, name, binary, root, downsampling):
        self.name = name
        self.root = root / name
        self.root.mkdir()
        self.storage = self.root / "data"
        self.port = unused_port()
        self.base = "http://127.0.0.1:" + str(self.port)
        self.command = [
            str(binary.resolve()),
            "-httpListenAddr=127.0.0.1:" + str(self.port),
            "-storageDataPath=" + str(self.storage),
            "-retentionPeriod=12",
            "-inmemoryDataFlushInterval=1s",
            "-dedup.minScrapeInterval=0",
            "-memory.allowedBytes=256MiB",
            "-storage.minFreeDiskSpaceBytes=1MB",
            "-search.disableCache=true",
            "-search.latencyOffset=0s",
            "-loggerLevel=INFO",
        ]
        if downsampling:
            self.command.append("-storage.downsampling.enabled=true")
        self.downsampling = downsampling
        self.process = None
        self.log = None
        write_json(self.root / "command.json", self.command)

    def start(self):
        self.log = (self.root / "server.log").open("ab")
        self.process = subprocess.Popen(self.command, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(self.name + " 启动失败；参见 server.log")
            try:
                request(self.base, "/health")
                return
            except (OSError, TimeoutError):
                time.sleep(0.1)
        raise AssertionError(self.name + " 启动超时")

    def stop(self):
        if self.process is not None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise AssertionError(self.name + " 正常关闭超时")
            finally:
                self.process = None
                self.log.close()
                self.log = None

    def ingest(self, samples):
        lines = []
        for metric in (METRIC, DUPLICATE_METRIC):
            selected = [row for row in samples if row["metric"] == metric]
            lines.append(json.dumps({
                "metric": {"__name__": metric},
                "timestamps": [row["timestamp"] for row in selected],
                "values": [row["value"] for row in selected],
            }))
        request(self.base, "/api/v1/import", data=("\n".join(lines) + "\n").encode())

    def force_merge(self, stage):
        request(self.base, "/internal/force_flush")
        deadline = time.monotonic() + 45
        # 每次都调用强制归并；单一已降采样 part 也必须支持再次归并。
        request(self.base, "/internal/force_merge")
        time.sleep(0.2)
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(self.name + " 在归并过程中退出")
            metrics = request(self.base, "/metrics")
            active = metric_value(metrics, "vm_active_force_merges")
            inmemory = metric_value(metrics, 'vm_rows{type="storage/inmemory"}')
            parts = sum(metric_value(metrics, 'vm_parts{type="storage/' + kind + '"}')
                        for kind in ("small", "big"))
            merges = sum(metric_value(metrics, 'vm_active_merges{type="storage/' + kind + '"}')
                         for kind in ("inmemory", "small", "big"))
            if active == 0 and inmemory == 0 and merges == 0 and parts == 1:
                (self.root / (stage + "-metrics.txt")).write_text(metrics)
                return self.part_metadata(stage)
            if active == 0 and merges == 0:
                request(self.base, "/internal/force_merge")
            time.sleep(0.2)
        raise AssertionError(self.name + " 未能归并为一个 file part")

    def part_metadata(self, stage):
        parts = []
        for manifest_path in sorted((self.storage / "data" / "small").glob("*/parts.json")):
            manifest = json.loads(manifest_path.read_text())
            for kind in ("Small", "Big"):
                for name in manifest.get(kind) or []:
                    path = self.storage / "data" / kind.lower() / manifest_path.parent.name / name
                    metadata = json.loads((path / "metadata.json").read_text())
                    if self.downsampling:
                        assert metadata.get("Mode") == "downsampling", metadata
                        assert metadata.get("FormatVersion") == 3, metadata
                    else:
                        assert metadata.get("Mode") != "downsampling", metadata
                    parts.append({"path": str(path), "metadata": metadata,
                                  "files": {item.name: item.stat().st_size for item in path.iterdir()
                                            if item.is_file()}})
        assert len(parts) == 1, parts
        write_json(self.root / (stage + "-parts.json"), parts)
        return parts

    def query(self, stage, metric, start, end, resolution=None, field=None, range_query=False):
        params = {"nocache": "1"}
        if range_query:
            params.update({"query": metric, "start": start / 1000, "end": end / 1000,
                           "step": resolution, "max_lookback": resolution})
            path = "/api/v1/query_range"
            suffix = "range"
        else:
            # matrix selector 的左边界不包含在结果中，因此扩展 1 ms。
            params.update({"query": metric + "[" + str(end - start + 1) + "ms]", "time": end / 1000})
            path = "/api/v1/query"
            suffix = "matrix"
        if field is not None:
            params["query.field"] = resolution + ":" + field
        response = json.loads(request(self.base, path, params))
        name = "-".join(filter(None, (stage, metric, resolution, field, suffix)))
        write_json(self.root / (name + ".json"), {"path": path, "params": params, "response": response})
        assert response.get("status") == "success", response
        assert response["data"]["resultType"] == "matrix", response
        results = response["data"]["result"]
        assert len(results) == 1, response
        assert results[0]["metric"] == {"__name__": metric}, response
        return [(round(float(timestamp) * 1000), float(value)) for timestamp, value in results[0]["values"]]


def fixture(base):
    phases = []
    for phase, offsets in enumerate(((0, 60_000, 299_999), (1, 120_000), (180_000, 240_000))):
        samples = []
        for bucket in range(36):
            for position, offset in enumerate(offsets):
                samples.append({"metric": METRIC, "timestamp": base + bucket * 300_000 + offset,
                                "value": (bucket - 17) * 1.25 + phase * 0.5 + position * 0.125})
        for bucket in (0, 11, 12, 23, 24, 35):
            values = ((-8.0, 7.0), (12.0, -100.0), (3.0, 12.0))[phase]
            for value in values:
                samples.append({"metric": DUPLICATE_METRIC,
                                "timestamp": base + bucket * 300_000 + 299_999, "value": value})
        # 倒序写入，覆盖非时间顺序输入与历史数据追加。
        phases.append(list(reversed(samples)))
    return phases


def aggregate(samples, metric, resolution):
    groups = {}
    for sample in samples:
        if sample["metric"] == metric:
            groups.setdefault(sample["timestamp"] // resolution, []).append(sample)
    result = []
    for bucket in sorted(groups):
        group = groups[bucket]
        last = max(group, key=lambda sample: (sample["timestamp"], sample["value"]))
        values = [sample["value"] for sample in group]
        result.append({"timestamp": last["timestamp"], "last": last["value"],
                       "sum": math.fsum(values), "count": float(len(values)),
                       "min": min(values), "max": max(values)})
    return result


def assert_samples(actual, expected, description):
    assert len(actual) == len(expected), (description, "样本数", len(actual), len(expected))
    max_error = 0.0
    for got, want in zip(actual, expected):
        assert got[0] == want[0], (description, "时间戳", got, want)
        assert math.isclose(got[1], want[1], rel_tol=1e-10, abs_tol=1e-9), (description, "数值", got, want)
        max_error = max(max_error, abs(got[1] - want[1]))
    return {"check": description, "rows": len(actual), "max_absolute_error": max_error}


def verify_stage(stage, servers, samples, base, summary):
    end = base + 3 * 3_600_000 - 1
    original = servers[0]
    # 原版记录所有输入行；新格式每个逻辑摘要对应五个单值 Block 行。
    for server in servers:
        parts = json.loads((server.root / (stage + "-parts.json")).read_text())
        physical_rows = sum(part["metadata"]["RowsCount"] for part in parts)
        expected_rows = len(samples)
        if server.downsampling:
            expected_rows = sum(len(aggregate(samples, metric, resolution)) * len(FIELDS)
                                for metric in (METRIC, DUPLICATE_METRIC)
                                for resolution in RESOLUTIONS.values())
        assert physical_rows == expected_rows, (stage, server.name, "metadata.RowsCount", physical_rows, expected_rows)
        summary["checks"].append({"check": stage + ":" + server.name + ":metadata-physical-rows",
                                  "rows": physical_rows})
    expected_raw = sorted((row["timestamp"], row["value"]) for row in samples if row["metric"] == METRIC)
    raw = original.query(stage, METRIC, base, end)
    summary["checks"].append(assert_samples(raw, expected_raw, stage + ":original:raw-matrix"))
    # 原版对重复时间戳的查询可能做 dedup；该响应保留为证据，不作为 count 和 sum 的输入。
    original.query(stage, DUPLICATE_METRIC, base, end)
    for resolution, milliseconds in RESOLUTIONS.items():
        expected = aggregate(samples, METRIC, milliseconds)
        start = base + milliseconds - 1
        baseline = original.query(stage, METRIC, start, end, resolution, range_query=True)
        summary["checks"].append(assert_samples(
            baseline, [(row["timestamp"], row["last"]) for row in expected],
            stage + ":original:" + resolution + ":bare-range"))
    if len(servers) == 1:
        print(stage + ": 原始查询与写入样本一致", flush=True)
        return
    candidate = servers[1]
    # 核心序列的参考值也从原版裸查询返回值独立聚合，避免仅验证本脚本的写入预期。
    baseline_samples = [{"metric": METRIC, "timestamp": timestamp, "value": value} for timestamp, value in raw]
    for metric in (METRIC, DUPLICATE_METRIC):
        reference = baseline_samples if metric == METRIC else samples
        for resolution, milliseconds in RESOLUTIONS.items():
            expected = aggregate(reference, metric, milliseconds)
            write_json(candidate.root / (stage + "-" + metric + "-" + resolution + "-expected.json"), expected)
            for field in FIELDS:
                actual = candidate.query(stage, metric, base, end, resolution, field)
                want = [(row["timestamp"], row[field]) for row in expected]
                summary["checks"].append(assert_samples(
                    actual, want, stage + ":candidate:" + metric + ":" + resolution + ":" + field + ":matrix"))
                if metric == METRIC:
                    actual_range = candidate.query(stage, metric, base + milliseconds - 1, end,
                                                   resolution, field, range_query=True)
                    summary["checks"].append(assert_samples(
                        actual_range, want, stage + ":candidate:" + resolution + ":" + field + ":bare-range"))
    print(stage + ": 查询与独立参考聚合结果一致", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original", type=pathlib.Path, required=True, help="开源 v1.151.0 binary")
    parser.add_argument("--candidate", type=pathlib.Path, help="当前分支 binary；省略时仅验证原版与 fixture")
    parser.add_argument("--output", type=pathlib.Path, required=True, help="不存在的新产物目录")
    parser.add_argument("--base-ms", type=int, help="按 1h 对齐且位于 retention 内的起始时间戳")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    base = args.base_ms if args.base_ms is not None else (int(time.time() * 1000) // 3_600_000 - 24) * 3_600_000
    assert base % 3_600_000 == 0, "base-ms 必须按 1h 对齐"
    phases = fixture(base)
    write_json(args.output / "fixture.json", {"base_ms": base, "phases": phases})
    summary = {"status": "running", "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "base_ms": base, "query_field_format": "5m:last", "checks": [], "stages": [],
               "tolerance": {"relative": 1e-10, "absolute": 1e-9}, "binaries": {},
               "scope": "单节点 HTTP 裸查询；36 个 5m 格子、3 个 1h 格子、三轮写入与归并、重复归并、正常重启"}
    servers = []
    try:
        for name, binary, downsampling in (("original", args.original, False), ("candidate", args.candidate, True)):
            if binary is None:
                continue
            summary["binaries"][name] = {"path": str(binary.resolve()),
                                        "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
            server = Server(name, binary, args.output, downsampling)
            servers.append(server)
            server.start()
        samples = []
        for number, batch in enumerate(phases, 1):
            stage = "phase" + str(number)
            samples.extend(batch)
            for server in servers:
                server.ingest(batch)
                server.force_merge(stage)
            verify_stage(stage, servers, samples, base, summary)
            summary["stages"].append(stage)
            write_json(args.output / "summary.json", summary)
        for server in servers:
            server.force_merge("rewrite")
        verify_stage("rewrite", servers, samples, base, summary)
        summary["stages"].append("rewrite")
        write_json(args.output / "summary.json", summary)
        for server in servers:
            server.stop()
            server.start()
            server.part_metadata("restart")
        verify_stage("restart", servers, samples, base, summary)
        summary["stages"].append("restart")
        summary["status"] = "passed"
    except Exception as error:
        summary["status"] = "failed"
        summary["error"] = repr(error)
        raise
    finally:
        for server in servers:
            server.stop()
        summary["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        summary["checks_count"] = len(summary["checks"])
        write_json(args.output / "summary.json", summary)
    print(json.dumps({"status": summary["status"], "checks": summary["checks_count"],
                      "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
