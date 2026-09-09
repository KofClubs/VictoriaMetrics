#!/usr/bin/env python3
"""使用独立数据目录，对照原始存储与降采样存储的 HTTP 查询结果。"""

import argparse
import bisect
import datetime
import hashlib
import json
import math
import pathlib
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request

sys.dont_write_bytecode = True


RESOLUTIONS = {"5m": 300_000, "1h": 3_600_000}
FIELDS = ("last", "sum", "count", "min", "max")
METRIC = "downsampling_compare_value"
CONTROL_METRIC = "downsampling_compare_control"
INPUT_STEPS_MS = (14_000, 15_000, 16_000)


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


def binary_manifest(binary, mode):
    if mode == "cluster":
        return {"path": str(binary.resolve()), "executables": {
            name: hashlib.sha256((binary / name).read_bytes()).hexdigest()
            for name in ("vminsert", "vmstorage", "vmselect")}}
    return {"path": str(binary.resolve()), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}


class Server:
    def __init__(self, name, binary, root, downsampling, mode="single", tenant="0:0"):
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
        self.cluster = None
        if mode == "cluster":
            from downsampling_cluster import ClusterRuntime
            self.cluster = ClusterRuntime(binary, self.root, self.storage, downsampling, tenant, unused_port)
            self.base = self.cluster.http["vmstorage"]
        write_json(self.root / "command.json", self.cluster.commands if self.cluster is not None else self.command)

    def url(self, path):
        return self.cluster.url(path) if self.cluster is not None else self.base + path

    def request(self, path, params=None, data=None):
        if self.cluster is not None:
            return self.cluster.request(path, params, data)
        return request(self.base, path, params, data)

    def wait_ingested(self):
        if self.cluster is not None:
            self.cluster.wait_ingested()

    def request_force_merge(self):
        # force_merge 异步执行，必须等待本次请求完成，不能仅依赖空闲指标。
        log_path = self.root / ("vmstorage.log" if self.cluster is not None else "server.log")
        offset = log_path.stat().st_size
        self.request("/internal/force_merge")
        deadline = time.monotonic() + 120
        pending = b""
        while time.monotonic() < deadline:
            assert self.process.poll() is None, self.name + " 在强制归并过程中退出"
            with log_path.open("rb") as source:
                source.seek(offset)
                pending += source.read()
                offset = source.tell()
            if b'error in forced merge for partition_prefix=""' in pending:
                raise AssertionError((self.name, "强制归并失败", pending.decode(errors="replace")))
            if b'forced merge for partition_prefix="" has been successfully finished' in pending:
                # appmetrics 的 /metrics 响应缓存 1 秒，等待过期后再读取终态统计。
                time.sleep(1.05)
                return
            time.sleep(0.05)
        raise AssertionError(self.name + " 未观察到本次强制归并完成")

    def start(self):
        if self.cluster is not None:
            self.cluster.start()
            self.process = self.cluster.processes["vmstorage"]
            return
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
        if self.cluster is not None:
            self.cluster.stop()
            self.process = None
            return
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
        for metric in sorted({row["metric"] for row in samples}):
            selected = [row for row in samples if row["metric"] == metric]
            lines.append(json.dumps({
                "metric": {"__name__": metric},
                "timestamps": [row["timestamp"] for row in selected],
                "values": [row["value"] for row in selected],
            }))
        self.request("/api/v1/import", data=("\n".join(lines) + "\n").encode())

    def force_merge(self, stage):
        self.wait_ingested()
        request(self.base, "/internal/force_flush")
        deadline = time.monotonic() + 45
        # 每次都调用强制归并；单一已降采样 part 也必须支持再次归并。
        self.request_force_merge()
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
                self.request_force_merge()
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
                        assert metadata.get("FormatVersion") == 2, metadata
                        assert metadata.get("SemanticsVersion") == 2, metadata
                        assert metadata.get("NumericCodec") == "decimal-values", metadata
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
        response = json.loads(self.request(path, params))
        name = "-".join(filter(None, (stage, metric, resolution, field, suffix)))
        write_json(self.root / (name + ".json"), {"path": path, "url": self.url(path), "params": params, "response": response})
        assert response.get("status") == "success", response
        assert response["data"]["resultType"] == "matrix", response
        results = response["data"]["result"]
        assert len(results) == 1, response
        assert results[0]["metric"] == {"__name__": metric}, response
        return [(round(float(timestamp) * 1000), float(value)) for timestamp, value in results[0]["values"]]


def fixture(base):
    """两条完整时间线按 14、15、16 秒循环采样，中间一小时最后追加。"""
    phases = [[], [], []]
    for metric_index, metric in enumerate((METRIC, CONTROL_METRIC)):
        for ordinal, timestamp in enumerate(sample_timestamps(base, base + 3 * 3_600_000)):
            hour = (timestamp - base) // 3_600_000
            phase = (0, 2, 1)[hour]
            value = (ordinal % 97 - 48) * 0.125 + metric_index * 100.25
            phases[phase].append({"metric": metric, "timestamp": timestamp, "value": value})
    for samples in phases:
        # 倒序导入不会增加样本；三批的时间戳集合严格互斥。
        samples.reverse()
    return phases


def sample_timestamps(start, end):
    """生成左闭右开区间内的采样时间戳，不依赖查询网格或降采样结果。"""
    timestamp = start
    ordinal = 0
    while timestamp < end:
        yield timestamp
        timestamp += INPUT_STEPS_MS[ordinal % len(INPUT_STEPS_MS)]
        ordinal += 1


def range_expected(points, start, end, step):
    """在求值网格上返回 (t-step, t] 内的末点；空窗口不返回样本。"""
    assert step > 0, "query_range step 必须大于零"
    assert all(points[index][0] > points[index - 1][0] for index in range(1, len(points))), "参考输入必须按时间严格递增"
    timestamps = [timestamp for timestamp, _ in points]
    result = []
    for timestamp in range(start, end + 1, step):
        index = bisect.bisect_right(timestamps, timestamp) - 1
        if index >= 0 and timestamps[index] > timestamp - step:
            result.append((timestamp, points[index][1]))
    return result


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


def assert_samples(actual, expected, description, exact=False):
    assert len(actual) == len(expected), (description, "样本数", len(actual), len(expected))
    max_error = 0.0
    for got, want in zip(actual, expected):
        assert got[0] == want[0], (description, "时间戳", got, want)
        matches = got[1] == want[1] if exact else math.isclose(got[1], want[1], rel_tol=1e-10, abs_tol=1e-9)
        assert matches, (description, "数值", got, want)
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
                                for metric in (METRIC, CONTROL_METRIC)
                                for resolution in RESOLUTIONS.values())
        assert physical_rows == expected_rows, (stage, server.name, "metadata.RowsCount", physical_rows, expected_rows)
        summary["checks"].append({"check": stage + ":" + server.name + ":metadata-physical-rows",
                                  "rows": physical_rows})
    for metric in (METRIC, CONTROL_METRIC):
        expected_raw = sorted((row["timestamp"], row["value"]) for row in samples if row["metric"] == metric)
        assert len({timestamp for timestamp, _ in expected_raw}) == len(expected_raw), "输入时间戳必须唯一"
        raw = original.query(stage, metric, base, end)
        summary["checks"].append(assert_samples(raw, expected_raw, stage + ":original:" + metric + ":raw-matrix", exact=True))
        # 先逐点验证原版 HTTP 原始结果，再直接以实际输入计算五特征参考值。
        for resolution, milliseconds in RESOLUTIONS.items():
            start = base + milliseconds - 1
            baseline = original.query(stage, metric, start, end, resolution, range_query=True)
            summary["checks"].append(assert_samples(
                baseline, range_expected(expected_raw, start, end, milliseconds),
                stage + ":original:" + metric + ":" + resolution + ":bare-range"))
            if len(servers) == 1:
                continue
            candidate = servers[1]
            expected = aggregate(samples, metric, milliseconds)
            write_json(candidate.root / (stage + "-" + metric + "-" + resolution + "-expected.json"), expected)
            for field in FIELDS:
                actual = candidate.query(stage, metric, base, end, resolution, field)
                want = [(row["timestamp"], row[field]) for row in expected]
                summary["checks"].append(assert_samples(
                    actual, want, stage + ":candidate:" + metric + ":" + resolution + ":" + field + ":matrix"))
                actual_range = candidate.query(stage, metric, start, end, resolution, field, range_query=True)
                summary["checks"].append(assert_samples(
                    actual_range, range_expected(want, start, end, milliseconds),
                    stage + ":candidate:" + metric + ":" + resolution + ":" + field + ":bare-range"))
    print(stage + ": 查询与独立参考聚合结果一致", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original", type=pathlib.Path, required=True, help="开源 v1.151.0-cluster 组件目录；single 模式为 binary")
    parser.add_argument("--candidate", type=pathlib.Path, help="当前分支 binary；省略时仅验证原版与 fixture")
    parser.add_argument("--output", type=pathlib.Path, required=True, help="不存在的新产物目录")
    parser.add_argument("--base-ms", type=int, help="按 1h 对齐且位于 retention 内的起始时间戳")
    parser.add_argument("--mode", choices=("single", "cluster"), default="cluster",
                        help="cluster 模式的 binary 参数必须为含三个集群组件的目录")
    parser.add_argument("--tenant", default="0:0", help="集群租户 account:project")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    base = args.base_ms if args.base_ms is not None else (int(time.time() * 1000) // 3_600_000 - 24) * 3_600_000
    assert base % 3_600_000 == 0, "base-ms 必须按 1h 对齐"
    phases = fixture(base)
    write_json(args.output / "fixture.json", {"base_ms": base, "input_steps_ms": INPUT_STEPS_MS,
                                             "unique_timestamps_per_series": True, "phases": phases})
    summary = {"status": "running", "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "base_ms": base, "query_field_format": "5m:last", "mode": args.mode, "tenant": args.tenant, "checks": [], "stages": [],
               "input_steps_ms": INPUT_STEPS_MS, "input_rows": sum(map(len, phases)),
               "tolerance": {"relative": 1e-10, "absolute": 1e-9}, "binaries": {},
               "scope": ("真实集群 1+1+1" if args.mode == "cluster" else "单节点") +
                        " HTTP 裸查询；36 个 5m 格子、3 个 1h 格子、三轮写入与归并、重复归并、正常重启"}
    servers = []
    try:
        for name, binary, downsampling in (("original", args.original, False), ("candidate", args.candidate, True)):
            if binary is None:
                continue
            summary["binaries"][name] = binary_manifest(binary, args.mode)
            server = Server(name, binary, args.output, downsampling, args.mode, args.tenant)
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
        summary["max_absolute_error"] = max((check.get("max_absolute_error", 0) for check in summary["checks"]), default=0)
        write_json(args.output / "summary.json", summary)
    print(json.dumps({"status": summary["status"], "checks": summary["checks_count"],
                      "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
