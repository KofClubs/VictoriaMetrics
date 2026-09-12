#!/usr/bin/env python3
"""验证集群降采样查询协议与原版组件的兼容边界。"""

import argparse
import json
import pathlib
import sys
import time
import urllib.error

sys.dont_write_bytecode = True

from downsampling_compare import Server, assert_samples, binary_manifest, range_expected, write_json


def assert_downsample_query_rejected(server, path, params, evidence):
    server.cluster.assert_running()
    log_path = server.root / "vmstorage.log"
    log_offset = log_path.stat().st_size
    try:
        result = server.request(path, params)
    except urllib.error.HTTPError as error:
        try:
            response = json.loads(error.read().decode())
        finally:
            error.close()
        server.cluster.assert_running()
        with log_path.open("rb") as log:
            log.seek(log_offset)
            appended = log.read()
            end_offset = log.tell()
        excerpt_path = server.root / (evidence + "-vmstorage.log")
        excerpt_path.write_bytes(appended)
        write_json(server.root / (evidence + ".json"), {
            "url": server.url(path), "params": params,
            "http_status": error.code, "response": response,
            "vmstorage_log": {"path": str(log_path), "start_offset": log_offset,
                              "end_offset": end_offset, "excerpt_path": str(excerpt_path)}})
        assert 400 <= error.code < 600 and response.get("status") == "error", response
        assert "search_downsampling_v2" in response.get("error", ""), response
        assert b'unsupported rpcName: "search_downsampling_v2"' in appended, \
            ("本次请求未产生不支持降采样 RPC 的 vmstorage 日志", str(excerpt_path), response)
        return {"check": server.name + ":" + evidence, "http_status": error.code,
                "unsupported_rpc_log": str(excerpt_path)}
    raise AssertionError(("旧 vmstorage 的降采样查询未明确失败", path, result))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original", type=pathlib.Path, required=True)
    parser.add_argument("--candidate", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    summary = {"status": "running", "checks": [], "binaries": {
        "original": binary_manifest(args.original, "cluster"),
        "candidate": binary_manifest(args.candidate, "cluster")}}
    base = (int(time.time() * 1000) // 3_600_000 - 24) * 3_600_000
    metric = "downsampling_protocol_compatibility"
    rows = [{"metric": metric, "timestamp": base + offset, "value": value}
            for offset, value in ((1000, 2), (15000, 8), (31000, 4))]
    write_json(args.output / "fixture.json", rows)
    try:
        for name, new_select in (("new-select-old-storage", True), ("old-select-new-storage", False)):
            binaries = args.output / (name + "-binaries")
            binaries.mkdir()
            for component in ("vminsert", "vmstorage", "vmselect"):
                use_candidate = new_select if component == "vmselect" else not new_select
                source = args.candidate if use_candidate else args.original
                (binaries / component).symlink_to((source / component).resolve())
            server = Server(name, binaries, args.output, False, "cluster", "31:47")
            try:
                server.start()
                payload = {"metric": {"__name__": metric},
                           "timestamps": [row["timestamp"] for row in rows],
                           "values": [row["value"] for row in rows]}
                server.request("/api/v1/import", data=(json.dumps(payload) + "\n").encode())
                server.force_merge("flushed")
                actual = server.query("flushed", metric, base, base + 300000)
                expected = [(row["timestamp"], row["value"]) for row in rows]
                summary["checks"].append(assert_samples(actual, expected, name + ":native-raw"))
                start, end, step = base + 16000, base + 32000, 16000
                actual = server.query("flushed", metric, start, end, "16s", range_query=True)
                summary["checks"].append(assert_samples(actual, range_expected(expected, start, end, step),
                                                         name + ":native-range"))
                if new_select:
                    params = {"query": metric + "[300001ms]", "time": (base + 300000) / 1000,
                              "nocache": "1", "resolution": "5m", "feature": "sum"}
                    summary["checks"].append(assert_downsample_query_rejected(
                        server, "/api/v1/query", params, "unsupported-downsampling-v2-matrix"))
                    params = {"query": metric, "start": start / 1000, "end": end / 1000, "step": "16s",
                              "nocache": "1", "resolution": "5m", "feature": "sum"}
                    summary["checks"].append(assert_downsample_query_rejected(
                        server, "/api/v1/query_range", params, "unsupported-downsampling-v2-range"))
            finally:
                server.stop()
        summary["status"] = "passed"
    except Exception as error:
        summary["status"] = "failed"
        summary["error"] = repr(error)
        raise
    finally:
        summary["checks_count"] = len(summary["checks"])
        write_json(args.output / "summary.json", summary)
    print(json.dumps({"status": summary["status"], "checks": summary["checks_count"]}))


if __name__ == "__main__":
    main()
