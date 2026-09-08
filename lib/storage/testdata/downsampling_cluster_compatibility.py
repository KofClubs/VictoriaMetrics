#!/usr/bin/env python3
"""验证集群字段查询协议与原版组件的兼容边界。"""

import argparse
import json
import pathlib
import sys
import time
import urllib.error

sys.dont_write_bytecode = True

from downsampling_compare import Server, assert_samples, binary_manifest, write_json


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
            for offset, value in ((1000, 2), (2000, 8), (299999, 4))]
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
                if new_select:
                    params = {"query": metric + "[300001ms]", "time": (base + 300000) / 1000,
                              "nocache": "1", "query.field": "5m:sum"}
                    try:
                        result = server.request("/api/v1/query", params)
                    except urllib.error.HTTPError as error:
                        result = error.read().decode()
                        response = json.loads(result)
                        write_json(server.root / "unsupported-downsampling-v2.json", {
                            "url": server.url("/api/v1/query"), "params": params,
                            "http_status": error.code, "response": response})
                        assert response.get("status") == "error", response
                        assert "search_downsampling_v2" in response.get("error", ""), response
                        summary["checks"].append({"check": name + ":downsampling-v2-rejected", "http_status": error.code})
                    else:
                        raise AssertionError(("旧 vmstorage 的字段查询未明确失败", result))
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
