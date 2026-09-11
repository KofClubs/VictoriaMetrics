#!/usr/bin/env python3
"""使用真实集群验证相同标签与时间戳在不同 account/project 中的隔离。"""

import argparse
import datetime
import json
import pathlib
import sys
import time

sys.dont_write_bytecode = True

from downsampling_compare import (CONTROL_METRIC, FEATURES, METRIC, RESOLUTIONS, Server,
                                  aggregate, assert_samples, binary_manifest, fixture, range_expected, write_json)


TENANTS = ("11:17", "11:18", "12:17")
TRANSFORMS = ((1, 100), (2, 1000), (-3, -1000))
METRICS = (METRIC, CONTROL_METRIC)


def verify(stage, servers, inputs, base, summary, output):
    end = base + 3 * 3_600_000 - 1
    for server in servers:
        parts = json.loads((server.root / (stage + "-parts.json")).read_text())
        actual = sum(part["metadata"]["RowsCount"] for part in parts)
        expected = sum(len(rows) for rows in inputs.values())
        if server.downsampling:
            expected = sum(len(aggregate(rows, metric, resolution)) * len(FEATURES)
                           for rows in inputs.values() for metric in METRICS
                           for resolution in RESOLUTIONS.values())
        assert actual == expected, (stage, server.name, "全租户物理行数", actual, expected)
        summary["checks"].append({"check": stage + ":" + server.name + ":physical-rows", "rows": actual})
    for tenant in TENANTS:
        for server in servers:
            server.cluster.tenant = tenant
        prefix = stage + "-tenant-" + tenant.replace(":", "-")
        original = servers[0]
        for metric in METRICS:
            raw = original.query(prefix, metric, base, end)
            expected_raw = sorted((row["timestamp"], row["value"]) for row in inputs[tenant] if row["metric"] == metric)
            summary["checks"].append(assert_samples(raw, expected_raw, prefix + ":original:" + metric + ":raw", exact=True))
            reference = [{"metric": metric, "timestamp": timestamp, "value": value} for timestamp, value in raw]
            for resolution, milliseconds in RESOLUTIONS.items():
                start = base + milliseconds - 1
                actual = original.query(prefix, metric, start, end, resolution, range_query=True)
                wanted_range = range_expected(raw, start, end, milliseconds)
                summary["checks"].append(assert_samples(actual, wanted_range,
                                                         prefix + ":original:" + metric + ":" + resolution + ":bare-range"))
                expected = aggregate(reference, metric, milliseconds)
                write_json(servers[1].root / (prefix + "-" + metric + "-" + resolution + "-expected.json"), expected)
                for feature in FEATURES:
                    actual = servers[1].query(prefix, metric, base, end, resolution, feature)
                    wanted = [(row["timestamp"], row[feature]) for row in expected]
                    summary["checks"].append(assert_samples(actual, wanted, prefix + ":" + metric + ":" + resolution + ":" + feature))
                    actual = servers[1].query(prefix, metric, start, end, resolution, feature, True)
                    wanted_range = range_expected(wanted, start, end, milliseconds)
                    summary["checks"].append(assert_samples(actual, wanted_range,
                                                             prefix + ":" + metric + ":" + resolution + ":" + feature + ":bare-range"))
        write_json(output / "summary.json", summary)
    # 未写入的租户必须为空，不能透出其它租户的相同指标。
    for server in servers:
        server.cluster.tenant = "999:999"
        selectors = [(None, None)] if not server.downsampling else [(r, f) for r in RESOLUTIONS for f in FEATURES]
        for resolution, feature in selectors:
            params = {"query": METRIC + "[10800000ms]", "time": end / 1000, "nocache": "1"}
            if feature is not None:
                params.update({"query.resolution": resolution, "query.feature": feature})
            response = json.loads(server.request("/api/v1/query", params))
            assert response == {"status": "success", "data": {"resultType": "matrix", "result": []}}, response
            name = stage + "-absent-tenant-" + str(resolution) + "-" + str(feature)
            write_json(server.root / (name + ".json"), {"url": server.url("/api/v1/query"), "params": params, "response": response})
            summary["checks"].append({"check": server.name + ":" + name, "rows": 0})
    summary["stages"].append(stage)
    write_json(output / "summary.json", summary)
    print(stage + ": 三个租户与空租户的隔离、全部特征与裸查询验证通过", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original", type=pathlib.Path, required=True)
    parser.add_argument("--candidate", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    base = (int(time.time() * 1000) // 3_600_000 - 24) * 3_600_000
    phases = {}
    for tenant, (multiplier, offset) in zip(TENANTS, TRANSFORMS):
        phases[tenant] = [[{**row, "value": row["value"] * multiplier + offset} for row in batch] for batch in fixture(base)]
    write_json(args.output / "fixture.json", {"base_ms": base, "timestamp_step_ms": [14000, 15000, 16000],
                                              "tenants": phases})
    summary = {"status": "running", "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "topology": "真实集群 1 vminsert + 1 vmstorage + 1 vmselect，replicationFactor=1",
               "tenants": list(TENANTS), "absent_tenant": "999:999", "checks": [], "stages": [], "binaries": {}}
    servers = []
    inputs = {tenant: [] for tenant in TENANTS}
    try:
        for name, binary, downsampling in (("original", args.original, False), ("candidate", args.candidate, True)):
            summary["binaries"][name] = binary_manifest(binary, "cluster")
            server = Server(name, binary, args.output, downsampling, "cluster", TENANTS[0])
            servers.append(server)
            server.start()
        for phase in range(3):
            stage = "phase" + str(phase + 1)
            for tenant in TENANTS:
                batch = phases[tenant][phase]
                inputs[tenant].extend(batch)
                for server in servers:
                    server.cluster.tenant = tenant
                    server.ingest(batch)
            for server in servers:
                server.force_merge(stage)
            verify(stage, servers, inputs, base, summary, args.output)
        for server in servers:
            server.force_merge("rewrite")
            previous = json.loads((server.root / "phase3-parts.json").read_text())
            current = json.loads((server.root / "rewrite-parts.json").read_text())
            assert {part["path"] for part in previous}.isdisjoint(part["path"] for part in current), \
                (server.name, "强制重写未产生新的 part 代次")
            summary["checks"].append({"check": server.name + ":rewrite-part-generation"})
        verify("rewrite", servers, inputs, base, summary, args.output)
        for server in servers:
            server.stop()
            server.start()
            current = server.part_metadata("restart")
            previous = json.loads((server.root / "rewrite-parts.json").read_text())
            assert {part["path"] for part in current} == {part["path"] for part in previous}, \
                (server.name, "重启改变已完成发布的 part 代次")
            summary["checks"].append({"check": server.name + ":restart-part-generation"})
        verify("restart", servers, inputs, base, summary, args.output)
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
