#!/usr/bin/env python3
"""使用真实集群验证相同标签与时间戳在不同 account/project 中的隔离。"""

import argparse
import datetime
import json
import pathlib
import sys
import time

sys.dont_write_bytecode = True

from downsampling_compare import (CONTROL_METRIC, FEATURES, METRIC, Server,
                                  aggregate, assert_samples, binary_manifest, fixture, range_expected, write_json)
from downsampling_inspect import inspect_part, resolution_ms, validate_config, Zstandard
from downsampling_multiseries_compare import MultiServer


TENANTS = ("11:17", "11:18", "12:17")
TRANSFORMS = ((1, 100), (2, 1000), (-3, -1000))
METRICS = (METRIC, CONTROL_METRIC)
RESOLUTIONS = {"5m": 300000, "30m": 1800000, "1h": 3600000, "2h": 7200000}
STARTUP_CONFIG = {"base_resolution": "5m", "tenant_resolutions": [
    {"tenant": "11:17", "resolutions": ["1h"]},
    {"tenant": "11:18", "resolutions": ["30m", "2h"]}]}
UPDATED_CONFIG = {"base_resolution": "5m", "tenant_resolutions": [
    {"tenant": "11:17", "resolutions": ["30m"]},
    {"tenant": "11:18", "resolutions": ["1h", "2h"]}]}


def configured_resolutions(config, tenant):
    base, tenants = validate_config(config)
    return tenants.get(tuple(map(int, tenant.split(":"))), [base])


def config_request(server, stage, expected, update=False):
    params = {"method": "PUT", "data": json.dumps(expected).encode()} if update else {}
    response = json.loads(server.request("/internal/downsampling/config", **params))
    write_json(server.root / (stage + "-config.json"), {"method": "PUT" if update else "GET",
        "request": expected if update else None, "response": response})
    assert response["status"] == "success", response
    if update:
        return config_request(server, stage + "-get", expected)
    assert validate_config(response["data"]) == validate_config(expected), response
    return {"check": stage + ":live-config", "config": response["data"]}


def cross_partition_fixture():
    # 使用上个月的月界，保证月界两侧样本均在过去，且在 12 个月 retention 内。
    current = datetime.datetime.now(datetime.timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    boundary = int((current - datetime.timedelta(days=1)).replace(day=1).timestamp() * 1000)
    metric = "downsampling_cross_partition_config"
    before = [{"metric": metric, "timestamp": boundary + offset * 60000, "value": value}
              for offset, value in ((-40, 2), (-10, 3))]
    after = [{"metric": metric, "timestamp": boundary + offset * 60000, "value": value}
             for offset, value in ((10, 5), (40, 7))]
    target = next(days for days in (7, 90, 365) if before[0]["timestamp"] // (days * 86400000)
                  == after[-1]["timestamp"] // (days * 86400000))
    return metric, boundary, before, after, str(target) + "d"


def verify_cross_partition(candidate, output, summary):
    directory = output / "cross-partition"
    directory.mkdir()
    old = {"base_resolution": "5m", "tenant_resolutions": [{"tenant": TENANTS[0], "resolutions": ["1h"]}]}
    new = {"base_resolution": "5m", "tenant_resolutions": [{"tenant": TENANTS[0], "resolutions": ["30m"]}]}
    server = MultiServer("candidate", candidate, directory, True, "cluster", TENANTS[0], old)
    metric, boundary, before, after, target = cross_partition_fixture()
    write_json(directory / "fixture.json", {"before": before, "after": after, "target": target,
                                            "old_config": old, "new_config": new})
    try:
        server.start()
        server.ingest(before)
        old_parts = server.force_merge("old-config")
        assert len(old_parts) == 1, old_parts
        summary["checks"].append(config_request(server, "new-config", new, update=True))
        server.ingest(after)
        server.wait_ingested()
        server.request("/internal/force_flush")
        partition = datetime.datetime.fromtimestamp(boundary / 1000, datetime.timezone.utc).strftime("%Y_%m")
        server.request_force_merge(partition)
        parts = server.part_metadata("mixed-config")
        assert len(parts) == 2, parts
        unchanged = next(part for part in parts if part["path"] == old_parts[0]["path"])
        assert unchanged["metadata"] == old_parts[0]["metadata"], ("旧 part 被意外重写", parts)
        newer = next(part for part in parts if part["path"] != unchanged["path"])
        zstd = Zstandard()
        reports = [inspect_part(part["partition"], pathlib.Path(part["path"]), zstd) for part in parts]
        actual_resolutions = {report["path"]: report["resolutions_ms"] for report in reports}
        assert actual_resolutions[unchanged["path"]] == [300000, 3600000], reports
        assert actual_resolutions[newer["path"]] == [300000, 1800000], reports
        write_json(directory / "inspection.json", {"status": "passed", "parts": reports})
        milliseconds = resolution_ms(target)
        expected = aggregate(before + after, metric, milliseconds)
        assert len(expected) == 1 and expected[0]["sum"] == 17 and expected[0]["count"] == 4, expected
        for feature in FEATURES:
            actual = server.query("mixed-config", metric, boundary - 3600000, boundary + 3600000, target, feature)
            summary["checks"].append(assert_samples(actual, [(expected[0]["timestamp"], expected[0][feature])],
                "single-node-cross-partition:" + target + ":" + feature, exact=True))
        summary["cross_partition"] = {"status": "passed", "target": target, "source_resolutions_ms": [3600000, 1800000],
            "parts": [part["path"] for part in parts], "input_rows": 4, "output_rows_per_feature": 1}
    finally:
        server.stop()


def verify(stage, servers, inputs, base, summary, output, stored_config=STARTUP_CONFIG):
    end = base + 3 * 3_600_000 - 1
    for server in servers:
        parts = json.loads((server.root / (stage + "-parts.json")).read_text())
        actual = sum(part["metadata"]["RowsCount"] for part in parts)
        expected = sum(len(rows) for rows in inputs.values())
        if server.downsampling:
            expected = sum(len(aggregate(rows, metric, resolution)) * len(FEATURES)
                           for tenant, rows in inputs.items() for metric in METRICS
                           for resolution in configured_resolutions(stored_config, tenant))
            layout_reports = []
            zstd = Zstandard()
            for part in parts:
                assert validate_config(part["metadata"]["downsampling_config"]) == validate_config(stored_config), part
                path = pathlib.Path(part["path"])
                report = inspect_part(path.parent.name, path, zstd)
                assert report["resolutions_ms"] == sorted(RESOLUTIONS.values()), report
                layout_reports.append(report)
            write_json(server.root / (stage + "-inspection.json"), {"status": "passed", "parts": layout_reports})
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
                params.update({"resolution": resolution, "feature": feature})
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
               "startup_config": STARTUP_CONFIG, "runtime_config": UPDATED_CONFIG,
               "query_resolutions": RESOLUTIONS, "tenants": list(TENANTS), "absent_tenant": "999:999", "checks": [], "stages": [], "binaries": {}}
    servers = []
    inputs = {tenant: [] for tenant in TENANTS}
    try:
        for name, binary, downsampling in (("original", args.original, False), ("candidate", args.candidate, True)):
            summary["binaries"][name] = binary_manifest(binary, "cluster")
            server = Server(name, binary, args.output, downsampling, "cluster", TENANTS[0], STARTUP_CONFIG)
            servers.append(server)
            server.start()
        summary["checks"].append(config_request(servers[1], "startup", STARTUP_CONFIG))
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
        summary["checks"].append(config_request(servers[1], "update", UPDATED_CONFIG, update=True))
        # 更新只改变后续作业；旧 part 必须仍能按自身 metadata 选择最大整除源。
        for server in servers:
            server.part_metadata("updated-before-rewrite")
        verify("updated-before-rewrite", servers, inputs, base, summary, args.output)
        for server in servers:
            server.force_merge("rewrite")
            previous = json.loads((server.root / "phase3-parts.json").read_text())
            current = json.loads((server.root / "rewrite-parts.json").read_text())
            assert {part["path"] for part in previous}.isdisjoint(part["path"] for part in current), \
                (server.name, "强制重写未产生新的 part 代次")
            summary["checks"].append({"check": server.name + ":rewrite-part-generation"})
        verify("rewrite", servers, inputs, base, summary, args.output, UPDATED_CONFIG)
        for server in servers:
            server.stop()
            server.start()
            current = server.part_metadata("restart")
            previous = json.loads((server.root / "rewrite-parts.json").read_text())
            assert {part["path"] for part in current} == {part["path"] for part in previous}, \
                (server.name, "重启改变已完成发布的 part 代次")
            summary["checks"].append({"check": server.name + ":restart-part-generation"})
        verify("restart", servers, inputs, base, summary, args.output, UPDATED_CONFIG)
        summary["checks"].append(config_request(servers[1], "restart", STARTUP_CONFIG))
        for server in servers:
            server.force_merge("restart-rewrite")
        verify("restart-rewrite", servers, inputs, base, summary, args.output)
        verify_cross_partition(args.candidate, args.output, summary)
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
