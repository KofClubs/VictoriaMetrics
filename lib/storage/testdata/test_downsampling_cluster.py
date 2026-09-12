#!/usr/bin/env python3
"""不启动集群进程，验证测试适配器的路由、接收等待与失败传播。"""

import io
import json
import pathlib
import sys
import tempfile
import unittest
import urllib.error
import urllib.parse
from unittest import mock

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from downsampling_cluster import ClusterRuntime
from downsampling_cluster_compatibility import assert_downsample_query_rejected
from downsampling_compare import Server, aggregate
from downsampling_cluster_tenants import (STARTUP_CONFIG, UPDATED_CONFIG, config_request,
                                          configured_resolutions, cross_partition_fixture)
from downsampling_inspect import resolution_ms
from downsampling_multiseries_compare import MultiServer


class Response(io.BytesIO):
    status = 200


class ClusterRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = pathlib.Path(self.directory.name)
        ports = iter(range(18000, 18005))
        self.runtime = ClusterRuntime(self.root, self.root, self.root / "data", True, "11:17", lambda: next(ports))

    def test_startup_configuration_is_explicit_and_scoped(self):
        arguments = self.runtime.commands["vmstorage"]
        config = json.loads(next(value.split("=", 1)[1] for value in arguments
                                 if value.startswith("-storage.downsampling.config=")))
        self.assertEqual(config, {"base_resolution": "5m", "tenant_resolutions": [
            {"tenant": "11:17", "resolutions": ["1h"]}]})
        for enabled in (True, False):
            ports = iter(range(19000, 19005))
            runtime = ClusterRuntime(self.root, self.root, self.root / "data", enabled, "11:17",
                                     lambda: next(ports), STARTUP_CONFIG)
            flags = [value for value in runtime.commands["vmstorage"] if value.startswith("-storage.downsampling.")]
            if enabled:
                self.assertEqual(json.loads(flags[1].split("=", 1)[1]), STARTUP_CONFIG)
            else:
                self.assertEqual(flags, [])

    def test_configuration_update_uses_put_body_and_storage_route(self):
        payload = json.dumps(UPDATED_CONFIG).encode()
        with mock.patch("downsampling_cluster.urllib.request.urlopen", return_value=Response(b"{}")) as request:
            self.runtime.request("/internal/downsampling/config", data=payload, method="PUT")
        outgoing = request.call_args.args[0]
        self.assertEqual(outgoing.full_url, self.runtime.http["vmstorage"] + "/internal/downsampling/config")
        self.assertEqual(outgoing.get_method(), "PUT")
        self.assertEqual(json.loads(outgoing.data), UPDATED_CONFIG)

    def test_configuration_round_trip_rejects_stale_runtime_snapshot(self):
        server = mock.Mock(root=self.root)
        response = json.dumps({"status": "success", "data": UPDATED_CONFIG})
        server.request.return_value = response
        result = config_request(server, "updated", UPDATED_CONFIG, update=True)
        self.assertEqual(server.request.call_count, 2)
        self.assertEqual(server.request.call_args_list[0].kwargs["method"], "PUT")
        self.assertEqual(server.request.call_args_list[1].kwargs, {})
        self.assertEqual(result["config"], UPDATED_CONFIG)
        server.request.return_value = json.dumps({"status": "success", "data": STARTUP_CONFIG})
        with self.assertRaises(AssertionError):
            config_request(server, "stale", UPDATED_CONFIG, update=True)
        self.assertEqual(configured_resolutions(STARTUP_CONFIG, "11:17"), [300000, 3600000])
        self.assertEqual(configured_resolutions(STARTUP_CONFIG, "11:18"), [300000, 1800000, 7200000])
        self.assertEqual(configured_resolutions(STARTUP_CONFIG, "12:17"), [300000])

    def test_cross_partition_fixture_has_one_target_bucket_and_all_contributions(self):
        metric, boundary, before, after, resolution = cross_partition_fixture()
        self.assertLess(before[-1]["timestamp"], boundary)
        self.assertGreater(after[0]["timestamp"], boundary)
        rows = aggregate(before + after, metric, resolution_ms(resolution))
        self.assertEqual(rows, [{"timestamp": after[-1]["timestamp"], "last": 7, "sum": 17,
                                 "count": 4, "min": 2, "max": 7}])
        self.assertEqual(len(aggregate(before, metric, 3600000)), 1)
        self.assertEqual(len(aggregate(after, metric, 1800000)), 2)

    def test_tenant_switch_routes_to_correct_component(self):
        runtime = self.runtime
        runtime.tenant = "0011:0018"
        self.assertEqual(runtime.tenant, "11:18")
        self.assertEqual(runtime.url("/api/v1/import"), runtime.http["vminsert"] + "/insert/11:18/prometheus/api/v1/import")
        self.assertEqual(runtime.url("/api/v1/query_range"), runtime.http["vmselect"] + "/select/11:18/prometheus/api/v1/query_range")
        for path in ("/metrics", "/internal/force_flush", "/internal/force_merge"):
            self.assertEqual(runtime.url(path), runtime.http["vmstorage"] + path)
        runtime.tenant = "12"
        self.assertEqual(runtime.tenant, "12:0")
        runtime.tenant = "4294967295:4294967295"
        self.assertIn("/select/4294967295:4294967295/", runtime.url("/api/v1/query"))

    def test_invalid_tenant_is_rejected_without_changing_route(self):
        for tenant in ("", "-1:0", "1:2:3", "1/2", "１:2", "4294967296:0", "1:4294967296", None):
            with self.subTest(tenant=tenant), self.assertRaises(AssertionError):
                self.runtime.tenant = tenant
            self.assertEqual(self.runtime.tenant, "11:17")

    def test_query_parameters_are_preserved_in_both_http_query_routes(self):
        response = json.dumps({"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"__name__": "probe"}, "values": [[1, "2"]]}]}}).encode()
        combinations = [(resolution, feature) for resolution in ("5m", "1h")
                        for feature in ("last", "sum", "count", "min", "max")]
        combinations.append(("16s", None))
        for server_type in (Server, MultiServer):
            # 只测试请求构造和路由，不分配端口或启动进程。
            server = server_type.__new__(server_type)
            server.root, server.cluster = self.root, self.runtime
            for range_query in (False, True):
                path = "/api/v1/query_range" if range_query else "/api/v1/query"
                for resolution, feature in combinations:
                    with self.subTest(server=server_type.__name__, path=path, resolution=resolution, feature=feature):
                        with mock.patch("urllib.request.urlopen", return_value=Response(response)) as request:
                            if server_type is Server:
                                server.query("request", "probe", 0, 1000, resolution, feature, range_query)
                            else:
                                server.query_many("request", "probe", "probe", 0, 1000, resolution, feature, range_query)
                        url = request.call_args.args[0]
                        parsed = urllib.parse.urlparse(url if isinstance(url, str) else url.full_url)
                        self.assertEqual(parsed.path, "/select/11:17/prometheus" + path)
                        parameters = urllib.parse.parse_qs(parsed.query)
                        self.assertNotIn("query.field", parameters)
                        if feature is None:
                            self.assertNotIn("resolution", parameters)
                            self.assertNotIn("feature", parameters)
                        else:
                            self.assertEqual(parameters["resolution"], [resolution])
                            self.assertEqual(parameters["feature"], [feature])

    def test_import_count_advances_only_after_success(self):
        payload = json.dumps({"metric": {"__name__": "probe"}, "timestamps": [1, 2], "values": [3, 4]}).encode()
        with mock.patch("downsampling_cluster.urllib.request.urlopen", return_value=Response(b"")):
            self.runtime.request("/api/v1/import", data=payload)
        self.assertEqual(self.runtime.expected_rows, 2)
        with mock.patch("downsampling_cluster.urllib.request.urlopen", side_effect=OSError("连接失败")):
            with self.assertRaises(OSError):
                self.runtime.request("/api/v1/import", data=payload)
        self.assertEqual(self.runtime.expected_rows, 2)
        malformed = json.dumps({"timestamps": [1, 2], "values": [3]}).encode()
        with mock.patch("downsampling_cluster.urllib.request.urlopen") as request:
            with self.assertRaises(AssertionError):
                self.runtime.request("/api/v1/import", data=malformed)
            request.assert_not_called()

    def test_wait_ingested_waits_for_exact_vmstorage_count(self):
        self.runtime.expected_rows = 5
        with mock.patch.object(self.runtime, "request", side_effect=[
            "vm_rows_added_to_storage_total 0\n", "vm_rows_added_to_storage_total 2\n",
            "vm_rows_added_to_storage_total 5\n",
        ]) as request, mock.patch("downsampling_cluster.time.sleep"):
            self.runtime.wait_ingested()
            self.assertEqual(request.call_count, 3)
        for metrics in ("vm_rows_added_to_storage_total 6\n", "other_metric 5\n"):
            with mock.patch.object(self.runtime, "request", return_value=metrics), self.assertRaises(AssertionError):
                self.runtime.wait_ingested()

    def test_wait_ingested_rejects_dead_component(self):
        self.runtime.processes["vminsert"] = mock.Mock(returncode=1, **{"poll.return_value": 1})
        with mock.patch.object(self.runtime, "request") as request, self.assertRaises(AssertionError):
            self.runtime.wait_ingested()
        request.assert_not_called()

    def test_start_failure_stops_started_components_and_closes_logs(self):
        storage = mock.Mock(pid=123, **{"poll.return_value": None, "wait.return_value": 0})
        with mock.patch("downsampling_cluster.subprocess.Popen", side_effect=[storage, OSError("不可执行")]), \
                mock.patch("downsampling_cluster.urllib.request.urlopen", return_value=Response(b"")):
            with self.assertRaisesRegex(OSError, "不可执行"):
                self.runtime.start()
        storage.send_signal.assert_called_once()
        storage.wait.assert_called_once_with(timeout=45)
        self.assertEqual(self.runtime.processes, {})
        self.assertEqual(self.runtime.logs, {})

    def test_start_does_not_replace_a_running_cluster(self):
        self.runtime.processes["vmstorage"] = mock.Mock()
        with mock.patch("downsampling_cluster.subprocess.Popen") as start, self.assertRaises(AssertionError):
            self.runtime.start()
        start.assert_not_called()


class CompatibilityAssertionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.server = mock.Mock()
        self.server.cluster = mock.Mock(spec=ClusterRuntime)
        self.server.name = "new-select-old-storage"
        self.server.root = pathlib.Path(self.directory.name)
        self.server.url.return_value = "http://localhost/select/31:47/prometheus/api/v1/query"
        self.params = {"resolution": "5m", "feature": "sum"}
        self.log = self.server.root / "vmstorage.log"
        self.log.write_text("先前的启动日志\n")

    def test_only_protocol_error_is_accepted(self):
        response = {"status": "error", "error": "cannot call search_downsampling_v2: unsupported RPC"}
        error = urllib.error.HTTPError(
            "http://localhost", 422, "", {}, io.BytesIO(json.dumps(response).encode()))
        self.addCleanup(error.close)
        offset = self.log.stat().st_size
        appended = 'cannot execute "search_downsampling_v2": unsupported rpcName: "search_downsampling_v2"\n'

        def reject(*_args):
            with self.log.open("a") as log:
                log.write(appended)
            raise error

        self.server.request.side_effect = reject
        result = assert_downsample_query_rejected(self.server, "/api/v1/query", self.params, "rejected")
        self.assertEqual(result["http_status"], 422)
        evidence = json.loads((self.server.root / "rejected.json").read_text())
        self.assertEqual(evidence["params"], self.params)
        self.assertEqual(evidence["response"], response)
        self.assertEqual(evidence["vmstorage_log"]["start_offset"], offset)
        self.assertEqual(evidence["vmstorage_log"]["end_offset"], self.log.stat().st_size)
        self.assertEqual(pathlib.Path(result["unsupported_rpc_log"]).read_text(), appended)
        self.assertEqual(self.server.cluster.assert_running.call_count, 2)

    def test_rpc_eof_without_new_unsupported_log_cannot_pass(self):
        # 上一次请求的同名错误不能作为本次连接故障的依据。
        self.log.write_text('unsupported rpcName: "search_downsampling_v2"\n')
        response = {"status": "error", "error": "cannot call search_downsampling_v2: EOF"}
        error = urllib.error.HTTPError(
            "http://localhost", 422, "", {}, io.BytesIO(json.dumps(response).encode()))
        self.addCleanup(error.close)
        self.server.request.side_effect = error
        with self.assertRaisesRegex(AssertionError, "本次请求未产生"):
            assert_downsample_query_rejected(self.server, "/api/v1/query", self.params, "connection-failure")
        excerpt = self.server.root / "connection-failure-vmstorage.log"
        self.assertEqual(excerpt.read_bytes(), b"")

    def test_empty_success_or_unrelated_error_cannot_pass(self):
        self.server.request.return_value = json.dumps({"status": "success", "data": {"result": []}})
        with self.assertRaises(AssertionError):
            assert_downsample_query_rejected(self.server, "/api/v1/query", self.params, "success")
        error = urllib.error.HTTPError(
            "http://localhost", 500, "", {}, io.BytesIO(b'{"status":"error","error":"unrelated"}'))
        self.addCleanup(error.close)
        self.server.request.side_effect = error
        with self.assertRaises(AssertionError):
            assert_downsample_query_rejected(self.server, "/api/v1/query", self.params, "unrelated")


if __name__ == "__main__":
    unittest.main()
