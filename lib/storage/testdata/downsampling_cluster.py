"""对照测试使用的真实集群进程与 HTTP 路由适配。"""

import json
import re
import signal
import subprocess
import time
import urllib.parse
import urllib.request


class ClusterRuntime:
    def __init__(self, binaries, root, storage, downsampling, tenant, unused_port):
        assert re.fullmatch(r"\d+(?::\d+)?", tenant), "tenant 必须为 account 或 account:project"
        self.root = root
        self.tenant = tenant
        self.processes = {}
        self.logs = {}
        self.expected_rows = 0
        ports = set()
        while len(ports) < 5:
            ports.add(unused_port())
        storage_http, insert_http, select_http, insert_rpc, select_rpc = sorted(ports)
        self.http = {"vmstorage": "http://127.0.0.1:" + str(storage_http),
                     "vminsert": "http://127.0.0.1:" + str(insert_http),
                     "vmselect": "http://127.0.0.1:" + str(select_http)}
        common = ["-memory.allowedBytes=256MiB", "-loggerLevel=INFO"]
        self.commands = {
            "vmstorage": [str((binaries / "vmstorage").resolve()), "-httpListenAddr=127.0.0.1:" + str(storage_http),
                          "-storageDataPath=" + str(storage), "-retentionPeriod=12",
                          "-inmemoryDataFlushInterval=1s", "-dedup.minScrapeInterval=0",
                          "-storage.minFreeDiskSpaceBytes=1MB", "-storage.vminsertConnsShutdownDuration=0s",
                          "-vminsertAddr=127.0.0.1:" + str(insert_rpc),
                          "-vmselectAddr=127.0.0.1:" + str(select_rpc)] + common,
            "vminsert": [str((binaries / "vminsert").resolve()), "-httpListenAddr=127.0.0.1:" + str(insert_http),
                         "-storageNode=127.0.0.1:" + str(insert_rpc), "-replicationFactor=1"] + common,
            "vmselect": [str((binaries / "vmselect").resolve()), "-httpListenAddr=127.0.0.1:" + str(select_http),
                         "-storageNode=127.0.0.1:" + str(select_rpc), "-replicationFactor=1",
                         "-dedup.minScrapeInterval=0", "-search.disableCache=true", "-search.latencyOffset=0s"] + common,
        }
        if downsampling:
            self.commands["vmstorage"].append("-storage.downsampling.enabled=true")
        self.write_manifest()

    def write_manifest(self):
        manifest = {"topology": "1 vminsert + 1 vmstorage + 1 vmselect", "replication_factor": 1,
                    "tenant": self.tenant, "http": self.http, "commands": self.commands,
                    "processes": {name: process.pid for name, process in self.processes.items()}}
        (self.root / "cluster.json").write_text(json.dumps(manifest, indent=2) + "\n")
        (self.root / "command.json").write_text(json.dumps(self.commands, indent=2) + "\n")

    def large_queries(self):
        for name in ("vmstorage", "vmselect"):
            self.commands[name] = [arg for arg in self.commands[name] if not arg.startswith("-memory.allowedBytes=")]
            self.commands[name].append("-memory.allowedBytes=512MiB")
        self.commands["vmselect"].extend(["-search.maxPointsPerTimeseries=1000000", "-search.maxQueryDuration=2m"])
        self.write_manifest()

    def url(self, path):
        tenant = urllib.parse.quote(self.tenant, safe=":")
        if path == "/api/v1/import":
            return self.http["vminsert"] + "/insert/" + tenant + "/prometheus" + path
        if path.startswith("/api/"):
            return self.http["vmselect"] + "/select/" + tenant + "/prometheus" + path
        return self.http["vmstorage"] + path

    def request(self, path, params=None, data=None):
        url = self.url(path)
        if params:
            url += "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=120) as response:
            result = response.read().decode()
        if path == "/api/v1/import":
            self.expected_rows += sum(len(json.loads(line)["timestamps"]) for line in data.splitlines() if line)
        return result

    def start(self):
        self.expected_rows = 0
        for name in ("vmstorage", "vminsert", "vmselect"):
            log = (self.root / (name + ".log")).open("ab")
            self.logs[name] = log
            self.processes[name] = subprocess.Popen(self.commands[name], stdout=log, stderr=log)
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                assert self.processes[name].poll() is None, name + " 启动失败，参见组件日志"
                try:
                    with urllib.request.urlopen(self.http[name] + "/health", timeout=1) as response:
                        assert response.status == 200
                    break
                except OSError:
                    time.sleep(0.1)
            else:
                raise AssertionError(name + " 启动超时")
        self.write_manifest()

    def wait_ingested(self):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            metrics = self.request("/metrics")
            match = re.search(r"^vm_rows_added_to_storage_total (\d+)$", metrics, re.MULTILINE)
            if match and int(match.group(1)) == self.expected_rows:
                return
            if match and int(match.group(1)) > self.expected_rows:
                raise AssertionError(("集群接收行数超过实际写入数量", int(match.group(1)), self.expected_rows))
            time.sleep(0.05)
        raise AssertionError(("vminsert 写入未全部到达 vmstorage", self.expected_rows))

    def stop(self):
        errors = []
        for name in ("vmselect", "vminsert", "vmstorage"):
            process = self.processes.pop(name, None)
            if process is None:
                continue
            try:
                process.send_signal(signal.SIGTERM)
                try:
                    code = process.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise AssertionError(name + " 正常关闭超时")
                assert code == 0, (name, "退出代码", code)
            except Exception as error:
                errors.append(repr(error))
            finally:
                self.logs.pop(name).close()
        assert not errors, errors
