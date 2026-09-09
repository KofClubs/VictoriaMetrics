#!/usr/bin/env bash
# 构建并运行完整的降采样集群对照测试，保留日志、数据和结果清单。
set -euo pipefail

testdata_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec python3 -B -E - "$testdata_dir" "$@" <<'PY'
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import time

testdata = pathlib.Path(sys.argv[1]).resolve()
parser = argparse.ArgumentParser(
    prog="downsampling_e2e.sh",
    description="一键运行降采样集群测试：两套组件构建、UT、三小时、三租户、93 天对照、重启一致性、兼容性及文件检查。")
parser.add_argument("--output", type=pathlib.Path, help="尚不存在的输出目录；默认在系统临时目录中自动创建")
args = parser.parse_args(sys.argv[2:])
for tool in ("git", "go"):
    if shutil.which(tool) is None:
        parser.error("未找到必需命令：" + tool)
source = pathlib.Path(subprocess.check_output(
    ["git", "rev-parse", "--show-toplevel"], cwd=testdata, text=True).strip())
baseline_ref = "v1.151.0-cluster"
try:
    baseline_commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", "refs/tags/" + baseline_ref + "^{commit}"], cwd=source, text=True,
        stderr=subprocess.PIPE).strip()
except subprocess.CalledProcessError:
    parser.error("本地缺少基准 tag " + baseline_ref + "，请先获取该 tag")
sys.path.insert(0, str(testdata))
from downsampling_inspect import Zstandard
try:
    zstd_library = Zstandard().library
except Exception as error:
    parser.error("无法加载系统 libzstd：" + str(error))
if args.output is None:
    output = pathlib.Path(tempfile.mkdtemp(prefix="vm-downsampling-e2e-")).resolve()
else:
    output = args.output.expanduser().resolve()
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error("输出目录已存在，拒绝覆盖：" + str(output))

baseline = output / "baseline-source"
logs = output / "logs"
logs.mkdir()
environment = os.environ.copy()
# 防止环境变量禁用各 Python 测试脚本中的断言。
environment.pop("PYTHONOPTIMIZE", None)
environment["PYTHONDONTWRITEBYTECODE"] = "1"
python = [sys.executable, "-B", "-E"]
layout_tests = (
    "^TestDownsample(FilePhysicalLayout|IterationMultiTSID|IterationReaderReuse|ClusterTenantIndexBoundaries)$")
long_required_coverage = ["same_tsid_5m_over_8192_rows_and_multiple_blocks",
                          "multiple_partitions", "multiple_physical_tsids"]
manifest = {"status": "running", "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "output": str(output), "candidate_source": str(source), "baseline_ref": baseline_ref,
            "baseline_commit": baseline_commit, "zstd_library": zstd_library, "steps": [], "results": {},
            "coverage_validation": {
                "go-layout-ut": {
                    "test_pattern": layout_tests,
                    "scope": "同 (resolution, feature) 内同 TSID 与不同 TSID 跨 index，租户切 row，文件物理分组",
                    "fixtures": ["每分辨率 738 批次，超过默认每 index 的 736 个 header 上限",
                                 "180 条时间线，以 132 header/index 使同一 TSID 跨相邻 index",
                                 "AccountID 与 ProjectID 分别切换时 row 保持同租户"]},
                "long": {
                    "required": long_required_coverage,
                    "scope": "160 条时间线的真实 E2E 文件；同列跨 index 按实际产物报告，不作为本场景必选项"}}}


def save_manifest():
    temporary = output / "manifest.json.tmp"
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(output / "manifest.json")


class Interrupted(Exception):
    def __init__(self, signum):
        self.signum = signum
        super().__init__("测试收到信号 " + str(signum))


def interrupt(signum, _frame):
    raise Interrupted(signum)


def group_exists(process):
    try:
        os.killpg(process.pid, 0)
        return True
    except ProcessLookupError:
        return False


def stop_group(process):
    # 每个阶段具有独立进程组；仅清理本次创建的进程，不使用日志中的历史 PID。
    if not group_exists(process):
        process.wait(timeout=5)
        return
    handlers = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                pass
        if group_exists(process):
            os.killpg(process.pid, signal.SIGTERM)
            deadline = time.monotonic() + 5
            while group_exists(process) and time.monotonic() < deadline:
                time.sleep(0.1)
            if group_exists(process):
                os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
    except ProcessLookupError:
        process.wait(timeout=5)
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def run_step(name, command, cwd=source):
    command = [str(argument) for argument in command]
    log_path = logs / (name + ".log")
    record = {"name": name, "command": command, "cwd": str(cwd), "log": str(log_path), "status": "running"}
    manifest["steps"].append(record)
    save_manifest()
    print("\n[" + name + "]", flush=True)
    started = time.monotonic()
    process = None
    try:
        with log_path.open("wb") as log:
            # 创建阶段进程时暂存信号，取得进程句柄后再执行中断处理。
            pending_signals = []
            handlers = {sig: signal.signal(sig, lambda number, _frame: pending_signals.append(number))
                        for sig in (signal.SIGINT, signal.SIGTERM)}
            try:
                process = subprocess.Popen(command, cwd=cwd, env=environment, start_new_session=True,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            finally:
                for sig, handler in handlers.items():
                    signal.signal(sig, handler)
            if pending_signals:
                raise Interrupted(pending_signals[0])
            with process.stdout, selectors.DefaultSelector() as reader:
                os.set_blocking(process.stdout.fileno(), False)
                reader.register(process.stdout, selectors.EVENT_READ)

                def copy_output():
                    try:
                        chunk = os.read(process.stdout.fileno(), 65536)
                    except BlockingIOError:
                        return False
                    if not chunk:
                        if reader.get_map():
                            reader.unregister(process.stdout)
                        return False
                    log.write(chunk)
                    log.flush()
                    sys.stdout.buffer.write(chunk)
                    sys.stdout.buffer.flush()
                    return True

                # 后代可能继续持有管道；以阶段进程的退出状态决定何时清理。
                while process.poll() is None:
                    if reader.select(timeout=0.1):
                        copy_output()
                while copy_output():
                    pass
            record["exit_code"] = process.wait()
            if record["exit_code"]:
                raise RuntimeError(name + " 失败，退出码 " + str(record["exit_code"]) + "；日志：" + str(log_path))
        record["status"] = "passed"
    except BaseException as error:
        record["status"] = "interrupted" if isinstance(error, Interrupted) else "failed"
        record["error"] = str(error)
        raise
    finally:
        if process is not None:
            try:
                stop_group(process)
            finally:
                if process.stdout is not None:
                    process.stdout.close()
                record["exit_code"] = process.returncode
        record["elapsed_seconds"] = round(time.monotonic() - started, 3)
        save_manifest()


def read_result(path):
    result = json.loads(path.read_text())
    if result.get("status") != "passed":
        raise RuntimeError("测试结果未通过：" + str(path))
    return result


def inspect(name, series, tenants, coverage=()):
    path = output / name / "inspection.json"
    command = python + [tests / "downsampling_inspect.py", "--data-dir", output / name / "candidate/data/data",
                        "--output", path, "--expected-series", series]
    for tenant in tenants:
        command += ["--expected-tenant", tenant]
    for field in coverage:
        command += ["--require", field]
    run_step("inspect-" + name, command)
    report = read_result(path)
    manifest["results"][name]["inspection"] = {key: report[key] for key in
        ("active_parts", "unique_tsids", "physical_blocks", "physical_rows", "index_blocks", "coverage")}
    save_manifest()


exit_code = 0
baseline_attempted = False
signal.signal(signal.SIGINT, interrupt)
signal.signal(signal.SIGTERM, interrupt)
print("测试输出目录：" + str(output), flush=True)
try:
    manifest["candidate_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    manifest["candidate_status"] = subprocess.check_output(["git", "status", "--short"], cwd=source, text=True)
    manifest["go_version"] = subprocess.check_output(["go", "version"], text=True).strip()
    manifest["python_version"] = sys.version
    (output / "candidate.patch").write_bytes(subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=source))
    tests = output / "test-sources"
    files = sorted(testdata.glob("*.py")) + [testdata / "downsampling_e2e.sh"]
    manifest["test_files_sha256"] = {}
    for file in files:
        snapshot = tests / file.name
        snapshot.parent.mkdir(exist_ok=True)
        shutil.copyfile(file, snapshot)
        manifest["test_files_sha256"][file.name] = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    save_manifest()
    baseline_attempted = True
    run_step("baseline-source", ["git", "worktree", "add", "--detach", baseline, baseline_commit])
    for name, tree in (("original", baseline), ("candidate", source)):
        binaries = output / name
        binaries.mkdir()
        run_step("build-" + name, ["go", "build", "-p", "4", "-o", str(binaries) + "/",
                                  "./app/vminsert", "./app/vmstorage", "./app/vmselect"], tree)
        manifest[name + "_sha256"] = {exe: hashlib.sha256((binaries / exe).read_bytes()).hexdigest()
                                      for exe in ("vminsert", "vmstorage", "vmselect")}
        save_manifest()
    run_step("python-ut", python + ["-W", "error::ResourceWarning", "-m", "unittest", "discover", "-s", tests,
                                   "-p", "test_downsampling_*.py", "-v"])
    # 新格式每列独立成组。160 条 E2E 时间线不保证超过同列 736 header/index；
    # 固定 Go fixture 必须独立通过，不能把 feature 切换算作同列跨 index。
    run_step("go-layout-ut", ["go", "test", "-p", "4", "./lib/storage", "-run", layout_tests, "-count=1", "-v"])
    run_step("go-ut", ["go", "test", "-p", "4", "./lib/storage", "./lib/vmselectapi", "./app/vmstorage",
                       "./app/vmselect/netstorage", "./app/vmselect/prometheus", "-run",
                       "Test(Downsample|Downsampling|CheckDownsampling|MustOpenStorageDownsampling|EstimateDownsample|ReserveDownsample)",
                       "-skip", layout_tests, "-count=1"])
    common = ["--original", output / "original", "--candidate", output / "candidate"]
    cluster = ["--mode", "cluster", "--tenant", "11:17"]
    cases = [("short", "downsampling_compare.py", common + cluster),
             ("tenants", "downsampling_cluster_tenants.py", common),
             ("long", "downsampling_multiseries_compare.py", common + cluster + ["--days", "93", "--series", "160", "--dense-series", "4"]),
             ("restart", "downsampling_restart.py", ["--candidate", output / "candidate", "--tenant", "11:17"]),
             ("compatibility", "downsampling_cluster_compatibility.py", common)]
    for name, script, options in cases:
        run_step(name, python + [tests / script] + options + ["--output", output / name])
        report = read_result(output / name / "summary.json")
        manifest["results"][name] = {
            "summary": str(output / name / "summary.json"), "checks": report["checks_count"],
            "max_absolute_error": max((check.get("max_absolute_error", 0) for check in report["checks"]), default=0)}
        if name == "restart":
            if report.get("exact_comparisons") != 20 or not report.get("exact_compared_points", 0):
                raise RuntimeError("重启一致性测试未完成全部二十组非空快照比较")
            manifest["results"][name].update(exact_comparisons=report["exact_comparisons"],
                                            exact_compared_points=report["exact_compared_points"])
        if name in ("short", "restart"):
            inspect(name, 2, ["11:17"])
        elif name == "tenants":
            inspect(name, 6, ["11:17", "11:18", "12:17"])
        elif name == "long":
            inspect(name, 160, ["11:17"], long_required_coverage)
        save_manifest()
    manifest["e2e_checks"] = sum(result["checks"] for result in manifest["results"].values())
    manifest["max_absolute_error"] = max(result["max_absolute_error"] for result in manifest["results"].values())
    manifest["status"] = "passed"
except Interrupted as error:
    exit_code = 128 + error.signum
    manifest.update(status="interrupted", error=str(error))
    print(str(error), file=sys.stderr, flush=True)
except Exception as error:
    exit_code = 1
    manifest.update(status="failed", error=str(error))
    print(str(error), file=sys.stderr, flush=True)
finally:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    # 只移除本次创建的基准 worktree；二进制、数据和全部证据均保留。
    if baseline_attempted:
        try:
            with (logs / "cleanup-baseline.log").open("w") as log:
                inventory = subprocess.check_output(["git", "worktree", "list", "--porcelain", "-z"],
                                                    cwd=source, text=True, timeout=30)
                registered = {pathlib.Path(item[len("worktree "):]).resolve()
                              for item in inventory.split("\0") if item.startswith("worktree ")}
                if baseline in registered:
                    subprocess.run(["git", "worktree", "remove", "--force", str(baseline)], cwd=source,
                                   stdout=log, stderr=subprocess.STDOUT, check=True, timeout=30)
                elif baseline.exists():
                    shutil.rmtree(baseline)
            manifest["baseline_source_removed"] = True
        except Exception as error:
            manifest["cleanup_error"] = str(error)
            if not exit_code:
                exit_code = 1
                manifest["status"] = "failed"
    manifest["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    manifest["exit_code"] = exit_code
    save_manifest()
print("\n结果：" + manifest["status"] + "；清单：" + str(output / "manifest.json"), flush=True)
sys.exit(exit_code)
PY
