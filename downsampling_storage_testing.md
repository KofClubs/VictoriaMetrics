# 降采样存储测试说明

## 当前布局与本次验证（2026-09-09）

当前生产实现以 [文件布局](downsampling_storage_file_layout.md) 为准：集群版 89 字节原生 header、113 字节嵌入式 metaindex；
五路磁盘 spill 在分辨率结束时按 feature 完整输出，values/index/metaindex 全局按 resolution/feature/TSID/时间排列。
timestamps 按生成顺序只写一次（五列分别编码并校验一致性）；打开时有界五路遍历全部 index，查询只读目标列，归并五路对齐。
`TSID.Less` 先比较 AccountID、ProjectID；租户变化仍须切 row。分段 index flush 不得导致 feature 从 4 回退到 0。

**当前 feature 编号为 0..4。旧 99/112 字节格式，以及 `ae899eded` 的 89/113 字节、feature 1..5 格式都不兼容，且无迁移。** 版本号和 magic 仍为 2，不能据此复用旧摘要目录。
检查器已适配当前格式；下文历史集群与一键结果仍属于旧布局，不代表当前布局已通过 E2E。
本次更新文档、检查器和一键验证入口，不修改生产 Go 代码；用户已完成当前布局的一键集群测试，结果及产物已复核通过。

当前空间测试覆盖：

| 测试 | 覆盖 |
|---|---|
| `TestEstimateDownsamplePartSize` | raw 两分辨率放大、摘要物理行除五向上取整、混合输入、零行、无效输入及溢出；不依赖源压缩大小 |
| `TestEstimateDownsampleOutputSize` | 独立 89/113 字节公式：一份 timestamps、五份最终 values、五列独立 index/metaindex、五份 spill header/values、64 KiB metadata |
| `TestDownsampleSpaceEstimateOverflow` | `math/big` 独立参考；行数/批次数饱和前后、组合加法、payload/五列乘法溢出及饱和算术边界 |
| `TestDownsampleSpaceBoundCoversEncodedParts` | 满 Block、单行多 TSID、强制每列每 Block 独占 index；实测五路 spill 字节、timestamps 不重复、最终五文件无 spill、逐列 index/metaindex 统计及打开校验 |
| `TestReserveDownsampleSpaceConcurrentAndCachedFreeSpace` | 并发预留、幂等释放、空闲空间缓存债务与过期 |
| `TestDownsampleAvailableSpaceBoundaries` | 安全余量、预留、请求边界及无效行数 |

实盘测试比较“最终文件总字节 + flush 前全部 spill 字节”与预算，这是覆盖逐列删除过程的**保守峰值包络**，
不是对瞬时峰值的采样。独立公式为 `110 × 逻辑行数 + 5105 × 五列批次数 + 65536`，超出 uint64 时饱和。

本轮生产代码审查中已运行：

- `go test ./lib/storage -count=1`：通过，23.558s。
- `go test -race ./lib/storage -run 'Test(Downsample|Downsampling|CheckDownsampling|MustOpenStorageDownsampling)' -count=1`：通过，7.454s。
- storage、vmselectapi、vmstorage、netstorage、prometheus 的降采样定向测试：通过；vmstorage 无匹配测试但完成编译。
- `TestDownsampleFilePhysicalLayout`：四个子场景全部通过，覆盖两个分辨率、738 批次跨默认 index 阈值、Account/Project 租户边界及零 payload。

检查器与入口更新后的验证：

- `python3 -B -E -W error::ResourceWarning -m unittest discover -s lib/storage/testdata -p 'test_downsampling_*.py' -v`：46 项通过，含新增的 15 项检查器测试，0.185s。
- 一键入口中的 `go-layout-ut` 和 `go-ut` 两条实际命令均通过；storage 分别耗时 2.649s、3.726s。普通定向测试跳过独立布局组，并纳入空间估算、预留测试。
- `bash -n`、入口 `--help`、内嵌 Python 语法及参数检查、`git diff --check` 均通过。

检查器独立解析固定偏移的 89 字节原生 header、113 字节 metaindex 和 32 字节 TSID；resolution/feature 从 metaindex 继承。
按五列同步比对 Block 身份和共享时间戳，验证 timestamps 按分辨率只存一份、values 按 resolution/feature 连续、每 row 同租户，以及排序、统计、完整文件覆盖和损坏拒绝。
当前 Go writer 的四组实盘 fixture 已由新 Python CLI 全部检查通过：不同 TSID 跨 index、五租户边界、零 payload、同 TSID 跨 index；分别为 7380、3300、30、7380 个单特征 Block。
交叉验证证据：`/var/folders/tk/llwph05x6_xgbmqxbxkq_6m40000gn/T/vm-downsampling-inspector-check-m0jx8mk8/cross_validation.json`；同目录保留 Go overlay、fixture 清单、各组 `inspection.json`、`python-ut.log` 和含脚本 SHA256 的 `validation.json`。
`feature_switch` 单独报告；跨 TSID、同 TSID 延续和多个 index 的覆盖只在同一 `(resolution, feature)` 内计算。

## 当前布局一键结果

证据目录：`/private/var/folders/tk/llwph05x6_xgbmqxbxkq_6m40000gn/T/vm-downsampling-e2e-8n7u6mcp`，总清单为其中的 `manifest.json`。本次于 2026-09-09 运行，15 个阶段均为 `passed`、退出码均为 0。
候选为 `dcb40bdc9774d39ff696ea7449aad26247e93def` 加本次文档和测试工具更新。复核时 HEAD、工作区 diff 与清单记录一致，12 个测试脚本的快照/工作区 SHA256 和两套共六个二进制 SHA256 全部匹配。

| 项目 | 本次结果 |
|---|---|
| Python UT | 46 项通过，含 15 项检查器测试 |
| Go 布局/遍历及其余降采样定向 UT | 两个独立阶段均通过，含空间估算和预留测试 |
| 三小时集群对照 | 240 项通过 |
| 三租户集群对照 | 759 项通过，租户为 11:17、11:18、12:17 |
| 160 条时间线、93 天集群对照 | 562 项通过 |
| 重启一致性 | 103 项通过；二十组快照、780 个查询点严格一致 |
| 原版组件兼容性 | 6 项通过 |
| 当前 89/113 字节文件检查 | short、tenants、long、restart 四组全部通过 |

E2E 合计 **1670 项，最大绝对误差为 0**。长周期最终为 4 个 part、160 个 TSID、5660 个单特征 Block、694200 个物理行、40 个 index。
多租户产物为 30 个 index（两分辨率 × 五特征 × 三租户），每个 row 保持同租户。
长周期产物未命中同列跨 index；同 TSID 跨相邻 index 由本次 `go-layout-ut` 的固定 fixture 验证，不将 feature 切换当作该覆盖。多租户 E2E 实际命中了同列不同 TSID 跨相邻 index。
本次验收范围是三项文件布局约束及现有写入、重复归并、字段查询和正常重启流程；仍保留下文的覆盖边界。

## 集群测试场景与判定

一键入口使用当前工作区作为候选，基线为 `v1.151.0-cluster`（`e7a3dc606a1d804cb3101fa73eb30cb0946ab2a9`）。具体 commit、工作区改动、脚本和二进制 SHA256 以对应运行 manifest 为准。降采样存储与字段查询版本均为 2；历史旧布局结果单独列在后文。

每侧运行真实的一个 vminsert、一个 vmstorage 和一个 vmselect，replicationFactor=1，绑定 loopback，使用独立目录。vmstorage 与 vmselect 均设置 dedup interval 为零。脚本默认使用 cluster 模式；保留的 single 模式不属于本轮验证范围。

主数值场景的原始输入仅使用有限浮点数，每条时间线在活跃区段内按 14、15、16 秒循环采样，时间戳唯一。本轮不验证同时间戳覆盖。先严格比较基线 raw 查询与实际输入，再独立计算 `5m/1h × last/sum/count/min/max` 的期望值；标签、序列集合、点数和时间戳必须相同，特征值容差为相对 `1e-10`、绝对 `1e-9`。

## 参考计算与查询语义

- 以 `timestamp // resolution` 分组，区间左闭右开。共享时间戳取区间内最大时间戳，last 取该样本的值；sum 使用 `math.fsum`，count 为原始样本数的浮点表示，min/max 为区间极值。空区间不产生摘要。
- `query.field` 使用 `5m:last`、`1h:count` 等十种组合，经 vmselect 的 tenant 查询路径传入。期望值不调用生产 accumulator 或 reader。
- `/api/v1/query` 使用 `metric[窗口]` 读取实际点，按共享时间戳比较。查询窗口左端不包含，为验证 `[start,end]`，窗口长度取 `end-start+1ms`。时间裁剪只判断共享时间戳，不重新计算部分格子的特征。
- `/api/v1/query_range` 使用裸 selector，`step=max_lookback=resolution`。每个求值时刻 `t` 选择 `(t-step,t]` 内的末点，响应时间戳为 `t`；空窗口不返回点。不能用摘要共享时间戳代替求值时间戳。
- 旧短周期脚本混淆了上述两类时间戳，且输入恰好位于求值网格而掩盖错误。本轮已修正，并用不对齐网格的输入及手算 UT 验证。

实际查询的固定手算样例，时间戳为相对输入起点的偏移：

| 区间 | 共享时间戳（ms） | last | sum | count | min | max |
|---|---:|---:|---:|---:|---:|---:|
| 首个 5m | 299000 | -3.5 | -99.75 | 21 | -6 | -3.5 |
| 第二个 5m | 599000 | -1 | -43.75 | 20 | -3.375 | -1 |
| 第三个 5m | 884000 | 1.375 | 4.75 | 19 | -0.875 | 1.375 |
| 首个 1h | 3584000 | -0.375 | -146.625 | 240 | -6 | 6 |

这四组共 20 个字段值已直接与重启后的 HTTP 响应比较，通过；未使用聚合函数生成这些固定期望值。

## 脚本与场景

| 文件（位于 lib/storage/testdata） | 验证内容 |
|---|---|
| `downsampling_e2e.sh` | 串行执行完整构建与验证流程；统一证据、错误退出及子进程清理 |
| `downsampling_cluster.py` | 三组件启动、tenant 路由、写入计数、进程存活及退出清理；HTTP import 成功后仍等待 vmstorage 接收全部样本 |
| `downsampling_compare.py` | 两条时间线、三小时、1440 个样本；按第一小时、第三小时、第二小时分批倒序写入，逐轮归并，再重写、重启；全部十字段均比较 matrix 与 query_range |
| `downsampling_cluster_tenants.py` | 同一 vmstorage 上的 `11:17`、`11:18`、`12:17` 三个 tenant，共六条时间线、4320 个样本；同名指标使用不同值，验证字段隔离及空租户；检查重写与重启的 part 代次 |
| `downsampling_multiseries_compare.py` | 160 条时间线、93 天、2381868 个样本；4 条持续采样，其余包含按日、间断、提前结束和延后开始场景，活跃片段内仍按 14～16 秒采样；跨月、标签筛选、空结果、裁剪窗口、多线裸查询及重启 |
| `downsampling_restart.py` | 两条时间线、三小时、1440 个样本；归并后查询十字段，重启全部三组件，再查询并严格比较二十组非空快照 |
| `downsampling_cluster_compatibility.py` | 两个混部方向的原生 matrix/range 查询，以及新 vmselect 对旧 vmstorage 的字段请求明确失败；失败响应必须包含字段 RPC 标识，且本次请求新增的 vmstorage 日志须明确记录 unsupported rpcName；普通连接错误不能通过 |
| `downsampling_inspect.py` | 当前 89/113 字节、feature 0..4 独立文件检查；五列共享时间戳、全局列布局、单 row 租户、连续负载与索引遍历覆盖 |
| `test_downsampling_inspect.py` | 独立二进制 fixture 验证解析、覆盖报告，以及错列、旧编号、跨租户和损坏文件拒绝 |

长周期按周写入和归并，每日预留 45 秒内的样本在最后补写。补写与主批次的时间戳集合互斥，用于验证同格子内 raw 与已有摘要的持续归并，特别是 sum/count 的贡献累计。93 天表示输入历史跨度，不表示测试持续运行 93 天。

写入阶段验证前调用 force_flush/force_merge，等待本次异步 merge 完成及 metrics 缓存过期，并确认无 inmemory 行、无活动 merge、每个非空月份只有一个 file part。重启一致性场景在重启后直接查询原有 part。测试保存实际请求、响应、期望值、输入和活动 part metadata。

### 重启一致性

`downsampling_restart.py` 复用短周期输入，使用非零 tenant `11:17`，按 14、15、16 秒循环采样，无重复时间戳。三批数据全部写入、落盘和归并后，确认活动 part 为降采样格式、物理行数为 390，并记录查询快照。每次多线查询覆盖两个 TSID，分别执行十个字段的 matrix selector 与裸 `query_range`，共二十组快照、780 个查询点。

脚本正常关闭 vmselect、vminsert、vmstorage，确认旧进程全部以 0 退出，再用相同命令和原数据目录启动三个新进程。记录重启前后 PID，确认活动 part 路径、metadata 和文件大小不变。重启后直接查询，不再次写入或触发归并；查询同时设置 `nocache=1` 和 `-search.disableCache=true`。

重启前后仅规范化时间线顺序，标签、点数、时间戳及响应中的数值字符串必须严格相等，不使用容差。另将前后两次响应分别与独立数学参考比较，防止同样错误的两次结果通过。参考值比较使用本文约定的浮点容差。空结果、重复时间线和非有限值不得作为本场景的通过结果。

单独执行时提供当前分支已构建的三个集群组件目录：

```sh
python3 -B -E lib/storage/testdata/downsampling_restart.py --candidate /path/to/cluster-binaries --output /tmp/vm-downsampling-restart-result
```

输出包含输入 fixture、每次 HTTP 请求和响应、`before-restart-snapshot.json`、`after-restart-snapshot.json`、`restart-processes.json`、part 清单及 `summary.json`。快照和进程证据位于 `candidate/` 下；summary 记录二十组严格比较及各自 SHA256。本场景验证正常停机后的持久化读取，不覆盖崩溃恢复。

## UT 与复现方法

Python UT 使用手算结果验证五特征、格子边界、共享时间戳、求值网格、空窗口、count 累加及输入性质；另验证集群路由、写入计数、错误处理和资源清理。重启比较器 UT 验证仅时间线顺序变化可以通过，标签、时间戳、点数或数值字符串变化必须失败；容差内的微小数值变化同样失败。在候选源码目录执行：

```sh
python3 -B -W error::ResourceWarning -m unittest discover -s lib/storage/testdata -p 'test_downsampling_*.py' -v
go test -p 4 ./lib/storage ./lib/vmselectapi ./app/vmstorage ./app/vmselect/netstorage ./app/vmselect/prometheus -run 'Test(Downsample|Downsampling|CheckDownsampling|MustOpenStorageDownsampling|EstimateDownsample|ReserveDownsample)' -count=1
```

在候选源码根目录执行一键入口；从其他目录使用脚本的绝对路径调用，入口按自身路径定位源码：

```sh
./lib/storage/testdata/downsampling_e2e.sh
```

入口自动创建全新的系统临时目录，并在终端打印输出路径。结束时应显示 `结果：passed`，且输出目录的 `manifest.json` 中 `status` 为 `passed`、`exit_code` 为 `0`。
各阶段日志位于 `logs/`，集群场景的 `summary.json` 与 `inspection.json` 位于相应子目录；失败或中断时保留证据。

指定保存位置时，目录必须尚不存在：

```sh
./lib/storage/testdata/downsampling_e2e.sh --output /tmp/vm-downsampling-result
```

依赖 bash、Python 3、满足 go.mod 要求的 Go、Git、系统 libzstd，以及本地 `v1.151.0-cluster` tag。入口自动创建基准 detached worktree，构建两套三组件，运行 Python UT、Go 定向 UT、独立 Go 布局/遍历 UT、五组 E2E 和四组文件检查。长周期固定使用 93 天、160 条时间线、4 条持续采样线。新格式每个 index 最多容纳 736 个单列 header；该场景不保证同列跨 index。一键中的独立 Go 布局/遍历阶段强制验证同列跨 index，集群长周期仍要求同 TSID 超过 8192 个 5m 摘要行且跨 Block、多物理 TSID 和跨月。所有阶段串行运行，关闭 Python 优化模式，保证测试断言执行。

每个阶段的命令、日志、退出码和结果写入输出目录；`manifest.json` 保存源码 commit、工作区状态、脚本和 binary SHA256 及总结果，`test-sources/` 保存测试脚本副本，Python 测试与文件检查实际从该副本执行。失败立即停止，返回非零退出码；中断或失败时清理本次阶段的子进程，结束时移除本次基准 worktree，保留二进制、数据和证据。

检查器的 `--data-dir` 指向直接包含 small/big 的目录。短周期、租户场景的预期 TSID 数量分别为 2、6；租户检查需重复传入三个 `--expected-tenant`，匹配精确集合。系统须提供 libzstd，检查器仅用它解压。

## 历史旧布局结果与证据（本次未重跑）

历史完整一键运行证据：`/private/tmp/vm-downsampling-oneclick-restart-20260909`，14 个阶段全部通过。两套组件由入口重新构建，使用全新数据目录；候选为 `7d7887c44` 加当时测试修订，未修改生产 Go。测试入口维护在 `experimental/downsampling` 分支的 `lib/storage/testdata/`。以下数字仅对应该历史 manifest，不适用于当前布局。

| 项目 | 结果 |
|---|---|
| Python UT | 31 项通过，含 8 项重启比较器 UT，启用 ResourceWarning 错误检查 |
| Go 降采样定向 UT | 通过；app/vmstorage 完成编译，无匹配测试用例 |
| 三小时集群对照 | 240 项通过 |
| 三租户集群对照 | 759 项通过 |
| 160 条时间线、93 天集群对照 | 562 项通过 |
| 重启严格一致性 | 103 项通过；二十组快照、780 个查询点严格一致 |
| 原版组件兼容性 | 6 项通过 |
| 四组独立文件检查 | 通过 |
| 实际输入间隔检查 | 通过 |
| 固定手算值对照实际响应 | 20 个字段值通过 |

集群 E2E 共 **1670 项**，所有数值比较的最大绝对误差为 **0**。重启测试的前后快照 SHA256 全部相同，两个完整快照文件的字节内容亦相同；结果见 `restart/summary.json`，三个组件的正常退出和新 PID 见 `restart/candidate/restart-processes.json`。长周期最终包含 160 个 TSID、4 个月份的 part、5660 个 Block、694200 个物理行和 12 个 index。

一键输出中的 `manifest.json` 保存源码 commit、binary 与脚本 SHA256、命令和结果索引；各场景的 `summary.json` 保存逐项检查，`inspection.json` 保存物理文件及遍历覆盖证据；`compatibility` 保存本次请求对应的 vmstorage 错误日志摘录。独立输入复核及手算响应证据另保存在 `/private/tmp/vm-downsampling-e2e-review-20260909/input-validation.json` 和该目录下的 `short/golden-values.json`。

一键入口还使用假 Go 命令验证故障处理：构建失败并遗留持有 stdout 的后代、SIGINT、SIGTERM。三个场景分别返回 1、130、143，状态分别为 failed、interrupted、interrupted；均清除阶段子进程与本次基准 worktree。验证命令为 `python3 -B -E /private/tmp/vm-downsampling-oneclick-harness-checks-20260909/check_runner.py --run`，结果见同目录的 `summary.json`。该验证独立于数值 E2E。

## 覆盖边界

- 当前打开校验遍历全部 index，但不解码 timestamps/values 数值；五路各保留当前 index，metaindex 仍整体驻留且受 64 MiB 上限约束。
- 检查器不解码 timestamps/values 的数值内容；数学正确性由 Go 数值测试和本次集群 E2E 的独立参考比较验证。
- 历史文件检查只验证旧布局索引和负载引用结构；历史数学正确性由十字段查询与独立参考比较验证，不能外推到当前布局。
- 历史 E2E 命中同 TSID 跨 Block、不同 TSID 跨 index、多个月份；当前同一 resolution/feature 内的同 TSID 和不同 TSID 跨 index 由一键 Go 布局/遍历阶段验证；集群产物的 coverage 另行如实报告。
- 历史字段查询在完整归并后验证，不证明跨未归并 part 查询侧再聚合、raw/摘要混合查询、多 vmstorage 副本故障切换或崩溃恢复。
- Python 数值参考限于有限值；NaN/Inf 需 Go 特殊值 UT，不能由有限值 E2E 推断。
- 已运行完整 storage、降采样定向 race 和当前格式的完整集群 E2E；完整 storage race 与全仓库回归不属于本次已运行结果。
