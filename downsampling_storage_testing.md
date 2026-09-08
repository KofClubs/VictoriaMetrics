# 降采样存储测试记录

## 续接入口

- 当前状态：UT 与端到端方法已补齐；新增 180 TSID 遍历 UT、定向 race 和相关包回归通过。160 条时间线、93 天时间跨度的最终双实例测试 562 项通过，独立索引检查六项覆盖全部命中。本轮任务完成，最终证据见本文末尾。
- 设计：[downsampling_storage_design.md](downsampling_storage_design.md)。实现：[downsampling_storage_implement.md](downsampling_storage_implement.md)。
- 基线：本地 `v1.151.0` tag 已存在，当前分支初始提交为 `83fc70c6aced8c99a0a445a872ee891191b98517`；原版构建使用隔离的原始源码，不包含工作区修改。
- 所有实例仅绑定 loopback，使用独立临时数据目录、端口及日志；不读取或覆盖用户已有存储。
- 恢复任务时先检查本文最后的进度、构建路径、运行进程和日志，不因二进制存在而推断测试已经完成。

## UT 测试方法

UT 从数学计算、文件字节、reader/merger、查询和存储生命周期分别验证。每个测试必须说明构造输入、独立期望值及要捕获的错误；不能只断言生产 writer 与 reader 的往返结果相同。

| 层次 | 构造方法 | 核对方式及主要错误 |
|---|---|---|
| 数学计算 | 边界时间戳、乱序样本、同时间戳多值、NaN/Inf、多个部分摘要及固定种子随机输入 | 原始输入按 `timestamp / resolution` 独立分桶；核对共享最大时间戳、last 平局规则、sum/count 全贡献累加和逐列特殊值传播；再比较多轮摘要归并 |
| Block 复用 | 五个精度、scale 各异的单值字段，含低精度时间戳与高精度数值 | 检查 writer 中真实 Block 状态与原生 header，经 BlockRef、Block 解码后逐样本核对，再检查 CopyFrom、再次编码和 Reset |
| 文件结构 | 多 TSID、多分辨率、多 index、非零负载与常量零负载 | 不调用降采样 reader/header decoder，以固定 91/96 字节偏移独立解析文件，逐项验证标识、offset/size、五列及共享时间戳、物理统计与无额外尾部 |
| 归并与落盘 | raw 与 summary 混合源、多个重叠 part、跨窗口和跨 Block 输入、迟到写入及再次 merge | 参考源贡献独立计算目标摘要；核对唯一计入、保留 count、物理 rows 统计，以及 inmemory 保持 raw、所有文件目标为摘要 |
| 查询与遍历 | 全部十种字段、连续及非连续 TSID 集合、缺失 TSID、不同 TimeRange、同 reader 复用 | 比较完整 TSID 集合及每条线的全部时间戳和值，不能只比较总行数；检查 Block/index 切换、首尾退出、过滤和状态清理 |
| 失败与恢复 | 截断、非法版本、缺失/错位字段、相邻 index 空洞或重叠、重复批次键、空间不足及发布故障 | 无效新格式必须报错，不能退化为 raw；合法 raw 重叠边界保留。取消/失败不删除活动源，发布后的目标不被误删，重启可恢复 |

普通回归命令在仓库根目录执行：

```sh
go test ./lib/storage ./lib/encoding ./lib/decimal ./app/vmstorage ./app/vmselect/netstorage ./app/vmselect/prometheus ./app/vmselect/promql ./app/victoria-metrics -count=1
```

定向检查可按以下命令重现。命令均为实际可执行形式；不得将可选参数方括号原样交给 shell。

```sh
go test ./lib/storage -run 'Test(Downsample|CheckDownsamplingOpen|MustOpenStorageDownsampling|EstimateDownsample|ReserveDownsample)' -count=1
go test ./lib/storage ./app/vmselect/prometheus ./app/vmselect/promql -run '^TestDownsampleQueryField' -count=1
go test -race ./lib/storage -count=1
go test -race ./app/vmselect/prometheus ./app/vmselect/promql -run '^TestDownsampleQueryField' -count=1
go vet ./lib/storage ./app/vmstorage ./app/vmselect/prometheus ./app/vmselect/promql ./app/vmselect/netstorage
```

同一 checkout 不同时启动多个全量 storage 测试进程：原有测试使用部分固定临时路径。定向测试使用 `t.TempDir()`；race 检查并发数据访问，不能替代数学或文件格式断言。普通数值使用精确可表示样例或明确的浮点容差；低精度样例按相同编解码边界比较，不要求不同编码次数逐位一致。

## 端到端测试方法

1. **确定二进制来源。** 在独立、无修改的 `v1.151.0` worktree 构建原版，在当前分支构建候选。保存 tag/commit、Go 版本、构建命令、源文件及 binary SHA256。候选生产文件变化后重建并重跑，不能继续引用旧 binary 的结果。
2. **隔离运行。** 两个实例绑定不同 loopback 端口，使用独立数据目录和日志。原版关闭降采样，候选启用 `-storage.downsampling.enabled=true`；均使用 `-dedup.minScrapeInterval=0`。测试输入须处于 retention 允许的时间范围内。
3. **写入同一输入。** 将完全相同的 metric、标签、毫秒时间戳及数值经 `/api/v1/import` 写入两端。保存输入及分批顺序，包含正常批次与迟到追加。此测试使用历史数据的时间跨度，不等同于相同时长的持续压力运行。
4. **确认存储状态。** 调用 `/internal/force_flush` 和 `/internal/force_merge`，等待 active merge 为零、inmemory 行数为零。单月测试要求一个活动 file part；多月测试要求每个非空 month partition 各一个活动 file part，不能要求整个存储只有一个 part。读取活动清单和 metadata，验证候选每个 part 的 v3 标识、行数、Block 数和两种分辨率。
5. **核验原始基线。** 对裸区间 selector 的返回结果按完整标签集归组，与已写入输入逐条核对，先证明基线和 fixture 正确。含同时间戳重复输入的专项保留原版实际响应，以写入贡献列表为 sum/count 的参考，避免查询行为消除重复贡献。
6. **建立独立参考。** 普通样例从原版裸查询的实际点按标签集、分辨率和 bucket 分组，独立计算 last/sum/count/min/max 及最大时间戳。参考计算不调用候选存储算法，也不以候选结果生成期望值。
7. **核验全部查询。** 候选分别指定十种 `query.field`。裸区间 selector 比较保存时间戳与字段值；`query_range` 使用裸 metric selector，按标准 PromQL lookback 和求值网格构造期望。多序列查询必须核对完整标签集、序列集合、每条线的点数、全部时间戳与值，拒绝重复返回的标签集、漏线、串线或多余样本。
8. **重复生命周期。** 在递增写入里程碑、迟到追加、已有摘要再次 merge，以及正常关闭重启后重复上述核验。全量 selector、非连续标签子集、空结果、局部时间范围和跨月窗口均应覆盖。
9. **保留证据。** 将每次请求参数、完整 HTTP 响应、独立期望、metadata、实际 index 覆盖情况、日志和阶段检查点写入新产物目录。异常时保留失败阶段和原因并关闭测试进程；只有实际完成的检查记为通过。

单次数值比较使用相对容差 `1e-10`、绝对容差 `1e-9`，标签、序列集合、点数和实际保存时间戳严格匹配。该容差不是对任意低精度配置的承诺。query_range 的返回时间为求值时间，不能误当作磁盘共享时间戳。

## 验证目标

1. 每个分辨率、每个特征值使用独立 `storage.Block`；索引保存分辨率与特征标识，由 reader 选择。
2. 同一分辨率、同一批区间的五个 Block 共享一份持久化时间戳负载，各自保留 values、scale、precision、offset 和 size。
3. 测试参数 `query.field` 指定分辨率与特征，约定值为 `5m:last`、`5m:sum`、`5m:count`、`5m:min`、`5m:max` 及对应的 `1h:*`。非法值必须报错。
4. 分别构建开源 v1.151.0 和当前分支，写入同一批输入；基线保存原始数据，当前分支启用降采样。
5. 使用裸 metric selector 的 PromQL 查询，不用 PromQL 聚合函数代替待验证的存储聚合。通过区间 selector 检查保存的点，并用 query_range 裸线验证 HTTP 查询链路。
6. 在明确完成落盘后比较结果，覆盖多轮写入、迟到数据、强制 merge 和重启。不能把 inmemory 原始值当作降采样结果。

## 判定规则

- 基线裸查询必须与原始输入一致；存在重复时间戳的专项样例单独记录原版查询合并行为。
- 降采样期望值由测试脚本独立分桶计算。last 取最大时间戳，时间戳相同时取较大值；sum/count/min/max 按全部输入贡献计算。
- 五个特征的共享时间戳必须一致，且等于对应区间内最大输入时间戳；只比较被查询范围实际覆盖的摘要。
- 数值采用明确浮点容差，记录实际误差，不要求 raw 的裸值与 sum/count 直接相等。
- 原始 HTTP 响应、独立参考结果、命令及汇总保存在测试产物目录；本文件记录路径和结论。
- `query.field` 用于本轮单机存储验证，不将其等同于完整的分布式查询、跨未合并 part 聚合或生产查询设计。

## 阶段状态

| 阶段 | 状态 | 证据 |
|---|---|---|
| 单特征 Block 重构与 UT | 完整回归通过 | 91 字节单特征 header、原生 Block 编解码、实际布局和损坏输入 UT |
| 原版 v1.151.0 构建 | 已完成 | `/tmp/vm-downsampling-comparison-20260908/vm-original` |
| query.field 与查询选择 | 定向 UT 与 race 通过 | 单机 HTTP 参数贯通至存储 reader，隔离查询缓存 |
| 当前分支构建 | 已完成 | `/tmp/vm-downsampling-comparison-20260908/vm-current` |
| 同输入双实例对照 | 最终 175 项通过 | `comparison-final/summary.json`，与最终候选 binary SHA256 一致 |
| 回归、race 与结果审查 | 全部通过 | 基础代码全量 storage race 169.678s；新增遍历 UT race 4.225s，加入新测试后的相关包回归通过 |
| 多 TSID 遍历 UT | 已完成 | 180 TSID、长序列跨 Block/index、高位分组切换、过滤及对象复用 |
| 多线长时间跨度端到端 | 最终 562 项通过 | 160 条线、93 天、四个月、全量长跨度 query_range、迟到写入、rewrite 和重启 |
| 实际文件索引覆盖 | 六项全部命中 | 最终目录 index-inspection.json；同 TSID/不同 TSID 跨 index、单 TSID 多 Block、多 partition |

## 实施检查点

- 本轮已将核心文件层、查询参数和对照脚本分为独立工作范围；主执行者负责归并统计、空间预算、接口集成及本记录。
- 新文件格式须区分旧的未发布五列格式，避免用同一版本号解释不同结构。旧原始格式的读取退化保持有效。
- 文件行数与 Block 数量必须重新核对，不能继续沿用“一个五列组合计作一个 Block”的旧口径。

## 原版构建结果

- 源码目录：`/tmp/vm-downsampling-comparison-20260908/original-source`，detached worktree，提交 `83fc70c6aced8c99a0a445a872ee891191b98517`，对应 `v1.151.0`。
- 工具链：`go version go1.27.1 darwin/arm64`。
- 命令：`go build -o /tmp/vm-downsampling-comparison-20260908/vm-original ./app/victoria-metrics`，在上述原版源码目录执行，已成功完成。
- 构建记录：`/tmp/vm-downsampling-comparison-20260908/build-manifest.json`。
- 对照数据：核心序列覆盖连续 36 个 5m 区间、共 3h；每个区间先写起点、内部点、右端点前 1ms，再分两轮追加乱序历史点。重复时间戳使用单独序列，避免将基线查询的合并行为误当作原始输入贡献。

## 新格式与接口检查点

- 单特征索引条目使用 91 字节：分辨率 8 字节、feature ID 1 字节、时间戳 precision 1 字节，以及原始 `blockHeader` 的 81 字节。五个条目关联为一个聚合批次，共 455 字节；每个条目对应一个真实 `Block`。
- 文件格式使用版本 3，与前轮未发布的五列版本 2 区分；时间戳负载仍只保存一份，五个 header 指向同一 offset/size。
- 文件 `RowsCount` 和 `BlocksCount` 按单值 Block 统计，各为聚合批次口径的五倍。归并统计及空间估算已同步调整并通过统一回归。
- 查询使用 `query.field` 选择后进入原始 `BlockRef/Block` 单值读取链路。对独立时间戳 precision 的兼容必须有 UT；查询缓存禁止跨字段复用。

## 集成前验证检查点

- 文件层定向 UT：`go test ./lib/storage -run '^TestDownsample(NativeBlockReuse|PreviousFormatRejected|FieldHeader|File|Header|Metadata|Reader|Codec|V3|Writer|Pool)' -count=1` 通过（1.384s），日志 `/tmp/vm-downsample-field-codec-r3.log`。
- 查询普通 UT 及 race：`go test [-race] ./lib/storage ./app/vmselect/prometheus ./app/vmselect/promql -run '^TestDownsampleQueryField' -count=1` 均通过；覆盖全部字段、参数错误、标准 BlockRef 往返、独立精度、raw 隔离、对象复用和缓存禁用。
- 原版预验证通过 20 项检查：`/tmp/vm-downsampling-comparison-20260908/baseline-run-2/summary.json`。这仅验证基线与测试输入，不能代替双实例验收。
- 聚合中间态统一命名为 `downsampleBatch`，代码文件为 `downsample_batch.go`；实际持久化字段仍为原生 `storage.Block`。
- 此集成检查点的后续工作均已执行，结果见基础双实例结果；新的多时间线验证单独记录。

## 首轮双实例与回归结果

- 当前分支构建命令：`go build -o /tmp/vm-downsampling-comparison-20260908/vm-current ./app/victoria-metrics`，已通过；日志 `/tmp/vm-downsample-v3-build-20260908.log`。
- 相关包回归全部通过：`go test ./lib/storage ./lib/encoding ./lib/decimal ./app/vmstorage ./app/vmselect/netstorage ./app/vmselect/prometheus ./app/vmselect/promql ./app/victoria-metrics -count=1`，storage 26.475s，日志 `/tmp/vm-downsample-v3-regression-20260908.log`。
- 首轮双实例对照通过 175 项，三轮输入、摘要重写和重启均通过，产物 `/tmp/vm-downsampling-comparison-20260908/comparison-run-1/summary.json`。全部测试进程已关闭。
- 收尾审查发现防损坏边界：v3 索引必须拒绝相邻重复批次键；随后已补严格递增校验及故障 UT。正常 writer 不产生此输入，首轮对照未受影响；修正后重建并再次执行双实例对照，确保测试产物对应最终生产代码。
- 明确区分原始点与裸线：区间 selector 核验磁盘共享时间戳，query_range 保留 PromQL 的 lookback 和求值时间网格。

## 基础双实例结果（2026-09-08，175 项）

最终二进制对照通过 **175 项检查**，所有已比较数值的最大绝对误差为 **0**。数值容差为相对 `1e-10`、绝对 `1e-9`；样本数量及保存时间戳严格匹配。该结论适用于本轮构造数据，不表示任意浮点输入或不同编码次数逐位相同。

- 最终产物：`/tmp/vm-downsampling-comparison-20260908/comparison-final`，入口为 `summary.json`。
- 候选构建记录：`/tmp/vm-downsampling-comparison-20260908/candidate-build-manifest.json`，包含构建命令、工具链、修改的 Go/Python 文件哈希及二进制构建信息。
- 原版 SHA256：`5f7e6cd5b3dd6c20d83f2253d0a66744f91cde6a93a2584546c6876b06c1f812`。
- 候选 SHA256：`5ba6b06d7f45cd3a09c5b6b191dd010912d558ce1b23d658cbf7ea78170cc5fa`。
- 原版 worktree 再次检查无改动；最终对照记录的候选哈希与上述构建记录一致。
- 原始 HTTP 请求与完整响应在 `original/`、`candidate/` 下；`fixture.json` 为输入，分阶段参考结果及 `*-parts.json` 保存独立聚合结果和实际文件 metadata。
- 所有实例仅使用此次临时目录；测试结束后均已关闭。临时产物可能被系统清理，重现入口为仓库内的 `lib/storage/testdata/downsampling_compare.py`。

实际执行命令：

```sh
python3 lib/storage/testdata/downsampling_compare.py \
  --original /tmp/vm-downsampling-comparison-20260908/vm-original \
  --candidate /tmp/vm-downsampling-comparison-20260908/vm-current \
  --output /tmp/vm-downsampling-comparison-20260908/comparison-final
```

再次执行时须使用不存在的新 `--output` 目录。脚本自动选择 loopback 端口，并为两个实例使用隔离目录。

| 阶段 | 原版 RowsCount / BlocksCount | 当前分支 RowsCount / BlocksCount | 结果 |
|---|---:|---:|---|
| 第一轮写入并 merge | 120 / 2 | 240 / 20 | 通过 |
| 第二轮迟到写入并 merge | 204 / 2 | 240 / 20 | 通过 |
| 第三轮迟到写入并 merge | 288 / 2 | 240 / 20 | 通过 |
| 单 part 再次 merge | 288 / 2 | 240 / 20 | 通过 |
| 关闭后重启 | 288 / 2 | 240 / 20 | 通过 |

每个阶段均确认只剩一个 file part、inmemory 行数为零。当前分支始终声明 `FormatVersion=3`，240 个物理行等于 48 行五特征摘要；两条 TSID、两种分辨率、五个特征构成 20 个原生 Block。最终 `timestamps.bin` 为 31 字节，`values.bin` 为 18 字节，`index.bin` 为 464 字节。该低基数构造输入不用于评价生产压缩率。

### 裸查询与具体数值

HTTP 裸线查询使用 `query=downsampling_compare_value`，通过 `query.field=5m:sum` 等选择字段，分别以 `step=5m`、`step=1h` 检查十种组合。另使用裸区间 selector `downsampling_compare_value[10800000ms]` 核对实际保存的时间戳。测试不使用 PromQL 聚合函数计算待验证结果；脚本依据输入独立分桶。

第一行 5m 摘要的输入时间相对区间起点为 `0, 1, 60000, 120000, 180000, 240000, 299999` 毫秒，对应值为 `-21.25, -20.75, -21.125, -20.625, -20.25, -20.125, -21`。裸区间查询与独立参考的结果如下，重启后保持一致：

| 分辨率及样例 | 共享时间戳（ms） | last | sum | count | min | max |
|---|---:|---:|---:|---:|---:|---:|
| 首个 5m 区间 | 1788739499999 | -21 | -145.125 | 7 | -21.25 | -20.125 |
| 首个 1h 区间 | 1788742799999 | -7.25 | -1164 | 84 | -21.25 | -6.375 |

重复时间戳专项在同一时间戳写入 `[-8, 7, 12, -100, 3, 12]`，得到 `last=12, sum=-74, count=6, min=-100, max=12`。因此全部输入贡献均参与聚合，同时间戳 last 的选择规则没有使 sum/count 丢失贡献。

### 文件结构与 Block 复用证据

- `TestDownsampleNativeBlockReuse` 检查 writer 的五个实际 `Block`、原生 81 字节 header、编码与解码状态、独立精度，以及 CopyFrom、再次编码和 Reset。
- `TestDownsampleQueryFieldBlockRef` 与独立精度测试覆盖标准 `BlockRef.Marshal → Init → MustReadBlock → Block.UnmarshalData` 链路；reader 根据扩展索引选择单字段。
- `TestDownsampleFilePhysicalLayout` 直接读取五个实际文件，以固定字节位置检查 91 字节单特征 header、96 字节 metaindex 和 metadata，独立拼接预期 values/timestamps 字节；不调用降采样 reader 或 header decoder。多 index 样例覆盖 **3300 个 Block、16500 个物理行、6 个 metaindex 行**。
- 同一批次五个 header 引用相同时间戳 offset/size，每列的 values offset/size、scale、precision 和 marshal type 单独验证。整个文件必须恰好等于预期负载，不能含重复时间戳副本、交错值或未引用尾部。
- 收尾新增重复批次键故障 UT，同 index 与跨 index 的重复键均被拒绝；合法 raw 的相同 Block 边界仍完整保留。定向验证通过（0.718s），日志 `/tmp/vm-downsample-duplicate-batch-r2.log`。

### 最终检查状态

- 全量相关包普通回归已通过；索引收尾修正另有定向 UT 通过。最终代码执行 `go test -race ./lib/storage -count=1` 通过（169.678s），日志 `/tmp/vm-downsample-v3-final-race-20260908.log`。
- `go vet ./lib/storage ./app/vmstorage ./app/vmselect/prometheus ./app/vmselect/promql ./app/vmselect/netstorage` 通过，日志 `/tmp/vm-downsample-v3-final-vet-20260908.log`。
- 完整生产查询、跨未合并 part 的查询聚合、分布式字段协议与生产负载性能不属于本轮对照结论；测试在每阶段 force_flush/force_merge 完成后比较完整摘要。

- 最终源文件哈希与候选构建记录再次核对一致，Go 格式、`git diff --check` 与文档本地链接检查通过。IndexDB、TSID、raw_row、inmemory_part、block_header、part_header 及原始 merge 实现未修改。
- 本轮工作已完成：单特征 Block 实现、UT、双二进制构建、175 项裸查询对照、完整 storage race、静态检查及三份文档均已同步。仓库保留未提交改动，测试实例已退出。

## 多时间线与长时间跨度检查点

- 当前阶段：全部工作已完成，最终结果为 562 项通过；以下目标与中间记录用于解释覆盖构造，最终目录为 `multiseries-long-range-final`。
- 输入目标：160 条带标签时间线、93 天历史时间跨度，按周 HTTP 写入；4 条密集线每 5m 两个原始点，其余包括稀疏、间断、提前结束及延迟出现的时间线。完整 31 天 month 中，每条密集线有 8928 个 5m 摘要，超过单 Block 的 8192 行上限。
- 端到端检查目标：约 31/62/93 天的递增里程碑、全历史迟到追加、摘要重写、重启；十字段全量和过滤查询；每个非空 month 独立完成 merge。
- UT 精确边界目标：180 个 TSID，第 144 个 TSID 的 8229 个摘要按 8192 与 37 两个 Block 放在相邻 index 两侧，核验同 TSID 内续接及随后切换 TSID；两种分辨率、全部五特征及 reader 重置均覆盖。
- 索引证据目标：独立解析实际 v3 文件，证明同 TSID 多 Block、跨 TSID 多 index 和多个 month partition。端到端是否自然命中同 TSID 跨 index 如实报告；该特定边界由 UT 显式构造。

### TSID 遍历 UT 完成记录

新增 `lib/storage/downsample_iteration_test.go`，包含 `TestDownsampleIterationMultiTSID` 与 `TestDownsampleIterationReaderReuse`。输入为 180 个完整 TSID，每种分辨率 9123 行摘要、181 个聚合批次；实际文件共 1810 个单特征 Block、91230 个物理值行、4 个 index。第 144 个 TSID 有 8229 行，按 8192 与 37 划分两个 Block，并通过 metaindex 首末 TSID 断言确认它们分处相邻 index。

- TSID 的 MetricGroupID、JobID、InstanceID 分别分组变化；跨 InstanceID 时 MetricID 反向跳变。fixture 额外断言实际存在该逆序，避免只验证 MetricID 排序。
- 数值由分辨率、TSID 序号、feature 和行号组成可区分的确定性编码，包含可精确表示的 0.25 小数。预期直接由这些维度计算，不调用 reader/merger 生成。
- 十个字段逐样本检查完整 TSID、时间戳及值；既有 `BlockRef.Marshal → Init → MustReadBlock → Block.UnmarshalData` 链路实际参与测试。
- 过滤包括全部、非连续及缺失 TSID；Instance/Job/MetricGroup 的分组边界；前一 index 末点、后一 index 首点、跨 index 闭区间、相交但无点区间及序列末点。
- 在同一对象上执行 `5m → 1h → 5m`，先后读取边界、末尾、缺失及开头 TSID；检查重复 EOF、错误后 SetFilter 恢复和 reset 后重新读取，确认游标与错误状态不会残留。

实际执行并通过：

```sh
go test ./lib/storage -run '^TestDownsampleIteration' -count=1
# PASS，0.730s
go test -race ./lib/storage -run '^TestDownsampleIteration' -count=1
# PASS，4.225s
```

本次未发现生产代码缺陷；包含新 UT 的相关包完整回归及长时间跨度双实例验证随后均已通过，详见最终记录。

### 多时间线首轮结果与补充覆盖

- 输入跨度为 `2026-06-07T00:00:00Z` 至 `2026-09-08T00:00:00Z` 前 1ms，共 93 天。每条线具有 `__name__`、`series_id`、`cohort`、`pattern`、`job`、`instance` 完整标签；7 种 job 和 13 种 instance 触发物理 TSID 分组变化。
- 首轮实际运行 `downsampling_multiseries_compare.py --days 93 --series 160 --dense-series 4`，产物 `/tmp/vm-downsampling-comparison-20260908/multiseries-run-1`；day31、day62、day93、late-history、rewrite、restart 共六阶段 **414 项通过，最大绝对误差 0**。
- 包含新 UT 的相关包完整回归通过，storage 25.883s；命令与前述普通回归相同，日志 `/tmp/vm-downsample-multiseries-regression-20260908.log`。本轮未修改生产代码，已核对两个既有二进制哈希及候选构建的生产源哈希仍一致。
- 独立文件审查首轮通过：4 个活动 part、4 个 month partition、160 个物理 TSID、6050 个单特征 Block、694200 个物理行、14 个 index。完整 7 月和 8 月的 4 条密集线，每个字段有 8928 行，分别拆为 `[8192,736]`；真实文件还命中两处同 TSID 跨相邻 index、四处不同 TSID 跨相邻 index。报告为产物目录的 `index-inspection.json`。
- 首轮全量十字段查询使用裸区间 selector，query_range 只验证一条密集线。为补齐标准 PromQL 多线求值的覆盖，正在增加全量及非连续子集的 query_range，逐线验证稀疏/间断序列的 lookback 与空点删除，再进行最终完整重跑。
- query_range 参考规则已对照既有 `rollup.go`：显式 `max_lookback=step` 时窗口为 `(求值时间-step, 求值时间]`，取窗口内末点；无点时不返回该点，全部求值点为空的序列不返回。测试输入无 NaN/stale；不将此简化参考推广到任意 PromQL 表达式。

## 多时间线长时间跨度最终结果（2026-09-08）

本轮最终执行 **562 项检查，全部通过**，最大绝对误差为 **0**，实际耗时 **41.910s**。检查包括 546 次数据查询响应核验、12 次分阶段物理行数及 partition 核验、4 次 rewrite/restart 的活动 part 路径核验。时间戳、完整标签、序列集合和点数均严格匹配。

### 输入规模与阶段

- 160 条带完整标签的时间线，包含 7 种 job、13 种 instance；4 条密集线及 156 条稀疏线。稀疏线包括按日写入、间断、提前结束及延迟出现模式。
- 时间跨度为 `[2026-06-07T00:00:00Z, 2026-09-08T00:00:00Z)`，共 93 天；这是历史时间跨度的模拟，不是连续运行 93 天的压力测试。
- 按周及里程碑切分为 15 个普通写入批次，每批实际向两个实例导入相同 NDJSON 并完成 flush/merge；之后追加一批覆盖历史的 11760 个迟到样本。输入总计 248808 行，保存在 `input.ndjson`。
- 密集线每 5m 的两个普通样本位于区间末尾前 2ms 和前 1ms，迟到样本每天选择一个历史区间追加。稀疏线在选定日期和区间写入两个样本，再追加一个迟到样本。不同时间线的值按序号、日期和区间变化，能够识别串线及错位。

| 阶段 | 全历史序列数 | 原始输入/原版物理行 | 5m 逻辑摘要行 | 1h 逻辑摘要行 | 候选物理值行 |
|---|---:|---:|---:|---:|---:|
| day31 | 160 | 78600 | 39300 | 6564 | 229320 |
| day62 | 160 | 158994 | 79497 | 14025 | 467610 |
| day93 | 160 | 237048 | 118524 | 20316 | 694200 |
| late-history | 160 | 248808 | 118524 | 20316 | 694200 |
| rewrite | 160 | 248808 | 118524 | 20316 | 694200 |
| restart | 160 | 248808 | 118524 | 20316 | 694200 |

候选物理行始终等于 `(5m 逻辑摘要行 + 1h 逻辑摘要行) × 5`。迟到样本改变已有摘要的特征值，未产生新的 bucket；其 count 和 sum 的变化已逐线逐点核对。

### 查询范围与判定

| 查询场景 | 输入选择与覆盖 | 比较内容 |
|---|---|---|
| full | 每个阶段的完整已写入历史与全部 160 条线 | 裸区间 selector 的全部标签、每条线的全部保存时间戳及十字段值 |
| subset | `s000/s003/s022/s080/s159` 非连续子集 | 返回集合严格等于参考集合；不依赖 TSID 或响应数组顺序 |
| empty | 不存在的 `series_id` | 十字段返回空集合，不残留上次查询结果 |
| clipped | 从阶段中间日期偏移 42 分钟 123ms 开始的局部窗口 | 只保留覆盖到共享时间戳的摘要，不按原始贡献重新裁剪 |
| cross-month | 首个月边界两侧各 90 分钟 | 跨 partition 的 TSID 续接与闭区间点过滤 |
| bare-line | 密集线末 3h 的裸 query_range | 原始与候选的标准 lookback/求值网格 |
| multi-range | 全量 selector，分别覆盖完整 31/62/93 天以及后续完整 93 天历史 | 标准 PromQL 多线长查询实际穿过 TSID 内多个 Block/index 和多个 month；十组合均验证 |
| subset-range | 非连续子集末 48h | 密集/稀疏混合、提前结束或间断引起的空求值点与空序列 |

query_range 的 `step` 与 `max_lookback` 均等于指定分辨率；参考按每条线独立寻找 `(t-step,t]` 的末点。最终全量 5m:last 长查询返回 160 条线、118524 个点，与原始基线聚合及磁盘摘要逐点一致。末 48h 子集只应返回仍有有效点的 4 条线，该数量也有断言，不能要求所有历史存在的线在任意窗口都返回。

两端 rewrite 后每个月的活动 part 路径均实际改变，证明发生了重写；重启后路径均保持，同时全部查询结果保持正确。不能仅以 active merge 为零判断重写已经发生。

### 实际文件覆盖证据

独立工具按活动 `parts.json` 读取实际文件，使用系统 `libzstd` 解压后，以固定 91/96 字节偏移解析字段；不调用生产 header decoder，不读取 IndexDB，不以该工具替代查询数值验证。最终报告六个 coverage 字段全部为 true：

- 同一 TSID 在单个 month 的 5m 行数超过 8192，并由多个 Block 存储。
- 同一 TSID 实际跨越相邻 index。
- 同一分辨率包含多个 index。
- 不同 TSID 在相邻 index 间切换。
- 多个 month partition。
- 多个物理 TSID。

| month partition | 单特征 Block 数 | 物理值行 | index 数 |
|---|---:|---:|---:|
| 2026_06 | 1600 | 175500 | 4 |
| 2026_07 | 1620 | 238290 | 4 |
| 2026_08 | 1620 | 229320 | 4 |
| 2026_09 | 1210 | 51090 | 2 |

总计 4 个活动 part、160 个全局物理 TSID、6050 个单特征 Block、694200 个物理行、14 个 index。1210 份共享时间戳负载由 6050 个 header 引用。7 月和 8 月各 4 条密集线，每个字段都有 8928 行，实际分为 `[8192,736]` 两个 Block；命中 2 处同 TSID 跨 index、4 处不同 TSID 跨 index。索引排序、五特征 TSID 集合、逐字段行数、values 连续覆盖、单份时间戳和 metadata/metaindex 计数全部通过。具体 TSID、part 路径及 index 编号见报告，不依赖人为假设的 TSID 分配顺序。

### 重现命令与产物

两个二进制沿用上文已经构建的原版 v1.151.0 与候选版本。本轮没有生产代码修改；已再次核对原版 worktree 无改动、候选生产源哈希以及两个 binary 的 SHA256 均与构建记录一致。新增测试源的哈希单独保存在最终目录的 `test-run-manifest.json`。

```sh
python3 lib/storage/testdata/downsampling_multiseries_compare.py \
  --original /tmp/vm-downsampling-comparison-20260908/vm-original \
  --candidate /tmp/vm-downsampling-comparison-20260908/vm-current \
  --days 93 --series 160 --dense-series 4 \
  --output /tmp/vm-downsampling-comparison-20260908/multiseries-long-range-final
```

该命令记录本轮参数；重新执行必须指定不存在的新输出目录。默认结束日期为当前 UTC 日期起点；复现同一批时间戳时使用 `test-run-manifest.json` 的 `--base-ms`，并保证仍在 retention 范围内。

独立文件审查命令如下；需 Python 3 和系统 `libzstd`，不需要额外 Python 包。它应在测试实例关闭后执行，避免活动清单在扫描中变化。

```sh
python3 lib/storage/testdata/downsampling_inspect.py \
  --data-dir /tmp/vm-downsampling-comparison-20260908/multiseries-long-range-final/candidate/data/data \
  --output /tmp/vm-downsampling-comparison-20260908/multiseries-long-range-final/index-inspection.json \
  --expected-series 160 \
  --require same_tsid_5m_over_8192_rows_and_multiple_blocks \
  --require same_tsid_across_adjacent_indexes \
  --require multiple_indexes_within_one_resolution \
  --require different_tsids_across_adjacent_indexes \
  --require multiple_partitions \
  --require multiple_physical_tsids
```

| 产物 | 内容 |
|---|---|
| `multiseries-long-range-final/summary.json` | 最终 562 项检查、阶段规模、完整标签、浮点误差及 binary 哈希 |
| `fixture-config.json`、`input.ndjson` | 构造参数、完整标签及实际发送的分批输入 |
| `original/`、`candidate/` 中的 `*-request.json`、`*-response.json`、`*-expected.json` | 查询参数、完整原始 HTTP 响应及独立参考值 |
| `*-parts.json`、`*-metrics.txt`、`server.log`、`command.json` | 实际文件、落盘与 merge 状态、进程日志和启动参数 |
| `index-inspection.json` | 最终实际文件的物理 TSID/Block/index 覆盖与所有结构性断言 |
| `test-run-manifest.json` | 新增测试源哈希、复现命令、环境和最终结果哈希 |

上述路径均相对于 `/tmp/vm-downsampling-comparison-20260908/multiseries-long-range-final`；运行日志为同级 `multiseries-long-range-final.log`。此前 `multiseries-run-1` 和 `multiseries-final` 保留阶段性结果，最终结论只使用本节目录。

### 完成状态

- 新增遍历 UT 普通及 race 通过；加入新 UT 后的相关包完整回归通过（storage 25.883s）。
- 本轮 `go vet ./lib/storage`、Go 格式、Python 语法、差异、文档链接及构建/测试源哈希检查通过。
- 562 项长时间跨度端到端检查与六项实际文件覆盖均通过，两个测试进程正常退出。
- 未发现需修复的生产实现缺陷；本轮仅增加测试、测试工具和记录。长时间跨度正确性结论不等同于长期持续负载的性能或可靠性验收。
