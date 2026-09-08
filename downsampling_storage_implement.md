# 降采样存储实现记录

## 续接入口

- 设计依据：[downsampling_storage_design.md](downsampling_storage_design.md)。
- 代码基线：`multi-value`，`83fc70c6a`。
- 当前状态：已完成每个分辨率、每个特征对应一个真实 `storage.Block` 的重构，测试参数 `query.field` 已贯通。已有基础验证通过；新增 180 TSID 遍历 UT 与 race、160 条线/93 天跨度的 562 项端到端检查、最终独立文件索引审查及相关包回归均已通过，本轮任务完成。前轮五列物理 Block 方案不再作为目标架构；旧验证记录仅是历史检查点。本轮测试进展另见 [downsampling_storage_testing.md](downsampling_storage_testing.md)。
- 续接时先检查 `git status`、本文的阶段状态及未完成事项，再阅读对应代码和测试；不能仅依据已有文件判断功能完成。
- 本文由主执行者维护；每完成一个阶段、发生接口调整、测试失败或任务中断时更新。

## 固定约束

1. 同时输出固定 `5m/1h`，五特征为 `last/sum/count/min/max`，全部使用 `float64`；同一摘要行只保存一份最大时间戳。
2. 降采样与原始 dedup 互斥；全部实际输入参与计算。相同时间戳的 last 优先数值再取较大值；sum/count 仍累计全部贡献。
3. 所有 NaN 在降采样内规范化为 `decimal.StaleNaN`，按列传播；复用现有 decimal、values 和 timestamps 编解码及精度规则。
4. 保持原始 inmemory 布局、编解码、merge、IndexDB 标签与 TSID 发号不变；新文件输出全部经过降采样，已有 raw 文件可共存。
5. v3 每个分辨率、每个特征使用一个原生 Block，五个单值 Block 依次写入 values.bin，共享一份 timestamps.bin 负载；单特征索引负责分辨率、feature ID、独立精度及 offset/size。
6. reader/writer/merger/block 使用包级 sync.Pool，Reset 清理引用并限制保留容量；新注释使用规范汉语。
7. 文件区间 retention、原子发布、原始格式退化和启动预检查均按设计执行；完整生产查询不在范围内；本轮实现单机测试参数 `query.field`。
8. 降采样的单列 block 处理必须复用 `storage.Block` 的现有逻辑；仅复用 `encoding`、`decimal` 或模仿对象池写法，不满足该要求。

## 阶段状态

| 阶段 | 状态 | 内容 |
|---|---|---|
| 1. 数学计算 | 已实现并通过定向 UT | 原始样本提升、摘要归并、特殊值、分桶及独立参考 UT |
| 2. 文件格式与读写 | v3 完整回归与 race 通过 | 每个分辨率、每个特征独立使用原生 `storage.Block`，91 字节单特征 header |
| 3. 有界归并 | v3 完整回归与 race 通过 | 聚合中间态为 downsampleBatch，摘要源物理统计按五个字段计数 |
| 4. 存储接入 | 已接通并通过生命周期 UT | 配置互斥、启动预检查、全部 dump/flush/merge 入口、原子发布和回归 |
| 5. 全量核查 | 已完成 | 原生 Block 复用、物理文件 UT、相关包回归、全量 race、vet 和 175 项双实例对照均通过 |

## 前轮验证记录（历史检查点）

- 基线回归：`go test ./lib/storage` 通过（20.923s），日志 `/tmp/vm-downsampling-storage-baseline-20260908.log`。
- 工具链：`go1.27.1 darwin/arm64`，模块要求 Go 1.26.6。
- 数学层：`go test ./lib/storage -run '^TestDownsample'` 通过（0.538s，运行时仅数学层 UT 已就绪）；包含 50 轮随机参考、多级归并和特殊值。
- 配置层：`go test ./lib/storage -run 'Test(CheckDownsamplingOpen|MustOpenStorageDownsampling)' -count=1` 通过（0.867s）。
- 文件层、归并和真实 Storage 生命周期 UT 已通过；完整命令和收尾改动的验证状态见下文。

## 续接事项与范围边界

- 当前任务已完成：单列编解码和状态管理统一到现有 Block 实现，分辨率与特征记录在扩展索引。后续续接先核对当前文件与构建哈希；结果和重现命令见测试记录。
- 启用入口为 `-storage.downsampling.enabled=true` 或 `OpenOptions.DownsamplingEnabled=true`，默认关闭；启用要求 dedup interval 为零。存在活动 v3 文件时，不允许关闭开关后继续打开该存储；未发布的 v2 格式明确拒绝。
- 完整生产查询、自定义分辨率与主动全量迁移不在本次实现范围内。`query.field` 已接入单机 HTTP 测试路径，仅选择已落盘摘要；跨未合并 part 的查询聚合未实现。
- 基准为单次试运行，尚未执行生产规模、持续负载、多设备或故障介质测试，不能据此给出生产吞吐和压缩率结论。
- 下文保留实施检查点及问题修复过程；其中阶段性的“待验证”以末尾最终验证结果为准。

## 当前接口与职责

- 数学层：`downsamplePoint` 为共享时间戳和固定五个浮点值；`downsampleAccumulator.AddRaw/AddSummary/Reset` 实现聚合；分桶辅助函数检查时间域和分辨率。
- 文件层：`downsampleBatch` 为聚合中间态，保存一个 TSID、分辨率、共享时间戳切片及五列切片，五列分别保存精度，时间戳保存自己的精度。
- writer：`Init(path, compressLevel)`、`WriteBlock`、`Finish() (partHeader,error)`、`Abort()`；Finish 完成同步，Abort 关闭并删除未发布目标。
- reader：`Init(part,resolution)`、`SetFilter(TSID,min,max)`、`NextHeader/Header/ReadBlock/Error`；原始格式在 ReadBlock 逐样本提升，索引读取不一次性载入全部 header。
- 格式检测：`detectDownsampleFormat(path) (bool,error)` 与打开预检查共用；part 持有 v3 元数据和 metaindex，原始 inmemory 初始化不变。
- 启用状态：`OpenOptions.DownsamplingEnabled` → `Storage.downsamplingEnabled`；主执行者接入 partition 文件输出。
- 归并：每个源保留一个索引游标，通过 TSID 堆枚举；另复用窗口 reader、原生解码 Block 与 downsampleBatch，以完整 bucket 窗口处理重叠输入。
- 查询：`query.field=5m:sum` 等十种组合，reader 的 `FieldHeader` 选择单特征并进入既有 BlockRef 查询路径。聚合工作批次与原生字段 Block 分别管理，不改变原始网络协议。

## 前轮接入与审查检查点（历史记录）

- 已在 `partition.mergeParts` 的直接 dump 分支之前接入文件目标分派，保留原始内存输出；新的 `downsample_partition.go` 负责预留空间、归并、Finish 与原子源替换。
- 文件过期清理改为目标区间的保守判断；原始 inmemory 过期判断不变。small/big 调度依据输出上界选择候选，不修改共享 part 的实际 size。
- 窗口归并工作集上限为 1024 个 bucket，源游标上限为 1024；每个源按目标分辨率只读取一份表示，输入 block 可重复解码但不重复计入。
- 已审查发现并交由文件层修正：Finish 后尚未发布的取消必须仍可 Abort 删除；原始 block 合法上限是 2×8192 行，不能错误收紧；v2 时间域、单行 delta2 类型组合和字段边界需要单独测试。
- 此检查点尚未修改 IndexDB、TSID、rawRow、inmemoryPart、原始 Block 的实现文件；本轮后续已对 block.go 提取共享方法，原入口语义不变。

## 首轮集成验证问题

- 命令：`go test ./lib/storage -run 'Test(Downsample|CheckDownsamplingOpen|MustOpenStorageDownsampling)' -count=1`。
- 结果：失败，不能作为已通过记录。日志：`/tmp/vm-downsampling-focused-20260908.log`。
- 问题一：writer.Finish 传零行进行最终空间检查，被普通 block 参数校验拒绝。空间层新增 `checkDownsampleFinishSpace` 按待写 index/metaindex 估算；待重跑验证。
- 问题二：新增测试中的原始 reader 显式关闭后再次通过 put 关闭，触发测试清理 panic；已交由测试实现者移除重复关闭，不修改原始 reader 行为。
- 进一步审查：writer 空间检查采用稳定的 partition 父目录，避免 fs 空间缓存按每个新 part 路径无限增长；真实 ENOSPC/EDQUOT 与预算不足统一归类，清理未发布目标并保留源。

## 第二轮集成与发布审查

- 全部新增定向测试第二轮通过：`go test ./lib/storage -run 'Test(Downsample|CheckDownsamplingOpen|MustOpenStorageDownsampling|EstimateDownsample|ReserveDownsample)' -count=1`（1.781s）。日志 `/tmp/vm-downsampling-focused-20260908-r2.log`。
- 归并与真实生命周期专项通过：`go test ./lib/storage -run 'TestDownsample(Merger|Partition|Storage)' -count=1`（0.898s）。
- 文件层修正已覆盖 Finish 后 Abort、原始 16384 行、单行 delta2 非法组合、ZSTD 解压上限、五列 offset 连续和正常大容量复用。
- 独立审查发现发布边界风险：活动清单提交后的源清理若 panic，外层不得 Abort 已发布目标。已在进入 `swapSrcWithDstParts` 前转移目标清理所有权；提交前致命错误留下的孤立目标由既有重启清单机制回收。此修正需纳入最终回归。
- 正在执行相关包正常回归；全量 storage 测试不能由多个进程同时运行，部分原始测试共用工作目录路径。

## 文件格式定稿与相关包回归

- 格式为 column 25 字节、block header 202 字节、metaindex 行 96 字节；六列共用 block.RowsCount 声明解码长度，不重复存储六份行数。全部字段偏移、编码方式及 JSON 必需字段已补入设计第 4.5 节。
- 相关包回归：`go test ./lib/storage ./lib/encoding ./lib/decimal ./app/vmstorage ./app/vmselect/netstorage ./app/victoria-metrics` 通过。storage 22.956s；其他有测试包均通过，victoria-metrics 编译通过但无测试文件。日志 `/tmp/vm-downsampling-regression-20260908.log`。
- 配置/空间定向 race 通过（2.533s）；全量 storage race 待主执行者统一运行。
- 单次基准已运行：`go test ./lib/storage -run '^$' -bench '^BenchmarkDownsample' -benchmem -benchtime=1x -count=1`，17 个子基准通过（1.629s）。数学 raw/summary 两项为 0 allocs/op；文件输出大小仅反映构造数据，不构成性能承诺。
- 强制 merge 和最终 flush 的 optimal 候选若超过 1024 源，继续分批处理；不因归并器容量上限永久拒绝历史大源集合。新增分批测试验证贡献守恒、唯一处理和源标记保持。
- 发布边界修正、分批保护和补充低精度测试仍需最终统一回归；不得将前一次命令结果视为这些后续改动已验证。

## 发布恢复专项验证

- `go test ./lib/storage -run '^TestDownsampleRecovery' -count=1` 通过（0.679s）。
- 覆盖清单提交后同步 panic：故障注入源引用计数错误，确认五列目标仍存在、能独立读取且可重启恢复。
- 覆盖未发布损坏孤立目标：启动预检查只处理清单活动源，正式打开时按原清单规则清理孤立目录，不影响活动数据。
- 已补充低精度与独立 scale 两轮归并 UT，通过同一编码边界的参考验证，不要求不同编码次数逐位一致。

## 最终资源与索引复核

- 最终相关包正常回归通过：`go test ./lib/storage ./lib/encoding ./lib/decimal ./app/vmstorage ./app/vmselect/netstorage ./app/victoria-metrics`，storage 20.923s。日志 `/tmp/vm-downsampling-final-regression-20260908.log`。
- `go vet ./lib/storage ./app/vmstorage` 通过；日志 `/tmp/vm-downsampling-vet-20260908.log` 为空。
- 全量 storage race 通过：`go test -race ./lib/storage`（161.089s），日志 `/tmp/vm-downsampling-race-20260908.log`。该二进制生成于最后的资源释放、索引定位和统计修正之前。
- 已将 merger.Reset 提前到发布前，先关闭原始文件 reader 再删除旧源，避免仍打开的工作句柄影响 NFS 清理。
- 索引定位已完成：SetFilter 根据分辨率和 TSID 二分定位首个候选 metaindex，保持原始格式相等边界前一项的覆盖规则；同一 part 的 Init 复用文件句柄。新增双分辨率、多 index、重复 TSID、重叠时间范围和高基数定位测试，定向命令 `go test ./lib/storage -run '^TestDownsampleReader(Seek|RawSeek)' -count=1` 通过（0.620s）。
- 归并统计按实际处理的源物理行递增，原始源的两个分辨率不重复统计；仅当两个目标区间均过期时将原始行计为删除。新增断言要求预先取消的作业统计为零，避免将尚未处理的元数据行数计入指标。
- 正在对上述最终代码运行相关包回归；随后运行全部新增测试的 race、静态检查和改动范围核查。

## 前轮 v2 验证结果（2026-09-08，仅供追溯）

新增 49 个顶层 UT，并通过子测试覆盖数学规则、文件结构、归并边界和存储生命周期；基准另有 17 个子项。

| 验证 | 结果 | 运行范围与证据 |
|---|---|---|
| 当前代码相关包回归 | 通过 | `go test ./lib/storage ./lib/encoding ./lib/decimal ./app/vmstorage ./app/vmselect/netstorage ./app/victoria-metrics`；storage 21.058s、vmstorage 1.133s，编码及相邻包通过，victoria-metrics 编译通过；日志 `/tmp/vm-downsampling-current-regression-20260908.log` |
| 全量 storage race | 通过 | `go test -race ./lib/storage`；161.089s；日志 `/tmp/vm-downsampling-race-20260908.log`。该轮早于最后三处修正，后续验证见下一行 |
| 当前代码全部新增测试 race | 通过 | `go test -race ./lib/storage -run 'Test(Downsample\|CheckDownsamplingOpen\|MustOpenStorageDownsampling\|EstimateDownsample\|ReserveDownsample)' -count=1`；4.247s；覆盖索引定位、发布前资源释放和统计修正；日志 `/tmp/vm-downsampling-current-race-20260908.log` |
| 当前代码静态检查 | 通过 | `go vet ./lib/storage ./app/vmstorage`；日志 `/tmp/vm-downsampling-current-vet-20260908.log` 为空 |
| 格式与差异检查 | 通过 | 修改的 Go 文件 `gofmt -l` 无输出；`git diff --check` 无错误；新增文件另行检查空白和文档链接 |
| 当前代码基准试运行 | 通过 | `go test ./lib/storage -run '^$' -bench '^BenchmarkDownsample' -benchmem -benchtime=1x -count=1`；17 个子基准，1.184s；日志 `/tmp/vm-downsampling-bench-20260908.log` |

基准环境为 Apple M5 Pro、Go 1.27.1、darwin/arm64。数学层每次处理 8192 条输入，raw 为 101292 ns/op、summary 为 84292 ns/op，均为 0 B/op、0 allocs/op。文件基准包含实际同步写出，覆盖密集、稀疏和高基数输入，以及 raw 转换、四源摘要归并、摘要重写和文件读写；单次结果只用于保存可复现的初始测量。

最终只读审查确认：原始文件 writer 与 `MustStoreToDisk` 的生产调用均位于 `partition.mergeParts`，且在启用降采样时由文件目标分派提前接管。周期 flush、关闭、snapshot、后台 merge 和强制 merge 均汇入该入口。`mustMergeInmemoryPartsFinal` 仍使用原始 reader/writer 及 `partInmemory`，未引入降采样计算。

原有文件实际修改仅为 `app/vmstorage/main.go`、`lib/storage/storage.go`、`lib/storage/part.go` 和 `lib/storage/partition.go`。IndexDB、TSID、`raw_row.go`、`inmemory_part.go`、`block.go`、`block_header.go`、`part_header.go`、原始 `merge.go` 及编解码库均未修改。

## 代码与测试定位

| 职责 | 生产实现 | 验证文件 |
|---|---|---|
| 五特征数学规则 | `downsample.go` | `downsample_test.go`：独立参考、随机样例、重复贡献、NaN、分桶、Reset |
| 五列文件与索引 | `downsample_batch.go`、`downsample_codec.go`、`downsample_part.go`、`downsample_reader.go`、`downsample_writer.go` | `downsample_codec_test.go`：字段、offset/size、常量列、损坏输入、旧格式、索引定位及对象池；`downsample_layout_test.go`：独立物理布局验证；`downsample_layout_corruption_test.go`：跨 index 偏移损坏与过滤读取 |
| 窗口归并 | `downsample_merger.go` | `downsample_merger_test.go`：多轮归并、混合源、重叠 block、窗口、retention、删除、取消与独立列精度 |
| 文件生命周期 | `downsample_partition.go` 及 `partition.go` 接入 | `downsample_partition_test.go`、`downsample_recovery_test.go`、`downsample_batch_test.go`：raw 内存、文件输出、snapshot、关闭重开、发布故障与分批守恒 |
| 启动和空间 | `downsample_open.go`、`downsample_space.go` 及配置接入 | `downsample_open_test.go`、`downsample_space_test.go`：活动清单、互斥、无副作用拒绝、预算及并发 |
| 性能 | 上述实现 | `downsample_timing_test.go`：数学计算、转换、摘要归并、读写与分配次数 |

上述为前轮 v2 的布局检查点：列描述 25 字节、组合 header 202 字节、metaindex 行 96 字节，现已由 v3 单特征索引替换。当前布局以设计第 4.5 节和本文末尾为准。

## 前轮 v2 列存文件结构专项复核（仅供追溯）

- 核验范围：设计第 4.2、4.3、4.5 节规定的每 block 五列顺序、单份时间戳、分辨率排序、实际文件 offset/size 及各级统计。
- 已有 UT 覆盖读写往返、列标识和 offset 非法输入、常量列零负载、跨 index 读取；这些测试主要通过生产 writer/reader 配合验证。
- 已新增 `downsample_layout_test.go`，直接读取五个实际文件，使用固定字段偏移和标准大端解码检查结构，再用既有基础 codec 独立核对列负载。该测试不调用降采样 reader、header unmarshal 或 part 打开逻辑，避免两端同源错误互相掩盖。
- `go test ./lib/storage -run '^TestDownsampleFilePhysicalLayout$' -count=1 -v` 通过（0.688s），日志 `/tmp/vm-downsampling-layout-20260908.log`。三个子测试分别验证 6 个双分辨率 block、660 个跨 index block，以及 6 个全部采用零负载编码的 block。
- 独立审查未发现 writer 的列顺序或字段错位问题，但发现 reader 连续扫描相邻 index 时未校验跨 index 的数据 offset 衔接。已补充校验与 `downsample_layout_corruption_test.go` 的三个顶层 UT；过滤跳过中间 index 的场景继续正常定位，不为校验而退回全量索引扫描。
- 故障 UT 的首轮构建发现测试时间基值被推断为 int，已在 fixture 中显式使用 int64；随后 `go test ./lib/storage -run '^TestDownsampleReaderCrossIndex' -count=1` 通过（0.833s）。
- 本轮增加 4 个顶层 UT（1 个物理布局测试、3 个跨 index 测试），降采样新增顶层 UT 总数为 53。全量 storage 回归、相关 race 和静态检查均通过，命令及结果如下。

```text
go test ./lib/storage -count=1
PASS，21.456s

go test -race ./lib/storage -run 'Test(Downsample|CheckDownsamplingOpen|MustOpenStorageDownsampling|EstimateDownsample|ReserveDownsample)' -count=1
PASS，4.607s

go vet ./lib/storage
PASS
```

对应日志为 `/tmp/vm-downsampling-layout-regression-20260908.log`、`/tmp/vm-downsampling-layout-race-20260908.log`、`/tmp/vm-downsampling-layout-vet-20260908.log`。修改文件的 gofmt、diff 及新增文件空白检查均通过。本轮生产修改仅为 reader 的结构性校验及状态清理，未改变 writer、文件格式或数学计算。

专项测试以整个数据文件的预期负载拼接结果核对实际字节，要求所有列及 block 的 offset 连续、size 与编码长度一致，文件末尾没有额外负载。header、metaindex 和 metadata 使用各自独立的字节位置及字段期望值核对，统计不按五列重复计数。

| 子测试 | 实际文件结构 |
|---|---|
| `multiple_blocks_and_resolutions` | 6 个 block、30 个物理摘要行、2 个 metaindex 行；values 158 字节，timestamps 57 字节 |
| `multiple_index_blocks` | 660 个 block、3300 个物理摘要行、4 个 metaindex 行；values 17490 字节，timestamps 6270 字节 |
| `zero_payload_columns` | 6 个 block、6 个物理摘要行；values 与 timestamps 均为 0 字节，各列使用现有常量编码，从 header 恢复 |

第一个子测试的首个 block 含 4 行，五列实际布局如下：

| 特征 | values.bin offset | size |
|---|---|---|
| last | 0 | 6 |
| sum | 6 | 3 |
| count | 9 | 0 |
| min | 9 | 4 |
| max | 13 | 6 |

其中 count 是常量列；零负载使用 `FirstValue`、`Scale` 和共享 `RowsCount` 恢复完整列，不代表缺失该特征。时间戳数据每个 block 只有一列，索引仍按既有编解码约定保存首值和时间范围。

新增故障测试保持单个 index 内的列偏移关系及文件边界合法，分别将中间 index 的 timestamps 或五列 values 偏移整体增加、减少一个字节，确认连续扫描会拒绝跨 index 的空洞和重叠。另验证 TSID 二分定位、时间窗口跳过 index、同 part Init、SetFilter 和 reset。

reader 的新增校验仅覆盖同次扫描中已经读取且物理相邻的 index。过滤跳过的 index 不补读；重新 Init 后清空历史边界，不跨分辨率调用比较偏移。因此，正常 writer 的全文件连续性由独立物理布局 UT 核验，选择性读取不能等同于对整个文件进行完整性扫描。

## 前轮 storage.Block 复用审查（v2 历史记录）

当时的审查结论：v2 降采样链路没有直接复用 `storage.Block` 的单列处理流程，只复用了底层编解码函数和部分时间戳辅助校验。不满足完整复用 Block 逻辑的要求；此前“实施完成”的状态据此更正。

| 实际实现 | 代码证据 | 与要求的差异 |
|---|---|---|
| 独立多列工作结构与 Reset | `downsample_batch.go` 的 `downsampleBlock/Reset` | 未由 `Block` 承担单列状态及其生命周期 |
| writer 直接编码 | `downsample_writer.go` 的 `WriteBlock` 直接调用 `encoding.MarshalTimestamps/MarshalValues` | 未复用 `Block.MarshalData` 的处理逻辑 |
| reader 直接解码 | `downsample_reader.go` 的 `decodeColumn/ReadBlock/readRawBlock` | 未复用 `Block.UnmarshalData`；自行组织解码、时间戳修复和长度检查 |
| 独立归并工作 block | `downsample_merger.go` 使用 `downsampleBlock` | 原始 Block 的读取状态和复用接口未接入该适配层 |

已有 UT 证明的是当前计算结果、文件布局、资源行为及错误处理，不能据此证明复用了 Block。后续重构须保持已验证的 v2 磁盘布局、单份时间戳、五列参数及原始 inmemory 行为，并重新执行相关 UT、race 和文件字节布局验证。本轮仅核实复用情况并更正文档，未修改生产代码。

## 单特征 Block 重构检查点（2026-09-08）

- 已完成每个分辨率、每个特征一个原生 `storage.Block` 的重构。`Block.MarshalData/UnmarshalData` 的原入口保留原语义，降采样经共享实现传入独立的时间戳 precision；未另行实现单列 codec。
- v3 单特征 header 为 91 字节，其中原生 blockHeader 为 81 字节；五个 header 的批次为 455 字节。metadata 与 metaindex 的 RowsCount/BlocksCount 均按实际单值 Block 统计。旧版本 2 明确拒绝。
- writer 五个 Block 各自持有正常工作缓冲，时间戳负载实际只写入一次；reader 通过原生 Block 解码。`downsampleBatch` 仅为五特征聚合临时批次，不是替代 Block 的持久化类型。
- 已同步归并统计及空间预算，新增物理统计 UT。文件层及 query.field 定向普通 UT 通过，查询定向 race 通过。完整命令与后续测试状态见测试记录。
- 待执行：重命名后的统一回归、构建当前分支、双实例裸查询对照、最终 race 与文档核对。之前 v2 的验证结果不得作为本轮 v3 代码已通过的证据。

## 收尾审查检查点

- 相关包完整回归通过（storage 26.475s），两个二进制均已构建，首轮双实例 175 项通过，最大绝对误差为 0；三轮写入、再次 merge 和重启均通过。
- 补充 reader 拒绝重复 v3 批次键的结构性校验，保留合法 raw 重叠边界；修正后需重新构建、对照，并运行全量 storage race。
- 设计已区分磁盘时间戳只保存一次与五个原生 Block 的有界工作副本，明确 query_range 的标准 PromQL 时间网格与存储 TimeRange 的区别。

## 本轮最终完成状态（v3，2026-09-08）

本轮要求已全部完成，未执行 git commit。后续工作以此节及顶部阶段表为准，前轮 v2 的历史验证不代表当前架构。

| 验证项 | 最终结果 |
|---|---|
| 真实 Block 复用 | writer 使用五个原生 Block；reader 共用 Block 解码；分辨率和 feature ID 位于单特征扩展索引 |
| 文件结构 | 每字段 91 字节索引条目，五字段关联为 455 字节批次；单份磁盘时间戳；RowsCount/BlocksCount 按单值 Block 计数 |
| 相关包正常回归 | 全部通过，storage 26.475s；收尾索引故障 UT 另行通过 |
| 最终全量 storage race | `go test -race ./lib/storage -count=1` 通过，169.678s |
| 查询定向 race 与静态检查 | 全部通过 |
| 双实例裸查询 | 原版 v1.151.0 与最终分支 binary，175 项通过；本轮输入最大绝对误差 0 |
| 输入与文件生命周期 | 三轮相同输入、迟到数据、重复时间戳、再次 merge、关闭与重启均通过 |
| 完整性与修改范围 | 格式、差异、文档链接及构建源哈希检查通过；IndexDB、TSID、raw_row、inmemory_part、block_header、part_header、原始 merge 实现未修改 |

最终测试证据在 [downsampling_storage_testing.md](downsampling_storage_testing.md)，包含两个 binary 的路径、SHA256、构建命令、原始 HTTP 结果、metadata、具体五特征样例和全部日志。最终产物目录为 `/tmp/vm-downsampling-comparison-20260908/comparison-final`，测试实例均已退出。

范围边界保持明确：`query.field` 是单机字段选择测试接口，需完成 flush/merge 后核验完整摘要；未实现跨未合并 part 的查询侧再聚合、分布式字段协议及生产性能验收。

## 多时间线与长时间跨度验证续接（2026-09-08）

- 新增任务：把 UT 和端到端测试方法写入测试记录，并实际验证多条时间线长周期输入在 TSID 内及 TSID 间的存储/查询遍历。
- 已补充测试记录的方法章节：输入构造、独立参考、具体命令、标签与点级判定、落盘完成条件、多 partition 清单、失败保留及重启复核。
- 并行工作范围：`downsample_iteration_test.go` 为精确遍历边界 UT；`testdata/downsampling_multiseries_compare.py` 为 160 线/93 天的双实例测试；`testdata/downsampling_inspect.py` 为独立实际文件索引审查。暂不修改生产算法。
- 待完成：运行上述 UT/race 与真实双实例、检查跨 Block/index/TSID/partition 的覆盖证据，定位并修复任何实际失败，再更新最终记录。

- 遍历专项 UT 已完成并冻结：180 TSID、4 index、1810 单特征 Block、91230 物理行；同 TSID 的 8229 行跨 Block/index。完整 TSID 分组与 MetricID 逆序、十字段、过滤和复用均验证。普通 UT 0.730s、定向 race 4.225s 通过；未修改生产代码。相关包完整回归正在运行。

- 多时间线首轮已通过414项，4month/160 TSID实际文件全部通过独立索引检查；密集线在7/8月拆8192+736行，实际同TSID跨index亦命中。包含新增UT的相关包回归通过（storage25.883s），生产代码未修改。收尾扩展多线query_range全量及子集，之后重跑最终对照和独立文件检查。

## 多时间线验证最终完成状态（2026-09-08）

- 测试方法、输入构造、独立参考、具体命令和判定标准已写入 [downsampling_storage_testing.md](downsampling_storage_testing.md)。
- 新增 `downsample_iteration_test.go`：180 TSID，完整高位分组及 MetricID 跨组逆序；8229 行长序列跨 Block/index，十字段、过滤、EOF 和对象复用逐样本验证。普通 UT 0.730s、race 4.225s 通过。
- 新增 `testdata/downsampling_multiseries_compare.py`：160 条时间线、93 天、15 个普通批次及迟到追加，共 248808 原始行；六阶段 562 项全部通过，最大绝对误差 0。全量 query_range 覆盖完整 31/62/93 天，子集查询覆盖稀疏及空点。两端每月 rewrite 确实替换 part，重启保持文件及结果。
- 新增 `testdata/downsampling_inspect.py`：独立审查最终活动文件；4 month、160 TSID、6050 Block、694200 物理行、14 index。7/8 月每条密集线拆为 8192+736 行，同 TSID/不同 TSID 跨 index 均有实际证据；六项覆盖全部命中。
- 含新增 UT 的相关包完整回归通过（storage 25.883s），静态及格式检查通过。生产源及已有两个 binary 哈希保持一致，无生产修改。
- 最终产物：`/tmp/vm-downsampling-comparison-20260908/multiseries-long-range-final`。该目录的 `summary.json`、`index-inspection.json`、`test-run-manifest.json` 和完整 HTTP 结果为最终证据；所有测试实例已关闭，工作区改动尚未提交。
- 本轮无未完成事项。长期持续负载、故障介质及生产性能测试仍属于另外的验证范围，不能用历史数据跨度测试替代。
