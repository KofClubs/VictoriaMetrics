# 降采样存储设计

基线：集群版 `v1.151.0-cluster`；实现分支：`experimental/downsampling`。降采样存储和字段查询的版本统一为 **2**。实现状态见 [实现记录](downsampling_storage_implement.md)，验证方法与结果见 [测试说明](downsampling_storage_testing.md)。

## 1. 数据语义

固定分辨率为 `5m=300000ms` 和 `1h=3600000ms`。区间编号为 `timestamp / resolution`，区间左闭右开。只对实际输入产生摘要，不填充空区间。

五个特征共用区间内最大的样本时间戳，数值全部为 `float64`，包括 count：

| 特征 | raw 输入 | 摘要输入 |
|---|---|---|
| last | 取最大时间戳对应的值 | 选择共享时间戳最大的源摘要的 last |
| sum | 累加全部值 | 累加源 sum |
| count | 每条样本贡献 1 | 累加源 count |
| min / max | 取最小值 / 最大值 | 合并源 min / max |

相同时间戳的 last 优先选择数值，再取较大值；此规则不删除其他特征的输入。降采样不执行 dedup，所有实际进入区间的输入均参与计算。

所有 NaN 统一为 `decimal.StaleNaN`，不区分来源。较新的 last 标记覆盖较早数值；sum/min/max 按列传播标记，count 对 raw 标记仍计入 1。运算产生的 NaN 使用相同规则，不影响其他列。原始摄取对普通 NaN 的处理保持不变。

编解码直接复用原生 `Block.MarshalData`、`Block.UnmarshalData` 及现有 decimal 算法。时间戳和五个 value 列共用源 raw 的 `precisionBits`，默认 64；各列仅独立保留 scale 和编码类型，不再单独设置精度或在归并时取最小精度。不同区间的源精度不同时拆分输出 Block；同一区间的源精度不一致时报错。浮点误差及有损编码沿用原有规则，不附加精度保证。

## 2. 存储与归并

```text
raw 写入 → raw inmemory → 文件输出分派 → 降采样 part
raw 文件 / 降采样 part → 按分辨率与完整 TSID 归并 → 降采样 part
```

`partition.mergeParts()` 在单 inmemory dump 快径之前拦截全部文件目标。inmemory 的数据结构、编码和归并保持 raw；IndexDB、TSID 分配和原始摄取不变。

一个分辨率、一个特征使用一个真实 `storage.Block`，完整复用单列编解码与状态转换。`downsampleBatch` 仅组织共享时间戳和五列浮点缓冲；writer 持有五个 Block，按 last、sum、count、min、max 依次写出整个列负载。

归并以完整 TSID 分组，包含 AccountID、ProjectID。每个源、每个目标分辨率只贡献一份表示：raw 逐样本提升；摘要读取相同分辨率，不能同时累计该源的 5m 与 1h。sum/count 累加已有特征，不能对摘要行重新计数。

reader、writer、merger、batch 通过 `sync.Pool` 复用。Reset 清除文件句柄、借用对象、TSID、切片引用及统计，异常大缓冲不长期留在池中。归并窗口最多 1024 个区间，单次最多 1024 个源；输出最多 8192 行一个 Block，不能拆分同一区间。

## 3. 文件布局

| 文件 | 内容 |
|---|---|
| timestamps.bin | 每个五字段批次只存一份时间戳负载 |
| values.bin | 每批依次连续存储 last、sum、count、min、max 的单列 Block 负载 |
| index.bin | 分辨率、特征标识、时间戳精度及完整原生 blockHeader |
| metaindex.bin | 分辨率、首末 TSID、时间范围、index offset/size 和物理统计 |
| metadata.json | 固定配置、版本及 part 物理统计 |
| parts.json | 复用 partition 的活动 part 清单及原子发布机制 |

排序键为 `(ResolutionMs, TSID.Less, MinTimestamp, Feature)`。每个 index 仅包含一种分辨率和完整五字段批次；先写 5m，再写 1h。同批五个 header 的 TSID、行数、时间范围、时间戳描述及 PrecisionBits 一致；values 的 offset/size、FirstValue、Scale 和 marshal type 各自独立。保留现有 TimestampPrecisionBits 磁盘字段，但必须等于每个原生 blockHeader 的 PrecisionBits；不接受精度不一致的旧试验文件。常量列允许 size=0，不表示缺少特征。

### 3.1 扩展 header

cluster TSID 为 32 字节，字段顺序为 AccountID:uint32、ProjectID:uint32、MetricGroupID:uint64、JobID:uint32、InstanceID:uint32、MetricID:uint64。原生 blockHeader 为 89 字节，保持 [block_header.go](lib/storage/block_header.go) 的布局。

| offset | 字节数 | 字段 |
|---:|---:|---|
| 0 | 8 | ResolutionMs |
| 8 | 1 | Feature：1=last、2=sum、3=count、4=min、5=max |
| 9 | 1 | TimestampPrecisionBits |
| 10 | 89 | 完整原生 blockHeader |

单特征条目为 99 字节，五字段批次为 495 字节；65536 字节的 index 解压上限容纳至多 132 个批次。生产尺寸从实际 marshal 输出计算，UT 和 Python 检查器按固定尺寸独立验证。

### 3.2 metaindex

每行为 112 字节：

| offset | 字节数 | 字段 |
|---:|---:|---|
| 0 | 8 | ResolutionMs |
| 8 | 32 | 首 TSID |
| 40 | 32 | 末 TSID |
| 72 | 8 | MinTimestamp |
| 80 | 8 | MaxTimestamp |
| 88 | 4 | BlockHeadersCount |
| 92 | 8 | IndexBlockOffset |
| 100 | 4 | IndexBlockSize |
| 104 | 8 | RowsCount |

整数复用 `lib/encoding`：无符号整数按大端保存，有符号整数先 ZigZag 再按大端保存。index 与 metaindex 压缩帧分别以 `VMDSIX`、`VMDSMI` 加 `00 02` 开头，随后为 ZSTD 负载。

### 3.3 metadata

| 字段 | 约束 |
|---|---|
| FormatVersion、SemanticsVersion | 均为 2，使用同一个版本常量 |
| Mode | downsampling |
| Resolutions | [300000,3600000] |
| BucketOrigin | 0 |
| NumericCodec | decimal-values；表示复用现有编码，不另设编码版本 |
| Retention | bucket-end |
| MinDedupInterval | 0 |
| RowsCount、BlocksCount | 实际单特征 Block 的物理行数和 Block 数，均按五字段成组统计 |
| MinTimestamp、MaxTimestamp | part 实际共享时间戳的首末范围 |

上述字段不可缺失或为 null。物理 RowsCount 与浮点特征 count 是不同概念。所有 offset/size、统计、排序、时间域、编码类型和解压长度必须通过校验。

## 4. 配置、兼容与发布

- `-storage.downsampling.enabled=true` 或 `OpenOptions.DownsamplingEnabled=true` 启用功能，默认关闭。vmstorage 要求 `dedup.minScrapeInterval=0`。
- 合法 raw 文件可与降采样文件共存，raw 文件参与后续 merge 时转换。缺少五特征的合法 raw 文件使用原始 reader；损坏的降采样文件、非法版本或元数据冲突必须报错，不能退化为 raw。不兼容其他试验格式，不自动迁移试验产物。
- 启动预检查在目录锁和恢复状态检查之后、IndexDB 初始化及后台任务之前执行。存在活动降采样 part 时不能关闭开关打开；snapshot 中的非活动文件不构成此冲突。
- retention 按区间右端点判断：右端点不大于 deadline 时删除，否则保留整份摘要。两种分辨率独立判断，不能从已裁剪的细分辨率重建粗分辨率。月 partition 规则保持不变。
- 新目标完成写入和同步后，通过原有活动清单发布；失败或取消清理未发布目标并保留源，已发布目标不得被 Abort 删除。空间预算覆盖新旧 part 共存、五列和两种分辨率的输出上界。

## 5. 字段查询

`query.field` 指定十种组合，例如 `5m:last`、`1h:count`。参数经 HTTP、EvalConfig、SearchQuery 传到存储 reader，非法值报错，字段查询禁用结果缓存和缓存时间对齐。

字段 RPC 为 `search_downsampling_v2`。在原有 tenant、时间范围和筛选条件负载之后，追加 8 字节分辨率与 1 字节特征编号；网络编号为 0..4，依次表示 last/sum/count/min/max，与磁盘编号分开处理。响应继续使用原生 MetricBlock。字段请求要求全部目标节点成功，不支持该请求的节点必须返回错误。

该接口仅读取已落盘摘要；未指定字段时沿用原始查询路径。存储 TimeRange 必须覆盖摘要的共享时间戳，才返回该摘要，不重新裁剪区间内部的原始贡献。HTTP 查询继续遵循 PromQL lookback 和求值网格。

vmselect 与 vmstorage 的 dedup 配置分别生效，测试必须在两个进程均设为零。跨未合并 part 的摘要再聚合、raw/摘要混合查询、自定义分辨率及完整生产查询不在当前范围内。

## 6. 实现位置

| 文件或模块 | 职责 |
|---|---|
| `lib/storage/downsample.go`、`downsample_batch.go` | 数学计算、共享时间戳、五列缓冲和对象池 |
| `downsample_codec.go`、`downsample_part.go` | header、metadata、格式检测、严格校验 |
| `downsample_writer.go`、`downsample_reader.go`、`downsample_merger.go` | Block 复用、列读写、有界归并及 Reset |
| `downsample_partition.go`、`downsample_space.go` | 文件目标归并、发布、retention 和空间预算 |
| `partition.go`、`part.go`、`storage.go`、`downsample_open.go` | 文件输出分派、part 打开、启用配置和启动预检查 |
| `block.go` | 保持原生单列编解码实现不变，时间戳与 values 共用 PrecisionBits |
| `downsample_query.go`、`downsample_search_protocol.go`、各层 search | 字段选择、请求编解码及 TSID 遍历 |
| `app/vmselect/prometheus`、`promql`、`netstorage`；`lib/vmselectapi`；`app/vmstorage` | HTTP 参数、集群 RPC、缓存隔离及存储调用 |

除带完整路径的条目外，文件均位于 `lib/storage`。
