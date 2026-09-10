# 降采样存储设计

本文定义当前集群版实现的数据语义、文件格式、读写流程和运行边界。代码入口见[实现说明](downsampling_storage_implement.md)，审查要点见[审查说明](downsampling_storage_review.md)，测试命令与用途见[测试说明](downsampling_storage_test.md)。

## 1. 适用范围与术语

通过 `-storage.downsampling.enabled=true` 或 `OpenOptions.DownsamplingEnabled=true` 启用文件降采样，默认关闭。启用时要求存储端 `dedup.minScrapeInterval=0`。

分辨率固定为 `5m`（300000 毫秒）和 `1h`（3600000 毫秒），特征固定为 `last`、`sum`、`count`、`min`、`max`。磁盘格式和字段查询协议统一采用编号 `0..4`，依次对应上述五个特征。格式版本和语义版本均为 `2`。

| 术语 | 含义 |
|---|---|
| part | 存储片段，可以是内存中的原始数据，也可以是磁盘中的原始数据或降采样数据 |
| TSID | 一条时间序列的完整标识，包含租户信息 |
| bucket | 按指定分辨率划分的左闭右开时间区间 |
| 原生 `Block` | 一个 TSID 的一列时间戳和一列数值；在降采样文件中对应一个分辨率、一个特征 |
| 多特征 Block 批次 | 一个 TSID、一个分辨率下，共享时间戳和行数的五个单特征 Block |
| `downsampleSample` | 一个 bucket 的共享时间戳、五个特征值和源精度，也用于保存聚合状态 |
| `downsampleDecodedResolutionFeaturesBlock` | reader 输出的指定分辨率多特征解码结果，保存一份时间列和五份浮点数值列 |

内存数据的缓冲、序列化和内存归并使用原始格式。降采样处理目标为磁盘文件的写出或归并任务，IndexDB、TSID 分配和原始数据接收流程使用原有实现。

降采样专属生产代码按 block、metaindex row、reader、writer、merger、part、partition、query 八个模块组织。block 定义样本、分桶计算、多特征解码缓冲及 header 校验；metaindex row 定义索引行及其编解码；part 管理格式标识和元数据大小限制；query 包含字段解析、查询协议编解码和 part 搜索定位。各模块的代码导航与调用边界见实现说明。

## 2. 数据语义

### 2.1 分桶与特征计算

时间单位为毫秒，`bucketID = timestamp / resolution`，区间为 `[bucketID × resolution, (bucketID + 1) × resolution)`。只输出有实际贡献的 bucket。

五个特征共用 bucket 内最新贡献的时间戳，数值类型均为 `float64`，包括 `count`。

| 特征 | 原始样本输入 | 同分辨率降采样输入 |
|---|---|---|
| `last` | 选择时间戳最大的样本值 | 选择共享时间戳最大的源记录的 `last` |
| `sum` | 累加样本值 | 累加源 `sum` |
| `count` | 每条样本贡献 1 | 累加源 `count` |
| `min` | 选择最小样本值 | 选择最小源 `min` |
| `max` | 选择最大样本值 | 选择最大源 `max` |

相同时间戳的 `last` 优先选择非 NaN 值；两者均为非 NaN 时选择较大值。该选择规则只影响 `last`，不会删除其他特征的输入。降采样不执行去重，所有进入 bucket 的输入均参与计算。

`downsampleSample.Merge` 在每次合并结束时统一扫描五个结果值，将输入及运算产生的 NaN 规范化为 `decimal.StaleNaN`。reader 负责原生解码，writer 直接编码聚合后的样本，两者均不另行扫描或改写 NaN。较新的标记可以覆盖较早的 `last` 数值；`sum`、`count`、`min`、`max` 按特征传播标记。原始标记样本仍向 `count` 贡献 1；原始数据接收阶段对 NaN 的处理由原有流程决定。

### 2.2 精度与编码

时间戳和五个数值列共用源 `precisionBits`，有效范围为 `1..64`，通常为 64。各数值列独立保存 `Scale`、`FirstValue` 和编码类型，使用原有 decimal 转换与原生 `Block` 编解码。

`downsampleSample.precisionBits=0` 表示空槽；首次贡献写入完整样本，后续贡献通过 `Merge` 累加。同一 bucket 的源精度不一致时返回错误；不同 bucket 的精度不同时，writer 拆分输出 Block。浮点转换和有损压缩遵循原生编码语义。

编码前要求时间戳严格递增，且一个输出批次内每个 bucket 只有一行。有损时间戳编码可能改变行所属的 bucket；磁盘校验依据已编码的时间范围和顺序执行，同一 TSID 的相邻 Block 时间范围不得重叠。

### 2.3 保留期限

保留期限按 bucket 的右端点判断：右端点不大于 `retentionDeadline` 时删除，否则保留该 bucket 的完整贡献。两种分辨率独立处理，粗分辨率结果不从已经按保留期限裁剪过的细分辨率结果重建。

启用降采样时，整块磁盘 part 的过期判断同样检查两种分辨率：只有 `MaxTimestamp` 所在的两个 bucket 均已结束且过期，才删除该 part。内存 part 和整个月份分区的清理沿用原有规则。

源行统计与浮点特征 `count` 分开计算。原始源只在首个分辨率累计统计；只有两个目标分辨率的 bucket 均过期时，原始行才计入删除行数。降采样源按各分辨率的五个单特征 Block 统计物理行数。

## 3. 文件布局

### 3.1 文件与排序规则

每个降采样 part 包含五个最终文件：

| 文件 | 内容 |
|---|---|
| `timestamps.bin` | 每个多特征批次的一份编码时间戳负载 |
| `values.bin` | 五个特征各自的编码数值负载 |
| `index.bin` | 分块压缩的原生 `blockHeader` |
| `metaindex.bin` | 压缩的降采样 metaindex 行，用于定位 index block |
| `metadata.json` | part 统计、版本、分辨率和计算语义 |

`parts.json` 是分区的活动 part 清单，位于 `smallPartsPath`，不属于单个 part 目录。

文件的物理排序规则为：

```text
timestamps.bin       : resolution → TSID.Less → MinTimestamp
values.bin/index.bin : resolution → feature → TSID.Less → MinTimestamp
metaindex.bin        : 与 index.bin 中各 index block 的顺序一致
```

先写 5m，再写 1h。同一 `(resolution, feature)` 的全部数值 Block 和索引条目连续存放。时间戳不按 feature 重复存储；同一批次的五个 header 指向相同的时间戳 `(offset, size)`。

每个 index block 及其对应的 metaindex 行只属于一个分辨率、一个特征、一个租户。一个 index block 可以包含同租户的多个 TSID，也可以包含同一 TSID 的多个 Block。租户由 `TSID.AccountID` 和 `TSID.ProjectID` 确定，不单独增加磁盘字段。

### 3.2 六个 TSID 的顺序示例

假设 `A < B < C < D < E < F` 符合 `TSID.Less` 顺序，且每个 TSID 在每个分辨率下恰有一个多特征批次。下图按每行从左至右、各行从上至下连接，表示同一文件中的连续顺序。

```text
timestamps.bin
  5m，共享时间列：[A] [B] [C] [D] [E] [F]
  1h，共享时间列：[A] [B] [C] [D] [E] [F]

values.bin
  5m，last ：[A] [B] [C] [D] [E] [F]
  5m，sum  ：[A] [B] [C] [D] [E] [F]
  5m，count：[A] [B] [C] [D] [E] [F]
  5m，min  ：[A] [B] [C] [D] [E] [F]
  5m，max  ：[A] [B] [C] [D] [E] [F]
  1h，last ：[A] [B] [C] [D] [E] [F]
  1h，sum  ：[A] [B] [C] [D] [E] [F]
  1h，count：[A] [B] [C] [D] [E] [F]
  1h，min  ：[A] [B] [C] [D] [E] [F]
  1h，max  ：[A] [B] [C] [D] [E] [F]
```

此例有 12 份时间列负载和 60 份数值列负载；A 的五个 5m 特征共同引用 A 的 5m 时间列。若同一 TSID 有多个 Block，则在各自区段内按时间展开为 `A1、A2、…、B1、B2、…`。

方框表示逻辑编码负载，不表示固定字节长度。常量编码允许零字节负载，定位信息仍存在于 header；header 存放在 `index.bin`，不夹在时间戳或数值负载之间。

### 3.3 TSID 与原生 blockHeader

当前集群版 TSID 为 32 字节，序列化顺序如下，比较顺序与字段顺序相同：

| 字节偏移 | 字节数 | 字段 |
|---:|---:|---|
| 0 | 4 | `AccountID` |
| 4 | 4 | `ProjectID` |
| 8 | 8 | `MetricGroupID` |
| 16 | 4 | `JobID` |
| 20 | 4 | `InstanceID` |
| 24 | 8 | `MetricID` |

每条索引条目直接使用 89 字节的原生 `blockHeader`：

| 字节偏移 | 字节数 | 字段 |
|---:|---:|---|
| 0 | 32 | `TSID` |
| 32 | 8 | `MinTimestamp` |
| 40 | 8 | `MaxTimestamp` |
| 48 | 8 | `FirstValue` |
| 56 | 8 | `TimestampsBlockOffset` |
| 64 | 8 | `ValuesBlockOffset` |
| 72 | 4 | `TimestampsBlockSize` |
| 76 | 4 | `ValuesBlockSize` |
| 80 | 4 | `RowsCount` |
| 84 | 2 | `Scale` |
| 86 | 1 | `TimestampsMarshalType` |
| 87 | 1 | `ValuesMarshalType` |
| 88 | 1 | `PrecisionBits` |

header 不含分辨率、特征或独立的时间戳精度字段。分辨率和特征由所属 metaindex 行给出；`PrecisionBits` 同时约束时间戳和值。同一批次的五个 header 必须具有相同的 TSID、行数、时间范围、时间戳 offset/size/编码类型和精度。

二进制整数复用 `lib/encoding`：无符号整数使用大端序，有符号整数先进行 ZigZag 转换，再按固定宽度大端序写入。

### 3.4 index 与 metaindex

```text
index.bin 的每个 index block：
  "VMDSIX\x00\x02" + ZSTD(blockHeader × BlockHeadersCount)

metaindex.bin：
  "VMDSMI\x00\x02" + ZSTD(downsampleMetaindexRow × 行数)
```

上述 8 字节前缀位于 ZSTD 帧之外。一个 index block 的解压上限为 65536 字节，包含 1～736 条原生 header；`BlockHeadersCount` 表示该分辨率、该特征下的单列 Block 数，不要求为 5 的倍数。

`downsampleMetaindexRow` 直接嵌入原生 `metaindexRow`，然后追加特征、分辨率、末 TSID 和行数。当前集群版每行 113 字节：

| 字节偏移 | 字节数 | 字段 |
|---:|---:|---|
| 0 | 32 | `TSID`：该 index block 的首 TSID |
| 32 | 4 | `BlockHeadersCount` |
| 36 | 8 | `MinTimestamp` |
| 44 | 8 | `MaxTimestamp` |
| 52 | 8 | `IndexBlockOffset` |
| 60 | 4 | `IndexBlockSize` |
| 64 | 1 | `feature`：0=last、1=sum、2=count、3=min、4=max |
| 65 | 8 | `ResolutionMs` |
| 73 | 32 | `LastTSID` |
| 105 | 8 | `RowsCount` |

前 64 字节由 `metaindexRow.Marshal` 生成，字段之间没有对齐填充。代码通过实际 marshal 长度计算结构尺寸；上表针对当前仓库的集群版 TSID。

metaindex 行的时间范围覆盖其全部 header，`RowsCount` 为该 index block 所属单列的物理行数，须满足 `BlockHeadersCount ≤ RowsCount ≤ BlockHeadersCount × 8192`。首末 TSID 必须有序且属于同一租户。writer 在 index 达到大小上限、租户变化或特征结束时输出当前 index，保证行内归属一致。

### 3.5 metadata.json

`downsamplePartMetadata` 嵌入原生 `partHeader`。以下字段必须存在且不能为 `null`：

| 字段 | 约束 |
|---|---|
| `FormatVersion`、`SemanticsVersion` | 均为 2 |
| `Mode` | `downsampling` |
| `Resolutions` | `[300000, 3600000]`，顺序固定 |
| `BucketOrigin` | 0 |
| `NumericCodec` | `decimal-values` |
| `Retention` | `bucket-end` |
| `MinDedupInterval` | 0 |
| `RowsCount`、`BlocksCount` | 所有分辨率、所有单特征 Block 的物理统计，均为 5 的倍数 |
| `MinTimestamp`、`MaxTimestamp` | part 实际共享时间戳的范围 |

非空 part 要求行数和 Block 数均大于零，且 Block 数不大于行数。物理 `RowsCount` 与浮点特征 `count` 含义不同。

## 4. 归并与读取

### 4.1 调度与动态分桶

生产归并在一个 UTC 自然月分区内执行。源可以同时包含内存原始 part、磁盘原始 part 和磁盘降采样 part。降采样沿用原始归并的选源规则：常规后台选择最多 15 个 part；刷盘和强制合并在找不到均衡组合时可以选择全部剩余源，不另设降采样源数量上限。

遍历顺序为：

```text
resolution → TSID → 源 part → Block 批次 → feature
```

每个分辨率建立一次 reader 堆，堆只按完整 TSID 排序。处理一个 TSID 时，先扫描所有相关源的 block header，得到完整时间范围，并将索引游标推进到下一个 TSID。聚合槽数为：

```text
bucketCount = maxTimestamp / resolution - minTimestamp / resolution + 1
```

`currentTSIDBucketSamples` 按该范围设置长度，包含中间空 bucket；容量不足才分配，否则清空有效范围后复用。31 天在 5m 和 1h 分辨率下分别最多需要 8928 和 744 个槽。`downsampleMaxPooledBuckets=8928*2` 只控制 `reset` 时保留的池缓存容量，不限制本次计算的槽数。

随后，`currentSourceReader` 重新定位这些源的当前 TSID，读取 payload 并累加到对应槽位。索引扫描与数据读取使用不同 reader，避免数据读取改变堆中已推进的索引位置。所有源贡献完成后，样本切片直接交给 writer，之后再处理下一个 TSID。

已删除的 MetricID 对应整条 TSID 跳过处理，并累计删除的源物理行。

同一源在一个目标分辨率下只贡献一种表示：原始样本按该分辨率分桶，降采样源只读取相同分辨率的五个特征。

### 4.2 多特征解码

`ReadBlock` 返回 `downsampleDecodedResolutionFeaturesBlock`。主 reader 默认定位 last，其余四个 feature 的 reader 按需创建，只负责各列索引的推进与对齐。

`Init` 绑定源 part、分辨率及可选特征，未指定特征时使用 last。`SetFilter` 设置目标 TSID 和时间范围；对于降采样 part，先在 metaindex 中二分定位分辨率和特征区段，再利用 `LastTSID` 定位可能包含目标 TSID 的首个 index。迭代时继续筛选时间范围相交的 index 和 Block。

首列通过原生 `Block.UnmarshalData` 完整解码时间戳和值；后四列验证共享时间戳描述后，由 `downsampleReader.readNativeValues` 仅读取各自 values，并直接调用原有 `encoding.UnmarshalValues` 解码，复用该原生 Block 中的时间戳。reader 负责清除上一列的值、校验时间戳与数值行数一致，并清理编码缓冲和重置读取位置；共享的 `block.go` 保持原有实现。时间戳在本次多特征批次内只读取、解码一次，下一批次重新读取；查询单列读取也独立解码。

原始输入通过原生 Block 解码，并展开为五个特征的输入值，由 merger 完成分桶。解码结果只保留当前批次，reader 不缓存整个数值文件。

### 4.3 文件访问与所有权

| 文件 | reader 字段 | writer 字段及偏移 |
|---|---|---|
| `timestamps.bin` | `timestampsReader`、`timestampsSize` | `timestampsWriter`、`timestampsOffset` |
| `values.bin` | `valuesReader`、`valuesSize` | `valuesWriter`、`valuesOffset` |
| `index.bin` | `indexReader`、`indexSize` | `indexWriter`、`indexOffset` |
| `metaindex.bin` | 打开 part 时载入 `dsMetaindex` | `metaindexWriter`，整体输出 |
| `metadata.json` | 打开 part 时解析为 `dsMetadata` | `Finish` 中的局部 writer，整体输出 |

当前 reader 通过 `filestream.ReadAtCloser` 按 header 中的 offset/size 读取。`filestream.ReaderAt` 封装文件打开、普通文件检查、偏移读取和关闭，直接写入调用方缓冲，不持有顺序游标，也不缓存整个文件；原有顺序读取接口 `filestream.ReadCloser` 保持不变。时间列和值列采用串行读取和可复用缓冲，文件整体读取顺序不是连续顺序扫描。

磁盘降采样 part 持有 `dsTimestampsFile`、`dsValuesFile`、`dsIndexFile` 三个长期句柄，reader 借用这些句柄。reader 为磁盘原始源自行打开三个文件，并负责关闭；内存源读取已有缓冲。调用方必须在 reader 使用期间持有源 part 引用。不同查询、打开校验和归并任务分别使用独立 reader 实例。

## 5. 写出与临时存储

### 5.1 样本直接编码

`WriteSamples` 只读借用当前 TSID 的 bucket 样本，不保留其引用。writer 跳过空槽，按最多 8192 个有效样本和连续相同精度拆分输出批次。

每个输出批次提取一份时间列，逐特征复用一份浮点缓冲，经 decimal 转换后初始化对应的原生 Block。该过程不构造完整的五列输出对象；连续单列取数和 `Block.Init` 的自有缓冲拷贝仍然存在。

五个原生 Block 分别调用 `MarshalData`，比较时间戳编码字节和描述是否一致。当前写入端对时间戳执行五次编码校验，向 `timestamps.bin` 只写入一次。各特征的「原生 header + values payload」分别追加到对应 spill，spill 不保存时间戳负载。

### 5.2 SpillWriter 与列顺序

`filestream.SpillWriter` 管理临时字节流，提供以下接口：

| 接口 | 行为 |
|---|---|
| `Write([]byte)` | 追加到内存；超过阈值时，将满块追加到同一个临时文件 |
| `ReadAll(func(io.Reader) error)` | 以流方式读出全部已写字节，只允许调用一次 |
| `Size()` | 返回已接收字节数，包含缓冲中的数据 |
| `Close()` | 丢弃未消费缓冲，关闭文件并删除临时文件和目录 |

每个 spill 的内存阈值由常量 `spillMaxMemorySize` 设为 **16 MiB**，缓冲按需增长，容量不超过阈值。累计数据不超过阈值（含恰好达到阈值）时，不创建任何临时文件或目录。满块之后仍有数据写入时，才将该满块落盘；超大的单次写入按同样规则分段处理。后续满块追加到同一个文件，最后一段保留在内存中。`ReadAll` 依次读取文件前缀和内存尾部，不为读回而将尾部落盘。

临时文件位于目标目录下独占的 `.spill-*` 子目录。子目录权限为 `0700 & ~umask`，文件权限为 `0666 & ~umask`。满块直接写文件，不再叠加写缓冲；文件读回沿用 filestream 的缓冲大小，使用独立读缓冲池，归还时解除文件引用。内存尾部不入池，在消费完成、关闭或出错时释放引用。读写沿用 filestream 的 I/O 统计，纯内存路径不计入实际文件 I/O；临时数据不执行 fsync。开始读回或发生错误后，禁止继续写入。

`ReadAll` 单独校验文件前缀的长度，不能以内存尾部补齐被截断的文件。回调必须消费全部逻辑字节。完成或出错后均释放内存、关闭并尝试删除临时文件；删除失败返回错误并保留路径，允许通过 `Close` 重试。

分辨率结束或 `Finish` 时，`flushResolution` 按 last、sum、count、min、max 的顺序，逐个完整读回 spill：

1. 校验原生 header、时间戳引用、Block 顺序与连续性。
2. 向 `values.bin` 追加 values，确定最终 `ValuesBlockOffset`。
3. 将更新后的 header 加入 index；达到容量上限、租户变化或特征结束时输出 index 和对应 metaindex 行。
4. 校验五列的 Block 数和行数一致，逐列关闭并删除已消费的 spill。

由此保证整个 part 的列顺序。时间戳 offset 在批次编码时确定；values offset 在 spill 读回时确定；index offset/size 在压缩输出时确定。

## 6. 格式校验与资源边界

### 6.1 打开校验

`openDownsamplePart` 有界读取 metadata 和 metaindex，核验版本、语义字段、行长度、排序、租户及物理统计，再为每个分辨率使用五个 reader 遍历全部 index header。

校验包括：

- 五列同时存在或同时结束；各对应 Block 的共享时间戳描述完全一致。
- 每条 header 的行数、时间域、编码类型、精度、offset/size 合法，且与所属 metaindex 行一致。
- 同一 index 行内的 TSID 均属于同一租户，并处于首末 TSID 范围内；行内和跨 index 的 TSID、时间顺序合法。
- index 对 `index.bin` 连续覆盖；以 last 列为唯一共享时间戳序列，跨分辨率连续覆盖 `timestamps.bin`；全部特征连续覆盖 `values.bin`。
- 拒绝缺列、多余 Block、截断、偏移溢出、越界、空洞、重叠和未引用尾部；零长度常量负载允许相邻 offset 相同。

各特征的 index 分块边界可以不同，校验按逻辑 Block 对齐五列。常量编码须对应零长度负载，二阶差分编码至少包含两行；offset 与 size 的和既不能越过文件范围，也不能超出有符号 64 位偏移范围。

打开校验读取全部索引，但不解码时间戳和值的实际数据。payload 的解压、数值解码和时间边界检查发生在实际读取 Block 时。

### 6.2 大小与内存

| 项目 | 上限或处理方式 |
|---|---|
| 降采样单列 Block 行数 | 8192 |
| 原始输入 Block 行数 | 16384 |
| 单时间列或数值列的编码负载 | 128 KiB |
| 单 index block 的压缩数据（含标识） | 128 KiB |
| 单 index block 的解压数据 | 64 KiB，当前集群版最多 736 条 header |
| metaindex 文件和解压数据 | 分别限制为 64 MiB |
| metadata 文件 | 64 KiB |
| 单个 spill 的内存缓冲容量 | 16 MiB，按需分配，关闭后不保留到池中 |

索引 reader 各自只保留当前 index block，源 part 的 metaindex 整体驻留内存。merger 的聚合状态按当前 TSID 的时间范围分配。writer 保留当前批次及其编码缓冲、当前分辨率的五个 spill 缓冲、临时文件读回缓冲、当前未输出的 index，以及整个目标 part 尚未压缩的 metaindex；压缩时复用压缩缓冲。五个 spill 当前持有的缓冲容量合计最多 80 MiB，不含扩容过程中尚待 GC 的旧分配及其他工作缓冲。

这些是单项边界，不是进程内存的统一额度。总占用还受源 part 数量、并发任务、对象池保留容量和系统页缓存影响。`reset` 清除 `currentResolutionReaders`、`currentResolutionReaderHeap` 和 `currentTSIDReaders` 中的全部源引用，将切片长度归零，保留底层数组用于复用；reader 指针切片不设容量丢弃阈值。bucket 切片仍以 `downsampleMaxPooledBuckets = 17856` 控制 `reset` 后保留的池缓存容量，该阈值不限制任务的 bucket 数量。

### 6.3 磁盘空间预算

令 `R` 为两个分辨率合计的降采样逻辑行数，`B` 为多特征批次数，`F=5`、`H=89`、`M=113`。[downsample_writer.go](lib/storage/downsample_writer.go) 中的 `estimateDownsampleOutputSize` 采用以下保守预算：

| 部分 | 字节数上界 |
|---|---|
| 最终时间戳和值负载 | `10 × R × (F + 1)` |
| index 与 metaindex | `F × B × ((2H + 256 + 8) + (2M + 256 + 8))` |
| 与最终输出同时存在的 spill | `10 × R × F + F × B × H` |
| metadata | 64 KiB |

[downsample_part.go](lib/storage/downsample_part.go) 中的 `estimateDownsamplePartSize` 根据源统计计算所需行数：原始输入按两个目标分辨率估算；降采样源的物理行数除以五并向上取整。未知 TSID 分布时取 `B=R`，按每行单独占据 Block 和 index 的情况估算。writer 中的空间加法和乘法在溢出时饱和到 `math.MaxUint64`，不将删除源文件作为可用空间。

[downsample_partition.go](lib/storage/downsample_partition.go) 管理预算类型、进程级预算、预留与缓存有效期。进程内所有目录的降采样任务共享磁盘预算，并保留 `freeDiskSpaceLimitBytes` 指定的最低空闲空间。已释放的预算继续计入占用两秒，覆盖空闲空间查询的缓存周期。writer 在写批次、读回 spill 和最终输出前重新查询可用磁盘空间，该查询仍有两秒缓存；空间不足返回可识别的 `errDownsampleNoSpace`。预算检查不能替代实际 I/O 错误处理。

## 7. 发布、失败处理与启动

最终文件使用原有 `filestream.MustCreate` 创建，字段直接声明为 `filestream.WriteCloser`，权限为 `0666 & ~umask`。文件关闭调用原有 `MustClose`；存储层在调用前移除持有引用，避免后续重复关闭。已有目标目录会导致初始化失败，该目录不归当前 writer 清理。

降采样自身的校验、取消、spill、偏移读取以及接口返回的写入错误由本模块处理。最终文件创建、缓冲刷出、同步和关闭复用原始链路的 `Must` 行为，目录同步复用 `fs.MustSyncPath`；这些共享操作失败时仍采用原有致命错误处理。`lib/filestream` 的既有 Writer、Reader、接口和缓冲池，以及 `lib/fs` 的实现保持不变。新增组件只有纯临时字节流 `SpillWriter` 和独立偏移读取 `ReaderAt`。

`Finish` 完成所有 spill、index、metaindex、metadata 的写入以及文件和目录同步后，返回可发布的 partHeader。没有有效行时不发布空 part。目标在发布前通过 `openDownsamplePart` 再次校验。

分区在活动清单所在目录创建独占临时子目录，在其中通过原有 writer 写入 `parts.json`；文件关闭并同步后，通过原子 rename 替换清单。临时子目录由本次作业清理，既有临时目录及其内容不受影响。rename 是提交点：

| 阶段 | 失败处理 |
|---|---|
| 提交前 | 保留源 part、旧清单和活动集合，关闭目标句柄，删除未发布目标、spill 和临时清单 |
| 提交后返回错误 | 目标已经发布，内存活动集合与新清单保持一致，保留旧磁盘源文件，禁止 Abort 已发布目标；实际目录同步仍遵守共享 Must 语义 |
| 正常提交完成 | 按引用计数回收已替换的源 part |

writer 自身验证、spill 读回及接口返回的写入错误会阻止发布，并触发 Abort；后续写入继续返回已记录的错误。Abort 通过原有 MustClose 关闭最终文件，再删除未发布目录；只有 spill 可以直接丢弃尚未写出的缓冲。reader 会尝试关闭所有自有句柄并聚合关闭错误，借用的降采样句柄仍由 part 管理。清理失败同样返回错误；只有实际删除成功，才能认为目标已清理。

`Merge` 返回前归还全部 reader 和多特征解码对象，统计按值返回。对象归还或句柄关闭后清除引用，后续重置不重复释放该资源。writer 的 `reset` 只清理内存状态；`Abort` 负责未发布目标的资源清理，目录删除失败时保留路径及已有错误，后续仅重试尚未完成的删除。降采样专属错误和日志均使用英文，并以 `[downsampling]` 开头。

降采样任务的普通错误和取消以错误返回，调度层结束本次任务。周期刷盘保留失败源；关闭或快照所需的最终刷盘，在降采样失败后将剩余内存源按原始格式持久化。最终刷盘还会在持有 `partsLock` 时同步清单目录，即使没有剩余内存源也执行。

原始格式回退或最终持久化仍失败时，采用存储层的致命错误处理。通用文件系统接口、已提交源回收和引用计数等程序不变量保留原有语义，不统一转换为可忽略的降采样错误。

启动预检查、分区发现和活动清单解析由 `downsample_partition.go` 负责。预检查在取得目录锁、检查恢复状态之后，IndexDB 和后台任务初始化之前执行。存在 `parts.json` 时，预检查只检查清单引用的活动 part；缺少清单时，按原有目录发现规则识别 small、big 下的 part。

分区发现排除快照目录、空目录和带有 `.delete-this-dir` 删除标记的目录。清单中的非法 part 名称、未知字段、重复字段、重复 part 名称，以及活动 part 目录缺失，均返回错误。存在活动降采样 part 时必须启用降采样，快照中的非活动文件不构成此开关冲突。

合法原始文件可以与降采样文件共存，并在后续归并时转换。格式检测结合 metadata 和文件标识；带有降采样语义但版本缺失、标识矛盾或内容损坏的文件会报错，不按原始格式打开。降采样 reader 只接受本文规定的布局；版本值相同并不代表其他二进制布局兼容。当前不提供布局迁移。

## 8. 字段查询

`query.field` 使用 `分辨率:特征` 形式，例如 `5m:last` 或 `1h:count`，共十种合法组合。参数经 HTTP、EvalConfig、SearchQuery 传递到存储端。非法参数返回错误；字段查询禁用结果缓存及为缓存进行的时间范围对齐。

| 查询方式 | 读取范围 |
|---|---|
| 指定 `query.field` | 只读取已落盘的降采样 part 中对应分辨率、对应特征；跳过原始 part |
| 未指定 `query.field` | 使用原有查询流程；跳过降采样 part |

字段 RPC 为 `search_downsampling_v2`。它保留原有租户、时间范围和筛选条件的组织方式，在查询负载后追加 8 字节分辨率和 1 字节特征编号；编号与磁盘格式相同。响应继续使用原生 MetricBlock。字段请求要求全部目标存储节点成功，节点不支持该协议时返回错误，不接受部分结果。

查询由 `partSearch` 使用 `downsampleReader` 定位目标列 header，再由 `BlockRef` 和原生 `Block` 按偏移读取 payload；查询不读取其余四个特征的 index 和 payload。存储层按共享时间戳选择记录，不恢复或重新裁剪 bucket 内原始贡献；HTTP 查询继续遵循 PromQL 的回看窗口和求值网格。

vmselect 与 vmstorage 的去重配置独立生效，需要保留全部降采样记录时，两端都应设置 `dedup.minScrapeInterval=0`。当前字段接口不聚合尚未合并的跨 part 记录，不组合原始与降采样结果，也不提供自定义分辨率或任意时间区间的精确原始聚合。
