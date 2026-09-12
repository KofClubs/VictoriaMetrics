# 降采样存储设计

本文定义当前集群版实现的数据语义、文件格式、读写流程和运行边界。代码入口见[实现说明](downsampling_storage_implement.md)，审查要点见[审查说明](downsampling_storage_review.md)，测试命令与用途见[测试说明](downsampling_storage_test.md)。

## 1. 适用范围与术语

通过 `-storage.downsampling.enabled=true` 或 `OpenOptions.DownsamplingEnabled=true` 启用文件降采样，默认关闭。启用时要求存储端 `dedup.minScrapeInterval=0`。

分辨率由不可变的基础分辨率和各租户的额外分辨率组成。默认仅保留 `5m` 基础分辨率；每个租户始终包含基础分辨率，其额外分辨率必须更大且为基础分辨率的整数倍。特征固定为 `last`、`sum`、`count`、`min`、`max`，磁盘格式和查询协议统一采用编号 `0..4`。

启动参数 `-storage.downsampling.config` 接收完整 JSON，例如：

```json
{
  "base_resolution": "5m",
  "tenant_resolutions": [
    {"tenant": "1:0", "resolutions": ["1h"]},
    {"tenant": "2:0", "resolutions": ["30m", "2h"]}
  ]
}
```

租户使用规范的 `accountID:projectID` 十进制字符串。上例中 `1:0` 保存 5m、1h，`2:0` 保存 5m、30m、2h，未列出的租户只保存 5m。时间间隔必须为正整数毫秒，接受 Go duration 语法及整数 `d`、`w`、`y`；1d=24h、1w=7d、1y=365d。配置拒绝未知字段、重复键、重复租户、重复分辨率、null、非整数倍及不合法时间间隔，输入大小不得超过 64 KiB。解析时还以规范化配置和最大长度的统计／时间字段构造 metadata，要求其编码不超过 64 KiB，避免启动或 API 接受后无法生成 part。租户数和分辨率数不另设固定上限，受实际编码大小约束。

`GET /internal/downsampling/config` 返回当前配置，`PUT` 用完整 JSON 替换租户额外分辨率；可用 `-downsamplingConfigAuthKey` 设置该接口的附加认证，原有 HTTP 全局认证同样生效。基础分辨率写入 storage 的 `metadata/downsampling.json`，启用后不可在运行中或重启时更改，即使当前没有活动 part。租户配置运行时更新只影响后续归并，重启后取启动 JSON；已开始的任务和已有 part 各持有自己的不可变配置快照。更新配置不会立即重写旧 part。

更新 API 只作用于接收请求的 vmstorage，不向其他节点广播。请求必须包含完整配置，未列出的租户恢复为仅基础分辨率；例如：

```sh
curl --request PUT 'http://127.0.0.1:8482/internal/downsampling/config' \
  --header 'Content-Type: application/json' \
  --data '{"base_resolution":"5m","tenant_resolutions":[{"tenant":"1:0","resolutions":["1h"]},{"tenant":"2:0","resolutions":["30m","2h"]}]}'
```

配置了接口认证或全局 HTTP 认证时，请求应携带对应凭据。成功响应的 `data` 返回当前完整配置；非法配置返回 HTTP 400，当前配置不变。

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

降采样专属生产代码按 config、block、metaindex row、reader、writer、merger、part、partition、query 九个模块组织。block 定义样本、分桶计算、多特征解码缓冲及 header 校验；metaindex row 仅定义索引行及其编解码；part 管理格式标识、元数据大小限制和原生 header 索引解码；query 包含分辨率与特征解析、查询协议编解码和 part 搜索定位。各模块的代码导航与调用边界见实现说明。

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

五个数值列保留源 `precisionBits`，有效范围为 `1..64`，通常为 64；各列独立保存 `Scale`、`FirstValue` 和编码类型。时间戳始终无损编码，精确保留 bucket 内有效贡献的最大时间戳。专属 `marshalDownsampleBlock` 复用原生 `Block.MarshalData`，仅在源精度低于 64 时替换时间戳编码及其 header 描述，共享 `block.go` 保持不变。

`downsampleSample.precisionBits=0` 表示空槽；首次贡献写入完整样本，后续贡献通过 `Merge` 累加。同一 bucket 的源精度不一致时返回错误；不同 bucket 的精度不同时，writer 拆分输出 Block。浮点转换和有损压缩遵循原生编码语义。

编码前要求时间戳严格递增，且一个输出批次内每个 bucket 只有一行。时间戳编码不得移动 bucket 或改变最新贡献的位置；同一 TSID、同一分辨率的相邻 Block 时间范围不得重叠。

### 2.3 保留期限

保留期限由基础输入统一处理：保守保留当前配置与源 part 配置中尚有效的粗粒度 bucket 所需贡献。基础样本先聚合一次，再供全部额外分辨率使用，不能提前删除仍被有效粗粒度 bucket 需要的基础贡献。

为此，每个源 TSID 根据当前配置及源 part 配置的分辨率，将保留边界向前扩展到相关 bucket 的最早起点。基础列保存该前缀，全部额外列完整表达同一份保留输入，不再次按自身 bucket 右端点删除前缀，否则查询选择最大整除列时会遗漏基础列仍有的贡献。查询在完整聚合之后应用输出时间范围和保留期限。内存原始归并在降采样模式下向原始 merger 传入配置计算的保守保留边界；内存及磁盘 part 删除通过 `partExpired` 同时考虑当前配置与源 part 配置。整个月份分区删除通过 `retentionExpired` 检查分区边界和全部活动 part，避免粗粒度 bucket 横跨月末时提前删除其贡献。未启用降采样时沿用原始保留规则。

源行统计只针对实际读取的基础输入：原始样本每行计一次，降采样基础记录按五个单值 Block 的物理行换算。旧额外分辨率由基础列重建，不重复读取或计数。物理行统计与浮点特征 `count` 分开计算。

## 3. 文件布局

### 3.1 文件与排序规则

每个降采样 part 包含五个最终文件：

| 文件 | 内容 |
|---|---|
| `timestamps.bin` | 每个多特征批次的一份编码时间戳负载 |
| `values.bin` | 五个特征各自的编码数值负载 |
| `index.bin` | 分块压缩的原生 `blockHeader` |
| `metaindex.bin` | 压缩的降采样 metaindex 行，用于定位 index block |
| `metadata.json` | 原生 partHeader 的五个统计字段及 downsampling_config；完整降采样文件的写入完成标志 |

`parts.json` 是分区的活动 part 清单，位于 `smallPartsPath`，不属于单个 part 目录。

文件的物理排序规则为：

```text
timestamps.bin       : resolution → TSID.Less → MinTimestamp
values.bin/index.bin : resolution → feature → TSID.Less → MinTimestamp
metaindex.bin        : 与 index.bin 中各 index block 的顺序一致
```

分辨率按毫秒数递增排列，只包含本 part 实际写入的分辨率；每段只包含配置了该分辨率的租户及其 TSID。同一 `(resolution, feature)` 的全部数值 Block 和索引条目连续存放。时间戳不按 feature 重复存储；同一批次的五个 header 指向相同的时间戳 `(offset, size)`。

每个 index block 及其对应的 metaindex 行只属于一个分辨率、一个特征、一个租户。一个 index block 可以包含同租户的多个 TSID，也可以包含同一 TSID 的多个 Block。租户由 `TSID.AccountID` 和 `TSID.ProjectID` 确定，不单独增加磁盘字段。

### 3.2 六个 TSID 的顺序示例

沿用上面的配置，假设租户 `1:0` 有 A、B，租户 `2:0` 有 C、D，租户 `3:0` 有 E、F，并且 `A < B < C < D < E < F` 符合 `TSID.Less`。每个 TSID 在其各分辨率下恰有一个多特征批次：

```text
timestamps.bin
  5m ：[A] [B] [C] [D] [E] [F]
  30m：[C] [D]
  1h ：[A] [B]
  2h ：[C] [D]

values.bin（index.bin 的 header 顺序相同）
  5m,last  → A B C D E F
  5m,sum   → A B C D E F
  5m,count → A B C D E F
  5m,min   → A B C D E F
  5m,max   → A B C D E F
  30m,last → C D
  30m,sum  → C D
  30m,count→ C D
  30m,min  → C D
  30m,max  → C D
  1h,last  → A B
  1h,sum   → A B
  1h,count → A B
  1h,min   → A B
  1h,max   → A B
  2h,last  → C D
  2h,sum   → C D
  2h,count → C D
  2h,min   → C D
  2h,max   → C D
```

各行从左至右、各行从上至下构成同一文件的连续顺序。此例有 12 份时间列负载和 60 份数值列负载；同一 TSID、同一分辨率的五个特征引用同一份时间列。若有多个 Block，则在各自区段内展开为 `A1、A2、…、B1、B2、…`。

方框表示编码负载，不表示固定长度。常量编码允许零字节负载，定位信息仍存在于 header；header 只存于 `index.bin`。

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

header 不含分辨率、特征或独立的时间戳精度字段。分辨率和特征由所属 metaindex 行给出；`PrecisionBits` 保留源值精度，时间戳编码固定无损；解码依据各自的编码类型，不需要额外的时间戳精度字段。同一批次的五个 header 必须具有相同的 TSID、行数、时间范围、时间戳 offset/size/编码类型和精度。

二进制整数复用 `lib/encoding`：无符号整数使用大端序，有符号整数先进行 ZigZag 转换，再按固定宽度大端序写入。

### 3.4 index 与 metaindex

```text
index.bin 的每个 index block：
  "VMDSIX\x00\x02" + ZSTD(blockHeader × BlockHeadersCount)

metaindex.bin：
  "VMDSMI\x00\x02" + ZSTD(downsampleMetaindexRow × 行数)
```

上述 8 字节是固定文件签名，位于 ZSTD 帧之外；完整比较这些字节，不据此选择兼容分支。一个 index block 的解压上限为 65536 字节，包含 1～736 条原生 header；`BlockHeadersCount` 表示该分辨率、该特征下的单列 Block 数，不要求为 5 的倍数。

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

`downsamplePartMetadata` 只包含原生 `partHeader` 的五个统计字段及 `downsampling_config`。降采样 metadata 中以下字段必须完整且不能为 `null`：

| 字段 | 约束 |
|---|---|
| `downsampling_config` | 生成本 part 时的基础分辨率及实际出现租户的额外分辨率快照；不包含无关租户 |
| `MinDedupInterval` | 0 |
| `RowsCount`、`BlocksCount` | 所有分辨率、所有单特征 Block 的物理统计，均为 5 的倍数 |
| `MinTimestamp`、`MaxTimestamp` | part 实际共享时间戳的范围 |

非空 part 要求行数和 Block 数均大于零，且 Block 数不大于行数。物理 `RowsCount` 与浮点特征 `count` 含义不同。仅非 `null` 的 `downsampling_config` 标识降采样格式，并且必须能解析为有效配置；空对象、字符串、缺少基础分辨率等内容均报错。该字段缺失或为 `null` 时按原始 metadata 校验。配置决定每个租户允许保存的分辨率；实际写入的分辨率按有序 metaindex 的区段确定，不在 metadata 中另存列表。已配置的额外分辨率可以没有实际数据。

`metadata.json` 在四个 `.bin` 文件完成写入、关闭和同步，且全部 spill 清理完毕、part 目录同步后最后生成。writer 直接创建并写入该文件，关闭并同步后，再同步 part 目录和父目录。识别完成标志必须成功解析 JSON，并校验有效配置及完整统计字段，不能只检查文件是否存在。活动集合仍由分区的 `parts.json` 决定。

## 4. 归并与读取

### 4.1 调度与动态分桶

生产归并在一个 UTC 自然月分区内执行。源可以同时包含内存原始 part、磁盘原始 part 和磁盘降采样 part。降采样沿用原始归并的选源规则：常规后台选择最多 15 个 part；刷盘和强制合并在找不到均衡组合时可以选择全部剩余源，不另设降采样源数量上限。

降采样文件归并在等待后台并发名额或强制合并调度名额时检查取消信号；取消后释放尚未开始处理的源合并标记，保留源数据，不再等待空闲名额。同一批已开始的任务仍需完成退出和清理，并在读取、聚合、写出和目标校验循环中检查同一信号。

计算顺序为：

```text
TSID → 汇总全部源的基础样本 → 写基础列 → 从基础样本推导本租户各额外分辨率
```

整次归并只建立一次基础 reader 堆，堆只按完整 TSID 排序。处理一个 TSID 时，顺序扫描所有相关源的首列 block header，按值保存到各源 reader 的 `currentTSIDBlockHeaders`，同时汇总完整时间范围和源行数，并将首列索引游标推进到下一个 TSID 或源末尾。降采样源只读取与当前基础分辨率相同的五列，原始源只读取一次原生数据；不读取旧额外列，也不按目标分辨率重扫原始数据。

基础分辨率的时间范围槽数为 `maxTimestamp / base - minTimestamp / base + 1`。当槽数不大于源行数时，按完整范围分配密集槽；范围更稀疏时，用 bucket 到槽号的映射只为实际遇到的 bucket 分配状态，全部源贡献完成后按时间排序。额外分辨率顺序消费已完成的基础样本，只保存非空 bucket，不再读取源文件。

密集 31 天在 5m 基础分辨率下需要至多 8928 个槽；自定义基础分辨率按实际范围计算，并在分配前申请归并内存额度。基础槽和额外分辨率输出缓冲在任务内分别复用，writer 只读借用样本；任务结束后解除样本引用并归还额度，不在 merger 池中保留大数组。

`readSource` 使用缓存 header 读取 payload；其他四个特征的索引游标只向前对齐，不重新解码首列索引，不改变堆使用的首列 header。各源 header 消费后清空，reader 关闭时释放容量。当前 TSID 的所有基础与额外列完成后再处理下一 TSID。

已删除的 MetricID 只扫描基础首列 header 并累计删除的输入物理行，不保存 header 或读取 payload。

### 4.2 多特征解码

`ReadBlock(decoded, header)` 按已保存的首列 header 填充 `downsampleDecodedResolutionFeaturesBlock`，固定对齐同一分辨率的五个特征。reader 内嵌五个轻量索引游标，每个游标保存对应特征的索引位置、当前 header 和工作缓冲，不持有文件，不单独进入对象池。索引与 payload 均由外层 reader 持有的三个文件读取对象访问。

`Init(p, resolution)` 绑定源 part 和分辨率，顺序扫描常驻内存的 metaindex，为该分辨率的各特征建立完整索引区间；原始源使用全部原生索引。`NextHeader` 只推进降采样 last 列或原始索引，`Header` 返回的位置在下次推进时失效，调用方须按值保存后才能延后读取。`ReadBlock` 按同一源的 TSID、时间顺序消费保存的 header，即使首列已经推进到下一 TSID 或 EOF，也可读取此前的数据。 首列推进和其他特征向前对齐时均检查当前归并的取消信号；共享文件 I/O 调用本身仍按原有方式完成。reader 不提供 TSID 定位、任意时间窗口或单特征查询入口。

首列通过原生 `Block.UnmarshalData` 完整解码时间戳和值；后四列验证共享时间戳描述后，由 `downsampleReader.readNativeValues` 仅读取各自 values，并直接调用原有 `encoding.UnmarshalValues` 解码，复用该原生 Block 中的时间戳。reader 负责清除上一列的值、校验时间戳与数值行数一致，并清理编码缓冲和重置读取位置；共享的 `block.go` 保持原有实现。时间戳在本次多特征批次内只读取、解码一次，下一批次重新读取；查询单列读取也独立解码。

原始输入通过原生 Block 解码，并展开为五个特征的输入值，由 merger 完成分桶。解码结果只保留当前批次，reader 不缓存整个数值文件。

### 4.3 文件访问与所有权

| 文件 | reader 字段 | writer 字段及偏移 |
|---|---|---|
| `timestamps.bin` | `timestampsReader`、`timestampsFileSize` | `timestampsWriter`、`timestampsBlockOffset` |
| `values.bin` | `valuesReader`、`valuesFileSize` | `valuesWriter`、`valuesBlockOffset` |
| `index.bin` | `indexReader`、`indexFileSize` | `indexWriter`、`indexBlockOffset` |
| `metaindex.bin` | 打开 part 时载入 `dsMetaindex` | `metaindexWriter`，整体输出 |
| `metadata.json` | 格式检测及打开 part 时解析为 `dsMetadata` | `Finish` 最后直接创建、写入、同步并关闭 |

归并专属的 `downsampleReader` 通过 `filestream.ReadAtCloser` 按 header 中的 offset/size 读取。`filestream.ReaderAt` 封装文件打开、普通文件检查、偏移读取和关闭，直接写入调用方缓冲，不持有顺序游标，也不缓存整个文件。其接口只有 `Path`、`Size`、`ReadAt` 和 `Close`，读取和关闭错误通过返回值处理；不提供 `MustReadAt`、`MustClose`，不能作为查询的 `fs.MustReadAtCloser` 使用。原有顺序读取接口 `filestream.ReadCloser` 保持不变。时间列和值列采用串行读取和可复用缓冲，文件整体读取顺序不是连续顺序扫描。

磁盘 part 的查询读取对象统一由原有 `timestampsFile`、`valuesFile`、`indexFile` 三个 `fs.MustReadAtCloser` 字段持有，实际类型均为 `*fs.ReaderAt`。原始与降采样查询共用 mmap、页驻留状态检查、系统调用回退、读取统计和索引缓存；`fs.disableMmap`、`fs.disableMincore` 的含义及实现保持不变。文件按原有机制延迟打开，不额外保存一组降采样查询句柄。

归并 reader 为原始磁盘和降采样磁盘源均自行打开三个文件，并负责关闭，不借用 part 的查询读取对象。内存源读取已有缓冲。part 打开校验使用独立的临时偏移读取对象，在返回前关闭，不将校验句柄交给查询。新目标校验接收本次归并的取消信号，在 metaindex、索引组和基础列对齐循环中检查，取消后关闭校验句柄并阻止发布。调用方在归并 reader 使用期间持有源 part 引用；查询也沿用原有引用计数，旧 part 退出活动集合后，仍须等待最后一个引用释放才关闭及删除。归并调度沿用 `isInMerge`，不新增查询与归并之间的共享锁。

## 5. 写出与临时存储

### 5.1 样本直接编码

`WriteSamples` 只读借用当前 TSID 的 bucket 样本，不保留其引用。writer 跳过空槽，按最多 8192 个有效样本和连续相同精度拆分输出批次。

每个输出批次提取一份时间列，逐特征复用浮点缓冲与 decimal 整数缓冲，并依次初始化同一个 `currentFeatureBlock`。每列编码完成后立即写入对应 spill，不同时保留五个原生 Block 的工作缓冲。连续单列取数和 `Block.Init` 的自有缓冲拷贝仍然存在。

每个特征均通过 `marshalDownsampleBlock` 复用原生编码并保证时间戳无损。首列的编码时间戳独立复制到 `currentBlockTimestampsData`，并以局部 header 保存共享描述，供后四列逐一核对；下一列编码不会覆盖校验基准。写入端对时间戳执行五次编码校验，向 `timestamps.bin` 只写入一次。writer 同时持有的分辨率数是本 part 所有租户实际输出分辨率的并集大小，每个分辨率各有五个 spill：last 追加「原生 header + timestamps payload + values payload」，其余特征追加「原生 header + values payload」。暂存 header 的时间戳偏移相对于该分辨率；同一批次只暂存一份时间戳。后续列失败时，整个未发布目标由 `Abort` 删除。

### 5.2 SpillWriter 与列顺序

`filestream.SpillWriter` 管理临时字节流，由 `NewSpillWriter(dir, name)` 创建；`name` 标识同一目录中的独立 spill。提供以下接口：

| 接口 | 行为 |
|---|---|
| `Write([]byte)` | 预算允许时追加到内存；超过单个缓冲阈值或进程预算不足时，追加到同一个临时文件 |
| `Read(func(io.Reader) error)` | 以流方式读出全部已写字节；可重复调用，不关闭或删除临时文件 |
| `Size()` | 返回已接收字节数，包含缓冲中的数据 |
| `Close()` | 释放内存，关闭并删除本 spill 创建的临时文件 |

每个 spill 的内存阈值默认为 **16 MiB**，通过 `-downsampling.spillMaxMemorySize` 配置，必须大于零；实例在首次非空写入时固定该值。全部 spill 共享进程级内存预算 `min(256 MiB, memory.Allowed()/10)`，扩容时同时计入仍在使用的旧缓冲和完整新分配。预算允许且累计数据不超过单个阈值（含恰好达到阈值）时，不创建临时文件；满块后仍有数据时将该满块追加到同一个文件。预算不足时，先写出已有尾部并释放缓冲，再直接写入文件，不等待其他 spill 释放额度。后续写入可重新申请内存。`Read` 依次读取文件前缀和内存尾部，不为读回而将尾部落盘；`Close` 释放缓冲额度。

临时文件直接位于调用方指定目录，名称为 `.spill-<name>`，文件权限为 `0666 & ~umask`。降采样 writer 使用 `<resolution毫秒数>-<feature>` 区分 spill，独占目标 part 目录，按需为实际分辨率建立五个 spill。满块直接写文件，不再叠加写缓冲；文件读回沿用 filestream 的缓冲大小，使用独立读缓冲池，归还时解除文件引用。内存尾部不入池，在 `Close` 时释放引用。读写沿用 filestream 的 I/O 统计，纯内存路径不计入实际文件 I/O；临时数据不执行 fsync。

`Read` 单独校验文件前缀长度，不能以内存尾部补齐被截断的文件；回调必须消费全部逻辑字节。读取成功或失败均不关闭、删除或封闭 spill，调用方最后必须显式执行 `Close`。写入失败会封闭 spill 并立即尝试清理；删除失败返回错误并保留路径，允许通过 `Close` 重试。单个 spill 不支持并发使用。

writer 允许按 TSID 交错写入各分辨率；每个分辨率独立校验 TSID、时间戳与 bucket 顺序。`Finish(stopCh)` 接收当前任务的取消信号，对实际分辨率排序，再逐个调用 `flushResolution`，按 last、sum、count、min、max 消费各 spill：

1. 校验原生 header、相对时间戳引用、Block 顺序与连续覆盖。
2. 读取 last 中的唯一时间列并追加到 `timestamps.bin`，将五列时间偏移统一转换为最终文件偏移。
3. 向 `values.bin` 追加当前列的 values，确定最终 `ValuesBlockOffset`。
4. 将更新后的 header 加入 index；达到容量上限、租户变化或特征结束时输出 index 和 metaindex 行。
5. 核对五列 Block 数、行数与完整时间列范围，逐列清除引用并关闭已消费 spill；读回或关闭失败均退出本次写入。

全部最终数据在 `Finish(stopCh)` 中按文件全局顺序生成。入口、每个 feature、每条 spill header 读取前，以及四个 bin 同步完成但尚未创建 metadata 时检查取消；取消返回 `errForciblyStopped`，进入统一 Abort，释放尚存资源并删除未发布目录。已关闭句柄不重复关闭。时间列与值列最终偏移在 spill 读回时确定，index 偏移与长度在压缩输出时确定。

## 6. 格式校验与资源边界

### 6.1 打开校验

`openDownsamplePart` 有界读取 metadata 和 metaindex，核验配置、统计字段、文件签名、行长度、排序及租户归属，再由 part 层为每个分辨率维护五个索引游标，遍历全部 index header；不创建 `downsampleReader`。校验使用能返回错误的临时 `filestream.ReadAtCloser`，在成功和失败路径上均关闭并合并关闭错误；通过校验的 part 使用原有 `fs.ReaderAt` 建立查询读取对象。

校验包括：

- 顺序读取 metaindex 中实际存在的分辨率区段，必须包含基础分辨率；每条额外列属于该租户配置，且额外列的 TSID 必须有基础数据。已配置但没有实际数据的额外分辨率无需出现；实际出现的每个分辨率均须通过完整的五特征校验。
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
| 单个 spill 的内存缓冲容量 | 默认 16 MiB，由 downsampling.spillMaxMemorySize 配置；按需分配，关闭后不保留到池中 |

降采样新增的可增长工作内存使用三份相互独立的进程级预算，并发任务或查询共享对应额度：

| 用途 | 预算 | 计费与不足时的处理 |
|---|---|---|
| SpillWriter 缓冲 | `min(256 MiB, memory.Allowed()/10)` | 按实际容量计费，扩容期间同时预留新旧缓冲；不足时直接落盘，不等待 |
| 归并 header、基础样本、派生样本及稀疏 bucket 映射 | `min(256 MiB, memory.Allowed()/16)` | 切片按实际容量计费，稀疏映射预留 1 KiB 加每项 256 字节；不足时返回错误并进入任务清理，不等待 |
| 查询目标 bucket 聚合 | `min(256 MiB, memory.Allowed()/20)` | 首次创建映射预留 1 KiB，每个 bucket 另按 256 字节保守计费，包含映射扩容和排序键；按当前查询的历史最大 bucket 数持有额度，直到 `tableSearch.reset` 释放；不足时返回查询错误 |

`memory.Allowed()` 来自 `-memory.allowedBytes` 或 `-memory.allowedPercent` 计算的可用额度。上述预算仅约束对应的降采样工作分配，不能相加后当作进程 RSS 硬上限；它们不包含全部原始链路共享缓冲、对象池、源 part 常驻 metaindex、文件 mmap、系统页缓存以及等待垃圾回收的无引用对象。writer 仍持有当前批次编码缓冲、当前 index 和目标 metaindex；reader 仍持有当前 index block 的工作缓冲。磁盘预留额度同样不约束这些内存。

每个源 reader 只缓存当前 TSID 的首列 header，不缓存完整 payload。merger 根据时间范围和源行数选择密集基础槽或实际 bucket 映射，再逐个生成额外分辨率。writer 的实际分辨率数是本 part 各租户输出分辨率的并集，每个分辨率有五个 spill；总 spill 缓冲受进程预算约束，不按分辨率或任务数量无限叠加。资源释放时先清除持有引用，再归还相应额度；失败和取消路径也必须释放，重复清理不得重复归还。

### 6.3 磁盘空间预算

令 `R` 为全部实际分辨率合计的降采样逻辑行数，`B` 为多特征批次数，`F=5`、`H=89`、`M=113`。[downsample_writer.go](lib/storage/downsample_writer.go) 中的 `estimateDownsampleOutputSize` 采用以下保守预算：

| 部分 | 字节数上界 |
|---|---|
| 最终时间戳和值负载 | `10 × R × (F + 1)` |
| index 与 metaindex | `F × B × ((2H + 256 + 8) + (2M + 256 + 8))` |
| 与最终输出同时存在的 spill | `10 × R × (F + 1) + F × B × H` |
| metadata | 64 KiB |

[downsample_part.go](lib/storage/downsample_part.go) 中的 `estimateDownsamplePartSize` 根据源统计计算所需行数：原始输入使用原始行数；降采样源优先累加基础 last 列的 metaindex 行数，没有索引统计时按物理行数除以五并向上取整。再乘当前配置的单租户最大分辨率数，保守估算每条源输入最多生成的输出。这一系数用于逐行磁盘估算，不代表 writer 同时活跃的分辨率并集大小。未知 TSID 分布时取 `B=R`，按每行单独占据 Block 和 index 的情况估算。writer 中的空间加法和乘法在溢出时饱和到 `math.MaxUint64`，不将删除源文件作为可用空间。

[downsample_partition.go](lib/storage/downsample_partition.go) 管理预算类型、进程级预算、预留与缓存有效期。进程内所有目录的降采样任务共享磁盘预算，并保留 `freeDiskSpaceLimitBytes` 指定的最低空闲空间。已释放的预算继续计入占用两秒，覆盖空闲空间查询的缓存周期。writer 在写批次、读回 spill 和最终输出前重新查询可用磁盘空间，该查询仍有两秒缓存；空间不足返回可识别的 `errDownsampleNoSpace`。预算检查不能替代实际 I/O 错误处理。

## 7. 发布、失败处理与启动

最终文件使用原有 `filestream.MustCreate` 创建，字段直接声明为 `filestream.WriteCloser`，权限为 `0666 & ~umask`。文件关闭调用原有 `MustClose`；存储层在调用前移除持有引用，避免后续重复关闭。已有目标目录会导致初始化失败，该目录不归当前 writer 清理。

降采样自身的校验、取消、spill、偏移读取以及接口返回的写入错误由本模块处理。最终文件创建、缓冲刷出、同步和关闭复用原始链路的 `Must` 行为，目录同步复用 `fs.MustSyncPath`；这些共享操作失败时仍采用原有致命错误处理。`lib/filestream` 的既有 Writer、Reader、接口和缓冲池，以及 `lib/fs` 的实现保持不变。新增组件只有纯临时字节流 `SpillWriter` 和独立偏移读取 `ReaderAt`。

非空目标的 `Finish(stopCh)` 按以下顺序完成，取消信号沿 spill 组装流程透传；不需要取消的调用传入 `nil`：

1. 读回并清理全部 spill，完成 `timestamps.bin`、`values.bin`、`index.bin` 和 `metaindex.bin` 的写入。
2. 通过原有 `MustClose` 刷出缓冲、同步并关闭四个 `.bin` 文件，再同步 part 目录。
3. 通过原有 `MustCreate` 直接创建 `metadata.json`，写入完整 JSON，再通过 `MustClose` 刷出缓冲、同步并关闭文件。
4. 同步 part 目录及其父目录，返回可发布的 partHeader。

内容合法且完整的 metadata 是降采样 part 的写入完成标志；仅存在 metadata 文件或数据文件不足以判断完成。没有有效行时不发布空 part。目标在活动集合发布前仍须通过 `openDownsamplePart` 再次校验；metadata 写入完成不替代活动清单提交。

分区在活动清单所在目录创建独占临时子目录，在其中通过原有 writer 写入 `parts.json`；文件关闭并同步后，通过原子 rename 替换清单。临时子目录由本次作业清理，既有临时目录及其内容不受影响。`parts.json` 的 rename 是活动集合的提交点：

| 阶段 | 失败处理 |
|---|---|
| 提交前 | 保留源 part、旧清单和活动集合，关闭目标句柄，删除未发布目标、spill 和临时清单 |
| 提交后返回错误 | 目标已经发布，内存活动集合与新清单保持一致，保留旧磁盘源文件，禁止 Abort 已发布目标；实际目录同步仍遵守共享 Must 语义 |
| 正常提交完成 | 将已替换源标记为可删除并释放引用；引用归零后依次执行 `decRef → part.MustClose → fs.MustRemoveDir`，原始及降采样磁盘源采用相同流程 |

writer 自身验证、spill 读回及接口返回的写入错误会阻止发布，并触发 Abort；后续写入继续返回已记录的错误。Abort 通过原有 MustClose 关闭最终文件，再删除未发布目录；只有 spill 可以直接丢弃尚未写出的缓冲。归并 reader 会尝试关闭所有自有句柄并聚合关闭错误，不关闭 part 的查询读取对象；后者由原有 part 引用生命周期管理。清理失败同样返回错误；只有实际删除成功，才能认为目标已清理。

`Merge` 返回前归还全部 reader 和多特征解码对象，统计按值返回。对象归还或句柄关闭后清除引用，后续重置不重复释放该资源。writer 的 `reset` 只清理内存状态；`Abort` 负责未发布目标的资源清理，目录删除失败时保留路径及已有错误，后续仅重试尚未完成的删除。降采样自身的存储与参数校验诊断使用英文，并以 `[downsampling]` 开头；共用 HTTP 和 RPC 查询流程的超时、写出、节点故障等错误沿用原有格式。

降采样任务的普通错误和取消以错误返回，调度层结束本次任务。周期刷盘保留失败源；关闭或快照所需的最终刷盘，在降采样失败后将剩余内存源按原始格式持久化。最终刷盘还会在持有 `partsLock` 时同步清单目录，即使没有剩余内存源也执行。

原始格式回退或最终持久化仍失败时，采用存储层的致命错误处理。通用文件系统接口、已提交源回收和引用计数等程序不变量保留原有语义，不统一转换为可忽略的降采样错误。

启动预检查、分区发现和活动清单解析由 `downsample_partition.go` 负责。预检查在取得目录锁、检查恢复状态之后，IndexDB 和后台任务初始化之前执行。存在 `parts.json` 时，预检查只检查清单引用的活动 part；缺少清单时，按原有目录发现规则识别 small、big 下的 part。

分区发现排除快照目录、空目录和带有 `.delete-this-dir` 删除标记的目录。清单中的非法 part 名称、未知字段、重复字段、重复 part 名称，以及活动 part 目录缺失，均返回错误。存在活动降采样 part 时必须启用降采样，快照中的非活动文件不构成此开关冲突。

合法原始文件可以与降采样文件共存，并在后续归并时转换。`detectDownsampleFormat` 只读取 metadata：`downsampling_config` 缺失或为 `null` 时校验原始 partHeader；非 `null` 时必须是有效配置，同时完整校验五个统计字段。配置或统计损坏时返回错误，不退回原始格式。缺少 metadata 的旧原始格式继续按目录名解析。格式识别不读取 metaindex，也不检查 `.bin` 文件大小或类型；固定签名、文件类型、长度和索引一致性由 `openDownsamplePart` 校验。格式检测成功不等于数据文件已经完整校验。降采样只接受本文规定的布局，当前不提供布局迁移。

## 8. 降采样查询

HTTP 使用 `resolution` 和 `feature` 两个参数，例如 `resolution=2h&feature=sum`。两者必须同时提供且各出现一次；空值、非法值、重复参数和旧参数 `query.field` 返回参数错误。分辨率使用与配置相同的时间间隔语法，并须为源 part 基础分辨率的整数倍；不要求它已经作为额外分辨率配置或落盘。特征固定为五种。两者均未提供时使用原始查询路径。

参数以 `DownsampleQuery{ResolutionMs, Feature}` 经 HTTP、`EvalConfig`、`SearchQuery` 和 RPC 进入 `Search.Init → tableSearch.Init → partitionSearch.Init → partSearch.Init`。指定参数只读取降采样 part，不将原始 part 临时转换；不指定参数只读取原始 part。

`partSearch` 根据每个 part 自身的配置和实际 metaindex，逐租户选择「能整除目标分辨率的最大已存分辨率」，同一 part 只选一种输入表示。例如请求 2h：已有 1h 就读 1h，只有 30m 就读 30m，只有基础 5m 就读 5m；请求 90m 时可选 45m 或 30m，不能选 1h。不同 part 可以选择不同输入分辨率，避免同时读取基础列与额外列而重复计数。

选定列的 index 继续使用 `ibCache` 和 `p.indexFile.MustReadAt`；原生 header 交给 `BlockRef.MustReadBlock`，通过 part 原有 `fs.ReaderAt` 读取时间列和单个请求特征。mmap、页状态判断、系统调用回退、读取统计、时间范围筛选和文件 I/O 的 `Must` 语义保持一致，查询不使用 `downsampleReader`，也不读其他四个特征。

`tableSearch` 复用原有 part／partition 堆按 TSID 收集全部选定输入，再按目标 bucket 聚合请求特征：sum/count 累加，last 按最大共享时间戳选择，min/max 取极值。即使输入跨 block、index、part 或月份分区，同一个目标 bucket 也只在当前 TSID 的全部贡献完成后输出。状态仅保存当前 TSID 的非空 bucket；输出按行数与精度拆分成独立、不可变的原生编码 Block，已复制的 BlockRef 不依赖后续游标缓冲。

输入时间范围先扩展到完整目标 bucket，避免查询边界截断贡献；聚合完成后，以结果的最大共享时间戳应用原请求范围及保留期限。HTTP 仍遵循 PromQL 的回看窗口和求值网格，不恢复 bucket 内的原始样本或精确裁剪部分贡献。源精度在同一输出 bucket 内必须一致。原有查询 deadline 与 pace 检查覆盖聚合入口、构建排序键及输出过滤循环，失败时停止当前查询；排序为一次同步操作，输入规模受查询 bucket 预算约束。预算按查询高水位持有至 `tableSearch.reset`，不在 TSID 切换时提前归还仍在复用的映射和切片容量。

降采样 RPC 使用 `search_downsampling_v2`，在原查询负载后追加 8 字节分辨率和 1 字节特征，响应仍为原生 MetricBlock。`execSearchQueryRequest` 在既有发送流程中选择 RPC，结果收集共用 `collectResults`。节点错误、部分响应、副本容错、慢副本、超时和通用错误沿用原始策略；参数与协议校验单独保留，不为旧节点新增回退。

降采样禁用未区分分辨率与特征的结果缓存及相应时间对齐，保留索引缓存。vmselect 与 vmstorage 的去重配置独立生效，需要保留全部降采样记录时两端均应设置 `dedup.minScrapeInterval=0`。跨 part／partition 聚合发生在单个 vmstorage 的查询内；多个 vmstorage 的网络响应继续遵守现有分布式查询机制。
