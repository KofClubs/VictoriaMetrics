# VictoriaMetrics 五特征降采样存储设计

## 1. 设计目标与范围

本设计以 `multi-value` 分支、提交 `83fc70c6a`（`docs/changelog: cut release v1.151.0`）为代码基线，在 `lib/storage` 的文件输出与读取边界实现五特征降采样。本文规定新增类型、文件格式和配置项；实现进度与验证结果见 [downsampling_storage_implement.md](downsampling_storage_implement.md)。代码依据中的行号对应上述基线提交，定位时应以文件及函数名为准。

启用降采样后，每次文件输出均同时保存 `5m` 和 `1h` 两种分辨率的 `last`、`sum`、`count`、`min`、`max`。每个分辨率的每行摘要共用一个时间戳。已有原始文件允许继续保留，并在参与 merge 时转换。

### 1.1 实现范围

| 范围 | 设计要求 |
|---|---|
| 文件输出 | 覆盖 inmemory dump、flush、small/big part merge、强制 merge，以及关闭和 snapshot 触发的 flush；转换发生在目标文件写入之前 |
| 存储内部读取 | 支持原始与降采样格式识别、顺序读取、归并所需的索引读取及混合源处理 |
| 生命周期 | 支持 part 原子发布、重启恢复、取消与失败清理、retention、MetricID 删除及启动配置校验 |
| inmemory | 保持 `rawRow`、`rawRowsMarshaler` 的内存输出、`inmemoryPart`、原始 `Block/blockHeader/partHeader` 布局、内存编解码和 merge 行为不变 |
| 序列标识 | 保持 TSID 类型、比较规则、metric identity 和 IndexDB 标签索引不变；扩展数据 part 自身的文件索引 |
| Block 逻辑复用 | 降采样单列处理完整复用 `storage.Block` 的现有逻辑；多列组织层负责五列关联、共享时间戳及 v3 索引，不另行实现一套单列 block 编解码和状态管理 |
| 对象复用 | 新增磁盘 reader、writer、merger 和 block 使用包级 `sync.Pool`，复用工作缓冲并限制容量 |

磁盘转换器使用内存工作缓冲，不改变 inmemory 存储格式。降采样与原始 dedup 互斥，所有实际进入转换器的输入均参与计算。

本设计包含用于单机对照测试的 `query.field` 参数和单特征读取，不包含完整生产查询聚合、分布式字段协议、自定义分辨率、分辨率集合变更、主动全量迁移，以及任意区间内部的原始样本裁剪或局部删除。查询边界见第 10 节，测试过程见 [downsampling_storage_testing.md](downsampling_storage_testing.md)。存储功能的正确性、恢复行为和资源消耗须通过第 11 节的验证。

### 1.2 数据术语

| 术语 | 含义 |
|---|---|
| 原始样本 | 一个时间戳与一个数值组成的输入 `(t, v)` |
| 降采样区间（bucket） | 按固定分辨率划分的左闭右开时间区间 |
| 摘要行 | 同一 TSID、同一分辨率、同一区间的一份五特征统计及共享时间戳 |
| 源贡献 | 源 part 中一条原始样本，或一条摘要所代表的全部输入贡献 |
| 部分摘要 | 仅包含某个源 part 或某次 merge 所处理输入的摘要；同一区间可以在不同 part 中分别存在 |
| Block | 原有 `storage.Block`，保存一个 TSID、一个分辨率、一个特征的一组单值样本 |
| 聚合批次 | 同一 TSID、分辨率及时间戳序列对应的五个特征 Block 的组织单位，不是新的物理 Block 类型 |
| v1 / v3 | 原始单值格式与单特征 Block 降采样格式的简称，不是 VictoriaMetrics 发布版本号；未发布的版本 2 不兼容 |

存储归并以物理 TSID 为序列键。不同 part 中相同时间戳的记录仍是各自的源贡献；实现须避免重复处理同一份源数据，但不识别或删除业务上的重复写入。

## 2. 数据模型与计算规则

### 2.1 分辨率与区间划分

固定分辨率集合为 `{300000, 3600000}` 毫秒，分别表示 `5m` 和 `1h`，两者同时启用。内部聚合函数使用分辨率参数 `R` 复用计算逻辑，配置接口不提供分辨率列表。

对当前解码得到的毫秒时间戳 `t`，采用以 Unix epoch 为原点的整数分桶：

```text
bucketID(t, R) = t / R
5m: bucketID = t / 300000
1h: bucketID = t / 3600000
区间 k = [k × R, (k + 1) × R)
```

时间戳沿用 [time.go](lib/storage/time.go) 的非负时间域 `[minUnixMilli, maxUnixMilli]`，起点为 `1970-01-02T00:00:00Z`；零日期保留，负时间戳不在支持范围内。在该时间域中，整数除法等价于向下取整。区间端点计算必须检查整数溢出。

位于区间右端点的样本进入下一区间；空区间不产生记录，也不补零。`5m` 和 `1h` 均与 UTC 日期及月 partition 边界对齐，单个区间不跨月。现有月分区边界计算见 [time.go](lib/storage/time.go) 95–101 行。

### 2.2 五特征与共享时间戳

对同一 TSID、同一分辨率、同一区间内的输入定义：

| 字段 | 类型 | 计算规则 |
|---|---|---|
| `lastTimestamp` | `int64` | 全部输入时间戳的最大值，单位为 Unix 毫秒 |
| `last` | `float64` | 选择最大时间戳对应的值；时间戳相同时，优先非 NaN 数值，再取较大数值；候选全部为标记时保留 `StaleNaN` |
| `sum` | `float64` | 原始输入累加数值；摘要输入累加已有 `sum` |
| `count` | `float64` | 每条原始输入累加 `1.0`；摘要输入累加已有 `count` |
| `min` | `float64` | 合并输入的最小值；标记处理见第 2.4 节 |
| `max` | `float64` | 合并输入的最大值；标记处理见第 2.4 节 |

同一摘要行的五个特征共同使用唯一的 `lastTimestamp`，持久化时只保存一份时间戳列。`min`、`max` 对应原始样本的时间戳不另行保存。两种分辨率的行数和共享时间戳可以不同，各自保存时间戳列。

`last` 始终先比较时间戳。较新时间戳仅有标记时，不能回退选择较早时间戳的数值。同时间戳的选值规则参考 [dedup.go](lib/storage/dedup.go) 中 `DeduplicateSamples()` 的数值比较和非 `StaleNaN` 优先逻辑（48–68、74–88 行）；该规则只决定 `last`，不删除其他特征的输入。

例如同一区间输入 `(10:01,100)`、`(10:04,3)`、`(10:04,8)`，编码前得到：

```text
lastTimestamp=10:04, last=8, sum=111, count=3, min=3, max=100
```

即使时间戳和值完全相同，多次写入仍分别参与计算。`sum` 和 `count` 均不具备幂等性；重复统计同一份源贡献会改变结果。

### 2.3 原始样本提升与摘要归并

记摘要为 `A = (T, L, S, C, N, X)`，分别对应共享时间戳、`last/sum/count/min/max`。普通数值样本提升为：

```text
(t, v) → (t, v, v, 1.0, v, v)
```

原始 `StaleNaN` 样本提升为：

```text
(t, StaleNaN) → (t, StaleNaN, StaleNaN, 1.0, StaleNaN, StaleNaN)
```

两个源摘要按 TSID 和目标区间对齐后，按以下规则归并：

```text
T = max(A.T, B.T)
L = A.L                  当 A.T > B.T
    B.L                  当 B.T > A.T
    同时间戳选值(A.L,B.L) 当 A.T == B.T
S = A.S + B.S
C = A.C + B.C
N = min(A.N, B.N)
X = max(A.X, B.X)
```

各列独立执行第 2.4 节的标记规则。`last` 选择某个源，不影响另一源的其他特征贡献；两个摘要的共享时间戳相同，也必须合并两份贡献。`count` 不对摘要行重新计数，不转换为整数，不强制取整；`sum` 不以 `last` 代替。

生成每个目标分辨率时，每个源 part 只选择一份覆盖其贡献的数据表示：

- 原始 part：逐样本计算目标分辨率的摘要。
- 降采样 part：读取目标分辨率自身的表示。生成 `1h` 时读取该 part 的 `1h`，不得同时累加其 `5m`。
- 细分辨率转粗分辨率：仅在源区间完整对齐、所需贡献完整可用且语义兼容时成立。`5m → 1h` 可以满足这些条件，`1h → 5m` 无法恢复所需细节。

缺失区间只有在能够确认源表示完整时才能解释为无样本。不得从另一分辨率补入同一份贡献，也不得使用经 retention 删除后残缺的 `5m` 重建完整 `1h`。本设计的 v3 part 同时声明两种分辨率，归并优先使用目标分辨率自身的表示；其中一种分辨率没有行时仍按其实际内容处理。

例如 retention deadline 为 10:06 时，`[10:00,10:05)` 的 `5m` 行可能已过期，而 `[10:00,11:00)` 的 `1h` 行仍有效。保留的 `1h` 必须继续从自身表示读取，不能由剩余 `5m` 替代。

### 2.4 NaN 与 StaleNaN

降采样内部遇到或计算产生的任何 NaN 均规范化为 `decimal.StaleNaN`，不区分来源或位模式。在解码结果进入聚合前、特征运算完成后，以及交给 decimal 编码前执行统一处理：

```go
if math.IsNaN(v) {
    v = decimal.StaleNaN
}
```

| 对象 | 处理规则 |
|---|---|
| 共享时间戳 | 所有输入均参与最大值计算，包括标记 |
| `last` | 时间戳优先；同时间戳时优先数值，再取较大值；仅有标记时保留标记 |
| `count` | 原始标记仍贡献 `1.0`；摘要按本列执行浮点加法并规范化结果 |
| `sum/min/max` | 原始标记在提升时使这三列均为标记；摘要归并时，任一输入列为标记，则对应输出列继续为标记 |

标记按列传播。某列产生 NaN 不改变其他列，不跳过整条输入，也不从 `last` 或 `sum` 的状态推断整行状态。`sum/min/max` 在执行普通运算前显式判断本列的标记，避免依赖浮点 NaN 比较结果；运算新产生的 NaN 使用同一规范化规则。`count` 同样不设置来源例外。

例如原始输入 `(10:01,2)`、`(10:02,3)`、`(10:04,StaleNaN)`，共享时间戳为 10:04，`last/sum/min/max` 为 `StaleNaN`，`count=3`。同时间戳同时存在数值和标记时，`last` 选择数值，其他列仍按各自规则计算。全标记区间采用相同规则。

算术运算产生 NaN 的示例为 `(10:01,+Inf)`、`(10:02,-Inf)`：

```text
lastTimestamp=10:02, last=-Inf, sum=StaleNaN, count=2, min=-Inf, max=+Inf
```

此例仅 `sum` 规范化为标记。reader、writer、merger 使用相同的逐列规则，不增加标记来源字段、辅助特征或专用数值编码。

现有 [decimal.go](lib/decimal/decimal.go) 376–428 行支持 `+Inf`、`-Inf`、`StaleNaN`，但 `FromFloat()` 不支持普通 NaN；[storage.go](lib/storage/storage.go) 1928–1933 行在摄取时过滤普通 NaN、保留 `StaleNaN`。原始摄取与 inmemory 行为保持不变；上述规则适用于实际进入降采样链路的输入及其运算结果。

### 2.5 数值表示与编解码

五个特征均使用与原始 values 相同的 `float64` 精度，`count` 不例外。精度由浮点运算、decimal 转换及现有编解码算法共同决定。

| 环节 | 复用方式与代码依据 |
|---|---|
| 原始输入 | [raw_row.go](lib/storage/raw_row.go) 29–44 行使用 `int64` 时间戳和 `float64` 数值；137–149 行执行 decimal 转换 |
| 特征值编码 | decimal 转换后由原有 `Block` 承担单值编码及状态转换；`MarshalData` 与降采样专用入口共用同一实现 |
| 特征值解码 | 由 `Block.UnmarshalData` 共用逻辑恢复单列，再按原有 decimal 规则得到浮点值 |
| 时间戳编解码 | 由 `Block` 共用逻辑执行编码、解码、有序性修复和范围校验；降采样入口允许时间戳 precision 独立于数值 precision |
| 精度传递 | 合并多个输入时，沿用参与贡献的源 `PrecisionBits` 取最小值的规则，参考 [merge.go](lib/storage/merge.go) 125 行 |

每列独立保存 `FirstValue`、`Scale`、`PrecisionBits`、`ValuesMarshalType` 及文件位置。归并前先恢复浮点数值，不直接对不同 scale 的整数尾数相加或比较。时间戳沿用原有精度传递，不单独强制为 64，不修改全局 `-precisionBits` 配置及校验。

分桶、共享时间戳选择和后续归并均以当前解码结果为输入，不额外保存编码前的时间戳或 bucket 标识。浮点舍入、decimal 转换、低精度编码、运算顺序及编码次数造成的差异均沿用现有行为；不增加精确 `sum`、整数 `count`、误差补偿或特殊值精度保护。

不要求不同 merge 树的结果逐位相同，也不要求编码前后 bucket 不变、NaN 位模式绝对保留或任意规模下的 `count` 精确计数。解码后出现重复 bucket 不能单独作为损坏判据。文件长度、偏移、行数、编解码类型及缓冲长度等结构性校验仍须完整执行。

## 3. 存储架构与归并流程

### 3.1 现有链路与接入边界

```text
Storage.add
  → rawRow / rawRowsShards
  → inmemoryPart.InitFromRows
  → rawRowsMarshaler.marshalToInmemoryPart
  → blockStreamWriter.MustInitFromInmemoryPart
  → 原始 inmemory part

原始 inmemory part
  → 内存 merge → 原始 inmemory part
  → partition.mergeParts 选择文件目标
      ├─ 单 part 直接 dump：inmemoryPart.MustStoreToDisk
      └─ 普通文件输出：blockStreamWriter.MustInitFromFilePart
          → mergeBlockStreams → WriteExternalBlock
```

| 代码位置 | 现有行为 | 设计接入方式 |
|---|---|---|
| [raw_row.go](lib/storage/raw_row.go)，`marshalToInmemoryPart`，99–153 行 | inmemory 创建也使用 `WriteExternalBlock` | 保留通用原始 writer 的行为，在文件输出边界分派 |
| [inmemory_part.go](lib/storage/inmemory_part.go)，15–23、38–56 行 | 内存缓冲布局固定，直接 dump 原样复制到文件 | 保持结构及 `MustStoreToDisk()` 不变，启用降采样时由调用方使用转换器 |
| [partition.go](lib/storage/partition.go)，`mergeParts`，1244–1287 行 | 先选择目标类型，并包含单 part dump 路径 | 统一分派原始内存输出和降采样文件输出 |
| [partition.go](lib/storage/partition.go)，`getDstPartType`，1353–1366 行 | 源包含文件 part 时，目标为文件 part | 不需要将摘要还原为原始 inmemory part |
| [merge.go](lib/storage/merge.go)，99–148 行 | TSID 切换、大 block 和拆分均可触发提前写出 | 新归并器必须处理跨 block 的完整区间 |
| [block_stream_merger.go](lib/storage/block_stream_merger.go)，`NextBlock`、`blockStreamReaderHeap.Less` | 按 block 排序，不提供五特征逐行归并 | 新增源游标和区间归并器 |
| [block.go](lib/storage/block.go)，21–39、114–137 行 | 原始 `Block` 的 timestamp/value 长度必须相同 | 多列组织层关联五列；单列处理复用 `storage.Block`，保持其长度和状态约束 |

### 3.2 文件输出分派

完成第 7 节的初始化校验后，`partition.mergeParts()` 按目标类型与启用状态分派：

```text
目标为 partInmemory
  → 原始 merge / writer

目标为 partSmall 或 partBig，启用降采样
  → downsampleMerger
  → downsampleWriter
  → v3 文件 part

目标为 partSmall 或 partBig，未启用降采样
  → 原始文件 merge / writer
```

启用降采样时，[partition.go](lib/storage/partition.go) 1249–1255 行的单 inmemory part 路径必须调用转换器，禁止原样 dump。未启用时保留原有路径。

定时 flush、空间压力触发的输出、正常关闭、snapshot flush、small/big merge、强制 merge 和历史原始 part 转换均进入此分派。转换器直接读取内存缓冲或已有文件，目标文件从首次写入起只包含摘要负载，不创建原始数据的临时目标 part。

### 3.3 跨 block、跨 part 的有界归并

原始 reader 保证 block 按 TSID 和最小时间戳排序，但同一 part 的 block 可以重叠。顺序连接 block 不保证样本全局有序，仅保留前一 block 的末尾区间也不足以处理任意重叠。

归并以受限区间窗口为基础。在固定目标分辨率下，合并键为 `(TSID, bucketID)`；每次作业只处理选定源集合，不等待未来写入或区间结束。

1. 固定源 part 集合、配置和 retention deadline，持有源引用。按第 2.3 节为每个目标分辨率选择源表示。
2. 使用各源 index 的有序游标和堆枚举 TSID，不一次性载入全部序列或原始样本。
3. 对当前 TSID，按升序处理互不重叠的窗口；每个窗口最多包含 `B` 个完整 bucket，`B` 由工作内存预算确定。
4. 通过各源格式的 index 找到所有与窗口相交的 block，逐个解码。仅将 bucketID 属于当前窗口的贡献加入状态；原始样本先提升，摘要按特征规则归并。
5. 扫描完全部源后，按 bucketID 输出非空状态。每个状态包含共享最大时间戳和五个特征，窗口内最多保留 `B` 个状态。
6. 跨窗口 block 允许重复解码，但每条贡献仅在其所属窗口计入一次。根据下一条候选样本或 block 的起始 bucket 跳过空窗口，不逐单位扫描长期空白区间。
7. 输出缓冲达到行数或字节上限时形成 block。编码前，同一目标 part、同一 TSID、同一分辨率、同一 bucket 只输出一行完整摘要，不能因缓冲切换拆成多个部分行。不同 part 可以保留部分摘要。

该流程的工作集包括受限聚合状态、有限 index 缓冲、单个 block 解码缓冲和输出缓冲，不保存逐区间原始样本列表。编码后时间戳的再次读取遵循第 2.5 节。

按行或 bucket 的堆式归并可以作为优化，但只有尚未读取 block 的起始 bucket 已越过当前 bucket，且所有活动游标均已消费当前 bucket，才可完成该行。活动 block 和状态必须有容量上限；超限时关闭并删除未发布输出，再从头执行窗口归并，不得向原输出追加重试结果。

原始源适配器直接将解码结果交给聚合器，不调用 dedup；接收已部分消费的原始 `Block` 时，从 `nextIdx` 开始读取，不重复处理已消费的前缀。原始 merge 的整数 scale 校准及逐样本 retention 裁剪不用于摘要状态；文件转换按目标分辨率独立判断区间过期。

## 4. 磁盘格式与文件索引

### 4.1 格式边界

磁盘格式使用版本 3。每个分辨率、每个特征保存独立 `storage.Block`，以扩展索引条目包装原有 `blockHeader`。原始 Block 与 blockHeader 的内存布局、原始序列化和 TSID 格式保持不变。版本 2 的未发布五列格式明确拒绝，不将其解释为版本 3 或原始格式。

| 文件 | v3 内容 |
|---|---|
| `timestamps.bin` | 每个聚合批次的一份时间戳负载，供该批次五个 Block 共同引用 |
| `values.bin` | 每个 Block 的单列 values 负载，按 last、sum、count、min、max 顺序写出 |
| `index.bin` | 每个 Block 独立的扩展索引条目，包含分辨率、feature ID、时间戳 precision 与原生 blockHeader |
| `metaindex.bin` | 按分辨率组织的 index block 索引及格式标识 |
| `metadata.json` | part 格式、语义、分辨率及物理统计 |
| `parts.json` | 原有 partition 活动 part 清单，继续承担原子发布职责 |

### 4.2 单特征 Block 与共享时间戳

一个实际 Block 只保存一个特征。reader 根据索引的分辨率与 feature ID 选择所需 Block；归并时将同一批次五个 Block 关联，恢复五个浮点特征以计算新摘要。

```text
聚合批次 0：Block(5m,last) Block(5m,sum) Block(5m,count) Block(5m,min) Block(5m,max)
聚合批次 1：Block(5m,last) Block(5m,sum) Block(5m,count) Block(5m,min) Block(5m,max)
...
聚合批次 n：Block(1h,last) Block(1h,sum) Block(1h,count) Block(1h,min) Block(1h,max)
```

每个 Block 内 values 连续编码。五个 Block 的 `RowsCount`、TSID、时间范围、时间戳 offset/size、时间戳 marshal type 和时间戳 precision 必须一致；数值 `FirstValue`、`Scale`、`PrecisionBits`、offset/size 和 marshal type 各自独立。时间戳负载只写入 `timestamps.bin` 一次，不分别为五个特征重复保存。

单列编解码及状态转换使用 `storage.Block` 的共用实现，包括已编码状态、`nextIdx`、长度验证、时间戳修复和 Reset。聚合批次只组织特征关联及工作缓冲，不承担另一套单列编解码。原始 `MarshalData/UnmarshalData` 沿用原有精度参数，降采样入口额外传递独立的时间戳 precision。

### 4.3 排序与索引

物理排序键为 `(ResolutionMs, TSID.Less, MinTimestamp, Feature)`。同一批次五个 feature ID 按 `1=last`、`2=sum`、`3=count`、`4=min`、`5=max` 排列；先写 5m，再写 1h。

一个 index block 只包含一种分辨率，并包含完整的五 Block 批次；切换分辨率或索引容量不足时 flush。metaindex 记录分辨率、首末 TSID、时间范围、实际 Block header 数量、物理行数及 index block 的 offset/size。reader 可以先二分定位分辨率与 TSID，再筛选目标特征。

相邻 values 负载满足 `next.Offset = current.Offset + current.Size`，常量编码的零负载允许 offset 不变。时间戳 offset 只在切换批次时推进；同一批次五个 header 共同引用相同位置。

### 4.4 结构校验与统计

reader 和 writer 校验版本、分辨率、feature ID 与顺序、五个 Block 的共享字段、原生 header、编码类型、解码行数、文件边界、offset/size、时间域及整数溢出。index、metaindex 与 part 的统计必须一致，解压大小有明确上限。连续扫描已读取的相邻 index 时，校验跨 index 的数据偏移衔接；选择性读取不补读被过滤的 index，不能等同于全文件完整性扫描。

现有常量列编码可使用 `size=0`，依靠 header 中的首值、scale 和行数恢复单列，不表示特征缺失。低精度解码仍沿用既有行为，不额外要求编码前后 bucket 不变。

`BlocksCount` 统计实际单特征 Block 数量；`RowsCount` 统计这些 Block 的物理行数。两者均为五 Block 聚合批次口径的五倍。`count` 是独立浮点特征，不是文件行数。归并统计对原始源仅计一次输入行，对摘要源按五个特征的物理行数计数；空间估算须在物理行与逻辑摘要行之间正确换算。

### 4.5 v3 字段布局

整数序列化复用 `lib/encoding` 的定长函数。无符号整数按大端保存；带符号整数先按现有 ZigZag 规则转换，再按大端保存。一个独立 Block 的扩展索引条目为 91 字节：

| offset | 长度 | 字段 |
|---|---|---|
| 0 | 8 | `ResolutionMs`，int64，300000 或 3600000 |
| 8 | 1 | `Feature`，1 至 5 |
| 9 | 1 | `TimestampPrecisionBits`，1 至 64 |
| 10 | 81 | 原有 `blockHeader.Marshal` 的完整输出 |

原生 blockHeader 的 81 字节布局保持原样，下表 offset 相对于原生 header 起点：

| offset | 长度 | 字段 |
|---|---|---|
| 0 | 24 | TSID |
| 24 | 8 | MinTimestamp |
| 32 | 8 | MaxTimestamp |
| 40 | 8 | FirstValue |
| 48 | 8 | TimestampsBlockOffset |
| 56 | 8 | ValuesBlockOffset |
| 64 | 4 | TimestampsBlockSize |
| 68 | 4 | ValuesBlockSize |
| 72 | 4 | RowsCount |
| 76 | 2 | Scale |
| 78 | 1 | TimestampsMarshalType |
| 79 | 1 | ValuesMarshalType |
| 80 | 1 | PrecisionBits |

五个独立条目连续占 455 字节，作为一次聚合批次的索引组织单位。`blockHeader` 中的 `RowsCount` 是该单值 Block 的行数，part 与 metaindex 累计五份物理行。

metaindex 行为 96 字节：

| offset | 长度 | 字段 |
|---|---|---|
| 0 | 8 | ResolutionMs |
| 8 | 24 | 首 TSID |
| 32 | 24 | 末 TSID |
| 56 | 8 | MinTimestamp |
| 64 | 8 | MaxTimestamp |
| 72 | 4 | BlockHeadersCount，实际单特征条目数，必须为 5 的倍数 |
| 76 | 8 | IndexBlockOffset |
| 84 | 4 | IndexBlockSize |
| 88 | 8 | RowsCount，实际单值 Block 物理行数 |

index 压缩帧前缀为 ASCII `VMDSIX` 加十六进制 `00 03`，metaindex 前缀为 ASCII `VMDSMI` 加 `00 03`，其后为 ZSTD 负载。

metadata 必需字段为：`FormatVersion=3`、`SemanticsVersion=1`、`Mode=downsampling`、`Resolutions=[300000,3600000]`、`BucketOrigin=0`、`NumericCodec=decimal-values-v1`、`Retention=bucket-end`、`MinDedupInterval=0`，以及物理 `RowsCount/BlocksCount` 和 `MinTimestamp/MaxTimestamp`。必需字段不得缺失或为 null。

## 5. 读取与格式兼容

### 5.1 读取分派与原始格式退化

| 输入 | 存储读取行为 |
|---|---|
| 合法 v1 原始 part | 使用原始 reader 读取 `(timestamp, value)`；参与降采样 merge 时逐样本提升为五特征 |
| 合法 v3 part | 使用 v3 reader 读取共享时间戳和完整五列，按目标分辨率选择源表示 |
| 声明 v3 但缺列、截断或损坏 | 报告格式错误，不退化为 v1 |
| 未知版本、未知编码或元数据冲突 | 明确失败，错误包含 part 路径及原因 |

“没有五特征时退化”仅适用于合法原始格式。不能把整个原始 block 视为 `count=1` 的一行，也不能把全部特征填为原始 value。读取失败不能作为原始格式识别方式。

合法历史原始 part 可能没有 `metadata.json`；保留 [part_header.go](lib/storage/part_header.go) 138–142 行的 `ParseFromPath()` 兼容路径，以及历史 `parts.json` 缺失时的合法目录识别规则。v1 缺少新增分辨率字段不构成错误。

存储 reader 同时提供顺序遍历和第 3.3 节所需的窗口索引读取。查询链路的 `BlockRef` 和原单列协议不由此自动获得五特征支持，适配范围见第 10 节。

### 5.2 格式识别与旧程序行为

v3 使用明确的二进制 magic/version，并在 `metaindex.bin` 的 ZSTD 负载之前设置与原格式不兼容的前缀，使旧程序在打开 part 时明确失败。仅增加 JSON 字段不足以拒绝旧程序。

依据 [metaindex_row.go](lib/storage/metaindex_row.go) 的 `unmarshalMetaindexRows()`（129–138 行），旧入口直接执行 ZSTD 解压；新入口必须先识别并处理版本前缀，再调用相应解码函数。格式检测与第 7 节的启动预检查共用实现，正式 reader 继续执行完整结构校验。

新程序兼容 v1/v3 共存；旧程序不能读取 v3。关闭降采样开关不恢复原始样本，也不把五列投影为原始单列。snapshot 或备份恢复后，新程序按实际文件格式读取，继续执行相同的启动校验和归并规则。

## 6. part 生命周期与保留策略

### 6.1 写入与原子发布

一个 v3 part 同时声明固定的 `5m/1h`。某一分辨率因无输入或 retention 没有摘要行属于合法情况；两个分辨率均为空时，按空输出处理，不发布空目标。

五列、两种分辨率、索引和元数据必须全部写完、校验并同步后，才能发布目标 part。两种分辨率作为同一 part 一并发布，读者只能看到原活动集合或完整的新集合。

复用 [partition.go](lib/storage/partition.go) 的 `openCreatedPart()`（1444–1467 行）、`swapSrcWithDstParts()`（1479–1529 行）及 `mustWritePartNames()`（1884–1896 行），扩展新格式打开与校验后完成活动清单替换。旧源按现有引用生命周期释放，避免读者仍持有源时提前删除。

写出失败、取消或优化路径重试时，关闭文件并删除未发布目标，保留有效源 part。不得原地覆盖源文件，不得将未发布目标加入恢复后的活动集合，也不得回退写入原始目标 part。

### 6.2 原始文件与摘要文件共存

启用降采样不要求预先转换全部原始文件。旧 part 按原格式读取，在参与普通或强制 merge 时转换；未参与 merge 的原始 part 可以保留至后续转换或过期清理。所有新增文件输出均采用 v3。

本设计不设置全量转换期限，不要求活动 v1 数量归零，不增加主动迁移任务或独立迁移进度清单。现有 [table.go](lib/storage/table.go) 的 `historicalMergeWatcher()`（492–496 行）在 dedup 关闭时直接返回，不能用作降采样迁移机制。

转换只统计源文件实际保留的样本，不恢复此前被 dedup、retention 或其他已有逻辑删除的数据。历史 snapshot 和备份不随活动 part 转换而改写。

### 6.3 retention

文件中的摘要按完整区间判断过期。对共享时间戳 `t`、分辨率 `R` 和本次作业固定的 `retentionDeadline`：

```text
bucketEnd = (t / R + 1) × R
bucketEnd <= retentionDeadline → 整行过期
bucketEnd > retentionDeadline  → 保留整行
```

两种分辨率独立判断，不逐点裁剪摘要，不要求在区间到期时立即删除文件。实际空间回收继续服从原有调度。

例如 `[10:00,10:05)` 的 `5m` 摘要共享时间戳为 10:01，deadline 为 10:02 时保留，达到 10:05 时过期。同一区间的不同部分摘要，即使共享时间戳不同，也使用同一区间右端点判断。

文件转换和清理须满足以下条件：

- 原始文件输入按各目标分辨率计算保留范围，不先按原始时间戳删除仍能贡献有效区间的样本；生成 `1h` 前不因对应 `5m` 已过期而删除其输入。
- 降采样源读取目标分辨率自身的摘要，不从经 retention 删除后的残缺细摘要重建粗摘要。
- 只有能够证明所有相关分辨率的剩余区间均已过期，才允许跳过整个 block 或删除整个文件 part；不能直接复用原始 `MaxTimestamp < deadline` 捷径。
- 降采样模式下，尚未转换的原始文件也按目标区间采用保守清理条件，避免删除粗分辨率仍需的贡献。
- 原始 inmemory retention 行为不变，不恢复在进入转换器前已被删除的输入。

接入位置包括 [merge.go](lib/storage/merge.go) 79–89、205–219 行的原始过期过滤，[partition.go](lib/storage/partition.go) 1427–1431 行的 deadline 传递，以及 1583–1612 行的过期 part 清理。新文件链路独立处理区间过期，原始 inmemory 分支保持原样。

固定分辨率与 UTC 月分区边界对齐，沿用 [table.go](lib/storage/table.go) 446–484 行的整月 partition 保留判断和调度，不增加跨月区间算法。

### 6.4 序列删除

沿用现有 deleted MetricID 集合，在转换与摘要归并中跳过已删除的序列，参考 [merge.go](lib/storage/merge.go) 79–89 行。删除作用于整条序列；摘要内部不提供任意原始样本或局部时间段删除能力。

## 7. 配置与启动校验

### 7.1 启用配置与互斥规则

在 `storage.OpenOptions` 增加降采样启用状态，由存储端命令行入口传入。启用参数拟定为 `-storage.downsampling.enabled`，实现时校验名称冲突；固定分辨率不增加列表参数，也不增加目录级分辨率配置记录。

配置入口为 [app/vmstorage/main.go](app/vmstorage/main.go)；现有 dedup 参数及 storage 初始化见 123、154–166 行。单机入口 [app/victoria-metrics/main.go](app/victoria-metrics/main.go) 97 行调用 `vmstorage.Init()`。

| 配置与活动文件 | 初始化行为 |
|---|---|
| 启用降采样，`dedup.minScrapeInterval=0`，活动文件为合法 v1/v3 | 允许打开，所有输入直接参与降采样 |
| 启用降采样，dedup interval 非零 | 报告配置冲突并终止初始化 |
| 未启用降采样，目录为空或仅有 v1 | 保留原始存储和 dedup 行为 |
| 未启用降采样，存在活动 v3，包括 v1/v3 混合目录 | 报告配置冲突并终止初始化，提示启用降采样 |

命令行入口和 `storage.MustOpenStorage()` 均落实校验，覆盖直接调用 storage API 的路径，不静默修改任何一个配置。dedup interval 当前为进程全局状态，见 [dedup.go](lib/storage/dedup.go) 9–26 行；`MustOpenStorage()` 检查 `GetDedupInterval()`。

interval 为 0 时，原始 `deduplicateSamplesDuringMerge()` 直接返回，见 [block.go](lib/storage/block.go) 152–169 行。新增降采样 reader、writer、merger 不调用 dedup，无需改写原始 inmemory 去重算法。

### 7.2 活动 part 的只读格式预检查

预检查必须覆盖全部活动 part，并在任何 partition 后台数据任务启动前完成。顺序为：

```text
取得 storage 目录 flock
  → 检查恢复状态
  → 校验启用配置与 dedup
  → 只读枚举全部活动 part 并检查格式
  → 打开 table / partition
  → 启动后台数据任务
```

接入点位于 [storage.go](lib/storage/storage.go) 的 `MustOpenStorage()`：227 行取得目录锁，229–233 行检查恢复状态；在此之后、276 行旧 IndexDB 初始化及 291 行 `mustOpenTable()` 之前执行预检查，并保持目录锁覆盖初始化过程。

不能等逐个 part 打开时才判断启用状态。[table.go](lib/storage/table.go) 657–686 行并行打开 partition，而 [partition.go](lib/storage/partition.go) 291 行已启动包含 merge、flush 和过期清理的后台任务（225–234 行）。全部活动文件预检查先于这些入口，才能避免发现冲突前已有 partition 开始修改数据。

预检查遵循以下规则：

1. 只读枚举 `data/small`、`data/big` 和 IndexDB 目录提供的 partition 名称集合，不读取或修改 IndexDB 索引内容。
2. 有 `parts.json` 时，以其 Small/Big 活动清单为准；缺少清单时保留合法历史目录的识别规则，参考 [partition.go](lib/storage/partition.go) 1913–1929 行。
3. 排除 snapshot、临时目录、事务目录、未发布输出及非活动孤立文件；沿用现有特殊目录跳过规则。未知格式、损坏或相互冲突的活动元数据明确报错。
4. 与正式 reader 共用格式检测，只检查元数据和格式标识，不解码样本；错误应包含具体 part 路径。
5. 不调用带修改副作用的初始化或枚举函数。[table.go](lib/storage/table.go) 的 `mustPopulatePartitionNames()` 在 704–708 行清理未完整删除的目录，预检查须提取同等识别规则的只读实现。

预检查失败时，不启动数据后台任务，不转换或删除源 part，不改写 `parts.json`。仅在 snapshot 中存在 v3、活动集合为空或全为 v1 时，不构成“关闭开关且存在活动 v3”的冲突。

## 8. 对象复用与资源管理

### 8.1 对象池与缓冲所有权

为 `downsampleWriter`、`downsampleReader`、`downsampleMerger`、`downsampleBatch` 分别设置包级 `sync.Pool`，采用 `get → 使用 → reset → put` 生命周期。参考 [block.go](lib/storage/block.go) 65–78 行、[block_stream_writer.go](lib/storage/block_stream_writer.go) 210–223 行、[merge.go](lib/storage/merge.go) 19–38 行和 [raw_row.go](lib/storage/raw_row.go) 156–169 行。

五个特征使用固定数组组织 `float64` 状态或列切片；固定分辨率使用包内数组。decimal 所需 `int64` 缓冲、编码字节缓冲、索引缓冲和归并状态均可复用，不在每个 writer 内另建对象池，不逐样本或逐区间分配对象。

`reset` 清空长度、错误、上下文及 part/reader 引用，保留正常容量；归还前关闭文件。writer 完成编码前，reader 不得复用已传入切片；缓存不得持有将归还对象池的可变切片。

对活动 reader 堆、block、列值、编码和解压缓冲分别限制容量。归还异常大对象时释放过大的底层数组，避免池长期保留峰值内存。不得采用逐区间 `map[int64][]sample`。聚合批次仅保留一组共享时间戳；writer 的五个原生 Block 各自持有由 `Block.Init` 复制的工作切片，并通过同一 Block 编码流程获得一致的时间戳负载，文件仅写入其中一份。该工作内存开销受 block 行数上限约束。

`sync.Pool` 可被 GC 清空，正确性不依赖对象身份或必然命中。现有 [inmemory_part.go](lib/storage/inmemory_part.go) 85–107 行使用有界 channel 管理大对象，保持该实现不变。

### 8.2 内存与磁盘预算

merge 工作内存由窗口状态数、活动索引和源游标数、单 block 解码上限及输出缓冲共同限制。窗口大小与并发作业数纳入预算，不能以对象池代替容量控制。

降采样的逻辑值数量约为 `5 × (N5m + N1h)`，另加各分辨率的一份时间戳与索引。低频序列可能在两个分辨率各产生一行，实际文件体积可能增大；五列与两种分辨率仍须完整保存。

[partition.go](lib/storage/partition.go) 的 `getDstPartType()`（1353–1366 行）及 `ForceMergeAllParts()` 空间检查（1089–1096 行）以源字节数估算目标。v3 必须改用源行数、目标区间数上界、各列最坏编码长度、索引与元数据开销，以及新旧 part 同时存在的空间成本，适配 small/big 选择和并发 merge 预算。

运行期间检查剩余空间；不足时中止并清理未发布输出，保留源。压缩级别选择、merge 调度和现有 rows merged 等统计须使用明确的物理行数及字节数语义，避免将两种分辨率当作两份原始输入。

## 9. 代码修改位置

下表按职责列出实现位置。新增文件名表示职责划分；原始内存结构和算法保持第 1 节规定的边界。

| 文件或模块 | 修改内容 |
|---|---|
| [lib/storage/partition.go](lib/storage/partition.go) | 在 `mergeParts` 按目标类型分派，拦截直接 dump；接入新文件读写、原子发布、区间过期清理、空间估算及统计；`mergePartsInternal` 保持原始归并行为 |
| [lib/storage/block.go](lib/storage/block.go) | 共用单列编解码及状态转换实现，允许降采样入口使用独立时间戳 precision；原始接口与行为不变 |
| [lib/storage/part.go](lib/storage/part.go) | 文件格式识别与 reader/metaindex 分派，v3 文件资源及缓存生命周期；原始 inmemory 初始化保持原路径 |
| [lib/storage/part_header.go](lib/storage/part_header.go) | 保持文件及原始 `partHeader` 布局不变；v3 在独立元数据结构中复用原有统计字段 |
| [lib/storage/storage.go](lib/storage/storage.go) | 扩展 `OpenOptions`；在 `MustOpenStorage()` 校验 dedup 互斥，执行后台任务启动前的活动文件格式预检查 |
| [lib/storage/table.go](lib/storage/table.go) | 保持文件不变；新增预检查参考其目录识别规则，独立执行只读枚举，不调用原初始化清理逻辑 |
| [app/vmstorage/main.go](app/vmstorage/main.go) | 定义降采样启用选项、校验互斥并传入 storage；保持原有精度配置 |
| 新增 `lib/storage/downsample.go` | 固定分辨率和 feature 定义、bucket 运算、原始样本提升、五特征合并、统一 NaN 规则 |
| 新增 `lib/storage/downsample_batch.go` | 浮点摘要工作批次、共享时间戳与五列切片、容量限制及对象池 |
| 新增 `lib/storage/downsample_codec.go` | 单特征 v3 扩展索引与原生 blockHeader 复用，长度、offset/size 和整数溢出校验 |
| 新增 `lib/storage/downsample_part.go` | v3 part 元数据、metaindex、完整排序键校验，以及预检查和 reader 共用的格式检测 |
| 新增 `lib/storage/downsample_reader.go` | 原始/摘要源适配、目标分辨率选源、顺序与窗口索引读取、五列解码及资源复用 |
| 新增 `lib/storage/downsample_writer.go` | 磁盘多列写入、共享时间戳编码、索引分辨率切换、统计、同步关闭和资源回收 |
| 新增 `lib/storage/downsample_merger.go` | 有界窗口归并、跨 block/part 聚合、混合源处理、取消、retention 和 MetricID 删除 |
| 新增 `lib/storage/downsample_partition.go` | 文件目标归并、发布前资源释放、失败清理、文件过期判断、候选空间调度及源集合分批 |
| 新增 `lib/storage/downsample_open.go` | 启动前的 dedup 互斥校验、活动清单和目录只读枚举、活动文件格式预检查 |
| 新增 `lib/storage/downsample_space.go` | 输出空间上界、并发预算、空闲空间缓存保护、逐 block 和最终索引写出前的空间检查 |
| 相邻 `*_test.go`、`*_timing_test.go` | 数学规则、文件格式、归并、兼容、初始化、恢复、资源及性能验证 |

`block_stream_writer.go`、`block_stream_reader.go`、`merge.go` 提供原始适配和资源管理依据；不在其通用内存路径加入隐式降采样开关。TSID/IndexDB 序列标识、原始摄取、原始 block 编解码及 inmemory 池不作五特征改写。

## 10. 查询接口边界

存储层收到的 `TimeRange` 与降采样区间相交，且覆盖该摘要保存的共享时间戳时，该摘要可参与返回；否则忽略。存储过滤沿用闭区间判断，分桶区间仍为左闭右开，不按边界重新裁剪摘要所含的原始贡献。HTTP `query` 与 `query_range` 仍执行标准 PromQL 的 lookback、窗口扩展及时间网格求值，可能使用 HTTP 起始时间之前的摘要，返回时间为求值时间。裸区间 selector 用于核对实际保存的共享时间戳与数值。

例如区间为 `[10:00,10:05)`，共享时间戳为 10:04。存储 `TimeRange=[10:03,10:05]` 包含摘要；`TimeRange=[10:01,10:02]` 与区间相交但未覆盖共享时间戳，不包含摘要。现有闭区间过滤依据为 [block.go](lib/storage/block.go) 的 `filterTimestamps()`（331–349 行）。

单机对照测试通过 `query.field` 选择一个分辨率与一个特征，例如 `5m:last` 或 `1h:count`。参数经 HTTP 查询、EvalConfig、SearchQuery 和存储 search 逐层传递；非法值报错，字段查询关闭结果缓存及缓存时间对齐，防止不同特征之间互相污染。裸区间 selector 的导出快径也必须携带字段选择。

指定字段时仅读取匹配的降采样磁盘 Block，不将 raw inmemory 或历史 raw 值伪装为 sum/count。未指定字段时保留原始查询路径，不将降采样特征混入原始结果。因此对照测试须先确认 flush/merge 完成。选出的单值引用继续复用 `BlockRef/Block` 读取；解码提示兼顾独立时间戳 precision，不重编码数值负载。该查询路径保持原生 Block 的解压行为；归并 reader 的有界 ZSTD 预处理不扩展到原始查询实现。

该参数不扩展现有分布式网络协议，不承担跨未合并 part 的摘要组合、完整逻辑序列归并或 raw/摘要混合查询。完整生产查询仍须单独设计。端到端对照包括真实构建的 v1.151.0 与当前分支、相同写入、十种字段组合、连续 merge 和重启，过程记录在测试文档。

| 接口位置 | 查询适配边界 |
|---|---|
| [search.go](lib/storage/search.go) 的 `BlockRef.MustReadBlock()`（72–82 行）及 `Search/tableSearch/partitionSearch/partSearch` | 查询使用独立的随机读取路径，不由顺序 `blockStreamReader` 适配覆盖 |
| [storage.go](lib/storage/storage.go) 2067–2084 行、[search.go](lib/storage/search.go) 287–314 行 | 同一 metric 可能重新生成 TSID，查询需要恢复 MetricName 并按逻辑序列归组 |
| [netstorage.go](app/vmselect/netstorage/netstorage.go) 1141–1175 行 | 已有按 MetricName 归组引用的逻辑；五特征需在适当归并完成前保留完整状态 |
| [netstorage.go](app/vmselect/netstorage/netstorage.go) 602、619 行 | 字段查询沿用原有函数调用；启用降采样要求 dedup interval 为零，因此该链路不消除样本。降采样计算本身不执行 dedup |
| `SearchQuery`、`BlockRef`、`MetricBlock`、portable 接口，以及 [vmstorage.go](app/vmstorage/vmstorage.go)、[server.go](lib/vmselectapi/server.go) | 需明确分辨率和五列表示与原单列协议的关系，并管理并行读取与缓冲生命周期 |

## 11. 验证要求

### 11.1 功能与故障验证

| 验证项 | 覆盖内容 |
|---|---|
| 分桶与五特征 | 两种固定分辨率、边界样本、空区间、单样本、乱序输入、同时间戳多值、重复写入、共享最大时间戳、`last` 时间戳优先及同时间戳选值 |
| 原始与摘要归并 | 原始样本提升、`sum/count` 累加、不同 bucket 对齐、多个 part 同区间、相同共享时间戳摘要、迟到写入、连续多次 merge、同源分辨率唯一选取；原始输入直接生成 `1h` 与先生成 `5m` 再重聚合为 `1h` |
| 特殊值 | 原始标记、全标记、同时间戳标记与数值、较新标记、`+Inf/-Inf`、运算 NaN 逐列传播、count 与时间戳不因其他列标记而遗漏 |
| 编解码与精度 | 五列独立 scale/首值/精度/marshal type，时间戳原有精度传递与有序性处理；count 使用浮点值，不增加取整或精度保护 |
| 落盘覆盖 | 单 inmemory dump、普通 flush、压力输出、关闭、snapshot flush、small/big merge、强制 merge；全部新增目标仅含摘要；inmemory 行为不变 |
| 窗口归并 | 原始 block 部分消费（`nextIdx != 0`）、输入超过现有 `maxRowsPerBlock`（8192 行）、TSID 切换及末尾区间；同 part 重叠 block、跨窗口 block 重复解码但唯一计入、空窗口跳跃、有界工作集、输出缓冲切换不拆分同 bucket、优化超限后清理并重试 |
| 格式与索引 | 五列完整且等长、一列共享时间戳、offset/size、常量列零负载、结构统计、完整排序键、多 index block、切换分辨率 flush、不存在的区间与分辨率、缓存隔离 |
| retention 与删除 | 区间右端点等于 deadline 时过期，deadline 位于区间内部时保留；同 bucket 部分摘要一致过期；5m 已过期而 1h 有效；原始文件保守清理；UTC 月边界；MetricID 删除；inmemory 行为不变 |
| 历史兼容 | 合法 v1 逐样本退化、历史缺 `metadata.json` 和 `parts.json`、v3 缺列不退化、v1/v3 混合源、旧程序拒绝 v3、snapshot/backup 恢复后读取和 merge |
| 初始化 | 启用与 dedup 互斥、直接 storage API 调用、关闭开关且存在活动 v3、空目录和仅 v1、仅 snapshot 有 v3、多 partition 启动顺序、清单异常；预检查失败无后台数据修改 |
| 发布与恢复 | 两种分辨率同 part 发布、单分辨率零行、全空输出；列或分辨率未写完时崩溃、发布前后重启、源引用释放、转换失败保留源 |
| 错误与资源 | 截断、损坏、未知版本/编码、异常长度和解压大小、整数溢出、取消、磁盘不足、全部文件关闭、Pool 重用无残留、异常大对象释放、并发 race |

数值测试以普通 `float64` 运算为参考，并在相同编码边界对照现有 `decimal.AppendFloatToDecimal()`、`encoding.MarshalValues/UnmarshalValues()` 和 `decimal.AppendDecimalToFloat()`。可精确表示的小规模样例直接断言；一般场景不要求不同 merge 顺序或编码次数逐位一致，也不对 count 增加任意规模整数精确性断言。

兼容验收允许活动 v1/v3 共存，不以全部历史文件转换完成为条件。存储验收覆盖内部读取、后续 merge 和重启恢复，本轮另以单机 `query.field` 对照验证单特征读取，完整生产查询不作为存储验收前提。

### 11.2 性能与容量评估

基准覆盖高频密集序列、低频稀疏序列、高基数、重叠 part、迟到数据和极值数值，分别测量原始转换、摘要 merge、索引定位及五列编解码的吞吐、延迟、CPU、峰值内存、`allocs/op` 和实际磁盘字节数。

单独评估两种分辨率分别遍历、窗口重复解码、优化超限重试的 I/O 成本，以及不同窗口大小、block 限额和并发 merge 预算的影响。空间估算须覆盖稀疏输入的输出放大与源目标并存，验证空间不足时能够安全取消。

性能与压缩率以测量结果确定，不预设五特征双分辨率必然节省空间，也不在缺少基准数据时承诺固定吞吐、开销比例或完成工期。
