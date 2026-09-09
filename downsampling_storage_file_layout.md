# 降采样文件布局（当前实现）

> 状态：已实现，以 `experimental/downsampling` 当前工作区生产代码为准；集群版基线为 `v1.151.0-cluster`。
> 本文替代 [downsampling_storage_design.md](downsampling_storage_design.md) 第 3 节的旧布局描述。
> 实现状态与验证范围分别见 [实现说明](downsampling_storage_implement.md) 和 [测试说明](downsampling_storage_testing.md)。
> **兼容性：当前格式为 89 字节原生 header / 113 字节 metaindex，feature 编号为 0..4。**
> 旧 99/112 字节布局，以及 `ae899eded` 生成的 89/113 字节但 feature 为 1..5 的布局，均不兼容。
> FormatVersion、SemanticsVersion 和 magic 仍为 2，不代表旧实验文件可读；没有迁移或旧布局读取路径。
> 旧实验数据须保留备份，并从原始数据在新目录重建，不能直接复用旧摘要目录。

## 0. 需求逐条映射

| # | 需求 | 本文落点 |
|---|---|---|
| 1 | 复用原生 `blockHeader`，不加字段；resolution/feature 位于 metaindex row，版本位于文件 magic 与 metadata | §2、§3 |
| 2 | `downsampleMetaindexRow` 直接嵌入 `metaindexRow` | §3.1 |
| 3 | 每个 row 控制的 headers 与 blocks 全属于同一 `(resolution, feature)` | §3.2 |
| 4 | 同一 row 允许多 TSID，但必须同租户 `Account:Project`，租户变化时切 row | §4 |
| 5 | 五路磁盘 spill 按 `(resolution, feature)` 全局集中输出，timestamps 按生成顺序写一次 | §5 |

---

## 1. 排序键与文件布局总览

新排序键（也是 index.bin / values.bin 中 block 的磁盘顺序）：

```text
(ResolutionMs, feature, TSID.Less, MinTimestamp)
```

与旧键 `(ResolutionMs, TSID.Less, MinTimestamp, Feature)` 的区别：

- `ResolutionMs` 仍为主键，feature 从批次内末位提升为第二键。磁盘、查询 API 与 `downsampleFeatureLast..Max` 常量统一使用 0=last、1=sum、2=count、3=min、4=max。
- 同一 `(ResolutionMs, feature)` 的全部 block 在 index.bin、values.bin 中**物理连续**；一个 index block（= 一个 metaindex row）仅属于一个 `(ResolutionMs, feature)`。
- feature 由 metaindex row 统一声明，不再重复存入每条 header。`TSID.Less` 依次比较 AccountID、ProjectID、MetricGroupID、JobID、InstanceID、MetricID，租户已包含在 TSID 排序中。

各文件语义变化：

| 文件 | 旧布局 | 新布局 |
|---|---|---|
| timestamps.bin | 每批一份时间戳负载，按生成顺序追加 | **不变**：仍每批一份，按生成顺序追加；同分辨率 5 个 feature 共享 timestamps.bin 中**同一 payload 的同一 `(offset,size)`**（时间戳是跨列共享元数据，不属于任何单一 feature，故不参与 feature 分组） |
| values.bin | 每批依次 last→max 交错 | 按 `(ResolutionMs, feature)` 分组：同组 values 负载连续，组间再切换 |
| index.bin | 每 99 字节一条 `downsampleFieldHeader` | 每 89 字节一条**原生 `blockHeader`**，index block 内只含同一 `(ResolutionMs, feature)` |
| metaindex.bin | 112 字节 `downsampleMetaindexRow` | 113 字节新 `downsampleMetaindexRow`（集群版，嵌入 `metaindexRow` + 新增字段；单机版 97 字节） |
| metadata.json | 不变 | 不变（RowsCount/BlocksCount 统计口径不变，仍按单特征 Block 计） |

### 1.1 各文件分块 ASCII 图

下列图中 `[tsN]` 表示一批共享时间戳 payload，`[vN]` 表示单列 value payload，
`[bhN]` 表示一条原生 `blockHeader`（89 字节），`[mrN]` 表示一条 `downsampleMetaindexRow`（113/97 字节）。

**timestamps.bin** — 输入按分辨率递增，因此先连续存储 5m 时间列，再存储 1h 时间列；每组内部按 `(TSID.Less, MinTimestamp)` 生成 block。同一 `(resolution, 批次)` 的 5 个 feature 引用同一 `(offset,size)`：

```text
+----------------------------------------------------------------------+
| timestamps.bin                                                        |
+----------------------------------------------------------------------+
| 5m: [ts0][ts1][ts2] ... | 1h: [ts0][ts1][ts2] ...                    |
+----------------------------------------------------------------------+
   (feature 0..4 共享各自 resolution/batch 的同一 offset/size)
```

**values.bin** — 按 `(ResolutionMs, feature)` 分组，组内单列 value payload 连续：

```text
+------------------------------------------------------------------------------+
| values.bin                                                                    |
+------------------------------------------------------------------------------+
| 组(res=5m,f=0): [v0][v1][v2]... | 组(res=5m,f=1): [v0][v1][v2]... | ...       |
| 组(res=1h,f=0): ...             | ...                                          |
+------------------------------------------------------------------------------+
```

两组 timestamps、十组 values 是连续的逻辑分组；常量或单行编码允许 payload 为零字节，定位信息仍在 index 中，不保证每组都有非空字节区段。

**index.bin** — 一个 index block 对应一个 metaindex row；block 内只含同一 `(ResolutionMs, feature)` 的 89 字节 `blockHeader`：

```text
+---------------------------------------------------------------------------------+
| index.bin                                                                        |
+---------------------------------------------------------------------------------+
| idxBlk0 = "VMDSIX\x00\x02" + ZSTD( [bh0][bh1]...[bh_{N0-1}] )                    |
| idxBlk1 = "VMDSIX\x00\x02" + ZSTD( [bh0][bh1]...[bh_{N1-1}] )                    |
| ...                                                                              |
+---------------------------------------------------------------------------------+
   ^ idxBlk0 由 metaindex row 0 定位 (IndexBlockOffset/Size, BlockHeadersCount=N0)
   ^ idxBlk1 由 metaindex row 1 定位 (IndexBlockOffset/Size, BlockHeadersCount=N1)
```

**metaindex.bin** — `VMDSMI\x00\x02` + ZSTD 压缩的 `downsampleMetaindexRow × N`，
按 `(ResolutionMs, feature, 租户 AccountID:ProjectID, TSID.Less, MinTimestamp)` 排序：

```text
+-----------------------------------------------------------------------------------+
| metaindex.bin                                                                      |
+-----------------------------------------------------------------------------------+
| "VMDSMI\x00\x02" + ZSTD( [mr0][mr1][mr2]...[mr_{N-1}] )                            |
+-----------------------------------------------------------------------------------+
   mr_k = metaindexRow(TSID/BlockHeadersCount/MinTS/MaxTS/IndexBlockOffset/IndexBlockSize)
          + feature(1B) + ResolutionMs(8B) + LastTSID + RowsCount(8B)
```

**metadata.json** — part 级元数据（`partHeader` 嵌入 + 降采样扩展字段）：

```text
{
  "RowsCount":        <五列物理行数>,
  "BlocksCount":      <五列物理 Block 数>,
  "MinTimestamp":     <int64>,
  "MaxTimestamp":     <int64>,
  "MinDedupInterval": 0,
  "FormatVersion":    2,
  "SemanticsVersion": 2,
  "Mode":             "downsampling",
  "Resolutions":      [300000, 3600000],
  "BucketOrigin":     0,
  "NumericCodec":     "decimal-values",
  "Retention":        "bucket-end"
}
```

---

## 2. index.bin：复用原生 blockHeader

### 2.1 已移除的旧包装

生产 codec/writer/reader 不再使用旧 `downsampleBlockHeader`、`downsampleFieldHeader`、批次内部列描述及其尺寸常量。

- index 条目 = 原生 `blockHeader`（集群版 89 字节，`marshaledBlockHeaderSize`），直接复用 `blockHeader.Marshal/Unmarshal`。
- `ResolutionMs`、`feature` 位于 `downsampleMetaindexRow`；版本由 index/metaindex magic 与 metadata 声明，**不是 row 的额外字段**。
- 原生 header 不增加字段；旧 `TimestampPrecisionBits` 包装字段已删除。

### 2.2 原生 blockHeader 足够自描述

`blockHeader` 已含 TSID、MinTimestamp、MaxTimestamp、TimestampsBlockOffset/Size、ValuesBlockOffset/Size、RowsCount、FirstValue、Scale、两个 marshal type、PrecisionBits。
新布局下每个 index 条目描述「一个 TSID 的一列在一个分辨率下的一个 Block」：

- 时间戳描述：`TimestampsBlockOffset/Size/MarshalType` 指向 timestamps.bin 中**被 5 列共享**的负载。
- 值描述：`ValuesBlockOffset/Size/MarshalType/FirstValue/Scale` 指向 values.bin 中该列的负载。
- `PrecisionBits`：时间戳与值共用的精度，等于旧 `TimestampPrecisionBits`（旧字段随之删除）。

### 2.3 index block 结构

```text
index block = "VMDSIX\x00\x02" + ZSTD( blockHeader × BlockHeadersCount )
```

- `BlockHeadersCount` 现在是「该 `(ResolutionMs, feature)` 组内的单列 Block 数」，**不再**要求被 5 整除。
- 单 index block 解压上限仍为 `maxBlockSize = 65536`；每条 89 字节，至多 `65536 / 89 = 736` 条（旧为 132 批 × 5 = 660 条）。

### 2.4 不再需要的成组语义

以下「五字段成组」约束一并移除或改写：

- `downsampleMetaindexRow.unmarshal` 中 `BlockHeadersCount%countOfDownsampleFeatures != 0` 校验 → 删除。
- `downsample_part.go` 中 `RowsCount%5 != 0 || BlocksCount%5 != 0` 仍保留（统计口径是「物理单列 Block 数」，每批仍贡献 5 个单列 Block）。
- `downsample_space.go` 的空间估算从 `downsampleBlockHeaderSize` 改为 `marshaledBlockHeaderSize`。

---

## 3. downsampleMetaindexRow：嵌入 metaindexRow

### 3.1 结构定义

```go
type downsampleMetaindexRow struct {
    metaindexRow          // 直接嵌入：TSID / MinTimestamp / MaxTimestamp /
                          //   IndexBlockOffset / BlockHeadersCount / IndexBlockSize

    feature       uint8    // 0=last 1=sum 2=count 3=min 4=max
    ResolutionMs int64    // 300000 或 3600000
    LastTSID     TSID     // 保留：末 TSID，用于排序/二分定位
    RowsCount    uint64   // 保留：该 index block 的物理行数（用于统计一致性）
}
```

嵌入字段语义与原生 `metaindexRow` 完全一致：`TSID` 为该 index block 的首 TSID，`BlockHeadersCount` 为该组单列 Block 数，
`MinTimestamp/MaxTimestamp` 为该组时间范围，`IndexBlockOffset/IndexBlockSize` 定位压缩后的 index block。

### 3.2 marshal 布局（集群版，TSID=32 字节）

| offset | 字节 | 字段 | 来源 |
|---:|---:|---|---|
| 0 | 32 | TSID（首 TSID） | 嵌入 `metaindexRow` |
| 32 | 4 | BlockHeadersCount | 嵌入 |
| 36 | 8 | MinTimestamp | 嵌入 |
| 44 | 8 | MaxTimestamp | 嵌入 |
| 52 | 8 | IndexBlockOffset | 嵌入 |
| 60 | 4 | IndexBlockSize | 嵌入 |
| 64 | 1 | feature | 新增 |
| 65 | 8 | ResolutionMs | 新增 |
| 73 | 32 | LastTSID | 新增（保留） |
| 105 | 8 | RowsCount | 新增（保留） |

合计 **113 字节**。`feature` 与 `ResolutionMs` 构成该 row 控制的全部 block 的 `(ResolutionMs, feature)` 身份。

实现约定：

- `marshal` 先 `m.metaindexRow.Marshal(dst)`，再依次 `append(dst, m.feature)`、`MarshalInt64(ResolutionMs)`、`LastTSID.Marshal`、`MarshalUint64(RowsCount)`。
- `unmarshal` 先 `m.metaindexRow.Unmarshal(src)` 消费前 64 字节，再解剩余字段。
- `downsampleMetaindexRowSize` 保持运行时计算（TSID 大小随集群/单机而变），单机版会少 16 字节（首/末 TSID 各少 8），对应为 97 字节。
- `feature` 用单字节紧凑存储，不做对齐填充；如后续需要 8 字节对齐，可在 `feature` 后加 7 字节保留位（本文不采用，保持与旧格式一致的紧凑约定）。

### 3.3 校验

`unmarshal` 中的合法性校验改写为：

```text
validDownsampleResolution(ResolutionMs)
feature 在 [0, countOfDownsampleFeatures)
BlockHeadersCount > 0（不再 %5）
IndexBlockSize ∈ [len(downsampleIndexMagic), downsampleMaxIndexSize]
IndexBlockOffset 不溢出
MinTimestamp/MaxTimestamp 在时间域内且 Min<=Max
LastTSID 不早于 TSID（TSID.Less 序）
```

---

## 4. 同租户约束

租户 = `AccountID:ProjectID`，**不额外落盘**，一律从 `TSID.AccountID/ProjectID`（集群版 TSID 前 8 字节）提取。
`downsampleMetaindexRow` 不再含独立的 AccountID/ProjectID 字段，同一 row 内全部 `blockHeader.TSID` 必须同租户。

### 4.1 写入端切 row 条件

在把 block 并入当前 index block 之前，若当前组已有内容且租户发生变化，则 flush 当前组、开新组：

```text
if curMr.BlockHeadersCount > 0 &&
   (h.TSID.AccountID != curMr.TSID.AccountID || h.TSID.ProjectID != curMr.TSID.ProjectID) {
    flushIndex()
}
```

`TSID.Less` **先比较 AccountID，再比较 ProjectID**，之后才比较 MetricGroupID、JobID、InstanceID、MetricID。
因此同一 `(resolution, feature)` 内租户单调且连续；但排序本身不会阻止一个 index block 跨越租户边界，
writer 仍须显式比较租户并切 row，不能仅依赖 index 大小阈值。

### 4.2 读取端校验

- `openDownsamplePart` 解码每个 row 时校验首末 TSID 同租户，并执行 §7 的五路全索引校验。
- `downsampleReader.validateIndex` 逐条校验 header 与 row 同租户、TSID 位于首末范围内、
  `(TSID.Less, MinTimestamp)` 有序；同 TSID 的相邻 Block 时间范围不得重叠。

---

## 5. 写入路径：五路磁盘 spill 与有界外部转置

### 5.1 WriteBlock：时间戳立即写一次，五列分别暂存

1. 校验分辨率、行数、精度、编码前 bucket 唯一，以及输入 `(ResolutionMs, TSID.Less, MinTimestamp)` 顺序。
2. 分辨率变化时先 `flushResolution()` 完成上一分辨率；不允许回到较小分辨率。
3. 五列分别调用原生 `Block.MarshalData`，比较时间戳编码字节及描述一致性。**当前是五次编码校验、一次磁盘写入**，不是只编码一次。
4. 将第一列的 timestamps payload 按生成顺序追加到 `timestamps.bin`，五个 header 引用同一最终 offset/size。
5. 每列向自己的 `.downsample-spill-*` 临时文件追加「89 字节原生 header + values payload」。spill 不保存 timestamps payload；其中 values offset 是占位值，输出时重算。

writer 只保留当前批次的五个 Block、单列 spill 读取缓冲、当前 index 和有上限的 metaindex；不在内存中积累全部批次。

### 5.2 flushResolution：保证整个 part 的全局顺序

每个 spill 已按 `(TSID.Less, MinTimestamp)` 排序。分辨率切换或 `Finish()` 时，依次完整消费 feature 0..4 的 spill：

- 逐条读取 header 和 values，校验 header、时间戳引用范围、顺序及连续性。
- 将 values 追加到最终 `values.bin`，重写 `ValuesBlockOffset`，将原生 header 追加到 index。
- index 达到大小阈值或租户变化时 `flushIndex()`；每列结束也 flush，保证 row 不跨 feature。
- 校验五列 Block 数和行数一致；每列消费完成后关闭并删除对应 spill。

这是一种利用输入有序性的外部转置，不需要全量展开、内存排序或通用外部归并排序。
最终 values/index/metaindex 按 `(ResolutionMs, feature, TSID.Less, MinTimestamp)` **全局**排列。

**分段 flush 不能破坏全局排序。** 同一分辨率若先输出片段 A 的 feature 0..4，再输出片段 B 的 feature 0..4，
会从 feature 4 回退到 feature 0，违反格式。允许在同一 feature 内分多个 index block，或分段追加各自 spill；
不能把每个输入片段的五列直接轮流写入最终文件。当前实现只在分辨率结束时完整转置。

### 5.3 offset、发布与失败处理

- timestamps offset 在 `WriteBlock` 中确定，转置时不改写、不重复写入。
- values offset 在消费 spill 时确定；index offset/size 在 `flushIndex` 压缩并追加时确定。
- `Finish` 完成 spill、index、metaindex、metadata 的写入及文件/目录同步后才返回可发布的 partHeader。
- 部分写入失败会锁定 writer 错误，禁止继续发布；`Abort` 关闭句柄并删除未发布目标及 spill。

### 5.4 空间上界与溢出

令 R 为摘要逻辑行数（已合并两个分辨率），B 为五 Block 批次数，H=89、M=113、F=5。
`estimateDownsampleOutputSize` 的保守磁盘预算为：

- 最终 payload：`10 × R × (F+1)`，即一份 timestamps 与五份 values。
- 五列独立 index/metaindex：`F × B × ((2H+256+8) + (2M+256+8))`。
  最坏每个单列 Block 独占一个 index 和一个 metaindex row；metaindex 帧开销按 row 重复计入是保守上界。
- spill 共存峰值：`10 × R × F + F × B × H`，只含五列 values 与原生 headers，不重复计 timestamps。
- metadata：`downsampleMaxMetadataSize`（64 KiB）。

预算覆盖最终输出与全部 spill 同时存在的保守峰值；实际逐列删除 spill，峰值通常更低。
raw 输入按两个分辨率放大行数，摘要物理行数除以五并向上取整；未知 TSID 分布时取 B=R。
所有加法、乘法饱和到 `math.MaxUint64`，不允许回绕低估；不把删除源文件当作可用空间。
writer 在写批次、转置和最终索引输出前复查空间，实际 I/O 失败仍走 Abort。

---

## 6. 读取与归并

### 6.1 查询只读目标列

`downsampleReader.current` 是原生 `blockHeader`。`Init(p, resolution, feature)` 接受 API 编号 0..4，
省略 feature 时默认 last。`SetFilter` 二分定位目标 `(resolution, feature)` 的 metaindex 范围，
再按 LastTSID 和时间范围筛选；每次只解压一个 index block。

`partSearch` 初始化时传入目标 feature，`FieldHeader` 直接返回该列 header，payload 继续由 `BlockRef/Block` 读取。
**打开 part 的全索引校验与查询单列读取是两个阶段**：查询不为其他四列读取 index 或 values，
但不能据此声称打开 part 时也只读一列。

### 6.2 归并五路对齐

`ReadBlock` 重建完整 `downsampleBatch`：主 reader 默认读取 last，`FieldHeader` 按需创建其余四列 peer reader，
按 `(TSID.Less, MinTimestamp)` 推进并对齐。五列必须具有相同的 TSID、RowsCount、Min/MaxTimestamp、
PrecisionBits、TimestampsBlockOffset/Size/MarshalType；随后分别解码五列 values。

merger 仍按 `(resolution, TSID, 时间窗口)` 生成五列批次，最终 feature 全局排列由 writer 的 spill 转置完成，
不是让 merger 按 feature 分别生成批次。raw 输入仍走原生 Block 解码路径。

---

## 7. 打开时有界五路全索引校验

`openDownsamplePart` 先有界读取 metadata/metaindex，校验版本、row 长度、租户、排序、index 连续覆盖及物理统计，
再调用 `validateDownsamplePartIndexes`，对每个分辨率用五个 reader 同步遍历**全部** index headers：

- 五列同时存在或同时结束，拒绝缺列、多余 Block 和共享时间戳描述不一致。
- 每个 index 内及跨 index 的 TSID/时间顺序、租户、行数、首末 TSID、时间范围与 metaindex 一致。
- 以 last 列为唯一时间戳序列，跨分辨率累计 timestampEnd，验证从零到 timestamps 文件末尾的连续覆盖。
- 每列 values 连续，列间及分辨率间首尾相接，最终覆盖 values 文件全长。
- 拒绝越界、偏移溢出、空洞、重叠、截断和未引用尾部；零长度 const payload 允许相同相邻 offset。

五路各只保留当前 index block（解压上限 64 KiB），不建立随 part 增长的 header/offset 集合。
metaindex 本身仍整体驻留，压缩文件与解压数据各受 64 MiB 上限约束；“有界”不表示整个打开过程只有五个 index 的内存。
打开校验不解码 timestamps/values 数值，数值解码与边界检查在实际读取 Block 时完成。

---

## 8. 验证状态

当前 Go 布局测试已使用 89/113 字节断言，并包含多 index、租户边界、零负载及损坏场景。
空间测试覆盖五列独立索引预算、spill 共存上界和饱和溢出，具体运行结果见测试说明。
Python `downsampling_inspect.py` 已适配 89/113 字节与 feature 0..4，独立解析 metaindex/header，按五列校验共享 timestamps、values 连续布局及租户边界。
检查器 UT 覆盖正确文件、旧编号及损坏文件。当前布局完整一键测试已于 2026-09-09 通过：15 个阶段、46 项 Python UT、1670 项 E2E 和四组文件检查；数值比较最大绝对误差为 0。具体证据及覆盖边界见测试说明。
