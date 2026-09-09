# 降采样存储（file-based downsampling）Code Review

> 评审范围：`experimental/downsampling`（HEAD）相对基准分支 `control/downsampling` 的全部**非测试**主干逻辑。
> 测试文件（`*_test.go`、`testdata/*.py`、`downsampling_e2e.sh`）按要求排除在本评审之外。

---

## 1. 变更概览

| 项 | 值 |
|---|---|
| 基准分支 | `control/downsampling` |
| 当前分支 | `experimental/downsampling`（HEAD） |
| 变更文件 | 60 个 |
| 新增/删除行 | **+10998 / -32** |
| 功能版本 | `downsampleVersion = 2` |

### 1.1 核心功能摘要

- 固定分辨率：`5m = 300000ms`、`1h = 3600000ms`（`downsample.go`）。
- 五个聚合特征：`last / sum / count / min / max`（`downsampleFeaturesCount = 5`）。
- 区间语义：左闭右开，`bucketID = timestamp / resolution`，只对实际输入产生摘要、不填充空区间。
- 存储格式：`timestamps.bin`（共享时间戳负载）、`values.bin`（五列单特征 Block 负载）、`index.bin`（五字段批次 header）、`metaindex.bin`、`metadata.json`。
- 新开关：`-storage.downsampling.enabled`（默认 false），与 `-dedup.minScrapeInterval != 0` 互斥。
- 字段查询：`query.field=5m:last` 等十种组合；RPC 名为 `search_downsampling_v2`。

### 1.2 改动文件清单（非测试）

**新增（`lib/storage/`）**

| 文件 | 行数 | 职责 |
|---|---|---|
| `downsample.go` | 132 | 常量、`downsamplePoint`、`downsampleAccumulator`、NaN 归一、分桶 |
| `downsample_batch.go` | 43 | 临时批次 + 对象池 |
| `downsample_codec.go` | 226 | header/metaindex 编解码与严格校验 |
| `downsample_writer.go` | 320 | 五列写盘、索引刷新、Finish/Abort |
| `downsample_reader.go` | 529 | 单 index block 解码、按窗口定位、原生 Block 还原 |
| `downsample_merger.go` | 395 | 有界窗口归并、跨分辨率调度 |
| `downsample_open.go` | 209 | 启动预检查、parts.json 严格解析 |
| `downsample_part.go` | 251 | 元数据、格式探测、part 打开 |
| `downsample_partition.go` | 192 | 文件目标归并、retention、合并调度 |
| `downsample_query.go` | 80 | `DownsampleQueryField` 解析与 block 选择 |
| `downsample_search_protocol.go` | 36 | `search_downsampling_v2` 编解码 |
| `downsample_space.go` | 189 | 磁盘空间预算、reserve/retire、溢出安全 |

**新增（vmselect 侧）**

| 文件 | 行数 | 职责 |
|---|---|---|
| `app/vmselect/netstorage/downsample_query.go` | 21 | 请求编解码 + 全节点成功语义 |
| `app/vmselect/prometheus/downsample_query.go` | 27 | `query.field` 参数解析 |

**修改（核心，已逐文件评审）**

| 文件 | 要点 |
|---|---|
| `app/vmstorage/main.go` | 新增 flag、dedup 互斥校验 |
| `app/vmstorage/vmstorage.go` | `sr.InitWithDownsampleField` |
| `lib/storage/part.go` | 新增 `dsMetadata/dsMetaindex/dsFiles/dsFileSizes`，格式探测路由 |
| `lib/storage/part_search.go` | `initWithDownsampleField`、`nextDownsampleBlock` |
| `lib/storage/partition.go` | `errDownsampleNoSpace` 分支、`splitDownsampleMergeBatch`、`mergeDownsampleParts` 分派、`filePartExpired` |
| `lib/storage/partition_search.go` | `initWithDownsampleField`、`MustClose` 释放顺序 |
| `lib/storage/search.go` | `InitWithDownsampleField`、`SearchQuery.DownsampleField`、`Unmarshal` 清理 |
| `lib/storage/storage.go` | `downsamplingEnabled`、`OpenOptions.DownsamplingEnabled`、启动预检查 |
| `lib/storage/table_search.go` | `initWithDownsampleField` |
| `app/vmselect/netstorage/netstorage.go` | `rpcName` 参数、`collectDataSearchResults` |
| `app/vmselect/prometheus/prometheus.go` | field 解析、`noCache`、参数透传 |
| `app/vmselect/promql/eval.go` | `EvalConfig.DownsampleField`、`MayCache()=false` |
| `lib/vmselectapi/server.go` | `search_downsampling_v2` 分派、`processSearch(ctx, downsample)` |

---

## 2. 分模块评审发现

### 2.1 `downsample.go` — 数学计算与分桶

- `downsampleAccumulator.AddSummary`：last 语义实现与设计一致——时间戳更大时直接覆盖；**相同时间戳**时「较新标记优先、再取较大值」（`math.IsNaN(last) || (!math.IsNaN(next) && next > last)`），且不会让较新标记回退到较早数值。
- sum/count 累加：任一输入为 NaN 即传播 `decimal.StaleNaN`；count 对 raw NaN 样本仍计 1（`AddRaw` 中 `values = {v, v, 1, v, v}`）。
- min/max：NaN 传播 + `math.Min/Max` 合并。
- `normalizeDownsampleValue` 将**所有** NaN（含普通 NaN、`StaleNaN`、`Inf/NaN` 之外的异常值）统一为 `decimal.StaleNaN`，不区分来源，符合设计。
- `downsampleBucketID`：整数除法定位左闭右开区间；拒绝 `timestamp < minUnixMilli` 或 `> maxUnixMilli`。
- `downsampleBucketEnd`：在 `(bucketID+1)*resolution` 前先检查 `bucketID >= MaxInt64/resolution`，**溢出防护正确**。

> 🟡 关注点：`count` 以 `float64` 存储（设计已声明）。当单区间 count 超过 `2^53`（约 9e15）时会丢失整数精度，超长窗口聚合理论上存在精度风险。当前分辨率 5m/1h 下极难触达，但建议在测试中确认该边界是否有意接受。

> 🟡 关注点：`AddSummary` 的 sum 若溢出为 `+Inf`，`math.IsNaN` 判否、不会归一为标记，`+Inf` 会保留。设计只对「运算产生的 NaN」归一，`Inf` 未覆盖，属已知语义边界。

### 2.2 `downsample_codec.go` — 编解码与校验

- 尺寸在运行时计算（`downsampleFieldHeaderSize` 等），正确处理**集群版 TSID 含 AccountID/ProjectID、比单机多 8 字节**的问题，未硬编码——正确。
- **双编号体系**（务必区分）：
  - 磁盘特征编号：`1..5`（last=1 … max=5），由 writer 以 `uint8(i+1)` 写入；
  - 查询/网络编号：`0..4`（`downsampleFeatureLast=0 … max=4`），见 §2.9/§2.10。
  - `downsampleFieldHeader.unmarshal` 校验磁盘 `Feature ∈ [1,5]`，`downsampleBlockHeader.unmarshal` 校验 `fh.Feature == uint8(i+1)`，二者自洽。
- `downsampleFieldHeader.unmarshal` 校验：
  - 长度必须等于 `downsampleFieldHeaderSize`；
  - `TimestampPrecisionBits ∈ [1,64]`；
  - `TimestampPrecisionBits == bh.PrecisionBits`（**共享精度约束**，与仓库记忆一致）；
  - 行数/时间范围/偏移溢出。
- `downsampleBlockHeader.unmarshal`：强校验同一批次五列共享 TSID/Resolution/RowsCount/时间范围/时间戳 offset+size+MarshalType/TimestampPrecisionBits，且 values 列 offset 连续。
- `downsampleMetaindexRow.unmarshal`：校验排序、`BlockHeadersCount % 5 == 0`、行数上界、`IndexBlockSize` 上界。
- `checkDownsampleExtent`：`offset > fileSize || size > fileSize-offset` 的越界防护正确。

> 🟢 无阻塞问题。校验覆盖了长度、范围、排序、一致性、越界，符合「损坏文件必须报错、不得退化为 raw」的设计约束。

### 2.3 `downsample_writer.go` — 写盘

- `WriteBlock` 校验：分辨率合法、`0 < n ≤ maxRowsPerBlock`、`precisionBits ∈ [1,64]`、五列行数一致、时间戳严格递增且**同 bucket 不重复**、block 排序正确、跨分辨率触发 `flushIndex`。
- 时间戳负载只写一次（`i==0`），后续特征引用相同 offset/size；非首列校验时间戳编码与首列一致（`bytes.Equal`）。
- `flushIndex`：index block 前缀 `downsampleIndexMagic`，ZSTD 压缩后大小上界双重校验（`> downsampleMaxIndexSize` 或 `> 2*原始+256+magic`，防压缩膨胀）。
- `Finish`：metaindex 压缩、写 `metadata.json` 并 `f.Sync()`，随后对所有文件 `Sync+Close`，最后 `syncDownsampleDir`（目录 + 父目录）——**崩溃一致性处理到位**。
- `Abort`：关闭文件句柄并 `RemoveAll` 未发布目标。
- `reset`：对大缓冲按 `downsampleMaxIndexSize/downsampleMaxPooledRows` 阈值释放，避免峰值容量长期驻留池中。

> 🟢 实现稳健。`errors.Join` 聚合 I/O 错误、`io.ErrShortWrite` 检测、溢出检查（`^uint64(0)-w.ph.RowsCount < …`）均正确。

### 2.4 `downsample_reader.go` — 读取与还原

- 每次只解码一个 index block，按 metaindex 定位窗口，不载入全部 header。
- `SetFilter`：
  - 摘要路径用 `LastTSID` 单调性 `sort.Search` 定位起始 index（保留边界块）；
  - raw 路径用 `TSID` 且 `start--` 保留可能包含目标的前一块。
- `NextHeader`：排序校验（`h.less(&r.previous) || (!h.raw && !r.previous.less(&h))`）、filter 提前终止、时间窗口相交过滤。
- `validateIndex`：校验 index 与 metaindex 统计一致（rows/first/last/min/max）、相邻 block 与**相邻 index block** 的负载偏移连续性、首偏移为零、末 index block 与文件尾对齐（`m.IndexBlockOffset+size == fileSize`）。
- `prepareBlockPayload`：仅对 `ZSTDNearestDelta(2)` 解压并转换为非压缩 MarshalType，`rows*10` 上界限制展开大小。
- `readRawBlock`：raw 提升时，`count` 恒为 1（对 NaN 样本亦如此），`sum/min/max = v`，与设计一致。
- `readAt`：摘要用 `os.File.ReadAt`，raw 文件路径用 `os.File`，inmemory 用 `fs.MustReadAtCloser`（先经 `checkDownsampleExtent` 校验，安全）。
- `reset`：释放自有文件句柄、按阈值回收大缓冲。

> 🟢 正确性良好。调用方须在 reader 存活期间持有 part 引用（注释已声明），`partition_search.MustClose` 释放顺序已配合（§2.12）。

### 2.5 `downsample_batch.go` — 批次与池

- `Reset` 释放超阈值缓冲、保留正常容量；池化实现符合设计「异常大缓冲不长期留在池中」。

> 🟢 无问题。

### 2.6 `downsample_merger.go` — 归并

- `Merge`：源数量上限 `downsampleMaxMergeSources=1024`、窗口 `1..downsampleWindowBuckets(1024)`；先累加 sourceRows 做溢出检查。
- 双分辨率循环：先 5m 再 1h，每个分辨率独立 `initSources` + 堆归并。
- `downsampleCursorHeap`：`Less` 用 `Header().TSID.Less`；`Push` panic（只做 `heap.Init`，不动态 push）——合理。
- `collectSources`：消费当前 TSID 所有索引项，推进游标到下一 TSID，并累计 `rowsDeleted`（`downsampleSourceRowWidth`：摘要=5、raw=1）。
- `mergeTSID`：以 `minTimestamp/resolution` 为 firstBucket，`nextBucket` 跳过空区间；`firstBucket != MaxInt64` 作循环终止。
- **精度一致性**：`readWindow` 内同 bucket 精度不一致报错；`mergeTSID` 内跨 bucket 精度不同时 `flushOutput` 拆分输出——与设计「不同区间拆分、同区间报错」一致。
- retention 统计：
  - raw 行只在 5m 分辨率（第一次遍历）计数，且**仅当 5m 与 1h 两个 bucket end 都 <= deadline 才计删除**——避免重复计数；
  - 摘要行每分辨率计数一次。
- `readWindow`：`SetFilter(tsid, firstBucket*resolution, maxUnixMilli)`，读取相交 block，逐样本分桶、retention 过滤、`AddSummary`。
- `Reset`：释放 reader/batch、`clear` 后 `[:0]`、峰值容量（>1024 / >downsampleWindowBuckets）置 nil。

> 🟢 归并语义与设计文档高度一致。
> 🟡 关注点：`mergeTSID` 中 `firstBucket+int64(len(m.states))-1` 理论上可能溢出 int64，但 `firstBucket` 来自真实数据桶、远小于 `maxUnixMilli/resolution`，实际不可达。建议加一行防御性注释或 `min()` 前先做溢出说明。

### 2.7 `downsample_open.go` + `downsample_part.go` — 打开与校验

- `checkDownsamplingOpen`：在持 flock 时执行，遍历 small/big/indexdb 三根目录发现 partition，读取 `parts.json` 或历史目录，`detectDownsampleFormat` 探测活动 part；发现降采样 part 但开关关闭时**报错**（不允许退化为 raw）。
- `readDownsamplePartitionNames`：与 `fs.IsPartiallyRemovedDir` 语义一致（空目录 / `.delete-this-dir` 视为非活动）。
- `parseDownsamplePartNames`：严格 JSON——拒绝重复字段（大小写不敏感）、未知字段、非对象、尾部垃圾数据。防止损坏清单隐式丢失活动 part。
- `readDownsampleMetadata`：
  - metadata.json 缺失 → 按 raw 解析路径（`ParseFromPath`）；
  - 存在但缺 `FormatVersion` 且含降采样语义字段 → **报错**（不误判为 raw）；
  - 必填字段（含 `MinDedupInterval`）缺失或 null → 报错。
- `downsamplePartMetadata.validate`：版本/模式/分辨率/BucketOrigin/NumericCodec/Retention/MinDedupInterval 全量校验；`RowsCount/BlocksCount` 须为 5 的倍数。
- `detectDownsampleFormat`：读 metaindex 前 8 字节——摘要要求 `VMDSMI\x00\x02`，raw 要求 ZSTD 帧头 `0x28 0xb5 0x2f 0xfd`；同时校验三个数据文件为普通文件。
- `openDownsamplePart`：metaindex 解压上限、长度对齐、`IndexBlockOffset` 连续性、排序、统计一致性（rows/blocks/min/max/nextOffset==fileSize）全量校验；错误路径关闭已打开句柄。

> 🟢 打开/校验链路完整，严格性符合设计。`detectDownsampleFormat` 对「未知 metaindex 标识」明确报错而非静默降级。

### 2.8 `downsample_partition.go` — 分区归并、retention、调度

- `mergeDownsampleParts`：
  - 仅文件目标；`isDedupEnabled()` 时直接报错；
  - 用 `errors.Join(errDownsampleNoSpace, err)` 包装 `ENOSPC/EDQUOT`，统一重试语义；
  - `estimateDownsamplePartSize` 预算 → `reserveDownsampleSpace`；
  - 发布前 `m.Reset()` 释放 reader 游标，避免旧源删除时仍被占用（尤其 NFS 文件句柄）；
  - `publicationStarted` 标志：清单提交后即使源清理失败也不删除目标。
- `filePartExpired`：降采样模式下须**两种分辨率**的 bucket end 都 <= deadline 才判过期，保守清理。
- `getFilePartsToMerge`：降采样模式用输出空间上界做调度；`minParts = max((maxParts+1)/2, 2)`；最终阈值 `max(defaultPartsToMerge/2, minMergeMultiplier)`。
- `splitDownsampleMergeBatch`：限制单次源数 <= 1024，剩余留给后续批次。

> 🟢 归并/发布/清理生命周期处理正确。
> 🟡 关注点：`getFilePartsToMerge` 的贪心窗口选择逻辑较复杂，建议用注释或单元测试明确「均衡性」目标（`multiplier >= bestMultiplier` 的选择策略）的预期行为，便于后续维护。

### 2.9 `downsample_query.go` — 字段选择

- `ParseDownsampleQueryField`：解析 `5m:last / 1h:count` 等；空串返回 nil（走原始查询路径）。
- `DownsampleQueryField.valid()`：分辨率合法 + `Feature < 5`（查询/网络编号 0..4）。
- `nextDownsampleBlock`：`SetFilter` → `NextHeader` → `FieldHeader(feature)` → `BlockRef.init`；feature 索引直接对应 `h.Columns[feature]`（`Columns[0]=last`）。

> 🟢 正确。注意查询侧 `Feature ∈ [0,4]`，与磁盘编号 `[1,5]` 通过 `Columns[feature]` 索引天然解耦，`FieldHeader` 内部再映射回磁盘 `feature+1`。

### 2.10 `downsample_search_protocol.go` — RPC 编解码

- `MarshalDownsampleWithoutTenant`：在原有查询负载末尾追加 **8 字节 Int64 分辨率 + 1 字节特征**（网络编号 0..4），不改变原生协议。
- `UnmarshalDownsample`：要求尾部 >= 9 字节，解码后校验 `field.valid()`，返回剩余尾部供调用方拒绝未消费数据。

> 🟢 协议简洁清晰，与 `search_downsampling_v2` 命名一致。

### 2.11 `downsample_space.go` — 空间预算

- `errDownsampleNoSpace` 作为统一「空间不足」信号，与 `errForciblyStopped` 区分（后者不可重试）。
- `estimateDownsamplePartSize`：raw 行乘 2 分辨率；摘要物理行 `/5` 向上取整；`addDownsampleSpace/multiplyDownsampleSpace` 溢出安全。
- `reserveDownsampleSpaceWithBudget`：进程级共享预算，`held = reserved + retiredBytes`；`release` 幂等（`sync.Once`）；释放后 `retiredBytes += size` 且 2s 后过期（对齐 `fs.MustGetFreeSpace` 的 2s 缓存），避免「删除源文件前」把其空间立即借给后续作业。
- `checkDownsampleAvailableSpace`：逐次相减避免 `available + held + requested` 溢出。
- `checkDownsampleWriteSpace/checkDownsampleFinishSpace`：写入中作业只复查物理空间，不重复扣除自身预留。

> 🟢 空间预算设计严谨（reserve/retire/过期/溢出均覆盖）。外部进程耗尽磁盘仍可能触发实际 I/O 错误，`mergeDownsampleParts` 已用 `ENOSPC` 包装兜底，writer 保留 Abort 路径。

### 2.12 既有文件集成（`part.go`、`partition.go`、`search.go`、`storage.go` 等）

- `partition_search.MustClose`：先 reset readers 池、再释放 part 引用，**避免释放引用后并发文件删除与 reader 竞态**——顺序正确。
- `mergeParts` 仅在 `dstPartType != partInmemory` 时走 `mergeDownsampleParts`；inmemory dump 快径保持 raw。
- `search.go Unmarshal` 先 `sq.DownsampleField = nil` 再反序列化，避免残留。
- `storage.go MustOpenStorage`：在目录锁与恢复检查后、IndexDB 初始化与后台任务前调用 `checkDownsamplingOpen`。
- `vmstorage.go`：`sr.Init` → `InitWithDownsampleField`。

> 🟢 集成点插入位置与顺序正确，符合设计文档第 4 节的「启动预检查时机」要求。

### 2.13 vmselect 侧（netstorage / prometheus / promql / vmselectapi）

- `marshalDataSearchQuery`：`DownsampleField != nil` 时选 `search_downsampling_v2`，否则 `search_v7`。
- `collectDataSearchResults`：**字段查询要求全部节点成功**（`collectAllResults`，不允许部分/副本容错）——因为旧节点不支持 `search_downsampling_v2`，任一节点失败即整体失败。属**有意设计**。
- `prometheus`：`getDownsampleQueryField` 要求恰好一个非空 `resolution:feature`；设置 field 时强制 `noCache`。
- `promql/eval.go`：`EvalConfig.DownsampleField` 透传；`MayCache()` 在 field 非空时返回 false；`evalRollupFuncNoCache` 设置 `sq.DownsampleField`。
- `vmselectapi/server.go`：`readSearchQueryVersion(downsample)`、`processSearch(ctx, downsample)`、分派 `search_downsampling_v2`。

> 🟢 端到端字段查询链路打通，缓存隔离正确。
> 🟡 运维提醒：由于字段查询「全节点成功」语义，集群滚动升级期间（部分节点尚未支持 `search_downsampling_v2`）字段查询会整体失败，需在发布方案中规划升级顺序。

---

## 3. 跨模块关注点与风险清单

### 🟡 P1 — 需要确认/建议补充测试

1. **count 的 float64 精度**（§2.1）：> 2^53 时精度丢失，确认是否有意接受。
2. **sum 溢出为 +Inf 的归一**（§2.1）：Inf 不归一为标记，确认语义边界。
3. **滚动升级期间字段查询全节点失败**（§2.13）：发布顺序需规划。
4. **`getFilePartsToMerge` 调度均衡性**（§2.8）：贪心策略建议补注释/测试明确预期。

### 🟢 P2 — 风格/一致性（不阻塞）

- **中文注释**：新增代码注释为中文，与 VictoriaMetrics 上游英文注释惯例不一致。若计划回馈上游，建议统一为英文；若仅内部维护，可保留。

### ✅ 已确认正确的关键不变量

- 精度一致性：`TimestampPrecisionBits == BlockHeader.PrecisionBits`，五列共享（仓库记忆 + §2.2）。
- NaN → `decimal.StaleNaN` 全局归一，count 对 raw NaN 仍计 1。
- 保留 v2 磁盘 `TimestampPrecisionBits` 字段，与原生 `BlockHeader.PrecisionBits` 相等，不一致报错。
- 双分辨率独立判断 retention；raw 行仅当两目标区间均过期才计删除（不重复计数）。
- 读者生命周期：`MustClose` 先释放 reader 再释放 part 引用；归并发布前 `m.Reset()` 释放 NFS 句柄。
- 空间预算 reserve/retire 过期对齐 `fs.MustGetFreeSpace` 2s 缓存；减法式溢出防护。

---

## 4. 任务清单（给我发任务）

按「每个文件/子系统一个评审单元」拆分，建议按下列顺序推进（先数学语义 → 编解码 → I/O → 归并 → 生命周期 → 集成 → 协议）：

| # | 任务 | 交付物 | 依赖 |
|---|---|---|---|
| 1 | 评审 `downsample.go` 数学语义与分桶 | 结论 + 关注点 | 无 |
| 2 | 评审 `downsample_codec.go` 编解码/校验 | 结论 + 关注点 | 1 |
| 3 | 评审 `downsample_writer.go` 写盘/Finish/Abort | 结论 + 关注点 | 2 |
| 4 | 评审 `downsample_reader.go` 读取/还原/validateIndex | 结论 + 关注点 | 2 |
| 5 | 评审 `downsample_merger.go` 窗口归并与 retention 统计 | 结论 + 关注点 | 1,4 |
| 6 | 评审 `downsample_batch.go` + 对象池 | 结论 | 1 |
| 7 | 评审 `downsample_open.go` + `downsample_part.go` 打开/探测/校验 | 结论 + 关注点 | 2,3 |
| 8 | 评审 `downsample_partition.go` 归并调度/发布/retention | 结论 + 关注点 | 5,7 |
| 9 | 评审 `downsample_space.go` 空间预算 | 结论 + 关注点 | 8 |
| 10 | 评审 `downsample_query.go` + `downsample_search_protocol.go` | 结论 | 2 |
| 11 | 评审存储层集成（`part.go`/`partition.go`/`partition_search.go`/`search.go`/`storage.go`/`table_search.go`） | 结论 | 7,8 |
| 12 | 评审 vmstorage 开关接线（`main.go`/`vmstorage.go`） | 结论 | 11 |
| 13 | 评审 vmselect 链路（netstorage/prometheus/promql/vmselectapi） | 结论 + 运维提醒 | 10 |
| 14 | 对照设计文档交叉验证语义（`downsampling_storage_design.md`） | 差异清单 | 1–13 |
| 15 | 汇总风险分级（P1/P2）+ 撰写评审结论 | 本文档 | 全部 |

---

## 5. 如何完成全部 code review（方法论）

### 5.1 总原则

1. **先读设计、再读实现**：以 `downsampling_storage_design.md`（语义/存储/兼容/查询）与 `downsampling_storage_implement.md`（实现记录）为「预期」，逐条对照实现是否一致。
2. **自底向上**：数学/分桶 → 编解码 → writer/reader → merger → 生命周期 → 集成 → 协议。上层依赖下层，先确认下层语义正确，上层评审才有意义。
3. **正确性 + 健壮性双线**：每个模块同时检查「正常路径语义正确」和「异常路径（损坏文件、ENOSPC、取消、溢出、竞态）安全」。
4. **不变量清单驱动**：把关键不变量（精度一致、NaN 归一、双分辨率 retention、reader 释放顺序、空间预算过期对齐）列成 checklist，逐模块打勾。

### 5.2 每模块具体检查点

- **数学语义**：`last` 同时间戳取舍、`count` 对 NaN 计 1、sum/count 累加、min/max 合并、NaN/Inf 归一是否与设计一致。
- **编解码**：marshal/unmarshal 对称性；尺寸运行时计算（集群 TSID +8 字节）；长度/范围/排序/一致性/越界校验是否完备；磁盘编号 [1,5] 与查询编号 [0,4] 的映射是否正确。
- **writer/reader**：跨分辨率 index flush、时间戳只写一次、压缩膨胀上界、崩溃一致性（Sync 顺序）、Abort 幂等；reader 单 index 解码、窗口定位、`validateIndex` 的负载连续性与文件尾对齐、raw/摘要还原路径。
- **merger**：窗口分桶（左闭右开）、空区间跳过、跨分辨率去重、`downsampleSourceRowWidth` 行数换算、retention 双分辨率删除判定、精度不一致（同桶报错/跨桶拆块）、停止信号检查。
- **生命周期**：reader 释放先于 part 引用；发布前 `m.Reset()`；`publicationStarted` 保护已发布目标；`MustClose` 顺序。
- **空间预算**：reserve/retire/过期对齐 2s 缓存、溢出安全的 add/multiply/available 减法、ENOSPC→`errDownsampleNoSpace` 包装。
- **协议/查询**：`search_downsampling_v2` 追加 8+1 字节、尾部 9 字节校验、全节点成功语义、缓存禁用。

### 5.3 验证命令

```bash
# 全量单测
go test ./lib/storage -count=1

# 降采样相关 + 竞态检测
go test -race ./lib/storage -run 'TestDownsample' -count=1

# 查看变更范围
git --no-pager diff --stat control/downsampling...HEAD

# 查看单文件差异（评审时逐文件）
git --no-pager diff control/downsampling...HEAD -- lib/storage/downsample_merger.go
```

### 5.4 评审产出模板（每个单元）

```
模块: <文件名>
语义预期: <设计文档对应章节>
正确性: ✅ / 🟡 / 🔴 + 说明
健壮性: ✅ / 🟡 / 🔴 + 说明
关键不变量核对: [精度一致] [NaN归一] [retention] [释放顺序] ...
关注点/风险: P1/P2 分级
结论: 通过 / 有条件通过 / 需修改
```

---

## 6. 评审结论

非测试主干逻辑整体实现质量高，与设计文档语义高度一致；关键不变量（共享精度、NaN 归一、双分辨率 retention、reader 释放顺序、空间预算过期对齐）均正确落地。未发现阻断性问题（P0）。

需关注事项集中在：
1. `count` 的 float64 精度上限与 sum 溢出为 +Inf 的语义边界（P1，确认即可）；
2. 字段查询「全节点成功」语义在滚动升级期间的整体失败风险（P1，发布方案需规划）；
3. 中文注释与上游英文惯例不一致（P2，风格）；
4. `getFilePartsToMerge` 贪心调度的均衡性预期建议补注释/测试（P2）。

建议补充/复核的测试：count 大数精度、sum 溢出、损坏文件（截断 metaindex/偏移越界/精度不一致）、ENOSPC 模拟、取消信号中途停止、跨分辨率 retention 边界。
