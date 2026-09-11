# 降采样存储实现说明

本文说明当前模块职责、调用链和资源归属。数据语义与文件格式见[设计文档](downsampling_storage_design.md)，审查重点见[审查说明](downsampling_storage_review.md)，测试入口与覆盖范围见[测试说明](downsampling_storage_test.md)。

## 代码导航

| 代码 | 职责 |
|---|---|
| [downsample_block.go](lib/storage/downsample_block.go) | 定义分辨率、特征、bucket 样本及多特征解码结果，实现分桶、同 bucket 样本合并、NaN 规范化和解码缓冲复用；定义原始 block 行数与缓冲池容量上限，校验分辨率、header 及其排序，比较共享时间戳描述和租户。 |
| [downsample_metaindex_row.go](lib/storage/downsample_metaindex_row.go) | 仅定义 metaindex 行类型及编码长度，实现行的 `marshal`／`unmarshal`。 |
| [downsample_reader.go](lib/storage/downsample_reader.go) | 仅供归并使用，统一读取原始内存 part、原始磁盘 part 和降采样磁盘 part；按分辨率和 TSID 遍历索引，将原生 `Block` 解码为五特征结果。 |
| [downsample_writer.go](lib/storage/downsample_writer.go) | 从 bucket 样本生成单特征原生 `Block`，组织 spill、最终文件及索引，以 metadata 标记写入完成，完成 `Finish`／`Abort`；定义单列编码负载大小上限，负责输出编码大小估算、写入空间检查、空间算术及 `errDownsampleNoSpace`。 |
| [downsample_merger.go](lib/storage/downsample_merger.go) | 组织源 reader，收集当前 TSID 的完整时间范围，按 bucket 累加样本并调用 writer。 |
| [downsample_part.go](lib/storage/downsample_part.go) | 定义格式版本、index／metaindex 文件标识，以及 index、metaindex、metadata 大小上限；仅依据 metadata 探测格式，打开时校验实际数据文件，建立文件句柄及 metaindex，按 metaindex 约束解码原生 block header，根据源 part 统计估算目标大小。 |
| [downsample_partition.go](lib/storage/downsample_partition.go) | 处理启动预检查、分区及活动清单发现、文件选源、降采样作业与清单发布；管理进程级磁盘预算、预留和缓存有效期内的额度释放。 |
| [downsample_query.go](lib/storage/downsample_query.go) | 解析分辨率与特征，编解码降采样查询协议；为原有 part 搜索读取选定列的 index，后续筛选及 BlockRef 读取复用原有逻辑。 |

这八个文件按降采样存储职责组织。底层新增组件包括临时字节流 [spill_writer.go](lib/filestream/spill_writer.go) 和供归并及打开校验使用的偏移读取 [reader_at.go](lib/filestream/reader_at.go)；最终文件使用原有 `filestream.WriteCloser`。共用实现包括原生编解码 [block.go](lib/storage/block.go)、part 引用生命周期 [part.go](lib/storage/part.go)、归并调度及刷盘 [partition.go](lib/storage/partition.go)、查询遍历 [part_search.go](lib/storage/part_search.go)、查询文件读取 [fs/reader_at.go](lib/fs/reader_at.go) 和 RPC 分派 [vmselectapi/server.go](lib/vmselectapi/server.go)。

block 中的 `downsampleMaxRawRows` 和 `downsampleMaxPooledRows` 分别限制原始输入 block 的行数和对象池保留的行容量；writer 中的 `downsampleMaxColumnSize` 限制已编码的单列负载。`prepareBlockPayload` 检查行数与编码的组合，并限制压缩负载的展开大小，`checkDownsampleExtent` 检查负载偏移、大小及文件边界。

reader、writer、merger 的函数按类型声明、面向调用方的生命周期与主操作、内部辅助、对象池排列。方法可见性沿用原始存储类型的职责划分：`Init`、`NextHeader`、`ReadBlock`、`Close`、`Merge`、`WriteSamples` 等调用入口使用大写名称，内部辅助使用小写名称。reader 的原生 block 读取辅助及 merger 的 `reset` 仅供内部调用。heap 和文件接口方法保留接口要求的名称。

配置入口位于 [vmstorage/main.go](app/vmstorage/main.go) 和 `storage.OpenOptions.DownsamplingEnabled`。开关默认关闭；启用时要求 dedup 间隔为零。内存 part 使用原始格式，文件目标进入降采样路径。

## 文件归并调用链

`partition.mergePartsWithDownsampling` 先确定目标类型。降采样已启用且目标为文件时，调用 `mergeDownsampleParts`；目标仍为内存 part 时使用原始数据归并路径。

一次文件作业按以下顺序执行：

1. 根据源 part 统计估算目标与 spill 所需磁盘空间，取得进程级预算预留，再取得并初始化 `downsampleWriter`。
2. 调用 `downsampleMerger.Merge`。先处理 5m，再处理 1h；每个分辨率建立源 reader 堆，并按 TSID 顺序处理。
3. `collectSources` 顺序遍历当前 TSID 的所有相关首列 header，汇总最小、最大时间戳，按值追加到各源 reader 的 `currentTSIDBlockHeaders`，同时把首列索引游标推进到下一 TSID 或源末尾。已删除 TSID 只累计统计，不保存 header 或读取 payload。
4. `mergeTSID` 按完整时间范围计算 bucket 槽数，分配或复用 `currentTSIDBucketSamples`，清空当前有效槽位。
5. `readSource` 遍历已保存的 header，调用同一源 reader 的 `ReadBlock(decoded, header)` 直接读取 payload；其他四列索引顺序对齐，不改变堆所使用的首列 header，也不重复读取首列索引。每个已解码输入样本按时间戳定位槽位，经保留期限和精度检查后调用 `downsampleSample.Merge`。当前源消费完成或读取失败时清空其 header 缓存长度。
6. `partWriter.WriteSamples` 接收当前 TSID 的 bucket 样本，跳过空槽，按有效行数及精度拆块并写出。
7. `Merge` 返回前通过 `reset` 归还全部 reader 和多特征解码缓冲，清除本次作业状态。外层使用返回的源物理行统计，调用 writer 的 `Finish`，再检查调用方持有的取消信号。
8. 非空目标由 `openDownsamplePart` 重新打开并校验，然后进入清单提交。空目标由 writer 删除，发布过程只处理应移除的源。

实际降采样数据读取顺序为 `resolution → TSID → 源 part → Block 批次 → feature`。归并过程中，每个源在当前分辨率内的各列索引只向前读取；原始源仍按 5m、1h 分别读取一次索引和数据。最终 values 与 index 的顺序由 writer 的 spill 转置确定，详见设计文档。值列读取在五个特征区段之间按偏移切换，不是整个 values 文件的连续顺序扫描，也不会将整个特征列一次性读入内存。

## Merger 工作状态

| 字段 | 保存的状态及释放时机 |
|---|---|
| `currentResolutionReaders` | 当前分辨率取得的全部源 reader，包括初始化失败和已出堆的实例；用于统一归还。 |
| `currentResolutionReaderHeap` | 尚未读完索引的源 reader，按 TSID 排序；每个分辨率重新建立。 |
| `currentTSIDReaders` | 当前 TSID 涉及的源 reader 借用列表；每次收集前清空，归还 reader 前移除引用。 |
| `currentTSIDBucketSamples` | 当前 TSID、当前分辨率的 bucket 样本；有效长度由 header 时间范围决定，容量可复用。 |
| `currentSourceBlock` | 当前源 block 的多特征解码缓冲；每次 `ReadBlock` 重设内容，`Merge` 返回前由 `reset` 归还。 |
| `partWriter` | 借用外层创建的目标 writer；merger 调用 `WriteSamples`，外层负责 `Finish`／`Abort`。 |
| `stopCh` | 当前作业的取消信号；`Merge` 返回前清除引用，外层仍使用传入的信号检查发布前取消。 |
| `mergeStats` | 当前作业累计的源物理行统计；跨分辨率累计，按值返回后由 `reset` 清零。 |

`downsampleDecodedResolutionFeaturesBlock` 只组织解码结果。原始数据源读取时展开五特征，但此时尚未分桶；聚合和 NaN 规范化统一发生在 `downsampleSample.Merge`。该方法在复制或计算后扫描一次五个结果值，同时处理输入标记和运算产生的 NaN。reader 不增加解码后的规范化扫描；writer 直接提取聚合结果到 `currentFeatureValues`，交给原生 decimal／`Block` 编码，不再规范化，也不接收另一个五列输出缓冲。

merger 的清理入口分为两层：`closeResolutionReaders` 归还当前分辨率的全部源 reader，供分辨率切换使用；每个源的索引与 payload 共用同一个 reader。`reset` 在此基础上归还解码缓冲，并清空作业状态。归还对象前先清除持有引用；归还 merger 对象池时再次检查空状态，不重复归还已经释放的资源。

## Reader 与原生 Block

`downsampleReader` 仅由归并任务持有，统一访问原始内存、原始磁盘和降采样磁盘数据源；part 打开校验及查询不使用该 reader。`Init(p, resolution)` 顺序扫描内存中的 metaindex，为该分辨率建立完整索引区间，不执行二分定位。`NextHeader` 只推进 last 或原始索引；merger 在 TSID 变化时结束当前 TSID 的 header 收集，再按保存的 header 读取数据。reader 不提供 TSID 定位或时间窗口。同一 part 再次 `Init` 时复用文件句柄和工作缓冲；换 part 时先释放旧 reader 自有资源。

磁盘读取使用 `filestream.ReadAtCloser`：原始磁盘和降采样磁盘源均由 reader 自行打开 timestamps、values、index 三个文件，不借用 part 的查询读取对象。实际打开、普通文件检查和偏移读取封装在 `filestream.ReaderAt` 中，不增加文件缓冲或顺序游标。内存源通过现有内存缓冲接口读取。reader 的生存期由调用方持有的 part 引用覆盖。

`detectDownsampleFormat` 只通过偏移 reader 读取 metadata 并校验 JSON、版本及必需字段，不读取 metaindex 或检查 `.bin` 文件。`openDownsamplePart` 使用独立临时偏移 reader 校验实际文件的标识、文件边界及索引，返回前关闭全部校验文件并合并关闭错误。通过校验后，part 的 `timestampsFile`、`valuesFile`、`indexFile` 统一使用原有 `fs.ReaderAt`，供查询读取并由 `part.MustClose` 释放；part 不保存额外的降采样文件句柄。

多特征 `ReadBlock(decoded, header)` 使用调用方按值保存的首列 header，直接调用内部方法 `readNativeBlock`，完整读取并解码原生 `Block`，不修改首列索引游标或当前 `Header`。后续四列只向前推进到对应 header，核对同一时间戳描述，再由 `readNativeValues` 只读取 values，直接调用原有 `encoding.UnmarshalValues` 解码，复用首列的已解码时间戳。`readNativeValues` 负责清除上一列的值、校验时间戳与数值行数一致，并清理编码缓冲和重置读取位置；共享的 `block.go` 保持原有实现。复用范围仅限当前多特征批次；下一次调用从首列重新读取。单列查询由 `partSearch` 直接定位原生 header，交给 `BlockRef` 和原生 `Block` 独立读取、解码 payload。

reader 的 `currentResolutionFeatureIndexes` 内嵌五个 `downsampleIndexCursor` 值，分别推进 last、sum、count、min、max 的 header，并在 `ReadBlock` 时核对对应关系；原始源只使用 last 对应的游标。游标仅保存索引位置、header 和当前 index block 的缓冲，不持有文件或源 part，不单独分配 reader，也不进入独立对象池。全部索引与 payload 由外层 reader 访问，part 的 metaindex 常驻于 part 对象。源与分辨率分别由 `currentSourcePart`、`currentResolution` 标识；`currentFeatureBlock` 复用原生解码状态，`currentBlockDecompressedPayload` 仅用于限长解压。跨 index 校验直接使用已解码索引的末尾 header，结束偏移按需计算，不另存重复的 header 或有效性标志。

`currentTSIDBlockHeaders` 独立保存当前 TSID 的首列 header 值，不引用会被下一 index 解码覆盖的切片。它只缓存 header，容量随单个 TSID 的 block 数增长，消费后清空长度，当前分辨率内可复用。`Init` 重设索引遍历和 header 缓存长度；`Close` 释放整个 header 缓存，关闭全部自有文件并合并关闭错误，清除源引用和各游标状态，并按既有限额保留其他工作缓冲。调用文件关闭前先解除引用，重复关闭不会再次释放；初始化失败通过统一的错误出口执行 `Close`。

## Writer 与文件资源

空间检查按数据和资源归属分工：part 中的 `estimateDownsamplePartSize` 汇总源行数，writer 中的 `estimateDownsampleOutputSize` 估算最终编码与 spill 共存的大小，空间加乘运算在溢出时饱和到最大值。partition 中的 `downsampleSpaceBudget`、`downsampleDiskBudget` 和 `reserveDownsampleSpace` 管理跨任务预留及缓存有效期内的额度释放。writer 通过 `checkDownsampleWriteSpace`、`checkDownsampleFinishSpace` 和 `checkDownsamplePathSpace` 复查物理空间，空间不足通过 `errDownsampleNoSpace` 识别。

`WriteSamples` 借用输入样本，逐块提取一份有效时间戳和一个特征的浮点值，再交给原生 `Block` 编码。输入切片不会被修改或长期持有。每个输出 block 最多 8192 行；精度变化会开始新 block，五列使用一致的行边界。

最终文件字段直接使用 `filestream.WriteCloser`，通过原有 `MustCreate` 创建、`Write` 写入、`MustClose` 关闭。临时清单写入本次作业独占的临时子目录，关闭后 rename 到正式清单路径，再清理子目录。最终文件创建、缓冲刷出、同步和关闭保留共享 Must 语义，目录同步调用原有 `fs.MustSyncPath`；降采样不复制或修改这些实现。

`currentResolutionFeatureSpills` 保存当前分辨率各特征的 header 和 values。writer 逐列复用一个 `currentFeatureBlock`，每列编码并校验后立即写入对应 spill。首列编码时间戳独立复制到 `currentBlockTimestampsData`，用于核对后四列的编码结果；向最终文件只写一份，时间戳不进入 spill。`partHeader` 汇总整次写入统计，`previousBlockHeader` 保留上一批次编码前的时间边界，`currentIndexMetaindexRow` 与 `currentIndexBlockData` 共同描述尚未输出的 index block。

`SpillWriter` 的内存阈值由 `downsampling.spillMaxMemorySize` 配置，默认 16 MiB，缓冲按需分配。数据不超过阈值时不创建临时文件；满块之后还有数据写入时，才在目标 part 目录中创建 `.spill-<feature>` 并写出该满块。后续满块追加到同一个文件，单次超大输入也分段处理，最后一段保留在内存中。默认配置下，五个 spill 当前持有的缓冲容量合计最多 80 MiB；扩容时待 GC 的旧分配、其他缓冲和并发任务另行占用内存。

文件权限使用 `0666` 并受 umask 影响。`Read` 核验文件前缀长度后回到起点，依次读取文件和内存尾部，要求回调完整消费字节流；可重复读取，不改变已存字节或释放资源。调用方在成功或失败后显式执行 `Close`，释放内存、关闭并删除临时文件，不同步临时文件。写入失败在内部封闭 spill 并尝试清理；删除失败保留路径，后续 `Close` 可再次尝试。满块直接写文件；只有文件读回缓冲使用独立池，归还时解除文件引用。内存尾部不入池。I/O 统计区分逻辑读写与实际文件读写，纯内存路径没有实际文件 I/O。

切换分辨率或调用 `Finish` 时，writer 按特征依次读回 spill，写入最终 values 和 index。非空目标的 `Finish` 先读回并清理全部 spill，完成四个 `.bin` 文件，关闭并同步这四个文件，再同步目标 part 目录。随后通过原有 `MustCreate` 直接创建 `metadata.json` 并写入完整 JSON，通过 `MustClose` 同步并关闭文件，最后同步 part 目录和父目录。

合法且完整的 metadata 通过 `FormatVersion` 区分降采样格式，作为 part 写入完成标志；原始 metadata 不含该版本字段。`Finish` 不修改活动集合，外层仍须重新打开并校验目标、检查取消，再提交 `parts.json`。格式检测必须解析并校验完整 metadata，不能只检查文件是否存在；活动集合的发布仍以 `parts.json` 的原子重命名为准。

writer 的 `reset` 只清理逻辑状态和工作缓冲，文件清理由 `Finish` 或 `Abort` 明确执行。尚有未完成目标或待删除目录时，`Init` 拒绝覆盖当前清理责任。消费 spill 后先清除 writer 中的引用，再统一关闭并合并读取、关闭错误；残留文件由目标目录的清理兜底。`Abort` 释放文件和 spill 时清除对应引用；目录删除失败保留路径，并累积清理错误而不覆盖首次工作错误。再次清理只重试目录删除。

## 发布与失败处理

| 阶段 | 当前处理 |
|---|---|
| reader 或归并校验失败 | 错误终止归并，关闭并归还 reader，由外层调用 `Abort` 清理未发布目标。reader 返回错误不直接设置 `writer.err`。 |
| writer 自身校验、写入或收尾失败 | writer 在内部失败处理中记录错误，禁止继续调用 `WriteSamples` 或 `Finish`，并尝试 `Abort`；清理包含四个数据文件、spill 及 metadata，外层仍负责清理未发布目标。 |
| `parts.json` rename 前 | 使用副本构造候选 part 集合。临时目录创建、接口返回的写入错误、取消检查或 rename 失败时，不替换活动集合；清理本次临时目录及目标。 |
| `parts.json` rename 后 | 将目标视为已发布。后续返回错误时，内存活动集合与清单保持一致，保留目标及旧源磁盘文件，不 Abort 目标；实际目录同步使用共享 Must 语义。 |
| 发布成功 | 标记源可删除并释放引用；原始和降采样磁盘源均通过 `decRef → part.MustClose → fs.MustRemoveDir` 回收，未结束的查询引用延迟关闭和删除。 |
| 后台归并或周期刷盘失败 | 调度识别 `errDownsampleMergeFailed`，结束当前降采样任务。提交前失败的源仍在活动集合中。 |
| 最终刷盘失败 | 对仍在内存的源单独按原始格式落盘，不修改共享降采样开关；即使已无剩余内存源，也再次同步当前清单目录。 |

降采样作业中的普通错误通过返回值处理，清理错误与原错误合并返回。最终文件创建、关闭及目录同步、源 part 回收、原始数据最终持久化、启动打开及程序不变量仍使用各自现有的 `Must`／FATAL 处理；持续文件系统故障也可能使清理或最终持久化失败。具体边界见审查说明。

降采样专属错误和日志使用英文，消息以 `[downsampling]` 开头。底层错误通过 `%w` 保留原因，多个独立错误通过 `errors.Join` 汇总，取消和空间不足仍可通过 `errors.Is` 识别。

## 查询调用链

HTTP 层在 [prometheus/downsample_query.go](app/vmselect/prometheus/downsample_query.go) 分别解析 `query.resolution` 和 `query.feature`，两者必须同时提供。结果为 `DownsampleQuery{ResolutionMs, Feature}`；未提供两个参数时为 `nil`。重复参数、空值、不支持的分辨率或特征，以及旧参数 `query.field` 均返回错误。

该选择经 PromQL 的 `EvalConfig`、`SearchQuery`，在 [netstorage.go](app/vmselect/netstorage/netstorage.go) 的 `execSearchQueryRequest` 内直接选择 `search_downsampling_v2` 并编码查询负载，再传至 vmstorage。未指定降采样时继续使用 `search_v7`。存储层沿原有 `Search.Init → tableSearch.Init → partitionSearch.Init → partSearch.Init` 逐层透传同一个 `*DownsampleQuery`；选择参数追加在各层原有参数表末尾。

`partSearch` 根据分辨率和特征取得对应 metaindex 范围，直接读取目标 index 并使用原有 `ibCache`。缓存未命中时调用 `p.indexFile.MustReadAt`；index 解码后仍是原生 `blockHeader`，后续 TSID／时间范围筛选、`BlockRef` 构造以及 `Block` 的 payload 读取与解码复用原有逻辑。三个查询文件读取对象均为 `fs.ReaderAt`，沿用 mmap、页驻留状态检查、系统调用回退、读取统计以及 `fs.disableMmap`、`fs.disableMincore` 配置。查询文件 I/O 与原始查询使用相同的 `Must` 语义。

查询持有原有 part 引用，不创建 `downsampleReader`；后者的全部实例仅在归并任务内使用。归并独占自身的工作缓冲与文件读取对象，原有 `isInMerge` 防止重复选取正在合并的源。发布目标后，旧 part 退出活动集合，但仍由尚未结束的查询引用保持存活，最后一个引用释放才关闭文件及删除可丢弃的目录。文件读取期间不新增查询与归并之间的共享锁，两条路径仍会竞争磁盘带宽、CPU 和系统页缓存。

指定分辨率和特征时只读取已落盘的降采样 part；未指定时查询原始 part，并跳过降采样 part。降采样请求禁用未区分分辨率和特征的结果缓存，保留原有索引缓存。该路径不在查询时将原始数据转换为降采样记录，也不为跨 part 的重叠降采样记录执行专门的 bucket 再聚合。

`processBlocksInternal` 统一调用原有 `collectResults(partialSearchResults, ...)`，降采样与原始请求共用部分结果、副本容错、跳过慢副本及错误分类策略。结果是否完整由原有配置和节点响应决定；HTTP 与 RPC 的通用错误、超时及写出流程沿用原有处理，不保存降采样错误前缀状态。降采样参数和协议负载仍由各自的解析与校验入口检查。
