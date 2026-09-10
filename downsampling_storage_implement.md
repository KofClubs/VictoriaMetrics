# 降采样存储实现说明

本文说明当前模块职责、调用链和资源归属。数据语义与文件格式见[设计文档](downsampling_storage_design.md)，审查重点见[审查说明](downsampling_storage_review.md)，测试入口与覆盖范围见[测试说明](downsampling_storage_test.md)。

## 代码导航

| 代码 | 职责 |
|---|---|
| [downsample_block.go](lib/storage/downsample_block.go) | 定义分辨率、特征、bucket 样本及多特征解码结果，实现分桶、同 bucket 样本合并、NaN 规范化和解码缓冲复用；定义原始 block 行数与缓冲池容量上限，校验分辨率、header 及其排序，比较共享时间戳描述和租户。 |
| [downsample_metaindex_row.go](lib/storage/downsample_metaindex_row.go) | 定义 metaindex 行类型及编码长度，实现行的 `marshal`／`unmarshal`。 |
| [downsample_reader.go](lib/storage/downsample_reader.go) | 遍历单个源的索引，按分辨率、特征和 TSID 定位，读取原生 `Block` 或多特征解码结果；校验行数与编码的组合及文件负载范围。 |
| [downsample_writer.go](lib/storage/downsample_writer.go) | 从 bucket 样本生成单特征原生 `Block`，组织 spill、最终文件及索引，完成 `Finish`／`Abort`；定义单列编码负载大小上限，负责输出编码大小估算、写入空间检查、空间算术及 `errDownsampleNoSpace`。 |
| [downsample_merger.go](lib/storage/downsample_merger.go) | 组织源 reader，收集当前 TSID 的完整时间范围，按 bucket 累加样本并调用 writer。 |
| [downsample_part.go](lib/storage/downsample_part.go) | 定义格式版本、index／metaindex 文件标识，以及 index、metaindex、metadata 大小上限；探测格式，打开并校验降采样 part，建立文件句柄及 metaindex，根据源 part 统计估算目标大小。 |
| [downsample_partition.go](lib/storage/downsample_partition.go) | 处理启动预检查、分区及活动清单发现、文件选源、降采样作业与清单发布；管理进程级磁盘预算、预留和缓存有效期内的额度释放。 |
| [downsample_query.go](lib/storage/downsample_query.go) | 解析查询字段，编解码带字段选择的查询协议，并在 part 搜索中定位目标单列 header。 |

这八个文件按降采样存储职责组织。底层新增组件包括临时字节流 [spill_writer.go](lib/filestream/spill_writer.go) 和偏移读取 [reader_at.go](lib/filestream/reader_at.go)；最终文件使用原有 `filestream.WriteCloser`。共用实现包括原生编解码 [block.go](lib/storage/block.go)、part 引用生命周期 [part.go](lib/storage/part.go)、归并调度及刷盘 [partition.go](lib/storage/partition.go)、查询遍历 [part_search.go](lib/storage/part_search.go) 和 RPC 分派 [vmselectapi/server.go](lib/vmselectapi/server.go)。

block 中的 `downsampleMaxRawRows` 和 `downsampleMaxPooledRows` 分别限制原始输入 block 的行数和对象池保留的行容量；writer 中的 `downsampleMaxColumnSize` 限制已编码的单列负载。reader 中的 `validateDownsampleRowCodec` 检查行数与编码的组合，`checkDownsampleExtent` 检查负载偏移、大小及文件边界。

reader、writer、merger 的函数按类型声明、面向调用方的生命周期与主操作、内部辅助、对象池排列。方法可见性沿用原始存储类型的职责划分：`Init`、`Close`、`Merge`、`WriteSamples` 等调用入口使用大写名称，内部辅助使用小写名称。reader 的 `readFieldBlock` 和 merger 的 `reset` 仅供各自内部调用；查询使用的 `FieldHeader` 保留大写。heap 和文件接口方法保留接口要求的名称。

配置入口位于 [vmstorage/main.go](app/vmstorage/main.go) 和 `storage.OpenOptions.DownsamplingEnabled`。开关默认关闭；启用时要求 dedup 间隔为零。内存 part 使用原始格式，文件目标进入降采样路径。

## 文件归并调用链

`partition.mergePartsWithDownsampling` 先确定目标类型。降采样已启用且目标为文件时，调用 `mergeDownsampleParts`；目标仍为内存 part 时使用原始数据归并路径。

一次文件作业按以下顺序执行：

1. 根据源 part 统计估算目标与 spill 所需磁盘空间，取得进程级预算预留，再取得并初始化 `downsampleWriter`。
2. 调用 `downsampleMerger.Merge`。先处理 5m，再处理 1h；每个分辨率建立源 reader 堆，并按 TSID 顺序处理。
3. `collectSources` 遍历当前 TSID 的所有相关 header，汇总最小、最大时间戳，同时把各索引 reader 推进到下一 TSID 或源末尾。
4. `mergeTSID` 按完整时间范围计算 bucket 槽数，分配或复用 `currentTSIDBucketSamples`，清空当前有效槽位。
5. `readSource` 使用独立的 `currentSourceReader` 回读各源中当前 TSID 的 block。每个已解码输入样本按时间戳定位槽位，经保留期限和精度检查后调用 `downsampleSample.Merge`。
6. `partWriter.WriteSamples` 接收当前 TSID 的 bucket 样本，跳过空槽，按有效行数及精度拆块并写出。
7. `Merge` 返回前通过 `reset` 归还全部 reader 和多特征解码缓冲，清除本次作业状态。外层使用返回的源物理行统计，调用 writer 的 `Finish`，再检查调用方持有的取消信号。
8. 非空目标由 `openDownsamplePart` 重新打开并校验，然后进入清单提交。空目标由 writer 删除，发布过程只处理应移除的源。

实际降采样数据读取顺序为 `resolution → TSID → 源 part → Block 批次 → feature`。最终 values 与 index 的顺序由 writer 的 spill 转置确定，详见设计文档。当前代码没有将同一源的整个特征列一次性读入内存。

## Merger 工作状态

| 字段 | 保存的状态及释放时机 |
|---|---|
| `currentResolutionReaders` | 当前分辨率取得的全部源 reader，包括初始化失败和已出堆的实例；用于统一归还。 |
| `currentResolutionReaderHeap` | 尚未读完索引的源 reader，按 TSID 排序；每个分辨率重新建立。 |
| `currentTSIDReaders` | 当前 TSID 涉及的源 reader 借用列表；每次收集前清空，归还 reader 前移除引用。 |
| `currentTSIDBucketSamples` | 当前 TSID、当前分辨率的 bucket 样本；有效长度由 header 时间范围决定，容量可复用。 |
| `currentSourceReader` | 第二遍实际数据读取使用的 reader，避免改变堆中已推进的索引位置；`Merge` 返回前归还。 |
| `currentSourceBlock` | 当前源 block 的多特征解码缓冲；每次 `ReadBlock` 重设内容，`Merge` 返回前由 `reset` 归还。 |
| `partWriter` | 借用外层创建的目标 writer；merger 调用 `WriteSamples`，外层负责 `Finish`／`Abort`。 |
| `stopCh` | 当前作业的取消信号；`Merge` 返回前清除引用，外层仍使用传入的信号检查发布前取消。 |
| `mergeStats` | 当前作业累计的源物理行统计；跨分辨率累计，按值返回后由 `reset` 清零。 |

`downsampleDecodedResolutionFeaturesBlock` 只组织解码结果。原始数据源读取时展开五特征，但此时尚未分桶；聚合和 NaN 规范化统一发生在 `downsampleSample.Merge`。该方法在复制或计算后扫描一次五个结果值，同时处理输入标记和运算产生的 NaN。reader 不增加解码后的规范化扫描；writer 直接提取聚合结果到 `currentFeatureValues`，交给原生 decimal／`Block` 编码，不再规范化，也不接收另一个五列输出缓冲。

merger 的清理入口分为两层：`closeResolutionReaders` 只归还当前分辨率的索引 reader，供分辨率切换使用；`reset` 在此基础上归还独立数据 reader 和解码缓冲，并清空作业状态。归还对象前先清除持有引用；归还 merger 对象池时再次检查空状态，不重复归还已经释放的资源。

## Reader 与原生 Block

`Init` 绑定源 part 和分辨率；`SetFilter` 重置索引游标并选择相交的 block。同一 part 再次 `Init` 时复用主文件句柄和工作缓冲；换 part 时先释放旧 reader 自有资源。

磁盘读取使用 `filestream.ReadAtCloser`：原始磁盘数据源由 reader 自行打开 timestamps、values、index 三个文件；降采样源借用 part 已有的三个偏移 reader。实际打开、普通文件检查和偏移读取封装在 `filestream.ReaderAt` 中，不增加文件缓冲或顺序游标。内存源通过现有内存缓冲接口读取。reader 的生存期由调用方持有的 part 引用覆盖。

元数据、metaindex 及格式标识同样通过偏移 reader 读取，读取结束后合并关闭错误。part 直接将该组件交给原有查询与生命周期接口，使用兼容的 MustReadAt／MustClose，无需额外的文件适配器。

多特征 `ReadBlock` 的首列通过内部方法 `readFieldBlock` 完整读取并解码原生 `Block`。后续四列先核对同一时间戳描述，再由 `readNativeValues` 只读取 values，直接调用原有 `encoding.UnmarshalValues` 解码，复用首列的已解码时间戳。`readNativeValues` 负责清除上一列的值、校验时间戳与数值行数一致，并清理编码缓冲和重置读取位置；共享的 `block.go` 保持原有实现。复用范围仅限当前多特征批次；下一次调用从首列重新读取。单列查询通过 `FieldHeader` 定位，再由 `BlockRef` 和原生 `Block` 独立读取、解码 payload。

用于其他特征的 `peers` 持有索引位置，共享降采样 part 的文件。`SetFilter`、切换源或关闭 reader 时归还这些实例。单个 reader 只保留当前 index block 的压缩、解压缓冲，part 的 metaindex 则常驻于 part 对象。

reader 的 `Close` 直接释放自有文件和特征 reader，再清空迭代状态；`SetFilter` 重设过滤状态。两者共用 `closePeers` 归还特征 reader，已归还的引用立即清除。初始化失败通过统一的错误出口执行 `Close`。

## Writer 与文件资源

空间检查按数据和资源归属分工：part 中的 `estimateDownsamplePartSize` 汇总源行数，writer 中的 `estimateDownsampleOutputSize` 估算最终编码与 spill 共存的大小，空间加乘运算在溢出时饱和到最大值。partition 中的 `downsampleSpaceBudget`、`downsampleDiskBudget` 和 `reserveDownsampleSpace` 管理跨任务预留及缓存有效期内的额度释放。writer 通过 `checkDownsampleWriteSpace`、`checkDownsampleFinishSpace` 和 `checkDownsamplePathSpace` 复查物理空间，空间不足通过 `errDownsampleNoSpace` 识别。

`WriteSamples` 借用输入样本，逐块提取一份有效时间戳和一个特征的浮点值，再交给原生 `Block` 编码。输入切片不会被修改或长期持有。每个输出 block 最多 8192 行；精度变化会开始新 block，五列使用一致的行边界。

最终文件字段直接使用 `filestream.WriteCloser`，通过原有 `MustCreate` 创建、`Write` 写入、`MustClose` 关闭。临时清单写入本次作业独占的临时子目录，关闭后 rename 到正式清单路径，再清理子目录。最终文件创建、缓冲刷出、同步和关闭保留共享 Must 语义，目录同步调用原有 `fs.MustSyncPath`；降采样不复制或修改这些实现。

当前分辨率的五个 `SpillWriter` 保存各特征的 header 和 values。时间戳不进入 spill；五个原生 Block 的时间戳编码经一致性检查后只写一份。

`SpillWriter` 使用常量 `spillMaxMemorySize = 16 MiB`，按需分配内存。数据不超过阈值时不创建临时资源；满块之后还有数据写入时，才创建私有目录和文件并写出该满块。后续满块追加到同一个文件，单次超大输入也分段处理，最后一段保留在内存中。五个 spill 当前持有的缓冲容量合计最多 80 MiB；扩容时待 GC 的旧分配、其他缓冲和并发任务另行占用内存。

文件权限使用 `0666` 并受 umask 影响，临时目录由 `os.MkdirTemp` 以 `0700 & ~umask` 创建。`ReadAll` 核验文件前缀长度后回到起点，依次读取文件和内存尾部，要求回调完整消费字节流。完成或失败时释放内存、关闭并删除临时资源，不同步临时文件。删除失败保留路径，后续 `Close` 可再次尝试。满块直接写文件；只有文件读回缓冲使用独立池，归还时解除文件引用。内存尾部不入池。I/O 统计区分逻辑读写与实际文件读写，纯内存路径没有实际文件 I/O。

切换分辨率或调用 `Finish` 时，writer 按特征依次读回 spill，写入最终 values 和 index。`Finish` 完成剩余索引、metaindex 与 metadata，关闭并同步最终文件，再同步目标目录及其父目录。`Finish` 结束只表示目标文件已完成，活动 part 集合由外层提交清单后更新。

writer 的 `reset` 只清理逻辑状态和工作缓冲，文件清理由 `Finish` 或 `Abort` 明确执行。尚有未完成目标或待删除目录时，`Init` 拒绝覆盖当前清理责任。`Abort` 释放文件和 spill 后立即清除对应引用；目录删除失败保留路径，并累积清理错误而不覆盖首次工作错误。再次清理只重试目录删除。

## 发布与失败处理

| 阶段 | 当前处理 |
|---|---|
| reader 或归并校验失败 | 错误终止归并，关闭并归还 reader，由外层调用 `Abort` 清理未发布目标。reader 返回错误不直接设置 `writer.err`。 |
| writer 自身校验、写入或收尾失败 | writer 在内部失败处理中记录错误，禁止继续调用 `WriteSamples` 或 `Finish`，并尝试 `Abort`；外层仍负责清理未发布目标。 |
| `parts.json` rename 前 | 使用副本构造候选 part 集合。临时目录创建、接口返回的写入错误、取消检查或 rename 失败时，不替换活动集合；清理本次临时目录及目标。 |
| `parts.json` rename 后 | 将目标视为已发布。后续返回错误时，内存活动集合与清单保持一致，保留目标及旧源磁盘文件，不 Abort 目标；实际目录同步使用共享 Must 语义。 |
| 发布成功 | 标记源可删除并释放引用，后续回收通过原有 part 生命周期完成。 |
| 后台归并或周期刷盘失败 | 调度识别 `errDownsampleMergeFailed`，结束当前降采样任务。提交前失败的源仍在活动集合中。 |
| 最终刷盘失败 | 对仍在内存的源单独按原始格式落盘，不修改共享降采样开关；即使已无剩余内存源，也再次同步当前清单目录。 |

降采样作业中的普通错误通过返回值处理，清理错误与原错误合并返回。最终文件创建、关闭及目录同步、源 part 回收、原始数据最终持久化、启动打开及程序不变量仍使用各自现有的 `Must`／FATAL 处理；持续文件系统故障也可能使清理或最终持久化失败。具体边界见审查说明。

降采样专属错误和日志使用英文，消息以 `[downsampling]` 开头。底层错误通过 `%w` 保留原因，多个独立错误通过 `errors.Join` 汇总，取消和空间不足仍可通过 `errors.Is` 识别。

## 查询调用链

HTTP 层在 [prometheus/downsample_query.go](app/vmselect/prometheus/downsample_query.go) 解析 `query.field`。字段经 PromQL 的 `EvalConfig`、`SearchQuery` 和 [netstorage/downsample_query.go](app/vmselect/netstorage/downsample_query.go) 进入 `search_downsampling_v2`，再传至 vmstorage 的 storage 搜索入口。

`partSearch` 为字段查询取得独立 reader，只遍历所选分辨率、特征的 header。`BlockRef` 和原生 `Block` 读取相应 payload。查询结束先释放 reader，再释放 part 引用；查询与归并不共用正在工作的 reader 实例。

指定字段时只读取已落盘的降采样 part；未指定字段时查询原始 part，并跳过降采样 part。字段请求禁用结果缓存，并要求所有目标 storage 节点成功返回。该路径不在查询时将原始数据转换为降采样记录，也不为跨 part 的重叠降采样记录执行专门的 bucket 再聚合。
