# 降采样存储实现说明

本文说明当前模块职责、调用链和资源归属。数据语义与文件格式见[设计文档](downsampling_storage_design.md)，审查重点见[审查说明](downsampling_storage_review.md)，测试入口与覆盖范围见[测试说明](downsampling_storage_test.md)。

## 代码导航

| 代码 | 职责 |
|---|---|
| [downsample.go](lib/storage/downsample.go) | 定义分辨率、特征、`downsampleSample`，实现分桶、同 bucket 样本合并和 NaN 规范化。 |
| [downsample_decoded_resolution_features_block.go](lib/storage/downsample_decoded_resolution_features_block.go) | 保存一个 TSID、一个目标分辨率下的一份已解码时间列和五个特征列，管理可复用缓冲。 |
| [downsample_codec.go](lib/storage/downsample_codec.go) | 定义格式标识与 metaindex 行，校验原生 header、共享时间戳描述和负载范围。 |
| [downsample_reader.go](lib/storage/downsample_reader.go) | 遍历单个源的索引，按分辨率、特征和 TSID 定位，读取原生 `Block` 或多特征解码结果。 |
| [block.go](lib/storage/block.go) | 原生时间戳和值的编解码；完整解码与降采样值列读取共用 `unmarshalValues`。 |
| [downsample_merger.go](lib/storage/downsample_merger.go) | 组织源 reader，收集当前 TSID 的完整时间范围，按 bucket 累加样本并调用 writer。 |
| [downsample_writer.go](lib/storage/downsample_writer.go) | 从 bucket 样本生成单特征原生 `Block`，写入时间戳与 spill，组织最终文件及索引，完成 `Finish` 或 `Abort`。 |
| [spill.go](lib/filestream/spill.go) | `SpillWriter` 管理临时字节流的创建、缓冲写入、一次流式读回及删除。 |
| [downsample_space.go](lib/storage/downsample_space.go) | 估算输出和 spill 共存时的磁盘空间，管理进程内共享的磁盘预留额度。 |
| [downsample_partition.go](lib/storage/downsample_partition.go) | 建立降采样文件作业、核对目标、提交 `parts.json`，并将错误交给调度层处理。 |
| [partition.go](lib/storage/partition.go) | 选择源和目标类型，分派原始数据与降采样归并，处理周期刷盘、最终刷盘和强制归并。 |
| [downsample_open.go](lib/storage/downsample_open.go) | 存储启动预检查，读取活动 part 清单，检查开关与活动文件格式。 |
| [downsample_part.go](lib/storage/downsample_part.go)、[part.go](lib/storage/part.go) | 探测格式，打开并校验降采样 part，持有文件句柄和 metaindex，接入原有 part 引用生命周期。 |
| [downsample_query.go](lib/storage/downsample_query.go)、[part_search.go](lib/storage/part_search.go) | 解析字段选择，定位目标单列 header，将 payload 读取交给 `BlockRef` 和原生 `Block`。 |
| [downsample_search_protocol.go](lib/storage/downsample_search_protocol.go)、[vmselectapi/server.go](lib/vmselectapi/server.go) | 编解码及分派带字段选择的数据查询协议。 |

配置入口位于 [vmstorage/main.go](app/vmstorage/main.go) 和 `storage.OpenOptions.DownsamplingEnabled`。开关默认关闭；启用时要求 dedup 间隔为零。内存 part 使用原始格式，文件目标进入降采样路径。

## 文件归并调用链

`partition.mergePartsWithDownsampling` 先确定目标类型。降采样已启用且目标为文件时，调用 `mergeDownsampleParts`；目标仍为内存 part 时使用原始数据归并路径。

一次文件作业按以下顺序执行：

1. 预留目标与 spill 所需磁盘空间，取得并初始化 `downsampleWriter`。
2. 调用 `downsampleMerger.Merge`。先处理 5m，再处理 1h；每个分辨率建立源 reader 堆，并按 TSID 顺序处理。
3. `collectSources` 遍历当前 TSID 的所有相关 header，汇总最小、最大时间戳，同时把各索引 reader 推进到下一 TSID 或源末尾。
4. `mergeTSID` 按完整时间范围计算 bucket 槽数，分配或复用 `currentTSIDBucketSamples`，清空当前有效槽位。
5. `readSource` 使用独立的 `currentSourceReader` 回读各源中当前 TSID 的 block。每个已解码输入样本按时间戳定位槽位，经保留期限和精度检查后调用 `downsampleSample.Merge`。
6. `partWriter.WriteSamples` 接收当前 TSID 的 bucket 样本，跳过空槽，按有效行数及精度拆块并写出。
7. `Merge` 返回前关闭并归还全部 reader。外层取得源物理行统计，调用 writer 的 `Finish`，再次检查取消信号，再通过 `merger.Reset` 归还剩余解码缓冲。
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
| `currentSourceBlock` | 当前源 block 的多特征解码缓冲；每次 `ReadBlock` 重设内容，`merger.Reset` 归还。 |
| `partWriter` | 借用外层创建的目标 writer；merger 调用 `WriteSamples`，外层负责 `Finish`／`Abort`。 |
| `stopCh` | 当前作业的取消信号；reader 关闭后仍保留，供 `Finish` 后再次检查。 |
| `mergeStats` | 当前作业累计的源物理行统计；跨分辨率保留，`Reset` 清零。 |

`downsampleDecodedResolutionFeaturesBlock` 只组织解码结果。原始数据源读取时展开五特征，但此时尚未分桶；真正的聚合发生在 `downsampleSample.Merge`。writer 直接读取聚合后的样本，不接收另一个五列输出缓冲。

## Reader 与原生 Block

`Init` 绑定源 part 和分辨率；`SetFilter` 重置索引游标并选择相交的 block。同一 part 再次 `Init` 时复用主文件句柄和工作缓冲；换 part 时先释放旧 reader 自有资源。

磁盘读取使用 `os.File.ReadAt`：原始磁盘数据源由 reader 自行打开 timestamps、values、index 三个文件；降采样源借用 part 已有的三个文件句柄。内存源通过现有内存缓冲接口读取。reader 的生存期由调用方持有的 part 引用覆盖。

多特征 `ReadBlock` 的首列通过 `ReadFieldBlock` 完整读取并解码原生 `Block`。后续四列先核对同一时间戳描述，再由 `readNativeValues` 只读取、解码 values，复用首列的已解码时间戳。复用范围仅限当前多特征批次；下一次调用从首列重新读取。查询使用的单列 `ReadFieldBlock` 仍独立完成完整解码。

用于其他特征的 `peers` 持有索引位置，共享降采样 part 的文件。`SetFilter`、切换源或关闭 reader 时归还这些实例。单个 reader 只保留当前 index block 的压缩、解压缓冲，part 的 metaindex 则常驻于 part 对象。

## Writer 与文件资源

`WriteSamples` 借用输入样本，逐块提取一份有效时间戳和一个特征的浮点值，再交给原生 `Block` 编码。输入切片不会被修改或长期持有。每个输出 block 最多 8192 行；精度变化会开始新 block，五列使用一致的行边界。

最终文件由 `filestream.CreateExclusive` 创建，通过 `filestream.Writer` 写入并关闭。当前分辨率的五个 `SpillWriter` 保存各特征的 header 和 values。时间戳不进入 spill；五个原生 Block 的时间戳编码经一致性检查后只写一份。

`SpillWriter` 在首次非空写入时创建私有临时目录和文件。文件权限使用 `0666` 并受 umask 影响，临时目录由 `os.MkdirTemp` 创建。`ReadAll` 刷新缓冲后在同一文件上回到起点，要求回调完整消费字节流；完成或失败时关闭、删除临时资源，不同步临时文件。删除失败保留路径，后续 `Close` 可再次尝试。

切换分辨率或调用 `Finish` 时，writer 按特征依次读回 spill，写入最终 values 和 index。`Finish` 完成剩余索引、metaindex 与 metadata，关闭并同步最终文件，再同步目标目录及其父目录。`Finish` 结束只表示目标文件已完成，活动 part 集合由外层提交清单后更新。

## 发布与失败处理

| 阶段 | 当前处理 |
|---|---|
| reader 或归并校验失败 | 错误终止归并，关闭并归还 reader，由外层调用 `Abort` 清理未发布目标。reader 返回错误不直接设置 `writer.err`。 |
| writer 自身校验、写入或收尾失败 | writer 在内部失败处理中记录错误，禁止继续调用 `WriteSamples` 或 `Finish`，并尝试 `Abort`；外层仍负责清理未发布目标。 |
| `parts.json` rename 前 | 使用副本构造候选 part 集合。临时清单写入、关闭、取消检查或 rename 失败时，不替换活动集合；清理临时清单及目标。 |
| `parts.json` rename 后 | 将目标视为已发布，内存活动集合与清单保持一致。若清单目录同步失败，保留目标及旧源磁盘文件，返回错误，不 Abort 目标。 |
| 发布成功 | 标记源可删除并释放引用，后续回收通过原有 part 生命周期完成。 |
| 后台归并或周期刷盘失败 | 调度识别 `errDownsampleMergeFailed`，结束当前降采样任务。提交前失败的源仍在活动集合中。 |
| 最终刷盘失败 | 对仍在内存的源单独按原始格式落盘，不修改共享降采样开关；即使已无剩余内存源，也再次同步当前清单目录。 |

降采样作业中的普通错误通过返回值处理，清理错误与原错误合并返回。源 part 回收、原始数据最终持久化、启动打开及程序不变量仍使用各自现有的 `Must`／FATAL 处理；持续文件系统故障也可能使清理或最终持久化失败。具体边界见审查说明。

## 查询调用链

HTTP 层在 [prometheus/downsample_query.go](app/vmselect/prometheus/downsample_query.go) 解析 `query.field`。字段经 PromQL 的 `EvalConfig`、`SearchQuery` 和 [netstorage/downsample_query.go](app/vmselect/netstorage/downsample_query.go) 进入 `search_downsampling_v2`，再传至 vmstorage 的 storage 搜索入口。

`partSearch` 为字段查询取得独立 reader，只遍历所选分辨率、特征的 header。`BlockRef` 和原生 `Block` 读取相应 payload。查询结束先释放 reader，再释放 part 引用；查询与归并不共用正在工作的 reader 实例。

指定字段时只读取已落盘的降采样 part；未指定字段时查询原始 part，并跳过降采样 part。字段请求禁用结果缓存，并要求所有目标 storage 节点成功返回。该路径不在查询时将原始数据转换为降采样记录，也不为跨 part 的重叠降采样记录执行专门的 bucket 再聚合。
