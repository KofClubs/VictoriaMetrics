# 降采样存储实现说明

本文说明当前模块职责、调用链和资源归属。数据语义及文件格式见[设计文档](downsampling_storage_design.md)，审查重点见[审查说明](downsampling_storage_review.md)，测试入口见[测试说明](downsampling_storage_test.md)。

## 代码导航

| 文件 | 职责 |
|---|---|
| [downsample_config.go](lib/storage/downsample_config.go) | 严格解析 JSON，保存不可变配置快照，提供租户分辨率访问、运行时更新，以及基础分辨率的持久化和启动检查。 |
| [downsample_block.go](lib/storage/downsample_block.go) | 定义五种特征、bucket 样本和多特征解码缓冲；实现分桶、样本合并、NaN 规范化、header 校验，以及复用原生 Block 的无损时间戳编码。 |
| [downsample_metaindex_row.go](lib/storage/downsample_metaindex_row.go) | 定义 metaindex 行及其编解码。 |
| [downsample_reader.go](lib/storage/downsample_reader.go) | 仅供归并使用，统一读取原始内存、原始磁盘和降采样磁盘 part，顺序扫描指定分辨率的索引并解码共享时间戳及五特征。 |
| [downsample_writer.go](lib/storage/downsample_writer.go) | 接收 bucket 样本，逐列复用原生 Block；管理各分辨率 spill、文件排序、索引、空间检查和 Finish／Abort。 |
| [downsample_merger.go](lib/storage/downsample_merger.go) | 建立一次基础 reader 堆，按 TSID 聚合基础样本，再推导本租户的额外分辨率；管理任务状态和源物理行统计。 |
| [downsample_part.go](lib/storage/downsample_part.go) | 定义格式、metadata 和大小限制，探测格式并校验全部索引与文件范围，建立原有查询读取对象，估算输出空间。 |
| [downsample_partition.go](lib/storage/downsample_partition.go) | 启动检查、活动清单发现、选源、作业、发布、保留期限及进程级磁盘预算。 |
| [downsample_query.go](lib/storage/downsample_query.go) | 解析选择参数、编解码 RPC，按租户和 part 选择最大可用整除分辨率，跨 part／partition 聚合目标 bucket。 |

这九个文件按存储职责组织。底层专属组件为 [spill_writer.go](lib/filestream/spill_writer.go) 和 [reader_at.go](lib/filestream/reader_at.go)；最终文件直接使用原有 `filestream.WriteCloser`。共享 [block.go](lib/storage/block.go)、[fs/reader_at.go](lib/fs/reader_at.go)、part 引用计数与回收规则保持原有实现。

reader、writer、merger 的声明顺序为类型、调用入口、内部辅助、对象池。`Init`、`NextHeader`、`ReadBlock`、`Close`、`Merge`、`WriteSamples` 为调用入口；原生读取辅助、清理辅助和 `reset` 使用小写名称。heap 与文件接口保留接口要求的方法名。

## 配置与快照

[vmstorage/main.go](app/vmstorage/main.go) 解析 `-storage.downsampling.enabled` 和 `-storage.downsampling.config`，传入 `storage.OpenOptions`。默认关闭；启用时要求 `dedup.minScrapeInterval=0`，未提供 JSON 时默认仅含 5m 基础分辨率。

`DownsamplingConfig` 保存基础分辨率和按租户排序的额外分辨率；额外分辨率必须是更大的整数倍。Parse 同时检查输入 JSON 和最坏 metadata 编码的 64 KiB 边界，后者包含规范化后的完整配置及最大长度的数值字段，不另设租户或分辨率数量上限。公开访问器返回副本，存储内部只读借用快照。`Storage.UpdateDownsamplingConfig` 校验基础分辨率不变，然后原子替换配置；`GET／PUT /internal/downsampling/config` 是对应的运行时接口。更新只影响新任务，重启后的租户配置来自启动 JSON。基础分辨率独立持久化到 `metadata/downsampling.json`，启动时同时检查该文件和活动降采样 part。

`mergeDownsampleParts` 开始时取得一次配置指针，空间预算、writer 和 merger 共用该快照。writer 的 `partTenants` 记录实际写入租户；Finish 将配置缩小到这些租户，写入 metadata；实际分辨率由 metaindex 的有序区段确定。读取旧 part 及查询时使用其自身配置，不能套用最新租户配置解释已有列。

## 文件归并调用链

`partition.mergePartsWithDownsampling` 对文件目标进入 `mergeDownsampleParts`，内存目标继续原始归并。文件作业依次执行：

1. 取得配置快照，估算最终文件与 spill 的空间，预留进程级磁盘预算，初始化 writer。
2. `initSources` 为每个源初始化一个基础 reader，校验降采样源基础分辨率一致，再建立一次 TSID 堆。
3. `collectSources` 收集当前 TSID 的全部基础首列 header，按值缓存到各 reader，并汇总时间范围及输入行数；首列游标前进到下一 TSID 或 EOF。删除的 TSID 只统计，不缓存 header 或读取 payload。
4. `mergeTSID` 根据完整时间范围与源行数选择密集槽或稀疏 bucket 映射，随后 `readSource` 按缓存 header 解码一次基础输入，累加到 `currentTSIDBaseBucketSamples`。旧额外列不读取。
5. 基础结果按 bucket 排序后直接交给 `WriteSamples`；`aggregateSamples` 从这份完整结果逐次推导本租户的额外分辨率，复用一份输出缓冲。所有分辨率完成后才处理下一 TSID。
6. `Merge` 返回前释放 reader 和解码缓冲，按值返回基础输入物理行统计；外层以同一个取消信号调用 `Finish(stopCh)`，再次检查取消，重新打开并验证非空目标，最后提交清单。

原始和降采样输入各读一次基础索引与数据。值列仍按逻辑 block 在五个特征区段间发起偏移读取；时间戳在一次多特征解码中只读、解码一次。最终列顺序由 writer 的 spill 读回确定。

## Merger 工作状态

| 字段 | 内容与生命周期 |
|---|---|
| `baseResolutionReaders` | 本次作业取得的全部源 reader，包括出堆或初始化失败的实例，统一释放。 |
| `baseResolutionReaderHeap` | 尚未读完基础索引的 reader，按完整 TSID 排序，整次归并只建一次。 |
| `currentTSIDReaders` | 当前 TSID 的源 reader 借用列表，每次收集前清空。 |
| `currentTSIDBaseBucketSamples` | 当前 TSID 的基础聚合结果；密集时间范围按槽排列，稀疏范围只保留实际 bucket。 |
| `currentTSIDBaseBucketIndexes` | 稀疏范围中 bucket 到实际槽号的映射，排序后释放。 |
| `currentTSIDResolutionSamples` | 当前额外分辨率的非空 bucket，跨额外分辨率复用。 |
| `currentSourceBlock` | 当前输入 block 的共享时间列和五个数值列，任务结束归还。 |
| `mergeMemoryLimiter`、`mergeMemoryBytes` | 本次任务使用的进程预算及实际持有额度；非阻塞申请，返回前清理并归还。 |
| `currentTSIDBaseSamplesMemory`、`currentTSIDResolutionSamplesMemory`、`currentTSIDBucketIndexesMemory` | 分别记录基础／派生样本容量与稀疏映射预留，扩容或释放时对应更新，防止遗漏和重复归还。 |
| `partWriter`、`stopCh` | 外层 writer 和取消信号的借用引用，返回前清除。 |
| `mergeStats` | 基础输入物理行统计；原始输入每行计一次，降采样基础输入按五特征换算，旧额外列不重复计数。 |

`downsampleSample.Merge` 统一合并数值并规范化 NaN；reader 和 writer 不重复规范化。基础贡献的保留边界由当前配置与源 part 配置共同计算，避免粗粒度 bucket 尚有效时丢失其基础前缀。所有额外结果完整聚合这份基础输入，不再单独裁剪保留前缀，确保查询选择任一可用源分辨率时贡献一致。降采样启用时，内存原始归并透传 `downsampleRetentionStart` 计算的保守边界；所有 part 清理由 `partExpired` 判断，整月清理由 `retentionExpired` 核对当前配置和活动 part 的旧配置。原始 merger 和关闭降采样时的清理规则保持原有实现。

`reset` 通过 `closeBaseReaders` 清除借用引用、归还全部源 reader，然后归还解码对象并清空作业状态。基础及派生样本、稀疏映射和 header 缓存解除引用后归还内存额度，不把未计费的大数组留在 merger 池中。reader 指针切片清空引用后仍保留底层容量；关闭或归还前先移除持有引用，后续清理不重复释放。

## Reader 与原生 Block

`downsampleReader` 仅由归并持有，查询及 part 打开校验不使用它。`Init(p, resolution)` 顺序扫描 metaindex，设置各特征的完整索引范围。`NextHeader` 只推进 last 或原始索引，`Header` 在下一次推进后失效；merger 必须按值保存。`ReadBlock(decoded, header)` 按保存的 header 读取，即使首列已推进至下一 TSID 或 EOF，也不改变堆使用的 header。

磁盘源由 reader 自行打开并关闭 timestamps、values、index 三个 `filestream.ReadAtCloser`；内存源读取已有缓冲。reader 不借用 part 的查询对象，也不持有裸文件。调用方的 part 引用覆盖 reader 生命周期。

首列通过原生 `Block.UnmarshalData` 解码；后四列顺序对齐 header、核对共享时间戳描述，再由 `readNativeValues` 仅读取并解码数值。五个内嵌 `downsampleIndexCursor` 只保存索引位置与工作缓冲，不持有独立文件或进入对象池。`currentTSIDBlockHeaders` 消费后清空长度，Close 时释放容量；全部文件关闭错误合并返回，重复 Close 不再次关闭。

`NextHeader` 推进及其余特征向前对齐时检查当前任务的取消信号，避免跳过长索引区段时一直处理已取消任务。新目标的打开校验接收同一个 stopCh，在入口、metaindex 扫描、索引组遍历和基础列对齐过程中检查；取消会关闭已经打开的校验句柄并阻止发布。共享文件 I/O 或同步操作本身不能在调用中途由该信号打断。

part 打开校验使用独立临时偏移 reader，校验并关闭后，查询统一使用原有 `timestampsFile`、`valuesFile`、`indexFile` 三个 `fs.ReaderAt`，由 `part.MustClose` 管理。

## Writer 与文件资源

`Init(path, compressLevel, config)` 接收当前任务的配置快照。`WriteSamples` 只读借用输入，拒绝租户未配置的分辨率，跳过空槽，按精度变化或 8192 个有效样本拆块。每批提取一份时间戳，逐特征复用浮点缓冲、decimal 整数缓冲和同一个 `currentFeatureBlock`。数值保留源精度，时间戳经 `marshalDownsampleBlock` 无损编码，五列编码时间戳逐字节及 header 描述一致。

`partResolutionSpills` 按实际分辨率保存五个 spill、相对时间列长度和上一批次 header；各分辨率独立校验 TSID／时间顺序。last spill 保存 header、唯一时间列和 last 值，其余四个 spill 保存 header 和各自值列。文件名为 `.spill-<resolution毫秒数>-<feature>`，可以交错接收不同分辨率而不影响最终排序。

SpillWriter 按需增长内存，单个阈值默认 16 MiB，由 `-downsampling.spillMaxMemorySize` 配置，必须大于零，并在首次非空 Write 时固定。所有实例共享 `min(256 MiB, memory.Allowed()/10)` 的进程预算；扩容先预留完整新容量，旧容量在拷贝完成并解除引用后归还。预算不足时写出已有尾部、释放缓冲，再直接写文件，不等待其他任务。预算足够且数据未超过单个阈值时不创建文件；满块追加到同一个文件。Read 依次消费文件前缀和内存尾部，最终由 Close 释放内存额度、关闭及删除文件，临时文件不执行 fsync。

`Finish(stopCh)` 对实际分辨率排序，再按五特征读回 spill；入口、各 feature 和各条 header 前检查取消，四个 bin 同步后、metadata 创建前再次检查。取消触发统一 Abort，保留取消错误，尚存句柄各关闭一次并删除目标目录；无需取消的调用使用 `Finish(nil)`。last 时间列写入最终 timestamps，五列相对时间偏移转换为最终偏移，values 和 index 依序输出。index 满、租户变化或特征结束时写 metaindex 行。消费完每个 spill 后先清除引用，再关闭并合并读回、关闭错误。

四个 `.bin` 文件完成并通过原有 MustClose 同步关闭、part 目录同步后，才直接创建 metadata.json，写入本 part 的配置快照与统计，再同步关闭文件及目录。降采样 metadata 只含五个原生统计字段和有效的 downsampling_config，构成写入完成标志；外层仍须打开校验和提交 parts.json。配置字段缺失或为 null 时按原始 metadata 校验，非 null 但配置无效则报错。

最终文件使用共享 filestream.WriteCloser，创建、缓冲刷出、同步、关闭及目录同步保留原有 Must 语义。writer 的 reset 仅清理内存状态；Abort 释放自有文件与全部分辨率 spill，并删除未发布目录。目录删除失败保留路径，后续只重试未完成操作。普通写入、spill、校验或取消错误使整个目标失效，不允许继续写入或发布。

归并还共享 `min(256 MiB, memory.Allowed()/16)` 的进程预算，覆盖当前 TSID 的 header、基础样本、派生样本和稀疏 bucket 映射；切片按实际容量计费，稀疏映射预留 1 KiB 加每项 256 字节。申请失败立即返回错误，外层清理未发布目标，不等待其他归并释放内存。三类内存预算相互独立，只约束各自的新增工作分配，不是进程 RSS、共享缓冲、mmap 或系统页缓存的硬上限。

空间估算包含最终文件和 spill 中各一份共享时间列，按当前配置的单租户最大分辨率数保守估算每条输入的输出量；该磁盘估算系数不等于 writer 的分辨率并集大小。饱和算术防止溢出。partition 管理进程级额度，writer 在写入和读回时重新检查可用磁盘空间，不能预先将源文件计为空闲空间。

## 发布与查询

`parts.json` 原子 rename 是活动集合提交点。提交前失败保留源与旧清单并清理目标；提交后返回错误仍保留已发布目标、使内存集合与清单一致，并保留旧源磁盘文件。正常提交后，原始和降采样旧源均标记可删除，最后一个引用释放后按 `decRef → part.MustClose → fs.MustRemoveDir` 回收。查询持有旧 part 时延迟删除，不新增查询与归并间的共享锁。

后台降采样普通失败结束当前任务。关闭或快照所需的最终刷盘失败时，剩余内存源按原始格式持久化。共享文件 Must 操作、启动检查、原始数据最终持久化和程序不变量保留原有错误语义；专属返回错误使用英文 `[downsampling]` 前缀，保留原始原因和清理错误。

HTTP 的 resolution／feature 经 EvalConfig、SearchQuery、`search_downsampling_v2` 沿原 Init 参数表透传。`partSearch.initDownsampleQuery` 逐 part、逐租户选择能整除目标分辨率的最大实际源列。目标可以是未配置的更粗分辨率，同一 part 不能同时贡献基础列与额外列。

原有 index 缓存、TSID 堆、BlockRef 和 fs.ReaderAt 负责定位与读取。`tableSearch` 使用 `downsampleQueryState` 聚合当前 TSID 在所有 part／partition 的同 bucket 单特征贡献，完整聚合后过滤输出时间范围，生成独立不可变的原生编码 Block。输入范围扩展至完整 bucket；时间戳无损，值保留源精度。聚合入口、构建排序键及输出过滤循环沿用原有 deadline 和 pace 检查，失败时不输出未完成的 TSID。排序仍是一次同步调用，其规模受 bucket 预算约束。

查询 bucket 共享 `min(256 MiB, memory.Allowed()/20)` 的进程预算，首次创建映射预留 1 KiB，每个 bucket 另按 256 字节保守计费，涵盖映射扩容和排序键。一次 tableSearch 按历史最大 bucket 数持有额度，TSID 切换不提前归还仍可能由映射或切片持有的容量；reset 解除引用后统一归还。预算不足直接返回错误，使用原有查询错误处理，不等待其他查询。

指定降采样只读取降采样 part，原始查询只读取原始 part。结果缓存因未区分选择参数而禁用，索引缓存保留。RPC 仍由原有 `execSearchQueryRequest` 发送，`collectResults` 统一处理节点错误、部分响应、副本和慢副本策略，不增加通用错误前缀或要求全部节点成功。
