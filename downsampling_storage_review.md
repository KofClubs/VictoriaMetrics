# 降采样存储审查说明

本文列出当前实现的审查入口、不变量和实际限制。数据语义与文件格式见[设计文档](downsampling_storage_design.md)，模块职责和调用链见[实现说明](downsampling_storage_implement.md)，测试入口及覆盖范围见[测试说明](downsampling_storage_test.md)。

## 审查入口

| 变更涉及的内容 | 优先核对的代码 |
|---|---|
| JSON 配置、基础分辨率持久化、快照及动态更新 | [downsample_config.go](lib/storage/downsample_config.go)、[vmstorage/main.go](app/vmstorage/main.go) |
| 聚合数学、多特征缓冲、精度、保留期限 | [downsample_block.go](lib/storage/downsample_block.go)、[downsample_merger.go](lib/storage/downsample_merger.go) |
| 分辨率、header 校验与排序、共享时间戳和租户判断 | [downsample_block.go](lib/storage/downsample_block.go) |
| metaindex 行类型、编码长度及编解码 | [downsample_metaindex_row.go](lib/storage/downsample_metaindex_row.go) |
| 索引遍历、原生解码、行数与编码的组合、文件负载范围 | [downsample_reader.go](lib/storage/downsample_reader.go)、[block.go](lib/storage/block.go) |
| 样本写入、分块、单列负载上限、文件排列、输出估算和空间复查 | [downsample_writer.go](lib/storage/downsample_writer.go)、[spill_writer.go](lib/filestream/spill_writer.go) |
| 偏移读取、文件检查及关闭错误 | [reader_at.go](lib/filestream/reader_at.go) |
| 格式标识、索引与元数据大小上限、原生 header 索引解码、打开校验和源 part 大小估算 | [downsample_part.go](lib/storage/downsample_part.go)、[part.go](lib/storage/part.go) |
| 启动预检、清单发现、选源、预算预留、发布和清理 | [downsample_partition.go](lib/storage/downsample_partition.go)、[partition.go](lib/storage/partition.go) |
| 降采样查询和 RPC | [downsample_query.go](lib/storage/downsample_query.go)、[part_search.go](lib/storage/part_search.go)、[netstorage.go](app/vmselect/netstorage/netstorage.go)、[vmselectapi/server.go](lib/vmselectapi/server.go) |

降采样专属代码按 config、block、metaindex row、reader、writer、merger、part、partition、query 九个文件划分。reader、writer、merger 的调用入口集中在内部辅助之前，对象池放在末尾。方法可见性按实际调用职责确定：原生 block 读取辅助及 merger 的 `reset` 是内部辅助；heap 和文件接口要求的大写方法须保留。

## 聚合和遍历不变量

- 配置快照不可变；基础分辨率启用后不可更改，租户额外分辨率必须是更大的整数倍。运行时更新只影响新任务，已有 part 按自身 metadata 配置读取，未出现的租户不能进入目标 metadata。配置解析须预检规范化配置和最大数值字段组成的 metadata 不超过 64 KiB，不能只检查输入 JSON 长度，也不另设固定数量上限。
- metadata 仅保存配置和统计，实际分辨率从有序 metaindex 区段确定，必须包含基础分辨率。每个实际区段的五特征必须完整，每条额外列须属于该租户配置且对应 TSID 有基础数据；配置中没有实际数据的额外分辨率可以缺席。
- `Merge` 整次只建立一次基础 reader 堆，按 TSID 处理。基础输入只聚合一次，再从内存基础结果推导本租户额外分辨率，不读取旧额外列。堆只保证 TSID 顺序，不保证同一 TSID 跨源 block 的时间顺序。
- 当前 TSID 的全部基础 header 决定时间范围。范围槽数不大于源行数时采用密集槽；稀疏范围通过 bucket 映射只给实际数据开槽，聚合后排序。额外分辨率只保存非空 bucket，不能因小基础分辨率和长空白区间分配巨型数组。
- 原始数据中同一 TSID 的相邻 block 可重叠，每条样本贡献都要处理。降采样 header 遵守自身的严格排序和时间范围约束，不能把该约束用于原始数据。
- `currentTSIDReaders` 保存当前 TSID 的来源；各源的首列 header 按值保存在 `currentTSIDBlockHeaders`，首列游标此时已推进到下一 TSID 或 EOF。实际数据由同一个 reader 按缓存 header 读取，不能覆盖堆所依赖的首列 header，也不能持有会被后续 index 解码覆盖的 header 指针。
- `currentSourceBlock` 只是多特征解码缓冲；样本累加进入 `currentTSIDBaseBucketSamples`。`partWriter` 是当前目标 writer 的借用引用，由 merger 调用其 `WriteSamples`。
- `downsampleSample.precisionBits == 0` 表示空槽。首次贡献复制完整样本；进入样本的源精度必须已验证为 1..64。同 bucket 精度不同返回错误，不静默降到另一精度。
- `last` 首先比较时间戳；时间戳相同时数值优先于 NaN，两个数值取较大者。较新的 NaN 不回退成较早数值。sum、count、min、max 传播 NaN；min/max 在 NaN 与无穷同时出现时仍须保留 NaN。输入及运算产生的 NaN 仅在 `downsampleSample.Merge` 结果中统一规范化，reader/writer 不另行处理，writer 接收已经过该方法处理的样本。
- 原始样本展开时 count 为 1，NaN 样本也贡献一次 count。基础保留前缀必须覆盖当前配置及源 part 配置中尚有效的粗粒度 bucket；全部额外列必须完整表达同一保留输入，不再次按自身 bucket 右端点删除前缀；否则最大整除列选择会遗漏贡献。
- 内存原始归并、内存／磁盘 part 删除、整月分区删除同样保护粗粒度 bucket 所需前缀；整月判断不能只比较月末与原始保留期限。关闭降采样时保留原始规则。
- `mergeStats` 只统计实际读取的基础输入物理行；原始每行计一次，降采样基础行按五个 Block 换算。旧额外列不读取、不重复计数。

## 读取和编码不变量

- `downsampleReader` 仅由归并任务持有，统一读取原始内存、原始磁盘和降采样磁盘源；查询与 part 打开校验不取得该 reader。
- `Init` 顺序扫描 metaindex 并建立当前分辨率各列的完整索引区间。每个源在当前分辨率内的首列索引只向前扫描一次，数据读取直接使用缓存 header；其他四列游标向前对齐该 header，不重新定位或回读首列索引。原始源和降采样基础源在整次归并中只扫描一次。reader 不接收 TSID 定位或任意时间窗口；查询的单特征选择和范围过滤仍由查询入口处理。
- 查询分辨率和特征沿原有 `Init` 参数表逐层透传，最终由 `partSearch` 选择 metaindex／index；得到原生 header 后复用原有 TSID、时间范围、`BlockRef` 及 `Block` 逻辑。索引及 payload 均通过 part 原有的三个 `fs.ReaderAt` 读取对象访问，复用 mmap、页驻留状态检查、系统调用回退、读取统计和 `ibCache`，不更改 `fs.disableMmap` 或 `fs.disableMincore`。不得另设平行搜索入口或在查询中聚合原始样本。
- 请求发送在原有 `execSearchQueryRequest` 内选择 RPC 和编码；结果收集共用 `collectResults`，部分响应、副本策略及节点错误分类与原始查询一致。不得因设置降采样选择改为等待全部节点成功，也不为通用 HTTP／RPC 错误添加专属前缀状态。参数与线协议校验仍须保留。
- 多特征 `ReadBlock` 的首列完整读取、解码时间戳。后四列必须核对 TSID、行数、精度、时间范围、时间戳 offset／size／codec，再复用已解码时间戳。
- 共享时间戳只在当前一次多特征读取内复用。换 block、TSID、源 part、分辨率，或再次调用 `ReadBlock`，都重新读取首列。单列查询由 `partSearch` 直接读取选定列索引，得到原生 header 后，由 `BlockRef` 和原生 `Block` 独立完成 payload 读取与解码。
- 值列保留各自的 Scale、编码类型和 payload 校验。`downsampleReader.readNativeValues` 直接使用原有 `encoding.UnmarshalValues`，负责清除上一列的值、核对时间戳与数值行数、清理编码缓冲和重置读取位置；共享的 `block.go` 保持原有实现。跳过时间戳读取时仍须检查值列范围、行数和解压上限。
- 原始输入允许有损精度，按原生解码语义读取；输出值保留源精度。降采样写入和查询生成的时间戳必须无损编码，准确保留有效贡献的最大时间，不能被低精度数值编码移动。
- `WriteSamples` 只读借用输入，空槽不计输出行数。按精度变化或 8192 个有效样本切块，五列必须使用相同边界和共享时间戳描述。
- writer 逐列复用一个原生 `currentFeatureBlock`，编码后立即写入该特征 spill。时间戳只持久化一份；首列编码字节须独立保存，供后四列核对，不能引用会被下一列编码覆盖的缓冲，也不能只比较输入时间戳相同。
- writer 的活跃分辨率数按本 part 各租户实际输出分辨率取并集，不能按单租户最大数量估算内存；每个分辨率各持有五个 spill；last 保存唯一时间列，其他四列只存 header 和值。暂存时间偏移为分辨率内相对位置，Finish 按分辨率升序读回后转换成最终偏移。timestamps 按分辨率、TSID、时间排列，values／index 再包含 feature 顺序，不允许租户列缺失被其他租户数据填充。
- 一个 metaindex 行所覆盖的全部 header 必须属于同一分辨率、同一特征和同一租户；允许同租户多个 TSID 共处一个 index block。

查询聚合还须核对以下边界：

- 同一 part 只选择一种输入分辨率，依据不可变 part 配置及实际 metaindex；请求分辨率须为基础分辨率整数倍，不能选择无法整除目标的列。
- tableSearch 在当前 TSID 全部源贡献完成后才输出，跨 index、part 和月份边界不得提前结束同一 bucket；sum/count 不得被原有时间戳去重代替。
- 输入范围扩展到完整目标 bucket，输出在聚合后按最大共享时间戳过滤，不能因查询起止点截断贡献。
- 查询只读取一个特征，数据读取和 deadline／pace 检查复用原生路径；输出 Block 独立且不可变，复制 BlockRef 后即使游标推进或搜索重置也不能指向被覆盖的缓冲。
- 原始查询行为、索引缓存、mmap、节点部分响应和副本策略保持一致；降采样结果缓存继续禁用。

## 格式识别和损坏处理

`detectDownsampleFormat` 只读取 `metadata.json`。`downsampling_config` 缺失或为 `null` 时按原始 partHeader 校验；非 `null` 时必须能解析为有效配置，空对象、字符串、缺少基础分辨率等内容必须报错，五个统计字段也须完整非 `null`。降采样 metadata 只保存这六个字段。只有配置和统计均完整合法才构成降采样写入完成标志，不能只检查文件存在。缺少 metadata 的旧原始格式继续按目录名解析。格式识别不读取 metaindex，也不检查 `.bin` 文件的大小或类型；实际文件固定签名、类型、长度和索引一致性由打开路径检查，不增加版本兼容分支。

`openDownsamplePart` 常驻 metaindex，在 part 层独立遍历五列索引，检查列齐全、统计、共享时间戳描述，以及 timestamps／values 的连续覆盖；此过程不使用归并专属的 `downsampleReader`。校验使用临时 `filestream.ReadAtCloser`，关闭后才建立原有 `fs.ReaderAt` 查询读取对象；校验及关闭错误正常返回，不将临时句柄存入 part。打开检查不解码所有数值 payload；具体 payload 的编码、长度、行数和时间范围错误在读取时检查。

顺序扫描必须覆盖同一 TSID 横跨多个 index、原始 metaindex 首 TSID 相等以及原始 block 时间范围重叠的边界。缓存 header 的消费不能使任一列索引倒退；各列仍须执行块内及跨 index 的顺序、租户和负载偏移校验。跳过已删除 TSID 时不读取其 payload，其他四列随后继续向前对齐下一条需要读取的 TSID。

## 所有权和失败不变量

| 资源或阶段 | 必须核对的行为 |
|---|---|
| 源 part 引用 | 覆盖 reader 使用、目标验证和发布过程；先归还 reader，再释放其依赖的 part 引用。查询也持有独立引用；发布后仍有查询引用的旧 part 不得关闭或删除，最后一个引用释放后才回收。 |
| 全部源 reader | 取得后先登记再 Init；清理包含已出堆和初始化失败的实例。归还前清空堆及当前 TSID 的借用引用，关闭错误不能被取消错误掩盖。 |
| 当前 TSID 的 header | 每个源只暂存当前 TSID 的首列 header 值；已删除 TSID 不保存。按实际容量计入归并预算；消费完成清空长度，任务结束时移除容量后归还额度。提前取消或首列扫描失败也必须统一清理。 |
| 归并文件句柄 | 原始磁盘与降采样磁盘源均由 reader 自行打开、关闭三个 `filestream.ReadAtCloser`，不设置借用标记。五个特征索引游标均为内嵌值，不持有文件、不进入对象池。归并不借用 part 的查询读取对象，storage 不直接持有底层文件句柄。 |
| 查询文件读取对象 | 原有 `timestampsFile`、`valuesFile`、`indexFile` 均为 `fs.ReaderAt`，由 part 统一释放，不再保存额外的降采样句柄。索引未命中 `ibCache` 时调用 `p.indexFile.MustReadAt`，payload 复用 `BlockRef.MustReadBlock`。 |
| 解码缓冲 | 每个 block 重设逻辑长度；`Merge` 返回前通过 `reset` 归还 reader 和多特征缓冲，清除作业引用。外层使用返回统计和调用方的取消信号。 |
| SpillWriter | 单个阈值默认 16 MiB，必须大于零，首次非空写入固定；所有实例共享 `min(256 MiB, memory.Allowed()/10)` 预算，扩容同时计入新旧缓冲。不足时写出尾部并直接落盘，不等待。Read 只读消费文件前缀及内存尾部，不关闭或清理；最后由 Close 释放额度和文件。写入失败同样封闭并尝试清理，删除失败保留重试路径。 |
| filestream.ReaderAt | 仅用于归并和打开校验。打开时验证普通文件和非负大小；初始化失败关闭已打开句柄并保留关闭错误。ReadAt 直接使用调用方缓冲，Close 缓存结果且只执行一次；关闭与读取不得并发发生。 |
| 最终文件 writer | 直接复用 filestream.WriteCloser；创建和关闭保留原有 Must 行为。降采样释放前清除引用，不重复关闭；目录清理仍由降采样负责。 |
| WriteSamples 后段失败 | 前段已写出也必须使整个未发布 writer 失败，并清理 timestamps、spill 和其他目标文件；不能仅丢弃最后一块继续发布。 |
| Finish(stopCh) | 当前任务取消信号透传到 spill 组装，入口、各 feature、各 header 及 metadata 创建前检查；取消返回原错误并 Abort，已关闭句柄不得重复关闭。全部 spill 完成并清理、四个 `.bin` 完成写入和同步关闭、part 目录同步之后，才直接创建和写入 `metadata.json`；同步关闭后再次同步 part 目录和父目录。合法完整的 metadata 表示写入完成，调用方仍须检查取消及验证目标。Finish 不修改活动 part 集合。 |
| 清单提交前 | 不改活动源集合；临时清单与未发布目标由错误路径清理。预算不得依赖预先删除源文件。 |
| 清单提交后 | `parts.json` 的 rename 是活动集合提交点。随后返回错误仍须保留目标、更新内存活动集合，并保留旧源磁盘文件；不能因返回错误而 Abort 已发布目标。实际目录同步调用共享 fs.MustSyncPath。 |
| 正常提交后回收 | 原始与降采样磁盘源均标记为可删除；最后一个引用释放后通过 `decRef → part.MustClose → fs.MustRemoveDir` 关闭并删除。仍有查询引用时必须保留旧 part，不能与提交后同步返回错误时保留旧文件的异常路径混淆。 |
| 最终刷盘 | 失败后仅对仍在内存的源按原始格式落盘；即使没有剩余内存源，也确认当前清单目录已同步。 |

内存计费必须核对扩容的新旧容量、TSID 切换后实际保留的引用及额度的最终归还。申请预算使用非阻塞操作；不能持有一部分额度再等待另一任务释放，以免查询或归并互相等待。失败或取消后必须清理，重复 reset／Close 不得重复归还额度。

清理职责应按资源划分：整次归并共用各源的一个基础 reader，索引与 payload 的自有句柄只关闭一次；归并结束清空整次作业。成功关闭、归还或删除后立即清除持有引用或路径；后续清理只重试尚未完成的操作。降采样自身的存储与参数校验诊断以 `[downsampling]` 开头，包装错误时保留原始原因，不能用清理错误覆盖首次失败；共用查询流程沿用原有错误处理。

归并 reader 每次推进 header 或向前对齐其他特征时检查取消；发布前打开校验在 metaindex、索引组及基础列对齐循环中检查同一信号。查询聚合及输出沿用 deadline／pace 检查；同步排序由 bucket 预算限制规模。取消检查不等于可以中断正在执行的系统文件 I/O、fsync 或共享 Must 操作。

查询与归并使用独立的文件读取对象及工作缓冲。归并源的排他调度沿用 `isInMerge`，旧 part 的延迟回收沿用引用计数，不新增查询与归并之间的共享锁。资源竞争仍包含磁盘带宽、CPU、系统页缓存，以及持有旧 part 的长查询造成的磁盘占用；现有发布锁与共享 `Must` 故障语义保持不变。

空间审查应贯通三层：part 按源统计估算目标大小，writer 计算编码上界并复查物理空间，partition 管理进程级预留及空闲空间缓存期间的额度释放。核对饱和算术、`errDownsampleNoSpace` 的错误识别以及预留释放时机；写入复查不得再次扣除已经反映在空闲读数中的本任务输出，也不得把尚未删除的源计为空闲空间。

文件系统可能拒绝关闭、删除或同步。审查错误路径时应核对实际尝试、路径保留和错误传播，不能把“调用了 Abort”写成“任何故障下均已删除全部文件”。降采样自身返回的错误退出当前作业；最终文件创建、缓冲刷出、同步和关闭、目录同步、程序不变量、通用 part 回收和原始数据最终持久化仍遵守原有 `Must`／FATAL 语义。

## 当前规模和功能限制

| 项目 | 当前限制或行为 |
|---|---|
| 分辨率与特征 | 默认仅 5m 基础分辨率，基础不可更改；租户额外分辨率为更大的整数倍，五特征固定。配置及 metadata 各限制 64 KiB。 |
| Dedup | 降采样要求 dedup 间隔为零。 |
| 月份边界 | 生产选源位于单个 UTC 自然月 partition 内；`downsampleMerger.Merge` 自身不校验所有输入同月。 |
| 源数量 | 后台文件选源最多选择 `defaultPartsToMerge = 15` 个候选；强制归并和刷盘的选源可能更多。merger 没有另设源数量硬上限。 |
| Bucket 缓冲 | 基础样本在密集槽与实际 bucket 映射之间选择，额外分辨率只保留非空 bucket；全部受归并预算约束，任务结束后解除引用并归还额度，不保留未计费的大数组。 |
| Reader 列表缓存 | `reset` 清除三个 reader 指针切片中的全部源引用，将长度归零并保留底层数组复用，不设容量丢弃阈值。 |
| 当前 TSID 的 header 缓存 | 每个源按当前 TSID 的实际 block 数暂存首列 header，不保存完整 payload；容量在当前分辨率内复用，Close 时释放。 |
| 单个负载与 index | 时间戳或值列磁盘 payload 最多 128 KiB；index 压缩输入最多 128 KiB、解码最多 64 KiB；降采样 block 最多 8192 行，原始输入读取最多 16384 行。 |
| Metaindex | part 打开后常驻；编码和解码长度上限为 64 MiB。该上限不是进程总内存上限。 |
| 归并内存预算 | 所有任务共享 `min(256 MiB, memory.Allowed()/16)`，覆盖 header、基础／派生样本实际容量；稀疏映射预留 1 KiB 加每项 256 字节；不足立即返回错误并清理，不等待。 |
| 查询内存预算 | 所有查询共享 `min(256 MiB, memory.Allowed()/20)`，首次创建映射预留 1 KiB，每个 bucket 另保守计 256 字节；单次查询按历史最大 bucket 数持有额度，直到 tableSearch.reset 解除引用并归还；不足立即返回查询错误。 |
| 内存边界 | Spill、归并、查询使用独立预算；只约束对应的新增工作分配。原始共享缓冲、常驻 metaindex、对象池、mmap、系统页缓存及待垃圾回收对象仍占内存，不能据此保证进程 RSS 上限。磁盘预留与内存预算相互独立。 |
| 数值 | sum、count 使用 float64；存在浮点舍入、整数计数精度及溢出边界，不能视为任意规模的精确整数或实数计算。 |
| 降采样查询 | 同时指定 `resolution` 和 `feature` 时只查询已落盘的降采样记录，两者均未指定时只查询原始 part；不拼接原始数据；每个 part 按租户选择最大可用整除分辨率，再在单个 vmstorage 内跨 part／partition 聚合目标 bucket。 |
| 查询失败与缓存 | 节点错误、部分结果、组内及跨组副本容错、跳过慢副本均沿用原始查询的 collectResults 策略。降采样禁用未区分分辨率和特征的结果缓存，保留原有索引缓存。 |

值列读取仍按一个逻辑 block 的五个特征依次发起 `ReadAt`，在同一 values 文件的不同列区段之间切换。共享时间戳避免同一多特征读取中的重复 I/O 和解码，但不代表值列已经改为整列顺序归并，也不构成磁盘吞吐或总内存的性能保证。性能结论应对应测试说明中的实际输入、运行环境和测量结果。
