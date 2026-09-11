# 降采样存储审查说明

本文列出当前实现的审查入口、不变量和实际限制。数据语义与文件格式见[设计文档](downsampling_storage_design.md)，模块职责和调用链见[实现说明](downsampling_storage_implement.md)，测试入口及覆盖范围见[测试说明](downsampling_storage_test.md)。

## 审查入口

| 变更涉及的内容 | 优先核对的代码 |
|---|---|
| 聚合数学、多特征缓冲、精度、保留期限 | [downsample_block.go](lib/storage/downsample_block.go)、[downsample_merger.go](lib/storage/downsample_merger.go) |
| 分辨率、header 校验与排序、共享时间戳和租户判断 | [downsample_block.go](lib/storage/downsample_block.go) |
| metaindex 行类型、编码长度及编解码 | [downsample_metaindex_row.go](lib/storage/downsample_metaindex_row.go) |
| 索引遍历、原生解码、行数与编码的组合、文件负载范围 | [downsample_reader.go](lib/storage/downsample_reader.go)、[block.go](lib/storage/block.go) |
| 样本写入、分块、单列负载上限、文件排列、输出估算和空间复查 | [downsample_writer.go](lib/storage/downsample_writer.go)、[spill_writer.go](lib/filestream/spill_writer.go) |
| 偏移读取、文件检查及关闭错误 | [reader_at.go](lib/filestream/reader_at.go) |
| 格式标识、索引与元数据大小上限、原生 header 索引解码、打开校验和源 part 大小估算 | [downsample_part.go](lib/storage/downsample_part.go)、[part.go](lib/storage/part.go) |
| 启动预检、清单发现、选源、预算预留、发布和清理 | [downsample_partition.go](lib/storage/downsample_partition.go)、[partition.go](lib/storage/partition.go) |
| 降采样查询和 RPC | [downsample_query.go](lib/storage/downsample_query.go)、[part_search.go](lib/storage/part_search.go)、[netstorage/downsample_query.go](app/vmselect/netstorage/downsample_query.go) |

降采样专属代码按 block、metaindex row、reader、writer、merger、part、partition、query 八个文件划分。reader、writer、merger 的调用入口集中在内部辅助之前，对象池放在末尾。方法可见性按实际调用职责确定：原生 block 读取辅助及 merger 的 `reset` 是内部辅助；heap 和文件接口要求的大写方法须保留。

## 聚合和遍历不变量

- `Merge` 先遍历分辨率，再按 TSID 处理。堆只保证 TSID 顺序，不保证同一 TSID 跨源的 block 按时间排列。
- 当前 TSID 的所有相关 header 都参与最小、最大时间戳计算。槽数为两端 bucket 编号之差加一，空槽不输出；不能用源 part 的整体时间范围代替当前 TSID 的范围。
- 原始数据中同一 TSID 的相邻 block 可重叠，每条样本贡献都要处理。降采样 header 遵守自身的严格排序和时间范围约束，不能把该约束用于原始数据。
- `currentTSIDReaders` 保存当前 TSID 的来源，索引 reader 此时已推进到下一 TSID。实际数据由独立的 `currentSourceReader` 回读，不能覆盖堆所依赖的索引状态。
- `currentSourceBlock` 只是多特征解码缓冲；样本累加进入 `currentTSIDBucketSamples`。`partWriter` 是当前目标 writer 的借用引用，由 merger 调用其 `WriteSamples`。
- `downsampleSample.precisionBits == 0` 表示空槽。首次贡献复制完整样本；进入样本的源精度必须已验证为 1..64。同 bucket 精度不同返回错误，不静默降到另一精度。
- `last` 首先比较时间戳；时间戳相同时数值优先于 NaN，两个数值取较大者。较新的 NaN 不回退成较早数值。sum、count、min、max 传播 NaN；min/max 在 NaN 与无穷同时出现时仍须保留 NaN。输入及运算产生的 NaN 仅在 `downsampleSample.Merge` 结果中统一规范化，reader/writer 不另行处理，writer 接收已经过该方法处理的样本。
- 原始样本展开时 count 为 1，NaN 样本也贡献一次 count。是否过期按 bucket 结束时间判断，不按降采样记录的共享时间戳提前删除。
- `mergeStats` 统计源物理行。原始数据只在首个分辨率计数，只有两个分辨率均过期才记为完全删除；降采样记录按五个单值 Block 的物理行换算。

## 读取和编码不变量

- `downsampleReader` 仅由归并任务持有，统一读取原始内存、原始磁盘和降采样磁盘源；查询与 part 打开校验不取得该 reader。
- `Init` 准备当前分辨率的完整索引扫描，`SeekTSID` 仅为 merger 第二遍数据读取定位当前 TSID。reader 不接收任意时间窗口，merger 负责在下一 TSID 处停止；不得将查询的单特征选择或范围过滤重新放入归并 reader。
- 查询分辨率和特征沿原有 `Init` 参数表逐层透传，最终由 `partSearch` 选择 metaindex／index；得到原生 header 后复用原有 TSID、时间范围、`BlockRef` 及 `Block` 逻辑。索引及 payload 均通过 part 原有的三个 `fs.ReaderAt` 读取对象访问，复用 mmap、页驻留状态检查、系统调用回退、读取统计和 `ibCache`，不更改 `fs.disableMmap` 或 `fs.disableMincore`。不得另设平行搜索入口或在查询中聚合原始样本。
- 多特征 `ReadBlock` 的首列完整读取、解码时间戳。后四列必须核对 TSID、行数、精度、时间范围、时间戳 offset／size／codec，再复用已解码时间戳。
- 共享时间戳只在当前一次多特征读取内复用。换 block、TSID、源 part、分辨率，或再次调用 `ReadBlock`，都重新读取首列。单列查询由 `partSearch` 直接读取选定列索引，得到原生 header 后，由 `BlockRef` 和原生 `Block` 独立完成 payload 读取与解码。
- 值列保留各自的 Scale、编码类型和 payload 校验。`downsampleReader.readNativeValues` 直接使用原有 `encoding.UnmarshalValues`，负责清除上一列的值、核对时间戳与数值行数、清理编码缓冲和重置读取位置；共享的 `block.go` 保持原有实现。跳过时间戳读取时仍须检查值列范围、行数和解压上限。
- 输入可以使用有损精度。时间戳沿用原生解码的顺序修复，之后检查行数、首尾和范围；不能假设所有输入精度均为 64，也不能假设有损解码后仍保持编码前的 bucket 唯一性。
- `WriteSamples` 只读借用输入，空槽不计输出行数。按精度变化或 8192 个有效样本切块，五列必须使用相同边界和共享时间戳描述。
- 时间戳只持久化一份。五列原生编码的结果仍需比较一致，不能只比较输入时间戳相同。
- 最终 values、index 按分辨率、特征、TSID、block 顺序排列。分段写入时不能把不同特征的片段穿插到最终文件中。
- 一个 metaindex 行所覆盖的全部 header 必须属于同一分辨率、同一特征和同一租户；允许同租户多个 TSID 共处一个 index block。

## 格式识别和损坏处理

`detectDownsampleFormat` 只读取 `metadata.json`，检查 JSON、`FormatVersion` 及格式必需字段；不读取 metaindex，也不检查 `.bin` 文件的大小或类型。原始 metadata 不含 `FormatVersion`；含降采样语义但缺少版本、版本不支持或内容不完整时必须返回错误。只有成功解析且必需字段完整合法的 metadata 才构成完成标志，不能只检查文件是否存在；缺少 metadata 的旧原始格式继续按目录名解析。实际数据文件类型、magic、长度及索引一致性由打开路径检查，不能把元数据格式识别通过当成数据完整性验证通过。

`openDownsamplePart` 常驻 metaindex，在 part 层独立遍历五列索引，检查列齐全、统计、共享时间戳描述，以及 timestamps／values 的连续覆盖；此过程不使用归并专属的 `downsampleReader`。校验使用临时 `filestream.ReadAtCloser`，关闭后才建立原有 `fs.ReaderAt` 查询读取对象；校验及关闭错误正常返回，不将临时句柄存入 part。打开检查不解码所有数值 payload；具体 payload 的编码、长度、行数和时间范围错误在读取时检查。

`SeekTSID` 从可能包含目标 TSID 的 index 开始读取，定位后的相邻 index 继续接受偏移与顺序校验，不能将跨越未读取前缀误判为连续性错误。定位必须覆盖一个 TSID 横跨多个 index，以及原始 metaindex 首 TSID 相等的边界情况。任意时间窗口与单特征筛选属于查询入口，不属于归并 reader。

## 所有权和失败不变量

| 资源或阶段 | 必须核对的行为 |
|---|---|
| 源 part 引用 | 覆盖 reader 使用、目标验证和发布过程；先归还 reader，再释放其依赖的 part 引用。查询也持有独立引用；发布后仍有查询引用的旧 part 不得关闭或删除，最后一个引用释放后才回收。 |
| 全部源 reader | 取得后先登记再 Init；清理包含已出堆和初始化失败的实例。归还前清空堆及当前 TSID 的借用引用，关闭错误不能被取消错误掩盖。 |
| 归并文件句柄 | 原始磁盘与降采样磁盘源均由 reader 自行打开、关闭三个 `filestream.ReadAtCloser`，不设置借用标记。五个特征索引游标均为内嵌值，不持有文件、不进入对象池。归并不借用 part 的查询读取对象，storage 不直接持有底层文件句柄。 |
| 查询文件读取对象 | 原有 `timestampsFile`、`valuesFile`、`indexFile` 均为 `fs.ReaderAt`，由 part 统一释放，不再保存额外的降采样句柄。索引未命中 `ibCache` 时调用 `p.indexFile.MustReadAt`，payload 复用 `BlockRef.MustReadBlock`。 |
| 解码缓冲 | 每个 block 重设逻辑长度；`Merge` 返回前通过 `reset` 归还 reader 和多特征缓冲，清除作业引用。外层使用返回统计和调用方的取消信号。 |
| SpillWriter | 单个缓冲按需增长，默认阈值 16 MiB，由 downsampling.spillMaxMemorySize 配置；不超过阈值时不创建临时文件，超过时将满块追加到同一文件。Read 可重复读取完整字节流，独立校验文件前缀长度，内存尾部不落盘。Read 不封闭或清理 spill；调用方在成功和失败后均须显式 Close。写入失败封闭并尝试清理；资源只释放一次，删除失败保留重试所需路径。 |
| filestream.ReaderAt | 仅用于归并和打开校验。打开时验证普通文件和非负大小；初始化失败关闭已打开句柄并保留关闭错误。ReadAt 直接使用调用方缓冲，Close 缓存结果且只执行一次；关闭与读取不得并发发生。 |
| 最终文件 writer | 直接复用 filestream.WriteCloser；创建和关闭保留原有 Must 行为。降采样释放前清除引用，不重复关闭；目录清理仍由降采样负责。 |
| WriteSamples 后段失败 | 前段已写出也必须使整个未发布 writer 失败，并清理 timestamps、spill 和其他目标文件；不能仅丢弃最后一块继续发布。 |
| Finish | 全部 spill 完成并清理、四个 `.bin` 完成写入和同步关闭、part 目录同步之后，才直接创建和写入 `metadata.json`；同步关闭后再次同步 part 目录和父目录。合法完整的 metadata 表示写入完成，调用方仍须检查取消及验证目标。Finish 不修改活动 part 集合。 |
| 清单提交前 | 不改活动源集合；临时清单与未发布目标由错误路径清理。预算不得依赖预先删除源文件。 |
| 清单提交后 | `parts.json` 的 rename 是活动集合提交点。随后返回错误仍须保留目标、更新内存活动集合，并保留旧源磁盘文件；不能因返回错误而 Abort 已发布目标。实际目录同步调用共享 fs.MustSyncPath。 |
| 正常提交后回收 | 原始与降采样磁盘源均标记为可删除；最后一个引用释放后通过 `decRef → part.MustClose → fs.MustRemoveDir` 关闭并删除。仍有查询引用时必须保留旧 part，不能与提交后同步返回错误时保留旧文件的异常路径混淆。 |
| 最终刷盘 | 失败后仅对仍在内存的源按原始格式落盘；即使没有剩余内存源，也确认当前清单目录已同步。 |

清理职责应按资源划分：分辨率切换只归还该分辨率的索引 reader，归并结束再清空整次作业。成功关闭、归还或删除后立即清除持有引用或路径；后续清理只重试尚未完成的操作。英文诊断以 `[downsampling]` 开头，包装错误时保留原始原因，不能用清理错误覆盖首次失败。

查询与归并使用独立的文件读取对象及工作缓冲。归并源的排他调度沿用 `isInMerge`，旧 part 的延迟回收沿用引用计数，不新增查询与归并之间的共享锁。资源竞争仍包含磁盘带宽、CPU、系统页缓存，以及持有旧 part 的长查询造成的磁盘占用；现有发布锁与共享 `Must` 故障语义保持不变。

空间审查应贯通三层：part 按源统计估算目标大小，writer 计算编码上界并复查物理空间，partition 管理进程级预留及空闲空间缓存期间的额度释放。核对饱和算术、`errDownsampleNoSpace` 的错误识别以及预留释放时机；写入复查不得再次扣除已经反映在空闲读数中的本任务输出，也不得把尚未删除的源计为空闲空间。

文件系统可能拒绝关闭、删除或同步。审查错误路径时应核对实际尝试、路径保留和错误传播，不能把“调用了 Abort”写成“任何故障下均已删除全部文件”。降采样自身返回的错误退出当前作业；最终文件创建、缓冲刷出、同步和关闭、目录同步、程序不变量、通用 part 回收和原始数据最终持久化仍遵守原有 `Must`／FATAL 语义。

## 当前规模和功能限制

| 项目 | 当前限制或行为 |
|---|---|
| 分辨率与特征 | 固定 5m／1h 和 last／sum／count／min／max；格式定义见设计文档。 |
| Dedup | 降采样要求 dedup 间隔为零。 |
| 月份边界 | 生产选源位于单个 UTC 自然月 partition 内；`downsampleMerger.Merge` 自身不校验所有输入同月。 |
| 源数量 | 后台文件选源最多选择 `defaultPartsToMerge = 15` 个候选；强制归并和刷盘的选源可能更多。merger 没有另设源数量硬上限。 |
| Bucket 缓冲 | 按当前 TSID 的完整时间跨度分配。`downsampleMaxPooledBuckets = 17856` 仅限制 `reset` 后的池缓存容量，不限制输入跨度。 |
| Reader 列表缓存 | `reset` 清除三个 reader 指针切片中的全部源引用，将长度归零并保留底层数组复用，不设容量丢弃阈值。 |
| 单个负载与 index | 时间戳或值列磁盘 payload 最多 128 KiB；index 压缩输入最多 128 KiB、解码最多 64 KiB；降采样 block 最多 8192 行，原始输入读取最多 16384 行。 |
| Metaindex | part 打开后常驻；编码和解码长度上限为 64 MiB。该上限不是进程总内存上限。 |
| 总内存与 I/O | 每个源保留索引工作缓冲；默认配置下，单个 writer 的五个 spill 当前持有的缓冲容量合计最多 80 MiB，不含扩容时尚待 GC 的旧分配。并发任务还会增加 reader、writer、bucket 和编码缓冲。磁盘预留额度不约束堆内存或系统页缓存；当前没有统一 merge 内存预算。 |
| 数值 | sum、count 使用 float64；存在浮点舍入、整数计数精度及溢出边界，不能视为任意规模的精确整数或实数计算。 |
| 降采样查询 | 同时指定 `query.resolution` 和 `query.feature` 时只查询已落盘的降采样记录，两者均未指定时只查询原始 part；不自动拼接原始数据与降采样数据，也不为不同 part 的重叠降采样记录专门再聚合。 |
| 查询失败与缓存 | 降采样请求要求所有目标 storage 节点成功，并禁用结果缓存。 |

值列读取仍按一个逻辑 block 的五个特征依次发起 `ReadAt`，在同一 values 文件的不同列区段之间切换。共享时间戳避免同一多特征读取中的重复 I/O 和解码，但不代表值列已经改为整列顺序归并，也不构成磁盘吞吐或总内存的性能保证。性能结论应对应测试说明中的实际输入、运行环境和测量结果。
