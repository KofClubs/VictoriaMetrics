# 降采样存储审查说明

本文列出当前实现的审查入口、不变量和实际限制。数据语义与文件格式见[设计文档](downsampling_storage_design.md)，模块职责和调用链见[实现说明](downsampling_storage_implement.md)，测试入口及覆盖范围见[测试说明](downsampling_storage_test.md)。

## 审查入口

| 变更涉及的内容 | 优先核对的代码 |
|---|---|
| 聚合数学、精度、保留期限 | [downsample.go](lib/storage/downsample.go)、[downsample_merger.go](lib/storage/downsample_merger.go) |
| 索引遍历、原生解码、共享时间戳 | [downsample_reader.go](lib/storage/downsample_reader.go)、[block.go](lib/storage/block.go)、[downsample_codec.go](lib/storage/downsample_codec.go) |
| 样本写入、分块和文件排列 | [downsample_writer.go](lib/storage/downsample_writer.go)、[spill.go](lib/filestream/spill.go) |
| 格式识别和打开校验 | [downsample_open.go](lib/storage/downsample_open.go)、[downsample_part.go](lib/storage/downsample_part.go)、[part.go](lib/storage/part.go) |
| 选源、发布、取消和清理 | [partition.go](lib/storage/partition.go)、[downsample_partition.go](lib/storage/downsample_partition.go)、[downsample_space.go](lib/storage/downsample_space.go) |
| 字段查询和 RPC | [downsample_query.go](lib/storage/downsample_query.go)、[part_search.go](lib/storage/part_search.go)、[downsample_search_protocol.go](lib/storage/downsample_search_protocol.go)、[netstorage/downsample_query.go](app/vmselect/netstorage/downsample_query.go) |

## 聚合和遍历不变量

- `Merge` 先遍历分辨率，再按 TSID 处理。堆只保证 TSID 顺序，不保证同一 TSID 跨源的 block 按时间排列。
- 当前 TSID 的所有相关 header 都参与最小、最大时间戳计算。槽数为两端 bucket 编号之差加一，空槽不输出；不能用源 part 的整体时间范围代替当前 TSID 的范围。
- 原始数据中同一 TSID 的相邻 block 可重叠，每条样本贡献都要处理。降采样 header 遵守自身的严格排序和时间范围约束，不能把该约束用于原始数据。
- `currentTSIDReaders` 保存当前 TSID 的来源，索引 reader 此时已推进到下一 TSID。实际数据由独立的 `currentSourceReader` 回读，不能覆盖堆所依赖的索引状态。
- `currentSourceBlock` 只是多特征解码缓冲；样本累加进入 `currentTSIDBucketSamples`。`partWriter` 是当前目标 writer 的借用引用，由 merger 调用其 `WriteSamples`。
- `downsampleSample.precisionBits == 0` 表示空槽。首次贡献复制完整样本；进入样本的源精度必须已验证为 1..64。同 bucket 精度不同返回错误，不静默降到另一精度。
- `last` 首先比较时间戳；时间戳相同时数值优先于 NaN，两个数值取较大者。较新的 NaN 不回退成较早数值。sum、count、min、max 的 NaN 传播与原生数值转换应保持一致。
- 原始样本展开时 count 为 1，NaN 样本也贡献一次 count。是否过期按 bucket 结束时间判断，不按降采样记录的共享时间戳提前删除。
- `mergeStats` 统计源物理行。原始数据只在首个分辨率计数，只有两个分辨率均过期才记为完全删除；降采样记录按五个单值 Block 的物理行换算。

## 读取和编码不变量

- 多特征 `ReadBlock` 的首列完整读取、解码时间戳。后四列必须核对 TSID、行数、精度、时间范围、时间戳 offset／size／codec，再复用已解码时间戳。
- 共享时间戳只在当前一次多特征读取内复用。换 block、TSID、源 part、分辨率，或再次调用 `ReadBlock`，都重新读取首列。查询单列 `ReadFieldBlock` 仍独立完成完整解码。
- 值列保留各自的 Scale、编码类型和 payload 校验。`Block.unmarshalValues` 与完整原生解码共用相同数值恢复逻辑；不能因跳过时间戳读取而跳过值列范围、行数或解压上限检查。
- 输入可以使用有损精度。时间戳沿用原生解码的顺序修复，之后检查行数、首尾和范围；不能假设所有输入精度均为 64，也不能假设有损解码后仍保持编码前的 bucket 唯一性。
- `WriteSamples` 只读借用输入，空槽不计输出行数。按精度变化或 8192 个有效样本切块，五列必须使用相同边界和共享时间戳描述。
- 时间戳只持久化一份。五列原生编码的结果仍需比较一致，不能只比较输入时间戳相同。
- 最终 values、index 按分辨率、特征、TSID、block 顺序排列。分段写入时不能把不同特征的片段穿插到最终文件中。
- 一个 metaindex 行所覆盖的全部 header 必须属于同一分辨率、同一特征和同一租户；允许同租户多个 TSID 共处一个 index block。

## 格式识别和损坏处理

审查 `detectDownsampleFormat`、metadata 解析和打开路径时，核对元数据、magic、数据文件类型是否一致。格式不完整或标识矛盾应返回错误，不能按原始格式接受损坏的降采样数据。

`openDownsamplePart` 常驻 metaindex，并使用五路索引遍历检查列齐全、统计、共享时间戳描述，以及 timestamps／values 的连续覆盖。打开检查不解码所有数值 payload；具体 payload 的编码、长度、行数和时间范围错误在读取时检查。

过滤读取只扫描候选 index。跨 index 连续性校验区分物理相邻索引与被过滤跳过的索引，不能要求跨过未读取范围的偏移直接相接。修改过滤边界时，需覆盖一个 TSID 横跨多个 index，以及原始 metaindex 首 TSID 相等的边界情况。

## 所有权和失败不变量

| 资源或阶段 | 必须核对的行为 |
|---|---|
| 源 part 引用 | 覆盖 reader 使用、目标验证和发布过程；先归还 reader，再释放其依赖的 part 引用。 |
| 全部源 reader | 取得后先登记再 Init；清理包含已出堆和初始化失败的实例。归还前清空堆及当前 TSID 的借用引用，关闭错误不能被取消错误掩盖。 |
| 文件句柄 | 读取原始磁盘数据的 reader 关闭自有的三个 `os.File`；读取降采样数据的 reader 借用 part 句柄，不代替 part 关闭。当前数据读取使用 `os.File.ReadAt`。 |
| 解码缓冲 | 每个 block 重设逻辑长度；`Merge` 返回前关闭 reader，外层在 Finish 和取消检查后通过 `Reset` 归还剩余多特征缓冲。 |
| SpillWriter | 临时文件由组件创建和删除；只允许一次完整流式消费。读取、回调或清理失败均返回错误，删除失败保留后续重试所需路径。 |
| WriteSamples 后段失败 | 前段已写出也必须使整个未发布 writer 失败，并清理 timestamps、spill 和其他目标文件；不能仅丢弃最后一块继续发布。 |
| Finish | 完成最终文件写入、关闭和目录同步；调用方仍须检查取消并验证目标。Finish 不修改活动 part 集合。 |
| 清单提交前 | 不改活动源集合；临时清单与未发布目标由错误路径清理。预算不得依赖预先删除源文件。 |
| 清单提交后 | rename 是提交点。随后同步失败仍须保留目标、更新内存活动集合，并保留旧源磁盘文件；不能因返回错误而 Abort 已发布目标。 |
| 最终刷盘 | 失败后仅对仍在内存的源按原始格式落盘；即使没有剩余内存源，也确认当前清单目录已同步。 |

文件系统可能拒绝关闭、删除或同步。审查错误路径时应核对实际尝试、路径保留和错误传播，不能把“调用了 Abort”写成“任何故障下均已删除全部文件”。普通降采样失败退出当前作业；程序不变量、通用 part 回收和原始数据最终持久化仍有 `Must`／FATAL 路径。

## 当前规模和功能限制

| 项目 | 当前限制或行为 |
|---|---|
| 分辨率与特征 | 固定 5m／1h 和 last／sum／count／min／max；格式定义见设计文档。 |
| Dedup | 降采样要求 dedup 间隔为零。 |
| 月份边界 | 生产选源位于单个 UTC 自然月 partition 内；`downsampleMerger.Merge` 自身不校验所有输入同月。 |
| 源数量 | 后台文件选源最多选择 `defaultPartsToMerge = 15` 个候选；强制归并和刷盘的选源可能更多。merger 没有另设源数量硬上限。 |
| Bucket 缓冲 | 按当前 TSID 的完整时间跨度分配。`downsampleMaxPooledBuckets = 17856` 仅限制 Reset 后的池缓存容量，不限制输入跨度。 |
| Reader 列表缓存 | Reset 中容量超过 1024 时释放列表，仅控制池留存，不限制一次 merge 的源数。 |
| 单个负载与 index | 时间戳或值列磁盘 payload 最多 128 KiB；index 压缩输入最多 128 KiB、解码最多 64 KiB；降采样 block 最多 8192 行，原始输入读取最多 16384 行。 |
| Metaindex | part 打开后常驻；编码和解码长度上限为 64 MiB。该上限不是进程总内存上限。 |
| 总内存与 I/O | 每个源保留索引工作缓冲，并发任务还会增加 reader、writer、bucket 和编码缓冲。磁盘预留额度不约束堆内存或系统页缓存；当前没有统一 merge 内存预算。 |
| 数值 | sum、count 使用 float64；存在浮点舍入、整数计数精度及溢出边界，不能视为任意规模的精确整数或实数计算。 |
| 字段查询 | 指定 `query.field` 只查询已落盘的降采样记录，未指定时只查询原始 part；不自动拼接原始数据与降采样数据，也不为不同 part 的重叠降采样记录专门再聚合。 |
| 查询失败与缓存 | 字段请求要求所有目标 storage 节点成功，并禁用结果缓存。 |

值列读取仍按一个逻辑 block 的五个特征依次发起 `ReadAt`，在同一 values 文件的不同列区段之间切换。共享时间戳避免同一多特征读取中的重复 I/O 和解码，但不代表值列已经改为整列顺序归并，也不构成磁盘吞吐或总内存的性能保证。性能结论应对应测试说明中的实际输入、运行环境和测量结果。
