# 降采样存储测试说明

本文说明当前测试的用途、运行方式和判定条件。存储语义与[文件布局](downsampling_storage_design.md#3-文件布局)见[设计文档](downsampling_storage_design.md)，生产代码入口见[实现说明](downsampling_storage_implement.md)，审查关注点见[审查说明](downsampling_storage_review.md)。

## 运行方式

以下命令均在仓库根目录执行。Go 版本应满足当前 `go.mod`；Python 测试使用 Python 3，文件检查器还需要系统可加载的 `libzstd`。`-count=1` 禁用 Go 测试结果缓存，`-E` 避免 Python 环境变量影响断言执行。

```sh
# storage 全量回归与 filestream 单元测试
go test ./lib/storage -count=1 -timeout=10m
DISABLE_FSYNC_FOR_TESTING=false go test ./lib/filestream -count=1 -timeout=3m

# 降采样查询、HTTP 参数、查询配置和集群 RPC
go test -p 4 ./lib/vmselectapi ./app/vmstorage ./app/vmselect/netstorage ./app/vmselect/promql ./app/vmselect/prometheus -run '^Test(Downsample|Downsampling)' -count=1 -timeout=3m

# Python 输入、参考计算、集群运行器和文件检查器单元测试
python3 -B -E -W error::ResourceWarning -m unittest discover -s lib/storage/testdata -p 'test_downsampling_*.py' -v

# 降采样及相关 I/O 的并发与静态检查
DISABLE_FSYNC_FOR_TESTING=false go test -race ./lib/filestream ./lib/storage -run '^Test(Spill|ReaderAt|Downsample|Downsampling|UnmarshalDownsample|CheckDownsampling|MustOpenStorageDownsampling|EstimateDownsample|ReserveDownsample|Block)' -count=1 -timeout=5m

# 查询文件读取、索引缓存及关闭回归；分别使用默认 mmap 配置和显式禁用 mmap 配置。
go test ./lib/storage -run '^TestDownsampleQueryIndexAccess$' -count=1 -timeout=3m
go test ./lib/storage -run '^TestDownsampleQueryIndexAccess$' -count=1 -timeout=3m -fs.disableMmap=true

# 并发查询跨越归并发布，检查旧 part 引用、读取及删除时机。
DISABLE_FSYNC_FOR_TESTING=false go test -race ./lib/storage -run '^TestDownsamplePartitionConcurrentQueryMerge$' -count=1 -timeout=2m

go vet ./lib/filestream ./lib/storage ./lib/vmselectapi ./app/vmstorage ./app/vmselect/netstorage ./app/vmselect/promql ./app/vmselect/prometheus
```

单独检查一个问题时，使用下文中的测试名，例如：

```sh
go test ./lib/storage -run '^TestDownsampleMergerSourcePrecision$' -count=1 -v
```

Go 单元测试、race 和 vet 均以退出码 0 为通过条件；Python 单元测试应显示 `OK` 且退出码为 0。测试输出默认写入终端，Go 测试的 `t.TempDir()` 目录随测试清理，不产生持久化验收清单。需要保存日志时可自行重定向输出。

## storage 单元测试

降采样生产代码分为 9 个文件；测试文件均位于 `lib/storage`，使用 `package storage`，按生产文件归类为 9 个同名 `_test.go`，另有 `downsample_supplement_test.go`。测试使用独立的原始输入参考值或原生编码器校验结果；实际文件布局另有按固定字节偏移解析的检查。下文按对应生产逻辑列出测试及覆盖范围。

### 配置、快照与基础分辨率持久化

生产文件：[downsample_config.go](lib/storage/downsample_config.go)；测试文件：[downsample_config_test.go](lib/storage/downsample_config_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsamplingConfigJSON` | 自定义基础分辨率、租户额外分辨率排序、未配置租户回退基础、JSON 往返、part 租户子集和公开访问器返回副本。 |
| `TestDownsamplingConfigRejectsInvalidJSON` | 拒绝未知／重复字段、null、重复租户或分辨率、非规范租户、非整数倍、非法时间间隔和超长 JSON。 |
| `TestDownsamplingConfigMetadataSize` | 请求 JSON 未满 64 KiB 但规范化 metadata 会超限时拒绝；查找可接受边界，使用生产 metadata 编码核对最大字段长度、下一租户超限及嵌套配置读回。 |
| `TestDownsamplingConfigSnapshots` | 运行时更新不改变旧任务快照，公开副本不能修改 storage，基础分辨率不可更改；并发读取和更新快照保持完整。 |
| `TestDownsamplingBasePersistsWithoutParts` | 预检查只读；首次启用即保存基础分辨率，快照包含该标志，没有活动 part 时重启仍拒绝修改基础。 |

多数既有文件测试显式配置 5m 基础和测试租户的 1h 额外列；这不是生产默认配置。默认仅 5m，自定义和稀疏分辨率由配置、writer、part、merger 与查询专门用例覆盖。

### 样本与解码 block

生产文件：[downsample_block.go](lib/storage/downsample_block.go)；测试文件：[downsample_block_test.go](lib/storage/downsample_block_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleDecodedBlockCapacity` | Reset 清除 TSID、分辨率和精度；时间列与五个特征列保留正常容量，释放超过池阈值的缓冲。 |
| `TestDownsampleBucket` | 5m/1h 的区间边界、非法时间和分辨率、时间域最后一个区间的右端点。 |
| `TestDownsampleSampleMergeNormalization` | 首次 Merge 保留五列的有限值、正负零和无穷值位模式；不同 NaN 位模式统一为 StaleNaN，源样本保持不变。 |
| `TestDownsampleSampleMergeRaw` | 将原始样本展开为五个特征，检查重复时间戳、乱序输入、时间戳相同时 last 的比较规则，以及 NaN/Inf 的计算规则。 |
| `TestDownsampleSampleMerge` | 合并已有五特征样本，累加非整数 count；1/32/64 精度继承、首次复制、重复及自引用输入、源样本不变。 |
| `TestDownsampleSampleNaNColumns` | 检查五列分别出现 NaN、输入次序变化、sum/count 异号无穷相加及自合并的结果，验证 min/max 遇到 NaN 与无穷时仍传播 NaN，并确认源样本未被修改。 |
| `TestDownsampleSampleReset` | 空状态、带精度的 StaleNaN 样本、重复 Reset、清空后使用另一精度。 |
| `TestDownsampleSampleRandomizedReference` | 原始样本直接聚合、多级分组合并、5m 再合并为 1h，与独立参考值一致。 |
| `TestDownsampleHeaderValidation` | 拒绝 header 的非法行数、精度、编码、时间、负载大小与偏移，检查单行二阶差分编码和截断。 |

### metaindex 行编码

生产文件：[downsample_metaindex_row.go](lib/storage/downsample_metaindex_row.go)；测试文件：[downsample_metaindex_row_test.go](lib/storage/downsample_metaindex_row_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleMetaindexTimeBounds` | 拒绝 metaindex 时间范围低于或高于支持的时间域。 |

### 磁盘 part 与元数据

生产文件：[downsample_part.go](lib/storage/downsample_part.go)；测试文件：[downsample_part_test.go](lib/storage/downsample_part_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleFormatDetection` | 仅有 metadata 时即可识别合法降采样或原始格式；配置缺失或 null 按原始统计校验，非 null 的无效配置报错。缺少 metadata 的旧原始格式仍按目录名识别。拒绝新目录缺失 metadata、空文件、截断或非法 JSON、null 和空对象；降采样实际打开仍拒绝缺失数据文件。 |
| `TestDownsamplePartOpenCancellation` | 打开前已取消时不访问目标文件；索引读取期间取消后停止后续长扫描，返回取消错误。 |
| `TestDownsampleMetadataValidation` | 非 null 配置存在时，拒绝五个统计字段缺失或为 null、配置无效，以及超出支持时间域的 part 统计；有效配置与统计可往返。 |
| `TestDownsamplePartTenantResolutions` | 接受稀疏租户分辨率布局，区分已配置和实际落盘的额外列；拒绝额外 TSID 没有基础数据、租户未配置的额外列，以及整个基础分辨率区段缺失。 |
| `TestUnmarshalDownsampleIndexBlock` | 解码结果可追加到既有 header 缓冲；截断、非法 header 及统计不一致时保留原前缀，不暴露部分解码结果。 |
| `TestDownsampleRejectsInvalidMarker` | 损坏的 metaindex/index 固定文件签名不影响 metadata 格式识别，实际打开 part 时必须拒绝。 |
| `TestDownsampleLayoutMetaindexIdentityCorruption` | 拒绝 metaindex 中非法特征、分辨率或不一致的租户身份。 |
| `TestEstimateDownsamplePartSize` | 分别估算原始源、降采样源及混合源的输出空间，检查空输入、非法输入和溢出边界。 |
| `TestDownsamplePartIndependentFeatureIndexes` | 单独改变 sum 列的 index 分块边界，保持其他四列不变；part 打开校验按逻辑 Block 对齐五列，接受分块边界不同的合法文件。 |

### 顺序读取、header 缓存与文件所有权

生产文件：[downsample_reader.go](lib/storage/downsample_reader.go)；测试文件：[downsample_reader_test.go](lib/storage/downsample_reader_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleFileRoundtrip` | 多 TSID、两个分辨率、五特征及 NaN 的完整写入和读回；物理统计与格式识别。 |
| `TestDownsampleFileConstantAndEmptyResolution` | 常量 count 列使用零负载；完整扫描读回首个 block，没有数据的分辨率正常结束。 |
| `TestDownsampleReaderCancellation` | 取消后首列推进和其他特征的向前对齐均退出，关闭时清除信号引用，避免影响复用实例。 |
| `TestDownsampleReaderRawInmemory` | 读取原始内存 part，展开五特征；普通值和 StaleNaN 各贡献一次 count。 |
| `TestDownsampleFileAllConstantPayloads` | 单行数据的时间列和全部特征列均为零负载，空文件仍可识别格式并完整读回。 |
| `TestDownsampleReaderReadErrors` | values 文件在打开后被截断时，读取必须返回错误。 |
| `TestDownsampleCodecRowCountAndDecodeLimit` | 拒绝单行二阶差分编码，以及解压后超过声明行数所允许大小的列负载；检查 `checkDownsampleExtent` 对负载偏移溢出的拒绝。 |
| `TestDownsampleReaderLargeRawBlock` | 完整读取原始链路允许的双倍行数 block，五特征展开不截断。 |
| `TestDownsampleReaderResolutionAndSharedTSID` | 按分辨率完整扫描，不遗漏同一 TSID 横跨相邻 index 的 block；首列推进到下一 TSID 或 EOF 后，按缓存 header 正确读回五列，首列 header 保持不变。 |
| `TestDownsampleReaderRawEqualBoundaryAndReuse` | 两轮 Init 后完整扫描原始 part，覆盖相邻 metaindex 首 TSID 相等和同 TSID 的重叠 block；按保存的 header 读回数据不改变已推进的首列位置，同一源复用文件句柄。 |
| `TestDownsampleClusterTenantIndexBoundaries` | 按完整 TSID 顺序读回 AccountID、ProjectID 和相邻 index 的首尾项，五特征齐全，不混入其他租户。 |
| `TestDownsampleIterationReaderReuse` | 多次切换分辨率并重新初始化后完整扫描，每次多特征 ReadBlock 同时核对五列，结果不遗漏或重复。 |
| `TestDownsampleReaderCrossIndexOffsets` | 拒绝跨 index 的非法偏移和负载范围。 |
| `TestDownsampleReaderCrossIndexInitAndReset` | 完整扫描跨 index 的数据，Close 清除上一次索引边界状态，重新初始化后可再次完整读取。 |
| `TestDownsampleLayoutIndexTenantCorruption` | 打开 part 及逐 header 读取均拒绝 index 内混入其他租户。 |
| `TestDownsampleReaderDuplicateBatchKey` | 拒绝降采样 block 的重复键，包括跨 index 边界的重复。 |
| `TestDownsampleReaderRawBlockOrder` | 同一 index 内及跨 index 均允许原始 block 的时间范围相等或重叠，拒绝同 TSID 的最小时间戳倒退；结束或错误后清除当前 header，保留读取错误。 |
| `TestDownsampleReaderCloseFiles` | 通过测试 reader 注入关闭错误，验证关闭全部自有文件并保留错误；重复 Close 不再关闭文件。 |
| `TestDownsampleReaderFilesIndependentFromQueries` | 降采样 reader 自有三个文件，顺序读取五特征。Close 恰好关闭各文件一次，part 的三个 `*fs.ReaderAt` 查询对象仍可读取。 |
| `TestDownsampleReaderInitFailureReleasesPartialSource` | 新源初始化中途失败时释放已打开的文件，清除状态后可重新初始化。 |
| `TestDownsampleReaderInitCloseFailure` | 换源时旧句柄关闭失败，保留实际错误，不继续打开新源；同源切换分辨率遇到非法 feature 时也释放全部句柄及 header 缓存，保留校验与关闭错误，重复 Close 不再次关闭文件。 |
| `TestDownsampleReaderSharedTimestampsValuesOnly` | 首列解码后关闭独立时间戳句柄，后四列仍只解码 values 并复用同一时间戳缓冲；下一次多特征读取必须重新读时间戳；独立单列读取仍由原生 Block 完成。 |
| `TestDownsampleReaderSharedTimestampsSwitches` | block、TSID、源 part、分辨率切换后，与各列单独使用原生 Block 读取的结果一致。 |
| `TestDownsampleReaderSharedTimestampsValidation` | 拒绝不一致的共享时间列描述及损坏的 values。 |

### 写入、布局与失败清理

生产文件：[downsample_writer.go](lib/storage/downsample_writer.go)；测试文件：[downsample_writer_test.go](lib/storage/downsample_writer_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleWriterOrderAbort` | 允许按 TSID 交错写入 5m／1h，拒绝同一分辨率中的 TSID 倒退；失败后清理全部分辨率。 |
| `TestDownsampleWriterExistingDirectory` | 拒绝已有目录，随后 Abort 不删除该目录中原有文件。 |
| `TestDownsampleWriterBufferCapacity` | writer 的整数、单列浮点值和时间戳缓冲在 reset 后清空内容、保留正常容量，并释放异常容量。 |
| `TestDownsampleNativeBlockReuse` | writer 逐列复用一个原生 Block，各特征输出与独立原生编码参考一致；数值保留 8/64 源精度，时间戳独立按 64 位无损编码；共享时间戳、查询 BlockRef 读取和编码状态一致。小数据不创建 spill 临时文件，完成后只留下五个最终文件。 |
| `TestDownsampleFilePhysicalLayout` | 独立按 89 字节 header、113 字节 metaindex 解析真实文件；四个子场景为多 block/分辨率、738 批次跨 index、租户切 row、零负载列。检查列顺序、共享时间戳、连续偏移、文件完整覆盖及统计。 |
| `TestDownsampleWriterTenantResolutionLayout` | 按 TSID 写入三个租户的 5m／30m／1h／2h 稀疏列；独立解析真实 bin，核对分辨率、特征、租户顺序、共享时间偏移及负载，metadata 只保留实际租户配置。 |
| `TestEstimateDownsampleOutputSize` | 用独立布局公式核验最终文件与 spill 各一份共享时间列、五特征、索引和 metadata 的空间预算，以及每行和每批次的增量。 |
| `TestDownsampleSpaceEstimateOverflow` | 用大整数参考检查空间估算、加法和乘法的饱和边界，避免回绕或提前饱和。 |
| `TestDownsampleSpaceBoundCoversEncodedParts` | 实测最终文件与 spill 的保守峰值包络不超过预算；覆盖满 block、单行多 TSID 和频繁 index 切换。 |
| `TestDownsampleAvailableSpaceBoundaries` | 检查安全余量、已预留空间和请求量的边界与溢出；拒绝非法写入行数，并保留空间不足错误标记。 |
| `TestDownsampleWriterFinishCancellation` | Finish 前已取消时不写最终 payload；首个 timestamps payload 写入后取消时中止后续组装。两种情况均返回取消错误、完整删除目标，最终句柄恰好关闭一次。 |
| `TestDownsampleWriterFinalFileFailures` | 四个最终二进制文件分别注入接口返回的写错误及短写；原错误不丢失、每个句柄只释放一次、整个未发布目录删除。 |
| `TestDownsampleWriterMetadataCompletion` | 四个 bin 逐一关闭时，metadata 不得提前出现；任一关闭后的模拟中断不产生完成标志，Abort 清理目录且各句柄只关闭一次。成功输出的 metadata 统计正确、part 可打开；空输出无完成标志。 |
| `TestDownsampleWriterSpillAndValidationFailures` | 截断 header 或 last spill 的共享时间列、后续输入无效、写入未配置租户分辨率、后续特征写入失败；即使同批次的时间戳和前几列已写入，也完整清理目标并禁止发布，随后同一 writer 可重新初始化。 |
| `TestDownsampleWriterFinalFilePermissions` | 最终文件权限与同进程使用 `os.WriteFile(..., 0666)` 创建的参考文件一致。 |
| `TestDownsampleWriterAbortRetriesOnlyDirectory` | 目录连续删除失败后只重试目录，已释放文件和 spill 不再关闭；保留首次写入错误及各次清理错误，清理完成后可复用 writer。 |
| `TestDownsampleWriterFinishedTargetOwnership` | Finish 完成后保留目标，重新初始化及归还对象池不重复关闭或删除已完成文件；显式 Abort 可删除当前未发布目标。 |
| `TestDownsampleWriterPoolKeepsPendingCleanup` | 归还 writer 时若目录仍无法删除，保留路径和错误，不将对象放回池中。 |
| `TestDownsampleWriterSamplesPreserveInput` | 8/64 精度下写入经 Merge 构造的样本，包含由非标准 NaN 规范化得到的 StaleNaN，跳过前、中、后空槽；空输入不生成 block；写入后输入逐 bit 不变，读回与原生编码参考一致。 |
| `TestDownsampleWriterSamplesCancellationAfterOutput` | 已写入一批样本后取消后续写入，返回取消错误并清理既有输出、spill 和句柄，禁止发布。 |

### 归并与 reader 释放

生产文件：[downsample_merger.go](lib/storage/downsample_merger.go)；测试文件：[downsample_merger_test.go](lib/storage/downsample_merger_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleMergerMemoryBudget` | 用小额预算分别触发 header、密集基础槽、稀疏映射和派生输出的分配失败；验证返回错误、释放全部计费容量，并由 Abort 清理未发布目录。 |
| `TestDownsampleMergerMemoryGrowthAndSharedBudget` | 多个 merger 共享额度，扩容同时计入新旧容量；申请失败保持已有数据，释放后可继续申请，reset 完整归还。 |
| `TestDownsampleMergerTenantResolutions` | 三租户六 TSID、原始内存与磁盘源混合；更新租户配置后合并新旧降采样 part，额外列只从基础列重建，重写及旧 part 快照保持正确。 |
| `TestDownsampleMergerArbitraryBaseAndSparseRange` | 1ms／10m 基础分辨率下，三条样本跨完整支持时间域；结果正确且不按巨大空白范围分配基础槽。 |
| `TestDownsampleMergerDerivedCancellationAndPrecision` | 派生 bucket 精度冲突、已取消的派生聚合和未初始化 writer 均返回错误。 |
| `TestDownsampleMergerRetainedPrefixThroughQueryDivisor` | 旧 2h 配置变为 1h 时保留基础前缀；派生列完整表达该前缀，2h 查询选 1h 输入后五特征仍包含全部贡献。 |
| `TestDownsampleMergerMoreThan1024Sources` | 一次合并 1025 个独立 part，同 bucket 的贡献不遗漏、不重复，两个分辨率及统计正确。 |
| `TestDownsampleMergerRepeatedMerge` | 原始样本首次降采样、原始 part 与降采样 part 合并、已有降采样 part 重写，以及同一 merger 再处理其他 TSID。 |
| `TestDownsampleMergerSourcePrecision` | 低精度继承；同 bucket 的 8/64 精度冲突返回错误；跨 bucket 的 8→64→8 切换拆成不同 block，重写后保持正确。 |
| `TestDownsampleMergerDynamicBucketRange` | 按当前 TSID 各源基础 header 的 min/max 和源行数选择密集槽或实际 bucket 映射；多源及重叠 block、短/长/短序列、稀疏空槽、两个分辨率和实例复用。合成跨月输入仅用于直接调用 merger 的范围测试。 |
| `TestDownsampleMergerReadsIndexesOnce` | 降采样磁盘、原始磁盘和原始内存混合源，包含跨 index 的长 TSID、派生分辨率和部分删除；记录实际磁盘 index 偏移，核对基础索引只读一次、旧额外索引不读、缓存只含当前 TSID 且消费后清空、首列 header 不变，以及派生结果和基础物理行统计。 |
| `TestDownsampleMergerRetentionAndDeletedMetricID` | 按 bucket 右端点判定保留期限，删除指定 MetricID，重写时保留尚有效的 1h 贡献。 |
| `TestDownsampleMergerEmptyTSIDBeforeRetainedTSID` | 前一个 TSID 全部过期后不输出空 block，下一 TSID 的样本和精度不受影响。 |
| `TestDownsampleMergerSpecialValues` | NaN、正负无穷值经过首次合并和重写后保持五特征语义。 |
| `TestDownsampleMergerSummaryPhysicalStatistics` | 输出按实际分辨率和五特征统计，输入只统计基础物理行；旧额外列和删除时间线不重复计数。 |
| `TestDownsampleMergerOutputBlockBoundary` | 连续与稀疏两种输入均含 8193 个有效 bucket；按有效行数拆出 8192 行 block 和尾块，不计入空槽。 |
| `TestDownsampleMergerCancellation` | 已取消任务不统计尚未读取的数据；调用方执行 Abort 后删除目标目录，merger 可重新使用。 |
| `TestDownsampleMergerSharedPrecisionAndColumnScales` | 各特征独立 scale、五列共享时间戳和精度，多轮合并与原生编码参考结果一致。 |
| `TestDownsampleMergerResetClosesAllReaders` | reset、重新初始化源和开始另一归并时释放全部基础 reader，包括已经离开堆的实例；保留关闭错误并清除引用。 |
| `TestDownsampleMergerClosesBeforeReturning` | 成功、关闭失败、写入与关闭同时失败、取消与关闭同时失败时，归并返回前释放全部 reader 和引用，保留实际错误。 |

### 分区、启动、发布与空间预留

生产文件：[downsample_partition.go](lib/storage/downsample_partition.go)；测试文件：[downsample_partition_test.go](lib/storage/downsample_partition_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleFailureBeforePublication` | 取消、提交前取消、打开目标前注入错误、目标无效、提交前注入错误均保留源和原清单，清除未发布目标。 |
| `TestDownsamplePartitionCancelledWhileWaitingForMergeSlot` | 并发队列已满时，取消使降采样任务及时退出并释放源的合并标记，不占用或归还其他任务的名额。 |
| `TestDownsampleManifestTemporaryFileCollision` | 临时清单使用独占子目录，保留已有暂存内容；成功发布或取消后均清理本次子目录，取消时原清单保持不变。 |
| `TestDownsampleFailureSchedulers`、`TestDownsampleFailureStopsRemainingBatches` | 各调度入口收到 I/O 错误或取消后退出，停止继续调度剩余批次。 |
| `TestDownsampleFailureFinalFlushPreservesRaw`、`TestDownsampleFailureFinalLifecycle` | 最终刷盘失败时，改以原始格式持久化数据；关闭存储和创建快照的过程中不丢数据。 |
| `TestDownsampleFailureAfterPublication` | 在提交后的同步测试钩子注入错误，新目标与活动清单保持一致，旧磁盘源仍保留；不把已提交目标当作未发布结果删除。 |
| `TestDownsampleFailureFinalSyncAfterPublication` | 在提交同步测试钩子注入错误后，最终刷盘仍再次同步目录；已发布的降采样目标不会被回退流程生成的原始文件覆盖。 |
| `TestCheckDownsamplingOpen` | 活动清单与格式检查；覆盖空目录、原始与降采样 part 混合、快照与临时目录、部分删除、缺失 part 和损坏 metadata。 |
| `TestCheckDownsamplingOpenInvalidManifests` | 拒绝非法 JSON、未知或重复字段、重复 part、非法名称、路径和尾部数据。 |
| `TestMustOpenStorageDownsamplingRejectsDedup`、`TestMustOpenStorageDownsamplingRejectsDisabledModeBeforeBackgroundWork` | 拒绝降采样与非零 dedup 同时启用；未启用降采样却存在活动降采样 part 时，在后台任务启动前失败。 |
| `TestCheckDownsamplingOpenDisabledPreservesDedup` | 未启用降采样时，启动检查不改变原有去重配置与存储文件。 |
| `TestDownsamplePartitionInmemoryAndFailedOutput` | 内存 part 之间合并后仍保持原始格式；降采样输出失败时不替换源 part。 |
| `TestDownsamplePartitionSmallBigMerge` | 分别生成 small 与 big 源，再合并为单个 big 目标，验证源列表变化和结果。 |
| `TestDownsamplePartitionConcurrentQueryMerge` | 原始磁盘及降采样磁盘源各由十个查询持有旧 part 快照，贯穿归并和新 part 发布；发布后旧 BlockRef 仍能读取，最后一个查询结束才删除旧 part，新 part 的两分辨率、五特征结果正确。降采样源包含同一 TSID 跨 index 的情况。 |
| `TestDownsamplePartitionRetentionBoundaries` | 7h bucket 跨月时，part 和整月清理均保护其前缀；删除当前额外配置后，旧 part 配置继续生效，完全过期后释放，关闭降采样时原始边界不变。 |
| `TestDownsamplePartitionInmemoryRetention` | 调用真实原始内存归并：降采样启用时透传保守边界并保留粗粒度 bucket 前缀，关闭时仍按原始保留期限删除。 |
| `TestDownsamplePartitionPartExpired` | 启用降采样时，原始内存、原始磁盘和降采样 part 均受配置的保守 bucket 边界保护；关闭降采样后使用原始过期边界。 |
| `TestDownsamplePartitionFileOutput` | 单个内存 part 首次落盘及多个内存 part 合并落盘，均通过正常分区入口生成单个降采样 part，结果与原始输入参考一致。 |
| `TestDownsampleRecoveryKeepsCommittedTargetAfterPanic` | 清单提交后在旧源回收处注入 panic；已提交目标仍可独立打开，存储重新打开后数据不变。 |
| `TestDownsampleRecoveryDiscardsUnpublishedPart` | 再打开时清理未被活动清单引用的残缺 part 和临时清单目录，保留原活动清单与数据。 |
| `TestReserveDownsampleSpaceConcurrentAndCachedFreeSpace` | 同一文件系统并发预留不超额；释放幂等，已释放预算须等空闲空间缓存更新后才能重用。 |

### 降采样查询与集群协议

生产文件：[downsample_query.go](lib/storage/downsample_query.go)；测试文件：[downsample_query_test.go](lib/storage/downsample_query_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleIterationMultiTSID` | 180 条时间线及同一 TSID 跨相邻 index；全部、非连续和缺失 TSID，边界时间窗，两个分辨率和五特征的查询遍历。 |
| `TestDownsampleQueryParse` | 接受动态合法分辨率与固定五特征；分辨率和特征均为空时使用原始查询路径，缺少其一或非法值返回错误。 |
| `TestDownsampleQueryBlockRef` | 两分辨率、五特征及多 TSID 筛选；同 MetricID 的不同 JobID 和租户按完整 TSID 区分，指定分组仅返回该组，缺失分组为空。另以 40 行不规则时间戳与小数值检查共享精度 64。各场景均经原生 header 序列化及 BlockRef 解码，检查未指定降采样选择及范围外查询为空。 |
| `TestDownsampleQueryRawIsolationAndReset` | 降采样查询不读取原始 part；同一 partSearch 恢复原始查询后仍能读取原始样本。 |
| `TestDownsampleSearchProtocolValidation` | 设置降采样选择不改变原生负载；扩展编码保留原生前缀，拒绝截断及非法分辨率或特征；原生解码清除残留选择，并保留扩展尾部供 RPC 严格拒绝。 |
| `TestDownsampleQueryMemoryBudget`、`TestDownsampleQueryConcurrentMemoryBudget` | 用小额预算验证重复贡献和跨 TSID 容量复用不重复计费、超限前停止分配、并发查询共享总额，以及重复 reset 不重复归还。 |
| `TestDownsampleQueryMemoryFailureStopsIteration` | 真实查询迭代中耗尽预算，返回错误并停止，不返回未完整聚合的 TSID；reset 后归还额度。 |
| `TestDownsampleQueryOutputDeadline` | 输出开始及过滤循环检查原有查询期限，超时终止而不继续输出。 |
| `TestDownsampleQueryIndexAccess` | 十种分辨率与特征组合均只读取选定列的 index；三个查询文件使用实际 `*fs.ReaderAt`，首次读取和关闭核验 mmap 资源的建立及释放，重复查询命中原有 `ibCache`。支持默认 mmap 配置及 `fs.disableMmap=true`；损坏索引的解码错误保留原因并终止迭代，后续调用不再次读取。 |
| `TestDownsampleQuerySearchPropagation` | 在非零租户写入并归并后，从 Search 入口验证两分辨率、五特征及共享时间戳；同一 Search 对象再次执行原始查询时，不保留上次的降采样选择。 |
| `TestDownsampleQuerySourceTimeRange` | 查询边界扩展到完整目标 bucket，并钳制到有效时间域。 |
| `TestDownsampleQueryLargestStoredDivisor` | 多租户各自选择最大已存整除分辨率，覆盖 2h／90m／1h 请求和 1h／45m／30m／5m 源；拒绝与基础分辨率不兼容的请求。 |
| `TestDownsampleQueryBucketsAcrossPartsAndPartitions` | 同 bucket 跨 block、index、part 和月份分区完整聚合五特征；先完整聚合再按最大时间戳筛选，保存的 BlockRef 在搜索重置后仍可读取。 |

### 跨模块验证与共享测试工具

测试文件：[downsample_supplement_test.go](lib/storage/downsample_supplement_test.go)。保存以下两个跨模块测试，以及全部共享测试数据构造、独立参考计算、故障注入、断言和基准辅助函数。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleClusterTenantIsolation` | 多租户写入、合并与降采样查询保持隔离。 |
| `TestDownsampleStorageLifecycle` | 写入、刷盘、强制合并、快照、关闭和重新打开的完整存储生命周期。 |

storage 全量回归还会运行原始存储链路的已有测试，包括 `block_test.go` 中的 `TestBlockMarshalUnmarshalPortable`、`TestBlockUnmarshalPortableDoS`，用于验证原生 Block 编解码与异常输入限制。

## app 与 API 单元测试

| 文件及测试 | 验证内容 |
|---|---|
| [app/vmstorage/vmstorage_test.go](app/vmstorage/vmstorage_test.go)：`TestDownsamplingConfigHTTP` | GET／PUT 配置接口、租户配置替换、非法请求、基础分辨率变更拒绝、HTTP 方法及认证。 |
| [app/vmselect/prometheus/downsample_query_test.go](app/vmselect/prometheus/downsample_query_test.go)：`TestDownsampleQueryHTTPParameter` | HTTP 瞬时查询和范围查询接口分别解析 resolution 和 feature；拒绝缺少其一、空值、重复参数、错误转义、不支持的组合及旧参数 query.field。 |
| [app/vmselect/promql/downsample_query_test.go](app/vmselect/promql/downsample_query_test.go)：`TestDownsampleQueryCacheAndCopy` | 降采样查询禁用未区分特征的结果缓存，子查询配置复制保留分辨率、特征与禁用缓存的状态。 |
| [app/vmselect/netstorage/netstorage_test.go](app/vmselect/netstorage/netstorage_test.go)：`TestDownsampleSearchRPCDispatchAndPartialResults` | 原始、5m sum、1h max 查询实际发送逐租户 RPC 并接收原生 block；另一节点返回可按原有策略容错的过载错误，分别验证允许部分响应、禁止部分响应时返回 HTTP 503，以及 replicationFactor=2 时返回完整响应。 |
| [lib/vmselectapi/downsample_search_test.go](lib/vmselectapi/downsample_search_test.go)：`TestDownsampleSearchRPCDispatch` | 原生与降采样 RPC 分发、租户和十种分辨率与特征组合传递，返回实际 block。 |
| 同文件：`TestDownsampleSearchRPCRejectsInvalidPayload` | 拒绝分辨率或特征缺失、截断、非法值及多余尾部，错误请求不进入搜索。 |

节点结果收集统一走原有 `collectResults`，不另设降采样专属的“全部节点成功”断言或通用错误前缀断言。参数解析、租户与分辨率／特征传递、线协议校验及结果缓存旁路仍由上述测试验证。

## filestream 单元测试

`go-io-ut` 实际运行 `go test -p 4 ./lib/filestream -count=1`，包含该包全部测试。新增组件的错误处理由 spill 和偏移 reader 用例验证；最终文件创建、关闭及目录同步继续采用共享 Must 语义，不属于降采样可恢复错误测试。

| 文件及测试 | 验证内容 |
|---|---|
| [spill_writer_test.go](lib/filestream/spill_writer_test.go)：`TestSpillWriterRoundTrip`、`TestSpillWriterLazyCreationAndDiscard` | 纯内存和混合存储的完整读写；恰好达到阈值时不创建文件，多次落盘只用同一个文件，尾部保持内存；小数据不依赖临时目录存在。 |
| 同文件：`TestSpillWriterInvalidMemoryLimit` | 零或负阈值立即返回错误并清理，避免写入循环无进展。 |
| 同文件：`TestSpillWriterSharedMemoryBudget`、`TestSpillWriterBudgetFallbackFailure` | 多个 spill 共享容量，扩容同时计入新旧缓冲，预算不足直接落盘；验证数据完整、写出失败清理、额度复用与重复 Close 不重复归还。 |
| 同文件：`TestSpillWriterAppendAfterIncompleteRead` | 不完整读回之后继续追加，文件偏移仍指向末尾，既有数据不会被覆盖。 |
| 同文件：`TestSpillWriterMetrics` | 缓冲与实际 I/O 分别计数，读写实例计数在资源释放后恢复。 |
| 同文件：`TestSpillWriterPermissions`、`TestSpillWriterCreateFailure` | 权限与同进程创建的文件一致，创建失败返回原错误。 |
| 同文件：`TestSpillWriterWriteFailures`、`TestSpillWriterLaterDumpFailure`、`TestSpillWriterSeekFailures` | 满块写出时的错误、短写及后续满块失败，返回的已接收字节数与实际落盘字节数分别核验；文件长度查询及回到起点失败的错误传播与清理。 |
| 同文件：`TestSpillWriterReadFailures`、`TestSpillWriterConsumerFailures` | 读错误、截断、无进展、回调错误、消费不完整均不能当作成功。 |
| 同文件：`TestSpillWriterReadIsReadOnlyAndRepeatable` | 精确消费全部字节无需额外读取 EOF；Read 不关闭或删除文件，可重复读取完整字节流，最后由 Close 释放资源。 |
| 同文件：`TestSpillWriterCleanupRetry`、`TestSpillWriterCloseErrorStillRemoves`、`TestSpillWriterWriteErrorCleanupRetry` | 删除失败可重试；关闭失败仍尝试删除；写失败后的清理重试保留原错误。 |
| 同文件：`TestSpillWriterBoundedBuffers`、`TestSpillWriterIndependentInstances` | 内存缓冲按需增长，容量不超过配置阈值；单次超大输入按块处理；多个实例的文件和状态彼此独立。 |
| 同文件：`TestSpillWriterInputDoesNotAlias`、`TestSpillWriterCloseDiscardsMemoryTail` | 写入后修改输入不影响已接收数据；关闭时丢弃内存尾部并删除已落盘前缀，不为销毁而追加写文件。 |
| 同文件：`TestSpillWriterFileExtentValidation`、`TestSpillWriterTruncatedPrefixCannotUseMemoryTail` | 拒绝文件前缀截断或多余字节；即使读取途中发生截断，也不能用内存尾部补齐缺失的文件数据。 |
| [reader_at_test.go](lib/filestream/reader_at_test.go)：`TestReaderAtOffsetsAndCursor`、`TestReaderAtConcurrentReads` | 乱序偏移读取互不影响，不改变顺序游标；多个 goroutine 读取同一文件的不同区域。 |
| 同文件：`TestReaderAtTruncatedFile`、`TestReaderAtReadFailuresAndCounts` | 截断、EOF、读取错误、短读及非法计数；实际 I/O 计数准确，错误后可再次读取其他偏移。 |
| 同文件：`TestReaderAtOpenFailures`、`TestReaderAtRejectedFileCleanup` | 不存在的文件、目录、Stat 失败及无效大小；初始化失败关闭句柄，保留原始错误和清理错误。 |
| 同文件：`TestReaderAtInvalidOffsets`、`TestReaderAtCloseOnce` | 拒绝负偏移和范围溢出；关闭只执行一次，保留关闭错误，关闭后读取返回 ErrClosed。 |
| [filestream_test.go](lib/filestream/filestream_test.go)：`TestWriteRead` | 完整文件写入、顺序读取和内容往返。 |

## 性能测试

四个入口分别位于样本、归并、读取和写入对应的测试文件：

```sh
go test ./lib/storage -run '^$' -bench '^BenchmarkDownsample' -benchmem -count=1
```

| 文件及 Benchmark | 衡量内容 |
|---|---|
| [downsample_block_test.go](lib/storage/downsample_block_test.go)：`BenchmarkDownsampleSample/Raw`、`/Sample` | 分别使用 8192 条原始样本和五特征样本，测量纯计算吞吐和内存分配。 |
| [downsample_merger_test.go](lib/storage/downsample_merger_test.go)：`BenchmarkDownsampleMerge` | `Dense`、`Sparse`、`HighCardinality` 三种负载；分别测量原始源合并、多个降采样源合并，以及已合并降采样 part 的重写成本。子模式名为 `Raw`、`Summary`、`SummaryRewrite`。 |
| [downsample_reader_test.go](lib/storage/downsample_reader_test.go)：`BenchmarkDownsampleFileRead` | 三种负载下遍历两个分辨率并解码全部特征的成本。 |
| [downsample_writer_test.go](lib/storage/downsample_writer_test.go)：`BenchmarkDownsampleFileWrite` | 样本直接进入 writer 后的编码、写文件和同步关闭成本；测试数据转置、目录统计和删除不计时。 |

SpillWriter 单独测量纯内存和超阈值落盘两条路径：

```sh
go test ./lib/filestream -run '^$' -bench '^BenchmarkSpillWriter' -benchmem -count=1
```

| 文件及 Benchmark | 衡量内容 |
|---|---|
| [spill_writer_test.go](lib/filestream/spill_writer_test.go)：`BenchmarkSpillWriter/Memory`、`/Spilled` | 分别写入并完整读回 1 MiB 和 17 MiB；测量内存复制、按需落盘、流式读取及资源清理的耗时和分配。输入在计时前准备，消费端直接丢弃读出数据。 |

结果包含 Go 的耗时、分配数和内存指标，以及测试自行报告的行吞吐、行数或文件字节指标。Benchmark 没有固定性能通过阈值；比较改动时应使用同一机器、Go 版本和参数，功能正确性仍由单元测试判定。

## Python 单元测试

文件位于 `lib/storage/testdata/`，使用标准库 `unittest`；检查器通过系统 `libzstd` 解压，不需要 Python zstandard 包。

| 文件及测试组 | 验证内容 |
|---|---|
| [test_downsampling_oracles.py](lib/storage/testdata/test_downsampling_oracles.py)：`InputTests` | 三小时输入、长周期稠密与稀疏输入、日/批次边界、活跃时间窗、单时间线跨 block 和标签；核验输入时间戳互斥及采样规则。 |
| 同文件：`OracleTests` | 手算 5m/1h 五特征与边界、共享最大时间戳、真实输入前几个 bucket、不等批次 count 累加；query_range 求值时间戳、左开右闭窗口、空窗口，以及 matrix 仅按共享时间戳裁剪。 |
| [test_downsampling_cluster.py](lib/storage/testdata/test_downsampling_cluster.py)：`ClusterRuntimeTests` | 租户路由、非法租户、短、长场景请求构造器在两种 HTTP 路由中分别传递分辨率和特征，原始查询不附加降采样参数；成功导入才累计输入数，等待 vmstorage 实收样本，组件异常检测及启动失败清理。 |
| 同文件：`CompatibilityAssertionTests` | 只有本次降采样请求的协议错误及随后追加的不支持 RPC 的日志，才能证明协议不兼容；普通 EOF、旧日志、返回空结果的成功响应和无关错误均不能通过。 |
| [test_downsampling_restart.py](lib/storage/testdata/test_downsampling_restart.py)：`RestartResponseTests` | 允许时间线和标签顺序变化；拒绝空结果、重复时间线、失败/部分响应、非法值、重复或倒序时间戳；标签、点数、时间戳和数值字符串必须严格一致。 |
| [test_downsampling_inspect.py](lib/storage/testdata/test_downsampling_inspect.py)：`InspectTests` | 使用独立构造的二进制测试数据，检查六字段降采样 metadata、配置缺失/null 的原始格式识别和非法配置拒绝、从 metaindex 推导的实际分辨率及基础区段完整性、89/113 字节偏移、全局列顺序、共享时间戳、租户和零负载；拒绝错误特征编号、身份/统计/顺序、缺列、多 block、跨租户、跨 index 重叠、偏移空洞/溢出、截断及未引用尾部；核验 CLI 报告和覆盖要求。 |

## 一键集群 E2E

在仓库根目录运行：

```sh
./lib/storage/testdata/downsampling_e2e.sh
```

入口自动创建全新的系统临时目录，并打印其路径。需要固定保存位置时，传入尚不存在的目录：

```sh
./lib/storage/testdata/downsampling_e2e.sh --output ./downsampling-e2e-result
```

### 依赖与构建

- 需要 Bash、Python 3、Git、满足源码 `go.mod` 的 Go，以及可加载的系统 `libzstd`。
- 本地必须已有 `v1.151.0-cluster` tag。入口只解析本地 tag，不自动获取；缺少时应先获取该基线引用。
- 入口为基线建立独立 detached worktree，以当前工作区作为候选，分别执行 `go build -p 4` 构建 vminsert、vmstorage、vmselect。它不复用用户先前构建的二进制。
- 获取基线 tag，以及 Go 按当前工具链、vendor、模块缓存和环境配置补充所缺工具链或依赖，可能需要网络。集群测试本身使用本机 loopback 地址，不依赖外部服务。
- 需要能创建进程组、启动多个组件、监听本地端口，并有空间保存两套二进制、输入、数据目录和日志。入口会操作本次创建的基线 worktree；测试失败或中断后保留证据并清理本次子进程和 worktree。

short、long 和 restart 为测试租户显式配置 5m 基础和 1h 额外分辨率，验证十种分辨率／特征组合；tenants 使用不同租户的 5m／30m／1h／2h 配置，验证二十种查询组合。生产默认仅有 5m。降采样请求始终同时发送 `resolution` 与 `feature`；原始基线查询不发送这两个参数。组合字符串只用于测试名称和报告索引。

每侧集群为一个 vminsert、一个 vmstorage、一个 vmselect，replicationFactor 为 1。测试关闭 dedup 和查询结果缓存，查询使用 `nocache=1`；写入后等待 vmstorage 的实收计数，随后执行 force_flush/force_merge，等待内存行及活动归并归零。

### 运行资源与超时

场景按顺序运行；对照阶段同时启动两套三组件集群。组件的 `-memory.allowedBytes` 默认为 256 MiB，long 场景的 vmstorage 和 vmselect 为 512 MiB。该参数限制缓存额度，不限制整个进程 RSS；Python 的输入、JSON 响应和参考结果也占内存。降采样自身的 spill、归并与查询工作预算见[设计文档](downsampling_storage_design.md#62-大小与内存)。

当前入口没有阶段总超时。内部 HTTP 请求通常设 120 秒超时，集群启动及单进程正常关闭设 45 秒超时，归并等待另有场景期限；这些局部超时不能保证整个阶段一定在固定时间内结束。用户中断或阶段退出后，入口按本次创建的进程组清理，正常停止超时后升级信号。

`*-metrics.txt` 保存部分刷盘或归并完成后的指标快照，包含 RSS、Go 堆和 goroutine 指标；它不是持续采样，也不记录峰值。现有脚本没有总 RSS 中止阈值；仅凭通过结果、单次指标或运行时长不能排除瞬时内存压力、I/O 阻塞或其他系统资源竞争。

### 阶段与场景

完整流程按以下顺序执行 16 个阶段，任一步失败即停止：

| 阶段名 | 内容及通过要求 |
|---|---|
| `baseline-source` | 从本地基线 tag 创建独立 worktree。 |
| `build-original`、`build-candidate` | 分别构建基线与候选三个组件，记录二进制 SHA256。 |
| `python-ut` | 从保存的测试脚本副本运行全部 `test_downsampling_*.py` 单元测试。 |
| `go-io-ut` | 运行 `lib/filestream` 全量单元测试。 |
| `go-layout-ut` | 单独运行 `FilePhysicalLayout`、`IterationMultiTSID`、`IterationReaderReuse`、`ClusterTenantIndexBoundaries` 四项测试，强制覆盖同列跨 index 及租户边界。 |
| `go-ut` | 运行 storage、vmselectapi、vmstorage、netstorage、prometheus 的降采样定向测试，跳过已单独执行的四项布局测试。当前脚本不含 promql 包，本文前面的包测试命令单独包含它。 |
| `short` | `downsampling_compare.py`：两条时间线、三小时，14/15/16 秒循环采样；按第一、第三、第二小时分三批倒序导入。每批落盘归并后验证，再重写、重启；十种分辨率与特征组合均检查 matrix 和 query_range。 |
| `inspect-short` | 检查候选活动文件布局，要求两个 TSID、租户集合恰为 `11:17`。 |
| `tenants` | `downsampling_cluster_tenants.py`：三租户、六时间线，分别配置 5m+1h、5m+30m+2h、仅5m；七个阶段验证二十种查询组合、空租户、运行时更新、未重写旧 part、重启和重写后的配置。另启动独立集群验证相邻月份的新旧配置 part 合成一个目标 bucket。 |
| `inspect-tenants` | 要求六个 TSID、上述三个租户的精确集合，每个 metaindex row 只属于同一租户。 |
| `long` | `downsampling_multiseries_compare.py`：93 天历史跨度、160 条时间线，其中 4 条持续采样，其余覆盖每天短时活动、间断、提前结束、延后开始。按周写入归并，在第 31/62/93 天检查；补写每日预留的 45 秒片段，再重写、重启。检查标签筛选、空结果、时间裁剪、多时间线及跨月数据。 |
| `inspect-long` | 要求 160 个 TSID、租户 `11:17`，并满足同一 TSID 的 5m 行数超过 8192 且跨 block、多分区、多物理 TSID 三项覆盖。 |
| `restart` | `downsampling_restart.py`：候选集群三批写入完成后获取十种分辨率与特征组合的 matrix/range 二十组非空快照；正常关闭并重新启动三个组件，不再写入或归并，核验旧进程正常退出、新进程启动、活动 part 不变、前后响应严格一致。 |
| `inspect-restart` | 检查重启后仍有两个 TSID、租户 `11:17`，持久化文件布局完整。 |
| `compatibility` | `downsampling_cluster_compatibility.py`：新 vmselect/旧存储组件、旧 vmselect/新存储组件两个混部方向均支持原生 matrix/range；新 vmselect 向旧 vmstorage 的降采样请求必须明确返回协议错误，并在本次请求之后追加的日志中出现 `unsupported rpcName: "search_downsampling_v2"`。 |

租户场景的七个阶段依次为：

| 场景内阶段 | 验证内容 |
|---|---|
| `phase1`、`phase2`、`phase3` | 三批倒序时间输入逐次归并；`11:17` 配置 1h，`11:18` 配置 30m／2h，`12:17` 只有基础 5m。三个租户均查询 5m／30m／1h／2h × 五特征的 matrix 和 query_range；未落盘分辨率由查询选择整除源列后聚合。 |
| `updated-before-rewrite` | PUT 将 `11:17` 改为 30m、`11:18` 改为 1h／2h；尚未重写的 part 继续按旧 metadata 正确查询，不能套用新配置解释旧文件。 |
| `rewrite` | 强制重写生成新 part 代次，物理列及 metadata 使用更新后的租户配置，全部查询结果保持一致。 |
| `restart` | 重启不改变已发布 part 代次，其 metadata 仍为更新配置；GET 当前配置恢复启动值，查询继续按 part 自身配置读取。 |
| `restart-rewrite` | 再次强制重写后，物理列恢复启动配置，全部查询结果保持一致。 |

随后在 `tenants/cross-partition/` 启动独立候选集群：月界前的 part 保存 1h 额外列，PUT 配置后月界后的 part 保存 30m，旧 part 不重写。脚本从 7d／90d／365d 中选择覆盖两个月份输入的同一个目标 bucket，实际查询五特征，每个特征恰好一行，sum=17、count=4，并检查两个 part 的独立文件布局。该子场景验证单个 vmstorage 跨 part／partition 的最大整除源选择及聚合，不增加顶层阶段数量。

长周期的“93 天”指输入时间跨度。每条时间线的正常批次和迟到补写时间戳互斥，用于验证同 bucket 追加贡献后的 sum/count。真实集群文件是否命中同一 `(resolution, feature)` 内跨 index，由检查报告如实记录；必需的同 TSID 和不同 TSID 跨 index 覆盖由 `go-layout-ut` 保证，不能把 feature 切换当成跨 index 覆盖。

### 数值判定

先将基线集群的原始查询结果与实际输入严格比较，再独立按 `timestamp // resolution` 分组计算五特征：共享时间戳取最大时间戳，last 取对应值，sum 使用 `math.fsum`，count 为贡献数，min/max 为极值。空 bucket 不输出点。候选结果的标签、序列集合、点数和时间戳必须一致，数值使用相对容差 `1e-10`、绝对容差 `1e-9`。

matrix selector 验证实际存储点，裁剪仅判断共享时间戳，不重算部分 bucket。query_range 使用 `step=max_lookback=resolution`，每个求值时刻选择左开右闭窗口内的末点，响应时间戳为求值时刻。Python oracle 单测分别验证两种语义。

重启快照比较不使用数值容差：仅规范化时间线和标签顺序，数值字符串、时间戳及点数必须严格相等；前后响应还分别与独立参考值比较，并拒绝健康单节点场景中的部分结果。兼容性场景只验证单 vmstorage、replicationFactor=1 的组件混搭；唯一节点不能处理降采样 RPC 时，共用结果收集路径仍返回错误。预期失败必须同时有本次请求的协议错误和新增的不支持 RPC 日志，连接故障不能代替协议拒绝；该场景不代表多节点查询必须全部成功。

### 结果与定位

终端应显示 `结果：passed`，进程退出码为 0；输出根目录 `manifest.json` 的 `status` 必须为 `passed`、`exit_code` 必须为 0，所有 16 个阶段均应为 `passed` 且退出码为 0。重启阶段必须完成二十组非空快照比较，四个顶层文件检查报告也必须为 `passed`；tenants 必须完成七个场景内阶段及 `cross_partition.status=passed`，独立子场景的 inspection.json 也须通过。

| 输出位置 | 内容 |
|---|---|
| `manifest.json` | 源码 commit、工作区状态、阶段命令与结果、脚本和二进制 SHA256、场景结果索引及覆盖要求。 |
| `<场景>/<版本>/*-metrics.txt` | 部分刷盘及归并完成后的组件指标快照，非持续采样或峰值记录。 |
| `candidate.patch`、`test-sources/` | 当前已跟踪文件相对 HEAD 的补丁、测试脚本副本；Python 单测和检查器实际使用该副本。未跟踪源码仅出现在工作区状态中，不由 patch 保存。 |
| `logs/<阶段名>.log` | 构建、单元测试、E2E 与检查器日志；`cleanup-baseline.log` 记录 worktree 清理。 |
| `original/`、`candidate/` | 两套集群二进制。 |
| `<场景>/summary.json` | 每项数值或行为检查、错误及场景总结果。 |
| `short/inspection.json`、`tenants/inspection.json`、`long/inspection.json`、`restart/inspection.json` | 活动 part、TSID、物理行/block、index 数量及实际遍历覆盖。 |
| `<场景>/original/`、`<场景>/candidate/` | 场景组件日志、实际请求与响应、数据目录；各脚本还保存输入或输入配置。 |
| `tenants/cross-partition/` | 独立跨月集群的 fixture.json、inspection.json、配置请求、查询响应、组件日志和新旧 part 数据；汇总结果记录在 tenants/summary.json 的 cross_partition 中。 |
| `restart/candidate/` | `before-restart-snapshot.json`、`after-restart-snapshot.json`、`restart-processes.json` 等正常重启证据。 |
| `compatibility/` | 两种混部环境、请求响应及本次不支持 RPC 的日志摘录。 |

单独重跑文件检查时，`--data-dir` 必须指向直接包含 `small/` 和 `big/` 的目录；`--expected-tenant` 可重复传入，表示精确租户集合。例如对已完成的 short 场景：

```sh
python3 -B -E lib/storage/testdata/downsampling_inspect.py \
  --data-dir ./downsampling-e2e-result/short/candidate/data/data \
  --output ./downsampling-e2e-result/short/inspection-recheck.json \
  --expected-series 2 --expected-tenant 11:17
```

各场景可使用对应 Python 脚本的 `--help` 查看独立运行参数。`--original`、`--candidate` 在 cluster 模式下均指向含三个组件的目录；`downsampling_restart.py` 只要求候选目录。

## 覆盖边界

Go 特殊值用例覆盖 NaN/Inf，集群数值场景的输入仅为有限值。文件检查器核验布局和索引引用，不解码时间列与值列的实际数值；数值正确性由 Go 编码测试及 HTTP 对照测试检查。

E2E 在刷盘和归并完成后查询，重启场景验证正常停机后的持久化读取。Go 专门用例覆盖动态配置快照、最大整除列、保留边界和跨 part／partition 聚合；tenants E2E 进一步验证真实运行时配置更新、重启及单节点跨月聚合。该 E2E 不覆盖原始与降采样混合查询、多 vmstorage 副本故障切换、进程崩溃或机器断电。部分发布恢复和 I/O 失败由 Go 故障注入测试单独验证。
