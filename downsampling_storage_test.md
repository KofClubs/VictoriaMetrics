# 降采样存储测试说明

本文说明当前测试的用途、运行方式和判定条件。存储语义与[文件布局](downsampling_storage_design.md#3-文件布局)见[设计文档](downsampling_storage_design.md)，生产代码入口见[实现说明](downsampling_storage_implement.md)，审查关注点见[审查说明](downsampling_storage_review.md)。

## 运行方式

以下命令均在仓库根目录执行。Go 版本应满足当前 `go.mod`；Python 测试使用 Python 3，文件检查器还需要系统可加载的 `libzstd`。`-count=1` 禁用 Go 测试结果缓存，`-E` 避免 Python 环境变量影响断言执行。

```sh
# storage 全量回归与 filestream 单元测试
go test ./lib/storage -count=1 -timeout=10m
DISABLE_FSYNC_FOR_TESTING=false go test ./lib/filestream -count=1 -timeout=3m

# 字段查询、HTTP 参数、查询配置和集群 RPC
# vmstorage 在此命名筛选下只做编译检查。
go test -p 4 ./lib/vmselectapi ./app/vmstorage ./app/vmselect/netstorage ./app/vmselect/promql ./app/vmselect/prometheus -run '^TestDownsample' -count=1 -timeout=3m

# Python 输入、参考计算、集群运行器和文件检查器单元测试
python3 -B -E -W error::ResourceWarning -m unittest discover -s lib/storage/testdata -p 'test_downsampling_*.py' -v

# 降采样及相关 I/O 的并发与静态检查
DISABLE_FSYNC_FOR_TESTING=false go test -race ./lib/filestream ./lib/storage -run '^Test(Spill|Writer|Downsample|Downsampling|CheckDownsampling|MustOpenStorageDownsampling|EstimateDownsample|ReserveDownsample|Block)' -count=1 -timeout=5m
go vet ./lib/filestream ./lib/storage
```

单独检查一个问题时，使用下文中的测试名，例如：

```sh
go test ./lib/storage -run '^TestDownsampleMergerSourcePrecision$' -count=1 -v
```

Go 单元测试、race 和 vet 均以退出码 0 为通过条件；Python 单元测试应显示 `OK` 且退出码为 0。测试输出默认写入终端，Go 测试的 `t.TempDir()` 目录随测试清理，不产生持久化验收清单。需要保存日志时可自行重定向输出。

## storage 单元测试

以下文件均位于 `lib/storage/`。测试使用独立的原始输入参考值或原生编码器校验结果；实际文件布局另有按固定字节偏移解析的检查。

### 分桶与样本计算

文件：[downsample_test.go](lib/storage/downsample_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleBucket` | 5m/1h 的区间边界、非法时间和分辨率、时间域最后一个区间的右端点。 |
| `TestDownsampleNormalizeValue` | 保留有限值、正负零和无穷值；不同 NaN 位模式统一为 StaleNaN。 |
| `TestDownsampleSampleMergeRaw` | 将原始样本展开为五个特征，检查重复时间戳、乱序输入、时间戳相同时 last 的比较规则，以及 NaN/Inf 的计算规则。 |
| `TestDownsampleSampleMerge` | 合并已有五特征样本，累加非整数 count；1/32/64 精度继承、首次复制、重复及自引用输入、源样本不变。 |
| `TestDownsampleSampleNaNColumns` | 检查五列分别出现 NaN、输入次序变化和算术产生 NaN 时的计算结果，并确认源样本未被修改。 |
| `TestDownsampleSampleReset` | 空状态、带精度的 StaleNaN 样本、重复 Reset、清空后使用另一精度。 |
| `TestDownsampleSampleRandomizedReference` | 原始样本直接聚合、多级分组合并、5m 再合并为 1h，与独立参考值一致。 |

`downsample_testutil_test.go` 提供将原始样本展开为五个特征、转换测试数据的辅助函数，不含独立测试。

### 归并与直接写样本

文件：[downsample_merger_test.go](lib/storage/downsample_merger_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleMergerMoreThan1024Sources` | 一次合并 1025 个独立 part，同 bucket 的贡献不遗漏、不重复，两个分辨率及统计正确。 |
| `TestDownsampleMergerRepeatedMerge` | 原始样本首次降采样、原始 part 与降采样 part 合并、已有降采样 part 重写，以及同一 merger 再处理其他 TSID。 |
| `TestDownsampleMergerSourcePrecision` | 低精度继承；同 bucket 的 8/64 精度冲突返回错误；跨 bucket 的 8→64→8 切换拆成不同 block，重写后保持正确。 |
| `TestDownsampleMergerDynamicBucketRange` | 按当前 TSID 各源 header 的整体 min/max 开槽；多源及重叠 block、短/长/短序列、稀疏空槽、两个分辨率和实例复用。合成跨月输入仅用于直接调用 merger 的范围测试。 |
| `TestDownsampleMergerRetentionAndDeletedMetricID` | 按 bucket 右端点判定保留期限，删除指定 MetricID，重写时保留尚有效的 1h 贡献。 |
| `TestDownsampleMergerEmptyTSIDBeforeRetainedTSID` | 前一个 TSID 全部过期后不输出空 block，下一 TSID 的样本和精度不受影响。 |
| `TestDownsampleMergerSpecialValues` | NaN、正负无穷值经过首次合并和重写后保持五特征语义。 |
| `TestDownsampleMergerSummaryPhysicalStatistics` | 两个分辨率、五个特征按物理行和 block 统计；删除一条时间线后的统计正确。 |
| `TestDownsampleMergerOutputBlockBoundary` | 连续与稀疏两种输入均含 8193 个有效 bucket；按有效行数拆出 8192 行 block 和尾块，不计入空槽。 |
| `TestDownsampleMergerCancellation` | 已取消任务不统计尚未读取的数据；调用方执行 Abort 后删除目标目录，merger 可重新使用。 |
| `TestDownsampleMergerSharedPrecisionAndColumnScales` | 各特征独立 scale、五列共享时间戳和精度，多轮合并与原生编码参考结果一致。 |

文件：[downsample_writer_samples_test.go](lib/storage/downsample_writer_samples_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleWriterSamplesPreserveInput` | 8/64 精度下直接写样本，跳过前、中、后空槽；空输入不生成 block；写入后输入逐 bit 不变，包含非标准 NaN；读回与原生编码参考一致。 |
| `TestDownsampleWriterSamplesLaterBlockFailure` | 第一块写入完成后，在第二块注入写错误或取消信号；返回相应错误，清理已写目标、中间文件和句柄，禁止继续发布。 |

### 编码、文件格式与 reader 定位

文件：[downsample_codec_test.go](lib/storage/downsample_codec_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleFileRoundtrip` | 多 TSID、两个分辨率、五特征及 NaN 的完整写入和读回；物理统计与格式识别。 |
| `TestDownsampleFileConstantAndFilter`、`TestDownsampleFileAllConstantPayloads` | 常量列的零负载、时间和 TSID 过滤、没有数据的分辨率、全部常量的文件往返。 |
| `TestDownsampleHeaderValidation`、`TestDownsampleMetadataValidation` | header 行数、精度、编码、时间、偏移和截断校验；metadata 必需字段校验。 |
| `TestDownsampleRejectsUnknownVersionAndMarker`、`TestDownsampleTimeBounds` | 拒绝未知格式或标记，校验可写时间域及越界值。 |
| `TestDownsampleWriterOrderAbortAndPool` | 拒绝分辨率逆序，Abort 删除目标，解码缓冲超出池容量阈值时释放。 |
| `TestDownsampleWriterExistingDirectory` | 拒绝已有目录，随后 Abort 不删除该目录中原有文件。 |
| `TestDownsampleWriterAbortAfterFinish` | Finish 完成后尚未提交的目标仍可由 Abort 完整删除。 |
| `TestDownsampleReaderRawInmemory`、`TestDownsampleReaderLargeRawBlock` | 读取原始内存 part 并展开五特征，兼容原始大 block。 |
| `TestDownsampleReaderReadErrors`、`TestDownsampleCodecRowCountAndDecodeLimit` | 读取错误、行数不一致和解码大小限制。 |
| `TestDownsamplePoolNormalAndLargeCapacity` | reader 解码缓冲与 writer 单列缓冲保留正常容量、释放异常容量。 |
| `TestDownsampleFileMultiIndex`、`TestDownsampleReaderSeekResolutionAndSharedTSID` | 多 index 文件、指定分辨率定位、同一 TSID 跨相邻 index、缺失 TSID 不误读其他范围。 |
| `TestDownsampleReaderRawSeekEqualBoundaryAndReuse` | 在原始 metaindex 中定位 TSID；目标与某行首个 TSID 相等时，保留前一个 index，避免遗漏跨 index 的同 TSID 重叠 block。同一源重新初始化时复用文件句柄。 |
| `TestDownsampleReaderSeekHighCardinality` | 在大量 TSID 的降采样 metaindex 中按分辨率和 TSID 二分定位，跳过无关索引。 |
| `TestDownsampleNativeBlockReuse` | 各特征使用原生 Block 编解码；8/64 精度、共享时间戳、查询 BlockRef 读取和编码状态一致。 |
| `TestDownsampleFieldHeaderSharedTimestamps` | 五列 header 必须具有相同 TSID、行数、精度和时间列描述。 |
| `TestDownsampleClusterTenantIsolation` | 多租户写入、合并与字段查询保持隔离。 |
| `TestDownsampleClusterTenantIndexBoundaries` | AccountID 或 ProjectID 变化时切换 metaindex row，每行仅包含同一租户。 |

文件：[downsample_reader_shared_timestamps_test.go](lib/storage/downsample_reader_shared_timestamps_test.go)。

| 测试 | 验证内容 |
|---|---|
| `TestDownsampleReaderSharedTimestampsValuesOnly` | 首列解码后关闭独立时间戳句柄，后四列仍只解码 values 并复用同一时间戳缓冲；单列查询及下一次多特征读取必须重新读时间戳。 |
| `TestDownsampleReaderSharedTimestampsSwitches` | block、TSID、源 part、分辨率切换后，与各列单独使用原生 Block 读取的结果一致。 |
| `TestDownsampleReaderSharedTimestampsValidation` | 拒绝不一致的共享时间列描述及损坏的 values。 |

布局与遍历测试：

| 文件及测试 | 验证内容 |
|---|---|
| [downsample_layout_test.go](lib/storage/downsample_layout_test.go)：`TestDownsampleFilePhysicalLayout` | 独立按 89 字节 header、113 字节 metaindex 解析真实文件；四个子场景为多 block/分辨率、738 批次跨 index、租户切 row、零负载列。检查列顺序、共享时间戳、连续偏移、文件完整覆盖及统计。 |
| [downsample_iteration_test.go](lib/storage/downsample_iteration_test.go)：`TestDownsampleIterationMultiTSID` | 180 条时间线及同一 TSID 跨相邻 index；全部、非连续和缺失 TSID，边界时间窗，两个分辨率和五特征的查询遍历。 |
| 同文件：`TestDownsampleIterationReaderReuse` | 多次切换分辨率、特征、过滤条件及缺失 TSID 后，reader 不遗漏或重复数据。 |
| [downsample_layout_corruption_test.go](lib/storage/downsample_layout_corruption_test.go)：`TestDownsampleReaderCrossIndexOffsets` | 拒绝跨 index 的非法偏移和负载范围。 |
| 同文件：`TestDownsampleReaderCrossIndexFilterAndReset` | 跨 index 过滤、二分定位和 Reset 清理上一次索引边界状态，允许跳过不相交 index。 |
| 同文件：`TestDownsampleReaderCrossIndexSkippedCorruption` | 过滤读取不补读已跳过的损坏 index；全量扫描及正常打开 part 的校验均须读取并拒绝该损坏。 |
| 同文件：`TestDownsampleLayoutMetaindexIdentityCorruption`、`TestDownsampleLayoutIndexTenantCorruption` | 拒绝 metaindex 身份不一致，以及 index 内混入其他租户。 |
| 同文件：`TestDownsampleReaderDuplicateBatchKey`、`TestDownsampleReaderRawDuplicateBoundaryAllowed` | 拒绝降采样 block 的重复键；允许原始 block 具有相同时间边界，并完整读取各自的样本。 |

### 文件关闭、写失败与发布恢复

| 文件及测试 | 验证内容 |
|---|---|
| [downsample_reader_close_test.go](lib/storage/downsample_reader_close_test.go)：`TestDownsampleReaderCloseOwnFiles`、`TestDownsampleReaderCloseBorrowedFiles` | reader 自行打开的原始文件句柄全部释放，关闭错误合并返回；借用的降采样 part 文件句柄仍保持打开。 |
| 同文件：`TestDownsampleReaderInitCloseFailure` | 换源时旧句柄关闭失败，保留实际错误，不继续打开新源。 |
| 同文件：`TestDownsampleMergerResetClosesAllReaders`、`TestDownsampleMergerClosesBeforeReturning` | 已离开堆的 reader 仍由归并任务持有；成功、关闭失败、写入与关闭同时失败、取消与关闭同时失败时，全部实例和引用得到释放。 |
| [downsample_writer_failure_test.go](lib/storage/downsample_writer_failure_test.go)：`TestDownsampleWriterFinalFileFailures` | 四个最终二进制文件分别注入写错误、短写、关闭及 Abort 错误；原错误不丢失、每个句柄只释放一次、整个未发布目录删除。 |
| 同文件：`TestDownsampleWriterSpillAndValidationFailures` | spill 创建失败、截断 header、metadata 创建失败、后续输入无效；失败后禁止发布，清理后同一 writer 可重新初始化。 |
| 同文件：`TestDownsampleWriterFinalFilePermissions` | 最终文件权限与同进程 filestream 创建的文件一致。 |
| [downsample_failure_test.go](lib/storage/downsample_failure_test.go)：`TestDownsampleFailureBeforePublication` | 取消、提交前取消、打开目标前注入错误、目标无效、提交前注入错误均保留源和原清单，清除未发布目标。 |
| 同文件：`TestDownsampleFailureSchedulers`、`TestDownsampleFailureStopsRemainingBatches` | 各调度入口收到 I/O 错误或取消后退出，停止继续调度剩余批次。 |
| 同文件：`TestDownsampleFailureFinalFlushPreservesRaw`、`TestDownsampleFailureFinalLifecycle` | 最终刷盘失败时，改以原始格式持久化数据；关闭存储和创建快照的过程中不丢数据。 |
| 同文件：`TestDownsampleFailureAfterPublication` | 提交后目录同步失败时，新目标与活动清单保持一致，旧磁盘源仍保留；不把已提交目标当作未发布结果删除。 |
| 同文件：`TestDownsampleFailureFinalSyncAfterPublication` | 提交同步失败后，最终刷盘仍再次同步目录；已发布的降采样目标不会被回退流程生成的原始文件覆盖。 |
| [downsample_recovery_test.go](lib/storage/downsample_recovery_test.go)：`TestDownsampleRecoveryKeepsCommittedTargetAfterPanic` | 清单提交后在旧源回收处注入 panic；已提交目标仍可独立打开，存储重新打开后数据不变。 |
| 同文件：`TestDownsampleRecoveryDiscardsUnpublishedPart` | 再打开时丢弃未被活动清单引用的残缺 part。 |

### 分区、启动和空间预算

| 文件及测试 | 验证内容 |
|---|---|
| [downsample_partition_test.go](lib/storage/downsample_partition_test.go)：`TestDownsamplePartitionInmemoryAndFailedOutput` | 内存 part 之间合并后仍保持原始格式；降采样输出失败时不替换源 part。 |
| 同文件：`TestDownsamplePartitionSmallBigMerge`、`TestDownsamplePartitionFileOutput` | small/big 归并和落盘入口生成正确目标，核验输出类型与结果。 |
| 同文件：`TestDownsamplePartitionFilePartExpired` | 启用降采样时，原始 part 与降采样 part 均按最粗分辨率区间的右端点判定过期；关闭降采样后保留原始格式的过期边界。 |
| 同文件：`TestDownsampleStorageLifecycle` | 写入、刷盘、强制合并、快照、关闭和重新打开的完整存储生命周期。 |
| [downsample_open_test.go](lib/storage/downsample_open_test.go)：`TestCheckDownsamplingOpen` | 活动清单与格式检查；覆盖空目录、原始与降采样 part 混合、快照与临时目录、部分删除、缺失 part 和损坏 metadata。 |
| 同文件：`TestCheckDownsamplingOpenInvalidManifests` | 拒绝非法 JSON、未知或重复字段、重复 part、非法名称、路径和尾部数据。 |
| 同文件：`TestMustOpenStorageDownsamplingRejectsDedup`、`TestMustOpenStorageDownsamplingRejectsDisabledModeBeforeBackgroundWork` | 拒绝降采样与非零 dedup 同时启用；未启用降采样却存在活动降采样 part 时，在后台任务启动前失败。 |
| 同文件：`TestCheckDownsamplingOpenDisabledPreservesDedup` | 未启用降采样时，启动检查不改变原有去重配置与存储文件。 |
| [downsample_space_test.go](lib/storage/downsample_space_test.go)：`TestEstimateDownsamplePartSize` | 分别估算原始源、降采样源及混合源的输出空间，检查空输入、非法输入和溢出边界。 |
| 同文件：`TestEstimateDownsampleOutputSize`、`TestDownsampleSpaceEstimateOverflow` | 独立布局公式和大整数参考验证时间列、五特征、索引、spill、metadata 预算及饱和算术。 |
| 同文件：`TestDownsampleSpaceBoundCoversEncodedParts` | 实测最终文件与 spill 的保守峰值包络不超过预算；覆盖满 block、单行多 TSID 和频繁 index 切换。 |
| 同文件：`TestReserveDownsampleSpaceConcurrentAndCachedFreeSpace`、`TestDownsampleAvailableSpaceBoundaries` | 并发预留、幂等释放、空闲空间缓存、安全余量和请求边界。 |

### 查询与集群协议

| 文件及测试 | 验证内容 |
|---|---|
| [lib/storage/downsample_query_test.go](lib/storage/downsample_query_test.go)：`TestDownsampleQueryFieldParse` | 接受十种合法的 `query.field`；空字段使用原始查询路径，非法值返回错误。 |
| 同文件：`TestDownsampleQueryFieldBlockRef`、`TestDownsampleQueryFieldSharedPrecision` | 指定 TSID、分辨率、特征后仍使用标准 BlockRef；8/64 精度与共享时间列保持正确。 |
| 同文件：`TestDownsampleQueryFieldRawIsolationAndReset`、`TestDownsampleQueryFieldDoesNotChangeSearchProtocol` | 原始查询与降采样字段查询相互隔离；复用对象不残留字段选择，原始搜索协议保持不变。 |
| [lib/storage/downsample_search_protocol_test.go](lib/storage/downsample_search_protocol_test.go)：`TestDownsampleSearchProtocolValidation` | 扩展负载保留原始前缀；拒绝截断和非法字段；旧解码器保留扩展尾部供 RPC 严格拒绝。 |
| [app/vmselect/prometheus/downsample_query_test.go](app/vmselect/prometheus/downsample_query_test.go)：`TestDownsampleQueryFieldHTTPParameter` | HTTP 瞬时查询和范围查询接口解析合法字段，拒绝空值、重复参数、错误转义及不支持的组合。 |
| [app/vmselect/promql/downsample_query_test.go](app/vmselect/promql/downsample_query_test.go)：`TestDownsampleQueryFieldCacheAndCopy` | 字段查询禁用未区分特征的结果缓存，子查询配置复制保留字段与禁用缓存的状态。 |
| [app/vmselect/netstorage/downsample_query_test.go](app/vmselect/netstorage/downsample_query_test.go)：`TestDownsampleClusterSearchQueryFieldRoundtrip` | 租户和字段选择经过集群请求序列化后不丢失，字段请求使用 `search_downsampling_v2`。 |
| 同文件：`TestDownsampleClusterSearchRejectsPartialResults` | 字段查询遇到不支持协议或节点错误时，不以部分结果冒充成功。 |
| [lib/vmselectapi/downsample_search_test.go](lib/vmselectapi/downsample_search_test.go)：`TestDownsampleSearchRPCDispatch` | 原生与字段 RPC 分发、租户和十字段传递，返回实际 block。 |
| 同文件：`TestDownsampleSearchRPCRejectsInvalidPayload` | 拒绝字段缺失、截断、非法值及多余尾部，错误请求不进入搜索。 |

storage 全量回归还会运行原始存储链路的已有测试，包括 `block_test.go` 中的 `TestBlockMarshalUnmarshalPortable`、`TestBlockUnmarshalPortableDoS`，用于验证原生 Block 编解码与异常输入限制。

## filestream 单元测试

`go-io-ut` 实际运行 `go test -p 4 ./lib/filestream -count=1`，包含该包全部测试。返回错误的读写与关闭行为由以下用例验证，不依赖 `lib/fs` 的测试筛选。

| 文件及测试 | 验证内容 |
|---|---|
| [spill_test.go](lib/filestream/spill_test.go)：`TestSpillWriterRoundTrip`、`TestSpillWriterLazyCreationAndDiscard` | 完整读写、延迟创建文件，以及未消费内容的丢弃。 |
| 同文件：`TestSpillWriterPermissions`、`TestSpillWriterCreateFailure` | 权限与同进程创建的文件一致，创建失败返回原错误。 |
| 同文件：`TestSpillWriterWriteFailures`、`TestSpillWriterFlushAndSeekFailures` | 写错误、短写、缓冲刷出及定位失败的错误传播与清理。 |
| 同文件：`TestSpillWriterReadFailures`、`TestSpillWriterConsumerFailures` | 读错误、截断、无进展、回调错误、消费不完整均不能当作成功。 |
| 同文件：`TestSpillWriterExactReadAndSealedCallback` | 精确消费全部字节可通过；消费回调期间拒绝再次写入或嵌套调用 ReadAll。 |
| 同文件：`TestSpillWriterCleanupRetry`、`TestSpillWriterCloseErrorStillRemoves`、`TestSpillWriterWriteErrorCleanupRetry` | 删除失败可重试；关闭失败仍尝试删除；写失败后的清理重试保留原错误。 |
| 同文件：`TestSpillWriterBoundedBuffers`、`TestSpillWriterIndependentInstances` | 读写缓冲大小有界，多个实例的文件和状态彼此独立。 |
| [writer_error_test.go](lib/filestream/writer_error_test.go)：`TestWriterCreateAndCreateExclusive` | 普通创建可截断已有文件，独占创建拒绝覆盖；创建失败时返回错误，权限与同进程 `os.Create` 一致。 |
| 同文件：`TestWriterCloseReleasesAfterFailures`、`TestWriterClosePreservesFailedWrite` | 缓冲刷出、短写、同步或关闭失败后仍释放句柄；首次写错误持续返回，禁止重试写入。 |
| 同文件：`TestWriterAbortDiscardsBufferedData` | Abort 不刷出或同步缓冲，但仍关闭句柄并保留关闭错误；文件路径及已有文件由调用方管理。 |
| 同文件：`TestWriterCloseRealReadOnlyFileFailure` | 真实只读句柄的缓冲刷出失败，原文件内容保持不变，句柄仍关闭。 |
| 同文件：`TestWriterSyncFailureWithFsyncEnabled` | 开启 fsync 的关闭路径传播注入的同步错误并释放句柄；当前进程禁用 fsync 时，另起子进程验证。 |
| 同文件：`TestWriterMustFlushAndMustCloseSuccess` | MustFlush、MustClose 正常刷出内容，重复关闭可完成。 |
| [filestream_test.go](lib/filestream/filestream_test.go)：`TestWriteRead` | 完整文件写入、顺序读取和内容往返。 |

## 性能测试

入口位于 [downsample_timing_test.go](lib/storage/downsample_timing_test.go)：

```sh
go test ./lib/storage -run '^$' -bench '^BenchmarkDownsample' -benchmem -count=1
```

| Benchmark | 衡量内容 |
|---|---|
| `BenchmarkDownsampleSample/Raw`、`/Sample` | 分别使用 8192 条原始样本和五特征样本，测量纯计算吞吐和内存分配。 |
| `BenchmarkDownsampleMerge` | `Dense`、`Sparse`、`HighCardinality` 三种负载；分别测量原始源合并、多个降采样源合并，以及已合并降采样 part 的重写成本。子模式名为 `Raw`、`Summary`、`SummaryRewrite`。 |
| `BenchmarkDownsampleFileRead` | 三种负载下遍历两个分辨率并解码全部特征的成本。 |
| `BenchmarkDownsampleFileWrite` | 样本直接进入 writer 后的编码、写文件和同步关闭成本；测试数据转置、目录统计和删除不计时。 |

结果包含 Go 的耗时、分配数和内存指标，以及测试自行报告的行吞吐、行数或文件字节指标。Benchmark 没有固定性能通过阈值；比较改动时应使用同一机器、Go 版本和参数，功能正确性仍由单元测试判定。

## Python 单元测试

文件位于 `lib/storage/testdata/`，使用标准库 `unittest`；检查器通过系统 `libzstd` 解压，不需要 Python zstandard 包。

| 文件及测试组 | 验证内容 |
|---|---|
| [test_downsampling_oracles.py](lib/storage/testdata/test_downsampling_oracles.py)：`InputTests` | 三小时输入、长周期稠密与稀疏输入、日/批次边界、活跃时间窗、单时间线跨 block 和标签；核验输入时间戳互斥及采样规则。 |
| 同文件：`OracleTests` | 手算 5m/1h 五特征与边界、共享最大时间戳、真实输入前几个 bucket、不等批次 count 累加；query_range 求值时间戳、左开右闭窗口、空窗口，以及 matrix 仅按共享时间戳裁剪。 |
| [test_downsampling_cluster.py](lib/storage/testdata/test_downsampling_cluster.py)：`ClusterRuntimeTests` | 租户路由、非法租户、两种 HTTP 查询保留字段；成功导入才累计输入数，等待 vmstorage 实收样本，组件异常检测及启动失败清理。 |
| 同文件：`CompatibilityAssertionTests` | 只有本次字段请求的协议错误及随后追加的不支持 RPC 的日志，才能证明协议不兼容；普通 EOF、旧日志、返回空结果的成功响应和无关错误均不能通过。 |
| [test_downsampling_restart.py](lib/storage/testdata/test_downsampling_restart.py)：`RestartResponseTests` | 允许时间线和标签顺序变化；拒绝空结果、重复时间线、失败/部分响应、非法值、重复或倒序时间戳；标签、点数、时间戳和数值字符串必须严格一致。 |
| [test_downsampling_inspect.py](lib/storage/testdata/test_downsampling_inspect.py)：`InspectTests` | 使用独立构造的二进制测试数据，检查 89/113 字节偏移、全局列顺序、共享时间戳、租户和零负载；拒绝错误字段编号、身份/统计/顺序、缺列、多 block、跨租户、跨 index 重叠、偏移空洞/溢出、截断及未引用尾部；核验 CLI 报告和覆盖要求。 |

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

每侧集群为一个 vminsert、一个 vmstorage、一个 vmselect，replicationFactor 为 1。测试关闭 dedup 和查询结果缓存，查询使用 `nocache=1`；写入后等待 vmstorage 的实收计数，随后执行 force_flush/force_merge，等待内存行及活动归并归零。

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
| `short` | `downsampling_compare.py`：两条时间线、三小时，14/15/16 秒循环采样；按第一、第三、第二小时分三批倒序导入。每批落盘归并后验证，再重写、重启；十字段均检查 matrix 和 query_range。 |
| `inspect-short` | 检查候选活动文件布局，要求两个 TSID、租户集合恰为 `11:17`。 |
| `tenants` | `downsampling_cluster_tenants.py`：同一 vmstorage 的 `11:17`、`11:18`、`12:17` 三租户、六条时间线；同名指标不同值，检查十字段隔离、空租户、重写与重启后的 part 代次。 |
| `inspect-tenants` | 要求六个 TSID、上述三个租户的精确集合，每个 metaindex row 只属于同一租户。 |
| `long` | `downsampling_multiseries_compare.py`：93 天历史跨度、160 条时间线，其中 4 条持续采样，其余覆盖每天短时活动、间断、提前结束、延后开始。按周写入归并，在第 31/62/93 天检查；补写每日预留的 45 秒片段，再重写、重启。检查标签筛选、空结果、时间裁剪、多时间线及跨月数据。 |
| `inspect-long` | 要求 160 个 TSID、租户 `11:17`，并满足同一 TSID 的 5m 行数超过 8192 且跨 block、多分区、多物理 TSID 三项覆盖。 |
| `restart` | `downsampling_restart.py`：候选集群三批写入完成后获取十字段的 matrix/range 二十组非空快照；正常关闭并重新启动三个组件，不再写入或归并，核验旧进程正常退出、新进程启动、活动 part 不变、前后响应严格一致。 |
| `inspect-restart` | 检查重启后仍有两个 TSID、租户 `11:17`，持久化文件布局完整。 |
| `compatibility` | `downsampling_cluster_compatibility.py`：新 vmselect/旧存储组件、旧 vmselect/新存储组件两个混部方向均支持原生 matrix/range；新 vmselect 向旧 vmstorage 的字段请求必须明确返回协议错误，并在本次请求之后追加的日志中出现 `unsupported rpcName: "search_downsampling_v2"`。 |

长周期的“93 天”指输入时间跨度。每条时间线的正常批次和迟到补写时间戳互斥，用于验证同 bucket 追加贡献后的 sum/count。真实集群文件是否命中同一 `(resolution, feature)` 内跨 index，由检查报告如实记录；必需的同 TSID 和不同 TSID 跨 index 覆盖由 `go-layout-ut` 保证，不能把 feature 切换当成跨 index 覆盖。

### 数值判定

先将基线集群的原始查询结果与实际输入严格比较，再独立按 `timestamp // resolution` 分组计算五特征：共享时间戳取最大时间戳，last 取对应值，sum 使用 `math.fsum`，count 为贡献数，min/max 为极值。空 bucket 不输出点。候选结果的标签、序列集合、点数和时间戳必须一致，数值使用相对容差 `1e-10`、绝对容差 `1e-9`。

matrix selector 验证实际存储点，裁剪仅判断共享时间戳，不重算部分 bucket。query_range 使用 `step=max_lookback=resolution`，每个求值时刻选择左开右闭窗口内的末点，响应时间戳为求值时刻。Python oracle 单测分别验证两种语义。

重启快照比较不使用数值容差：仅规范化时间线和标签顺序，数值字符串、时间戳及点数必须严格相等；前后响应还分别与独立参考值比较。兼容性场景的预期失败必须是可归因到字段 RPC 的错误，连接故障不能代替协议拒绝。

### 结果与定位

终端应显示 `结果：passed`，进程退出码为 0；输出根目录 `manifest.json` 的 `status` 必须为 `passed`、`exit_code` 必须为 0，所有 16 个阶段均应为 `passed` 且退出码为 0。重启阶段必须完成二十组非空快照比较，四个文件检查报告也必须为 `passed`。

| 输出位置 | 内容 |
|---|---|
| `manifest.json` | 源码 commit、工作区状态、阶段命令与结果、脚本和二进制 SHA256、场景结果索引及覆盖要求。 |
| `candidate.patch`、`test-sources/` | 当前已跟踪文件相对 HEAD 的补丁、测试脚本副本；Python 单测和检查器实际使用该副本。未跟踪源码仅出现在工作区状态中，不由 patch 保存。 |
| `logs/<阶段名>.log` | 构建、单元测试、E2E 与检查器日志；`cleanup-baseline.log` 记录 worktree 清理。 |
| `original/`、`candidate/` | 两套集群二进制。 |
| `<场景>/summary.json` | 每项数值或行为检查、错误及场景总结果。 |
| `short/inspection.json`、`tenants/inspection.json`、`long/inspection.json`、`restart/inspection.json` | 活动 part、TSID、物理行/block、index 数量及实际遍历覆盖。 |
| `<场景>/original/`、`<场景>/candidate/` | 场景组件日志、实际请求与响应、数据目录；各脚本还保存输入或输入配置。 |
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

E2E 在刷盘和归并完成后查询，重启场景验证正常停机后的持久化读取。它不等同于未归并多 part 的查询侧再聚合、原始数据与降采样数据的混合查询、多 vmstorage 副本故障切换、进程崩溃或机器断电测试。部分发布恢复和 I/O 失败由 Go 故障注入测试单独验证。
