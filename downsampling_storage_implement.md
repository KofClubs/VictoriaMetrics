# 降采样存储实现状态

总体设计见 [设计文档](downsampling_storage_design.md)；当前磁盘格式以 [文件布局](downsampling_storage_file_layout.md) 为准，验证范围见 [测试说明](downsampling_storage_testing.md)。

## 当前约定

- 实现分支：`experimental/downsampling`；基线：`v1.151.0-cluster`。以下状态依据当前工作区生产代码，不以历史候选 commit 代替。
- FormatVersion、SemanticsVersion 均为 2，字段 RPC 为 `search_downsampling_v2`，NumericCodec 为 `decimal-values`。
- 固定 5m/1h、last/sum/count/min/max 五个浮点特征、共享最大时间戳；复用原生 `Block`。inmemory 保持 raw，IndexDB 与 TSID 发号不变。
- **当前采用 89/113 字节布局，feature 统一为 0..4。旧 99/112 字节格式，以及 `ae899eded` 的 89/113 字节、feature 1..5 格式均不兼容，且无迁移或旧布局读取路径。** 版本号和 magic 未变不代表兼容；旧摘要不能直接复用，应保留备份并从原始数据在新目录重建。

## 当前已实现

| 模块 | 当前行为 |
|---|---|
| codec | 集群版 index 条目为 89 字节原生 `blockHeader`；113 字节 `downsampleMetaindexRow` 嵌入原生 `metaindexRow`，追加 feature、resolution、LastTSID、RowsCount；版本在 magic/metadata 中 |
| writer | 五个 `filestream.SpillWriter` 保存各列 header+values，封装临时文件与流式读回；最终文件通过 filestream 写入，values/index/metaindex 按 `(resolution, feature, TSID.Less, MinTimestamp)` 全局排列 |
| 失败处理 | 降采样 writer/reader/merger 的验证、I/O 或提交前失败返回错误并删除未发布目标、spill 和临时清单；调度结束本次降采样并保留源；final flush 必要时将剩余内存源按 raw 落盘 |
| timestamps | 五个原生 Block 分别编码并校验一致性，按批次生成顺序只写一次；五列共享同一 offset/size，不进入 spill |
| index 分块 | 大小阈值、租户变化或 feature 结束时 flush；每 row 只含同分辨率、同特征、同租户，可含多个 TSID |
| 打开校验 | 有界五路遍历全部 index，校验列齐全、共享时间戳描述、排序、统计及 timestamps/values 全文件连续覆盖；不积累全量 header/offset 集合，不解码数值 payload |
| 字段查询 | 二分定位目标 resolution/feature，只遍历该列 index，payload 由原生 BlockRef/Block 读取；不等同于打开时只校验一列 |
| 归并 | 主 reader 与四个 peer reader 按 TSID/时间对齐五列，校验共享描述后重建 batch；merger 按 resolution/TSID/窗口生成，writer 负责 feature 转置 |
| 空间预算 | 一份 timestamps、五份 values、五列独立 index/metaindex、spill 与最终输出共存峰值及 metadata；加乘饱和防溢出，不依赖删除源文件 |

`TSID.Less` 实际先比较 AccountID、ProjectID，再比较 MetricGroupID、JobID、InstanceID、MetricID。
租户在同一 resolution/feature 内有序，但仍须显式切 row，避免一个 index 跨租户。

分段 flush 只能在同一 feature 内切 index，或继续追加各列 spill；不能将片段 A 的五列全部输出后再输出片段 B 的五列，
否则 feature 从 4 回退到 0，破坏全局排序。当前 writer 在分辨率切换或 Finish 时完整转置。

## 验证状态与范围

- 本次错误处理范围为 spill 及降采样自己的 writer/reader/merger；`lib/fs`、通用 part 关闭和源 part 回收保持原实现。回退后的验证结果见测试说明，本次 E2E 待用户运行。
- 当前 Go 布局/损坏测试采用 89/113 字节及 feature 0..4。完整 storage、降采样跨包定向及 storage 定向 race 已通过，详见测试说明。
- Python `downsampling_inspect.py` 已适配当前格式；检查器 UT 和 Go writer 实盘交叉验证用于核对新解析器。
- 一键入口单独运行布局与遍历 Go UT，保证同一 `(resolution, feature)` 内跨 index 的覆盖；160 条时间线的集群长周期场景如实报告实际覆盖，不将 feature 切换计为同列跨 index。
- spill 重构前的当前格式完整一键集群测试已通过：15 个阶段、1670 项 E2E，最大绝对误差为 0；重启前后二十组快照、780 个查询点严格一致。证据：`/private/var/folders/tk/llwph05x6_xgbmqxbxkq_6m40000gn/T/vm-downsampling-e2e-8n7u6mcp/manifest.json`。本次 spill/错误处理重构的 E2E 由用户重新运行，不能沿用上述结果。
- 跨未归并 part 查询侧再聚合、raw/摘要混合查询、多节点副本故障及崩溃恢复仍不在本次验证范围内。
