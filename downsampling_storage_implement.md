# 降采样存储实现状态

总体设计见 [设计文档](downsampling_storage_design.md)；当前磁盘格式以 [文件布局](downsampling_storage_file_layout.md) 为准，验证范围见 [测试说明](downsampling_storage_testing.md)。

## 当前约定

- 实现分支：`experimental/downsampling`；基线：`v1.151.0-cluster`。以下状态依据当前工作区生产代码，不以历史候选 commit 代替。
- FormatVersion、SemanticsVersion 均为 2，字段 RPC 为 `search_downsampling_v2`，NumericCodec 为 `decimal-values`。
- 固定 5m/1h、last/sum/count/min/max 五个浮点特征、共享最大时间戳；复用原生 `Block`。inmemory 保持 raw，IndexDB 与 TSID 发号不变。
- **旧实验 v2 的 99/112 字节布局与当前 89/113 字节布局不兼容，且无迁移或旧布局读取路径。** 版本号和 magic 未变不代表兼容；旧摘要不能直接复用，应保留备份并从原始数据在新目录重建。

## 当前已实现

| 模块 | 当前行为 |
|---|---|
| codec | 集群版 index 条目为 89 字节原生 `blockHeader`；113 字节 `downsampleMetaindexRow` 嵌入原生 `metaindexRow`，追加 feature、resolution、LastTSID、RowsCount；版本在 magic/metadata 中 |
| writer | 五路磁盘 spill 保存各列 header+values；分辨率结束时依次消费五列，最终 values/index/metaindex 按 `(resolution, feature, TSID.Less, MinTimestamp)` 全局排列 |
| timestamps | 五个原生 Block 分别编码并校验一致性，按批次生成顺序只写一次；五列共享同一 offset/size，不进入 spill |
| index 分块 | 大小阈值、租户变化或 feature 结束时 flush；每 row 只含同分辨率、同特征、同租户，可含多个 TSID |
| 打开校验 | 有界五路遍历全部 index，校验列齐全、共享时间戳描述、排序、统计及 timestamps/values 全文件连续覆盖；不积累全量 header/offset 集合，不解码数值 payload |
| 字段查询 | 二分定位目标 resolution/feature，只遍历该列 index，payload 由原生 BlockRef/Block 读取；不等同于打开时只校验一列 |
| 归并 | 主 reader 与四个 peer reader 按 TSID/时间对齐五列，校验共享描述后重建 batch；merger 按 resolution/TSID/窗口生成，writer 负责 feature 转置 |
| 空间预算 | 一份 timestamps、五份 values、五列独立 index/metaindex、spill 与最终输出共存峰值及 metadata；加乘饱和防溢出，不依赖删除源文件 |

`TSID.Less` 实际先比较 AccountID、ProjectID，再比较 MetricGroupID、JobID、InstanceID、MetricID。
租户在同一 resolution/feature 内有序，但仍须显式切 row，避免一个 index 跨租户。

分段 flush 只能在同一 feature 内切 index，或继续追加各列 spill；不能将片段 A 的五列全部输出后再输出片段 B 的五列，
否则 feature 从 5 回退到 1，破坏全局排序。当前 writer 在分辨率切换或 Finish 时完整转置。

## 验证状态与待办

- 本次仅修改三份指定文档与 `lib/storage/downsample_space_test.go`，不修改生产代码；空间定向测试结果见测试说明。
- Go 布局/损坏测试源码已适配 89/113 字节；本次空间测试不替代完整布局、归并、查询或集群回归。
- Python `downsampling_inspect.py` 仍使用旧 99/112 字节布局，**尚未适配**。需另行更新检查器并用新目录重跑一键 E2E、文件检查和重启验证。
- 历史一键证据 `/private/tmp/vm-downsampling-oneclick-restart-20260909` 记录 14 阶段、31 项 Python UT、1670 项 E2E 及二十组重启快照通过；这些是旧布局结果，不证明当前布局通过。
- 历史入口失败/中断清理证据为 `/private/tmp/vm-downsampling-oneclick-harness-checks-20260909/summary.json`，同样未在本次重跑。
- 跨未归并 part 查询侧再聚合、raw/摘要混合查询、多节点副本故障及崩溃恢复仍不在已验证范围内。
