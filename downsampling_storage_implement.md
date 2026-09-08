# 降采样存储实现状态

设计见 [设计文档](downsampling_storage_design.md)，验证见 [测试说明](downsampling_storage_testing.md)。本文仅记录当前状态与续接事项。

## 当前约定

- 分支：`experimental/downsampling`；基线：`v1.151.0-cluster`，commit `e7a3dc606a1d804cb3101fa73eb30cb0946ab2a9`。
- 降采样存储与字段查询的版本统一为 2；FormatVersion、SemanticsVersion 均为 2，帧标识末尾为 `00 02`，字段 RPC 为 `search_downsampling_v2`。
- NumericCodec 为 `decimal-values`，复用原有编码算法，不附加编码版本。
- 固定 5m/1h、五个浮点特征、共享最大时间戳；每个分辨率与特征使用真实 storage.Block。inmemory 保持 raw，IndexDB 与 TSID 分配不变。

## 完成情况

| 模块 | 状态 |
|---|---|
| 数学计算、Block 复用、列存、归并及存储接入 | 已实现 |
| 统一版本标识、测试常量及协议名称 | 已完成 |
| 精简设计与测试文档 | 已重构 |
| Go 普通回归与 vet | 通过 |
| 集群对照 | 1300 项通过 |
| 独立文件检查 | 通过 |
| 完整 storage race | 通过 |

## 维护约定

当前任务已完成，无待处理卡点。后续修改先核对源码与 manifest，再重跑受影响的验证；状态和结果只更新本表与测试说明，不追加过程流水账。

本轮证据目录：`/tmp/vm-downsampling-version2-20260908`。修改保留在工作区，尚未提交。
