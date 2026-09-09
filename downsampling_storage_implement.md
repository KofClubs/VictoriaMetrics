# 降采样存储实现状态

设计见 [设计文档](downsampling_storage_design.md)，验证方法与证据见 [测试说明](downsampling_storage_testing.md)。

## 当前约定

- 实现分支：`experimental/downsampling`；基线：`v1.151.0-cluster`。本轮候选生产源码为 `069bf4ce8`。
- 降采样存储与字段查询的版本均为 2；FormatVersion、SemanticsVersion 均为 2，字段 RPC 为 `search_downsampling_v2`，NumericCodec 为 `decimal-values`。
- 固定 5m/1h、五个浮点特征、共享最大时间戳；每个分辨率与特征复用 storage.Block。inmemory 保持 raw，IndexDB 与 TSID 发号不变。

## 完成情况

| 模块 | 状态 |
|---|---|
| 数学计算、Block 复用、列存、归并及存储接入 | 已实现，本轮未修改生产代码 |
| 一键集群测试入口 | 完整一键运行通过；构建失败、SIGINT、SIGTERM 清理验证通过 |
| 集群 Python 测试 | 已修正求值时间戳参考；默认 cluster，输入采用 14～16 秒唯一样本 |
| Python UT | 23 项通过 |
| Go 降采样定向 UT | 通过 |
| 集群对照 | 1567 项通过，最大绝对误差 0 |
| 文件结构、采样间隔及手算值核验 | 通过 |

## 续接信息

一键入口已完成并验证，无待处理事项。源码与测试入口统一维护在 `experimental/downsampling` 分支。一键运行证据为 `/private/tmp/vm-downsampling-oneclick-final-20260909`；失败与中断验证见 `/private/tmp/vm-downsampling-oneclick-harness-checks-20260909/summary.json`。

后续修改先核对 manifest 与源码，再重跑受影响的验证。当前查询测试仅覆盖已完整归并的摘要；跨未归并 part 再聚合、raw/摘要混合查询和多节点副本故障测试尚未实施。状态与结果直接更新本文及测试说明，不追加过程流水账。
