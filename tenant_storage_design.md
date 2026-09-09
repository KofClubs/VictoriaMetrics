# 多租户独立存储目录设计

## 目标与结论

以 `(AccountID, ProjectID)` 为租户标识，在每个 vmstorage 节点内为每个租户建立独立的 `storage.Storage` 及根目录。数据、IndexDB、持久缓存、retention 和 snapshot 随所属 Storage 管理；保留 TSID 发号算法、IndexDB 编码及底层 block 读写与 merge 逻辑。

该方案在现有实例边界上具有可行性，但不是修改目录名称即可完成的功能。必须配套实现租户路由、写入接收确认、实例生命周期、缓存总预算和管理接口。可承载租户数量需要通过压力测试确定。

代码依据为 `v1.151.0-cluster`（`e7a3dc606a1d804cb3101fa73eb30cb0946ab2a9`）；规划时工作区为 `control/downsampling`。本文为实施方案，尚未修改生产代码或完成验证。

## 隔离边界

- 一个租户对应一个完整 Storage，不同租户不共享数据 part、IndexDB 文件或持久缓存文件。
- 同一租户仍可能分布在多个 vmstorage 节点上；目录隔离发生在每个节点内，不改变现有集群分片与副本规则。集群范围的停用、备份、恢复和删除需要协调所有相关节点。
- 同一进程仍共享 CPU、部分内存缓存、对象池及 merge 并发限制；同一文件系统仍共享容量和 I/O。独立目录不等于资源配额、安全沙箱或进程故障隔离。
- Storage 内仍保留真实的 AccountID、ProjectID，不改写为零。目录归属由管理器校验，不能依赖目录名称自动限制输入。

## 目录与实例组织

```text
<storageDataPath>/
  tenants/<AccountID>/<ProjectID>/
    flock.lock
    cache/
    metadata/
    snapshots/
    data/
      small/<YYYY_MM>/
      big/<YYYY_MM>/
      indexdb/<YYYY_MM>/
    indexdb/                    # 兼容旧 IndexDB 时保留
```

`metadata/` 是原生存储元信息目录；指标的 HELP、TYPE 等 metadata 当前为内存数据，并不因目录拆分而获得持久化能力。snapshot 还会使用原生 data 子目录中的 snapshot 路径，不改写其内部结构。

在 `app/vmstorage` 增加租户管理器，统一持有租户配置、Storage 实例和使用引用：

```text
写入请求 → 解码、租户准入与分组 → 租户管理器 → 对应 Storage.AddRows
查询请求 → 提取租户标识       → 租户管理器 → 对应 Storage.Search
管理请求 → 指定租户           → 租户管理器 → snapshot / flush / merge
```

首期采用显式租户清单、数量上限和受限并发的启动打开流程，不实现无限制自动创建或空闲实例自动关闭。已有混合存储目录不得直接作为隔离模式根目录使用。新增租户、更新 retention 及离线恢复可先通过受控重启完成。

路径仅由规范化后的两个 `uint32` 标识生成。管理器维护根目录锁和租户配置；每个子 Storage 继续使用原生目录锁。查询未知租户返回空结果，不创建目录；写入未知租户必须经过明确的准入处理，不能回落到公共 Storage。

## 写入与接收确认

`MetricRow.MetricNameRaw` 的前八字节包含 AccountID 和 ProjectID；metadata Row 已有对应字段。写入和 `RegisterMetricNames` 均按行分组，不能假定一个 RPC 或一次回调只包含一个租户。分组缓冲区使用对象池复用，Reset 必须清除行引用、租户键和使用引用，避免持有已经归还给解析器的内存。

现有 `ParseBlock`、`ParseMetricsMetadataBlock` 在调度异步解码后即返回成功，回调错误仅记录日志；legacy 接收路径也先确认再处理。因此，不能只修改 `VMStorage.WriteRows` 并依赖返回 error 传播租户路由失败。

实施要求：

1. 成功确认前完成整个接收批次的租户识别、准入校验和 Storage 引用获取；范围必须覆盖全部回调分片。检查失败时尚未向任何租户追加该批数据。
2. 将确认时点推进至本批数据已交给对应 Storage 的接收流程；仍沿用原生内存写入的持久性边界，不把确认表述为已经落盘。新旧接收路径及 metadata 路径都需同步改造与测试。
3. 单租户停用、配额超限和租户目录异常不能直接映射为节点级 `ErrReadOnly`。当前 vminsert 会据此将整个节点标记为只读；普通错误也会触发节点故障及重路由。独立停写需要同时改造 vminsert 的租户准入、批次组织与错误分类，不能仅靠 vmstorage 管理接口实现。
4. 首期不提供运行期间的独立停写或容量配额。非法租户在进入发送队列前拒绝；vmstorage 保留批次校验，配置不一致必须显式报错。相关配置必须在集群内一致发布。
5. 批次预检不能提供跨 Storage 事务或消除网络重试。不得在部分追加成功后因可预检的租户错误触发整批重试；连接中断导致的重复写入仍遵循原有传输语义。

## 查询与管理

vmselect 已按 tenant 拆分查询 RPC，服务端可以使用现有 AccountID、ProjectID 选择 Storage，无须因目录隔离修改查询 block 格式。

必须覆盖 `InitSearch`、metric names、label names/values、tag suffixes、series count、TSDB status、DeleteSeries，以及 `setupTfss` 内部的 Graphite 查询。`RegisterMetricNames` 和 metadata 写入同样需要分组路由。

跨租户入口由管理器汇集结果：`Tenants` 保持时间范围过滤；未指定 tenant 的 metadata 查询保持全局排序和 limit；metric names 使用统计需明确总量字段的全局语义；现有无 tenant 参数的统计重置继续作用于全部租户。不能简单枚举目录替代 `SearchTenants`。

| 管理能力 | 实施范围 |
| --- | --- |
| retention、写入时间范围、series 数量限制 | 使用各 Storage 的 `OpenOptions`；首期配置变更通过受控重启生效 |
| snapshot 创建、列举、删除及过期清理 | 管理请求显式指定租户，复用该 Storage 的实现；补齐备份工具的租户定位 |
| flush、force merge、日志与指标 | 路由到指定租户；异步任务持有引用至实际结束 |
| 备份与恢复 | 备份单租户 snapshot；恢复前停止并关闭目标 Storage，首期通过维护窗口完成 |
| 租户停用及删除 | 后续增加集群准入撤销、请求排空和关闭流程；删除后保留停用状态，阻止旧请求重新创建目录 |
| 容量配额及公平调度 | 后续单独设计；磁盘剩余空间检查和 series 数量限制均不是容量配额 |

原全局 snapshot 接口不能默默选取任意租户：隔离模式要求明确租户，遗漏时返回错误；若以后提供全局操作，需要列出各租户结果，不能宣称顺序创建的多个 snapshot 属于原子快照。管理操作继续使用现有鉴权，租户参数本身不构成授权。

## 生命周期与资源预算

管理器以 `Acquire/Release` 保护 Storage。关闭顺序为：禁止新引用 → 等待在途写入、查询及管理任务结束 → `Storage.MustClose` → 释放目录锁。查询引用必须由 `BlockIterator` 持有至 `MustClose`，不能在 `InitSearch` 返回时释放。重新打开必须等待前一次关闭完成。

`Storage.MustClose` 要求外部已停止使用；`table.MustClose` 遇到剩余 partition 引用会 panic。关闭不能依靠从 map 中删除实例完成。

当前每个 Storage 的三类缓存默认上限分别按进程允许内存的约 37%、6.25%、10% 计算，partition IndexDB 还另有缓存。这些比例是配置上限，不是实际 RSS；直接复制多份仍会失去总预算约束。需要为实例及其 IndexDB 增加明确的缓存预算输入，并保留进程共享缓存预算。不得通过反复修改全局 `Set*CacheSize` 配置租户。

同时评估每租户的基础开销、月份数、part 数、文件句柄、后台任务和打开峰值；限制打开并发及总实例数。原生 block、reader、writer、merger 对象池继续共享，保持现有 Reset 约定。共享 block cache 按 part 身份和 offset 索引，可继续使用。

首期不自动关闭空闲租户：当前指标 metadata 保存在内存，提前关闭会改变其约一小时的保留行为。若需要大量稀疏租户，应先完成缓存与后台任务容量验证，再设计 metadata 生命周期和实例淘汰。

`MustOpenStorage` 为 panic 风格，且缺乏完整初始化失败回滚。首期明确采用启动校验失败即启动失败的策略；若需要运行时隔离单租户打开错误，必须增加可返回 error 且完整回收资源的打开流程。同进程运行期间的任意故障隔离仍不能由此保证。

指标区分进程级与租户级，避免把全局 counter 对每租户重复导出或累加；租户指标使用 AccountID、ProjectID 标签，单独核算总内存预算和实际占用。不同租户目录位于同一文件系统时，其磁盘容量不得重复相加。

## 主要修改位置

| 位置与代码依据 | 修改内容 |
| --- | --- |
| `app/vmstorage/main.go`：单次 `MustOpenStorage`、server 初始化、HTTP 管理与指标 | 增加租户配置和管理器；改造启动、停止、管理入口与指标输出 |
| `app/vmstorage/vmstorage.go`：`WriteRows`、`WriteMetadata`、各查询方法 | 分离单实例操作与租户路由；迭代器、异步管理任务持有 Storage 引用 |
| `lib/protoparser/clusternative/stream/streamparser.go`、`lib/vminsertapi/server.go` | 调整异步解析、批次预检、引用取得及成功确认时点 |
| `app/vminsert/netstorage/insert_ctx.go`、`netstorage.go` 及各写入入口 | 租户准入；后续独立停写所需的批次与错误处理，保留原有分片规则 |
| `lib/vmselectapi/api.go`、`app/vmselect/netstorage/netstorage.go` | 逐项核对路由和跨租户语义；已有单 tenant 查询请求可继续复用 |
| `lib/storage/storage.go`：`OpenOptions`、缓存初始化、`MustClose`、snapshot | 增加实例资源预算；复用存储生命周期及管理能力 |
| `lib/storage/index_db.go`：缓存初始化、`generateTSID`、`generateUniqueMetricID` | 仅实例化资源预算；不修改索引编码、TSID 结构或发号算法 |
| `lib/storage/table.go`、`partition.go`、`metricsmetadata/storage.go` | 核对 retention、引用及后台任务边界；保留原生数据组织和 metadata 语义 |

目录隔离与降采样分别位于租户路由层和 Storage 内部。后续接入降采样分支时，继续复用该分支的 block 与聚合实现，降采样相关 Version 保持 2；目录拆分本身不增加文件或查询格式版本。

## 迁移与实施顺序

1. **建立隔离基础**：使用新的空根目录，实现租户配置、完整 Storage 路由、接收确认改造、查询覆盖、资源预算、独立 retention 与 snapshot。测试期间不接管旧混合目录。
2. **完成管理与规模验证**：补齐备份恢复、指标及错误测试，测量目标租户数下的常驻内存、启动时间、文件句柄、写入吞吐与查询延迟，再确定部署上限。
3. **扩展动态管理**：按实际需要实现在线创建、租户停写、关闭和删除；先解决上游准入与重试语义，再开放管理操作。
4. **单独实施历史迁移**：混合 part 可以含多个租户，不能整体搬移。raw 数据可选择按租户导出重放，此方式重新生成 MetricID；若要求保留 TSID，则必须设计原生离线拆分工具，同时处理 IndexDB 映射、删除标记、metadata 和 part 清单。

降采样数据迁移必须保留各分辨率的五个特征值及其共享时间戳，不能把查询得到的单个特征值重新作为 raw 写入。迁移应使用稳定快照或明确的写入截止点，在独立目标目录校验后切换；保留原目录。新目录产生写入后，回退还需要处理切换后的新增数据，不能仅修改目录配置。

保留 TSID 的离线拆分按源 vmstorage 分别进行，不能假定 MetricID 在不同节点间全局唯一。IndexDB 条目必须按 namespace 解析：删除标记没有 tenant 字段，租户字段在其他 namespace 中的位置也不统一。迁移工具需处理各 partition 的删除状态，重建目标缓存，并重新生成 part 索引、offset、size、统计和活动清单。

## 验证与验收

| 验证范围 | 输入与通过条件 |
| --- | --- |
| UT：路由与目录 | 使用相同 metric name/labels、不同 tenant 和不同 values，包含同 AccountID 不同 ProjectID；混合批次写入、重复 flush/merge、关闭重开后，逐项检查 block、IndexDB 和持久缓存的租户归属，每个目录只能读出本租户 TSID 和样本 |
| UT：生命周期 | 受控关闭时并发执行查询、写入、snapshot 和 force merge；迭代器未关闭时不得释放 Storage，不发生重复打开、panic、引用泄漏或跨租户对象复用污染 |
| UT：接收确认 | 在多回调分片批次中注入后段租户准入错误、引用获取失败和连接中断；成功确认不得先于租户准入，预检失败不得留下前段租户写入；分别验证数据、metadata 及 legacy 路径 |
| UT：管理与查询覆盖 | 单租户 retention、snapshot 创建与删除、维护窗口恢复及 DeleteSeries 不改变另一租户结果；覆盖 labels、series、Graphite、metadata、使用统计、时间范围租户枚举及全局操作 |
| 集群 E2E | 两套相同拓扑的集群分别运行基准 tag 与候选实现，至少包含两个 vmstorage，正常写入对比使用单副本；使用 Python 生成多租户、多 TSID、跨月份、分批及乱序输入，比较每租户原始查询、PromQL、跨租户查询与重启后结果 |
| 降采样集成 E2E | 使用基准集群按租户导出的 raw 样本计算 5m/1h 的五特征期望值，对比隔离后的降采样查询；同时遍历实际 part，验证每个 part 只包含所属 tenant，合并前后数值、时间戳及 TSID 遍历均正确 |
| 容量与故障测试 | 逐步增加租户数、历史月份与并发；报告 RSS、缓存占用、文件句柄、goroutine、启动峰值、吞吐和延迟；分别验证启动时目录打开失败，以及运行期间只读和管理操作失败的影响范围 |

测试必须同时验证文件归属和查询结果：仅验证 PromQL 无串租户，不能证明物理文件已经隔离。基准集群与候选集群应保持相同的副本、dedup、retention 和查询设置；多副本及故障重试测试单独评估重复样本，不以正常写入结果替代。

实施前仍需确定单节点活跃租户规模，以及独立管理的优先级。本文默认首期重点是目录隔离、独立 retention 与 snapshot，不承诺未经测试的容量上限。
