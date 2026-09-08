# 降采样存储测试说明

## 范围与判定

基线为原版 `v1.151.0-cluster`，候选为当前分支。降采样存储和字段查询版本均为 2。每侧运行真实的一个 vminsert、一个 vmstorage 和一个 vmselect，replicationFactor=1，全部绑定 loopback，使用独立目录。

验证 raw 写入 → 降采样落盘 → 持续归并 → 字段读取与重启。独立参考按原版查询结果或明确的写入贡献分桶计算五特征；标签、序列集合、点数和时间戳严格相同，数值容差为相对 `1e-10`、绝对 `1e-9`。

## Go 测试

| 层次 | 验证内容 |
|---|---|
| 数学计算 | raw/摘要、多轮归并、同时间戳贡献、NaN/Inf、边界与独立随机参考 |
| Block 与文件 | 原生 Block 编解码、独立 99/112 字节解析、五列顺序、共享时间戳、offset/size、metadata 及损坏拒绝 |
| 存储生命周期 | inmemory 保持 raw，dump/flush/merge/关闭/snapshot 输出摘要；失败、空间不足及发布清理 |
| 遍历与 tenant | 同 TSID 跨 Block/index、跨 TSID 与 AccountID/ProjectID、过滤、缺失序列及对象复用 |
| RPC | 十字段、tenant 保留、原生请求兼容、无效负载、服务端分派、原生 MetricBlock 响应及节点错误传播 |

在仓库根目录执行；同一 checkout 的完整 storage 测试串行运行：

```sh
go test ./lib/storage ./lib/encoding ./lib/decimal ./lib/vmselectapi ./app/vmstorage ./app/vmselect/netstorage ./app/vmselect/prometheus ./app/vmselect/promql ./app/vminsert ./app/vmselect -count=1
go test -race ./lib/storage -count=1
go test -race ./app/vmselect/netstorage ./lib/vmselectapi -run '^TestDownsample' -count=1
go vet ./lib/storage ./lib/vmselectapi ./app/vmstorage ./app/vminsert ./app/vmselect ./app/vmselect/netstorage ./app/vmselect/prometheus ./app/vmselect/promql
```

## Python 集群对照

分别在隔离基线源码目录和当前源码目录构建三个组件，保存 commit、源码和 binary SHA256；候选生产代码变化后必须重建。每组 binary 目录包含 vminsert、vmstorage、vmselect：

```sh
go build -o /tmp/vm-downsampling-version2-20260908/candidate/ ./app/vminsert ./app/vmstorage ./app/vmselect
```

测试步骤：

1. 经 vminsert 的 `/insert/<account:project>/prometheus/api/v1/import` 写入相同输入；等待 vmstorage 实际接收全部行。
2. 调用 vmstorage 的 force_flush/force_merge，等待本次异步请求完成及 metrics 缓存过期；确认无 inmemory 行、无活动 merge，每个非空月各一个 file part。
3. 通过 vmselect 的 `/select/<account:project>/prometheus/api/v1/query` 与 query_range 比较全部十种 query.field；使用裸 selector，避免以 PromQL 聚合替代存储计算。
4. 覆盖分批写入、迟到输入、摘要重写及重启；严格验证重写改变 part 代次、重启保持已完成发布的代次。两个进程均设置 dedup interval 为零。
5. 独立解析实际文件，核对版本 2、完整 tenant、五列字节覆盖、共享时间戳及统计；如实记录实际命中的 index 边界。

以下命令的 output 必须为不存在的新目录；复现时替换该目录名：

```sh
python3 lib/storage/testdata/downsampling_compare.py --mode cluster --tenant 11:17 --original /tmp/vm-downsampling-version2-20260908/original --candidate /tmp/vm-downsampling-version2-20260908/candidate --output /tmp/vm-downsampling-version2-20260908/reproduce-3h
python3 lib/storage/testdata/downsampling_cluster_tenants.py --original /tmp/vm-downsampling-version2-20260908/original --candidate /tmp/vm-downsampling-version2-20260908/candidate --output /tmp/vm-downsampling-version2-20260908/reproduce-tenants
python3 lib/storage/testdata/downsampling_multiseries_compare.py --mode cluster --tenant 11:17 --days 93 --series 160 --dense-series 4 --original /tmp/vm-downsampling-version2-20260908/original --candidate /tmp/vm-downsampling-version2-20260908/candidate --output /tmp/vm-downsampling-version2-20260908/reproduce-multiseries
python3 lib/storage/testdata/downsampling_cluster_compatibility.py --original /tmp/vm-downsampling-version2-20260908/original --candidate /tmp/vm-downsampling-version2-20260908/candidate --output /tmp/vm-downsampling-version2-20260908/reproduce-compatibility
```

文件检查使用 `downsampling_inspect.py`，指定 `--data-dir`、`--output`、`--expected-series`，并用重复的 `--expected-tenant` 校验精确租户集合。检查器按固定偏移解析，仅依赖系统 libzstd 解压，不使用生产 decoder。

## 当前结果

证据目录：`/tmp/vm-downsampling-version2-20260908`。以下结果来自当前代码重新构建的候选与全新测试数据。

| 项目 | 结果 |
|---|---|
| Go 普通回归、RPC 定向 race、vet | 通过 |
| 3 小时对照 | 175 项通过 |
| 同一 vmstorage 三 tenant 隔离 | 560 项通过 |
| 160 条时间线、93 天对照 | 562 项通过 |
| 原版组件兼容性 | 3 项通过 |
| 独立文件检查 | 通过 |
| 完整 storage race | 通过（182.881s） |

集群对照共 1300 项，数值比较的最大绝对误差为 0。构建、Python 测试命令和结果、源码及 binary 哈希见 `manifest.json`；Go 验证与代码、文档检查见 `review.json`。

长跨度产物包含 160 个 TSID、4 个 part、6050 个 Block、694200 个物理行、14 个 index。实际文件命中同 TSID 跨 Block、不同 TSID 跨 index；同 TSID 跨相邻 index 由定向 UT 覆盖，本次 E2E 未自然命中。

覆盖范围为存储与字段测试接口。93 天表示输入的历史跨度，不表示持续运行 93 天；本轮不验证多 vmstorage 副本、故障切换或跨未合并 part 的查询侧再聚合。
