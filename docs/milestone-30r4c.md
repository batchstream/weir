# M30R4C — 两个准确残留的单次清理失败，零 DELETE

2026-09-29（Asia/Shanghai）。**本轮清理未完成，两个残留仍待处理；没有 namespace 实际不存在的证据。**
唯一 cleanup-only invocation 在完整盘点第一批触及单命令本地截止后停止，`confirmed=false`。
成功读取过准确 Namespace/Quota 身份、空 Pod 列表和 Secret 零计数，但不能据此越过完整归属门槛。
本轮没有新建对象、部署/负载、observer 调查或重跑，没有第二窗口、恢复定时器或预算外查询。

## 冻结边界与最小实现

- 干净 main 基线：`4b03cb127de27fea1308f342f9ff28488e517ce9`。
- 实现、聚焦测试和唯一在线执行：`db68a3845291e20d7ecbf750c91184101fd97a7b`。
- 固定 context：`arn:aws:eks:us-west-1:956540890581:cluster/data-team`。
- namespace/owner：`weir-qual-m30r4-20260928-233348`。
- 全新证据根 `.testdata/m30r4c/` 为 0700；`plan.json` 为 0400，27 项输入逐项冻结。
- plan SHA256：`ab9c3df19691a0ed27413e101c48b5faf4f61aa1fb845a7484cbf0b7a0754dee`。
- `invocation.json` exclusive-create 一次；总 180 秒、最后 45 秒 namespace/readback 预留、每 CLI 含 Stop/Wait 最多 25 秒、
  kubectl request-timeout 10 秒、单命令 8 MiB/总证据 128 MiB 保持不变。

仅调整既有 `eks_cleanup.py` 输入边界。`owned` 明确当前两个或原有五个 DELETE 目标，`stopped` 明确已关闭的历史对象；
两者必须准确划分冻结原始 `owned.json` 中全部六个唯一 UID，owner/name/kind/UID 不得改变。
仅历史 Job/Pod UID 用于识别已有 Event；它们若作为持久对象重新出现，仍被当作外来对象拒绝，绝不采用或删除。
旧五目标入口保留，统一使用同一输入合同，没有第二套清理 CLI/包装或执行框架。

共享 `Run.cleanup/inventory/foreign_check/delete` 未改动。保持完整 API catalog、正式发现 PodMetrics 只读分类、
Quota spec/status hard/status used `count/secrets=0`、默认 UID、外来对象及条件 DELETE 守卫。
入口不再重复查询已被共享 `delete()` 的成功 GET 证实不存在的 namespace 及其子对象；
namespace 已不存在时仅首次 GET 即停止。DELETE exit0、Terminating、非零空 GET 均不构成不存在的证据。

## 唯一在线窗口与准确残留

窗口开始 UTC `2026-09-28T16:17:53.488928Z`，monotonic `386941.910243875`。
总 deadline `387121.910243875`，对象阶段 deadline `387076.910243875`，最后 45 秒预留没有挪用。
实际在线耗时 **40.906769 秒**；父进程含本地 Git 校验/启动/Wait **40.993853 秒**，exit1。
失败时仍处于对象阶段，`result.json.deadline` 是当时对象阶段 deadline；完整总 deadline 见 invocation/cleanup-budget。

| 命令 | 本窗口开始后 | 耗时 | 结果 |
| --- | --- | --- | --- |
| 3 Namespace GET | 0.000 秒 | 4.857 秒 | exit0，原 UID/owner，Active |
| 4 Quota GET | 4.860 秒 | 2.149 秒 | exit0，原 UID/owner |
| 5 Pod 列表 | 7.012 秒 | 4.408 秒 | exit0，stdout 为空 |
| 6 Namespace GET | 11.422 秒 | 2.710 秒 | exit0，原 UID/owner，Active |
| 7 完整 namespaced/list API 发现 | 14.136 秒 | 1.732 秒 | exit0 |
| 8 metrics API 正式发现 | 15.869 秒 | 1.855 秒 | exit0，PodMetrics 只读语义通过 |
| 9 Quota GET | 17.728 秒 | 2.122 秒 | exit0，spec/status hard/used 的 count/secrets 均为字符串 0 |
| 10 完整盘点第一批 20 类型 | 19.853 秒 | 21.049 秒 | 本地截止，Stop/Wait 后 exit-15，stdout 0 bytes |

命令10 stderr 仅为 85-byte Endpoints deprecated 警告，没有 API read-timeout 错误。
这是共享单命令执行额度 21 秒触发的本地截止；不能归因于总窗口耗尽或指定网络根因。
后续批次、完整 `foreign_check`、默认对象 fresh UID 检查、删除前最终 Namespace 身份检查均未完成。
不使用部分盘点、旧默认 UID/Secret 快照或最后 45 秒绕过守卫，立即结束本次 invocation。

| 当前删除目标 | UID | 本轮结果 |
| --- | --- | --- |
| ResourceQuota/budget | `2deaa5be-0822-4db5-83bc-df4d2ec9233e` | DELETE 0 次；命令9最后成功读取仍存在，未清除 |
| Namespace/weir-qual-m30r4-20260928-233348 | `f6b048cc-447d-4ee8-8835-11ef13542758` | DELETE 0 次；命令6最后成功读取 Active，未清除 |

以上是本窗口最后成功观测，不声称失败后又取得最新状态。没有最终 namespace absence GET。
两对象的 raw GET、UID/owner、逐项结果和完整命令时间线见 `cleanup-audit.json` 及 `command-*.{json,out,err}`。
共 10 条编号 CLI：2 条本地 Git、8 条只读 kubectl；9 exit0、1 exit-15，全部结束并经共享 `stop_group` Stop/Wait。
没有 mutation/DELETE、Secret GET/LIST、force/finalizer、默认对象逐项删除、集群/节点/业务修改或扩容。

## 历史工作负载与默认身份

原 M30R4 成功空 GET 保留；本轮不将下列 UID 放入 DELETE 集合，不创建/重新采用它们。

| 历史对象 | UID | 原 M30R4 关闭证据 |
| --- | --- | --- |
| Job/loopback | `6a24a607-61fc-4e85-9e40-60b34f37a5ef` | 命令124 exit0 空 GET |
| Pod/loopback-4rs4j | `27e6d830-67fa-4f70-9de4-64db840734a6` | 命令128 exit0 空 GET |
| ConfigMap/configuration | `51b30696-baf1-4eb7-a6ba-3e26dd7aafc8` | 命令131 exit0 空 GET |
| NetworkPolicy/default-deny | `574bf5a6-ab12-4a3b-99ed-558467ef4a85` | 命令134 exit0 空 GET |

本轮命令5的成功空 Pod 列表是新的无 Pod 观测；原精确 Pod 关闭证据仍有效保留。
本轮没有完整 fresh Job/ConfigMap/Policy 缺席及默认对象身份盘点，不能冒称取得这些新证据。
冻结默认 ConfigMap/kube-root-ca.crt UID `7e99a235-b98e-47b5-937f-f1836aa4ac0e`、
ServiceAccount/default UID `d045fac6-a713-43f6-9fe0-0e28d0256cbd` 仅供盘点，不授予逐项删除权限。

## 离线验证与证据封存

聚焦 `eks_pacing_test.py` **40 项通过，56.867 秒**。新增覆盖两目标成功/恰好两次条件 DELETE、历史 Job/Pod Event、
陌生 Event/Pod/持久对象、历史对象重新出现、默认 UID 变化、同名新 UID/owner 变化、Quota 缺失/Secret 未知或非零、
盘点不完整/本地超时、证据写入失败、重复 invocation、namespace 已不存在、DELETE 不确定只读核对及非零空 GET 拒绝。
保留原五目标和完整 API/PodMetrics 守卫覆盖；真实 fake CLI 逐项验证结束/exit 与 Stop/Wait。
首轮新增断言误把“有逗号的批次”当作全部批次，漏计最后单类型批次；已修正为准确列表投影计数。
首败日志原样保留，不是产品失败或 EKS 成功证据。所有 fake 只算离线测试。

完整普通 Python 回归 **198 项通过，168.821 秒**；优化模式共 **198 项，173 通过/25 项既有 skip，163.636 秒**。
原始日志与完整退出/Wait 记录见本证据根 `all-normal.*`、`all-optimized.*` 和最终交付记录。
命令为 `python3 [-O] -m unittest discover -s scripts -p '*_test.py'`。
没有 Go/module/产品功能变更，因此不重复 Go/race/构建，不将 Python 测试当成 Go 或原生生产资格。

执行前后逐项复核 M30R4 **544 份**以及 M30/M30R/M30R2/M30R3 **1001 份**封存文件，
并核验 **204 个 Go/module、Weir70/helper81 正式输入**与准确 image source `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。
原 M30R4 manifest 保持 `87fbd519d33fa9beacfaf7e734c3ce7c3bbd44fe968a3fe0cd54dbc1d4925bfd`；
旧四轮 manifest 见 `history-before.json`、`history-final.json` 和原 M30R4 报告。
只按封存清单校验历史证据，制品仅使用明确公开文件；未读取匿名客户端状态、kubeconfig、credential、token 或敏感文件。
本轮 `manifest.json` 封存原始命令、输入/计划、测试、UID 结果及校验记录，排除自身和避免自引用的 `delivery.json`。
实现/交付 SHA、最终 manifest SHA/计数、main 状态和本地进程结束证据见 `delivery.json` 与完成回调。

## 原失败和资格门槛不变

M30R4 原 `result=failed`、`cleanup=false` 永久保留；本次清理同样失败，没有新的采样通过结论。
原两 observer 缺 `observer_end`、Weir 尾行截断、client snapshot 缺失仍未解决。
M30R4 的资源窗口与一次 release 远端完成确认仅记为统筹已独立复核的有限机制证据。
`resource_evidence=partial/not-qualified`、`timing=not-run`、`candidate=null` 不变。
Auth 排除、ProgramTransform V1 延期不变；CNI/跨节点、其他规格/后端、六平台、容量/过载/恢复及 24h 仍 required。
本轮不 push、Actions、发布、镜像更改或部署。一次失败回调交还统筹规划，然后停止，不自行续派或恢复。
