# M26R5C — 清理生命周期边界与五 UID 回收

2026-09-28；基线 `96ab67e5c9c7f35375cb1a37dd09b29e80be7b4b`，
真实清理使用实现 `7655f561a0e51d7ad9c4b620064d915ca40d5a50`。
清理后离线回归的失败记录修复为 `fa89628c216434eed0efa3e81502ebbe69298e3e`；原冻结实现、计划与成功请求记录不改写。
交付 SHA、最终证据 manifest/hash 及进程收口见 `.testdata/m26r5c/closeout.json`。
任务 SHA256 `4000f423f08bd8636c4f222d7fc6fd351975ced68aea0950f70d216f1933baed`。

**本独立清理阶段完成；原 M26R5 仍为 failed。** 原功能双短 trial 已获独立有限接受；
原入口清理失败及原 180 秒恢复超时均保留。此次是另行明确规划的一次清理窗口，
未延长、重置或改写原 attempt。未运行 SDK、Docker ES 前置、Go 产品测试或功能负载，
没有新建集群对象、容器或网络，没有 push、CI、镜像构建/发布。

共享 `eks_pacing.Run.inventory()` 仍枚举全部可 list 的 namespaced API，并核验投影 namespace。
只有准确 `metrics.k8s.io/v1beta1 / PodMetrics`，且实际 discovery 的 pods 描述为 namespaced、
kind 一致、verbs 恰为 get/list 时，才按只读派生视图单独存证；不进入 owned、不发送 DELETE。
缺 discovery、额外 mutation verb、未知 API/版本/kind、可疑投影仍拒绝。
分类依据是 [Kubernetes 指标管线语义](https://kubernetes.io/docs/tasks/debug/debug-cluster/resource-metrics-pipeline/)
和 [metrics-server v0.8.0 Pod API 实现](https://raw.githubusercontent.com/kubernetes-sigs/metrics-server/v0.8.0/pkg/api/pod.go)，
不把它们称为线上 metrics-server 准确构建版本证明，也不把 RBAC 无 delete 权限当作 API 只读证明。

持久对象按准确 API/kind/name/UID/owner 核验；默认对象必须保留原 UID、无归属漂移。
历史 Pod UID 只用于识别其 Event，不授权删除 Pod。Secret 不查询，由登记 quota 的 spec/status
hard=0、used=0 提供保守证明。普通自有对象删除后，在 quota 仍存在时重做完整盘点；
quota 倒数第二、namespace 最后删除。每次删除前再次 GET 核验身份，DeleteOptions 只含原 UID
precondition 与 Orphan；模糊响应只 GET 核对、不重放。盘点、输出、身份或删除失败即停止。

清理入口只装载冻结计划并复用共享 Run，不调用原功能入口，不替换共享函数。
每个 CLI 为 Stop/Wait 预留 4 秒，清理执行最多 21 秒；kubectl request-timeout=10s，
单项 8MiB/总 128MiB 上限保留。完整 API 清单按最多 20 种资源分批，未以 delete verb 过滤。

离线回放保留真实 44 行原始 metadata inventory、全部 UID 和实际 discovery 及来源 hash；
旧提交重现 `foreign resource: PodMetrics/loopback-wftg6/<no value>`，新代码分出 1 条指标视图。
原投影没有 namespace 字段，回放明确从原固定 namespace 命令补入新字段；不是新线上观测。
外来 Pod、空/替换 UID、API/name/owner 漂移、默认 UID/owner 变化、错 namespace、mutation verb、
discovery 缺失/失败、Secret 非零、nil labels、超时/输出失败及最后盘点新增 foreign 均覆盖。
所有离线 DELETE 检查已登记 UID；模糊响应无重放；专用入口只删五 UID 且拒绝二次 invocation。
定向普通 14 项通过。第一次清理后全量普通模式 140 项中 1 项失败：删除失败仅有顶层错误，
逐对象失败记录缺失；随后补回该记录，定向 8 项通过，并离线重跑回归。没有再调用线上清理。
最终全部 scripts Python：普通模式 140/140 通过、0 skip；优化模式 115 通过、25 项既有明确 skip，0 意外 skip。
受影响的 eks_pacing 30 项在两种模式均通过。
模板检查只运行离线标准库 Go template 小程序，不是重跑 Go 产品测试。
首次回放夹具提取命令资源参数的下标错误和首次审计 Counter 用法错误已修正，失败日志保留；
均为本地证据接线问题，没有线上重试。

## 唯一真实清理

证据目录 `.testdata/m26r5c/cleanup/` 权限 0700；plan 权限 0400，SHA256
`fe4709616fd918db7ee28efbaefdda5cde33b6904712b23912c79542e478a293`，冻结 221 项代码/产品/原证据输入。
固定 context `arn:aws:eks:us-west-1:956540890581:cluster/data-team`；
namespace/owner `weir-qual-m26r-r5-20260928-0749`。
2026-09-28 **08:36:23.848089Z 至 08:38:32.789934Z**，单调总耗时 **128.940 秒 < 180 秒**。

首次核验五 UID/owner、Job terminal、Pod 列表为空；两次完整盘点分别 54/51 个持久对象，
其余是原默认对象及指向已登记 Job/历史 Pod 的 Event，没有外来资源。
两次 quota 的 spec/status hard 与 used 均为 0；未查询 Secrets。
两次实时 metrics API discovery 均仅 get/list，本次已无 PodMetrics 行；非空视图分类的验证来自原始离线回放。

| 删除对象 | 准确 UID | 条件 DELETE 开始 UTC | 结果 |
| --- | --- | --- | --- |
| Job/loopback | `39cb191b-7cd4-4c4e-a020-ca57ea34c4c3` | 08:37:16.156793 | 一次 DELETE，GET 确认不存在 |
| ConfigMap/configuration | `cc615037-15d0-4f14-91ee-964c1bfd4074` | 08:37:22.502029 | 一次 DELETE，GET 确认不存在 |
| NetworkPolicy/default-deny | `798e9fb4-995b-4c5c-ad70-b63cc4b86674` | 08:37:28.737039 | 一次 DELETE，GET 确认不存在 |
| ResourceQuota/budget | `6bbc15fb-7300-4670-9358-63c646f4ffe1` | 08:38:11.307028 | 最后完整检查后删除，GET 确认不存在 |
| Namespace/weir-qual-m26r-r5-20260928-0749 | `b045b500-f1d7-4b85-97a5-75f172b44d06` | 08:38:17.221321 | 最后删除，独立 GET 确认不存在 |

原默认 ConfigMap `990e357b-63d8-40fb-ab1a-362d7cfeb4cf`、ServiceAccount
`2e92a844-314a-4562-9d98-c3ff0af36e82` 两次 UID 一致，没有逐个删除。
原 Pod `66924d31-e9c3-4fc5-9d5c-975f1d55001c` 没有 DELETE 请求。
最终额外 GET 确认 namespace、Job、原 Pod 均为 NotFound；不是仅依据 DELETE exit0。
44 条 CLI（2 git、42 kubectl）全部 end/exit0，最长 9.123 秒，所有请求和子进程结束。
精确 DELETE 共 5 次，没有 force/finalizer 操作、批量 label 删除或模糊请求重发。

清理前后 200 产品输入、1073 本轮/189 R4/3120 历史证据逐项 hash 相同。
原 M26R5 result SHA256 `06d81b6cb71756d4e80927e467c94a66adb1eba0062067c850b8d0df9eee2b97`；
原本轮 manifest `1c454dc7c0b8252ec654bb009b73c5ce66b6025f71ff8b603b50a8bf995fe0be`，
R4 manifest `dc2402ef7adb8d2fc37044d71e0e375599d822331065710180a7f473c40fb8ae`，
历史 manifest `6eaeecf9547aecb0256d336da39186423e2a82af66618c7f312e7f1318e28616`。
新原始请求、分类、最终状态、离线回放、失败日志及核验脚本统一列于 `.testdata/m26r5c/manifest.json`。

candidate=null、完整 resource=partial、CNI unqualified、startup-signal-registration-window 未解决；
容量、过载、恢复、跨节点、完整平台/后端及 24h 门槛均不因此通过。
Weir 认证继续排除，通用 ProgramTransform 首版延期；本阶段不开启下一阶段或恢复定时器。
