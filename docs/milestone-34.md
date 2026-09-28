# M34：有界只读盘点结束，已查五个节点均未达到原资源门槛

2026-09-29（北京时间）。本地基线 `c33ddbacb07b1dd5c6152e3772e8106688cc1843`；本次仅文档提交，交付 SHA 见 `.testdata/m34/delivery.json`。原准确 M32R2 image source `278264db2f9617ad583c6b56d19b8aaf5943e771` 及 M33 接线保持不变。

**本次有限评估没有观察到符合原门槛的候选。** 完整目录含14节点，静态排除8个，按确定顺序检查5个，剩余1个适格节点因上限未评估。五个已查节点均 CPU 余量不足，不能据此宣称整个集群没有资源；不继续扫描、重试或等待业务变化。

## 范围与窗口

所有11条数据读取显式固定 context `arn:aws:eks:us-west-1:956540890581:cluster/data-team`，region `us-west-1`。仅一次最小 node 目录、五次准确 node GET 和五次对应 `spec.nodeName` 限定的全namespace Pod资源投影。可选 cluster 身份请求未执行，控制面版本未刷新；未更改 current-context。

UTC `2026-09-28T23:52:39.534822Z`—`23:53:06.321420Z`，总26.787秒；原始 monotonic start `414227.751971416`、deadline `414827.751971416`，600秒总窗未续期。每候选120秒共享两次读取，实际最长4.948秒；全部请求10秒flag、CLI25秒及4秒Stop/Wait，派发前均保留完整29秒余额。11/12数据请求额度，全部exit0、stderr空、0重试；五次 `node_gate` 资源拒绝原样保存。

目录使用 `--chunk-size=0`，投影 `itemsType=[]interface {}`、continue=null、remainingItemCount=null，14项全部解析，没有截断或续页。只保留名称/UID、架构/OS/内核/kubelet、allocatable、四项健康条件及调度/污点/删除状态；未读取完整labels、annotations、地址或providerID。没有硬件标签请求。

目录排序为 allocatable CPU降序、内存降序、名称升序，最多5个。每个准确GET与目录的UID、架构、版本、状态及allocatable完全匹配；Pod投影完整、全部node匹配且UID唯一。未查询旧M33节点的Pod，也未查询其namespace。

## 已检查节点与资源账本

下表五个节点均为 **linux/arm64**，内核 **`6.12.77-99.140.amzn2023.aarch64`**，kubelet **`v1.35.3-eks-bbe087e`**；Ready=True、三项压力=False，无污点、不可调度或删除状态。

| 顺序 | 节点 | UID | 两次GET快照区间（UTC，2026-09-28） |
|---|---|---|---|
| 1 | `ip-172-31-1-184.us-west-1.compute.internal` | `a78458e1-fecc-4c98-a8b3-7f01594d7400` | 23:52:42.589—23:52:47.221 |
| 2 | `ip-172-31-12-172.us-west-1.compute.internal` | `f1e076d4-6830-46fc-8276-c3d1097557eb` | 23:52:47.234—23:52:51.869 |
| 3 | `ip-172-31-114-215.us-west-1.compute.internal` | `1068c5f7-2c71-45ae-938c-6af681aeee9b` | 23:52:51.884—23:52:56.804 |
| 4 | `ip-172-31-178-253.us-west-1.compute.internal` | `41943f2c-a5bd-470e-b08c-7c753946800f` | 23:52:56.832—23:53:01.496 |
| 5 | `ip-172-31-186-217.us-west-1.compute.internal` | `8d5af613-e136-4a16-9a23-cb21555957f9` | 23:53:01.503—23:53:06.304 |

以下每格为 **allocatable / 保守allocated / 剩余**。内存与ephemeral使用bytes。原门槛保持 **7 CPU、5,905,580,032 bytes（5632MiB）内存、5,368,709,120 bytes（5GiB）ephemeral、3 Pod**。

| 顺序 | CPU | memory bytes | Pod数 | 逐项门槛 CPU / 内存 / ephemeral / Pod |
|---|---|---|---|---|
| 1 | 7.910 / 6.350 / 1.560 | 31982452736 / 31151095808 / 831356928 | 58 / 19 / 39 | FAIL / FAIL / PASS / PASS |
| 2 | 7.910 / 6.450 / 1.460 | 31982452736 / 15917383680 / 16065069056 | 58 / 20 / 38 | FAIL / PASS / PASS / PASS |
| 3 | 7.910 / 5.950 / 1.960 | 31850659840 / 16861102080 / 14989557760 | 58 / 55 / 3 | FAIL / PASS / PASS / PASS |
| 4 | 7.910 / 5.060 / 2.850 | 31850659840 / 17624465408 / 14226194432 | 58 / 5 / 53 | FAIL / PASS / PASS / PASS |
| 5 | 7.910 / 4.090 / 3.820 | 31850659840 / 14445182976 / 17405476864 | 58 / 30 / 28 | FAIL / PASS / PASS / PASS |

五个节点的 ephemeral 均为 **18,182,813,665 / 0 / 18,182,813,665 bytes**，全部通过。Pod原始项数依次19、20、55、5、30；全部通过保守计账，没有未知/DRA/resize错误。节点3的Pod余量恰为3，只是该项通过。

直接复用未修改的 `eks_resources.projection(..., scoped=True)`、`allocated` 与 `node_gate`：具名spec/allocated/actuated逐资源max，init/sidecar峰值、Pod-level与overhead全部保留。没有把未知当0，没有用利用率或普通容器简单相加代替请求账本。每次读到的资源快照非原子、没有预留；任何后续独立阶段仍须重新正式准入。

## 排除与未评估范围

以下七个节点的allocatable CPU均仅3.920，本身低于7，因此只出现在目录，未查询Pod：

- `ip-172-31-11-106.us-west-1.compute.internal`
- `ip-172-31-14-228.us-west-1.compute.internal`
- `ip-172-31-3-155.us-west-1.compute.internal`
- `ip-172-31-4-62.us-west-1.compute.internal`
- `ip-172-31-5-117.us-west-1.compute.internal`
- `ip-172-31-7-19.us-west-1.compute.internal`
- `ip-172-31-9-189.us-west-1.compute.internal`

旧节点 `ip-172-31-12-243.us-west-1.compute.internal` / UID `18f03c58-83c1-424e-be34-44ef88c07831` 为第八项排除，未再读取其Pod。原M33 6.900＜7失败保留。

静态适格但超过5个上限而**未评估**：`ip-172-31-88-88.us-west-1.compute.internal` / UID `a88b0a1f-bc55-4487-afdf-2dfb8d1b5942`，目录allocatable为7.910 CPU、31,850,659,840 bytes内存；其allocated与剩余为**unknown**，不能声称满足或不满足余量门槛。本轮不继续查询。

## 证据、零创建与后续需求

`.testdata/m34/` 保存冻结runner源码/工具/输入hash、准确argv、完整最小投影raw、返回码/bytes/时序/Wait/句柄状态、目录选择与全部排除、每候选保守账本及逐项门槛。`verification.json` 为独立离线重算；`manifest.json` 按文件hash/size封存，`delivery.json` 分列实际交付SHA、既有origin/main跟踪引用及源码不变证明。4462份历史封存逐项前后核验不变。

调查前用M33原始node/Pod响应离线重现6.900 CPU及原拒绝；投影与旧准确命令一致，合成目录检验排序、旧节点排除、5项上限、分页不完整拒绝和Pod越界拒绝。没有重跑Go或完整Python测试。一次性runner仅位于忽略目录，复用已有CLI owner；被动return trace记录child PID、Wait与双输出句柄关闭，未替换生产函数或guard。

Namespace/Pod/Job/Service/Quota/Policy/ConfigMap/DB创建、prepare/run、exec/logs、镜像拉取或构建、本地容器、采样、seed/trial/负载、索引PUT/文档变更全部0；无自有远端对象或UID，未进入清理，不能写作六UID清理成功。没有全cluster Pod扫描、Secret/凭据文件/业务数据读取、权限/CNI重查、push/Actions、节点或业务修改、付费资源或timer。所有调查CLI及runner已结束。

下一步由统筹向用户请求指定现成隔离环境/节点：linux/arm64、Ready且无压力/污点/删除/不可调度，并在完整保守请求计账后余量至少7 CPU、5632MiB内存、5GiB ephemeral及3Pod。缺口是本次已查环境容量；不自动腾挪业务、扩容或购买，也不降低门槛。当前五个节点CPU最高余量3.820，无法支持在同一账本上额外请求4 CPU的Weir参考实例；没有新的正式阈值或性能/隔离/跨worker资格结论。

CNI disabled为**M33 UTC 2026-09-28 23:32:32.099—23:32:34.471**的观察，本轮未复核，不能称实时状态已改变。原新镜像approved不变，resource partial/not-qualified、timing not-run、performance candidate=null，CNI/跨worker/六平台/两规格/各后端/安全/容量/恢复/24h仍未完成；认证排除、通用脚本首版延期保持。

仅在本地main提交本报告与readiness，既有origin/main跟踪引用 `fa871b29ec5156256d8892cea22c1a2d047521c2` 未刷新或推送：交付后ahead3，不能当远端实时状态。产品、Go/module、runtime、资格执行脚本、制品与门槛均未改。main干净、证据封存与自有进程收口后，向统筹一次完成/环境阻塞回调，然后停止。
