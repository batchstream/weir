# M35：最后未评估节点返回 NotFound，停止只读检查

2026-09-29（北京时间）。本地 main 基线 `f0ac94395f2e2e646accbf580de99f36c0fbffe6`。本轮仅提交本报告与 readiness；交付 SHA、封存 manifest 哈希见 `.testdata/m35/delivery.json`。

**唯一准确 node GET 返回 NotFound，结果为 unknown，未观察到候选。** 固定 context 为 `arn:aws:eks:us-west-1:956540890581:cluster/data-team`，目标 `ip-172-31-88-88.us-west-1.compute.internal`，预期 M34 UID `a88b0a1f-bc55-4487-afdf-2dfb8d1b5942`。服务端原始错误：

```text
Error from server (NotFound): nodes "ip-172-31-88-88.us-west-1.compute.internal" not found
```

没有取得新 node 投影，无法确认本次 UID、架构、内核、kubelet、健康状态或 allocatable，也无法逐项比较 M34 字段变化。M34 的 linux/arm64、内核 `6.12.77-99.140.amzn2023.aarch64`、kubelet `v1.35.3-eks-bbe087e` 和 7.910 allocatable CPU 仅是历史值，不作为本轮观测。未发出 Pod 请求；完整请求账本、allocated、remaining 和全部门槛结果均为 **unknown / 未评估**，没有将错误当成零资源。

## 请求与停止边界

唯一不可续期120秒窗口 UTC `2026-09-29T00:33:51.223876Z`—`00:33:54.466263Z`，实际 **3.242351秒**；monotonic 起点 `416699.38738075`、截止 `416819.38738075`。实际 node CLI 区间 `00:33:51.224371Z`—`00:33:54.462835Z`，3.238429秒，exit1，stdout 0 bytes、stderr 91 bytes。

实际 **1/2 条集群数据请求**：准确 node GET 一次，Pod GET 零次。固定 context、10秒请求flag、25秒CLI加4秒Stop/Wait保持，派发余额119.999515秒。NotFound 后立即停止，0重试、0目录重扫、0其他节点/cluster身份/AWS/auth/CNI/namespace查询；没有后续在线检查或原地修复重跑。

原最低余量仍为 **7 CPU、5632 MiB内存、5 GiB ephemeral、3 Pod**，适用于 Weir/ES/发生器整套组合。原 `eks_resources.projection(..., scoped=True)` / `allocated` / `node_gate` 未改；spec/allocated/actuated逐资源max、init/sidecar峰值、Pod级请求及overhead口径保持。本轮因第一步失败未运行在线账本，不声称容量不足的具体数值或整个集群无资源；M33/M34其他节点结果各自保留原时间。

## 离线核验、封存与交付

忽略目录 `.testdata/m35/` 保存任务/状态/目标/独立M34报告快照、工具与388项输入哈希、短单节点runner、冻结准确argv、完整原始最小响应、返回码/字节/UTC与monotonic时序、Wait/句柄记录、离线核验及 manifest。没有修改旧runner、产品、Go/module、runtime、资格脚本、镜像或门槛。

在线请求前用M34原目录验证目标身份/静态投影，用一个旧node/Pod原始负例复现1.560 CPU余量及拒绝；分页不完整、越界节点、重复UID、DRA与未知resize负例全部拒绝。这些是离线回放，不是本轮在线快照。历史manifest字段格式曾导致两次本地准备错误，均在首次集群请求前修正于新忽略审计脚本，错误记录保留；没有改正式执行器。4586份历史封存逐项前后重验不变。未运行全仓Go、完整Python、后端或容器测试。

CLI已Wait且双输出句柄关闭；runner已exit1，CLI进程组及记录的执行/核验PID已结束。0远端写入、对象创建、部署、prepare/run、exec/logs、镜像操作、容器、DB、采样或负载；无自有远端UID及清理动作，不称六UID清理成功。没有秘密读取、业务/节点/系统变更、push/Actions或timer。

仅本地main文档提交，工作树干净；既有 `origin/main` 跟踪引用仍 `fa871b29ec5156256d8892cea22c1a2d047521c2`，交付后ahead4，**未刷新远端引用**。封存后向统筹一次交付/unknown回调并停止，由统筹独立复核并向用户提出指定现成隔离环境的需求；不自动续阶段、寻找替代节点、降低门槛、腾挪业务或扩容购买。

原新镜像EKS完整资源/流验证、容量与延迟冻结、两规格、六原生平台、各后端、跨worker、网络隔离、恢复及24h资格仍未完成；resource partial/not-qualified、timing not-run、performance candidate=null。CNI disabled仅为M33 UTC 2026-09-28 23:32:32—34历史观察，本轮未复核。用户排除Weir认证、首版延期通用脚本的决定保持。
