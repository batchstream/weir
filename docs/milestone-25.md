# M25 — EKS 专用 namespace 预检；入口失败，原生探针未运行

2026-09-28；执行聊天 `01a0e5c6-d2d7-7c93-8c74-fa88e796caee`。
**本次没有取得准确镜像在 EKS 上的运行、发生器计时或容器资源资格。**
唯一冻结 invocation 在 namespace 清单解析处失败，尚未创建任何 Job/Pod。
这是本阶段新入口的缺陷，不归因于 Weir、EKS 性能或外部权限。
修复后只完成自有资源回收和离线回归，没有覆盖计划或再次运行探针。

## 身份、前提与冻结计划

- 本地 main 基线：`a04322233ce3c53238cc2ddec514f7b68e43ecd4`。
- 本次实际执行的入口：`f25ebccb3c9e4fd15f14e7228116659974ad392d`。
- 清理模板修复及最终离线回归：`6152372b83a5a7ae338478ede8a4571193a63410`。
- 准确产品/工具镜像 source 仍是 `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`；
  本阶段没有修改产品/helper Go、module、Dockerfile、CI、镜像或发布任何内容。
- M24 已获统筹的**有限镜像交付验收**。统筹重新取得两次 Actions 原始记录、
  独立匿名下载/审计 OCI、四个 Linux binary 交叉重编和离线测试；首败次胜仍保留。
  这不授予 EKS、发生器 5ms、后端容量或 24h 资格。

固定镜像如下；arm64 manifest/binary 及两架构完整身份见 [M24](milestone-24.md)。

```text
ghcr.io/batchstream/weir@sha256:cc6428d1ead507e531f95b8c45926f8bf31abf8ba9cb89cf6e8eca4a865b1f10
ghcr.io/batchstream/weir-qualification@sha256:8a5c4dbca08daae24f62798c40f1607bbfaa5362b5b86db9a5651eb5475b3057
```

受控证据根为 `.testdata/m25/run-20260928-0230/`，目录 0700，Git 忽略。
`plan.json` 与 `plan.sha256` 为 0400；冻结 hash：
`7334d23a8ce202bb9fa658e068ef6da8e358c009ca449da1375f826cf176a8ee`。
计划保存授权 target、namespace owner、节点 name/UID、代码 SHA/输入 hashes、准确镜像、
七个固定 Job 模板、采样/计时门槛及全部预算。真实环境绑定仅保存在该本地目录，
不写入公开源码或通用示例。`invocation.json` 排他创建，拒绝再次执行同一计划。

原定顺序为产品 `-version`、工具 `-mode snapshot`、50 ops/s ×20 秒三轮、
200×20 秒和800×20 秒各一轮。最多23,000 planned，0数据库 mutation。
每轮独立 Job，前轮 Job/Pod 清理后才可创建下一轮；不带 legacy-expiry、不重试坏点。
all/read/put 的 dispatch lag p99≤5ms、arrival p95≤100ms/p99≤250ms、零丢弃/错误/UNKNOWN；
样本包围测量、间隔≤6秒、单样本≤2秒，CPU 最大区间 fraction<0.9、无新 throttle，均未放宽。

## 实际只读环境与隔离边界

02:30–02:32 UTC 的多次 API 读取确认授权 EKS ACTIVE、control plane 1.36，
38/38 Ready 节点；先前30节点快照不是资源预留，也不是本次状态。
选取按名称排序的第一台满足余量的既有 Linux/arm64 节点，没有按性能试跑挑选节点。
该节点无 taint、无 pressure、未禁止调度；name/UID 在冻结后、首次写入前再次核验。

| 选择性观察 | 实际值与限制 |
| --- | --- |
| 内核 / kubelet | `6.12.77-99.140.amzn2023.aarch64` / `v1.35.3-eks-bbe087e` |
| Allocatable | 7.910 CPU、31,232,864 KiB 内存、58 Pod slots；不是物理硬件独占量 |
| 保守扣除后的初始余量 | 5.195 CPU、29,642,031,104 B 内存、52 slots |
| 扣除模型 | regular + 所有 init + Pod-level + overhead + status allocated/runtime resources 求和；会重复计入重叠，作为明确保守上界 |
| 准入下限 | 本轮最多1 CPU/512 MiB之外，仍保留至少1 CPU/1 GiB和2个空槽位 |
| CNI | VPC CNI `v1.21.1-eksbuild.7`，network-policy agent `v1.3.2-eksbuild.2` |
| 网络策略 | `NETWORK_POLICY_ENFORCING_MODE=standard`，agent 明确 `--enable-network-policy=false`；**实际网络隔离不具备资格** |

每次读取记录命令起止时间，不把不同 API 响应称为原子快照。
没有查询既有业务的 env、日志或配置，没有查询 Kubernetes Secret，也没有 endpoint 探测。
非终态 Pod 查询仅输出 node assignment、phase 和资源字段；未保存业务 Pod/namespace 名称。
没有改变全局 context、节点/CNI/kubelet/RBAC/autoscaler/AWS 资源，也没有触发测试 Pod 调度或扩容。

唯一新 namespace 带 owner 和 restricted Pod Security 标签；成功创建 default-deny
ingress/egress NetworkPolicy 与 ResourceQuota。Quota 限制1个 Pod、1个 Job、1 CPU/512 MiB，
禁止 Service/PVC/Secret；Secret 的零数量证据来自 Quota status，没有读取 Secret API。
策略对象存在不代表执行隔离，本次 CNI 参数明确不支持该资格。

冻结模板使用 nodeName、无自定义 toleration、preemptionPolicy Never、Job backoffLimit0、
restartPolicy Never、Job deadline120秒/Pod deadline100秒；不创建裸 Pod。
仅使用1 CPU/512 MiB、requests=limits、GOMAXPROCS1、65532非root、只读根、drop ALL、
seccomp RuntimeDefault、禁提权、禁自动token挂载/ServiceLinks；无凭据、volume、sidecar或主机访问。
API 通常注入的两条 node-lifecycle toleration 仅允许固定默认值，不用于接受有 taint 的节点。
**这些是模板约束，本次未产生实际 Pod，不能当作实际 cgroup/进程/镜像证据。**

## 失败与恢复清理

实际 namespace 创建于02:32:17 UTC；Policy和Quota随后创建成功，均保留 create UID。
尚未进入任何 Job 创建时，命令0040在汇总 namespace metadata 的 Go template 中执行
`index .metadata.labels ...`。标准 `kube-root-ca.crt` ConfigMap 没有 labels，导致
`index of untyped nil`。入口 exit1；第一次 finally 清理在命令0044遇到同一缺陷，
原 `result.json` 保留 `cleanup.confirmed=false`，没有倒写为成功。

修复为先判定 labels 存在，再取 owner。离线以真实 Go `text/template` 引擎验证
缺 labels 的 ConfigMap/ServiceAccount，以及 Pod 投影的意外 literal env 值脱敏。
随后只运行归属核验与清理，未创建新对象、未执行第二次原生 invocation、未修改冻结 plan。
恢复清理载入原成功 create UID 和已观察到的两个标准 namespace 默认对象 UID，
重新完整列出该 namespace 的非秘密资源 metadata；无 Job、Pod 或外来资源。

Policy、Quota、namespace 分别使用携带 `DeleteOptions.preconditions.uid` 的精确 API DELETE，
删除前逐项核验 owner/name/UID，未 force、grace0、删 finalizer、delete-all 或删除已有同名资源。
命令0059于02:35:18 UTC成功确认 namespace 不存在。恢复结果独立保存为 `cleanup-recovery.json`：
**恢复清理 confirmed=true；首次入口清理仍是 failed。**

从首次 finally 清理检查到最终确认约141.0秒，小于异常清理180秒界限；
从namespace创建请求开始计算的存活上界约183.3秒，小于15分钟总界限。
所有本地子进程均已 Wait，无测试/远端自有资源留存。
原 kubectl template 错误会附带调试原文，其中含该新 namespace 的标准公开 CA ConfigMap；
它仅保存在受控忽略日志，不提交或发布。没有 token、私钥、Secret 内容或其他业务数据交付。

## 结果与尚未运行的项目

| 项目 | 本次状态 |
| --- | --- |
| 准确产品镜像 `-version` / imageID | not-run |
| 工具 snapshot / binary / cgroup / PID / affinity | not-run；实际值未知，不能以模板资源替代 |
| 50×20秒第1/2/3轮 | 全部 not-run，无 timing-pass/rate |
| 200×20秒、800×20秒 | 全部 not-run |
| 实际计时 planned / DB mutations | 0 / 0；23,000仅为原计划上限 |
| resource-evidence | not-run |
| namespace 边界 | 新建且独占；仅基础对象，失败后已确认回收 |
| 网络边界 | unqualified，agent disabled，未做实际网络探测 |
| generator-ready-for-next-investigation | false，尚无本次发生器证据 |
| DB capacity candidate / full-calibration | null / not-run |

M22R 的 Linuxkit bounded-50 dispatch p99=8.3ms>5ms NO-GO 完整保留。
本次没有计时结果，不证明新 EKS runner 更好或更差，不证明 Weir 吞吐上限。
Weir认证排除、ProgramTransform首版延期/UNSUPPORTED，以及其他平台、后端安全、
完整容量/过载/24h门槛均未改变。修复的正向完整 EKS 路径仍未运行；后续是否安排新的
有界尝试由统筹决定，本聊天不续阶段、不启动自动任务。

## 离线验证、入口与证据

新增 `scripts/eks_pacing.py` 和独立的 `eks_pacing_report.py`；仅 `WEIR_EKS_M25=1`
显式入口可访问 kubectl/AWS，优化模式拒绝真实入口。默认 Go/Python 测试不访问集群。
固定模板不接受任意 kubectl 参数透传。旧 `capacity_report.resource_gate` 和 Docker profile 未改。

固定 Go1.27.1、GOENV/GOWORK/GOPROXY/GOSUMDB关闭：

| 检查 | 结果 |
| --- | --- |
| 默认 CGO0 `go test -count=1 -timeout=5m ./...` | PASS，62.381秒 |
| 默认 CGO1 `go test -race -count=1 -timeout=5m ./...` | PASS，65.604秒 |
| `go vet ./...` | PASS，0.334秒 |
| 既有容量 Python 回归 | 30项 PASS |
| 既有入口 Python 回归 | 27项 PASS |
| 新 EKS 普通/优化模式最终回归 | 各12项 PASS，1.719/1.416秒；优化模式只验证离线逻辑和真实入口拒绝 |

真实写入前11项负面测试已通过，但没有覆盖默认 metadata 的真实模板解析，因而未挡住
本次入口缺陷；第12项针对这个遗漏。测试覆盖 context/node确认、余量、碰撞、UID/image漂移、
Job/Pod注入、计数/桶/分位数/取消/采样缺失、timeout/取消/输出溢出、UID条件删除、
诊断失败继续回收和 foreign 对象拒删。合成资源/计时正例不等于原生成功。
开发期一次辅助模板检查遗漏显式GOROOT而报1.27.0/1.27.1不匹配，修正固定GOROOT后通过；
上述正式默认test/race/vet从开始就固定GOROOT，均一次通过。

受控根的 `command-NNNN.{json,out,err}` 逐次保存 argv、起止时间、exit 和有界输出；
`plan.*`、`owned.json`、节点请求快照、`cni-selected.txt`、原 `result.json`、
`cleanup-inventory.json`、`cleanup-recovery*.json`、UID DeleteOptions 和最终 manifest 均保留。
离线检查原文在 `.testdata/m25/validation-20260928/`。不提交环境绑定或原始在线报告。

实际命令范围：AWS仅 `eks describe-cluster` 的 ARN/name/version/status 投影；
kubectl只读为精确context/timeouts下的 can-i、节点字段、跨namespace资源字段、固定aws-node CNI字段、
本轮namespace/Quota检查、API discovery及本轮namespace metadata。
持久写操作仅3次create和3次UID条件DELETE；另有三次server dry-run create。
Job/Pod/logs/exec操作未发生，生产工作负载修改、CI/push/tag/release均未发生。

官方语义依据：[nodeName绕过scheduler](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)、
[NetworkPolicy需要实际插件执行](https://kubernetes.io/docs/concepts/services-networking/network-policies/)、
[EKS network-policy约束](https://docs.aws.amazon.com/eks/latest/userguide/cni-network-policy.html)。
节点绑定不替代余量检查；策略对象和namespace本身不替代可验证的运行隔离。
