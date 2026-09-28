# M25R — 一次 EKS 补证：Pod 准入阻塞，NO-GO

2026-09-28；执行聊天 `01a0e5ee-665f-7da3-97cc-214d8e795138`。
**唯一新 invocation 未运行任何 Pod 或计时探针，未取得 EKS 发生器资格。**
原 M25 无 labels 缺陷没有复现；本轮版本 Job 通过 server dry-run 并创建，
随后控制器创建 Pod 被 Priority admission 拒绝。全部自有资源已按 UID 清理。
这是入口模板与 Pod 准入规则不兼容，不是 Weir 功能或 EKS 性能测量失败。

## 冻结身份与范围

- baseline 与实际执行实现均为 `b2b32d6f4d349c3ba148521244de7f7869a7906b`，冻结时 main 干净。
  本轮不改入口、校验器、产品/helper、协议、module、镜像或 CI；交付仅本报告和 readiness。
- 镜像 source 固定为 `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`，准确身份沿用
  已获统筹有限交付验收的 [M24](milestone-24.md)：

```text
ghcr.io/batchstream/weir@sha256:cc6428d1ead507e531f95b8c45926f8bf31abf8ba9cb89cf6e8eca4a865b1f10
ghcr.io/batchstream/weir-qualification@sha256:8a5c4dbca08daae24f62798c40f1607bbfaa5362b5b86db9a5651eb5475b3057
```

新受控证据 `.testdata/m25/r1-20260928-0258/`，根权限 0700、Git 忽略；
`plan.json` / `plan.sha256` 权限 0400，hash：
`95acb31f455365b66128f7947f348bec3f7f9e565dfa13cffe6fdbafb2260629`。
冻结内容含授权 target、独占 namespace owner、node name/UID、源码及全部工具输入 hash、
两个镜像的 index/arm64 manifest/binary、七个 Job 模板和完整门槛。实际环境绑定仅在受控证据。
`invocation.json` 排他创建；没有第二次尝试、换节点或修改冻结输入。

计划仍为 version、snapshot、50 ops/s×20秒三轮、200与800各20秒；最多五探针、
23,000 planned、0 DB mutation。all/read/put dispatch lag p99≤5ms，arrival
p95≤100ms/p99≤250ms、零 drop/error/UNKNOWN、采样完整、CPU区间<0.9且无新 throttle，均不变。
原 [M25](milestone-25.md) 的 plan/invocation/result 未改，原 cleanup=false 与恢复清理=true 分别保留。

## 实时预检与模板

02:56–02:59 UTC 重新读取授权 EKS ACTIVE / control plane 1.36；31/31节点 Ready。
按名称选择第一台符合原准入余量的既有 Linux arm64 节点，无 taint、pressure 或调度禁用；
冻结后及 Job 创建前再次核验同一 UID。所有观察有各自起止时间，不称原子快照。

| 选择性观察 | 本轮实际值 |
| --- | --- |
| 内核 / kubelet | `6.12.77-99.140.amzn2023.aarch64` / `v1.35.3-eks-bbe087e` |
| Allocatable | 7.910 CPU、31,232,864 KiB、58 Pod slots |
| 保守剩余 requests 余量 | 5.195 CPU、29,642,031,104 B、52 slots；来自本轮读取，非复用旧快照 |
| 扣除方式 | regular、全部 init、Pod-level、overhead、status allocated/runtime 求和，明确重复计入重叠 |
| CNI | `v1.21.1-eksbuild.7`；policy agent `v1.3.2-eksbuild.2`，`--enable-network-policy=false` |

新 namespace 的 restricted 标签、default-deny Policy、Quota 创建及归属校验通过。
Quota 上限1 Pod/1 Job、1 CPU/512 MiB，Service/PVC/Secret为0；Secret计数仅来自Quota，未读Secret。
模板固定 nodeName、preemptionPolicy Never、无自定义 toleration、requests=limits、
工具 GOMAXPROCS1、非root65532、只读根、drop ALL、seccomp RuntimeDefault、禁提权、
禁自动token与ServiceLinks，无volume/sidecar/凭据/主机访问。
Job/Pod deadline为120/100秒，backoffLimit0、restartPolicy Never。
**模板不是实测容器资源；本轮 CPU/memory/swap/PID/affinity 全部没有运行时证据。**
network-isolation仍为unqualified，未改CNI或探测既有端点。

## 本次失败与清理

02:58:26 UTC 发起 namespace 创建；命令0040成功投影默认对象的 metadata。
命令0043版本 Job server dry-run成功，0046实际Job create成功。
然而 Job dry-run 不执行其未来 Pod 的 Priority admission。
按该 Job UID 精确读取的 Event 保留 `FailedCreate`：Pod 显式 `Never` 与 admission
从默认 PriorityClass 规则计算的 `PreemptLowerPriority` 不一致，拒绝创建。
这与 [Kubernetes v1.36 Priority admission 源码](https://github.com/kubernetes/kubernetes/blob/v1.36.0/plugin/pkg/admission/priority/admission.go#L91-L111)
的资源范围及 [Pod policy 一致性检查](https://github.com/kubernetes/kubernetes/blob/v1.36.0/plugin/pkg/admission/priority/admission.go#L155-L175) 一致。

Event记录02:59:32–03:00:35共7次控制器 FailedCreate，均没有接受创建的Pod；
这不是7次发生器运行。入口仅观察Job终态，未在首次FailedCreate Event时立即退出；
03:01:32服务端deadline触发 `FailureTarget/Failed: DeadlineExceeded`，命令0092保留完整状态。
原入口结果 `error="Job controller failure"`、exit1保留；细因另存
`diagnostics/version-events.json`，没有用诊断替换原错。后续六Job全部未创建。
Job dry-run不能覆盖Pod admission、FailedCreate未即时停止，是此次暴露的入口缺口；
冻结后不修改实现、不撤掉Never约束、不创建/修改全局PriorityClass，也不重新执行。

Job、Policy、Quota、namespace均核验name/owner/createUID并以DeleteOptions UID precondition删除。
清理前完整非秘密资源metadata清单仅包含确证自有资源、固定UID默认对象及本Job关联Event；
没有foreign对象、force/grace0、finalizer操作或未知对象删除。
命令0111于03:02:31前确认namespace消失；随后独立精确只读检查再次为空。
namespace create请求至确认消失244.218秒；首次Job清理至消失58.677秒，
分别低于15分钟和180秒。`cleanup.confirmed=true`，无诊断错误，所有本地子进程已Wait。

## 结果分列

| 项目 | 本轮结果 |
| --- | --- |
| 产品 version / 实际 imageID、containerID、binary输出 | not-run；仅Job创建，未产生Pod |
| snapshot / resource-evidence | not-run；实际1CPU/512MiB/swap0与PID/共享affinity均未知 |
| 50×20秒第1/2/3轮 | 全部not-run；各轮实际planned=0，latency/CPU无数据 |
| 200×20秒、800×20秒 | 全部not-run；各轮实际planned=0，latency/CPU无数据 |
| timing_pass / generator-ready-for-next-investigation | false / false；准入阻塞的NO-GO，非计时门槛实测失败 |
| namespace边界 / network-isolation | 新独占namespace已清理 / unqualified，agent disabled |
| 实际计时planned / DB mutation | 0 / 0 |
| DB capacity candidate / full-calibration / 24h | null / not-run / not-run |

无数据可用于重算计数、桶、分位数或CPU区间；不以合成回归、模板或脚本退出状态代替原生证据。
M22R的最低50档8.3ms>5ms NO-GO仍保留，本轮没有证据判断EKS runner更好或更差。
Weir认证排除、ProgramTransform首版延期/UNSUPPORTED，以及其余生产门槛不变。

## 离线验证与证据索引

写入前固定Go1.27.1，GOENV/GOWORK关闭、GOPROXY/GOSUMDB=off、独立空HOME：

| 验证 | 结果与命令总耗时 |
| --- | --- |
| EKS普通 / 优化模式 | 各12项PASS，4.484 / 1.580秒；优化模式真实入口仍拒绝 |
| 全部脚本离线回归 | 68项PASS，5.221秒；含归属、失败清理、超时/取消/输出界限 |
| 默认CGO0 `go test -count=1 -timeout=5m ./...` | PASS，61.981秒 |
| CGO1 `go test -race -count=1 -timeout=5m ./...` | PASS，65.179秒 |
| `go vet ./...` | PASS，0.268秒 |

补充真实Go模板验证缺labels/null/空labels/有owner的标准ConfigMap与ServiceAccount；
既有Pod投影脱敏回归和精确合法默认/异常默认拒绝检查通过。这些离线检查未覆盖真实Priority admission。
补充旧namespace检查首次误继承离线空HOME，本地context查找失败；错误保留，恢复已授权CLI配置
后的只读检查确认旧namespace不存在，未读取raw kubeconfig或秘密文件，也未发生远端写入重试。

原生根保留 `command-0001`–`0111` 的argv/起止/exit/有界stdout/stderr、冻结plan、
node-check、namespace默认对象UID、`owned.json`、4份UID删除体、清理清单及原 `result.json`。
`diagnostics/` 保存按Job UID筛选的Event、最终namespace不存在的独立读取及 `audit.json`。
离线证据为 `.testdata/m25/r1-validation-20260928/`；只索引日志/输入，不读取证据HOME。
本轮CLI持久写入为4次create、4次UID DELETE；4次server dry-run；无Pod create/logs/exec。
只读范围为授权AWS集群投影、权限、节点/资源字段、固定CNI字段和本namespace对象/关联Event；
无既有业务env/spec/日志、Secret、服务/数据库、扩容、AWS修改、push/CI/发布。

交付等待统筹独立验收；执行聊天停止，不创建新namespace、阶段或定时任务。
