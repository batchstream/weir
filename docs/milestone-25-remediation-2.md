# M25R2 — Pod dry-run 成功，入口 metadata 校验阻塞，NO-GO

2026-09-28；执行聊天 `01a0e605-8658-7361-914c-83afcf4ebd20`。
**唯一冻结尝试没有创建实际 Job/Pod，没有 version、snapshot 或计时数据。**
Pod server dry-run 已通过 Priority admission，但返回额外 region/zone 标签，
被新入口对 metadata labels 的完整相等比较拒绝。三个自有对象已按 UID 清理，
namespace 消失已另行只读确认。本阶段不能宣称 EKS 发生器资格。

## 实现与冻结身份

- baseline：`c9f25c49682bb386506b8d191122217f4c7a784c`。
- 实现：`c6fc02495bf8942eea7d5512c90f30905d83facb`；仅修改
  `scripts/eks_pacing.py` 与离线回归。产品/helper Go、协议、module、镜像、CI 均未改。
  完成本地验证并提交、main 干净后才准备并冻结。
- 本轮证据：`.testdata/m25/r2-20260928-0330/`，0700、Git 忽略；
  `plan.json` 和 `plan.sha256` 为0400。冻结 hash：
  `ee8bb56ce3e9ee1ea15fa484a6374b46e49151892665406f3e23181f4cc29773`。
  包含授权 target、唯一 owner/namespace、node name/UID、七个工具输入 hash、
  七步 Job 模板、准确镜像及门槛。实际环境绑定只保存在受控证据。
- `invocation.json` 排他创建；冻结后没有改代码、plan、门槛或重新创建 namespace。
  本文和 readiness 为随后仅文档交付，不能冒充运行实现。

镜像 source 仍为 `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`，
准确身份沿用已获统筹有限独立验收的 [M24](milestone-24.md)：

| 镜像 | Index digest | arm64 manifest | arm64 binary SHA256 |
| --- | --- | --- | --- |
| `ghcr.io/batchstream/weir` | `sha256:cc6428d1ead507e531f95b8c45926f8bf31abf8ba9cb89cf6e8eca4a865b1f10` | `sha256:9dc3cb7fb9e69a332d10a1f49941bd80e9752778078335ac6dff7c22a6799856` | `e152805a350d5b2968df18f736e587b649f9ef2cf3cc6c650aa44bd37cbf3b41` |
| `ghcr.io/batchstream/weir-qualification` | `sha256:8a5c4dbca08daae24f62798c40f1607bbfaa5362b5b86db9a5651eb5475b3057` | `sha256:012713da8f0f0ccec1d24fe589c22b19789e177a1fc82702dd3701c190e55b05` | `fd5a5d4f1f8127a0ca30d3155d3f9170f6ab0bc70d82f9f28b01fb287e7e27fc` |

没有新拉取凭据、发布、push 或 CI。本轮没有实际容器 imageID/containerID/binary 输出，
上表是冻结的 M24 身份，不能当作本轮原生运行证据。

## 统筹约束修正与入口变化

原 M25 提示词由统筹强制的 `preemptionPolicy=Never` 与普通 Priority admission 不兼容。
本轮模板省略 preemptionPolicy、priority 与 priorityClassName；实际及 dry-run Pod 必须返回
`priority=0`、`PreemptLowerPriority`、无 PriorityClass。Job 模板允许字段未填，
不能用这种缺省豁免实际 Pod 的必需默认。未知 spec/容器字段继续拒绝。
这与 [v1.36 Priority admission](https://github.com/kubernetes/kubernetes/blob/v1.36.0/plugin/pkg/admission/priority/admission.go#L155-L175) 的计算与一致性检查相符。

普通策略仅在精确绑定已核验 nodeName/UID 时成立。创建入口先比较冻结 Job 模板，
空/变更 nodeName 在实际 create 前拒绝；每次创建前仍核对同一节点身份、Ready、pressure、
taints 和保守余量。根据 [scheduler 的已绑定 Pod 分支](https://github.com/kubernetes/kubernetes/blob/v1.36.0/pkg/scheduler/eventhandlers.go#L117-L129)
及 [assignedPod 判断](https://github.com/kubernetes/kubernetes/blob/v1.36.0/pkg/scheduler/eventhandlers.go#L414-L417)，
非空 nodeName 的 Pod 不进入待调度队列；这不证明共享节点没有资源竞争。

每次实际 Job create 前，先 Job dry-run，再从同一冻结模板派生 Pod 做 server dry-run，
保存请求及选择性非秘密响应。派生 Pod 无伪造 controller UID，不能持久创建或 fallback。
dry-run 调用者与未来 Job controller 身份不同，实际 Job/Pod 的 owner 链及资源检查仍保留。

运行观察增加精确 namespace/当前 Job UID 的 Event 查询，再核对关联 UID/name/kind；
首次观察到 FailedCreate 保存有界原因、次数和时间并退出。无 Event 仍由 Job deadline 兜底。
清理错误另存，不能掩盖原错误；不增加应用重试，也不保证观察前 controller 没有异步重试。
**本轮没有创建 Job，因此真实 FailedCreate 早停路径只具备离线回归证据。**

## 一次尝试与失败原因

重新读取授权 EKS ACTIVE/control plane 1.36，观察到34节点、其中33 Ready；
选定节点为 Ready Linux arm64、无 taint/pressure/调度禁用，并在版本步骤前再次核验。
内核 `6.12.77-99.140.amzn2023.aarch64`、kubelet `v1.35.3-eks-bbe087e`。
该节点 allocatable 为7.910 CPU、31,232,864 KiB、58 Pod slots；本轮保守扣除请求后
剩5.195 CPU、29,642,031,104 B、52 slots，满足原门槛。
regular/init/Pod-level/overhead/allocated/runtime 资源重叠明确重复扣除，观察不是原子快照或资源预留。

CNI `v1.21.1-eksbuild.7`、policy agent `v1.3.2-eksbuild.2` 仍为
`--enable-network-policy=false`。未改 CNI 或进行任何网络端点探测，network-isolation 仍 unqualified。

03:30:42.918 UTC 开始创建新 namespace；restricted 标签、default-deny Policy、Quota 成功。
Quota 为1 Pod/1 Job、1CPU/512MiB，Service/PVC/Secret计数0；Secret计数仅读取Quota。
非root65532、只读根、drop ALL、禁提权、seccomp、无host/token/volume/sidecar，
Job/Pod 120/100秒、工具GOMAXPROCS1均保留。**这些模板没有成为运行时资源证据。**

命令0045 Job server dry-run成功；0046在03:31:35.687–03:31:38.583 UTC完成真正 Pod server dry-run，exit0。
返回的 name、namespace、owner label、nodeName 正确，未注入 ownerReferences，普通优先级默认符合预期。
但 labels 新增 `topology.kubernetes.io/region` 与 `topology.kubernetes.io/zone`，
触发 `pod_dry_run` 的完整 metadata 比较，原错误 **`Pod dry-run metadata drift`** 保留。
补充只对已保存响应运行 spec 校验通过；没有绕过 metadata 校验去创建资源。

新增标签与 [v1.36 PodTopologyLabels 对 nodeName 非空 Pod 复制节点拓扑标签的实现](https://github.com/kubernetes/kubernetes/blob/v1.36.0/plugin/pkg/admission/podtopologylabels/admission.go#L141-L164)
一致。这是标准路径相符的来源判断，未读取线上 apiserver 配置来证明具体插件配置。
本轮入口未兼容该 metadata 默认；API 并未拒绝 Pod dry-run，不能将该失败归咎 Priority admission、
Weir 产品或 EKS 性能。离线 fake CLI 未模拟此标签注入，测试通过不代表完整实际准入兼容。
冻结后没有补丁或第二次尝试。

## 结果与清理

计划仍为 version、snapshot、50ops/s×20秒三轮、200及800各20秒，最多五探针、23,000 planned。
all/read/put dispatch p99≤5ms、arrival p95≤100ms/p99≤250ms、零drop/error/UNKNOWN、
完整采样、CPU区间<0.9且无新throttle均未改变。

| 项目 | 本轮结果 |
| --- | --- |
| 产品 version / 工具 snapshot | not-run / not-run |
| 50×20秒第1、2、3轮 | 全部not-run；每轮实际planned=0 |
| 200×20秒、800×20秒 | 全部not-run；每轮实际planned=0 |
| 实际 Job / Pod / timed probe / planned | 0 / 0 / 0 / 0 |
| timing_pass / generator-ready-for-next-investigation | false / false；入口NO-GO，无性能结论 |
| resource-evidence | not-run；CPU/memory/swap/PID/affinity均无实测 |
| namespace边界 / network-isolation | 自有namespace已回收 / unqualified |
| DB mutation / capacity candidate / full-calibration / 24h | 0 / null / not-run / not-run |

没有计数样本或直方图可重算，不能将缺失数据当零延迟/零CPU或合格。
M22R最低50档8.3ms>5ms的历史NO-GO不改写；Weir自身认证排除、ProgramTransform首版延期/UNSUPPORTED，
其他生产资格门槛保持不变。

清理清单只包含本轮自有 Policy/Quota、已固定 UID 的标准默认对象及 namespace 归属证据。
三次 DELETE 均核对 owner/create UID 并使用 UID precondition，未force、清finalizer或删除未知对象。
命令0061于03:32:30.055 UTC确认 namespace消失，随后独立精确只读查询再次为空。
实际 namespace create请求至确认消失107.137秒；首次清理读取至消失51.464秒，
分别低于15分钟和180秒。原 `cleanup.confirmed=true`、diagnostic_errors为空。
本轮61条入口命令均完成并Wait：5次server dry-run、3次持久create、3次精确UID DELETE，
没有实际Job/Pod create、logs、exec或按Job UID的Event查询（尚未创建Job）。
外层原生driver退出1、child已Wait；离线测试和所有本地子进程均已结束。

## 离线验证与证据索引

固定Go driver/compiler1.27.1，GOENV/GOWORK关闭、GOPROXY/GOSUMDB=off、独立空HOME：

| 检查 | 结果 / 命令耗时 |
| --- | --- |
| 默认CGO0非缓存 `go test -count=1 -timeout=5m ./...` | PASS / 62.118秒 |
| CGO1 `go test -race -count=1 -timeout=5m ./...` | PASS / 65.930秒 |
| `go vet ./...` | PASS / 0.280秒 |
| 全部脚本离线回归 | 75项PASS / 11.631秒 |
| 最终EKS普通 / 优化模式 | 各19项PASS / 5.637、5.206秒；优化模式实际入口仍拒绝 |

回归包含真实Go-template的无labels/null/空labels及Event投影，普通默认与未知字段拒绝，
外部CLI进程seam证明Pod dry-run失败后零实际Job/Pod create，首次FailedCreate只观察一次即停止，
foreign Event UID拒绝、无Event deadline兜底、UID清理及原错/清理错误分开。
没有新增生产函数变量替换、接口框架或平台编排层。

- 原生根保存 `command-0001`–`0061` 的argv、起止、exit、有界stdout/stderr，
  frozen plan、两种dry-run请求、Pod准入响应、owned/default UID、三个删除体及原result。
- `diagnostics/audit.json` 重核七个输入hash、计划/模板、全部命令、创建/删除UID、清理计时与失败差异；
  `diagnostics/independent-absence.json` 为最终独立不存在检查。
- 离线及外层driver证据为 `.testdata/m25/r2-validation-20260928-0326/`，
  包含validation、命令记录、前两轮namespace不存在检查和driver-stop；不读取其HOME。
- 原 M25、M25R 原生目录共565个已列文件（含各自manifest）按本轮开始前hash复核未变；
  原M25 cleanup=false与恢复清理=true、原M25R FailedCreate和cleanup=true均保留。

本地main提交、无push；所有自有资源与测试已停止。等待统筹独立验收，不自行开启下一阶段、namespace或定时器。
