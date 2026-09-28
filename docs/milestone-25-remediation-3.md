# M25R3 — 元数据边界修复与一次 EKS 短时计时通过

2026-09-28；执行聊天 `01a0e61f-61d6-7fe0-ab48-6f33664f0f27`。
**唯一新冻结尝试完成 version、snapshot 和全部五轮计时，23,000 次全部完成，
dispatch p99 门槛通过；全部自有资源已回收。** 这仅是该 EKS profile 的短时发生器证据，
不是数据库容量、网络隔离、24h 或整体生产资格，仍待统筹独立验收。

## 修复、回归与冻结

- baseline：`cd9cef81db95b4a1ea3501512426b074e4e0d104`。
- 实现：`24976ddadd498cf39e86eed59010312d0bdcafc1`，仅两个 EKS Python 文件。
  `pod_dry_run` 精确比较 apiVersion/kind/name/namespace 与请求标签的每个键值，拒绝
  ownerReferences 注入；接受服务端追加标签，不用额外标签授予归属，不建立标签白名单。
  实际 Pod UID/Job controller 链、nodeName/实时 nodeUID、priority0/普通策略/无 class、
  镜像、资源、安全和未知 spec 拒绝，以及 FailedCreate 早停、期限、取消、输出界限、UID 清理均未改变。
- 从 M25R2 命令0046实际响应保留完整结构，替换 namespace/owner/UID/node/拓扑值，
  再加入一个无关标签，存入离线 CLI fixture。先在旧实现复现 metadata drift，再修复。
  正向路径经过实际子进程 CLI seam 进入模拟 Job create、Pod 检查和 UID 清理；
  owner 缺失/覆盖、name/namespace/kind 漂移、伪造 ownerReferences，以及附加标签与
  sidecar/host/volume/envFrom/镜像/资源/security/priority/nodeName/未知字段共存仍在 create 前拒绝。
  实际 Pod 的额外标签不放宽 UID/controller/owner 检查，外来 Pod 不登记、不删除。
  原 nil metadata、精确 Event UID 早停、deadline、失败清理回归继续通过；这些是离线证据。
- 本地验证、实现提交且 main 干净后冻结；证据 `.testdata/m25/r3-20260928-0355/` 为0700、Git忽略。
  `plan.json`/`plan.sha256` 为0400，hash：
  `99b8d8057386b0065c4f54376555913811ef611ef99646ab0b97d1e001724ed8`。
  七个工具输入、七步模板、节点 UID、授权目标、镜像、安全、资源和门槛均冻结。
  invocation 排他创建，仅执行一次；冻结后未改实现、plan 或门槛，随后仅提交结果文档。

## 制品与运行边界

镜像 source 固定为 `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`，沿用 [M24](milestone-24.md) 制品：

| 镜像 | Index digest | arm64 manifest | 冻结 binary SHA256 |
| --- | --- | --- | --- |
| `ghcr.io/batchstream/weir` | `sha256:cc6428d1ead507e531f95b8c45926f8bf31abf8ba9cb89cf6e8eca4a865b1f10` | `sha256:9dc3cb7fb9e69a332d10a1f49941bd80e9752778078335ac6dff7c22a6799856` | `e152805a350d5b2968df18f736e587b649f9ef2cf3cc6c650aa44bd37cbf3b41` |
| `ghcr.io/batchstream/weir-qualification` | `sha256:8a5c4dbca08daae24f62798c40f1607bbfaa5362b5b86db9a5651eb5475b3057` | `sha256:012713da8f0f0ccec1d24fe589c22b19789e177a1fc82702dd3701c190e55b05` | `fd5a5d4f1f8127a0ca30d3155d3f9170f6ab0bc70d82f9f28b01fb287e7e27fc` |

实际七个 Pod 的 imageID 均为对应 index reference，containerID 均已保存。
产品 `-version` 精确核验上述 source、go1.27.1、linux/arm64、clean-commit、dirty=false；
五次 pace 输出的实际 executable SHA256 均等于工具 binary hash。产品 binary hash 来自固定 M24
制品映射，本轮没有额外读取运行中产品二进制。无新镜像、凭据、push、CI 或发布。

重新读取 EKS ACTIVE/control plane1.36，30/30节点 Ready；选定既有 Linux arm64 节点
内核 `6.12.77-99.140.amzn2023.aarch64`、kubelet `v1.35.3-eks-bbe087e`，无 pressure/taint/调度禁用。
本次保守剩余 requests 余量为5.195 CPU、29,642,031,104 B、52 slots，满足原门槛；
每步重查同一 nodeUID，不是沿用旧快照，也不是原子读取或资源预留。环境标识仅保存在受控证据。

唯一新 namespace 保留 restricted 标签、default-deny、1活动Job/Pod、1CPU/512MiB，
Service/PVC/Secret计数0；Secret计数仅来自Quota。每个实际Job前均有真正Job和Pod server dry-run。
Pod响应的额外region/zone标签正常通过，实际Pod仍核对UID/controller链和严格spec。
普通priority0/PreemptLowerPriority/无class与精确nodeName、非root65532、readOnly、dropALL、
禁提权、seccomp、无host/token/volume/sidecar、GOMAXPROCS1、Job/Pod120/100秒均保持。
未部署服务/DB，未改现有workload、节点、CNI、PriorityClass或任何全局资源，未扩容/购买。

## 五轮实际结果

每轮20秒、12个完整资源样本；下表 dispatch 顺序为 all/read/put，arrival为三类中的最大值。
原all/read/put dispatch p99≤5ms、arrival p95≤100ms/p99≤250ms、CPU区间<0.9等门槛不变。

| 轮次 / ops/s | planned=started=completed=success | dispatch p99 ms | arrival p95/p99 ms | 最大采样区间 CPU 占比 |
| --- | ---: | --- | --- | ---: |
| 1 / 50 | 1,000 | 1.1 / 1.1 / 4.6 | 4.2 / 4.6 | 0.190387 |
| 2 / 50 | 1,000 | 1.1 / 1.1 / 4.7 | 4.0 / 4.7 | 0.192631 |
| 3 / 50 | 1,000 | 1.1 / 1.1 / 4.5 | 4.2 / 4.5 | 0.190652 |
| 4 / 200 | 4,000 | 1.1 / 1.1 / 1.1 | 1.1 / 1.1 | 0.203746 |
| 5 / 800 | 16,000 | 1.1 / 1.1 / 1.1 | 1.1 / 1.1 | 0.199002 |

合计23,000；各轮drop/error/UNKNOWN/late/cancelled_future/worker_expired均0。
已从原始JSON重算计数、桶、分位数、read+put和十秒窗口合计、测量/采样覆盖，全部符合固定规则。
snapshot加五轮共61个样本，实际quota=1、memory.max=536,870,912 B、swap.max=0，
GOMAXPROCS1、nonroot/security、无TCP连接/监听的自观测检查通过，OOM/restart及新增throttle均0。
采样RSS峰值17,453,056 B、memory.current峰值6,975,488 B、FD7、goroutines68、pids.current7；
这是采样峰值，不是连续硬峰值。

| 资格项 | 结果及限制 |
| --- | --- |
| timing_pass / 各速率 | true；三次50和一次200、800全部通过 |
| resource-evidence | 原结果为 `visible-boundaries-pass`；可见pids.max=37,697，affinity=0–7、exclusive_cpu=false。仅可见cgroup边界证据；共享CPU、完整宿主/祖先边界和持续负载资格仍partial，不能称独占核心 |
| network-isolation | unqualified；CNI1.21.1/agent1.3.2仍disabled。Policy对象及无网络代码路径不能证明实际网络隔离；未探测业务、公网或metadata端点 |
| generator-ready-for-next-investigation | true，仅允许统筹评估下一步；不授权自行启动DB/服务或容量测试 |
| DB mutation / capacity candidate / full-calibration / 24h | 0 / null / not-run / not-run |

原M25、M25R、M25R2三次失败证据保留；M22R最低50档8.3ms>5ms的历史NO-GO不改写。
本轮不能解释Weir吞吐上限。Weir认证继续排除，通用ProgramTransform首版延期/UNSUPPORTED，其余生产门槛不变。

## 清理与验证索引

256条入口命令均exit0且子进程Wait；17次server dry-run、10次持久create（3基础对象+7Job），
7个Pod均由Job controller创建，17次精确UID precondition DELETE覆盖全部自有对象。
未持久创建裸Pod，未force/清finalizer/删除外来对象。Job/Pod逐步回收后才进入下一步。
namespace创建请求至确认消失592.154秒，最终清理83.518秒，低于15分钟/180秒硬界；
`cleanup.confirmed=true`、diagnostic_errors为空。另行固定context/精确name只读查询再次确认不存在。

固定Go1.27.1、GOPROXY/GOSUMDB=off、GOENV/GOWORK关闭，Go验证使用独立空HOME：

| 验证 | 结果 / 秒 |
| --- | --- |
| CGO0非缓存 `go test -count=1 -timeout=5m ./...` | PASS / 62.631 |
| CGO1 `go test -race -count=1 -timeout=5m ./...` | PASS / 66.024 |
| `go vet ./...` | PASS / 0.270 |
| 最终EKS普通 / 优化模式 | 各23项PASS / 10.700、10.181 |
| 最终全脚本离线回归 | 79项PASS / 13.242 |

原生根保存命令0001–0256、plan/hash/invocation/result、所有dry-run请求/响应、Job/Pod UID、
containerID/imageID、原始version/snapshot/pace与完整样本、清理清单及删除体。
`diagnostics/audit.json` 重核七个冻结输入、全部命令/UID/资源/计时；
`diagnostics/independent-absence.json` 保存另行不存在检查。
`.testdata/m25/r3-validation-20260928-0348/` 保存红灯回归、去环境绑定核对、全部离线验证、
旧namespace不存在证明、外层driver Wait记录和证据manifest；原三轮776个已列证据文件hash/长度均未变。

本地main提交、无push；全部自有资源、测试和子进程已停止。等待统筹独立验收，不开启下一阶段、namespace或定时器。
