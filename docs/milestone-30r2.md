# M30R2 — 节点预检通过，释放回执丢失后停止

2026-09-28。**本轮 NO-GO：无负载采样未完成。** 唯一原生 invocation 已通过固定节点资源预检、
Job/Pod server dry-run、实际创建和一次准确 helper 传入；释放 FIFO 的 exec 在回执前连接被对端重置。
不重放释放或可能已发生的空索引 PUT，不现场修改实现、不第二次 invocation/namespace。
Weir/ES observer 和 client snapshot 均未执行，`resource_evidence=partial/not-qualified`、
`timing=not-run`、`candidate=null`。旧 M30/M30R 失败与证据保持原样。

**清理仍有一个确认缺口：** 五个自有 namespaced 对象（含 Job/Pod）已按准确 UID 删除并读回不存在。
namespace DELETE exit 0、响应为 Terminating，但最后 GET 在原清理截止前停止，exit -15；
不能把空输出当不存在。待统筹收口的 namespace 为 `weir-qual-m30r2-20260928-222403`，
UID `6a72c1a1-68a9-4a15-920c-94f49d0a0557`。没有扩大 180 秒清理窗口或执行截止后的集群命令。

## 源码、输入与制品

- 干净 main baseline：`ec41435f0f6ee0e1989bb38a1aab7a44b000a09f`。
- 本轮实现、最终受测版本和实际 EKS 执行源码：`238fabdceb6a5288c801daad5701c5b6d0f06bfb`。
- 最终交付仅随后更新本文/readiness；完整 SHA 见证据根 `delivery.json` 及完成回调。
- 实际公开 image source：`4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。
- 74 个工具/配置输入冻结；204 个 Go/module、产品 70 项和 helper 81 项正式输入逐项与 image source 对比未变。
  未修改 Go/模块/正式镜像输入，未 build 新镜像、下载 registry、push、Actions 或发布。

仅复制旧 M30R 的五份明确公开文件至新 `artifact/`：index、manifest、config、完整应用层及 qualification。
重验 index→arm64 manifest→config/source/platform→layer 的准确 digest/长度、gzip diffID、
唯一 regular 0555 member，以及本地 binary 内容和 0555 权限。未读取或复制旧匿名客户端状态/秘密。
应用层 10,810,877 bytes；helper 21,046,963 bytes。

| 制品 | 固定身份 |
| --- | --- |
| Weir index | `sha256:2a3ca21b950f42449b01543e96655e0484e432f8ecbf29b46c49f7fd426c229a` |
| Weir arm64 manifest / binary | `100102a8c319a24f571f2aaa3041d6fafc79338840829e792f404e48d117dd5e` / `f2d107995762040c55b03cb37e94dd9e6beaa14a99d563b278b4bd9c2bd4db7e` |
| Qualification index | `sha256:fbef16495a37b89d44cfce51c38096e01e982ab875e3352b67c59480fdbe61a7` |
| Qualification arm64 manifest / config | `9eb2ff4243d7c021abe047da28867a9b3df65695660675ebcb761387b4f50351` / `063aa9659604ca3b7ab3814e31341727fe2ccce6f6758456d3f97c4b911209ee` |
| Qualification layer / diffID | `e7020f259dcaf846bb2772aca7dd4f2bc353ef7292b95549e4f541509eea03c7` / `6decd3193e4ce7cc311e886ca1f801031a16fced6b24da3cbcecc71fc853a86d` |
| Qualification binary | `d41f70ca4bbe129bff11b76f3d973cdb288d839c4df72a348cac04a233b76482` |
| ES 8.19.22 / JDK 27 | `docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237` |

## 共享查询改造及离线验收

`Run.nodes(selection)` 必须接收明确 name/UID；单个 node GET 先验证身份和状态，再以
`--all-namespaces --field-selector=spec.nodeName=<name> --chunk-size=0` 获取该节点完整最小资源投影。
准备、进入原生窗口以及 Job 派发前复核均使用同一冻结范围，不遍历候选节点。
M25、loopback 和 socket diagnostic 旧调用点同步要求明确节点，不会退回全量查询。

保留具名 spec/allocated/actuated 最大值、init/native-sidecar、Pod-level、overhead、resize、deleting 等原模型。
新增响应 envelope/列表形状、分页残留、字段完整性、每行 nodeName、非空唯一 UID 和输出边界检查。
CLI 非零退出在解析前拒绝，即使 stdout 为合法 `[]`。未知资源表示仍拒绝，不按 namespace/owner 过滤计账。
实际 argv、原始最小字段、解析账本均保留；不输出业务 env/完整 Pod 配置或 Secret。

写入前资源 GET 共享一个冻结的 120 秒截止，随 plan 传入执行阶段，不重置。
只有明确读取超时、尚无 mutation/owned 对象且未使用恢复时，才允许全阶段一次相同 argv 的补充 GET；
首次失败和原因保留。权限、身份、语义、容量、本地输出/截止失败不重试。
发生写入后，派发复核受原 native 900 秒截止约束，禁止这条恢复路径；不扩展原生窗口。
**本轮实际恢复次数为 0**，三次节点资源读取均首次成功。

证据入口改为受限 `.testdata/<合法单层run名>/native`，owner 必须匹配 `weir-qual-<run名>-…`；
拒绝路径穿越/符号链接，冻结绝对路径，prepare create-only、invocation 独占创建。
没有增加每轮目录枚举、controller、provider 或宽松准入比较器。

最终普通模式 **178/178 通过**（133.577 秒）；优化模式 **153 通过、25 项既有 skip**（127.483 秒）。
完整真实 M30 Job 响应、明确标注为离线构造的 Pod Running/Completed 状态，以及只读挂载、UID、资源、
上传/释放/取消/截止/清理反例继续保留。新增真实子进程 CLI 测试覆盖范围、空响应/超时、完整性、UID 漂移、
共同截止、输出上限、一次恢复及禁止重放。普通全套首轮 5 failure/19 error 来自未同步的旧 CLI 夹具/候选输入；
原失败日志保留，修正调用点后通过。未将合成旧负载测试当作本轮 EKS 工作量。

## 实际准备、派发及失败

固定 context `arn:aws:eks:us-west-1:956540890581:cluster/data-team`，region us-west-1，
cluster ACTIVE/Kubernetes 1.36。唯一节点
`ip-172-31-12-243.us-west-1.compute.internal`，UID `18f03c58-83c1-424e-be34-44ef88c07831`，
原生 linux/arm64；内核 `6.12.77-99.140.amzn2023.aarch64`，kubelet `v1.35.3-eks-bbe087e`。
当前 CNI `--enable-network-policy=false`，不把 namespace/default-deny 当已证隔离。

| 节点资源读取 | 命令 | Pod 响应 | 耗时 / exit |
| --- | --- | --- | --- |
| prepare | 4–5 | 6 Pods / 6019 bytes | Pod LIST 5.539 秒 / 0 |
| native 写入前 | 36–37 | 6 Pods / 6019 bytes | Pod LIST 3.078 秒 / 0 |
| Job 派发前 | 57–58 | 6 Pods / 6019 bytes | Pod LIST 7.589 秒 / 0 |

三次均计入 CPU 0.880、内存 780,140,544 bytes、6 Pods、ephemeral 0；余量为
**7.030 CPU / 31,202,312,192 bytes memory / 18,182,813,665 bytes ephemeral / 52 Pods**。
原 7CPU/5632MiB/5GiB/3Pod 门槛未变。这是非原子瞬时计账，不是预留。

prepare 112.339 秒，32 条 CLI；plan SHA256
`8966c13a5a4145fd53d6f9a0bf92c493bdc342c94f372046509e493786ae00fe`。
唯一 namespace `weir-qual-m30r2-20260928-222403`；原 900 秒整体、420 秒 artifact-ready、300 秒传输及 180 秒清理预算均未放大。
单 Pod 有效峰值仍 6CPU/4608MiB/2560MiB ephemeral；helper 64MiB emptyDir 在原预算内。
bootstrap 唯一 RW，Weir/ES helper 挂载只读；固定 nodeName、无 Service/host namespace/hostPath/特权 exec。

Job dry-run 55、Pod dry-run 56 均通过，实际 Job create 59 后取得唯一 Pod UID。
上传一次 stdin 21,046,963 bytes，写入耗时 34.660 秒，exec 全程 35.642 秒，exit 0，
有完整 size/hash 回执；0555 检查通过后才输出回执。上传前后 namespace/Job/Pod/bootstrap imageID/containerID 一致。

**命令 74** 执行 `helper-release.sh`，4.923 秒后 exit 1，stdout 为空：
`error reading from error stream … read: connection reset by peer`。
没有 `release.json`，未继续运行版本检查、main boundary、observer 或 snapshot。
原窗口内失败诊断保留：bootstrap 日志命令 76 本地 6 秒截止（exit -15、无正文），
ES 日志命令 78 同样触及本地截止（最终 exit 1，仅保留部分原始日志）；不能把这些日志当完整回执。

随后只读 Pod 状态显示 bootstrap 在 `2026-09-28T14:29:26Z` Completed/exit 0，
Weir 和 qualification 分别于 14:29:28Z、14:29:30Z Running，ES 也 Running，restartCount 均为 0。
bootstrap 结束时间与 release stream reset 同秒；是否为 init 容器结束导致 exec 连接关闭仍是待证解释。
这说明远端启动已推进，但不修复丢失回执；空索引 PUT 的管理结果仍 UNKNOWN，最多一次，不重放。

## 当前资源 raw 与操作计数

| 角色 | 本轮实际材料 | 尚未取得 |
| --- | --- | --- |
| Weir（2CPU/1GiB） | 准入资源/只读挂载、准确 index imageID、Running 状态 | 进程 UID/hash/FD/PID/cgroup/CPU/memory/swap/cpuset/io/TCP/metrics 原样样本 |
| ES（3CPU/3GiB，heap1GiB） | 准入资源、准确 imageID、Running、部分启动日志含 ES8.19.22/JDK27 | 正式 JVM 定位、同 UID observer、proc/cgroup 与 stats 样本 |
| client（1CPU/512MiB） | 准入资源、准确 index imageID、Running | snapshot 及实际进程/内核资源限额 |

样本数 **0/0/0**，不借旧 EKS 或 Docker FD/PID 限额填空。
`operation-audit.json` 保留全部命令 argv/end/exit/输出大小；118 条编号 CLI + 1 条上传 exec = **119 条**，
115 条 exit 0，4 条非零（74/76/78/118）。其中 kubectl 112、AWS 1、git 6。

- Kubernetes GET 61（含节点 GET 3、全部 namespace 的限定节点 Pod LIST 3）、auth 23、API discovery 3、logs 5；本地 current-context 1。
- server dry-run 6，持久 CLI create 5，Job controller 创建 Pod 1；准确 UID DELETE 6，全部 DELETE CLI exit 0。
- exec 2：上传一次成功、释放一次失败；Weir/ES observer 0，client snapshot 0。
- 空 records 索引 PUT 预留 1、尝试范围 0–1、完整回执完成数 unknown；文档 mutation、seed、setup、pace、trial、planned 全部 0，mutation replay 0。
- observer metrics/stats HTTP 0，client snapshot HTTP 0，main boundary HTTP 0。
  bootstrap guard 的两次 GET 未取得完整日志回执；kubelet startup/readiness 与 Weir 启动读取次数 unknown。
  kubectl 内部 discovery/多资源 GET 的 HTTP 总数也 unknown，不能以 CLI 条数替代 HTTP 请求数。

## 有界清理与待收口 UID

主流程含失败诊断 243.810 秒；清理 176.042 秒，在 180 秒中保留 Stop/Wait 时间后停止。
所有本地测试/CLI 已 Wait；上传 exec 有正常 completion，释放 exec 无正常回执。
Job DELETE 93 后读回不存在；Pod DELETE 96 后，**命令 98 exit 0 且 stdout 空**，确认准确 Pod 已消失。
这提供本轮远端 helper/容器结束证据，不以本地 kubectl 退出代替远端收口。

| 对象 | 准确 UID | 结果 |
| --- | --- | --- |
| Job loopback | `9fbdb062-78d7-42aa-9a25-2243a93e81b3` | 删除并确认不存在 |
| Pod loopback-gtfc9 | `bde37cbc-ed59-4b62-9041-f102f99b8a48` | 删除并确认不存在 |
| ConfigMap configuration | `6f1c1d68-f620-46a0-94df-ae8d18c78441` | 删除并确认不存在 |
| NetworkPolicy default-deny | `2ce6a260-ba74-4459-82bd-36359e296450` | 删除并确认不存在 |
| ResourceQuota budget | `63551bb9-4963-49d2-936d-0a090261c958` | 保留至最终盘点后删除、确认不存在 |
| Namespace weir-qual-m30r2-20260928-222403 | `6a72c1a1-68a9-4a15-920c-94f49d0a0557` | DELETE 117 成功/Terminating；GET 118 截止，消失未确认 |

最后盘点仍可见默认 ConfigMap `kube-root-ca.crt` UID `bd0f37a9-8a3e-431c-a76c-1f2cd4430a1b`、
ServiceAccount `default` UID `d0b591f9-7ae4-471a-8d02-9c174a347f98`，以及 22 个唯一自有事件 UID；
这些由 namespace 生命周期收口，未单独扩大删除范围。完整去重清单见 `closure-analysis.json`。
无 force、finalizer 操作、外来对象删除或清理截止后请求。
原 `native/result.json` 保留 `cleanup.confirmed=false` 及保守的 remote_helpers unknown 字段；
补充分析只依据原窗口内的 Pod 不存在证据说明进程收口，不改写原始结果或冒称 namespace 已消失。

## 证据与剩余门槛

证据根 `/Users/liran/Projects/liran/go/weir/.testdata/m30r2/`：source freeze、plan/invocation、制品链、
输入证明、普通/优化测试、全部 CLI/exec/UID/失败原文、query/operation audit、closure analysis。
`manifest.json` 逐文件 hash/size 封存，排除自身及避免自引用的 `delivery.json`；根内没有匿名登录状态或秘密。
旧 M30 342 文件和 M30R 60 文件逐项 hash/size 未变；各 manifest SHA256 保持
`8fcc58766c27be59c6e318ae24bb7d77bb2e49b21f61ceed52231f78380b48da`、
`266df5e8afa2f3d8890b45ca6d814e469d253c16dcdb9e22bf6d6cab233c951d`。

本轮没有取得可用于下一负载合同的三角色原始采样。将失败、当前状态和 namespace 确认缺口交统筹，随后停止；
不自开下一聊天/阶段/定时器。完整资源、计时、容量/过载/恢复/24h、CNI/跨节点、六原生平台及其他规格/后端安全
仍 required；Weir Auth 排除、ProgramTransform 首版延期保持。
