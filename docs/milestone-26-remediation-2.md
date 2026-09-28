# M26R2 — 精确准入语义与一次原生回环尝试

2026-09-28；执行聊天 `01a0e687-53f5-7c41-b9bf-534b526f34b8`。
基线 `31bed877888492c3e4361bf8081abcaa9c2a17f9`；实现
`b13cc34c99fc460a279e0429324a7ca194dab6f5`。最终交付 SHA 见受控 `closeout.json`。
正式任务 SHA256：`0f8fcc9d4bef45ad6a55ce7b08418904e4c37ac2aee48145bd7c7aa0e3544b3d`。

**NO-GO：准入语义修复已通过真实 Job/Pod dry-run 及实际创建响应；唯一新尝试随后在
bootstrap 的 TCP local-address 检查 exit23 停止。ES 已启动，Weir/client 未启动，
两trial、空索引PUT与document mutation均0。六个自有对象按UID回收，namespace独立确认不存在。**
没有冻结后修补、重跑或新增namespace；真实短闭环资格仍未通过。

## 实现与离线证据

共享准入比较只在明确位置处理等价语义：普通/init 容器的 requests 与 limits 分别核对
资源键和值，Quota hard 与 emptyDir.sizeLimit 复用精确 quantity 解析；不复用向上取整的
资源计账结果。字符串最多64字符、指数有界，局部 Decimal 精度96保留全部有效数字，
不用 float、误差容忍、减少资源或逐值白名单。`False`、`0`、`0.0`、null 与缺字段保持区别。
依据固定 [Kubernetes Quantity v0.36.0](https://raw.githubusercontent.com/kubernetes/apimachinery/v0.36.0/pkg/api/resource/quantity.go)
的 canonical 表示；容器顺序、未知字段、资源键、镜像/命令/env、node/priority、安全与挂载继续严格。

探针仅补 [core/v1 Probe](https://raw.githubusercontent.com/kubernetes/api/v0.36.0/core/v1/types.go)
与 [v1.36 默认处理](https://raw.githubusercontent.com/kubernetes/kubernetes/v1.36.0/pkg/apis/core/v1/defaults.go)
明确的 scalar 默认值；initialDelaySeconds 缺失等于 int 0，其他显式时间/阈值和 exec 命令仍相同。
指针字段不默认化，未知0/false/null不忽略；额外合法 labels 仍允许且不授予归属。
Job dry-run 补核 apiVersion/kind/name/namespace/请求labels/无额外owner，保留实际 UID/controller 检查。

初始 resource_evidence 改为 not-run；保存了实际容器 runtime status 后才标 partial。
bootstrap 管理操作仍在 Job create 前保守预留1；未尝试持久 Job create 时，result 将实际上界
单列为0，不抹去 reservation 文件，也不改写原 M26R generic partial 历史。

`scripts/fixtures/eks-loopback-admitted-job.json` 保留真实 M26R 0050 响应结构，只替换
namespace/owner、JobUID、nodeName。离线 Pod 用该 spec 加已记录的 M25 Pod 默认行为构造，
**不是本轮真实 Pod 证据**。固定伪 CLI 复用既有 Pod/资源/trial 样本，通过真实共享 create 路径，
依次返回 Job dry-run、Pod dry-run、实际 create 和 current Pod；没有 mock 掉准入比较。

66种 spec 负例分别置于 Job 与 Pod dry-run（132组、198条CLI dry-run），全部零持久 Job：
资源±1byte/±1m及更细差异、limits缺失、未知resource/probe、delay/period/timeout/threshold、
命令/镜像/env、安全/volume、额外或乱序init/Always、node/priority及合法额外label共存恶意spec。
另测 UID/owner漂移、Quota、精确Decimal低位、pointer/null/bool类型、替换UID拒绝删除。
完整合成两trial lifecycle经计数/审计/预算/资源检查及六个UID清理；创建前失败保持not-run，
创建后失败停止trial并清理。完整伪CLI证据在 `.testdata/m26r2/offline/`，全都标记 synthetic。

| 验证 | 结果 / 墙钟秒 |
| --- | --- |
| 全Python普通 | 106通过，0 skip / 43.726 |
| 全Python `-O` | 106发现，81通过，25既有skip / 41.332 |
| Go1.27.1 CGO0 default非缓存 | PASS / 61.775 |
| Go1.27.1 CGO1 race非缓存 | PASS / 65.283 |
| Go1.27.1 vet | PASS / 0.266 |

Go显式GOROOT、GOENV/GOWORK关闭、GOTOOLCHAINlocal、GOPROXY/GOSUMDBoff，test均-count=1。
Go产品/helper/protocol/module/镜像/CI均未改，无push；启动模板与有序脚本也未改。
Weir直接exec、ES sidecar→单次空索引init→Weir/client、非root/原挂载/全127.0.0.1保持。

## 原生冻结与准确制品

证据 `.testdata/m26r/native-r2-20260928-0554/`；父目录及本轮证据目录0700、plan0400。
本地实现提交且main干净后冻结，唯一新owner/namespace为 `weir-qual-m26r-20260928-0554-r2`。
plan保存19项工具/测试/fixture输入、199项产品输入、三镜像、命令、模板、资源与全部门槛。

`.testdata/m26r2/registry/` 使用新空Docker/registry配置与清空继承环境匿名复核两GHCR
index→arm64 manifest→config/source→提取binary。两binary分别25,661,537/21,031,382字节，
SHA256仍为 `e152805a350d5b2968df18f736e587b649f9ef2cf3cc6c650aa44bd37cbf3b41`、
`fd5a5d4f1f8127a0ca30d3155d3f9170f6ab0bc70d82f9f28b01fb287e7e27fc`。
ES manifest/config/entrypoint和固定Kubernetes语义源码也重新下载核验；未读个人凭据或Secret。

准确source仍为 `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`；三个固定reference：

- Weir：`ghcr.io/batchstream/weir@sha256:cc6428d1ead507e531f95b8c45926f8bf31abf8ba9cb89cf6e8eca4a865b1f10`
- helper：`ghcr.io/batchstream/weir-qualification@sha256:8a5c4dbca08daae24f62798c40f1607bbfaa5362b5b86db9a5651eb5475b3057`
- ES8.19.22：`docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`

arm64 manifest分别为 `9dc3cb7fb9e69a332d10a1f49941bd80e9752778078335ac6dff7c22a6799856`、
`012713da8f0f0ccec1d24fe589c22b19789e177a1fc82702dd3701c190e55b05`；ES为直接manifest。
ES config仍为 `a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160`，linux/arm64、1000:0。

原资源和预算不变：峰值6CPU/4608MiB/2560MiB、node余量7CPU/5632MiB/3slots/5GiB、
startup150s、remote900s+cleanup180s、单命令合计8MiB、全部证据128MiB。
两trial through-Weir→direct-ES均rate50、warm20+measure20、1KiB/corpus1000、90Read/10Put、
64worker/4conn、deadline1s/expiry20ms/catchup8；每trial预留1200文档，总2400/含seedplanned6000。
派发p99≤5ms、arrival p95≤100ms/p99≤250ms、零drop/error/UNKNOWN/restart全部保留。


## 实际结果与清理

plan SHA256：`a8e5a0c32b91f5f45e98652e369b8043ad541520a9b03101fc8f71ea2fd53f6f`。
唯一context：`arn:aws:eks:us-west-1:956540890581:cluster/data-team`。
nodeName `ip-172-31-12-243.us-west-1.compute.internal`、
nodeUID `18f03c58-83c1-424e-be34-44ef88c07831`；22节点初次快照与创建前复核均满足
原余量门槛：7.030CPU、31,202,312,192B memory、52 slots、18,182,813,665B ephemeral。
这些是非原子请求快照，不是资源预留或利用率证明。

0050/0051为真实Job/Pod server dry-run，均exit0且语义校验通过；0054持久Job create成功。
Pod `loopback-jldtv` / `cfde5620-b2c1-4f48-8685-e27c6e3fafdc`，PodIP `172.31.0.147`。
ES与bootstrap实际imageID都等于固定ES manifest；containerID分别为：

- ES：`containerd://c1b7ccf6b10c8997eec6dba77414b9ae3314ff3e8535125f74734697bc2e7fb3`
- bootstrap：`containerd://027a1bf9d5ed4714eaa57a374deebb4672fcf06a68c1c3e20972c82e79f0d262`

`final-bootstrap.log`确认bash/curl/nc/timeout可用，ES HTTP返回8.19.22，所需settings通过，
记录了127.0.0.1映射的9200/9300监听。05:57:24Z bootstrap exit23，位置是
`eks_loopback_check.sh` 的 TCP local-address guard，早于UDP检查、自己PodIP负向探测及管理PUT。
**被拒绝的行在guard之后才会打印，故原证据没有具体触发行；无法确定地址、socket状态或来源，
不能将该失败直接归因为外部连接，也不能宣称监听/负向边界已通过。** 冻结后未补日志重试。

ES于05:57:35Z exit143，日志记录stopping/stopped/closed；两个init重启计数0，未见OOMKilled。
入口首次报错为 `container terminated/OOM: elasticsearch`，保留原样；它是通用终止错误文本，
不代表已确认OOM。Weir与qualification均停在PodInitializing、无实际imageID/containerID。
完整runtime-identity、产品-version、effective-config、Weir readiness、全部负向边界、RPC/DB
审计、客户端样本、Weirmetrics/Guard/ESstats与两trial均not-run；已有ES/init状态及日志仅为partial。

bootstrap reservation仍保留reserved1/实际上界1/完成未知的原记录。单独收口核对其exit23发生在
管理reserved/started标记和唯一PUT之前，且无任何exec/trial，故**实际管理写0、文档写0**；
没有重放或释放reservation。document budget为planned0/reserved0，两个trial均未开始。

| 自有对象 | UID | 条件DELETE命令 |
| --- | --- | --- |
| Job | `45c8ccff-abe1-47bd-8b4a-98077b551a74` | 0086 |
| Pod | `cfde5620-b2c1-4f48-8685-e27c6e3fafdc` | 0089 |
| Quota | `ad2565e3-6390-49a9-bf90-950e95cc0a7f` | 0092 |
| Policy | `bb670b52-3aee-4600-9aaa-926fea456cc2` | 0095 |
| ConfigMap | `0fd278bb-1033-42ae-a515-d8ad589c23ed` | 0098 |
| Namespace | `60f2aedb-2416-44f1-b963-8cbffcff14e2` | 0101 |

全部104条prepare/run命令都有end/exit，6次dry-run、5次持久create、1个controller Pod、6次UID删除。
仅0078/0080读取未启动Weir/client日志exit1，原stderr和diagnostic error文件保留；清理confirmed=true、
diagnostic_errors=[]。namespace创建请求至消失213.328秒，清理73.464秒；入口remote_elapsed含
清理216.186秒。0104与另存 `closeout/command-0001` 都返回空，独立确认namespace消失。
没有force/finalizer、陌生资源删除、业务/节点/CNI/全局修改、扩容或收费资源。

`.testdata/m26r2/audit.json` 逐项核对plan、全部工具/产品输入与实现Git对象、命令结束、精确UID删除
和历史hash；`manifest.json`保存本轮原生及本地证据hash/长度，总计约56MB，小于128MiB。
所有native、验证、匿名registry和审计子进程已Wait；本地main最终提交干净，无push。

## 资格边界与历史

candidate=null；容量、跨节点、过载、恢复、24h均not-run。CNI agent仍disabled，网络隔离unqualified。
同Pod短闭环不代表独占CPU、独立主机对比或吞吐容量；没有完整Weir/ES进程连续样本时资源仍partial。
原M26的94项与M26R的315项证据逐项hash/长度未变；原监听后signal注册窗口与exit-15
失败仍未解决，没有混改固定产品或宣称优雅关闭通过。Weir认证继续排除，通用ProgramTransform
V1延期，其他生产门槛不减。完成后只回调统筹一次并停止，不新开阶段或定时器。
