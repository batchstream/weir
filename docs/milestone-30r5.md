# M30R5 — 本地流取证与 observer 生命周期修复

2026-09-29（Asia/Shanghai）。**仅本地机制通过；M30R4 原生采样仍不合格，真实 reset 根因未知。**
本阶段没有访问 EKS（包括 GET）、创建集群对象、负载、push、Actions、registry 或镜像操作。
没有产品、Go/module、正式 helper 或清理器改动。Auth 排除、ProgramTransform V1 延期及其余资格门槛保持。

基线 `31c73b6afb759b937dc334841a0eadde5a0fc691`，唯一 checkout 写入者，本地 main 提交。
实现 SHA：`9bbb2b3f0aee0f11f5b2460636e38e249cdd1db0`；文档交付 SHA 及最终 manifest SHA 见 `.testdata/m30r5/delivery.json`。

## 已证明的事实与因果边界

原 `.testdata/m30r4/native/` 不变：Weir 279,120 bytes，五完整样本、末行截断；ES 88,355 bytes，五完整样本；
两流均缺第六完整样本及正常 observer_end，exec exit1/reset，client snapshot 未运行。
Weir 首/第五样本 `15:43:06.960560Z` / `15:43:14.960776Z`；
ES 首/第五样本 `15:43:13.684520Z` / `15:43:21.685025Z`，ES reset 日志约 `15:43:23.923Z`。
每样本约 27–56ms，不能以五条完整记录替代六条/正常结束合同。

旧 execute 先启动 Weir，再同步 monitor，启动 ES 后才统一 poll。真实命令109–111（Namespace/Job/Pod）
占 **8.571337 秒**，这段代码不会排空已启动 Weir 的 stdout/stderr。
ES 失败后，命令112–119先做诊断 GET/logs，约 **44.140 秒**之后才到 finally/Stop。
命令120已属于 Stop 后的 cleanup-before-pod，不计入这个阻塞区间。
旧 elapsed 69.060/60.467 是后来 record/Stop 时的累计值，包含失败后的处理；不是准确远端运行时长。

真实本地 producer 每两秒输出约48KiB JSON及48KiB stderr，共六条和结束记录：
停止排空8秒时，sequence1写入阻塞 **5.981秒**，随后样本集中排出；持续排空时最大写入耗时 **0.496ms**。
这是本地管道背压的负/正控制，证明交错控制面调用可以阻塞写入。
两次都完整退出，并未模拟出真实 EKS reset。实际首个 Weir 样本出现在8.571秒窗口末尾，
且正式 helper 的 evidenceWriter 有1秒写截止，而这个 Python producer 没有；**不能据此把历史尾部丢失归因于背压**。

旧共享 Observer.stop 会吞掉已经观察到的 sample error、非零退出和输出超限异常。
新增真实管道回归在旧源码下失败，最终源码下通过；错误证据不能因稍后 Stop/exit0而消失。
这个局部缺陷可独立修复，但原 M30R4 已判失败，不把修复描述为发现了新的成功原生结果。

## 准确客户端与六次有界实验

当前 `/usr/local/bin/kubectl`：`v1.35.2-eks-f69f56f`，commit
`8b30555995a33227bad00fda6379ad2102dd9a6c`，Go1.25.7，**darwin/amd64**；
SHA256 `d6487d72d341c1db4d7fe6840e96cd3c99d75e8fe6dcef205ceb1c987ebf84f5`。
没有升级/替换工具，不称为 darwin/arm64 原生运行。
版本输出为 clean，但 Go buildinfo另有 `vcs.modified=true`，两份原始记录均保留。

所有 wire 命令显式 `--kubeconfig=/dev/null --server=http://127.0.0.1:<独占端口>`，
独占空 HOME/cache，仅传入 PATH/HOME/KUBECONFIG；不继承认证/代理环境，不加载现有配置或 credential exec 插件。
服务端记录所有路径和 authorization_present=false；只返回静态 discovery/Pod 与一个 v5 升级流。
使用已有 gorilla/websocket v1.5.3及标准库，没有实现 WebSocket 框架或修改产品 module。

| 实验 | 冻结输入 / 结果 | 总耗时 |
| --- | --- | --- |
| 1 | 原最小 API 缺 discovery，CLI在升级前 exit1；保留失败 | 1.862秒 |
| 2 | 同一夹具延迟普通请求11秒；discovery内部出现四次10秒请求失败，未升级 | 40.172秒 |
| 3 | 原取消控制同样停在缺 discovery，未证明 stdin 语义 | 0.156秒 |
| 4 | 真实producer，延迟排空8秒；写入阻塞，最终六样本/结束/exit0 | 10.050秒 |
| 5 | 相同producer持续排空；六样本/结束/exit0 | 10.072秒 |
| 6 | 冻结一次静态 discovery 修正，三臂对照，参数不调优 | 合计23.357秒 |

实验6三臂：

- 保持 `--request-timeout=10s`，升级后每两秒六样本，10.1秒后结束；收齐288,300 bytes stdout与54 bytes stderr，CLI **exit0，11.107秒**。
- 同客户端/参数，只有 Pod GET 延迟11秒；在 **10.205秒 exit1**，明确 Client.Timeout awaiting headers，未进入 exec。
- 升级后本地2秒关闭stdin；服务端收到二进制 `255,0`（base64 `/wA=`），排出取消尾记录并正常关闭；CLI **exit0，2.043秒**。

这证明当前准确客户端的 `10s` 不是所测 v5 流持续读取的硬截止，且普通 Pod GET 有10秒截止；
本阶段**不改 exec/GET 的 timeout flag**。合成服务端没有实现 EKS 对 exec URL `timeout=10s` 的处理，
也没做延迟升级握手实验，因此真实服务端/代理截止、握手上限、TLS/SPDY路径仍未证明。

调查起点17:04:08Z、固定最迟17:34:08Z；六次实验均含关闭/Wait且少于60秒，完成时间见 investigation-complete.json。
六个登记实验共含8次客户端/producer执行，第6实验的三臂在同一个23.357秒窗口完成；没有后续实验、试不同timeout直到成功、EKS验证或新资源。实验1–3的夹具失败保留，实验2内部GET数不冒称一次HTTP。
初版服务端源码保留，但重建前未记录其二进制hash；不能宣称初版binary已精确封存。
准确 kubectl、Python、Go、最终服务端及两版fixture源码身份见 tool-identities.json。

## 固定源码核查与尚未解决的问题

优先读取已有本地 client-go v0.35.2 源码；另取公开、无认证的上游
[kubectl v1.35.2 exec.go](https://github.com/kubernetes/kubernetes/blob/v1.35.2/staging/src/k8s.io/kubectl/pkg/cmd/exec/exec.go)。
[source/provenance.json](../.testdata/m30r5/source/provenance.json)逐文件记录来源URL、版本、长度和SHA256，tag对象也封存；上游tag解析到commit `fdc9d74cbf2da6754ebf81d56f80ae2948cd6425`。
准确 EKS fork commit的公开源码映射未建立；一次 aws/kubernetes准确commit URL返回404。
**上游源码只用于解释对照，不等于该 fork 的完整源码证明**，不使用当前最新版推断旧二进制。

上游所示：exec将调用上下文传给StreamWithContext；WebSocket构建独立请求并调用DialContext；
多路读取将数据写入各stream的io.Pipe，写入可阻塞；stdout/stderr复制完成后才读终态错误；
stdin EOF通过v5 stream-close发送。正式helper只读核对：stdin读结束会取消观察并Join；
第六次sample编码成功后才写observer_end，单次输出写截止1秒。这些未改动。

真实EKS reset发起方、远端准确退出时刻、网络/代理/API server/kubelet行为及未落盘尾字节仍unknown。
后续若统筹批准真实验证，需要新的单次完整流、首个本地退出观察/EOF/Stop时间、前后身份及原始错误；
仍失败时才按新的明确授权获取对应端的升级/关闭证据。此处不自开线上阶段。

## 最小生命周期变更

无负载预检每次只拥有一个 observer：开始前身份确认，持续排空两管道，要求八条完整记录
（identity、sequence0–5、正常observer_end）、exit0与双EOF，Stop/Wait后再作身份复核，随后才下一角色和client snapshot。
完整sample/身份/limits/间隔校验仍由原observation_report执行；字段256KiB、4096FD枚举、2秒间隔和64MiB总输出界限不降。
每角色独占 operation 文件在身份读取前创建；同角色二次调用不能生成新child。

保留原30秒本地额度，并提前从该角色身份检查开始计算；受原900秒总deadline约束。
其中预留4秒EOF排空和4秒既有Stop/Wait，不在所有child创建后新开窗口；余额不足拒绝新exec。
这个更早计时的额度可能在慢控制面提前拒绝，**没有声称已适合真实EKS延迟**，也不通过加时间求绿。
失败/取消从observe返回前已关闭stdin、排空并Wait；外层诊断GET/logs开始时没有自有observer活着。
普通180秒cleaner、M30R4C2独立300秒与完整五类分批盘点不变。upload/release/DB单次路径未重写或重放；上传调用处仅保留首个异常，Stop错误附加为note，避免非零退出遮住SIGINT。

共享Observer每次poll对每个pipe最多读16×64KiB，使连续stdout不能无限占住stderr及deadline检查。
记录双EOF及首个失败，Stop后仍返回该失败；正常/取消、退出后缓冲尾部、partial、非零和超限保持可区分。
新增 started、first_exit_observed、stop_requested、stopped单调时间；首次观察退出不是实际远端退出时间。
没有线程池、async框架、通用executor或可替换生产函数变量。

## 回归、封存与资格

| 范围 | 通过 | 既有skip | unittest耗时 |
| --- | --- | --- | --- |
| 受影响普通 | 46 | 0 | 20.963秒 |
| 受影响优化 | 38 | 8 | 18.048秒 |
| 全套普通 | 218 | 0 | 218.767秒 |
| 全套优化 | 193 | 25 | 219.049秒 |

命令和开始/结束/退出码见各suite JSON；聚焦模块为observer_lifecycle_test、eks_resource_preflight_test、resource_local_test、capacity_cleanup_test。全套使用`python3 [-O] -m unittest discover -v -s scripts -p '*_test.py'`；每suite外层360秒上限。新的11项测试覆盖正常六样本、双EOF/退出缓冲、取消/真实SIGINT、deadline余额和耗尽、双pipe慢/高量输出、64MiB界限、partial/非零/sequence、身份漂移、诊断前Join及重复调用。

旧源码首败见 lifecycle-red.err；共享Stop增强后的真实SIGINT+EOF/exit9取消回归另在cancel-unwind-red.err复现异常覆盖，调用处修复后才计最终通过；一次聚焦测试运行器错误地从scripts目录启动，导致已有相对fixture路径找不到，
原日志保留为harness-cwd-first-*。修正运行器cwd为仓库根之后才计入最终结果，未为此修改产品或测试断言。
普通/优化测试同时覆盖共享Docker采样调用点、上传shell/截断/hash/取消、release不重放与清理边界。
没有Go/产品变更，因此没有重复Go/race/镜像/数据库测试或用离线结果替代原生资格。

证据根 `.testdata/m30r5/`：冻结实验、源码/工具身份、原始stdout/stderr/exit/时钟、负正控制、测试首败与最终日志、
最小diff、正式输入/历史核验、进程/端口关闭记录。manifest逐文件hash/size封存，排除自身及避免自引用的delivery.json。
M30R4 **544**、M30R4C **67**、M30R4C2 **197**以及更早 **1001**份封存文件均不变。
M30R4 manifest `87fbd519d33fa9beacfaf7e734c3ce7c3bbd44fe968a3fe0cd54dbc1d4925bfd`；
M30R4C2 manifest `c3e083aac7f3ccb3ca2124c85a3d88fc119ffb6307ca2472a70283fec4e7365b`。
204 Go/module、Weir70/helper81正式输入逐文件与image source `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`相同。
所有自有实验/测试进程结束，临时监听已关闭；准确PID与端口在cleanup-evidence.json，不扫描用户秘密或匿名客户端状态。

原M30R4缺observer_end/client、`resource_evidence=partial/not-qualified`、`timing=not-run`、`candidate=null`不改写。
M30R4C2清理成功保持；本轮没有新的远端清理结论。CNI/多节点、六平台、其他规格/后端、容量/恢复/24h仍required。
本地main交付、干净、不push；一次完成回调后停止，不建新聊天、不恢复timer、不访问EKS。
