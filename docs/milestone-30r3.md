# M30R3 — 一次释放结果确认与清理收口；真实窗口在写入前停止

2026-09-28。**本轮 NO-GO：没有取得三角色无负载样本。** 实现完成并通过全部 Python 普通/优化测试；
唯一新 native invocation 在写入前的固定节点 Pod 读取触及共同资源窗口的本地截止。
没有创建 namespace/Job/Pod、上传 helper、执行 release、索引 PUT 或采样。未现场改代码、重开窗口或补跑。
`resource_evidence=partial/not-qualified`、`timing=not-run`、`candidate=null`。

## 源码与边界

- 干净 main baseline：`aa01c572d9a90312d627e65775b0cc8d30cb954f`。
- 实现、最终受测代码及唯一实际准备/执行源码：`fea02e9c40385049851bd3eaaaffc19aea3fc4bf`。
- 最终交付仅随后更新本文/readiness；完整交付 SHA 见证据根 `delivery.json` 和完成回调。
- 实际公开 image source：`4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。
- 75 项工具/配置输入冻结，计划绑定其中 29 项执行依赖；完整列表见 `source-freeze.json`、`native/plan.json`。
  204 Go/module、Weir 70 项和 helper 81 项正式构建输入逐项与 image source 核验未变。
- 未 push、Actions、发布、构建新镜像、下载 registry、修改 Go/module、运行负载或扩容。

只复制五份明确公开制品：index、manifest、config、application.tar.gz、qualification。
准确 index→arm64 manifest→config/source/platform→应用层 digest/长度、gzip diffID、唯一 regular 0555 member、
binary 内容及权限均重新验证。层 10,810,877 bytes；helper 21,046,963 bytes。
没有读取/复制匿名客户端状态、凭据、Secret 或业务配置。

| 制品 | 固定身份 |
| --- | --- |
| Weir index | `sha256:2a3ca21b950f42449b01543e96655e0484e432f8ecbf29b46c49f7fd426c229a` |
| Weir arm64 manifest / binary | `100102a8c319a24f571f2aaa3041d6fafc79338840829e792f404e48d117dd5e` / `f2d107995762040c55b03cb37e94dd9e6beaa14a99d563b278b4bd9c2bd4db7e` |
| Qualification index | `sha256:fbef16495a37b89d44cfce51c38096e01e982ab875e3352b67c59480fdbe61a7` |
| Qualification arm64 manifest / config | `9eb2ff4243d7c021abe047da28867a9b3df65695660675ebcb761387b4f50351` / `063aa9659604ca3b7ab3814e31341727fe2ccce6f6758456d3f97c4b911209ee` |
| Qualification layer / diffID | `e7020f259dcaf846bb2772aca7dd4f2bc353ef7292b95549e4f541509eea03c7` / `6decd3193e4ce7cc311e886ca1f801031a16fced6b24da3cbcecc71fc853a86d` |
| Qualification binary | `d41f70ca4bbe129bff11b76f3d973cdb288d839c4df72a348cac04a233b76482` |
| ES 8.19.22 / JDK 27 | `docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237` |

## 实现及离线证据

`transfer()` 仍要求准确字节/hash/0555、完整上传回执和前后 namespace/Job/Pod UID、bootstrap imageID/containerID
一致且零重启。先独占落盘唯一 release 尝试，再执行一次。完整回执只证明 transport 回复；已退出的非零 CLI
保留命令、exit、stderr，缺失回复维持 UNKNOWN。取消、本地超时、输出/证据错误和矛盾回执都停止。
不重新上传、换 Pod、补发 release 或重放空索引 PUT。

两条 transport 路径汇合到同一个完成检查，全部仍在原 420 秒启动截止内：准确对象与容器身份、无 lastState/
restart、bootstrap Completed/exit0、完整有序的 waiting→hash 验证→guard→唯一管理 started/ack/completed。
新 fixture ConfigMap 设置 immutable，实际 UID/data 与冻结脚本逐字核对；Pod/Job 准入核对脚本执行路径。
有限日志检查完整首尾、唯一回执、guard HTTP 输出、四张 socket 表的顺序/原字节长度和逐行决策，拒绝缺失、
乱序、截断及矛盾日志。完整日志取得前后再次确认同一容器身份。Ready、exit0、文件或索引存在均不单独授予成功。
没有增加产品重试、认证、幂等、多轮握手、后台 bootstrap、等待 exec 结束的 sleep 或超时扩展。

清理先核验 namespace UID/owner，再逐对象 fresh UID/owner + UID 条件 DELETE，先 Job/Pod 后其他登记对象。
删 Quota 前保留**一次新的完整 API 盘点**、foreign 检查、当前 namespace UID、准确 Quota 的 Secret count=0；
无 quota/完整性/归属证据则不删 namespace。未知 CRD、默认对象准确 UID、事件归属和只读 PodMetrics 分类保持原规则。
外来对象不阻止先停止准确自有对象，但阻止 Quota/namespace 整体删除。UNKNOWN DELETE 仍只读确认、不重放。
原 180 秒中显式保留 45 秒给 namespace 删除及 exit0 空输出的消失确认，包含 CLI Stop/Wait 余量；无并发/force/finalizer。

普通模式 **186/186 通过**，149.426 秒；优化模式 **161 通过、25 项既有 skip**，143.736 秒。
新增正反例在两种模式下均执行：正常/丢失/空回复、完整远端确认、原 reset/真实状态回放、终态失败、UID/containerID/imageID
漂移、重启/lastState、配置变化、日志缺失/乱序/截断/矛盾、上传失败、取消和截止、重复调用拒绝。
真实子进程/有界 stdin 与输出管道验证 SIGINT/SIGTERM、超时、停止及 Wait；已有真实 shell guard 夹具也生成完整日志走新解析器。
清理回归覆盖先停自有 Pod、逐 UID/owner、一次完整盘点在 Quota 前、foreign/Secret 计数/metrics/最终 GET 非零、预算预留，
保留原 M26R5 全盘点反例。离线 mock/派生状态不作为 EKS 证据。

`eks-resource-release-m30r2.json` 原样保留 M30R2 reset、上传身份、准确 Pod/Job 状态、计划和 UID；正例日志、微型 helper、
immutable ConfigMap 都明确标注为离线合成。原缺日志状态仍 UNKNOWN；不据新正例倒写历史 PUT 成功。
首轮定向测试的夹具/断言失败日志均保留；修复后完整两种模式通过。历史 seal 审计首次使用错误 size 字段产生 KeyError，
随后按原 bytes/size schema 完整复核，未修改历史材料。

## 唯一真实窗口和准确失败点

固定 context `arn:aws:eks:us-west-1:956540890581:cluster/data-team`，region us-west-1；
cluster ACTIVE/Kubernetes 1.36。固定 node `ip-172-31-12-243.us-west-1.compute.internal`，
UID `18f03c58-83c1-424e-be34-44ef88c07831`，linux/arm64，内核 `6.12.77-99.140.amzn2023.aarch64`，
kubelet `v1.35.3-eks-bbe087e`。CNI 最小字段再次确认 `--enable-network-policy=false`，隔离仍 unqualified。

准备 namespace 名称 `weir-qual-m30r3-20260928-230549`；命令 3 exit0 且空 stdout 确认当时不存在，之后全部命令只读。
prepare **110.596 秒**成功；计划 SHA256 `85fec20783f5e3036322436f015fd901e5b567e3e4998e8cdf9a4e411567e5ba`。
唯一 native invocation 用时 **13.578 秒**，主流程记录 13.393 秒；在任何写入前失败。

| 节点读取 | 命令 | 耗时 / exit | 原始结果 |
| --- | --- | --- | --- |
| prepare node | 4 | 2.418 秒 / 0 | 准确 node/UID、完整最小字段 |
| prepare 同 node 全 namespace Pods | 5 | 3.579 秒 / 0 | 6019 bytes、6 Pods |
| native 写入前 node | 36 | 4.083 秒 / 0 | 同一 node/UID |
| native 写入前同 node Pods | 37 | 9.247 秒 / -15 | stdout/stderr 均 0 bytes；无完整响应 |

第一次计账 CPU used0.880、memory used780,140,544、6 Pods、ephemeral0；余量为
**7.030 CPU / 31,202,312,192 bytes memory / 18,182,813,665 bytes ephemeral / 52 Pods**，原
7CPU/5632MiB/5GiB/3Pod 门槛未改。这只是第一次非原子观察，不作为第二次派发资格或资源预留。

共享资源窗口 monotonic `382625.866364750 → 382745.866364750`，精确 120 秒、未重置。
命令 37 于该窗口第 **106.799 秒**开始，剩余13.201秒；扣除4秒 Stop/Wait预留后，本地命令 allowance约9.201秒。
命令在9.247秒完成停止/Wait，exit -15。它是本地截止，**没有明确 API 读取超时错误**；不能推断网络或服务端根因。
既有一次只读恢复只适用于指定 CommandFailure/明确读取超时，因此本轮未使用恢复，未把空输出算成0占用。
没有改变查询策略、120秒窗口、门槛或写后规则来求通过。

原计划仍冻结900秒整体+180秒清理、上传300秒、artifact420秒；单Pod6CPU/4608MiB/2560MiB ephemeral、
64MiB helper emptyDir、三角色资源/安全/loopback边界和正式采样器合同均未变，但本轮没有创建这些对象。

## 操作计数、收口与证据

37 条编号 CLI 全部 Wait：36 条 exit0，命令37 exit-15。kubectl30、AWS1、git6；kubectl 包含
GET6（node2、同node全namespace Pod LIST2、namespace1、CNI1）、auth23、current-context1。
没有 server dry-run、持久 create、UID DELETE、exec、上传、release、bootstrap/采样 HTTP。
空索引 PUT 实际尝试/完成均0，seed/setup/pace/trial/planned/文档 mutation/重放均0；observer/client snapshot均0。
kubectl内部HTTP总数unknown，不把CLI条数当HTTP请求数。

自有 UID 清单为 **空**，没有远端对象/进程启动，因而没有待删对象或 helper 需停止。
原 `native/result.json` 的 `cleanup.confirmed=false`、remote_helpers unknown 是尚无namespace登记时的保守通用字段，
原样保留；`closure-analysis.json` 依据全部只读argv解释零资源事实，**不把本轮写成真实清理成功**。
没有截止后补GET、扩大180秒或重新运行；所有本地测试/准备/native子进程均结束并Wait。

证据根 `/Users/liran/Projects/liran/go/weir/.testdata/m30r3/`：75项输入冻结、公开制品完整链、输入证明、
全部普通/优化与首败测试日志、唯一plan/invocation、37条CLI原始stdout/stderr/exit、窗口计时审计、零写入收口分析。
`manifest.json` 逐文件hash/size封存，排除自身和避免自引用的`delivery.json`。本轮没有远端upload/release/日志/资源样本，
对应证据标为未执行，不用离线文件填充真实结果。

旧 M30 342、M30R 60、M30R2 447 文件逐项hash/size未变，manifest分别保持
`8fcc58766c27be59c6e318ae24bb7d77bb2e49b21f61ceed52231f78380b48da`、
`266df5e8afa2f3d8890b45ca6d814e469d253c16dcdb9e22bf6d6cab233c951d`、
`5aaeef433197d1091dbe561642d2ed48a81afebf06ad985a614274a17298dc63`。
M30R2 namespace 已在本轮前由统筹新的exit0空GET确认不存在；其原180秒窗口的确认缺口和PUT UNKNOWN保留，
未重新清理旧namespace。

释放结果确认和新清理阶段目前仅离线通过；三角色真实proc/FD/PID/cgroup/CPU/memory/swap/cpuset/io/TCP/metrics/stats
仍未取得，不能冻结负载合同。完整资源、计时、容量/过载/恢复/24h、CNI/跨节点、六原生平台及其他规格/后端安全
仍required；Auth排除、ProgramTransform V1延期保持。交统筹一次回调后停止，不开启下一阶段/聊天/定时器。
