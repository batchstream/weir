# M30R4 — 静态预检与动态资源窗口分阶段；采样及完整清理未通过

2026-09-28。**本轮仍 NO-GO。** 静态预检结束后才启动唯一120秒资源窗口，prepare与native写入前
复核已在同一窗口成功完成。一次release在CLI reset后通过同一不可变身份、成功终态及完整有序日志
确认远端完成；没有重放。Weir/ES各只取得5个完整样本，均无normal observer_end，client snapshot未执行。
清理确认Job/Pod/ConfigMap/NetworkPolicy消失，但最终完整盘点在对象阶段截止处失败，Quota及namespace保留。
`resource_evidence=partial/not-qualified`、`timing=not-run`、`candidate=null`。

## 冻结实现与制品

- 干净main基线：`78a7baff6f821d31738ca07a2e601256fe162b97`。
- 实现、完整测试和唯一真实执行源码：`92b04b5167127c067413624aec35c00f6e82d1d6`。
- 实际公开image source：`4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。
- 75项完整输入冻结；plan绑定29项执行依赖。204 Go/module及Weir70/helper81正式输入逐项与image source相同。
- 最终交付仅追加本报告/readiness；准确交付SHA和manifest SHA见证据根`delivery.json`及完成回调。
- 没有push、Actions、发布、Go/module或正式构建输入修改、新镜像、registry下载、负载或扩容。

只复用五份明确公开文件：index、manifest、config、application.tar.gz、qualification；没有读取匿名客户端状态、
kubeconfig、credentials/token、`.env*`、`.ssh`、pem/key或Kubernetes Secret。helper完整index→arm64 manifest→
config/source/platform→layer digest/长度→gzip diffID→唯一regular 0555 member→binary字节及本地权限再次验证。

| 制品 | 固定身份 |
| --- | --- |
| Weir index | `sha256:2a3ca21b950f42449b01543e96655e0484e432f8ecbf29b46c49f7fd426c229a` |
| Weir arm64 manifest / binary | `100102a8c319a24f571f2aaa3041d6fafc79338840829e792f404e48d117dd5e` / `f2d107995762040c55b03cb37e94dd9e6beaa14a99d563b278b4bd9c2bd4db7e` |
| Qualification index | `sha256:fbef16495a37b89d44cfce51c38096e01e982ab875e3352b67c59480fdbe61a7` |
| Qualification arm64 manifest / config | `9eb2ff4243d7c021abe047da28867a9b3df65695660675ebcb761387b4f50351` / `063aa9659604ca3b7ab3814e31341727fe2ccce6f6758456d3f97c4b911209ee` |
| Qualification layer / diffID | `e7020f259dcaf846bb2772aca7dd4f2bc353ef7292b95549e4f541509eea03c7` / `6decd3193e4ce7cc311e886ca1f801031a16fced6b24da3cbcecc71fc853a86d` |
| Qualification binary | `d41f70ca4bbe129bff11b76f3d973cdb288d839c4df72a348cac04a233b76482` |
| ES 8.19.22 / JDK27 | `docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237` |

应用层10,810,877 bytes，helper21,046,963 bytes。原单Pod6CPU/4608MiB/2560MiB ephemeral、helper64MiB磁盘emptyDir、
Weir2CPU/1GiB、ES3CPU/3GiB/heap1GiB、client1CPU/512MiB均保持。非root/dropALL/RuntimeDefault、
Weir/client只读root、bootstrap唯一RW helper、运行期Weir/ES helper RO、immutable精确ConfigMap和脚本校验保持。
仅loopback，没有Service、跨Pod、host namespaces/hostPath、ptrace/debug；metadata-disable保持。

## 最小实现与离线测试

`eks_loopback.prepare_context()` 将23次权限检查、CNI及Git检查移至`check_node()`之前；完整helper验证和输入hash
也先于动态阶段。旧loopback/pacing准备调用点同步调整，避免静态检查落入资源窗口。保存静态结束及plan准备单调时戳。
run必须加载已有冻结窗口，缺失窗口直接拒绝；正常执行与补充GET均不修改原start/deadline。

每次资源GET派发前要求余额至少25秒CLI + 4秒Stop/Wait，不足时报告“资源窗口余额不足”，不创建该CLI。
足够时仍传入完整25秒，单Kubernetes请求10秒不变。唯一补充GET仍只适用于尚未写入、明确API读取超时的相同
node/UID/范围，原命令/错误保留；权限、UID、语义不完整、资源不足、本地截止或写后失败都不恢复。
没有增加budget framework，也没有改release、cleanup、资源计账算法或正式Go采样器。

测试先记录旧实现的失败：余额守卫4项失败、静态顺序2个子例失败；日志原样保留。受控时钟模拟23次权限检查与
CNI共144秒，期间窗口仍未启动；之后prepare两次GET与native两次GET共用同一start/deadline。
覆盖prepare后等待121秒、读取消耗120秒、缺失冻结窗口、13.201/28.999秒余额零派发、29秒完整allowance、
下一GET或补充GET余额不足零新增CLI、足够余额时范围和10/25秒不变、整个窗口最多一次恢复。
保留非零`[]`、UID漂移、资源不足、权限/不完整/本地截止/写后不恢复及原release/清理全部正反例。

完整普通模式 **191通过，152.416秒**；`-O` **166通过、25项既有skip，145.951秒**。
命令分别为`python3 [-O] -m unittest discover -s scripts -p '*_test.py'`。
真实子进程、管道、Stop/Wait与shell guard回归在原套件中执行；没有重新运行Go测试或构建来冒充新Go资格。

`m30r3-window-offline-replay.json`直接引用旧37条命令单调时戳：旧Pod GET在窗口第106.799秒启动，余额13.201秒，
旧本地allowance约9.201秒，实际9.247秒后exit-15且空stdout/stderr；属于本地截止，不是明确API read-timeout。
新守卫在此余额下会拒绝启动命令。这是**离线时间算术回放，不是新EKS证据**，M30R3旧失败未改写。

## 唯一真实阶段时间线

固定context `arn:aws:eks:us-west-1:956540890581:cluster/data-team`，region `us-west-1`。
固定node `ip-172-31-12-243.us-west-1.compute.internal`，UID `18f03c58-83c1-424e-be34-44ef88c07831`。
三轮资源读取全部只读该node、计入全部namespace的同节点Pod，没有换候选或全局扫描。
CNI最小投影再次返回`--enable-network-policy=false`；namespace/default-deny不构成已证网络隔离。

namespace/owner：`weir-qual-m30r4-20260928-233348`。全新create-only evidence根、唯一独占invocation。
prepare总125.848秒，其中静态结束在父进程启动后112.785秒：monotonic **384410.321932166**。
资源窗口随后开始：**384410.322576875 → 384530.322576875**，精确120秒。
plan准备时窗口年龄13.051秒，SHA256 `b84b66258cba2ba90ff53c48539eb942c6bac947fa84ccc5eb52a3600916a11a`。

| 阶段/GET | 命令 | 窗口内启动偏移 | 命令耗时 | exit |
| --- | --- | --- | --- | --- |
| prepare node | 31 | 0.001秒 | 6.380秒 | 0 |
| prepare 同node全namespace Pods | 32 | 6.386秒 | 6.661秒 | 0 |
| native写入前 node | 36 | 13.250秒 | 4.005秒 | 0 |
| native写入前同node Pods | 37 | 17.261秒 | 9.403秒 | 0 |
| native写后、Job创建前 node | 57 | 148.806秒 | 4.636秒 | 0 |
| native写后、Job创建前同node Pods | 58 | 153.449秒 | 12.306秒 | 0 |

最后两条按既有规则使用原900秒native内的启动子窗口，无恢复；不是续期120秒预检。
写入前Pod GET启动时余额102.739秒，四次GET在窗口26.664秒左右全部完成；23次权限/CNI/Git都在窗口开始前完成。
三次Pod响应均6019 bytes/6 Pods，账本spare均 **7.030CPU / 31,202,312,192 memory bytes /
18,182,813,665 ephemeral bytes / 52 Pods**，满足原7CPU/5632MiB/5GiB/3Pod门槛。非原子观察不等于资源预留。
资源恢复实际 **0次**；原窗口内容与冻结plan完全一致。

## 一次触发确认成功，采样仍不完整

Job/Pod server dry-run、精确资源/安全准入均通过。一次helper上传21,046,963 bytes，完整hash回执exit0，
上传CLI约53.450秒；前后namespace/Job/Pod UID和bootstrap imageID/containerID相同。
唯一release命令79收到`{"artifact":"released"}`，但CLI connection reset、exit1；保留原stdout/stderr及transport=failed。
随后同一bootstrap容器Completed/exit0、restart0/无lastState，immutable配置、完整有序waiting→hash→guard→
管理started/ack/completed日志及读取前后身份都通过，远端结果为`confirmed Completed0 with full ordered logs`。
**M30R3的新release结果确认机制本轮得到这条原生路径证据**；不把transport失败改称成功，也不倒写M30R2的UNKNOWN。
空records索引PUT尝试/完成各1，无重新上传、release或PUT重放；seed/setup/pace/trial/planned/document mutation均0。

实际Weir版本与image source吻合，ES main loopback检查及原生身份取得。内核`6.12.77-99.140.amzn2023.aarch64`，
CLK_TCK=100，JDK binary SHA256 `4ff04917307c25c355f2a96325a214f7cdf1b6261b474c4112166e0fbe73c11f`。
Weir/ES observer各启动一次，正式2秒/256KiB/4096FD枚举边界没有改动。

| 原生raw | Weir | ES |
| --- | --- | --- |
| 完整JSON样本 | sequence0–4，共5个 | sequence0–4，共5个 |
| 完整流 | 第7行/第6样本截断，尾部无换行 | 末尾有换行，但第6样本及结束记录缺失 |
| observer_end | 无 | 无 |
| CLI | exit1；broken pipe及connection reset | exit1；connection reset |
| target / observer PID | 1 / 696 | 64 / 211 |
| 同容器UID | 65532 | 1000 |
| FD soft / hard | 1048575 / 1048576 | 1048576 / 1048576 |
| 已观察FD | 9 | 566–567 |
| pids.current / pids.max | 15 / 37697 | 103–104 / 37697 |
| cpu.max | 200000 100000 | 300000 100000 |
| memory.max / swap.max | 1073741824 / 0 | 3221225472 / 0 |
| cpuset | 0–7 | 0–7 |

10条完整样本逐条通过既有sample_check，其真实proc/stat/status/limits/cgroup/CPU/memory/PIDs/swap/cpuset/io/TCP、
metrics/stats及身份原字节均保留。Weir raw279,120 bytes，其中末行36,859 bytes为不完整JSON；ES raw88,355 bytes。
这些只是失败流中的局部证据，不能通过完整流资格，也不能用旧FD/PID或合成结果填充。client snapshot未执行。
首要错误为ES observer退出；finally停止Weir时另外记录truncated observer output。不据stream reset推断根因或因果。

## 有界清理与准确残留

native主流程493.115秒，清理131.068秒，总子进程624.395秒；900+180总边界没有扩展。
清理deadline **385096.687415833**，对象阶段deadline **385051.687415833**，最后45秒预留未被挪用。
先核验namespace，再逐对象fresh UID/owner、UID条件DELETE；Job/Pod/ConfigMap/Policy均各删除一次并exit0空GET确认。

| 对象 | UID | 本轮收口 |
| --- | --- | --- |
| Job loopback | `6a24a607-61fc-4e85-9e40-60b34f37a5ef` | 命令123删除；124确认消失 |
| Pod loopback-4rs4j | `27e6d830-67fa-4f70-9de4-64db840734a6` | 命令126删除；128确认消失 |
| ConfigMap configuration | `51b30696-baf1-4eb7-a6ba-3e26dd7aafc8` | 命令130删除；131确认消失 |
| NetworkPolicy default-deny | `574bf5a6-ab12-4a3b-99ed-558467ef4a85` | 命令133删除；134确认消失 |
| ResourceQuota budget | `2deaa5be-0822-4db5-83bc-df4d2ec9233e` | 保留，未尝试DELETE |
| Namespace weir-qual-m30r4-20260928-233348 | `f6b048cc-447d-4ee8-8835-11ef13542758` | 保留，未尝试DELETE，消失未确认 |

Quota前只尝试一次新的完整盘点。命令137新读取准确Quota，`status.used.count/secrets=0`；没有读取Secret。
命令138/139两批列表成功；第三批命令140开始时总余额66.049秒、对象阶段余额21.049秒，扣4秒Stop/Wait后
本地allowance约17.049秒，实际17.100秒退出-15，stdout/stderr均空。没有明确API读超时证据，也没有完整最终
foreign盘点或其后的namespace UID复核，因此不能删除Quota/namespace，后续45秒不能绕过归属门槛使用。
没有补发盘点/DELETE、预算外GET、force/finalizer、外来删除或重新打开清理窗口。

**M30R3清理机制只取得先停准确自有工作负载的局部原生证据，完整清理仍未通过。**
原`result.json`保留`cleanup.confirmed=false`和`remote_helpers_closed_by=unknown`；独立`closure-analysis.json`
依据命令124/128的exit0空结果确认Job/Pod已消失、远端进程已停止，不将其等同namespace清理成功。
剩余准确namespace及Quota UID交统筹；默认ConfigMap/ServiceAccount UID保留在`namespace-defaults.json`。

## 操作计数与封存

140条编号CLI全部结束并Wait：137 exit0、2 exit1（release79、ES诊断logs115）、1 exit-15（盘点140）。
另3条Observer CLI：upload exit0、Weir/ES exit1，全部Wait；合计143 CLI，其中kubectl136、AWS1、git6。
编号kubectl主动作：GET82、auth23、current-context1、create11（server dry-run6、持久create5）、
api-resources2、logs6、exec4、UID DELETE4。Observer的3次exec另计，合计exec7。
所有本地测试/prepare/native/upload/observer PID结束，未保留后台自有进程。

已证HTTP：空索引PUT1；bootstrap guard GET2、main guard GET2；Weir/ES各5条完整HTTP响应记录。
两observer最终HTTP尝试/响应总数因缺失/截断为unknown；client snapshot HTTP0。
kubelet探针和kubectl内部HTTP总数unknown，CLI数不等于HTTP数。原operation-audit中Weir计数unknown保留，
独立分析只给出5条完整响应记录下界，未填补未知尾部。

证据根 `/Users/liran/Projects/liran/go/weir/.testdata/m30r4/`，包括冻结输入/制品链、测试首败及最终日志、旧时戳离线回放、
唯一plan/invocation、全部CLI原始stdout/stderr/exit、上传/release/有序bootstrap日志、真实失败流、局部样本验证、
时间线/HTTP计数/UID残留分析。`manifest.json`逐文件hash/size封存，排除自身及避免自引用的`delivery.json`。
旧M30/M30R/M30R2/M30R3共1001份封存文件未变，manifest分别为
`8fcc58766c27be59c6e318ae24bb7d77bb2e49b21f61ceed52231f78380b48da`、
`266df5e8afa2f3d8890b45ca6d814e469d253c16dcdb9e22bf6d6cab233c951d`、
`5aaeef433197d1091dbe561642d2ed48a81afebf06ad985a614274a17298dc63`、
`52cb30c81b97864fbdf679f761acb3c09111d7ef29919b37603bd6b563a029ec`。

原M30R3无资源，不重新清理旧namespace。CNI/跨节点、六原生平台、其他规格/后端、完整资源、计时、容量/过载/恢复/24h
仍required；Auth排除和ProgramTransform V1延期不变。raw交统筹冻结后续合同；一次回调后停止，不自开负载、聊天或定时器。
