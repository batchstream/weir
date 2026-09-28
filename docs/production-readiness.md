# Weir 生产资格验收清单

日期：2026-09-28。本文件是**必需验收目标及证据缺口清单，不是生产就绪声明**。
M23 已获统筹独立验收：最终 main `d4081c449b6e80e639a1ce6930a090746bd2c813`，
完整 DNS fixture、默认 CGO0/race/vet、integration/Windows静态检查通过；原失败历史保留。
[M24](milestone-24.md)的固定源码 Actions/两个公开 GHCR 包交付与匿名原生短时 smoke
已获统筹有限独立验收；准确镜像source为 `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`。
第1次打包入口失败保留；第2次四个job成功，两Linux架构默认CGO0/race/vet及准确image启动通过。
CI与本机空凭据上下文均完整下载两包index/双平台内容并核对hash；不代表EKS、发生器5ms、DB容量或24h资格。
[M25](milestone-25.md)在授权现有EKS完成只读预检，并创建全新独占namespace及Policy/Quota；
唯一冻结入口因缺labels的标准对象触发metadata模板错误，**尚未创建任何Job/Pod**。
首次入口/清理失败保留；修复模板后仅做UID条件恢复清理，已确认namespace消失，没有第二次原生尝试。
准确image运行、snapshot和五个计时探针全部not-run，resource-evidence=not-run、
generator-ready-for-next-investigation=false、candidate=null、full-calibration=not-run。
现有CNI明确禁用network-policy agent，网络隔离未资格；不能把模板配额或namespace当作实际隔离证据。
[M25R](milestone-25-remediation.md)以最终修复基线、新独占namespace完成一次补证尝试：
默认metadata解析及版本Job server dry-run通过，但Pod因显式Never与Priority admission计算的
PreemptLowerPriority不一致被拒绝；Job在120秒期限后失败，没有运行Pod、snapshot或计时探针。
本轮仍为准入阻塞NO-GO，timing_pass=false、resource-evidence=not-run、generator-ready=false；
准确运行时imageID/binary与资源/latency仍未知，candidate=null、full-calibration/24h=not-run。
全部4个自有对象按UID删除并独立确认namespace消失，cleanup=true；原M25失败未改写。
[M25R2](milestone-25-remediation-2.md)修正统筹强制Never的不兼容约束，补Pod级server dry-run和
按Job UID观察FailedCreate的早停。实现 `c6fc02495bf8942eea7d5512c90f30905d83facb` 完成本地验证后，
新冻结namespace唯一尝试的Job/Pod server dry-run均成功；普通优先级与固定nodeName符合预期，
但Pod响应的region/zone拓扑标签触发入口过严的metadata相等比较，仍在实际Job create前停止。
Job/Pod/probe/planned均0，version/snapshot/resource-evidence=not-run、timing_pass/generator-ready=false；
这是入口兼容缺口，无EKS性能结论。3个自有对象按UID回收、namespace独立确认消失，cleanup=true。
冻结后未改实现或重试；真实FailedCreate早停仅有离线证据。原失败保留，DB candidate=null、
full-calibration/24h=not-run，CNI隔离仍unqualified；统筹已独立复现此入口缺口。
[M25R3](milestone-25-remediation-3.md)实现 `24976ddadd498cf39e86eed59010312d0bdcafc1` 将metadata
检查收敛到请求身份和自有标签键值，允许额外标签且不借此授予归属；实际UID/controller与spec严格检查保留。
基于真实响应结构的外部CLI回归通过后，新冻结namespace唯一尝试完成version、snapshot、
50ops/s三轮及200/800各一轮，均20秒，合计23,000次全部完成、零drop/error/UNKNOWN。
最差dispatch p99为4.7ms≤5ms，最大采样CPU区间占比0.203746、无新throttle，timing_pass=true。
可见cgroup资源边界通过，但CPU仍共享、完整资源资格partial；CNI agent仍disabled，网络隔离unqualified。
generator-ready-for-next-investigation=true仅指统筹可评估下一步；DB candidate=null、容量/24h仍not-run。
全部17个自有对象按UID回收、namespace另行确认不存在，cleanup=true；三次旧失败未改写。
本轮短时计时证据已获统筹有限独立验收，不增加整体生产资格。
[M26](milestone-26.md)的单Pod回环短闭环在写入前停止：34节点快照按沿用的M25保守重复计账，
最大CPU余量5.195低于要求7；spec-only诊断为7.005，不能把保守入口阻塞说成实际CPU耗尽。
另确认准确产品启动前要求ES就绪且索引已存在，三普通容器的有界启动接线尚未实现/资格化。
本轮仅报告，无产品/helper/入口改动；plan未冻结、native invocation/namespace/Job/Pod/DBmutation均0。
准确ES manifest/config经匿名只读校验，但实际imageID/readiness/回环监听/RPC/DB审计/资源均not-run；
candidate=null，完整校准/过载/恢复/24h仍not-run，CNI agent仍disabled、网络隔离unqualified。
本机缺索引启动反例与正向控制的SIGTERM退出断言失败均保留；默认test/race/vet及脚本回归通过。
无自有集群资源，所有本地子进程已Wait；统筹复核指出资源重复计账与启动接线均属测试前提缺口。
[M26R](milestone-26-remediation.md)实现 `2852dd36c15b05fc12535ae8622be0b54c20682c`：
共享具名spec/allocated/actuated逐项max、有序init/原生sidecar阶段峰值、Pod-level覆盖及一次overhead；
真实创建前预检CPU余量7.030达到原7门槛，不能归因为资源不足。新增ES原生sidecar→一次空索引
bootstrap init→准确Weir/client启动布局，Pod峰值仍6CPU/4608MiB/2560MiB。
唯一冻结尝试的Job server dry-run获API接受，但入口将1024Mi→1Gi、3072Mi→3Gi的规范化误判
为资源漂移；响应还省略零值探针默认字段。仍为入口缺陷NO-GO，Pod dry-run/实际Job/Pod/DB写均0。
离线95项及优化模式70项（25项既有skip）、Go默认/race/vet通过，未覆盖上述API规范化；冻结后未改实现或重试。
四个自有对象均按UID删除，namespace另行查询不存在，cleanup=true；RPC/DB审计/运行时imageID与
资源证据均not-run（原result的generic partial初始化值不代表实际采样）。candidate=null，
校准/过载/恢复/24h仍not-run，网络隔离仍unqualified。原M26证据94项hash不变、exit -15及
signal注册窗口未解决；本轮提交后停止，待统筹独立验收。
[M26R2](milestone-26-remediation-2.md)实现 `b13cc34c99fc460a279e0429324a7ca194dab6f5`：
共享准入按字段比较精确quantity与固定probe默认值，保留资源键/limits/类型/未知字段/UID等边界。
真实0050响应经外部CLI完整回放，132组spec负例零持久Job；合成两trial和失败UID清理通过。
普通Python106通过，-O为81通过/25既有skip；固定Go1.27.1离线非缓存default/race/vet通过。
唯一新冻结尝试的真实Job/Pod dry-run及实际创建都通过，ES8.19.22已启动；bootstrap在TCP
local-address guard exit23停止，触发行未记录，具体地址/来源无法确定，不宣称边界通过。
随后ES exit143，无restart/OOMKilled记录；Weir/client未启动，空索引管理写/文档写/两trial均0。
六个自有对象按UID清理，namespace另行确认不存在，cleanup=true；只取得ES/init的partial运行材料。
RPC/DB审计/客户端资源/Weirmetrics等not-run，candidate=null，容量/跨节点/过载/恢复/24h仍not-run；
网络隔离unqualified。原M26与M26R证据未变，signal窗口/exit-15未解决；未冻结后修补或重试。
[M26R3](milestone-26-remediation-3.md)完成拒绝前有界原始表与完整终止诊断，guard语义不变。
本地唯一冻结尝试因网络inspect JSON格式错误停在ES创建前，自有network已回收，未重跑。
唯一EKS只读取证保存三条本PodIP到169.254.169.254:80的FIN_WAIT2记录，bootstrap exit23，
ES143在后，无OOM/restart；同快照新旧guard均23。属于实际非回环边界NO-GO，不能放行，
具体进程/HTTP请求/响应仍未知，M26R2旧缺失行不倒写。诊断完成、修复待统筹独立决策。
测试管理/文档写0，两个main未启动；六对象UID清理并确认namespace消失。
Python普通124通过，优化99通过/25既有skip；固定Go/协议/镜像/CI输入未变，本轮未重复Go测试。
本轮未修改配置再试；candidate=null，功能闭环/容量/过载/恢复/24h未跑，signal窗口仍未解决。
[M26R4](milestone-26-remediation-4.md)实现 `8a080b76693e4042fe0e45b900fbe9bf07283d46`：
共享 ES fixture 固定 `AWS_EC2_METADATA_DISABLED=true`，仅新增该非秘密字段投影，guard 不变。
准确镜像内 SDK2.31.78 在 network-none 的两全新 JVM 对照通过：false 向自有假 loopback 发2请求，
true 发0请求并明确禁用；未访问真实 IMDS。唯一 ES 本地启动因容器 hostname 解析失败导致
日志初始化 exit1，就绪/版本/settings/socket 采样未到达；固定计划未改写或重跑，故 **EKS 未运行**。
普通 Python130通过，优化105通过/25既有skip；真实 API 形状、两trial与失败/取消/UID清理的离线接线通过。
两自有容器均按精确ID停止/Wait/清理，所有验证进程结束，200产品输入及3120既有证据未变。
EKS namespace/Job/Pod/管理写/文档写/真实trial均0，功能闭环仍未通过，candidate=null；
CNI/跨节点/容量/过载/恢复/24h、startup signal窗口与其他资格缺口不变。
[M26R5](milestone-26-remediation-5.md)实现 `f7e4973f3c6d6119c24e128cf2bebdecd339813b`：
新的network-none容器具名hostname→127.0.0.1 hosts映射，本地第1次getent诊断exit2保留；
第2次修正AI_ADDRCONFIG诊断接线后，真实libc/JDK/ES8.19.22/settings与3次socket样本通过。
复用已接受SDK证明，189当前/3120历史证据与200产品输入未变；定向普通/优化各3通过。
唯一EKS尝试完成bootstrap/main完整回环边界、原预算两trial：4000load全success、
2400文档mutation、零drop/error/UNKNOWN/restart，400条Put全部APPLIED/version1。
最差dispatch p99=1.8ms，arrival p95/p99=9.8/18ms，完整客户端样本通过；整体资源仍partial。
但共享清理将本Pod的只读PodMetrics视图判为外来对象，**原入口passed=false/cleanup=false**；
独立恢复清理在原180秒窗口耗尽，首次Job UID删除响应不确定；后续只读确认Job仍存在。
Job/Pod随后按原900秒deadline停止，Pod已不存在；仍剩NS/Quota/Policy/ConfigMap/Job共5个
已核验UID对象；截至原阶段收口未获追加清理授权，**当时namespace未回收**，工作负载停止不等于cleanup=true。
原失败和冻结计划不改写，未重跑测试；CNI/容量/跨节点/24h与signal窗口仍未资格，candidate=null。
[M26R5C](milestone-26-remediation-5-cleanup.md)实现 `7655f561a0e51d7ad9c4b620064d915ca40d5a50`：
共享清理按正式指标 API 语义及实际 get/list discovery 区分只读 PodMetrics，持久对象仍完整盘点；
真实原始 44 行清单回放及身份/foreign/输出/超时拒绝通过。quota 保留到最后完整检查，NS 最后删除。
一次独立清理窗口于 2026-09-28 08:36:23Z—08:38:32Z 完成，128.940秒；仅五个原登记 UID
各一次条件 DELETE，独立 GET 确认 namespace、Job、原 Pod 不存在，cleanup=true；44条CLI全部结束。
本次没有创建对象/负载或 SDK/ES/Go 重跑，200产品输入及1073/189/3120份原证据 hash 不变。
全部 Python 普通140通过，优化115通过/25既有skip；清理后仅在 `fa89628` 补回逐对象失败记录。原 M26R5 及原180秒恢复仍failed，后续回收不倒写原成功。
candidate=null、resource partial、CNI/容量/跨节点/完整平台/24h及启动signal窗口门槛保持。
[M27](milestone-27.md)实现 `e072d3b9f1ab23801ead183beb1cb786ec1b8fa6`：CLI在Open前统一注册信号，
startup随信号取消，取消后的Start不发布ready；全部成功组装退出路径保留独立5秒drain，部分失败仍用1秒清理context。
启动取消明确exit1，正常serving信号关闭无错误才exit0；脱敏错误、UNKNOWN/无重放及资源owner保持。
准确本地Darwin arm64 CGO0/CGO1 race与network-none原生Linux arm64 CGO0新产物的真实SIGTERM边界通过；
两个平台各固定10次首条监听即信号、两种握手各3次；在途UNKNOWN单次发送、默认30秒输入stall下5秒drain、重复Close回归通过。
全默认非缓存test/race、相关三轮race、普通/integration vet及六目标命令和integration编译/静态vet通过，待统筹独立验收。
Linux日志驱动启动前失败、额外app测试缺仓库示例输入的失败均保留；只纠正自身夹具，分别补首次执行/单失败case与原未运行server组，未重复整套求绿。
7个自有容器全部按准确ID回收，测试进程已Wait；没有EKS或push/CI/镜像更新。
旧公开GHCR仍source `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`；合成HTTP不构成真实DB资格，Windows/其他架构未原生运行。
本轮仅补局部生命周期证据，capacity candidate=null及所有其他required缺口不变，原M26R5失败不倒写。
状态只对列出的平台、版本、部署形态成立。`已实现` 不等于 `已真实资格验证`；
交叉编译、合成协议故障、本机真实后端、多机故障、长时运行分别记录。
没有任何未决项因为登记在这里就自动 PASS 或延期。

当前源码位置和历史路径映射见[代码导览](code-organization.md)；结构整理不改变下列资格状态。

M21状态更正：**M21R已获统筹有限独立验收**：三个归属根因修复、同小规格
原生三副本两worker功能通过；原M21失败保留。统筹外层全网络ID断言失败，默认bridge
变化时间早于首次fixture Docker修改，环境原因未证实，不能称全部检查通过。
统筹193mutation=180APPLIED/13UNKNOWN，187version1，9owner回收；CP events.max1429/OOM0
仍是压力而非余量。详情见[M21R](milestone-21-remediation.md)。
原[M22](milestone-22.md)**独立审查未通过**：最低50ops/s测量431/3000迟到丢弃、2569成功、candidate=null，确认/直连/过载恢复未运行。原失败及完整证据保留，不是Weir吞吐上限证据。
[M22R](milestone-22-remediation.md)的工具修复与有限发生器NO-GO调查已获统筹有限独立复核；不能称全部验收检查通过。统筹在最终main `16c7f35303123dde809ed768c76680e4bf5dcb3e` 首次CGO0默认测试遇到历史DNS夹具UDP/TCP同号端口碰撞，后续race通过不撤销该失败。这是测试夹具缺陷，没有产品resolver/后端回归证据。[M23](milestone-23.md)只修复完整socket获取、有限碰撞重试与失败清理，并补确定性回归；不改变产品DNS或容量门槛。

M22R执行者原生timing-only50档1000计划/143丢弃、wake p99=8.9ms仍保留。统筹最终入口六个20s原生计时探针中，修订bounded-50虽零丢弃，dispatch p99=8.3ms仍超过5ms：**generator_qualified=false、candidate=null**。200档短时通过不允许跳过最低档，不能据此解释Weir吞吐上限或断言VM/内核原因；合适native runner仍是后续容量工作的外部输入。完整ES/Weir容量阶梯、确认、直连、过载/恢复均not-run。工具修复、有限发生器调查和M23离线夹具回归都不构成容量、24h或其他native平台资格；其他矩阵门槛继续required/blocked。

## 目标来源与范围

用户经统筹聊天 `01a0da6a-11af-75c0-91e0-c07178d7c3cc` 于 2026-09-27 补充授权：
Weir 类似 Envoy/Traefik 的使用方式，支持多平台、多架构、单实例及云原生集群。
本节将统筹《Weir 生产资格目标与推进规则》的实质要求保留在仓库，验收不依赖本机聊天文件。

- 单一二进制、同步 App → Database 数据平面。独立运行不依赖 Kubernetes 或其他 Weir。
  多副本不是数据库复制层；不新增 Raft、leader、分布式锁、全局 same-key ordering、持久队列。
- Core 管执行语义，Adapter 管数据/表达式语义；每请求一个 Store/Service，流固定目标。
  未知 mutation 不重放；优先静态配置、普通 DNS、标准 LB 和滚动重启，不扩建控制平面。
- 初始目标为 Linux/macOS/Windows × amd64/arm64，Linux binary/OCI/Kubernetes 优先。
  每组合都需 native-run、conformance、资源/生命周期、至少 24 小时 soak 的独立证据。
  原生架构 VM 可用；跨架构模拟、交叉编译只能作补充。OS 最低版本与首个仍受支持的
  Kubernetes 次版本必须在打包阶段核实并冻结，不能声称所有版本。
- Kubernetes 必测 1/3 副本，至少两个 Linux worker node；本机三进程不代表多节点资格。
  覆盖 DNS、长连接负载分布、扩缩容、可信内网可达入口/必要后端连接、实际可达探针、滚动/drain/回滚、
  Pod/节点故障、后端必要连接配置更换及最大副本数下总 DB 连接/并发预算。禁止靠分布式配额系统绕开测量。
- 统筹负责参考负载的校准与门槛冻结，不等待用户提供业务 RPS 才推进。
  候选规格 2 vCPU/1 GiB，另测 4 vCPU/2 GiB；独立 load generator/DB，记录硬件网络拓扑。
  1 KiB、16 KiB、最大允许文档；读多、写多、热点、Bulk、Scan/Native 共存。
  对每后端作语义等价直连对照，测吞吐/延迟/资源曲线，再冻结 RPS、p95/p99、错误与恢复预算。
  独立验收不能降低流量/放宽阈值调绿；稳定回归超过 10% 必须解释、修复或由统筹明确接受。
- 冻结后每个平台/配置至少 24 小时：70% 可持续容量、受控 2 倍容量过载、慢消费者、故障恢复。
  不允许 OOM/死锁/泄漏/账本超额；warm-up 后 OS 内存、goroutine、句柄、连接须有稳定上界。
  Go heap、OS RSS/commit memory 和 cgroup 分别测量。当前短时本机测试不能代替这些门槛。
- 用户已明确回答“同意首版延期通用脚本（推荐）”。ProgramTransform 移出 V1 必需门槛，继续 UNSUPPORTED 并保留未来完整架构需求。[M13](milestone-13.md) 调查交付获独立验收、候选 NO-GO，不能写成 runtime 安全通过；BackendExpression/Native 是当前 V1 的转换与原生能力范围。其他六平台、后端、部署、资源、容量和 24h soak 门槛不变。
- 原阶段只准备和验证本地产物；M24另获明确授权正常push现有batchstream/weir、Actions构建并公开
  ghcr.io/batchstream/weir与ghcr.io/batchstream/weir-qualification。授权不含Git tag/Release、
  生产部署、已有秘密访问或收费资源；本阶段不执行EKS测试。
  缺 runner 是外部证据阻塞，先推进独立工作，确需机器/费用时提出最小需求。

用户随后明确缩减范围：**不新增 Weir 自身认证功能，按可信内网程序验收**。
网络隔离/访问边界由部署环境负责；新增 Weir TLS/mTLS、身份认证、Store/操作族授权、账户、
令牌、RBAC、证书管理不在范围，**不算生产阻断，也不写成已实现**。已有 peer mTLS/授权及证书辅助已在 [M8](milestone-8.md) 删除，application/peer/RemoteWeir
统一使用明文 HTTP/2；保留不同 hop 入口规则及共享资源预算。数据库要求的凭据/TLS 是必要标准
连接配置，不是新的 Weir 身份系统；不关闭标准证书验证，不读取已有秘密。协议/URI/文档校验、无隐式重放、UNKNOWN、
资源边界、runtime 隔离、背压/关闭/故障恢复仍必需；不外推该部署 profile 到无隔离公网。

## 状态定义

| 标记 | 含义 |
| --- | --- |
| 已实现 | 对应实现可定位；仍可能缺真实运行/故障/持续负载证据 |
| 已真实资格验证（有限范围） | 仅所列固定版本与场景实际执行；不外推平台、拓扑或安全能力 |
| 未验证 | 没有足够执行证据；不得写 PASS |
| 阻塞 | 已知实现/安全/环境缺口阻止该项宣称合格 |
| 用户已批准延期 | 明确移出首版范围，保留未来要求；不表示已实现或安全合格 |

## 功能、安全与运行验收

| 必需项 | 当前状态与证据 | 后续资格门槛/阻塞 |
| --- | --- | --- |
| CLI启动信号/取消/有界退出 | [M27](milestone-27.md)本地源码单次信号所有权、独立startup/drain、错误保留与真实Darwin/Linux arm64进程边界通过；固定10次监听即信号、握手每种3次，待独立验收；夹具失败与补证分别保留 | 合成HTTP非DB资格；旧GHCR未含修复，Windows/其他native平台、注册前/SIGKILL及整体生产资格不由此覆盖 |
| 原生 BackendExpression | 已实现；固定后端有限真实验证见 [M7](milestone-7.md)、`internal/backend/{mongodb,search}/expression.go`、对应 tests、`internal/server/expression_integration_test.go` | 仅明确 profile；不含泛化语言、跨记录、pipeline/upsert、其他版本/拓扑 |
| 通用 ProgramTransform | **用户已批准首版延期；未来实现仍阻塞**。`internal/protocol/protocol.go` 仍拒绝；[M1](milestone-1.md) 的 GopherLua 失败保留；[M13](milestone-13.md)、`experiments/goluaprobe` 实证 golua v0.3.0 编译低估、VM 分配先做后查/漏账、字符串/helper fuel 和取消缺口，NO-GO | 必须闭环确定性、编译/VM/helper/bridge 分配前计账与取消；极值/opaque userdata 透传不等于通用 typed transform。四种 action、安全输出、完整 codec、Mongo transaction/Search OCC 是未来恢复该功能的 required 门槛；首版不再规划 VM fork 或通用 RMW 接线 |
| Weir TLS/认证/授权 | **用户明确排除，不算阻断；既有体系已移除**；[M8](milestone-8.md)、`internal/server/forwarding.go` | application/peer 均为明文 HTTP/2，入口可达来源由部署隔离负责；hop 没有密码学身份保证。旧 identity/allow/server_name 配置严格拒绝，无开关、证书 fixture 或空壳权限接口 |
| Mongo 后端连接及隐式重放 | credential-free 与显式 SCRAM-SHA-256/verified TLS 已实现且有限真实验证；生产 Open/应用装配、解密后 wire guard、391/回复丢失与生命周期实测见 [M10R](milestone-10-remediation.md)；M10 原失败保留 | 固定 Go1.27/driver2.9.1/Mongo8.0.32 单直接非分片副本集 endpoint；仅有界单 HTTP OCSP responder。其他认证、HTTPS OCSP/多个 responder、SRV/多节点、跨平台/长测仍未通过 |
| Search 后端连接及隐式重放 | 单静态 DNS/IP endpoint 的 HTTP 或 verified HTTPS/可选 Basic；[M17](milestone-17.md)已获统筹有限独立验收：ES8.19.22/OS2.19.6 原生语义、app direct/peer、故障请求计数、DNS/TLS/Close及M12R多进程预算 | 旧profile明确拒绝；单具体index/primary、显式CA的有限证据。[M18](milestone-18.md)排除当前入口的部分SNI/SPDY/CRL条件，但JDK危险API及部分插件路径仍无法判定，OS安全门槛blocked；唯一官方3.8.0候选不足以直接替代。系统roots正向、其他拓扑、远端无限故障与长测未资格 |
| 普通 Read/写入/Bulk | 已实现且有限真实资格验证；[M1](milestone-1.md)、[M2](milestone-2.md)、[unary 专项](unary-response-deadline.md) | 新平台/适用连接配置/网络故障需重验；0 假 APPLIED、0 已执行却 NOT_STARTED、0 静默重放、0 关联/流内顺序错误 |
| Native/Scan 与 peer 生命周期 | 已实现并本机 direct/两跳、真实后端验证；[M3](milestone-3.md)、[M4](milestone-4.md)、[M5](milestone-5.md)、[M6](milestone-6.md) | M8 已补明文 preface/header/partial-frame 和连接 setup 期限；后续环境重验 END/EOF、部分结果、背压、取消、drain、句柄/连接/游标泄漏；流不可迁移 |
| 多实例共享后端/直接 native writer | `local.concurrency` 已实现；原 [M12](milestone-12.md) 失败保留。[M12R](milestone-12-remediation.md) 在 `383b4aa` 已获统筹有限独立验收：Mongo 本地 dial/raw/closing C+1 owner、分层 remote-tail、replacement 两 Local 全生命周期；本机 Mongo/ES/OS 真实预算回归通过 | 本地源码硬界与真实回收条件分开；replicas×pool 不是 DB accepted/远端工作的无条件硬上限。原M21失败保留；M21R同小规格生命周期已获统筹有限独立验收，外层网络断言失败保留；跨物理主机、复制切换、参考容量/soak仍未通过 |
| 复制/故障切换/网络 UNKNOWN | 实际确认后丢回复及协议故障已验证；M1/M2/M5/M7 | 单成员 Mongo、单 primary 零 replica Search 不代表复制切换资格。后端 primary 切换/节点失联/跨机网络仍未验证；M9 的 DNS 变化证据仅限自有 loopback fixture；不新增猜测性重放 |
| Linux RSS/cgroup | [M14](milestone-14.md) 原交付因身份误判/失败清理未验收；[M14R](milestone-14-remediation.md) 已获统筹有限独立验收：修复相关语义身份与瞬时 unknown 恢复，保留单 Guard/每层配对/RSS/config/固定观测，Linux arm64 Guard/CLI 三轮通过；M19R再做Guard三轮及准确归档应用观测 | 单可见有限 cgroup；无有限界/祖先拓扑仅有文件决策测试。Linux Mongo 被 kernel 7.0.12 阻塞，真实 CLI 使用自有 macOS Mongo；Windows Go fallback 不算 OS 内存资格；Darwin 补救见下一行 |
| macOS 当前 OS 内存 | [M19R](milestone-19-remediation.md) 实现固定 purego v0.10.2 / 系统libproc当前physical footprint；单Guard、无效闭锁/低位恢复；CGO0与CGO1 race分别三轮Guard/app原生短测；统筹已有限独立验收 main6573fd0/source484e4bd | 统筹明确接受上游维护内部fakecgo的依赖边界；Weir无私有ABI fork。只有限Darwin arm64证据，amd64未native；同步内核调用不可硬取消、预算非OS硬限额。原[M19](milestone-19.md)未实现历史保留 |
| 可复现打包/供应链 | [M15](milestone-15.md)、[M16](milestone-16.md)、[M17](milestone-17.md)已获有限独立验收；Go1.27.1/grpc1.83.2、六目标/双OCI、标准CycloneDX及冻结库扫描、准确制品回归证据保留 | [M18](milestone-18.md)逐项登记28个OS High/Critical公告的已知条件与缺口，未消除blocked或更改活跃profile；原始匹配未抑制。ES外部CDX两处SPDX enum失败由统筹复现，Weir8份CDX通过；供应链表示失败与运行风险分开。unsigned政策不等于发布签名，独立安全复核仍required |
| DNS/端点与 gRPC LB | 有界静态 1–8 endpoint/Service、普通 Go DNS、标准 gRPC pick-first、URI/request-ID rendezvous 已实现；本机真实 DNS、三执行进程共享真实后端、连接替换/无重放证据见 [M9](milestone-9.md) | 静态成员通过重启；旧 stream 不再平衡。[M21](milestone-21.md)补单VM两worker上Service长连接、有限新连接与1→3扩容观察（M21R已有限独立验收，外层网络断言失败保留）；生产DNS/跨物理机、缩容可用性、参考容量仍未资格；本地owner界不冒充DB远端硬界 |
| Kubernetes 部署/探针/滚动 | [M20](milestone-20.md)已获统筹有限独立验收：准确source315819fc、原生arm64/K8s1.36.4单副本2CPU/1GiB、探针/Service/DB停顿/活动替换；关闭实验明确stall3s。原[M21](milestone-21.md)失败保留；[M21R](milestone-21-remediation.md)记录3副本/2worker、C2→C1→C2不可变静态配置、默认stall30s关闭、短时worker暂停/恢复与无重放已获统筹有限独立验收，外层网络断言失败保留 | M21为每Pod0.5CPU/384MiB、256MiB进程预算的单VM correctness smoke；不代表三副本参考2CPU1GiB或4CPU2GiB容量、跨物理主机失联、永久节点丢失重调度、跨版本升级或24h |
| 持续负载/SLO/过载恢复/soak | 有短时 bounded regression；无生产容量声明 | 统筹校准并冻结上述参考负载门槛；每平台至少 24h 独立验收。尚未测量的 RPS/p99/恢复时限均未验证 |
| 运维/升级/回滚 | README 与 M1–M9 有本机操作、限制、故障语义；`internal/app` 有进程生命周期测试 | [M21](milestone-21.md)补静态配置滚动/回滚、最大存活Local计账、Service连接及UNKNOWN操作说明；同image不同并发配置不证明跨版本或后端凭据/TLS更换兼容；参考容量、完整崩溃/灾难恢复仍required |

## 平台证据矩阵

每格都需要记录 OS/内核/架构/CPU、Go、依赖、backend 精确版本及日志。
M14R、M15、M16、M17、M18 已获有限独立验收；M18 仅为风险调查，OpenSearch 安全门槛仍阻塞。M17六目标归档/双OCI再次复现；准确Darwin arm64归档与
Linux arm64产品image分别连接ES8.19.22/OS2.19.6 verified HTTPS，各三轮有限业务运行。
Linux使用7.0.12-linuxkit/aarch64/cgroup-v2；实际Search后端也为native Linux arm64。
M16准确image连接自有Darwin Mongo8.0.32的历史证据不冒充新M17 image重新跑过Mongo；
本轮Mongo仅有界TLS/丢ACK冒烟。Linux Mongo资格仍阻塞。
编译器下限（macOS13+、Linux3.2+、Windows10/Server2016+）与实际测试版本分开，
历史见 [M15](milestone-15.md)/[M16](milestone-16.md)，最新制品及安全阻断见
[M17](milestone-17.md)，当前组件风险处置见[M18](milestone-18.md)。M18的arm64加载快照和候选离线扫描不增加任何native矩阵资格。以下未验证不是放弃平台；所有平台都未达到整体qualified。

| 平台/架构 | build / reproducible | native-run/conformance | resource/lifecycle | ≥24h soak | 整体 qualified / 阻塞 |
| --- | --- | --- | --- | --- | --- |
| Linux amd64 | M24六目标binary/archive双次一致，两个Linux OCI公开发布；M17历史保留 | M24原生默认CGO0/race/vet与准确image `-version` 通过；后端conformance未验证，待统筹独立验收 | 未验证 | 未验证 | 否；仅原生离线测试和制品短启动，后端/资源/容量/24h仍未通过 |
| Linux arm64 | M24六目标binary/archive双次一致，两个Linux OCI公开发布；M20历史保留 | M24新增原生默认测试和准确image `-version`；M20旧准确image/K8s1.36.4/ES8.19.22单副本已获有限独立验收；原M21失败保留；M21R旧image有限功能已独立验收，外层网络断言失败保留；M17历史保留 | M20参考单Pod规格已验收（stall3s）；M21小规格0.5CPU/384MiB、256MiB预算/default stall30s、5s关闭与有限资源回收已有限独立验收；M14R/M19R历史保留 | 未验证 | 否；M24未跑新image后端/EKS，M21R仅有限多worker功能获独立验收，参考三副本容量/跨机/24h未验证；OS安全与Linux Mongo内核阻塞 |
| macOS amd64 | M24六目标双次编译/归档一致；M19R历史保留、仅Darwin链接purego | 未验证 | 未验证 | 未验证 | 否；native runner待接入 |
| macOS arm64 | M24六目标双次编译/归档一致；M19R历史保留 | M19R旧准确归档TLS Mongo读写/UNKNOWN/取消/关闭；M17两Search历史资格保留 | M19R CGO0/race分列三轮footprint/迟滞/恢复，应用取消/Close；准确归档正常及在途SIGTERM通过；强压仅app test构建 | 未验证 | 否；只有本机短时资源证据，完整生产范围和长测未资格 |
| Windows amd64 | M24六目标双次编译/归档一致；M17历史保留 | 未验证 | 关闭/句柄/commit memory未验证 | 未验证 | 否；native runner待接入 |
| Windows arm64 | M24六目标双次编译/归档一致；M17历史保留 | 未验证 | 未验证 | 未验证 | 否；native runner待接入 |

后续串行阶段由统筹安排：M9 已完成有限静态端点/DNS 的本机资格，M8 已移除 Weir 认证并保留 hop/校验/限额/UNKNOWN；M10 原阶段未通过，M10R 修复 Mongo TLS 与有界 wire reader 层次并补有限连接证据；M11 仅补 Search 标准后端连接与有限无重放证据；M12 原预算失败保留，M12R 已通过本机有限独立验收。M13 固定候选调查独立验收为 NO-GO；用户随后明确批准 ProgramTransform 首版延期，继续 UNSUPPORTED，未来要求保留。M14 仅补 Linux 内存 profile 与有限原生运行证据，原失败和 [M14R 补救](milestone-14-remediation.md) 分别保留；无关挂载不再改变静态身份，只有完整可信的相关变化才永久闭锁。M15 本地可复现打包和有限准确产物运行已获独立验收。M16已获标准SBOM、冻结库扫描及必要补丁的有限独立验收；M17更新到ES8.19.22/OS2.19.6，功能/制品已获统筹有限独立验收，安全目标未闭环。M18有限调查及一个官方3.8.0候选比较已获统筹独立验收；部分入口条件已排除，JDK/插件剩余证据及最小官方上游路线明确，OpenSearch安全门槛仍blocked，活跃profile不变。M19调查历史保留；统筹明确允许固定purego上游内部机制后，M19R已接线当前footprint，并单列CGO0与CGO1有限原生证据，见补救报告；这不是整体资格。OS bundled JDK/插件安全阻断、其他平台原生运行、完整部署矩阵、跨物理机Kubernetes/版本升级、真实发布签名、独立安全审查、参考负载校准/长测仍 required。
可按实际依赖拆分调整，只有一个 checkout 写入者。发现正确性/安全回归先修复。
所有必需项没有已知 P0/P1、对应矩阵证据齐备、独立验收通过、main 干净且自有资源回收后，
才能宣布**对应范围**合格；部分平台通过不能结束整个目标，缩小范围须用户确认。
