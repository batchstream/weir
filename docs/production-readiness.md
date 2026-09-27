# Weir 生产资格验收清单

日期：2026-09-28。本文件是**必需验收目标及证据缺口清单，不是生产就绪声明**。
状态只对列出的平台、版本、部署形态成立。`已实现` 不等于 `已真实资格验证`；
交叉编译、合成协议故障、本机真实后端、多机故障、长时运行分别记录。
没有任何未决项因为登记在这里就自动 PASS 或延期。

当前源码位置和历史路径映射见[代码导览](code-organization.md)；结构整理不改变下列资格状态。

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
- 只准备和验证本地产物；当前授权不含 push、发布、生产部署、已有秘密访问或收费资源。
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
| 原生 BackendExpression | 已实现；固定后端有限真实验证见 [M7](milestone-7.md)、`internal/backend/{mongodb,search}/expression.go`、对应 tests、`internal/server/expression_integration_test.go` | 仅明确 profile；不含泛化语言、跨记录、pipeline/upsert、其他版本/拓扑 |
| 通用 ProgramTransform | **用户已批准首版延期；未来实现仍阻塞**。`internal/protocol/protocol.go` 仍拒绝；[M1](milestone-1.md) 的 GopherLua 失败保留；[M13](milestone-13.md)、`experiments/goluaprobe` 实证 golua v0.3.0 编译低估、VM 分配先做后查/漏账、字符串/helper fuel 和取消缺口，NO-GO | 必须闭环确定性、编译/VM/helper/bridge 分配前计账与取消；极值/opaque userdata 透传不等于通用 typed transform。四种 action、安全输出、完整 codec、Mongo transaction/Search OCC 是未来恢复该功能的 required 门槛；首版不再规划 VM fork 或通用 RMW 接线 |
| Weir TLS/认证/授权 | **用户明确排除，不算阻断；既有体系已移除**；[M8](milestone-8.md)、`internal/server/forwarding.go` | application/peer 均为明文 HTTP/2，入口可达来源由部署隔离负责；hop 没有密码学身份保证。旧 identity/allow/server_name 配置严格拒绝，无开关、证书 fixture 或空壳权限接口 |
| Mongo 后端连接及隐式重放 | credential-free 与显式 SCRAM-SHA-256/verified TLS 已实现且有限真实验证；生产 Open/应用装配、解密后 wire guard、391/回复丢失与生命周期实测见 [M10R](milestone-10-remediation.md)；M10 原失败保留 | 固定 Go1.27/driver2.9.1/Mongo8.0.32 单直接非分片副本集 endpoint；仅有界单 HTTP OCSP responder。其他认证、HTTPS OCSP/多个 responder、SRV/多节点、跨平台/长测仍未通过 |
| Search 后端连接及隐式重放 | 单静态 DNS/IP endpoint 的 HTTP 或 verified HTTPS/可选 Basic；[M17](milestone-17.md)已获统筹有限独立验收：ES8.19.22/OS2.19.6 原生语义、app direct/peer、故障请求计数、DNS/TLS/Close及M12R多进程预算 | 旧profile明确拒绝；单具体index/primary、显式CA的有限证据。[M18](milestone-18.md)排除当前入口的部分SNI/SPDY/CRL条件，但JDK危险API及部分插件路径仍无法判定，OS安全门槛blocked；唯一官方3.8.0候选不足以直接替代。系统roots正向、其他拓扑、远端无限故障与长测未资格 |
| 普通 Read/写入/Bulk | 已实现且有限真实资格验证；[M1](milestone-1.md)、[M2](milestone-2.md)、[unary 专项](unary-response-deadline.md) | 新平台/适用连接配置/网络故障需重验；0 假 APPLIED、0 已执行却 NOT_STARTED、0 静默重放、0 关联/流内顺序错误 |
| Native/Scan 与 peer 生命周期 | 已实现并本机 direct/两跳、真实后端验证；[M3](milestone-3.md)、[M4](milestone-4.md)、[M5](milestone-5.md)、[M6](milestone-6.md) | M8 已补明文 preface/header/partial-frame 和连接 setup 期限；后续环境重验 END/EOF、部分结果、背压、取消、drain、句柄/连接/游标泄漏；流不可迁移 |
| 多实例共享后端/直接 native writer | `local.concurrency` 已实现；原 [M12](milestone-12.md) 失败保留。[M12R](milestone-12-remediation.md) 在 `383b4aa` 已获统筹有限独立验收：Mongo 本地 dial/raw/closing C+1 owner、分层 remote-tail、replacement 两 Local 全生命周期；本机 Mongo/ES/OS 真实预算回归通过 | 本地源码硬界与真实回收条件分开；replicas×pool 不是 DB accepted/远端工作的无条件硬上限。M21已补单VM内两worker/三副本的小规格生命周期执行者证据，待统筹验收；跨物理主机、复制切换、参考容量/soak仍未通过 |
| 复制/故障切换/网络 UNKNOWN | 实际确认后丢回复及协议故障已验证；M1/M2/M5/M7 | 单成员 Mongo、单 primary 零 replica Search 不代表复制切换资格。后端 primary 切换/节点失联/跨机网络仍未验证；M9 的 DNS 变化证据仅限自有 loopback fixture；不新增猜测性重放 |
| Linux RSS/cgroup | [M14](milestone-14.md) 原交付因身份误判/失败清理未验收；[M14R](milestone-14-remediation.md) 已获统筹有限独立验收：修复相关语义身份与瞬时 unknown 恢复，保留单 Guard/每层配对/RSS/config/固定观测，Linux arm64 Guard/CLI 三轮通过；M19R再做Guard三轮及准确归档应用观测 | 单可见有限 cgroup；无有限界/祖先拓扑仅有文件决策测试。Linux Mongo 被 kernel 7.0.12 阻塞，真实 CLI 使用自有 macOS Mongo；Windows Go fallback 不算 OS 内存资格；Darwin 补救见下一行 |
| macOS 当前 OS 内存 | [M19R](milestone-19-remediation.md) 实现固定 purego v0.10.2 / 系统libproc当前physical footprint；单Guard、无效闭锁/低位恢复；CGO0与CGO1 race分别三轮Guard/app原生短测；统筹已有限独立验收 main6573fd0/source484e4bd | 统筹明确接受上游维护内部fakecgo的依赖边界；Weir无私有ABI fork。只有限Darwin arm64证据，amd64未native；同步内核调用不可硬取消、预算非OS硬限额。原[M19](milestone-19.md)未实现历史保留 |
| 可复现打包/供应链 | [M15](milestone-15.md)、[M16](milestone-16.md)、[M17](milestone-17.md)已获有限独立验收；Go1.27.1/grpc1.83.2、六目标/双OCI、标准CycloneDX及冻结库扫描、准确制品回归证据保留 | [M18](milestone-18.md)逐项登记28个OS High/Critical公告的已知条件与缺口，未消除blocked或更改活跃profile；原始匹配未抑制。ES外部CDX两处SPDX enum失败由统筹复现，Weir8份CDX通过；供应链表示失败与运行风险分开。unsigned政策不等于发布签名，独立安全复核仍required |
| DNS/端点与 gRPC LB | 有界静态 1–8 endpoint/Service、普通 Go DNS、标准 gRPC pick-first、URI/request-ID rendezvous 已实现；本机真实 DNS、三执行进程共享真实后端、连接替换/无重放证据见 [M9](milestone-9.md) | 静态成员通过重启；旧 stream 不再平衡。[M21](milestone-21.md)补单VM两worker上Service长连接、有限新连接与1→3扩容观察（执行者证据，待统筹验收）；生产DNS/跨物理机、缩容可用性、参考容量仍未资格；本地owner界不冒充DB远端硬界 |
| Kubernetes 部署/探针/滚动 | [M20](milestone-20.md)已获统筹有限独立验收：准确source315819fc、原生arm64/K8s1.36.4单副本2CPU/1GiB、探针/Service/DB停顿/活动替换；关闭实验明确stall3s。[M21](milestone-21.md)仅记录3副本/2worker、C2→C1→C2不可变静态配置、默认stall30s关闭、短时worker暂停/恢复与无重放的执行者证据，待统筹验收 | M21为每Pod0.5CPU/384MiB、256MiB进程预算的单VM correctness smoke；不代表三副本参考2CPU1GiB或4CPU2GiB容量、跨物理主机失联、永久节点丢失重调度、跨版本升级或24h |
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
| Linux amd64 | M17 六目标双次 hash 一致 | 未验证 | 未验证 | 未验证 | 否；native runner/冻结环境待接入 |
| Linux arm64 | M20六目标binary/archive/双OCI双次一致 | M20准确image/K8s1.36.4/ES8.19.22单副本已获有限独立验收；M21同准确image三副本两worker Service/配置滚动/暂停恢复执行者证据待验收；M17历史保留 | M20参考单Pod规格已验收（stall3s）；M21小规格0.5CPU/384MiB、256MiB预算/default stall30s、5s关闭与有限资源回收待统筹验收；M14R/M19R历史保留 | 未验证 | 否；M21仅有限多worker执行者证据待验收，参考三副本容量/跨机/24h未验证；OS安全与Linux Mongo内核阻塞 |
| macOS amd64 | M19R CGO0双次归档一致、仅Darwin链接purego | 未验证 | 未验证 | 未验证 | 否；native runner待接入 |
| macOS arm64 | M19R CGO0双次归档一致 | M19R准确归档TLS Mongo读写/UNKNOWN/取消/关闭；M17两Search历史资格保留 | M19R CGO0/race分列三轮footprint/迟滞/恢复，应用取消/Close；准确归档正常及在途SIGTERM通过；强压仅app test构建 | 未验证 | 否；只有本机短时资源证据，完整生产范围和长测未资格 |
| Windows amd64 | M17 六目标双次 hash 一致 | 未验证 | 关闭/句柄/commit memory未验证 | 未验证 | 否；native runner待接入 |
| Windows arm64 | M17 六目标双次 hash 一致 | 未验证 | 未验证 | 未验证 | 否；native runner待接入 |

后续串行阶段由统筹安排：M9 已完成有限静态端点/DNS 的本机资格，M8 已移除 Weir 认证并保留 hop/校验/限额/UNKNOWN；M10 原阶段未通过，M10R 修复 Mongo TLS 与有界 wire reader 层次并补有限连接证据；M11 仅补 Search 标准后端连接与有限无重放证据；M12 原预算失败保留，M12R 已通过本机有限独立验收。M13 固定候选调查独立验收为 NO-GO；用户随后明确批准 ProgramTransform 首版延期，继续 UNSUPPORTED，未来要求保留。M14 仅补 Linux 内存 profile 与有限原生运行证据，原失败和 [M14R 补救](milestone-14-remediation.md) 分别保留；无关挂载不再改变静态身份，只有完整可信的相关变化才永久闭锁。M15 本地可复现打包和有限准确产物运行已获独立验收。M16已获标准SBOM、冻结库扫描及必要补丁的有限独立验收；M17更新到ES8.19.22/OS2.19.6，功能/制品已获统筹有限独立验收，安全目标未闭环。M18有限调查及一个官方3.8.0候选比较已获统筹独立验收；部分入口条件已排除，JDK/插件剩余证据及最小官方上游路线明确，OpenSearch安全门槛仍blocked，活跃profile不变。M19调查历史保留；统筹明确允许固定purego上游内部机制后，M19R已接线当前footprint，并单列CGO0与CGO1有限原生证据，见补救报告；这不是整体资格。OS bundled JDK/插件安全阻断、其他平台原生运行、完整部署矩阵、跨物理机Kubernetes/版本升级、真实发布签名、独立安全审查、参考负载校准/长测仍 required。
可按实际依赖拆分调整，只有一个 checkout 写入者。发现正确性/安全回归先修复。
所有必需项没有已知 P0/P1、对应矩阵证据齐备、独立验收通过、main 干净且自有资源回收后，
才能宣布**对应范围**合格；部分平台通过不能结束整个目标，缩小范围须用户确认。
