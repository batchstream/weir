# Weir 生产资格验收清单

日期：2026-09-27。本文件是**必需验收目标及证据缺口清单，不是生产就绪声明**。
状态只对列出的平台、版本、部署形态成立。`已实现` 不等于 `已真实资格验证`；
交叉编译、合成协议故障、本机真实后端、多机故障、长时运行分别记录。
没有任何未决项因为登记在这里就自动 PASS 或延期。

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
- ProgramTransform 未豁免。后续必须做有限安全可行性阶段；不合格继续 UNSUPPORTED。
  若要排除首版范围，必须明确说明并取得用户范围确认，不能将表达式/内部计数器当作替代。
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
| 用户确认后才可延期 | 必需能力；当前没有豁免授权 |

## 功能、安全与运行验收

| 必需项 | 当前状态与证据 | 后续资格门槛/阻塞 |
| --- | --- | --- |
| 原生 BackendExpression | 已实现；固定后端有限真实验证见 [M7](milestone-7.md)、`internal/{mongo,search}store/expression.go`、对应 tests、`internal/server/expression_integration_test.go` | 仅明确 profile；不含泛化语言、跨记录、pipeline/upsert、其他版本/拓扑 |
| 通用 ProgramTransform | **阻塞；用户确认后才可延期**。`internal/protocol/protocol.go` 拒绝；`internal/luaprobe/runtime_test.go`、[M1](milestone-1.md) 证明 GopherLua 隔离不合格 | 确定性、fuel/CPU、编译/分配/宿主 helper/取消隔离、类型保真；Mongo transaction 与 Search OCC 完整闭环 |
| Weir TLS/认证/授权 | **用户明确排除，不算阻断；既有体系已移除**；[M8](milestone-8.md)、`internal/server/peer.go` | application/peer 均为明文 HTTP/2，入口可达来源由部署隔离负责；hop 没有密码学身份保证。旧 identity/allow/server_name 配置严格拒绝，无开关、证书 fixture 或空壳权限接口 |
| 后端必要连接配置及隐式重放 | 当前 Search credential-free loopback HTTP；Mongo Native/表达式拒绝 driver Auth。`internal/{mongo,search}store/{adapter,expression}.go`、Search `transport.go` | 选定 backend profile 所需标准 TLS/凭据连接仍未验证；审计适用重认证/连接恢复/HTTP 重试，不构建 Weir 身份系统，不跳过标准证书验证，不读取已有秘密 |
| 普通 Read/写入/Bulk | 已实现且有限真实资格验证；[M1](milestone-1.md)、[M2](milestone-2.md)、[unary 专项](unary-response-deadline.md) | 新平台/适用连接配置/网络故障需重验；0 假 APPLIED、0 已执行却 NOT_STARTED、0 静默重放、0 关联/流内顺序错误 |
| Native/Scan 与 peer 生命周期 | 已实现并本机 direct/两跳、真实后端验证；[M3](milestone-3.md)、[M4](milestone-4.md)、[M5](milestone-5.md)、[M6](milestone-6.md) | M8 已补明文 preface/header/partial-frame 和连接 setup 期限；后续环境重验 END/EOF、部分结果、背压、取消、drain、句柄/连接/游标泄漏；流不可迁移 |
| 多实例共享后端/直接 native writer | 原生 writer 有限竞争验证；本机多进程 peer 有限验证；M1/M2/M5/M7 | 多个独立 Runtime/AIMD 同目标的容量竞争、max replicas × pool/C 总预算未资格；每实例控制不代表全局上限 |
| 复制/故障切换/网络 UNKNOWN | 实际确认后丢回复及协议故障已验证；M1/M2/M5/M7 | 单成员 Mongo、单 primary 零 replica Search 不代表复制切换资格。后端 primary 切换/节点失联/跨机网络、DNS 变化仍未验证；不新增猜测性重放 |
| Linux RSS/cgroup | `internal/overload/guard.go` 有实现；既有交叉编译证据 | **未真实验证** RSS/cgroup-v2 与容器压力/回收；macOS 当前 Go Sys-HeapReleased 降级信号不是 RSS |
| 可复现打包/供应链 | 固定 `go.mod/go.sum`、协议生成和本地 bootstrap；默认离线测试 | 六组合制品、双架构 OCI、校验和/SBOM/依赖安全扫描、固定版本安全复核与可复现构建未验收；固定版本不是永久安全承诺 |
| DNS/端点与 gRPC LB | 当前静态固定 IP 端点与有界明文 peer；显式内网/通配 IP 绑定，默认 loopback；本机恢复见 M8 | 生产 DNS/有界解析/标准 resolver/balancer、长连接新调用分布、扩缩容与重连仍阻塞；不能假设旧 stream 自动再平衡 |
| Kubernetes 部署/探针/滚动 | **未实现/未验证**；M7 未执行此未来阶段 | 规范 Deployment/Service、requests/limits、non-root/read-only、Secret 引用；1/3 replicas、两 worker；probe 实际可达，loopback diagnostics 不可直接当 Pod-IP HTTP probe |
| 持续负载/SLO/过载恢复/soak | 有短时 bounded regression；无生产容量声明 | 统筹校准并冻结上述参考负载门槛；每平台至少 24h 独立验收。尚未测量的 RPS/p99/恢复时限均未验证 |
| 运维/升级/回滚 | README 与 M1–M8 有本机操作、限制、故障语义；`internal/app` 有进程生命周期测试 | 容量规划、UNKNOWN 处理、版本兼容、配置/后端连接替换、滚动升级/回滚演练及崩溃后恢复说明未完成 |

## 平台证据矩阵

每格都需要记录 OS/内核/架构/CPU、Go、依赖、backend 精确版本及日志。
本阶段实际执行平台是 macOS arm64、Go 1.27.0；后端 ES/OS 容器运行于 Linux arm64
并不构成 Weir Linux 进程资格。以下“未验证”不是自动放弃该平台。

| 平台/架构 | build | native-run/conformance | resource/lifecycle | ≥24h soak | 整体 qualified / 阻塞 |
| --- | --- | --- | --- | --- | --- |
| Linux amd64 | 既有交叉编译仅作证据；发布构建未验收 | 未验证 | RSS/cgroup/信号未验证 | 未验证 | 否；native runner/冻结环境待接入核实 |
| Linux arm64 | 发布构建未验收 | 未验证 | 未验证 | 未验证 | 否；native runner/OCI/K8s 环境待接入核实 |
| macOS amd64 | 未验证 | 未验证 | 未验证 | 未验证 | 否；原生 runner 待接入核实 |
| macOS arm64 | 本机 go test/build 已验证 | M1–M8 有限本机测试 | 有限取消/drain/ledger；OS 内存资格不足 | 未验证 | 否；生产范围与长测仍未资格 |
| Windows amd64 | 未验证 | 未验证 | 关闭、句柄、commit memory 未验证 | 未验证 | 否；原生 runner 待接入核实 |
| Windows arm64 | 未验证 | 未验证 | 未验证 | 未验证 | 否；原生 runner 待接入核实 |

后续串行阶段由统筹安排：有界 DNS/端点与部署入口资格（M8 已移除 Weir 认证并保留 hop/校验/限额/UNKNOWN）→ 后端必要连接/无重放及多实例保护 → 通用 runtime
安全可行性 → 跨平台运行/打包 → OCI/Kubernetes → 参考负载校准/长测/独立核验。
可按实际依赖拆分调整，只有一个 checkout 写入者。发现正确性/安全回归先修复。
所有必需项没有已知 P0/P1、对应矩阵证据齐备、独立验收通过、main 干净且自有资源回收后，
才能宣布**对应范围**合格；部分平台通过不能结束整个目标，缩小范围须用户确认。
