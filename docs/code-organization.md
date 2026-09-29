# 当前代码导览

这是当前实现的导航，不是目标架构或生产资格声明。设计契约见
[双语架构](architecture.zh-CN.md)，本次整理和回归见
[结构整理报告](code-organization-review.md)。一个仓库、一个 Go module；
公共客户端仍导入 `github.com/batchstream/weir/api/weir/v1`。

## 包职责和依赖

| 位置 | 真实职责与所有权 |
| --- | --- |
| `cmd/weir` | CLI flags/JSON 输入、无配置访问的构建身份/loopback exec探针、进程信号、调用 Node 的启动和关闭 |
| `scripts/package.py`、`packaging/` | 干净提交导出、六目标双次构建/归档/依赖清单和本地 OCI；不进入运行时依赖图 |
| `api/weir/v1`、`api/weir/search/v1` | 公共 proto 和生成代码；独立于服务端内部包 |
| `internal/app` | 严格静态配置、完整图校验、具体后端/Service/listener 组装、进程生命周期和 diagnostics |
| `internal/server` | 应用/peer HTTP/2 和 RPC、精确路由、固定 Local/Remote 选择、转发和响应寿命 |
| `internal/store` | 单 Store 的 pending/result/session 账本、调度、微批、AIMD、Adapter drain/Close |
| `internal/execution` | Adapter 的小型多实现契约、opaque Plan、Feedback、ScanPage、Native source/sink |
| `internal/backend/mongodb` | Mongo URI/连接/TLS/OCSP/wire guard、后端资格、CRUD、BSON codec、表达式、事务型 Lua RMW、Native、Scan |
| `internal/backend/search` | ES/OpenSearch 的明确 profile、HTTP/TLS 连接、JSON/响应边界、CRUD/OCC、条件式 Lua RMW、表达式、Native、PIT Scan |
| `internal/protocol` | 规范资源 URI、公共 framing/媒体格式/操作校验、Failure/outcome 构造、`lua.v1` 信封校验；不解释后端文档 |
| `internal/value` | 有界 ordered typed value、Mongo BSON codec 与保留原始数值词法的 JSON codec；无存储访问职责 |
| `internal/luaworker`、`internal/luaengine` | 父进程 bounded runner/IPC 与独立 worker Lua VM；仅后者链接 Lua 引擎 |
| `internal/overload` | 一个 Guard；Linux RSS/config 与可见 cgroup-v2 每层 current/max 独立滞回、静态 profile 校验、固定 Snapshot；Darwin 当前 physical footprint；Windows 明确 Go 降级 |
| `internal/netlimit` | peer、Mongo 与 Search 实际复用的标准 Go DNS 有界 I/O；调用方保留并发、地址选择和生命周期 |
| `internal/testutil` | 仓库资源定位；子包 testmongo/testsearch/testdns/testmetrics 为自有测试设施；testmemory 仅 integration Darwin mmap/SDK oracle |
| `experiments/luaprobe` | 历史 Lua 可行性探针；生产 runtime 不再依赖此目录 |
| `experiments/goluaprobe` | 固定 golua v0.3.0 的 test-only 源码/资源反例与最小 typed Value 传递；编译、VM 分配、helper fuel/取消初筛失败，不是产品 runtime；证据见 [M13](milestone-13.md) |

实际生产依赖方向（省略标准库、第三方库和公共 pb）：

```text
cmd/weir -> app -> server -> store -> execution
                -> backend/mongodb -> execution, protocol, value
                -> backend/search  -> execution, protocol
                -> overload
server, store -> protocol
server, backend/mongodb, backend/search -> netlimit
protocol, execution -> api/weir/v1
```

`execution` 不导入 `store`，两个后端也不导入 `store`、`app`、`server`。
`server` 和 `store` 都不导入 BSON/JSON 或具体后端。
`app` 是具体依赖的唯一服务端组装位置；示例客户端可使用后端公开的 descriptor 常量。
指标保留在其资源所有者所在包，没有独立全局 registry 或 observability 管理层。

## 从哪里开始读

| 路径 | 入口文件/符号 |
| --- | --- |
| 配置到实例 | `app/config.go`: Decode、Config.Validate、Local.runtimeLimits；`app/assembly.go`: Open、openLocal |
| 进程内存 | `overload/guard.go`: 单采样和状态；`proc.go`: 可移植 Linux 文件解析；`memory_darwin.go`: 固定 libproc 绑定和 footprint 校验；`process_other.go`: 非 Darwin fallback |
| 启动/关闭 | `app/node.go`: Node、Start、Close、listenerEnded；`app/diagnostics.go`: 有界 HTTP/1 探针；`app/metrics.go`: 节点采集和注册 |
| listener 到 handler | `server/server.go`: Config/New；`listener.go`: Serve/Shutdown、连接额度；`admission.go`: 共享应用/peer 准入 |
| 协议与路由 | `server/http_transport.go`: delivery、输入 credit、HTTP/2 deadline；`forwarding.go`: hop/诊断 metadata；`service.go`: Service/resolve |
| 五个 RPC | `server/unary.go`: Read/Mutate/single；`bulk.go`: Bulk 接收/结果循环；`native.go`、`scan.go`: 各自 framing |
| Remote | `server/remote.go`: RemoteWeir、endpoint 生命周期/affinity/选择；`remote_dns.go`: DNS worker/socket 所有权；`remote_relay.go`、`remote_bulk.go`: 有界 pump |
| 本地执行 | `store/runtime.go`: Submit、Ticket、Session、调度 loop、controller、Close；`scan.go`、`native.go`: 共用调度器的 live session；`metrics.go`: 账本观测 |
| Mongo | `backend/mongodb/adapter.go`: Open/Prepare/Execute；`uri.go`、`dial.go`、`tls.go`、`wire.go`: 连接所有权与边界；`codec.go`、`expression.go`、`native.go`、`scan.go`: 数据语义 |
| Search | `backend/search/adapter.go`: Open/Prepare/Execute；`connection.go`、`dial.go`、`transport.go`: 配置、DNS/TLS 所有权和有界 HTTP；`bulk.go`、`json.go`、`expression.go`、`native.go`、`scan.go`: 后端语义 |

没有单独 `config` 包：配置的唯一消费者是 app/CLI，并复用真实 transport、Store 和
Mongo/Search 校验。没有拆开 `service`/`transport` 包：固定 Service choice 很小，转发与
listener 的 delivery/credit/drain 状态紧密相连；拆包会额外暴露这些内部机制。
Store 的单锁账本和调度保持在一起，不按文件行数制造新的状态所有者。

## 一个普通写

1. `Server.serveHTTP` 在解码前检查进程准入、hop 和方法，建立原始寿命、输入/输出
   stall 边界与 delivery；Read/Mutate 进入 `unary.go`。
2. `single` 调用 `resolve` 取唯一 Service，公共协议校验后，本地路径调用
   `Runtime.Prepare` → `Adapter.Prepare`。只有 Adapter 解码文档或后端 options。
3. `Runtime.Submit` 在单一锁下预留 pending/result credit；调度器按已准备的 opaque
   token 组批、按该流的 key 排序，取得执行额度，调用 `Adapter.Execute`。
4. Adapter 返回每项证据和独立拥塞反馈。Runtime 完成 Ticket，AIMD 消费反馈；
   `Ticket.Wait` 给 handler 结果，delivery 保留 Ticket 到传输结束再 Ack。
   APPLIED 不意味着客户端一定收到了回复；已发送但缺失的 mutation 回复仍是不确定。

Bulk 也使用相同的 Prepare/Submit/Ticket 路径。取消不推断回滚；微批不是事务；
没有新增重试、全局 key 排序或本地快通道。

## 一个 Remote 请求

`resolve` 选择 RemoteWeir 后，`forwardContext` 消耗一个原 hop 预算；relay 为这次
delivery 占一个该 Service 共享的 relay slot。`selectClient` 按原 affinity 规则从
固定 endpoint 集中选一次，只在原始 deadline 内有限等待。随后使用生成的公共 RPC
客户端；没有本地 Adapter plan、DB 调度器、业务重放或应用层换端点循环。
Bulk/Native/Scan 固定这个连接直到结束，并验证 End、计数、EOF/最终状态。
delivery/pump 结束释放 relay credit；DNS 更新只影响后续连接与新调用。

## 启动到 drain/Close

`Decode/Validate` 先检查整个图。`app.Open` 构造共享 admission、Local runtime 与
Remote endpoint 所有者，再构造 transports、绑定 listeners、注册每节点 metrics 和
可选 diagnostics。部分初始化失败也经 `Node.Close` 回收已构造资源。
`store.New` 接收已构造 Adapter 的所有权，包括其校验失败时的 Close。

`overload.New` 同步首次观测并发布准入状态，`Node.Start` 启动同一 Guard 的 100ms 采样循环和 listeners，进入 serving。
`Node.Close` 首先降低 readiness/停止新准入，停止并 join Guard，开始所有 runtime drain，并行等待
有界 listener shutdown 与 runtime Close，然后关闭 listener、Remote sockets/DNS，
最后关闭 diagnostics。Runtime 继续派发有限已准入工作，在原 drain 期限后取消并关闭
唯一 Adapter。没有额外关闭 driver 的管理器，也没有引入第二条关闭链。

## 测试和 fixture

默认 `go test ./...` 不连接真实 Mongo/Search；本机有界 TCP/TLS/DNS 单测只使用自有
loopback 服务。真实测试保留 `//go:build integration` 和明确环境 opt-in：

```sh
GOPROXY=off GOSUMDB=off go test -count=1 ./...
GOPROXY=off GOSUMDB=off go test -race -count=1 ./...
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...

# 旧 credential-free replica set；脚本只管理有 owner 标记的自有实例。
scripts/mongo-local.sh start
GOPROXY=off GOSUMDB=off scripts/test-integration.sh -race -count=1 -timeout=10m -v
scripts/mongo-local.sh stop

# 每个测试生成自己的 requireTLS/SCRAM 单成员进程和临时材料，无需启动旧实例。
GOPROXY=off GOSUMDB=off WEIR_M10_INTEGRATION=1 WEIR_MONGO_PROFILE=tls \
  go test -race -tags integration -p 1 -count=1 -timeout=10m ./... -v

# 两个 Search profile 分别运行；这些全仓命令也需要上面的旧 Mongo 实例。
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch \
  scripts/test-integration.sh -race -count=1 -timeout=10m -v
scripts/search-local.sh stop elasticsearch
scripts/search-local.sh start opensearch
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=opensearch \
  scripts/test-integration.sh -race -count=1 -timeout=10m -v
scripts/search-local.sh stop opensearch
scripts/mongo-local.sh stop
```

failCommand 是 server-global，真实故障测试包间必须 `-p 1`，不得并行运行这些命令。
未开启 Search/TLS opt-in 时的 skip 不证明对应 profile 已运行。

`testmongo.Open(t)` 返回一个 `*Fixture`：`Admin` 用于独立观察/failCommand，`DB` 是
唯一数据库，`URI` 是被测生产 Open 使用的连接；TLS 时该 URI 是最小权限应用用户。
`StartProxy(t, fixture)` 直接使用同一个对象的 URI/TLS 材料，绝不按数据库名查全局表。
proxy 只用于测试观察和故障注入，直连 mongod TLS 成功另外验证。
清理顺序由 t.Cleanup 保持：被测 Adapter/代理/观察客户端先关闭，再停止自有 mongod、
校验 owner、移除本次生成的数据与证书/keyfile，保留 owner 和日志。

`testutil.Root(t)` 从当前包向上寻找确切的 Weir go.mod，用于 fixture 二进制和资源路径，
也用于 CLI 子进程构建与示例配置；首行模块声明支持 LF、CRLF 和末尾无换行，
不再假设目录深度，不读取开发者凭据。
Mongo/Search 通用 Adapter 测试 builder 位于各包 `fixture_integration_test.go`。
server 的 `fixture_integration_test.go`、`scan_fixture_integration_test.go`、
`replica_fixture_integration_test.go` 提供本地、stream 和多 runtime 场景数据；
故障特有 helper 保留在其测试文件中，未建立通用测试服务器框架。

Mongo 的 `rmw_conformance_test.go` 保留原事务计数器、新事务重算和同事务 commit-only
资格状态机及原有断言；它仍是 conformance harness，不是公共 ProgramTransform 的后端集成测试。
生产 Scan 需要的 `nativeAttemptContext` 在 `attempt.go`，固定驱动的期限/取消语义未改。
当前 Lua 接线位于 `internal/value`、`internal/luaengine`、`internal/luaworker` 和各后端 `program.go`。

`experiments/goluaprobe` 与 `experiments/luaprobe` 保留为历史候选调查，不是产品 runtime。
`cmd/weir` 不链接 Lua VM；单独的 `cmd/weir-lua-worker` 链接 GopherLua 并按 `lua.v1`
协议服务。此前资源初筛 NO-GO 针对当时的候选与实现路径，不等于当前 worker 的安全资格结论；
当前 worker 无硬进程内存上限，不能接收不可信脚本。

M12R 基线 `383b4aa` 的本地 raw/dial/closing owner、分层 remote-tail 与 replacement 两 Local
完整生命周期已获统筹有限独立验收；原 M12 失败保留。本地 owner 上限不等于无条件的
DB accepted/远端工作总上限，runtime、平台、容量及部署资格仍分别 required。

历史报告保留当时路径。本次现址映射：`internal/mongostore` → `internal/backend/mongodb`，
`internal/searchstore` → `internal/backend/search`，`internal/test*` → `internal/testutil/test*`，
`internal/luaprobe` → `experiments/luaprobe`；完整文件映射见结构报告。

Search 原生 HTTPS/Basic fixture、显式连接和独立观察入口、权限及清理见 [M11](milestone-11.md)。`netlimit` 复用 peer/Mongo/Search 的有界 DNS I/O；各 backend 保留并发 gate、地址选择与关闭所有者。

Linux 内存 profile、可见层级与读取边界、明确降级/未知、原生测试和环境限制见 [M14](milestone-14.md)。
`proc.go` 只负责有界文件读取/解析与静态 profile，不是资源框架；可移植解析测试使用自有 temp 文件。
[M14R](milestone-14-remediation.md) 将身份收窄到已验证的相关 cgroup/mount/层级/limit，完整观测后再确认变化。
`scripts/test-memory-linux.py` 独立有界回收预先登记的候选资源和宿主 Popen，记录原失败及每项清理结果；
离线故障注入入口是 `python3 -m unittest discover -s scripts -p test_memory_linux_test.py`。
`memory_linux.go` 才选择实际 /proc，其他 OS 不读取 Linux 文件。app metrics 仅读 Snapshot，不启动第二采样器。
Darwin 当前 physical footprint 已由 [M19R](milestone-19-remediation.md) 接线并获统筹有限独立验收；固定 purego v0.10.2，CGO0/CGO1 原生短测分别记录。原 [M19](milestone-19.md) 未实现历史保留。
本地 Bulk 过载关闭输入后继续交付已准入 Ticket，最后返回 ResourceExhausted；不清除结果账本或重放写入。
历史记录：ProgramTransform 曾获首版延期。2026-09-29 用户重新开启决策并要求实现；现已提供需显式配置的 `lua.v1`，但缺少硬进程内存上限，仍不得接收不可信脚本，也不代表生产资格通过。

M15 的 `packaged_integration_test.go` 显式使用归档提取的 binary 和已加载的准确 OCI config ID；
自有 Darwin Mongo TLS fixture、三轮 Linux arm64 PID1/non-root/read-only/有限资源、标准 TLS 拒绝与 UNKNOWN 无重放按 [M15](milestone-15.md) 分开记录。
helper 仅通过独立只读 fixture 挂载，产品 image 不包含测试代码。默认测试不执行 Docker/真实 DB。
