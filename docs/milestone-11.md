# M11：Search 标准后端连接与无隐式写入重放

日期：2026-09-27。基线为干净 local main
`a6df4ea06be436d19cf12d69404719ebf5811079`。交付 SHA 记录于本地提交及
`.testdata/m11/final-state.log`，不在提交中自引用。执行环境为 macOS 26.6.2 arm64、
Go 1.27.0；Docker Engine 29.7.2 的 Linux arm64 数据库容器。不是 Linux Weir
native-run，也不是整体生产资格。没有 push、PR、发布、生产部署或 Weir 身份功能。

## required / implemented / qualified / blocked

| 必需项 | 实现及本轮证据 | 边界 |
| --- | --- | --- |
| 单静态 Search endpoint，HTTP 或验证 HTTPS/可选 Basic | `connection.go`、`dial.go`、`transport.go`；两个产品各自原生 TLS、安全插件/账号；生产 Open、app JSON Decode/Open direct/peer 全操作 | 无节点发现、sniff、多个 endpoint、业务重试、热重载 |
| 同一纯配置规则与无副作用 preflight | app Validate 调用 Search ValidateConfig，完整图先检查；默认矩阵、未知字段、缺失 CA 不访问、后续无效服务不触网/绑定 | 只新增 backend connection，废弃 Weir identity/server_name 等仍拒绝 |
| 普通与 Native 无隐式写入重放 | 固定 Go 源码审计；真实两种 HTTPS 后端的普通/Bulk/表达式/Native：丢回复、截断、损坏、实际 401/403、连接关闭后新调用 | 计数与独立读回，不用相同最终值单独证明；不推断复制切换 |
| 有界 DNS/TLS/HTTP/Native/Close | 默认三轮 race、实测硬期限、socket/握手数、取消重复、慢消费/上传、账本归零；真实确认提交后 drain 和部分 app 初始化失败 | DNS 地址变化/流固定是自有真实 DNS+合成 TLS fixture；两种真实 DB 另测 DNS hostname/SAN/SNI |
| 旧 HTTP 与 peer 回归 | 原有明确 opt-in 集成套件；peer DNS 三轮 race；全仓默认非缓存 test/race、default/integration vet | HTTPS 与 HTTP 分别运行；未启用 profile 的 skip 不算该 profile 通过 |
| 后续生产必需项 | 未在本阶段实现/资格，仍按 production-readiness 清单推进 | ProgramTransform、跨平台、总 DB 预算、多副本/复制切换、平台/打包/K8s、容量/soak 都未因此通过 |

本阶段资格只覆盖上述固定版本、本机单 index/primary、零 replica fixture。
系统 roots 的选择和未知根拒绝有默认测试；正向真实数据库证据使用本轮显式 CA。
没有修改本机 trust store 来制造系统 roots 成功证据。旧固定数据库版本只是回归基线，
未作持续安全支持或完整漏洞审计声明；升级必须重新验证，而不是自动继承这里的结论。

## 配置、职责与严格边界

`local.search` 保留 `url`、`index`、`profile`，增加可选 `connection`：
`username`、`password`、`ca_file`。adapter Config 的标准 `*net.Resolver` 是 I/O
外部依赖 seam；app 不暴露 DNS-server、resolver 或 server_name JSON 开关。
README 中只有无秘密占位例子。

- URL 最多 1024 bytes；一个 `http`/`https` host 和 1–65535 显式端口。拒绝
  userinfo、路径（包括尾部 `/`）、query/fragment（包括空 `?`/`#`）、percent 编码、
  zone、未指定/组播 IP、歧义纯数字短 IP、非法 DNS label、尾点、非 ASCII 名称。
  DNS 小写化，IP canonicalize/unmap，端口数字规范化；TLS 使用规范 hostname，
  不把 DNS 解析后的 IP 替换为验证名。DNS name 最多 253、label 最多 63 bytes。
- HTTP 不接受 connection；HTTPS 可无 connection 使用系统 roots/无凭据，或
  CA-only，或显式 Basic。非空 connection 必须有实际内容。凭据配对、非空，
  username/password 最多 128/256 bytes；拒绝 username 冒号、控制符、无效 UTF-8。
  CA path 最多 2048 bytes。Open 复制连接值，配置改动须重启。
- CA 文件先检查普通文件，再有界读取 256 KiB+1 sentinel；无内容、无效、缺失、超限
  都脱敏失败。部署须提供不可变普通文件，未实现可变文件/网络文件系统管理器。
  明确 CA 使用独立 trust pool；无 CA 时保留标准 Go/OS roots 行为。
- Go TLS 最低 1.2，标准链/SAN/有效期校验；不关闭验证、不自建 TLS、不支持客户端证书、
  任意 server_name、额外机制或自动账户。资格/操作错误不输出 URL、CA 内容、密码、Authorization。
- `search` 继续拥有 JSON、source/index/pipeline、HTTP/TLS 和连接状态；app 只校验装配，
  Core 没有引入 JSON/BSON 或具体 backend import。`netlimit/dns.go` 从既有 peer DNS
  提取实际重复的有界标准 I/O，保留 peer 自身选择、refresh、gate 和生命周期所有者。
  没有 AuthProvider、ConnectionManager、registry、函数变量替换或生产测试开关。

记录 API 的 default pipeline bypass、final pipeline 拒绝，以及 Native/表达式不能绕过
既有 index/source/ingest 限制均保持。Scan 仍是固定 PIT 的完整 hit traversal；不放宽
shard/replica/query profile，不增加公共 RPC。

## 固定 Go 1.27.0 审计与 outcome

| 固定源码 | 结论及实现约束 |
| --- | --- |
| `net/http/transport.go:844–890` `shouldRetryRequest` | fresh HTTP/1 连接错误不重试。reused 连接的 nothingWritten 仍需无 body 或 GetBody；之后还检查 replayability 和特定首字节/idle-close 错误，不能只看方法名 |
| `net/http/request.go:1561–1583` | `Body` nil/NoBody 或 GetBody 才可能 replayable；GET/HEAD/OPTIONS/TRACE 或 idempotency header 可允许重放。我们不加任何此类写入 header |
| `net/http/transport.go:1596` | getConn 使用 `WithoutCancel`；configureRequest 通过私有 context value 保留原寿命，dialer 将其取消连接到 DNS/TLS，避免调用取消后留满两秒后台拨号 |
| `net/lookup.go`、`lookup_unix.go`、`dnsclient_unix.go` | 每次纯 Go LookupHost 同步等待 A/AAAA；不用 LookupNetIP singleflight 的提前返回路径。标准 resolver 编解码与系统配置，socket 所有者在取消时关闭 I/O 并等待释放 |
| `crypto/tls/common.go:67–72` | 标准库 plaintext 16 KiB；TLS1.3 ciphertext 16 KiB+256，旧版本 +2048；普通 handshake 64 KiB、certificate message 256 KiB。不是仅分配业务文档大小 |
| `crypto/tls/conn.go` Close | TLS close-notify 可能等待；Search 持有 raw TCP，故障/Close 直接关闭 raw，HTTP framing 完整性仍是成功门槛 |

所有实际 mutation 路径已逐一核对：

- Put/Create/Delete 和物理 Bulk 是非空 NDJSON POST；Replace 先读取 seq/term，再同样
  bulk POST 加 OCC；均设置 `GetBody=nil`。Delete 没有无 body HTTP DELETE 的重试漏洞。
- BackendExpression 是非空 Update POST，GetBody=nil，`retry_on_conflict=0`，
  `doc_as_upsert=false`。没有丢回复后重新算或再次 Update。
- Native 的资格 GET 和实际 GET/POST 都经过独立 transport，MaxConns=1、禁 keepalive；
  POST 先验证非空首 item，再只用一次 source，GetBody=nil。header allowlist 拒绝
  Host、Authorization、Proxy-Authorization、两种 idempotency header。
- PIT open/fetch/close 是非空 body 的请求，亦不回卷。普通无 body Read/元数据 GET
  可在标准库的上述 reused/特定错误条件下重新取连接；受原 call deadline 限制，
  没有应用重试次数框架，也不声称所有读取都只发一次。

认证失败、证书失败不是自动拥塞或重放理由。无法证明写入确认时仍 UNKNOWN；检查
index 阶段拒绝可以 NOT_APPLIED。实际 401/403 mutation 目前保守 UNKNOWN/UNAVAILABLE，
不新增一套权限 outcome。Native 把完整 401/403/重定向响应交给调用者；丢失/截断是
RESPONSE_INCOMPLETE。完整但损坏的 JSON 在 Native 中仍是 RESPONSE_COMPLETE（仅字节
完整，不代表 JSON/业务成功），记录 API 则保持 UNKNOWN。普通写确认提交后 drain 也
保持 UNKNOWN，不错误报告 NOT_STARTED。连接恢复只执行新的独立调用。

## 数字资源边界与实测

| 所有者/资源 | 硬边界 |
| --- | --- |
| 每 adapter 普通池 | P=1–32，M11 app P=4（M12 后随 local.concurrency）；HTTP/1，最多 P active/idle TCP；idle 30 秒 |
| Native 专用池 | 另加 1 条 fresh HTTP/1 TCP，无 keepalive；不是隐藏在普通池预算内 |
| 共同 dial owner | slot 从 DNS 开始持有到 raw socket Close；最多 P+1；无额外等待队列。普通+Native 后端 TCP ≤P+1，握手 ≤P+1 |
| DNS sockets | 每 resolving slot 同时最多 2（A/AAAA）；完成查询后才能拨一个 TCP。包括 DNS 的所有自有 sockets ≤2(P+1)，app 为 10；纯 IP 时 ≤5 |
| DNS retained/input | 最多 8 个答案，每 DNS socket 4098 bytes+1 检测字节（TCP 长度前缀计入）；两查询有界。无 cache、周期 worker、发现或后台重连 |
| 连接与调用 | DNS+TCP+TLS 最多 2 秒，原请求取消可提前结束；普通请求 header/body 在同一 2 秒 call context 内，header timeout 2 秒 |
| TLS retained | 标准验证后最多 8 个 peer cert，每 DER 64 KiB；更早分配受 Go certificate-message 256 KiB 限制。系统 roots 是 Go/OS 的配置资源；未声称可用 Go context 中断所有 OS 证书计算 |
| HTTP response | header 32 KiB；metadata 256 KiB、bulk/scan envelope 1 MiB、Read MaxDocument+32 KiB。禁自动 compression、proxy、redirect |
| Native | 物理执行由 Store `selectLocked` 创建的 ctx 限制，默认 `BackendTimeout=2s`，`executeNative` 使用同一 ctx。RPC 默认 5 分钟/最大 15 分钟，adapter fallback 15 分钟；全部取最严格的剩余期限，RPC 寿命不承诺连续执行 5 分钟；输入/输出 stall 由现有 server delivery 管理。总 body/response 各 8 MiB，item 256 KiB，metadata line 4 KiB，4096 items，chunk 64 KiB |
| Runtime/Native | 仍一个 live Native/Scan slot、Native 4 MiB PageBytes，保留原 pending/result/permit 账本；取消中断 Source/Sink，join 后释放；无第二业务账本 |
| Close | 取消 adapter lifetime，关闭全部 raw socket，等待正在进行的 dial/DNS/TLS，再释放两个 transport 的 idle 状态；重复 Close 幂等。Node 原 drain 预算和部分初始化回收不变 |

测量是短时本机证据，不是容量/SLO：

- silent TLS peer 在无调用方 deadline 时，三轮分别约 2.0009、2.0021、2.0022 秒失败。
  每轮 8×16 个并发连接尝试，测试配置最多 3 个握手 slot；取消/Close <1 秒，
  slot 归零，goroutine 3→3。并未声称实测 P=32 的生产容量。
- 普通 2+Native 1 三个 TLS socket 同时卡 header/body，三轮 Close+调用 join 都低于
  1 秒（记录样本约 90–200 微秒），socket 归零。没有 caller deadline 的慢 header/body
  约 2.00 秒终止；不是只因 context 存在就标记通过。
- 慢 Native consumer 和首 item 上传，各保留一个 live session/4 MiB；第二个 session
  被拒绝。Close 中断后 active/retained/live/Native bytes/socket 全部为 0。
- 每轮 24 次取消 Open，goroutine 4→4；16 次取消普通请求，不调用 Adapter.Close，
  detached dial/DNS slot 在 300ms 检查界限内归零。DNS NXDOMAIN/空答案、超时、9 答案、
  超 4 KiB TCP 答案均拒绝；真实 DNS 地址变化后新调用恢复，已有 Native stream 完成原字节流。
- 两个真实安全 backend 上，确认写入后以 30ms drain deadline 关闭：观察到约 30–31ms，
  UNKNOWN、业务请求一次、独立版本读回正确，全部账本/socket 释放。
- app 部分初始化：连续三次先打开有效 Store、再因不存在 index 失败，后端
  `/_nodes/stats/http` 的 current_open 返回基线；listeners 没有开始服务。
- 共用 DNS 提取后，原 peer 三轮最大图试验仍为 16 Services/128 endpoints、32 parallel
  resolutions、64 A/AAAA，Close 后 goroutine 返回基线；原 refresh/固定 Bulk 流测试通过。

## 原生安全 fixture 与权限

`internal/testutil/testsearch/secure.go` 仅编入 integration。每次启动都有唯一
`weir-m11-{product}-{pid}-{sequence}-{nonce}` 名称、`weir.owner=weir-milestone-11`、loopback
随机 host port、2 CPU/1536 MiB 上限、512 MiB JVM heap；禁止 pull，要求已存在固定镜像：

- Elasticsearch：`docker.elastic.co/elasticsearch/elasticsearch@sha256:2f602552550869fb29b6fd5848c5118d3ef3a2e1d5d45802e3ab9088cb2de8e2`
- OpenSearch：`opensearchproject/opensearch@sha256:1f8b88245a6af61e7aa500afe0e87d43401e4b33140bb47230a919428ce3f7cb`

测试程序生成 12 小时 CA、server/admin 材料和随机密码；agent 工具从未打开 PEM、key、
.env 或用户已有秘密。ES 启用 security 与 HTTP/transport TLS，使用 fixture file realm。
OS 保留安全插件、禁 demo 初始化，节点 transport TLS 和 REST TLS 都启用；独立 admin
证书通过验证 hostname 的 securityadmin 初始化配置，没有 `-nhnv`、不安全客户端或关闭插件。
TLS 代理只用于计数/故障，另有直连真实 native HTTPS 的完整资格测试。

官方配置依据：[ES 8.17 security settings](https://www.elastic.co/guide/en/elasticsearch/reference/8.17/security-settings.html)、
[ES HTTPS](https://www.elastic.co/guide/en/elasticsearch/reference/8.17/security-basic-setup-https.html)、
[OS 2.19 TLS](https://docs.opensearch.org/2.19/security/configuration/tls/)、
[OS securityadmin](https://docs.opensearch.org/2.19/security/configuration/security-admin/)。这些是配置依据，运行证据来自本轮日志。

Fixture 显式返回 Backend（应用连接）、Admin（独立观察）、Denied（只读账号），没有按
index/database 查 credential 的全局 registry/fallback。生产 Open 只用应用 pair。
实际权限如下，均只在测试 fixture 配置，不成为 Weir RBAC：

| 产品 | 应用角色实际授予 | 本轮证实的需求/限制 |
| --- | --- | --- |
| ES | cluster `monitor`；自有 `weir_m11_*` index 的 `read, write, view_index_metadata` | 根版本/集群设置/index 资格读取、文档写与动态 mapping、PIT/Scan。没有 index 创建/删除、cluster 配置或账户管理权限 |
| OS | cluster `cluster:monitor/main`, `cluster:monitor/state`, `indices:data/write/bulk*`；自有 index 的 `indices:admin/get`, `indices:admin/mapping/put`, `indices:data/read/*`, `indices:data/write/*` | settings 读取实际需要 state；bulk 同时需要 cluster/index scope；新增文档字段实际需要 mapping/put。使用标准操作族授权，不声称这是每个内部 action 的绝对最小集合 |

生产 operator 应把对应 index pattern 收窄到其配置的具体 index。应用/只读账号均不能
管理用户或集群；只读账号真实 mutation 返回 403。fixture 管理员仅引导/观察/清理，
不拿管理员成功冒充生产 Open 成功。[OS 权限文档](https://docs.opensearch.org/2.19/security/access-control/permissions/)
也说明 bulk 的 cluster 和 index 双 scope 需求。

## 失败历史与有限补救

原始日志保留，不覆盖失败：

1. `elastic-minimal-1.log`：ES 首次 native HTTPS/Basic Open+写后读成功。
2. `opensearch-minimal-1.log`、`opensearch-minimal-diagnostic.log`：securityadmin
   返回成功，但安全插件初始化线程尚未就绪，首次 Basic probe 仍 503。诊断 bootstrap.log
   已确认配置写入成功。补一个最多 20 秒的独立 readiness GET 等待，不重试业务写。
3. `opensearch-minimal-remediation.log`：进入应用资格后暴露缺少 `cluster:monitor/state`。
   `opensearch-minimal-permissions.log`：资格可用，但新增字段缺少 mapping/put，保守 UNKNOWN。
   根据原生拒绝日志补 fixture 需要的权限；`opensearch-minimal-mapping.log` 最小生产 Open+
   写后读通过。没有降低 TLS/安全插件要求，也没有使用超级管理员替代应用账号。
4. `connection-unit-1.log` 和 `opensearch-secure-race-3.log` 是测试代码初版的编译错误
   （unused import、Ticket.Wait 签名），后续独立日志修复通过；不计作运行通过。

## 命令与可复查日志

所有 Go 验证均使用 `GOPROXY=off GOSUMDB=off`，以下省略重复前缀。
完整日志在 `.testdata/m11/`；每个 secure fixture 另留 owner、database.log 和 OS
bootstrap.log。没有把材料内容写入报告。

| 命令 | 结果/日志 |
| --- | --- |
| `go test -count=1 ./...` | PASS，`default-test.log` |
| `go test -race -count=1 ./...` | PASS，`default-race.log` |
| `go vet ./...`；`go vet -tags integration ./...` | PASS，`default-vet.log`、`integration-vet.log`；末轮 `default-vet-final.log`、`integration-vet-final.log` |
| `go test -count=1 ./docs ./internal/backend/search ./internal/app` | PASS，`final-focused-test.log` |
| `go test -race -count=3 ./internal/backend/search ./internal/app -run 'TestSearch(Connection\|TLS\|DNS)' -timeout=2m -v` | PASS，`boundary-race-3-request-cancel.log` |
| `go test -race -count=3 ./internal/backend/search -run 'TestSearchTLSHandshakePoolCloseBound\|TestSearchDNSPinsActiveNativeStream' -timeout=1m -v` | PASS，`handshake-stream-race-3.log`；补无 caller deadline 握手和固定流证据 |
| `go test -race -count=3 ./internal/server -run '^TestDNS' -timeout=4m -v` | PASS，`peer-dns-race-3.log` |
| `WEIR_SEARCH_SECURE_INTEGRATION=elasticsearch go test -race -tags integration -p 1 ./internal/backend/search ./internal/app -run 'TestSecureSearchQualification\|TestSearchTLSApplicationAssemblyAllOperations' -count=3 -timeout=6m -v` | PASS，`elasticsearch-secure-race-3-final.log` |
| 同上，`WEIR_SEARCH_SECURE_INTEGRATION=opensearch` | PASS，`opensearch-secure-race-3-final.log`；两个产品独立运行 |
| `WEIR_SEARCH_SECURE_INTEGRATION=elasticsearch go test -race -tags integration -p 1 ./internal/backend/search -run '^TestSecureSearchQualification$' -count=1 -timeout=5m -v` | PASS，`elasticsearch-secure-dns-final.log`；补最后加入的真实 DB DNS/SNI 与清理实现 |
| 各产品独立运行 `WEIR_SEARCH_SECURE_INTEGRATION=... go test -race -tags integration -p 1 ./internal/backend/search -run '^TestSecureSearchProductionOpen$' -count=1 -timeout=4m -v` | PASS，`elasticsearch-final-fixture-smoke.log`、`opensearch-final-fixture-smoke.log`；最终唯一名称、排他目录和清理检查已实跑 |
| `scripts/mongo-local.sh start`；`scripts/search-local.sh start elasticsearch`；`WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -race -count=1 -timeout=10m -v` | PASS，`elasticsearch-http-regression.log`（app 36.018s、Search 20.702s、server 97.912s） |
| 停止 ES 后独立启动 OS；同上 `WEIR_SEARCH_INTEGRATION=opensearch` | PASS，`opensearch-http-regression.log`（app 35.031s、Search 20.267s、server 98.186s） |

真实数据库套件 `-p 1`，failCommand/共享 backend 串行；安全 profile 与旧 HTTP
不同时运行，不与另一真实数据库套件并行。默认 loopback 单测与编译检查不连接真实服务。

## 清理与停止边界

每个新安全 fixture 在 cleanup 中核对 marker、container owner 和 image，只停止/删除
本轮自己的容器和 data，删除其 materials，保留脱敏日志；没有删除历史资源。
固定的两个镜像均无声明的匿名 volume，容器删除同时释放其内部数据。
旧 M2 HTTP 容器原配置/镜像保留，只为回归启动后再停止；旧 Mongo 只停止自有进程并保留历史数据。
最终所有权、材料目录、进程/listener、工作树及 SHA 核验记录到 `final-state.log`。
完成回调在所有写入与测试停止后发送给统筹；不创建 heartbeat，不安排下一阶段。
