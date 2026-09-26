# 第九阶段：有界静态 RemoteWeir 端点与普通 DNS

日期：2026-09-27。起点：`8e944e19113f1d87bdcf691169b46453bc6d2092`，干净本地 `main`。
结束 SHA 由本阶段提交记录和最终交接消息给出，不让提交内容自引用自身 SHA。
本阶段仅闭环静态端点/DNS；未 push、PR、发布、生产部署、使用收费资源或启动下一阶段。

## 实现与契约

- 单一配置模型为 `remote.endpoints: ["host:port", ...]`，1–8 个成员，原 `relays`
  仍是每 Service 共用 1–16 额度。旧 `endpoint` 字段严格拒绝，无双轨兼容逻辑。
  `server.CanonicalEndpoints` 在任何 channel 创建前完整校验；IPv4/IPv6/DNS、端口、
  大小写/根点/IPv4-mapped IPv6、重复、非法 resolver target 的规则见双语架构 14.3。
- canonical host:port 是稳定身份。Read/Mutate 共用 canonical URI；Native/Scan 用 resource；
  Bulk 用有界 request ID，一次选择，全流固定。完整 SHA-256、u32 big-endian 长度 framing、
  最大 digest、字典序 tie-break 及三个固定测试向量写入双语架构。没有后端字节解码、
  same-key 全局排序、动态成员控制平面、数据库复制或跨 Store fanout。
- 每身份一个标准 gRPC ClientConn/pick-first。先从 READY 快照选择；观察到 IDLE 时提示
  连接管理器为未来调用重连。全冷时只选一个非 SHUTDOWN 成员，持有已有 relay credit，
  最多等 min(原 deadline, 2 s)；TF/SHUTDOWN 立即失败。没有第二次选路或应用重试。
- 标准 Go `Resolver.LookupHost`、`PreferGo`、`StrictErrors`，绝对名称的 A/AAAA；
  固定 IP 无 DNS 路径。不查 TXT/SRV，不接受外部 service config、proxy、retry、hedging。
  每结果最多 8 地址，超量全部拒绝后才可能创建 SubConn；排序/去重只影响连接候选。
- DNS worker 为每身份固定一个，每 Service 两个 lookup credits。一次 DNS I/O 2 s，
  每 lookup 两个 DNS socket，每连接交给解析器 ≤4098 bytes（含 TCP 两字节长度前缀），
  最多额外读取 1 byte 检测超量。
  wire 超量使整次查询失败，即使另一地址族有效。Go 的 UDP/TCP 编解码未被重写。
  成功每 30 s 刷新，ResolveNow 合并、最短 1 s；失败 1/2/4/8/16/30 s backoff。
  NXDOMAIN、空/超量、timeout 均发布空集合，未来连接不无限使用 stale。最大图一个 Service
  的 4 轮解析 I/O 为 8 s，因此计划刷新/准入/I/O 窗口上界为 30+8=38 s，另加有限本地
  解析工作和系统调度等待；这不是实时 OS 保证。恢复需下一次合法结果。
- DNS 替换/撤回让 pick-first 优雅排空旧 SubConn，在途流不搬家、不重建。每身份
  所有在拨/存活/排空 TCP socket 合计最多 2；两条旧流占满时新连接失败，不能增加 socket。
  原流最长仍受 Bulk 15 min、Scan/Native 5 min、unary 30 s 及 stall/原 caller 期限限制。
  RemoteWeir 显式拥有排空 socket，Close 恰一次取消解析/拨号、join 解析/I/O 并关闭旧连接。
- TCP dial 2 s；连接 backoff base=100 ms、multiplier=1.6、jitter=0.2、nominal cap=2 s，
  **含 jitter 实际最大 2.4 s**。gRPC setup 的期限取 max(backoff, 2 s)，故也最多 2.4 s。
  选择等待单独最多 2 s，不因下一跳/DNS IP 重置原调用 deadline 或 hop。
- 保留明文 intranet、独立 application/peer hop 规则、UNKNOWN、NativeCompletion、
  End/count/final status、APPLIED 的独立执行证据。未恢复 Weir TLS/认证/授权。

实现定位：`internal/server/endpoints.go`（身份/选择/连接所有权）、`peer_dns.go`
（标准 DNS 的有限适配）、`relay.go`/`relay_bulk.go`（一次派发），`internal/app/config.go`
与 `stores.go`（严格配置/组装）。没有改动 Adapter、Store 调度器或 backend 连接 profile。

## 固定版本库审计

固定 Go **1.27.0**，gRPC **v1.79.3**；未升级依赖。以下路径均为该版本源码，行号以本次
本机 module cache / GOROOT 为准，升级需重新审计和资格验证。

| 源码 | 本次确认 |
| --- | --- |
| grpc `stream.go:685` `shouldRetry` | `committed` 最先禁止重放；DisableRetry 之前仍有 nil transport stream + allowTransparentRetry，以及 firstAttempt + unprocessed 分支，不能把 DisableRetry 当作绝无透明尝试 |
| `stream.go:805,924,991` | Context() commit attempt；零 retry buffer 在首个非空 protobuf 消息提交后 commit；Bulk/Native 在发送第一帧前主动 Context()；unary/Scan 保留零 buffer |
| `internal/transport/http2_client.go:791,1245,1391` | 明确 transport 未接收、REFUSED_STREAM、GOAWAY last-stream 边界才标记 unprocessed；正常断线/Unavailable/已收到请求不等同此证据 |
| `balancer/pickfirst/pickfirst.go:211,238,333,494,520,690` | ReportError 可无限沿用旧结果，故错误发布空集合；最多按输入地址创建 SubConn；旧 READY 在新集合中则复用，否则 Shutdown；Shutdown 优雅 drain；READY 丢失后为 IDLE，未来调用提示 Connect |
| `clientconn.go:937,1189,1318,1658`；`balancer_wrapper.go:355` | 被删除 SubConn 已不在 ClientConn 的 active map；显式追踪旧 TCP socket 是关闭所有权所需，不只是计数；setup 使用 backoff 与 MinConnectTimeout 较大值 |
| `internal/backoff/backoff.go:54` | jitter 在 MaxDelay 截断之后施加，实际 cap 为 2.4 s |
| grpc `internal/resolver/dns/dns_resolver.go:206,321,339` | 标准 DNS resolver 无地址数 cap，成功只在 ResolveNow 后再解析，错误保留旧结果；未直接采用其无限 stale 策略，也未修改全局 resolver 注册或全局时间变量 |
| Go `net/lookup.go:193,303`；`lookup_unix.go:53` | PreferGo LookupHost 同步收拢 A/AAAA；LookupNetIP/lookupIPAddr 的 singleflight 取消可先返回，故最终选择 LookupHost，取消同时关闭本次 DNS socket |
| Go `net/dnsclient_unix.go:108,136,169,594,671,828` | UDP buffer 1232，TCP 长度字段最多 65535；A/AAAA 两个 query worker，绝对名不扩展 search domains；我们的 4098-byte read cap 在超量 TCP body 交给 Go parser 前失败 |
| Go `net/addrselect.go:14,44` | Go 会做 RFC6724 排序和串行、无数据包的 UDP route probe；同步 LookupHost 保持其工作在同一 lookup credit 内，随后按 canonical IP 排序发布，排序不改变身份 |

DNS read cap 限制每 A/AAAA 回答交给 parser 的数据约 4 KiB；保守临时地址项 ≤512/lookup，
最终发布 ≤8。Go 在读 TCP 长度前缀后仍可能先分配最多 65535-byte buffer，这个固定框架
开销没有伪装成 4 KiB。可信机器的系统 hosts/DNS 配置由标准 Go 处理，未修改它们。

## 资源公式与实测

设 Remote Services 数 `R≤16`，总身份 `N=ΣE_r≤8R≤128`，其中 DNS 身份 `D≤N`，
活跃 lookup `Q≤Σmin(2,D_r)≤32`，已准入 remote RPC `P≤min(process sessions,Σrelays_r)≤64`。

| 资源 | 硬约束 / 最大静态图 |
| --- | --- |
| gRPC ClientConn | `N≤128`；DNS 回答不创建新 ClientConn |
| 当前发布 IP / active pick-first SubConn | `8D+(N-D)≤1024`；没有发布后再截断 |
| 出站 peer TCP，在拨/存活/旧连接 | `H≤2N≤256`，且每身份 ≤2；旧流占额不能绕过 |
| DNS I/O socket | `≤2Q≤64`；Go 的不发包 route probe 串行发生在查询结果收拢后，不并行展开每个地址 |
| DNS worker / query worker | `D≤128` / `2Q≤64`；lookup 和关闭都保留原 credit；无每请求解析 goroutine |
| relay / Bulk 关联 | 每 Service `L_r≤16`；全图实际 relay `P≤64`，Bulk 每 relay 最多 8 个关联项，未增加缓存 |
| relay pumps | Bulk 3、Native 1、Scan/unary 0；全图最多 `3P≤192`；保留 HTTP/gRPC 双完成所有权 |
| resolver hint / refresh | 每 DNS worker 一个容量 1 的合并提示和一个 timer；无累计历史列表 |
| metrics | 每 Remote 仍 34 series；connectivity 的五个 state gauge 改为端点**数量**，sum=`E_r`；无 hostname/IP/request ID 标签 |
| 最大进程静态 metrics | `41 + 41*listeners + 113*local + 34*remote`，Services≤16，仍最大 1931（16 LocalStores、2 data listeners）；16 Remote 为 544 remote series |

固定库创建点的保守 goroutine 预算为 `5N + 2A + 8H + 8Q + 8P ≤5504`，其中
`A=8D+(N-D)`。5N 覆盖 resolver/balancer/state serializers、DNS worker 和连接 timer 回调；
2A 覆盖 SubConn connect/backoff 与并发关闭；8H 覆盖 TCP dial cancellation、setup monitor、
reader/writer 与关闭回调；8Q 覆盖 A/AAAA query、dial cancellation/timeout/关闭；8P 覆盖
已准入 RPC 的取消观察、relay pumps 与 watchdog。默认禁用 idle timeout，不启用健康 RPC、
keepalive worker、retry/hedging 或额外 stats handler；库升级/新增这些机制须重新核算。
这些是固定创建点随上表有限对象数的保守上包络，不是某次测得峰值，也不是整个 Go 进程的
RSS/线程配额：Go GC/runtime、既有入站 Server、diagnostics 和 backend driver 另有基线。
未把此公式外推为全进程/数据库资源总预算、OS RSS/cgroup 或长期 plateau 资格。

实测（最终三轮 race 的逐轮值见 `.testdata/m9/dns-final-race3.log`）：

- 16×8 DNS cold storm：128 channels，32 lookups，64 A/AAAA queries；所有 DNS 指向本项目
  UDP/TCP loopback fixture，丢包时立即并发 Close；socket/lookup credits 归零，goroutine
  回到测试 baseline。三轮均为 baseline=4、peak=580、after=4，Close 约 6.3–6.8 ms。
- 16×8 literal-IP warm graph：128 live TCP、256 socket limit、544 remote series；
  使用 8 个自有 peer server，各接受 16 个 ClientConn。关闭后两端 socket 归零，goroutine
  回到 baseline 允许的短暂框架清理余量；三轮 baseline=18、peak=914、after=18，未调用数据库。
- 真实周期 DNS A→AAAA：约 30 s 刷新，旧 Bulk 约 120 个已确认操作保持 IPv4，新 unary
  仅在 IPv6 执行一次；旧新 sockets=2，Close 强制回收 active map 外的旧流连接。

## 验收覆盖与真实/注入区分

| 测试 / 场景 | 证据边界 |
| --- | --- |
| `TestEndpointCanonicalBounds`、`TestRemoteEndpointListConfiguration` | 规范化、旧字段拒绝、重复与 1–8 边界，非法 URI/target，不解码 backend 文档 |
| `TestEndpointRendezvousVectors`、`TestEndpointSelectionLocalityDistributionAndMembership` | 固定向量；120 个不同 URI 实际分布于 3 endpoints；同 URI Read/Mutate、opaque bytes、配置顺序、增删成员只改变 winner、后续调用避开故障端点 |
| `TestEndpointBulkPinnedAndServiceCredits`、既有 `TestPeerRelayAdmissionAndSharedListeners` | 24 个不同资源操作整流固定；双端点真实 application/peer 仍共用一个 relay slot，不能按成员扩额 |
| `TestDNSRealAnswersOrderAndTCP`、`TestDNSRealFailureClearsAndRecovers` | **真实 UDP/TCP DNS** A/AAAA、多地址/顺序、TCP fallback、NXDOMAIN、空、9 地址、约 10 KiB wire 超量、延迟、丢包和恢复；publication sink 记录结果，不是手工注入地址 |
| `TestDNSPickFirstMultipleAddressesAndOriginalDeadline` | 实际 A 地址 TCP stall、AAAA 健康 socket，标准 pick-first bounded Happy Eyeballs；原 30 ms caller 期限包含解析/连接等待 |
| `TestDNSRealAddressReplacementNewCallsAndClose`、周期刷新测试 | 实际 DNS A→AAAA 和真实 HTTP/2 socket；新连接恢复/旧流固定、socket Close 恰一次，未修改系统 DNS/hosts/接口 |
| `TestDNSMaximumGraphResolutionStormAndCancellation`、`TestEndpointMaximumGraphConnectionsAndCleanup` | 最大解析/连接图、并发 Close/metrics、归零和 goroutine 记录 |
| `TestEndpointWireUnreceivedAndReceivedNoReplay` | 原始真实 HTTP/2：REFUSED_STREAM、GOAWAY(last=0) 未接收；DATA 后断线、GOAWAY(last=current)、Unavailable 已接收；两个 READY 端点合计 header=1、已接收请求为 0 或 1，没有应用切换 |
| `TestEndpointStreamsNeverMigrate` | Bulk 已 APPLIED、Native 已 Head、Scan 已 document 后断 socket；另一端点 READY 也不重开/迁移，保守非 OK，无假 End/complete |
| `TestPeerRealFiveRPCs`、`TestPublicExpressionUnaryBulkAndOpaquePeers` | Mongo/ES/OS direct/1-hop/2-hop；remote 最终有两个**独立 Adapter/Runtime/driver**共享同一真实 DB/index，合计记录执行数、opaque、Bulk order、Native/Scan、表达式与单 Store |
| `TestPeerRealAcknowledgedReplyLossNoReplay` | 三后端 × DB/peer/application 三段 × ordinary/Native/expression，两个执行端点；peer 故障只消费一次共享 loss budget，另一端点可成功返回。每个新请求独立计数，两个接收端和实际 backend 合计始终 1；客户端 UNKNOWN/incomplete/non-OK，APPLIED 证据未改写，read-back 仅在测试 |
| `TestEndpointIndependentProcessesDistributionReplacement` | 三个真实 CLI executor 共享同一 Mongo DB/Search index，160 个 Read 分布、SIGTERM 一端后健康端点继续服务、同端口 replacement 再加入；独立进程真实 driver，不是 mock |
| `TestEndpointDNSAcrossProcesses` | 两个真实 CLI executor 分别绑 IPv4/IPv6 同端口，转发进程使用生产 RemoteWeir/Server 和标准 Resolver.Dial 指向本轮 DNS fixture；A→AAAA、SIGTERM、新 Read 恢复。DNS dependency 仅测试 child 注入，未给 CLI 增加 DNS-server 参数或配置模型 |
| 原 peer/transport/process 全回归 | 慢消费者/持续 producer、队列/执行/发送取消、原 deadline/hop 环路、End 后非 OK/额外帧/截断、drain、SIGTERM、执行与结果额度释放；旧 API/协议字段未变 |

没有手工 resolver 地址更新作为实际 DNS 资格替代。publication 测试控制 DNS 回答并调用
标准 ResolveNow；端到端测试由连接失败或真实 30 s timer 触发查询。后端 fixture 是
MongoDB **8.0.32** / mongosh **2.6.0**、Elasticsearch **8.17.0**、OpenSearch **2.19.0**；
Mongo driver **2.9.1**。Search 镜像沿用带 owner label 的固定 digest。
平台为 macOS **26.6.2 (25G83)**、Darwin **25.6.0**、arm64、Go **1.27.0**。
Linux 容器里的 ES/OS 不是 Weir Linux native-run 证据。

## 命令、日志与失败历史

全部本轮日志在忽略目录 `.testdata/m9/`，历史 `.testdata` 和已停止 fixture 数据不删除。
启动前确认 27028/19200/19201 无 listener，两个 Search 容器均 exited 且 owner/digest 匹配，
然后才用项目 ownership-check scripts 启动。真实 backend 包只串行 `-p 1`；Mongo
failCommand 没有与另一个 backend suite 并行。默认 DNS/选择 race 使用独立自有 loopback。

```sh
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch
scripts/search-local.sh start opensearch
GOPROXY=off GOSUMDB=off go test -race ./internal/server ./internal/app \
  -run 'TestDNS|TestEndpoint|TestRemoteEndpoint|TestPeerRelayAdmission' -count=3 -v
# 以下对 elasticsearch、opensearch 分别运行，全程串行。
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -race -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
  go test -race -tags integration -p 1 ./internal/server ./internal/app \
  -run 'TestPeer|TestPlaintext|TestAcceptance|TestUnary|TestNativeWire|TestPublicExpression|TestIndependentWeirProcesses|TestDiagnosticProcessSIGTERM|TestDiagnosticsMaximumStaticSeries|TestEndpointIndependent|TestEndpointDNSAcross' -count=3 -v
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
# 全部真实后端回归结束后，先停自有 fixture，再执行默认离线检查。
scripts/search-local.sh stop elasticsearch
scripts/search-local.sh stop opensearch
scripts/mongo-local.sh stop
GOPROXY=off GOSUMDB=off go test -count=1 ./...
GOPROXY=off GOSUMDB=off go test -race -count=1 ./...
```

开发历史如实记录：

1. `first-test.log`：旧连接期限测试假设两个 hop 串行拨号；新有界预连接并行，实际只等约
   60 ms。改为验证真实经过的 setup 时间全部扣原 350 ms deadline，不要求虚构的串行等待。
2. `dns-first.log`：macOS 未配置 127.0.0.2，绑定失败；改为系统已有 `127.0.0.1`/`::1`，
   未增加接口 alias。模拟 DNS 多回答中的 loopback 地址无需真实 listener，不会发业务数据。
3. `dns-endpoints-race3-first.log`：关闭 TCP 后原 endpoint 仍可用，pick-first 进入 IDLE，
   不会自行生成失败解析。增加派发前 IDLE Connect 提示，并让替换 fixture 真实停旧 listener、
   触发 failed reconnect/ResolveNow；`dns-replacement.log` 通过。
4. `real-multi-first.log`：ES fixture 启动尚未 ready 即启动了首轮定向测试；Mongo 通过，
   Search 明确 connection refused。等待启动脚本成功后有限重跑，`real-multi-ready.log`
   的 server 套件通过，未绕过 readiness 或隐藏错误。
5. 同一 `real-multi-ready.log`：DNS 测试 child 漏了 production Server 所需 Admission，
   启动失败；补齐真实 Admission 后 `process-dns-ready.log` 通过。
6. 固定 Go 源码复核发现 LookupNetIP singleflight 的提前取消返回和 RFC6724 route probe。
   最终改同步 LookupHost + DNS byte cap，保留原有 `dns-endpoints-race3.log`、
   `full-elasticsearch.log`/`full-opensearch.log` 作为收紧前记录；最终证据使用 `dns-final-race3.log`
   与 `final-full-*`/`real-*-race3.log`，不把旧通过冒充新实现结果。

最终结果均 PASS；所有测试使用 `-count=1` 或 `-count=3`，不依赖测试结果缓存。

| 最终执行 | 日志（均在 `.testdata/m9/`） |
| --- | --- |
| DNS/端点/relay 边界 race，连续 3 轮 | `dns-final-race3.log`；server 149.086 s |
| Mongo + Elasticsearch 全量普通 / race | `final-full-elasticsearch.log`、`final-full-elasticsearch-race.log` |
| Mongo + OpenSearch 全量普通 / race | `final-full-opensearch.log`、`final-full-opensearch-race.log` |
| Elasticsearch profile 的真实 peer/transport/process race，连续 3 轮 | `real-elasticsearch-race3.log`；server 80.643 s、app 91.336 s |
| OpenSearch profile 的真实 peer/transport/process race，连续 3 轮 | `real-opensearch-race3.log`；server 80.424 s、app 91.343 s |
| 后端停止后 `GOPROXY=off GOSUMDB=off go test -count=1 ./...` | `stopped-backends-offline-test.log`；server 58.365 s |
| 后端停止后 `GOPROXY=off GOSUMDB=off go test -race -count=1 ./...` | `stopped-backends-offline-race.log`；server 60.883 s |
| 默认 / integration 两种构建标签的 vet | `stopped-backends-vet.log`、`stopped-backends-vet-integration.log`，退出 0 |
| style、diff、协议/依赖保持不变 | `final-style.log`、`final-diff-check.log`、`protocol-dependencies-unchanged.log` |

关闭动作及 ownership 检查见 `stop-*.log`、`cleanup.log`。Mongo PID 98983 已退出；
27028/19200/19201 均无 listener；两个 Search 容器均为 exited，owner 仍为
`weir-milestone-2`。逐个检查本轮日志记录的 175 个进程 PID，没有遗留 Weir/测试 child。
真实 backend suite 完成后才关闭 fixture；以上最后两次默认测试是在三个 backend 全部
停止后运行。历史数据和日志保留，未删除容器/数据库目录，未触碰无关服务。

实现、双语架构、README 和本报告一并形成一个本地 main 提交。最终 SHA、干净状态与
资源停止证据通过交接消息提交给统筹；交接后本执行者停止 checkout 操作和 fixture 工作，
没有 push、PR、下一阶段任务或自动轮询。

## 保留限制

本机静态集合/普通 DNS/长连接语义资格不等于生产或云原生资格。无动态配置、SRV、xDS、
主动 DB 探活、stream migration、应用 retry/failover、Provider 或集群调度器。DNS polling
忽略 TTL，错误 fail-closed 可能降低可用性；两个旧长流占额可暂时阻止新连接，但不突破界限。
Read/Mutate locality 不提供 same-key ordering。成员共同 Store/Adapter/profile 由部署保证。

跨节点 Kubernetes/Service/LB、扩缩容和真实网络分区、后端标准 TLS/凭据连接 profile、
多实例总 DB 连接/并发预算、ProgramTransform runtime 隔离、六平台 native-run、Linux
RSS/cgroup、参考负载/SLO 和各平台 ≥24h soak 均仍未合格；没有缩减这些必需目标。
Diagnostics 仍 loopback。后续由统筹另行串行安排，本执行者完成交接即停止。
