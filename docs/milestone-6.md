# 第六里程碑：最小健康检查与有界运行指标

> 历史资格记录：本文的 Weir peer mTLS/身份/权限配置及相关测试事实保留。
> 当前可信内网明文入口、配置和资格见 [M8](milestone-8.md)；Weir 认证体系已由 M8 移除。

起点：`482887e6029ae764d0bf1f77a4346fe25ad4e5d3`，干净 main。
实现提交：`a1642304f14e36918d3bdeddab09a741bafcc3f8`。
本轮只实现可选 loopback 诊断 HTTP、基本生命周期和有限数据平面指标。
不增加数据库调用、调参、重放或数据能力；不推送、发布、部署。

实现前确定的小型清单按状态拥有者划分：Node/Guard、Admission/RemoteWeir、LocalStore、
RPC/transport。以下最终清单保留这些边界；Native 全 exchange 与纯 record/Scan execution
耗时分开，已执行证据与传输事件分开，Bulk 等待额度不计作重复拒绝。

## 配置、调用与 HTTP 边界

诊断默认关闭。旧 flags 模式新增 `-diagnostics 127.0.0.1:7449`；静态 JSON 在顶层增加
`"diagnostics": "127.0.0.1:7449"`。`-config` 仍与其他 flags 互斥。IPv6 可用 `[::1]:7449`；
拒绝 `localhost`、空 host、通配 IP 和非 loopback IP。端口 0 用于隔离测试。

```sh
# 使用已有隔离后端；诊断自身从不访问数据库。
go run ./cmd/weir -diagnostics 127.0.0.1:7449
curl --fail http://127.0.0.1:7449/livez
curl --fail http://127.0.0.1:7449/readyz
curl --fail http://127.0.0.1:7449/metrics
```

每 Node 一个独立 `prometheus.Registry`，使用 Prometheus Go client **v1.23.2** 的
`promhttp.HandlerFor`，没有全局 registry/mux、Go/process 默认 collectors、压缩、pprof、
配置/文档 dump、管理写接口或后台遥测队列。原有 Go/gRPC/x/net 版本未升级。

诊断 HTTP/1 连接上限 **4**、同步 handler 并发 **2**，不启用 keep-alive 或 HTTP/2。
仅允许无 body、无 query 的 GET。满连接在 Accept 后立即关闭；满 handler 返回 503；
异常路径/方法/body/query 使用固定响应和标签。读 header、整个读阶段、写阶段均 **1 s**，
`MaxHeaderBytes=4096`（Go HTTP/1 解析器另有固定约 4096-byte read slop，不宣称精确 heap）。
读和写阶段可相继发生，不能把 1 s 说成任意连接的统一总寿命。

Prometheus gather/编码在这两个同步 handler 内完成。没有 `http.TimeoutHandler` 或
promhttp Timeout 启动 detached gather。固定 registry 中每个 collector 只取短快照，
不进行网络访问；counter/histogram 每个标签组合预先建立。慢读只占有限连接/handler/
编码响应，写期限到期回收。内核 listen backlog、socket buffers、Go HTTP 固定缓冲不属于
业务 session，也不等同应用等待队列。额度耗尽时诊断自己也会失败，不能承诺无限探测容量。

Node 唯一负责 bind/启动/关闭；端口冲突使 Open 返回明确的 diagnostic startup error，
释放先前的数据 listener、Runtime 和 remote clients。禁用诊断不启动诊断 listener/pump；
计数器仍由已有状态拥有者轻量维护。数据 listener 的独立 Serving barrier 后 Node 才进入 serving。

## Health 与生命周期

- `/livez=200` 只证明当前诊断处理路径可响应；不检查数据库、Store 队列、remote endpoint 或 Guard latch。
- `/readyz=200` 表示静态配置/所有必需 LocalStore 已通过启动验证，数据 listeners 进入服务生命周期，
  Node 没有 drain 或观察到 listener 的致命退出。它不是“所有后端健康”的承诺。
- Open 完成而 Start 尚未调用：内部 readiness=false，listener 只 bind，尚不处理 HTTP。
  Start 后 `constructed -> serving`；意外 listener 退出为 `failed`；Close 从任意状态进入 `draining -> closed`。
- Close 的同一 barrier 先标记 not ready、停止新业务准入；Guard 停止与后端 join 在状态锁外进行。
  诊断保持可用，直到已有有界业务 drain、Runtime/adapter 与 remote cleanup 完成，最后关闭诊断。
  无在途工作时这段可观察窗口可能很短，不人为延迟退出来等待一次 scrape。
- 重复或并发 Close 只计一次 drain；`node_drain_seconds` 包含数据/后端/remote cleanup，
  不包含最后关闭诊断的步骤。关闭后 HTTP 不再可访问，测试仍可 gather 自有 registry 验证终态。
- 继承既有最多 5 s 数据 drain、额外 Runtime worker join/adapter cleanup 上界；不是任意故障下恰好 5 s。
  `transport_forced_closes_total{reason="drain"}` 是 shutdown 到期采取 force-close 的动作，
  `reason="abort"` 是实际首次关闭一个被 watchdog 中断的连接；不推算丢失了多少操作。

## 最终指标清单与准确性

以下名称都带 `weir_` 前缀；没有列出的架构候选指标不属于本轮实现。
所有普通 gauge/counter 为标量，唯有明确标出的静态/枚举标签。

### Node / Guard / 诊断

| 名称 | 类型、标签 | 精确含义/更新位置 |
| --- | --- | --- |
| `node_state` | gauge, `state=constructed/serving/failed/draining/closed` | Node 短锁快照，one-hot。 |
| `node_ready` | gauge | 同一 serving 状态，0/1。 |
| `node_drains_total` | counter | Node 首次 Close，含部分启动回收；不按 caller 重复计数。 |
| `node_drain_seconds` | histogram, seconds | Close 数据资源回收结束观察一次。 |
| `memory_budget_bytes` | gauge, bytes | Guard 取配置与可识别 Linux cgroup v2 memory.max 的较小值。 |
| `memory_sample_bytes` | gauge, `source=unobserved/linux_rss/go_sys_minus_released`, bytes | 既有 100 ms Guard 的最近采样；非当前来源为 0。Linux /proc RSS 可用时使用它；其他平台或读取失败使用 Go Sys-HeapReleased。 |
| `memory_sample_observed` / `memory_latched` | gauge, 0/1 | 是否已经实际采样；80% latch、70% 解除，与业务准入共用判断。 |
| `diagnostic_connections` / `diagnostic_connections_limit` | gauge, connections | 当前诊断连接与 4 额度。 |
| `diagnostic_handlers` / `diagnostic_handlers_limit` | gauge, handlers | 正在处理（含当前 scrape）的请求与 2 额度。 |
| `diagnostic_rejections_total` | counter, `reason=connections/handlers/request` | 诊断自身拒绝。它可随异常诊断请求增长，业务 counter 不随 scrape 增长。 |

内存数字不是 ledger reservation，也不是 RSS 硬限制。macOS 本次只验证 Go 降级信号。
RSS 之外的 OS/driver/GC/socket 余量不能从这些值推导出来；没有新增第二套采样或健康评分。

### 入站与 RemoteWeir

| 名称 | 类型、标签 | 精确含义/更新位置 |
| --- | --- | --- |
| `ingress_connections` / `_limit` | gauge | application/peer 共用 Admission 的连接保留/上限。 |
| `ingress_sessions` / `_limit` | gauge | 从解码前准入直到 HTTP、RPC 和既有 pump 都结束的会话保留/上限。 |
| `admission_rejections_total` | counter, `reason=connections/sessions/draining/overload/ingress/method/route/permission/operation/hop` | 对应入站额度、drain/latch、metadata 信任校验、未知 wire path、resource 路由、授权、协议操作校验、forward budget 的拒绝分支。不是所有业务失败的总数。 |
| `remote_relays` / `_limit`、`remote_sockets` / `_limit` | gauge, `service` | 配置 Service 的 relay 保留与上限、socket 额度与上限 2；socket 包括尚未完成 dial 的保留。 |
| `remote_connectivity` | gauge, `service`, `state=IDLE/CONNECTING/READY/TRANSIENT_FAILURE/SHUTDOWN` | gRPC ClientConn 当前连接状态，纯读取，不触发 Connect；IDLE 不代表数据库不可用。 |
| `relay_rejections_total` | counter, `service`, `reason=relay/socket` | 本跳 relay 满额或固定 socket 额度耗尽。 |
| `relay_terminations_total` | counter, `service`, `method=Read/Mutate/Bulk/Scan/Native`, `status=ok/canceled/deadline/non_ok` | 成功取得 relay 额度后，本跳 forwarding 函数返回一次；包括下游错误和向上游 Send 错误。 |
| `relay_incomplete_total` | counter, `service`, `method=Bulk/Scan/Native` | 下游缺 End、End 后非 EOF/非 OK 或额外 frame 的已确认终态校验事件；其他非法 frame/count 仍由 non_ok 表达。 |
| `rpc_completions_total` | counter, `listener=application/peer`, `method=Read/Mutate/Bulk/Scan/Native/other`, `status=ok/canceled/deadline/non_ok` | gRPC stats.End；OK 可以包含业务 failure envelope，不等于 DB 成功或客户端交付。 |
| `transport_failures_total` | counter, `listener`, `method`, `phase=input/output` | creditedBody 实际非 EOF read error 或 deadlineWriter Write/Flush error，每个 admitted RPC 每方向至多一次。包括取消/本地中断，不根据 error 文本猜原因。 |
| `watchdog_expirations_total` | counter, `listener`, `phase=open/input_or_result/output` | 既有 Bulk 明确 timer 分支，`input_or_result` 无法区分用户与后端，不伪装纯 input stall；没有把所有超时猜成停滞。 |
| `transport_forced_closes_total` | counter, `listener`, `reason=drain/abort` | 上述实际强制关闭动作。 |

unknown Store 和任意 URI 只进入固定 route/permission bucket；未知 wire method 进入固定 method
拒绝计数，不能注册新的 method 标签。无证书/失败 TLS handshake 在 HTTP/RPC 之前，当前没有
独立失败原因 counter；连接容量仍受保护，不能假称已解析到对应业务调用。

### LocalStore

以下指标均使用静态 `store` 标签，与配置 Service 所拥有的唯一 LocalStore 一一对应。

| 名称 | 类型、额外标签 | 精确含义/更新位置 |
| --- | --- | --- |
| `store_pending_entries` / `store_pending_reserved_bytes` | gauge, entries/bytes | `queue` 和 `pendingBytes`；Scan 的唯一 continuation 预留从准入一直保留至 cleanup/consumer 都释放，包含它正在 fetch、emit 或 cleanup 的时段，不能当作纯等待队列长度。 |
| `store_pending_entries_limit` / `store_pending_reserved_bytes_limit` | gauge | 对应账本额度。 |
| `store_result_reserved_entries` / `store_result_reserved_bytes` 及各自 `_limit` | gauge, entries/bytes | `live` 和 `resultBytes`；准入时即预留，包含尚未完成的工作。 |
| `store_retained_results` / `store_retained_result_reserved_bytes` | gauge, entries/bytes | `live` 中 state=ready 的记录结果、Native End 或 Scan page，以及这些 ready 项对应的 ResultBytes 预留；不是全部 result reservation，也不是实际 payload/heap 字节。 |
| `store_active_executions` / `store_window` / `store_window_limit` | gauge, executions | 当前 active 许可、C、Cmax。Native 在上传/后端/交付整个 exchange 期间持有许可，active 不是精确 DB socket 数。 |
| `store_live_sessions` / `store_live_sessions_limit` | gauge, sessions | Scan/Native 共用 live session，占用 0/1，上限 1。 |
| `store_scan_sessions` / `store_native_sessions` | gauge, sessions | 当前 live session 的类型。 |
| `store_scan_page_reserved_bytes` / `store_native_reserved_bytes` | gauge, bytes | plan.PageBytes 的预留工作集，不是实际 heap/当前 page 文档长度；包含 cancellation 后仍被消费方借用的保留。 |
| `store_scan_pages` / `store_scan_cleanups` | gauge, 0/1 | 已存在的一页/正在 cleanup 的状态。 |
| `store_cooldown` / `store_draining` / `store_closed` / `store_overloaded` | gauge, 0/1 | Runtime 短锁快照；没有额外 control loop。 |
| `store_feedback` | gauge, `feedback=unobserved/healthy/congested/neutral` | 最近一次完成物理执行的 adapter/controller 输入证据，one-hot；不承诺当前 DB 健康，也不表示 stale feedback 被 controller 接纳。 |
| `store_rejections_total` | counter, `reason=draining/overload/canceled/session/live_session/budget/capacity/prepare` | Prepare/StartScan/StartNative 与 Submit 的拒绝决定；Bulk 的 capacity 分支不计入，以免把正常 Changed 等待计作拒绝。overload 等分支按实际拒绝决定计一次，不是去重后的逻辑请求数。 |
| `store_records_total` | counter, `operation=read/mutate`, `outcome` | `completeLocked`，仅已准入记录一次终态。read 为 success/failure；mutate 为 applied/not_applied/not_started/unknown/invalid。拒绝、Scan、Native 不混进记录数。 |
| `store_executions_total` | counter, `kind=record/scan/native` | 实际调用 Adapter.Execute/FetchScan/ExecuteNative 前一次，指物理 adapter 调用，不等于网络包/driver 内部命令数。 |
| `store_record_batch_operations` | histogram, operations | 每次 record Adapter.Execute 的 plans 数；`_count` 是物理记录批次，`_sum` 是它们包含的逻辑记录数。 |
| `store_queue_wait_seconds` | histogram, `kind=record/scan/native`, seconds | 每个实际派发项从准入/AdvanceScan 到 dispatch，包含排序、收集与许可等待；队列取消的未派发项不进入 histogram。 |
| `store_execution_seconds` | histogram, `kind=record/scan`, seconds | record batch 或 Scan fetch 的 Adapter 调用耗时；包括 adapter 内部工作，不包含网络交付等待。 |
| `store_native_exchange_seconds` | histogram, seconds | Native Adapter 调用，明确包含客户端上传/下载等待，不命名为 backend latency。 |
| `store_window_changes_total` | counter, `direction=increase/decrease` | feedback 处理前后 C 的实际变化；每物理执行至多一次，不按批内 item 重复计拥塞；stale 和 C=1 的拥塞不计 decrease。 |
| `store_native_completions_total` | counter, `completion=not_started/response_complete/response_incomplete/invalid` | 已准入 Native 在 completeLocked 的实际 NativeEnd，一次；拒绝前的 NativeEnd 通过拒绝计数表达。 |
| `store_scan_terminations_total` | counter, `result=exhausted/failure/interrupted` | 一次 cleanup 后计终态；native page 正常耗尽且 cleanup 成功为 exhausted；page/cleanup failure 为 failure；没有 terminal page 即关闭为 interrupted。 |

Bulk 沿用既有准入等待逻辑；极短 overload 置位/解除竞争中，同一未准入项可能重新尝试，
对应 overload denial 是新的准入决定。拒绝 counter 不是逐请求终态计数，不能拿它与 records_total 相加。

所有 bytes 账本仅表示已预留额度。pending、active、ready 可以帮助定位积压，但由于 Scan
保留 continuation 与 result 的提前保留，不能把这些 gauge 相加推导互斥阶段总数。
Scan 即使已本地耗尽也可能丢失 End；Native RESPONSE_COMPLETE 只说明本节点看到的原生响应
完整性。此前记录 APPLIED 不能因外层传输失败撤销。纯 transport timeout 不自动增加 DB UNKNOWN。

## Series 上界与内存边界

时间 histogram buckets 固定 `{.001,.01,.1,1,10}` 秒；drain 为 `{.01,.1,1,5,10}` 秒；
批大小为 `{1,2,4,8,16,128}`。每个时间 histogram 含 `+Inf` bucket、sum、count，共 8 series；
批大小共 9。没有 native histogram、exemplar、动态 buckets 或动态 label set。

- Node + Guard：21 series；共享 Admission：14；启用诊断自身：7。
- 每个数据 listener：41（最多两个）。
- 每 LocalStore：113（25 scalar gauges + 4 feedback，27 counters，57 histogram series）。
- 每 RemoteWeir Service：34（4 occupancy/limit + 5 connectivity + 20 termination + 3 incomplete + 2 rejection）。
- 合法静态配置有 `L+R<=16`，`D<=2`：总数为 `42 + 41*D + 113*L + 34*R`，
  最大 **1932 series/Node**。关闭诊断时没有 HTTP exposition；registry 不注册那 7 个诊断 series。

额度上界来自配置验证与预创建标签，而不是按请求淘汰 label cache。最长 Store/Service 名称
仍由既有规范限制。Counter 数值可增长；finite series 不等于有限整型可永久表示任意计数，
Prometheus float64 在极大计数上有精度限制。进程重启 counter 重置，没有持久收据或恢复 API。

每次 gather 的响应大小随这个固定图有界；最多两个同时编码的响应。Prometheus 内部可为
collector 启动有限 gather workers，不会每个业务操作创建 goroutine。没有业务操作 goroutine/
telemetry queue，Runtime 锁内不做 HTTP 编码/网络输出/等待 scrape。Snapshot 先释放 Store 锁，
再发送 metrics。跨 Store / listener / Guard 的 snapshot 不是全进程原子快照。

## 验收方法、真实证据与复现

平台 macOS arm64、Go **1.27.0**、grpc-go **1.79.3**、protobuf **1.36.11**、
MongoDB driver **2.9.1**、x/net **0.55.0**；隔离后端 MongoDB **8.0.32**、mongosh **2.6.0**、
Elasticsearch **8.17.0**、OpenSearch **2.19.0**。仅新增固定 Prometheus client 及其依赖。
没有读取用户已有身份/凭据；peer CA/私钥由测试创建并归测试 fixture 所有。

开始前在 `482887e6` 核实干净 main，运行离线 test/race/vet 和两个真实 integration profiles 的 race 基线。
基线日志 `.testdata/m6/baseline-elasticsearch.log`、`baseline-opensearch.log` 均通过。
没有发现需要先修复的阻塞性数据平面回归。

默认测试只使用内存 adapter、临时 loopback sockets 和测试生成身份，不访问 MongoDB/Search。
断言使用标准 Prometheus pedantic registry、promhttp 和 `expfmt.TextParser`，不是响应字符串匹配。
业务指标与诊断自身的拒绝指标分开；测试不以 polling 差值实现事件 counter。

| 验收点 | 对应证据 |
| --- | --- |
| Open/Start/drain/closed、满载/overload/断连不误判 ready、forward-only、多 registry | `TestDiagnosticsLifecycleIsolationAndNoSyntheticExecutions`；两 Node 完全独立，真实未解码 RPC 占满 session 时诊断仍响应；`TestDiagnosticsDisabledAndFatalListenerReadiness`。 |
| 已知批次/记录/拒绝/AIMD | `TestMetricsExactBatchOutcomesAdmissionAndAIMD`：3 项一个 physical call，APPLIED/NOT_APPLIED/UNKNOWN 各 1，queued cancellation 的 NOT_STARTED=1；一次容量拒绝，5 次 Bulk 容量等待不重复计拒绝；过期 epoch/Cmin 不伪计减窗。 |
| collector 只读/高基数/输出不泄漏 | 重复 scrape，100 个固定种子的随机 Store/URI/request ID/method 输入，series 不增长；标准解析后检查输出不含测试 payload/secret/identity/endpoint。 |
| Scan/Native 保留和释放 | `TestNativeSharesLedgerSessionAndC1` 同时核对 parsed metrics；Native 取消完整释放、不会产生 DB 降窗；Scan continuation 与工作集保留一致。原有全部 Scan/Native 清理和背压回归继续运行。 |
| 诊断连接/handler/期限 | 4 个慢 header 耗尽连接，额外连接立即关闭；两个真实 stopped-reader scrapes 耗尽 handler，第三个请求快速 503；恢复后额度归零；超大 header、带 body/query/异常请求有界。 |
| race / repeat Close / partial Open | `TestDiagnosticsScrapeCloseRace`、并发负载/取消/registry gather；诊断端口冲突连续三次回收先前 listener，旧 partial adapter/listener 测试继续通过。 |
| 最大静态配置 | `TestDiagnosticsMaximumStaticSeries` 对真实隔离 MongoDB 构造 16 LocalStore、两个数据 listener；HTTP scrape 恰好 1932 series，execution=0，所有 Store overload 不降低 readiness。 |
| 两跳归属 | 离线 `TestMetricsTwoHopsCountOnlyFinalExecution`：C 的 execution=1，A/B 各 relay=1，A/B 没有 LocalStore metrics。 |
| 三后端真实 CRUD/Bulk/Scan/Native | `TestPeerRealFiveRPCs` 在 MongoDB、ES、OS 分别以 0/1/2 hops 运行；各次 CRUD/Bulk records=40、physical record calls=40、Native complete=1、Scan exhausted=1；每个参与 relay 的五 RPC 调用总计 11。 |
| 已确认写入后丢响应 | 原 `TestPeerRealAcknowledgedReplyLossNoReplay`：DB leg 本地 UNKNOWN，peer/application leg 本地 APPLIED，真实效果与恰好一次后端写入保持；remote 仅记录自己实际看到的状态。外部 fault proxy 在本节点已成功返回后丢弃回复，本节点可能仍观察 OK，不能编造传输失败。 |
| 确认本节点输出失败与 APPLIED 独立 | 原 `TestUnaryAppliedMutationLosesBlockedReply`：真实 Create 已生效，HTTP/2 zero window 使响应 reset；scrape 精确 APPLIED=1、physical execution=1、Mutate output failure=1，不重放。 |
| 三个独立 Weir 进程 | `TestIndependentWeirProcesses` 运行 Mongo/Search CLI 示例，真实诊断 HTTP 确认 C records=8、A/B relays=8，各 forwarding-only 进程没有假 Runtime。原逐节点 SIGTERM/替换与后续 Read 恢复仍通过。 |
| SIGTERM 先降 readiness | `TestDiagnosticProcessSIGTERMReadinessBeforeExit` 保留一个输入等待 RPC，SIGTERM 后实际 HTTP `/readyz=503`、`/livez=200`、draining=1，再有界退出。race binary 包括 race runtime 的退出等待；不能当作普通 binary 最小退出时延。 |
| scrape 与业务同时运行 | `TestMetricsConcurrentScrapesBusinessAndCancellation` 两跳、两个有限 scrape workers 与请求/取消并行；三轮重复的九个 GC 后采样 baseline/current goroutines 均 27，pending/active/retained/result bytes/live sessions 最终均 0。 |

```sh
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
GOPROXY=off GOSUMDB=off go test -race \
  ./internal/app ./internal/server ./internal/store ./internal/overload \
  -run 'TestDiagnostics|TestMetrics|TestNativeShares|TestGuard' -count=3 -v

# 仅使用这些带所有权检查的隔离 fixture。
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch
scripts/search-local.sh start opensearch
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch \
  scripts/test-integration.sh -race -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=opensearch \
  scripts/test-integration.sh -race -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
  go test -race -tags integration -p 1 ./internal/app ./internal/server \
  -run 'TestDiagnosticProcessSIGTERM|TestDiagnosticsMaximumStaticSeries|TestPeerRealFiveRPCs|TestUnaryAppliedMutationLosesBlockedReply' \
  -count=3 -v
```

Integration runner 串行 packages，避免 MongoDB server-global failpoint 相互干扰。两套 profile
均实际执行 MongoDB；Search 必须显式选择产品，未选择不代表产品验收通过。所有数据库、
index、代理和 peer identity 都由 fixture 创建/清理，无生产访问。

本轮开发时两处原有 transport 测试直接构造空 Server，新增 owned metrics 后暴露 nil fixture；
已把 fixture 改为正常构造 Admission/transport metrics，没有给生产代码加入测试替换函数或 nil fallback。
后续全部结论以修正后的最终 PASS 日志为准。

## 原有背压验收计时假设的修正

最终完整集成的一轮出现 `TestPeerBulkBackpressureAndDrainEveryHop` 失败：第 80/160 ms
发送数分别为 1672/1961、2264/3675，原断言认为输入无界，但当时 Runtime retained 均为 4、
pending=0、active=0。该断言没有等待固定 wire buffers 填满，也没有证明生产者仍存活。
失败原始日志保留为 `.testdata/m6/elasticsearch-before-backpressure-check.log`，没有丢弃。

为区分数据平面回归与测量问题，使用 `git archive 482887e6` 导出隔离基线源码，未重置工作树：

- 原始基线与当前版本的该测试在 `GOMAXPROCS=1 -race -count=10` 都通过；这不能证明时间假设可靠。
- 仅在基线副本的测试 producer 加入每 50 帧 5 ms 的发送节拍，三种 drain 位置都出现相同误报
  （851/1501、751/1401、851/1551），所有 retained 仍为 4。没有改基线生产代码或传输额度。
- 正式测试把验证改为：在 1.5 s 有限窗口内找到连续 100 ms 无推进的平台，再额外观察 100 ms，
  发送数必须完全相等、retained<=8，而且 producer 不能已因取消/watchdog 退出。
  测试专用 Stall 从 500 ms 调至 2 s，保证平台证明先于 watchdog；原 70 ms drain 和 1 s shutdown
  断言保持。没有修改生产调度、执行、重试、流控或完成语义。
- 当前版本新断言连续 `-race -count=10`（30 个 drain 子场景）通过；原始基线副本保留慢 producer
  并使用相同平台断言后也 `-race -count=3` 通过。

复核日志：`baseline-backpressure-singlecpu.log`、`current-backpressure-singlecpu.log`、
`baseline-backpressure-paced.log`（预期失败）、`baseline-backpressure-paced-stable.log`、
`final-backpressure-race10.log`，均在 `.testdata/m6/`。基线副本和小型校验脚本保留为本轮忽略的证据。
这是补强验收测量边界，不把真实数据回归改名成可观测性问题。

## 最终验证与本地收尾

最终结果基于包含 ready-result 字节预留指标的实现，最大静态图为 1932 series。
以下日志位于本机忽略目录 `.testdata/m6/`；命令及复现方式见前文，日志未纳入 Git。

| 最终检查 | 结果与日志 |
| --- | --- |
| 默认离线 tests / race | PASS，`final-offline.log`、`final-offline-race.log`。 |
| 默认 / integration-tag vet | PASS，无诊断，`final-vet.log`、`final-vet-integration.log`。 |
| 诊断、计数、Guard、Native 账本专项三轮 race | PASS，`final-diagnostics-race3.log`；九个负载后采样 goroutines=27，ready bytes 和其余账本均归零。 |
| MongoDB + Elasticsearch 完整 integration profile | PASS，`final-elasticsearch-race.log`；最大图 1932、真实三进程和全部既有故障回归均执行。 |
| MongoDB + OpenSearch 完整 integration profile | PASS，`final-opensearch-race.log`；独立选择 OpenSearch，未以 Elasticsearch 结果替代。 |
| 最大图 / SIGTERM / 真实五 RPC / APPLIED 后输出失败三轮 race | PASS，`final-real-diagnostics-race3.log`；最大图三轮均为 1932，race 子进程 SIGTERM 后约 2.104–2.118 s 退出。 |
| 背压平台断言十轮 race | PASS，`final-backpressure-race10.log`；修正依据及预期失败证据见前节。 |
| 格式、变更 Go 文件 AST 风格检查、diff whitespace | PASS，`gofmt -l`、`final-style.log` 与 `git diff --check` 无输出；同时人工复核更新点和标签来源。 |

验证后使用原有带所有权检查的脚本停止本轮启动的 MongoDB、Elasticsearch、OpenSearch。
确认 27028/19200/19201 没有监听，两个 `weir-milestone-2` 容器均 exited，无残留 Weir/test
mongod 进程。测试代理和新生成身份随各自临时 fixture 回收；已有停止容器、标记数据目录、
本轮日志与隔离基线源码保留，未递归删除用户资产。核查记录为 `cleanup.log`。

收尾范围仅为本地提交、快进合并 main、安全删除已合并的 `randy/diagnostics-m6` 分支；
不进行 push、PR、release、生产部署、跨 chat 消息或下一里程碑工作。

## 限制与停止边界

这是有限基本观测，指标不是收据、去重索引、审计账本、结果恢复机制或投递保证。
没有新增管理写/数据库探测/自动调参、多 endpoint、failover、重试、tracing exporter、pprof、
dashboard、告警平台、配置/数据 dump 或用户认证。原有 trace context 转发不等于已实现 tracing。
架构中尚未实施的 telemetry 候选仍是目标设计，本轮不自动创建或实施后续阶段。

只对上述本机固定版本、有限负载与故障模型资格验证。未验证 Linux RSS/cgroup 的运行行为、
生产部署、长期 soak、多机网络环境或进程崩溃后可靠指标交付。memory latch 不是硬隔离；
诊断也有自身容量，不能用于断言“所有后端永远健康”。架构目标契约未变，中英文架构文件保持原样。
