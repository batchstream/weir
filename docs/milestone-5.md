# 第五里程碑：静态可信 peer 组合

本轮从干净的 `1e20380` 开始，在 `randy/peer-m5` 实现。支持 direct local、
A → B → Database、A → B → C → Database、local/remote 混合和 forwarding-only。
沿用五个 `weir.v1` RPC 与现有 protobuf；没有 Forward RPC 或内部 execution-plan 协议。
这仍不是完整 V1，也不是生产就绪。没有推送、PR、发布或生产部署。
实现与测试提交：`b5a3713`（`feat: add bounded authenticated peer forwarding`）。

## 所有权与路径

实现前确定、实现后保持的责任边界：

- `app.Node` 唯一拥有静态 Services、两个 listeners、Runtimes、peer clients 和进程 Guard。
  Server 借用引用，关闭 transport 时不关闭 Runtime。Runtime 唯一拥有 Adapter，
  Adapter 独占数据库 clients/pools。初始化失败释放此前已构造的资源。
- 精确 Store name → Service。Service 是 LocalStore/RemoteWeir 两种明确变体。
  每个 unary 请求或流的 Open 只选一次路由；后续帧不换 Store、Service 或 endpoint。
  LocalStore 继续走同一 `store.Runtime`；RemoteWeir 不构造 Runtime、数据库连接池、
  微批队列或 AIMD，也不解析 BSON/JSON/Native descriptor/body。
- application/peer 使用同一 Server handler、协议验证和 Service 分发；不同之处是 ingress 信任策略。
  两个 listeners 共享进程 session/connection 额度、overload latch 和 drain barrier。
- session 在解码前准入，RemoteWeir 另有 relay 额度。额度保留到 handler、HTTP 交付和
  输入 pump 全部退出，不能在下游返回后提前释放而让慢上游积压结果。
- `internal/overload.Guard` 直接采样进程，向 Admission 与真实 Runtimes 发信号。
  forwarding-only 的 targets 只有 Admission，没有假 Runtime。100 ms 采样，80% 关闭准入、
  70% 恢复；macOS 使用 `Go Sys - HeapReleased` 降级信号，不是 RSS 硬上限。
  Linux RSS/cgroup v2 路径保留但本轮没有 Linux 运行资格。

## 静态配置与两节点运行

旧的 `go run ./cmd/weir`、本地 MongoDB/Search flags 与示例保持可用。
新增 `-config <JSON>`，与任何本地 flag 互斥；没有环境变量覆盖、reload 或远端配置下载。
JSON 上限 128 KiB、嵌套深度 12，拒绝重复对象字段、未知字段、尾随内容、重复 Store/Service、
无效引用、未使用 Service、LocalStore 别名、不合法额度和 identity/grants。
构造双方 transports、验证本地可检查的配置与证书后才 bind listeners。
端口冲突时也会回收先前成功绑定的 listener。

以下是本机开发用 A → B → MongoDB 示例。使用新的临时目录生成一天有效的测试身份，
不需要控制平面或外部身份平台；不要使用生产 CA。需要本机 OpenSSL。

```sh
scripts/mongo-local.sh start
go build -o .testdata/weir ./cmd/weir
peer_demo=$(mktemp -d "${TMPDIR:-/tmp}/weir-peer-demo.XXXXXX")
chmod 700 "$peer_demo"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -days 1 -subj /CN=weir-demo-ca \
  -addext basicConstraints=critical,CA:TRUE \
  -keyout "$peer_demo/ca.private" -out "$peer_demo/ca.crt"
for node in a b; do
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=$node.weir.test" \
    -keyout "$peer_demo/$node.private" -out "$peer_demo/$node.csr"
  cat > "$peer_demo/$node.ext" <<EXT
subjectAltName=DNS:$node.weir.test
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
EXT
  openssl x509 -req -days 1 -in "$peer_demo/$node.csr" \
    -CA "$peer_demo/ca.crt" -CAkey "$peer_demo/ca.private" -CAcreateserial \
    -extfile "$peer_demo/$node.ext" -out "$peer_demo/$node.crt"
done
chmod 600 "$peer_demo/"*.private
cat > "$peer_demo/b.json" <<JSON
{
  "peer": "127.0.0.1:7448",
  "identity": {
    "certificate": "$peer_demo/b.crt",
    "private_key": "$peer_demo/b.private",
    "ca": "$peer_demo/ca.crt"
  },
  "allow": [{
    "identity": "a.weir.test", "store": "mongo",
    "operations": ["read", "mutate", "scan", "native"]
  }],
  "services": [{"name": "mongo-local", "local": {"mongo": {
    "uri": "mongodb://127.0.0.1:27028/?directConnection=true",
    "database": "weir_m1", "collection": "records"
  }}}],
  "routes": [{"store": "mongo", "service": "mongo-local"}]
}
JSON
cat > "$peer_demo/a.json" <<JSON
{
  "application": "127.0.0.1:7447",
  "identity": {
    "certificate": "$peer_demo/a.crt",
    "private_key": "$peer_demo/a.private",
    "ca": "$peer_demo/ca.crt"
  },
  "services": [{"name": "mongo-peer", "remote": {
    "endpoint": "127.0.0.1:7448", "server_name": "b.weir.test", "relays": 8
  }}],
  "routes": [{"store": "mongo", "service": "mongo-peer"}],
  "initial_forwards": 4
}
JSON
# Keep each Weir in its own terminal; use the generated absolute config paths.
printf 'Config directory: %s\n' "$peer_demo"
.testdata/weir -config "$peer_demo/b.json"
# Another terminal:
.testdata/weir -config /absolute/path/to/weir-peer-demo.XXXXXX/a.json
# Another terminal:
go run ./cmd/weir-example -address 127.0.0.1:7447
go run ./cmd/weir-native-example -address 127.0.0.1:7447
```

Stop both Weir processes with Ctrl-C, then `scripts/mongo-local.sh stop` when that
owned fixture is no longer needed. The sample shell commands are a reproduction recipe;
qualification uses the independently generated in-process CA described below.
The fixtures create/own their isolated database; Weir does not provision collections/indexes.

For A → B → C, put the LocalStore on C, change B's Service to RemoteWeir(C),
and give C an explicit grant for `b.weir.test`. B still grants A its own allowed scope.
For mixed local/remote, add a second Service and exact Store route to A. For Search,
use `local.search` with `url`, pre-created `index`, and exact `profile`
(`elasticsearch-8.17.0` or `opensearch-2.19.0`); see milestone 2 for fixture setup.
Neither TLS nor the static graph proves that peers agree on the logical Store's database,
collection/index, selectors or Native profile. Operators must configure those consistently.

Defaults may be omitted. `limits` supports `connections`, `sessions`, `unary_ms`, `bulk_ms`,
`scan_ms`, `native_ms`, `stall_ms`; `memory_mib` defaults to 512 (range 64–65536).
Each RemoteWeir requires one explicit IP:port, a verified DNS `server_name`, and `relays` 1–16.
No DNS discovery, load balancing, proxy environment, multiple endpoints or failover is used.
A RemoteWeir referenced by several Store routes shares its client and relay budget.
Local `batch_operations` absent/0 means the existing default 16; 1 makes singleton batches.

## 信任、上下文与期限

application 是明确的 loopback IP 明文开发入口，不允许 `0.0.0.0` 或未认证公网入口。
peer 为独立 TLS 1.3 listener：验证证书链、有效期、客户端身份；client 还验证 server name。
本进程 identity 需唯一 DNS SAN，且证书同时支持 client/server EKU。
每个 peer identity 必须显式映射到 Store 与 read/mutate/scan/native 操作族。
CA 信任不等于授权；Bulk 的 Open 检查 Store，每项再检查操作族和 Store 一致性。
Native 权限独立于 mutate 权限：Native 可以执行已支持的原生写操作。

peer 请求的 `weir-remaining-forwards` 必须恰好一个 canonical `0`–`8` 字符；
缺失、重复、空白、前导零、负数、超范围均拒绝。public 拒绝客户端提供该字段，
内部初始化默认预算 4（可配置 0–8）。每次 remote dispatch 要求 >0 并恰好减一，
收到 0 仍可 local 执行，peer 从不重置预算。A → B → A 环不保存 visited 列表，
测试预算 0/1/4/8 均有界返回 ResourceExhausted。

只转发 hop、一个 1–128 字节可打印 `weir-request-id` 和可选固定格式 W3C `traceparent`。
缺少 request ID 时生成 128-bit 随机诊断 ID；不传 Authorization、baggage、tracestate
或客户端自称身份。诊断字段不参与授权、幂等性或排序；下一跳认证的是当前 Weir 节点。

每跳取原有效 deadline 与本地 method lifetime 的较小值。连接、准入后的等待、上传、
后端调用、下游接收和上游结果发送都消耗同一预算。无客户端 deadline 仍有入口有限期限。
新增测试在两个真实 TLS handshakes 各耗费 60 ms：350 ms 入口 lifetime 到后端剩约
217 ms，调用在约 352 ms 非 OK 终止，没有重启 350 ms。传输最终写期限到达时可能
返回 HTTP/2 RST_STREAM，而非成功交付 DeadlineExceeded trailer；两者都不是执行证据。
取消向下游传播，不能推导数据库回滚。

## 五个 RPC 的行为与证据

| RPC | relay 行为 |
| --- | --- |
| Read/Mutate | 调用同名 unary，保留完整结果和 unknown protobuf fields。已收到 APPLIED 不改为 NOT_APPLIED；请求可能已发送后丢失结果只返回保守非 OK，不从状态码推导 NOT_STARTED。 |
| Bulk | Open 固定目标；最多 8 个在途索引关联，不保存文档。普通协议无效项在本跳返回原 index 的 error，不发往下游；有效项重编号为连续下游 index，结果原地映射回原 index，立即发送，不排序。授权/framing 错误非 OK 终止。验证下游 End/最终 OK/全部关联已交付后，将 End 计数换算为原客户端计数。 |
| Scan | 一次原始 Scan，不持有 cursor/PIT。逐文档转发原字节；检查尺寸/累计数，保留 failure End。先验证 End 后最终 gRPC OK，再转发 End；缺失 End、额外帧或最终非 OK 均失败，不重开遍历。 |
| Native | Open、descriptor、body、Head、completion 保持原义；上传 pump 与接收并行，允许未 half-close 时 early response。Send EOF 不能覆盖完整 early response；Recv 一方决定最终状态。验证 Head/chunk/End grammar 及 End 后最终 OK；不解析原生内容、不恢复 exchange。 |

Bulk 同 key 顺序仍仅由最终 Runtime 对同一 live Bulk 保证，UNKNOWN 后也不能推导
后端完成顺序。不提供跨客户端、跨入口或跨节点的全局顺序。
只有本节点确实没有发起相应下游执行的普通拒绝，才构造 NOT_STARTED。
客户端始终需要验证 End 和最终 OK；上游交付仍可能在收到 End 后中断。

## 每个节点的资源边界

下列为独立额度，不把配置预算误称为进程最大 RSS：

| 项目 | 默认 / 固定限制 |
| --- | --- |
| 静态图 | Services、routes 各 1–16；grants 最多 128 条、peer identities 最多 32 |
| 入站连接与 session | application + peer 共用；默认各 16，可配置 1–64；每 HTTP/2 connection 同时最多 8 streams |
| 出站连接 | 每 RemoteWeir 一个 ClientConn、最多 2 sockets（允许 GOAWAY drain 与 replacement 短暂共存）；固定 endpoint，无额外连接池 |
| live relay | 每 RemoteWeir 显式 1–16，同时受共享 session 限制；无应用等待队列，满额立即 ResourceExhausted |
| 方法 lifetime | unary 默认/最大 30 s；Bulk 15 min；Scan/Native 5 min，可缩短；stall 默认/最大 30 s |
| 消息 | gRPC 最大 frame `F=300 KiB`；document 256 KiB；Native chunk 1–64 KiB，body 每方向最多 8 MiB（最终 Adapter 可更窄） |
| relay 应用状态 | 每方向最多一个待转发 frame；Bulk 最多 8 个小索引条目，另最多一个本跳拒绝结果，无全流聚合 |
| relay pumps | Bulk 固定 3 个：按需读上游、上传下游、按需读下游；Native 固定 1 个上传 pump；Scan/unary 没有额外 relay pump；计时器只在到期调度取消，不为每个 chunk 创建 pump |
| 最终 Runtime | 延续 pending 256 / 8 MiB、results 128 / 16 MiB、C 最大 4、batch 16 / 1 MiB、每 Bulk outstanding 8；Scan/Native 共用每 Store 一个 live session；具体 page/body 预留见里程碑 3/4 |

HTTP/2/gRPC 缓冲单独核实：

- 两种 Server 的 Go 1.27 HTTP/2 receive stream/connection 初始上限均为 65535 bytes，
  frame 16 KiB。源码 `net/http/internal/http2/config.go` 确认 65535 满足 connection 的
  最小值，不会退回 1 MiB 默认值。header list 为配置 16 KiB 加 Go 的固定 320-byte 换算余量。
- gRPC ServeHTTP eager request read-ahead 由 `creditedBody` 限制到 `F+5` bytes/已准入 RPC。
  解码返回 credit 后，允许已有一个应用 frame 和下一帧 wire credit 共存；两者不是同一份额度。
- peer client static stream/connection windows 为 65535，关闭 BDP 动态增长。
  **static 不等于永不临时增窗**：grpc-go `inFlow.maybeAdjust` 在主动 Recv 大消息时可增加
  bounded message delta。因此每条流保守计入 `65535 + F` 接收额度，再加当前解码 frame。
  MaxCallRecvMsgSize 在读 body 前拒绝超过 F 的长度。连接级额度限制网络停读，不能代替逐流额度。
- client read/write buffers 各 16 KiB，接收 header list 16 KiB；grpc 每流 writeQuota 默认
  64 KiB，单次已接收 Send 可超过 quota 一个最大消息，所以另计 `64 KiB + F` 的发送排队上界。
  protobuf 编解码副本、框架对象、HPACK/TLS records、Go allocator 和 OS socket buffers 也存在；
  它们随固定 connections/sessions/message limits 有界，不声称已获得逐字节 RSS 公式。
- HTTP/2 配额等待属于已准入有限调用的 transport 背压，消耗原 deadline。读/写 stall、
  method lifetime、final-status write deadline 共同限制停读。框架 goroutines 随连接和已准入
  streams 增长；应用 pumps 不随累计文档/chunk 数增长。

过载阻止新 logical operation；Bulk 持续到来的项属于新操作，触发 barrier 后半关闭下游、
交付已接收结果并非 OK 终止。已准入 Native body 和 Scan 输出可继续完成/清理。
本机 repeated-cancel 实测所有节点 session/relay 归零，最终 Runtime 的 Pending、Active、
Retained、ResultBytes、Scan/Native sessions/bytes 全部归零。warm 连接仍存在，不能要求
回到尚未建连的 cold goroutine 数。内存采样是有限轮次的稳定性证据，不是长期 soak 或 RSS 证明。

## gRPC 无重放审计与实际故障

固定 grpc-go **v1.79.3**；client 使用 DisableRetry、DisableServiceConfig、
MaxRetryRPCBufferSize(0)、WaitForReady(false)、passthrough endpoint、WithNoProxy。
无应用 retry、hedging、endpoint 切换、读回猜测、流恢复。连接重建只服务未来调用，
dial 2 s，backoff 100 ms–2 s，socket 额度仍生效。

审阅固定版本 `stream.go` 的 `shouldRetry` / `bufferForRetryLocked` 与
`internal/transport/http2_client.go` 的 unprocessed 设置：DisableRetry 检查之前仍有
透明 transport 分支，不能仅凭选项声称没有任何重建。只有 transport 尚未得到 stream、
明确 REFUSED_STREAM 或 GOAWAY(lastStreamID 以下界之外) 的应用未接收证据可触发此分支；
普通断线/Unavailable 不构成该证据。零 retry buffer 在首个非空消息提交后禁止 replay；
Bulk/Native 创建 stream 后立即调用 Context() 明确 commit attempt，不保存历史 frames。
Scan/unary 的首条消息也受零 buffer 限制。

实际 fault proxy **先读到真实后端成功确认，再截断回复或关闭实际 peer socket**：

| 后端 | 丢弃位置 | 普通 Mutate | Native 写 |
| --- | --- | --- | --- |
| MongoDB 8.0.32 | DB → final、final → relay、relay → application | 每个场景 update command=1；独立读取 n=1 | 每个场景 findAndModify=1；独立读取 n=1 |
| Elasticsearch 8.17.0 | 同上三段 | 每个场景 `_bulk`=1；独立 GET `_version=1` | 每个场景 native `_bulk`=1；独立 GET `_version=1` |
| OpenSearch 2.19.0 | 同上三段 | 每个场景 `_bulk`=1；独立 GET `_version=1` | 每个场景 native `_bulk`=1；独立 GET `_version=1` |

客户端保持 UNKNOWN / RESPONSE_INCOMPLETE / 保守非 OK（依故障段而定），
没有第二次 backend command。独立读取只在测试中发生，生产路径没有 read-back。
各场景的 command/ack 断言和日志位于 `TestPeerRealAcknowledgedReplyLossNoReplay`。
这证明已覆盖故障模型下不重放，不把任意网络故障、恶意可信 peer 或数据库行为纳入证明。

## 关闭与清理

`Node.Close` once：先停止共享新准入，停止 Guard、各 Runtime BeginDrain，
在原 deadline 与最多 5 s drain 内并行等待 transports 与 Runtimes。
Bulk 不等待无限 producer half-close；已读下一帧但未准入的操作没有下游执行，
流以非 OK 表达不能完整核对剩余结果。到期关闭 transport、取消下游，join 固定 pumps。
Server.Close 不碰 Runtime；之后装配层关闭 listeners 与各 RemoteWeir，Runtime 关闭 Adapter 一次。
Runtime 原有独立清理允许额外最多 3 s worker join 与 Adapter 的 2 s close；因此整体最坏
清理预算不是简单的 5 s。真实 fixtures 在更短范围内完成，不能承诺所有故障下 5 s 退出。
部分初始化失败（第二个 listener 占用、第二个真实 Adapter 失败）释放已成功构造资源。
退出/断连不把未收到的 mutation 结果当作未执行。

## 验收与复现记录

平台 macOS arm64、Go **1.27.0**、grpc-go **1.79.3**、protobuf **1.36.11**、
MongoDB driver **v2.9.1**、x/net **v0.55.0**；实际后端 MongoDB **8.0.32**、mongosh **2.6.0**、
Elasticsearch **8.17.0**、OpenSearch **2.19.0**。未修改依赖版本或架构目标契约。

开始扩展前已运行离线 test/race/vet 与两个完整 integration profiles 的 race 回归。
包括 unary input/output/final status、Scan page/C=1/cancel/cleanup、Native early response/
slow consumer/no replay、CRUD/Bulk UNKNOWN/order。基线全部通过后开始实现。

最终结果与日志在本机忽略目录 `.testdata/m5/`；可使用以下命令重现（不依赖这些日志文件）：

```sh
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
GOPROXY=off GOSUMDB=off go test -race ./internal/server -run '^TestPeer' -count=3 -v
# Start only isolated, owned fixtures first.
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch
scripts/search-local.sh start opensearch
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -race -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=opensearch scripts/test-integration.sh -race -count=1 -v
# Repeat real peer cases separately for each selected Search profile.
GOPROXY=off GOSUMDB=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
  go test -race -tags integration ./internal/server -run '^TestPeerReal' -count=3 -v
GOPROXY=off GOSUMDB=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=opensearch \
  go test -race -tags integration ./internal/server -run '^TestPeerReal' -count=3 -v
```

Integration runner 串行 packages，避免 MongoDB failpoint 干扰；MongoDB 在两个 Search profiles
均实际运行。默认 suite 不访问后端，仅用本机 ephemeral sockets 与进程内测试 CA，
不读用户已有证书/密钥/环境凭据。

| 验收组 | 实际测试 |
| --- | --- |
| 身份/权限/hop | 无证书、不可信 CA、错误服务端 SAN、同 CA 未授权身份、Store/操作族/逐 Bulk 项、public spoof、全部非法 hop、zero local、环路，均由真实 TLS/HTTP2 验证。 |
| direct/1/2 hops | 三种真实数据库分别执行五 RPC、opaque bytes、同 key Bulk、End/count/status；离线另验证 41 项夹杂无效项与故障 peer 的乱序结果关联。 |
| Native | 两跳真实 ES/OS header-limit early response，idle 与持续上传均在 half-close 前完成；Mongo/ES/OS 两跳 200 KiB 慢下载验证 stall/lifetime；离线双向阻塞与上传停读。 |
| Scan | 两跳真实 Mongo/ES/OS 大小超限第二页，先前文档保留、failure End 与 final OK 正确；离线第二页故障断言只 fetch 两次、cleanup 一次，不重开。 |
| Deadline | 原始客户端 180 ms deadline、取消和 metadata 过滤；另两个真实 handshake 共耗时约 134 ms，350 ms 入口期限不重置。 |
| Fault terminal | 缺少 End、End 后非 OK、额外 frame、错误计数均非 OK；完整终态不会由 relay 凭空合成。 |
| 有界性 | 每段 65535 初始窗口小于 200 KiB 响应；上游停读、下游停读、双向阻塞、Bulk 持续 producer 背压停止；分别 drain 三个节点，producer 无需 half-close。 |
| 资源 | 12 轮取消采样全部 logical 额度归零；共享 listener session/connection、Remote relay 上限、真实 Guard 过载、blocked remote 与 local 隔离、断线后新调用恢复、重复 Close、部分初始化失败。 |
| 独立进程 | `TestIndependentWeirProcesses` 构建 race binaries，在独立 A/B/C 进程经 mTLS 执行 Mongo/Search 两个 CLI 示例；分别 SIGTERM C/B/A、等待退出、在同端口启动 replacement，原有 clients 的后续独立 Read 恢复。不是在途调用恢复。 |

2026-09-26 至 27 日完成的最终验收：

| 命令/范围 | 结果 | 本机日志（`.testdata/m5/`） |
| --- | --- | --- |
| `go test ./...` | PASS | `final-offline.log` |
| `go test -race ./...` | PASS | `final-offline-race.log` |
| `go vet ./...`、`go vet -tags integration ./...` | PASS，无诊断 | `final-vet.log`、`final-vet-integration.log` |
| 全部离线 `TestPeer* -race -count=3` | PASS，13.7 s | `final-peer-race3-corrected.log` |
| 装配层并发 Close / 部分初始化 `-race -count=3` | PASS | `final-assembly-race3.log` |
| 两 listener 共用连接/session/relay 额度 `-race -count=3` | PASS | `final-shared-admission-race3.log` |
| MongoDB + Elasticsearch 完整 integration `-race` | PASS，含三独立进程及逐节点重启 | `final-elasticsearch-race-corrected.log` |
| MongoDB + OpenSearch 完整 integration `-race` | PASS，含三独立进程及逐节点重启 | `final-opensearch-race.log` |
| MongoDB + Elasticsearch 全部 `TestPeerReal* -race -count=3` | PASS，22.5 s | `final-peer-real-elasticsearch-race3.log` |
| MongoDB + OpenSearch 全部 `TestPeerReal* -race -count=3` | PASS，22.3 s | `final-peer-real-opensearch-race3.log` |

三轮离线取消验证共九个 GC 后采样，HeapAlloc 3.9–8.6 MiB、goroutines 均 27，
建连前 cold baseline 为 15；三条固定网络连接解释了 warm 增量，轮次没有持续增长。
三个节点各自的 Bulk 停读样本在 80/160 ms 两次检查中发送数不再增加
（4424–4544 个很小的请求已占用沿途固定 wire buffers；不是应用 frame 队列），
drain 后所有会话与结果额度归零。每次 TLS setup 样本约 134 ms，后端剩余约 217 ms。

测试开发时曾把“入口期限到期”错误地只接受 DeadlineExceeded，而实际 HTTP/2 写期限
可以先产生 RST_STREAM；修正为验证有限时间内非 OK，并保留对两跳实际剩余期限的检查。
上表只引用修正后完成的 PASS 日志。AST 风格检查未发现本轮新增的 inline struct
return/argument、超过四参数函数或超过三参数的 inline-if 调用；原有 Search 测试中的
四处既有 inline-if 调用未在本轮修改。`git diff --check` 通过。

验收后停止本轮启动的隔离 MongoDB、Elasticsearch、OpenSearch；保留带所有权标记的
fixture 数据目录、停止状态容器和测试日志，以便复现。独立 Weir 进程、代理、临时 CA/私钥
由测试 cleanup 回收，没有读取用户现有身份或生产凭据。

## 未支持范围与剩余限制

仅每 Service 一个静态 endpoint。没有多 endpoint 选择、failover、动态发现/reload、mesh、
provider、跨 Store fanout/事务、全局排序、远端 batching/DB 调度、完整用户认证或 Native/Scan
能力扩展。AtomicTransform 仍为 UNSUPPORTED。可信 peer 必须遵守相同 Store/profile 契约，
证书轮换、吊销策略和生产身份发行不在本轮范围。

连接共享使一次必要的强制 close 可能截断同连接其他 RPC；它们必须保守处理缺失结果。
已执行与响应交付之间仍有不可消除的不确定性。内存 Guard 是准入保护、不是硬隔离，
需要结合实际预算/工作负载；本轮只验证本机有限轮次故障，无长期负载、生产网络、
多节点数据库、跨可用区故障切换或 Linux 运行资格。
