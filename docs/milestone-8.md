# 第八阶段：可信内网明文入口与 peer

日期：2026-09-27。起点：`cd2523beda02810ef733a831ba6719bbd75e71bd`，干净 `main`。
按用户最新决定删除 Weir 自身身份、权限及专用 TLS/mTLS，直接在本地 main 实现与提交。
结束 SHA 以本阶段提交记录及交付消息为准，提交内容不自引用自身 SHA。
本阶段不是整个 Weir 的生产就绪声明；不 push、PR、发布、部署或启动下一阶段。

## 删除和保留的责任边界

- 删除 `Identity`、`Grant`、`Permission`、`PeerPolicy`、grants/Allow、`authorize`、
  操作族权限映射、`remote.server_name`、证书文件读取/链校验/装载与 TLS server/client 配置。
  删除仅为身份存在的 `internal/testpeer/identity.go`；所有 transport/fault/process fixtures
  改为明文，不创建或读取测试证书。无 disabled/trusted 开关、空授权函数或兼容模型。
- application、peer 和 RemoteWeir 使用同一个 `weir.v1` 五 RPC 协议及明文 HTTP/2 gRPC。
  `server.Config.Peer` 只选择真正的 hop 入口规则，不是认证功能开关。
  没有 Forward RPC、协议身份字段、backend 文档转码或 runtime/调度器副本。
- 保留严格 Store/URI/operation/envelope 验证、表达式媒体及大小限制、固定路由/Service/端点、
  bounded request ID/traceparent、UNKNOWN、无自动重放和完整 End/final-status 规则。
  Protobuf 与生成代码逐字不变；已有 UNAUTHENTICATED/PERMISSION_DENIED 枚举保持 wire 编号，
  不代表存在 Weir 认证实现。
- 保留数据库 driver 的标准 TLS/凭据行为。Mongo `nativeNoReplay`、driver Auth 的
  reauthentication/replay 排除、Search Native 的禁止 auth/Host override 仍在原位置。
  不修改后端 profile，不关闭证书校验，不读取 `.env`、已有证书/私钥或凭据。
- 保留唯一 Store Runtime/scheduler/ledger、relay credits、connection/session/stream/byte/result
  额度、过载 Guard、取消/drain/backpressure。删除 permission 指标标签后，共享 Admission
  为 13 series；最大静态图是 `41 + 41*D + 113*L + 34*R = 1931`，`D<=2,L+R<=16`。
  真实最大图测试重新核对 1931，执行计数没有因指标采集或 relay 重复。

这不是“零 TLS 字符串”的清理。数据库及 Go/gRPC 标准依赖可能包含 TLS，历史 M5–M7
保留当时真实 mTLS 资格及旧命令，并在开头注明被 M8 取代。只删除 Git 跟踪的专用身份源码，
没有搜索、打开或删除既有秘密、用户证书、历史 fixture 资产和诊断日志。

## 部署前提、配置和 hop

Weir 无密码学身份、传输加密或操作权限控制。部署网络隔离负责两个入口的可达来源。
能访问 peer 的调用方能提供规范 hop；不能宣称 hop 来源已认证，也不宣称未隔离公网可用。

- CLI 默认 application `127.0.0.1:7447`。静态配置显式选择 application/peer 地址。
  两者接受显式 IP:port，包括内网地址、`0.0.0.0`、`[::]`。所有测试实际只绑定 loopback；
  通配/非 loopback 只做纯配置验证，没有开放公网、修改防火墙、网络或 sysctl。
- 端口必须为十进制 0–65535；RemoteWeir 端口不能为 0，仍是一个固定 IP:port。
  Diagnostics 仍默认关闭、仅 loopback，本阶段没有扩大探针部署范围。
- 固定端口的重复 IP、IPv6 等价表示、通配重叠、与 diagnostics 的冲突在 Validate 拒绝。
  多个端口 0 各自申请独立 ephemeral listener；装配按入口位置区分 application/peer，
  不再靠地址字符串相等来决定规则。所有 bind 失败仍回收此前已构造的资源。
- 配置仍限 128 KiB、深度 12、1–16 Services/routes，拒绝重复/未知字段、无效引用、
  LocalStore 别名、未使用 Service 和无效资源额度。旧 `identity`、`allow`、
  `remote.server_name` 明确返回 `json: unknown field ...`，不默默忽略。
- application 拒绝任何 `weir-remaining-forwards`，内部创建默认 4/范围 0–8 的预算。
  peer 必须恰一个 canonical 单字符 `0`–`8`；缺失、重复、空白、前导零、负数及超限拒绝。
  每次 remote dispatch 恰好减一；0 可 local，不能继续 forward；peer 永不重置。
- 每次转发替换 outgoing metadata，只带一个 1–128 byte 可打印 request ID、可选固定格式
  traceparent 和 hop；不累计原 metadata，不带任意 baggage 或 Authorization。
  request ID 是诊断值，不是身份、幂等键或排序域。

当前两节点可运行配置见 `examples/peer-a.json`、`examples/peer-b.json` 和 README。
这些示例使用原 owned `weir_m1.records` fixture，不需要 OpenSSL 或证书辅助工具。
多跳保持逻辑 Store/backend profile 一致性是部署责任，Weir 不协调错误配置。

## 明文传输边界

继续使用固定 Go 1.27 的 `net/http` unencrypted HTTP/2 + gRPC ServeHTTP，删除 ServeTLS 分支。
不是以 handler timeout 代替传输寿命：

| 阶段 | 实际界限 |
| --- | --- |
| 无数据或 partial preface | 原生 `ReadHeaderTimeout=min(5s, Stall)`，在 HTTP/2 handler 前生效 |
| SETTINGS、partial frame/header、未结束 CONTINUATION | 原生 HTTP/2 `SendPingTimeout=Stall`、`PingTimeout=min(5s, Stall)`；不完整 frame/header 不产生完整 frame 进度，PING ACK 也不能穿过未完成帧，超时关闭连接 |
| 正常 idle connection | 仍为 1 min idle；有界 PING 是 transport 活性检查，不是 DB probe/重试 |
| partial gRPC prefix/body | 保留 `creditedBody` 与 stream read deadline；实际进度只续 stall，不重置原 method lifetime；解码后不再用输入 stall 取消后端 |
| 收发与 final status | 原 method lifetime、response stall、HTTP/2 stream read/write deadlines 和 WriteByteTimeout；发送结束前不释放 session/result/relay credits |
| 连接/窗口 | application+peer 共用默认各 16 connections/sessions、可配置 1–64；每连接 8 streams；两端初始 connection/stream windows 65535，frame 16 KiB |
| relay | 每 RemoteWeir 1–16 slots、1 ClientConn、最多 2 sockets；每方向有限 frame、Bulk 最多 8 关联项；无新等待队列 |

以上依赖固定 Go 1.27 HTTP/2 的完整帧事件和标准 PING 处理；没有自行解析生产 HTTP/2 frame，
没有新 per-connection watcher 或证书替代层。慢输入连接可能因此关闭并截断同连接其他 RPC，
它们只能按缺失结果保守处理。正常应用响应的期限、结果 credit 与 HTTP/gRPC 双完成所有权
仍沿用 [unary 专项](unary-response-deadline.md)。

`TestPlaintextPrefaceAndHeaderLifetime` 在两个入口分别用真实 socket 发送无 preface、partial
preface、缺 SETTINGS、partial frame header、partial HPACK body、连续未完成 CONTINUATION，
以及完整 headers 后无 gRPC 数据、partial prefix、partial body。75 ms Stall 下，前两项约
75 ms、HTTP/2 incomplete header/frame 约 150 ms、partial RPC 约 75 ms 有限终止；客户端
1 s deadline 不能成为关闭证据。检查连接/session 归零且 Adapter 未执行。

原 TLS delay test 改成两个真实 TCP accept 各延迟 60 ms，实际约 123 ms setup 从原 350 ms
入口 lifetime 扣除，后端剩余约 227 ms；无 deadline 调用仍有限非 OK，不从下一跳重新计时。
纯证书/权限拒绝断言删除；原实际传输、背压、取消、回复丢失和进程生命周期均保留并改写。

## 无重放与实际资格矩阵

生产 RemoteWeir 继续启用 DisableRetry、DisableServiceConfig、MaxRetryRPCBufferSize(0)、
WaitForReady(false)、passthrough 固定端点、WithNoProxy、2 s dial 及 100 ms–2 s reconnect backoff。
Bulk/Native 首次建立 stream 后继续 commit attempt；没有 mutation retry、hedging、failover、
read-back 推断或流恢复。连接恢复只供后续新 RPC。gRPC 的明确未接收透明连接尝试边界仍按
M5 固定版本审计，不能只靠 DisableRetry 就宣称所有 transport 内部尝试都不存在。

| 验证 | 范围和证据 |
| --- | --- |
| 五 RPC | `TestPeerRealFiveRPCs`：MongoDB、Elasticsearch、OpenSearch 分别 direct/1-hop/A→B→C；opaque 字节、同 key Bulk、End/count/final OK、最终 Store 和各 relay 指标 |
| BackendExpression | `TestPublicExpressionUnaryBulkAndOpaquePeers` 增加 1-hop，覆盖 0/1/2 hops 的 Mutate/Bulk、原生字节、无效表达式零执行和单物理执行计数；ProgramTransform 在 direct/1/2 hops 均 UNSUPPORTED+NOT_STARTED |
| hop/路由/metadata | 真实 peer 缺失/非法/重复/超限，application 拒绝自带 hop，0-hop local，0/1/4/8 环路有限失败；未知 Store、逐项操作族、跨 Store、固定 Service、bounded diagnostics 和替换 metadata |
| 完整 transport lifetime | 原 acceptance 双 65535-window、blocked unary/Scan/Native、partial body、half-close、End 后非 OK/额外帧/错误计数、上传/停止读取、input/output/final-status deadline，真实 wire 继续执行 |
| 回复丢失 | `TestPeerRealAcknowledgedReplyLossNoReplay`：三个后端，DB/peer/application 三段，ordinary/Native/expression；先观察真实 ack 再丢回复/关闭真实 socket，backend command=1；客户端 UNKNOWN/RESPONSE_INCOMPLETE/保守非 OK，测试独立 read-back 仅验证效果 |
| application 故障位置 | 故障 fixture 只保留收到的 hop，不注入固定预算；application 段实际使用无 hop 的应用入口，peer 段保留真实预算 |
| 取消/drain/账本 | 默认与完整集成覆盖排队取消零执行、执行中取消保守结果、session/relay/result/byte 归零、shared listener admission、blocked remote 与本地 Store 隔离、三跳各位置 drain 和慢 producer 背压平台 |
| 独立进程 | `TestIndependentWeirProcesses` 构建 race binary，明文 A/B/C 运行 Mongo/Search 两套 CLI；逐节点 SIGTERM、同端口 replacement、原 client 新 Read 恢复；不声称恢复在途 mutation。Readiness drain 测试继续执行 |
| 指标 | 最大图准确 1931 series；三个独立进程 C records=8、A/B relays=8，无 forwarding-only 假 Runtime；数据库 APPLIED 与外层输出失败保持独立证据 |

平台：macOS arm64、Go **1.27.0**、gRPC **1.79.3**、protobuf **1.36.11**、x/net **0.55.0**、
Mongo driver **2.9.1**；MongoDB **8.0.32**、mongosh **2.6.0**、Elasticsearch **8.17.0**、
OpenSearch **2.19.0**，镜像和版本不变。ES/OS 容器在 Linux 运行不代表 Weir 的 Linux 资格。

## 实际命令与失败历史

开始前 main/SHA/工作区验证通过，没有额外仓库 AGENTS 文件；按任务内指令执行。
默认离线 test/race、两套 vet 基线通过。27028/19200/19201 无 listener，两个 owned Search
容器均 exited 后，才使用原有带 ownership 检查的脚本启动本轮 fixture。所有测试日志与辅助
检查源码保存在忽略目录 `.testdata/m8/`，历史数据/日志不删除。

```sh
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
go vet ./...
go vet -tags integration ./...
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch
scripts/search-local.sh start opensearch
# 以下逐条串行；runner 内置 -p 1，避免 Mongo failCommand 全局干扰。
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=opensearch scripts/test-integration.sh -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -race -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=opensearch scripts/test-integration.sh -race -count=1 -v
GOPROXY=off GOSUMDB=off go test -race ./internal/server ./internal/app \
  -run 'TestPeer|TestPlaintext|TestExpressionValidation|TestEphemeral|TestIntranet|TestRemoved' -count=3 -v
# 对 elasticsearch 和 opensearch 各自执行，串行运行两种 profile。
GOPROXY=off GOSUMDB=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
  go test -race -tags integration -p 1 ./internal/server ./internal/app \
  -run 'TestPeer|TestPlaintext|TestAcceptance|TestUnary|TestNativeWire|TestPublicExpression|TestIndependentWeirProcesses|TestDiagnosticProcessSIGTERM|TestDiagnosticsMaximumStaticSeries' -count=3 -v
GOPROXY=off GOSUMDB=off scripts/generate.sh
GOPROXY=off GOSUMDB=off go mod tidy -diff
git diff --exit-code -- api go.mod go.sum
```

开发中失败及复核修正如实保留：

1. `first-compile.log`：替换 handshake fixture 时，第一次编译缺少尚未补入的
   `delayedPeerListener`；补齐真实监听 wrapper 后通过。
2. `first-test.log`：旧 `TestExpressionRequiresMutatePermission` 仍要求 PermissionDenied。
   删除已废弃权限语义，将其改为 direct/1/2 hops 的非法表达式及 ProgramTransform 零执行验证。
3. 代码复核发现允许多个相同 `127.0.0.1:0` 后，旧按地址字符串选择入口会把 application
   错当 peer；改为按配置入口位置选择，并新增真实双 ephemeral listener 的相反 hop 规则测试。
4. 补强 fault fixture 的 application 入口和 hop 保留后，重新进行全部相关真实三轮 race；
   没有通过注入预算绕过 application 拒绝内部 metadata 的规则。

## 最终检查与资源收尾

统筹独立复核后，将中文架构阶段表遗留的 `secure listener` 修正为“隔离内网监听器”，与英文一致；此次仅检查文档 diff 和 `git diff --check`，未改代码或启动测试/fixture。

完整普通/race 两套 profile 已通过。新增 partial-body、metadata 和 fault-fixture 精化只改变
测试覆盖，生产行为不再变化；随后两套三轮真实专项及停止后默认 suite 均实际通过。

| 检查 | 结果 / `.testdata/m8/` 日志 |
| --- | --- |
| 基线默认 offline test/race 与 default/integration vet | PASS；`baseline-test.log`、`baseline-race.log`、`baseline-vet.log`、`baseline-vet-integration.log` |
| ES / OS 完整 integration 普通与 race | PASS；`full-elasticsearch.log`、`full-opensearch.log`、`full-elasticsearch-race.log`、`full-opensearch-race.log` |
| 明文、peer、配置与表达式边界三轮 race | PASS；`final-plaintext-race3.log`；另 `plaintext-body-race3.log` 覆盖两入口真实 partial RPC |
| 两套真实关键三轮 race | PASS；`real-elasticsearch-race3.log`（server 80.1 s/app 55.5 s）、`real-opensearch-race3.log`（各三轮，含全部三段回复丢失） |
| 协议重新生成、依赖整理 | PASS，无 diff；`generate.log`、`tidy.log`、`protocol-dependency-diff.log` |
| 最终 vet、风格、双语文档与 whitespace | PASS，无诊断；`final-vet.log`、`final-vet-integration.log`、`final-style.log`、`final-gofmt.log`、`final-diff-check.log`、`docs.log`；当前 JSON 示例解码另见 `examples.log` |
| fixture 停止后默认 offline test/race | PASS；`stopped-backends-offline.log`、`stopped-backends-offline-race.log`；另以 `-count=1` 重跑，不依赖缓存，日志 `stopped-backends-offline-uncached.log`、`stopped-backends-offline-race-uncached.log` |

收尾通过原 ownership-check scripts 停止本轮启动的 MongoDB 与两个 Search 容器。
`cleanup.log` 确认 27028/19200/19201 无 listener、两个 `weir-milestone-2` 容器 exited；
`processes-after.txt` 为空，无残留 Weir/example、server/app/store test 或 mongod 进程。
保留已停止容器、标记数据目录和全部日志；测试自有临时 Weir、代理与 socket 已由各 fixture
cleanup 回收。停止后用 `GOPROXY=off GOSUMDB=off` 实际重跑默认 suite。

```sh
scripts/mongo-local.sh stop
scripts/search-local.sh stop elasticsearch
scripts/search-local.sh stop opensearch
GOPROXY=off GOSUMDB=off go test -count=1 ./...
GOPROXY=off GOSUMDB=off go test -race -count=1 ./...
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
```

本阶段相关实现、测试、双语架构、当前示例和本报告在本地 main 提交后停止，向统筹报告
最终 SHA 及上述证据。不自行续开阶段；没有分支/worktree/PR、push、发布或生产部署。

## 保留限制

静态单 IP endpoint，没有 DNS、多端点选择或部署可达性资格；diagnostics 仍 loopback-only。
标准后端 Auth/TLS 的新增 profile、认证重连语义、复制/primary 切换、多节点、生产网络、
Linux/Windows 原生运行、OCI/Kubernetes、长期 RSS/cgroup/soak 和负载 SLO 均未由本阶段验证。
ProgramTransform 继续 UNSUPPORTED。依赖升级或换 HTTP/2 transport 必须重新资格。
当前 Go 降级内存采样不是 RSS；资源预留有界不是完整生产内存公式。
后续门槛由统筹按 [production-readiness](production-readiness.md) 串行推进。
