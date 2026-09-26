# Unary 响应期限修复与回归证据

日期：2026-09-26。起点：`37d1454`。本轮开始时 HEAD 仍为该提交，工作树已有未提交的发送期限候选修复。本修订不改变 Read/Mutate/Bulk 的 RPC 形态或存储语义，
不启用新的 capability，不推送/部署。

## 原结论撤回与复现

用户指出 unary 服务端期限只覆盖 handler，这一问题属实。原版所谓“期限覆盖发送”的
验收结论应撤回，不能用 Bulk 停滞测试或普通 unary 成功调用证明它。

新增 `TestUnaryDeadlineCoversResponseSend` 后，在修改实现之前，真实 MongoDB 上连续三次：

- UnaryLifetime = 100ms，Stall = 100ms。
- gRPC 客户端固定 stream window 64 KiB、connection window 256 KiB，关闭动态 window。
- 调用真实 unary Read wire method，等待响应 headers 后暂停 RecvMsg 600ms。
- 文档 BSON 为 204831 bytes，明显超过 stream window，确保仍有数据未发送。
- 三次均在恢复读取后收到完整响应；测试失败。原始日志：`.testdata/unary-deadline-before.log`。

## 独立材料复核与本轮增补

先按用户提供的 overlay 原样运行，当前工作树三次通过，没有重复改写已有发送路径。
再用 `git archive 37d1454` 导出隔离源码副本，加入同一原始用例，三次均在约
603–605ms 得到完整 204834-byte 文档及 OK/EOF，按预期失败。没有切换/回滚工作树或修改
Go module cache；源码副本只是忽略目录下的复现证据。

原始用例已整理为仓库内 `acceptance_unary_transport_test.go`，保持双方接收窗口均为
**65535 bytes**、200 KiB BSON、headers 后暂停 600ms，以及检查文档之后的最终 status/EOF。
正式测试不依赖 overlay 或 Projectless 临时目录，并覆盖无 deadline、较长 2s deadline、
较短 50ms deadline。测试工具的本地等待上限不会作为 RPC deadline 发给服务器。

扩展验证发现另一个直接相关缺口：总寿命 2s、Stall 100ms 时，未发送数据、部分前缀和
部分请求体三种输入停滞均未在 500ms 内终止。修复仅利用当前 transport 已有的
SetReadDeadline：在实际 body read 前设置 min(原总期限, 当前时间+Stall)，输入持续推进
可更新停滞预算，但不能延长总寿命；stats.InPayload 确认 unary 请求解码完成后恢复原
读期限，不再把后端执行或等待客户端 half-close 算作输入停滞。deadline 缩短也不能
覆盖一个已在生效的更短输入停滞期限。本轮未再引入新的传输架构或改变 RPC 形态。

## 根因与修复边界

原 unary interceptor 的 context 只传给应用 handler，defer cancel 和 session 释放都在
handler 返回时发生。`single` 同时 Ack 结果额度。gRPC 的 sendResponse/WriteStatus 在这些
步骤之后；native gRPC transport 还会在 DATA/trailers 入队而非实际发完时结束上下文。
因此简单移到 stats.End/tap、加另一个 handler timeout，仍不能证明完整响应有期限。

固定版本源码核对：

- `google.golang.org/grpc@v1.79.3/server.go`：processUnaryRPC、sendResponse、ServeHTTP。
- `internal/transport/http2_server.go`：finishStream 先 cancel，再排队 trailers。
- `internal/transport/handler_server.go`：HandleStreams 的 body read-ahead、writeStatus 的 flush；
  该 transport 的 Drain 未实现，不能照搬 grpc.GracefulStop。
- Go 1.27 的 `net/http.ResponseController.SetWriteDeadline`：HTTP/2 stream 原生写期限。

采用现有公共 API，而不是 fork gRPC、窥探私有字段或自行解析生产 HTTP/2 帧：

1. 用 Go 1.27 `net/http.Server` 的 unencrypted HTTP/2 接入固定 gRPC ServeHTTP transport。
   仍只允许 CLI loopback 地址；仍限制 16 连接、16 应用 session、每连接 8 个 stream。
2. HTTP 请求入口建立总寿命并设置原生 read/write deadline。gRPC 解析出更短 caller deadline
   后继续收紧，绝不重置原预算。
3. Unary handler 返回时把写期限收紧至 min(原期限, 当前时间+Stall)，覆盖响应编码、DATA 和
   trailers。最终期限由 HTTP/2 stream 持有到 END_STREAM/RESET，不随 handler 返回取消。
4. Unary 结果 credit 与应用 session 保留到 handler transport 的 DATA flush/关闭；最终
   trailers 仍由 HTTP/2 deadline 管理。关闭和结果交接用同一锁裁决，迟到结果立即 Ack，
   迟到 handler 不再调用已结束 HTTP writer。应用 slot 只有 HTTP transport 和 gRPC 处理
   **两方都结束**才释放；stats.End 仅结束处理方所有权，不单独证明发送完成。
5. HTTP/2 stream timeout 直接 reset，不需要客户端恢复读取。不能交付终态 envelope 时，
   客户端只得到非 OK/截断；写入结果是 UNKNOWN，不能编造 NOT_STARTED/NOT_APPLIED。
6. net/http 负责 graceful HTTP/2 shutdown；到 drain deadline 关闭连接并 Stop gRPC，
   不调用不支持该 transport 的 grpc.GracefulStop；Stop 等待当前有界 handler 退出。

原有 Bulk 输入/发送 watchdog 保留，仍可能关闭连接；底层 TCP 写无进度也受 HTTP/2
WriteByteTimeout 限制。因此连接级错误仍可截断其他 RPC；正常 unary stream timeout
不必杀死同连接的健康 stream。

### 保留输入背压，避免换 transport 后引入无界缓冲

gRPC ServeHTTP 会在内部 goroutine 持续读 request.Body，直接使用会绕过应用 Recv 背压。
`creditedBody` 在原生 body 前施加 **MaxFrame + 5 = 307205 bytes** 额度。读取先扣减；
只有 stats.InPayload 报告已解码的准确 wire bytes 才归还，短读退回未使用部分。
耗尽时不再读底层 body；取消/Close 唤醒等待并关闭原 body。这里只计字节，不解析 BSON/JSON，
不另建待处理操作队列，不把 stats.End 当成输出完成。

同时保留 HTTP/2 64 KiB stream / 256 KiB connection 输入窗口、16 KiB frame 上限、header
预算和 gRPC MaxRecv/MaxSend frame 上限。HTTP/2 原生 header 记账有固定额外开销；
不得把配置值冒充精确 Go heap 或 RSS。

## 回归与实际结果

真实 MongoDB 8.0.32、Go 1.27.0、gRPC v1.79.3，不使用 fake Send 或预定错误标签代替传输。

| 测试 | 证明的性质 |
| --- | --- |
| TestAcceptanceUnaryTransportLifetime | 原始独立用例的正式版本，精确 65535-byte 双窗口，覆盖无/长/短客户端 deadline，并检查最终 status/EOF |
| TestUnaryInputStall | 无数据、部分 gRPC 前缀、部分请求体分别在约 100ms 回收，不等 2s 总寿命 |
| TestUnaryInputProgressRenewsOnlyStallBudget | 分块输入总耗时大于停滞额度，持续有进度时不误杀 |
| TestUnaryDecodedInputDoesNotTimeoutBackend | 完整消息但不 half-close，后端阻塞 250ms，不被过时的 100ms 输入计时器取消 |
| TestAcceptanceNormalUnaryResponses | 精确双窗口下正常小/200 KiB 大响应及旧期限后的连接复用 |
| TestUnaryCancelSendDeadlineRacesReleaseResources | 12 轮真实发送完成/取消/deadline 竞争，检查结果、slots、连接及实际 goroutine 栈 |
| TestUnaryShutdownRacesWithCancelAndSend | shutdown、取消、恢复读并发，停止发送和回收均有界 |
| TestUnaryConnectionAbortDoesNotAffectOtherConnections | 既有 Bulk watchdog 关闭连接时同连接 unary 截断，另一连接继续提供 Read |
| TestUnaryDeadlineCoversResponseSend/both | 精确复现 100ms lifetime/stall + 暂停读 600ms；必须失败而非完整响应+OK |
| 同测试 /lifetime | 100ms lifetime、2s stall，单独证明总期限覆盖发送 |
| 同测试 /stall | 2s lifetime、100ms stall，单独证明发送停滞限制 |
| 三种模式的资源检查 | headers 后 session=1/retained=1，约 98–101ms 内均归零，客户端到 600ms 才恢复读 |
| TestUnaryLifetimeIncludesHandlerTime | 后端真实阻塞 140ms 后，响应仍在约 200ms 总寿命结束，不从 handler 返回重新计时 |
| TestUnaryRejectedFramesReleaseDeliverySlot | 超大请求、未注册/非规范方法路径拒绝后不遗留 slot/结果 |
| TestUnaryCompletedResponseDoesNotExpireConnection | 健康响应完成后不留下定时器误杀可复用连接 |
| TestUnaryStalledStreamDoesNotBlockOtherStreams | 同连接其他 stream 在该 stream 停滞/超时前后继续成功 |
| TestUnaryAppliedMutationLosesBlockedReply | 真实 HTTP/2 zero receive window 阻塞小型 Create ack；服务端 reset，数据库确实已写入，客户端结果仍 UNKNOWN |
| TestUnaryShutdownWhileResponseBlocked | 25ms drain 强制结束正在发送的 unary，并回收结果/session |
| TestUnaryResultHandoffAfterTransportClosed | 真实 Runtime 结果在 transport 关闭后交接，不泄漏 credit |
| TestDeliverySlotRequiresBothCompletions | HTTP 先结束或 gRPC 先结束都不能提前释放 slot；未派发拒绝也不泄漏 |
| TestCompletedDeliveryNeverTouchesResponseWriter | 已关闭 delivery 的迟到回调不能访问已释放 writer |
| TestHandlerTransportReadAheadIsBounded | 一帧额度耗尽即停读；只按已消费字节恢复；取消无遗留等待任务 |
| TestHandlerTransportCloseInterruptsRead / ShortReadRefundsCredit | Close 打断底层 read、幂等回收、短读不丢额度 |

修复后以上真实 unary 场景在 race 模式下重复三轮通过；原始复现的客户端状态为
HTTP/2 reset 映射的 gRPC INTERNAL，**不是 OK，也不是可供推断未生效的终态结果**。
完整集成 suite（包括 MongoDB RMW、commit reply loss、Bulk 顺序/乱序/计数、持续生产/慢读、
overload、partial init 和关闭）也在 `-race -count=3` 下重跑通过。

```sh
# 启动任务所有的隔离副本集；不接受生产 URI。
scripts/mongo-local.sh start
WEIR_INTEGRATION=1 go test -race -tags integration ./internal/server -run 'TestAcceptance|TestUnary' -count=3 -v
scripts/test-integration.sh -race -count=3 -v
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
go vet ./...
go vet -tags integration ./...
```

本机日志：`.testdata/unary-deadline-before.log`、`unary-deadline-after.log`、
`unary-transport-race.log`、`server-transport-race.log`、`integration-unary-fix-race.log`。
独立复核及输入边界日志：`unary-independent-baseline.log`、`unary-independent-current.log`、
`unary-input-before.log`、`unary-acceptance-phase.log`、`unary-acceptance-extended.log`、
`unary-acceptance-races.log`；最终完整 suite 记录在 `integration-unary-acceptance-final.log`。

## 不扩大结论

- 此修复有针对性地补齐 unary 响应期限证据；不恢复“完整 V1/生产就绪”的说法，
  也不把一轮通过代替外部验收。
- gRPC ServeHTTP 是实验性 API。只对已固定版本和当前有限 Read/Mutate/Bulk profile 验证；
  升级 Go/gRPC、增加功能或改变 transport 必须重跑流控、内存与关闭测试。
- 已在期限前完整交给 transport/socket 的小响应，客户端应用后来才读取是正常的；
  没有应用级 delivery ACK，服务器不能撤回已发送的数据。复现必须像本测试一样超过固定窗口。
- Unix 本机短时 heap 平台不是真实 RSS/生产高水位资格验证；Linux 仍只有交叉编译证据。
- AtomicTransform 仍 UNSUPPORTED；没有新队列、重试、协议字段或存储功能。
