# 第四里程碑：受限、无状态、有界的 Native

本次从干净的 `95223d4` 开始，在本地 `randy/native-m4` 实现，代码/测试/示例提交为
`028a407`。既有第三里程碑已提交，
没有覆盖前序未提交文件。本报告记录实际 profile 和验收，不是完整 V1 或生产资格。
AtomicTransform 继续 UNSUPPORTED；没有新增 SDK、远端 Store、动态配置、认证系统或部署。

## 1. 第三里程碑入口闭环

已阅读两个架构文档、milestone-1/2/3 和 unary 期限报告，并检查原始
`.testdata/m3/final-*` 日志与实现：页面完整验证后才发布，Scan 页间释放许可，
清理与消费者归还共同释放 reservation；传输期限持续覆盖最终 status。
本轮实际重跑后才开始 Native，全部通过：

```sh
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
go vet ./...
go vet -tags integration ./...
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch go test -p 1 -race -tags integration \
  ./internal/store ./internal/mongostore ./internal/searchstore ./internal/server \
  -run 'Scan|WireBounds|TestAcceptance|TestUnary' -count=1 -v
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=opensearch go test -p 1 -race -tags integration \
  ./internal/searchstore ./internal/server -run Scan -count=1 -v
```

日志为 `.testdata/m4/preflight-*.log`。三个固定后端分别运行；没有根据前序总结补写 PASS。
覆盖完整性、C=1 慢消费、取消、cursor/PIT 清理、drain、输入/输出/最终 status 和旧 unary。
未发现阻塞 Native 的第三里程碑问题。

## 2. 实际支持矩阵

实现前已在本报告确定最小矩阵与上限，随后按实现和实验细化。未知操作默认拒绝。

| 后端 | resource | Native 操作 | 选项 |
| --- | --- | --- | --- |
| MongoDB 8.0.32 | 配置的 database/collection | ordered BSON `count` | query(document)、limit/skip(int32/int64)、hint(string/document) |
| MongoDB 8.0.32 | 同上 | `findAndModify` document update 或 remove；无跨 RPC 状态 | query/sort/fields/update(document)、new/upsert/remove(bool)、hint(string/document) |
| Elasticsearch 8.17.0 | 配置的 concrete index | `GET /_doc/<id>`，空 body，half-close 后派发 | 可选 realtime=true/false |
| Elasticsearch 8.17.0 | 同上 | `POST /_bulk`，index/create/delete | 可选 refresh=true/false/wait_for；未指定时保留后端默认语义 |
| OpenSearch 2.19.0 | 同上，独立真实资格验证 | 同一 HTTP 操作集 | 同上 |

Mongo 命令名必须为第一个字段且 collection 与 resource 相同。没有重写用户字段顺序、
类型或 body；驱动仍添加自己的 database/session/deadline 协议字段。禁止 `$db` 覆盖、
lsid/transaction 控制、writeConcern/readConcern 等未列选项、getMore/killCursors、find/
aggregate/watch、管理命令、更新 pipeline、JavaScript/CodeWithScope 和 `$where`/
`$function`/`$accumulator`。后者按字段名递归拒绝，即使它们出现在普通用户数据中也不例外。
这是一项狭窄 Native profile 限制，不是通用查询/变换解释器。

Mongo Native 仅启用无认证 client：固定 driver 的重新认证路径可能重放命令，因而有
Authenticator 的配置不宣称符合 Native 单次执行契约，直接 UNSUPPORTED。记录 API 不受此
新增 Native capability 限制影响。本地固定副本集无凭据；没有读取凭据文件。

Search GET ID 仅 1..512 个 ASCII unreserved 字符，拒绝 `.`/`..`、百分号编码、斜线、
绝对 URL、authority、路径逃逸。query 必须能按 Go `url.Values.Encode` 原样往返，拒绝重复
或未知参数。bulk metadata 只允许 `_id`（必填字符串，1..512 bytes）和 `_index`（省略或
等于当前 concrete index）；每个 item 在发送前完整检查，禁止目标覆盖、routing、pipeline、
update/script 等未支持字段。source 只检查有界 JSON 结构，保持原字节，不翻译文档语义。

写入前在执行许可内重新读取 index 资格，要求当前 default/final pipeline 都为空或
`_none`。不追加 pipeline=_none、不修改后端设置、不套用记录 API 的 Put/Replace/OCC。
索引销毁、alias 重定向或 ingest 配置并发改变仍属于既有 namespace 资格边界，不能声称
它们与写入之间有事务锁。固定单 shard、stored source 等 Store 启动资格保持原有要求。

不支持任意 URL、凭据、remote fetch/reindex、后台任务、schema/index/cluster 管理、
跨 RPC cursor/session/resume。Native 不是数据库协议隧道。

## 3. Descriptor、framing 和完成证据

`api/weir/v1/weir.proto` 增加 Native 双向流与架构指定字段号，旧 RPC/字段号保持不变。
NativeCompletion 数值为 0 unspecified、1 NOT_STARTED（Go 常量 `NATIVE_NOT_STARTED`）、
2 RESPONSE_COMPLETE、3 RESPONSE_INCOMPLETE。没有 APPLIED、PARTIALLY_APPLIED 或 retryable。

输入为 Open → 非空、有界 chunks → half-close；输出为可选 Head → 有序 chunks → 恰一 End。
本次成功收到原生回复时总是发送 Head；准入/验证拒绝可只有 End。客户端必须检查 End
及其 Failure 组合，随后实际收到最终 gRPC OK/EOF；End 后还有帧也是协议错误。
Send 成功、handler 返回或账本归零都不证明客户端已收到完整响应。

| completion | 意义 | Failure |
| --- | --- | --- |
| NOT_STARTED | 请求的原生命令确定没有派发 | 必须存在；此前可以有 Store/写入资格 metadata 查询 |
| RESPONSE_COMPLETE | 完整原生响应及本 profile 所需 framing 已交付到输出发送路径 | 必须为空；客户端仍需最终 gRPC OK |
| RESPONSE_INCOMPLETE | 可能已派发，不能提供完整原生回复 | 必须存在 |

缺 End、截断或非 OK 均不能推导未执行/回滚。Mongo `ok:0`、HTTP 4xx/5xx、bulk `errors:true`
都是原生响应。完整收到它们依然 RESPONSE_COMPLETE，HTTP 200 也不表示每项写入成功。

Mongo descriptor media type 为 `application/vnd.weir.mongodb-command.v1+protobuf`，data
为空 protobuf；body_media_type 必须 application/bson。响应 Head 的 body_media_type 为
application/bson，无额外 metadata；chunks 拼接后是驱动收到的原始 BSON command reply。
`RunCommand.Raw` 与 `CommandError.Raw` 保留原生错误，不能从 Go error 重建 BSON。
回包只校验有界 BSON 和必要 command envelope，不解释写效果。

Search descriptor schema 在独立的 `api/weir/search/v1/http.proto`，Core 不导入/解析它：

```proto
message Header { string name = 1; repeated string values = 2; }
message Request {
  string method = 1; string path = 2; string query = 3;
  repeated Header headers = 4;
}
message Response { uint32 status_code = 1; repeated Header headers = 2; }
```

请求与响应 metadata 的 media type 均为 `application/vnd.weir.search-http.v1+protobuf`；
body 是原生 bytes。bulk 的 body_media_type 为 application/x-ndjson；GET 不带 body media。
path 是 resource-relative，Adapter 加上配置的 index，host/port 永远只来自 Store 配置。

请求 header 名必须小写且只允许 accept、content-type、x-opaque-id，每个 Header 1..8 个
值，每值至多 128 bytes，无控制字符。Accept 仅 application/json；Content-Type 必须匹配
body media，未提供时由该明确 media 设置。拒绝 Host、认证、代理、逐跳、Content-Length、
幂等键等所有其他请求头。允许的重复值按顺序保留。

响应 metadata 保留 net/http 解析后的 status 和白名单字段：content-type、content-length、
warning、x-opaque-id、x-elastic-product、location、retry-after、etag。名称小写，同名值顺序
保留；不承诺原始 HTTP header 行次序、大小写或逐字节 wire transcript。HTTP transport 会
规范化 framing/Connection，protobuf metadata 也不会作为 HTTP 逐跳头透传。Native 不解析
JSON body；非 JSON、空 body 和 redirect 可完整返回。未知编码、101 upgrade 和 trailers
不在当前 profile 内，不能伪装成成功空响应。Content-Length/chunk framing 由固定 Go HTTP
解析器验证，长度不足、未终止 chunk、连接丢失和 body 超限均不完整；不根据 JSON 语义
判断一个已完整 framing 的 body 是否是数据库成功。

## 4. 调度、所有权与有限资源

一次 Native 只调用一次共同 Submit，复用 Store 的 FIFO/live 账本和同一 AIMD execution
window，不按 chunk 准入，无 Native 队列。Native 和 Scan **合计一个 live session/Store**。
Scan 页间让出 permit；Native 从开始消费上传、资格查询到后端 exchange/连接清理完成一直
持有 permit。C=1 会暂时阻塞普通操作，靠固定 exchange/lifetime/stall 期限结束；没有保留
槽位或优先级 lane。排队期间不会启动上传 pump；仅继承固定 transport read-ahead。

| 项目 | 实际上限/所有者 |
| --- | --- |
| Open | descriptor data 64 KiB；总 Open protobuf ≤64 KiB+4 KiB+256 bytes；URI 4 KiB |
| body chunk / gRPC frame | 1..64 KiB / 300 KiB；unknown fields 仍计入 transport/Open 字节 |
| Mongo command / response | 各 4 MiB；command limit+1 bounded read，完整验证和 half-close 后才派发 |
| Mongo 结构 | 深度 100、65536 节点，顶层最多 32 字段、无重复 command 字段 |
| Mongo native/decoder reservation | 128 MiB/Native；与 Scan 共用一个 slot，覆盖输入/驱动副本和既有 wire guard 的两份至多 48 MiB 消息 |
| Mongo guard | 非 cursor metadata 64 KiB、容器 4096 节点；findAndModify 的 value document 允许到 4 MiB，最终回复另受总 4 MiB 上限 |
| Search bulk item | 256 KiB，含 metadata/source/newlines；metadata line 4 KiB；source 深度 32、4096 JSON 节点，metadata 64 节点 |
| Search total upload | 8 MiB、最多 4096 items；只缓存当前 item、4 KiB reader 和有限帧，不缓存整段上传 |
| Search response | 8 MiB；每次读/发送最多 64 KiB；响应头解析 32 KiB，公开 metadata 64 KiB |
| Search session reservation | 4 MiB，供当前 item、decoder、帧与 metadata；既有固定 HTTP/gRPC buffers 另有 transport 边界 |
| Native output credit | Mongo 64 KiB+512 bytes；Search 128 KiB+512 bytes；保留到 HTTP transport/gRPC 处理及接收 pump 都退出 |
| Native pumps | 每已派发 session 一个接收 goroutine，单个有界 frame handoff；Store 一个 runner；Search HTTP/1 至多一对 transport read/write goroutine，无每 chunk goroutine |
| 连接 | Mongo 复用 Adapter pool；Search 普通 pool 不变，Native Adapter transport 至多一个连接且不复用，metadata 与命令顺序执行 |
| application transport | 既有 16 connections/16 sessions、每连接 8 streams、300 KiB+5 bytes read-ahead credit 和固定 HTTP/2 输入窗口 |

这些 reservation 是保守工作集额度，不是精确 malloc/Go heap/RSS 峰值证明。不能把 Scan
128 MiB 与 Native 128 MiB 当成每 Store 同时可用的两个预算。Adapter 独占后端状态；Core
只看到 opaque plan、字节 source/sink 和 completion，不解析 BSON/HTTP path/body。

每 exchange 最多一个 feedback 样本。本次 Native 保守返回 Neutral：不把客户端慢上传/
慢下载、非法输入、未知原生业务错误或整个 exchange 超时统一作为 DB 拥塞，不增长/降低
窗口；原有普通记录/Scan 的显式反馈保持原样。

## 5. 期限、early response 和清理

| 阶段 | 期限 |
| --- | --- |
| Open/descriptor、实际等待下一上传 frame | Stall 默认 30s；真实 body read 进度只更新 stall，不能延长 lifetime |
| 排队及总 Native lifetime | 默认/最大 5min，从 HTTP 入口起算；调用方只能缩短 |
| 执行许可内的整个 Native exchange | 既有 BackendTimeout 默认 2s、最大 10s；包括上传、metadata、后端和输出 |
| Search connect/header/metadata | 固定 connect/header 2s，metadata call 2s，并受更短 parent 期限限制 |
| 输出及最终 status | 原生 HTTP/2 write/flush deadline，默认 stall 30s 与绝对 lifetime 取小值；handler 返回后仍有效 |
| cleanup/Close | Native 不创建待 kill 的 cursor；取消 socket/source，join 当前 source read；接收 pump 在最终 status/传输取消后 join；沿用 Runtime 3s worker join、Mongo Adapter 2s Disconnect |

初次真实 early-response 测试发现：直接关闭上传 HTTP body，会使 gRPC `RecvMsg` 自行写入
Canceled status，从而把完整原生 400 覆盖。本实现把已准入 source 与一个固定接收 pump
做有界交接；停止 Adapter 上传不强制给正在 Recv 的 gRPC 注入本地错误。先交付完整响应/
End/OK，最终 status 或传输取消唤醒挂起 Recv，然后 delivery join pump 并归还预算。
不无限 drain 剩余请求，不依赖客户端先 half-close；持续发送和停止发送两种情况都测试。
实际 framing/客户端取消/期限错误仍可得到非 OK，不能伪成功。

后端提前回复实验使用原生 HTTP header limit 拒绝：在合法有界 descriptor 中发送足够多
的允许 header，ES/OS 在聚合上传 body 前返回完整 400。没有把本地合成错误冒充该实验。
重复 Content-Type 的初次实验未提前回复，保留为诊断，不作为通过证据。

drain 拒绝新的 Native，但已准入 exchange 可在原期限/drain deadline 内结束；取消不再
消费新 chunk、不重放命令。Native 结果/session credit 与传输结束绑定，连接/handler
竞态不会把账本归零当作交付完成。现有 Bulk watchdog 或 TCP 写停滞仍可能关闭整个 gRPC
连接并影响同连接其他 RPC；普通 mutation 丢失结果仍为 UNKNOWN。

## 6. 无隐式重放审计及故障证据

- Mongo driver 2.9.1：retryReads=false、retryWrites=false、MaxAdaptiveRetries=0、
  overload retargeting=false、Direct=true，无 Native transaction/commit-resolution loop。
  `processRunCommand` 未启用普通 retry mode。391 reauthentication 是额外重发入口，因此
  Native 拒绝带 Authenticator 配置；在无认证真实 fixture 上注入 391/16500，保留原生
  error reply 且只有一次命令。未修改 module cache。
- Go 1.27 `net/http.Transport.shouldRetryRequest`：GET 在复用连接断开后可能重发。
  Native 使用 Adapter 内独立的 DisableKeepAlives HTTP/1 transport，使 reused-connection
  分支不可达；固定 loopback endpoint，无环境 proxy、自动 compression、HTTP/2 backend
  transport、redirect follow、GetBody、幂等键、endpoint failover 或第二次尝试。
  Native 资格 metadata 请求也走该 transport。
- 示例 gRPC client 显式 DisableRetry 和 DisableServiceConfig；测试禁用 retry，固定本机
  listener。服务端不能禁止任意外部调用者主动重试；本次没有新的 SDK retry 层。
  gRPC 在应用确定尚未收到调用时的透明 transport retry，不等于派发后的执行重放。

| 实验证据 | 区分与结果 |
| --- | --- |
| Mongo findAndModify 成功后丢弃回复 | 字节代理先读取真实后端完整成功，再断开；只观察一次命令，独立读取 n=2，Native RESPONSE_INCOMPLETE |
| Mongo 正常/业务错误原始回复 | 代理原回复 SHA-256 与公开 BSON 完全相同，包括真实 ok:0；没有重建响应 |
| Search create bulk 成功后丢弃回复 | 代理读完真实 native response 后断开；一次 `_bulk`，独立 GET 找到已写文档，Native 不完整，不重试 create |
| 普通后续非法 item | 错误 index/pipeline/routing/framing 等不会被发送；正常 ES/OS 会聚合 body，本机观察前序文档不存在，也仍不能对调用者宣称未派发 |
| 请求 framing 故障后前序效果 | 明确的故障代理把第一个已验证 item 结束为一次原生命令并确认真实写入，再丢弃回复；客户端随后提供越权 item，Native 不完整、一次后端命令、独立 GET 证实前序效果。不是声称正常 ES/OS 在未结束 bulk 内逐项执行 |
| 原生 mixed-error | 真实重复 create 与 mapping error 留在完整 bulk response，未归一化成全部写入成功/失败 |
| early response | ES/OS 原生 header limit 400；客户端尚未 half-close，含持续上传版本；完整 End 和最终 gRPC OK |
| 合成 HTTP framing | 本地 HTTP server 的空 body、重复允许头、redirect、多帧、长度不足、截断 chunk、过大 body/length、断开、trailers/upgrade；只证明 framing/限制，不冒充真实后端故障 |

## 7. 验收、复现和最小双向调用

版本：Go 1.27.0 darwin/arm64、gRPC 1.79.3、protobuf 1.36.11、protoc 33.4、
protoc-gen-go-grpc 1.5.1、Mongo driver 2.9.1；MongoDB 8.0.32 单成员 weir_m1 副本集，
mongosh 2.6.0。Elasticsearch 8.17.0 和 OpenSearch 2.19.0 使用
`scripts/search-local.sh` 中固定 digest，与第二里程碑一致。没有更新依赖。

真实 suites 只访问带所有权标记的 loopback fixture，各测试独立数据库/index，Mongo
failpoints 按 appName 限定并跨 package 串行。没有访问生产、读取凭据或修改宿主内核。
默认测试离线；synthetic HTTP/2/TCP 测试仅使用本进程本机端口。

```sh
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
go vet ./...
go vet -tags integration ./...
WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -race -count=1 -v
WEIR_SEARCH_INTEGRATION=opensearch scripts/test-integration.sh -race -count=1 -v
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch go test -p 1 -race -tags integration \
  ./internal/store ./internal/mongostore ./internal/searchstore ./internal/server \
  -run 'Native|TestAcceptance|TestUnary' -count=3 -v
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=opensearch go test -p 1 -race -tags integration \
  ./internal/searchstore ./internal/server -run Native -count=3 -v
```

最终日志位于 `.testdata/m4/final-offline-{test,race}.log`、`final-{integration-,}vet.log`、
`final-full-{elasticsearch,opensearch}-race.log` 和
`final-qualification-{elasticsearch,opensearch}-race3.log`。诊断失败独立保留；未把 mock、
初次失败或仅编译标签当成通过。最终执行结果见本节末尾。

主要测试还覆盖 Native/Scan 双向 session 互斥、队列拒绝/排队取消不读 body、C=1 保留
permit 与普通操作恢复、超一帧上传/回复、空/超限 chunk、重复 Open、Mongo command 超限
零派发、原生回复超限、固定 65535 双窗口慢下载、Open 无数据/半 prefix/半 body、零窗口
阻塞仅 End 的最终 status、取消/drain、12 轮资源采样及既有部分初始化/重复 Close。
旧 unary 未注册方法测试原来用 `/Native` 作样本，本次改为 `/Unregistered`，保持原断言。

可编译的最小调用在 `cmd/weir-native-example/main.go`：一个 goroutine 上传，主 goroutine
立即 Recv，限制 chunk，检查 Head 顺序/metadata、恰一 End、合法 completion/Failure 和最终
EOF；任意返回路径 cancel 并 join sender。不会先上传完再开始下载，也不把 Send EOF 当成
完整结果或数据库未执行。

```sh
# 已按 README 启动匹配的本地 Store 后：
go run ./cmd/weir-native-example -store mongo -database weir_m1
go run ./cmd/weir-native-example -store search -index weir_m2_example
```

Mongo 示例为 ordered BSON count；Search 示例为 native GET /_doc/example，404 也可以是完整
原生回复。示例只打印 metadata/字节计数，不缓存整段返回体；业务调用者自行解释原生 body。

最终实际结果：上述离线 test/race、两种 vet、两个完整真实 integration suite 和两组
三轮关键 race 验收全部通过。MongoDB 同时在两个完整 suite 中执行，两个 Search 产品独立
执行；原有 CRUD/Bulk/Scan、UNKNOWN、pipeline/OCC、partial init/Close 和 unary 回归通过。
另以 `-race -count=3` 运行精确 4 MiB Mongo command、256 KiB item、8 MiB 累计上传/
回复及超限 chunked 回复边界，日志为 `final-byte-boundaries-race3.log`。固定工具重新生成
全部 protobuf 绑定逐字节一致，代码规则 AST 检查和 `git diff --check` 通过。

三轮 Native 取消实验每轮 12 次，每 4 次 GC 后采样一次；全部采样的 Pending、PendingBytes、
Active、Retained、ResultBytes、LiveSessions、ScanSessions 和 NativeSessions/NativeBytes
均为零。以下是采样范围，不是峰值或 RSS：

| fixture | 样本数 | HeapAlloc bytes | goroutines / fixture 初始基线 |
| --- | ---: | ---: | ---: |
| MongoDB（两个产品组合） | 18 | 1,415,840–3,852,760 | 23 / 19 |
| Elasticsearch | 9 | 1,523,240–1,902,056 | 16 / 12 |
| OpenSearch | 9 | 1,408,192–1,551,264 | 16 / 12 |

额外 4 个 goroutine 属于建立连接后的固定 fixture；不是持续增长的 Native pump。最终
transport slot 的归还在 join 接收 pump 后，配合关闭测试验证本地所有权回收。

实际构建并运行 server/native-example，分别连接 Mongo+Elasticsearch 和 Mongo+OpenSearch，
四次调用均验证完整 Head/End/最终 OK；两个 Search GET 都是原生 200。日志为
`final-cli-smoke.log`。示例独有数据库/index 和临时 Weir 进程已清理；验收结束后只停止本轮
从停止状态启动的三个任务 fixture，保留其目录/容器和所有测试日志，未停止其他服务。
实现、测试和文档只作本地提交，没有推送、PR、合并、发布或生产部署。

## 8. 依据和未验证范围

实际源码审计：固定 Mongo driver 的 `mongo/database.go`、`single_result.go`、`errors.go`、
`x/mongo/driver/operation.go`；固定 Go 的 `net/http/{transport,request}.go`；gRPC
`stream.go` 的 RecvMsg 自动 WriteStatus 和 `internal/transport/handler_server.go`。

原生语义参考（资格仍以固定本机版本实际实验为准）：

- [MongoDB count](https://www.mongodb.com/docs/manual/reference/command/count/)
- [MongoDB findAndModify](https://www.mongodb.com/docs/manual/reference/command/findandmodify/)
- [Elasticsearch 8.17 Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/8.17/docs-bulk.html)
- [OpenSearch 2.19 Bulk API](https://docs.opensearch.org/2.19/api-reference/document-apis/bulk/)

未验证/不承诺：其他版本、认证/TLS、生产代理、multi-shard/multi-node、Linux RSS、长时间
稳态/吞吐资格、任意脚本/原生命令、任意 Search query、跨 RPC 事务/恢复、后台任务或全部
写入成功。Native HTTP 禁用连接复用是本次确保单次执行的明确代价。没有修改双语架构，
因为目标契约未变；本报告里的更小上限、允许集和排除项属于实现 profile。
