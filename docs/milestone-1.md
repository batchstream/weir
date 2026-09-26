# Milestone 1：实现、验证与边界

验证日期：2026-09-26。工作目录 `/Users/liran/Projects/liran/go/weir`。

**这是第一个本地可验证里程碑，不是完整 V1，也不是生产就绪发布。**
真实 MongoDB CRUD/Bulk、事务冲突与响应丢失验证已执行。通用 Lua runtime
资源隔离验证的结论是**不合格**，因此 AtomicTransform（程序及表达式）均未启用。
这不是用 mock 或弱沙箱替代通用转换。

**验收结论修正：`37d1454` 的 unary 期限仅覆盖 handler，未覆盖响应发送；用户给出的
100ms/600ms 场景已三次复现失败。撤回此前整体验收通过的结论。**
本次修复及新增回归证据见 `unary-response-deadline.md`，不能用旧通过记录代替此项验证。
新一轮独立材料复核先确认已有发送修复通过，再补齐输入停滞、无/长/短客户端期限、正常大响应和资源回收竞争；没有重复更换传输方案。


## 1. 初始状态与文档修订

开始时 HEAD 为 `4d1e6c8`，仅有未跟踪的 `docs/architecture.md`，没有现有实现、
Go module 或待合入代码。未发现磁盘上的额外 AGENTS.md；执行请求中给出的规则。
没有读取 `.env`、私钥、凭据文件，也没有访问生产数据。

双语架构文档仅定义目标架构与协议契约；本里程碑的授权范围、实现状态、固定版本、
验证结果及限制在本文件记录，不写入架构文档。
`architecture.md` 与 `architecture.zh-CN.md` 按章节对译，保留 1–20 节、小节、S1–S9、D1–D16
及附录覆盖索引。`docs/baseline_test.go` 校验结构、引用和关键契约的一致性；
它不是自动语义翻译验证的替代。

本文件及专项报告记录三项后端验证发现：驱动 CSOT 的提交重试/取消行为、gRPC unread DATA
对错误 trailers 的阻塞、GopherLua 的隔离缺口。第 16 节未来布局未机械照搬：
当前一个具体 Adapter 由 Runtime 直接拥有，Core 只使用不透明 plan 元数据；
没有为单实现创建空接口或测试替换函数。公共 schema/生成 Go 类型共放 `api/weir/v1`。

## 2. 固定版本与复现环境

| 项目 | 本次版本/约束 |
| --- | --- |
| Go | 1.27.0，darwin/arm64；`go.mod`、`.go-version` |
| module | `github.com/batchstream/weir`，仅一个 module |
| MongoDB Go driver | `go.mongodb.org/mongo-driver/v2 v2.9.1` |
| gRPC Go | v1.79.3；期限修复后使用 ServeHTTP（实验性、固定版本验证） |
| HTTP/2 transport | Go 1.27 net/http；RPC schema 不变，完整响应写期限由 HTTP/2 stream 持有 |
| Protobuf Go / protoc-gen-go | v1.36.11 |
| protoc | 33.4，项目内独立 binary |
| protoc-gen-go-grpc | v1.5.1 |
| GopherLua 候选 | v1.1.1，仅 `_test.go` 使用，不进入服务运行图 |
| 数据库 | MongoDB Community 8.0.32，macOS arm64，单成员 `weir_m1` 副本集 |
| MongoDB gitVersion | `f9eb55a7cc900f33a722a5b32a0fdbf54385c749` |
| mongosh | 2.6.0；启动脚本校验版本并禁用 rc 加载 |
| 后端 URI | 固定测试地址 `127.0.0.1:27028`，directConnection，禁止测试读取外部 URI |
| 监听 | 仅 `127.0.0.1:7447` 默认；CLI 拒绝非显式 loopback IP |

所有间接 Go 依赖也固定在 `go.mod`/`go.sum`。本机 Homebrew protoc 因 abseil 动态库缺失
不能运行；没有修改系统工具，使用固定下载。Docker Engine 29.7.2 可用，但已有
`mongo:8.0` 镜像（digest `4968f22d0c6c10ef29952f3e807f62872ba22b3312f25803564fbfc08255efc2`）
拒绝在该 Docker Linux 内核上启动；未改内核或主机设置，改用隔离的官方原生 MongoDB。

下载的 SHA-256：

```text
protoc-33.4-osx-universal_binary.zip
0745eb76adabd8ead6203cb094807e2e0a82f4222dcc7363986a611f02ca07ba
mongodb-macos-arm64-8.0.32.tgz
f81cb258434d548dca7244d599c82eb339043d8dedd0b1b807870c9d263117f2
```

`scripts/bootstrap-tools.sh` 校验这些摘要；不会运行压缩包中的 Compass 安装脚本。
`scripts/generate.sh` 固定插件版本，并确定性地将生成代码的三处结构体字面量整理成
命名变量，遵守本仓库风格。连续两次生成的 SHA-256 一致，没有手改不可复现的生成物。

## 3. 已实现表面与最小调用

```text
Application -> gRPC Read / Mutate / Bulk
            -> StoreRuntime 的唯一准入/待处理账本
            -> 有界调度、微批、显式 AIMD
            -> MongoDB Adapter 独占 client/pool
            -> MongoDB 8.0.32
```

- Read：单记录 BSON，显式 missing；不先 count。
- Put：原生 replace+upsert；Create：原生 insert；Replace：存在时原生 replace，缺失
  NOT_APPLIED + PRECONDITION_FAILED；Delete：原生 delete，缺失也在肯定确认后 APPLIED。
- 原始 BSON 必须有匹配 URI 的首字段 `_id`。key 为 string/ObjectId/canonical signed integer。
  新写入应用 codec 的类型/结构验证；Read 按不透明 BSON 返回，不进行通用转码。
- 固定一个已预建的普通 collection，simple collation，拒绝 view/capped/mongos/standalone。
  启动验证 MongoDB **8.0.32**；其他版本不是自动兼容。单一 direct target，禁止发现扩池及
  failover。禁止 client timeoutMS 覆盖 Runtime 的期限归属。
- 非零 outcome、有限 Failure 文本；不向错误复制后端返回的用户数据。没有自动普通重试，
  driver retryReads/retryWrites/adaptive retries 和 overload retargeting 均关闭。
- Bulk：Open -> 连续从 0 开始的 index -> half-close；结果按完成顺序，成功必须最后恰一 End，
  received/result 数量一致。一个流一个 Store。同流同 key 执行有序，独立同 key Read 可并发。
- 同流排序不是条件工作流；UNKNOWN 后继不承诺数据库的最终完成次序。
- Create/Put/Delete 可以物理微批；Replace/Read 为单项（不以 aggregate matched count 冒充
  Replace 逐项证据）。Driver 可把一个兼容写批拆为不同原生命令，但都在同一个执行预算内。
- AtomicTransform 返回 UNSUPPORTED + NOT_STARTED。Native/Scan 不在本次 schema 中，调用
  未注册 RPC 得到 gRPC UNIMPLEMENTED；不存在暗中模拟的 capability。

本地复现：

```sh
cd /Users/liran/Projects/liran/go/weir
scripts/bootstrap-tools.sh
scripts/mongo-local.sh start
go run ./cmd/weir
# 另一个终端：
go run ./cmd/weir-example
```

实测 example：Put 返回 APPLIED，Bulk 返回 indexes 0、1、2，均非 missing；程序检查
End/计数和 EOF 后成功退出。Ctrl-C 使服务按 drain 路径正常退出。`-batch=false`
只改变物理批次大小，仍走完全相同的准入、结果额度、执行和关闭路径。

## 4. 有界性与故障语义

这些是**本次验证初值**，不是生产 sizing：

| 资源 | 实现上限/默认 |
| --- | --- |
| 记录文档 / Protobuf frame | 256 KiB / 300 KiB |
| URI / request headers / Failure text | 4 KiB / 16 KiB HTTP header 预算（另有标准 HTTP/2 记账开销） / 1 KiB |
| BSON 结构 | 深度 32，节点 4096；字符串必须合法且完整终止 |
| Store pending | 256 项、8 MiB 保守计账输入 |
| 结果全局预留 | 128 项、16 MiB |
| Bulk outstanding | 每流 8 项；另最多一个未准入帧/一个无效结果缓冲 |
| handler transport 输入 read-ahead | 每个已准入 RPC 最多 300 KiB + 5 bytes；仅已解码消息返还对应 wire bytes |
| Read 结果 credit | 最大文档加 512 bytes envelope/终态开销 |
| Mutation 结果 credit | 每项 512 bytes |
| 执行窗口 | 从 C=1 开始，Cmax=4；pool max=4，maxConnecting=2 |
| 连接数 / 应用 session / 每连接 stream | 16 / 16 / 8 |
| 物理 batch | 16 项、1 MiB 保守原生请求预算、1ms collection |
| 单次后端执行 / unary / Bulk | 2s / 30s / 15min |
| 输入/发送停滞 | 30s（集成测试使用 300ms 缩短验证时间） |
| CLI drain / Adapter cleanup | 5s / 2s；Runtime 最终 worker join 另有 2s 硬边界 |
| 进程内存预算 | 默认 512 MiB，80% latch、70% clear、100ms 采样 |

计账包括 protobuf unknown fields 和条目/plan 保守开销；队列/排序索引只引用同一条目。
在**准入时就按最坏情况预留结果空间**，派发前已经有 credit。后端 completion 不调用
网络 Send，也不会被一个慢接收者阻塞。结果由发送/丢弃方明确 Ack 释放，取消 active 不
提前释放其输入与结果空间。全部参与者取消才取消共享 I/O。

Driver 的单次原生回复可能先物化大于 Weir 256 KiB 的记录，之后再拒绝；这部分不能
假称只用了 256 KiB。固定直连目标和经验证的原生 message 上限（不超过 48 MiB）提供额外
有界 decoder 工作集，另含每个目标的 driver monitor sockets。这里不是精确 RSS 公式。

Linux guard 使用 `/proc/self/statm` RSS，并按可用 cgroup-v2 `memory.max` 收窄预算。
macOS 实测使用 **Go Sys-HeapReleased 的降级信号**，不是 RSS。Linux 分支仅交叉编译，
没有 Linux/cgroup 运行资格验证；不能宣称已测精确容器峰值。

调度器用一个锁内的 queued -> dispatched 转移裁决取消；只有未派发可 NOT_STARTED。
可能发送后取消/超时/丢响应默认 UNKNOWN，只有 Adapter 的更强证据可收窄。没有读取当前
文档来猜测“这次写入是否成功”，也没有盲重放不确定项。

AIMD 只用显式后端拥塞/归因 timeout，按 epoch 忽略旧 flight，同一批仅一个样本；floor=1，
100–300ms cooldown，饱和健康样本达到 max(4,C) 且间隔至少 250ms 才增长。正常 missing Read
是健康执行，不是后端错误。无延迟桶、EWMA、lanes 或全局并发承诺。

### gRPC 停滞与关闭

原先仅在 unary interceptor 中 WithTimeout，并在 handler 返回后 Cancel/Ack，不能覆盖
实际响应发送。用户复现和本地三次重现推翻了这部分原验收结论。当前用 Go 1.27 HTTP/2
stream 的 SetWriteDeadline 配合 gRPC ServeHTTP，将最早的服务端/调用者期限保持到响应
DATA/trailers 完成或 reset。Unary 在 handler 返回时再收紧为响应停滞预算，而不延长总期限。
输入读取也使用 min(原期限, 最近实际 read 的时间+Stall)；解码完成后解除输入停滞限制，
防止仅因后端耗时或客户端未 half-close 而误杀正常操作。
Unary 结果 credit 保留到 DATA flush/transport 结束；应用 session 等 HTTP transport 与 gRPC
处理两方均结束才释放。最终 trailers 仍受
HTTP/2 自有期限控制。结果恰好在关闭后完成的竞争也必须归还额度，不访问已释放的 writer。

ServeHTTP 内部会主动读取 request.Body，因此额外设置一个最大 gRPC 帧的输入 credit；
只有 stats.InPayload 确认已解码的 wire bytes 才返还。这不是另一个 pending 操作队列。
没有用 stats.End 判定输出已送达，也没有单纯延时关闭已完成的健康连接。

HTTP/2 stream 写超时可仅 reset 该流；Bulk 原有 watchdog 或连接写超时仍可能关闭对应
连接，截断同连接的其他 RPC。底层 reset 的 gRPC 客户端状态可能是 INTERNAL，而不是可
交付的 DEADLINE_EXCEEDED envelope；关键是非 OK/截断，缺失写入结果仍为 UNKNOWN。
已经在期限前发送完成的数据不能因客户端应用稍后才读取而撤回。

Shutdown 由 net/http 管理 HTTP/2 graceful drain；gRPC ServeHTTP 的 transport 不实现 Drain，
因此不能调用 grpc.GracefulStop。停止新准入、继续已准入工作，到 drain deadline 强制
关闭 HTTP 连接并 Stop gRPC，等待当前有界 handler 退出，解除阻塞的发送/接收。Adapter 仍仅由 Runtime 关闭一次。
本次已对 unary 阻塞发送及此前 Bulk/执行/事务关闭场景重新运行真实后端测试。

## 5. MongoDB 原子 RMW 高风险验证

实现基础：`internal/mongostore/rmw.go`；真实测试：`integration_test.go`；
故障代理：`internal/testmongo/proxy.go`。测试使用实际官方 driver、实际副本集、
命令 monitor 协调真实竞争，以及实际已确认回复的 TCP 丢弃，不用 mock 标签代替后端。

内部有限 conformance transform 对 `n` 加类型相同的 one；缺失时创建 `_id`、Int64 n。
这便于观测是否重跑和丢更新，并复用 Value/codec/事务状态机；它不开放为公共 DSL，
也不声称是通过安全评审的通用 runtime。

| 实验 | 实际结果 |
| --- | --- |
| 事务 read/transform/write/majority commit | 正常 APPLIED |
| 原生 `$inc` 竞争 | 2 个事务、2 次求值、1 次 commit；保留原生更新，n=12 |
| 原生完整 replace 竞争 | 新事务重算，n=21，保留原生 marker |
| 原生 delete | 新 snapshot 观察缺失，创建 n=1 |
| 同 ID delete+recreate | 新事务重算到 n=21，不覆盖新生记录内容 |
| 原先缺失时原生 insert | 新事务读取插入者的数据，再算到 n=21 |
| 每次 native write 都制造冲突 | 总事务 attempts 恰 5，未 commit，NOT_APPLIED |
| 非 `_id` unique index 冲突 | 1 次求值，0 commit，NOT_APPLIED，不盲重试 |
| 真正 commit 后丢一个成功回复 | 同 session/txn 发出 2 次 wire commit，1 次求值，最终 APPLIED |
| 所有成功 commit 回复都丢弃 | 最多 10 次 wire commit，1 次求值/事务，UNKNOWN；数据库 n=1 |
| 成功回复丢失后，服务器再给 BadValue | ambiguity latch 保留，UNKNOWN；不重跑转换 |
| commit 被真实 failCommand 阻塞 250ms，原 deadline=60ms | 约 60–65ms 返回 UNKNOWN，不等完整 250ms |
| 提交进行中取消并实际关闭 Adapter | UNKNOWN、只求值一次；worker 结束且 client 已 Disconnect，无回滚推断 |
| 普通 insert bulk 成功回复丢失 | 两条真实记录落库；两项 UNKNOWN；线上 insert 仅一次，无自动重放 |

测试逐字段检查只存在 `_id`、`n` 和原生 writer 的 `native` 字段。无 revision、hash-CAS、
整文档相等谓词、本地记录锁、旁路表、时间戳或 metadata 注入。

### 驱动提交控制的实证修正

固定 driver 的 `x/mongo/driver/operation.go` 会在 CSOT deadline 存在时把原生 commit
retry-once 改为无次数限制。单纯隐藏 Deadline 又不够：其
`topology/context_listener.go` 仅对 Canceled 主动关闭 socket，若只传 DeadlineExceeded
却无 socket deadline，会等待阻塞响应。本测试实际捕获了这种错误方案，并修正为：

- 保留外层原 deadline/error 和 session values。
- 用 `context.WithoutCancel` + `WithCancel` + `AfterFunc` 把父取消桥接为无 Deadline
  的 driver context；使 native retry-once 和实际 socket cancellation 同时成立。
- 总事务最多 5；**整个逻辑操作**最多调用 CommitTransaction 5 次，驱动每调用最多 2 次
  wire commit，总上限 10。未知后永不新开事务/重新转换。
- 整体执行至多 min(原 deadline, 2s)，cleanup 每次至多 200ms，最终不超过原执行期限+200ms。
  cleanup 的 abort 不能清除已发生的 ambiguity；未知 commit 的 EndSession 不新发 abort。

这是对固定 driver 行为的有测试依赖，不是可不经验证套用到未来 driver 的通用技巧。

## 6. Runtime / Codec 高风险验证

`internal/value` 定义精确整数、有序 Object、Missing/Null 和 Extended；
`internal/mongostore/codec.go` 与 Lua 无依赖；Lua probes 只依赖 Value/codec 的既有接口。

通过的可行性向量：

- Int32/Int64 checked 加减乘、`9007199254740993 + 1` 精确、Int32/Int64 溢出失败。
- 混宽算术失败，显式 checked conversion 后成功；缩窄越界失败，未实现的数值转换明确拒绝。
- 修改 `n` 时其他字段的 Int32/Int64 宽度、ObjectId、Decimal128、binary subtype/bytes、顺序
  不变。Float64 特殊位模式、duplicate field 可无损往返；重复名访问拒绝歧义。
- Missing 与 Null 在 codec 和 Lua typed userdata 中不同。没有原生 Extended 伪造 helper。
- 同 source/current 的成功结果多次字节一致；不打开 OS、文件、网络、时钟等 Lua 库。
- 深度/节点/文档大小边界及畸形 BSON 拒绝；合法输入 fuzz 要求逐字节 round-trip。
- Lua 无限循环在 context 取消下退出并 Close；非尾递归受小 stack/registry 限制。

**未通过的安全要求（因此不能启用）**：

1. GopherLua `SetMx` 是全进程 heap 的周期采样，超限调用 `os.Exit`；不是每 invocation 的
   分配额度，也不是分配前限制。本次不调用它以免伤害进程。
2. 有限危险向量 `string.rep("x", 8*1024*1024)` 能直接分配 8 MiB，超过假定 1 MiB invocation
   目标，无 per-state allocation hook。没有用真正可能 OOM 的无界向量冒险。
3. 已取消 context 下 LoadString 仍成功编译，编译不受该取消控制。
4. 1ms deadline 的宿主 helper 仍阻塞约 40ms；VM context 检查不能中断正在执行的 Go helper。
5. 没有完整 instruction/fuel（含 helper work）、编译、分配与实际时间的联合强隔离契约。

这些测试通过的含义是**成功复现并确认候选失败**，不是 runtime 安全验收通过。
通用 AtomicTransform 和后端表达式在入口始终 UNSUPPORTED。未另建弱 runtime、隔离
子进程框架或未审查的替代语言。未来 runtime 决策和启用是独立里程碑。

## 7. 实际测试与证据

最终已停止本任务 MongoDB 并确认测试监听端口关闭，再执行全部默认测试。以下离线命令使用已缓存依赖，禁止 Go 网络解析：

```sh
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
go vet ./...
```

真实后端及 fault suite（`-p 1` 避免服务端全局 failpoint 互相覆盖）：

```sh
scripts/test-integration.sh -count=1 -v
scripts/test-integration.sh -race -count=1 -v
scripts/test-integration.sh -race -count=3 -v
go vet -tags integration ./...
```

额外执行：BSON/URI 各数秒 fuzz；Linux amd64 交叉构建（仅编译）；连续两次协议重新生成并
校验摘要；真实服务启动、example 调用和 SIGINT 正常退出。

验收定位：

| 要求 | 测试证据 |
| --- | --- |
| 校验/队满/overload | `TestValidationAndUnsupported`、`TestAdmissionBoundsAndReservation`、`TestGRPCUnaryAndUnsupported` |
| 取消派发竞争 | 离线状态转移测试及真实 `TestNativeCancellationDispatchRace`（50 次，NOT_STARTED 项必须不存在） |
| 混合 deadline | `TestNativeBatchMixedDeadlines`：短调用过期，两条真实写入仍成功，长调用收到 APPLIED |
| 逐项/全批/不确定 | duplicate-key 混合成功、网络 close、write-concern fault、真实成功 insert reply drop；无回退重放 |
| Delete missing batch | 一条 native delete command、零 find，两项 APPLIED |
| Unary 端到端期限 | 原始 65535-byte 双窗口独立用例，无/长/短客户端 deadline；输入停滞/进度、正常大响应、发送/取消/shutdown 竞争、资源回收及零窗口写入回执丢失；见专项报告 |
| Bulk 完成/截断/duplex | 81 项混合流、错误 Store、空流、非连续 index、重复 Open、End/计数/EOF |
| 排序/并发 | 同 key Put/Read 交替；独立同 key 两个 200ms read 约 210ms 完成；乱序结果按 index 关联 |
| 慢/停止读取/持续生产 | HTTP/2 实测上游 Send 背压；单流 peak retained=8，修订后的短样本 heap 约 18–20MB 平台（不是 RSS/峰值保证），stall 后回收 |
| 连接/session | 真实 loopback TCP 超额连接立即关闭、重复 Close 不重复释放；超额应用 session 拒绝 |
| 关闭 | 排队/active、无 half-close 的 Bulk、真正阻塞的 result Send、事务 commit deadline/实际 Adapter 关闭、graceful accepted drain |
| 部分初始化/生命周期 | 五次不存在集合启动失败、pool 连接回到基线附近；重复 Close 后 client 已 Disconnect |
| 真 RMW / runtime codec | 第 5、6 节对应 suites |

本机原始日志与生成摘要在忽略目录 `.testdata/`，主要是 `offline-test.log`、
`offline-race.log`、`vet.log`、`integration-race.log`、`fuzz-codec.log`、`fuzz-uri.log`、
`example.log`、`generated.sha256`。测试数据库为每次独立的 `weir_test_<pid>_<counter>`，
cleanup 只删除自己创建的数据库；结束前实查没有残留 `weir_test_` 数据库。测试服务和 mongod 均已停止；脚本只停止带本任务所有权路径的 mongod，保留数据和日志便于检查。

## 8. 没有完成或没有声称验证的内容

- 没有完整 V1、生产发布/部署、远程转发、发现/控制面、搜索后端、Native/Scan、多语言 SDK、
  Kafka、持久队列、动态配置、通用 retry、复杂 lanes、鉴权/TLS 或生产 telemetry。
- GopherLua **不满足资源隔离**；通用变换未开放，内部 counter conformance 不是任意程序实现。
- 单本机单成员副本集通过，不代表分片、跨主机 stepdown/failover、网络分区或多节点一致性已验收。
- 慢消费者/持续生产为短时压力与确定性额度验证，不是数小时稳定性、真实 cgroup RSS、生产性能
  或大文档/多数副本延迟资格测试。更大文档、更多连接/并发必须重新核算。
- Linux 运行与其他 MongoDB/driver/协议工具版本未资格验证；升级必须重跑原生故障 suite。
- 连接级 stall 清理有已声明的同连接影响；客户端必须正确处理缺失结果为 UNKNOWN。
- HTTP/2 期限修复切换了 gRPC transport 接入方式；ServeHTTP API 是实验性的，仅对固定 Go/gRPC 和本地测试 profile 验证，不扩大生产就绪声明。
- 实现与验证结束时未提交；随后依据用户明确指令，将双语架构基线和里程碑实现整理为本地提交，
  具体记录以 `git log` 为准。未推送、创建 PR、合并或 release；工具缓存和测试数据不纳入提交。
