# Weir：从零设计的架构与协议

状态：V1 设计提案；2026-09-26 已明确授权实施第一个里程碑。后续阶段仍须分别授权。

这是新设计，不是 Sink 迁移计划。Sink 的 API、包边界、配置格式、部署角色及存储元数据均不构成兼容约束。此处保留原始设计文档；已授权里程碑的实现及验证证据见 `milestone-1.md`，不表示批准或完成全部 V1。本文件与 `architecture.md` 按章节、条款和引用标识对应维护；中文段落重新排版，不改变规范含义。

## 第一个里程碑的验证配置（2026-09-26）

首个实现有意收窄范围：仅回环 gRPC listener、一个 Store、通过单一 direct connection 目标访问 MongoDB 8.0.32 副本集上的一个预建普通集合（无发现/failover），原始 BSON Read/Put/Create/Replace/Delete 及双向 Bulk。通用 AtomicTransform 和后端表达式返回 UNSUPPORTED。Native、Scan、搜索后端、peer、TLS/认证、控制面和部署均未实现。输入 BSON 的第一个字段必须是与资源匹配的显式 `_id`；执行文档化的类型、深度、节点限制，不接受有损转换。所有额度均为验证初值，不是生产建议。

实际验证对后文契约作出三项具体化，而不降低其要求：

- GopherLua v1.1.1 无法提供每次调用的分配、编译取消和宿主 helper 隔离。它仅存在于测试中。有限计数器转换用于事务一致性验证，不是替代的通用语言；程序转换仍关闭。
- MongoDB Go driver v2.9.1 在 deadline/CSOT 模式下会无限次重试提交。显式提交循环保留外层原始 deadline/error 与 session，将父 context 的取消桥接到无 Deadline 的原生 context。驱动 socket listener 需要 Canceled，而不能仅收到缺少 socket deadline 的 DeadlineExceeded；因此既保留 retry-once 次数上限，也保留原始期限的实际取消。真实响应丢弃测试验证每个逻辑 RMW 最多十次线上提交、相同 session/事务以及不确定后不重新执行转换。升级该驱动必须重新验证。
- gRPC v1.79.3 可能将 trailers 排在未读取的 DATA 后。输入或发送停滞超时会关闭这一条 transport 连接，而不只是结束 handler。同连接其他 RPC 可能截断；缺失的写入结果仍为 UNKNOWN。明确有界的故障范围优于无限保留阻塞缓冲区。

当前只建立 `api/weir/v1`、`internal/protocol`、`internal/store`、`internal/server`、`internal/mongostore`、`internal/value` 等确有用途的包。StoreRuntime 直接拥有唯一具体 Adapter，只比较不透明 plan 元数据；不导入 BSON 或检查字段。不为匹配第 16 节未来布局而建立单实现接口；真正批准第二种实现时再提取边界。结果额度在准入时提前按最坏情况预留，早于后文要求的派发时刻。

## 决策摘要

Weir 是可组合、同步的存储数据平面。一个 `weir` 二进制承载配置的 listener、route、本地 Store 和远程 Weir Service。主要路径为：

```text
Application
    |
Listener
    |
Route（仅逻辑 Store 身份）
    |
Service
    +-- LocalStore --> StoreRuntime --> Scheduler --> Adapter --> Database
    |
    +-- RemoteWeir --> 另一 Weir listener --> 相同执行路径
```

贯穿设计的不变量是：**资源有界、后端原生原子性、不透明文档、明确的写入结果、流式背压**。

与 Sink 有意不同的决策：

- 不设 Gateway、Engine、Worker、异步接收或持久交付。
- 一个逻辑 RPC/流只访问一个 Store、一个 Service，不作 Store 扇出。
- 每个本地 Store 只有一个调度器和待处理账本，不按 RPC 类型分开。
- 文档为携带媒体类型的字节；只有 Adapter 和转换 codec 解码。
- 写入报告执行证据，不提供 `retryable` 布尔值。
- MongoDB 通用转换使用事务，不注入隐藏 revision。
- 不抽象可移植的 query、count、revision、visibility 或 durability。
- Read、Mutate、Native、Scan 是四种语义操作；第五个 RPC Bulk 只是 Read/Mutate 的有界封装，不是另一条执行路径。
- V1 Scan 是实时、背压驱动的遍历，游标寿命限于一个 RPC；不提供跨 RPC 恢复令牌或可移植快照承诺。
- 应用与认证 peer listener 使用相同 `weir.v1` RPC；转发只增加可信 hop 元数据，不增加消息协议。
- 同 key 顺序仅限一个 Bulk 流；独立 Read 不串行。
- Scan 游标寿命与后端 fetch 并发分开限制；Cmin 为 1。
- Native 报告响应完整性，不规范化数据库写入效果。
- 配置静态验证、组装后不可变；变更通过滚动重启完成。

## 1. 范围、原则与保证

### 1.1 产品边界

Weir 控制 Application -> Database 路径，包括路由、连接汇聚、有界准入与调度、微批处理、局部排序、自适应并发、过载保护、后端执行、原子转换、原生访问、流、可观测性和关闭。

它不是队列、交付平台、ETL、ORM、schema/index 管理器、通用查询语言、SQL 层、跨记录事务协调器、分布式锁、工作流引擎、service mesh、exactly-once 处理器或透明数据库协议隧道。应用可以自建上游队列和消费者，Weir 不感知也不管理它们。

### 1.2 语义保证

1. 已识别且准入的记录操作恰有一个本地终态结果；传输故障后不保证结果送达。
2. 成功写入具有 Adapter 文档所述的单记录原子性；Bulk 流或物理数据库批次均不是事务。
3. 之前的尝试可能生效后，不自动开始新的写入尝试。同一事务的原生提交解析不是新尝试。
4. 路由、调度、计账、转发和拥塞控制不检查文档字段，不依赖 BSON/JSON。
5. 输入、待处理集合、执行集合、保留结果、转换工作集和传输缓冲区都有有限上限。
6. Bulk 在当前流内保持同记录帧顺序；独立调用无共享顺序保证，端点亲和只是优化。
7. 用户文档不含 Weir 字段；也不用旁路 revision 集合或持久去重账本变相代替隐藏字段。

这些不等于全局线性一致、全局排序、精确集群并发或 exactly-once。读一致性、确认持久性及搜索可见性仍由后端/Store 定义。弱持久性配置下，已确认写入仍可能丢失。V1 拒绝无法提供有效生效证据的无确认记录写入。

### 1.3 隔离与信任边界

每个本地 Store 独立拥有调度器、pending 限额、自适应窗口、批次状态、Adapter client/pool 和有界转换缓存。慢 Store 不借用其他 Store 的队列或连接；但各 Store 仍共享进程 CPU、内存、网络及 OS。这是资源隔离，不是硬安全或 CPU 预留隔离；硬租户隔离需要独立进程。

TLS、认证和 Store 级授权属于 listener。后端凭据来自配置，不接受文档/原生 URL 中的凭据。Core 按 Store 和语义操作族授权，包括 Bulk 的每个 Read/Mutate。后端命令限制属于 Adapter；任意字段级授权及每请求后端身份模拟不在 V1 范围。

## 2. Listener、Route、Service 与组装

### 2.1 Listener

使用 gRPC、Protobuf、HTTP/2。应用和认证 peer listener 暴露相同服务和消息，但采用不同监听端口以区分信任策略。应用入口拒绝保留的转发元数据；peer 入口要求认证的 Weir 身份及有效 hop 元数据。共享 schema 不等于共享授权，不另建 mesh RPC 包。

Listener 管理帧/metadata 大小、传输连接额度、认证、deadline、协议校验、结果关联及有界发送；不实现存储语义或工作队列。

必须在应用 interceptor 前限制连接及 HTTP/2 并发 stream。只在 unary interceptor 限流已经晚于请求解码分配，不能作为唯一保护。应用和 peer transport 都要有界。V1 禁用压缩；大小限制针对真实 Protobuf 消息而非压缩后大小。过量应用 RPC 立即拒绝，不排队等待 handler。

### 2.2 Route 与 Service

Route 是精确的 `Store name -> Service reference` 映射，不使用正则、字段检查、最长前缀、资源改写或默认 Store。未知 Store 必须拒绝。

| Service | 拥有 | 不拥有 |
| --- | --- | --- |
| LocalStore | 唯一 StoreRuntime 的引用 | 另一队列或执行算法 |
| RemoteWeir | 有界 peer 连接、端点选择、流转发 | 数据库池、记录调度器、微批器、自适应 DB 控制器 |

Service 是具有两种真实实现的小型分发边界，不是动态插件框架。应用和 peer handler 均调用这一边界。关闭批处理时只把物理批次变为单项，不能建立绕过准入的本地捷径。

### 2.3 一个请求一个 Service

Read/Mutate/Native/Scan 各包含一个资源。Bulk 以规范 Store 根 URI 开始；所有操作必须属于该 Store。节点只解析一次并在整个流内固定 Service 和远程端点。执行后不能改变 Store、Service、端点、凭据或路由；同一 Service 内可产生多个物理批次。

错误 Store 的 Bulk 操作返回 `INVALID_ARGUMENT + NOT_STARTED`，不执行，也不回滚更早的结果。流 envelope 无效则终止流；不能推断此前未报告的操作失败。

### 2.4 配置与拓扑

启动顺序：严格解码配置 -> 校验整个图 -> 构建不可变 Service/StoreRuntime -> 打开 listener -> ready。组装有唯一所有者；部分失败必须逆向释放已建资源。不设全局注册表、运行时 provider、配置 watcher 或配置代际协议。

概念性示例不是最终配置文件 schema：单节点将 `mongo` 指向本地 MongoDB Store；组合节点可同时配置本地 MongoDB、搜索 Store 和远程 Weir；转发节点只含 RemoteWeir；多个节点可以指向同一逻辑后端。

所有拓扑运行同一个二进制，不增加 Gateway/Engine/Worker 模式。每个节点保持自己的本地额度。多节点不会自动获得全局顺序、集群级并发控制或分布式锁。

## 3. 规范资源与不透明数据

### 3.1 URI 规则

规范形式为 `weir://<store>[/<adapter-defined-path>]`。Core 检查语法、长度并提取 Store；Adapter 定义路径语法和后端身份，Core 不推测 collection/index 名称。协议必须独立于语言 URL 库规定规范化规则：

- scheme 必须是小写 `weir`；Store 为 1–63 个小写 ASCII 字符，以字母开始，后续为字母、数字或单个连字符，不能以连字符结束。不允许 userinfo、port、query、fragment、authority 别名或隐式 Store。
- 根无尾随 `/`；非根路径不含空段或 `.`、`..`。
- 先按字面 `/` 分段，每段仅解码一次；百分号编码的斜线是段内数据，不再次分段。
- 段文本须有效 UTF-8，精确保留 Unicode 码点，不归一化不同后端标识；拒绝控制字符。
- 非保留 ASCII 与 `:` 原样输出，其他字节使用大写百分号编码。拒绝应原样输出字节的转义及小写转义。仅接受重新编码后与原字符串完全相同的形式。
- 类型化 key 必须通过 parse/format 往返校验，不得存在后端相等而 URI 拼写不同的已接受 key。

| Adapter | 示例 | V1 身份限制 |
| --- | --- | --- |
| MongoDB | `weir://mongo`、`weir://mongo/catalog`、`weir://mongo/catalog/products`、`weir://mongo/catalog/products/s:123` | 字符串、`oid:` 加 24 位小写十六进制 ObjectId、规范有符号整数 `i:`；整数 BSON 宽度共享同一整数身份。不接受任意对象、数组、decimal 或非整数数值 ID。 |
| Elasticsearch/OpenSearch | `weir://search/products/s:123` | 具体 index 和精确字符串 ID；仅默认后端 routing。不支持 alias、wildcard、data-stream rollover 目标、数字到字符串便捷别名或隐藏自定义 routing。 |

Adapter 校验大小写和 collation，不猜测不支持的身份形式。MongoDB 使用简单身份比较，不允许请求选择语言学 collation。特殊 ID、自定义 routing、alias 可通过授权 Native 访问，不加入本地记录排序。

完整规范 URI 是记录身份；Bulk sequence key 为服务器自有 live stream 身份与记录 URI 的组合，不用客户端 request ID。BatchKey 是另一种 Adapter 生成的兼容性 token；同批不等于同记录或同序列，不同 Store 不共批。记录保证不覆盖命名空间销毁/重建或 alias 改指向。

### 3.2 文档与 options 表示

`Document = { media_type: string, data: bytes }`。每个 Document 都有自己的媒体类型，包括转换输入与 read/scan 输出；不从请求继承，不嗅探内容，不隐式填充。

媒体类型为小写 `type/subtype`，最多 127 ASCII 字节，不允许参数、空白和别名，如 `application/json`、`application/bson`、`application/cbor`。协议层验证拼写，Adapter 验证支持性；增加媒体类型不需要中央 enum 或 Core switch。

Core 可计算字节并限制大小，但不解析字段。MongoDB Put/Create/Replace 必须携带匹配 URI 的显式 `_id`，保留其他用户字段；Weir 不增加 ID、revision、时间戳或包装。搜索 `_source` 是用户 JSON，任何 source 字段都不被 Core 当作 ID。

未指定 `read_media_type` 时使用 Store 默认表示，否则必须精确支持；没有通用转码器。会丢失原生类型的转换可拒绝。SDK 使用调用者选定编码器，不先把任意结构体转成 JSON。

可选 `adapter_options` 是另一小型、媒体类型标记的不透明值，用于原生操作选择（如 refresh），不是通用查询语言。未知选项拒绝；Core 仅限大小。省略时使用不可变 Store 默认值，不能通过 options 绕过授权。

## 4. 执行结果与重试策略

### 4.1 记录写入结果

Outcome 是关于逻辑写入的证据，与错误类别独立。每个终态写入结果必须有非零 outcome：

| Outcome | 精确含义 | 自动重放 |
| --- | --- | --- |
| NOT_STARTED | 本操作未开始任何后端执行尝试；校验、准入或排队取消已阻止它。 | 原理上安全，Core 不自动重试。 |
| NOT_APPLIED | 已开始执行/观察，但 Adapter 能证明本逻辑写入没有已提交效果；包括明确冲突、已中止事务、未写入的 Keep/对观察到的缺失作 Delete。 | 仅 Adapter 冲突算法内部重试；调用者自行制定错误策略。 |
| APPLIED | 后端在配置的确认语义下肯定确认完成；不等于字节发生变化或所有读者可见。 | 不自动重放。 |
| UNKNOWN | 可能已经提交，没有确定终态证据。 | 不自动重放。 |

Failure 可选，包含有界 code/message，不含 retry 布尔。终态不允许 unspecified。APPLIED 可以伴随单独的后写可见性检查失败；不能因为序列化或发送失败把已知生效降为 NOT_APPLIED。

Keep、针对观察到缺失而不发写入的 Delete 为 NOT_APPLIED 且无 Failure。Create-existing、Replace-absent 为 NOT_APPLIED + PRECONDITION_FAILED。缺失 Read 为正常 missing。普通 Delete 获肯定确认后即 APPLIED，记录原先不存在也如此；不能为区分存在性额外读取。相同字节的 Put 获确认也为 APPLIED。无 affected-row count 承诺；零 matched/deleted count 不证明 Delete 失败或未执行。逐项错误与不明确确认必须按实际证据分类。

### 4.2 证据边界

```text
decoded -> validated -> queued -> dispatched -> backend evidence -> terminal
               |           |          |
          NOT_STARTED  NOT_STARTED    UNKNOWN，除非有更强证据
```

调度器以单个状态迁移裁决排队/派发竞争。后端可能收到请求后，没有回复、调用者取消、abort 失败或 gRPC deadline 都不能证明未生效。更深层 Adapter 可以提供 NOT_STARTED/NOT_APPLIED 证据，Core 不能仅凭网络错误推断。批次故障逐项处理；未知项 UNKNOWN，不把全批标为未执行。

丢失结果天然有歧义。客户端已发送但没收到完整终态的写入视为 UNKNOWN，即使服务端可能拒绝了它；确定从未发送的操作可本地标为 NOT_STARTED。没有状态查询 RPC 或持久操作账本。随后读到的文档通常不能证明哪个并发写入生效。

### 4.3 严格限定的重试矩阵

| 场景 | V1 行为 |
| --- | --- |
| Read 在结果送达前遇传输故障 | 默认不重放；显式 SDK 策略可在原 deadline 内重试，但可能读到更新数据。 |
| 部分交付的 Native/Scan | 不透明重启、拼接或重建游标。 |
| 明确 ES 条件冲突 | 在总预算内重新读、重新执行确定性转换。 |
| 明确 MongoDB 事务中止/冲突 | 可新开事务并重新执行，见第 11 节。 |
| MongoDB 提交不明确 | 仅解析/重试同一 session、同一事务的提交，不能重跑转换。 |
| 普通写入发送不明确 | UNKNOWN；无 failover、重试中间件或 SDK 自动重放。 |

gRPC、HTTP client、driver、proxy、SDK 配置须一致。不设置 gRPC retry/hedging 或搜索写入 failover，不设无界 wait-for-ready。gRPC 能证明应用从未接收请求的透明重试不算可能执行后的重放 [D3]。MongoDB 普通 retryable writes 初始关闭；同事务 commit resolution 仍允许。`retryWrites=false` 并不关闭驱动要求的 commit retry [D1]。request ID/Bulk index 仅用于关联和诊断，不是幂等键。

## 5. 公共协议：`weir.v1`

协议对所有客户端语言保持同一语义，先用一个 Go module 中的 schema 和生成类型，不因 Sink 结构而另建协议仓库。所有 repeated/string/bytes 和流都有明确限制。

### 5.1 RPC 表面

| RPC | 形态 | 含义 |
| --- | --- | --- |
| Read | unary -> 单个结果 | 一个记录或 missing。 |
| Mutate | unary -> 写入结果 | 一个原子记录动作及执行证据。 |
| Bulk | 双向 Operation/Result 流 | 同一 Store 的 Read/Mutate，独立结果交付。 |
| Native | 双向 envelope/body/result 流 | 一个无状态后端交互，可流式输入输出。 |
| Scan | unary 请求、服务端 document/end 流 | 一个 adapter 定义的实时遍历。 |

Read/Mutate 是小型 unary，不为单个标量增加服务端流。Bulk 用有界帧而非无界 repeated-operations 支持任意长逻辑输入。HTTP Native 可能在输入 half-close 前返回错误，需要 duplex；Scan 不需要 client stream。SDK 可提供便捷封装，但不能在看似普通的方法内收集无界结果。

不提供 Query 或 Count RPC；保留原生 filter、sort、projection、aggregate、count、consistency 和 visibility 语义。Scan 标准化的是背压交付，不是查询语言。

### 5.2 核心消息清单

`?` 表示可选；所有 string/bytes/repeated 均有配置上限。错误文本只用于诊断，不能作为机器后端错误解析器。

| 消息 | 字段（编号：名称、类型） |
| --- | --- |
| Document | 1: media_type string；2: data bytes |
| Failure | 1: code FailureCode；2: message string |
| ReadRequest | 1: resource URI；2: read_media_type string?；3: adapter_options Document? |
| ReadResult | oneof：1 document Document；2 missing empty；3 failure Failure |
| MutateRequest | 1 resource URI；2 adapter_options Document?；action oneof：10 put Document；11 create Document；12 replace Document；13 delete empty；14 atomic_transform Transform |
| MutationResult | 1 outcome MutationOutcome；2 failure Failure? |
| Transform | oneof：1 program ProgramTransform；2 backend_expression Document |
| ProgramTransform | 1 runtime 有界版本标识；2 source bytes；3 input Document? |
| BulkOpen | 1 store 规范 Store 根 URI |
| BulkOperation | 1 index uint64；oneof：10 read ReadRequest；11 mutate MutateRequest |
| BulkRequestFrame | oneof：1 open BulkOpen；2 operation BulkOperation |
| BulkResult | 1 index uint64；oneof：10 read ReadResult；11 mutation MutationResult |
| BulkEnd | 1 received_count uint64；2 result_count uint64 |
| BulkResponseFrame | oneof：1 result BulkResult；2 end BulkEnd |
| NativeOpen | 1 resource URI；2 descriptor Document；3 body_media_type string? |
| NativeRequestFrame | oneof：1 open NativeOpen；2 chunk bytes |
| NativeHead | 1 metadata Document?；2 body_media_type string? |
| NativeEnd | 1 completion NativeCompletion；2 failure Failure? |
| NativeResponseFrame | oneof：1 head NativeHead；2 chunk bytes；3 end NativeEnd |
| ScanRequest | 1 resource URI；2 selector Document?；3 read_media_type string?；4 fetch_items_hint uint32 |
| ScanEnd | 1 document_count uint64；2 failure Failure? |
| ScanResponseFrame | oneof：1 document Document；2 end ScanEnd |

MutationOutcome：unspecified=0，NOT_STARTED=1，NOT_APPLIED=2，APPLIED=3，UNKNOWN=4。NativeCompletion：unspecified=0，NOT_STARTED=1，RESPONSE_COMPLETE=2，RESPONSE_INCOMPLETE=3。后者只描述命令派发/响应交付，不描述效果，合法组合见第 13 节。计数/index 不得溢出，达到 uint64 上限前关闭流。拒绝无进度空 chunk。

FailureCode：unspecified=0，INVALID_ARGUMENT=1，UNAUTHENTICATED=2，PERMISSION_DENIED=3，NOT_FOUND=4，PRECONDITION_FAILED=5，CONFLICT=6，UNSUPPORTED=7，RESOURCE_EXHAUSTED=8，UNAVAILABLE=9，CANCELLED=10，DEADLINE_EXCEEDED=11，INTERNAL=12。显式 Failure 不能为 unspecified。记录结果无 Failure 意味成功/no-op，不可能是 UNKNOWN；ScanEnd 无 Failure 表示完成；NativeEnd 无 Failure 表示响应传输完成，数据库成功与否仍在原生响应内。

Put 为完整 insert-or-replace；Create 仅在缺失时插入；Replace 仅替换存在记录；普通 Delete 对缺失也成功，获确认即 APPLIED。这些是能力检查后的原生记录原语，不是 merge/update operator。不支持者须拒绝，无字段 patch DSL，也无可移植 client revision/precondition token。

V1 写入不自动获取 post-image；后续 Read 可能看到另一写入，不能当作原子返回映像。原生 findAndModify 等保留自身 returned-image 契约。

### 5.3 流语法与完成

Bulk 输入严格为 Open、零或多个 Operation、客户端 half-close。index 从 0 连续递增，去重只需一个计数器而非无界集合。逻辑总操作数在 uint64 内不另设较小上限，但 outstanding 和寿命有界；空 Bulk 合法。

输出为按完成顺序的 Result，最后恰好一个 End 和 gRPC OK。成功必须每个接收操作对应一个结果且计数一致。End 是最终核算，不是持久接收确认；不逐项发 acceptance ack。客户端必须同时读写，不能发完无限流后才读结果。

格式有效但操作无效则返回 indexed terminal error；framing、授权或连接故障可能无 End 终止。已收到结果仍有效，缺失写入结果在客户端为 UNKNOWN。Half-close 不取消已接收工作，stream cancellation 会。不得缓冲结果来恢复输入顺序。

Native 输入为 Open、body chunk、half-close；输出为可选 Head、body chunk、End；预检拒绝可仅发 End，后端可提前响应。Scan 输出 Document 后跟 End，空遍历也必须 End。非 OK 状态或缺 End 是截断，不能当成功的空/完整结果。

能够发出终态 envelope 的应用错误使用 gRPC OK；transport/auth/framing 错误可使用 gRPC status。裸 DEADLINE_EXCEEDED/RESOURCE_EXHAUSTED 不能解释为写入未生效，即使拒绝发生在解码或 indexed result 分配之前。

### 5.4 元数据与期限

使用标准 gRPC deadline/cancellation 与 W3C trace context。入口缺少 `weir-request-id` 时生成一次有界值，转发保留。Bulk 身份为 `(request_id,index)`，unary index 为 0。ID 仅诊断，不授权、去重或定义排序域；trace baggage 不含文档数据。

有效 deadline 为调用者 deadline 与方法最大寿命的较早者；无调用者期限时也有有限默认值。每跳传播 gRPC 剩余 timeout 并扣除本地已耗时 [D4]，不能重置预算。同一 deadline 覆盖排队、转换重试、后端执行及结果交付。

### 5.5 线上字段必要性审查

必须跨进程保留资源/Store 绑定、文档媒体类型和字节、读表示/原生 options、写入动作/转换输入、执行结果/完整性/Failure、Bulk index/count/End、Native descriptor/chunk/metadata、Scan selector/fetch hint。它们分别确定目标、意图、原生解释、关联和完整性。

deadline、取消、trace/request context 由既有 metadata/gRPC 机制承载，不另加时间戳字段。BatchKey、排序映射、队列位置、并发窗口和本地 plan 不上线。没有 acceptance state、retry bool、幂等保证、通用 revision/query/sort/count、returned-image flag、程序 digest 注册协议或 V1 动态能力/控制面协议。

## 6. StoreRuntime 与 Adapter 契约

### 6.1 StoreRuntime 所有权

每个 StoreRuntime 拥有一个调度状态机、pending 账本、窗口/控制器、in-flight 工作、有界 live stream 核算、指标及唯一 Adapter 的生命周期。Adapter 独占后端 client/pool、driver session/cursor 和能力状态。Runtime 构建一次并在 drain 后 Close 一次，不另行关闭内部 client，不设 PoolManager。

组批和自适应算法属于 Runtime，不是独立排队服务。内部 work 引用校验后的请求，含资源、请求/index、可选 Bulk sequence key、操作类别、输入字节额度、deadline、结果预留及不透明 Adapter plan。Plan 包含身份/兼容性/BatchKey，不是第二套公共 API，不跨 peer 序列化；引用请求而不复制整份 payload。

### 6.2 Adapter 职责与形状

以下是概念契约，不是最终 Go 接口签名：

| 操作 | 责任 |
| --- | --- |
| Construct/Close | 独占有界 client/pool，维护能力配置并关闭。 |
| Prepare | 解析/规范校验资源、媒体/options/action，生成身份和物理兼容 plan；不作 I/O 或无界工作。 |
| Execute(batch,bounded emitter) | 执行兼容有限记录批次，每项一个终态及拥塞反馈；无隐藏队列或扇出。 |
| ExecuteNative | 有界无状态交互；整个后端交互/清理持有 permit；报告 transport 完整性，不解释效果。 |
| FetchScan | 在 permit 下打开/继续游标，取得最多一个有界 page 及原生完整性证据；不等待发送。 |
| CloseScan | 有界 cleanup context 释放 Adapter 游标，不需要新准入。 |

Core 根据 plan 选择单项/批次；Adapter 不另建组批队列。Native/Scan fetch 使用同一调度器，Scan session 可跨多个 permit。每后端一个具体实现已足够；ES/OpenSearch 可共享经过验证的 wire 实现和明确差异，不设动态 Provider。

Plan 提供已知规范 key、有界兼容 token、batch/stream 能力、输入输出边界、执行类别。Adapter 可以解码验证文档，Prepare 必须有界；昂贵编译仅在执行准入后进行。Core 不解释 plan 的后端值。

能力包括 record action、输入/read 媒体、Native descriptor、Scan、runtime/codec 和表达式格式。AtomicTransform 不是无条件能力：MongoDB 任意程序需要可用事务；搜索需要 OCC 和 `_source`。不支持的组合在写前拒绝。

需要新鲜后端 metadata 的校验在启动或执行 permit 内执行，不能藏在 Prepare 中。此类读已构成后端尝试，结果可能是 NOT_APPLIED 而非 NOT_STARTED。启动能力检查有界，不输出容量样本。

Permit 覆盖顺序执行的一个记录批次、完整转换重试循环、Native exchange 或一次 Scan fetch。Adapter 不在 permit 内隐藏无界并发；若必须并发，也须计入固定、有据可查的执行边界。客户端等待不会静默变为新的未计账队列。

## 7. 唯一 Store 调度器

### 7.1 状态与准入

使用小型锁保护状态机和每 Store 一个唤醒/计时机制。状态包括有界 FIFO、Bulk sequence head/in-flight 索引、pending/reserved 操作及字节数、active 数、live-long-session 数、控制器。key 索引只引用同一批条目，不持有第二份 payload。

准入是校验后的非阻塞状态迁移：检查一次 process overload latch；检查 Store pending 数/字节和会话 outstanding/结果预算；Native/Scan 另预留有限 live session，不建等待队列；插入一项，输入加固定条目开销只计账一次，然后唤醒。Scan 跨 fetch 保留同一条目。

放不下时 unary 返回 NOT_STARTED + RESOURCE_EXHAUSTED。Bulk 通常停止读取下一帧，最多保留一个已校验但尚未准入的当前帧；永远装不下的单项直接拒绝，不无限等。live stream 自身受 transport/session 限制。

已解码未准入帧计入 listener/session 内存边界。过载停止新 RPC/新操作；已准入 Native body、Scan continuation、结果与清理仍可在预算内继续，否则会阻止释放资源。组批、冲突重试和 continuation 不重新准入。

### 7.2 选择与流内排序

选择最老的**可执行**条目，而非严格队头。需 deadline 有效、同 Bulk predecessor 完成、结果额度充足、后端工作就绪。被序列/慢客户端阻塞的条目不妨碍无关工作。对有限 P 作 O(P) 扫描足够，不预设 lanes 或 heap。

同 Bulk 的 Read/Mutate 按记录 URI 帧顺序执行；key 为服务器自有 stream 身份加 URI，不同 stream/unary 不共享，客户端 request ID 不能合并排序域。

独立 Read（含同记录）可并发，不设全局记录锁、跨请求写链或读合并。并发写正确性由数据库提供；任何跨请求序列化需要另行需求与收益证据。

本地终态立即释放序列 key，不等待结果送达。UNKNOWN 后数据库仍可能晚完成，所以后继只保证本地派发次序，不能据此依赖后端顺序。后继不是条件工作流；有成功依赖的客户端必须先收到确定结果再发送后续。读后写仍遵循后端可见性。流结束后在有界 active cleanup 完成时释放索引。

### 7.3 微批算法

以最老可执行项为 seed，收集相同兼容 token 且不同 canonical record key 的可执行项，同时限制操作数、编码请求字节和预留结果字节。Token 包括 BatchKey、action、媒体、确认/refresh 选项、适用授权上下文和输出语义。Core 只比较 token，不解码文档计算它。

达到上限、seed 首次可执行后的短收集窗到期、或 deadline 余量不足时派发。收集窗不能被后续到达重置；不能先组批再放入另一 ready queue，必须留在唯一 pending 集合等 permit 和派发条件。

批次不是事务，不把独立操作合并成一次 commit，也不折叠同记录程序。每物理批次同 key 最多一项；同流后继等前驱，但独立同 key 可在不同批次并发。程序转换为单项；兼容简单读写可用原生 multi-get/bulk。物理批量与应用请求边界无关。

只有后端逐项证据足以支持承诺结果才可组批。MongoDB 总 matched count 不能证明哪个条件 Replace 成功，需逐项结果或单项 Replace [S5,D13]。普通 Delete 不承诺 affected count；完整成功确认且无逐项/write-concern 错误的 Delete bulk 可全部 APPLIED。混合/未知项按证据分类；有明确 ordered short-circuit 证据时才可 NOT_STARTED，网络故障不行。可能执行后不能回退为单项重放；只有明确无效果的不支持命令拒绝才允许能力回退。

派发前逐项预留结果槽与字节。Read 按最大文档结果而不是平均值；写入终态为固定有限上限。额度不足则缩小批次或留在 pending，不能执行完才发现无处放结果。

### 7.4 Deadline 与共享批次

派发前再检查每项，已过期者 NOT_STARTED。物理 batch 一般不能只取消一个成员；执行 context 由后端调用上限、shutdown deadline 与仍相关成员中最晚的 deadline 限制，不能让最早期限取消无关调用。已取消者不再是有兴趣的接收者；所有成员退出或执行上限到期才取消共享 I/O。后端完成/有界清理前保留输入和结果额度。

这不延长原 RPC 期限，也不允许过期后新尝试；只是承认已派发写入可能在调用者停止等待后生效。结果丢失的调用者视 UNKNOWN，存活成员仍可收到真实结果。后端耗时和调用者等待耗时分开统计。

### 7.5 流寿命不是数据库并发

Native/Scan 的有限 live session 与 C 分开；用于游标、upload pump、page、空闲客户端，不是另一个控制器/队列。初值每 Store 一个 session；Cmin=1，Cmax=1 也可支持长流。

Scan 在整个调用中保留一个 charged continuation 条目。fetch/emit 时该条目不可再执行；fetch 前预留一页预算，完成后释放 permit，再按 session 预算发送。页排空且输出额度可用后把同一条目移到 FIFO 尾部，公平让出执行机会；不并行 prefetch/迁移 cursor。

Native HTTP 可能耦合上传、处理与发送，交互未结束必须持有 permit。C=1 时可暂时阻塞短操作，这是明确的有界限制，不承诺短请求专用 slot。输入/发送停滞、后端和总寿命期限终止卡住的 exchange。Bulk 连接本身不占 DB permit，仅已派发工作占。

所有 session 有有限寿命并保留额度到 cleanup 完成。取消停止 fetch；drain 允许已准入 continuation 在截止前完成，随后关闭游标。cleanup 不再准入。降低 C 不撤销现有 permit，不设 Cmin>=long-sessions+1、优先 lane 或短请求延迟保证。

## 8. 自适应数据库并发

### 8.1 最小显式反馈 AIMD

StoreRuntime 内保留一个小型 AIMD，反馈来自真实后端工作，不另设 ticket、queue、probe 或 retry。Cmin=1，Cmax 为经 driver/CPU 预算验证的本地硬上限，active=A，启动 C=1。非 cooldown 时 A<C 才派发；Cmax 不按 replica 数分摊。

1. 每次派发记录 epoch。同 epoch 的明确后端过载、归因于后端的超时或临时不可用，把 C 减半向下取整、最小 1，推进 epoch 并清空增长 credit。
2. 新派发暂停一个短随机 cooldown，初值 100–300ms。不加指数恢复历史或合成探针。之后执行排队的不同请求，不重放已有写入。
3. 旧 epoch 完成只释放 permit，不调整 C；每物理批次一个拥塞样本，不按失败 item 连续减半。
4. 有可执行需求且窗口饱和时，当前 epoch 至少 max(4,C) 个健康执行才可 C+1，增长频率不超过每 250ms 一次。增长推进 epoch、清 credit；空闲不提供 credit。

Scan fetch 是执行样本，空闲 cursor/已发送文档不是。Native 只有 exchange 完成才给健康样本，只有明确归因于后端才给拥塞。忽略消费停滞；一次转换重试循环至多一个样本。确定性错误、precondition、记录冲突、调用者取消和 Weir 自身限额不是 DB 拥塞。

后端调用时间与排队、转换 CPU/backoff、编码、发送分开测量。V1 不使用延迟分桶、双 EWMA、移动基线或延迟触发减窗。因此对只有成功延迟恶化的场景反应可能较慢，这是已声明限制。新增延迟控制须以混合负载测量证明收益和稳定性。

### 8.2 限制及不协调的原因

控制器只承诺本地有界并发，不是后端容量预测器或精确全局最优解。同一数据库的多个 Store/节点各自控制。必须按真实负载验证，不能宣传精确全局连接/工作上限。

Redis、lease、leader、分布式 semaphore、合成 Ping 容量测试及 replica 配额划分解决更强而未承诺的问题，均不引入。启动拓扑检查与基本健康检查不生成容量样本。

## 9. 资源边界与背压

### 9.1 显式有限额度

以下是部署关系需验证的**示例初值**，不是内存保证，也不表示所有 Adapter 接受这些最大值：

| 边界 | 示例初值 | 所有者 |
| --- | --- | --- |
| 未压缩 Protobuf 帧 | 含 envelope 5 MiB | 每个应用/peer transport |
| 单记录文档 | 4 MiB，留出 envelope 空间 | listener、Adapter |
| URI / metadata / 错误文本 | 4 KiB / 16 KiB / 1 KiB | 协议/listener |
| Native chunk / descriptor/options | 64 KiB / 64 KiB | 协议/Adapter |
| 转换 source/input/output | 64 KiB / 文档上限 / 文档上限 | 转换 profile |
| transport stream/connection | 每连接及进程全有限；应用 session 示例总上限 64 | listener |
| Store pending | 4096 项，32 MiB 计账输入 | 调度器 |
| 执行窗口 | Cmin=1，Cmax=32 | Runtime |
| live Native/Scan | 每 Store 初始 1，与 C 独立 | session 预算/Adapter |
| 物理 batch | 128 项、8 MiB 原生请求、1ms 收集 | 调度器与 Adapter 精确大小校验 |
| Bulk outstanding/结果 | 每流 32 项、16 MiB credit | session 发送 |
| read-ahead | 一批 fetch、最多两个输出 frame | Adapter/relay |
| Scan 保留 page | 初始目标原生响应/decoder 16 MiB 加有界 framing 开销，需验证 | Adapter/session |
| Native 单体输入 | 如 BSON command 4 MiB，总量硬上限 | Adapter |
| 寿命 | 方法、后端、stream idle/send-stall、drain 都有限 | listener/Runtime |
| 转换资源 | 指令、分配字节、节点、深度、栈、source/compile、实际时间 | runtime |

示例期限：unary 30s、Bulk 15min、Native/Scan 5min、单次后端网络调用/fetch 10s、输入或发送停滞 30s、graceful drain 30s、最终 cleanup 2s。调用者只能缩短；遍历由多个有界 fetch 组成，不是整条流共享一次后端调用期限。HTTP Native 逻辑 upload 初始限 1 GiB，不一次性分配。所有值发布前都需负载验证。

单项必须连同原生 framing 在每层作为 singleton 装得下，否则写前拒绝。下游较小额度是正常失败，不能把一份原子文档拆成多次写。向 MongoDB 原生最大文档大小扩展须显式提高 Weir 帧/包络预算，不能自动推断。

Driver 可能完整物化原生响应批次，须记录其上限、控制 cursor count/bytes 或在 token 分配前限制增量 parser。ReadAll/完整 JSON decode 之后检查不构成内存边界。无法有界的操作不能宣传 streaming。Scan 也可因单文档太大失败，之前页为部分结果。

pending 字节记录保留的编码输入加固定条目开销，不冒充精确 heap。等收集、序列、并发时只计一次；派发后转移到有界 active ownership，完成后只剩结果额度。Scan 输入条目跨 fetch 保留，page 在释放 permit 前转至 session/result；不能再加 pending 条目。Plan/parser 的展开也须有限。

### 9.2 有界内存论证

固定配置下，保留内存的结构上界为：

```text
有限 transport connections/streams * 有限 transport/frame buffers
+ 各 Store pending 字节/条目预算之和
+ 各 Store Cmax * 有限 active batch/decoder/transform 工作集
+ 有限 sessions * 有限结果交付缓冲
+ live Scan sessions * 有限 page/cursor 状态
+ 有限后端 pools、caches、diagnostics
```

这不是精确 RSS 公式。分配器/GC、TLS、driver 和内核 socket 需要实测余量。不要用文档长度宣称精确 `max_active_heap_bytes`。校验并发乘最大 payload 对进程预算合理，再用慢消费者和最大文档测峰值；有界不等于适合某个容器。

### 9.3 进程过载保护

以 RSS（专用容器可用 cgroup 内存）对比显式预算，优先取配置和容器限额较小值；记录来源及不支持平台回退。仅 Go runtime 指标是降级信号，不是全进程内存证明。

约每 100ms 采样，80% 置 overload latch、70% 清除。高水位停止新准入，含已有流的新操作；低水位恢复。没有进程级等待队列或第二个 DB 控制器。已准入执行、结果和 cleanup 继续以释放内存，不能再受新准入条件阻塞。

Latch 不能防住任意突发分配或错误配置。frame/session/pending/concurrency/transform 上限是主要防线。CPU 也主要受这些额度约束，不另设计 CPU 调度器。

### 9.4 端到端流控

后端明确拥塞增加 -> C 收缩 -> Store pending 填满 -> Bulk receiver 停止 Recv -> 中间 relay 的单帧缓冲填满后停止 Recv -> HTTP/2 window 填满 -> 上游 Send 阻塞。

反方向慢消费者耗尽结果 credit，其进一步工作不可执行，Scan 等待 page 排空；共享批次其他成员仍可完成。Backend emitter 只能写入已预留的有界槽位，不能在全 Store 锁/调度循环中等待网络 Send。

全双工协议必须同时读写，两端都先写完再读可能死锁 [D5]。每方向仅有限 pump/frame，不收集全流。不受控/停止读取的客户端由 idle、send-stall、总寿命和连接上限收尾；结果缺失仍不是未生效证据。

## 10. AtomicTransform 与结构化值

### 10.1 两种显式转换形态

转换只针对一个记录：观察存在/缺失，执行确定性程序，用后端并发机制原子应用选择的效果。竞争可触发重复求值。不能访问第二记录、发事件、访问网络或产生应用控制的外部副作用。

ProgramTransform 含版本化 runtime、有界 source、可选编码 input，current/input 经 codec 进入 Value，Adapter 拥有读/验证/条件写循环。Backend expression 是携带媒体类型的原生确定性单记录表达式，不是便携 DSL；Adapter 验证并原子执行，Core 不解释。

V1 不把任意 Lua 降低到 MongoDB 或 Painless。安全原生表达式是快路径，程序是通用路径；Native 是更广命令的出口。Adapter 可支持其中一个、两个或都不支持。

程序输入为 `current = Missing | Present(Value)` 与可选同形 input，输出恰为：Replace(Value)（完整替换或缺失时创建）、Delete（观察到的存在记录删除，否则 no-op）、Keep（无效果）、Reject(有界文本)（NOT_APPLIED + PRECONDITION_FAILED）。

Missing 不等于 Null。Keep/Reject 是对观察版本的决定，不保证响应送达时条件仍成立。只读/no-op 事务不构成锁；写条件不变量需要真正的后端条件效果。

### 10.2 最小 Weir Value 模型

这是内部转换契约，不是所有请求的统一表示，更不是公共 Protobuf 文档模型。

| Value | 语义 |
| --- | --- |
| Null | 显式空，与缺失不同 |
| Bool | 精确布尔 |
| Int32 / Int64 | 精确有符号整数、保宽度且溢出检查的算术 |
| Float64 | IEEE binary64，保留有意义的位；不隐式缩窄整数 |
| String | UTF-8 字节 |
| Bytes | 不解释的字节序列 |
| Array | 有序 Value 序列 |
| Object | 有序 `(string,Value)` 字段序列，不是 map |
| Extended | 版本化 namespaced 类型 ID 与有界不透明字节 |

BSON 往返和稳定遍历需要有序 Object。Codec 可保留重复字段，但按名称访问重复名必须报歧义，不能任选一个；后端仍可拒绝重复文档。深度、字段/元素数、字符串长、解码分配和编码总长都需限制。

不贸然把 Decimal128、date、UUID、ObjectId、regex、binary subtype 变成可移植原语；例如保存为 `mongodb.bson.objectid.v1`、`mongodb.bson.decimal128.v1` 或类型化 binary Extended。BSON Int32/Int64 解码为对应整数，不是 Extended；Int64 即使值很小编码仍为 Int64 [D12]。其他数值/特殊位模式必须无损 Extended 或拒绝，不能经 float64 舍入。JSON 有符号 64 位精确整数域可解码为 Int64，其余可保留原 token 为编码专有 Extended。

同宽加减乘保持宽度，溢出在写前失败。混宽算术必须显式 checked conversion，整数/float 也必须显式转换。Int32 自增应加 Int32 one，不经 Lua 浮点隐式强制。未修改字段保持标签；Int64->Int32 要检查范围。类型化算术由固定 runtime profile 和 conformance vectors 定义，而不是某种 Lua/BSON 特例桥。

未知 Extended 可复制、移动、删除，不可伪造、隐式 JSON 化或参与数值/字符串运算。目标 codec 不支持的新值须写前拒绝；跨编码转码不在此保证内。

### 10.3 TransformCodec 与 runtime 分离

```text
原生不透明 current/input -> 有界 codec decode -> Weir Value
-> 确定性 runtime -> action + Value -> 有界 codec encode
-> Adapter 身份/输出校验 -> 原生原子写
```

Codec 位于后端/转换边界，只依赖 Value 而非 Lua；runtime 只依赖 Value，不依赖 BSON/JSON。N codec、M runtime 为 N+M 集成而非 N*M 特例。不透明 Adapter 在二者都没有时仍可用。

V1 可交付一个固定 Lua profile，但不能把 Lua 扩展进调度器或做存储过程服务。隐藏文件、网络、OS、时钟、随机、module loading、native pointer、locale 和 unrestricted debug。每次新状态，确定性对象遍历，精确整数构造/运算，显式 array/null/missing；不为方便而转 float。混宽规则见 10.2。

执行必须限制含 helper 工作的 fuel、分配字节、节点、深度、stack/call、source/compile、encoded output 和实际时间 watchdog。标准库能越限分配/阻塞的 runtime 不得批准进程内使用。编译在 Store execution admission 后，不在无界预准入 CPU 路径。

不可变编译产物可按 source digest+runtime/profile 版本做每 Store 字节及条目双有界缓存；每次仍带 source，无 digest-only 注册、部署或全局可变 registry。所需时间戳/seed 必须由调用者显式传入，跨重试不变。

负载可使 watchdog 失败时机不同，但相同输入的成功结果必须确定。测试覆盖 Int32 自增/溢出、未修改宽度、极值 Int64、显式混宽转换、Extended、重复字段、嵌套和耗尽，不能默认某个 Lua 引擎安全。

## 11. MongoDB：无自有文档元数据的原子 RMW

使用已确认的原生记录操作，遵循 URI 身份和唯一性/不可变 ID 规则。不使用 `_weir_revision`、`_sink_metadata`、revision 旁路集合、文档 hash CAS、分布式记录锁或整文档相等过滤来保证并发。匹配旧文档字段不能代替原生并发控制。

须验证 collection 布局：V1 允许无分片集合，或完整 shard key 就是 `_id` 的 ranged shard 集合，并要求简单 identity/index collation。其他布局拒绝 record API；`_id` 不是 shard key 时，默认 `_id` index 只保证各 shard 内唯一 [D7]。Native/Scan 仍可按其原生契约运行。布局检查在有界启动或 execution permit 内，cache 有界。不增加 shard-key DSL 或自动建全局唯一索引。布局/collation 并发改变不在 profile 保证内，须重新验证/重启。

### 11.1 原生表达式快路径

Adapter 可接受显式类型的确定性单记录 MongoDB update/update-pipeline 表达式，固定准确记录谓词并拒绝身份修改、跨集合访问、外部副作用、无界/非确定性行为。Options 保持原生并经验证。执行是一条原子 native update，不是客户端读写间隙。

支持集合是有限、测试过的 whitelist，不是乐观通用编译器。更广命令属于 Native，不能宣称可移植确定性转换。

初始 profile 为 `application/vnd.weir.mongodb-update.v1+bson`：有序 update 文档，顶层仅 `$set`、`$unset`、`$inc`，至少一个。拒绝 `_id`/子路径修改、位置 `$`、array filter、重复 operator/歧义字段和其他 operator；检查原生操作数类型、大小/深度。只操作现有记录（upsert=false），缺失为 NOT_APPLIED + PRECONDITION_FAILED。Pipeline 和更广 operator 仅 Native。该有限快路径不需要推测性的编译器。

### 11.2 通用程序路径

要求支持事务的部署、拓扑与存储配置，使用官方 driver session/transaction [D1]。默认 primary read、snapshot read concern、majority commit；不同 profile 需单独验证。事务内操作按顺序在同一 session 上进行。

```text
获取一个 Store execution permit
  启动新尝试的事务
  读取当前记录或缺失
  解码 current/input，执行确定性程序
  验证完整输出大小与身份不变
  替换/删除观察到的记录，或缺失时插入
  提交事务
  生成终态证据
释放 permit
```

原生客户端可自由 replace/update/delete。事务 write/commit 冲突必须防止覆盖其间提交的原生写；通过原生 abort/conflict 证据取得新 snapshot 并重新计算，不依赖 Weir revision。缺失 `_id` 插入竞争只有在明确未提交/中止后才能重试，不把其他 unique index 错误自动归为记录冲突。

### 11.3 事务状态机

| 事件 | 允许的下一步 | 停止时证据 |
| --- | --- | --- |
| commit 前读/解码/程序验证失败 | 有界 abort/close，不 commit | NOT_APPLIED + 相应错误 |
| Keep 或 Delete-of-observed-absence | 不写，关闭事务 | NOT_APPLIED，无 Failure |
| 明确 abort/conflict 或契约内 TransientTransactionError | 新事务、新读取、重跑转换 | 所有尝试都未提交时，耗尽预算为 NOT_APPLIED + CONFLICT |
| commit 获肯定确认 | 结束，不再执行 | APPLIED |
| UnknownTransactionCommitResult 或提交回复丢失 | 同 session/txn number 解析或重试 commit，不重读/重跑 | 原 deadline/commit-resolution 预算耗尽为 UNKNOWN |
| 明确最终拒绝且能证明未提交 | abort/close，按标签和证据分类 | NOT_APPLIED |
| commit 可能发出后取消/shutdown | 有界同尝试 cleanup，不能新写 | UNKNOWN，除非已有 APPLIED 证据 |

限制总事务尝试（初值五次）、短小有 cap 的冲突 jitter、总 program fuel 和 commit-resolution 时间，均受原 deadline 限制；每轮不重新发放完整 timeout/fuel。整个循环持同一个 permit，不重排队或递归准入。冲突等待有界。

一旦 commit 有歧义，普通后续错误不能清掉 latch，只有明确原生解析结果能。Abort 失败不是 rollback 证明。不能盲用无法约束 callback 重跑的便利事务 API，须审查 driver-native loop；普通 retryable write 关闭不禁止同 commit 的原生要求重试 [D1]。

No-op replacement/只读事务不等于独占锁。Keep/Reject/相同输出的决定只针对观察 snapshot，不针对发送时状态。真正改变状态的结果必须由原生事务验证读写依赖。集成测试必须覆盖原生 writer 冲突、同 ID 删除重建、缺失插入竞争。

### 11.4 不支持的部署与操作限制

Standalone/不支持事务的配置仍可支持原生 CRUD、Native 和已验证表达式，但不能宣称通用程序转换。应 UNSUPPORTED + NOT_STARTED，不增加元数据或采用非原子 read/replace。

集合/索引创建、sharding、事务前置准备由操作者负责，转换算法不偷偷创建它们。事务限制、session pinning、有界 server selection 需按支持版本验证。

## 12. Elasticsearch 与 OpenSearch 原子 RMW

支持的具体 index 必须具有可用 `_source` 和原生 OCC。Adapter 内部使用 `_seq_no` / `_primary_term` 条件对 [D2,D6]，分别验证后端与版本，不因 Elasticsearch 兼容就假设可用。

普通 index 请求也可能继承改变 source/目标的默认 ingest pipeline，这与客户端显式 Native 无关 [D8]。初始 profile 直接写 source：拒绝调用者 pipeline/routing override；使用验证过的 no-pipeline 选项绕过 default；source-writing 操作要求不存在有效 final pipeline。Elasticsearch `pipeline=_none` 只绕过 default，不绕过 final [D9]。每个 bulk item、每轮 transform 写都必须一致，而非只保护 unary Put。

这是窄 record-write profile，不是禁止 pipeline/Native。原生 writer 可继续使用默认 pipeline，Weir 不修改 index/pipeline 设置。Read/Delete/Scan 不因 pipeline 存在而关闭；存在 final pipeline 使初始 source-write profile 不支持，不代表所有 final pipeline 都会改目的地。Elasticsearch 明确禁止 final pipeline 改 `_index` [D8]。未来不改目的地的 pipeline profile 须另定 source/transform 契约，不在 Core 中解释。

有效设置在有界启动或执行 permit 内写前验证，无法验证就是 UNSUPPORTED，cache 有界。ingest/routing/source 配置并发修改须重新验证/重启。OpenSearch 的 bypass/update 行为独立验证 [D14]；update/expression 的 pipeline 若不能绕过就要求不存在，不能把 Index API 选项套到 Update API。

```text
GET 精确记录 -> source + 原生 seq/primary-term
解码 -> 确定性程序 -> 验证输出
带 token 的条件 index/delete
  acknowledged -> APPLIED
  definite conflict -> 在预算内重读、重算
  ambiguous send/response -> UNKNOWN，停止
```

GET 缺失且输出文档时用 create-only，不用无条件 index/upsert；明确 create conflict 才重启循环。缺失且 Keep/Delete 是观察型 no-op。其他验证错误直接终态，不当转换冲突。普通 Replace 也不得把并发已删记录重新创建，需原生条件行为。

同样限制总 attempts/fuel/deadline/短 backoff；通用网络重试或盲 `retry_on_conflict` 不是替代。支持的原生 script 可作快路径，但不转译任意 Lua，也不称 Painless 为 portable runtime。

原生 token 不进入 Core 或客户端通用 revision，仅 Native 可保留。禁用 `_source`、不兼容 synthetic source、source pruning 丢字段、不可用 OCC、unsupported routing/index/ingest 均禁用通用转换，不从 stored fields 重建有损文档，也包括禁用 sequence number 的配置 [D2]。操作中 index 删除重建不在保证内，token 跨 generation 不全局唯一。

普通 Replace 可读一次并条件替换一次，明确冲突返回 NOT_APPLIED + CONFLICT，不进入通用重试；允许冲突重算的是通用 transform。

初始搜索表达式 profile 为 `application/vnd.weir.search-update.v1+json`：原生 update body 恰含一个 `doc` 对象，针对存在记录，doc_as_upsert=false，无 script、服务端时间戳或调用者 retry count。原生 merge 语义有效；缺失为 NOT_APPLIED + PRECONDITION_FAILED。更广 script 仅 Native，通用程序仍用显式 GET/OCC。

## 13. Native 与 Scan 执行

### 13.1 Native 是无状态访问，不是隧道

Native 执行配置 Store 的一个 Adapter-defined interaction；descriptor 有类型、有界且对 Core 不透明。

| Profile | descriptor/body 契约 |
| --- | --- |
| `application/vnd.weir.mongodb-command.v1+protobuf` | descriptor 表示有序 BSON command body，database 来自 URI；整个 command 有界并写前校验。 |
| `application/vnd.weir.search-http.v1+protobuf` | descriptor 带 method、相对 path、原生 query 与允许的 repeated headers；body 为原生字节流；响应 metadata 保留 status 与允许 headers。 |

Schema 属于 Adapter 模块/文档，不是 Core switch。MongoDB descriptor data 为 empty Protobuf message，media type 定义 profile，body 放 command。搜索 descriptor 预留 1 method:string、2 path:string、3 query:string、4 headers:repeated Header；Header 为 1 name、2 repeated values。搜索响应 metadata 为 1 status_code:uint32、2 headers。body 媒体类型显式，metadata 不注入用户数据。

Adapter 校验凭据、目标/资源范围、操作类别、body 大小和 statelessness，不信任 client `read_only`；未知效果按可能写处理。HTTP verb 不足以证明，因为 POST 可读、原生命令可嵌套写。

HTTP 上游 host/auth 固定配置；拒绝绝对 URL、authority override、traversal、导致新上游请求的重定向、hop-by-hop/auth/Host headers、可导致 SSRF 的远程 fetch。重定向原样返回，不跟随。Native 可按后端语义访问同一授权 Store 的多记录/dataset，但不能跨 Weir Store/任意端点，也不新增跨记录原子保证。

MongoDB 保留有序 BSON command/response bytes，拒绝客户端 session、跨 RPC transaction control、逃逸 cursor/getMore、watch/change stream 等状态操作。内部单转换 session 不等于暴露应用 session。支持的游标遍历用 Scan。原生写无 Weir metadata 保护负担，因为不存在这些字段。V1 不开放 schema/index/cluster administration 或无界后台作业，不成为运维隧道。

### 13.2 Native 请求/响应边界

必须物化的 MongoDB command 有总量硬限制；切 chunk 不能令其无界。过大单体写前拒绝；大规模 record 用 Bulk，实时查询输出用 Scan。

支持的 HTTP Native 可背压流式 body 大于单帧，但 descriptor 需先验证，逻辑总量/寿命仍限制。如需检查 native item，则每个有限 item 在转发前验证；没有全量缓冲就不能宣称整 command 已全部验证。后续 item/chunk 无效时，前面已转发的可能生效；必须明确部分语义或拒绝该 streaming profile，不能偷偷缓存全上传。

回复保留原始字节、错误和 chunk 顺序，只检查 framing、安全范围、cleanup 和明确后端拥塞所必需信息，不增加效果解释器。HTTP 完整回复（含 200）表示 transport 完成，不代表所有 native item 成功；调用者解释原生 status/body。

### 13.3 Native 响应完整性，不规范化效果

| NativeCompletion | 含义 | Failure |
| --- | --- | --- |
| NOT_STARTED | 后端 command 从未派发 | 必须有，说明预检/准入/取消失败 |
| RESPONSE_COMPLETE | 完整原生响应及必要 framing 已收到并转发到 End 前 | 无；后端错误保留原生 status/metadata/body |
| RESPONSE_INCOMPLETE | 可能已派发，无法交付完整原生响应 | 必须有，有界 transport/framing/limit/cancel 错误 |

缺 End 或非 OK 终止在接收方均为 incomplete，即使下游曾看到完整响应。超时/断连不能推导 NOT_STARTED。后端可在上传 half-close 前完整拒绝，此时可 RESPONSE_COMPLETE，但 pump 必须按原生协议安全停上传。

完整原生错误不是 Weir Failure，完整 bulk reply 也不是全项生效证明。Native 不提供 APPLIED、NOT_APPLIED、PARTIALLY_APPLIED 或 read-only effect enum；这与记录 MutationOutcome 不同。内部只读分类可供授权/能力检查，但不是调用者安全断言。

Native 从不自动重放。可能派发后的部分效果/提交歧义由调用者按原生语义解释，response completion 不授权重试，也不将未知变未生效。Relay 不解析 body，只转发 End。

### 13.4 最小实时 Scan

Scan 统一的是 traversal delivery：资源、可选原生 selector、表示、有界 fetch hint、document stream、终态 count/error。MongoDB 可采用有序 BSON find/read-only aggregate selector，搜索可用 JSON；不标准化 projection/sort/count/pagination/filter。带输出写阶段的 selector 不属于 Scan。

MongoDB 每个原生结果文档以 BSON 输出；搜索每个原生 hit 以 JSON 输出并保留 hit metadata，不一定等于 Read 的 `_source`。Adapter 文档说明差异；不往已存用户数据加 wrapper/metadata，也不扁平化为假通用 search-row。

所有 Scan 均须验证完整性。MongoDB 拒绝 allowPartialResults=true，始终关闭 partial；任何 partialResultsReturned、cursor/shard 错误（包括后续 getMore）使遍历失败 [D16]。不能把可用 shard 子集当完整结果；cursor 控制、live/tailable 模式由 Adapter 拥有或拒绝。

cursor/PIT 仅活于一个 RPC，固定 Store/endpoint。每调度一次 open/fetch 最多得一个有界 page，释放 permit 后用 session 预算发送；页排空且 output credit 可用后通过同一 continuation 取下一页（7.5）。完成/取消/过期关闭原生状态。hint 受 item/bytes cap 限制，不先 count、不收全 hits、不存无界 resume positions。

搜索 Scan 须完成声明 selector 的全部遍历，不能只从 HTTP 成功响应提取 hits。V1 hit-traversal profile 保留原生 query/projection/支持的 ordering，但 pagination、PIT/cursor、fetch size 和 completeness controls 由 Adapter 独占。拒绝 partial、early termination、client pagination 和会被本流丢掉的 aggregation 等非 hit 输出。

PIT/cursor 创建及每次 fetch 均关闭 partial（验证过的请求可用 allow_partial_search_results=false），检查完整 envelope，要求 shard 完成、timed_out=false、无意外 early termination、framing 完好 [D10,D15]。字段因后端/版本不同，需 conformance test。缺失或无法验证的证据是失败，不是空页；query 正常 skipped shard 不当作失败。

最多保留一页并验证后才发 hits，避免尾部 metadata 逃过检查。失败页不发送，其前面的已发送页为部分结果，ScanEnd.failure 非空。Count 仅为实际交付数量，不证明完整。timeout -> DEADLINE_EXCEEDED，临时 shard failure -> UNAVAILABLE，畸形/矛盾 response -> INTERNAL，不自动重启。能发明确终态 Failure 时可 gRPC OK，否则非 OK。仅验证原生耗尽后可无 Failure 的 End。Native 不继承 Scan 这个完整性解释契约，仍返回完整原生 envelope。

V1 不设跨 RPC resume token，因为 snapshot、expiry、授权、混合 key 分页、replica affinity、cleanup 没有共同原生答案。live MongoDB cursor 避免不安全的混合 BSON `_id` keyset 分页；搜索可在同一调用中保留 PIT/search-after，不使 Weir 成为应用 session 服务。

不保证 portable snapshot；各 Adapter 说明原生可见性和并发变更行为。资源/快照过期或断连意味着部分结果加失败，不静默重启/拼接。需 resumability 或快照导出的应用使用原生能力，或自己重启并处理重复/漏项。未来恢复能力须单独审查。

## 14. Peer 转发与远程服务

### 14.1 在认证 peer listener 复用公共 RPC

RemoteWeir 使用相同 `weir.v1` Read/Mutate/Bulk/Native/Scan，unary 仍 unary，streaming 不改变形态。应用/peer 复用消息和验证/执行路径；没有 ForwardOpen/ForwardRequestFrame/ForwardResponseFrame 或 weir.mesh.v1。

唯一额外状态是可信 gRPC metadata [D11]：

| 上下文 | 规则 |
| --- | --- |
| weir-remaining-forwards | peer 必须恰有一个规范无符号十进制 0–8；重复、格式错、缺失都拒绝 |
| 应用入口 | 拒绝客户端提供该保留 metadata，内部创建初始预算 |
| peer 入口 | 认证 Weir 身份、授权 Store/操作族，保留/递减预算，不能重置 |
| request ID/trace/deadline | 使用既有有界机制，ID 不授权也不定义排序 |

header 存在不证明 peer 身份。独立 listener 使用 mTLS 或等效认证。只转发获准上下文，不转发原 authorization header 或任意 baggage。每跳重新验证并授权 Bulk 每项，终态规则与直接调用一致。

### 14.2 Deadline、hop 与错误

应用入口初始远程 hop 建议 4、配置最大 8；普通客户端不能提供。每次远程派发前必须 >0 并恰好减一。0 仍允许本地执行，不许再 forward。新 listener 不重置，不靠网络拓扑猜安全；无无界 visited list、动态发现或 consensus。

各跳传播剩余时间，扣除本地等待，维持原 deadline。取消尽量中止下游 I/O，不证明 rollback。写入可能送达后的 peer disconnect 为 UNKNOWN，除非收到更强完整结果；外层响应失败不能把下游 APPLIED 降为 NOT_APPLIED。

纯转发节点对每个新操作应用 overload latch，并限制 live relay/outstanding I/O；无本地 Store queue/DB controller。压力下停止新操作，但继续已准入 Native 的有限 body/results。

每方向最多一个有界 frame，立即转发；unary 只保留有限 request/result/status。NativeCompletion 和 ScanEnd.failure 原样保留，不重排、不汇总、不在 remote-only 节点重新组批或解析字节。仅最后的本地 StoreRuntime 组批。

### 14.3 端点选择与亲和

RemoteWeir 配置有限稳定 endpoint 身份、有限 channel、基本健康/连接状态、简单有界重连 backoff。配置名可用普通 DNS，不是动态 provider/control plane；无复杂 outlier scoring、hedging 或 ring ownership。

单记录 Read/Mutate 以 canonical URI bytes 和可用 endpoint identity 计算固定文档化 hash/framing 的 rendezvous score，取最高分，仅优化局部性，不保证串行。Native/Scan 可 hash resource；Bulk hash bounded request ID 并整流固定端点，不能承诺其中每个记录的亲和，否则需要 fanout。无公共 affinity hint。

派发前选择可用端点；连接建立失败且证明无语义请求发出时，可在原 deadline 内换端点。一旦写入或流的任何部分可能转发，不换端点重放。Read 重试也须明确策略且在结果交付前。Health 只影响未来调用，不证明当前写入状态。

同一 Service 所有 endpoint 必须代表同一逻辑 Store、授权、Adapter 语义和协议 profile；Weir 不协调不一致副本，部署/滚动期间须验证。宁可 UNSUPPORTED，不能静默改变意图。成员变化可破坏亲和，不得破坏原生正确性；一般一两跳足够，不把 hop budget 当鼓励深层链路。

## 15. 优雅生命周期、健康与可观测性

### 15.1 关闭状态机

Constructing -> Serving -> Draining -> Closed 单调迁移，drain 从进程统一 barrier 开始：

1. readiness 改 NOT_SERVING，拒绝新应用/peer 调用和 Bulk 操作，停止接新连接，保留已有结果路径。
2. 原 deadline 和 drain deadline 内继续派发有限已准入 pending，直接 flush 收集窗；不等无限 Bulk producer half-close。
3. 已接收未准入项可 NOT_STARTED + UNAVAILABLE；不继续读取无界输入只为制造拒绝结果。尽量完成已知结果，再关流；未报告已发送写入在客户端为 UNKNOWN。
4. 已准入 Native upload/Scan cursor 在原预算内继续，随后关闭 cursor，不迁移状态。
5. drain 到期，未派发项可报告 NOT_STARTED，取消 active I/O，以短 cleanup 额度关闭 session/cursor。取消不是 rollback 证据。
6. 有界发送结果，关闭 peer 连接，每 Adapter Close 恰一次，然后关闭 diagnostics。只 join 有限 goroutine，到 cap 强制关闭，不能无限等恶意客户端。

“停止调度再 drain”会把已准入工作卡死。应停新准入，继续调度已准入集合，到 deadline 再停执行。无重排到其他节点、后台重放、持久 accepted work 或 queue settlement。Crash 丢失内存工作，后端效果/未送达结果可能不确定；SIGKILL 不是优雅关闭。

### 15.2 健康

Liveness 表示进程能运行，不是所有数据库健康。Readiness 表示组装已验证且未 drain；常规 saturation/cooldown/短 memory latch 不应引起 readiness 抖动和级联重启。Store 级健康/能力独立暴露，不因一个失败 Store 隐藏其他可用路由。

优先用最近执行证据和基础 transport health；可独立有界连接检查，但 Ping 不提高 C。永久认证/拓扑失败应明确使必要 Store 构建失败；不隐藏部分 Store 启动 fallback，V1 开始服务前验证全部配置 Store。

### 15.3 Metrics 与 trace

使用 Prometheus/OpenTelemetry，不自建平台。标签只能是有限 listener/method、配置 Service/Store/Adapter、操作族、错误/outcome、有限减窗原因。

记录 pending/reserved 项/字节、active/live session、Scan fetch/emit、output credit、准入拒绝、排队/收集延迟；批次数/字节/填充率/每后端调用对应应用项数/单项回退；C/active/增减/cooldown、后端耗时、conflict/UNKNOWN；stream bytes/frames/completion/failure/stall/truncation/cursor cleanup/hop；内存预算/用量/latch、pool 使用/等待、转换限额/cache、生命周期/drain/强停次数。

不能用记录 ID、任意 URI、query、程序 source/digest、文档字段、request ID、endpoint churn 或错误文本作 metric 标签。有限配置 endpoint 身份可记录，不能形成任意维度。Histogram buckets 也必须有限。

Trace 分开 queue、backend、transform、emit、forward；request ID/index 可进采样日志/trace，不进 metric 标签。默认不记 payload/secret；错误文本有界并脱敏。服务端 APPLIED 日志是诊断，不是持久客户端回执。

## 16. 建议仓库与模块布局

起步只有一个仓库、一个 Go module：`github.com/batchstream/weir`。公共 schema 与生成类型保持清晰分离，SDK 无需导入 server。未来拆仓库是打包决定，不是 V1 依赖；不因 Sink 有协议仓库就另建。

```text
cmd/weir/                       CLI、进程信号所有权
proto/weir/v1/                  公共 schema 源码
protocol/weir/v1/               生成的公共消息/client/service stub
resource/                      规范 URI，无后端语法
internal/
  app/                         经验证组装和生命周期
  config/                      严格静态解码、默认值和验证
  transport/                   应用/peer handler、可信 metadata、有界 pump
  service/                     精确路由、LocalStore/RemoteWeir
  store/                       Runtime、work/result、调度、组批、自适应、Adapter 契约
  backend/
    mongodb/                   client/pool、语法、codec、事务 RMW
    search/                    经过独立验证的 ES/OpenSearch 机制
  transform/                   Value、codec/runtime、受限 Lua profile
  overload/                    内存采样与滞回 latch
  observability/               有界指标/trace/log
 docs/                         架构及 Adapter/操作者契约
```

这是未来布局提案，原设计任务不创建目录。完整形态由 app 导入具体后端组装，后端依赖 store 的小型执行契约及 transform 值/runtime 契约，store 不反向依赖具体后端；transport 不导入 BSON/JSON，RemoteWeir 不创建本地后端 plan。当前里程碑按文首限定使用更少包和直接 Adapter 所有权，不提前制造该接口图。

只在两种真实 Service、多个 Adapter、codec/runtime 和标准外部依赖边界使用必要接口。不增加函数变量测试入口、通用 retry 包、lane、plugin/provider registry 或单实现薄包装。完整多实现边界可用确定性 fake Adapter/clock 测调度，而非替换生产全局函数；真实后端正确性仍要真实测试。

## 17. 基于证据的 Sink 组件审查

### 17.1 检查的快照与方法

原设计检查 `batchstream/sink` 的 `a08a1c53c5de2045176be3910197f73d5fe139b9` 和 `batchstream/sink-protocol` 的 `31943c4a6984468bc57aca348723ab812d26b842`，是固定快照而非当前分支状态断言。

覆盖 schema、URI codec、routing/forwarding、batch/scheduler、反馈、内存 guard、storage interface、Mongo metadata/native-write、search OCC、Lua、流和组装。原设计仅选择性阅读测试，没有运行应用/后端实验；本里程碑新增证据另见 `milestone-1.md`。可行性和 backend qualification 仍是第 20 节门槛。

源码与陈旧文字冲突时以源码为据。例如 Sink architecture 说请求顺序返回，而固定协议明确允许乱序 indexed result [S1,S2]。Weir 保留有界乱序契约，不继承过时说明。

### 17.2 处置矩阵

| 原机制/证据 | 决定 | 理由 |
| --- | --- | --- |
| URI typed key、有限 envelope、index/Failure [S1] | 保留思路，重写协议 | 保护身份/关联；去掉旧 encoding/completion/async 负担。 |
| Gateway/Engine/Worker、独立 forward 协议 [S2] | 删除角色，简化转发 | 单二进制，真实 Service 变体，共用 RPC + metadata。 |
| Store/endpoint stream fanout [S2] | 删除 | 一个流一个 Store/Service，不管理扇出聚合。 |
| 显式反馈、自适应窗口/epoch [S3] | 保留并简化 | 一个 AIMD，不保留延迟分桶/双 EWMA/全局容量暗示。 |
| Admission ticket queue、重复队列 [S3,S4] | 合并 | 每 Store 唯一 pending/ledger，重试不重新准入。 |
| 微批、兼容 token、共享 deadline [S4] | 保留机制，重写所有权 | 同路径，单次计账，有界输出，取消不伤害其他成员。 |
| revision merge、存储 metadata、native-write 修复 [S5] | 删除 | 并发由事务/OCC 保证，不拥有用户字段。 |
| Core 编码转换、Lua 专用 merge [S5,S6] | 移至 Adapter/转换边界 | Core 文档不透明，Value 解耦 codec/runtime。 |
| 有界纯转换与 cache [S6] | 保留目标，重新验证 | 资源隔离必须实际通过，不能继承未经证明的 sandbox。 |
| Lua JSON/BSON 各自 bridge [S6] | 重写 | N+M，而非 N*M。 |
| Native/增量 search decoder [S7] | 保留有界 transport，删除效果规范化 | Native 原样响应；Scan 每页完整验证后发 hits。 |
| Query/count/page 全量与 portable continuation [S1,S7] | 删除/收窄 | 不提供通用分页，cursor 仅一个 live RPC。 |
| 有界 metrics、集中 lifecycle [S8] | 保留并简化 | 用 Store/Service 维度，不含角色/Kafka settlement。 |
| Kafka/Publisher/Consumer/Worker/topic/retry/DLQ/offset/queue codec [S9] | 删除 | 范围之外，不补一个 Queue 接口。 |
| 通用 retry、分布式排序、lane、xDS/Provider、复杂 outlier | 不引入 | 被拒绝的设计，不宣称旧源码全含它们；无已承诺性质需要它们。 |

## 18. 抽象与可移植性审计

### 18.1 对保留概念的五问审查

逐项审查所保护的需求、为何必要、更简单替代、所有权边界及最终保留形式：

| 概念 | 保护的需求及最简形式 |
| --- | --- |
| Listener/Route/Service | 信任边界与目标选择；有限 listener、精确 map、两种真实 Service，不增加角色或动态 provider。 |
| StoreRuntime/Adapter | 单次准入、后端原生执行与资源生命周期；小型明确所有权，不设 PoolManager。 |
| 一份 pending ledger/调度 | 有界内存和无重复计账；有限 O(P) 选择，不另加 lane/ready queue。 |
| BatchKey/微批 | 兼容原生调用降低成本；不透明 token、有限收集窗，不合并逻辑效果。 |
| Bulk sequence key | 同流同 key 契约必须保留；只引用有限 live work，不扩大成全局记录锁。 |
| AIMD/overload latch | 有限 DB 并发与进程保护；各自明确职责，不需要合成容量或第二 admission queue。 |
| opaque Document/Value | Core 不拥有字段，转换能无损处理；有限 Value + Extended，不统一所有编码。 |
| codec/runtime | 不同表示与语言解耦；N+M，纯执行资源限制，不制造插件系统。 |
| MutationOutcome | 区分未开始、确定未生效、生效、不确定；没有 retry Boolean/持久 receipt。 |
| Native completeness | 原生交互保留语义；只规范 transport，不跨后端解释任意效果。 |
| live Scan/continuation | 有界完整遍历交付；一页、一个原生 cursor、共享调度；无恢复服务/专用队列/短请求 floor。 |
| peer/hop | 跨进程保留意图/结果/deadline；现有 RPC+可信 metadata，不需另一 schema。 |
| immutable assembly/lifecycle | 启动失败回收与有限 drain；唯一 owner 和单调状态，不动态配置。 |

同流排序是协议要求，不能删掉却继续宣称相同语义；跨请求串行不实现。Compile cache、rendezvous affinity 是可选性能策略，不改变正确性、不产生控制面/所有权协议。

### 18.2 后端无关与原生契约

| 契约 | 可移植范围 |
| --- | --- |
| 不透明表示、有界交付、资源路由 | 与查询语言无关的字节/framing/execution 性质。 |
| Read/Create/Replace/Put/Delete 意图 | 仅作为显式能力；key/record/ack 由 Adapter 定义，不模拟不支持的原子性。 |
| 写入执行证据 | 对“知道什么”的描述可移植，证据来源原生。 |
| AtomicTransform 意图 | 只对支持的 Store；事务/OCC/expression 实现原生。 |
| Value | 小型内部 interchange，不是万能原生类型；Extended/拒绝防止损失。 |
| Native | 不可移植命令的有界 envelope/完整性；效果解释原生。 |
| Scan | 交付/生命周期/完整性；selector/order/visibility/snapshot 原生。 |
| visibility/durability/revision/query/count | 不通用，不在 Core 伪装抽象。 |

## 19. 最后一轮简化

本轮是设计已完成的审查，不是未来 TODO：

- 队列不重复：Store 只一份 pending，key/batch 仅引用；remote 只有限 I/O 帧，driver checkout 不超过 active permit。
- 准入不重复：逻辑操作一次；冲突重试、预留 continuation 不重入；session 限制状态而非另一个 DB scheduler，各 peer 保护自己的进程。
- 无角色模式：LocalStore/RemoteWeir 是路由目标的真实差异，不是运行角色。
- 无意外兼容：新 URI/API/outcome/config，无 Sink translator/mode/field/import/shim。
- Core 不耦合编码，Adapter/codec 拥有文档/options；无隐藏字段、revision 集合、旁路账本、ID 改写或原生写修复。
- 删除伪可移植 query/count/revision/completion visibility；原生 options 明确原生。
- 无分布式锁/lease/leader/replica quota/consensus/全局排序或容量承诺。
- 静态 Adapter/runtime，无 hot reload/Provider/xDS/plugin/program registration/通用 SDK retry。
- 不上线 BatchKey、execution plan、Lua registry、affinity hint、client read-only、Native effect enum 或 mesh wrapper；hop 用可信 metadata。
- 无 Kafka/异步交付/topics/offsets/DLQ/settlement、schema/index 管理或后台 job。
- session/frame/operation/result/pending/key/active/cursor/parser/cache/log/shutdown 均有限。
- 应用/peer、unary/Bulk、batch on/off 汇入同一 Runtime；Scan continuation 使用同一调度，空闲 cursor 不占 execution window。

保留 Bulk correlation、Native response completion、Scan terminal completeness、可信 hop，因为各自保护明确要求。删除 Native effect normalization、mesh framing、跨客户端读链、Delete affected-row 区分、延迟分桶、portable resume、operation folding、post-image、程序 lowering、公共动态 capability discovery。Adapter 生命周期明确，不另造 pool/session manager。

## 20. 架构批准后的分阶段计划

原设计不授权实施。2026-09-26 的请求授权文首收窄后的基线、可行性验证和单节点 MongoDB Read/Mutate/Bulk；下表仍是更大计划，不授权余下阶段。

| 阶段 | 范围 | 批准/退出证据 |
| --- | --- | --- |
| 0 契约确认 | 决策、后端/runtime profile、精确版本和额度 | 明确书面授权，不能只因文档存在而实施。 |
| 1 协议/语义向量 | schema、URI/media、outcome、NativeCompletion、ScanEnd、peer metadata | 无效变体/状态、流完成、原生错误与 transport 区分、spoof/malformed hop。 |
| 2 单节点 Runtime | 静态组装、单调度/账本、流内序列、AIMD、Adapter pool 所有权 | 取消竞争、独立 Read 并发、同流排序、Cmin=1/Scan 预算、单 Close、无重复计账、有限结果、离线测试。 |
| 3 MongoDB CRUD/通用转换 | 原生原语、Delete batching、事务 RMW、无损 codec/runtime | 原生 writer/插入竞争/commit ambiguity、整数精确/保宽、attempt/fuel、standalone 拒绝程序。 |
| 4 搜索后端 | ES/OpenSearch identity、ingest/source、OCC/Create/Replace | default bypass/final pipeline 拒绝、Native 不变、条件冲突/传输丢失；两个产品分别验证。 |
| 5 流式表面 | Bulk、raw Native、完整 page Scan、有界 session 和共享 fetch | partial shard、timeout/early termination、失败页不发、慢 Scan C=1 让出、Native stall、early response、cursor cleanup、RSS plateau。 |
| 6 远程组合 | 复用 RPC、peer auth/hop、deadline、affinity/health | direct/forward 同语义、形态不变、stream、spoof/重复/零 hop、丢结果不重放。 |
| 7 验证与运维 | metrics、drain、打包、secure listener、profile | 多控制器/stale epoch/负载、内存 CPU 边界、各状态 drain、单次 Close、有界指标。 |

表达式快路径只有 whitelist/validator 通过后才开放；否则 UNSUPPORTED。不为 benchmark 推测 Lua lowering。进程内 Lua 不能约束分配/helper/fuel 时，程序转换保持关闭，等待单独审查的 runtime 决策，不能默默降低 sandbox。

### 20.1 必须覆盖的失败与一致性场景

| 场景 | 必需结果 |
| --- | --- |
| 执行前 pending 满或进程 latch 拒绝 | NOT_STARTED，不开始后端尝试。 |
| 取消与派发竞争 | 单状态迁移，可能发送后不误报 NOT_STARTED。 |
| 共享批次混合 deadline | 一个过期不取消无关存活者，不重放过期写入。 |
| 跨客户端批次有慢接收者 | 预留有界输出，不阻塞全局 Send/其他客户端。 |
| Mongo commit 成功后丢响应 | 仅同事务解析，无法解析 UNKNOWN，算法不重复转换生效。 |
| 与原生 replace/update/delete 竞争 | 新 snapshot 重算，不丢更新、不加 metadata。 |
| ES 条件冲突与断连 | 只有明确冲突可重算，断连不触发重放。 |
| Native bulk 混合成功/错误 | 原样响应，不解释效果；完整回复才 RESPONSE_COMPLETE。 |
| Native 断连/提前拒绝 | 截断 incomplete；完整提前拒绝可 complete，不推定 rollback。 |
| Int32 自增/旁边字段修改 | checked 算术，原 Int32/Int64 宽度不变。 |
| 整数溢出/混宽/>2^53/Extended | 写前失败或显式转换，不经 JSON/float 强制。 |
| 恶意程序/selector/deep document | 在失控分配/执行前限制 allocation/fuel/depth/output/time。 |
| Bulk 中途跨 Store | 当前项拒绝，不 fanout/回滚此前。 |
| UNKNOWN 后同 key | 不承诺后端顺序，有条件依赖由客户端显式等待。 |
| 长期后端慢且上游持续生产 | pending/session 内存平台，HTTP/2 传递背压。 |
| 慢 Scan C=1 | page/cursor 占 session 不占 permit，短工作可派发；stall 后 cleanup。 |
| 慢 Native C=1 | 直到有界取消可占唯一 permit，不承诺短操作容量。 |
| 搜索 page timeout/shard/early termination | 失败页不输出，此前部分，必须 Failure，即使 HTTP 200。 |
| Mongo partial/getMore 丢 shard | 拒绝 partial 选项，不能成功 ScanEnd。 |
| search source write 受 ingest 改目标 | bypass default 或拒绝；初始拒绝 effective final，Native 不变。 |
| Delete bulk 有缺失 | 完整成功项 APPLIED，不需 deleted count/pre-read；不明确项 UNKNOWN。 |
| 独立同 key Read 与 Bulk sequence | 前者并发，后者严格本流次序，重复 diagnostic ID 也不共享序列。 |
| Adapter 构建/关闭 | 已建资源仅 Close 一次，Runtime 不另关 pool。 |
| peer cycle/spoof/missing hop/deadline | 拒绝无效信任上下文，不能重置 hop/deadline。 |
| 各排队/upload/事务/commit/Send 状态关闭 | 有限 drain、正确证据、无 detached replay/无限 goroutine。 |

默认离线测试不能开浏览器、连生产或执行长期外部工作流。真实后端/fault suite 必须显式 opt-in，使用隔离数据；编译/mock 不证明事务或真实流正确性。本任务不含生产 rollout/release。

### 20.2 需要确认的决策，而非遗留正确性漏洞

推荐的 V1 决策已有明确描述：四类操作加 Bulk、逐 fetch 调度且完整验证的 live Scan、应用/peer 共用 RPC、流内同 key 排序、精确整数转换、直接搜索 source-write profile、原生 response completeness、显式 AIMD、静态配置、无 post-image/resume/自动写入重放。授权可减少能力，不能降低 outcome/atomicity/资源不变量。

验证必须固定支持的 MongoDB/ES/OpenSearch 版本和 topology/ingest/partial profile，选择满足类型及资源要求的 Lua，验证表达式白名单并测量初值。这些是 release gate，不是采用非原子或无界实现的许可。

## 附录 A. 来源登记

本附录路径均相对所列固定仓库，不是新 Weir 工作目录。它们支持原始设计审查和后端事实，不表示这些 V1 能力已经实现。原设计直接阅读官方文档或其官方源码；部分文档站无法访问时采用 MongoDB driver specification 和 Elasticsearch 源文档。当前里程碑的真实测试证据单独记录。

### Sink 源码组

S2–S9 来源根：`https://github.com/batchstream/sink/tree/a08a1c53c5de2045176be3910197f73d5fe139b9`。

| ID | 已检查的证据和定位点 |
| --- | --- |
| S1 | `batchstream/sink-protocol` 固定于 `31943c4a6984468bc57aca348723ab812d26b842`: `proto/sink/sink.proto:11` (RPCs), `:28` (编码枚举/文档), `:41` (完成模式), `:65` (乱序结果), `:199` (Failure); `uri/address.go:41` (严格解析), `uri/key.go:41` (类型化 key 格式化). |
| S2 | `docs/architecture.md:10`; `proto/forward/forward.proto:7`; `internal/gateway/routes.go:31`; `internal/gateway/record_stream.go:15` (Store/端点扇出); `internal/gateway/connections.go:81` (有界 transport/重试配置); `internal/gateway/discovery.go:210` (亲和). |
| S3 | `internal/backpressure/controller.go:42`, `:275`, `:350` (窗口/代际/延迟/减窗); `internal/backpressure/storage.go:207` (发送等待/后端采样); `internal/backpressure/admission.go` (ticket 队列); `internal/backpressure/cold_start_regression_test.go:9`. |
| S4 | `internal/service/batcher.go:112`, `:235`, `:377`, `:474` (提交/选择/共享期限); `internal/service/stream.go:105`; `internal/storage/storage.go:14` (BatchKey 与 Storage), `:36` (错误分类). |
| S5 | `internal/service/write_group.go:31` (基于 revision 的合并循环); `internal/service/write.go:124` (Lua 专有 merge 解析); `internal/service/convert.go:17` (中央编码转换); `internal/storage/mongodb/bson.go:134` 以及 `:180` (元数据注入); `internal/storage/mongodb/write.go:288` (revision 条件); `internal/storage/mongodb/native_writes.go:13` (原生写入 revision 保护); `internal/storage/mongodb/conditional_bulk.go:19` (逐项与聚合条件结果). |
| S6 | `internal/merge/merge.go:23` (程序/输入契约); `internal/merge/lua_engine.go:19` 以及 `:92` (资源初值/cache/compile); `internal/merge/json_bridge.go`; `internal/merge/bson_bridge.go`; `internal/merge/lua_allocation.go`; `internal/merge/result_budget.go`. |
| S7 | `internal/storage/search/write.go:250` (原生条件字段); `internal/storage/search/revision.go`; `internal/storage/search/client.go:70` (端点/重试行为); `internal/storage/search/stream.go:11` (增量 hits); `internal/storage/mongodb/native.go`; `internal/storage/mongodb/native_query_validation.go`; `internal/storage/scan.go`. |
| S8 | `internal/capacity/guard.go:72` (每 context 一次准入), `:113` (滞回), `:128` (内存采样); `internal/metrics/store.go:13` (有界标签); `internal/app/lifecycle.go:17` (依赖角色的生命周期), `:83` (Close); `internal/app/app.go`; `internal/config`. |
| S9 | `internal/queue`, `internal/queue/kafka`, `internal/worker`, `internal/service/publish.go`, `internal/app/kafka.go`, 以及 S1 中的异步完成字段；均在 Weir 范围之外。 |

S1 来源根：`https://github.com/batchstream/sink-protocol/tree/31943c4a6984468bc57aca348723ab812d26b842`。定位点是调查入口，不授权复用旧包边界；区分已观察源码和新提案。

### 官方后端/传输资料

- **D1 - MongoDB 事务规范。** `https://github.com/mongodb/specifications/blob/f5ba7f7aaa0417e35671a3ed7a8cd26be3a8e5ba/source/transactions/transactions.md`。重点是 commitTransaction、事务身份、事务内可重试命令和错误报告，支持明确中止后重开与未知提交解析之间的区别。
- **D2 - Elasticsearch 乐观并发控制。** `https://github.com/elastic/elasticsearch/blob/a68c26a6a64a994ef98a2465dc25453df78a39f1/docs/reference/elasticsearch/rest-apis/optimistic-concurrency-control.md`。原生 sequence/primary-term 条件及禁用 sequence number 的限制。
- **D3 - gRPC 重试。** `https://grpc.io/docs/guides/retry/`。配置重试、透明重试、重放/commit 边界。关闭应用重试策略不等于所有 transport 都从不重试。
- **D4 - gRPC deadline。** `https://grpc.io/docs/guides/deadlines/`。明确 deadline 和剩余 timeout 传播，不重置新预算。
- **D5 - gRPC 流控。** `https://github.com/grpc/grpc.io/blob/516c96d2c1c04a141a619b9ffa3c7573ebee4f72/content/en/docs/guides/flow-control.md`。写缓冲、应用消费和全双工死锁。
- **D6 - OpenSearch Update Document API。** `https://docs.opensearch.org/latest/api-reference/document-apis/update-document/`。原生 if_seq_no/if_primary_term 条件；仍须按版本验证 profile。
- **D7 - MongoDB 唯一索引与分片。** `https://github.com/mongodb/docs/blob/4da4cc66f81e1aa701fb55ca8b6940851e409d0a/content/manual/manual/source/core/index-unique.txt`。分片唯一性限制要求正确 URI/record identity；不能漏掉必要 shard identity。
- **D8 - Elasticsearch ingest 与 index 设置。** `https://www.elastic.co/docs/manage-data/ingest/transform-enrich/ingest-pipelines`；`https://www.elastic.co/docs/reference/elasticsearch/index-settings/index-modules`。default/final pipeline 选择，以及禁止 final pipeline 修改 _index。
- **D9 - Elasticsearch Index API。** `https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-index`。pipeline=_none 绕过 default，不绕过已配置 final。
- **D10 - Elasticsearch Search API。** `https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search`。partial-result、timeout、shard failure 与 envelope 证据。
- **D11 - gRPC metadata。** `https://grpc.io/docs/guides/metadata/`。可携带有界转发状态；身份认证由 listener 负责，不来自不可信 header。
- **D12 - MongoDB Go BSON 编码。** `https://www.mongodb.com/docs/drivers/go/current/data-formats/bson/`。结构化转换往返必须保留 BSON Int32/Int64 宽度。
- **D13 - MongoDB Go bulk 操作。** `https://www.mongodb.com/docs/drivers/go/current/crud/bulk/`。聚合结果不同于逐项 verbose 结果；普通 Delete 不必暴露 affected-row 区别。
- **D14 - OpenSearch Index Document API。** `https://docs.opensearch.org/latest/api-reference/document-apis/index-document/`。固定产品/版本并测试 pipeline 和条件写。
- **D15 - OpenSearch Search API。** `https://docs.opensearch.org/latest/api-reference/search-apis/search/`。独立于 ES 验证 partial-result 控制和完整响应。
- **D16 - MongoDB find 与 cursor 响应。** `https://www.mongodb.com/docs/manual/reference/command/find/`。allowPartialResults 影响 find/getMore；partialResultsReturned 不能视为完整 Scan。Cursor batch 有 item/byte 限额，还需 framing/driver 分配余量。

## 附录 B. 要求覆盖索引

| 要求项 | 设计位置 |
| --- | --- |
| 1. 产品范围/非目标 | 1.1 |
| 2. 核心原则 | 1.2-1.3 |
| 3. 最小架构 | 决策摘要; 2 |
| 4. Listener 模型 | 2.1 |
| 5. Route/Service | 2.2 |
| 6. LocalStore/RemoteWeir | 2.2; 14 |
| 7. 单节点部署 | 2.4 |
| 8. 多节点部署 | 2.4; 14 |
| 9. 一个请求一个 Service | 2.3; 5.3 |
| 10. StoreRuntime | 6.1 |
| 11. 调度器 | 7 |
| 12. 局部序列/额度 | 7.2 |
| 13. BatchKey/微批 | 3.1; 7.3-7.4 |
| 14. 自适应并发 | 8 |
| 15. pending 边界 | 7.1; 9.1 |
| 16. 进程过载 | 9.3 |
| 17. 公共协议 | 5 |
| 18. Read/Mutate/Native/Scan 评估 | 5.1 |
| 19. Query/Count 决策 | 5.1; 18.2 |
| 20. 规范 URI | 3.1 |
| 21. 不透明文档 | 3.2 |
| 22. 媒体类型 | 3.2 |
| 23. TransformCodec | 10.3 |
| 24. Value 模型 | 10.2 |
| 25. AtomicTransform 语义 | 10.1 |
| 26. runtime 边界 | 10.3 |
| 27. MongoDB 原子 RMW | 11 |
| 28. ES/OpenSearch 原子 RMW | 12 |
| 29. 记录 outcome/Native completeness 边界 | 4; 13.3 |
| 30. 重试规则 | 4.3; 11.3; 14.3 |
| 31. Native 执行 | 13.1-13.3 |
| 32. 请求/响应流 | 5.1-5.3; 9; 13 |
| 33. 有界内存 | 9.1-9.2 |
| 34. 端到端背压 | 9.4 |
| 35. peer 复用/信任边界 | 2.1; 14.1 |
| 36. 多跳/循环 | 14.2 |
| 37. 端点选择/亲和 | 14.3 |
| 38. 优雅关闭 | 15.1 |
| 39. 健康/可观测性 | 15.2-15.3 |
| 40. Adapter 契约 | 6.2 |
| 41. 包/模块布局 | 16 |
| 42. Sink 保留/迁移/重写/删除审查 | 17; 附录 A |
| 43. 复杂度审查 | 5.5; 18; 19 |
| 44. 批准后的实施计划 | 20 |

整个设计仍可解释为 Listener -> Route -> Service -> 本地 Runtime/Scheduler/Adapter 或远程 Weir，执行有界、失败语义明确，不需要额外运行角色或外部协调服务。
