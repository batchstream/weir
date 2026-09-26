# 第三里程碑：单节点、有界、可背压、可验证完整性的 Scan

本次从干净的 `feb8bf8` 开始，使用本地 `randy/scan-m3` 分支。范围为现有单节点
MongoDB、Elasticsearch、OpenSearch Store 的 server-streaming Scan；不是完整 V1 或
生产资格认证。未增加 Native、Query、Count、resume token、跨 Store/节点扫描、SDK、
动态配置或通用变换。AtomicTransform 继续 UNSUPPORTED，记录 pipeline/OCC/重试规则不变。
实现与测试的本地提交为 `4fb97f5`；本任务不推送、不创建 PR、不合并或部署。

## 1. 协议与完成条件

新增架构已定义的 `ScanRequest`、`ScanEnd`、`ScanResponseFrame` 和 Scan RPC；不改变旧
RPC 或字段号。一次请求只解析一次路由，资源分别为配置的 collection 或 concrete index。

`document_count` 精确定义为**服务端文档帧 Send 返回成功的次数**，不代表客户端已经接收
或处理。客户端必须同时满足：恰一完整 End、实际观察的文档数等于 End.document_count、
End.failure 为空、随后收到最终 gRPC OK/EOF。缺 End、计数不符、非 OK、End 后额外帧均
不能成功；gRPC OK 加非空 Failure 也仍是失败。

失败页面不输出任何文档；前面成功输出的页面是部分结果。能发送明确终态时返回带 Failure
的 End；传输已经失效时为非 OK/截断。没有根据 total hits、HTTP 200、Send 返回或空 body
推断完整性；不重试失败页面、不重开 PIT/cursor，不拼接另一轮遍历。

双语架构唯一契约澄清：把旧的“实际交付数量”改成上述可验证的 Send/客户端计数语义。
架构没有写本次进度、资源成绩或资格结论。

## 2. 所有权和调度

| 对象 | 持有者和释放时点 |
| --- | --- |
| continuation | Runtime 的同一个 Ticket；进入唯一 FIFO/live 账本一次，保留输入字节和条目额度直到清理及发送方释放都完成 |
| execution permit | 既有 batch 调度路径；只在 open/fetch 时持有，Adapter 返回并完整验证页面后、唤醒消费者前释放 |
| session/page reservation | 每 Store 固定一个 Scan；独立于 C，按 Adapter 最坏页面/decoder 工作集预留；发送方停止借用页面且 native cleanup 完成后释放 |
| 当前页面 | Runtime 发布，handler 顺序发送；不为每个文档启动 goroutine；页面排空后同一 Ticket 移到 FIFO 尾部，原额度不再准入/计账 |
| cursor / explicit session | Mongo Adapter 的 plan，固定 client/目标；killCursors 或本 Scan 的 killSessions，然后 EndSession |
| PIT / search_after | Search Adapter 的 plan，固定 index/端点；每页保留最新返回 ID，最后 DELETE 实际持有的最新 PIT |
| cleanup | Runtime 最多一个并发 cleanup/Store，单独 2s context；不走普通准入、不受 overload 拒绝，不产生 DB 拥塞样本 |
| transport slot / 最终 status | 既有 HTTP/2 delivery；HTTP 与 gRPC 两方结束后释放 slot，原生写期限持续到 END_STREAM/reset，不以 handler 结束或账本归零代替传输完成 |

fetch/emit/cleanup 中的 continuation 不可执行。没有 Scan 队列、第二个 scheduler、预取、
每页新准入或通用工作流。已准入 continuation 可在 overload/drain 时继续，直至原期限或
强制关闭。一次 open/fetch 最多给既有 AIMD 一个样本；idle cursor、发送停滞、cleanup 都
不是 DB 拥塞样本，parked continuation 也不算健康增长所需的排队需求。

Cmin=Cmax=1 可用。慢消费只占 session/page，在页间释放 permit 让普通 Read/Mutate 调度。
这不是硬资源隔离或短请求延迟 SLA；数据库连接池、进程资源、同连接 HTTP/2 flow control
仍可能产生竞争。

## 3. 分别资格验证的后端 profile

工具链保持 Go 1.27.0、gRPC 1.79.3、Mongo driver 2.9.1、protobuf 1.36.11、protoc 33.4，
没有新增依赖。测试服务沿用带所有权标记的本机隔离实例及固定镜像，未修改宿主内核或读取
凭据。MongoDB 为 8.0.32 单成员 `weir_m1` 副本集；Search 镜像 digest 见第二里程碑。

| 后端 | 本次 Scan profile |
| --- | --- |
| MongoDB 8.0.32 | 固定非 capped 普通 collection，原生 BSON find；不支持 aggregate、mongos、tailable、awaitData、客户端 cursor/session 控制 |
| Elasticsearch 8.17.0 | 固定单 primary、默认 routing、stored full `_source` 的 concrete index；PIT + `_shard_doc` 升序 + search_after |
| OpenSearch 2.19.0 | 独立真实验证；相同 index 资格限制，PIT + `_doc` 升序 + search_after；该排序的唯一性依赖固定单 shard PIT reader，不推广到多 shard |

Mongo selector 是 BSON 文档，顶层仅 `filter`、`sort`、`projection`，值均为 BSON document；
省略 selector 即默认 find。保持原生过滤、排序、投影语义，不翻译 DSL。selector 使用已有
有界 BSON codec 支持的 scalar 类型：null/bool、int32/int64/double/string、ObjectId、
Decimal128、binary、date、timestamp、regex、嵌套 document/array；不开放 JavaScript 等
未支持 selector 类型。输出是完整的原生 BSON 结果字节，保留类型、字段顺序和 native
projection 形状，不加 Weir 字段；输出不受记录 URI 的 `_id` 类型限制，不用 keyset 分页。

`find`、`getMore` 使用同一显式 driver session，`RunCommand.Raw` 保留完整 envelope。
firstBatch/nextBatch 的类型、数量、namespace、cursor ID、ok、完整 framing 以及
partialResultsReturned 都检查；cursor ID=0 才是耗尽。空 batch 配非零 ID 仍要继续。
取消、timeout、cursor 错误或任何 partial 证据均失败。

固定 driver 的 RunCommand 会从 deadline 自动增加 maxTimeMS；这可用于 find，但 Mongo
不允许普通非 tailable getMore 带这个字段。getMore 复用现有 `nativeAttemptContext`：
保留 session，移除 driver 所见 deadline，用原 parent 的 AfterFunc/Cancel 关闭 socket。
原 Runtime fetch deadline 不延长。find 的原生 maxTimeMS 还会限制整个 cursor 累积的服务端
执行时间；因此昂贵扫描可能在 RPC lifetime 之前失败，不承诺无限后端执行额度。

Search selector 为 JSON object，顶层仅可选 `query` object，省略时 match_all。请求 size、
sort、PIT、search_after、timeout、partial、total hits 等均由 Adapter 持有；拒绝用户的
pagination、aggregations、early termination、projection/source、routing、script_fields、
rescore 等其他顶层字段。JSON/BSON 结构错误和非法控制项在后端 I/O 前拒绝；原生 query/filter
本身的语义有效性仍由数据库判断，没有另造本地 DSL 验证器。

Search 输出**原生 JSON hit**，包含 `_index`、`_id`、`_score`、`_source`、`sort` 及本 profile
返回的其他 hit metadata，不改成 Read 的 `_source`。RawMessage/整数解析不经过 float64。
PIT 创建是单独调度的一步，完整检查后让出 FIFO，再取第一页面。两个产品使用各自的 PIT
创建/删除 API；关闭 partial creation/search，并检查 shard total/successful/skipped/failed。
要求 total=successful=1、failed=0；skipped 属于 successful，不再相加，也不当作失败。

每个 fetch 必须有 took、timed_out=false、合法 shards、完整 hits/max_score、预期 index/
hit identity/source/sort；拒绝 early termination、缺字段、错误尾部、截断和不前进的排序。
PIT 上完整验证的空 hits 页面才表示 native search_after 耗尽。响应更新 PIT ID 后，即使
该页其他 metadata 失败，也保留最新 ID 用于清理。允许 final pipeline 存在，仍检查 full
stored source/routing/index 资格；不会因为 Scan 修改 pipeline/index 设置或放宽记录写入。

## 4. 字节、分配和期限边界

| 资源 | 本次实现边界 |
| --- | --- |
| selector / URI / protobuf frame | 16 KiB / 4 KiB / 300 KiB；unknown protobuf fields 仍计入输入大小 |
| selector 结构 | BSON/JSON 深度 32、节点 4096；Mongo 顶层仅 3 种选项，Search 仅 query |
| fetch items | 默认 32；hint=0 或大于 32 收窄到 32，其他 1..32；它只是数量上限，不是字节保证 |
| 单个输出 Document/hit | 256 KiB；包括 Search hit metadata；另预留 512 bytes 结果开销；最大 frame 300 KiB |
| Mongo 原生 wire | 48 MiB；长度在分配前检查；不协商 compression/exhaust，监控固定 poll |
| Mongo metadata 预解码 | guard 在给驱动任何 header 前检查：外层最多 32 字段，非 cursor metadata 合计 64 KiB，每个容器至多 4096 节点；OP_REPLY 握手接受原生 AwaitCapable 位，OP_MSG 只接受本 profile 的完整单 body framing |
| Mongo page/decoder reservation | 128 MiB/Store Scan：guard 与 driver 最多两份 48 MiB wire，最多 32×256 KiB 独立文档副本及有界 metadata/framing 余量；普通操作/monitor sockets 的 guard 工作集另按有界连接数计，不假称总进程仅用 128 MiB |
| Mongo 输出校验 | 原生 BSON nesting 至多 100、65536 节点/文档；按原始字节复制，避免一个输出 frame pin 住整份大 batch |
| Search 原生 body / metadata | 1 MiB / 256 KiB；Content-Length 前检及 LimitReader(limit+1)，完整 framing/结构验证前不做无界 ReadAll |
| Search decoder / page reservation | 16 MiB/Scan，含 capped reader 增长、JSON token/RawMessage 副本、完整页面、最多 16384 response nodes/深度 32；PIT ID 至多 16 KiB |
| Store live Scan / 保留页面 / cleanup | 各最多 1；cleanup 与正常 DB permit 分开，但复用同一有界 Adapter pool |
| Scan lifetime / backend fetch | 默认、最大 5min / 默认 2s（Runtime 可配置到 10s）；Search 单 HTTP call 另 cap 2s、native search timeout=1s；调用者只能缩短 |
| 输入/发送 stall / cleanup | 默认 30s / 2s；Scan 输入沿用 unary 单请求解码后的输入期限规则，输出沿用 streaming 原生 write/flush deadline |
| PIT keep_alive | 创建和每次 fetch 请求 60s；不因客户端慢而在后台刷新或重开 |

Mongo driver 的 `ExtractErrorFromServerResponse` 在 Adapter 看见 Raw 之前就会枚举顶层错误
数组，所以仅在 Adapter 里检查 native body 大小不够。新增接收 guard 在驱动分配/展开前
检查 envelope，自己只保留一份按长度预限的消息，不解析 selector、不产生命令/重试、不
缓存下一页。读取不完整时关闭该 DB socket，防止已消费部分 wire 之后错误复用连接。
这是一项与 Scan 原生 batch 内存有关的小范围共享连接边界修正，需要继续跑原 CRUD/RMW 回归。

真实大 batch 用例返回 **9,831,505 bytes** 原生回复；随后因单文档超过 256 KiB 拒绝整页，
没有公开文档输出。这个实验明确说明 fetch count 或公开输出额度不是 driver 分配额度。
128/16 MiB 是保守的已预留工作集边界，不是 malloc 精确值、总 Go heap 或 RSS 保证。
进程 guard 仍保持既有配置；macOS 资源采样只报告 Go 指标，不宣传 RSS 上限。

## 5. 停止与清理

排队取消不执行 fetch；active 取消关闭正在进行的有界 I/O；页面待发/发送取消不会取下一页。
取消可能先完成远端清理，但页面预算要等 handler 停止借用后才释放。CloseScan 和 Adapter
Close 幂等。Runtime 在 cursor cleanup 结束后才关闭 Adapter，最终 worker join 上限为 3s，
为取消退出和 2s cleanup 留出有界余量。

Mongo 已知 cursor 使用 killCursors，检查 killed/not-found 与 alive/unknown 数组；find 回复
丢失而 cursor ID 未知时，只对仍由本 Scan 独占的 explicit session 发 killSessions。不会
kill 其他 session。命令确认与本地释放分开；killSessions 是杀除请求，不伪装成所有远端
工作在同一时刻消失。若断网导致清理无法确认，session-idle timeout 和后端周期清理是最终
边界；该隔离版本默认 logicalSessionTimeoutMinutes=30，不能把普通无 session cursor 的
10min idle timeout 误用于这里，也不能把 30min 宣传成精确删除时刻。本机只读查询确认
logicalSessionRefreshMillis=300000（5min 周期），并确认 native maxMessageSizeBytes=48000000；
wire guard 的 48 MiB 是覆盖这一原生值的保守硬上限。

Search DELETE 已知最新 PIT，验证产品各自的删除结果。丢失 PIT 创建回复时无法取得 ID，
明确返回清理未确认；60s keep_alive 与后端 reaper 是回收边界。网络故障、重启和 backend
reaper 时序可能延迟资源删除；没有“断网仍零残留”的承诺。

原生 HTTP/2 input/write/final-status 期限继续生效，包括只有小 End 且接收窗口为零的情况。
已有 Bulk watchdog 或连接级写停滞仍可能关闭整个 gRPC 连接，截断同连接其他 RPC；没有完整
mutation 结果的调用仍是 UNKNOWN。正常 Scan stream 写期限可 reset 本 stream，但不扩大成
任何网络条件下都不影响共用连接的保证。

## 6. 实际验证与复现

基线先跑离线 test/race、两种 vet 和原始 `TestAcceptance|TestUnary -race -count=3`，通过
后才实现 Scan。所有默认测试离线；真实 suites 显式 opt-in，使用唯一命名的测试数据库/
索引，清理只针对这些资源。Mongo failpoints、连接总数用例必须跨 package/进程串行。

最终验收命令与日志位于忽略目录 `.testdata/m3/`：

```sh
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
go vet ./...
go vet -tags integration ./...
WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -race -count=1 -v
WEIR_SEARCH_INTEGRATION=opensearch scripts/test-integration.sh -race -count=1 -v
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch go test -p 1 -race -tags integration \
  ./internal/store ./internal/mongostore ./internal/searchstore ./internal/server \
  -run 'Scan|WireBounds|TestAcceptance|TestUnary' -count=3 -v
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=opensearch go test -p 1 -race -tags integration \
  ./internal/searchstore ./internal/server -run Scan -count=3 -v
```

| 验收方面 | 主要测试 |
| --- | --- |
| 空、单页、多页、静态无遗漏重复 | MongoScanTraversal / SearchScanTraversal：0、1、8、35 条；hint=8，逐个 ID 验证 |
| BSON/JSON 保真与字节边界 | BSON 原始字节/原生类型，JSON Int64 最大值和 hit metadata；精确 256 KiB、超限文档、9.8MB native batch |
| selector / parser / framing | 两套 selector 负例；Mongo wire guard 在驱动 header 前拒绝超限 metadata/长度；Search missing/null/tail/截断/duplicate/body limit |
| 首页、中途失败，不发失败页 | Mongo find/getMore 真实回复后注入 partial/missing/truncate/drop；Search open/第1/第2页 timeout、early、shards、missing、tail、drop |
| 真实状态失效与 I/O 取消 | 原生 killCursors、原生 DELETE PIT；Mongo failCommand 阻塞 find/getMore 中取消；Search 实际请求到达后阻塞回复并取消；无重开/静默重试 |
| Core continuation / FIFO / 单账本 | 离线真实状态转移、C=1、反复 advance、排队取消、drain/overload continuation、一次 cleanup、预算保留到消费者归还 |
| 完成/部分结果 | gRPC ScanEnd/计数/EOF，第二页超限保留第一页面计数并以 Failure+OK 结束 |
| 慢消费者、Store 隔离 | C=1 停读后普通 Read/Mutate 可执行、第二 Scan 被限流；同时两个 Store 各持有一个 Scan，取消一个不取消另一个 |
| 传输 | 固定 65535 双窗口，600ms 停读、独立 lifetime/stall、drain；无输入/半 prefix/半 body；零输出窗口阻塞 terminal End，不伪成功 |
| 重复资源采样 | 每 profile 12轮扫描/取消，4/8/12轮 GC 后采样 heap、Sys-HeapReleased、goroutine、TCP connections、账本和 page/session reservation；三轮 race 重复 |
| 旧功能 | 完整 CRUD/Bulk、UNKNOWN、Mongo RMW、Search pipeline/OCC/真实429、partial startup、重复 Close、原始 unary HTTP/2 回归 |

`final-offline-*`、`final-{integration-,}vet.log`、`final-full-*-race.log`、
`final-qualification-*-race3.log` 是最终资格日志。早期诊断日志保留，不把失败用例删掉或
混作最终成绩。代理注入的 envelope 故障证明解析/交付契约，**不是**真实多 shard 故障实验。

上述离线 test/race、两种 vet、两个完整真实 integration suite，以及三轮关键 race 验收均
通过。完整 suites 每次同时覆盖 MongoDB 和所选 Search 产品；因此 MongoDB 8.0.32、
Elasticsearch 8.17.0、OpenSearch 2.19.0 分别完成真实验证。固定工具重新生成 protobuf
绑定与提交内容逐字节一致。

三轮关键 race 日志中，12轮取消实验每4轮采样一次；Mongo 在两个产品组合中各运行3次，
每个 Search 产品运行3次。下表数值为 GC 后样本的最小/最大值，单位 bytes；不代表峰值。

| profile | 样本数 | HeapAlloc | Sys−HeapReleased | goroutines（fixture 基线） | 后端 TCP connections |
| --- | ---: | ---: | ---: | ---: | ---: |
| MongoDB | 18 | 1,860,000–5,424,160 | 15,583,496–22,522,120 | 23（19） | 1 |
| Elasticsearch | 9 | 1,615,256–3,870,848 | 16,517,384–20,547,848 | 16（12） | 1 |
| OpenSearch | 9 | 1,535,824–4,136,064 | 14,977,288–19,417,352 | 16（12） | 1 |

所有这些采样点的 Pending、PendingBytes、Active、Retained、ResultBytes 和
ScanSessions/ScanPageBytes/ScanPages/ScanCleanups 均为零。fixture 活跃时的4个额外
goroutine 数保持稳定；完整回归另验证关闭后的 delivery 释放。没有采集 RSS，未运行长稳。

## 7. 最小调用和未覆盖风险

沿用 README 的 Mongo-only 或双 Store 启动方式。`cmd/weir-example` 在 Put/Bulk 之后增加
一个仅选择 example 文档的 Scan，逐条处理 opaque Document，检查完整 End、Failure、计数
和最终 EOF。Search 写后 search 可见性由 native refresh 决定，示例不通过修改 index settings
或隐藏 refresh 来伪造立即可见。

实际运行构建后的 server/example，分别连接 Mongo+Elasticsearch 和 Mongo+OpenSearch。
两种组合均完成 Put、Bulk、Scan 的 End/计数/最终 OK 校验：Mongo 得到1条；Search 首次
在 native refresh 前得到0条，对测试索引显式 refresh 后再运行得到1条原生 hit。日志为
`final-cli-smoke.log`；唯一命名的数据库/索引及示例服务已清理。验收结束后停止的后端仅限
本任务从停止状态启动的有所有权标记的隔离实例，保留原数据目录和容器。

官方固定版本依据及本机驱动源码：

- [MongoDB getMore](https://www.mongodb.com/docs/manual/reference/command/getmore/)：session、partial evidence、native batch 与 maxTimeMS；实际资格固定在 8.0.32。
- [MongoDB killSessions](https://www.mongodb.com/docs/manual/reference/command/killsessions/)：只清理本 Scan 所有的 session。
- [Elasticsearch 8.17 PIT response](https://github.com/elastic/elasticsearch/blob/v8.17.0/server/src/main/java/org/elasticsearch/action/search/OpenPointInTimeResponse.java)：PIT/shard envelope。
- [OpenSearch 2.19 PIT response](https://github.com/opensearch-project/OpenSearch/blob/2.19.0/server/src/main/java/org/opensearch/action/search/CreatePitResponse.java) 与 [固定 _doc 排序实现](https://github.com/opensearch-project/OpenSearch/blob/2.19.0/server/src/main/java/org/opensearch/search/sort/FieldSortBuilder.java)。
- mongo-driver v2.9.1 `mongo/database.go`、`x/mongo/driver/{operation,errors}.go`、`topology/connection.go`；没有修改 module cache。

剩余限制：没有 multi-shard、多节点故障切换、TLS/认证、生产代理、Linux runtime/RSS、长期
稳定性或性能资格；没有任意原生命令/selector 透传、snapshot/export 或跨 RPC 恢复承诺。
Mongo 接收 guard 仅支持已验证的未压缩、非 exhaust 单 body 回复；其他 framing 拒绝，升级
driver/Go/backend 或扩大 profile 必须重跑全部真实回归。Search single-shard 排序不可被
直接推广到多个 shard。短时 Go 内存/连接/goroutine 样本不能证明数小时稳态或 RSS 峰值。
