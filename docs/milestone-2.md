# Milestone 2：双 Store 基础记录操作与验证

验证日期：2026-09-26。起点为 `c5816a5`，开工时工作树干净。
验收树包含后续已有的文档职责分离提交 `7f9a71c`，保留其变更。
这是单进程、单机测试环境中的第二个可验证里程碑，**不是完整 V1 或生产就绪发布**。
本次版本、实现状态和测试证据仅记录在这里；未修改中英文架构文档。

## 1. 实现范围

一个进程、一个 loopback gRPC listener，同时承载 `mongo` 和 `search`。
两者复用原有 Read / Mutate / 双向 Bulk 协议、唯一的 StoreRuntime 调度实现，
各自持有 Adapter、连接池、pending 账本、执行许可、AIMD 窗口和物理批次。

- MongoDB 原有 BSON CRUD、内部事务 conformance 保留，不改变原生执行算法。
- Search 提供 JSON Read、Put、Create、Replace、Delete；支持兼容写入微批。
- 配置为静态精确名称映射；unary 只路由一次，Bulk 根据 Open 固定 Runtime。
  后续跨 Store 操作得到 INVALID_ARGUMENT + NOT_STARTED，不重新路由或扇出。
- AtomicTransform 继续 UNSUPPORTED；未增加 Native、Scan、Query、Count、通用转换运行时、
  跨记录事务、转发、动态配置、队列、SDK 或部署功能。
- 不推送、创建 PR、合并、发布或访问生产服务。

## 2. 固定版本与资格矩阵

| 组件 | 实测固定版本 / 平台 |
| --- | --- |
| Weir / 测试执行器 | Go 1.27.0，darwin/arm64 |
| MongoDB | Community 8.0.32，原有本地单成员副本集 `weir_m1`，127.0.0.1:27028 |
| MongoDB driver | `go.mongodb.org/mongo-driver/v2 v2.9.1`，原配置不变 |
| Elasticsearch | **8.17.0** default distribution，linux/arm64 Docker，127.0.0.1:19200 |
| OpenSearch | **2.19.0** distribution=opensearch，linux/arm64 Docker，127.0.0.1:19201 |
| Search HTTP client | Go 1.27.0 `net/http`，HTTP/1.1；没有另加 SDK/框架 |
| gRPC / Protobuf | v1.79.3 / v1.36.11；HTTP/2 ServeHTTP 期限修复保留 |
| x/net | v0.55.0，仅现有 wire-level 回归使用 |
| Docker Engine | 29.7.2；仅测试后端，不代表 Weir 的 Linux 运行资格 |

`go.mod` / `go.sum` 未变更；没有升级依赖或宣传整个 ES/OpenSearch 版本范围。
选择的是可复现、逐一验证的固定版本，不是最新版本或生产安全版本推荐。
其他产品、版本、平台、集群拓扑均未纳入支持声明。

后端根响应已核对：

- Elasticsearch build hash：`2b6a7fed44faa321997703718f07ee0420804b41`。
- OpenSearch build hash：`fd9a9d90df25bea1af2c6a85039692e815b894f5`。

固定镜像 manifest digest（启动脚本直接使用 digest）：

```text
Elasticsearch: sha256:2f602552550869fb29b6fd5848c5118d3ef3a2e1d5d45802e3ab9088cb2de8e2
OpenSearch:    sha256:1f8b88245a6af61e7aa500afe0e87d43401e4b33140bb47230a919428ce3f7cb
```

### Search 的有限直接记录 profile

| 能力 / 条件 | Elasticsearch 8.17.0 | OpenSearch 2.19.0 |
| --- | --- | --- |
| 精确 ID GET / Create / 条件式 Index / Delete | 实测 | 独立实测 |
| `_seq_no` + `_primary_term` | Adapter 内部使用，客户端不见通用 revision | 同左，独立实测 |
| 默认 pipeline | 每个源写入项及 bulk 请求显式 `pipeline=_none` | 同左，独立实测 |
| final pipeline | Put/Create/Replace 在写前拒绝；Read/Delete 不因它而禁用 | 同左，独立实测 |
| 线程池满的原生 429 类型 | `es_rejected_execution_exception` | `rejected_execution_exception` |
| 记录来源 | 存储的完整 `_source`，禁止 disabled / includes / excludes / synthetic source | 同左 |
| index | 配置中的一个既存 concrete、standard、单 primary shard index | 同左 |
| routing | 默认路由；拒绝 required routing、分区路由、请求 routing override | 同左 |
| 自动创建 | 要求 effective `action.auto_create_index=false`；不代替操作员修改设置 | 同左 |

实际测试主索引使用一个 primary、零 replicas。多 primary 被明确拒绝；不同 routing 在
多 shard 下可能让同 ID 成为不同记录，不把这种身份模型偷偷映射成简单 ID。
Native 写入者也须遵守同一记录/配置资格边界；不支持依赖自定义 routing 的记录模型。
未验证多节点、复制故障切换或配置在线变更。namespace 删除重建、alias retarget、
ingest/source/routing/自动建索引设置并发变化不属于记录写竞争，须重新资格验证。

显式拒绝 alias、wildcard、data-stream 目标和未知版本；不创建 index、mapping、pipeline。
启动检查版本、自动建索引策略和具体 index；每次执行在原有 Store 许可内重新检查 index
metadata。无法确认资格就失败，不乐观写入。没有无界 capability cache。
_source 不适用时 Read/源写入拒绝，但仍可执行有资格的 Delete。

## 3. 语义、证据与网络边界

Read 使用 `GET /<index>/_doc/<exact-ID>?realtime=true`，不是 search 查询。
只有相符的 `_index` / `_id`、明确 `found=false` 和原生 404 才表示 missing。
index 不存在、错误、不完整响应不伪装成记录缺失。返回 `application/json` 的 RawMessage
`_source`，不把数值通过 float64 解码重编码；9223372036854775807 已实测保真。
源写入只做 NDJSON 必需的 JSON compact，不注入 ID、版本或 Weir 字段。

- Put：bulk `index`，插入或完整替换。
- Create：bulk `create`，原生存在冲突为 NOT_APPLIED + PRECONDITION_FAILED。
- Replace：一次 GET 获取原生 token，再一次 conditional `index`，不做重试循环。
  原先不存在为 NOT_APPLIED + PRECONDITION_FAILED；并发 native 更新、删除或删除重建
  为 NOT_APPLIED + CONFLICT，不通过 upsert 重新创建或覆盖竞品写入。
- Delete：bulk `delete`，不预读；原生 `deleted` 或完整 `not_found` 确认均 APPLIED。
  其他 404 或缺少逐项证据不是成功删除。

APPLIED 表示原生写入确认，不等于 search refresh；请求固定 `refresh=false`、
`wait_for_active_shards=1`，不提供客户端 refresh/options/profile override。
已知 primary 成功但 replica 确认失败的完整回复保留 APPLIED，并附有界失败；此边界有离线
解析测试，多节点复制故障不在本次实测声明中。

物理 bulk 校验全体条数、位置、动作、index、ID、状态、结果和原生确认字段。
混合失败按项返回；HTTP 200 不是全批成功。缺项、截断、身份/映射不符时不信任不完整证据，
返回 UNKNOWN，不回退单项重放。业务冲突不作为拥塞；物理执行最多产生一个反馈样本。
OpenSearch 的真实拥塞测试首次暴露不同的错误名称，随后用有限 profile 修正并重新验证，
而不是假设所有错误与 Elasticsearch 一样。未知错误保守 UNKNOWN。

HTTP 行为：

- Adapter 独占 Transport；单一明确 loopback URL、固定端点，无 proxy 环境继承或发现。
- 所有 mutation（含 Delete）使用非空 POST bulk；`GetBody=nil`，没有幂等头、retry/hedging。
  审查 Go 1.27 `net/http/transport.go:shouldRetryRequest` 与 `request.go:isReplayable`：
  这种 POST 不满足复用连接的自动重放条件。真实成功回复丢失测试确认只有一次后端写请求。
- 禁止 HTTP 重定向；307 返回 UNKNOWN，未发送到 Location。HTTP/2 后端 transport 未启用，
  不依赖其额外重试行为。GET 保留 Go 标准库有限的安全连接重建行为，不是 mutation replay。
- 连接池上限等于 Store Cmax；checkout、dial、headers、body read、取消都在调用 context 内。
  每次 HTTP exchange 另有 2s 上限，不能延长共享执行或 caller 的原 deadline。
- 原生错误 reason、文档及凭据不进入公共 Failure 或生产日志，只返回固定有界消息。
- 正常 Close 取消 Adapter 生命周期，关闭自己的 idle pool；Runtime 不接触原生 client。

## 4. 共享 Core 的重构

新增 `internal/execution` 只包含 Plan、Feedback 和真实双实现 Adapter 接口：
`Prepare / Execute / Close`。公共工作项保存 op 引用、record key、Core 只比较不解释的 token、
输入/结果字节额度及 opaque backend plan。MongoDB 的 BSON、ID、action、事务状态和 Search
的 JSON、OCC、原生错误及 HTTP client 都留在 Adapter 内。

`internal/store` 的生产 imports 已核对，不再包含 mongostore/searchstore 或 BSON/JSON。
调度器取消转移、单 ledger、同流序列、独立 Read 并发、微批、共享 deadline、结果预留和
AIMD 算法没有另建 Search 路径。batch 关闭只改变 BatchOperations，不绕过这些路径。

`internal/app.OpenStores` 顺序构造所需 Adapter 和 Runtime，任一启动失败就释放此前已构造
的 Runtime。`store.New` 明确接管 Adapter，包括 limits 校验失败时的关闭。
原生 client/pool 仅由 Adapter 关闭；并发重复 Runtime.Close 的离线测试证实只调用一次。
没有 BackendProvider、StorageEngine、插件注册表或 Store 之外的新队列。

Server 复制并验证静态路由表，拒绝同 Runtime 的重复别名。两 Store 共用一个进程内存
Guard 采样，分别停止新准入；没有额外进程等待队列。全局 session/连接预算仍共享，
不是硬租户隔离或 CPU/网络保留。共享资源尚未耗尽时，慢/失败 Store 不占另一 Store 的
DB 许可和队列。已准入工作不再经过过载准入检查，继续交付与清理。

## 5. 资源边界

沿用已有默认：每 Store pending 256 项 / 8 MiB、结果 128 项 / 16 MiB、Cmax=4、
batch 16 项 / 1 MiB / 1ms、每 Bulk outstanding=8、记录 256 KiB、frame 300 KiB。
不同 Store 的额度相加，不把两份预算描述成一个全局硬上限。

Search 额外的有限工作集：

| 项目 | 限制 |
| --- | --- |
| index metadata / 根版本 / 集群设置回复 | 每次最多 256 KiB |
| GET 原生回复 | 256 KiB 文档 + 32 KiB envelope 预算 |
| bulk 原生回复 | 最多 1 MiB |
| HTTP response headers | 32 KiB |
| native bulk | 硬上限 128 项 / 8 MiB；默认由 Core 收紧为 16 项 / 1 MiB |
| JSON | 深度 32；输入 4096 节点，原生回复 16384 节点；拒绝重复字段、非法 UTF-8/孤立 surrogate |
| ID / index | 精确非空字符串 ID 最多 512 bytes；index 使用明示有限命名规则 |

body 先经过 `LimitReader(cap+1)`，Content-Length 过大提前失败，再 ReadAll；不是先无界
ReadAll 再检查。有界字节上的 token walk 限深度/节点/重复字段，然后才解码原生 envelope；
不会先创建无界通用 JSON 树。数值以 Number/token 或 RawMessage 保留。
Plan 保守计账包含原生 framing 预算，组批前总量已受限；无每条排队工作的独立 goroutine。

保留 unary HTTP/2 stream 级 deadline、input credit、结果 ticket handoff 和 slot 双方完成
规则。结果额度归零只表示应用结果已释放，不代表客户端已收齐 DATA/trailers；最终传输
deadline 和非 OK 终态另测。Bulk watchdog 仍可关闭整连接，影响同连接其他 RPC；缺少完整
mutation 结果仍是 UNKNOWN，不能从 gRPC status 推导 NOT_STARTED/NOT_APPLIED。

macOS Guard 仍是 Go Sys-HeapReleased 降级信号，不是 RSS。短时慢消费测试验证额度和清理，
不宣称精确 RSS、长时间稳定性、峰值内存或生产 sizing。

## 6. 回归命令与实际证据

开工前先运行、通过原 unary 专项和完整现有 MongoDB integration/race，以及离线
`go test ./...`、`go test -race ./...`、`go vet ./...`，之后才扩展后端。
固定 65535-byte stream/connection 接收窗口小于 200 KiB 响应；检查完整消息后的最终
status/EOF，不把客户端预取缓冲或 Runtime.Retained=0 当作交付证据。

重构后同样执行完整 suites；Search tests 显式选择 profile，未选择时不算已验证。

最终结果：本节所有列入资格声明的后端都完成真实验证，**有限范围的第二里程碑验收通过**。

| 最终验收 | 实际结果 |
| --- | --- |
| 默认 `go test ./... -count=1` | PASS；MongoDB、ES、OpenSearch 测试服务全部停止后执行 |
| 默认 `go test -race ./... -count=1` | PASS；同样在真实后端停止后执行 |
| `go vet ./...` / `go vet -tags integration ./...` | PASS |
| MongoDB + Elasticsearch 8.17.0 完整 integration | PASS，`-race -count=3`，包含原有 MongoDB 与 unary 回归 |
| MongoDB + OpenSearch 2.19.0 完整 integration | PASS，`-race -count=3`，独立执行相同资格范围 |
| fixture 清理顺序调整后的 Search / 双 Store 回归 | 两产品分别 PASS，`-race -count=3` |
| 真实 CLI 双 Store smoke | 两产品分别 PASS；同一进程中 Mongo 和 Search 示例都获得 APPLIED、三项 Read、End/EOF，SIGTERM 正常退出 |

测试服务已停止，仅保留本任务的容器、数据目录和日志；停止前确认当前测试节点无残留
`weir_m2_*` 索引、`weir_test_*` 或 CLI smoke 数据库。没有停止其他服务或删除已有数据。

```sh
# 缓存依赖下的默认离线测试；不访问真实后端。
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
GOPROXY=off GOSUMDB=off go vet ./...

scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch \
  scripts/test-integration.sh -race -count=3 -v
scripts/search-local.sh start opensearch
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=opensearch \
  scripts/test-integration.sh -race -count=3 -v
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
```

完整 runner 使用 `-p 1`，避免不同包的 MongoDB 全局 failpoint 竞争；不并行运行两个
product 的完整 suite。Search fixture 在发送写入前校验固定 loopback endpoint 的产品版本
和专用 cluster name；每次只创建/删除自己的 `weir_m2_<pid>_<counter>` 索引及关联 pipeline。
脚本限定自己的容器名、owner label 和固定镜像 digest；不接管其他容器，不改宿主 sysctl。
测试节点显式使用很小的 write 线程池/队列以触发真实拒绝，不作为生产参数建议。

| 场景 | 测试 / 观察 |
| --- | --- |
| unary 输入/发送完整期限 | 现有 `TestAcceptanceUnaryTransportLifetime`、所有 `TestUnary*` 重跑；无/长/短客户端 deadline、固定双窗口、最终非 OK、input stall/进度、零窗口真实写入回执丢失、同连接影响及清理 |
| 普通 CRUD 和 ID | `TestSearchCRUD`：missing、存在/不存在 Replace、Create 冲突、缺失 Delete、删除重建、UTF-8/斜线/百分号/查询符号精确 ID、最大 Int64 |
| bulk 逐项证据 | `TestSearchMixedBulk`：成功写入、Create 冲突、mapping 拒绝、missing Delete 同批；冲突不是拥塞；原生读取确认实际效果 |
| OCC 与 native 竞争 | `TestSearchReplaceNativeCompetition`：GET 观察后、条件写前执行真实 native 更新、删除、删除重建，三者均 CONFLICT，不覆盖 native 状态 |
| 丢失已确认回复 | `TestSearchCommittedReplyLossAndIncompleteBulk`：代理先取得真实 backend 写入成功，再丢弃/截断/删项/交换项；结果 UNKNOWN，只有一次 bulk，原生 `_version=1` |
| 隐式重定向 | 同 suite 的 307 场景：未跟随 Location，原生数据仍缺失 |
| ingest 与资格 | `TestSearchIngestAndQualification`：native default pipeline 确实重写目标，Weir 逐项绕过；final pipeline 写前拒绝而 Read/Delete 可用；source disabled/pruned、required routing、多 primary、alias、错误产品拒绝 |
| 真正拥塞 | `TestSearchRealBackendCongestion`：并发原生写入使小队列发生真实 429；区分两产品错误名，逐项 NOT_APPLIED，原生 GET 抽查拒绝项不存在；与正常业务冲突测试分开 |
| 共享批次 deadline/取消/关闭 | `TestSearchSharedDeadlineCancellationAndDrain`：短 caller 过期但同一批长 caller 收到 APPLIED；真实双写已落库且只有一次 bulk；cancel-all、drain、重复 Close 与额度回收 |
| 单节点双 Store / Bulk 固定目标 | `TestDualStoreRoutesAndSearchBulkOrder`：同 listener 写 Mongo/读写 Search，24 项同 key 交替严格有序，跨 Store 第 25 项 NOT_STARTED，End/计数/EOF 完整 |
| 一个 Store 变慢、统一过载 | `TestDualStoreSlowBackendAndOverloadProgress`：Search 等待真实回复时 Mongo 写入仍成功；窗口独立；过载拒绝新调用，不阻塞已准入结果 |
| 独立 Read / 失败冷却 | `TestDualStoreIndependentSearchReadsAndCooldown`：同 key 两个 Search GET 同时到达代理；后端回复超时后 Search 窗口从 2 缩至 1，Mongo 在等待期间及冷却期间继续写入 |
| Search 大响应/慢消费者 | `TestSearchUnaryAndSlowBulkTransport`：200 KiB 源、65535 双窗口 unary 暂停 600ms 后非 OK；持续生产且不读 Bulk 时 retained 峰值为 8，发送被背压并终止，结果额度与 active 清零 |
| 部分启动与所有权 | `TestPartialStartupReleasesConstructedMongo`：五次第二 Store 启动失败，真实 Mongo 连接数回到基线；`TestRuntimeOwnsAdapterExactlyOnce` 检查构造失败和并发多次 Close |
| 本地边界/格式 | JSON 深度/节点/Unicode/重复字段/大整数，HTTP body/header/encoding 边界、错误状态与 product profile、缺少原生确认不能仅凭 HTTP 200/404 成功 |

本机详细日志保存在忽略目录 `.testdata/m2/`。最终资格日志使用
`elasticsearch-qualification-race.log`、`opensearch-qualification-race.log`；
早期 `os-complete-race.log` 记录了发现错误类型差异时的失败，未删除或冒充通过。
`preflight-unary.log`、`preflight-integration-race.log` 是扩展前的入口验收证据。
`final-offline-{test,race,vet}.log`、`final-integration-vet.log` 记录最终离线验收；
`{elasticsearch,opensearch}-cleanup-regression.log` 和对应的 `-cli.log` 记录补充回归与 CLI smoke。
这些日志不提交；可使用上述固定环境和命令重新生成。

## 7. 双 Store 启动和最小调用

```sh
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch
# 仅由操作者为隔离测试预建；重复执行时如已存在，可直接复用。
curl --fail -X PUT http://127.0.0.1:19200/weir_m2_example \
  -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}'
go run ./cmd/weir \
  -search-url http://127.0.0.1:19200 \
  -search-profile elasticsearch-8.17.0 -search-index weir_m2_example
```

另一个终端对同一 listener 发起调用：

```sh
go run ./cmd/weir-example               # BSON MongoDB Store
go run ./cmd/weir-example -store search # JSON Search Store
```

两个调用都使用相同的 Read/Mutate/Bulk 客户端绑定。示例先 Put，再并行发送/接收三次 Read
Bulk，校验结果、End、计数和最终 EOF。Search URI 为
`weir://search/weir_m2_example/s:example`，URI 决定 ID；示例 JSON 只有 `n`。

使用 OpenSearch 时，把预建索引请求和 `-search-url` 改为 `http://127.0.0.1:19201`，
并显式设置 `-search-profile opensearch-2.19.0`。切换的是本地静态配置，不会运行时发现或
故障后切换产品。默认不传 `-search-url` 时保留 MongoDB-only 启动方式。

Ctrl-C/SIGTERM 按原有 drain 路径关闭服务，再停止测试后端：

```sh
scripts/search-local.sh stop elasticsearch
scripts/search-local.sh stop opensearch
scripts/mongo-local.sh stop
```

容器和本地数据库数据/日志保留以便复核，不删除其他服务或修改宿主配置。

## 8. 官方依据与剩余风险

本次实际阅读的固定版本官方资料 / 源码：

- [Elasticsearch 8.17 Index API](https://github.com/elastic/elasticsearch/blob/v8.17.0/docs/reference/docs/index_.asciidoc)：Create、完整 index、OCC、确认契约。
- [Elasticsearch 8.17 GET API](https://github.com/elastic/elasticsearch/blob/v8.17.0/docs/reference/docs/get.asciidoc)：实时精确 GET 与原生 envelope。
- [Elasticsearch 8.17 common parameters](https://github.com/elastic/elasticsearch/blob/v8.17.0/docs/reference/rest-api/common-parms.asciidoc)：`pipeline=_none` 不跳过 final pipeline。
- [OpenSearch 2.19 IngestService](https://github.com/opensearch-project/OpenSearch/blob/2.19.0/server/src/main/java/org/opensearch/ingest/IngestService.java)：默认与 final pipeline 分别解析、执行。
- [OpenSearch 2.19 TransportBulkAction](https://github.com/opensearch-project/OpenSearch/blob/2.19.0/server/src/main/java/org/opensearch/action/bulk/TransportBulkAction.java)：bulk、pipeline 与自动建索引分支。
- [OpenSearch 2.19 exception naming](https://github.com/opensearch-project/OpenSearch/blob/2.19.0/libs/core/src/main/java/org/opensearch/OpenSearchException.java)：`getExceptionName` 去掉 OpenSearch 前缀，不能硬套 ES 错误名。
- 本机固定 Go 1.27 GOROOT 的 `net/http/transport.go`、`request.go`；gRPC v1.79.3 的 handler transport 与既有 unary 专项报告。

文档依据不是替代实际测试，特别是 OpenSearch pipeline / OCC / 429 已分别运行真实验证。
本次没有新增依赖或随版本名称泛化能力。

剩余风险 / 不声明覆盖的项目：

- 未验证其他版本、multi-primary、多节点复制/故障切换、TLS/认证、生产代理或 Linux Weir。
- 不提供通用转换、任意查询、扫描、写后 search 可见性保证、跨 Store 原子性或 exactly-once。
- GET/metadata/bulk 为多次顺序交换；额外 metadata 开销尚未做生产性能优化或基准。
- 只测本地短时限额、持续生产与取消竞争；不把它们描述成数小时稳定性或 RSS 资格认证。
- Go/gRPC ServeHTTP 仍是固定版本的实验性接入。升级 Go、gRPC 或后端必须重跑原始 unary
  回归和真实故障 suites；不能以一次 handler 完成或 Retained=0 代替 transport 完成。
- namespace 或源/路由/ingest 资格配置并发改变仍需重新验证，不是本次记录 OCC 的承诺。
