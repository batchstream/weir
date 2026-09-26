# 第七阶段：公开原生表达式 AtomicTransform

> 历史资格记录：本文的 Weir peer mTLS/身份/权限配置及相关测试事实保留。
> 当前可信内网明文入口、配置和资格见 [M8](milestone-8.md)；Weir 认证体系已由 M8 移除。

验证日期：2026-09-27。起点：`137c16adc94eed7543db82d7b1612c2c806e61c1`，干净 `main`。
本轮按授权直接在 main 开发并本地提交；不创建分支/worktree/PR，不 push、发布或访问生产。
这是有限原生表达式资格，**不是生产就绪，也不是通用 ProgramTransform**。
最终提交 SHA 由本阶段提交记录及交付报告确定，不在自身提交内容中自引用。

## 公开能力、边界与固定范围

| profile | 后端与范围 | 执行、结果 |
| --- | --- | --- |
| `application/vnd.weir.mongodb-update.v1+bson` | MongoDB Community **8.0.32**，driver **2.9.1**；原有 fixed direct 单成员 replica set、既存普通 collection、simple collation、无 Auth profile | 有序 BSON，至少一个顶层 `$set/$unset/$inc`；固定 URI `_id`，一条 acknowledged update，`multi=false,upsert=false,w=majority`；无预读 |
| `application/vnd.weir.search-update.v1+json` | Elasticsearch **8.17.0** default distribution、OpenSearch **2.19.0** 分别真实验证；原有 concrete standard 单 primary、零 replica 测试 index、默认 routing、完整 stored `_source` | 恰一个 `doc` object；一条 POST Update，原生递归对象 merge、数组替换、显式 null；固定 `retry_on_conflict=0,doc_as_upsert=false,refresh=false,wait_for_active_shards=1,timeout=1s`；无客户端 GET/merge/index |
| ProgramTransform | 所有 Store | **UNSUPPORTED + NOT_STARTED**；GopherLua 不进入服务执行图，内部事务计数器不是公开 runtime |

Go **1.27.0** darwin/arm64、gRPC **1.79.3**、protobuf **1.36.11**、x/net **0.55.0**。
依赖与后端版本未升级；ES/OS 镜像 digest 仍是 [M2](milestone-2.md) 及 `scripts/search-local.sh`
中的固定值。Mongo/mongosh 仍为 8.0.32/2.6.0。Weir 本机运行不代表 Linux/Windows 资格。
本轮未增加 RPC、protobuf 字段、portable patch DSL、脚本、pipeline、upsert、表达式编译器，
也未扩大 backend/version/topology、普通记录 API、Native 或 Scan 的既有能力。

实现：`internal/protocol/protocol.go` 只检查 oneof、media envelope 与字节上限；
`internal/mongostore/expression.go`、`internal/searchstore/expression.go` 拥有 profile、路径/值、
后端资格及结果证据。接入现有 Adapter Prepare/Execute 的 mutation plan，不包装成 Native RPC。
表达式 singleton 派发，复用唯一 pending/result 账本、同 key Bulk 顺序、取消/deadline、
AIMD、结果交付和 metrics；一物理执行最多一个反馈，无表达式队列或 retry loop。
relay 保留 opaque bytes，mutate 授权自然覆盖表达式，未增加权限种类。

## 输入和数据语义

- expression 新增小上限 **16 KiB**；不提高原 256 KiB 文档、300 KiB frame、4 KiB URI 上限。
  byte limit 先于 adapter decode；BSON codec/JSON token walk 增量限制深度 **32**、节点 **4096**。
  有界 Prepare 临时解析状态在返回时丢弃，plan 只持有原始输入；没有编译或后台状态。
- Mongo 最多 **128** 条更新路径；每条最多 **1024 bytes / 32 components**。
  拒绝 `_id` 及后代、空 component、包含 `$`/NUL、以数字开头的 component（不支持数组下标
  或 numeric object-key 歧义）、相同/祖先后代冲突、重复 operator 和任意层重复 BSON 字段。
  冲突跨所有 operator 检查，不依赖 BSON 输入次序。
- `$set` 值是普通数据；不把其内 `$literal` 等用户字段递归当表达式。沿用 codec 的
  null/bool/string/int32/int64/double、ObjectID/Decimal128/binary/date/timestamp/regex、object/array；
  不支持的 BSON code/其他类型与非法编码拒绝。`$unset` 的值遵循 Mongo 忽略 operand 的语义。
- `$inc` 接受 int32/int64、有限 double、有限 Decimal128；拒绝 null/string/bool、NaN/Infinity
  operand。**这是 native 数值规则，不是 ProgramTransform checked-width 算术**。
  实测 int32 Max+1 提升 Int64；Int64 Max+1 明确拒绝且整项不生效；Decimal128 1.25+1.25=2.50；
  大整数 9007199254740993+1 精确。缺字段会按 native `$inc` 新建字段；已有 null 拒绝。
  不预读检查原文档中数值类型，原生已有类型/混合算术与 double 精度仍由后端定义。
- Search 拒绝所有 caller body/query options（script/upsert/retry/routing/pipeline/target 等），
  doc 内字段名最长1024 bytes，拒绝空名、点号/NUL及 `_id/_index/_routing/_version/_seq_no/_primary_term`
  元数据名；所有层 duplicate 拒绝。JSON array/object/string 编码有界；非法 UTF-8/unpaired surrogate
  拒绝。数值使用原始 bytes/RawMessage，不经过 Go float64，max Int64 与 2^53 以上整数实测保真。
  native Update 可重新序列化 `_source`；不承诺 JSON 字节/空白/数值拼写不变。
- 不注入公共 revision、ID、时间戳或隐藏 metadata，不创建 collection/index/锁/旁路表。
  未涉及字段与源类型按 backend native update 保留；Search 数值/merge 不是跨后端统一语义。

## 确认与失败语义

| 实际证据 | outcome |
| --- | --- |
| wire/profile/编码/路径/大小拒绝，尚未后端尝试 | NOT_STARTED + INVALID_ARGUMENT/UNSUPPORTED |
| Search permit 内 metadata 确认不具备资格 | NOT_APPLIED + UNSUPPORTED；不能验证 transport 时为有界 failure，无 update |
| Mongo `n=0,nModified=0`；Search `document_missing_exception`/404 | NOT_APPLIED + PRECONDITION_FAILED，不创建记录 |
| Mongo `n=1,nModified=0/1`；Search 完整 updated/noop 回复 | APPLIED；acknowledged no-op 包括空 operand/空 doc，不是 Program Keep，也不承诺字节改变 |
| 明确原生 conflict | NOT_APPLIED + CONFLICT；无重算/重试，不算拥塞 |
| 明确类型/mapping/文档拒绝 | NOT_APPLIED + PRECONDITION_FAILED；无重试，不算拥塞 |
| 缺 matched、错误 ID、缺 seq/term/shards、重复/截断 envelope、未知证据、写关注歧义 | UNKNOWN + bounded failure，绝不从 HTTP 200 或 driver 零值造成功 |
| 实际 primary 完成但完整回复报告 replica failure | 保留 APPLIED + UNAVAILABLE（Search 合成解析向量）；不伪造复制拓扑验证 |
| 已确认后外层回复丢失 | 最终执行节点已有 APPLIED 保留；调用者 non-OK/未知，不重放 |
| 派发后取消/超时/断连，无正向确认 | UNKNOWN；未派发取消才 NOT_STARTED |

Mongo 用 RunCommand.Raw 保留确切 `n/nModified/writeErrors/writeConcernError`；只认有限明确错误码，
不把 driver UpdateResult 中缺字段的默认零当 missing。副本集的 `electionId/opTime/clusterTime`
是有界确认元数据，不写入文档。多数写关注错误优先保留 UNKNOWN。
Search updated 要求匹配 ID/index、有效 version/seq/term 和成功 primary shard；noop 必须是
固定版本实测的零 shard work 完整确认。所有解析只处理预先限定的 response bytes/nodes。

Mongo 普通 retryWrites/retryReads/adaptive retry/overload retargeting 关闭，RunCommand 不设置普通
retry policy。**driver reauthentication 可能重放**，所以与既有 Native 一样，表达式在 Prepare
拒绝 Auth 配置；未以“retryWrites=false”声称认证路径已安全。该连接能力仍留后续资格。
Search Update 使用带非空 body 的 POST、`GetBody=nil`、无 idempotency header、禁止 redirect，
单固定目标，无 SDK replay/hedging/failover；普通 client 的连接复用不授权 mutation 重发。
后端 retry_on_conflict 固定0，caller 无法覆盖。read-only 资格检查不是 mutation 重放。

## Search pipeline/source 独立资格

| 当前 index 条件 | 表达式 ES 8.17.0 | 表达式 OS 2.19.0 | 其他既有 API |
| --- | --- | --- | --- |
| stored full source、default/final 均 absent 或 `_none` | 支持、真实验证 | 支持、独立真实验证 | 维持原 profile |
| 存在 default pipeline | 写前拒绝 | 写前拒绝 | 普通 Put/Create/Replace 仍用其既有 `_none` bypass；Read/Delete/Scan 不因此禁用 |
| 存在 final pipeline | 写前拒绝 | 写前拒绝 | 普通 source writes 既有拒绝；Read/Delete/Scan 不因此禁用 |
| disabled/pruned/synthetic source、required routing、非 concrete 标准单 primary index | 按既有 index/source 边界拒绝 | 独立按其边界拒绝 | 不把 expression 限制升级成全 Store 开关 |

写前 metadata 检查在现有执行 permit 内，沿用 `inspect`，无新增 cache；不改 index/pipeline。
namespace/index generation、routing/source/ingest 设置并发变更仍不支持，需重新资格验证/重启。
每个 default/final case 都实际发送原生 Update：两产品的 `pipeline=_none` 参数均返回400；
无该参数的原生更新实际触发 marker pipeline。每个 case 改变 doc 并把 marker 设 false，避免
沿用前一 case 已有字段或误测 noop。Weir 表达式在这些配置下未修改记录。

固定源码：[ES RestUpdateAction](https://github.com/elastic/elasticsearch/blob/v8.17.0/server/src/main/java/org/elasticsearch/rest/action/document/RestUpdateAction.java)、
[OS RestUpdateAction](https://github.com/opensearch-project/OpenSearch/blob/2.19/server/src/main/java/org/opensearch/rest/action/document/RestUpdateAction.java)
均没有消费 pipeline 参数；[ES UpdateHelper](https://github.com/elastic/elasticsearch/blob/v8.17.0/server/src/main/java/org/elasticsearch/action/update/UpdateHelper.java)、
[OS UpdateHelper](https://github.com/opensearch-project/OpenSearch/blob/2.19/server/src/main/java/org/opensearch/action/update/UpdateHelper.java)
在后端读取/merge并条件 index。源码用于解释原生机制，资格结论来自两产品实际测试，不推断未来版本。
Mongo [$inc 文档](https://www.mongodb.com/docs/v8.0/reference/operator/update/inc/)
与实测一致：缺字段建立、null拒绝、空 operand为no-op；overflow结论来自固定版本实验。

## 验证索引与证据分层

| 场景 | 源码/实际证据 |
| --- | --- |
| profile/wire/missing/Program、byte/depth/node/path/type/duplicate/conflict | `internal/{protocol,mongostore,searchstore}/*expression*test.go`、`TestExpressionWireBoundaryIsOpaque`；离线 nil client 验证拒绝不用后端 |
| Mongo set/unset/inc、large integer/Decimal/overflow/null/noop、40 并发 inc 无丢更新 | `TestMongoExpressionAtomicAndNumeric`；真实读取结果，只用于验收不进入生产执行路径 |
| Mongo native update/replace/delete/recreate 竞争 | `TestMongoExpressionNativeCompetition`；command monitor 在发送 update 前完成 native writer，证明表达式作用于当时记录而非锁住预读；零 find、一次 update |
| Mongo 实际成功后 drop、truncated/missing-n、write concern、冲突 | `TestMongoExpressionReplyLossAndConcern`；实际 TCP 回复丢弃与合成 envelope 修改明确区分；failCommand 有限注入 concern/conflict；一次 update |
| Search merge/missing/noop/mapping、大整数、native writer、回复损坏/丢失/redirect | `TestSearchExpressionMergeMissingAndNoop`、`TestSearchExpressionRealReplyFaultsAndNativeCompetition`；真实 effects + exact one update/zero client GET |
| Search 429 容量拒绝 | `TestSearchExpressionRealCapacity`：有限4连接/8调用波次，真实线程池拥塞；有 APPLIED、有真实429，单项返回至多一个反馈 |
| Search 明确 conflict 证据 | `TestSearchExpressionRealConflictEvidence`：native writer 更新后，用受控 stale OCC 请求取得实际409，再走表达式回复分类；**这不是声称 proxy 暂停了公开 Update 的后端内部读取** |
| pipeline/source | `TestSearchExpressionPipelineAndSourceQualification`；两产品独立；实际 pipeline 行为与 Weir 拒绝，普通记录能力回归 |
| cancel/queued/deadline/overload/drain/结果额度 | `TestMongoExpressionCancellationLedgerAndDrain`、`TestSearchExpressionCancellationAndDrain`；数据库先完成，回复 gate 保留，再取消/超时；queued项不执行，最终账本归零；普通 shared-batch deadline测试保持 |
| direct/两跳 unary/Bulk、同key序列、partial failure、计数 | `TestPublicExpressionUnaryBulkAndOpaquePeers`；10 records/10 physical executions，每 relay 4 calls；格式错误表达式到末端拒绝、零 backend execution |
| mutate授权（既有范围） | `TestExpressionRequiresMutatePermission`；unary/Bulk 禁止表达式时零执行。不新增认证能力 |
| DB/peer/application 三个位置实际确认后丢回复 | 扩展 `TestPeerRealAcknowledgedReplyLossNoReplay` 的 expression 分支；Mongo/ES/OS逐一，真实效果、单命令与最终 Store APPLIED/UNKNOWN 指标 |
| 既有 unary/peer/metrics/Native/Scan | 完整默认与两个 integration profiles；包含旧独立65535-window期限、五RPC、三进程、背压与诊断回归 |

真实写竞争符合 native 单记录序列：表达式不承诺冻结此前 snapshot；Search 冲突返回失败而不重算。
本机固定 Search write pool=1 的并发样本主要触发429，未证明随机竞争必然触发内部 OCC409。
因此分别保留原生竞争序列测试、真实429测试和受控 stale OCC 的真实409分类，不能把合成
回复分类或构造的条件请求冒充大规模并发/复制/故障切换资格。

## 实际命令与失败历史

开始前确认其他 Weir 执行聊天未运行、Git干净、27028/19200/19201无监听。
首轮 `go test ./...`、`go test -race ./...`、两个 vet 通过；之后都禁用 Go dependency network。
原有带所有权脚本启动隔离 fixture，关键真实 baseline race（unary/peer/Scan/Native）通过，
日志 `.testdata/m7/baseline-real.log`。只有显式 integration tag/environment 才访问固定本机后端。

```sh
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch
scripts/search-local.sh start opensearch
GOPROXY=off GOSUMDB=off go test ./...
GOPROXY=off GOSUMDB=off go test -race ./...
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
# 以下四套串行运行；runner 自带 -p 1，避免 Mongo server-global failpoint 相互干扰。
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=opensearch scripts/test-integration.sh -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -race -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_SEARCH_INTEGRATION=opensearch scripts/test-integration.sh -race -count=1 -v
GOPROXY=off GOSUMDB=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
  go test -race -tags integration -p 1 ./internal/mongostore ./internal/searchstore ./internal/server \
  -run 'TestMongoExpression|TestSearchExpression|TestPublicExpression|TestPeerRealAcknowledgedReplyLossNoReplay|TestExpression' -count=3 -v
GOPROXY=off GOSUMDB=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=opensearch \
  go test -race -tags integration -p 1 ./internal/mongostore ./internal/searchstore ./internal/server \
  -run 'TestMongoExpression|TestSearchExpression|TestPublicExpression|TestPeerRealAcknowledgedReplyLossNoReplay|TestExpression' -count=3 -v
GOPROXY=off GOSUMDB=off GOMAXPROCS=2 go test ./internal/mongostore -run '^$' -fuzz '^FuzzMongoExpression$' -fuzztime=5s -parallel=2
GOPROXY=off GOSUMDB=off GOMAXPROCS=2 go test ./internal/searchstore -run '^$' -fuzz '^FuzzSearchExpression$' -fuzztime=5s -parallel=2
```

开发期间实际失败保留，最终通过不抹除历史：

1. 原空 Transform 测试预期 UNSUPPORTED；新 wire shape 区分缺form=INVALID_ARGUMENT。
   将“程序仍关闭”旧测试改为显式 Program，新增空form测试，未弱化约束。
2. `mongo-expression-first.log`：保守白名单遗漏副本集 `electionId/opTime`，真实写完成但回UNKNOWN。
   通过 command monitor 获取实际回复后只补这两项，未改 matched 证据要求。
3. `expression-second.log`：把 Int64 `$inc` overflow 错按聚合提升假设放在复合更新，真实后端明确拒绝。
   分拆并核实 Int32提升/Int64拒绝/Decimal/large integer，见 `mongo-numeric-third.log`。
4. `contention-es-first.log`：要求同一有限随机压力同时出现409和429，实测32成功、224拥塞、0冲突，
   测试失败。没有修改后端池/增加retry；改为上述独立证据，明确未捕获公开Update内部竞争409。
5. AST检查找到两个 test-only append inline `bson.E`，改为命名变量；不改变运行语义。
6. 增补编码边界用例时一次编辑脚本误改测试字符串，gofmt/go test 报 missing comma；修复后再执行最终默认与真实检查，没有把未编译的用例计为通过。
7. 最终人工复核补拒绝矛盾的成功/错误证据及防止 Search shard 计数相加溢出；对应离线向量与两个真实profile专项三轮 race 重新运行。

日志保留在忽略目录 `.testdata/m7/`，测试源码/命令及必要结论纳入 Git。完整最终检查与资源
收尾结果在本报告末尾登记；不将“正在运行”列为成功。

## 生产资格目标与范围调整

新增 [production-readiness.md](production-readiness.md)，在仓库保留统筹下发的多平台/多架构、
Linux binary/OCI/Kubernetes优先、1/3副本两worker、参考规格/负载校准/24h soak和运维门槛。
状态区分已实现、有限真实资格、未验证、阻塞、用户确认后才可延期，不以交叉编译作native-run。

用户随后明确排除新增 Weir 认证相关功能，清单已覆盖此前入口安全门槛：可信内网由部署环境
隔离；新增 Weir TLS/mTLS/账户/令牌/RBAC/权限/证书管理不算阻断。用户又明确要求删除已有 Weir 认证；**下一独立阶段待清理**，M7不拆认证/传输，
原基线回归仍用于本阶段验收，不能写已删除或永久保留。数据库要求的必要连接配置与无隐式重放仍须资格；不关闭标准 TLS 校验、不读已有秘密。
通用 ProgramTransform 尚未豁免，runtime隔离、资源/背压、UNKNOWN/故障恢复仍是必需门槛。
M7不执行后续部署、平台、长测或通用runtime实施。

## 最终检查与资源收尾

| 检查 | 实际结果 / `.testdata/m7/` 日志 |
| --- | --- |
| 默认 test / race、default / integration-tag vet | PASS，`final-offline.log`、`final-offline-race.log`、`final-vet.log`、`final-vet-integration.log` |
| Elasticsearch 完整 integration、完整 race | PASS，`full-elasticsearch.log`、`full-elasticsearch-race.log`；均包含 Mongo、应用/peer/Native/Scan/metrics 全回归 |
| OpenSearch 完整 integration、完整 race | PASS，`full-opensearch.log`、`full-opensearch-race.log`；独立选定产品 |
| 最终解析收紧后的两个 profile 专项 `-race -count=3` | PASS，`final-expression-elasticsearch-race3.log`、`final-expression-opensearch-race3.log`；表达式/取消/native竞争/三层回复丢失/public Bulk及授权；Mongo在两套中均执行 |
| 有界 fuzz（两个workers，各5秒） | PASS，`final-fuzz-mongo.log`：9199 executions；`final-fuzz-search.log`：70062 executions。有限随机样本，不是形式化证明 |
| 修改文件 AST 风格、gofmt、diff whitespace | PASS，`final-style-all-changed.log` 空；只核验/格式化本轮修改文件，未改无关基线文件 |

完整后端 suite 后的最终修订仅收紧 malformed response/编码校验；通过默认全套及以上全部受影响
真实专项三轮复核，没有改变调度或其他 RPC。生产资格列表中的跨平台、复制切换、长期负载等
仍为未验证/阻塞，不因这些本机回归变成 PASS。

使用原有 ownership-check scripts 停止本轮启动的 MongoDB、Elasticsearch、OpenSearch。
`cleanup.log` 确认 27028/19200/19201 无监听，两个标记为 `weir-milestone-2` 的容器均 exited。
临时代理/测试生成身份由各 fixture 回收。保留已有停止容器、带标记的数据目录、全部测试日志；
没有删除用户资产或停止无所有权证明的服务。停止后再跑不依赖后端的默认 suite，结果另见
`stopped-backends-offline.log`、`stopped-backends-offline-race.log`。

本阶段交付后停止，不启动下一阶段。已知后续重点包括用户要求的 Weir 认证清理、可信内网
入口/DNS、必要后端连接与无重放、通用 runtime 安全可行性、六平台原生运行、Kubernetes
多节点和冻结参考负载下长测，详见 production-readiness。无 push、PR、发布或生产部署。
