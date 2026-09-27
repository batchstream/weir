# 代码组织与职责边界整理

2026-09-27。基线：本地干净 `main`，
`1cac5092be961833842b914b8e3ff8d11ce28f9f`。本阶段最终提交 SHA 和清理状态写入
`.testdata/code-organization/final-state.log`，并随完成消息交付统筹；提交不在自身内容中
嵌入自身 SHA。当前导览独立放在 [code-organization.md](code-organization.md)。

这是行为保持的组织整理，没有新增产品功能、依赖、认证、协议或连接 profile。
运行环境为 Darwin 25.6.0 arm64 / Go 1.27.0，MongoDB 8.0.32、固定 driver v2.9.1，
ES 8.17.0 / OS 2.19.0 为原有自有 loopback 容器。证据均为本轮执行，不引用前任 PASS
替代回归；不提升整体生产资格。

## 审查结论与取舍

先阅读架构的 Core/Adapter/Service/资源所有权及第 16 节、原规划、M10R 报告、独立
验收和生产目标，再记录全仓 import 图、符号及默认/integration 测试清单。

| 原始问题（具体位置） | 本轮处理与收益 |
| --- | --- |
| `app/stores.go` 实际拥有整个 Node，不只 Store；`diagnostics.go` 同时放节点状态、全图 metrics 注册和 HTTP 探针 | 按 assembly/node/metrics/diagnostics 分文件，同一 Node 仍拥有全过程，无第二条生命周期 |
| Config.Validate 与 openLocal 分别拼同一 Store limits | `Local.runtimeLimits` 是唯一拼装位置，默认值、校验和实际构造不再各写一次 |
| `server/server.go` 混合初始化、unary、Bulk pump、listener/关闭；`service.go` 混合路由、共享 admission 和 relay 占位 | 按职责拆文件；Service/resolve 聚合，admission 自成文件，Remote.enter 跟 relay 放一起 |
| `peer.go` 实际是入口/转发 metadata；`endpoints.go` 拥有整个 RemoteWeir 生命周期 | 改为 forwarding.go、remote.go/remote_dns.go，与 remote_relay/remote_bulk 一起可按调用顺序阅读 |
| mongostore/searchstore 名称把 Adapter 与 StoreRuntime 混淆，后端散落于 internal 根 | 一次迁入 backend/mongodb、backend/search；名称与真正客户连接/数据语义归属一致 |
| testmongo 用 DB 名查全局 secureFixtures，再由 URIFor 猜连接 profile | 显式 Fixture(Admin/DB/URI/TLS 材料) 从 Open 传给 Adapter builder、proxy、peer/replica；删除 map、查找、登记/注销及未知 DB 的隐式回退 |
| server fixture 只携带观察 client/DB，无法表达生产连接来源 | 用同一个 Mongo Fixture 引用替代 native/db 两个散落字段，无重复 profile 状态 |
| secure fixture 两处、CLI 子进程构建/示例加载使用固定 ../.. | 统一 testutil.Root 按准确 module 名向上定位，实际迁入三级目录后的 TLS 进程验证此路径 |
| 通用 integration builders 隐藏在首个测试场景文件 | Mongo、Search、server 本地/scan/replica builder 移到命名明确的 fixture 文件，故障专有 helper 留在对应断言附近 |
| rmw.go 中 IncrementConformance 没有任何生产调用；Lua 只有失败可行性探针却位于 internal 根 | RMW 计数器及事务 options 只编入 integration 测试；真实 Scan 共用的取消桥保留生产 attempt.go。Lua 移入 experiments，不制造已实现 ProgramTransform 的印象 |

保留了已经正确的边界：一个 module 和原公共生成客户端路径；Store 的单状态机与
execution 的小契约；backend 自有 codec/语言/客户端；Core opaque 文档；每 owner 自有
metrics。没有为了推荐目录新建 config/service/transform/observability 空包。
server 的 relay 与 delivery/credit/drain 需要直接协作，跨包会扩大内部 API，因此仍是
一个包。value 的 typed model 也不包装成虚构 runtime。

## 迁移分类

| 旧位置 | 新位置 | 类别 |
| --- | --- | --- |
| `internal/mongostore/*` | `internal/backend/mongodb/*`（package mongodb） | 整包路径/名称迁移；除下面列明的 test-only 分离外算法未改 |
| `internal/searchstore/*` | `internal/backend/search/*`（package search） | 整包路径/名称迁移 |
| `internal/testmongo,testsearch,testdns,testmetrics` | `internal/testutil/` 下同名子包 | fixture 所属分组；Mongo 显式数据流和 Root 是实质整理 |
| `internal/luaprobe` | `experiments/luaprobe` | 纯测试路径迁移 |
| `app/stores.go` | `assembly.go`、`node.go` | 初始化和运行生命周期分开；`stores_integration_test.go` 改 assembly 名称 |
| `app/diagnostics.go` 的节点状态/metrics | `node.go`、`metrics.go` | 文件聚合，不改锁/状态/采集行为 |
| `server/server.go` 的 RPC/连接生命周期 | `unary.go`、`bulk.go`、`listener.go` | 函数原样移动；New/Config/Limits 留 server.go |
| `server/service.go` 的 Admission/Remote.enter | `admission.go`、`remote_relay.go` | 资源所有权聚合 |
| `server/peer.go`、`endpoints.go`、`peer_dns.go` | `forwarding.go`、`remote.go`、`remote_dns.go` | 原样重命名 |
| `server/relay.go`、`relay_bulk.go` | `remote_relay.go`、`remote_bulk.go` | 重命名，前者接收原有 Remote.enter |
| 各包 integration_test.go 通用 builders、server scan/replica builders | `fixture_integration_test.go`、`scan_fixture_integration_test.go`、`replica_fixture_integration_test.go` | 同包测试依赖显式导航 |
| Mongo `rmw.go`、adapter.go 的 transactionOptions | `rmw_conformance_test.go`；生产共用取消桥到 `attempt.go` | 隔离实验/资格入口，状态机和全部 RMW 断言原样保留 |

少量同等行为的整理：共享 Local limits 构造、显式 Mongo fixture 传递、资产根目录定位；
Search 的四处既有 if 内四参数调用改为先命名 read plan，断言不变。
没有 re-export、alias 兼容壳、替换生产函数的变量、薄接口、注册管理器或新依赖。
README/readiness 当前路径更新；双语架构仅澄清建议目录不是强制，并链接实际导览。
历史资格报告保留原路径，未伪改历史证据。

## 语义保持证据

本地证据目录 `.testdata/code-organization/` 中：

- `imports-before.log` / `imports-after.log`：Core 不导入具体 backend/BSON/JSON，backend
  不反向导入 store/server/app；production-deps.log 和 production-symbols.log 确认
  默认构建不含 testutil/Lua/IncrementConformance。
- `ast-audit.log` / `audit.go`：按迁移后的包名对比全部函数 AST。基线 709 个函数，
  当前 710 个；新增 Root/runtimeLimits，删除 URIFor。生产 server/store、后端执行、
  TLS/wire、RMW 状态机和关闭函数体一致；生产实质改动限于 Validate/openLocal
  调用抽出的同一限额构造。测试变化为 fixture 引用、路径与四处命名 read plan。
- 默认测试入口 115 → 115，integration 220 → 220（含原有 fuzz 入口），无遗漏/新增
  skip。assertion-inventory.log 中 Fatal/Fatalf/Error/Errorf 分别为 1326/24/67/4，
  测试内 Skip=6、Run=70，前后均一致。测试场景、fault injection 与 outcome/账本断言保留。
- `protocol-generate.log`：固定 protoc 33.4 / Go plugin 1.36.11 / gRPC plugin 1.5.1
  重新生成；api、go.mod、go.sum 与基线完全一致。
- AST 风格检查、gofmt、git diff --check；没有参数/资源/期限调整。

## 实际验证

所有 Go 命令均带 `GOPROXY=off GOSUMDB=off`，实际故障包 `-p 1` 串行。
基线 `go test -count=1 ./...` 已通过并保存 baseline-test.log；以下为迁移后结果。

| 验证 | 本轮结果 / 日志 |
| --- | --- |
| 迁移后非缓存默认 race | PASS；migrated-default-race.log，server 60.666s |
| 完整 TLS/SCRAM integration race | PASS；tls-full-race.log：app 23.645s、Mongo 139.890s、server 219.974s、store 18.469s、fixture 7.902s；Search live 子测未启用 |
| TLS/OCSP/DNS、endpoint、关闭边界三轮 race | PASS；boundaries-race3.log，24 个顶层测试各 3 次，Mongo 12.666s、server 149.868s、app 3.994s、store 3.208s |
| 真实 TLS 关键三轮 race | PASS；tls-critical-race3.log，18 个顶层测试各 3 次：Mongo 260.177s、server 69.034s、app 7.724s、fixture 19.683s |
| credential-free Mongo + ES 完整 race | PASS；legacy-elasticsearch-full-race.log：app 36.161s、Mongo 20.981s、Search 11.967s、server 97.336s |
| credential-free Mongo + OS 完整 race | PASS；legacy-opensearch-full-race.log：app 35.111s、Mongo 20.779s、Search 11.558s、server 97.244s |
| 停止后默认非缓存 test/race、default/integration vet | PASS；stopped-default-test.log（server 61.153s）、stopped-default-race.log（server 64.548s）、stopped-default-vet.log、stopped-integration-vet.log，四个命令 exit 0 |
| protocol 重生成、依赖/生产构建/风格/import | PASS；protocol-generate.log、protocol-dependencies-unchanged.log、production-deps.log、production-symbols.log、ast-audit.log |

三轮实测中，五种 native writer 竞争仍各发生新事务重算（attempts=2、evaluations=2、
commits=1）。成功提交丢一次回复仍为同 session/txn 两次线上 commit、一次 evaluation、
APPLIED；持续丢回复仍是两次 commit、一次 evaluation，在约 702–703ms 后 UNKNOWN。
普通/表达式 391 各只有一次 update 并保持 UNKNOWN；合成 writeErrors[391] 仅证明驱动
错误分类分支，不把其中的 NOT_APPLIED 当作真实数据库未写证明。JSON Decode/Open 的
直接与 peer 路径实际验证 CRUD、精确 int64、表达式和 Bulk/Scan/Native End/EOF。

完整 TLS 命令：

```sh
WEIR_M10_INTEGRATION=1 WEIR_MONGO_PROFILE=tls \
  go test -race -tags integration -p 1 -count=1 -timeout=10m ./... -v
```

三轮真实 TLS 关键路径：

```sh
WEIR_M10_INTEGRATION=1 WEIR_MONGO_PROFILE=tls \
  go test -race -tags integration -p 1 -count=3 -timeout=10m \
  ./internal/backend/mongodb ./internal/server ./internal/app ./internal/testutil/testmongo \
  -run 'TestNativeRMW|TestCommit|TestRMW|TestCloseDuringCommit|TestAmbiguity|TestAcknowledgedOrdinary|TestMongoNativeReal|TestMongoExpression(ReplyLoss|Cancellation|NativeCompetition)|TestMongoSCRAMTLS|TestPeerRealAcknowledged|TestMongoTLSApplication|TestSecureMongoFixture|TestScanGRPCCompletionAndPartialFailure' -v
```

三轮本机 TLS/OCSP/DNS、端点和生命周期边界：

```sh
go test -race -count=3 -timeout=5m \
  ./internal/backend/mongodb ./internal/server ./internal/app ./internal/store \
  -run 'TestMongo(TLS|DNS)|TestDNS|TestEndpoint|TestAssemblyForwardOnlyPartialListenerAndConcurrentClose|TestDiagnosticsScrapeCloseRace|TestRuntimeOwnsAdapterExactlyOnce|TestScanCancelDuringFetchAndDrain|TestNativeSharesLedgerSessionAndC1' -v
```

真实 ES、OS 的自有实例启动/关闭次序见代码导览。实际完整命令分别选择后端：

```sh
WEIR_INTEGRATION=1 WEIR_MONGO_PROFILE=plain WEIR_M10_INTEGRATION=0 \
  WEIR_SEARCH_INTEGRATION=elasticsearch \
  go test -race -tags integration -p 1 -count=1 -timeout=10m ./... -v
# 停止 Elasticsearch 并启动 OpenSearch 后，同一命令改用 WEIR_SEARCH_INTEGRATION=opensearch。
```

两轮均开启
`WEIR_INTEGRATION=1` 和各自 `WEIR_SEARCH_INTEGRATION`，所以也覆盖 credential-free
Mongo 的完整既有套件。TLS 轮的 Search skip 不算 Search 证据；ES/OS 轮的 TLS skip
不算 TLS 证据。独立 DNS 子进程入口在父测试之外的 skip 保留，真正跨进程路径由
TestEndpointDNSAcrossProcesses 运行，不能只看父 PASS 推断子场景。

## 失败、清理与剩余范围

迁移中两轮编译失败保留在 compile-first.log / compile-second.log：拆文件遗漏 pb
import，以及包名/fixture 变量重名；修正后 compile-final.log /
compile-fixture-final.log 通过。没有通过删断言、提高期限、减少轮数或增加 skip 调绿。

所有真实测试和三轮 race 均一次通过，没有真实测试失败后重跑调绿。TLS full/critical
产生的 305 个自有 fixture 根已逐个检查：owner 与 mongod.log 保留，生成的 data、
server.pem、ca.pem、untrusted-ca.pem、keyfile 残留为 0；只检查本轮日志引用目录，
不打开私钥或历史 fixture。自有旧 Mongo 已停止，两个 Search 容器均为 owner 正确的
exited 状态，27028/19200/19201 无 listener。日志见 tls-fixture-cleanup.log、
stopped-backends.log、各 backend stop 日志。最终无项目测试/Weir/mongod 进程，
本轮检查二进制已删除；resource-cleanup-final.log 记录进程和短期构建材料检查。既有 .testdata 和固定 fixture 的历史数据/日志不删除。

结构上没有发现需要新增框架才能解决的阻断。保留的场景专有 builder 有不同的
transport 限额、代理故障和 cleanup 所有权，未把它们统一成可配置测试服务器。
server 仍有跨文件的私有 delivery、relay 和 metrics 状态，这是一个实际所有权边界；
当前文件导航已清楚，继续拆包需要新的独立理由。

生产资格限制保持不变：有限 Darwin/固定后端 profile；未取得通用 ProgramTransform、
Search TLS/认证、Mongo 多节点 failover、共享 DB 总预算、Linux/Windows 原生运行、
Kubernetes、性能/SLO 或 24h soak 资格。未 push、PR、发布、部署、访问已有秘密或
恢复定时任务。完成后向统筹交接并停止，由其独立核验。

## 根目录换行兼容补救（2026-09-27）

本补救基线为干净的 local main `ca9bed57801cda887b6a1b2d5e896db46352b22c`。
统筹独立验收发现 `testutil.Root` 把模块声明前缀固定为 LF，导致合法 CRLF go.mod
被拒绝。这是测试资源定位回归；上文真实 Mongo、ES、OS 的历史通过证据继续保留，
不能据其掩盖此问题。补救交付 SHA 与最终状态记录于
`.testdata/root-crlf-remediation/final-state.log`。

修改仅将首行按 LF 分开，去除末尾单个 CR，再准确比较完整模块声明；同时支持末尾
无换行。继续逐级向上查找并在文件系统根有限失败，不依赖 Git、网络或开发者路径。
没有通用 go.mod 解析器、文件系统接口、生产环境开关或隐藏 fallback；显式 Fixture
数据流与全部生产执行语义保持不变。

新 `internal/testutil/root_test.go` 直接调用 Root，覆盖 LF/CRLF/无末尾换行与
0/1/6 层目录，嵌套的其他模块不得抢先匹配。缺失模块、无关名称以及近似前缀、后缀、
子模块和嵌入名称通过测试子进程验证真实 Fatal 诊断、exit 1 和 10 秒上界。子进程
标记仅存在于 `_test.go`，不修改定位行为；无测试 skip，无既有断言或期限变更。

证据目录为 `.testdata/root-crlf-remediation/`。先逐字复制统筹的原 Root 和原探针，
记录源码 SHA256 与相等性：`before-probe.log` 为 LF PASS / CRLF FAIL、exit 1。
修后逐字复制实际 Root，使用完全相同探针，`after-probe.log` 为 LF/CRLF PASS、
exit 0。两次均在各自 `before-probe/`、`after-probe/` 目录运行
`GO111MODULE=off GOPROXY=off GOSUMDB=off go test -count=1 -v`，没有改工作树换行。
仓库新增回归在旧实现下的 `regression-before.log` 另保留 6 个预期失败
（CRLF/无末尾换行各三个深度），修后同一测试的三轮 race 全通过。

以下命令均使用 `GOPROXY=off GOSUMDB=off`，本次环境为 Darwin arm64 / Go 1.27.0：

| 命令 | 结果 / 日志 |
| --- | --- |
| `go test -race -count=3 ./internal/testutil -run '^TestRoot' -v` | PASS，15 个子场景各三轮；root-race3.log |
| `go test -count=1 ./...` | PASS；default-test.log，59.384s wall time |
| `go test -race -count=1 ./...` | PASS；default-race.log，61.230s wall time |
| `go vet ./...` | PASS；default-vet.log |
| `go vet -tags integration ./...` | PASS；integration-vet.log |
| 下列指定 TLS smoke | 两个顶层测试 PASS，0 skip；tls-smoke.log |
| 下列补充 TLS 应用 smoke | direct/peer 都 PASS，0 skip；tls-application-smoke.log |

```sh
GOPROXY=off GOSUMDB=off WEIR_M10_INTEGRATION=1 WEIR_MONGO_PROFILE=tls \
  go test -race -tags integration -p 1 -count=1 -timeout=5m \
  ./internal/testutil/testmongo ./internal/app \
  -run 'TestSecureMongoFixture|TestDiagnosticProcessSIGTERMReadinessBeforeExit' -v

GOPROXY=off GOSUMDB=off WEIR_M10_INTEGRATION=1 WEIR_MONGO_PROFILE=tls \
  go test -race -tags integration -p 1 -count=1 -timeout=5m ./internal/app \
  -run '^TestMongoTLSApplicationAssemblyAllOperations$' -v
```

实际检查了测试路径：指定 smoke 覆盖 Root 定位 mongod、TLS/SCRAM fixture 生命周期，
以及 Root 定位仓库构建 CLI 后 SIGTERM readiness=503、livez=200、有界退出
（本次 2.082s）。该 CLI 场景的服务使用 Remote 配置，因此补充已有应用测试验证
JSON Decode/Open 使用同一 TLS fixture，经 direct/peer 执行五类 RPC 后关闭。
没有启动旧 Mongo 或 Search，也没有重跑整个后端资格集合。

`tls-fixture-cleanup.log` 核对仅本轮三个新 fixture：owner 正确，生成的 data、
PEM、keyfile 均无残留，owner/mongod.log 保留，三个端口无 listener，CLI 子进程及
全部测试/mongod 进程退出。只检查材料路径存在性，未打开任何 PEM、私钥、keyfile
或历史秘密。`scope-check.log` / `production-deps.log` 确认生产 CLI 无 testutil
依赖，公共 api、go.mod/go.sum 未改，gofmt 与 diff 检查通过。

此证据修复了已知 P2，并验证本机资源定位与生命周期；结构阶段最终验收仍交由统筹。
Darwin 上的 LF/CRLF 测试不等于 Windows native-run，也不扩展 Linux、其他架构或
整个生产资格。ProgramTransform 仍 UNSUPPORTED，Search 连接、共享 DB 预算和
其他既定限制保持不变。本补救只提交 local main，未 push、PR、发布、部署或恢复定时任务。
