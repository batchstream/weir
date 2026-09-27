# M12：静态并发、共享后端与受控替换（未通过）

2026-09-27，基线 `24dbe4081276b3d7616a1cf26cc73bfecc60e729`，local main。
交付 SHA 见交接消息及 `.testdata/m12/final-state.log`。本阶段没有 push、PR、发布、
生产部署、其他 agent 或定时任务。只有本机 macOS arm64 的真实 CLI 子进程；
Go 1.27.0，Mongo driver 2.9.1，MongoDB 8.0.32 requireTLS/SCRAM，
Elasticsearch 8.17.0 和 OpenSearch 2.19.0 原生 HTTPS/Basic（固定镜像沿用 M11）。
测试只由程序生成和使用当轮凭据、证书，未打开已有秘密。

**M12 未通过完整预算资格，不能据此继续声称共享后端硬总上限成立。**
最小配置及 Native 时限回归已完成；两个 Search 产品分别取得有限三进程证据。
Mongo 真实 race 在取消/恢复后的连接高水位断言失败，下面保留原始数值、
模型缺口和需要统筹决定的最小后续范围。未扩大 timeout、减少负载或增加一个经验常数调绿。

## 配置和所有权

唯一产品字段为 `local.concurrency`：省略/零为 4，整数 1–32；负数或 >32 拒绝。
`Local.runtimeLimits()` 同时供整图纯校验、Store 构造和 Mongo/Search pool 使用，
非法后续 Local 在此前所有服务的 CA/DNS/DB/listener I/O 前拒绝。没有独立 pool 参数，
没有公开其他 Limits、分布式 quota、leader、自动 DB 身份归组或第二套调度器。

Read、记录写入、Bulk、Native、Scan fetch 共用单 Store 账本。AIMD 从 1 起步；
`window≤C`、`active≤C`，降窗不会撤回旧执行，故降窗瞬间不能要求 `active≤新 window`。
独立 Read 不参与跨请求同 key 排序，但仍占相同的执行额度。默认物理批最多 16 项，
并不等于 16 个同时 DB 请求。一个物理执行中的 metadata/读取/OCC/表达式步骤顺序进行。
默认逻辑在途项还受 128 result entries、16 MiB result reservation、每流 8 项等限制，
不能只用 `C×16` 当作新的准入额度。真实压力测试显式 `batch_operations=1`，
保留了真实 Bulk framing、同流顺序、End/EOF 和统一调度。

Native 整个交换继续占一个 permit。Store `selectLocked` 的默认
`BackendTimeout=2s` 也限制 Native，`executeNative` 使用该 ctx；RPC 默认 5 分钟/
最大 15 分钟与 adapter fallback 15 分钟都不能延长最严格预算。
`TestNativeDefaultPhysicalExecutionBudget` 实际等待默认 2 秒，5 分钟 caller 仍未到期，
确认 incomplete、permit/结果释放；没有将物理时限放大。M11 表和双语架构已补正。

Scan 和 Native 共用一个 live-session slot；不能在同 Store 同时开两个长会话。
Scan fetch 释放 permit 后可等待发送；取消/exhaustion 后 cleanup 有独立 2 秒预算和
最多一个清理执行，可能与普通 `C` 个执行重叠。cleanup 不另开 pool：Mongo killCursors/
killSessions 复用业务池；Search PIT DELETE 复用普通池。三个进程的测试同时运行
Native、Scan 和普通操作，另验证 Mongo 提前取消后的真实 killCursors。

## 固定源码审计与预算模型

设 `L_i` 是进程 i 内指向同一物理 DB 的 Local 集合，
`S(t)=Σ_{i∈live(t)} |L_i|`，`A_budget(t)=Σ C_ij`。
`live(t)` 必须包括 starting、serving、draining、terminating；运行期实际峰值 `I_peak`
由部署者约束，不是配置 replicas/HPA 上限的同义词。两个 Local 即使 URL、账号或目标相同
也有两个 adapter/pool；不同 DNS aliases 也可能是同 DB，运维显式归组。仅转发节点的
DB pool=0，其 peer/DNS/入站资源仍按自己的账本计入本地网络预算。

| 资源 | Mongo direct/poll | Elasticsearch / OpenSearch（两个产品分别验证） |
| --- | --- | --- |
| 本地 tracked physical execution | 每 Store ≤C；cleanup 逻辑预留另 ≤1 | 同左 |
| 普通/业务池 | P=C，min=0；idle 包含在 P，不额外相加 | 普通 HTTP/1 P=C；idle 包含在 P，idle timeout 30s |
| Native | 同一个 Mongo client/pool，无 Native 专用 DB pool | 专用 fresh HTTP/1 1 条，禁 keepalive；与普通 idle P 可以共存 |
| monitoring | 池外 1 条 polling hello；没有 streaming RTT socket | 无 driver monitoring socket/周期 backend probe |
| 建连 | `maxConnecting=2` 属于业务池，另有 poll；最多 K=3 条 setup 流程（2 个 pool 建连 worker + 1 个 poll） | 普通与 Native 共用 P+1 slots，DNS 开始到 raw Close 一直持有 |
| TLS/认证 | hello/SCRAM、TLS、适用 OCSP 都在固定 2s 连接预算中；TLS 本身不另开 DB socket | DNS/TCP/TLS 共享最多 2s；Basic 在同一 HTTP 请求中 |
| DNS | Go resolver A/AAAA；最多 K 个 driver dial 流程；Happy Eyeballs 的 TCP 候选和 resolver 退出尾部不能冒充业务池内连接 | 每 resolving slot ≤2 DNS sockets，完成后才能拨 1 TCP；自有网络 sockets ≤2(P+1)，DB TCP slots ≤P+1 |
| OCSP | 每并行 TLS 最多 1 个 HTTP responder；最多 K 个 responder 操作，DNS/HTTP 不属于 DB accepted sockets；测试 mongod 证书无 OCSP，不作为真实 OCSP 正向证据 | 当前 Search profile 没有另建 OCSP HTTP client |
| RMW/事务 | 生产支持原生 update expression，顺序使用同池；ProgramTransform 未实现。旧事务 conformance harness 不进入产品路径，不额外算一个事务池 | Replace 的 metadata/read/OCC write 顺序使用普通池；expression 原生 Update，retry_on_conflict=0，无隐藏并行池 |
| startup/shutdown | 资格 hello/buildInfo/listCollections、会话结束命令也走该 client；poll 在服务启动前/退出中仍可能存在 | 资格 metadata 和 Native 资格检查仍走各自有界 transports；close 关闭 raw sockets |

固定 driver 源码依据（相对 `go.mongodb.org/mongo-driver/v2@v2.9.1`）：

- `topology/server.go:747,862`：只有 streaming 才启动 rttMonitor；显式 poll 返回 false。
  `setupHeartbeatConnection` 使用独立 `s.conn`；不能凭习惯套池外两条监控连接。
- `topology/pool.go:1160–1194`：创建前在锁下检查 `len(p.conns)<maxSize`，新连接先放入 map；
  maxConnecting 不是在 P 外再增加两条正常业务连接。
- **`pool.go:726–750,934–938,1109–1114,1394–1399`：先从 pool map 移除并唤醒建连，
  再异步调用 closeConnection。P 约束 pool 账本，不证明所有尚未关闭 raw socket≤P。**
- `mongodb/wire.go` raw Close 避免 TLS close-notify 延迟，但当前没有 Search 那样从
  DNS/dial 到实际 raw Close 的独立总 slot owner，也不能证明数据库端立刻回收。
- Search `adapter.go` 两个 transport，`dial.go` 同一个 P+1 slot owner；Native 资格检查
  和实际请求可能顺序占用不同连接，普通 idle 必须保留在总数中。

完整的保守记账必须至少写成：

```text
tracked data executions <= Σ C_ij
cleanup execution reservations <= S
nominal Mongo pool + polling sockets = Σ (C_ij + 1)
Search locally owned DB TCP/dial slots <= Σ (C_ij + 1)
Search locally owned sockets including DNS <= 2 Σ (C_ij + 1)
DB accepted sockets = live owned/connecting DB sockets + remote retirement tail
DB outstanding work = tracked attempts + work continuing after timeout/lost reply
```

Mongo 的 driver-retired-but-not-yet-closed、Happy Eyeballs 连接候选、解析取消尾部以及
数据库/代理尚未处理关闭的连接必须另列，不能把它们默认为 0。业务调用结束、active=0、
关闭本地 TCP 均不证明远端工作已经结束。还要加外部 native writer、管理员/探针、DB 内部
连接和执行预算。测试使用独立 native writer/Admin，其连接不混入 executor 的观测代理。

**目前没有证明这些 Mongo 尾部在持续取消/恢复期间能由 C 与 I_peak 单独约束为固定常数。**
因此下面数字是明确无连接退休重叠假设下的稳态/规划基项；完整 Mongo 全过程硬 socket
预算仍未资格。不能将实测多出 1 条直接改成“永远加 1”或放大 pool 来掩盖。
Search 本地 slot 上界也不等价于 DB accepted sockets 的无条件远端上界。

| 明确部署例子 | Local 数 S | ΣC / cleanup 逻辑预留 | Mongo 名义 pool+poll / Search 本地 TCP slots | Search 含 DNS 网络 slots |
| --- | ---: | ---: | ---: | ---: |
| 3 个 executor，各一个 C=4 Local | 3 | 12 / 3 | 15 / 15 | 30 |
| 受控替换峰值 4 个 executor，各一个 C=4 Local | 4 | 16 / 4 | 20 / 20 | 40 |
| 3 个 executor，其中一个有两个同 DB C=4 Local | 4 | 16 / 4 | 20 / 20 | 40 |
| 上一行峰值加一个单 Local C=4 替换进程 | 5 | 20 / 5 | 25 / 25 | 50 |
| 本测试稳态 C=1、2、4，各一 Local | 3 | 7 / 3 | 10 / 10 | 20 |
| 本测试峰值加一个有两个 C=1 Local 的新进程 | 5 | 9 / 5 | 14 / 14 | 28 |
| 本测试旧 C=1 进程 Wait 退出后（3 个进程） | 4 | 8 / 4 | 12 / 12 | 24 |

Mongo 数字中加的是监控，Search 中加的是可与 idle 共存的 Native，不能因为算式相同就
认为所有者相同。Mongo C=4 的 K=3 只界定 driver 同时 setup 流程与适用 OCSP 操作，
不是另一个“所有 socket=3”的结论。纯 IP fixture 没有 DNS 数据库连接测量；Search 与
peer 的真实 DNS 三轮 race 是另外的边界回归，不把 DNS socket 当 DB accepted socket。

部署必须确保所有仍存活 executor 都在 I_peak 内，且下一轮替换等旧进程完全退出。
Kubernetes 的 terminating Pod 可能令资源超过 `replicas+maxSurge`；这不是只设置 maxSurge
就能得到的总预算。[官方 Deployment 说明](https://v1-36.docs.kubernetes.io/docs/concepts/workloads/controllers/deployment/#updating-a-deployment)
明确描述了这个终止期重叠。本阶段没有运行 Kubernetes，不作平台资格声明。

## 真实进程与观测

`internal/app/*shared_budget_integration_test.go` 构建带 race 的实际 `cmd/weir`，
通过 JSON 配置启动不同 PID、data/peer/diagnostics loopback 端口。每个后端串行独立运行：

1. 启动 C=1 → 加 C=2、4；24 个持续 Read producer，1.2 秒、代理每请求 10ms 延迟，
   让三个独立 AIMD 达到各自 C。使用同时阻塞的请求 barrier，实际 7 个在途交换保持不变，
   才读取各 Store/window/socket；不是把不同时间的 metrics 相加当同时峰值。
2. 每个代理的真实 upstream TCP 创建/关闭在同一锁内更新高水位。Mongo 代理同时转发
   SCRAM/hello/业务请求；Search 是 1:1 TLS socket relay，**没有代理 HTTP pool 合并连接**。
   代理入口 waiting/received 与 backend 完整 reply 分开，前者不是 DB 已执行数。
3. Mongo 真实 failCommand 16500、Search 单一代理合成 429 分别只降低 C=4 节点到 2，
   另一个 C=2 节点保持 2；合成 429 不声称数据库真实过载，真实压力另测。
4. 72 个持续 Read/Mutate/Bulk producer，450ms；wire barrier 保持 100ms 后释放。
   持续供给触发真实排队/拒绝，撤掉后新的独立 Read 恢复。Search 小写队列还会给出真实
   item capacity rejection；每项 Bulk result 与独立 DB readback 配对验证，不把拒绝改 APPLIED。
5. Mongo 三个 executor 各 12 次原子增量 + 独立 native writer 12 次，seed=1，读回=49。
   Search 三 executor 与 native Create 竞争同 key，只能有一个成功；没有全局 Weir 排序。
   Bulk、Native、Scan 的结果/End/EOF、普通操作共存和 Mongo cancel→killCursors 均覆盖。
6. forwarding-only 进程连三 executor，4 个同 key Bulk 项只落在一个固定 executor；
   前后无并发背景负载时的完整计数差分验证 total=4、唯一目标。入口没有 Local metrics/pool。
7. 3→4 时新进程有两个同 DB Local；旧进程一项写已获 backend ACK 但回复被代理保持，
   第二项排队后取消；SIGTERM 后释放丢回复，原调用 UNKNOWN，旧 Wait 退出且 sockets=0。
   排队写数据库不存在，旧 mutation 命令计数只增 1；新进程只执行一个新 ID 的独立写，
   Search 还读回 `_version=1`。未重放旧请求、未迁移 stream。随后才回收其余进程。

每次 PID、时间、窗口、端口、观测计数、退出证据在日志中。代理延迟、barrier、丢回复和
429 是明确拥有的合成网络/协议故障；数据库业务、TLS/SCRAM/Basic、实际提交和读回是真实。
本机 kill/restart 不是 DB primary failover、Kubernetes 或多 worker node 资格。

## 已发现的阻断和失败历史

最早 `mongo-race3.log` 三轮均 FAIL：

| 轮次 | 被测 executor PID / C | 稳态 barrier TCP | 后续事件高水位 | 被否定的假设 |
| --- | --- | ---: | ---: | --- |
| 1 | 61419 / 1 | 2 | 3 | 全过程 accepted upstream≤C+1 |
| 2 | 61440 / 2 | 3 | 4 | 同上 |
| 3 | 61461 / 1 | 2 | 3 | 同上 |

随后复核修正了本轮计数 helper 的观测干扰：原有 `defer backend.Close()` 捕获 raw TCP；
新增闭包最初引用了随后变为 TLS 的变量，可能等待 close-notify。最终显式保留 rawBackend，
恢复原始代理语义。未变更产品代码/执行时限、负载或峰值断言。`mongo-raw-close-race3.log`
仍出现峰值超限，另暴露取消后立即调用遇到 HTTP/2 reconnect 的测试时序假设。
恢复检查改为在**原有 3 秒总预算**内发送新的独立只读 probe，不重试任何 mutation。

最终 `mongo-recovery-race3.log`：两轮 FAIL、一轮 PASS，缺口仍可复现：

| 轮次 | C=1 / C=2 / C=4 的最终 upstream event 高水位 | 结果 |
| --- | --- | --- |
| 1，PID 62093/62094/62095 | 2 / **4** / 5 | FAIL，C=2 超过名义 3 |
| 2，PID 62110/62111/62112 | **3** / 3 / 5 | FAIL，C=1 超过名义 2 |
| 3，PID 62127/62128/62129 | 2 / 3 / 5 | PASS，取消后两个新 Read probe 恢复 |

这些是代理实际连接到真实 mongod 的 TCP，不是 sampled driver pool gauges。并不能据此
单独断定多出连接全部由 driver 异步 close 导致：当前串行故障代理可能在等待 backend
reply 时仍保留下游已经取消的 upstream，远端回收也有独立时序。**正因为尚不能完成
生产 raw owner、driver retirement 与故障代理保留的完整归因，本阶段不能宣称硬预算通过。**
稳态 barrier、UNKNOWN/no-replay 和全部进程清理仍有实际证据；早期未断言全过程峰值的
`mongo-run2.log` PASS 不覆盖这个缺口，不能替代最终复现失败。

最小下一步是统筹确定并单独审查 Mongo 连接退出/观测模型：在实际 socket owner 上保留
旧连接的计账到 raw Close，并将 driver 内退休和代理/DB 远端尾部分开验证；若需要改 Mongo
拨号/连接生命周期，保持 adapter 内一个 owner，不增加全局 quota，不改 mutation 语义。
未知尾部预算不能用一个经验倍数、增加连接池、降低持续供给或延长 timeout 解决。
本执行聊天不自行开启补救阶段或修改该所有权语义。

其他开发失败均保留：

- 初次 config 单测误匹配错误文字 `runtime limits`，实际是 `runtime bounds`；修正测试，
  同时验证 contacts=0。Native 默认时限回归当轮通过。
- `mongo-first.log`：ReadResult 是 oneof，测试误用直接 Failure 字段导致编译失败；改 GetFailure。
- `mongo-run1.log`：Native 误选未支持的 find；改为现有允许的 count，没有扩展产品命令。
- `elasticsearch-first.log`：真实过载后窗口已降低，不能再假设它仍是 4。独立降窗检查移到
  受控稳定段；过载仍用同一 72 producer/450ms，按当前 window 等待 barrier。
- `elasticsearch-run2.log`：Native 误用未支持的 `_count`；改为已有 `_doc` Native GET。
- `elasticsearch-run3.log`：错误预期 fixture 并发 4 项 Bulk 全成功。保留负载与独立读回，
  检查真实容量拒绝的 NOT_APPLIED+不存在，以及 APPLIED+存在；未降低流量调绿。
- `opensearch-race3.log` 前两轮通过、第三轮在过载后首个新 Read 遇到 HTTP/2 reconnect；
  修正为原 3 秒总预算内的新独立只读 probe，不重放写入；最终复跑见结果表。
- 首次全默认测试发现架构文档禁止引用阶段报告；改为引用既有 production-readiness 清单，
  未改该文档边界测试。

## 命令、最终日志与限制

所有 Go 命令使用 `GOPROXY=off GOSUMDB=off`，真实产品串行 `-p 1`，日志在 `.testdata/m12/`。

```sh
WEIR_M12_INTEGRATION=mongo WEIR_M10_INTEGRATION=1 \
  go test -race -tags integration -p 1 ./internal/app \
  -run '^TestMongoSharedProcessBudget$' -count=3 -timeout=3m -v
WEIR_M12_INTEGRATION=search WEIR_SEARCH_SECURE_INTEGRATION=elasticsearch \
  go test -race -tags integration -p 1 ./internal/app \
  -run '^TestSearchSharedProcessBudget$' -count=3 -timeout=4m -v
# OpenSearch 使用相同命令，把 profile 改为 opensearch。
go test -race ./internal/app ./internal/store ./internal/backend/search ./internal/server \
  -run 'TestLocalConcurrency|TestNativeDefaultPhysical|TestNativeSharesLedger|TestScanContinuation|TestSearch(Connection|TLS|DNS)|^TestDNS' \
  -count=3 -timeout=4m -v
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go vet -tags integration ./...
```

| 最终检查 | 实际结果 / 日志 |
| --- | --- |
| 全仓默认非缓存 test | PASS，`default-test.log`；初次文档失败保留于 `default-first.log` |
| 全仓默认非缓存 race | PASS，`default-race.log` |
| default / integration vet | exit 0，`default-vet.log`、`integration-vet.log` |
| 配置/Native/Scan/Search 连接与 DNS/peer DNS 三轮 race | 75 个顶层 PASS、无 FAIL/SKIP，`boundaries-race3.log`；peer 包 149.437s |
| Mongo 最新三轮真实共享预算 | **FAIL/FAIL/PASS**，`mongo-recovery-race3.log`，33.974s；全过程 socket 目标未合格 |
| Elasticsearch 三轮真实共享预算 | 3 PASS，`elasticsearch-race3.log`，70.722s；当时首个新 Read 即成功 |
| OpenSearch 最终三轮真实共享预算 | 3 PASS，`opensearch-final-race3.log`，98.979s；原 3s 预算内新只读 probes 可恢复 |
| 旧真实 Mongo TLS/SCRAM 回归 | PASS，`mongo-regression.log`；backend 16.786s、app 3.606s；ACK 丢回复无重放、cursor 取消/清理、SCRAM/TLS 391、direct/peer 全操作 |
| 旧真实 Elasticsearch HTTPS 回归及当前源码 M12 复核 | PASS，`elasticsearch-regression.log`；backend 30.978s、app 53.971s；M11 安全 profile、app direct/peer，另跑一轮共享预算 25.06s |
| 旧真实 OpenSearch HTTPS 回归 | PASS，`opensearch-regression.log`；backend 27.054s、app 25.429s；M11 安全 profile、app direct/peer |
| Go style / diff | 新 Go 文件 AST struct/参数/if 检查通过，`style.log`；`git diff --check` 通过 |

默认测试不启动数据库/容器；本轮真实 suite 是
显式 integration opt-in，skip 不是资格证据。Mongo 目标断言作为失败回归保留。
三个旧真实回归及当前源码 Elasticsearch 复核的完整命令保留在
`.testdata/m12/regressions.sh`，串行脚本 exit 0，三个日志均无 FAIL/SKIP。

## 资源回收与交接状态

最后测试结束后按本轮日志定位 38 个排他创建的 fixture 目录，逐一检查 owner 和目录项：
只剩 owner 与数据库/启动日志，Mongo data/证书/keyfile 和 Search materials 均已删除。
进程检查无 Weir、Go 测试或 mongod；fixture owner label 下无容器。
所有实际 Weir 子进程均完成 Wait，测试结尾 upstream sockets=0。
清理核验保留于 `.testdata/m12/cleanup.json`，本地提交和干净状态见
`.testdata/m12/final-state.log`。保留脱敏日志，不读取既有秘密或删除未知资源。
提交后停止 checkout 写入、测试及所有自有资源，再向统筹发送未通过/阻塞回调。

ProgramTransform、真正复制/primary 切换、六平台 native-run、Linux RSS/cgroup、OCI/
Kubernetes、参考负载校准和各平台 ≥24h soak 仍 required/unqualified。显式 CA 的成功
不代表系统 roots 正向真实数据库资格。短测吞吐/时延是负载参数和观察，不冻结生产 RPS/p99。
