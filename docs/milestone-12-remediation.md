# M12R：Mongo 本地连接所有权与分层预算

2026-09-27，local main，基线 `af541816dd74165afe4de96bc1723a7830631a3b`。
交付 SHA 见本地提交和 `.testdata/m12r/final-state.log`。M12 的 FAIL/FAIL/PASS
独立验收与原始日志保持不变；本次修正的是实现缺口和观测含义，不追认 M12 通过。
本机 Go 1.27.0 / macOS arm64、mongo-driver 2.9.1、MongoDB 8.0.32、
Elasticsearch 8.17.0、OpenSearch 2.19.0，仍不代表整体生产资格。

## 根因与因果证据

固定 driver `x/mongo/driver/topology/pool.go` 的 `removeConnection` 先删除 pool map
并唤醒创建；`getIdleConn`、`checkInNoEvent`、`removePerishedConns` 随后异步 Close。
所以 P 只限制 driver 的 map。`server.go` 的 direct/poll 只有一个 heartbeat connection，
不启用 streaming RTT monitor。`createConnections` 有两个固定 worker；即使 P=1，退休也可能与尚未退出的
creator 重叠。因此连同 poll，最多3个在途 dial 调用；持有 slot 的 TLS/OCSP
流程则最多 K=min(P+1,3)，等待 slot 的 worker 不能提前启动 DNS/TCP。

原代理 `Proxy.relay` 是单 reader 的串行 request/callback/backend/reply 循环。
等待 callback/DB reply 时不读 downstream EOF；上游 Dial/Close 与计数加减也不是 OS 原子操作。
其 upstream 高水位既不能证明本地 raw 超额数量，也不能证明取消后 DB 工作已经结束。
本次保留原串行故障代理、真实 ACK 后丢回复、391/Native/Scan/RMW 语义；没有封顶 upstream、
增加第二个协议 reader、建立 reader 队列或更改业务调度来压低观测数字。

三个互补探针（全部三轮 race；精确时刻见对应日志）：

1. 默认 `TestProxyRetainsUpstreamAfterDownstreamClose`：客户端 raw Close 已完成，
   第一条代理请求仍在 callback；新客户端进入，只有一条 live downstream，代理 upstream=2。
   释放 gate 后才收到 backend reply，最终 upstream=0。这是合成 wire server 的归因证据。
2. `TestMongoDriverRetirementOwnership`：真实 TLS/SCRAM mongod 和固定 driver，
   用 idle 过期强制 pool 退休，标准 PoolMonitor 只记录事件；外部 `net.Conn` seam 延迟 raw Close。
   例如 pool ID=1 在 `05:01:06.249060Z` 移除，raw Close 在 `.249070Z` 进入；
   新拨号等待，owned=peak=limit=2、closing=1、dialing=1，acquired 仍为2。
   释放 raw 后第三次 acquisition 才发生，最终 acquired=released=3、owned=0。
   这是隔离 driver 路径探针；产品功能资格另由实际 `Open`/CLI 测试给出。
3. `TestMongoOwnerRemoteTail`：生产 `Open(P=1)` 经代理连真实 TLS/SCRAM Mongo。
   旧 update 已到代理，取消后原调用 UNKNOWN、本地 raw 关闭：owned=1，代理 upstream=2。
   新独立 update 成功：本地 owned/peak=2/2，代理 upstream=3/3。
   释放旧请求 gate 后，真实 DB 仍完成旧 update；两个不同 ID 各恰好一个 update ACK，
   独立 DB readback 都存在。三轮 gate 释放至观测尾部回收约 5.4–6.9ms，随后全部为0。
   这只证明本次可达 backend、正常回复与释放故障 gate 的条件下恢复，绝不承诺任意分区的尾部时限。

源码位置：`mongodb/dial.go`、`wire.go`、`tls.go`、`owner_integration_test.go`，
`testmongo/proxy.go`、`proxy_test.go`。默认代理探针从 opt-in fixture 构造桥分离，
不把真实 mongod 启动带进默认测试。

## 最小所有权修复

每个 Mongo adapter 的 `boundedDialer` 独占 P+1 个 slot；同一 `local.concurrency` 提供 P。
slot 从 DNS 开始，覆盖 TCP、TLS、OCSP 等待、Mongo hello/SCRAM、使用/idle、driver 退休，
直至 **raw Close 返回**。`mongoConn.Close` 用一次性关闭释放 credit；Read 错误、重复 Close、
取消、部分 Open 失败不重复释放。不把 pool event 当作 raw 关闭。

最多3个 driver dial 调用在 owner 中活动；满额时这些已有 worker 在原连接期限内等待，
没有无界排队结构、额外池或重连循环。新的 pool member 不可能永久占用 P 以外的 slot：
有限 retired raw 关闭后会唤醒等待者，poll 仍有其名义第 P+1 个位置。P=1/2/4 均强制测试
满额、连续八次取消、退休重叠、重复 Close、Close 与 Dial/TLS/DNS 竞争，检查实际 acquisition
和最终 released 相等。标准 driver 的后台连接恢复不改变 mutation 的无重放策略。

DNS 使用真实共用 `netlimit.LookupHost`：每 lookup 最多两个 DNS I/O，4098 bytes+sentinel、
最多八个结果，取消关闭且 join 后才返回。Mongo 对有限地址列表串行 TCP 拨号，失败连接
先结束才试下一个；没有 Happy Eyeballs 双 socket，也没有应用级目标切换或业务重试。
原 hostname 继续用于 SAN/SNI。接入时发现强制给单标签 `localhost` 加尾点会失去 Go 的
host-file 匹配，已按 Go `absDomainName` 行为保留单标签、对多标签保持 absolute；
共享 Search/peer DNS 边界重新三轮 race。

原始 2 秒 connect budget 及更早 caller cancellation 不变。TLS 标准验证 → 固定 driver
OCSP 验证 → 解密后的 wire guard → driver hello/SCRAM/业务的顺序不变。OCSP 每个握手
仍最多一个 HTTP responder、无 redirect/proxy/keepalive；它有独立单 slot raw owner，
用于关闭/join HTTP 的 detached dial/DNS，绝不把 OCSP HTTP 当成 DB socket。
固定 OCSP verifier `contactResponders` 自己 `group.Wait()`，raw owner 再 join 剩余 HTTP dial。
未关闭 OCSP、证书校验、压缩/wire 限制，未改变 retryWrites/Reads/adaptive/retargeting。

Adapter Close 先禁止新拨号并取消 setup，再在原2秒内 Disconnect，最后关闭/join owner
剩余 raw 和 dial worker。已有 idle 上仍可发送 endSessions；失败不触发新建连。
最终一次 `backend_connections_closed` 日志只有 backend/store 和固定数值，不含 URI、
凭据、证书或文档。`weir_backend_connections_*` 在现有每 Store registry 下给出
owned/peak/limit/acquired/released，Mongo 另有 dialing/closing。每次 credit 转移在同一锁下
更新高水位；不是周期采样峰值。最大16 Mongo Local 的合法诊断图为2043固定 series。
Core 只透传标准 Prometheus Collector，不知道 Mongo/TLS。

Search 产品连接策略未重写；只在已有 channel slot 的 acquire/release 临界区记录相同
高水位/终态计数，并让现有 diagnostics 采集。其普通+Native P+1 上界仍由原 owner 实现。

## 本地硬上界与远端条件

设 live 包括 starting、serving、draining、terminating，S 为这些进程里所有同 DB Local 数。
每个 Local 单独计数；两个别名或两份相同 URI 不共享 pool。纯 forwarding 的 DB pool=0。

```text
tracked data executions <= Σ C
cleanup execution reservations <= S   (cleanup reuses the backend pool)
Mongo local dial/raw/closing slots <= Σ(C+1)   (C business + 1 poll)
Search local ordinary/Native slots <= Σ(C+1)  (C ordinary + 1 Native)
local DB + DNS sockets <= 2 Σ(C+1)            (DNS joins before DB TCP)
Mongo OCSP extra sockets <= 2 Σ min(C+1,3)    (separate HTTP/DNS owners)
remote DB accepted connections = accepted connections with live local counterparts + retirement tail
remote outstanding work = live tracked attempts + work surviving timeout/reply loss
```

OCSP 公式是与 DB/DNS 保守相加的严格上界，不是新的 pool 参数。它可能高估不能同时到达
的阶段组合。DB TLS/SCRAM 只用既有 socket；Native/Scan/RMW 不再另加 Mongo 业务池。
Search Native 额外一条可以与 P 条 ordinary idle 共存；Native 仍占一个 Store permit。
外部 native writer、fixture admin、探针及 DB 自身资源另计。

| 实例模型 | S | ΣC | Mongo/ Search 本地 DB slots | DB+DNS slots，不含 OCSP |
| --- | ---: | ---: | ---: | ---: |
| 3 executor，各一个 C4 Local | 3 | 12 | 15 | 30 |
| 上述受控替换，4 executor | 4 | 16 | 20 | 40 |
| 3 executor，其中一个有两个同 DB C4 Local | 4 | 16 | 20 | 40 |
| 上述加一个单 Local C4 replacement | 5 | 20 | 25 | 50 |
| 本测三个 executor：C1/C2/C4 | 3 | 7 | 10 | 20 |
| 本测重叠四进程，加两个 C1 Local | 5 | 9 | 14 | 28 |
| 旧 C1 Wait 退出后 | 4 | 8 | 12 | 24 |

这些是**本地 owner 保证**，不是 proxy/DB accepted 或 DB 工作的无条件瞬时上限。
重复取消的远端尾部不能用一个经验常数修正；需要实际部署限制存活实例与测量 backend/
网络回收，且保留 UNKNOWN。Kubernetes、跨节点 primary 切换/分区、容量/24h soak、
六平台 native-run、OCI，以及 ProgramTransform 仍 required/unqualified，不在本次实现。

## 真实进程与替换验收

保持原24 reader/1.2s steady、72 Read/Mutate/Bulk producer/450ms、100ms barrier、
3秒独立读恢复预算，以及 Native、Scan cleanup、三 executor+native writer 原子竞争、
forwarding 的四项固定目标 Bulk。没有降负载或将原 mutation 换成 read。

新 replacement 现在返回共同 executor 集合，两个 Local 从启动到最后 Wait 都检查。
四进程同时在五个 Local 上持有真实请求 barrier，记录各 PID、owned、active 和派生总界；
没有把不同时间的采样相加冒充同时峰值。随后两 Local 各8 reader/100ms，包含取消与恢复；
分别运行 Native，Search 特别同时保留两个 ordinary idle+两个 Native owner，实际 owned=4。
保留旧已 ACK 丢回复→UNKNOWN、queued cancel 无DB效果和原新独立 mutation；另给 extra
Local 一个新 ID mutation，精确计数为2而非重放旧写。独立读回（Search _version=1）覆盖两者。
最终四个 PID 的五份 owner summary 都必须 owned=0、acquired=released、peak≤各自C+1；
Mongo dialing/closing=0。代理 upstream 另测回收为0，记录其完整高水位而不与本地阈值混用。

Mongo 第一组三轮中，旧 C1/C2/C4 代理峰值分别为 `3/5/6`、`2/3/5`、`3/3/5`；
对应本地 owner 峰值三轮均 `2/3/5`，replacement 两 Local 各为2、全部最终0。
实际重叠 barrier owned 为12、12、13，硬界14；这是实际同时占用，不宣称恰好达到14。
原来能导致红测的 upstream 数字仍出现并保留，新断言验证了更直接的本地所有权。

## 命令和最终证据

全部命令前置 `GOPROXY=off GOSUMDB=off`。真实产品串行 `-p 1`，显式 opt-in。
完整日志及命令清单位于 `.testdata/m12r/`；skip 不作为真实 profile PASS。

| 检查 | 实际结果与日志 |
| --- | --- |
| 最终 owner/满额等待者/退休 raw/部分 Open/Close竞争、TLS/OCSP/DNS + 默认 proxy 探针，各3轮 race | PASS；`owner-workers-final-race3.log`，Mongo 13.808s，proxy 2.226s；30个顶层 PASS |
| 共用 DNS 的 Search 与 peer 边界，各3轮 race | PASS；`connection-boundaries-final-race3.log`，Search 30.331s、peer 149.440s；含真实30秒 DNS refresh固定流 |
| 最终 Mongo 生产 Open 的 remote-tail、固定driver退休、12次坏连接/4次恢复、真实 CLI C1/2/4+replacement，各3轮 race | PASS；`mongo-owner-final-race3.log`，Mongo 27.551s、app 36.308s |
| Mongo旧TLS/SCRAM负向、391、Native、ACK丢回复、游标清理、RMW竞争/commit丢回复、peer ACK与app全部操作，各3轮 race | PASS；`mongo-regression-final-race3.log`，Mongo 189.775s、server 57.786s、app 51.708s；还验证16 Local诊断图。该回归先于最终 waiter 推导补正，补正后的生产 Open/CLI 检查见上一行 |
| Elasticsearch 真实共享预算/两个replacement Local，各3轮 race | PASS；`elasticsearch-budget-race3.log`，73.927s |
| OpenSearch 同上，各3轮 race | PASS；`opensearch-budget-race3.log`，102.379s |
| 两种Search旧真实生产Open/丢回复/截断/权限/部分启动/direct/peer回归，产品串行各1轮 | PASS；`elasticsearch-regression.log`（33.516s+28.892s）、`opensearch-regression.log`（27.590s+24.860s） |
| 旧无认证Mongo连接/部分初始化、Native、ACK丢回复、Scan取消/清理，1轮race | PASS；`plain-mongo-regression.log`，5.340s；显式启停自有27028 replica set |
| 全仓最终非缓存 test / race | PASS；`final-default-test.log` / `final-default-race.log`，server 59.245s / 60.331s |
| 全仓最终 default / integration vet | PASS，均exit=0；`final-default-vet.log` / `final-integration-vet.log` |
| 修改及新增Go AST风格、diff whitespace | PASS；`style-final.log`，`git diff --check` 无输出 |

Mongo专项里未启用的Search子项 skip 不算Search通过；两产品证据来自各自日志。
最终 Mongo 三轮实际 executor PID（C1/C2/C4/replacement）：

| 轮次 | PID | 本地owner峰值（replacement为两份） | 代理峰值 | 同时五Local barrier owned |
| --- | --- | --- | --- | ---: |
| 1 | 69848 / 69849 / 69850 / 69852 | 2 / 3 / 5 / (2,2) | 3 / 3 / 5 / 4 | 13 |
| 2 | 69863 / 69864 / 69865 / 69867 | 2 / 3 / 5 / (2,2) | 2 / 3 / 5 / 4 | 13 |
| 3 | 69892 / 69893 / 69894 / 69910 | 2 / 3 / 5 / (2,2) | 2 / 5 / 5 / 5 | 13 |

所有owner最终0、acquired=released；第三轮连replacement代理也超过其本地名义4，
本地两owner仍各≤2。这是明确保留的跨层反例，不把红测数字藏掉。
ES三轮replacement PID=68757/68777/68801，OS=68907/68939/68974；
每个replacement两Local的peak都为2，各产品C1/C2/C4原节点peak=2/2/5，最终都为0。
Search五Local barrier实际owned分别8/9/8与8/8/8，Native阶段另强制replacement owned=4。
`evidence-summary.json` 汇总实际PASS、PID、owner终态和重叠观测，不代替原日志。

主要复现命令（完整选择器在 `commands.txt`）：

```sh
GOPROXY=off GOSUMDB=off go test -race ./internal/backend/mongodb ./internal/testutil/testmongo \
  -run 'TestMongo(Owner|TLS|DNS)|TestProxyRetains' -count=3 -timeout=90s -v
GOPROXY=off GOSUMDB=off WEIR_M10_INTEGRATION=1 WEIR_M12_INTEGRATION=mongo \
  go test -race -tags integration -p 1 ./internal/backend/mongodb ./internal/app \
  -run 'TestMongoOwnerRemoteTail|TestMongoDriverRetirementOwnership|TestMongoSCRAMTLSRepeatedFailureAndClose|TestMongoSharedProcessBudget' \
  -count=3 -timeout=3m -v
GOPROXY=off GOSUMDB=off WEIR_M12_INTEGRATION=search WEIR_SEARCH_SECURE_INTEGRATION=elasticsearch \
  go test -race -tags integration -p 1 ./internal/app -run '^TestSearchSharedProcessBudget$' \
  -count=3 -timeout=4m -v
# 随后将产品改为 opensearch 独立执行，不能与另一真实套件并行。
```

## 开发失败记录与范围

- `initial-owner.log` / `owner-boundary-race3.log`：单标签 localhost 被加尾点导致 SNI
  连接失败，旧测试又无条件等 SNI 通知直到超时。修复共享解析 canonicalization，并让
  测试仅在成功连接时等待 SNI；未削弱证书/SNI 断言。第一组三轮 proxy 探针也揭示
  accept 与计数不是原子操作；改为等观测事件完成再断言，保留独立层次。
- 这些初始失败日志保留；`owner-boundary-fixed-race3.log` 三轮通过。后续源码简化去掉
  wire wrapper 多余 raw Close 分支。最终复核按两个固定 creator+一个 poll 推导
  3个 dial worker（即使 P=1），与最多 min(P+1,3) 个持有槽位的 TLS 流程分开；
  显式 waiter 满额测试、最终边界/真实矩阵重新验证。

没有其他 agent、push、PR、发布、生产部署、既有秘密读取或定时任务。
资源清理已实际核验，见 `cleanup.json` / `cleanup.log`：本次139个 Mongo TLS fixture、
10个 Search fixture 的 owner marker 全部匹配，只剩 owner 与脱敏/轮转日志；无 data、
CA/私钥/keyfile/materials。无 mongod/Weir/test 子进程，M11 owner label 下无容器，
旧27028监听已关闭；旧明文fixture的历史数据保留。本次没有读取材料内容来做核验。
清理核验脚本初版误把 Mongo 轮转日志当成额外材料、随后遇到 ps 输出截断UTF-8；
修正为只准许明确日志名、仅采集 pid/comm 后通过，未因此删除任何文件。
本地提交后停止 checkout 写入与测试，向统筹报告并等待独立验收。
