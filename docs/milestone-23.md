# M23 — loopback DNS 夹具端口分配修复

日期：2026-09-28。执行聊天 `01a0e572-f652-7c63-ad1c-eb07e218044d`。
基线 `16c7f35303123dde809ed768c76680e4bf5dcb3e`；实现及回归提交
`a51b10a6cebcf4bb44e7f7c18ed9baaa44a7b866`。本次仅修改测试夹具和文档，执行证据待统筹独立验收。

## 原失败与修复

统筹在基线上首次 `CGO_ENABLED=0 go test -count=1 -timeout=180s ./...` 真实失败：
`TestDNSRealFailureClearsAndRecovers/nxdomain` 在 `peer_dns_test.go:111` 报
`listen tcp 127.0.0.1:55239: bind: address already in use`。其后 race 及三轮定向通过不撤销该失败。
这是历史夹具缺陷，没有产品 resolver/后端回归证据，也不归为 M22R 新增产品 bug。

`internal/testutil/testdns/server.go` 原先先绑定 UDP 随机端口，再尝试同号 TCP；两个协议的端口空间独立。
现在由 `listen` 持有完整 UDP/TCP pair 后才返回；`Start` 随后创建 Server、启动 goroutine 和发布地址。
TCP 碰撞时先关闭本次 UDP，再重新申请随机候选，最多 16 次；耗尽返回带原错误的诊断。
仅重试地址占用（Unix `EADDRINUSE`，Windows Winsock `10048`），其他 TCP 错误及 UDP 获取错误直接返回。
没有先探测再重绑、全局函数替换或端口租赁框架。

`Server.Address` 仍是两协议共用的一个地址；`Dial`、`Resolver`、应答、64 槽并发和原 cleanup 逻辑不变。
peer、Mongo、Search 和跨进程 integration 调用者均未修改；真实 UDP A/AAAA、truncated→TCP fallback、
NXDOMAIN/empty/excess/oversized-wire/drop/delay、取消、连接更新和流固定等原断言保留。

## 确定性回归

新增 `internal/testutil/testdns/server_test.go`，全部使用自己持有的 loopback sockets：

- 持有同号 TCP blocker，仅释放自有 UDP；固定该候选令 16 次尝试必然碰撞，验证有限失败、无半对返回、UDP 可重新绑定，blocker 仍能接受连接；释放 blocker 后完整 pair 可重新获取。
- 自有 UDP 占用及非法端口在第一阶段直接失败，不发布资源、不进入 TCP 碰撞重试。
- 同时持有 8 个独立夹具，并行解析同一域名的不同答案；逐个验证真实 UDP A/AAAA 和 TCP fallback，结束后两协议端口均可重新绑定。
- 一个等待 DNS frame 的 TCP 连接加 63 个一小时延迟查询占满 64 槽，额外 TCP 被关闭；cleanup 在一秒内取消并 join，Active/slots/conns 归零，既有 TCP 收到 EOF，监听端口释放。

## 实际验证

原生环境为 Darwin arm64，Go driver/compiler 均固定 1.27.1。命令从仓库根目录执行：

```sh
export GOROOT="$PWD/.tools/go1.27.1"
export PATH="$GOROOT/bin:$PATH"
export GOPROXY=off GOSUMDB=off GOWORK=off GOENV=off GOTOOLCHAIN=local
CGO_ENABLED=1 go test -race -count=3 -timeout=240s -v \
  ./internal/testutil/testdns ./internal/server ./internal/backend/mongodb ./internal/backend/search \
  -run '^Test(Listen|StartIndependentFixtures|StartCleanupJoinsBoundedWorkers|DNS|MongoDNS|SearchDNS)'
CGO_ENABLED=0 go test -count=1 -timeout=180s ./...
CGO_ENABLED=1 go test -race -count=1 -timeout=180s ./...
CGO_ENABLED=0 go vet ./...
CGO_ENABLED=0 go vet -tags=integration ./...
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go vet ./internal/testutil/testdns
```

| 检查 | 最终结果 / 秒 |
| --- | --- |
| 夹具、peer、Mongo/Search DNS 定向 race | PASS / 149.155；15 个顶层测试各三次，无 skip |
| 全默认 CGO0 非缓存 test | PASS / 62.162 |
| 独立全仓 CGO1 非缓存 race | PASS / 64.188 |
| vet / integration vet | PASS / 0.392 / 0.442；integration 仅静态检查 |
| Windows amd64 夹具 vet | PASS / 2.967；仅静态，不是原生运行 |

日志与完整命令、exit、PID 在 `.testdata/m23/validation.json` 和同名 `.log`；
`tested-files.json` 对应上述实现提交的两份 Go 文件，`focused-coverage.json` 核对逐项三轮结果。
首次预检因继承的 `GOROOT=/usr/local/go` 选到 Go 1.27.0 compiler 而编译失败，记录于 `preflight.txt`；
随后仅为命令固定 GOROOT/PATH，没有修改全局配置。Windows 分类补充前的通过记录另存
`before-windows-classification/`，最终表格均来自补充后的完整验证，没有混用版本。

所有测试命令已 Wait 退出；夹具自有查询、TCP 连接、UDP/TCP listeners 已回收。
默认测试没有启动 Docker、外部 DB 或浏览器；未查询公网或修改宿主 DNS，未关闭未知 listener/PID。
没有运行容量、Docker、Java 扫描或六平台打包；产品源码、协议、生产制品、M22R 校准器/门槛均未修改。

## 独立验收来源与剩余限制

原失败、同号 UDP/TCP 自有 socket 反例及 M22R 结论来自统筹工作目录
`/Users/liran/Projects/Codex-Projectless/2026-09-26/referenced-chatgpt-conversation-this-is-an` 的
`outputs/weir-m22r-review.md` 与 `work/weir-production/acceptance-m22r/` 下
`default-cgo0.log`、`validation.json`、`probes.py`、`negative-probes.json`；原证据保持不变。

M22R 工具修复与有限发生器 NO-GO 调查已获统筹有限独立复核，不能称全部检查通过。
统筹 bounded-50 零丢弃但 dispatch p99=8.3ms 超过 5ms，仍为 `generator_qualified=false`、`candidate=null`。
完整容量阶梯/确认/直连/过载恢复未运行，24h、其他 native 平台及其余资格矩阵仍未通过；M23 不豁免这些门槛。
Weir 自身认证继续排除；通用 ProgramTransform 首版明确延期并保持 UNSUPPORTED。
