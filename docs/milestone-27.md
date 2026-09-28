# M27 — CLI 启动期信号所有权与有界退出

2026-09-28。局部生命周期修复和指定范围验证完成，待统筹独立验收。没有授予整体生产、真实 DB、容量或全部平台资格。

## 源码与范围

- baseline：`e173a90b1c86645c00232ae6e043ef2e36b65d43`，干净 main。
- implementation / 实际编译受测源码：`e072d3b9f1ab23801ead183beb1cb786ec1b8fa6`。
- delivery：本报告的后续文档提交；提交后准确 SHA 写入 `.testdata/m27/delivery.json` 和同目录 `final-report.md`，并在唯一完成回调中提供，避免报告自引用提交哈希。
- 原始证据根：`/Users/liran/Projects/liran/go/weir/.testdata/m27`。`source-inputs.json` 固定全部 Go/module 输入；`build-plan.json`、`artifacts.json`、各 buildinfo 固定命令、产物身份和 CGO 差异。`artifact-provenance.json`逐项区分基线、仅开发使用的harness和8个最终产物；不能把目录快照中的旧产物误作最终源码编译结果。
- 只修改 CLI/app 生命周期、相应默认测试和必要中英文说明。没有 EKS、push、CI、镜像发布、秘密读取、认证/通用程序、宿主或 Docker 全局设置修改。

公开 GHCR 的已批准固定 M24 镜像仍对应 source `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`，本地修复未进入该镜像。原 M26R5 失败不改写，M26R5C 清理已独立接受，未重建或查询旧 namespace。capacity candidate 仍 null；完整资源、CNI、跨节点、后端安全、六平台完整资格和 24h 继续 required。

## 直接实现与退出语义

`run(args, output)` 在 `app.Open` 前统一 `signal.NotifyContext`，直到全部 Close 后才注销；5秒 startup 从信号 context 派生。Open 在资源取得边界检查取消，`Node.Start(ctx)` 在启动前和发布 serving 前检查取消；失败的部分组装仍走已有独立1秒清理 context。

Open 成功后用一个 defer 收口所有退出路径：独立 Background 的5秒 drain，原资源 owner 关闭，join 监听 goroutine，收集剩余监听错误。没有新增生产接口、可替换函数变量、测试 hook 或日志框架；监听信息改为直接写入已有 output writer。未使用取消后的 startup/signal context 作为 drain context。正常与部分初始化预算未增大。

启动阶段取消退出1：后端握手仍报告脱敏的 `local Store startup qualification failed`；Open 后 Start 前取消报告 `context canceled`。已正常 serving 时信号关闭且无错误退出0。真实配置/后端/监听/输出错误继续非零；没有“ctx canceled 就吞掉所有错误”的分支。同步发生输出错误的真实 SIGTERM 测试保留错误并退出1；Node.Close 等待全部监听报告，避免 select 先取到 signal 而丢失独立监听错误。

依据 [Go NotifyContext 语义](https://pkg.go.dev/os/signal#NotifyContext)，信号注册覆盖实际应用资源所有权；不承诺 SIGKILL 或程序尚未注册信号之前的任意时刻都能优雅关闭。Windows 的编译证据和 POSIX 的实际信号运行明确分开。

## 基线与历史失败

基线 CLI 在编辑生产源码之前编译，Darwin arm64 CGO0 SHA256：`fc97aa115773b06108de7e452bb5673d1ddbdf57ea11b963e9da2a3172696587`。将新握手屏障 harness 应用于该准确旧 binary，仅运行一次两个位置：初始版本握手、第一 Local/Remote 已取得后的第二 Local 握手。两次实际 SIGTERM 都由默认信号终止（Go ExitCode=-1、Wait=`signal: terminated`），不发生正常 owner-close 日志，见 `baseline-handshake.log`。

旧 M26 合成控制的 `exit=-15`、脚本和 stdout/stderr 均保留；`present-index-control-result.json` SHA256 仍为 `507e0daed2a96a3ead8e91d5491299913f3dc42f52ca87c41219146bc585b296`。未重跑旧控制争取全绿，也不由该单次旧失败外推所有关闭路径。

开发失败见 `development-notes.md`、`development-01.log`：测试遗漏 remote 路由，且错误地拒绝已有连接回收日志；修正接线与断言后普通/race通过。最初两次工具链调用失败来自自动下载校验和继承旧 GOROOT；仅对命令指定已缓存 Go1.27.1 的 GOROOT/GOTOOLCHAIN=local。没有安装/升级、sleep-to-green 或加长内部期限。

## 固定产物与实际进程结果

| 产物 | SHA256 |
| --- | --- |
| Darwin arm64 CLI / CGO0 | `abd9875285e6a381c472b899fad0654890a389365ac751745d6767a2f338912d` |
| Darwin arm64 CLI / CGO1 race | `f10d1e7a3dfe46b66951eb60dad7985a537bb4560b24feeb8094a09acb36ab0d` |
| Linux arm64 CLI / CGO0 | `4ec4af064b4679cd9cf61c3ef1c86e3e49100ce6fbee0e27c84475688a00a040` |
| Darwin 普通 / race 测试 binary | `1e298a239ae6fe2068c005d1a86dba8944010dbcd4ffbe039fe9105af4dc629f` / `86ec0da6a421a967a81f00e800f59397e0d775919be0598630150ab8ac7e3c04` |
| Linux CLI 测试 binary | `95d2073b00ec261c98d4eb6d77dee74d22989167aaedfbd3bc4f3a5c1a56b54b` |

Darwin 实际环境：macOS26.6.2/25G83，Darwin25.6.0 arm64，Go1.27.1。Linux 实际环境：同架构本机 VM 的 Linux7.0.12-linuxkit/aarch64，Go1.27.1 CGO0；ELF AArch64 静态 CLI/test binary，进程记录 runtime=linux/arm64、euid=65532。Linux 没有 race native 运行，Darwin 的 CGO1 race 独立列出，不外推。

| 边界与断言 | Darwin CGO0 / CGO1 race | Linux CGO0 |
| --- | --- | --- |
| 后端已收到握手再 SIGTERM；首 Local/Remote 后第二 Local 取消 | 各通过；普通固定每种3次 | 各通过；固定每种3次 |
| 第一条 listener 输出即 SIGTERM，不等诊断第二行/ready/延时 | 普通固定10/10；race通过 | 固定10/10 |
| Open 已成功、Start 尚未调用，真实 SIGTERM | 退出1，无 ready，全部地址可重新绑定 | 同样通过 |
| writer 内同步屏障、SIGTERM 与独立输出错误 | 退出1，错误保留，owner释放 | 同样通过 |
| 正常 ready → SIGTERM drain → closed，后端收全 mutation 后不返回 | readyz503/livez200；UNKNOWN=1，POST=1、重放0，正常退出0 | 同样通过 |
| 已占真实输入 slot、无 body、默认 stall30s | 独立约5秒 drain 强制收口，退出0；无已准入 mutation | 5.006秒 signal→退出，exit0 |
| 后端真实404/错误响应、version/help/probe/参数短路 | 脱敏非零且无 ready；原合同通过 | 同样通过 |
| app 取消、重复/并发 Close、监听报告 join；server 背压/无重放/共享资源 | 默认与三轮边界 race 通过 | app相关用例、server边界通过 |

所有 signal 都发送给本测试启动的准确 child，父 go test 不接收信号；每 child 有8秒外层 context、Wait 与紧急清理，未增加生产5秒期限。原始详细记录含 PID、准确 binary、触发事件、SIGTERM、退出/Wait、elapsed、stdout/stderr、连接峰值与归零、listener重新绑定及 UNKNOWN 分类。

普通 CLI 的握手/输出/serving用例运行真实新 CLI artifact。`before-start` 子进程直接调用实际 app API，在 Open 返回处建立屏障；`writer-error` 子进程通过既有 run writer 建立屏障。这两个用例运行相同源码的测试 binary，而非宣称能在外部准确拦截 CLI 内部任意指令。另有进程不退出的部分 Open 取消测试，确认 Runtime/Adapter 的连接确由 owner 释放，而不只依赖 OS 退出回收。

原始路径：`darwin-cli{,-race}.out`、`darwin-first-listener-10.out`、`darwin-handshake-3.out`、`boundary-race-3.out`；Linux 对应 `linux-v2/{cli,first10,handshake3,app}.out`、`linux-supplement/{peer-examples,server}.out`。合成 HTTP 仅验证本地连接取消、应用/传输生命周期及进程时序，不是 Mongo/ES/OpenSearch 事务、故障、容量或生产资格。

## Linux 执行环境与保留的夹具失败

缓存且已核验的执行环境 image：`sha256:ef36debc338afa91481a64a435dbe23400f6252742ff63aaca45cfeedcaebdd9`，linux/arm64。其旧 `/client` 未作为产品受测代码；只把本轮新编译 binary 只读挂载到 `/tests` 并用准确 entrypoint 执行，不能称旧镜像含新修复。

所有容器：network=none、无端口/host网络、user65532:65532、cap-drop ALL、no-new-privileges、只读根、256MiB tmpfs、2CPU/1GiB、memory-swap=1GiB、pids256、有限1MiB日志。固定源码/产物/镜像/命令计划先存盘并hash，标签与ID独立核验。补挂的三个静态输入仅 `go.mod` 和两个已审阅的仓库 examples JSON，read-only；只做 Decode，不连接示例中的后端。

失败记录均不倒写：

1. `linux/result.json` / 首计划 `11a7522a…`：Docker local 日志默认压缩与max-file=1不兼容，在创建 task 前被拒绝，PID=0、StartedAt为零，没有用例执行。精确容器已 Stop/Wait/remove。
2. 新冻结 `linux-plan-v2.json` / `40b23fa5…`：仅设该容器日志 compress=false，资源/场景/期限不变。CLI整组、10次即时信号、两种握手各3次通过；额外 app全套19项通过，`TestCurrentPeerExamples` 因仅挂载binary而没有模块文件失败。该套结果仍 failed。
3. `linux-plan-supplement.json` / `c542abf7…`：保留原结果，只补静态fixture并单独运行失败的 examples 用例，再执行未运行的原定server组，均通过；没有跳过失败case、重复整套产品试到成功、缩小原边界或修改生产源码。

整个Linux窗口从首前置命令到最后精确ID回收178.41秒（约3分钟），低于10分钟；各清理远低于120秒，7个容器全部不存在，包含未启动的失败对象。每个命令 argv、开始/elapsed/exit 与 inspect 实际隔离参数均保存；最终 `container-cleanup.json` 独立按7个准确ID读回0残留。

## 全量检查与静态范围

固定 Go1.27.1，GOPROXY=off/GOSUMDB=off/GOTOOLCHAIN=local：

- `go test -count=1 ./...`、`go test -race -count=1 ./...` 通过，分别61.665s/65.607s。
- CLI/app/server/store 相关取消/关闭/背压/UNKNOWN/无重放等边界 `-race -count=3` 通过，100.951s，75个顶层PASS；未运行真实 DB integration。
- `go vet ./...`、`go vet -tags=integration ./...` 通过。
- Linux/macOS/Windows × amd64/arm64：`go build ./cmd/...`、`go build -tags=integration ./...`、`go vet -tags=integration ./...` 全部通过。其余架构/Windows只是静态证据。
- 额外误用的 `GOOS=linux GOARCH=amd64 go build ./...` 曾失败：既有 testcapacity 的main入口仅在integration标签下存在。`build-linux-amd64.{json,err}`保留；未改这个无关辅助包。后续按实际产品入口和显式标签补全编译，没有把该失败写成通过。
- 最初全量测试未开-v，Go隐藏了成功child日志；`full-verbose-plan.json`在原两套已通过后只各补一次详细输出，用于保存PID/Wait/释放证据，不是失败重试。两套均通过（62.953s/65.797s），结果见 `full-verbose-{test,race}.{json,out}`。

## 交付与清理

96个带详细记录的 Darwin child 已Wait并用PID查询无存活；Linux子进程均在容器内Wait，所有容器经Stop/Wait/ID-owner remove与最终准确ID查询确认消失。自建HTTP、socket、gRPC、临时配置均由测试owner关闭/移除；部分初始化测试还在父进程存活时观察backend连接归零。没有新网络/volume/后台fixture、EKS对象或系统定时任务。

最终交付只补本报告和生产资格清单；全部 Go/module 输入相对implementation逐项hash不变。最终main干净，准确delivery SHA、证据manifest、最后清理核验写入本阶段证据目录，并向统筹发送一次完成回调后停止。
