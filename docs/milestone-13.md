# M13：固定 golua 的有限安全可行性

2026-09-27；local main 基线 `383b4aa1149996f6ff580bff9c1662e664275163`。
**NO-GO：golua v0.3.0 不能按当前安全契约进入产品接线。**
这是有界调查完成，不是通用 ProgramTransform 完成或获准延期。
公共入口仍 `UNSUPPORTED`；未改 proto、生产 Go 代码、配置、数据库事务或 OCC。
测试 PASS 表示反例成功复现，绝不表示被测候选安全。

## 结论表

| required | demonstrated | failed | not yet proven |
| --- | --- | --- | --- |
| 确定性 CPU，含 helper/字符串工作 | VM 每 opcode 扣 fuel；无限循环、递归、table 构造停止，重复逻辑用量一致 | concat、rep、reverse 的 8 B / 128 KiB 输入消耗相同 fuel；编译仅按源码长度估算 | 全指令/比较/hash/所有宿主调用的最坏工作量 |
| 分配前预留，覆盖编译和 VM | 部分寄存器、upvalue、load 数组先 Require | 4 KiB 源 AST 的存活 Stats 切片下界 32 KiB，却在 4,097 B 限额内成功；table 先修改再检查；返回空 table/closure 记账 0；format 输出超额后才收账 | Go 分配器开销、GC 滞后、进程 RSS 的总硬界 |
| 有界编译、原 caller 取消和 join | 普通/token/常量/1024 层括号/长字面量/无效源码有限实测；子进程均 Wait | 已取消 caller 下仍成功编译；编译中取消早于返回仍成功；无 parser/IR 取消与深度检查 | 任意允许源码的栈/复杂度上界；进程内编译中止 |
| watchdog/安全 host/Close | 未声明安全的 host 被拒绝；单 owner Close 与 caller cancel 竞争测试 | 1 ms 额度内 20 ms 宿主工作仍可返回成功；时钟只在 CPU 采样点检查 | 无竞争的 VM 外部取消接入；所有支持操作及时停止；非合作 host 隔离 |
| 最小环境与确定性 | 不加载标准库；常见能力拒绝；每次新 Runtime，无跨 invocation 复用 | readonly bindings 不能阻止 guest 重绑 `_ENV`；诊断用 pcall 将子配额终止变为 false | 完整不可变全局/语言 profile、错误文本和所有旁路 |
| 通用 typed transform 与四种 action | Value 的极值/宽度/Missing/Null/空容器/opaque 字节透传；复用 checked 算术/歧义访问；原生 Lua 整数溢出会 wrap | naive userdata bridge 共享 host slice；Lua 数值不能直接作为 checked Value | 分支+对象数组更新、Replace/Keep/Delete/Reject、创建/no-op、checked float 转换、独立输出、cycle/shared/depth/node/output/message 的构造前拒绝 |
| 聚合 fuel/deadline，至多 5 次重算 | 嵌套 context 和新 Runtime 重算演示总剩余 fuel、原 deadline；不会重置全额 | fuel 本身未覆盖上述工作，因此总额度仍非安全保证 | Mongo 事务和 Search OCC 完整接线；本阶段未执行 |
| 可重复验证与部署范围 | 本机有限测试、race/fuzz、依赖隔离和交叉编译，见下方日志 | 已知安全缺口保留 | 非本机 native-run、产品 runtime、跨平台资源资格、生产资格 |

资源初筛失败后按任务要求停止完整 bridge 建设。没有为了补齐功能表而引入
transform registry、第二套 Value/codec、语言前端、VM fork 或进程 sandbox。
`internal/value.Increment` 与 integration-only Mongo RMW harness 仍是既有有限测试基础。
Search 尚无通过通用无损转换资格的 codec。通用程序仍是架构必需项。

## 固定版本、预算与复现入口

- 官方模块 `github.com/arnodel/golua v0.3.0`，tag commit
  `a129ef2c7b875c34e71e71b591d19af3b9c244f1`；模块 `go 1.18`，Apache-2.0。
  [发布说明](https://github.com/arnodel/golua/releases/tag/v0.3.0) 的 Go 1.27 构建修复已由本机验证，不能推导安全性。
- 本机 Go 1.27.0、Darwin arm64；硬件信息在 `.testdata/m13/environment.txt`。
  模块从 `proxy.golang.org` 下载、`sum.golang.org` 校验，随后验证离线运行。
  module sum `h1:4I8NslwaS9qyOQAH78i9BsiqTYMlCt0PHEkfZGrcogw=`；
  go.mod sum `h1:eGGXpsuFME3WM1f/9DUQx54oEzQrC/BwgyST+lj2q0I=`。
- 直接引入只在 `experiments/goluaprobe/*_test.go`。上游 go.mod 还声明 strftime
  与 REPL edit/terminal 依赖；本探针实际导入的 golua 子包只使用标准库，
  生产 `cmd/weir` 不可达 golua/GopherLua/experiments。
  原生产依赖版本均不变；go.sum 保留官方模块图所需校验项。
- 普通实验 profile 在首次运行前固定：源码最多 **16 KiB**，**100,000 fuel**，
  **1 MiB 逻辑内存**，**50 ms Millis**。数字不是 Go heap/RSS 承诺。
  专项缩小的额度：parse 4,097 B（源 4,096 B）；table 写入 1 B；
  format 32 KiB（512 个 `%99d`，固定最多 50,688 B 输出）；clock 1 ms/固定 20 ms host 工作；
  聚合 fuel 100、最多 5 次。没有在失败后调大额度或调低攻击量。
- 源 strings/测试 vectors 的预构造归测试 owner；`runChunk` 在源码复制前 RequireBytes。
  Runtime/bootstrap/env 的固定分配由测试 owner 持有，未冒称纳入上游额度。
  AST 专项单独测 parse，VM 专项单独测执行，helper 输入在测量前预构造；
  这些隔离量测不等于完成整体 owner 模型。
- 高风险源码/循环/helper 路径使用自有 test child：内部测试上限 4 s，父 `CommandContext`
  上限 5 s、WaitDelay 1 s，`CombinedOutput` 总是 Wait；正常全部自行退出。
  子进程仅保护开发机，不作为候选进程内隔离能力；没有遗留编译 goroutine。
  最大危险输入固定；未试图 OOM、栈耗尽或改变系统级限制。

```sh
GOPROXY=off GOSUMDB=off go test -count=1 -timeout=60s -v ./experiments/goluaprobe
GOPROXY=off GOSUMDB=off go test -race -count=3 -timeout=90s -v ./experiments/goluaprobe
GOPROXY=off GOSUMDB=off GOMAXPROCS=2 go test -run '^$' \
  -fuzz FuzzTypedIntegerTransport -fuzztime=3s -parallel=1 ./experiments/goluaprobe
```

## 固定源码审计

以下行号属于固定 tag，不是默认分支。全行号快照与 SHA-256 保留在
`.testdata/m13/source-audit.txt`、`source-sha256.json`，版本原文见 `golua-version.json`。
源码结论、实验观测和未证明项分列，不把 README 的配额 API 当作隔离证明。

| 边界 | 固定源码与结论 |
| --- | --- |
| parse/AST/IR/bytecode | [`runtime/lib.go:288–437`](https://github.com/arnodel/golua/blob/a129ef2c7b875c34e71e71b591d19af3b9c244f1/runtime/lib.go#L288) 先创建 scanner，再按 len(source) 预估 AST、IR、unit；每阶段 CPU 为 size/4。`parsing/parser.go:351–427,554+` 递归/append，AST/IR 编译器不接收 caller context、逐节点预算或最大语法深度。不是保守最坏推导；释放逻辑 AST 额度也不等于 Go 回收。 |
| load/导入 | `runtime/loadunit.go:25–114` 对部分 opcode/constants 数组预扣，递归 refactor/maps、Closure 本体和环境 cell 不是完整计账。`runtime/lib.go:440–480` 的 source-or-code 二进制导入路径不使用；探针只调用文本编译且拒绝二进制前缀，不提供动态 load。 |
| VM CPU | `runtime/luacont.go:108–139` 每 opcode RequireCPU(1)，含循环。`runtime/lib.go:181–190` concat 只扣输出字节不扣复制 CPU；table hash、重排与字符串比较也不是逐字节 fuel。有限循环终止不等于所有 opcode 工作有同一计量单位。 |
| table/closure/stack | `runtime/runtime.go:315–318` 在 `Table.Set` 完成后才 RequireMem；`table.go:73–79` 不论真实扩容只返回 16；`hashtable.go:184+,344+,643+` 存在 allocate/copy/rehash。`luacont.go:350–351` 创建空 table 无 Require，`closure.go:18–27` 只扣 upvalue 数组、不扣 Closure 本体。寄存器/continuation 部分预扣，非尾递归用内存额度终止，但没有独立批准的 stack/depth profile。 |
| hard/soft/zero | `runtime/runtimecontext.go:137–211` 中 0 为 unlimited、达到非零界即超限；soft 只 Due，不自动终止。`runtimecontextmanager.go:106–163` 嵌套取父剩余额度的较小值、Pop 汇总 CPU/Memory。调用方不能把耗尽后的 0 当下一轮额度。 |
| panic/recover | `runtimecontextmanager.go:299–306` 以 ContextTerminationError panic 终止；`thread.go:308–329`、`runtime.go:268–299` 收口。`lib/base/pcall.go:7–24` 再包 CallContext，可将子配额错误作为 Lua false 返回；实测尾调用 pcall 外层 nil error、fuel 99,999。未证实能无限获得额外 fuel；本环境不开放 pcall/xpcall/runtime context。 |
| wall-time/caller | `runtimecontextmanager.go:20,175–186,260–264` 时钟在累计 CPU 跨 10,000 ticks 时抽查；不是可抢占 watchdog。parse/compiler/API 不接 Go context；SetStopLevel 同步改普通字段且 HardStop 会在调用者 panic，不能从取消 goroutine 当作安全 interrupt。没有进行故意 data race 来伪造接入。 |
| host helper | `runtime/gocont.go:91–106` 仅核查声明 flags、扣一次调用 CPU，然后直接调用 Go；flags 不证明函数安全。`lib/stringlib/stringlib.go:167–241` rep/reverse 有有限内存检查但没有按工作量扣 fuel，reverse 另有临时/最终复制。`format.go:41–211` 估算 args/format/input 后调用 fmt.Sprintf；输出空间由 Lua wrapper 的第 26 行事后扣账；width cap 99 仍不足。没有给未审核 helper 补声明。 |
| GC/coroutine/错误 | `runtime/runtime.go:100–140,245–299` bootstrap 先于 quota、Close/GC 可能调用 finalizer；`thread.go:137–165,176–232` coroutine 有 goroutine/恢复通道、会转送 quota panic，完整 join 安全未验证，因此不开放。`regpool.go`/continuation pools 的逻辑 Release 不保证 heap/RSS 已退还。`format.go:149–167` 的 `%p` 和 base tostring 可暴露地址，未开放；`parsing/parser.go:28–45` 错误可拼 token，尚无产品 message 构造前界。 |

默认 whitelist 为空，只有测试 owner 注入的 typed userdata 和 readonly binding proxy。
唯一自行声明 compliance 的拒绝写 helper 返回预分配的固定错误，不读取参数、不做 IO/遍历/复制；
没有把任意 Go helper 标成安全。诊断用 string/base 库只在对应反例中单独加载，未装入默认环境。
file/network/OS/env/clock/random/import/debug/load/rawset/metatable/coroutine/非确定性遍历均缺席并有调用拒绝测试。
`_ENV` 可由 Lua 语法重绑，host binding 未被改写但全局语法限制并未成立，保留反例。

## 实验证据与限制

- `TestParseAllocationGap`：`a=1;` 重复 1024 次，logical memory=4,096，
  hard=4,097；存活 `tree.Stats` 切片单独即至少 32,768 B，不包含节点/token。
  Go TotalAlloc 约 609,552 B 仅为累计分配观测，不能写成 peak heap 或 RSS。
- `TestCompileShapesAndCancellationGap`：16,000 B tokens 初次累计分配 12,155,296 B，
  logical memory=32,000（含源码），fuel=12,000；正常/常量/嵌套/长字面量成功，未闭合字面量拒绝。
  已取消 caller 对编译无影响；外层 precheck 可以修复进入前取消，不能终止正在运行的编译。并发取消只修改 Go context，主调用同步结束后 join。
  “取消早于返回”作为每次观测打印，未将一次无重叠的调度结果冒充中途取消证据。
- `TestTableAllocatesBeforeQuotaCheck`：hard memory=1，SetTable 最终报超额时 key 1 已存入 42。
  `TestReturnedTableAndClosureAreUncharged`：编译与执行分开后，两个存活返回对象的执行 logical memory=0。
- `TestVMConcatAndHostHelperFuelGaps`：输入由 8 B 增至 131,072 B，输出 concat/rep 为 262,144 B；
  两档 fuel 分别恒为 concat 18、rep 35、reverse 36。库作者安全声明没有补齐工作量。
  `TestHostFormatAllocatesPastBudget`：32,768 B 限额内返回 50,688 B 字符串，逻辑 memory=15,360。
- `TestMillisPollingAndPcallQuotaResult`：1 ms logical wall 限制中，明确有限的 20 ms host 工作
  后只再 RequireCPU(1) 可成功返回；不伪称该 Go callback 被批准为 guest helper。
  初版测试曾错误预期 pcall 传播终止，原 FAIL 在 `initial-probes.log` 保留；固定源码解释后
  改为断言真实反例，没有修上游或放宽候选安全要求。
- `TestVMFuelMemoryAndFreshInvocation`：loop/tables 在 99,999 ticks 停止；递归 memory=1,048,575
  时被拒绝；随后新 Runtime 的有限程序成功。新状态不池化、不复用跨请求 VM；
  上游状态内部 pools 仍存在，不能称作 Go allocation-free。
- `TestContextLimitsAndAggregateFuel`：4 个 child 的第 4 次 30 ticks 请求被父总 100 拒绝，已用90。
  `TestFreshRecomputationsShareHostBudget`：全新 Runtime 重算两次成功，第3次耗尽总100，余8；
  至多5次，原 deadline不重置。只是账本演示；每轮内部取消/CPU覆盖缺口仍然阻断。
- `TestCancelAndOwnerClose`：30 次原 caller cancel 与 owner 执行/Close 交错，取消者不读写 VM；
  所有自有 cancellation worker join。race PASS 不等于已实现运行中抢占取消。

typed 试验仅透传 `internal/value.Value`，不导入 BSON/JSON/后端：Int32/Int64 极值、Missing/Null、
空容器、有序/重复字段、Bytes、ObjectId/Decimal128/binary/未知 Extended 字节均保持；
复用 Value 的 checked 算术/显式整数缩窄，混宽/溢出和重复字段按名访问拒绝。
Lua 自身原生整数为 int64、`MaxInt64+1` 回绕；普通 Lua 数字没有 Value 的宽度或 checked 合约。
userdata 无 metatable，guest 算术/concat/字段修改拒绝；Go 取出的 slice 却仍别名输入。
这些是原始字节向量的 VM 传递证据，**不是 codec 往返、opaque copy/move/delete 或完整 typed transform**。
没有自动 table-to-Value/数字-to-Value 写回入口；typed float 转换、四 action、输出 cycle/shared table
拒绝、深度/节点/输出/message 预检查仍未证明，未把非法 action 变为 Replace。
有限 fuzz 仅覆盖精确 Int64 userdata 传递，不把它说成任意恶意源 fuzz。

## 一次有限替代对照与后续边界

只对第二家族 Wasm 做固定 Wasmtime/wasmtime-go **v37.0.0** 官方源码/文档对照，
没有下载 native 引擎、构建、执行、添加依赖或改语言契约。
[Config fuel/epoch](https://github.com/bytecodealliance/wasmtime/blob/v37.0.0/crates/wasmtime/src/config.rs#L504)
区分确定性 wasm fuel 与非确定性 epoch 中断；fuel 不能自动给 Go helper 工作计价。
[ResourceLimiter](https://github.com/bytecodealliance/wasmtime/blob/v37.0.0/crates/wasmtime/src/runtime/limits.rs#L19)
明确不覆盖全部 Store 内部和 embedder 分配；线性内存上限不能回答编译/总进程内存问题。
[Go binding](https://github.com/bytecodealliance/wasmtime-go/blob/v37.0.0/README.md#installation)
使用 CGo/Rust；本次未核验其所有 native 产物/平台，不声称六平台支持或运行通过。
这是对已发现 CPU/全内存缺口的有限对照，**不是第二个合格候选**，也不是“Wasm 一定无解”的结论。

小范围可保留的办法是：严格不加载未审核库、每次新状态、typed Value 保留标签、
caller 持有总剩余预算并在耗尽时拒绝。但这些办法不能修复核心 parse/IR/VM 分配与取消。
修补 golua 至少涉及编译器逐步资源/深度/取消、table 分配前计划、closure/table 完整计账，
以及字符串/比较/hash/host 的工作量检查；**不是一个接入开关或禁用单个 helper 可闭环**。
最小后续建议是先由统筹评估并请用户决定是否授权上游/维护 fork 的有限安全修补范围，
或改变程序输入/语言与隔离约束后再评估一个固定方案。不能擅自换成 Wasm 字节码、
外部进程、可信程序，不能把 ProgramTransform 从必需项移除。
本阶段不安排、不执行下一阶段，也不建议直接进入生产 runtime 接线。

## 仓库验证与交接

最终验证结果和命令保留 `.testdata/m13/commands.md`、各原始日志；初版失败日志保留。
以下均使用 `GOPROXY=off GOSUMDB=off`，test/race 均非缓存；生产代码与测试代码为交付版本。

| 验证 | 结果/原始日志 |
| --- | --- |
| 全仓 `go test -count=1 -timeout=180s ./...` | PASS；`test.log`，命令墙钟 60.234 s |
| 全仓 `go test -race -count=1 -timeout=180s ./...` | PASS；`race.log`，63.275 s |
| `go vet ./...` / `go vet -tags integration ./...` | 均 exit 0；`vet.log` / `vet-integration.log` |
| 候选 `-race -count=3` | PASS，51 个顶层 Test（17 × 3）；`final-race-probes.log`，三轮都观察到编译取消早于成功返回 |
| typed scalar fuzz，3 s / GOMAXPROCS=2 / parallel=1 | PASS，70,320 次；`final-fuzz.log`，不是 arbitrary-source fuzz |
| Linux、Darwin、Windows × amd64/arm64 | 6/6 `CGO_ENABLED=0 go test -c` PASS；`build-results.json`、逐平台日志；无跨平台执行声明 |
| production/probe 依赖图、proto/生产源码无改动、AST 风格、diff check、module verify | 均 PASS；`dependency-boundaries.json`、`style-final.log`、`module-verify.log`、`commands.md` |

交叉编译不是 native-run；本阶段不启动 Mongo/Search/容器，也不重跑未修改后端的真实矩阵。
源码/版本/命令/初版失败与最终日志均在 `.testdata/m13/` 保留；提交后 SHA 与清理状态记录于
该目录 `delivery.json`、`cleanup.json`。实验子进程均退出、取消 worker 均 join，不留后台测试。

M12R 在本阶段基线已由统筹独立验收：本地 raw/dial/closing owner、分层远端尾部、
replacement 两 Local 全生命周期的固定本机有限范围通过；原 M12 FAIL/FAIL/PASS 历史保留。
本地 C+1 不是 DB accepted/取消后远端工作的无条件总上限。统筹报告位于
`/Users/liran/Projects/Codex-Projectless/2026-09-26/referenced-chatgpt-conversation-this-is-an/outputs/weir-m12r-acceptance.md`。
没有从该验收推导 Kubernetes、复制切换、容量/soak、六平台或整体生产资格。

只提交本地 main。未 push、PR、发布、部署、读取已有秘密、启动其他执行者或定时任务。
所有测试/子进程停止并完成清理核对后，向获授权的统筹聊天回报本聊天 ID、交付 SHA、
NO-GO 与证据，随后停止写入和测试。
