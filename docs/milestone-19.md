# M19：Darwin 当前 OS 内存边界调查，产品接线阻塞

2026-09-28（Asia/Shanghai）。基线为干净 local main
`21c48e12693a13fa13e8fbf2ea1c04f0cdb479b7`，执行聊天
`01a0e456-92b1-7d53-bcf6-d9dbd83098b7`。

**M19 当前内存保护没有实现，也没有取得原生资源资格。** 有界调查确认公开
libproc API 可用，但未找到同时满足 CGO0 与禁止私有 runtime ABI/linkname 的
获准接线方式。依任务的停止条件，未修改 Guard、metrics、应用装配、模块依赖或
打包契约。此结论是约束阻塞，不是证明 macOS 无法采样，也不是认定 purego 有漏洞。

## 公开 API 与当前信号

本机 macOS 26.6.2 / 25G83，Darwin 25.6.0 / xnu-12377.161.14~5，
arm64 Apple M2 / Mac14,2，8 CPU、25769803776B RAM；Go1.27.1，
Command Line Tools SDK 27.0、Apple clang 21.0.0（clang-2100.3.34.2）。
SDK 版本与运行 OS 不同，分别记录，不将 SDK 版本当作实际测试 OS。

本机 SDK 的 `libproc.h` 声明 `proc_pid_rusage` 自 macOS 10.9 可用；成功为 0，
失败为 -1 并设置 errno。固定 Apple 源码为 tag `xnu-12377.1.9`、commit
`f6217f891ac0bb64f3d375211650a4c1ff8ca1ea`，不是本机内核的精确源码版本，
只用于公开 ABI/计量定义核验。本机 SDK 编译与运行探针另行验证实际接口：

- [libproc 声明](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/libsyscall/wrappers/libproc/libproc.h)、
  [实现](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/libsyscall/wrappers/libproc/libproc.c)：
  公开入口封装内核 proc info 调用；不据此在 Weir 中直接调用 raw trap。
- [resource.h](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/sys/resource.h)：
  V0 已包含 `ri_resident_size`、`ri_phys_footprint`；没有必要为一个当前内存数值
  使用最新 V6。V0 的 SDK size=96、alignment=8，两个字段 offset=64/72；V2 size=160。
  flavor 决定 copyout 长度，API 不返回结构长度，不能把返回 0 当作长度。
- [fill_task_rusage](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/osfmk/kern/bsd_kern.c)：
  resident 来自当前 phys_mem ledger，footprint 来自当前 phys_footprint ledger；
  都不是 lifetime peak，也不是 Go heap 或虚拟地址空间。
- [ledger 定义与单位](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/osfmk/kern/task.c)：
  两者为 bytes。footprint 包括压缩内部内存、非易失 purgeable、page table 等计账；
  resident 不表示压缩后的全部内存责任。若后续接线，倾向选 current physical footprint，
  source 必须明确为 footprint，不标作 RSS、不与 RSS 相加，也不与 `ps` RSS 作等值比较。

候选只查自身 PID，不需枚举其他进程。errno 必须在调用所在 OS 线程读取，或由 C
wrapper 当场返回；Go goroutine 迁移后另读 errno 不可靠。C 探针在同一 C 调用线程
保存 errno。产品的零值/溢出、ABI 布局断言、加载失败、安全拒绝与恢复仍待实现，
本次无负载 SDK 调用不能替代这些验收。

## CGO0 bridge 边界

已缓存的 `golang.org/x/sys v0.47.0/unix` Darwin 接口没有 `proc_pid_rusage`、
当前 task footprint 或 resident 的公开封装。`Getrusage` 的 maxrss 是最大值；
`SysctlKinfoProc` 的公开结构不是当前 resident/footprint 输出接口，不能解引用其内核指针。
没有将这两项当成当前内存替代源，也没有引入完整监控框架或每 tick 子进程。

只审计一个成熟 CGO0 bridge 候选：`github.com/ebitengine/purego v0.10.2`，
固定 commit `7f6f1382ade268f99d75e03a6b6447c2be53279c`。
官方 source archive SHA256 为
`f3606dbb92f4639b0529efe59ba34715d144d519e228b86886bdc990e862646f`。
未加入 go.mod/go.sum、未执行该 bridge、未生成含它的 Weir binary。

其公共 Dlopen/Dlsym/RegisterFunc API 和 Darwin 两架构支持有实际跨平台边界价值，
不等同于整个监控框架；该版本 go.mod 无额外 require。许可证主体 Apache-2.0，
复制的 Go runtime 文件另有 BSD-3-Clause 条款。未因许可或依赖体积否决它。
实际阻塞点在 CGO0 实现：

- [nocgo.go](https://github.com/ebitengine/purego/blob/7f6f1382ade268f99d75e03a6b6447c2be53279c/nocgo.go)
  在 `!cgo` Darwin 路径导入 `internal/fakecgo`，设置 runtime 初始化/线程相关函数。
- [go_runtime.go](https://github.com/ebitengine/purego/blob/7f6f1382ade268f99d75e03a6b6447c2be53279c/go_runtime.go)
  linkname 到 `runtime.cgocall`；
  [iscgo.go](https://github.com/ebitengine/purego/blob/7f6f1382ade268f99d75e03a6b6447c2be53279c/internal/fakecgo/iscgo.go)
  改写 `runtime.iscgo`。README 列明复制的 runtime/cgo ABI 文件，Darwin fakecgo
  使用自己的 G/ThreadStart 布局与线程 trampoline。
- 本地 Go1.27.1 的 runtime/cgocall.go 与 cgo.go 明确将相关符号称作 internal detail，
  同时为兼容 purego 保留类型签名。后者是实际兼容性证据，不能遗漏；但并未使其成为
  Go 公开 API，也未自动解除本任务禁止私有 ABI/linkname 的约束。

“允许很小的成熟公开 API bridge”与“不得使用私有 linkname/复制私有 ABI”在此候选上
需要明确边界解释。此次将禁令应用到被引入的实现，而非仅检查 Weir 自己的源码，
未自行豁免间接依赖。这不是宣称所有 CGO0 方案在理论上都不可能。

可审阅的最小决策只有两条：允许固定版本成熟 bridge 的上述内部实现（Weir 自身仍
不写私有 linkname/raw trap），或允许仅 Darwin 使用 SDK/cgo 的构建契约。
前者仍须完整 ABI/错误/取消/原生三轮/准确制品/依赖审计；后者仍可交付链接系统库的
单一 binary，但改变 CGO0 跨打包条件、需要受控 Apple SDK 构建环境。
二者本次均未选用。没有提出侧车、用户自行编译、复制 FFI 或定制 Go runtime。

## 预先冻结的探针与原始结果

证据根为 `.testdata/m19-feasibility/`。`profile.json` 在首次运行前冻结：
只做 API/layout/build 可行性，最多一个无负载探针进程，额外施压分配为 0，
运行 timeout=3s、单次 compile timeout=30s；不启动后端、不改系统。
压力档须在 bridge gate 通过后另行冻结，本次没有进入该门槛，不能称作三轮资源验证。
首次核验 19:28:25 UTC，19:33:50 UTC 决定停止产品接线，在约 30 分钟调查上限内。

| 探针 | 结果及边界 |
| --- | --- |
| SDK C native arm64，V0 | exit 0；resident=1327104B，footprint=901408B；布局断言通过 |
| 同进程非法 flavor=-1 | result=-1，errno=22/EINVAL；只证明真实 API 错误，不是 Guard 故障注入 |
| 同进程随后 V2 | result=0，errno=0；resident=1327104B，footprint=901408B |
| 独立 Go/cgo footprint probe，CGO1 | 构建/native exit 0；footprint=2671072B；build info 明示 CGO_ENABLED=1 |
| 同一 Go probe，CGO0 | 预期 exit 1：`build constraints exclude all Go files`；原日志保留 |
| SDK linked libraries | 仅本机系统 `/usr/lib/libSystem.B.dylib`；不是自带 dylib 或产品 CGO0 证明 |

C 与 Go/cgo probe 是不同进程，内存数值不能互作误差比较。未制造压力或观察压缩，
未与独立 oracle 对齐产品值。SDK 真实错误后恢复不能写成 Guard startup unknown、
瞬时采样恢复、准入高/中/低、Run/Close 或 SIGTERM 已通过。

可复验输入：`sdk_probe.c` SHA256
`8bce7ad2e96aabb4383e010c2349351a77de54934ac289a28ba6eef9ec8a0767`；
`cgo-probe/main.go` SHA256
`46ed7bf5c28afc555c93cb920fd5e2f0c5a60a99078e9a7d37fd0771a8008422`。
SDK binary SHA256 `df62313e19797ddbf3328eed9c81f6c0899c9832802dae777ca3037928c367e4`；
cgo probe SHA256 `b155da5d35c5fe903f84d5ce252898581494f2cee0d34d5f67657183934a5374`。
Go probe 的 build info 可继承外层 repository VCS 基线；它是未进入产品的隔离源码，
其身份应按上述输入/输出 hash 识别，不能称为基线提交中的 Weir 归档。

`probe-results.json` 保存命令、cwd、expected/actual exit 和时间；
`downloads.json` 保存官方 URL、固定版本和原始 SHA256，`sdk-headers/` 保留实际头文件，
`decision.json` 明示停止点。没有抓取移动 main 作为唯一证据。

## 产品状态、回归与交付边界

没有 M19 实现 SHA 或 M19 产品归档 source SHA。当前产品代码与基线完全相同；
历史准确归档 source 仍为 M17 的 `1bb93fd32e18eb79ba25802809eddfe877b28a4f`，
此处只是引用既有记录，不是重建/重跑该制品。最终提交为调查与状态文档交付。

Darwin 仍走 `go_sys_minus_released`，其既有 `process_valid=true` 不能理解为 OS
采样成功。OS 采样失败安全拒绝/恢复的 M19 目标未实现。Linux RSS/cgroup、80/70、
100ms、关闭预算、UNKNOWN/无重放完全未改；Windows 当前 OS 内存资格仍缺失。
无需因文档更新重新做六目标/双归档或 Linux 原生回归；没有将旧证据改称本次通过。
未新增 linked bridge，故未运行新的依赖安全扫描，不重复 M18 Java 数据库审计。

离线默认 test/race/vet、integration vet 和 Guard 三轮 race 的命令/exit/耗时保存在
`validation.json` 与同名日志；所有命令使用 repo Go1.27.1、GOTOOLCHAIN=local、
GOENV=off、GOWORK=off、GOPROXY=off、GOSUMDB=off。这些回归验证原有行为，
不能补足本次未实现的 native/app/artifact/soak 资格。

| 本次离线回归 | 结果 / 命令总耗时 |
| --- | --- |
| `go test -count=1 -timeout=180s ./...` | PASS / 70.336s |
| `go test -race -count=1 -timeout=180s ./...` | PASS / 77.640s |
| `go vet ./...` | PASS / 2.270s |
| `go vet -tags integration ./...` | PASS / 2.233s；仅静态检查，不是启动后端 |
| `go test -race -count=3 -timeout=60s -v ./internal/overload` | PASS / 3.264s；包含 Linux portable 文件反例 |

末尾手工 docs 检查曾继承宿主 `GOROOT=/usr/local/go`，导致 1.27.0 标准库与
1.27.1 compiler 版本不一致而失败。原失败保存在 `docs-inherited-environment-failure.log`；
恢复与上述回归一致的有限环境白名单、去除继承 GOROOT 后，最终 docs 测试 PASS。
没有清空系统缓存、改工具链或修改测试门槛。全仓回归从一开始就使用该有限环境，
不能将此环境错误误报为产品代码回归，也不能隐去失败。
`product-inputs.json` 逐字节核对 64 个打包输入与基线一致。

M18 的有界调查已获统筹独立验收；OS2.19.6 OpenSearch 安全门槛仍为
external/upstream-evidence blocked，3.8.0 未启用、不升级或定制数据库。
Weir 自身认证排除、ProgramTransform 首版延期保持不变。其他架构、Kubernetes、
复制切换、参考容量与各平台至少 24h soak 门槛不变。

全部探针由带 timeout 的同步 subprocess 执行并 Wait，未启动压力/后端/容器/网络资源。
只保留本阶段 owner、源码、日志、固定上游证据和两个探针制品；不清理旧阶段目录。
结束前核验本阶段会话全部退出、main 干净，再按授权回调统筹并停止；
不启动 timer、不创建下一阶段、不 push/PR/发布/部署。
