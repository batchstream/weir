# M19R：Darwin 当前 physical footprint 与有限原生验证

2026-09-28，local main。基线 `cc7236295e4ec71f19555ae4e24f0cee6ecd168a`，
执行聊天 `01a0e467-3e11-73c1-9e48-f16d3e6fbaa9`。原 [M19 调查](milestone-19.md)
及未实现历史保留；本报告单独记录补救。不是六平台或整体生产资格。

## 固定依赖与最小平台边界

统筹明确批准固定 purego v0.10.2 的公开 API，接受上游维护的内部 fakecgo/runtime
linkname。这是**依赖维护边界例外**，不是所有 private ABI 获准或依赖无风险。
Weir 没有复制/修改 runtime ABI、raw trap、trampoline、callback、fork 或任意 FFI 服务。
固定 commit `7f6f1382ade268f99d75e03a6b6447c2be53279c`；官方 archive SHA256
`f3606dbb92f4639b0529efe59ba34715d144d519e228b86886bdc990e862646f`。
通过 proxy.golang.org 和 sum.golang.org 取得正式 module：
`h1:W809HbnvzAxgdm+aOvlSekrM16wGCdT/e76+9tS7gzE=`，go.mod checksum
`h1:iIjxzd6CiRiOG0UyXP+V1+jWqUXVjPKLAI0mRfJZTmQ=`。
Apache-2.0、Go 派生 BSD-3-Clause 和归属说明纳入 `packaging/licenses/`，
打包器在 Darwin 归档携带原文；module graph 与 actual linked inventory 分开。

只在 `memory_darwin.go` import bridge，通过 Dlopen/Dlsym/RegisterFunc 固定绑定
`/usr/lib/libproc.dylib` 的 `proc_pid_rusage`，只用自身 PID、V0。
一个 sync.Once 绑定及一个库引用保留至进程退出，所有 Guard 复用；失败时释放已取得
引用并保存错误，不在 init 无条件 panic，不每 tick/每 Guard 重复 Dlopen。
不读取 errno，只用返回码作保守 unknown，避免跨线程取 errno。
固定无指针 V0 buffer 用 typed escaping pointer 同步传递，调用后 KeepAlive；
C 不保留指针、不回调，不用 uintptr 长期保存 Go 内存。
SDK 和 Go 检查 size=96、align=8、footprint offset=72；拒绝非零返回码、零值和
超过 MaxInt64 的异常 ledger 数值。失败 bytes=0、process_valid=false、unknown=true。

Darwin 采样当前 physical footprint，source=`darwin_phys_footprint`，单位 bytes。
它不是 RSS、峰值、虚拟空间或 Go heap；压缩内部内存等计账范围沿用 M19 的 Apple
ledger 核验，不与其他源相加。Darwin cgroup 为 not_applicable，预算为显式软准入阈值。
Windows 仍为 Go fallback，不能称作当前 OS 内存资格。Linux parser 继续可移植测试。
共享 `proc.go` 仅把无 `/proc` 的分支交给平台直接调用，没有 provider registry。

单 Guard、100ms、80/70、水位迟滞、首次观察先于准入均保持。
无效 OS 样本不能凭低 Go 值恢复；后续有效低位可以清除临时 unknown。
metrics 仅读 Snapshot，固定新增一个 source series，inactive sources=0；最大图现为
2058 series。没有动态 PID/path/error labels 或新采样线程。
同步内核 API 不可取消；正常短测中 Run/Close 可及时 join，但不能承诺内核挂死时
也能硬中止。不以丢弃 goroutine 并持续创建新调用伪装有界。

## 预冻结 profile 与证据分层

证据根 `.testdata/m19-remediation/`，`commands.jsonl` 逐条记录命令、cwd、有限 env、
exit 和耗时；原始日志不覆盖。`pressure-profile.json` 在首次负载前冻结：
256MiB 预算、基线≤128MiB，额外 mmap≤256MiB、最多一个施压进程；
高位85%、中位75%、低位释放全部自有 mmap；中位保持300ms；2s 收敛；
同信号 oracle 容差8MiB；Guard取消1s、app/CLI关闭3s；测试总timeout120s、三轮。
每次额外分配根据测得 baseline 计算整 MiB 页数，触及每个 OS page，不随失败修改
预算、目标档、水位、容差或关闭期限。SDK helper 只观察创建它的 test 父进程，输出
owner PID 与 footprint；测试核验 PID。它是独立 clang 可执行文件，没有把 CGO
链接进 CGO0 test executable。发布二进制没有 debug 分配入口。

环境：macOS26.6.2/25G83，Darwin25.6.0/xnu-12377.161.14~5，Apple M2/Mac14,2，
8 CPU、25769803776B RAM；repo Go1.27.1，SDK27.0。SDK 版本不等同于运行 OS。

最初 CGO0 可行性在分钟内完成：每轮8,000次并发调用/GC，非法 flavor=-1、
随后有效 V0，三次独立执行通过。该探针没有压力，不冒充后续 Guard/app 资格。
默认测试不施压或启动数据库；所有 native/app 压力仅在 integration + 显式 opt-in。

| 路径 | 三轮实测 |
| --- | --- |
| CGO0 Guard test executable | 最终高位104.366/108.853/105.758ms；释放恢复83.422/59.920/59.728ms；同信号差值保持8MiB内 |
| CGO0 app test executable | 高位52.504/76.618/76.187ms；恢复81.837/55.868/81.147ms；重复Close4.654/1.410/1.657ms |
| CGO1 Guard `-race` | 最终三轮通过；真实native错误/恢复、GC/并发、32次重复初始化及取消Run |
| CGO1 app `-race` | 修正测试基线预热后单独三轮通过；高位55.428/80.721/79.533ms，恢复55.765/30.177/55.482ms |

CGO0 Guard 与 app build info 独立保留，不能用 CGO1 race 为 fakecgo 背书。
最终Guard两条路径各三轮均在join后确认goroutines=2→2、file descriptors=5→5；
全部自有mmap释放，32次重复初始化不改变唯一libproc handle。编译escape日志也确认
V0 usage buffer moved to heap，没有临时栈地址越过同步FFI寿命。
Guard 单元测试的无效/溢出/首次unknown/恢复是纯决策反例；真实非法 flavor 在 opt-in
native test 直接调用系统 API，不能说成真实 OS 瞬时故障注入或加载失败已实测。

应用使用自有 Mongo8.0.32 TLS/SCRAM fixture 和已有 wire proxy。两名连续 Read
生产者含取消；高/中位新 mutation 得 ResourceExhausted、DB refused ID=0；
低位 Read/Mutate/Bulk 恢复。真实已确认写入后丢 ACK 得 UNKNOWN，DB count=1、
wire update恰一次、无隐式重放；重复Close并确认proxy sockets=0。

### 保留的失败与修复

`app-native-race3.log` 第一轮中档 footprint=216007664B，超过80%阈值214748365B，
测试按冻结门槛失败；另外两轮通过不能掩盖这次失败。没有 race detector 报告或
bridge ABI/GC 失败。根因是首次诊断/持续流量未纳入基线，race 运行时的新增工作集
使目标压力偏移。`app-before-warmup.go.txt` 保留原测试。修复将同样诊断/流量预热
500ms并GC后再测baseline，目标档和阈值全部不变；最终CGO0与CGO1各重跑三轮。
最初一次临时验证命令有 Python unmatched-parenthesis 语法错误，未执行任何测试；
修正命令后继续，没有改产品或测试门槛。

第二个测试错误在 `artifact-darwin-cgo0.log`：准确制品已完成读写、UNKNOWN及在途
SIGTERM，但新增的“1MiB预算启动闭锁”子步骤违反产品已有最小64MiB配置下限，
启动正确拒绝 `invalid process bounds`。修正仅删除这个无效测试前提，以正常预算
再启准确制品验证正常关闭；没有修改配置下限或降低压力档。原测试源码和日志保留。
**准确归档没有执行mmap高/中/低；该强压证据属于CGO0真实app装配test executable。**
不把归档层级的压力覆盖写成已通过，也不在发布binary留下debug分配入口。

## 准确制品、回归与供应链

实现/制品source SHA为 `484e4bddeeef9f76c0de407b70c4e76e95e098d6`。
后续测试修正/补充为 `eba3d1d221842956e8614033c4f65db681b9dd54` 和
`87e83d96139e13cc09caebf8931d67b8d3defd5f`；末尾文档交付不重新定义制品来源。
`product-inputs-final.json` 核验69项产品打包输入与source逐字节相同。

`scripts/package.py --output dist/m19-remediation` 从干净实现提交导出，使用两个
独立source/cache，CGO0六目标binary与六归档全部两次hash一致，耗时123.849s。
未启用OCI，不把历史image重新标为本次制品。`first/receipt.json` SHA256为
`ea7816a1fe9861ca574c41cb5abc048e0576b38dc47dab61ff3f76976942fc07`。

| 制品 | SHA256 |
| --- | --- |
| Darwin arm64 binary | `5711d45615965407327b587cd998d014811f57f89c4dbf4c1c0aadea315c995c` |
| Darwin arm64 archive | `621ee2cef178718f7ab27bcb7f026e472ab4d05157b5743b54469131329d41b4` |
| Darwin amd64 binary | `eaa2bcd9302dc646a61ae6a82e44cbc8424afca5a47312891814f5831b07a0bd` |
| Darwin amd64 archive | `2edd53a8a59479586362a945f9bc035b53211a533d1a3cba9ebeaef67b78155c` |
| Linux arm64 binary | `f8091e885879f9d955077f9771921ebc1b7a5673c660d387ea764be13bd59d44` |

Darwin arm64从准确归档提取、hash确认后启动（PID11068），验证footprint/source、
其他source=0、Read/Mutate/Bulk、取消stream、真实ACK丢失UNKNOWN/无重放。
被proxy暂存的在途mutation在SIGTERM drain中仍完成，Wait=7.531ms；第二实例正常
SIGTERM/Wait=1.221ms。CGO0测试driver也有独立build info；它不充当被测产品。
最大合法diagnostics图通过，实际2058静态series。

Linux7.0.12-linuxkit/aarch64/cgroup-v2、8CPU/8319770624B VM复用原Guard native
三轮（mmap≤112MiB，原128MiB预算、80/70、2s/8MiB/16MiB不变）。高位分别
89.077/48.948/83.201ms、恢复78.782/99.883/71.945ms。准确Linux arm64归档binary
在512MiB/2CPU/96PID/read-only容器验证Read/Mutate/Bulk、RSS=24494080B、
独立smaps=24584192B、cgroup有效且Darwin source=0，SIGTERM/Wait=5.860ms。
后端是自有Darwin Mongo8.0.32混合环境；原Linux Mongo内核阻塞仍在。
证据 `.testdata/weir-m19r-119c55464efd/`。派生harness差异保留于
`linux-harness.diff`：复制并校验准确binary、固定source，Guard三轮/app观测一轮；
复用原独立清理逻辑。没有运行原416MiB同组压力helper，未改变它的冻结门槛，
该强压历史仍只引用M14R；本轮无进程额外分配超过256MiB。

| 默认/静态回归 | 结果 |
| --- | --- |
| `CGO_ENABLED=0 go test -count=1 -timeout=180s ./...` | PASS，61.592s |
| `CGO_ENABLED=1 go test -race -count=1 -timeout=180s ./...` | PASS，64.562s |
| default / integration vet，Linux arm64 integration vet | 全部PASS；测试文件补充后重做相关vet |
| Guard portable三轮race | PASS，含M14R瞬时unknown、相关身份及无关mount反例 |
| app/server diagnostics/overload/admission三轮race | PASS，12.777s |
| fixture清理离线测试 / package离线测试 / docs | PASS |
| gofmt / 新改代码style / diff whitespace | PASS |

全部Go回归使用repo Go1.27.1、GOTOOLCHAIN=local、GOENV=off、GOWORK=off、
GOPROXY=off、GOSUMDB=off；只在最初固定新module获取时使用官方proxy/checksum。
每条命令与env见commands.jsonl；CGO0/CGO1明确分列，旧失败日志未覆盖。

固定archive和官方module有153个重叠文件逐字节对齐；原M19审计输入只读复用。
两Darwin链接清单和Go nm都含purego，四个非Darwin目标都没有其linked module或
symbol。`otool -L`仅列系统库/framework；libproc通过固定系统路径Dlopen，不是
用户需要另装的dylib。实际运行证明该系统路径可用，不以otool输出冒充动态加载证据。

有限新扫描在 `dist/m19-scans/`：重新取得官方Go完整index及当前graph对应278份OSV，
2026-09-27T20:02:31Z完成快照，DB modified=2026-09-24T20:07:49Z；没有更改旧DB。
复用已核SHA的govulncheck1.8.0、Syft1.52.0、Grype0.119.0；Grype冻结DB built=
2026-09-27T06:30:30Z，本次status valid=true，保留hash/120h age校验、禁止自动更新，
未伪装过期库。空HOME/显式配置、无远端许可扩展、无Docker auth、无ignore/VEX。

两架构source+binary的四份govuln原件均仅有原x/crypto v0.55.0三个module级
GO-2026-5932/6354/6355，和M17一致，没有package/function reachability finding。
实际两target依赖图不含受影响SSH/OpenPGP包。两份Grype各3个匹配（2 High、1
Unknown），不抑制；purego无新增匹配。这不是“0漏洞”或全场景安全证明。
两份Syft各23组件并包含正确purego版本；两份CycloneDX1.6严格schema均0错误。
扫描只涉及新Darwin制品及固定依赖，没有重新调查外部Java数据库。

## 清理与剩余资格

15个自有TLS fixture均只余owner、mongod及轮转日志，生成材料/data已由原helper
清理；两个Linux容器和唯一network逐项确认absent，宿主Mongo PID11552已Wait/退出。
原生Guard/app/制品进程均结束，proxy sockets=0，全部测试session已完成。
`cleanup-audit.json`记录复核；审计脚本曾过严地拒绝合法轮转日志和Docker network
专用not-found文本，修正仅涉及证据分类，不修改资源或把daemon错误当absent；
原问题记录于cleanup-audit-errors.json。静态源码、owner、原始日志、探针及准确
制品保留用于独立复核，没有删除旧阶段证据。最后只更新报告并做docs/diff检查，
本地main干净后回调统筹并停止所有写入/测试；不push/PR/发布/部署或开启timer。

认证排除、通用ProgramTransform首版延期/UNSUPPORTED保持。M18有界调查已获统筹
接受，OpenSearch安全为external/upstream-evidence blocked；不重复Java调查、
不升级DB。Darwin amd64缺 native runner，Windows、其他架构、完整OCI/Kubernetes、
复制故障、容量及各平台24h soak仍未资格。本机三轮短测不改变这些门槛。
