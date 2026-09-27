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
| CGO0 Guard test executable | 高位101.118/106.528/108.448ms；释放恢复87.981/59.049/59.745ms；SDK差值最大131072B |
| CGO0 app test executable | 高位52.504/76.618/76.187ms；恢复81.837/55.868/81.147ms；重复Close4.654/1.410/1.657ms |
| CGO1 Guard `-race` | 单独三轮通过；真实native错误/恢复、GC/并发、32次重复初始化及取消Run |
| CGO1 app `-race` | 修正测试基线预热后单独三轮通过；高位55.428/80.721/79.533ms，恢复55.765/30.177/55.482ms |

CGO0 Guard 与 app build info 独立保留，不能用 CGO1 race 为 fakecgo 背书。
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

## 交付验证与资格边界

实现提交之后才做准确归档，结果与 SHA 在本节后续补齐；当前已完成上述CGO0/CGO1
分层原生测试。准确归档不得用 test executable 替代，也不能把无 debug 入口的
发布 binary 说成做过 mmap 高/中/低压力。Linux 混合 backend 限制与原M14R保持。

认证排除、通用ProgramTransform首版延期/UNSUPPORTED保持。M18有界调查已获统筹
接受，OpenSearch安全为external/upstream-evidence blocked；不重复Java调查、
不升级DB。Darwin amd64缺 native runner，Windows、其他架构、完整OCI/Kubernetes、
复制故障、容量及各平台24h soak仍未资格。本机三轮短测不改变这些门槛。
