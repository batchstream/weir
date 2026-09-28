# M31：正式 qualification observer 的有界完成握手

2026-09-29。基线 `b7b7fd213b7e65449b1ed3df96df29a74a7a91e2`；实现提交 `3cf2c9416da0d4858a20e92d661aabdad6f09a78`。仅本地 main 实现、测试和提交，没有 push、Actions、镜像拉取/构建/发布、EKS/aws、数据库服务、业务请求或负载。

**三个实际 Python 消费者已接入完整校验后的 EOF 确认，正式 Go 完成函数通过 Darwin 和一次受限 Linux arm64 真实管道验证。正式资源仍为 partial/not-qualified，timing=not-run，candidate=null。** M30R7 已验收的是固定三臂诊断的有限缓解，M31 不将它或合成数据升级为资源资格；M30R6 reset 的具体原因仍 unknown。

## 完成和取消合同

`internal/testutil/testcapacity` 的 observe 模式在正常完整采样后交出 stdin 读取权、写出原有 `observer_end`，再等待接收端 EOF。四秒绝对截止从 terminal 编码前开始，包含 terminal 写入，处于原关闭窗口内。只有 terminal 写入成功、收到 EOF、没有取消/错误且尚未到截止才返回成功。无 ACK、非法控制字节、信号、采样/计数错误、读写失败明确非零；没有第二条 stdout ACK 回执、尾部 sleep 或自动重放。

采样期间只有一个 stdin reader。完成时用已有 pollable pipe 的 deadline 中断并 Join 它，再用无竞争的非阻塞读取检查已到达的提前 EOF/数据；由完成函数接管读取。这样接收方读完 terminal 后、Encode 尚未返回时关闭 stdin 的合法交错不会被采样 reader 当成提前取消。信号触发的读中断回调也在返回前 Join。所有权和错误原因由 context 贯穿，关闭拥有的 FD 后 Join。

共享 `scripts/observer_completion.py` 只服务 qualification observe：持续排空 stdout/stderr，验证 identity、目标/observer 身份、预定 role/count、连续 sequence、sample errors、时间和单调钟、raw 字段上限、合法 terminal，以及额外记录/尾片。各消费者仍执行原有资源语义检查。验证完成才关闭 stdin，之后仍须 exit0、双 EOF、Wait/Join 和三句柄关闭；再次校验可拒绝 ACK 后的额外记录、残片及非零退出。宿主 completion receipt 是本地证据，不是新的 stdout 协议。

失败清理与正常 EOF 分开：在通用 Stop 关闭 stdin 前，qualification abort 直接向空的非阻塞控制管道写一个取消字节，不先调用可能已经失败的 poll。若取消字节无法交付，先停止该 Observer 拥有的进程，仍执行 Stop/Wait。这样拒绝完整 terminal 后的清理不能误获 helper exit0。校准的刻意提前停止仅接受明确取消原因、exit1 和完整收口，记录 `cancelled-not-complete`；一个 observer 校验/记录失败也会继续收口其他 observer。

| 实际调用方 | 正常完成与边界 |
| --- | --- |
| `eks_resource_preflight.Run.observe` | 10 秒/6 样本；完整身份、raw 资源和结束校验后 EOF；Wait/双 EOF/关闭之后才做后身份控制面 GET。仍在原 whole-role 60 秒内。 |
| `resource_local.monitor_observations` / execute | 140 秒/71 样本；沿用 stream resource 检查，先确认再等待退出；load monitor 的错误/取消进入 qualification abort。没有运行完整 resource_local。 |
| `test-capacity.read_observer` / stop_observers | 2698 秒/1350 样本自然结束先做 resource/db gate 再确认；提前停止是非零取消，已经 Wait/关闭的 observer 才从 cleanup 集合移除。没有运行 45 分钟校准。旧入口明确拒绝 `-O` 的行为保留。 |

calibration 从同一冻结计划取得产品 binary hash，实际 helper hash 与 native identity 加入 profile/冻结输入；未再独立硬编码一个可能漂移的产品 hash。通用 `Observer.poll/stop`、artifact upload/release、snapshot/trial/setup/pace、Weir RPC/Core/Adapter/Proto、Native 重试、UNKNOWN/无重放与数据库写入预算均未改。

field 256 KiB、FD 4096、sample 2 秒/write 1 秒、64 MiB 输出、三种采样数量/时长原样保持。EKS role60、observer close4+StopWait4、主900/cleanup300、默认 cleaner180不变，没有向每层追加四秒或减少 raw 字段/资源阈值。

## 原失败、最终回归与证据

`.testdata/m31/` 保留每条 runner 的命令、明确 timeout、输入 hash、退出/Wait/耗时及 stdout/stderr，外层测试 runner 均不超过360秒。使用已有 Go1.27.1、GOENV/GOWORK/GOPROXY/GOSUMDB=off 和本地缓存；没有全仓 integration 或外部环境 opt-in。

开发期 `python-initial` 的旧 fixture 缺 native identity/时间字段、`python-completion-initial` 的首次采样时机和 stderr 预期失败均原样保留；修正测试夹具后通过。没有改写旧失败为通过。

两个额外真实进程负例单独保留：

- `baseline-replay/` 使用基线原版 `Run.observe` 接新 Go 完成函数：完整输出后仍等 child 退出，4.944 秒得到 `observer completion EOF timeout` / exit1；双 EOF/Join/关闭完成，说明只改 helper 会造成旧接收方死等。
- `rejected-stream-before/` 中完整 terminal 后身份 hash 被拒绝，但通用失败清理 EOF 使 helper 错误 exit0。最终 `rejected-stream-after/` 保留相同主错误 `target artifact drift`，helper 因取消字节 exit1，双 EOF/Join/关闭完成；三消费者新增同类真实管道回归。

| 检查 | 结果及源码边界 |
| --- | --- |
| 默认离线 `go test ./...` / `-race` / `go vet ./...` | 各一次通过；外层71.793 / 84.770 / 0.858秒；不接触 DB。 |
| focused integration 普通 / race / vet | 最终平台约束下全部通过；外层12.759 / 16.602 / 0.515秒。此前 verbose 普通和 race 各28顶层/42含子例通过，保留真实 PID/Wait 日志。 |
| Python 全套普通 / `-O` | 取消清理补丁前251项通过 / 251项中225通过、26项既有明确skip；324.136 / 308.635秒。不是最终全部源码的全套证明。 |
| 最终受影响 Python 普通 / `-O` | 普通92项通过，102.563秒；`-O` 92项中70通过、22项按旧入口限制skip，82.594秒。输入逐项匹配最终实现，覆盖新增取消补丁和所有受影响调用方。 |
| Linux 原生选定用例 | 11顶层/25含子例通过，无skip；一个准确缓存镜像、一次容器执行，详见下文。 |

Go 真实 os.Pipe/子进程覆盖完整接收后 EOF、30 次 terminal 已被读取但 Encode 未返回的交错、100 次未等待 reader 调度的提前 EOF、提前字节/读失败、无 ACK 四秒非零、慢/停止读取、写失败、信号（采样中和 terminal 后）、错误/缺失样本、取消及 Join。没有以字符串/mock Writer 代替真实输出管道。

Python 回归实际进入上述三个消费者。合成流覆盖完整/截断、role/count/sequence/identity/errors/end 错误、重复 terminal、额外记录/尾片、ACK 后异常、非零、提前退出、取消、记录 I/O 失败；每个真实 child 的 Wait 和三句柄关闭独立检查。calibration 多 owner 首败回归确认后续 owner 仍回收。

`final-affected-normal/external-{eks,local,calibration}/` 将同一个正式 Go 完成函数与实际 Python 接收逻辑通过外部管道串起：分别6/71/1350样本，stdout 34,499 / 398,556 / 7,558,921 bytes，exit0、双 EOF、Join、关闭以及 validate ≤ EOF ≤ Wait 时序全部通过；各约0.058 / 0.111 / 0.923秒。样本身份和资源字段是合成数据，长时间 coverage 由合成时间戳表示，绝不是执行了140秒/45分钟资源采样。`-O` 对支持优化的 EKS/local 同样运行真实 Go 管道；calibration 保持明确skip。

## 唯一受限 Linux arm64 执行

host Darwin25.6.0 arm64；Docker Desktop4.88.1/Engine29.7.2；VM Linux7.0.12-linuxkit aarch64。已有缓存镜像准确 ID：`sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160`。容器 `ca4db5838a7cedb0ec46ba1cbd518e180b98ff06f35be513422cbbea3439bce9`，owner `weir-m31-completion-22719`。

实际 inspect 确认 network none、UID/GID65532、dropALL、no-new-privileges、read-only、1 CPU/256 MiB/64 pids、nofile4096、私有cgroup，仅自有编译产物只读挂载。没有凭据、DB、host/生产网络、pull/build 或 Docker 设置变更。主预算180秒，实际10.964秒；准确清理预算60秒，实际0.127秒，ID/label absence 均确认。所有 Docker CLI 都有 Wait。

`linux-08.out` 是容器内实际执行日志：无ACK在4.036秒 exit1；提前EOF、非法字节、信号、sample/count错误、背压和写失败都非零；合法完成和边界交错通过。额外 `/proc` 自采样为 PID1、UID65532，RSS8,925,184，真实 namespace/cgroup/二进制 hash 被校验；已退出目标被拒绝。

这验证正式完成函数、真实 inherited process pipes 和有限 `/proc` 自采样，**没有运行完整 resource helper 的 proc + DB HTTP 采样链，也没有 EKS/kubectl 传输验证**。普通 Linux helper 仅离线交叉构建；不能把其生成或上述测试称作资源资格验收。

| 二进制 | SHA256 |
| --- | --- |
| Linux focused test | `730ad20dc836d110c65f95a1fe2479adff1ee602cd20a0b97a93cc08bbee20fa` |
| Linux qualification helper | `de91bb6056dd75944b140ad82ed746a1b635b67351cd7b2542214b1cea0daf3d` |

Linux 执行后仅给新 Go 测试文件增加 `linux || darwin` build constraint，防止不支持的 Windows 管道测试运行；加入约束前的测试原文单独保留。源码证明验证“最终测试文件 = 该约束前缀 + 已执行测试原文”，所有非测试 helper 输入精确匹配所执行二进制的构建输入。最终 host focused 普通/race/vet 在约束加入后再通过。本轮没有启动第二个 Linux 容器；Python 最终取消清理在 host 实际 Go 管道中复验。

原有四个容器 ID 及非默认网络不变；默认 `bridge` ID 从 `09d9a39f…` 变为 `a47bc049…`。差异完整记录在 `linux-inventory-review.json`，原因未确定；本阶段未发出网络 create/delete 或配置命令。不宣称全局盘点完全无变化，也未尝试修改或恢复外部环境。

## 源码、封存与交付边界

`source-input-proof.json` 保存基线、实现SHA、最终源文件副本/hash、正式构建输入清单、测试输入匹配结果以及所有本轮二进制 size/SHA256。历史204项 Go/module 中的变化仅在内部 helper；Weir产品正式70项全部不变，qualification正式输入由81增为82（新增完成控制文件），其余修改明确列出。module/dependency、产品构建输入不变。

新 helper 已不同于旧 image source `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。旧 `approvedImages`、SOURCE/digest 原样保留，不能拿旧镜像声称已包含此修复。既有 source-diff 守卫也仍会拒绝把新 helper 作为旧镜像合同执行。公开两个 GHCR 仓库的授权不等于本阶段发布；没有填写未知新 digest。

依照统筹 M30R7 integrity manifest 链，执行前后重验原3296份文件的 size/SHA256 全部不变。`cleanup-audit.json` 独立查询本轮记录的460个不同 host PID，保留 runner Wait、OWNER回收和容器 absence 证据；没有自有测试进程或容器残留。容器内 PID 与 host PID 分开，避免用 namespace PID 查询宿主。

当前 `.testdata/m31/manifest.json` 对本轮每份证据逐文件记录相对路径、size/SHA256，排除 manifest 自身及避免自引用的 `delivery.json`；包含原失败、最终源码/二进制、测试命令/日志/raw/回执、Linux盘点、前后历史核验和清理审计。最终本地交付SHA、manifest hash/数量和报告 hash 见 `delivery.json`。没有修改历史报告、计划或原失败文件。

六原生平台、两规格/后端、CNI、跨两worker、容量/延迟/恢复/24h门槛全部保持；Auth排除、ProgramTransform首版延期不变。本轮不重跑M30R7三臂、不归因CRI、不扩展timeout/protocol、不自行开始镜像或EKS阶段。完成一次授权回调后停止，无timer。
