# M14R：内存身份与 fixture 失败清理补救

2026-09-27；基线 `8d5aab1b9be2aaf54224380f709428b07d77efe9`，local main。
范围仅为原 M14 的两个根因和回归。原 [M14](milestone-14.md) 未通过独立验收的历史、
原生 Mongo 内核阻塞和全部原始失败日志保留，不把旧阶段倒写成通过。

## 相关静态身份

不再比较 membership/mountinfo 的原始字节。每次有界读取、解析后，身份只包含进程
cgroup-v2 leaf、所选 mount ID 和 major/minor、root/mount point 映射、可见相对层级
及每层有效 limit/接口状态。mount ID 和设备号防止只按路径漏掉替换挂载；数字解析
拒绝溢出。依据 [mountinfo 字段定义](https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html)，
optional propagation 字段、父挂载编号和 source 名称不作为内存身份。

先选择暴露最多祖先的适用 mount；同等覆盖时按 mount point 字典序。无关 mount
增删、无关 membership、行顺序不改变已选身份；同一路径多个适用 cgroup2 mount
属于不支持的歧义，返回当次 unknown。没有动态 reload、provider、inode 树扫描，
不越过 namespace 可见 root，不新增 cgroup-v1 或其他平台采样。

只在所有可见层读到完整有效的观测后，才与原可信 profile 比较并提交永久
`profile_changed`；真实迁移、mount ID/设备/映射变化和有效 limit 变化仍要求重启。
已确认变化后改回原身份也保持闭锁。读取失败、损坏、超长、溢出不更新原身份，
仅当次 unknown/拒绝。原本可信的顶层 current/max 同时消失也保留原身份，不能据此
推断 controller/limit 有效变化。只有首次发现就没有两份接口的可见顶层可以省略。原 profile 恢复后，RSS 与每个必要 cgroup 指标必须都可信且
达到各自 70% 才恢复；高/中间值、低 Go fallback 均不能清除 latch。startup unknown
遵循相同规则。

保留 100ms、80/70、32 层、membership 16KiB/64 行、mountinfo 128KiB/1024 行/64 字段、
路径 4096B、statm 256B、current/max 各 64B，以及 `os.Root` 和逐文件关闭。
合并原首次发现与观测的重复读取：每 tick（含首次）至多 67 个数据文件和 1 个 root，
没有额外 64 文件发现 pass。一个 Guard、固定 Snapshot、2057 最大静态 series 不变。
这些是采样保护，不能保证永不 OOM，也不能发现采样间已发生又撤销的所有变化。

## fixture 独立清理

`scripts/test-memory-linux.py` 在可能部分成功的 network create、container create/run
之前登记精确候选名字。初始化、build、运行都在同一 finally 范围；仅用精确名字和
owner label 查询，明确 daemon not-found 才算不存在；引擎错误/超时保留 unknown。

每个候选容器分别 inspect/验 owner、logs、有限 stop、再次确认退出、rm 并确认 absent。
logs 失败仍 stop；owner 不匹配或 inspect 失败只禁止修改该资源，继续其他容器、network
与宿主 Popen。没有 force rm、猜 PID 或全局 prune。宿主句柄始终 SIGTERM/Wait(10s)，
必要时 kill 自己的句柄并再 Wait(3s)；没有退出证据不删数据。

清理 receipt 同时保留原测试错误、每项结果和所有清理错误；有 unresolved 或 errors
就失败，不输出全部已停止。marker 不匹配不删文件；容器未确认回收时保留挂载中的
生成二进制。build 失败也只删除 owner 已确认的三个具体产物。旧 raw 日志不删除。
Docker info/inspect 在命令中直接格式化为内核/架构/cgroup、资源上限、image ID/digest、
owner 和状态，不先收集全量 JSON，不打开环境、代理配置或 credential 文件。

## 反例与离线证据

证据根 `.testdata/m14r-evidence/`。新测试先针对未改生产源码运行，随后针对补救源码运行。

- `profile-before.log`、`profile-final-against-baseline.log`：原生产源码三轮 FAIL，
  包括无关 mount、低位→malformed→恢复原低位、高/中间→unknown→恢复及未完整验证的
  身份差异。后一日志的源码副本在 `baseline-profile/`，来自指定基线 `git show`。
- `top-pair-before.log`：复查中三轮证明原本可信的顶层接口同时缺失仍误判永久变化；
  最终修为当次 unknown，恢复原 pair 后可恢复。该失败使用修改中源码的独立副本，
  不是系统 cgroup 操作。此前整套成功日志保留于 `pre-final/`，不冒充最终源码证据。
- `profile-race3.log`：新身份/状态及原权限、symlink、最大值边界三轮 race；
  mount 替换、设备、映射、迁移、limit、确认变化后改回和 startup unknown 均覆盖。
  旧 mount-change 用例改成有效 mount ID 替换；malformed 的拒绝及恢复由新反例单独验证。
- `fixture-before.log`：原 finalizer 的离线故障注入失败；`fixture-offline.log` 为补救后
  7 项测试。标准 subprocess/socket 边界使用惰性 doubles，没有 Docker/数据库。
  覆盖 inspect/logs 失败不跳过其他资源及宿主 Wait、部分创建后超时、最后一个 build
  失败、owner mismatch、Wait 失败保留数据、原失败与 receipt 共存及明确 absence。

文件模拟证明解析/决策；惰性 doubles 证明清理控制流。二者均不冒充原生内核迁移、
原生无有限界/多层祖先或真实 Docker 故障资格。

## 原生与真实后端证据

Linux VM 为 `7.0.12-linuxkit/aarch64`、cgroup-v2、8 CPU、8319770624B；宿主
Go1.27.0 darwin/arm64 交叉编译，同架构容器内原生执行，未使用 QEMU。
固定 local image ID `sha256:997ed65ff26fc20107e799f0fd1477e5af782b42bd651d870c5436b95fd0323c`，
Mongo multiarch digest `sha256:4968f22d0c6c10ef29952f3e807f62872ba22b3312f25803564fbfc08255efc2`。
没有 Linux Go/race runtime 资格声明；race 在 Darwin 执行。

Guard/CLI 各 512MiB memory/memory-swap、2CPU、96PID、read-only root、32MiB tmpfs。
Guard mmap≤112MiB/配置128MiB；CLI 配置512MiB，同组 helper mmap≤416MiB、45s watchdog。
保留原 80/70、84% 高/75% 中间、300ms 中间保持、2s 收敛/恢复、3s Close、RSS8MiB/
cgroup16MiB 误差门槛。没有修改 native 测试或 Bulk/账本/UNKNOWN/无重放实现。

真实后端使用明确 opt-in 的自有 Darwin Mongo8.0.32，随机 loopback、单成员副本集、
隔离 DB，经 owned wire proxy 连接 Linux CLI；这是混合环境。当前 Linux Mongo
启动兼容性阻塞沿用 M14 已确认结论，未重跑已知失败的数据库启动、未改内核或绕过检查。
本轮只重新核实内核/image 元数据，不能据此声称 Linux Mongo 原生资格。

本次首次 fixture `weir-m14-dad7cbadb414` 的 Guard/CLI 均三轮 PASS，但 network metadata
模板漏 JSON 右括号导致 finalizer 报错；容器和宿主均已回收，network 保留。
修正模板并增加离线模板 JSON 结构校验后，核验 owner 删除唯一网络，独立
`cleanup-repair.json` 记录成功。原 `cleanup.json` 和 `native.log` 保留 FAIL。

最终源码 native 日志为 `.testdata/weir-m14-4a2fccb3c941/{guard,cli}.log`，
入口日志 `m14r-evidence/native-delivery.log`。两个测试各三轮 PASS，完整 timeline 保留。

| 最终三轮观测 | 实测值 |
| --- | --- |
| Guard RSS 高位确认 | 85.749 / 50.898 / 79.719ms |
| Guard 释放恢复 | 69.250 / 100.913 / 73.208ms |
| CLI cgroup 高位确认 | 67.660 / 94.124 / 93.747ms |
| CLI 释放恢复 | 90.883 / 46.237 / 45.719ms |
| 高位 SIGTERM/Wait | 28.691 / 26.635 / 26.937ms |
| CLI / forward / helper PID | (12,20,29)、(35,42,49)、(54,61,68)，各轮均 Wait/absent |
| 数据语义与资源 | 每轮 wire update=5、被拒绝 ID=0；已准入 Bulk 返回 APPLIED，无隐式重放；proxy sockets=0；max/oom/oom_kill=0，OOMKilled=false |

耗时是 helper 确认之后测试观察到状态的时间，不是生产 SLO。RSS 高位时 cgroup
仍未到高位；同组 helper 高压时 CLI RSS 仍低，两条边界均真实触发。中间位保持 latch，
低位新独立 Read/Mutate/Bulk 恢复。首轮释放中的非原子样本差曾达 140906496B，原日志
明确标为 transitional；同一原定 2s 内收敛后才通过比较，没有扩大误差门槛。

## 命令、最终来源和资格边界

```sh
GOPROXY=off GOSUMDB=off go test -count=1 -timeout=180s ./...
GOPROXY=off GOSUMDB=off go test -race -count=1 -timeout=180s ./...
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
GOPROXY=off GOSUMDB=off GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go vet -tags integration ./...
GOPROXY=off GOSUMDB=off go test -race -count=3 -v ./internal/overload
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_memory_linux_test.py -v
WEIR_M14_INTEGRATION=1 WEIR_M14_HOST_MONGO=1 python3 scripts/test-memory-linux.py
```

关键 server/app 三轮 race、真实 TLS Mongo stream 六项及 diagnostics 两项的精确选择器、
环境、耗时与 exit status 见 `validation.json`/`verify.py`。真实 DB 与 native 串行，
Search 未启用子项 SKIP 不计通过。六目标 CLI `CGO_ENABLED=0 -trimpath` 构建的
file/架构/SHA256 在 `build-six.json`；交叉构建不代表各平台原生运行。

V1 保留 BackendExpression/Native；通用 ProgramTransform 经用户批准延期且继续
UNSUPPORTED，不重建 Weir 自身认证。Darwin/Windows 仍为 Go 降级，其他架构、
Linux Mongo 原生后端、OCI/Kubernetes、容量及 24h soak 门槛仍未通过。

## 最终验证与清理收口

| 最终源码验证 | 结果 |
| --- | --- |
| 全仓非缓存 test / race | PASS，命令总耗时 61.521s / 62.871s |
| default / integration / Linux integration vet | 全部 exit 0 |
| profile 三轮 race | 39 次顶层、117 次含子项 PASS，0 SKIP/FAIL |
| server/app 准入/Bulk/peer/Native/Scan/diagnostics 三轮 race | 72 次顶层、144 次含子项 PASS，0 SKIP/FAIL |
| fixture 离线故障注入 | 7 项 PASS；最后仅测试数据初始化按命名对象整理后再次运行，见 validation.json 的 fixture-offline-final |
| 真实旧 Mongo TLS stream 六项 | 6 顶层/16 含子项 PASS；8 个未启用 Search 子项 SKIP，不计 Search 证据 |
| 真实 DiagnosticsMaximumStaticSeries / DiagnosticProcessSIGTERMReadinessBeforeExit | 两项 PASS，series=2057；readiness=503 时 livez=200，并在原 3s 内退出 |
| 六目标有限 CLI build | Linux/Darwin/Windows × amd64/arm64 全部成功；不是平台 native 资格 |
| source/diff/style | 162 个 Go/module/Python 源文件 SHA256 核对一致；gofmt、diff whitespace 检查通过 |

`source-receipt.json` 对应最终源码；`cleanup-audit.json` 记录本阶段全部 3 个 native
fixture 和两套 TLS 回归产生的 26 个自有 fixture。三个 native 的 Mongo PID 均 absent，
6 个 container 与 3 个 network 均按精确名字确认 absent；26 个 TLS 目录 owner 正确，
只余 owner/日志。数据、生成 TLS 材料和所有生成二进制均已删除，未打开秘密内容。
本阶段没有遗留 mongod、weir 或 test 进程；测试 session 已退出。日志、失败历史、
源码副本及 receipts 保留，未触碰旧阶段资源。

只在 local main 提交；没有 push/PR/发布/部署、新执行者、定时任务或后继阶段。
完成提交与干净工作树核验后停止所有 checkout 写入和测试，按授权回调统筹。
