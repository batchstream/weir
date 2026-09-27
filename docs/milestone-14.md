# M14：Linux RSS / cgroup-v2 有限内存 profile

2026-09-27；基线 `67d2cd248f9ad8146076ee8570f30b60bd224b91`。
本阶段交付一个 Guard 的 Linux 有界观测、准入滞回与短时原生运行证据。
不是六平台、生产 OCI、Kubernetes、容量或 24h soak 合格声明。

## 范围决定

M13 固定 golua v0.3.0 的 NO-GO 调查已获统筹独立验收。用户随后明确回答
“同意首版延期通用脚本（推荐）”：ProgramTransform 继续 UNSUPPORTED、移出 V1
必需门槛，保留未来完整架构要求、实验和协议拒绝路径。没有实现或放宽 sandbox，
没有 VM fork、通用 Mongo RMW/Search OCC 接线。其他资格门槛不变。

## 信号、预算和有界策略

| 观测 | 含义与判断 |
| --- | --- |
| 配置 `memory_mib` / Snapshot.Budget | 独立进程预算；不再被静默替换为 cgroup limit |
| `linux_rss` / Snapshot.Bytes | `/proc/self/statm` resident pages × OS page size；与配置预算比较 |
| 每层 `memory.current` / `memory.max` | 同组及后代（含 siblings 对祖先的压力、page cache）的 usage 与该层 hard limit 配对；各有限层均参与判断 |
| `go_sys_minus_released` | macOS/Windows 明确降级；Linux RSS 失败时展示它，但 process_valid=false/unknown=true，不能据此解除闭锁 |

RSS 和 cgroup current 从不相加；cgroup current 不解释为 Weir heap。80%（含等于）
闭锁、70%（含等于）解除；任一高指标触发，所有必需指标可信且均低于或等于自己的
70% 才解除。处于中间区保持原 latch。整数阈值避免乘法溢出；有限 0 一直闭锁；
`max` 没有该层有限约束，但 RSS/config 检查仍生效。

`New` 同步完成第一次观测并向 admission/runtime 发布；`Run` 每约 100ms 更新。
不能从 startup unknown 先放行 100ms。临时 RSS/current/limit 读取或解析失败保守闭锁，
原 profile 恢复且全部低水位后才能恢复；不会把缺失当 0。拓扑或有效 limit 改变是
`profile_changed`，直到重启；不跟随运行时迁移、不动态重配控制层。

Linux 定位来自 `/proc/self/cgroup` 和 `/proc/self/mountinfo`，选择覆盖 leaf 且能看到
最多祖先的 cgroup2 mount。只向其可见 root 上溯，不扫描其他 cgroup、兄弟或宿主。
读不到的 namespace 祖先明确不可知。本机 namespace 只暴露一层。
静态 profile 比较完整 membership/mountinfo；包括无关 mount 变化也可能保守要求重启。
不匹配、v1、过深、缺 leaf 内存接口、无法解释的 namespace/mount 组合保守 unknown；
仅真正可读取的 `max` profile 才属于本实现的“无有限 cgroup”路径。可见顶层祖先若
两份 memory 接口都不存在则不参与；只缺一份、leaf 缺接口都不作无限推断。

硬边界：membership ≤16 KiB/64 行；mountinfo ≤128 KiB/1024 行、每行 ≤64 字段；
路径 ≤4096 字节；≤32 层；statm ≤256 B 且恰好 7 字段；current/max 各 ≤64 B。
常规每 tick 最多打开 67 个数据文件（3 个 proc + 64 个 cgroup）与 1 个 root；首次
发现最多额外读取 64 个数据文件和 1 个 root。逐文件关闭，无常驻 FD/后台第二采样器。
规范绝对路径、mount 的 space/backslash 转义和 os.Root 限制阻止越界；不支持的控制字符
escape 拒绝。纯函数及自有临时目录测试覆盖溢出、malformed、缺失、权限和 symlink 逃逸。

[内核 cgroup-v2 文档](https://docs.kernel.org/admin-guide/cgroup-v2.html)定义 current
包含后代、max 是 hard limit，但可短暂越界；Guard 不能保证永不 OOM。
[proc 文档](https://docs.kernel.org/filesystems/proc.html)说明 RSS 是异步估值，smaps
更精确但更昂贵。本实现不在生产 100ms 循环扫描 smaps，不宣称精确 heap 会计。

## 所有权、观测与 Bulk

app diagnostics 只读一个 Guard 的 Snapshot。旧 6 条 memory series 保留，预算含义
明确改为配置进程预算。新增 7 个无标签 gauge（process_valid、unknown、cgroup 的
finite/valid/current_bytes/limit_bytes/levels）、4 个固定 state 和 3 个固定 scope，
共 **14** 条；最大静态图 **2043 + 14 = 2057**，实际 16 Local/2 listener 验证通过。
cgroup snapshot/指标显示压力最高有限层的**自身** current/max 配对；无有限层显示 leaf
current、finite=false。它是压缩观测，不把所有祖先之和当预算；数值须结合 valid 使用。
不输出路径、文档、目标、请求 ID 或秘密。不查询 DB、不新增采样循环。

审查与确定性反例发现原本地 Bulk 收到 overload 输入终止就退出，取消尚未送达的
已准入结果。现在沿用该 session 的结果账本：停止输入、继续等待并发送已准入 Ticket，
Ack 后再返回 ResourceExhausted。无新队列、重放或写入结果推断；慢消费者仍受原 stall
和总 deadline 约束。forward-only 也使用进程 Guard。readiness/liveness 不随 overload
改变。Node.Close 取消并 join Guard，再完成已有 drain/adapter/diagnostic 关闭路径。

## 实际运行环境和预先固定门槛

- Docker engine：Linux `7.0.12-linuxkit`、`aarch64`、cgroup v2，8 CPU，8319770624 B VM 内存。
  宿主 Darwin/arm64；Go 1.27.0 在宿主交叉构建，产物在同架构 Linux VM 原生执行。
  没有 Linux Go 编译器/race runtime 资格声明；三轮 race 在 Darwin/arm64 执行。
- 已有官方 Mongo 8.0.32 arm64 镜像作为运行 fixture，image ID
  `sha256:997ed65ff26fc20107e799f0fd1477e5af782b42bd651d870c5436b95fd0323c`；
  official multiarch digest `sha256:4968f22d0c6c10ef29952f3e807f62872ba22b3312f25803564fbfc08255efc2`。
  重新核对 engine、镜像 OS/arch 和实际 uname/runtime；未用 QEMU。
- Guard 与 CLI 各在独立 512 MiB memory/memory-swap、2 CPU、96 PID、只读 rootfs、
  32 MiB `/tmp` 的自有容器中，全部 client 端口在容器 loopback，无宿主发布端口。
  Guard 自进程 mmap 上限 112 MiB、配置预算 128 MiB；CLI 配置 512 MiB，压力由
  同 cgroup 独立 test helper 已 touch 的 mmap 产生，上限 416 MiB、45s watchdog。
- 80/70 不变；helper 目标 84% 高、75% 中间（保持至少 300ms）、释放到低。
  helper 确认后观测收敛/恢复固定 2s，CLI SIGTERM/Wait 固定 3s；误差界 RSS 8 MiB、
  cgroup 16 MiB。不同采样时刻释放中的差异必须在原 2s 内收敛，不抬高误差或期限。

**原生 Linux Mongo 阻塞：** 初次自有 768 MiB Mongo 容器启动即 exit 1，明确提示该
内核与 Mongo 版本不兼容（不是 OOM；exec mongosh 随容器退出为 137）。官方
[8.0 发布说明](https://www.mongodb.com/docs/v8.0/release-notes/8.0/)确认 6.19–7.0.13
受影响、7.0.14 起解决。没有改内核、绕过启动检查或更改生产后端资格。

真实 CLI 证据使用单独 opt-in 的**自有 Darwin/arm64 MongoDB 8.0.32**，随机 loopback
端口、单成员 replica set、256 MiB WT cache、隔离 DB。Linux CLI 通过 owned wire proxy
连接 host.docker.internal；这条测试网络是自有 bridge（允许到宿主），不是 internal-only。
Mongo 不绑定公网，不发布数据库端口。明确是 Linux Weir + macOS 真 DB 混合环境；
确定性 fake adapter 只用于独立 Bulk 回归，不冒充数据库结果。

## 实测结果与日志

最终源码原生原始日志：`.testdata/weir-m14-2750c23d9d0b/guard.log`、`cli.log`、
`guard-state.json`、`cli-state.json`、`cleanup.json`、`engine.json` 和 `image.json`。
两个测试各三轮 PASS；关键值如下。

| 实际场景 | 三轮记录 |
| --- | --- |
| Guard 配置 RSS 高位确认 | 85.662 / 72.268 / 69.416 ms；RSS 约 113.3 MB，cgroup 约 115.6–116.1 MB，container 未高而进程 latch=true |
| Guard 中间 / 低 | 中间 RSS 约 99.7–101.8 MB 保持 latch；低位解除 88.459 / 64.826 / 92.177 ms；与 smaps_rollup RSS 差 0–8192 B |
| CLI / forward / helper PID | (12,19,27)、(34,41,48)、(54,61,68)；均已 Wait/消失 |
| 同组高压，CLI RSS 仍低 | CLI RSS 20.9–23.0 MB；组 current 451.1–455.3 MB，512 MiB limit，latch=true；确认耗时 25.388 / 88.673 / 88.711 ms |
| 中间滞回 | current 401.9–402.2 MB，保持 latch=true；没有由低 RSS 清除组压力 |
| 释放恢复 | current 68.6–70.0 MB；CLI 确认恢复 23.480 / 46.613 / 45.233 ms |
| 高压 SIGTERM / Wait | 25.984 / 25.999 / 27.739 ms；两个 CLI 与 helper 均 Wait，PID 消失、proxy sockets=0 |
| OOM / hard-limit events | 每轮 memory.events max=0、oom=0、oom_kill=0；容器 OOMKilled=false |

上表耗时是测试在 helper 确认后开始轮询的确认时间，不是调度精度承诺或生产恢复 SLO。
同进程 RSS 与独立 smaps、cgroup current 的完整非原子时间线均保留，不只报最优值。
最终 CLI 的 RSS 差为 0–2105344 B（包含启动/变化中的跨采样时刻）；不能把 Guard 小样本的 8 KiB 当作所有 CLI 场景误差。
CLI 每轮真实 Read/Mutate/Bulk 前后成功；高位新 Read/Mutate 与已有 Bulk 新 operation 拒绝；
已准入且在 proxy gate 等待的 Bulk 写在高位继续执行、返回 APPLIED。
每轮 wire update **恰好 5**（before unary+Bulk、admitted Bulk、after unary+Bulk），
被拒绝 ID 在 DB 中为 0。新独立调用恢复、没有隐式重放；未丢弃 UNKNOWN 契约。

失败历史保留：`weir-m14-04ac6a2254ed` 是 Linux Mongo 内核阻塞；`b861236761e3`
是 internal-only 网络无法解析宿主；`a5b79eb96ddc` 是 fixture 错把 application 地址
作为 peer；`b8458657038d` 一轮在释放中比较旧样本（差 23.9 MB）失败，其余两轮通过。
纠正的是 fixture 地址及同期限内的采样收敛检查，不是降低水位、扩大误差或恢复期限。
`.testdata/m14-evidence/bulk-local-before.log` 保留产品 Bulk 丢已准入结果反例，修复后三轮 race 通过。

## 复现与验证

```sh
GOPROXY=off GOSUMDB=off go test -count=1 -timeout=180s ./...
GOPROXY=off GOSUMDB=off go test -race -count=1 -timeout=180s ./...
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
GOPROXY=off GOSUMDB=off GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go vet -tags integration ./...
GOPROXY=off GOSUMDB=off go test -race -count=3 ./internal/overload

# 显式自有 native Linux 容器；默认仅使用已固定的本地镜像，不拉取/发布。
# 在本次受影响内核上 Linux Mongo 启动会失败并清理，不能当作 PASS。
WEIR_M14_INTEGRATION=1 python3 scripts/test-memory-linux.py
# 本次真实后端替代证据：本仓已有固定 Darwin Mongo 二进制 + mongosh。
WEIR_M14_INTEGRATION=1 WEIR_M14_HOST_MONGO=1 python3 scripts/test-memory-linux.py
```

默认测试不启动 Docker/DB。真实文件同时需要 integration、linux build tags 和显式 opt-in；
helper 额外验证 512 MiB cgroup、硬分配上限和 watchdog。脚本校验 engine/image 架构，
owner label/marker 后清理自己容器、网络、host mongod/data 和三个生成二进制，保留日志。

`.testdata/m14-evidence/` 保存非缓存全仓 test/race、default/integration/Linux integration vet，
解析/状态三轮 race、旧准入/Native/Scan/diagnostic 三轮 race、真实 Mongo TLS Native/Scan/
慢消费者/drain 回归，以及真实 16 Local 指标和 SIGTERM 测试。Search 子项明确 SKIP，
本阶段不据此新增 Search 真实资格。六个 CLI 有限 build 的 file/架构/SHA256 在
`build-six.json`；交叉编译不算对应平台 native-run。

## 资格边界与交接

已实现：Linux 静态可见 cgroup-v2 + RSS/config、未知保守策略、固定低基数观测和单 Guard join。
已有限实测：Linux arm64、单可见有限 cgroup、进程 RSS 高位、同组其他进程压力、真实
Weir CLI/forward-only、混合宿主 Mongo 后端、三轮过载/恢复/关闭。
无有限 cgroup、多个可见祖先压力只验证自有文件解析/决策，**没有取得对应原生运行资格**。
Linux 原生 Mongo 被 kernel 7.0.12 阻塞；不得由宿主后端结果外推。Darwin/Windows 仍为
Go 降级，其他架构、生产 OCI/Kubernetes、多节点、容量及 24h soak 全部仍 required/unqualified。

没有 push/PR/发布/生产部署、已有秘密读取、额外执行聊天、定时任务或下一阶段工作。
最终只交付 local main；完成后停止 checkout 写入和测试，按授权回调统筹。

最终清理审计保留在 `.testdata/m14-evidence/cleanup-audit.json` 与
`docker-cleanup-audit.json`：7 个 M14 fixture 目录及 13 个本阶段真实 TLS 回归 fixture，
仅保留 owner、PID receipt 与日志（含轮转日志）；无数据/证书/keyfile/生成二进制。
所有自有进程/容器/网络均已关闭和回收。构建/解析/诊断的临时二进制也已删除。

最终验证结果：全仓非缓存 test/race 均 PASS（server 58.743s / 60.253s）；
default、integration、Linux integration vet 均 exit 0；六组合有限 CLI build 全部通过。
新解析/状态三轮 race 与准入/Native/Scan/diagnostics 三轮 race 通过；真实指标/进程关闭
两项通过、真实 Mongo stream 六项通过（未启用 Search 子项不计 PASS）。
