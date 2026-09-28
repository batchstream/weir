# M26R3 — 只读 socket 取证与边界 NO-GO

2026-09-28；执行聊天 `01a0e6a9-ce02-75a3-87fb-b407c11abd38`。
基线 `0e35d1e5c515d9ed84a9d07baad5b0ed1369a9cf`；诊断实现
`486b8077d6f1045bb1419179b1215716b8aa2b73`；只读 EKS driver 实现
`fd18036b1c0cded9bd5ca3266653e3255382aa00`。最终交付 SHA 见受控
`.testdata/m26r3/closeout.json`。正式任务 SHA256
`86472e42ed0c78b73dd82d8be5c91e912701e0fb01c053f3b8d89c3d859ffe96`。

**诊断取证完成，真实非回环连接使边界继续 NO-GO；修复待统筹独立决策。**
本轮 ES/bootstrap 的自有 Pod 网络命名空间存在三条指向 `169.254.169.254:80`
的 TCP 关闭阶段记录，原 guard 正确以 23 拒绝。没有放宽规则、修改 ES 配置或再次试验。
M26R2 当时没有保存拒绝行，不能把本轮原始行倒写成旧现场。具体发起进程、组件、HTTP
方法/路径、响应内容及是否获取过元数据均未知；没有探测该地址或读取元数据/凭据。

## 实现与离线验证

shell 在判定前保存 TCP4/TCP6/UDP4/UDP6 表头与完整有界原文；后续判断使用保存的同一份
字节，明确标记 proc 遍历及跨表读取不是原子快照。每表最多 65,536 字节、256 行、读取
2 秒；多读 1 字节检测上限。缺失/短行/缺头/不完整为 33，超时 34、字节上限 35、日志失败
36、行上限 37；已取得内容保留。拒绝行另列 local/remote/state/UID/inode、process=unknown、
原始行及原 exit/reason。日志失败不会把已判定的 guard exit23 覆盖成成功，或继续到管理写。
原地址、端口、非 LISTEN 状态与 UDP 规则保持；未知非 LISTEN 状态没有新增白名单或安全结论。

Python 的固定 helper 只提供 TCP，继续只检查 TCP；失败包含原始表和字段，快照先保存后检查。
UDP 由 shell 边界检查。终止报告保留所有容器 state/reason/exitCode/finishedAt/restartCount/
lastState，不再笼统称 OOM；时间齐全且可区分时仅报告最早失败的 finishedAt，不推断因果。
bootstrap Completed/0 不算错误；OOMKilled、restart/history 仍拒绝。

独立只读 driver 复用共享准入、节点预检、create、归属和 UID 清理。冻结 ConfigMap 的
bootstrap.sh 为专用诊断脚本，不 source 原管理 bootstrap；原 ES sidecar、普通 init、两个
main 模板和限额保持。检查失败原码退出；检查成功打印 receipt 后固定 exit42，阻断 main。
没有 exec/seed/trial 分支。超时收口可再观察一次已创建 Job 的精确 controller Pod，登记其
UID 后取证清理；不采用陌生/替换对象。原功能入口仍保持原行为。

| 检查 | 结果 |
| --- | --- |
| 全 Python 普通 | 124 通过，0 skip，81.081 秒 |
| 全 Python `-O` | 124 发现，99 通过，25 既有 skip，76.947 秒 |
| shell 实际执行 | IPv4/IPv6、未知地址/状态、缺表/空表/短行/截断/限额、UDP、日志失败、同快照判定、42/23 终止 |
| 新 EKS 完整离线接线 | 实际准入响应形状 → Job/Pod dry-run → create → 42、23/ES143、超时、UID 漂移、缺 receipt、main/OOM 负例 → 有条件清理 |
| 原回归 | quantity/probe/资源/UID 负例及原两 trial 合成生命周期保持 |
| Go | 本轮不重跑；199 项固定产品输入及额外协议/模块/打包/CI 输入 hash 未变，不声称新增 Go 资格 |

初次新 driver 的离线超时用例暴露“创建耗尽预算前未登记 Pod UID”的收口缺口，修复后才
通过上述最终全套验证并提交/freeze；原失败日志保留。shell 旧版本在测试 fixture 中固定为
基线 Git 字节，仅测试重写 proc 路径，线上没有任意路径注入。离线日志见 `.testdata/m26r3/offline/`。

## 本地唯一尝试与 EKS 冻结

本地 Docker 只读预检：原生 Linuxkit `7.0.12/aarch64`，8CPU/8,319,770,624B，无运行容器，
固定 ES image 已在缓存。使用新空匿名 registry 配置，没有读已有配置或拉取/构建新镜像。
最初两次冻结前入口检查分别发现辅助脚本 import 路径、`docker network ls -a` 参数错误，
均为零资源创建，原日志保留。纠正后 fake CLI 完整生命周期通过，但它未执行真实 Docker
Go template，遗漏了网络 inspect JSON 结束括号。

唯一冻结本地计划 hash `d543ff3876ba81180a0d4cdd1b9c16524a78434e04e0b0518419e2e3ae45b340`，
owner `weir-m26r3-local-20260928`。仅创建 internal network
`ea4c7dd9b67c3a0309b8ef15fa6debc1323c0e8720aa9866d0dcbe853f50ded9` 后因上述解析错误停止；
该 network 按精确 ID/owner 回收，前后容器/network ID 集合一致。ES 和诊断容器均未创建，
没有本地 socket 证据，也没有本地 Linux 运行资格。冻结后未修补或重跑该 fixture。

本地无法确定，故使用唯一 EKS 尝试：证据目录 `.testdata/m26r3/eks-20260928-0644/`，
namespace/owner `weir-qual-m26r-20260928-0644-r3`；所有新证据目录 0700、plan 0400。
plan hash `13f748a453b911091bb849b707293abfbba4baf8e197f55bde747d57b0a7081e`。
固定 context `arn:aws:eks:us-west-1:956540890581:cluster/data-team`；node
`ip-172-31-12-243.us-west-1.compute.internal`，UID `18f03c58-83c1-424e-be34-44ef88c07831`。
初次和创建前快照均余 7.030CPU、31,202,312,192B memory、52 slots、18,182,813,665B ephemeral；
不是资源预留。模板峰值 6CPU/4608MiB/2560MiB，ES3CPU3072MiB/heap1024、init1CPU512MiB、
data emptyDir1GiB、原 Quota/security/loopback args/probes 不变。预算 management=0、document=0、planned=0。
固定三镜像沿用 M26R2（source `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`），15份匿名 registry
原证据重新核对 hash 后复用；没有重复大量下载。ES reference 为
`docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`，
config `a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160`。

## 原始现场与解释

`final-bootstrap.log`：实际内核 `6.12.77-99.140.amzn2023.aarch64`，uid1000:gid0，
net namespace `net:[4026533181]`，ES8.19.22/所需 settings 已验证；本 PodIP `172.31.0.206`。
TCP4/TCP6/UDP4/UDP6 原文分别 600/985/128/163 字节；UDP 两表只有表头。
TCP6 表头及第一条拒绝行原样如下（其余两条外连同表保存）：

```text
  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   2: 0000000000000000FFFF0000CE001FAC:CA3E 0000000000000000FFFF0000FEA9FEA9:0050 05 00000000:00000000 03:0000154C 00000000     0        0 0 3 00000000f1b0b952
socket-reject table=/proc/net/tcp6 exit=23 reason=tcp-local-address
```

小端 32-bit 分组解码得到 `[::ffff:172.31.0.206]:51774` → `[::ffff:169.254.169.254]:80`；
另两本地端口为 51792、51806，三条 state 都是 05。9200/9300 的 LISTEN 行确实绑定
v4-mapped 127.0.0.1，peer 为全零；拒绝发生在后续非回环记录，并非将监听零 peer 当外连。

[Linux v6.12 TCP 状态定义](https://raw.githubusercontent.com/torvalds/linux/v6.12/include/net/tcp_states.h)
将 05 定义为 FIN_WAIT2；[tcp6 输出实现](https://raw.githubusercontent.com/torvalds/linux/v6.12/net/ipv6/tcp_ipv6.c)
`get_timewait6_sock` 输出 `tw_substate`、timer=3，并将 UID/inode 位置写为零。
这些值不是 root 进程归属，也不能由非 LISTEN 状态推导安全。[6.12.77 stable 实现](https://raw.githubusercontent.com/gregkh/linux/v6.12.77/net/ipv6/tcp_ipv6.c)
具有相同字段输出；实际 Amazon 构建版本单列，没有声称整份厂商内核与 upstream 完全相同。
字段说明另见 [proc/net/tcp 文档](https://docs.kernel.org/networking/proc_net_tcp.html)。
[AWS 官方文档](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html)
列出的 IPv4 IMDS endpoint 即 `169.254.169.254`。这里没有采集 HTTP 流量或查询 metadata。

`.testdata/m26r3/replay/` 保存从本轮日志按长度提取的四张表；同一快照实际执行旧、新 shell
都 exit23，Python 同样按 local-address 拒绝。`socket-analysis.json` 保存逐行解码。
已证实的直接原因是本轮 Pod 命名空间中的非回环连接记录；具体组件/请求内容未知。
不能从加载了 repository-s3 模块等信息进一步认定发起者，亦不能声称所有底层 HTTP 请求均为 GET。
本轮显式诊断入口仅有 loopback GET/proc 读取，guard23 前就停止，未执行负向 connect。

## 运行身份、零写预算和清理

JobUID `e1a76d74-e259-4c4e-ac08-408873d60721`；Pod `loopback-dwplj`，
UID `4059698a-60ad-4722-a12d-531d2c0fae16`。两 init 实际 imageID 均为上述固定 ES reference；
containerID 分别为 `containerd://93f7f00403f0d1119fff6b687ae796c02526a698fa2874171648962ee89b88b6`
和 `containerd://38fed35323214639d8b64148794331af966eed589de0c5d466c451c5d0b7094f`。
bootstrap 06:45:50Z Error/23，ES 06:45:54Z Error/143；restart0、lastState空、无 OOMKilled。
ES日志另有 stopping/stopped/closed。Weir/client 均 PodInitializing，无运行时 imageID/containerID，
仅模板身份可核；没有产品运行资格。终止报告正确保留两者，最早失败 finishedAt 为 bootstrap。

92条 prepare/run 命令全部 end/exit0；6次 server dry-run、5次持久 create、1个 controller Pod。
没有 exec、索引管理、seed、trial 或文档写；实际测试 management/document mutation 均0。
入口 exit1、`evidence_collected=true`；`diagnostic_complete=false` 原样保留，后者专指 guard 全部
通过后的预定42 receipt，并不表示本轮未获得拒绝现场。`functional_pass=false`。

六个自有对象精确 UID 删除：Job0073、Pod0076、Quota0079、Policy0082、ConfigMap0085、Namespace0088。
对应 UID/请求/响应在 `owned.json`、`delete-*.json` 和命令记录；没有 force/finalizer 或未知对象删除。
namespace 生命周期173.093秒，收口清理73.514秒，remote含清理176.137秒，均在预算内。
最后 namespace 查询为空；额外 `closeout/command-0001` 再确认不存在，本地 owner 容器/network均0。
所有命令子进程均已 Stop/Wait，未遗留验证或 fixture 进程；本地 main 提交干净，无 push/CI/镜像变更。
原 M26/M26R/M26R2 的94/315/2237份证据 hash/长度逐项不变。

candidate=null；功能闭环、容量、过载、恢复、24h均 not-run，CNI隔离 unqualified。
startup-signal-registration-window / exit-15 仍 unresolved。Weir认证排除、通用 ProgramTransform V1延期，
其余门槛不减。到此只回调统筹并停止，不开启下一阶段或恢复定时任务。
