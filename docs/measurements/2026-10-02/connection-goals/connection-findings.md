# Weir 连接集中现场验证

最终 v2 在 MongoDB 上证明了连接集中的独立收益：相同 200/s、16 个实际业务进程，全部 4000 次读成功，数据库连接从直连的 32 条减少到单 Weir 的 5 条（减少 84.4%，含驱动监控连接）。ES 的较早版本矩阵也观察到 800/s 的 16 进程直连 64 条，旧版双 Weir 全部成功时 8 条；v1 单 Weir 800/s 的 32/64 会话容量格仍有拒绝，不能据此声明最终 v2 单实例已满额承载。

## 最终 v2 MongoDB 连接验证

复用背压 agent 明确交接后的独立 EKS fixture `weir-bp-9282b4b43a1e`，没有访问统筹的正式性能 fixture。MongoDB 连续保持同一 PID1、start_ticks18298、restart0，实际 cgroup quota 为 `50000 100000`（0.5 CPU），内存 1536 MiB；Weir 为 2 CPU / 2304 MiB，pool4、raw owner limit5、sessions32、connections64、最大批16、收集3ms、普通读上限16KiB。连接夹具的 DB CPU 为0.5，与正式吞吐对照的0.25 CPU分开记录。

全部旧 client/Weir/samplers 已停止后才开始。只读 probe 不创建、删除、重置或审计 collection，复用前一 agent 留下的1000条1KiB BSON读记录；每次成功读核验完整 BSON 内容。1/16个真实进程分摊相同200/s，worker8、pool4、有界队列256、计划到达起1s deadline、不重试。20s业务期后保留客户端12s再关闭；数据库PID1的socket fd inode与TCP表相交，每200ms只计local27017 ESTABLISHED，不为观察器建立Mongo连接。

| 200/s 路径 | 成功/计划 | active 实际连接 | 客户端关闭后连接 | 停 Weir 后连接 | DB CPU 核 | Weir CPU 核 | DB RSS MiB | Weir RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 进程直连 | 4000/4000 | 5 | 0 | — | 0.0532 | — | 328.9 | — |
| 16 进程直连 | 4000/4000 | 32 | 0 | — | 0.0542 | — | 327.7 | — |
| 1 进程 → 1 Weir | 4000/4000 | 2 | 2 | 0 | 0.0763 | 0.2347 | 328.1 | 24.6 |
| 16 进程 → 1 Weir | 4000/4000 | 5 | 5 | 0 | 0.0768 | 0.1605 | 329.4 | 28.1 |

每格active有94个内核连接样本，min/median/max均为表中相同数值；CPU/RSS各20个active样本覆盖约19s，RSS为采样最大值。全部0 client_drop、0 API失败、0 admission拒绝、0 OOM；四格只读，没有 mutation/APPLIED账本。两个单进程格都实际遍历全部1000 key四轮；16进程共用读序列，各240/260次读，联合260个唯一key，因此没有把其4000次读误称为4000个唯一key。

连接数包含Mongo驱动poll monitoring；pool4是数据连接配置上限，不是每格实际打开的数据连接数。直连16进程的理论数据池+监控最大为80，实测是32。Weir在此两格owner peak分别为2和5，都≤limit5（4个数据池credit+1个monitor/lifecycle credit）；停止后acquired/released分别2/2、5/5、owned0，数据库最后五个样本回0。业务客户端关闭后Weir保留2/5个后端连接属于池复用，不能当成泄漏；最终停止Weir与所有客户端后与最初空闲基线0精确一致。

这个低速同步零碎读场景只证明连接集中与正确性，**DB CPU没有下降**（16进程直接0.0542核，经Weir0.0768核），DB RSS也没有体现显著节省。两个Weir格平均批大小仅1.001和1.040；同步同key探针不能代表分散key的批量吞吐。不会根据这四个连接格声称已触发真实Mongo连接上限耗尽、已部署/触发HPA，或代表800/s承载能力。

[Mongo完整摘要](mongo-final-v2/fixture-summary.json)、[清理凭证](mongo-final-v2/cleanup.json)、每格helper命令/16个独立PID回执、fd样本、CPU/RSS与cgroup原始样本、metrics、shutdown raw-owner日志和远端archive哈希均已保存。namespace UID `aa430ac8-e524-4d80-8a40-3968fb5cc109`、pod UID `7cf12535-cef8-4c08-993b-67a5cec0ca2b`；任务结束删除owned namespace并确认 `NotFound`，原始删除响应保留，原拥有者随后核对精确namespace命令并停止local port-forward PID20664，原session24917退出143，两个agent均只读确认PID已不存在。

## ES 较早版本连接对照

数据库为 Elasticsearch 8.19.22，固定 0.25 核 / 1536 MiB。Weir 总 CPU 为 2 核，单实例 2 核或两个实例各 1 核；每实例后端并发 4、raw owner 上限 5、Route sessions 上限 32、应用 TCP connections 上限 64、声明内存预算 2560 MiB。这里使用编译自 `cmd/weir` 的旧版基线二进制，并非 integration 的服务替身。

数据集只初始化一次，1000 条精确 1 KiB 记录，只读。一个或 16 个真实业务进程分摊相同总 offered rate；每进程复用四条 gRPC 连接或一个直连 HTTP 池（上限四）。每进程有 8 个 worker，16 进程最多 128 个 RPC 在途；sessions32/64 分别只覆盖该最大在途数的四分之一/一半。每读使用一个短 Route，不重试。客户端队列有界为 256，调用 deadline 从计划到达时间起算一秒。测量 20 秒，再保持客户端打开 12 秒，随后关闭；数据库 `/proc/PID/fd` socket inode 与该 PID 的 `net/tcp{,6}` 相交，只计监听端口 9200 的 ESTABLISHED，采样间隔 200 ms。

| Offered/s | 路径 | 成功/计划 | active 连接中位数 | active 范围 | 备注 |
|---:|---|---:|---:|---:|---|
| 200 | 1 进程直连 | 4000/4000 | 4 | 4–4 | 全部成功 |
| 200 | 16 进程直连 | 4000/4000 | 16 | 16–32 | 全部成功 |
| 200 | 16 进程 → 1 Weir | 4000/4000 | 4 | 4–4 | 全部成功 |
| 200 | 16 进程 → 2 Weir | 4000/4000 | 8 | 8–8 | 全部成功 |
| 800 | 1 进程直连 | 16000/16000 | 4 | 4–4 | 全部成功 |
| 800 | 16 进程直连 | 16000/16000 | 64 | 64–64 | 全部成功 |
| 800 | 16 进程 → 1 Weir | 15713/16000 | 4 | 4–4 | 287 次会话 admission 拒绝 |
| 800 | 16 进程 → 2 Weir | 16000/16000 | 8 | 8–8 | 全部成功 |

800/s 单 Weir 的 287 次 `transport_ResourceExhausted` 与实际
`weir_admission_rejections_total{reason="sessions"}` 增量 287 完全一致；
connections、overload、ingress 等拒绝为零。不能把这格的较低数据库连接数描述为无代价的满吞吐收益。800/s 双 Weir 的全部读成功，相比 16 进程直连的 64 个连接，实际连接减至 8 个。两实例的总后端执行并发上限也从 4 增为 8，因此这是连接集中与会话分流验证，不是固定全局数据库执行并发的吞吐对照。

关闭业务客户端后，Weir 的后端 HTTP 池保持连接以复用，符合预期；停止自建 Weir 后，数据库最后五个样本全部为零。每实例 owner peak 4 ≤ limit 5，最终 owned=0、acquired=released=4，没有遗留连接。直连客户端关闭后同样回到零。这里的连接峰值是内核定期采样峰值；严格的生命周期上限另由 owner 的 peak / limit / final conservation 证据支持。

原生产 transport 配置允许 `max_connections <= 64`；16 进程各四条 gRPC 恰为 64，实际 connections admission 拒绝为零，所以使用 64 并没有隐藏连接拒绝。CPU HPA 本身不能保证处理会话容量满的情况，需要同时查看会话拒绝、occupancy，并确保客户端分流到新实例。横向扩展的连接总预算和终止 Pod 重叠说明见 `deploy/kubernetes/scaling.md`。

有效矩阵证据位于 `es-queued`（800/s）与 `es-stable`（200/s 四格及 800/s 双实例），摘要为 `connection-results.json`，原始文件有 `sha256.json`。两组的 containers、networks、images、volumes before/after inventory 精确相同，所有自建对象已回收。

预检失败和测量工具修正亦保留：最初 ES 同 UID 读 fd 被 JVM/内核权限拒绝，随后仅自建数据库容器增加 SYS_PTRACE，并以 root 采样固定 `/proc` 路径；首次预检期间 Docker Desktop 内置 bridge ID 改变，脚本没有操作该 bridge，其余 inventory 不变。早期只读 probe 队列错误地只有八个槽，冷态和预热态分别丢弃 9951、225 次计划读，均为 0 API 失败；改为原负载比较使用的有界 256 队列后，800/s 两个直连格均全部成功。以上失败不进入有效收益矩阵，既未重试数据库请求，也未延长 deadline。

第一轮冻结新版生产 `cmd/weir`（后续 controller v2 的正式结果另报），使用 `max_read_size: 16KiB` / `batch_collect: 3ms`，其余单实例参数相同，在独占 Docker 窗口补了 800/s 三次。下表 CPU 为进程 CPU 秒差除以采样时间，单位核；RSS 为 active 最大 MiB。采样覆盖 18.4–19.1 秒，有 18–19 个样本，数据库没有 OOM。

| 新版格 | 成功/16000 | sessions 拒绝 | DB 实际连接 | DB CPU 核 | Weir CPU 核 | DB RSS MiB | Weir RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| sessions32 第1次 | 14740 | 1260 | 4 | 0.158 | 0.321 | 867.1 | 32.0 |
| sessions32 第2次 | 15794 | 206 | 4 | 0.132 | 0.337 | 867.3 | 32.0 |
| sessions32 第3次 | 15733 | 267 | 4 | 0.137 | 0.350 | 867.6 | 31.6 |
| sessions64 探索 | 15559 | 441 | 4 | 0.164 | 0.296 | 872.9 | 31.2 |

四格全部 0 client_drop，完整 before/after metrics 的 `sessions` 拒绝数分别等于 API 失败数，其余原因包括 connections、overload、ingress 均为零。完整拒绝增量不能用只覆盖 active 部分时间的资源采样增量替代。sessions64 探索声明每实例 4608 MiB 内存，主对照声明 2560 MiB；实际 RSS 仅约 31–32 MiB，本轮小数据试验没有证明允许收窄最坏情况内存预算。未把探索格并入 sessions32 主对照，也未证明单纯调高 sessions 到 64 即可承载全部 800/s。

这些多进程 probe 共用到达时间与相同的 `read-(n%1000)` 序列，产生同步同 key 波次。`runtime` 的 sequence 是 `session.id + key`，不同 RPC 不会因同 key 全局串行；但 `selectLocked` 排除同一后端批内的重复 key。新版三次平均批大小为 1.040、1.009、1.010，sessions64 探索为 1.127。第2次 15660 个批中 15529 个为单条（99.16%）。因此本矩阵适合验证实际连接集中、会话保护与同步热点读，不能代表分散 key 的一般批量效率或吞吐上限。会话容量、每客户端 workers 与数据库执行并发是不同的边界。

新版所有格 owner peak=4≤limit5，停止 Weir 后 owner acquired=released=4，owned=0，数据库最后五样本为零。`es-final` 的 containers、images、volumes 和自建 networks 前后相同，但 Docker Desktop 内置 bridge ID 从 d361… 改为 b295…，原始 `cleanup.restored=false` 已保留，errors=[]；未把这个外部 ID 变化伪报为精确恢复。随后 `es-final-sessions64` 的完整 inventory 精确恢复，errors=[]。所有本任务自建 Docker 对象已回收，并将 Docker 窗口交给后端效率验证。

新版原始证据在 `es-final` 与 `es-final-sessions64`；所有有效与失败格保存在摘要和哈希清单。controller v2 调整在本组 ES 测量完成后发生，不能将这组较早二进制视为 v2 的最终对照。MongoDB 最终 v2 的实际监控连接与多进程连接验证已在上文补齐。此处没有部署 HPA，也没有修改生产环境。


## 二进制身份

以下hash来自各现场fixture真实文件，未把不同控制器版本并成同一最终结果：

| 版本与用途 | Weir SHA256 | helper SHA256 |
|---|---|---|
| ES 旧生产连接矩阵 | `2c17c1a698833ba8d4584bd0c4e360e6b6f2c70984f6119fff6bb206fe1d31e1` | `e31624eb12b06d2b034a0bc605b4c0c9446bdb199bc029ea4e5154efdd6684e8` |
| ES v1 三次与session64探索 | `ff00f5367c070eb4310f69a5275ec92a7388267f94dc284d8e3e8fca60688262` | `baaa8df8832eacc1ae8692f89c985bf25e18ad0f777558d89001fdf9e4de5574` |
| Mongo最终v2四格 | `100ae61e752c312ed22382ce53536a0dfb8572c4a22c56bd643b5b12152c4d8f` | `aaff403cca45426601b2906655ff0fd6460dea91c53d154d267b1c43e1ae4aad` |

Mongo进程sampler SHA `06f18570bc092306ec75f5a097bb3ad7d5ba7a64bf7e69e17150ddbb180b410f`，metrics sampler SHA `fa4d6534ef59c35e90e117119a7f8135f0b319f3435351f7b2698b57ca15fb0d`。整体证据哈希见 [sha256.json](sha256.json)。
