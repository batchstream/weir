# MongoDB 有限 CPU 的背压与恢复验证

最终 v2 在两次同进程 CPU 降低实验中都将窗口 4 收缩到 1，并在低 CPU 区间稳定保持；CPU 恢复后窗口回到 2，恢复约 2,500 次/秒成功吞吐。两次最终 v2 的全部请求 API 失败与 UNKNOWN 均为 0，36,137 个 APPLIED 写入都经独立数据库审计确认。恢复期间各出现一次 2→1→2 短暂波动，不能据此声称完全无振荡或必须恢复到配置上限 4。

对照为“旧实现及原参数”与“新实现及调优参数”的组合效果，不能将吞吐或 CPU 差异单独归因于 controller。旧版仅接受原默认 collect=1ms/read=2MiB；新版显式 collect=3ms/max_read_size=16KiB。两组均 batch=8、pool=4、workers=32、client_queue=32、同一 v2 generator。

测试为 90% 普通 Record Read、10% unique Put，固定 offered=2,500/s，预置 1,000 个读记录。warm=20s、measure=90s；CPU 变更通过 resize subresource 执行，实际 quota 的应用与确认有延迟。确认慢配额后保持至少 30s 再请求恢复，最终恢复阶段覆盖至少 30s。以下阶段吞吐只使用完全位于“已确认配额＋2s 到下一次变更请求”的 10s 窗；跨阶段窗保留原始文件但不并入阶段比较。

Mongo 固定 1,536MiB，CPU limit 500m→50m→500m，WT cache 256MiB、64MiB oplog；Weir limit 2 CPU/2,304MiB、client limit 2 CPU。三个容器的 requests 在最终组恒为 DB 50m、Weir 250m、client 50m，避免降 requests 后其他 Pod 占用调度余量而阻止恢复。所有阶段同一 mongod PID1/start_ticks=18298/containerID/restart=0，实际 cpu.max 为 50000→5000→50000 / 100000。

独立 namespace `weir-bp-9282b4b43a1e`，唯一 owner 标签；ARM 节点 `ip-172-31-159-242.us-west-1.compute.internal`。节点实际 CPU 在环境启动前 15m/0%、试验中采样 3582m/45%、最后无负载时 40m/0%。这仍是共享节点验证，不代表独占机器容量。

## 实际 CPU、RSS 与控制窗口

CPU 为被观察进程 /proc ticks 的平均核数；RSS 为阶段峰值 MiB；不将预留字节当成实际内存。DB sampler 在 database 容器观察 PID1=mongod，Weir sampler 在 weir 容器观察对应 server PID；CLK_TCK=100、page=4096 已现场核验。

| 组 | 阶段 | DB CPU 核 | DB RSS MiB | Weir CPU 核 | Weir RSS MiB | 窗口 min..max/末值 | 成功/s（完整窗） | p99 计划到完成 ms |
|---|---|---:|---:|---:|---:|---|---|---|
| mongo-current-2 | healthy | 0.3510 | 310.01 | 1.2045 | 24.79 | 4..4/4 | warm20s: 2500.00 | 见 raw warm histogram |
| mongo-current-2 | slow | 0.0478 | 311.70 | 0.1555 | 26.09 | 1..1/1 | 290.4/296.0 | 494.0/492.0 |
| mongo-current-2 | recovery | 0.3388 | 326.27 | 1.1900 | 25.05 | 1..2/2 | 2500.0/2500.0/2476.3 | 8.6/8.8/9.2 |
| mongo-baseline-3 | healthy | 0.4985 | 328.46 | 1.2850 | 25.05 | 4..4/4 | warm20s: 2440.85 | 见 raw warm histogram |
| mongo-baseline-3 | slow | 0.0448 | 328.58 | 0.1218 | 25.93 | 4..4/4 | 165.6/167.8/156.0 | 900.0/896.0/993.0 |
| mongo-baseline-3 | recovery | 0.4993 | 329.00 | 1.2897 | 25.41 | 4..4/4 | 2372.1/2443.3/2443.3 | 41.0/39.0/39.0 |
| mongo-current-3 | healthy | 0.3315 | 327.41 | 1.1730 | 25.03 | 4..4/4 | warm20s: 2500.00 | 见 raw warm histogram |
| mongo-current-3 | slow | 0.0470 | 329.48 | 0.1527 | 26.25 | 1..1/1 | 290.4/265.6 | 700.0/899.0 |
| mongo-current-3 | recovery | 0.3269 | 330.03 | 1.1675 | 25.30 | 1..2/2 | 2500.0/2455.3/2500.0 | 8.4/9.7/8.2 |
| mongo-baseline-4 | healthy | 0.4990 | 330.18 | 1.2940 | 26.05 | 4..4/4 | warm20s: 2471.55 | 见 raw warm histogram |
| mongo-baseline-4 | slow | 0.0455 | 330.29 | 0.1235 | 26.22 | 4..4/4 | 181.7/188.1 | 891.0/894.0 |
| mongo-baseline-4 | recovery | 0.4990 | 330.51 | 1.3103 | 26.17 | 4..4/4 | 2459.7/2475.4/2477.2 | 32.0/30.0/29.0 |

最终 v2 低配额阶段 DB CPU 0.0478/0.0470 核，接近 0.05 核上限；窗口为 1 的全部保守阶段样本没有反复 cooldown。平均 backend invocation 为 27.17/27.97ms，旧版为 78.26/71.72ms；这同时含批读优化、收集参数及并发收缩的效果。新版恢复约 2,500/s 时 DB CPU 为 0.339/0.327 核，旧版约 0.499 核。RSS 随同一个数据库缓存逐轮暖化，逐格峰值供审阅，不作跨格内存节省的因果结论。

## 全部测量期的请求结果

下表包含 90s 测量期及跨阶段窗；每格 offered=225,000。client_drop 是 generator 队列/到达预算拒绝后尚未开始的请求，不能算数据库错误；API failed 是已开始调用的终端失败，UNKNOWN 只针对写入不确定性。

| 组 | 成功 | client drop | API failed | UNKNOWN | APPLIED（全110s）/审计 |
|---|---:|---:|---:|---:|---|
| mongo-current-2 | 130819 | 94181 | 0 | 0 | 18059，全部存在；unknown_found=0 |
| mongo-baseline-3 | 117803 | 107170 | 27 | 4 | 16481，全部存在；unknown_found=2 |
| mongo-current-3 | 130122 | 94878 | 0 | 0 | 18078，全部存在；unknown_found=0 |
| mongo-baseline-4 | 120837 | 104075 | 88 | 9 | 16902，全部存在；unknown_found=9 |

最终 v2：450,000 offered 中 260,941 成功、189,059 client drop、API failed=0、UNKNOWN=0。旧版：238,640 成功、211,245 client drop、API failed=115（110 DeadlineExceeded、5 Internal）、UNKNOWN=13；33,383 个 APPLIED 都存在，另有 11 个 UNKNOWN 写入在数据库中找到。所有四格 audit error 均为 nil。较高 drop 反映低配额下的有限容量与有限客户端队列，未被省略。

## 控制动作与 hold

所有最终 v2 backend 拒绝事件为 0，因此 4→1 是成功慢响应触发，发生于 CPU 下降请求之后。各有 3 次低配额 latency reduction，以及恢复期 1 次额外 reduction。`weir_store_latency_recovery_hold` 保留最近 1s 饱和慢证据，阻止快速 burst 或另一目标的快速完成提前增长；它不暂停 dispatch。成功慢响应在 floor1 继续单并发，不反复生成 cooldown；明确 Congested 保留独立 cooldown。

- mongo-current-2: +0.57s:4, +29.57s:1, +72.57s:2, +104.57s:1, +105.57s:2; final latency/backend events=4/0。
- mongo-baseline-3: +0.42s:4; final latency/backend events=0/0。
- mongo-current-3: +0.47s:4, +29.47s:3, +30.47s:1, +75.47s:2, +94.47s:1, +97.47s:2; final latency/backend events=4/0。
- mongo-baseline-4: +0.19s:4; final latency/backend events=0/0。

完整 hold、cooldown、profile warm count、normalized latency ratio、pending/active/window、事件和 resource 样本见各 `analysis.json`、`summary.json` 及 raw archives。采样频率 1s，不能声称观察了每个毫秒级窗口变化。

## 保留的反例与限制

- `intermediate/mongo-current-pilot`：batch32、queue256 时成功 latency ratio 到 261×但无收缩；大批次减少在途数量和新的 batch profile 会阻碍饱和证据。恢复 quota 因 requests 释放后被其他调度占用而延迟约 104s，超出负载，故不算正式恢复证据。
- `intermediate/mongo-current-1`：v1 batch8 曾 4→3，但 CPU 仍低时快速 CFS burst 恢复4；queue256 也使客户端等待耗尽到达起算的 1s 期限。这是 v2 保留有限慢证据及恢复 hold 的来源。
- `intermediate/mongo-baseline-1/2` 分别是 queue256/v1 generator 的辅助对照，不混入最终四格。同容量阶段只能证明此次工作负载的效果。
- 目标、操作类别、批大小桶、请求平均字节形成至多64个可比基线；实际回复大小、读写比例或高目标 churn 仍可能改变成本，不能将相对延迟控制解释为通用最优容量估计。
- 此 fixture 使用 1GiB tmpfs data emptyDir，普通记录小、每 trial 重建同一 `weir_load.records`；df/memory.events 证明未触顶或OOM。它不测断电/重启数据持久性，也不代表磁盘大工作集。

## 可复查材料

`raw.tar.gz` 保存 generator 每窗结果、APPLIED ledger/audit、DB/Weir逐秒资源样本、完整 Prometheus 指标、进程 PID 和退出日志。`manifest.json` 保存实际 quota 变更请求/确认时间、进程身份、远端 binary SHA、archive SHA。`remote-binary-hashes.txt` 与 `v2-uploaded.json` 固定最终 artifacts；旧 baseline SHA: `2c17c1a698833ba8d4584bd0c4e360e6b6f2c70984f6119fff6bb206fe1d31e1`。

最终 Weir SHA: `100ae61e752c312ed22382ce53536a0dfb8572c4a22c56bd643b5b12152c4d8f`；generator SHA: `aaff403cca45426601b2906655ff0fd6460dea91c53d154d267b1c43e1ae4aad`。

实验 helper 为显式 opt-in，只操作唯一 owned namespace；复制环境后必须重新提供正确 owner、ARM artifacts 与网络隔离。正式 production v2 在现场期间保持冻结。

背压结束后全部负载、Weir、sampler 已退出，DB 恢复0.5核并明确交接 connection_validation。连接验证完成后，该 agent 删除唯一 owned namespace；独立 `kubectl get namespace` 确认 NotFound。本地 PID20664 经精确 namespace/命令核验后 SIGTERM，session退出143，`ps` 确认不存在。完整证据见 `cleanup.json`，未清理其他 namespace 或进程。
