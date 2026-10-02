# MongoDB v3 的实际背压与恢复验证

两次 v3 试验均在 MongoDB CPU 配额下降时将窗口 4 收缩到 1，并在实际 0.05 核阶段持续保持。恢复到 0.5 核后，两次都观察到窗口 1→2→3 的回升，随后也出现再次收缩。恢复吞吐分别为自身初始健康档的 102.9% 和 90.5%；不能声称无振荡、已完全承载 2,500/s，或所有恢复问题已消除。

测量期两格合计 offered=450,000，成功=245,758，client drop=204,242，API failed=0、UNKNOWN=0；完整 110s 的 33,636 个 APPLIED 写入全部经独立数据库审计确认。client drop 为开始调用前的客户端队列拒绝，没有算作数据库错误。

## 条件与阶段边界

同一 mongod PID1/start_ticks=54728/containerID=3f22f08b8b1c2278663a01dec3aed81ce6f4e82d1ba62641487e52a99a619665/restart0，实际 cpu.max 每轮均为 50000→5000→50000 /100000。DB requests 始终为50m；memory1536Mi、WT256Mi、oplog64Mi。数据使用普通磁盘 emptyDir 4Gi，最终使用约311Mi。Weir 为2核/2304Mi，pool4、sessions32、connections16、batch8、collect3ms、read16KiB；client workers32、queue32，90%普通读/10%unique Put，预置1000个读记录，warm20s＋measure90s，固定offered2500/s。

慢配额确认后至少30s才请求恢复。资源和控制统计排除确认后的2s过渡，阶段吞吐只用完全包含的10s测量窗。成熟脚本在warm结束约6s后请求慢配额，因此没有完整纯健康10s测量窗；健康吞吐使用完全处于原配额的20s warm，不能把首个跨阶段measure窗伪称健康值。全部跨阶段/过渡窗单独保留在analysis.json。

## 每轮吞吐与延迟

| 轮次 | 健康20s成功/s | 健康p99计划到完成ms | 慢阶段纯窗成功/s | 恢复纯窗成功/s | 恢复p99计划到完成ms | 恢复/自身健康 |
|---|---:|---:|---|---|---|---:|
| mongo-v3-1 | 2090.4 | 216 | 416.0/464.0 | 2253.6/2105.8/2092.6 | 93.0/143.0/153.0 | 102.9% |
| mongo-v3-2 | 2445.6 | 48 | 439.7/376.4 | 2173.2/2237.7/2231.3 | 75.0/96.0/143.0 | 90.5% |

第一轮恢复延迟与吞吐回到自身健康水平；第二轮恢复仍比自身健康吞吐低约9.5%，且p99较48ms健康值升到75–143ms，应保留为残留波动。仅CPU有余量不能证明控制器是唯一原因：该环境使用普通磁盘，节点也共享其他负载，未采集磁盘IO延迟。

## 实际资源与控制动作

| 轮次 | 阶段 | DB process/cgroup CPU核 | DB RSS峰值MiB | Weir process/cgroup CPU核 | Weir RSS峰值MiB | window min..max/末值 | hold阳性采样占比 | execution均值ms / p99桶上界ms |
|---|---|---:|---:|---:|---:|---|---:|---:|
| mongo-v3-1 | healthy | 0.2090/0.2095 | 211.03 | 0.5990/0.5994 | 26.08 | 4..4/4 | 0.0% | 5.86/100 |
| mongo-v3-1 | slow | 0.0467/0.0473 | 216.94 | 0.1345/0.1355 | 26.65 | 1..1/1 | 100.0% | 18.49/1000 |
| mongo-v3-1 | recovery | 0.2071/0.2072 | 236.26 | 0.5945/0.5953 | 26.70 | 1..3/1 | 87.5% | 3.40/100 |
| mongo-v3-2 | healthy | 0.2395/0.2394 | 255.33 | 0.6925/0.6933 | 26.06 | 4..4/4 | 0.0% | 3.35/100 |
| mongo-v3-2 | slow | 0.0464/0.0469 | 258.93 | 0.1318/0.1324 | 26.59 | 1..1/1 | 100.0% | 19.24/1000 |
| mongo-v3-2 | recovery | 0.2159/0.2164 | 276.05 | 0.6206/0.6214 | 26.68 | 1..3/1 | 78.8% | 3.19/100 |

DB慢阶段CFS throttled periods为329/330及306/330，恢复阶段两轮均0；两轮DB/Weir各阶段OOM kill增量均0。恢复hold阳性占比87.5%/78.8%是1Hz采样比例，不能解释为精确占用时间。窗口曾回升到3，排除了永久保持1的断言，但不排除恢复控制仍较保守。execution tail来自0.001/0.01/0.1/1/10秒累计桶，表中p99为桶上界，不是精确分位数。

两轮final latency/backend events分别8/0、10/0；恢复纯阶段增加5/6次latency event。低CPU稳定阶段没有反复cooldown，恢复期各有2个采样cooldown阳性。完整hold、cooldown、profile、latency ratio与窗口轨迹保存在analysis.json/summary.json和raw.tar.gz。

## 请求与审计

| 轮次 | 测量期成功 | client drop | API failed | UNKNOWN | APPLIED完整110s | 审计 |
|---|---:|---:|---:|---:|---:|---|
| mongo-v3-1 | 122192 | 102808 | 0 | 0 | 16369 | 全部存在、audit error nil、unknown_found0 |
| mongo-v3-2 | 123566 | 101434 | 0 | 0 | 17267 | 全部存在、audit error nil、unknown_found0 |

每轮unique写入ledger包含27500个计划写；未调用的写入在audit中为absent，并非APPLIED丢失。每轮重新建立weir_load.records的1000个读记录；raw保留完整调用结果、ledger与逐页audit。

## 环境、版本与清理

原优先节点ip-172-31-159-242已被集群回收。第一次预检在任何创建之前返回NotFound，证据保留于node-preflight-change.json/preflight-cleanup.json。替代节点ip-172-31-191-104.us-west-1.compute.internal为ARM m8g.2xlarge/8CPU；创建前实时847m/10%，创建后868m/10%，结束附近3422m/43%，清理后3339m/42%。这是共享节点测量，不是独占机器容量。全程未接触root性能测试node12-243。

旧v2背压证据使用tmpfs 1Gi和node159，v3使用普通磁盘4Gi和node191。仅以每轮自身健康→慢→恢复来判断恢复，旧baseline未重跑，不将跨环境吞吐/CPU差异单独归因于controller升级。v2连接代码证明保持，未重复连接测试。

唯一owner/namespace为weir-bp-v3-f53d66750b94，namespaceUID=8c755cef-7f36-4da4-bd84-278ab16ccc9a，PodUID=bc52d707-2ee5-42c6-88e7-c74937d10032。两轮负载、Weir、sampler停止后导出raw并校验SHA；删除唯一owned namespace并独立再次确认NotFound。本地精确Popen port-forward PID32579（19184:8080）退出-15，ps确认PID不存在。cleanup.json/errors=[]，没有留下云负载或本地forward。

最终冻结binary（linux/arm64）：

- client-final: `aaff403cca45426601b2906655ff0fd6460dea91c53d154d267b1c43e1ae4aad`
- weir-final: `4054ca6a7ef9d12886566124bb5153591d7beacb5cf83949de34fe6a058eee87`
- sampler: `06f18570bc092306ec75f5a097bb3ad7d5ba7a64bf7e69e17150ddbb180b410f`
- sampler-metrics: `fa4d6534ef59c35e90e117119a7f8135f0b319f3435351f7b2698b57ca15fb0d`

controller.go source SHA256: `c2986947c939ade6fe2e5f513cc3b24dd4002a78444dbe489f0961d2c97f07d0`。root全量测试与构建证据见../../build-manifest-v3.json；本目录保留源archive与canonical member rename的逐字节hash核验，未修改binary。

phase-runner.py、fixture-runner.py、analyze.py、各轮manifest.json/analysis.json/raw.tar.gz及SHA256SUMS可复查。raw直接保留Prometheus/cgroup/PID/CPU/RSS/请求结果；分析从archive只读解析，不覆盖v2档案。
