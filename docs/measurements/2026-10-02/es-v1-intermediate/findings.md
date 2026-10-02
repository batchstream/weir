# ES v1 中间版本资源对照（2026-10-02）

该宽矩阵使用 CFS 恢复抑制修正之前的 v1（SHA 见 provenance），最终 v2 确认另行报告，不能混用身份。ES 8.19.22 Linux arm64，DB 固定 0.25 CPU / 1536 MiB，Weir 2 CPU / 2304 MiB；单负载进程、32 workers、32 batch operations。所有路径同一最终生成器；4 HTTP/store 并发。每格 3 配对重复、10s warm + 20s measurement，先共同以 1 CPU 训练 JVM，每次重建相同 corpus；1024B 文档、1000 个均匀散列读 key、独立写 ID、无重试。

old-baseline/new-default 都保留 1ms collect 与 2MiB 普通 Read 预算；这里 default 指保留这些默认值，session/batch 的测试容量与旧版本固定相同。new-tuned 的声明与筛选结果见 results.json，不能将 16KiB 声明泛化到任意大文档。表格为重复中位数，原始值及 min/max 完整保留。p95 是成功请求从计划到达开始的延迟；client drop 与 API 错误独立列出。

|负载|offered/s|路径|成功/s|p95 ms|DB CPU|Weir CPU|DB RSS MiB|Weir RSS MiB|drop/API error（3次合计）|
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
|90% Read / 10% Put|800|direct|800.0|1.4|0.092|0.000|904.1|0.0|0/0|
|90% Read / 10% Put|800|old-baseline|800.0|3.8|0.126|0.295|902.4|26.2|0/0|
|90% Read / 10% Put|800|new-default|800.0|3.6|0.121|0.285|903.1|25.7|41/0|
|90% Read / 10% Put|800|new-tuned|800.0|5.6|0.077|0.226|903.6|25.3|0/0|
|90% Read / 10% Put|3200|direct|3103.8|84.0|0.204|0.000|932.8|0.0|4990/0|
|90% Read / 10% Put|3200|old-baseline|3152.4|48.0|0.203|0.662|931.8|27.3|3415/0|
|90% Read / 10% Put|3200|new-default|3196.2|29.0|0.195|0.631|932.4|27.5|359/0|
|90% Read / 10% Put|3200|new-tuned|3107.6|12.0|0.133|0.560|932.6|27.2|4091/0|
|100% Put|800|direct|800.0|1.6|0.100|0.000|951.0|0.0|0/0|
|100% Put|800|old-baseline|800.0|4.2|0.132|0.310|951.0|26.9|0/0|
|100% Put|800|new-default|800.0|3.7|0.123|0.289|950.2|25.5|0/0|
|100% Put|800|new-tuned|800.0|5.7|0.072|0.230|950.8|26.1|0/0|
|100% Put|3200|direct|3092.8|78.0|0.217|0.000|959.2|0.0|6854/0|
|100% Put|3200|old-baseline|3199.4|23.0|0.155|0.628|960.9|26.2|471/0|
|100% Put|3200|new-default|3171.6|27.0|0.158|0.601|962.1|27.4|1432/0|
|100% Put|3200|new-tuned|3179.6|7.3|0.121|0.577|961.7|27.5|821/0|

CPU 为进程真实 CPU seconds 的采样差值；DB /proc 与 cgroup 原始样本、Weir Prometheus CPU/RSS、memory.current、OOM、ES rejection/queue、生成器 CPU/RSS、采样错误与所有 APPLIED 落库审计保留在原始归档。预算不是实际 RSS；DB JVM/RSS 可能随测试顺序漂移，应结合重复范围判断。

## 实际 CPU 配额下降与恢复

同一最终 Weir 进程、pool4/read16KiB/collect1ms，mixed 3200/s；观测首个实际 Execute 后，DB .25CPU 20s → .08CPU 30s → .25CPU 30s；客户端连续 90s，并完整审计。两个重复独立重启 Weir。

|重复|阶段|quota|真实 DB CPU|窗口始→末[min,max]|latency/backpressure events|ratio max|pending max|
|---:|---|---:|---:|---|---|---:|---:|
|0|baseline|0.25|0.158|4→4[4,4]|0/0|156.89|30|
|0|limited|0.08|0.065|4→4[4,4]|0/0|449.58|18|
|0|restored|0.25|0.161|4→4[4,4]|0/0|84.97|18|
|1|baseline|0.25|0.158|4→4[4,4]|0/0|95.30|21|
|1|limited|0.08|0.063|4→4[4,4]|0/0|612.30|27|
|1|restored|0.25|0.164|4→4[4,4]|0/0|154.31|14|

恢复结果与阶段内每 10 秒成功吞吐、p95、API error、client drop 见 results.json；原始 controls 包含实际 quota、容器 ID、DB PID 和完整指标。取消会隔离时延学习，不能把未知/取消结果视为 DB 健康。

清理结果：{"pass": true, "errors": [], "inventory_restored": true, "non_default_inventory_restored": true, "default_bridge_id_changed": false, "remaining_owned_containers": [], "remaining_owned_networks": []}。

证据：[results.json](results.json)、[原始归档（含完整summary.json）](raw-evidence.tar.xz)、[SHA256](sha256.json)。
