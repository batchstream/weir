# ES v2 参数与直连性能参考（中间版本，2026-10-02）

v2 冻结时性能（实际 SHA/source manifest 完整保留；后续v3慢比例控制修正单独验证），同一 client-v2、同一 DB/JVM、同 64 workers；DB 0.25 CPU / 1536 MiB，Weir 2 CPU / 4608 MiB。direct HTTP pool4，Weir pool2 / collect5ms / max_read_size16KiB / batch32 / sessions64。每格3配对重复、10s warm+20s measurement、交替路径顺序；共同以1CPU训练JVM，每次重建相同1024B文档corpus，读1000均匀散列key、写独立ID、无重试。

最终调参完整地包含客户端并发、Weir会话/内存预算、批量窗口及普通读取profile，不能只归因于代码。direct与Weir使用相同最终生成器和并发，DB预算保持固定。16KiB声明不适合任意大文档；普通Read超限安全拒绝，写入/Lua/Scan/Native保留既有预算。中间v1四路径矩阵另行保留，不冒充最终代码。表格为重复中位数，p95从成功请求的计划到达开始计时；原始min/max、client drop、API error、UNKNOWN与所有APPLIED审计均保留。

|负载|offered/s|路径|成功/s|p95 ms|DB CPU|Weir CPU|DB RSS MiB|Weir RSS MiB|drop/API error（3次合计）|
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
|90% Read / 10% Put|800|matched-direct|800.0|1.7|0.095|0.000|890.0|0.0|0/0|
|90% Read / 10% Put|800|new-tuned|800.0|7.5|0.069|0.212|889.9|25.7|0/0|
|90% Read / 10% Put|3200|matched-direct|3155.6|74.0|0.209|0.000|899.5|0.0|3417/0|
|90% Read / 10% Put|3200|new-tuned|3150.7|21.0|0.121|0.541|898.8|27.4|4179/0|
|100% Put|800|matched-direct|800.0|9.8|0.104|0.000|937.0|0.0|0/0|
|100% Put|800|new-tuned|800.0|7.4|0.058|0.215|925.1|25.9|0/0|
|100% Put|3200|matched-direct|3119.6|84.0|0.218|0.000|941.7|0.0|4762/0|
|100% Put|3200|new-tuned|3167.9|17.0|0.111|0.575|947.0|27.6|2235/0|

CPU 为进程真实 CPU seconds 的采样差值；DB /proc 与 cgroup 原始样本、Weir Prometheus CPU/RSS、memory.current、OOM、ES rejection/queue、生成器 CPU/RSS、采样错误与所有 APPLIED 落库审计保留在原始归档。预算不是实际 RSS；DB JVM/RSS 可能随测试顺序漂移，应结合重复范围判断。

实际 CPU 下降与恢复在独立 v2 quota fixture 报告；本矩阵只评估固定 .25CPU。主 runner 在全部24格完成审计后受控停止，尚未执行旧参数可选phase；随后 finally inventory 清理成功，controlled-stop.json保留。

高负载client drop仍存在，不能声称任意负载均达到3200/s；batch等参数只对该1024B/均匀key实验有效。

清理结果：{"pass": true, "errors": [], "inventory_restored": true, "non_default_inventory_restored": true, "default_bridge_id_changed": false, "remaining_owned_containers": [], "remaining_owned_networks": []}。

证据：[results.json](results.json)、[原始归档（含完整summary.json与histograms）](raw-evidence.tar.xz)、[SHA256](sha256.json)。
