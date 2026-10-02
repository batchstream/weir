# ES v2 实际 CPU 下降与恢复（2026-10-02）

此实验与吞吐矩阵使用独立 owned fixture。冻结 weir-v2/client-v2 SHA 及 source manifest 保留；ES8.19.22，DB1536MiB、同 Java PID/startticks/StartedAt/restart0，Weir2CPU/2304MiB、单进程。每个重复重启 Weir，固定 batch8/pool4/worker32/session32/read16KiB/collect3ms/client_queue32；JVM先按同profile预热，连续3200/s mixed90/10负载的前20s 0.25CPU建立本进程时延baseline。

实际DB quota 0.25→.08→0.25：控制前读取并核验 cpu.max 和进程身份；每0.5s采样完整Prometheus、/proc、cgroup、ES队列与RSS；所有drop/APIerror/UNKNOWN和APPLIED审计完整保留。阶段跨界的10s窗口不当作单阶段结果。

## 实际 CPU 配额下降与恢复

v2背压单独使用batch8/worker32/session32/Weir2304MiB/pool4/read16KiB/collect3ms/client_queue32以形成真实store积压，不能把其性能与上方batch32吞吐配置混用。连续mixed3200/s：观测首个Execute后DB .25CPU20s → .08CPU30s → .25CPU30s；完整90s并审计。两个重复独立重启Weir、DB保持同PID/startticks/DockerStartedAt/restart0。实际cpu.max与recovery_hold样本在JSON和原始controls中。

|重复|阶段|quota|真实 DB CPU|窗口始→末[min,max]|latency/backpressure events|ratio max|pending max|
|---:|---|---:|---:|---|---|---:|---:|
|0|baseline|0.25|0.220|4→1[1,4]|3/0|201.88|31|
|0|limited|0.08|0.066|1→1[1,1]|0/0|754.34|29|
|0|restored|0.25|0.216|1→1[1,1]|0/0|184.90|24|
|1|baseline|0.25|0.166|4→4[4,4]|0/0|195.41|7|
|1|limited|0.08|0.064|3→1[1,3]|3/0|1058.82|28|
|1|restored|0.25|0.162|1→1[1,1]|0/0|113.70|24|

重复0在初始.25CPU阶段4→1；重复1初始4→4，limited阶段才3→1。两者恢复到.25CPU仍停在1。重复1完整恢复窗口60–70s成功2861.2/s/drop3388、70–80s3150.6/s/drop494（均API/UNKNOWN0）；DB proc/cgroup分别.1681/.1841和.1522/.1701。80–90s跨声明恢复段末端，另列而不混纯阶段。不能把此实验报告为完整稳定恢复成功；后续v3确认慢比例以过滤稀少尖峰，原始证据保持。

恢复结果与阶段内每 10 秒成功吞吐、p95、API error、client drop 见 results.json；原始 controls 包含实际 quota、容器 ID、DB PID 和完整指标。取消会隔离时延学习，不能把未知/取消结果视为 DB 健康。

清理结果：{"pass": true, "errors": [], "inventory_restored": true, "non_default_inventory_restored": true, "default_bridge_id_changed": false, "remaining_owned_containers": [], "remaining_owned_networks": []}。

证据：[results.json](results.json)、[原始归档（含完整summary.json与histograms）](raw-evidence.tar.xz)、[SHA256](sha256.json)。
