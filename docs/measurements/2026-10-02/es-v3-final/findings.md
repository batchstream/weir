# ES v3 最终参数与配额恢复验证（2026-10-02）

冻结weir-v3与client-v2身份见manifest及原始归档；ES8.19.22/Linux arm64、DB固定0.25CPU/1536MiB，Weir2CPU/4608MiB。性能矩阵使用64workers/sessions、batch32、pool2、collect5ms、普通Read16KiB，direct HTTP pool4；相同生成器，800/3200输入，90%Read/10%Put与纯Put，每格3次新配对重复、交替路径顺序、10s warm+20s measurement，无重试。1024B文档、1000均匀散列读key及独立写ID；每次重建corpus。共同JVM预热，完整CPU/RSS/cgroup/APPLIED审计保留。

参数调优包括客户端并发、会话/内存预算、收集窗与普通Read预算，不能只归因于代码。16KiB只适合有相应记录大小上界的读取；热点同key去重未实现。表格是重复中位数，min/max、所有drop/APIerror/UNKNOWN完整保留。成功p95从计划到达开始计时。

|负载|输入/s|路径|成功/s|p95 ms|DB CPU|Weir CPU|DB RSS MiB|Weir RSS MiB|drop/API/UNKNOWN（三次合计）|
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
|90%Read/10%Put|800|matched-direct|800.0|1.7|0.098|0.000|897.1|0.0|0/0/0|
|90%Read/10%Put|800|new-tuned|800.0|7.5|0.061|0.203|896.7|25.9|0/0/0|
|90%Read/10%Put|3200|matched-direct|3153.4|63.0|0.200|0.000|921.4|0.0|6294/0/0|
|90%Read/10%Put|3200|new-tuned|3140.7|9.7|0.111|0.514|910.1|27.8|3098/0/0|
|100%Put|800|matched-direct|800.0|44.0|0.117|0.000|944.6|0.0|0/0/0|
|100%Put|800|new-tuned|800.0|8.3|0.067|0.209|932.7|27.5|0/0/0|
|100%Put|3200|matched-direct|3097.1|85.0|0.221|0.000|950.1|0.0|8418/0/0|
|100%Put|3200|new-tuned|3193.9|9.8|0.102|0.561|954.6|27.8|1159/0/0|

## 实际配额下降与恢复

背压使用单独profile：32workers/sessions、batch8/pool4/collect3ms/Read16KiB/client_queue32、Weir2CPU/2304MiB。固定mixed2500/s、连续90s。首次实际Execute后：DB0.5CPU20s → 0.08CPU30s → 0.5CPU30s，两次独立重启Weir；ES容器、权威serverPID、startticks、StartedAt/restart0保持一致，逐次核验真实cpu.max。首20s为本Weir时延baseline建立阶段，预热和ready_profile记录均保留。

两个独立健康pilot均完整保留：第一次measurement窗口恒4、无decrease、API/UNKNOWN0，成功2441.75/2500，drop1165/50000；过严零drop守卫停止了该fixture。第二次成功2282.9/s、drop4342、API/UNKNOWN0，窗口3→2[min1,max3]、decrease5/increase4，hold非持续全1；因此不能宣称2500下健康窗口恒4。第二次audit后cat仪表命令在scratch镜像不可用，单独记录且不冒充负载错误。新fixture固定2500直接做两phase，没有再次健康pilot或恒4运行守卫，所有短时收缩/恢复及失败都保留。跨阶段的10s窗仅列为跨界，不作纯阶段对比。

|重复|阶段|quota|DB CPU|窗口始→末[min,max]|latency/backend事件|hold样本/总样本|
|---:|---|---:|---:|---|---|---|
|r000|baseline|0.5|0.245|4→4[4,4]|0/0|0/35|
|r000|limited|0.08|0.065|2→1[1,2]|3/0|4/12|
|r000|restored|0.5|0.179|2→4[2,4]|0/0|0/54|
|r001|baseline|0.5|0.134|4→4[4,4]|0/0|0/36|
|r001|limited|0.08|0.063|1→1[1,1]|6/0|9/15|
|r001|restored|0.5|0.135|1→3[1,3]|0/0|1/54|

阶段内每10s成功吞吐、p95、API错误、drop、UNKNOWN和实际DBCPU见results.json。UNKNOWN不视作成功；各run APPLIED全量落库审计、UNKNOWN实际落库与缺失单独记录。DB/JVM RSS没有明显降低的承诺；新增Weir会消耗自己的CPU/内存。

性能矩阵24run共有777,166个APPLIED（包含10s warm阶段），全部查到version1；APIerror/UNKNOWN/OOM均为0。800/s mixed成功p95从1.7ms增加到7.5ms，收集窗有明确时延代价；3200/s仍存在client drop，不能称任意负载满承载。实际WeirCPU约0.20–0.56core、RSS中位25.9–27.8MiB，DB/JVM RSS没有一致下降。

极低0.08CPU两正式phase共有API失败1207/742、UNKNOWN128/77；DeadlineExceeded为主，另有Internal和低额段sessions拒绝20/12。未知写入实际落库14/6；全部已确认APPLIED14392/15161均查到。窗口收缩提供了背压，但这个配额/期限组合仍无法消除超时与写入不确定性，不能称过载零失败或无数据不确定性。低额DB cgroup CPU约0.0784/0.0786core（配额约98%）、CFS throttle25.09/24.34s；恢复后的纯60–70/70–80s窗分别2465.7/2498.6与2500/2481.3成功/s，API/UNKNOWN0、p95约5.1–5.8ms。第二轮恢复窗口止于3也已恢复吞吐，不以必须回4单独判成败。

清理：{"pass": true, "errors": [], "inventory_restored": true, "non_default_inventory_restored": true, "default_bridge_id_changed": false, "remaining_owned_containers": [], "remaining_owned_networks": []}。初次pilot清理：{"pass": true, "errors": [], "inventory_restored": false, "non_default_inventory_restored": true, "default_bridge_id_changed": true, "remaining_owned_containers": [], "remaining_owned_networks": []}。仅默认bridge ID漂移不会被误称全inventory一致；所有owned资源必须为空且non-default inventory一致。

证据：[results.json](results.json)、[完整原始归档](raw-evidence.tar.xz)、[SHA256](sha256.json)。
