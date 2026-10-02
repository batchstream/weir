# ES v4 同资源公平恢复对照（2026-10-02）

冻结 v3/v4 使用同一新 helper，ES 8.19.22/Linux arm64、同一权威 Java PID/startticks/容器、逐段核验真实 cpu.max 50000→8000→50000 /100000、DB 1536MiB。输入固定2500/s、90%Read/10%Put、1s caller deadline、20s warm +120s measurement。降额在首次实际执行40s后，限额30s后恢复。API失败仍失败；已验证写确认与 UNKNOWN 分开统计，没有自动重试。

ABBA 顺序 v3/v4/v4/v3，两次/版本；压力 profile w32/s32/batch8/pool4/3ms/Read16KiB/queue32、Weir2CPU/2304MiB。v4另两轮仅 batch32/pool2/5ms 参数调优，w32/s32/内存不变。温暖期从健康统计排除；临界10s窗不作为纯阶段对比。观察CPU/窗口的采样时间先于顺序读取，配额变更请求前戳缺失；阶段时间仅保守/近似。

|run|版本/参数|全measurement成功/s|API DL/Internal/ResourceExhausted|UNKNOWN|drop|APPLIED审计/UNKNOWN实存|APPLIED但RPC失败|
|---|---|---:|---|---:|---:|---|---:|
|r000|bp-v3|1818.6|1397/119/17|141|80232|26602/9|0|
|r001|bp-v4|1871.5|1025/54/14|121|74329|27355/19|0|
|r002|bp-v4|1925.5|100/6/2|13|68832|28103/3|0|
|r003|bp-v3|1935.8|127/9/6|15|67566|28199/5|0|
|r004|bp-v4-tuned|2033.8|67/0/4|7|55878|29382/3|0|
|r005|bp-v4-tuned|2066.6|70/5/2|7|51931|29806/5|0|

|run|纯阶段|成功/s|成功p95 ms|API/UNKNOWN/drop|DB CPU process/cgroup|Weir CPU/RSS MiB|窗口起→末[min,max]|hold/样本|
|---|---|---:|---:|---|---|---|---|---|
|r000|baseline|2492.1|5.4|0/0/79|0.168/0.187|0.454/26.8|4→4[4,4]|0/21|
|r000|limited|7.1|1010.0|573/54/24356|0.066/0.077|0.100/27.6|1→1[1,1]|1/6|
|r000|restored|2453.0|5.6|0/0/2350|0.164/0.182|0.449/27.5|2→2[2,4]|3/119|
|r001|baseline|2500.0|5.0|0/0/0|0.123/0.140|0.453/24.7|4→4[4,4]|0/20|
|r001|limited|35.6|997.0|417/46/24227|0.064/0.078|0.107/27.9|1→1[1,1]|1/7|
|r001|restored|2489.1|5.1|0/0/547|0.133/0.151|0.463/27.8|3→4[3,4]|0/120|
|r002|baseline|2479.7|5.2|0/0/203|0.139/0.156|0.468/24.8|4→4[4,4]|0/20|
|r002|limited|299.3|514.0|69/9/21938|0.062/0.080|0.152/25.8|1→1[1,1]|15/15|
|r002|restored|2496.5|5.0|0/0/174|0.126/0.144|0.467/25.1|2→3[2,3]|0/120|
|r003|baseline|2500.0|4.9|0/0/0|0.114/0.132|0.447/24.6|4→4[4,4]|0/21|
|r003|limited|196.0|797.0|115/12/22925|0.062/0.080|0.160/25.7|1→1[1,1]|14/15|
|r003|restored|2493.8|5.0|0/0/309|0.124/0.142|0.460/25.0|3→3[3,3]|0/119|
|r004|baseline|2491.3|8.4|0/0/87|0.095/0.112|0.443/25.2|2→2[2,2]|0/20|
|r004|limited|602.0|392.0|6/1/18974|0.063/0.079|0.211/26.3|1→1[1,2]|9/18|
|r004|restored|2490.3|8.3|0/0/486|0.093/0.111|0.439/27.3|2→2[2,2]|0/121|
|r005|baseline|2483.8|8.6|0/0/162|0.110/0.128|0.451/25.9|2→2[2,2]|0/21|
|r005|limited|1270.0|157.0|0/0/12300|0.060/0.077|0.270/27.7|1→1[1,2]|14/21|
|r005|restored|2481.6|8.5|0/0/919|0.109/0.128|0.457/28.0|2→2[1,2]|2/118|

ABBA同profile的错误区间高度重叠：v3两轮API1533/142、UNKNOWN141/15；v4两轮API1093/108、UNKNOWN121/13。只有每版2轮且时间漂移明显，不支持live错误/UNKNOWN已被代码显著降低的结论。每轮恢复纯窗API/UNKNOWN均0；低配额仍远低于2500/s并出现大量真实client_queue_full。参数组两轮限额纯窗602/1270成功/s，成功p95392/157ms；改善属于batch32/pool2/5ms参数效果，仍不是同2500满承载。

Search代码只在标准HTTP Transport确定尚未拿到连接或Do前取消时返回NOT_APPLIED；拿到连接后仍UNKNOWN。server取消修复隔离当前RPC，保留独立stalled-peer watchdog。新helper在RPC错误时保留已验证mutation结果，但API仍失败。本轮10trials的额外APPLIED+RPCerror和NOT_APPLIED均0，未实际触发这两语义分支；它们由[完整离线与race验证](../validation/checks.json)及真实transport/gRPC针对性测试覆盖，不能冒称live证明减少不确定写。

## 单次高负载回归

同资源 v3/v4 3200/s × mixed/pure，各1次，仅检查回归，不替代前轮3重复性能矩阵。DB0.25CPU/1536MiB、Weir2CPU/4608MiB、w64/s64/batch32/pool2/5ms/Read16KiB/queue256、warm10s+measurement20s。

|run|版本|写比例|成功/s|p95 ms|DB CPU|Weir CPU/RSS MiB|drop/API/UNKNOWN|
|---|---|---|---:|---:|---:|---|---|
|r006|v3|10%|3200.0|8.6|0.103|0.546/25.6|0/0/0|
|r007|v4|10%|3200.0|8.6|0.099|0.536/25.2|0/0/0|
|r008|v4|100%|3165.3|38.0|0.113|0.553/27.0|694/0/0|
|r009|v3|100%|3195.7|8.9|0.102|0.554/27.5|87/0/0|

全试验APPLIED审计共379791，UNKNOWN实际落库44；完整 error_messages、每10s窗、warm、三阶段资源/控制器事件/hold、写outcome ledger与全APPLIED审计见results和raw。所有 transport_Internal 与 http2_internal_reset 的匹配只支持当前RPC写deadline reset解释，不是业务成功；不能保证所有deadline/UNKNOWN消除。NOT_APPLIED仅在真正未拿到连接/未发送证明下可确定，拿到连接后失败继续UNKNOWN。参数组与代码组分别报告，极端CPU容量不足的客户端drop保留。

清理：{"pass": true, "errors": [], "inventory_restored": false, "non_default_inventory_restored": true, "default_bridge_id_changed": true, "remaining_owned_containers": [], "remaining_owned_networks": []}。

证据：[results](results.json)、[raw archive](raw-evidence.tar.xz)、[SHA256](sha256.json)。
