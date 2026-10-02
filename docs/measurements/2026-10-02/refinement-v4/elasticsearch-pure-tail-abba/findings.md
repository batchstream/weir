# ES v4 纯写尾延迟独立 ABBA 复核（2026-10-02）

原十轮试验单次pure3200中v4 p9538ms、v3 8.9ms；v4 CFS1.159s对v3 .313s，客户端lag p9998ms对31ms；两版WeirCPU约.55、控制器window2恒定，GC11ms对10ms。限流/排队与尾延迟相伴，不能仅从原单次定代码因果。原异常、raw和SHA全部保留在[主对照](../elasticsearch/findings.md)。

本次独立新owned ES进程，v3/v4/v4/v3顺序每版2次，不与旧DB当3repeat合并。同冻结helper/binaries、同资源profile：DB.25CPU/1536MiB、Weir2CPU/4608MiB、w64/s64/batch32/pool2/5ms/Read16KiB/queue256，pure3200/s、warm10+measure20、deadline1s、不重试。共同JVM预热，逐格actualquota/PID/startticks/CPU/RSS与全APPLIED审计。

|run|版本|成功/s|p95/p99 ms|drop/API/UNKNOWN|DB CPU/CFS s|Weir CPU/RSS MiB|client lag p99 ms|
|---|---|---:|---|---|---|---|---:|
|r000|v3|2718.3|196.0/276.0|9634/0/0|0.184/6.407|0.471/27.4|199.0|
|r001|v4|2997.7|132.0/259.0|4047/0/0|0.138/2.791|0.533/27.6|190.0|
|r002|v4|3173.5|40.0/110.0|530/0/0|0.123/1.196|0.556/27.5|90.0|
|r003|v3|3145.1|54.0/154.0|1098/0/0|0.120/1.459|0.549/26.9|108.0|

|版本|2次成功/s中位[min,max]|p95 ms中位[min,max]|DB CPU中位|Weir CPU中位|drop/API/UNKNOWN合计|
|---|---|---|---:|---:|---|
|v3|2931.7[2718.3,3145.1]|125.0[54.0,196.0]|0.152|0.510|10732/0/0|
|v4|3085.6[2997.7,3173.5]|86.0[40.0,132.0]|0.130|0.544|4577/0/0|

全APPLIED落库审计361509；UNKNOWN实存0。每10s窗、client队列/lag、CFS/GC、真实resource receipt与原单次诊断完整保留。仅每版2次、同新DB进程的短测，避免宣称百分比延迟保证；原异常不能被这组成功覆盖。

清理：{"pass": true, "errors": [], "inventory_restored": true, "non_default_inventory_restored": true, "default_bridge_id_changed": false, "remaining_owned_containers": [], "remaining_owned_networks": []}。

证据：[results](results.json)、[raw archive](raw-evidence.tar.xz)、[SHA256](sha256.json)。
