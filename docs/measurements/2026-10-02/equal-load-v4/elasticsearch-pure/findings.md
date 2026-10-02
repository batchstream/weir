# ES v4 同实际数据库 CPU 吞吐对照（2026-10-02）

DB固定0.25CPU/1536MiB，同一ES8.19.22 Java进程，整个新cohort使用新建owned普通磁盘volume，逐trial重建owned index与1000keys。冻结v4与共同helper。Weir4CPU/4608MiB，client2CPU，worker/session64、queue256；直连pool4，Weir pool2/batch32/collect5ms/Read16KiB。两者是已有部署选型，不是pool单因素实验；单Weir进程。

每workload先共同预热至少60s，分别校准输入率（warm20/measure30，每path每round最多6次），随后锁率。正式warm20/measure60，每workload3pair D/W、W/D、D/W；至多2round完整pair补测，旧失配保留。唯一processCPU目标0.10核：两侧各0.095..0.105，pair相对均值差≤5%，同时满足采样覆盖。全部60s吞吐按success/60；CPU用完整正式窗口内全部合格request/read bracket的首末counter与实际read-complete采样span，不按名义60s代替counterspan、不挑10s窗、不折算吞吐。

健康状态另判：API/UNKNOWN/OOM0、drop≤0.1%、client与Weir CPU各低于配额90%、clientlagp99≤100ms，且每10s窗drop/planned>0.1%的窗少于2个。只有CPU与健康都合格的pair用于稳定吞吐结论；失败与缺额如实保留。

|workload/pair/round|D/W prefixes|D/W offered|D/W成功/s|D/W processCPU|CPU差|CPUmatched/healthy/selected|D/W高drop窗| W/D成功吞吐比|
|---|---|---|---|---|---:|---|---|---:|
|pure/0/2|r020/r021|90/1490|90.00/1490.00|0.10087/0.08801|13.62%|False/True/False|0/0|16.556|
|pure/1/2|r023/r022|90/1490|90.00/1486.65|0.09791/0.09573|2.25%|True/False/False|0/2|16.518|
|pure/2/2|r024/r025|90/1490|90.00/1490.00|0.09459/0.08856|6.58%|False/True/False|0/0|16.556|

|workload|最终联合合格pair数|D/W成功/s中位|W/D比中位[min,max]|
|---|---:|---|---|
|pure|0/3|无联合合格对|未建立同CPU稳定吞吐对照|

|run|stage/workload/path|rate|成功/s/p95ms|DB process/cgroupCPU/RSS MiB|Weir CPU/RSS MiB|drop/API/UNKNOWN|CPU span下界/首末gap秒|
|---|---|---:|---|---|---|---|---|
|r000|common-prewarm/pure/direct|800|7.03/994.0|0.23699/0.25037/864.0|—|13399/10390/7513|28.105/0.967/0.927|
|r001|common-prewarm/pure/weir|800|800.00/249.0|0.21986/0.23625/886.9|0.238/27.6|0/0/0|28.602/0.679/0.719|
|r002|calibration/pure/direct|670|661.67/407.0|0.15060/0.16997/901.9|—|250/0/0|29.156/0.565/0.279|
|r003|calibration/pure/direct|440|440.00/100.0|0.13942/0.15965/895.9|—|0/0/0|29.579/0.309/0.111|
|r004|calibration/pure/direct|320|320.00/123.0|0.11969/0.14368/903.4|—|0/0/0|29.044/0.112/0.843|
|r005|calibration/pure/direct|270|270.00/91.0|0.13474/0.16105/902.8|—|0/0/0|29.246/0.504/0.250|
|r006|calibration/pure/direct|200|200.00/7.5|0.11320/0.13819/896.6|—|0/0/0|29.282/0.413/0.305|
|r007|calibration/pure/direct|180|180.00/6.8|0.12794/0.15407/896.4|—|0/0/0|29.195/0.548/0.257|
|r008|calibration/pure/weir|2360|2245.97/122.0|0.14454/0.16297/916.3|0.523/27.0|3421/0/0|29.214/0.304/0.482|
|r009|calibration/pure/weir|1630|1586.60/184.0|0.11909/0.13703/910.2|0.408/26.9|1302/0/0|29.092/0.397/0.510|
|r010|calibration/pure/weir|1370|1364.43/8.9|0.09453/0.11353/909.5|0.381/28.8|167/0/0|29.151/0.529/0.320|
|r011|calibration/pure/weir|1450|1447.13/12.0|0.09729/0.11519/913.6|0.378/27.6|86/0/0|29.554/0.394/0.052|
|r012|calibration/pure/weir|1490|1490.00/9.9|0.10205/0.12057/926.8|0.403/29.0|0/0/0|29.351/0.204/0.445|
|r013|calibration/pure/direct|200|200.00/26.0|0.12444/0.14948/917.6|—|0/0/0|28.892/0.541/0.567|
|r014|calibration/pure/direct|160|160.00/6.7|0.12218/0.14760/916.6|—|0/0/0|29.227/0.357/0.415|
|r015|calibration/pure/direct|130|130.00/7.9|0.10814/0.13490/916.0|—|0/0/0|29.780/0.113/0.107|
|r016|calibration/pure/direct|120|120.00/10.0|0.11982/0.14590/919.1|—|0/0/0|28.869/0.488/0.643|
|r017|calibration/pure/direct|100|100.00/7.6|0.11182/0.13920/918.5|—|0/0/0|29.432/0.239/0.329|
|r018|calibration/pure/direct|90|90.00/8.3|0.09872/0.12443/918.1|—|0/0/0|29.190/0.323/0.487|
|r019|calibration/pure/weir|1490|1490.00/9.0|0.09830/0.11682/928.4|0.410/26.7|0/0/0|29.349/0.564/0.087|
|r020|formal/pure/direct|90|90.00/8.1|0.10087/0.12752/918.8|—|0/0/0|59.033/0.536/0.375|
|r021|formal/pure/weir|1490|1490.00/8.9|0.08801/0.10660/1003.8|0.395/27.4|0/0/0|59.040/0.483/0.477|
|r022|formal/pure/weir|1490|1486.65/9.4|0.09573/0.11412/1006.4|0.410/27.1|201/0/0|59.179/0.231/0.590|
|r023|formal/pure/direct|90|90.00/7.3|0.09791/0.12589/922.1|—|0/0/0|58.865/0.597/0.538|
|r024|formal/pure/direct|90|90.00/8.2|0.09459/0.12301/922.3|—|0/0/0|59.229/0.619/0.151|
|r025|formal/pure/weir|1490|1490.00/8.9|0.08856/0.10705/1009.0|0.406/27.3|0/0/0|59.344/0.411/0.245|


预热和校准不计正式结果。冷启动失败、未达目标校准、失配完整pair、替换轮与前置instrument/pacing失败全部保留。rate6400仅是冻结helper硬上限，pureformalcap3730；超过合法计划预算时没有增rate或重编helper。部分pair合格不等于完成3pair；目标不可达应报告缺额，不能插值或按CPU线性折算。

同CPU并不意味着IO、GC、内存负载相同。本组采用普通磁盘volume，不能与先前tmpfs矩阵当同环境重复合并；同进程的JIT/merge/GC漂移、CPU计数10ms量化和采样读区间仍有不确定性。idle/IO/df/indexsize/CFS/GC与完整每10s窗保留供复核。

按stage分开的APPLIED全内容审计、measurement ledger、API/UNKNOWN/drop见results。warm+measure写都审计；中断的未完成校准不声称完成审计。收到APPLIED且RPC失败仍API失败；拿到连接后失回复写保留UNKNOWN，不自动重试。

清理：{"pass": true, "errors": [], "inventory_restored": false, "non_default_inventory_restored": true, "default_bridge_id_changed": true, "remaining_owned_containers": [], "remaining_owned_networks": []}；ownedvolume：{"name": "weir-load-2683ec0c73c6-data", "errors": [], "remaining_owned_volumes": []}。

证据：[results](results.json)、[raw archive](raw-evidence.tar.xz)、[SHA256](sha256.json)。

本cohort是最后一次获授权的fresh pure-only独立DB与ordinaryvolume，未复用旧mixed/pure CPU或直连基线。round0旧fixture失败独立保留；本次只使用round1和round2，每path每轮最多6次校准，剩余补测只替换完整pair。

首次load前已验证exact-labelled闲置client重启sameCID/image/limits，top仅idle；每trial前寿命>=25min才重启client，并保留before/after及DB不重启证明。生产与helper冻结，无自动重试写。

次要观测：2对完整60s两侧健康、Weir实际CPU更低且成功吞吐更高，分别为0/2,2/2。这些观测独立于严格targetmatched，不按CPU折算，不等同最大容量或三重复稳定同CPU证明。每对实际CPU/RPS及不合格记录均见表与JSON。
