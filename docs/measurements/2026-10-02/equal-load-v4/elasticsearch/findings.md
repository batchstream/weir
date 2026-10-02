# ES v4 同实际数据库 CPU 吞吐对照（2026-10-02）

DB固定0.25CPU/1536MiB，同一ES8.19.22 Java进程，整个新cohort使用新建owned普通磁盘volume，逐trial重建owned index与1000keys。冻结v4与共同helper。Weir4CPU/4608MiB，client2CPU，worker/session64、queue256；直连pool4，Weir pool2/batch32/collect5ms/Read16KiB。两者是已有部署选型，不是pool单因素实验；单Weir进程。

每workload先共同预热至少60s，分别校准输入率（warm20/measure30，每path每round最多6次），随后锁率。正式warm20/measure60，每workload3pair D/W、W/D、D/W；至多2round完整pair补测，旧失配保留。唯一processCPU目标0.10核：两侧各0.095..0.105，pair相对均值差≤5%，同时满足采样覆盖。全部60s吞吐按success/60；CPU用完整正式窗口内全部合格request/read bracket的首末counter与实际read-complete采样span，不按名义60s代替counterspan、不挑10s窗、不折算吞吐。

健康状态另判：API/UNKNOWN/OOM0、drop≤0.1%、client与Weir CPU各低于配额90%、clientlagp99≤100ms，且每10s窗drop/planned>0.1%的窗少于2个。只有CPU与健康都合格的pair用于稳定吞吐结论；失败与缺额如实保留。

|workload/pair/round|D/W prefixes|D/W offered|D/W成功/s|D/W processCPU|CPU差|CPUmatched/healthy/selected|D/W高drop窗| W/D成功吞吐比|
|---|---|---|---|---|---:|---|---|---:|
|mixed/0/1|r016/r017|130/2240|130.00/2239.38|0.09790/0.10277|4.85%|True/True/True|0/1|17.226|
|mixed/1/1|r019/r018|130/2240|130.00/2231.27|0.09424/0.10941|14.90%|False/False/False|0/2|17.164|
|mixed/2/1|r020/r021|130/2240|130.00/2240.00|0.08995/0.09883|9.41%|False/True/False|0/0|17.231|
|mixed/1/2|r026/r025|140/2240|140.00/2200.83|0.09711/0.10598|8.74%|False/False/False|0/2|15.720|
|mixed/2/2|r027/r028|140/2240|140.00/2236.78|0.09537/0.10521|9.82%|False/False/False|0/1|15.977|

|workload|最终联合合格pair数|D/W成功/s中位|W/D比中位[min,max]|
|---|---:|---|---|
|mixed|1/3|130.00/2239.38|17.226[17.226,17.226]|

|run|stage/workload/path|rate|成功/s/p95ms|DB process/cgroupCPU/RSS MiB|Weir CPU/RSS MiB|drop/API/UNKNOWN|CPU span下界/首末gap秒|
|---|---|---:|---|---|---|---|---|
|r000|common-prewarm/mixed/direct|800|411.57/785.0|0.23564/0.24929/855.9|—|9072/2581/265|28.585/0.370/1.044|
|r001|common-prewarm/mixed/weir|800|796.73/362.0|0.21561/0.23223/873.9|0.235/29.1|98/0/0|28.610/0.692/0.698|
|r002|calibration/mixed/direct|800|800.00/52.0|0.13806/0.15638/883.9|—|0/0/0|29.360/0.268/0.371|
|r003|calibration/mixed/direct|580|580.00/24.0|0.13867/0.16195/889.2|—|0/0/0|29.142/0.360/0.497|
|r004|calibration/mixed/direct|420|420.00/7.2|0.13744/0.16264/889.1|—|0/0/0|29.447/0.385/0.168|
|r005|calibration/mixed/direct|310|310.00/8.5|0.13993/0.16570/889.8|—|0/0/0|29.212/0.370/0.418|
|r006|calibration/mixed/direct|220|220.00/9.5|0.13108/0.15713/890.6|—|0/0/0|29.492/0.186/0.321|
|r007|calibration/mixed/direct|170|170.00/6.5|0.11672/0.14423/890.5|—|0/0/0|29.473/0.249/0.277|
|r009|common-prewarm-continuation/mixed/direct|220|220.00/6.9|0.12455/0.15081/908.4|—|0/0/0|29.283/0.608/0.108|
|r010|common-prewarm-continuation/mixed/weir|220|220.00/9.1|0.05593/0.07854/908.6|0.172/25.2|0/0/0|29.260/0.468/0.271|
|r011|calibration/mixed/direct|120|120.00/6.2|0.09281/0.12100/908.9|—|0/0/0|29.122/0.563/0.315|
|r012|calibration/mixed/direct|130|130.00/7.3|0.09915/0.12608/909.1|—|0/0/0|29.403/0.314/0.283|
|r013|calibration/mixed/weir|3000|2913.13/26.0|0.12618/0.14421/916.8|0.608/29.0|2606/0/0|29.320/0.085/0.595|
|r014|calibration/mixed/weir|2380|2377.27/9.9|0.10646/0.12378/916.0|0.516/26.8|82/0/0|29.259/0.375/0.366|
|r015|calibration/mixed/weir|2240|2240.00/8.7|0.10086/0.11877/918.0|0.495/28.3|0/0/0|29.303/0.520/0.176|
|r016|formal/mixed/direct|130|130.00/8.4|0.09790/0.12444/912.8|—|0/0/0|59.469/0.418/0.112|
|r017|formal/mixed/weir|2240|2239.38/9.0|0.10277/0.12071/925.7|0.489/28.9|37/0/0|59.312/0.528/0.160|
|r018|formal/mixed/weir|2240|2231.27/11.0|0.10941/0.12756/925.6|0.496/28.6|524/0/0|59.088/0.523/0.388|
|r019|formal/mixed/direct|130|130.00/6.3|0.09424/0.12180/915.0|—|0/0/0|59.337/0.200/0.462|
|r020|formal/mixed/direct|130|130.00/5.8|0.08995/0.11685/915.5|—|0/0/0|59.368/0.487/0.144|
|r021|formal/mixed/weir|2240|2240.00/8.7|0.09883/0.11666/926.9|0.478/26.6|0/0/0|59.046/0.466/0.498|
|r022|calibration/mixed/direct|130|130.00/5.9|0.09174/0.11894/916.1|—|0/0/0|29.385/0.154/0.467|
|r023|calibration/mixed/direct|140|140.00/5.7|0.09757/0.12500/916.4|—|0/0/0|29.019/0.463/0.517|
|r024|calibration/mixed/weir|2240|2240.00/8.9|0.10118/0.11955/922.0|0.495/27.2|0/0/0|29.305/0.467/0.228|
|r025|formal/mixed/weir|2240|2200.83/9.2|0.10598/0.12486/939.4|0.491/29.3|2350/0/0|59.405/0.400/0.195|
|r026|formal/mixed/direct|140|140.00/6.1|0.09711/0.12434/923.5|—|0/0/0|59.033/0.317/0.650|
|r027|formal/mixed/direct|140|140.00/5.8|0.09537/0.12304/923.8|—|0/0/0|59.163/0.590/0.247|
|r028|formal/mixed/weir|2240|2236.78/9.2|0.10521/0.12346/940.4|0.507/27.2|193/0/0|59.831/0.099/0.069|
|r029|common-prewarm-continuation/pure/direct|800|800.00/6.5|0.11609/0.13500/931.2|—|0/0/0|29.160/0.541/0.298|
|r030|common-prewarm-continuation/pure/weir|800|800.00/8.2|0.06666/0.08503/931.4|0.253/26.6|0/0/0|29.207/0.569/0.224|
|r031|calibration/pure/direct|800|800.00/4.9|0.11451/0.13436/952.1|—|0/0/0|29.380/0.233/0.386|
|r032|calibration/pure/direct|700|700.00/4.2|0.10425/0.12323/942.5|—|0/0/0|29.307/0.241/0.451|
|r033|calibration/pure/direct|670|670.00/2.6|0.10009/0.11852/941.8|—|0/0/0|29.128/0.297/0.575|

同进程无业务original ESdiagnostics observer baseline实际processCPU0.01718核/cgroup0.04585核、span29.690s；proc-onlyidle约0.01655核。该结果否定了诊断采样单独造成0.13核Javafloor的假说，保持原诊断频率，不扣baseline。原每循环0.5s wait加exec/HTTP约1.69Hz，并非严格2Hz，也只读取thread_pool/http/jvm/process/indexing_pressure/fs指定类别。直连低rate校准CPU已从约0.139下降到0.1167，因此没有证据声称物理恒定CPUfloor。经root批准mixed额外commonwarm D/W220各30s，共60s，之后使用既定round1/2总重校准预算；r008中断无完整确认证据，不声明完成audit。

预热和校准不计正式结果。冷启动失败、未达目标校准、失配完整pair、替换轮与前置instrument/pacing失败全部保留。rate6400仅是冻结helper硬上限，pureformalcap3730；超过合法计划预算时没有增rate或重编helper。部分pair合格不等于完成3pair；目标不可达应报告缺额，不能插值或按CPU线性折算。

同CPU并不意味着IO、GC、内存负载相同。本组采用普通磁盘volume，不能与先前tmpfs矩阵当同环境重复合并；同进程的JIT/merge/GC漂移、CPU计数10ms量化和采样读区间仍有不确定性。idle/IO/df/indexsize/CFS/GC与完整每10s窗保留供复核。

按stage分开的APPLIED全内容审计、measurement ledger、API/UNKNOWN/drop见results。warm+measure写都审计；中断的未完成校准不声称完成审计。收到APPLIED且RPC失败仍API失败；拿到连接后失回复写保留UNKNOWN，不自动重试。

清理：{"pass": true, "errors": [], "inventory_restored": true, "non_default_inventory_restored": true, "default_bridge_id_changed": false, "remaining_owned_containers": [], "remaining_owned_networks": []}；ownedvolume：{"name": "weir-load-a05b8570df11-data", "errors": [], "remaining_owned_volumes": []}。

证据：[results](results.json)、[raw archive](raw-evidence.tar.xz)、[SHA256](sha256.json)。

本cohort最终mixed联合合格仅1/3对：17.226倍仅描述一对观测点，不支持完成3次重复的稳定改善结论。该对r017正式37drop，预热另有3601drop，均保留。其他4对失败全部保留。

原pure校准r034出现3002正式drop、123API失败（120Deadline/3http2_internal_reset）与123UNKNOWN，trial后未产生完整ledger/audit。136752个warm+measure API成功put未计入APPLIED审计；UNKNOWN实际是否落库未知。45min idle helper寿命与失败时序匹配，但finally前没有保存精确exitState，原因仅假说，不将API失败全归因夹具。此r034不作同CPU或安全审计合格数据。后续fresh pure独立cohort另存elasticsearch-pure，不跨DB PID合并。
