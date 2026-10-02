# ES v2 健康基线筛选与仪表失败证据（2026-10-02）

本目录保存全部过程，不能把未通过健康pilot或者错选Java launcher的样本当作正式验收。固定batch8/pool4/w32/session32/read16KiB/collect3ms/queue32；Weir2CPU/2304MiB；DB1536MiB，健康候选quota .5CPU。二进制SHA/source manifest和完整client/APPLIED审计/metrics/ES/proc/cgroup/config都在归档。

|fixture|输入/s|性质|成功/s|drop|API error/UNKNOWN|measurement window|DB proc/cgroup CPU|APPLIED/found|
|---|---:|---|---:|---:|---|---|---|---|
|invalid-process-sampler|3200|health-pilot|3035.4|3292|0/0|4→4 [4,4]|invalid PID/0.265|8945/8945|
|invalid-process-sampler|2500|health-pilot|2500.0|0|0/0|4→4 [4,4]|invalid PID/0.150|7460/7460|
|invalid-process-sampler|2500|backpressure|1652.3|75108|1189/135|4→1 [1,4]|invalid PID/0.138|14861/14884|
|invalid-process-sampler|2500|backpressure|1698.3|71656|493/58|4→2 [1,4]|invalid PID/0.132|15283/15293|
|healthy-2500-rejected|2500|health-pilot|2374.9|2501|0/0|4→1 [1,4]|0.196/0.214|7149/7149|
|healthy-1600-rejected|1600|health-pilot|1573.8|524|0/0|1→1 [1,1]|0.184/0.201|4627/4627|

第一个fixture枚举第一个comm=java，误选PID6 launcher；其/proc CPU/RSS/startticks不代表ES server，且部分pilot/phase与宿主离线工具测试UTC00:05:41.496–00:06:41.802重叠。cgroup/ES API/Weir数据及完整审计仍保留，不用于干净性能因果或正式权威PID验收。

后两个fixture使用ES /_nodes/_local/process提供的serverPID并核验/proc，且measurement不重叠宿主工具测试。2500在measurement内4→1；1600在10s warm内已降至1，measurement全1。实际CPU平均有余量而有周期CFS/GC尖峰；均未形成健康不收缩的基线，脚本没有进入quota phase。它们促成后续v3慢比例确认修正，不能改写为成功。

全部已完成run的APPLIED均查到version1；UNKNOWN中实际落库数量在audit明列。所有owned containers/network/imported image最终精确清理，inventory一致。pacing失败另存相邻es-v2-stable-startup-failure。

证据：[results.json](results.json)、[完整原始归档](raw-evidence.tar.xz)、[SHA256](sha256.json)。
