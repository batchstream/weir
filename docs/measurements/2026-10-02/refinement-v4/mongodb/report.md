# MongoDB v3/v4 公平恢复与吞吐回归实测

本轮四次 ABBA（v3→v4→v4→v3）均在同一 MongoDB 进程与同一 on-demand ARM 节点上完成。两版本都从降配压力恢复到健康期的 2500 次/秒；本轮未证明 v4 比 v3 增加恢复吞吐。慢阶段窗口均稳定为 1，健康窗口为 4，恢复窗口达到 3（末轮 v3 恢复初段为 2→3），不要求回到 4 才算恢复。没有触发恢复低于自身健康 95% 的额外诊断条件，因此没有运行固定窗口试验。

## 环境与测量边界

- MongoDB 8.0.32、普通磁盘 emptyDir 4Gi、WiredTiger 256Mi/oplog 64Mi、内存 1536Mi。CPU requests 固定 50m，cgroup 实测 limit 0.5→0.05→0.5；没有重启数据库。
- 节点 ip-172-31-12-243.us-west-1.compute.internal；mongod PID1/start_ticks 71349497/container 8f45791120de8808a2f4631677167ddbea4390b8e0313620f18415d7800f53ff/restart0。初选 Spot 节点启动前变 NotReady，预检失败发生在创建任何资源前，记录留在 precreate-attempt-1。
- 恢复负载 2500/s、90%读/10%唯一写、workers/sessions32、pool/cap4、batch8、collect3ms、read16KiB、queue32、Weir2CPU/2304Mi。所有试验共用修正后的同一客户端。
- warm20 + measure120；首轮开始前统一将降配调度从40改42秒，以容纳按到达时间归桶的20–30/30–40秒窗口及最大1秒调用完成时间。慢阶段实际 quota 确认后至少30秒再请求恢复，统计排除确认后2秒。
- 表中吞吐/资源按完整10秒窗口统计，再要求窗口末端+1秒早于下一次配额请求或最后正常 sampler 时间。每轮健康2窗、恢复4窗；慢期保守有效窗为2/3/2/2。到达时间窗、跨边界/settling窗、预热、原始指标均保留。1Hz CPU差分实际覆盖约19/29/39秒，100Hz/4096页在容器中实测；process CPU/RSS与cgroup值分别保存。

## ABBA 主恢复结果

| 格 | 健康/慢/恢复 成功RPS | 恢复/健康 | 恢复p99 arrival ms范围 | 慢/恢复hold采样占比 | measure drop | API/UNKNOWN |
|---|---:|---:|---:|---:|---:|---:|
| v3-a1 | 2500/554.8/2500 | 100% | 7.9–8.1 | 100%/0% | 81925 | 0/0 |
| v4-b1 | 2500/527.5/2500 | 100% | 8.0–8.0 | 100%/0% | 85421 | 0/0 |
| v4-b2 | 2500/545.2/2500 | 100% | 7.8–8.1 | 100%/0% | 82071 | 0/0 |
| v3-a2 | 2500/556.0/2500 | 100% | 7.8–8.0 | 100%/0% | 82221 | 0/0 |

| 格/阶段 | DB process CPU核 / RSS中位MiB | Weir process CPU核 / RSS中位MiB | DB IO PSI some占比 | CFS节流period/总period |
|---|---:|---:|---:|---:|
| v3-a1 healthy | 0.174 / 216.43 | 0.639 / 24.77 | 3.67% | 0/190 |
| v3-a1 slow | 0.047 / 232.57 | 0.154 / 25.65 | 0.53% | 189/189 |
| v3-a1 recovery | 0.214 / 252.63 | 0.677 / 24.97 | 3.50% | 0/390 |
| v4-b1 healthy | 0.184 / 279.39 | 0.659 / 24.65 | 3.60% | 0/190 |
| v4-b1 slow | 0.047 / 296.43 | 0.147 / 25.74 | 0.49% | 290/290 |
| v4-b1 recovery | 0.207 / 315.61 | 0.667 / 24.89 | 3.64% | 0/390 |
| v4-b2 healthy | 0.171 / 336.72 | 0.633 / 24.66 | 3.89% | 0/190 |
| v4-b2 slow | 0.048 / 344.87 | 0.152 / 25.86 | 0.54% | 191/191 |
| v4-b2 recovery | 0.209 / 348.40 | 0.669 / 24.97 | 3.38% | 0/390 |
| v3-a2 healthy | 0.171 / 346.27 | 0.627 / 24.60 | 3.80% | 0/190 |
| v3-a2 slow | 0.047 / 352.79 | 0.153 / 25.63 | 0.53% | 190/190 |
| v3-a2 recovery | 0.200 / 350.86 | 0.657 / 24.77 | 3.67% | 0/390 |

慢期 DB 实际约0.047–0.048核，全部采样 CFS periods 被节流，控制器 hold 为100%、执行窗口为1；恢复期 DB约0.200–0.214核、CFS节流为0，hold为0，成功吞吐恢复100%。执行时长 p99 仅有直方图桶上界：健康/恢复≤10ms，慢期≤1000ms；不能当精确p99。

IO观测包含目标进程 /proc/PID/io、每设备 cgroup io.stat、io.pressure 与 delayacct 字段。健康/恢复 PSI some 约3.4%–3.9%，慢阶段约0.5%，未看到恢复持续高等待与低吞吐。进程计数针对真实应用PID，cgroup包含sampler少量开销；字段存在不证明内核启用 delay accounting，blockdelay为0不能排除IO等待。这些统计不能直接推断单条Mongo命令IO延迟，也不能解释历史不同节点上v3的残差。DB RSS随连续试验/cache增长，不能将阶段内存差当版本导致的节省。

## 原性能参数的四格回归

每版本每profile仅1轮，warm10 + measure20；同客户端workers/sessions64、queue256、pool4，Weir cap2/batch32/read16KiB，mixed collect10ms、pure collect5ms，Weir2CPU/4608Mi，DB0.25CPU/1536Mi。实际memory.max/CPU配额单独读回；窗口均保持2。以下RPS、p95、drop、API、UNKNOWN均只含20秒measurement，APPLIED账本另含10秒warm。

| 格 | 成功RPS | p95 arrival ms | measure drop / warm drop | API/UNKNOWN | DB CPU核 | Weir CPU核 / RSS峰MiB | 全程APPLIED |
|---|---:|---:|---:|---:|---:|---:|---:|
| perf-v3-mixed | 3200.00 | 64 | 0 / 0 | 0/0 | 0.1105 | 0.6616 / 26.42 | 9600 |
| perf-v4-mixed | 3200.00 | 57 | 0 / 0 | 0/0 | 0.1042 | 0.6510 / 26.48 | 9600 |
| perf-v4-write | 3195.55 | 62 | 89 / 0 | 0/0 | 0.2005 | 0.7184 / 26.20 | 95911 |
| perf-v3-write | 3197.45 | 49 | 51 / 44 | 0/0 | 0.2000 | 0.7148 / 26.07 | 95905 |

混合两格都满载3200/s。纯写v3/v4分别3197.45/3195.55/s，measurement丢弃51/89次；v3另有warm44次丢弃。单次混合CPU约6%的差不能视为显著提升；纯写v4 p95比v3高13ms，同样只属于单次样本差，不能宣称尾延迟改善。本轮没有重跑旧直连矩阵，不能把旧v3直连对照冒充新v4实测。

## 完整性与清理

八格全程（含warm）计划1,784,000次，成功1,452,178、client drop331,822；API失败、UNKNOWN、DB/Weir OOM均0。351,200条写入账本包括317,840次APPLIED和33,360次未发出drop，所有APPLIED完整BSON审计、drop记录均缺失，无audit/run错误。128项原始核验（16项×8格）全通过；每个10秒窗口及整体计数守恒，原raw包SHA/远端二进制SHA均匹配。

新版测量helper保留已验证合法mutation outcome，即使后续RPC/trailer错误仍计API失败，绝不改成success。无结果仍UNKNOWN；错误消息只输出固定安全分类或有界指纹。该修复已由真实loopback覆盖，本轮API错误为0，因此没有触发这些诊断分类。旧v3数据及其旧helper口径保持不变。

Owned namespace weir-recovery-508aefc218c7 UID09efb636-a6e5-48cc-80de-18969bf07c5b、Pod UIDa41a1009-54a7-424c-9d59-69457e4fe07e 已删除，主清理与独立查询均返回NotFound；精确PF PID40497/19186退出-15，PID消失、端口connect_ex61，errors=[]。最终容器ps证明只有sleep/httpd服务及已退出的zombie负载；namespace销毁后没有遗留fixture。最终DB普通磁盘使用记录见归档database-final-disk.txt。

## 冻结二进制与证据

- `weir-v4` SHA256 `7b9d159a46ec82c3377302ea6d582ea161c2e99ca51d4e65844b4967c2628dcf`
- `weir-v3` SHA256 `4054ca6a7ef9d12886566124bb5153591d7beacb5cf83949de34fe6a058eee87`
- `client-final` SHA256 `8b63c1fe3f512652b069ef623fd46b86bbd9807fb4661dc25ef6a4f7cd33503b`
- `sampler-io` SHA256 `c2e3df6fb12414a81f69a2581725ded028341568604d7451c2381568266f5a73`

父任务冻结6文件源包SHA038bca362c0ffac8bd18ef9fad6963be660bca84a8021a0063383f9f4d3450eb；本工具只重命名并选择4文件的派生包SHAcce46336f9628c0052d078c907246f735c7a165e8514579dea18154aac0e3170，内容逐文件校验一致。

`results.json` 保存逐格分析、严格窗口、process/cgroup CPU、RSS、IO与所有检查；`raw-evidence.tar.xz` 保存8个完整原始包及展开的客户端/ledger/audit、DB/Weir采样、实际配置、配额/身份、Go测量源快照和Python编排。`sha256.json`逐一列完整archive member名的SHA，额外列报告/结果/cleanup等文件SHA。
