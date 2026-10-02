# MongoDB 相同实际数据库 CPU 测量

当前为中断cohort：18个完整格（1共同预热、11校准、6正式mixed），原mixed三pair仅2/3合格；补测未完成，pure尚未开始。故障cal4未观察到client_start，转发流lostconnection；远端partial缺失，不能据此判断DB/OOM/API业务故障。不得报告已完成三次匹配或稳定headline。

数据库固定 quota0.25核/1536Mi、WiredTiger256Mi、oplog64Mi和普通磁盘4Gi；目标仅实际数据库总进程 CPU0.10核。Direct和Weir路径独立校准后锁速率；同workers64、client2CPU、Weir4CPU/4608Mi；directpool4，Weircap2/batch32/read16KiB/mixed10ms/pure5ms。

共同预热60秒及空闲采样不计正式结论。校准warm20/measure30，正式warm20/measure60；CPU使用全测量内全部1Hz计数样本的实际first/last跨度。正式吞吐为success/60秒，延迟与丢弃均只取measurement；账本审计涵盖warm+measurement。

CPUmatched与healthy-qualified分列，所有失配与补测原样保留，不取10秒片段、不插值、不按CPU归一化吞吐。Direct没有运行Weir应用，Weir容器中sleep及观测器另列为idle容器，不算运行Weir的成本。

|格|路径|offered/s|success/s|DB CPU核|Weir CPU核|DB RSS峰值MiB|Weir RSS峰值MiB|p95/p99 ms|drop|API/UNKNOWN|CPU gate|health gate|
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
|formal-mixed-1-direct|direct|900|900.00|0.10237|—|313.6|—|2.4/3.5|0|0/0|True|True|
|formal-mixed-1-weir|weir|1745|1745.00|0.10441|0.5507|324.9|26.3|16.0/17.0|0|0/0|True|True|
|formal-mixed-2-direct|direct|900|900.00|0.10119|—|327.5|—|2.4/3.5|0|0/0|True|True|
|formal-mixed-2-weir|weir|1745|1745.00|0.10424|0.5559|326.6|26.3|16.0/16.0|0|0/0|True|True|
|formal-mixed-3-direct|direct|900|900.00|0.11288|—|328.9|—|2.6/3.8|0|0/0|False|False|
|formal-mixed-3-weir|weir|1745|1745.00|0.10881|0.5693|329.5|26.4|16.0/17.0|0|0/0|False|False|

|workload|pair/cohort|CPU相对均值差|CPU matched|healthy qualified|Weir/Direct成功吞吐比|
|---|---|---:|---|---|---:|
|mixed|1/0|1.97%|True|True|1.939|
|mixed|2/0|2.97%|True|True|1.939|
|mixed|3/0|3.67%|False|False|1.939|

|Weir正式格|采样区间execution计数|平均batch operations|execution平均ms|pending min/max|window min/max|hold采样占比|
|---|---:|---:|---:|---:|---:|---:|
|formal-mixed-1-weir|5790|17.78|3.18|0/19|2/2|0.0%|
|formal-mixed-2-weir|5794|17.77|3.18|1/20|2/2|0.0%|
|formal-mixed-3-weir|5801|17.75|3.19|0/19|2/2|0.0%|

上表控制器计数差分/平均值为全measurement内first/last指标采样之间的区间，未覆盖每端不足1Hz的少量事件；原始trace/finalcounter保留。低目标CPU时每批固定成本可能主导属于诊断推断，不据batch/IO数值证明单一原因。

正式所有attempted格完整warm+measure写账本：63480；APPLIED=63480，UNKNOWN_found=0。全部calibration/common/formal账本和totals在results.json分别列出。

只有同一workload满足三次健康合格配对时，才支持该目标CPU与该部署profile下的成功吞吐比较。此结果不测最大容量、不证明相同内存/IO，也不保证其他延迟约束或任意资源下的收益。单次CPU小差异不能称性能改善。

IO记录包含proc/io、cgroup io.stat和IO PSI；它们不是逐条数据库命令的IO延迟。blockdelay字段存在且值0不能证明kernel启用了delay accounting，也不能排除IO影响。数据库RSS/cache和IO允许不同，但必须保留供解释。

sampler-io wall_time/monotonic取collect开始，随后顺序读本地文件；冻结采样器没有读完成bracket，因此CPU边界是1Hz读数时序估计，不能声称计数精确发生在时戳瞬间。client_sample包含time/end/duration，raw保留完整读取bracket。

正式节点为on-demand ip-172-31-12-243.us-west-1.compute.internal；same mongod身份与实际25000/100000配额从各raw及manifest核验。前期proposal的128workers/9Gi未用于live，中央plan统一64workers/4608Mi后才创建/运行本fixture。

cleanup.json含精确owned UID/label删除及PF子进程退出；cleanup-independent.json含独立namespace NotFound。证据原始账本、配置、实际配额、PID、CPU/RSS/IO、控制器指标与远端hash全部归档。
