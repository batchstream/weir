# MongoDB 相同实际数据库 CPU 测量

完整cohort。

数据库固定 quota0.25核/1536Mi、WiredTiger256Mi、oplog64Mi和普通磁盘4Gi；目标仅实际数据库总进程 CPU0.10核。Direct和Weir路径独立校准后锁速率；同workers64、client2CPU、Weir4CPU/4608Mi；directpool4，Weircap2/batch32/read16KiB/mixed10ms/pure5ms。

共同预热60秒及空闲采样不计正式结论。校准warm20/measure30，正式warm20/measure60；CPU使用全测量内全部1Hz计数样本的实际first/last跨度。正式吞吐为success/60秒，延迟与丢弃均只取measurement；账本审计涵盖warm+measurement。

CPUmatched与healthy-qualified分列，所有失配与补测原样保留，不取10秒片段、不插值、不按CPU归一化吞吐。Direct没有运行Weir应用，Weir容器中sleep及观测器另列为idle容器，不算运行Weir的成本。

|格|路径|offered/s|success/s|DB CPU核|Weir CPU核|DB RSS峰值MiB|Weir RSS峰值MiB|p95/p99 ms|drop|API/UNKNOWN|CPU gate|health gate|
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
|formal-pure-1-direct|direct|338|338.00|0.12898|—|419.5|—|4.0/7.0|0|0/0|False|False|
|formal-pure-1-direct-r1|direct|320|320.00|0.08881|—|409.5|—|3.9/4.0|0|0/0|False|False|
|formal-pure-1-weir|weir|482|482.00|0.11525|0.2592|406.8|25.3|11.0/11.0|0|0/0|False|False|
|formal-pure-1-weir-r1|weir|410|410.00|0.09695|0.2230|406.0|25.2|11.0/11.0|0|0/0|True|True|
|formal-pure-2-direct|direct|338|338.00|0.11288|—|420.6|—|3.9/4.1|0|0/0|False|False|
|formal-pure-2-direct-r1|direct|320|320.00|0.09254|—|404.1|—|3.8/4.0|0|0/0|False|False|
|formal-pure-2-weir|weir|482|482.00|0.11508|0.2602|417.0|25.3|11.0/11.0|0|0/0|False|False|
|formal-pure-2-weir-r1|weir|410|410.00|0.09797|0.2236|404.0|25.2|11.0/11.0|0|0/0|True|True|
|formal-pure-3-direct|direct|338|338.00|0.10559|—|423.3|—|3.9/4.1|0|0/0|False|False|
|formal-pure-3-direct-r1|direct|320|320.00|0.09152|—|404.1|—|3.9/4.0|0|0/0|False|False|
|formal-pure-3-weir|weir|482|482.00|0.10949|0.2507|423.3|25.2|11.0/11.0|0|0/0|False|False|
|formal-pure-3-weir-r1|weir|410|410.00|0.09119|0.2173|404.1|25.3|11.0/12.0|0|0/0|False|False|

|workload|pair/cohort|CPU相对均值差|CPU matched|healthy qualified|Weir/Direct成功吞吐比|
|---|---|---:|---|---|---:|
|pure|1/0|11.24%|False|False|1.426|
|pure|2/0|1.93%|False|False|1.426|
|pure|3/0|3.63%|False|False|1.426|
|pure|1/1|8.76%|False|False|1.281|
|pure|2/1|5.69%|False|False|1.281|
|pure|3/1|0.37%|False|False|1.281|

次要观测仅列整60秒健康、两侧实际CPU相对差≤5%的配对，不要求各侧在0.095..0.105核。它不改变主门槛、有限重校准和原始失配记录，也不算固定0.10核重复或最大容量。

|pair/cohort|Direct实际CPU|Weir实际CPU|CPU相对差|成功吞吐比|
|---|---:|---:|---:|---:|
|2/0|0.11288|0.11508|1.93%|1.426|
|3/0|0.10559|0.10949|3.63%|1.426|
|3/1|0.09152|0.09119|0.37%|1.281|

|Weir正式格|采样区间execution计数|平均batch operations|execution平均ms|pending min/max|window min/max|hold采样占比|
|---|---:|---:|---:|---:|---:|---:|
|formal-pure-1-weir|9474|3.00|3.30|0/3|2/2|0.0%|
|formal-pure-1-weir-r1|8375|2.89|3.20|0/3|2/2|0.0%|
|formal-pure-2-weir|9468|3.00|3.28|0/3|2/2|0.0%|
|formal-pure-2-weir-r1|8385|2.88|3.25|0/3|2/2|0.0%|
|formal-pure-3-weir|9468|3.00|3.20|0/3|2/2|0.0%|
|formal-pure-3-weir-r1|8385|2.88|3.14|0/3|2/2|0.0%|

上表控制器计数差分/平均值为全measurement内first/last指标采样之间的区间，未覆盖每端不足1Hz的少量事件；原始trace/finalcounter保留。低目标CPU时每批固定成本可能主导属于诊断推断，不据batch/IO数值证明单一原因。

正式所有attempted格完整warm+measure写账本：372000；APPLIED=372000，UNKNOWN_found=0。全部calibration/common/formal账本和totals在results.json分别列出。

只有同一workload满足三次健康合格配对时，才支持该目标CPU与该部署profile下的成功吞吐比较。此结果不测最大容量、不证明相同内存/IO，也不保证其他延迟约束或任意资源下的收益。单次CPU小差异不能称性能改善。

IO记录包含proc/io、cgroup io.stat和IO PSI；它们不是逐条数据库命令的IO延迟。blockdelay字段存在且值0不能证明kernel启用了delay accounting，也不能排除IO影响。数据库RSS/cache和IO允许不同，但必须保留供解释。

sampler-io wall_time/monotonic取collect开始，随后顺序读本地文件；冻结采样器没有读完成bracket，因此CPU边界是1Hz读数时序估计，不能声称计数精确发生在时戳瞬间。client_sample包含time/end/duration，raw保留完整读取bracket。

正式节点为on-demand ip-172-31-12-243.us-west-1.compute.internal；same mongod身份与实际25000/100000配额从各raw及manifest核验。本fixture仅pure，前一mixed中断cohort独立保留，数据库PID不同不合并。中央plan统一64workers/4608Mi。

cleanup.json含精确owned UID/label删除及PF子进程退出；cleanup-independent.json含独立namespace NotFound。证据原始账本、配置、实际配额、PID、CPU/RSS/IO、控制器指标与远端hash全部归档。

Direct 和 Weir 均使用同一 localhost MongoDB URI、同一 1024-byte BSON/unique ID/ReplaceOne upsert 与 majority 写确认；Direct 等待单操作 ClientBulkWrite acknowledgement，Weir 等待 bulkWrite 命令及逐项结果校验、API 完成后才计成功。测试副本集只有一个成员；此为冻结代码和实际配置核验，没有进行 wire 抓包。pool4 与 Weir pool/cap2、batch32 属于两条部署路径的差异。详见 mutation-semantics-verification.json。
