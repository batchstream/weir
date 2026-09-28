# M28R — 报告守恒补救；一次原生启动仍 NO-GO

2026-09-28。**resource evidence 仍 partial，本阶段未完成原生完整采样。**
唯一一次新冻结 invocation 在负载前被新增的原始 limits 格式检查拒绝；没有第二次启动、
重写计划、修改冻结实现或重跑求绿。candidate=null，计时、容量、过载和 soak 均 not-run。

## 源码、制品与证据

- baseline：`ef755be2b57f61b0117b81631f7a408ce1a79ac2`。
- implementation / native artifact source：`1ffc092c2f85a992f341831f23a5833ff6a96073`。
- 本文与 readiness 是后续文档交付；完整 delivery SHA、260 份源码输入复核、manifest
  在 `.testdata/m28r/delivery.json`。实际原生 binary 不随文档提交重新构建。
- 原始证据根：`/Users/liran/Projects/liran/go/weir/.testdata/m28r/`。
  `fixture-1/plan.json`、`result.json`、两个原始 JSONL、57 条 CLI 的 argv/exit/输出、
  `native-launch.json`、`limits-diagnosis.json`、`operation-audit.json` 与 `final-cleanup.json`
  保留完整失败和回收记录。`computed-resource-report.json` 为 partial。
- `.testdata/m28/` 的全部 744 个现有文件逐个 hash 与本阶段开始时相同。
  M28 原三次失败、旧实际 source 与最终 source 的差异均不倒写。

| 本轮实际 Linux arm64 CGO0 产物 | SHA256 |
| --- | --- |
| Weir | `30effbe4b2f843f72cc3f86af0f57e58ad453624ee0e4e9eb700b674331276e0` |
| helper/client | `683d37ad6f81cc87e828f4d696fcf28c956f7ee66c4bd4b4691a861f32bc2d33` |
| observe.test | `872f0e339f49088eba0b062d2217bbfe51b7cf7784bd292e50cefdbfc7826761` |
| app.test | `d2df6a2c9cd3bae1e54cb6a22288e026b9c2256d4b37feef68e9ca6564f9de18` |

Go1.27.1，在 Darwin arm64 构建，在实际 Linux7.0.12-linuxkit/aarch64 执行；
交叉编译本身不算 native。环境 image 仍为
`sha256:ef36debc338afa91481a64a435dbe23400f6252742ff63aaca45cfeedcaebdd9`，
只提供执行环境，新准确 binary 从自有只读挂载执行。ES8.19.22 固定 manifest
`sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`、
config `sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160`；
没有镜像构建、依赖/JDK/DB升级、push、CI或EKS调用。

## 离线修复与实际边界

仅改 Python 取证入口/报告/测试；204 份 Go/module 输入与基线逐字节一致，
未改产品 runtime、Go observer、调度、协议、存储语义、AIMD 或 Guard。

报告复用共享 integer、窗口、直方图和 TCP 校验，并串联固定 profile 的完整记录：
1000 条连续 setup_progress → client_start → trial → client samples → audit → client_end → receipt。
检查非负有限整数（拒绝布尔）、冻结选项、warm/measure 和两个 measurement 十秒窗口，
all=read+put 及逐桶聚合；成功 Put 对应相同 ledger 分段的 APPLIED，直接 DB audit 的
version1/absent/UNKNOWN 和 pages 守恒。receipt 必须等于有证据的 seed 数加实际调用 Put 数，
合法发生器丢弃允许少于1200，不再只检查上限或无证据硬加 seed。该计数是 helper 开始调用数，
不是在任意失败下证明远端实际收到请求数；完整成功流另有 DB 内容/版本审计。
管理请求与文档 mutation 分列，setup 管理调用明确标为由完整记录及固定源码推导。

ES nodes 先验证对象及唯一节点再读取；空、缺失、错误类型、多节点和字段缺失返回 partial。
原始 proc/cgroup/socket 表、标准 metrics、ES stats、身份、序号、时基、正常终止和覆盖均检查；
合法空 io.stat 保持合法。TCP 表仍表示共享 network namespace，不是单个进程独占连接。
observer CPU/RSS 包含在容器总量中，不扣除；跨流按 UTC 对齐，不相减各 observer 的单调 epoch。

真正 `report()` 入口的完整 JSONL 正对照、合法迟到丢弃和慢计时 NO-GO 对照均通过。
逐项破坏对照覆盖原四项，以及类型/负数/布尔、setup/窗口/直方图/ledger/audit/receipt 不一致、
缺角色/终止/原始表/metrics、身份和采样缺口。持久化原四项对照见 `controls/`；
这些全部是 synthetic 校验器测试，不能代替 native 资格。

一次性入口用新 `.testdata/m28r/native-window.json` 的独占创建禁止再次启动；不再延续 M28 的
三次启动窗口。仍为原 50ops/s、20秒 warm+20秒 measure、through/direct 各一次的计划，
含 seed ≤6000 planned / ≤2400 document mutation，observer140秒/71样本、总900秒+清理120秒。
前置检查复用相同报告函数；执行失败测试验证先取消/Wait两个真实本地管道 child，随后
Stop→Wait→owner/ID核验→remove，日志失败不跳过回收，归属变化拒绝删除。

## 唯一原生尝试与剩余阻断

fixture15.972秒、清理3.003秒；外层启动至 Wait 共19.129秒。Weir/ES/client 均为独立
PID/mount/private cgroup namespace；实际 CgroupnsMode 已纳入 inspect 并核验。
ES为network-none，另外两角色只共享自有ES的loopback网络；监听127.0.0.1、无publish/host网络。
ES UID1000、Weir/client65532，各 observer 与目标同UID；dropALL/no-newpriv、只读根、
有限tmpfs/pids/log、no-swap。Weir2CPU/1GiB，ES3CPU/3GiB（heap1GiB/data1GiB），client1CPU/512MiB。
自身loopback hosts与metadata-disable保持；没有IMDS访问或全局修改。

最终 module-qualified JVM selector 已实际唯一选中 ES PID91，start_ticks=233，
Java SHA256 `4ff04917307c25c355f2a96325a214f7cdf1b6261b474c4112166e0fbe73c11f`；
Weir目标PID1。原生 getconf CLK_TCK=100；两角色各产生2个没有 sample.errors 的样本，
native self/退出/解析/JVM/真实pipe等12个测试及标准collector1个测试通过。
这关闭了“最终JVM匹配完全没有实际运行”的局部证据缺口，**不代表完整采样通过**。

仅两份样本的 observer 实测如下，不是140秒完整区间或瞬时峰值，也不从容器总量扣除：

| observer | sampled maximum RSS bytes | 单个约2秒区间 CPU cores | maximum sample ms |
| --- | ---: | ---: | ---: |
| Weir容器helper | 9,568,256 | 0.03497 | 88.663 |
| ES容器helper | 8,642,560 | 0.01497 | 58.098 |


阻断是本轮新增的 Python `/proc/<pid>/limits` 校验过严：Linux原始合法行
`Max open files            4096                 4096                 files     `
在 `files` 后带空格，`re.fullmatch(...files, line)` 因未接受尾部填充而拒绝。
字段本身是4096/4096/files，真实资源限额没有超限。合成正对照没有保留这种原始行尾空白，
因此离线测试漏掉了这一接线缺陷。现场原始表和原始错误 `raw limits/FD profile` 均保留，
没有规范化原始证据、替换样本、修改冻结报告函数或重新执行容器。

两个 observer 被取消后 exit1/context canceled、均已Wait，没有 observer_end。
完整报告因此 partial（原始机器可读错误为缺少 `type` 终止字段）；没有client trial流和前中后覆盖。
实际 trial/setup 命令、seed、planned load、document mutation **均0**，仅1次空 records index管理PUT。
没有产品错误APPLIED或重复写入证据，也没有本轮发生器计时结果。剩余工作首先是按真实
内核文本形状修复 limits 解析并补原始行测试；任何新原生阶段须由统筹另行安排。

## 验证、清理与限制

普通 Python44项通过；优化模式24项通过、20项既有旧入口测试按原规则skip。
最后一次入口小改后的普通/优化各5项通过。开发期两个临时测试目录缺少公开输入文件的失败日志保留。
Go未改，沿用统筹对准确基线的非缓存默认220、race220、相关race3共33和普通/integration vet；
本轮四产物重新编译，12+1个上述Linux CGO0测试真实执行。没有把既有Go测试或构建冒称完整资源验证。

全部57条CLI均有退出结果；两个observer exec先取消并Wait，再按准确ID/owner停止、Wait和移除
三个自有容器。最终另用自有空Docker配置按三个准确ID及owner查询均为空，两个exec PID和
原生入口PID均不存在。没有后台测试/exec或自有容器残留。
默认bridge ID本轮再次变化，其他网络及容器清单保持；这是观察到的环境变化，原因未证明，
没有恢复/删除任何他人网络或宣称全局状态完全不变。

M28原NO-GO和本次M28R NO-GO均保留。公开GHCR仍source `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`；
CNI/跨节点、六平台原生矩阵、其他规格/后端安全、容量和24h全部门槛不变。
认证排除、通用ProgramTransform V1延期且UNSUPPORTED；未开启下一阶段、聊天或定时任务。
