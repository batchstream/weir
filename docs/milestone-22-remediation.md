# M22R：校准工具可信性补救与有限原生复验

原 M22 **未通过**。基线 `7806c5ff679f51ebd364640116677f5c905ce525`；本轮仅修改校准工具，执行证据仍待统筹独立验收。原 `docs/capacity-baseline.json`、`docs/calibration-plan.json`、`scripts/capacity-plan.json` 及五次准备失败、最低50档失败全部保留。原测量3000 planned /2569 success /431 late-drop、296 APPLIED /104未发起，以及warm阶段192次丢弃不改写。这些结果不证明Weir低于50ops/s。

维护入口 `scripts/test-capacity.py` 现在只使用 [M22R固定计划](../scripts/capacity-plan-m22r.json)，通过 `capacity_contract.py` 共享门槛与预算。原M22文档中的命令为历史入口；当前完整校准还必须提供同工具hash的原生发生器资格receipt。产品source `315819fcd2c0cae1c22604e85ccdb5bb9291f585`、70项产品输入、OCI config `caa699e6ca172cbfa24ed4d311f4cb346817a354b05df52601abd954edcb4dab`、binary `9def37fc9f4d552d35af552f87f86ba0b45bd2bdcf948114237fb5d312c97a32` 不变。

## 固定计时契约

| 边界 | M22 | M22R |
| --- | --- | --- |
| 计划时刻 | 起点+n/rate | 相同；不移到新窗口 |
| timer返回后的第一个时刻 | 未记录 | wake；已过期的循环也记录实际观察时刻 |
| 构造 | 混入5ms丢弃判断 | 构造结束减wake；包含ID/Operation构造 |
| 决策 | 构造后超5ms立即丢弃 | 获得统计锁后、零缓冲handoff前；所有到期项均记录 |
| worker开始 | 未独立记录 | 从channel取得项后的首个时刻；handoff=worker-start减decision |
| 实际dispatch | Call前 | 创建原计划时刻+1s的context后、Call前；lag包含此前全部开销 |
| 逐条过期 | >5ms | >20ms；worker若到1s deadline或取消则不调用 |
| 追赶 | 受5ms过期截断 | 单次timer唤醒后至下次等待最多派发8条；无缓冲、64worker；多余明确drop |
| 资格 | lag p99≤5ms | **不变**，all/read/put分别判断；稳定drop/error/UNKNOWN=0 |

wake/decision/construct包括所有到期计划项；提前取消的未来项单列 `cancelled_future`，没有伪造timer或完成时延。arrival/dispatch/lag只含真正调用的项，包括失败与慢调用；drop不能伪造完成。worker到deadline的项只记录worker时刻与drop原因。所有固定桶均保留count、p50/p95/p99/max和overflow；计数恒等及原始桶重算用于资格。`worker_start`的p99与`lag`不可相减当作阶段p99；应看独立handoff/construct分布。

真实调用仍为原计划时刻开始的1s总deadline。20ms不是新的SLO，超过5ms的调用保留于lag分布。无busy-spin、runtime私有接口、额外队列、逐请求goroutine、host/VM调优或闭环降载。计时、记录、锁和client自采样开销均计入相同1CPU/512MiB/CPU5预算。编译出的expiry/catchup/deadline值在prepare与计划逐项核对，防止两套常量漂移。

## 缺证据失败关闭

角色明确为Weir、ES、client。Weir必须有正确标签的Linux Guard、有效性、内存budget/current/limit、全部相关Store账本、owner/peak/limit、业务/诊断连接及session；非此负载使用的Native/Scan账本要求可见且为零。ES不要求Weir指标，仍必须有实际进程/cgroup/CPU/句柄证据；client同样要求实际CPU quota/affinity/内存与GOMAXPROCS、goroutine。缺值从不默认为0；未知owner/connections输出null。

每个角色的memory.current/max/swap/events、cpu.max/stat、cpuset、pids、RSS、FD都须存在、可解析、有限、符合固定规格。CPU计数重置、缺cpu.stat、样本errors、缺指标、截断、重复、NaN/Inf、关键标签不匹配均拒绝。DB单节点、write/get队列/拒绝、JVM/process/http/IO字段和时间戳必须存在。DB累计拒绝用窗口内增量，避免过载后的旧累计计数伪装成恢复期新拒绝。

采样名义2s，**相邻单调时间间隔≤6s**，每次采样持续≤2s；wall与单调增量偏差≤1s。完整warm+measure负载、稳定测量及每个恢复10s cohort均须被前后样本夹住，边缘离窗口≤6s。单样本不能证明全窗；CPU同时报告全窗平均与最大相邻采样区间占quota比例，≥90%不合格。资源最大值仅是采样最大值，不是原子峰值；未声称准确制品的Go heap/goroutine普查。

活动调用期间每2s读取observer并检查硬边界、观测新鲜度及owned进程Running/OOM/restart。观测失效、硬账本/owner越界、OOM/restart及正确性响应/全量审计失败终止整个校准；不能降档再选候选。稳定SLO失败可按原搜索规则停止升档。过载允许预设拒绝，但发生器未完整施加2倍负载或恢复不合格时candidate必须null。

## 输出与资源回收

子进程只获得stdout/stderr管道，不获得证据文件描述符。每次读取≤64KiB，单流最多8MiB、combined最多16MiB，证据总量256MiB在写入前检查。瞬时正常退出、持续输出、stderr、非零退出和timeout均走同一捕获路径；超限保留有界前缀，在command记录中标 `truncated=true`，失败并Stop/Wait整个自有process group。解码只在有界bytearray上进行。不会先读完整文件再判长，也没有无限临时磁盘输出。

清理分别记录诊断失败和资源删除结果。stop后重新核验owner+准确ID+Running=false，诊断save/log失败仍尝试安全rm；每个独立owned资源分别尝试。磁盘耗尽时清理命令仍可使用有界内存捕获，证据写失败保留为失败，不跳过已确认的资源。归属不明、替换ID或仍运行的容器不强删。最终清理报告列出每个资源与诊断错误，不能把“已停止”当“已回收”。

INT/TERM使用独立异常，避免被Python selector的InterruptedError处理吞掉；清理期间忽略重复信号。Darwin对只剩zombie的process group可能返回EPERM，须查询该准确group的PID/PGID/state确认无活进程，不能直接吞掉EPERM。全新独占root、空HOME/Docker配置、只读碰撞预检、首次修改前库存、默认bridge单列和非默认对象不动的M21R边界保持。

## 单一mutation预算

controller在任何setup/trial发送前预留并保存预算。bootstrap1000、每trial seed1000、warm、measure、overload+recovery均进入同一账本；recovery不重复seed。`planned`含seed和计划调用，`load_planned`仅计划到达，`reserved`为全部计划document mutations；索引管理请求另属setup管理操作。全局5,000,000 planned、500,000 mutations、单trial100,000 mutations、35分钟负载、45分钟调用及180s清理不变。

setup逐次发起前输出started进度，正常/错误返回输出最终mutation receipt；完整trial以Put started对账。中途失败仍保留预留数和实际调用数。若进程被杀或输出截断导致最终receipt不在，actual-started保持null，报告下界和预留上界，不把未知计成0或释放预算后继续。全部写入仍只发送一次，所有实际id/version1/payload审计保持。

## 预先冻结的原生诊断

唯一入口：

```sh
WEIR_CAPACITY_INTEGRATION=1 python3 scripts/capacity_pacing.py \
  --owner weir-m22-UNIQUE --evidence /absolute/new/evidence/root
```

先完成离线反例、默认test/race/vet、integration vet和capacity三轮race。prepare核验产品70输入、完整OCI/binary，生成Linux arm64工具、记录host/VM/source/tool hashes与只读expanded remediation-plan。之后才导入自有client image和创建一个network=none的owned client；不启动Weir或ES。

固定先做原计时方法的50/200/800ops/s各20s（仅添加观测，原5ms逐条过期）。若50档wake p99已超过5ms，则没有支持“仅修唤醒后路径即可合格”的证据，完成原三点即停止。若wake路径可行，再做唯一允许的20ms过期/最多8条追赶修正，同三速率各20s。总计最多6探针、各≤20s、native诊断总≤300s；全部保留，没有重复同一点选绿。

`legacy-expiry`使用冻结的新仪表化引擎保留原timer/5ms过期策略，decision仍按新定义在统计锁后观察；包含新增观测开销及该边界差异，不称旧M22二进制逐位复现。

这些是同native Linux arm64发生器、64workers和真实1CPU/512MiB/CPU5的**timing-only诊断**。它们不发送RPC/DB mutation，不能冒充Weir/ES容量或网络资格。最低档新方法必须满足原dispatch p99、零drop和完整采样/计数，才允许新owner进行**最多一次**完整原M22容量调用。工具hash或profile hash改变将拒绝复用发生器receipt。

完整调用仍保留50…3200阶梯、20s warm+60s measure、三次120s确认、最多一档fallback、直连三点+两确认、2倍30s/70%120s恢复，以及原100/250ms时延、5ms lag和所有资源门槛。离线模拟覆盖完整成功、无候选、确认失败及一次降档、过载施压不足、恢复不合格、硬证据失败直接停止；真实未走到的分支明确not-run。

## 执行结果

**NO-GO，candidate=null。** 原生发生器最低档未合格，因此没有启动完整Weir/ES校准。执行证据待独立验收，脚本exit0仅表示本次有界调查与清理完成。

| 原方法速率 | planned / started / drop | 全部到期wake p99 / max | decision p99 | 已派发lag p99 | 平均CPU / throttle |
| --- | --- | --- | --- | --- | --- |
| 50 | 1000 /857 /143 | 8.9ms /9.604ms | 8.9ms | 5.0ms | 1.126% /0s |
| 200 | 4000 /3998 /2 | 2.1ms /5.298ms | 2.1ms | 2.1ms | 2.109% /0s |
| 800 | 16000 /15988 /12 | 1.6ms /13.014ms | 1.6ms | 1.6ms | 4.054% /0s |

三点共21000次计划、20843次worker调用、157次expired；全部为timing-only，真实DB mutation=0。50档的construct/handoff p99均为100us桶，max分别0.472/0.360ms；主要迟滞已在首次wake观测时出现。已派发lag排除了过期项，不能拿其5ms边界证明完整50档合格。200/800的较低wake p99不改变最低50档失败，也不证明Weir吞吐。

按预先冻结条件，50档wake p99>5ms后不运行新过期策略三点，不反复跑到偶然绿，不启动ES/Weir。真实确认、预设降档、直连、过载、恢复及产品id/version1/payload审计全部 **not-run**。这些算法路径仅有离线模拟；新增Weir/ES资源验收的正向原生路径也未执行。

真实client是Linux arm64、Linux7.0.12-linuxkit、GOMAXPROCS=1、cpuset5、cpu.max100000/100000、memory.max536870912、swap.max0。每点12个client样本完整夹住20s窗口，OOM/kill均0；最大所见RSS21,356,544B、cgroup18,382,848B、FD7。资源/CPU均为采样证据。总定时负载60s，native诊断墙钟60.436s，含准备/清理完整调用67.582s，未超冻结预算。

原生测量source `3c78a0aad16e695aa4daf26177ce5a3bca10ee87`，工具binary SHA256 `46605874af06709cd19fe6617cd6220b2dc9a76d398e094785be7b3f6deebd9f`。固定profile SHA256 `766534e4dba448becae84ccb50bc94179b563bf940a3f9be569375b344370a8b`；[expanded M22R plan](calibration-plan-m22r.json) SHA256 `f2c429dc82125a91da85d8f55733e476008f928ffa3d2c8d4a1ee935de6c4830`。145份native证据共1,203,069B已逐项重算hash、21项tool inputs已对原生source Git对象校验，70产品输入再次一致。原生manifest SHA256 `6aa562eb4bf2ce0f1ae4d62a6066e2af466ccada8ee64110d102997d4ea2731a`。

最终Python实现 `cc992f71a2d78cd86bc4eeea2fba1fc45fa183e8` 在测量后补齐Guard标签互斥语义、实际process affinity/TCP字段校验，并把超过四参数的入口整理为具名options、将辅助process-state查询的捕获也限定为64KiB；没有改Go发生器、资源或计时契约，没有新增探针。原生证据仍归属3c78a0a；新旧tool hashes分别列在[机器基线](capacity-baseline-m22r.json)，不拿最终工具hash冒充测量来源。当前入口也不会接受不匹配的旧工具资格receipt。

## 验证与交接边界

- 全默认CGO0 test count1：61.856s通过；CGO1全race count1：65.119s通过；vet/integration vet/Linux arm64 integration vet通过（Linux项仅静态）。普通和integration capacity各三轮race通过，8.773/9.016s。Go代码此后未变。
- 最终Python普通30项通过；优化模式30项发现、20项明确普通模式专属跳过，其余10项及优化入口提前拒绝通过；packaging5项通过。完整搜索的success/no-candidate/confirmation failure/fallback/underload/recovery failure/fatal observation分支均离线执行。
- 原审查四反例修后4/4通过。保留原反例文件，移植副本仅补新schema观测元数据、将旧文件描述符fake替换为真实受控17MiB输出child；拒绝超量与诊断失败后必须尝试rm的断言未弱化。另存合法合成counts/低延迟但无Weir metrics的完整evaluate反例：pass=false，owner/connections=null。
- 测试开发中清理EPERM/INT异常、优化测试导入失败等日志均保留；首次未设GOROOT的编译失败控制台记录保留于本聊天，显式设置固定GOROOT后通过。未把这些失败混成native容量失败，也没有删除原M22的五次准备失败。
- owned client `5687229cb393843d218d4cefe4e4d7dc1bf640524fb3801ba6cfdc8119b14e7e`已stop/rm，自有client image tag已删除，生成binary/tar已按hash记录回收。network=none，没有创建新网络/卷；Weir/ES/observer从未启动。最终只读复核running容器0、该owner容器/网络/tag0；原4个退出M2容器、redis_default及既有制品保持。
- 初始/首次修改前/最终库存已保存；本次default bridge与所有非默认对象无变化。M21R历史bridge原因仍未证实，原外层审计退出1继续保留。

最小外部决策是由统筹选择可在相同1CPU/512MiB预算下满足原50档计时资格的native Linux arm64 runner；可先只提供该client诊断环境，不能因此提高5ms门槛或扩大预算。本轮只证明当前固定环境/发生器组合未满足最低档，未证明具体VM成因。是否换runner由统筹决定，本聊天停止，不启动下一阶段、自动化或重复调查。

原生记录在 `.testdata/m22r/native-pacing`，补充反例、验证和逐桶重算在 `.testdata/m22r`。最终门槛逐项passed/failed/not-run及证据索引见机器基线/receipt。其他平台、profile、24h、Weir goroutine/heap、OpenSearch安全和Linux Mongo缺口保持；Weir认证排除、通用ProgramTransform首版延期/UNSUPPORTED保持。

最终[补救receipt](milestone-22-remediation-receipt.json)索引197份保留证据、1,423,813B，索引SHA256 `fad69feaf8008f3608b48b1c16117a9e894b0a8bff860994ed63338fc0451032`。索引不含其自身或Python缓存；原生145份manifest保持不变。
