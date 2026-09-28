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

这些是同native Linux arm64发生器、64workers和真实1CPU/512MiB/CPU5的**timing-only诊断**。它们不发送RPC/DB mutation，不能冒充Weir/ES容量或网络资格。最低档新方法必须满足原dispatch p99、零drop和完整采样/计数，才允许新owner进行**最多一次**完整原M22容量调用。工具hash或profile hash改变将拒绝复用发生器receipt。

完整调用仍保留50…3200阶梯、20s warm+60s measure、三次120s确认、最多一档fallback、直连三点+两确认、2倍30s/70%120s恢复，以及原100/250ms时延、5ms lag和所有资源门槛。离线模拟覆盖完整成功、无候选、确认失败及一次降档、过载施压不足、恢复不合格、硬证据失败直接停止；真实未走到的分支明确not-run。

## 执行结果

原生结果及最终门槛将在本轮诊断完成后补入；此段不预宣称资格。
