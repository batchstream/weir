# M28R2 — 字段语义修复与完整原生资源补证

2026-09-28。**本轮声明的本地 Linux arm64 可见 cgroup leaf 采样完整；计时仍 NO-GO，candidate=null。**
一次冻结 attempt 完成三个角色、140秒观察和两次固定短 trial；没有执行第二次。
这关闭 M28R 的 limits 解析和本轮资源采样缺口，不授予生产、容量或其他平台资格。

## 源码、制品和证据

- baseline：`41e65d5952d91cefe56f4b5dec7acd1ba8897e23`。
- Python 实现：`66829c0ba7c9c0a3e3575f3e2bf0f52e33622bd7`；后续交付仅改本文和 readiness。
- 复用 product/helper/test binary source：`1ffc092c2f85a992f341831f23a5833ff6a96073`。
  204份 Go/module 输入与该提交、M28R manifest、当前工作树逐字节一致；没有重编或修改 Go。
- 原始证据根：`/Users/liran/Projects/liran/go/weir/.testdata/m28r2/`。
  `fixture-1/plan.json`、`stage-budget.json`、265份 `source-inputs-66829c0….json`、
  `artifacts.json`、四条原始 JSONL、命令/exit、inspect/identity、`resource-report.json`、
  `operation-audit.json`、`final-cleanup.json`、`manifest.json` 和 `delivery.json` 保存准确来源和结论。
  delivery 文件记录完整交付 SHA、manifest SHA256/文件数和文档提交后的输入复核。

| 实际复用产物 | SHA256 |
| --- | --- |
| Weir | `30effbe4b2f843f72cc3f86af0f57e58ad453624ee0e4e9eb700b674331276e0` |
| helper/client | `683d37ad6f81cc87e828f4d696fcf28c956f7ee66c4bd4b4691a861f32bc2d33` |
| observe.test | `872f0e339f49088eba0b062d2217bbfe51b7cf7784bd292e50cefdbfc7826761` |
| app.test | `d2df6a2c9cd3bae1e54cb6a22288e026b9c2256d4b37feef68e9ca6564f9de18` |

Go1.27.1/CGO0，准确制品从新自有只读目录运行于同架构 Linux7.0.12-linuxkit/aarch64 Docker VM。
环境 image `sha256:ef36debc338afa91481a64a435dbe23400f6252742ff63aaca45cfeedcaebdd9` 只提供执行环境。
ES8.19.22 manifest `sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`，
config `sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160` 均未变化。

## 已修复与离线证明

`Max open files` 按唯一行的字段解析；合法空格、tab和行尾填充不改变语义。
soft/hard 必须是合法非负整数且均4096、单位必须为 files。缺行、重复（包括相同值）、
冲突、非法数值、错误单位、额外字段全部拒绝。只在解析局部拆 token，原始文件和字节不改。
同时核查实际 proc/stat/status、cgroup 标量和计数、TCP表、标准 Prometheus emitter 输出、
ES单节点 JSON；沿用现有 split/数值/集合检查，不增加其他平台或任意文本格式的兼容框架。

默认测试保存 M28R fixture-1 经审阅、不含秘密的原样2+2样本和身份记录，来源及hash见
`scripts/fixtures/m28r-resource/provenance.json`。全部样本的已取得字段检查和身份核验通过，
与统筹 `acceptance-m28r/raw-replay.json` 的诊断剩余字段结果一致，**无需先去掉任何原始空白**。
原流没有 observer_end/client/负载覆盖，原样完整 `report()` 继续 partial；没有添加结束帧。
新的组合测试将 limits/cgroup/TCP 空白变化与缺失、重复、冲突及 ES/metrics 破坏合并验证。
完整 synthetic 正对照、合法drop/慢计时NO-GO和原有守恒负对照保持，不把 synthetic 当原生资格。

同一 `resource_local.py` 复用显式 evidence/owner，冻结 stage budget 中的路径、owner和
fixture-1/fixture-2。解析后的路径限定在本repo自有.testdata；拒绝旧M28/M28R目录、碰撞及别名。
独占window与attempt claim防止目录改名后复用次数；第二次须保留首轮、清理成功、零负载、
原样回放和明确接线诊断、新实现提交。开始seed/trial前持久记录，之后不能第二轮重跑。
总900秒、每次清理120秒、6000planned/2400文档mutation保持；本轮只用了fixture-1。
负载prefix仍为已证明的m28r-through/direct。挂载改为逐项核验本轮准确路径与目标，未复制入口。

Python全套普通155通过；优化130通过、25既有skip。最终相关7模块普通69通过、优化56通过/13既有skip；
最后仅组合空白测试补充后，resource普通15通过，优化相关69项已包含该补充。
Go未变，引用统筹此前相同204输入的默认220/race220/聚焦33及vet；本轮未重复这些Go套件。
准确复用binary在本轮再次完成Linux CGO0采样/管道/JVM等12项及标准collector1项。

## 本轮原生实际完成

Weir2CPU/1GiB、ES3CPU/3GiB（heap1GiB、data tmpfs1GiB）、client1CPU/512MiB。
ES非root1000、Weir/client65532；observer与目标同UID/namespace/cgroup。独立PID/mount/private cgroup，
dropALL/no-newpriv、只读根、有限tmpfs/pids/log、no-swap。ES network-none，另外两角色只共享自有loopback网络，
监听127.0.0.1，无publish/hostPID/ptrace/提权；使用自有空Docker配置。未访问IMDS或既有秘密。
实际Weir PID1/start_ticks1288；ES JVM PID91/start_ticks223、Java hash
`4ff04917307c25c355f2a96325a214f7cdf1b6261b474c4112166e0fbe73c11f`；原生CLK_TCK=100。

负载前至少两份有效样本通过。Weir/ES各71份、正常observer_end，实际跨度140.000836/140.006509秒；
client-through/direct各23份。所有sample.errors为空，序号、身份、资源值、前中后覆盖和计数检查通过。
资源报告重放与现场报告的序列化结果一致：`complete-for-declared-visible-leaf-profile`。

| 角色 | sampled maximum RSS bytes | cgroup memory.current bytes | 最大gap秒 | 最大sample毫秒 |
| --- | ---: | ---: | ---: | ---: |
| Weir | 22,335,488 | 72,380,416 | 2.003286 | 81.278 |
| ES | 1,467,006,976 | 1,831,391,232 | 2.010648 | 54.975 |
| client-through | 19,308,544 | 69,427,200 | 2.055919 | 87.312 |
| client-direct | 17,805,312 | 69,574,656 | 2.053907 | 87.851 |

Weir/ES observer sampled maximum RSS分别14,888,960/13,275,136 bytes；最大采样区间CPU
0.035058/0.025002 core，均满足96MiB/0.25core。开销已计入容器总量，未扣除。
Weir/ES cgroup CPU累计增量13,217,120/18,947,637微秒，观察区间throttled增量均0，OOM事件均0。
这些是sampled maximum，不是瞬时峰值；隐藏祖先仍unknown，TCP表属于共享网络namespace。

固定1KiB、50ops/s、20秒warm+20秒measure，各一路through-Weir/direct-ES：

| trial | planned | success | drop | warm/measure发生器lag p99 | DB版本1 / absent |
| --- | ---: | ---: | ---: | --- | --- |
| through | 2000 | 1995 | 5 | 11ms / 12ms | 199 / 1 |
| direct | 2000 | 1995 | 5 | 13ms / 12ms | 200 / 0 |

所有发出操作成功、零失败/UNKNOWN；独立DB内容/version1审计与200槽ledger、窗口、receipt守恒。
两个trial共4000planned，加2000条seed，总6000；实际文档mutation为2000seed+399Put=2399。
首轮另有一次空索引metadata PUT，trial管理请求为2次delete/2次create/2次refresh，与文档mutation分列。
5ms发生器门槛未改变；drop及lag超限导致**timing NO-GO**，不能解释成Weir可持续吞吐上限。

## 清理、历史与仍未完成

fixture154.247秒、cleanup3.236秒，外层启动至Wait157.635秒。64条fixture CLI全部有退出记录；
预算内readiness的4次ES exit7和1次Weir probe exit1保留，之后正常ready，不属于第二次attempt。
两个observer正常退出并Wait，随后三个准确ID/owner容器依次Stop→Wait→remove。
另用新自有空Docker配置查询三个ID及owner均为空；两个exec PID和入口PID均不存在。
没有OOM/restart或后台自有测试。默认bridge ID发生变化、原因未知；非默认网络和其他容器清单保持，
没有修改或恢复他人网络。离线audit首次tuple/list表示比较失败的记录保留；修正比较表示后原始证据未变。

M28的744份和M28R的320份现存文件逐项hash不变，包括原manifest与delivery。
M28原三次失败、M28R失败及其partial结论保持，不倒写旧窗口或资格。
公开GHCR仍source `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`，M27/M28未发布。
本轮没有push/CI/镜像发布、EKS、容量阶梯、过载/恢复或24h。
计时、容量、CNI/跨节点、六平台原生矩阵、其他规格/后端安全及24h仍required；candidate=null。
Auth排除、通用ProgramTransform V1延期且UNSUPPORTED不变。阶段等待统筹独立复核，不自行启动下一阶段或定时器。
