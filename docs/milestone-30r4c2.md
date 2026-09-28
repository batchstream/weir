# M30R4C2 — 固定小批次完整盘点，原两项残留已回收

2026-09-29（Asia/Shanghai）。**本次 cleanup-only 成功；原 Namespace/Quota 两个准确 UID 已回收，无残留。**
唯一新 300 秒窗口实际耗时 **130.151434 秒**，Namespace 最终由命令30的 **exit0 空 GET** 确认不存在。
这是清理补救，不是 M30 采样或生产资格通过；M30R4 与 M30R4C 原失败永久保留。

## 根因和最小变更

基线 `3439dba0096d8198f37907fe0ac6cf13350598e4`；实现/在线执行 `8a6c8c4fb4e0b767ef9126dccac33578f1d8650e`。
只改 `eks_pacing.py`、`eks_cleanup.py`、已有离线 CLI fixture 与测试，没有 Go/module/产品、镜像或 observer 改动。
共享 inventory 固定每批最多 **5** 类、顺序查询；完整 discovery 中只排除 Secret 查询，未知资源/CRD不排除。
正式 PodMetrics API discovery 与只读视图验证保持。重复 catalog 拒绝；每批投影必须完整且具 namespace/行终止，
失败、截断或超限立即停止，不把此前成功批次作为完整盘点。末尾一类同样执行与验证。
每批原始 stdout/stderr/exit/时间仍由共享 CLI 保存，只有全部成功后才执行 foreign_check。

此前20类批次并非仅因本轮网络猜测：M30R4命令49与M30R4C命令10的完整argv相同，前者正常成功耗时21.584738秒，
后者在21.049492秒触发本地停止/exit-15；M30R4命令138同argv成功19.491152秒。
旧20类聚合已在正常路径超过21秒执行额度。原失败当时总180秒仍有余额，底层各API/网络具体延迟原因未知。
上述原始文件未修改，比较见 `batch-counterexample.json`。

cleanup-only仅新增接受冻结的300秒，保留180秒入口；deadline直接使用首次在线起点加计划秒数，传入已有共享cleanup。
普通共享fixture仍默认180秒。最后45秒Namespace/readback预留、单CLI最多25秒含4秒Stop/Wait、
单请求10秒、单命令8MiB/总128MiB不变。没有失败后缩批、补发、并发或重试框架。
二/五当前DELETE目标与原始六UID的划分、共享delete/foreign_check不重构。

## 冻结与唯一在线结果

- 固定context：`arn:aws:eks:us-west-1:956540890581:cluster/data-team`。
- namespace/owner：`weir-qual-m30r4-20260928-233348`。
- 全新证据根 `.testdata/m30r4c2/` 0700，计划0400，54项输入冻结，exclusive invocation只有一次。
- plan SHA256：`dc5bb7c4720d7f35413d60a78c361f565a0cfd62bb045ddc2a36b0598fff819d`。
- 起点UTC `2026-09-28T16:42:36.825389Z`；monotonic `388425.234850541`。
- 总deadline `388725.234850541`，对象deadline `388680.234850541`，不续期。
- 在线130.151434秒；外层进程含本地Git/启动/Wait共130.231214秒，exit0。

首次Namespace/Quota读取确认原UID/owner，Namespace Active；命令5空Pod列表。
命令7完整发现61种非Secret list资源；命令8正式metrics发现确认只读语义；命令9读取Quota三处Secret计数均0。
命令10–22完整盘点61类恰各一次，12批5类加末批1类；所有exit0。11条metadata中：
两个准确默认对象、原Quota、4个历史Pod Event各经core/events API返回一次；没有Job/Pod负载或外来持久对象。
历史Job Event的允许/拒绝边界在离线保留盘点中覆盖，本次fresh列表没有Job Event。
默认ConfigMap UID `7e99a235-b98e-47b5-937f-f1836aa4ac0e`、ServiceAccount UID `d045fac6-a713-43f6-9fe0-0e28d0256cbd`均未变。
完整foreign/default/Event检查后，命令23再次核对Namespace身份；命令24再次核对Quota身份与Secret0。

| inventory命令 | 类型数 | 耗时秒 | metadata行数 |
| --- | --- | --- | --- |
| 10 | 5 | 5.931 | 5 |
| 11 | 5 | 4.923 | 2 |
| 12 | 5 | 5.213 | 0 |
| 13 | 5 | 6.532 | 0 |
| 14 | 5 | 5.466 | 0 |
| 15 | 5 | 6.520 | 0 |
| 16 | 5 | 6.605 | 4 |
| 17 | 5 | 4.920 | 0 |
| 18 | 5 | 5.839 | 0 |
| 19 | 5 | 4.011 | 0 |
| 20 | 5 | 5.995 | 0 |
| 21 | 5 | 8.399 | 0 |
| 22 | 1 | 3.860 | 0 |

批次资源名逐项清单与并集验证见 `cleanup-audit.json`；每批原始证据为对应 `command-*.{json,out,err}`。
CLI最大耗时8.398720秒；没有触及原21秒执行额度或挪用最后45秒预留来完成盘点。

| 准确目标 | UID | DELETE | 成功不存在证据 |
| --- | --- | --- | --- |
| ResourceQuota/budget | `2deaa5be-0822-4db5-83bc-df4d2ec9233e` | 命令25，恰1次 | 命令26 exit0 空GET |
| Namespace | `f6b048cc-447d-4ee8-8835-11ef13542758` | 命令28，恰1次 | 命令30 exit0 空GET |

两次使用原UID的DeleteOptions preconditions，propagationPolicy=Orphan；没有force、finalizer修改或label批量删除。
命令29仍为原UID的Terminating，不作为成功；命令30真正不存在后才返回confirmed=true。
总30条CLI：2Git、26只读kubectl、2条件DELETE；全部exit0并经共享stop_group Stop/Wait。
无新建Kubernetes对象、负载、Secret查询/读取、默认对象逐项删除、集群/节点/RBAC/业务修改或扩容。
不读kubeconfig/credentials/token或证据白名单外匿名客户端状态。窗口结束后没有额外kubectl或新窗口。

原Job `6a24a607-61fc-4e85-9e40-60b34f37a5ef`、Pod `27e6d830-67fa-4f70-9de4-64db840734a6`、
ConfigMap `51b30696-baf1-4eb7-a6ba-3e26dd7aafc8`、NetworkPolicy `574bf5a6-ab12-4a3b-99ed-558467ef4a85`
本次DELETE均0。它们的原成功空GET124/128/131/134保留；历史Job/Pod UID只用于Event归属。

## 离线验收与封存

聚焦48项通过（95.100秒），另增不确定DELETE/证据丢失1项通过（2.151秒），共49项。
使用已有实际61类discovery/完整metadata fixture，按请求类型返回行并核验每类恰一次和末尾单资源；
不再把所有行塞进configmaps批次。覆盖默认UID、未知/晚到CRD、同名替换UID/owner、外来Pod/Event、
正常历史Job/Pod Event、Secret未知/非零、正式PodMetrics发现语义、首/中/末批失败/超时/截断/超限、
总300秒和45秒预留、磁盘/输出失败、冻结值篡改、第二次invocation、旧180秒/五目标入口。
不确定DELETE后只读核对，拒绝/丢失证据时不重放且不删除Namespace。

真实fake子进程反例固定启动2秒+每API1.05秒；整个时钟及子进程等待等比缩放20倍，
旧20类模型23秒在原21秒执行额度内失败/exit-15并Stop/Wait，新5类模型7.25秒完整13批通过。
没有放宽生产CLI时限。独立留存原始子进程命令、stdout/stderr、exit与时钟，见 `batch-cost-replay/`；
这些仅为离线证据，不算EKS通过。真实EKS证据单列于上节。
附加catalog对照最初误比较禁读Secret在原列表中的位置；实际61类查询序列完全一致，
差异记录见 `catalog-comparison.json`，不涉及产品修改或线上重跑。

完整Python普通 **207项通过，204.923秒**；优化模式 **207项，182通过/25项既有skip，202.267秒**。
原始日志/退出/Wait见 `all-normal.*`、`all-optimized.*`，逐项skip名称与原因见 `tests-summary.json`。
命令为 `python3 [-O] -m unittest discover -s scripts -p '*_test.py' -v`；没有重复Go/race/镜像/数据库测试。
执行前后逐项核验原M30R4C 67份、M30R4 544份、更早四轮1001份，以及204Go/module和Weir70/helper81正式输入。
M30R4C manifest保持 `e9624a6c1d4682951b24389c7c5a38f7c43b7a04b48dd2a7394f26fab5c33c8d`；
M30R4保持 `87fbd519d33fa9beacfaf7e734c3ce7c3bbd44fe968a3fe0cd54dbc1d4925bfd`。
准确image source仍 `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`；校验只读取封存白名单及公开正式输入。
本轮manifest封存计划/输入、CLI、测试、审计和历史校验；排除自身与避免自引用的delivery.json。
最终manifest hash/文件数、交付SHA及main/进程结束状态见delivery.json及完成回调。

## 原失败和资格不变

M30R4原failed/cleanup=false及M30R4C failed/cleanup=false不倒写。
原两observer缺observer_end、Weir尾行截断、client snapshot缺失仍未解决；
resource_evidence=partial/not-qualified、timing=not-run、candidate=null不变。
Weir自身Auth排除、ProgramTransform首版延期、CNI/跨节点、六平台、其他规格后端与24h门槛完全不变。
本轮未push/CI/发布/镜像/Go-module/产品改动、不继续采样、不修observer、不新建对象或负载。
清理补救完成后回调统筹并停止，不续派、不恢复timer、不规划下一阶段。
