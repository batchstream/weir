# M30R6：一次 EKS 无负载资源采样失败，六对象清理完成

2026-09-29。基线及唯一实际执行源码 `bc0f3734d48e07ded97d48daf1d44c8856c140cd`，image source `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。本阶段没有修改执行脚本、产品 Go/module、正式 helper 或镜像输入，没有重新下载、构建、push、Actions 或发布。

**本轮 NO-GO：Weir 唯一 observer 的升级流发生 connection reset by peer，exit1；仅取得序号 0–4 的五个完整样本，第六条截断，缺正常 observer_end。ES observer 和 client snapshot 按首败停止规则未运行。六个自有对象已在原 300 秒清理窗口内全部回收。**

`resource_evidence=partial/not-qualified`、`timing=not-run`、`candidate=null` 保持。此次 reset 的发起方与原因未知；也没有新证据解释 M30R4 reset。不能从相似症状归因到 request-timeout、网络设备、客户端或服务端。

## 固定身份与静态准入

- context：`arn:aws:eks:us-west-1:956540890581:cluster/data-team`；region `us-west-1`，cluster ACTIVE / Kubernetes 1.36。
- 唯一节点 `ip-172-31-12-243.us-west-1.compute.internal`，UID `18f03c58-83c1-424e-be34-44ef88c07831`。新查询确认 Linux arm64、Ready、无 taint/unschedulable/deleting；内核 `6.12.77-99.140.amzn2023.aarch64`，kubelet `v1.35.3-eks-bbe087e`。
- prepare 与 native 写前两次限定节点、全部 namespace 的 Pod 资源投影通过，均为六个已有 Pod。初始余量 CPU 7.030 核、内存 31,202,312,192 bytes、ephemeral 18,182,813,665 bytes、52 个 Pod 槽位；原最小 7 CPU / 5632 MiB / 5 GiB / 3 Pod 门槛不变。Job dry-run 后同节点再次核查通过。
- 23 项权限检查全部新执行。只读取 `aws-node` 的最小容器/image/policy flag 字段；CNI `v1.21.1-eksbuild.7`、agent `v1.3.2-eksbuild.2` 仍为 `--enable-network-policy=false`。namespace/default-deny 不构成已证明的网络隔离，`network_isolation=unqualified`。
- 仅复用 `.testdata/m30/artifact/` 的五个公开文件；现有 `verified_helper` 重算 index → arm64 manifest → config/layer/diffID → 唯一 binary 链。helper 为 21,046,963 bytes、0555、SHA256 `d41f70ca4bbe129bff11b76f3d973cdb288d839c4df72a348cac04a233b76482`。没有读取旧匿名 registry clientstate 或已有秘密。
- 204 个 Go/module 文件与 70 Weir / 81 helper 正式输入逐项匹配 image source；29 项原脚本冻结输入及 Python/kubectl/aws/git 工具身份保存。kubectl 为原 `v1.35.2-eks-f69f56f` darwin/amd64，SHA256 `d6487d72d341c1db4d7fe6840e96cd3c99d75e8fe6dcef205ceb1c987ebf84f5`。

准确 Weir index 为 `sha256:2a3ca21b950f42449b01543e96655e0484e432f8ecbf29b46c49f7fd426c229a`，arm64 manifest 为 `sha256:100102a8c319a24f571f2aaa3041d6fafc79338840829e792f404e48d117dd5e`；实际 `/weir -version` 为 clean `local-4abc8761…`、Go1.27.1、linux/arm64，完整样本的目标 binary hash 为 `f2d107995762040c55b03cb37e94dd9e6beaa14a99d563b278b4bd9c2bd4db7e`。

qualification index 为 `sha256:fbef16495a37b89d44cfce51c38096e01e982ab875e3352b67c59480fdbe61a7`，arm64 manifest 为 `sha256:9eb2ff4243d7c021abe047da28867a9b3df65695660675ebcb761387b4f50351`。ES 固定 `sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`；本次原始日志确认 ES8.19.22 / JDK27，Java binary hash `4ff04917307c25c355f2a96325a214f7cdf1b6261b474c4112166e0fbe73c11f`。四个实际 containerID/imageID 保存于 `native/runtime-identity.json`，不能用容器身份代替尚未取得的 ES/client 进程资源快照。

## 唯一窗口与首败

owner/namespace 为 `weir-qual-m30r6-20260928-182504`。prepare、native invocation 均恰好一次；原脚本 create-only 冻结计划 SHA256 `39a6bb4fa9777af4f4c29996b33630022a329544343e07085c89bdbb5291582d`，没有重新 prepare、第二 namespace、第二 observer exec 或临时补丁。

| 阶段 | 冻结预算与实际证据 |
| --- | --- |
| 静态阶段 | 63.875 秒后才启动动态资源窗 |
| 动态资源窗 | 同一 monotonic start `394670.289179166` / deadline `394790.289179166`；prepare 与 native 写前四次 GET 共 10.584 秒，派发前余额各 ≥29 秒；恢复 0 次 |
| 主窗口 | 原 900 秒，实际 300.193 秒，含失败后的有界诊断；原截止未移动 |
| startup / upload | artifact-ready 420 秒、upload 300 秒仍嵌入主预算；上传输入耗时 43.042 秒，完整 exec 43.775 秒，exit0、双 EOF、Join |
| Weir 角色 | 原 60 秒；前身份检查、exec 和失败收尾合计 20.845 秒。exec 13.815 秒后 exit1，未触及角色本地截止 |
| ES / client | 首败后未派发，没有第二次采样求绿 |
| 清理 | 同一起点 300 秒，含最后 Pod 身份读取，实际 105.087 秒；对象阶段与 namespace 45 秒预留不变 |

一次完整上传后，release exit0 且回执完整；同一 namespace/Job/Pod/bootstrap container 身份、init Completed0、immutable ConfigMap 原文与完整有序 bootstrap 日志共同确认远端管理完成。空 `records` 索引 PUT 恰好一次。无 upload/release/PUT 重放；seed/setup/pace/trial/文档负载均未运行，planned/document mutation=0。

同 Pod loopback 正向与 ownPodIP 负向检查完成，未创建 Service、跨 Pod/host namespace/hostPath 或真实 IMDS 访问。非 root/drop ALL/RuntimeDefault、适用 readonly root、64 MiB helper volume 的 bootstrap 独占 RW/运行期 RO、单 Pod 6 CPU / 4608 MiB / 2560 MiB ephemeral 预算均由原准入校验保持。

Weir observer 活着时没有任何阻塞控制面 CLI 与之重叠。其前置身份读为 command115–117；失败后的首个诊断 command118 在 observer 已 Stop/Join 后才开始。原始 stdout 247,333 bytes：identity + 五个完整样本换行记录，以及 4,091 bytes 未完成尾部；stderr 607 bytes 保存 TCP reset 报错。`weir-exec.json` 记录 exit1、双 EOF、joined=true、stopped 时间；首个 transport 错误及 stop_error 均保留，不将 EOF/Join 当采样成功。

完整五样本序号 0–4，覆盖 8.000449493 秒，单样本约 54–56 ms；逐条既有 `sample_check` 离线核验通过。目标 PID1、observer PID385、同 UID65532/namespace/cgroup，FD 9，soft/hard FD 1048575/1048576，pids.max 37697，CPU `200000 100000`，memory.max 1073741824，swap.max 0，cpuset `0-7`；RSS 20,684,800–21,295,104 bytes。proc/limits/cgroup/CPU/memory/PIDs/io/TCP/metrics 原文保留。以上仅为本次部分样本，不作为完整角色资格或后续负载阈值；隐藏祖先仍 unknown。第六条不能按完整样本计数，既有完整报告解析明确拒绝截断流。

管理 HTTP 计数为 PUT1；bootstrap/main 两次完整边界各含两个 GET。五个完整 Weir 样本提供五次成功 metrics 响应的下界，截断流的总次数 unknown，原 `operation-audit.json` unknown 不改写。ES observer HTTP0、client snapshot HTTP0；kubelet 探针次数 unknown。

## 清理与所有权

原 300 秒内全部准确 UID 条件 DELETE 各一次，propagationPolicy=Orphan，随后 exit0 空 GET 确认；没有 force、finalizer 修改、外来资源删除或预算外 cleanup-only。

| 对象 | 名称 | UID | DELETE / 空 GET |
| --- | --- | --- | --- |
| Job | loopback | `b2817d65-c08e-4158-a9e9-4e8dfa1e7625` | 129 / 130 |
| Pod | loopback-n9khp | `f1e2a68e-8bea-420f-af4c-d73a5bddb15b` | 132 / 134 |
| ConfigMap | configuration | `cf7ca5bb-52dc-43ad-ae6b-377f52dd9668` | 136 / 137 |
| NetworkPolicy | default-deny | `c6b9bad4-aded-471b-92c2-2e98e7c0095b` | 139 / 140 |
| ResourceQuota | budget | `6e2a0536-62ed-4028-be90-3d4535d35789` | 159 / 160 |
| Namespace | weir-qual-m30r6-20260928-182504 | `7edde9bf-df25-481a-a58e-1381cf40c69a` | 162 / 165 |

Quota 删除前重新读取完整 namespaced catalog：62 类中 Secret 按零 quota 计数核验，其他 61 类以 13 批、每批最多五类完整盘点。外来资源 0；两个 default 对象 UID 未变；44 条 Event API 行均引用准确自有 Pod/Job，未当作 44 个独立事件或外来对象；Secret used=0。namespace DELETE 后非空/Terminating 响应没有冒充消失，最后 command165 exit0、stdout 0 bytes 才确认 absence。`cleanup.confirmed=true`，无自有远端残留。

165 条普通 CLI 的 argv、stdout、stderr、起止和 exit 全部保存且均 exit0；单独的 upload exec exit0 与 Weir observer exit1 分别保留。原同步 CLI finalizer 完成 Stop/Wait 与管道关闭；两个 observer 有双 EOF/Join/stopped 记录。两个外层 runner 均 Wait、输出句柄关闭、未外层超时；launcher/prepare/native/upload/Weir 五个记录 PID 在离线收口时均已不存在。没有新增远端确认调用。

## 封存与限制

证据根 `.testdata/m30r6/`，包括只运行现有 prepare/run 的一次性本地 launcher、source/tool/正式输入冻结、五个公开 artifact、完整 native 计划/CLI/exec/raw、失败/取消状态、独立离线 role/timing/cleanup/process 审核及逐文件 size/SHA256 `manifest.json`。manifest 排除自身和避免自引用的 `delivery.json`；最终交付 SHA 与 manifest hash 见该 receipt。ES 失败诊断日志达到原 65,536-byte 上限，保留为有界截断诊断，不称完整 ES 日志；bootstrap 确认用的完整日志另存。

M30R5R 的 107 份及此前 1,977 份历史封存文件在执行前后重算未变，原通过/失败记录不改写。仅离线核验本次原始证据，没有重复上一阶段 235/210 本地测试，也没有运行无关 Go/race/DB/镜像测试。所有执行脚本和产品输入与基线一致；本地 main 仅提交本报告与 readiness 更新。

CNI、跨两 worker、六原生平台、两规格/后端、容量/延迟/恢复/24h 等门槛没有豁免；Auth 排除与 ProgramTransform 首版延期不变。本轮没有形成完整无负载资源证据，更没有容量或生产资格。完成封存、提交并回调统筹后停止，不自行开始新阶段或定时器。
