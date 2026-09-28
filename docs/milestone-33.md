# M33：准确新镜像接线完成，唯一 EKS prepare 因 CPU 余量不足停止

2026-09-29。基线 `fa871b29ec5156256d8892cea22c1a2d047521c2`；接线及唯一 prepare 源码 `1c73f03b781a81d8522924485f19f93e48b95825`；镜像源码仍为已独立接受的 `278264db2f9617ad583c6b56d19b8aaf5943e771`。最终报告交付 SHA 与封存清单见 `.testdata/m33/delivery.json`。

**本轮未取得 EKS 采样资格。唯一 prepare 在创建任何对象前被原资源门槛拒绝：固定节点 CPU 余量为 6.900，低于要求的 7 CPU。** 没有 plan/native invocation、namespace、Job、Pod、upload、release、空索引 PUT 或样本。没有重新 prepare、换节点、放宽资源条件、现场补丁或清理重试。原资源 partial/not-qualified、timing=not-run、candidate=null 保持。

## 接线与准确制品

仅更新 `scripts/eks_resource_preflight.py` 的 SOURCE、两包精确 image/manifest/config/binary 身份和 helper 应用层 hash/size，以及两个直接相关的内存合成模板期望。测试先确认旧镜像被拒绝，再仅投影 image 引用；历史 fixture/raw 未修改。运行类、消费者、完成握手、预算及其他 runtime 函数全部不变；`verified_helper` 仅更新冻结应用层常量，保留完整 index→manifest→config→gzip/diffID→唯一 binary 校验。

| 制品 | 准确身份 |
|---|---|
| Weir index | `sha256:aee0c24fd5a0edbe81d335522e2741b34ca267f8246c893e15d6ef0097c18bb4` |
| Weir arm64 manifest | `sha256:31a7d7a8d6db388448db51d19621a20ba93af18aff6073455f8c59121929b98c` |
| Weir binary | `48a81d3e3ea12dc51b0ba44d8ed879a2b62c46b947afee2e8fb27db6af81bac8` |
| qualification index | `sha256:364474990ae529650ac69a83f29e15cdbc82474c6eb5dca4ccb0d3cdf3874999` |
| qualification arm64 manifest | `sha256:87070a283529918acc3cd62a241135de82c90a3431ab48b2485c4e2e7f779c84` |
| qualification config | `sha256:409a8092911ed4ba7d2b1d7901b0df121fe3ebe54705f7d7ff42d44ea7a9db69` |
| helper 应用层 | `sha256:76da2b347b19b2c209008c671a7ff0f98ccb1aafd0aefe578bff1e1936f218e6`，10,818,785 bytes |
| helper diffID | `sha256:f9e7e1a7ac32aa511665a214ffa67f0f1ca5b08f350b17928d56e6db49138afd` |
| helper binary | `2629ed0d83623c4cb0c0895386e1abe1b87f78661a42c73562ab0154e4210e99`，21,051,489 bytes，0555 |

包名分别为 `ghcr.io/batchstream/weir` 和 `ghcr.io/batchstream/weir-qualification`。ES 保持 8.19.22/JDK27、digest `sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`；本轮未启动这些制品。

从统筹已验公开 `acceptance-m32r2/registry/qualification-full.tar` 只读取得五个文件，写入 `.testdata/m33/artifact/`。源 archive 为 23,757,312 bytes、SHA256 `ca7af8c5c21d4db635d065229e3164e34edba63b4b951071d33a530a87e2408d`；只读取准确 regular blob 和唯一应用成员，没有按 archive 路径解包或运行 binary。`artifact-proof.json` 保存本轮重算的五文件身份及完整链。没有重新下载、构建、读取匿名登录状态或借用旧 helper。

206 Go/module 文件、产品70/helper82正式输入逐项与镜像源码一致；`source-input-proof.json`、`change-scope.json` 保存证明。复用已验且 hash 相同的 M32R2 focused Go test binary，仅用于本地真实 pipe；没有重复全仓 Go、DB 或本地容器。

## 本地验证

每组显式外层180秒，命令、最终受测输入 hash、stdout/stderr、退出和 Wait 均保留。只运行直接相关测试，没有完整 Python 重跑。

| 检查 | 结果 | 外层耗时 |
|---|---|---|
| 资源预检、模板、准入、资源读取、期限与清理合同普通 | 84通过，0skip | 82.472秒 |
| 同组优化模式 | 84通过，0skip | 77.626秒 |
| observer 完成与生命周期普通 | 23通过，0skip | 110.112秒 |
| 同组优化模式 | 20通过，3项既有 calibration 明确拒绝优化入口的 skip | 81.466秒 |

真实 Go→EKS/local/calibration 普通短 pipe 分别6/71/1350合成样本，优化支持的EKS/local分别6/71；完整校验→EOF→exit0/双EOF/Wait/Join均通过。无ACK、截断、错误身份、非法控制、非零、超窗、取消、记录失败以及原5秒反例/12秒正例保留。合成采样时间不代表真实10/140/2698秒运行。142条 OWNER 原始回执保留；最终152个记录到的测试及 launcher/runner PID 新查询均不存在。

## 唯一 EKS prepare 的真实失败

context `arn:aws:eks:us-west-1:956540890581:cluster/data-team`，region `us-west-1`；候选 namespace 名 `weir-qual-m33-20260928-233135`。固定 node `ip-172-31-12-243.us-west-1.compute.internal`，UID `18f03c58-83c1-424e-be34-44ef88c07831`。

32条普通CLI全部exit0并Wait：当前context、cluster、候选namespace不存在、23项权限、选定CNI、Git身份，以及同一node和其Pod资源两次读取。`native/command-0031.out` 确认原生linux/arm64、Ready、无压力/污点/删除、内核 `6.12.77-99.140.amzn2023.aarch64`、kubelet `v1.35.3-eks-bbe087e`；控制面1.36/ACTIVE。Pod查询仅限这个node上的全namespace资源元数据，没有全局扫描或Secret读取。

| 资源 | allocatable | 保守已分配 | 剩余 | 最低要求 |
|---|---:|---:|---:|---:|
| CPU | 7.910 | 1.010 | **6.900** | **7** |
| memory bytes | 31,982,452,736 | 1,031,798,784 | 30,950,653,952 | 5,905,580,032 |
| Pod | 58 | 7 | 51 | 3 |
| ephemeral bytes | 18,182,813,665 | 0 | 18,182,813,665 | 5,368,709,120 |

原 `node_gate` 抛出 `ValueError: insufficient conservative spare capacity`；离线重算原始响应得到相同分配量和唯一CPU失败条件。不是API超时，不满足读取恢复条件，recovery=null。没有把资源快照当预留，也没有追查或修改其他工作负载。

prepare 于UTC `2026-09-28T23:31:35.173121Z`开始，`23:32:39.246283Z`结束，64.072秒，exit1、未触及外层905秒，输出句柄已关闭。静态阶段59.374秒后才启动唯一动态窗口：monotonic start `413022.687462458`，deadline `413142.687462458`。command31/32分别在 `413022.687987083` / `413024.693086916`开始，余额119.999/117.994秒，分别2.000/2.648秒完成；两次读取共占4.654秒。没有续期或第二次prepare。

主900/角色60/共享采样180/独立清理300秒未启动；upload300/artifact-ready420、EOF4/Stop4、namespace45 reserve和exec request10均未改变，也未用于现场执行。源冻结 SHA256 `a9f899400250aedc1a8e1cc292e76ad68a9bacbf1a586882ec34582f450d5769`；prepare未成功生成plan，故plan hash=null，不能虚构冻结计划或native invocation。

## 清理、封存与资格边界

Namespace/Quota/Policy/ConfigMap/Job/Pod **六类对象均未创建，UID均未分配**，没有自有远端资源或需要删除的对象。command3的exit0空stdout确认候选namespace在准备阶段不存在；其后只有上述只读检查。没有将“没有创建”写成“六UID清理通过”，没有额外GET、DELETE或另开清理窗口。upload/release/空索引PUT/seed/trial/planned/document mutation/三角色采样全部0。

`.testdata/m33/native/` 原样保留32组CLI argv/时序/stdout/stderr/exit、静态与资源窗口、原始node/Pod投影、原计账及CNI结果；外层 `prepare.err` 保留完整首错traceback，`prepare-audit.json` 是分开的离线复算，未替代或改写原raw。`.testdata/m33/manifest.json` 逐文件封存hash/size，排除自身与避免自引用的delivery。前4244份历史证据逐项重验不变。

CNI仍明确 `--enable-network-policy=false`，network isolation=unqualified。本轮连同Pod loopback和完成握手都未在EKS实测，不能称新镜像在线采样已通过，也不倒推历史reset具体丢失层。资源partial/not-qualified、timing not-run、candidate null，以及CNI/跨两worker/六原生平台/两规格/各后端/安全/容量/延迟/恢复/24h门槛保持；Weir认证排除、ProgramTransform首版延期不变。

仅本地main接线与报告两次提交，无push、Actions、镜像发布、集群/节点/CNI修改、业务数据或付费资源。全部自有进程收口、main干净后向统筹一次失败回调；后续由统筹决定，执行者停止，不自动重试或开启timer。
