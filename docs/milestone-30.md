# M30 — 新准确镜像 EKS 无负载采样预检

2026-09-28。**本轮原生预检 NO-GO：Job dry-run 的默认字段表示未匹配，未进入运行或采样。**
唯一 namespace 已完整清理。控制脚本已在清理后离线修正并回放真实响应，未再次访问 EKS 执行实验。
`resource_evidence=partial/not-qualified`、`timing=not-run`、`candidate=null`。

## 源码与准确制品

- baseline：`a039c49910273c5a3ddb5b4c9a68e3440c5197f7`。
- 实际原生尝试的实现：`fe78242427321d0ca9a3333a4526e7963c7ee019`。
- 清理后的离线修复：`567a984a0fbe6ca6ea768c543dab58de98143a35`，没有第二次原生 invocation。
- 最终交付为随后仅更新本文/readiness 的本地 commit，准确 SHA 见 `.testdata/m30/delivery.json` 和完成回调。
- image source 仍是 M29 的 `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`，不是以上脚本提交。
- 204 个 Go/module 文件、产品 70 项和工具 81 项正式构建输入与 M29 source 均相同，见 `product-input-proof.json`。
  本轮未改 Go、产品/helper 算法、正式镜像输入或节点限制；未 build/push/Actions/发布。

| 制品 | 准确身份 |
| --- | --- |
| Weir index | `sha256:2a3ca21b950f42449b01543e96655e0484e432f8ecbf29b46c49f7fd426c229a` |
| Weir arm64 manifest / binary | `sha256:100102a8c319a24f571f2aaa3041d6fafc79338840829e792f404e48d117dd5e` / `f2d107995762040c55b03cb37e94dd9e6beaa14a99d563b278b4bd9c2bd4db7e` |
| Qualification index | `sha256:fbef16495a37b89d44cfce51c38096e01e982ab875e3352b67c59480fdbe61a7` |
| Qualification arm64 manifest / config | `sha256:9eb2ff4243d7c021abe047da28867a9b3df65695660675ebcb761387b4f50351` / `sha256:063aa9659604ca3b7ab3814e31341727fe2ccce6f6758456d3f97c4b911209ee` |
| Qualification binary | `d41f70ca4bbe129bff11b76f3d973cdb288d839c4df72a348cac04a233b76482` |
| ES8.19.22 manifest / config | `sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237` / `sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160` |

复用 checksum 固定的 regctl v0.11.6，以新空 HOME/DOCKER_CONFIG/REGCTL_CONFIG 匿名读取 index、arm64
manifest、config、唯一应用层，每对象一次成功，无补充下载。总 437.868 秒，小于20分钟。
应用层 `sha256:e7020f259dcaf846bb2772aca7dd4f2bc353ef7292b95549e4f541509eea03c7` 为 10,810,877 bytes；
gzip 解压后的 diffID 为 `sha256:6decd3193e4ce7cc311e886ca1f801031a16fced6b24da3cbcecc71fc853a86d`。
只读取唯一 regular `qualification` member，拒绝额外文件、链接、路径穿越、异常权限/pax；0555 binary 为
21,046,963 bytes，准确 hash 与 M29 一致。未使用历史 partial tar；这是单 arm64 应用层验证，
不称为双平台完整匿名导出。该完整导出证据仍来自 M29 已独立验收的 CI。

## 已实现与离线验证

新增一个 `eks_resource_preflight.py` 入口，复用既有 EKS 对象/资源计账、准入、归属、UID 条件清理及
Observer 有界管道。镜像由明确冻结计划传入，旧 M25/M26 常量及默认行为保留。未建立另一套 controller。

拓扑保留 ES 原生 sidecar → bootstrap init → Weir/client。角色预算仍为 2CPU/1GiB、3CPU/3GiB、
1CPU/512MiB，有效 Pod 峰值 6CPU/4608MiB/2560MiB ephemeral。新增64MiB磁盘 emptyDir 在原预算内计账：
bootstrap 是唯一 RW 挂载者，运行期 ES/Weir 只读。非 root、dropALL、RuntimeDefault、固定回环、
metadata-disable、无 Service/host namespace/hostPath/ptrace 等边界保持。ES 原 M26 可写根配置保留。

固定 stdin 流写临时文件，限制大小、核对准确 hash、0555/原子就位，先取得完整 stdout 回执并再次核对
Namespace/Job/Pod UID、bootstrap imageID/containerID/Running/restart，才通过 FIFO 释放 init 屏障。
init 再次核验后才允许唯一空 `records` 索引 PUT。传输300秒、artifact ready420秒和整体900秒边界保留。
运行入口计划 Weir/ES 同容器同 UID 各10秒、2秒间隔、6样本及正常 observer_end，加一次 client snapshot；
共享原始进程语义，独立记录实际 FD/PID 限额，不调用本地140秒完整报告，不改 raw 或 Go 校验求通过。

原生前普通 Python 全套167通过/0skip（123.002秒）；优化142通过/25既有skip（116.875秒）。
最终证据记录补充后，相关10项普通回归通过；优化全套使用最终原生实现。
覆盖安全归档、stdin 截断/错误 hash、退出/取消/截止、UID 变化不释放、资源真实文本、六样本结束与清理。
首次 shell 反例有3个失败：本机 Bash3.2 的 standalone `[[ ]]` 未因 errexit 停止，后改为显式 `|| exit 41`；
原日志保留，未进入 EKS 前已验证普通/优化通过。

## 当前环境、唯一原生尝试与失败

固定 context/cluster 为 `arn:aws:eks:us-west-1:956540890581:cluster/data-team`，region us-west-1。
当前 context 与 cluster ACTIVE/v1.36 已读回。第一次只读全 Pod 资源投影发生10秒响应体超时；
在同一原900秒只读准备窗口内，仅补充一次准备，未改代码/超时/门槛。原失败 stdout/stderr 保留。

选定已有节点 `ip-172-31-12-243.us-west-1.compute.internal`，UID
`18f03c58-83c1-424e-be34-44ef88c07831`。节点报告 linux/arm64、kernel
`6.12.77-99.140.amzn2023.aarch64`、kubelet `v1.35.3-eks-bbe087e`，Ready且无压力/taint。
初始余量7.030CPU、31,202,312,192 bytes内存、18,182,813,665 bytes ephemeral、52Pod，
满足原7CPU/5632MiB/5GiB/3Pod门槛；Job 固定 nodeName，不依靠 scheduler 或扩容。
CNI nodeagent 实际参数仍为 `--enable-network-policy=false`，不宣称 namespace/default-deny 是有效网络隔离。

冻结计划 SHA256 `141400ce01b17fb431ab5d4e9cb0498a2ed9bb9b4b3a5fe53d3acd524e805231`。
唯一 native invocation 创建 namespace `weir-qual-m30-20260928-1316` 及 Quota/Policy/ConfigMap，
82.035秒后因 `container admission drift: volumeMounts` 停止。Job server dry-run exit0；其真实 JSON
省略 bootstrap helper mount 的默认 `readOnly:false`，冻结请求却显式包含 false，严格比较拒绝。
**没有持久 Job/Pod、helper 上传、exec、采样、索引 PUT、seed、trial、planned workload 或文档 mutation。**
原 result 的 PUT completed=null 保留；独立命令审计由零持久 Job/Pod、零 exec 证实本次实际PUT为0，未改原 raw。

清理结束后，仅把请求中的默认 false 省略，ES/Weir 的显式 true 保留；未放宽准入比较或资源条件。
完整原样响应已作为 `scripts/fixtures/eks-resource-admitted-job-m30.json` 回放：修复后的模板通过，
原模板仍复现失败，移除运行容器 readonly 仍拒绝。修复后普通11/优化11全部通过，无skip。
**这个修复只有离线证据，没有在 M30 再跑 EKS，不能把最终修复源码冒称原生已测源码。**

## 清理与证据

| 对象 | 登记及删除的准确 UID |
| --- | --- |
| Namespace | `840a6aef-1fac-42cb-9c83-58fef719ba30` |
| ResourceQuota/budget | `f7af0654-39e4-4a04-b54a-42f9354b6cfe` |
| NetworkPolicy/default-deny | `26b439f0-1341-4ef0-880a-3a6746dfc5f2` |
| ConfigMap/configuration | `6871b74e-9afc-4359-9985-17d6c125af3f` |

四次 UID 条件删除，无重放、force 或 finalizer 修改。Quota 保留到最终完整清单，Namespace 最后删除。
正常清理119.928秒；另在原清理 allowance 内、第161.129秒独立读回 namespace 不存在。
91条准备/执行/收口 CLI 全部有 end/exit，唯一 CLI 非零是原只读投影超时；主入口因为准入失败 exit1。
无远端 helper 或 DB 进程曾启动，全部自有本地工具/测试进程已结束，无集群残留。

证据根 `/Users/liran/Projects/liran/go/weir/.testdata/m30/`：准确 registry原文/层/binary、下载边界、
原始失败、冻结计划/输入、API响应、每条命令、四UID清理、独立不存在核验、离线修复和执行审计均保留。
`manifest.json` 逐文件 hash/size 封存，排除自身、最终 `delivery.json` 及不读取的自有匿名客户端状态目录；
最终 delivery 记录 manifest hash/文件数/提交SHA，避免自引用。
审计脚本首轮曾把 `auth can-i create` 误分类为 create 而 KeyError，已保留该审计失败说明，修正分类后重算；
不改变原命令或原生失败结论。

## 差异、未知与下一阶段边界

| 项目 | M28R2 本地 Docker | 本轮 EKS 证据 |
| --- | --- | --- |
| 内核 | `7.0.12-linuxkit/aarch64` | 节点 status 为上述 Amazon6.12；无容器内核实测 |
| FD soft/hard | 固定4096/4096 | **unknown**；未采样，旧1048575/1048576不当作当前值 |
| pids.max | Weir/client256，ES512 | **unknown**；旧37697不当作当前值 |
| CPU/memory/swap/cpuset/io | 本地冻结配额、no-swap及实际raw | 只有计划/Job dry-run预算；实际cgroup与进程均unknown |
| ES 文件系统 | readonly root及本地tmpfs | 保留M26可写根、1GiB data emptyDir及64MiB helper卷计划，未运行 |
| 只读 helper/同UID/JVM | 本地已取得完整证据 | 上传/exec/三角色身份、JVM定位、CLK_TCK、metrics/stats全部not-run |
| 网络 | 自有共享loopback、network-none | 当前CNI agent disabled；本轮PodIP/回环检查未执行，隔离unqualified |

统筹下一阶段须先审阅本次准入修复，并另行冻结新的无负载采样窗口。当前没有真实 EKS FD/PID、
swap/cpuset、同容器进程或 observer 开销样本，不能据此冻结短负载合同或直接运行两个 trial。
随后才由统筹根据真实 raw 决定 EKS 限额和平台合同；执行者不自动扩大到负载。
CNI/跨节点、六平台、其他规格/后端安全、容量、过载/恢复与24h继续required；M28R2计时NO-GO不豁免。
Weir auth排除和 ProgramTransform V1延期保持。不自开下一阶段、聊天或定时任务，完成回调后停止。
