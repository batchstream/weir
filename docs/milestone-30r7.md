# M30R7：固定 exec 输出尾部三臂诊断

2026-09-29。基线 `46226266a0ba6bb0218b2734e3c41de27d872020`；实现及唯一原生执行源码 `41f72ad575c4c97b9a1140ac7ab4e956be92f96d`。本地 main 提交，无 push/Actions、新镜像或 Go/module/正式 qualification helper 改动。

**本次固定条件下，完整接收后 EOF 确认保住了输出尾部；这只是有界缓解证据，不是本 EKS 丢字节层的确诊，也不代表正式 helper 已修复。** 一次 prepare、一个新 namespace、三个且仅三个串行原生 exec，均未启动 Weir、Java、ES 服务或任何数据库。无第四臂、线上补丁、参数/协议切换或重复求绿。原 M30R6 reset 根因 unknown、resource partial/not-qualified、timing not-run、candidate null 继续保留。

## 冻结内容与实现边界

新增 `scripts/eks_exec_tail.py`、`scripts/exec_tail_producer.sh` 和必要测试。复用既有 Observer 的双管道排空、Stop/Wait/关闭，及 eks_pacing 的资源窗、严格准入、UID 所有权、全 catalog 盘点和条件删除。共享 Observer 仅增加可选单流上限；原默认 64 MiB、普通 cleaner 180 秒、正式资源预检 60/300 秒及 helper 立即退出行为未改。

stdout 固定为 identity 64 bytes、6 条各 49,152 bytes JSON（序号 0–5，间隔 2 秒）、terminal 65 bytes，共 **295,041 bytes**。只输出固定字段和 X 填充，不采集环境、proc、秘密、业务或网络数据。stdin 只检查管道类型和 EOF/非法输入；不是读取文件内容的采样器。

完整 SHA256：`f419af8d5381ce027e53bafdc0264919a8a16f3261c3ebd036f935e83843e03b`。terminal 精确 ASCII 字节为 `{"kind":"terminal","records":6,"diagnostic":"m30r7-fixed-bytes"}` 加一个 LF。接收端同时校验长度、hash、所有记录及顺序、terminal 和逐字节相等，不能仅凭计数或 exit0 接受。stdout ≤1 MiB，stderr ≤64 KiB。

A 写完即退出；B/C 使用完全相同的 `eof` 命令，在同一 Bash 内 `read -t 4`。B 只有完整校验后才关闭 stdin，C 始终不发确认。提前输入/EOF 返回 72，非法数据/读取错误不能冒充 EOF 成功，确认超时返回 74。固定 `/usr/bin/timeout --signal=TERM --kill-after=1s 19s` 提供合计 20 秒保险；生产者显式跟踪、kill/Wait 自有 sleep 子进程。没有固定尾部 sleep 或无限等待。

固定 ES image `docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237` 仅作为 shell 文件系统。PID1 是有界 sleep，exec 运行 `/bin/bash --noprofile --norc`。一个 Job/Pod、一个容器，requests=limits 1 CPU / 256 MiB / 128 MiB ephemeral，Pod/Job 900 秒 deadline、grace10、Never/backoff0，UID/GID1000、drop ALL、RuntimeDefault、只读 root，仅只读不可变 ConfigMap。无 init/sidecar/RW 数据卷/Service/PVC/host namespace/hostPath/debug，token automount 和 service links 关闭，DNS None/127.0.0.1。

## 本地验证

保留开发期首败：Bash 3.2 大字符串替换触及本地截止；初轮测试还发现 Bash 3.2 的 EOF/超时语义差异，以及测试 runner 工作目录错误。未把它们删除或改写为成功。填充改为固定次数倍增，固定生产者要求 Bash ≥4；host Python 实际管道只验证控制器，准确 shell 行为另用镜像验证。

受影响普通测试 144 项通过（194.042 秒）；最后增加实际 runtime UID/GID 守卫后，最终入口 12 项普通复验通过（34.935 秒），最终受影响优化 Python 144 项通过（192.231 秒）。最终入口/优化测试输入逐项匹配执行源码。每 runner ≤360 秒、真实 Wait、输出句柄关闭。覆盖完整/截断/hash 错/乱序、提前 EOF/输入、无 ACK/非零、取消、输出/记录错误收尾、准入/UID/image/资源/resize 漂移、错误 profile/多臂/阶段预算及 foreign cleanup 拒绝；校验不依赖 assert。

唯一已缓存准确镜像本地容器：network none、≤1 CPU/256 MiB/64 pids、非 root、drop ALL、readonly，仅挂载自有只读脚本，无 pull/build/DB。主窗口 55.441 秒，准确清理 5.200 秒。A/B 都收到完整 295,041 bytes，分别 exit0；B 有 `ack-eof`。C 完整输出、exit74/`ack-timeout`，客户端耗时14.227秒；提前 EOF/数据各 exit72；相同 timeout 保险探针19.070秒 exit124。容器按准确 ID/label 回收并确认不存在。它是 shell/管道语义验证，不能替代 EKS 对照。

## 唯一 EKS 结果

context/region 固定为 `arn:aws:eks:us-west-1:956540890581:cluster/data-team` / us-west-1。node `ip-172-31-12-243.us-west-1.compute.internal`、UID `18f03c58-83c1-424e-be34-44ef88c07831` 未变。静态阶段后才启动唯一120秒资源窗；prepare/写前四次资源 GET 共9.154秒，均有25+4余额、请求flag10秒，恢复0次。原7 CPU/5632 MiB/5 GiB/3 Pod门槛保持，初始 CPU余量7.030。主900秒包含本次静态准备，实际266.066秒；native部分200.227秒。

namespace `weir-qual-m30r7-20260928-191719`。三臂共用 Pod `diagnostic-wjpxf`，UID `f341f771-b0fc-4005-96f0-3905433ad237`，containerID `containerd://3f92e7304245a7b2d614479aded41ddf5a9aad348e5389668cedd04fb412b9ae`。实际 imageID 与固定 digest 相同；运行时 UID/GID1000、supplementalGroups [0,1000] 原文保留。每臂前后健康/准入/不可变脚本/身份复核均通过；三个 exec 存活区间内控制面 CLI 重叠为0。

| 臂 | stdout完整性 | exit / stderr | exec / 含前后身份整体秒 |
| --- | --- | --- | --- |
| A 立即退出 | 249,915 bytes；identity+0–4完整，最后4,091-byte尾片；无terminal，长度/hash失败 | 1 / connection reset by peer | 13.455 / 29.907 |
| B 完整验证后 EOF | 295,041 bytes；全部长度/hash/顺序/terminal/字节相等通过 | 0 / `ack-eof` | 13.756 / 30.102 |
| C 不确认 | 与B相同完整 bytes/hash；始终未发ACK | 74 / `ack-timeout`，明确负例，不计成功资格 | 17.047 / 33.931 |

A raw SHA256 为 `90c164dd8379c5ace056f8e95f4bd3503159d56c7b58bd12374a4ffa32f83112`。B/C hash 均等于冻结完整值。三臂双 EOF、Wait/Join、三句柄关闭均完成；A 首败及 C 预期非零保留，未被 stage exit0 覆盖。stage exit0 只表示按计划完成并清理。

准确 kubectl v1.35.2 EKS fork、SHA256 `d6487d72d341c1db4d7fe6840e96cd3c99d75e8fe6dcef205ceb1c987ebf84f5` 未变。所有普通 GET/exec 保持 `--request-timeout=10s`。无协议环境切换、高详细认证日志、生产流量抓取或升级。新鲜 CNI agent `--enable-network-policy=false`，default-deny 对象不冒称有效隔离。artifact upload/release、DB/业务 HTTP 管理请求、文档/seed/trial/容量均0。

## 清理、封存和局限

同一起点300秒清理实际108.778秒，namespace预留45秒不变。准确六UID各一次 Orphan 条件 DELETE，逐个 exit0 空 GET 确认：

| 对象 | UID | DELETE / 空 GET command |
| --- | --- | --- |
| Job diagnostic | `a5c9ccc2-6d58-4535-9e45-c95abfd7eb02` | 100 / 101 |
| Pod diagnostic-wjpxf | `f341f771-b0fc-4005-96f0-3905433ad237` | 103 / 108 |
| ConfigMap configuration | `c9ce87f3-c178-426b-a31b-be8cddaa6342` | 110 / 111 |
| NetworkPolicy default-deny | `8dad1d96-f534-49cd-b62d-6f93330139a5` | 113 / 114 |
| ResourceQuota budget | `572366b8-8db8-4b7c-b3ad-43b9a27e2f5f` | 133 / 134 |
| Namespace | `d48873f7-f8c0-4329-9b89-60967a44a23b` | 136 / 139 |

两次 fresh catalog 各62类：Secret由零quota/used计数核验，其他61类各13批、每批≤5完整覆盖。foreign/default UID/Event归属守卫均通过；最终10条Event API行均引用自有UID，不将双API行数当独立事件数。137/138仍Terminating，只有139 exit0空stdout才确认namespace不存在。零残留，无force/finalizer、外来删除、额外cleanup-only或预算外GET。

`.testdata/m30r7/` 保存本地冻结计划/完整对象/脚本/payload、原始失败、host管道/准确镜像结果、所有139条普通CLI（全部exit0）、三条独立exec原始stdout/stderr/身份/时序/EOF/Wait结果、两份离线审核及逐文件size/SHA256 `manifest.json`。80条测试OWNER记录和其他runner/exec记录合计98个不同PID新查询均不存在；prepare/native外层runner亦Wait且关闭输出。最终manifest排除自身和避免自引用的delivery receipt，交付SHA及manifest hash见 `delivery.json`。

M30R6的623份和此前2084份封存文件执行前后共2707份hash/size核验未变；204 Go/module及Weir70/helper81正式输入继续匹配image source `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。合成shell字节不作为真实资源样本。

[上游 issue #142376](https://github.com/kubernetes/kubernetes/issues/142376)仍仅是假设线索。本次每臂只有一次，串行条件仍有时间差；没有捕获各层写入/丢失点，不能归因于CRI、kubelet、apiserver或kubectl中的某一层。C的完整输出说明有界等待也让本次尾部到达，但其非零防止把等待超时当成功；不是握手唯一性或普遍可靠性的证明。

下一阶段由统筹决定是否给正式 observer/helper 增加最小、有界、完整输出后 EOF 确认。本轮不实施该修复、不更新镜像或继续实验。CNI、跨两worker、六原生平台、两规格/后端、容量/延迟/恢复/24h门槛全部保持；Auth排除、ProgramTransform首版延期不变。完成回调后停止，无timer或续派。
