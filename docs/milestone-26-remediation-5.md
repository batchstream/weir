# M26R5 — 容器自身名称解析修复与有界功能前置

2026-09-28；执行聊天 `01a0e6f3-df11-7093-aaa3-2b0c03611163`。
基线 `91297e6f67ad24dceccc59b4f9300cdd34f65038`，实现 `f7e4973f3c6d6119c24e128cf2bebdecd339813b`。
交付 SHA 与完整证据清单记录于 `.testdata/m26r5/closeout.json`。
任务 SHA256 `9d3c9ad9ad24e3fd9a71c9e660587ea0e1e2ea8d8e6a729902582135503bc6fe`。

只新增有界本地 driver、定向测试和从 R4 实际 Docker 输出选择的非秘密 fixture。
新自有容器使用稳定 hostname 与同名 `127.0.0.1` hosts 映射；没有修改宿主 DNS/hosts、
Docker daemon、镜像、ES 正常 entrypoint/args、日志级别或正式 EKS guard。
Go 产品/helper/协议/模块/CI 与共享 Python/准入路径未改；没有 push、构建或发布。

复用 R4 SDK 原生 false=2 请求/true=0 请求的已接受证明，**本轮没有重跑 SDK**。
R4 的189份文件重算与 manifest `dc2402ef7adb8d2fc37044d71e0e375599d822331065710180a7f473c40fb8ae`
一致，200项产品输入及3120份M26系列历史证据未变；原SDK plan `7640d0ec46df684601e7cf2cef4354d4724e5b0e11bab9968b1dda1de99912c2`。
固定制品15份 registry/二进制文件重算通过，未重复下载/构建/Go资格。
共享代码未变，复用统筹已独立核验的全Python普通130通过与优化105通过/25skip；
本轮最终定向3项普通与优化均通过、0skip。实际执行派生shell的HTTP/settings/FIN_WAIT2拒绝，
覆盖缺失/错名/非回环映射和日志失败后精确ID清理，不以假CLI成功JSON代替原生证明。

两个本地目录 `.testdata/m26r5/es-attempt-{1,2}` 均0700，各plan0400，启动前冻结命令、
driver、挂载脚本与输入hash；保留两份driver、配置diff及第一份不可改写manifest。
ES manifest `c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`、
config `a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160` 保持准确。
Docker29.7.2/Linuxkit7.0.12/aarch64；network-none、无host端口、3CPU/3072MiB、heap1024MiB、
data tmpfs≤1GiB、1000:0/dropALL/NoNewPrivs1/seccomp2，日志4MiB×1。

| 本地尝试 | 原始结果 | plan SHA256 |
| --- | --- | --- |
| 1 | 映射与cgroup正确；getent诊断exit2，ES就绪/JDK/socket未到达；主动清理后ES143，无OOM/restart | `50b1f0b2227c64fe3c13b503528527f7e18f1b780b3d5b445b50b815f3cd29ba` |
| 2 | libc/JDK同名解析127.0.0.1；ES8.19.22/version/settings及3次间隔socket样本通过，13.553秒 | `1758af89677a18dff1712d0f64379e5452b28bce047de94789c1c80d1143f0a7` |

第一次的`getent ahostsv4`默认AI_ADDRCONFIG过滤不适用于仅loopback的network-none。
第二次仅给诊断加标准`--no-addrconfig`并记录实际glibc2.39版本，仍要求全部地址严格127.0.0.1，
实际JDK `InetAddress.getLocalHost/getAllByName`另行验证，ES保持正常启动。
依据[glibc getent源码](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/nss/getent.c)及
[loopback过滤实现](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/sysdeps/unix/sysv/linux/check_pf.c)；
原exit2/stdout/空stderr与退出状态保留，不声称第一次通过或为产品失败。
第二次启动期4次curl7亦完整保留，随后真实就绪；正常日志中的硬件地址与OpenTelemetry警告保留，未降日志。
派生检查只省略本地不存在的PodIP检查，地址/端口/UDP/FIN_WAIT2原规则逐字保留；
这三个短样本只是local-only前置，不能证明终生无外连、PodIP边界或CNI隔离。

两个container分别为`d040a12bd599bfafcfefafcc711216f638b647ecd4f79fa6ef9abb9718b8715b`与
`0ecf16aeb0e41d99cec48e27e454d53ea2e4b87adb1cffc07d1679eebd41e4f1`。
分别18/38条命令都有结束/exit；均按精确ID/owner Stop→Wait→停止确认→rm，ES受控exit143，
无OOM/restart，两轮前后容器/network ID集合一致、owner归零；第二次在第一次清理确认后才启动。
本地没有Weir/helper运行、管理写、seed或trial，没有真实IMDS探测。

EKS仅一次plan/invocation：`.testdata/m26r/native-r5-20260928-0749/`，plan SHA256
`d2bd131c834522637af032e25044551abb6856c8e2e1a31105b137c7e1f91ba1`。
namespace `weir-qual-m26r-r5-20260928-0749`，UID `b045b500-f1d7-4b85-97a5-75f172b44d06`；
Job UID `39cb191b-7cd4-4c4e-a020-ca57ea34c4c3`；Pod `loopback-wftg6`，
UID `66924d31-e9c3-4fc5-9d5c-975f1d55001c`。同一既有node
`ip-172-31-12-243.us-west-1.compute.internal`/`18f03c58-83c1-424e-be34-44ef88c07831`，
准备与create前核验，初始CPU余量7.030，其余原门槛均满足。
真实Job/Pod dry-run、准入、实际UID/controller、三准确镜像imageID/containerID、
ES sidecar→空records init→Weir/client顺序、bootstrap/main完整guard与自己PodIP负向检查均通过。
Weir实际source `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`/Go1.27.1/linuxarm64；
准确两GHCR digest与全部容器ID详见 `runtime-identity.json`，与冻结plan一致。
Store4/批16/768MiB、原Pod6CPU/4608MiB/2560MiB及所有安全字段未变。

两trial按through-Weir→direct-ES各一次50ops/s、warm20+measure20、1KiB/corpus1000、
90Read/10Put、64worker/4conn、deadline1s/expiry20ms/catchup8原profile完成。
各1200文档mutation预留全部实际开始，合计2400；6000planned含2000seed与4000load，
4000load全部success、零drop/error/UNKNOWN。每trial200计划Put全部APPLIED、200条version1，
完整关联/payload/ledger与DB审计通过；没有重放/预算释放或额外seed。管理操作完成7次（1+3+3）。

| trial | 最差dispatch p99 | 最差arrival p95 / p99 | 客户端样本 |
| --- | --- | --- | --- |
| through-Weir | 1.8ms | 9.8 / 18ms | 22 |
| direct-ES | 1.2ms | 7.0 / 7.1ms | 22 |

这里取warm、measure及两个10秒窗的all/read/put所有分组最差值，不删除warm。
原门槛5/100/250ms均通过；原始分桶/计数重新计算见 `.testdata/m26r5/eks-audit.json`。
客户端最大采样CPU区间分别0.260349/0.250629、无throttle；观察期无OOM/restart。
完整Weir/ES进程采样仍缺，resource-evidence=partial；这些短点不产生吞吐/性能比较结论。

**本阶段最终NO-GO：本地前置和两trial通过，资源清理未完成。** 两trial均passed=true，但资源inventory枚举到了
`metrics.k8s.io/v1beta1|PodMetrics|loopback-wftg6|<no value>`，共享foreign_check把本Pod的只读视图
当作外来可持久资源，在删除前拒绝；原result为passed=false/cleanup=false，200条CLI均有end/exit0，
原入口最终exit1。该失败不是功能/SLO失败，也不倒写成清理通过。
实际API discovery确认PodMetrics仅get/list，名字及owner与本轮Pod匹配；未读取业务Pod或秘密。
清理恢复单独冻结 `.testdata/m26r5/cleanup-recovery/plan.json`，没有修改原plan/result或共享源码，
没有重新运行负载；仅将这一个已证明的只读视图从持久对象归属检查中分开，未接管或删除该视图。
原清理180秒总窗口耗尽，首次精确Job UID删除命令被Stop/Wait为exit-15，结果不确定；
恢复结果confirmed=false/180.028秒保留。只读复查确认Job仍存在，未把该删除当作成功。
已请求仅追加清理时间的明确授权；截至收口未收到答复，未重置预算或继续删除。
原Job/Pod 900秒期限在08:07:26Z触发，Job在08:07:29Z记录Failed/DeadlineExceeded；
08:08Z再次按原UID核验Job failed=1/ready=0/terminating=0，原Pod查询为空。
所有本地fixture/验证子进程及本轮线上工作负载现已停止，但namespace尚存在，**不是cleanup=true**。
以下5个持久对象的原UID/owner已只读复核，待后续获得明确清理授权；没有force、finalizer修改或陌生对象删除。

| 剩余对象 | 精确UID |
| --- | --- |
| Namespace/weir-qual-m26r-r5-20260928-0749 | `b045b500-f1d7-4b85-97a5-75f172b44d06` |
| ResourceQuota/budget | `6bbc15fb-7300-4670-9358-63c646f4ffe1` |
| NetworkPolicy/default-deny | `798e9fb4-995b-4c5c-ad70-b63cc4b86674` |
| ConfigMap/configuration | `cc615037-15d0-4f14-91ee-964c1bfd4074` |
| Job/loopback | `39cb191b-7cd4-4c4e-a020-ca57ea34c4c3` |

完整剩余清单与停止证据位于 `.testdata/m26r5/closeout/`；原功能result、原清理失败及恢复失败均未改写。
本地main提交干净后向统筹一次具体失败回调并停止，清理缺口不以功能/SLO通过抵消。

candidate=null；完整容量、跨节点、过载、恢复、24h均not-run，CNI隔离unqualified。
startup-signal-registration-window仍未解决，不能因这次有序启动通过而称生命周期资格通过。
Weir认证排除、通用ProgramTransform首版延期与其他生产门槛保持；不自开阶段或恢复定时器。
