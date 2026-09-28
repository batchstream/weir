# M30R — EKS 无负载采样补证在只读准备阶段停止

2026-09-28。**NO-GO：全集群 Pod 资源投影读取响应体超时，无法确认节点余量；未进入原生采样链路。**
没有运行计划或 native invocation，没有创建 namespace/Job/Pod，没有上传、exec、采样或数据库操作。
按本轮真实失败后停止的约束，没有重试、延长请求截止或修改代码后重跑。
`resource_evidence=partial/not-qualified`、`timing=not-run`、`candidate=null` 保持。

## 准确源码与制品

- baseline：`a2046ffdc3060a6803f0904fa1328958fa36aaa2`，干净 main。
- 本轮实现及实际只读准备源码：`df2c1517d6b0293a08ae030216619d12070cbe8f`。
- 最终交付为随后仅更新本文/readiness 的本地提交，完整 SHA 见 `.testdata/m30r/delivery.json` 和完成回调。
- 实际公开 image source：`4abc8761f9f0e08af978d5ae5c14176f8188cfa3`，不是本轮脚本提交。
- 204 个 Go/module 文件、Weir 70 项及 qualification 81 项正式构建输入重新逐项核对，均未变。
  未 build、push、Actions、发布或修改 Go/模块/镜像输入。

复用 M30 的五个明确公开文件 `index.json`、`manifest.json`、`config.json`、`application.tar.gz`、
`qualification`，复制至全新 `.testdata/m30r/artifact/` 后重新校验。未读取或复制旧匿名客户端状态，
未下载 registry。校验 index→arm64 manifest→config/source/platform→应用层的身份和长度、gzip diffID、
唯一 regular 0555 `qualification` member 及本地二进制内容/权限。

| 制品 | 固定身份 |
| --- | --- |
| Weir index | `sha256:2a3ca21b950f42449b01543e96655e0484e432f8ecbf29b46c49f7fd426c229a` |
| Weir arm64 manifest / binary | `sha256:100102a8c319a24f571f2aaa3041d6fafc79338840829e792f404e48d117dd5e` / `f2d107995762040c55b03cb37e94dd9e6beaa14a99d563b278b4bd9c2bd4db7e` |
| Qualification index | `sha256:fbef16495a37b89d44cfce51c38096e01e982ab875e3352b67c59480fdbe61a7` |
| Qualification arm64 manifest / config | `sha256:9eb2ff4243d7c021abe047da28867a9b3df65695660675ebcb761387b4f50351` / `sha256:063aa9659604ca3b7ab3814e31341727fe2ccce6f6758456d3f97c4b911209ee` |
| Qualification layer / gzip diffID | `sha256:e7020f259dcaf846bb2772aca7dd4f2bc353ef7292b95549e4f541509eea03c7` / `sha256:6decd3193e4ce7cc311e886ca1f801031a16fced6b24da3cbcecc71fc853a86d` |
| Qualification binary | `d41f70ca4bbe129bff11b76f3d973cdb288d839c4df72a348cac04a233b76482` |
| ES 8.19.22/JDK27 | `docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237` |

应用层 10,810,877 bytes，helper 21,046,963 bytes。这是单 arm64 应用层的重新验证；
完整双平台匿名导出仍依赖已验 M29 CI，不冒称本轮重新完成。

## 最小实现与离线验证

复用 `eks_resource_preflight.py` 及共享准入、资源计账、Observer 和 UID 清理实现。
入口新增 `--evidence m30r`，只允许明确的 `m30`/`m30r` 两个证据目录及各自严格 owner 前缀；
计划冻结绝对证据路径。原 M30 plan/invocation 不复用、不覆写。
另在原整体 900 秒窗口内保留失败时准确自有 Pod 的有限容器日志，清理仍使用独立 180 秒上限。
没有新 controller、每轮脚本、宽松 spec 比较或字段白名单补丁。

M30 已验的默认 `readOnly:false` 省略保持，ES/Weir 的显式只读挂载不变。
原样完整 M30 Job 响应继续回放；新增 Pod 回放组合该真实 Job spec 与已记录 M25 Pod admission 默认字段，
metadata 和 runtime 状态为明确的离线构造，分别覆盖 bootstrap Running 和 Completed。
这不是本轮真实 Pod 准入证据。沿用传入、完整回执、释放、截断、错误 hash、UID 变化、
取消/截止、精确 UID 清理、六样本报告与缺失 end 的正反例。

- 普通全套：169 项通过，121.559 秒。
- 优化全套：144 项通过、25 项既有 skip，118.099 秒；优化模式不靠 `assert` 执行控制流校验。
- 最终日志错误对象的具名变量整理后，普通/优化各 12 项预检定向回归通过，无 skip。
- 首次新增离线回放失败：测试把 Job UID 改为 synthetic，却未同步真实 selector；修正夹具绑定后通过。
  原 `offline-focused.err` 保留。未因该错误修改生产准入比较器，也未进入 EKS。

## 实际准备、失败与收口

74 项脚本/夹具/配置输入、实现 SHA、公开 image source 和新 owner 在进入 EKS 前冻结于
`source-freeze.json`。准备命令为 `WEIR_EKS_M30=1 python3 scripts/eks_resource_preflight.py prepare
--evidence m30r --owner weir-qual-m30r-20260928-135240`。保留原 stdout/stderr、截止和退出记录。

固定 context `arn:aws:eks:us-west-1:956540890581:cluster/data-team` 与当前 context 一致；
AWS 只读返回 data-team、us-west-1、ACTIVE、Kubernetes 1.36。
节点最小投影返回 15 个 Linux arm64 节点。历史候选当前仍为
`ip-172-31-12-243.us-west-1.compute.internal`，UID `18f03c58-83c1-424e-be34-44ef88c07831`，
内核 `6.12.77-99.140.amzn2023.aarch64`、kubelet `v1.35.3-eks-bbe087e`，Ready、无压力/taint。
这只确认节点身份；allocatable 不是扣除现有请求后的可用余量，不能据此选择节点或继续创建。

第 5 条准备 CLI 对全集群 Pod 做既有最小资源投影时 exit 1：

```text
context deadline exceeded (Client.Timeout or context cancellation while reading body)
ValueError: command 5 exit 1; see retained stderr
```

该 CLI 实耗 13.381 秒（请求截止仍 10 秒），整个准备进程 21.010 秒。
2 bytes 的 stdout 和完整 stderr 原样保留；失败响应不参与资源计账。
未完成节点余量判断、RBAC/CNI 复查、计划冻结；`plan_sha256=null`，不存在 `native/plan.json` 或 `invocation.json`。
原 7CPU/5632MiB/5GiB ephemeral/3Pod 余量门槛没有降低。
900 秒原生窗口、300 秒传入和 420 秒 artifact-ready 阶段均未开始。

所有操作只有只读准备和收口：5 条准备 CLI + 1 条最终 namespace GET，全部有 end/exit；
5 条成功、上述 1 条失败。集群 create/delete、Job/Pod dry-run、exec、helper 上传均 0。
远端 Weir/ES/client 进程和样本均 0；HTTP 管理 PUT 0、观察 GET 0、trial/pace/setup/seed/planned/document mutation 全部 0。
没有本轮自有 UID，因而没有删除操作或待清理对象。
最终新 GET 对 `weir-qual-m30r-20260928-135240` exit 0 且空输出，确认 namespace 不存在。
所有本轮本地测试/CLI 均已 Wait 结束，没有远端 exec 需要终止；未重试原失败或调用恢复清理。

## 差异、未知和证据

| 角色 | 保留的计划预算 | 本轮真实运行资源 |
| --- | --- | --- |
| Weir | 2CPU / 1GiB，readonly root、helper RO | FD/PID/cgroup/CPU/memory/swap/cpuset/io、进程身份和 metrics 均 unknown / not-run |
| ES | 3CPU / 3GiB、1GiB heap，既有可写 root、helper RO | JVM/UID/FD/PID/cgroup、stats、实际内核和 CLK_TCK 均 unknown / not-run |
| client | 1CPU / 512MiB，readonly root | 自身 snapshot、UID/FD/PID/cgroup 全部 unknown / not-run |

单 Pod 有效峰值 6CPU/4608MiB/2560MiB ephemeral、64MiB helper emptyDir、bootstrap 唯一 RW 挂载及
ES sidecar→bootstrap→Weir/client 的拓扑保持为实现约束，没有本轮运行证据。
不将 Docker 的 FD4096/PID256/512 或旧 EKS 的 1048575/1048576/37697 当作当前实测。
CNI 本轮尚未复查，历史 disabled 不是新事实；namespace/default-deny 隔离继续 unqualified。

证据根 `/Users/liran/Projects/liran/go/weir/.testdata/m30r/`，含五个公开制品、重验/输入证明、测试原始日志、
74 项 source freeze、6 条 CLI 原文、失败及不存在收口、`result.json` 和 `operation-audit.json`。
`manifest.json` 逐文件 hash/size 封存，排除自身与避免自引用的 `delivery.json`；新根没有秘密/匿名客户端状态。
旧 M30 342 文件逐项重验，manifest SHA256 保持
`8fcc58766c27be59c6e318ae24bb7d77bb2e49b21f61ceed52231f78380b48da`，原生失败历史未改写。

本轮没有取得足以冻结 EKS 短负载合同的新资源 raw。后续处置由统筹决定，执行者不重跑、不开启负载或下一聊天。
完整资源/计时/容量、CNI/跨节点、六原生平台、其他规格/后端安全、过载/恢复及 24h 门槛均保留。
Weir Auth 排除、ProgramTransform 首版延期保持；不创建定时任务。
