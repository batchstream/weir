# M26R — 资源模型修正，唯一回环尝试在 Job dry-run 比较后停止

2026-09-28；执行聊天 `01a0e65c-d2dd-7571-9d3b-89aecdf99e97`。
基线 `1e245bb619a41abdb0a12911bea81cc88f7f9dc5`；实现提交
`2852dd36c15b05fc12535ae8622be0b54c20682c`。交付提交由证据 `closeout.json` 记录。
正式任务 SHA256：`2da21231a90291ecf50c408d6d05372012bc8e5fe852cfbb533ad37c793669d3`。

**NO-GO：新的具名资源模型通过离线回归与真实选择性预检，原 7 CPU 门槛得到满足；
但唯一冻结 invocation 因入口把 API 规范化后的资源 quantity 当成漂移而停止。
API 接受了 Job server dry-run，入口未接受其响应，尚未进行 Pod dry-run 或实际 Job create。
实际 Job、Pod、ES 管理写、document mutations 均为 0；四个自有对象已按精确 UID 回收。**
这次失败属于测试入口缺陷，无产品故障、资源不足或原生回环资格结论。
冻结后没有修改实现、plan 或重试。Go 产品/helper、module、镜像、CI 均未更改，没有 push。

## 资源模型与创建前证据

`scripts/eks_resources.py` 由 M25 与 M26R 共用，替代旧的多视图求和。
参照固定 [Kubernetes v1.35.3 helper](https://raw.githubusercontent.com/kubernetes/kubernetes/v1.35.3/staging/src/k8s.io/component-helpers/resource/helpers.go)
与 [v1.36.0 helper](https://raw.githubusercontent.com/kubernetes/kubernetes/v1.36.0/staging/src/k8s.io/component-helpers/resource/helpers.go)：
按容器 name 对齐 spec/allocated/actuated，逐资源取最大值；普通容器稳态和与有序 init
阶段峰值取最大值，restartable init 计入后续阶段和稳态；Pod-level CPU/memory 逐键覆盖，
overhead 只计一次。普通 init 完成后仍保留请求模型中的峰值，不把 limits 再加成 requests。
Infeasible 也保留 spec 的 max，是明确的保守上界，不声称精确 scheduler 输出。

选择性 kubectl 投影只返回身份和资源字段；没有导出业务 Pod 的完整 spec/env/日志。
新结构保留具名状态、init 顺序、resize 条件及未知资源特征。缺少 resize 所需状态、重复或
不匹配名字、未知 quantity/资源、溢出、DRA/ephemeral container 等使对应节点拒绝放行。
无 resize 时缺状态只使用已知 spec 并记录 status_present；未绑定 Pod 单列排除，非删除中
终态 Pod 排除，删除中的终态 Pod 保守计入。旧 flatten 快照不能作为新算法输入。

真实快照共 23 个节点，仅一个通过；首次预检与创建前复核都得到下列余量。
这是两个非原子 API 快照中的请求余量，既不是资源预留，也不是实时利用率。

| 项目 | 未改变的最低要求 | 创建前结果 |
| --- | ---: | ---: |
| CPU | 7 | 7.030（allocatable 7.910，已计请求 0.880） |
| memory | 5,632 MiB | 31,202,312,192 B |
| Pod slots | 3 | 52 |
| ephemeral-storage | 5 GiB | 18,182,813,665 B |

nodeName `ip-172-31-12-243.us-west-1.compute.internal`，
nodeUID `18f03c58-83c1-424e-be34-44ef88c07831`；linux/arm64，
kernel `6.12.77-99.140.amzn2023.aarch64`，kubelet `v1.35.3-eks-bbe087e`，control plane 1.36。
Ready、无 pressure/taint/unschedulable/deleting。逐 Pod 来源与计算保存在
`resource-accounting-4.json`、`resource-accounting-36.json`，即时复核为 `node-check-36.json`。
没有降低门槛、调整业务负载、修改节点或等待扩容来取得该结果。

## 已实现但尚未原生验证的启动布局

冻结布局采用 [Kubernetes 原生 sidecar 顺序](https://v1-36.docs.kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)：
ES 为首个 init（仅它设置 restartPolicy Always），只读回环 startupProbe 成功后进入一次性
普通 bootstrap init；后者先检查版本、配置、监听和自己 PodIP 负向边界，再只创建一次空
records 索引，完成后才启动直接执行 `/weir` 的产品容器与现有 idle client。
bootstrap 复用准确 ES 镜像，没有额外 setup1000、launcher、产品重试或新镜像。

| 阶段/容器 | CPU | memory | ephemeral-storage |
| --- | ---: | ---: | ---: |
| ES 原生 sidecar（heap 1,024 MiB） | 3 | 3,072 MiB | 2,048 MiB |
| 普通 bootstrap init | 1 | 512 MiB | 256 MiB |
| Weir main | 2 | 1,024 MiB | 256 MiB |
| qualification main | 1 | 512 MiB | 256 MiB |
| init 最大阶段（ES + bootstrap） | 4 | 3,584 MiB | 2,304 MiB |
| 稳态及 Pod 请求峰值 | 6 | 4,608 MiB | 2,560 MiB |

requests=limits，Quota 和总预算不变。ES 保持官方非 root 1000:0，仅自有可写层和受限
data emptyDir；bootstrap复用其安全上下文，根文件系统仍可写。Weir/client根文件系统只读，
全部 drop ALL、禁提权、RuntimeDefault，无 SA token。
Job 单 Pod/backoffLimit0，Pod Never；入口将任一容器实际重启视为失败。
共享 CLI、归属与 spec 检查仍拒绝未知容器/字段；M25 无 init 的约束保留。

ES manifest/config 原始字节已匿名重新校验；准确 image entrypoint 已提取检查，config history
包含 bash/curl/netcat。运行入口还会在管理写前验证实际工具可用性，但本次没有容器运行，
因此不能声称镜像内 bootstrap、readiness、回环边界、Weir 启动或关闭已经通过。
ES sidecar 预期在普通容器停止后由 kubelet 终止，常驻 main 不能仅靠 Job Complete 收口；
本次未走到这些路径，只有 namespace 资源清理实证。

## 唯一冻结尝试与具体缺陷

证据根 `.testdata/m26r/` 为 0700、Git 忽略；原生目录 `native-20260928-0520/`。
实现提交后 main 干净再冻结 `plan.json`（0400），SHA256：
`9f5a7afefd8eaa556daef010b588c4da70cae9a4e0182d227d2d8e77f1801f5f`。
plan 保存全部 tool/product 输入 hash、registry proof、准确镜像、模板、命令、nodeUID、
预算和门槛；收口审计逐项复核未变更。

唯一 context 为 `arn:aws:eks:us-west-1:956540890581:cluster/data-team`。
命令0039/0041/0044/0049分别创建 Namespace/Quota/Policy/ConfigMap；0050是唯一 Job
server dry-run，kubectl exit0。随后共享 `admitted_spec` 对容器嵌套字典作字面比较，报
`container admission drift: resources`，未进入实际 Job create。

保存的请求与0050响应显示四处等价序列化差异：

| 字段 | 请求 | API 返回 |
| --- | --- | --- |
| Weir requests/limits memory | `1024Mi` | `1Gi` |
| ES requests/limits memory | `3072Mi` | `3Gi` |
| Weir readinessProbe initialDelaySeconds | `0` | 省略 |
| ES startupProbe initialDelaySeconds | `0` | 省略 |

首处 quantity 差异触发停止；探针差异由保留响应的只读审计发现，未靠修改后再次运行验证。
离线夹具没有覆盖这些 API 规范化，故离线通过不能证明入口可用。
后续修复应覆盖真实响应形状，以规范化 quantity/标准零值默认字段做语义比较，同时保留
身份、安全、未知字段与容器列表的严格检查。本轮不执行该修复，也不消耗第二次 invocation。

## 准确制品、预算与资格状态

Weir/helper 的准确源码仍为 `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`，
全部 Go/module/两个 Dockerfile 与该 source 无差异。

| 制品 | 冻结 reference digest | linux/arm64 manifest |
| --- | --- | --- |
| `ghcr.io/batchstream/weir` | `cc6428d1ead507e531f95b8c45926f8bf31abf8ba9cb89cf6e8eca4a865b1f10` | `9dc3cb7fb9e69a332d10a1f49941bd80e9752778078335ac6dff7c22a6799856` |
| `ghcr.io/batchstream/weir-qualification` | `8a5c4dbca08daae24f62798c40f1607bbfaa5362b5b86db9a5651eb5475b3057` | `012713da8f0f0ccec1d24fe589c22b19789e177a1fc82702dd3701c190e55b05` |
| `docker.elastic.co/elasticsearch/elasticsearch` | `c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237` | 同 reference，8.19.22 |

前两个 binary SHA256 分别为 `e152805a350d5b2968df18f736e587b649f9ef2cf3cc6c650aa44bd37cbf3b41`、
`fd5a5d4f1f8127a0ca30d3155d3f9170f6ab0bc70d82f9f28b01fb287e7e27fc`，沿用 M24 映射。
ES config SHA256 为 `a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160`。
本次实际 PodUID、imageID、containerID 和运行时版本均未取得。

冻结预算仍是 remote900s+cleanup180s、startup150s；单流8MiB，入口合计8MiB
（严于授权16MiB），证据总128MiB。保留 result 的 remote_elapsed_seconds=106.959。
两 trial 顺序 through-Weir→direct-ES，均 rate50/warm20/measure20、64 workers、4连接，
1KiB/corpus1000、90%Read/10%Put、deadline1s、expiry20ms/catchup8；总 planned6000、
document mutations≤2400，每 trial 预留1200。dispatch p99≤5ms、arrival p95≤100ms/
p99≤250ms、零drop/error/UNKNOWN/restarts均未放宽。

| 项目 | 实际状态 |
| --- | --- |
| native invocation / Job dry-run / Pod dry-run | 1 / API exit0但入口比较失败 / not-run |
| 实际 Job / Pod / trial / document reservation / planned | 0 / 0 / 0 / 0 / 0 |
| document mutations / ES管理写 | 0 / 0，无不确定执行中的请求 |
| RPC、payload/id/version/APPLIED/UNKNOWN、DB审计、直方图 | not-run |
| 客户端与 Weir/ES资源、Guard/连接账本、stats | not-run |
| capacity candidate / 校准 / 过载 / 恢复 / 24h | null / not-run / not-run / not-run / not-run |
| 网络隔离 | unqualified；CNI agent仍为 `--enable-network-policy=false` |

`bootstrap-management.json` 在 Job 创建前已保守预留一次空索引管理操作（上界1）；
原记录不改写。全部命令证明未实际创建 Job、没有任何 exec，故收口将**实际管理写核对为0**。
原 `result.json` 的 `resource_evidence` 是入口初始化的 generic partial 字符串；因无 Pod，
本轮真实 runtime resource evidence 应为 **not-run**，不能据此推断已有部分运行时采样。

## 验证、回收与历史保留

固定仓库 Go1.27.1、显式 GOROOT、GOENV/GOWORK关闭、GOTOOLCHAIN=local、
GOPROXY/GOSUMDB=off；全部在实现提交和冻结前完成。

| 检查 | 结果 / 墙钟秒 |
| --- | --- |
| CGO0 `go test -count=1 -timeout=5m ./...` | PASS / 62.013 |
| CGO1 `go test -race -count=1 -timeout=5m ./...` | PASS / 65.635 |
| `go vet ./...` | PASS / 0.340 |
| 最终全脚本普通模式 | 95项 PASS / 19.005 |
| 最终全脚本 Python `-O` | 发现95项，70项 PASS、25项既有 skip / 15.495 |

回归覆盖三视图去重、resize、init相位/sidecar、Pod-level/overhead、本轮峰值、异常资源拒绝，
并通过外部伪 CLI 走投影与门禁；还覆盖启动模板、安全漂移、bootstrap边界和单次空索引写。
开发中旧伪资源响应与抽取 identity 时的 NameError 失败日志保留，修正后执行最终回归。
这些合成检查不包含真实 API quantity/default 规范化，是此次漏测的具体边界。

| 自有对象 | 精确 UID | 删除命令 |
| --- | --- | --- |
| Namespace `weir-qual-m26r-20260928-0520` | `5a95a7cc-92d8-43dd-bcd9-0abadca7b37e` | 0065 |
| ResourceQuota `budget` | `e1e8bb8f-4903-4d02-a8dc-f248da8879c6` | 0056 |
| NetworkPolicy `default-deny` | `466c9364-801c-4103-9f5e-4ccd5e17b5b0` | 0059 |
| ConfigMap `configuration` | `294cc84f-3e3c-40ef-a355-78b87309f4dc` | 0062 |

四次删除均带 UID preconditions、exit0；默认 SA/root CA 只在本轮 namespace 中核验身份，
没有采用未知资源。0068与另存 `closeout/command-0001` 都精确查询该 namespace，exit0且输出空，
cleanup=true。没有 force/finalizer、Service/PVC/Secret、全局/业务配置变更或后台负载。
所有受控子进程已 Stop/Wait；68条原生命令均保存 start/end/exit/stdout/stderr。

`registry/` 保存匿名准确镜像与固定 Kubernetes 源码依据；`validation/` 保存离线命令、
初始失败和最终结果；`closeout/admission-differences.json` 保存未修改请求/响应的比较；
`closeout.json` 和 `manifest.json` 记录提交、输入、最终状态和全部证据文件 hash/长度。
独立重算原 M26 manifest 的94个文件全部匹配，原历史未改写。

[原 M26](milestone-26.md)的未执行结论与本机正向控制 exit -15 失败继续保留。
监听后注册 signal 的窗口仍是后续生命周期核验项，本次未修产品、未重跑该控制，不能称
优雅关闭已通过。Weir自身认证仍排除，通用 ProgramTransform 首版延期/UNSUPPORTED，
其余必需生产门槛继续 required。本地 main 提交干净后仅回调统筹一次并停止，由统筹独立验收。
