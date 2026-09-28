# M26 — 同 Pod 回环 profile 在写入前停止

2026-09-28；执行聊天 `01a0e640-0c11-7fe1-ac6b-2ede0d796672`。
基线 `5432a218024db3332847313009eb8040118b765d`，main 干净、唯一写入者。
正式任务 SHA256：`057f64c50de4b2e235dcb681b909c4852f3d07b5f37b71af81cf101220b1dd42`。

**结果：blocked-before-freeze。沿用 M25 保守资源计账，没有节点达到 M26 的 7 CPU 余量要求；
未创建 namespace、Job、Pod 或后端，未冻结执行 plan，未开始原生 invocation。**
本次仅交付调查与报告，没有实现或声称验证完整 M26 运行入口。产品、helper、Python 入口、
协议、module、镜像与 CI 全部未修改。M25R3 已获统筹有限独立验收；原失败与其他生产目标保留。

## 只读资源预检及其口径

显式授权 context 的 AWS EKS 查询确认 data-team ACTIVE/control plane 1.36。
一次选择性节点与 Pod 资源快照共 34 个节点；只读取 node identity/conditions/allocatable/taints
和 Pod 资源字段，没有读取业务 env、日志或秘密。节点与资源查询非原子，不是资源预留。

直接复用未改动的 `scripts/eks_pacing.py:allocated`：合计普通容器、init、Pod 级、overhead、
allocated/runtime 等字段。此实现**有意重复计入多个观测来源**，不是 Kubernetes scheduler
的精确 requests，也不是实时 CPU 利用率。仅对 ephemeral-storage 用同样的来源合计补算。

| 余量 | M26 最低要求 | 保守 CPU 余量最大节点 |
| --- | ---: | ---: |
| CPU | 7 | 5.195 |
| memory | 5,632 MiB | 29,642,031,104 B |
| Pod slots | 3 | 52 |
| ephemeral-storage | 5 GiB | 18,182,813,665 B |

该节点 allocatable CPU=7.910，保守合计=2.715；Ready、无 pressure/taint/unschedulable。
精确 nodeName/UID 与逐节点拒绝理由保存在受控 `assessment.json`。其余节点也均未达到
此口径的 7 CPU 门槛；较空的其他节点还带有 autoscaler 删除候选 taint。

同一原始快照的 **spec-only 诊断值为 7.005 CPU**（含 init 的简单保守求和）；它排除了
allocated/runtime 重复项，未完整实现 resize/sidecar 的有效 requests 算法，未用于放行。
因此结论是“沿用的保守入口不满足”，不能写成“集群实际空闲 CPU 不足”或要求购买机器。
没有为了通过本轮而改变计账、降低 7 CPU 门槛、降低 6 CPU 容器总预算或更改现有节点。
后续若要收敛重复计账，需先单独审计资源算法及 resize/sidecar/overhead 反例，再由统筹安排；
本次没有凭这个诊断值执行部署。

## 启动依赖调查

源码与 M24 source `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139` 的全部 Go、module 和两个
Dockerfile 无差异。准确产品 `cmd/weir/main.go` 的 5 秒启动上下文调用 `app.Open`；
`internal/backend/search/adapter.go` 在开放任何监听前依次检查 ES 版本、auto-create=false
和已存在的具体索引。Weir 不创建索引，也没有等待后端的 CLI 模式。
qualification 的 `config`、`idle`、`snapshot`、`observe`、`pace`、`setup`、`trial`
均没有启动另一进程的模式；两个固定 distroless 制品的入口分别是 `/weir` 和 `/qualification`。

原 M22 先启动 ES、单独 setup1000，再启动 Weir；不能直接搬到 M26。
M26 每个 trial 已有自己的 setup1000，额外完整 setup 会增加 1,000 document mutations，
超过本轮 2,400 总预算。单独建立**空索引**属于可单列的管理操作，但仍需要先完成回环检查、
ES readiness，并在 Weir 首次启动前完成；普通容器同时部署本身没有提供这条已验证的启动链。
本轮未实现、未资格化符合准确制品及三普通容器边界的启动屏障，不假设额外 helper 模式存在。
这是一项待解决的测试布局前提，**不是证明所有其他接线方案都不可行，也不是产品故障结论**。

离线 source-level 反例用本机 Go1.27.1 CGO0 编译未修改的 Darwin arm64 产品，只连接
自有 127.0.0.1 合成 HTTP server：版本/auto-create 响应有效而索引返回404时，产品只发出
上述三个 GET，exit1、`local Store startup qualification failed`，没有开放监听、没有写入。
正向控制提供索引元数据后产品确实输出两个回环监听；夹具紧接日志发送 SIGTERM 得到 -15，
其“正常 exit0”断言失败。原夹具、输出、失败均保留，两个进程均 Wait、HTTP thread 均 Join；
没有据此声称优雅关闭通过，也没有重新运行该控制求绿。这些是本机合成证据，不是准确 OCI/EKS 实测。

[Kubernetes Pod networking](https://kubernetes.io/docs/concepts/workloads/pods/#pod-networking)
确认同 Pod 可共享 localhost；它不替代启动依赖或网络策略资格。
[PostStart 文档](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/)
明确 hook 与本容器 ENTRYPOINT 并行，因此不能把 Weir 自身 PostStart 当作其启动前屏障。

## 制品和资格边界

Weir/工具保持 [M24 的准确 index、arm64 manifest 和 binary 映射](milestone-24.md)。
官方 ES reference 固定为：
`docker.elastic.co/elasticsearch/elasticsearch@sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`。
首次无认证 curl 返回401，原命令保留；随后校验已有 regctl0.11.6 的官方 SHA256，使用全新空
Docker/registry 配置与清空的继承环境匿名读取公开 manifest/config，未登录或读取个人凭据。
原始字节 hash 匹配：manifest 为上述 digest，config 为
`sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160`；
config 确认 linux/arm64、8.19.22、用户1000:0。没有下载层、启动该镜像或宣称实际 imageID 验证。
官方[镜像配置文档](https://www.elastic.co/docs/deploy-manage/deploy/self-managed/install-elasticsearch-docker-configure)
要求注意默认 network.host=0.0.0.0 与可写数据/日志；本轮没有运行它，回环监听检查仍 not-run。

选择性 CNI 查询仍见 network policy agent `--enable-network-policy=false`；网络隔离 unqualified。
没有 Service、host 访问、ptrace/capability、CNI/全局变更、扩容、购买、push、CI 或发布。

| 项目 | 本轮状态 |
| --- | --- |
| single-Pod-loopback-limited-functional 实施/实证 | 未实施；预检阻塞，无真实 RPC/DB 结果 |
| plan / plan hash / native invocation | null / null / 0；没有可冻结的合格 nodeUID 与执行模板 |
| namespace / Job / Pod / runtime imageID / containerID | 均未创建或取得 |
| mutation reservations / document mutations / index 管理操作 | 0 / 0 / 0；没有中断后未知写入 |
| Weir→direct 两轮 rate50、20s warm+20s measure | 全部 not-run；6,000 planned 与2,400 mutation 仅为任务上限 |
| payload/id/version/APPLIED/UNKNOWN 审计、原始直方图 | not-run；没有伪造 receipt 或零错误通过结论 |
| 客户端与被测进程资源、连接/Guard/DB stats | not-run；没有完整采样资格 |
| 容量 candidate / 校准 / 过载 / 恢复 / 24h | null / not-run / not-run / not-run / not-run |

## 离线验证及收口

固定 Go1.27.1，GOENV/GOWORK 关闭、GOTOOLCHAIN=local、GOPROXY/GOSUMDB=off：

| 检查 | 结果 / 秒 |
| --- | --- |
| CGO0 `go test -count=1 -timeout=5m ./...` | PASS / 61.863 |
| CGO1 `go test -race -count=1 -timeout=5m ./...` | PASS / 65.496 |
| `go vet ./...` | PASS / 0.259 |
| 全脚本离线回归 | 79项 PASS / 13.857 |
| 优化模式 EKS 回归 | 23项 PASS / 9.425 |

最初默认/race/vet/本机 build 因继承 Go1.27.0 GOROOT 与1.27.1编译器冲突而失败；
显式固定仓库 SDK 的 GOROOT 后执行上述验证，保留初始失败。没有修改全局 Go 配置或产品代码。
启动控制的退出断言失败单独保留，不包含在上述通过项中。

证据根 `.testdata/m26/`（0700、Git忽略）：`preflight-20260928/` 保留命令0001–0008、
节点/资源原始投影、两种计账、CNI、准确 namespace 不存在查询、ES registry 原始字节与hash；
`validation-20260928/` 保留命令0001–0010、两轮SDK验证结果、本机反例脚本/config/输出与退出码。
`closeout.json`、`manifest.json` 保存最终状态、输入与所有证据文件的 hash/长度，供统筹独立复核。

本轮 Kubernetes 写命令为0，归属对象列表为空；候选 namespace
`weir-qual-m26-20260928-0424` 查询为空、从未创建，因此没有 UID 删除或空目录数据可回收。
全部本地子进程 Wait、合成 HTTP server 关闭并 Join；匿名读取子进程也已结束。
仅提交本报告与 readiness；main 本地干净、无push。回调统筹后停止，不新开阶段、namespace 或定时器。
Weir 自身认证继续排除；通用 ProgramTransform 首版延期/UNSUPPORTED；其他必需生产门槛仍 required。
