# M32：双架构 CI 接线与发布阻断

2026-09-29。基线 `314eccc41119aac355fe46cce7d3330675465253`；接线实现、冻结构建候选 source、实际 CI 受测 SHA 均为 `b420c8ea4957db177a801c1efe3b0321b14b893d`。最终文档交付与远端 main SHA 记录于 `.testdata/m32/delivery.json` 和完成回调；文档提交不另触发构建。

**M32 未完成镜像交付。唯一 CI run 的两架构普通 Python 回归均失败，发布及 smoke 被依赖门禁跳过；没有新镜像 source/digest。** 原失败完整保留，没有改断言、放宽时限、删除测试或第二次 run。当前公开制品仍来自 M29 的 `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`，不能声称包含 M31 修复。

## 本轮实际改动和本地验证

只修改 `.github/workflows/images.yml`、`scripts/ci_images_test.py`、`scripts/observer_completion_test.py`：

- 两原生 Linux runner 在当前准确源码下编译带 integration tag 的 focused Go test binary，将同一绝对路径导出为 `WEIR_COMPLETION_TEST_BINARY`，供普通及优化 Python 全发现使用。
- CI 中缺少绝对、存在且可执行的二进制时直接失败，避免外部管道回归静默 skip；保留本地可选入口和 calibration 的既有优化限制。
- 每个成功的实际 Go→Python 管道正例输出小型 `COMPLETION_PIPE` 回执，记录 source、平台、consumer、样本数、binary/stdout hash、验证→EOF→Wait 时序。没有上传二进制、全套 raw 或新增 artifact 平台。

206 个 Go/module 文件与 M31 基线逐字节不变；正式 Weir 70 项与 qualification 82 项输入不变。固定 Go1.27.1、Actions/工具/module/base pins、最小权限、手动触发、并发组与原 job 时限均保持。打包 allowlist、两轮复现、CGO0/trimpath/buildvcs=false/空 buildid/sourceRevision、同 SHA 禁止不同内容覆盖、匿名完整内容及原有短 smoke 代码均未修改。

本地只做受影响离线检查：普通 16 项通过；优化 14 项通过、2 项 calibration 旧限制 skip。实际普通 EKS/local/calibration 三消费者分别接收 6/71/1350 合成样本，优化 EKS/local 两消费者通过；本地记录不能代替 Linux CI。13 个 child 已 Wait/Join/关闭三管道，独立 PID 查询不存在。未启动本地容器、DB、负载或构建发布镜像。

首次 fresh fetch 确认准确 origin `https://github.com/batchstream/weir.git`，远端 main 为 `a039c49910273c5a3ddb5b4c9a68e3440c5197f7`，是本地基线祖先。审核累计 25 个历史提交的全部路径、91 个变更 blob 及本轮接线路径；没有发现秘密风格跟踪路径或凭据特征。`.testdata/`/`.tools/` 仍忽略且不在正式构建 context。完成普通 fast-forward push 后 API 确认远端与冻结 SHA 一致，再触发一次手动 run；运行期间未推送其他 main。

## 准确原生 CI 结果

唯一 [Actions run 36482306611](https://github.com/batchstream/weir/actions/runs/36482306611)，attempt 1，headSHA `b420c8ea4957db177a801c1efe3b0321b14b893d`。2026-09-28 20:52:58Z 创建，21:02:04Z 完成，结论 failure；未达到 90 分钟共同窗口，无需取消，没有第二次 run。

| Job | ID | 结论 |
| --- | --- | --- |
| test (ubuntu-24.04) | 109130683235 | failure；默认/race 成功，普通 Python 失败 |
| test (ubuntu-24.04-arm) | 109130683637 | failure；默认/race 成功，普通 Python 失败 |
| publish | 109134127331 | skipped；未构建或发布 |
| smoke-arm64 | 109134128717 | skipped；未执行 smoke |

两架构均真实通过默认 CGO0、独立 CGO1 race、普通/integration vet、helper integration race 以及 focused test binary 编译。每架构日志合计 35 条 Go package PASS（默认 17、race 17、helper 聚焦 1），无 cached；这是 package 数，不是单测数。显式 native/child/full-resource opt-in 未启用。执行到普通 Python 证明前面的 fail-fast shell 命令已成功，但整步仍是失败。

| 原生平台 | runner image / 内核 | 普通 Python | 优化 Python |
| --- | --- | --- | --- |
| Linux amd64/x86_64 | 20260920.314.1 / 6.17.0-1022-azure | 255 项：252 通过、3 error、0 skip；297.429s | 普通失败后未运行 |
| Linux arm64/aarch64 | 20260920.129.1 / 6.17.0-1022-azure | 255 项：252 通过、3 error、0 skip；270.698s | 普通失败后未运行 |

各 runner 为 4 logical CPU；原始 `CI_ENVIRONMENT` 保留实际内存、架构、Go/tool pins、run/source 身份。不能将已配置的优化命令写成已执行，也不能将本地优化结果填入 Linux 结果。

实际新增外部 Go 管道已执行，不是因缺 env 跳过：

| 架构 | 普通消费者 | 合成样本 | stdout bytes | 结果 |
| --- | --- | --- | --- | --- |
| amd64 | EKS | 6 | 34502 | exit0；validate ≤ EOF ≤ Wait |
| amd64 | local | 71 | 398559 | exit0；validate ≤ EOF ≤ Wait |
| arm64 | EKS | 6 | 34502 | exit0；validate ≤ EOF ≤ Wait |
| arm64 | local | 71 | 398559 | exit0；validate ≤ EOF ≤ Wait |
| 两架构 | calibration | 1350 计划 | 未封存完整成功流 | consumer 收尾失败，不计通过 |

这些只是短 pipe/合成时间戳回归，不是真实 10/140/2698 秒资源采样。负向取消、错误身份 terminal、多 owner 清理等其余用例按真实日志保留。

## 三个错误与停止理由

两架构发生相同错误，完整 traceback 在 `failure-excerpts.txt` 与原始日志：

1. `eks_exec_tail_test.AdmissionTests.test_frozen_plan_refuses_profile_arms_budget_and_payload_drift` 在正向 `tail.plan_check` 报 `ValueError: kubectl drift`。既有测试直接比较 runner 的 kubectl 可执行文件 hash 和历史固定 `KUBECTL_SHA`，没有隔离该环境依赖；失败发生在后续负例循环之前。该检查只是计算程序文件 hash，不是本轮运行 kubectl 或访问集群。
2. `CompletionConsumers.test_external_final_go_completion_to_actual_consumers` 的 calibration 子例报 `RuntimeError: observer exec did not stop/drain after stdin EOF`。
3. `CompletionConsumers.test_three_consumers_complete_and_reject_malformed_streams` 的 calibration/complete 合成进程正例报同一错误。

两个 calibration 错误均落在已有 `finish_observation → Observer.stop`，最后 child exit0、Wait/Join/管道关闭，但 consumer 的收尾判定失败，不能因此宣称完成成功。源码存在一个需要后续受控验证的窗口冲突：测试 `consumer` 给整个 local/calibration 处理 5 秒，而共享 `Observer.stop` 保留最后 4 秒清理，正常排空截止最多为该测试起点后 1 秒；1350 样本解析/校验占用同一窗口。这与 Linux 大流正例失败一致，但本轮没有额外探针确认唯一原因，也没有改生产关闭预算、提高测试时限或重试。

二进制已成功编译并进入真实消费者，因此不属于缺路径/调用错二进制的简单接线失败。既有环境依赖测试及 calibration 收尾错误需要统筹另行安排有界修复；依据 M32 的失败边界，本轮停止在首轮原始结果，不把它解释成可重复求绿的 CI 接线问题。未变更产品/helper Go、共享 Observer 或其他测试实现。

## 镜像、清理和原始证据

本轮 image build/reproducibility receipt、publish、匿名完整 export、六个准确 native smoke 全部 **not-run**；新 image source/index/platform/config/layer/binary digest 均为 null，而不是复用 M29 值冒充新产物。publish job 从未执行，因此没有本轮 builder、smoke container 或 registry login 待回收；不存在本轮 logout 成功记录。

每架构有 60 条 completion OWNER 记录，全部 reaped/Join/三管道关闭，包括 calibration 失败的 child。其余 20 条已有生命周期 OWNER 单独保留；其中故意注入 Wait 失败的负例保持 joined=false，同时记录 reaped_before_test_cleanup/fallback_waited=true，不能粗略写成所有 owner 标志都为 true。两 runner 最终 job cleanup 已完成；本地 watcher 已 Wait/exit1，无自有测试进程、容器或后台任务残留。没有全局 prune，也未修改本机 Docker/default bridge；既有 bridge ID 差异仍归因 unknown。

`.testdata/m32/` 保留准确 run/job API、原始 logs zip、每成员 hash、首次独立 arm64 job log、输入和源码快照、本地命令/结果、CI 小回执与失败 traceback、清理/历史完整性、最终交付记录。原始 zip 为 103,913 bytes，SHA256 `eec7fb2a007c4d60dbf5c5f7a1b429dbfca4cf18b7736ffe7c31cc8c30cc3c75`；单独 arm64 log 与 zip 对应完整 job log 逐字节相同。

`test-summary.json` 分开记录实际成功子集、255 项普通失败结果和优化 not-run。证据解析器首版误把所有 OWNER 当成 completion schema 的 KeyError 原样保留，修正为按实际字段/测试分类；不影响或改写 CI 原始失败。预先准备但未执行的成功验收/匿名下载脚本明确标记 `-not-run.py`，没有对应成功产物。`manifest.json` 逐文件记录 size/SHA256；其 hash、最终文档/local/remote SHA、报告 hash 在 `delivery.json`。历史 3581 份封存前后 size/SHA256 全部不变。

## 保留的资格缺口

现有 `scripts/eks_resource_preflight.py` SOURCE/IMAGES/helper layer 检查以及统筹 approvedImages/state 原样保留，仍指向 M29：

- Weir：`ghcr.io/batchstream/weir@sha256:2a3ca21b950f42449b01543e96655e0484e432f8ecbf29b46c49f7fd426c229a`。
- qualification：`ghcr.io/batchstream/weir-qualification@sha256:fbef16495a37b89d44cfce51c38096e01e982ab875e3352b67c59480fdbe61a7`。

本轮未新建 Git tag/Release，未操作其他包/权限/付费资源，未运行 EKS/aws、DB、负载或 timer。M31 新 helper 尚未交付准确公开镜像，亦未获 EKS/完整 proc+DB HTTP 验证。resource=partial/not-qualified、timing=not-run、candidate=null，原 reset/丢字节具体层 unknown；CNI、跨两 worker、六原生平台、两规格/各后端、安全阻断、容量/延迟/恢复及 24h 门槛全部保持。Auth 排除、ProgramTransform 首版延期不变。完成一次授权失败回调后停止，不自行续派或尝试 EKS。
