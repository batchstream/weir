# M29 — 准确源码与公开双架构镜像交付

2026-09-28。**本轮准确镜像交付及两种 Linux 原生离线回归、短 smoke 通过；不构成 EKS、容量或整体生产资格。**
执行聊天 `01a0e7da-59b1-7a60-ac1c-51f7819c085a`，待统筹独立复核。

## 源码与交付身份

- baseline：`79fcb2e3aea6831b77a888dc9335a069eb97fef8`，干净 main。
- implementation / image source / Actions 实际受测 SHA：`4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。
- 最终文档 delivery 为随后只提交本文和 readiness 的 commit；完整 SHA 在本轮封存
  `.testdata/m29/delivery.json`、`final-report.md` 和唯一完成回调中记录，避免提交哈希自引用。
  image source 不会随着文档提交移动，也不为了文档重新构建另一套镜像。
- 证据根：`/Users/liran/Projects/liran/go/weir/.testdata/m29/`；不提交、不进入镜像 context。
- 204 个 Go/module 文件与 baseline 完全一致；固定 Go1.27.1、工具/Actions pins、module、
  base image、allowlist、产品和 helper 算法均未改。sourceRevision 改变会改变 binary 身份，
  M28R2 复用的旧 `1ffc092…` binary hash 不能当作本次公开镜像 hash。

推送前重新 fetch，origin 为 `https://github.com/batchstream/weir.git`，远端
`a04322233ce3c53238cc2ddec514f7b68e43ecd4` 为本地祖先，无并发/分叉；正常 fast-forward push。
审阅全部将公开的历史路径及相关 fixture/config 来源：36 个既有待推送提交、346 个路径、
430 个不同 blob；未发现秘密风格路径或凭据模式命中。冻结后另保存当前346文件逐项hash。
自动模式检查不称为穷尽秘密检测；不打开秘密文件，不包含忽略的工具、证据或环境配置。

## 最小接线改动

唯一手动 workflow、固定 repo/main/source、并发组、标准托管 runner、权限和超时不变。
旧 Python 选择列表改为统一默认离线发现，实际包含 `test_capacity_test.py` 及 resource、EKS/local 报告回归：

```sh
python3 -m unittest discover -v -s scripts -p '*_test.py'
python3 -O -m unittest discover -v -s scripts -p '*_test.py'
```

两处离线模板测试原硬编码本机 `.tools/go1.27.1`，现复用已有测试惯例：该固定目录存在时使用它，
否则使用 PATH 上 CI 已准备且校验过的固定 SDK；不安装其他版本、不开启真实 EKS/DB。
没有为 CI 结果减少断言或新增 skip。

资格工具复用既有 `-mode snapshot`，每台原生 runner 各运行一次。保持禁网、只读根、
UID65532、drop ALL、no-new-privileges、私有 cgroup namespace、1CPU/256MiB/no-swap/128PID、
20秒外层退出边界，日志有限1MiB。没有 hostPath/hostPID/ptrace、shell 或 DB。
检查实际 config ID、隔离和资源设置、自身 process/observer 身份、准确 exe hash、16项原始字段、
错误状态和 runtime 数据；原始输出在断言前进入日志，失败不会补零或变为正式资源通过。

## 唯一 Actions run 与测试

[36418991126](https://github.com/batchstream/weir/actions/runs/36418991126)，
workflow_dispatch，API head SHA 与冻结 source 一致。2026-09-28 11:59:42–12:14:50 UTC，
约15分08秒；仅一次完整 run，四 job 均 success，无第二次触发。等待期间未移动 main。

| 检查 | Linux amd64 | Linux arm64 |
| --- | --- | --- |
| 默认 CGO0 非缓存全 test | PASS | PASS |
| 独立 CGO1 race 非缓存全 test | PASS | PASS |
| vet / integration vet | PASS / PASS（后者静态） | PASS / PASS（后者静态） |
| integration helper 离线边界 race | PASS | PASS |
| Python 普通 | 157通过 / 0skip，90.872秒 | 157通过 / 0skip，93.113秒 |
| Python `-O` | 132通过 / 25既有skip，85.784秒 | 132通过 / 25既有skip，87.490秒 |
| 准确产品 `-version` | source/Go/target/clean 匹配 | source/Go/target/clean 匹配 |
| 准确工具1秒 pace | 50planned / 50completed / 0drop | 50planned / 50completed / 0drop |
| 准确工具单次 snapshot | PASS，59.539ms | PASS，39.509ms |

每台日志35条 Go package PASS、0 cached 标记；不把 package 行数当作单测个数。
helper 中3个专用 Linux native/child opt-in 用例未启用，不将它们计为实际运行通过。
独立 snapshot smoke 只证明新采样代码的镜像接线；没有调用140秒 resource_report 或授予完整资源资格。

两台测试 runner 实际内核均 `6.17.0-1022-azure`，CPU为 x86_64 / aarch64，4逻辑CPU；
CI_ENVIRONMENT 保存实际 runner image、架构、内存与工具 pins。镜像 runner 的 Docker engine
OS/arch/kernel/CPU、准确 platform/config 和实际容器隔离另存日志，未使用模拟架构运行。
两个 snapshot 均 PID1/UID65532、target=observer、cgroup `0::/`、errors为空、cpuset `0-3`，
cpu.max=`100000 100000`、memory.max=268435456、swap.max=0、pids.max=128。
它们只是单次样本，不说明瞬时峰值、隐藏祖先、发生器5ms、容量或生产资源资格。

## 可复现构建和准确公开内容

沿用已审计 allowlist 和两次干净源码导出/空 GOCACHE。公开模块准备后，
GOENV/GOWORK关闭、GOTOOLCHAIN=local、GOPROXY/GOSUMDB关闭、只读 module 并 verify。
CGO0、trimpath、buildvcs=false、空buildid、sourceRevision及OCI检查保留。
产品六目标binary/archive双次一致；资格工具两Linux目标双次一致，两包OCI双次一致。
macOS/Windows输出仅为编译/归档证据，不冒称原生运行。

产品70项输入，工具81项输入；input-map SHA256为 `sha256(json.dumps(inputs, sort_keys=True).encode())`：

- weir：`2e0bc7c40b7cf5e8ae2f2677818f1e566720bb2900a2ab23c8e2bfad78e97f46`。
- qualification：`e84aab070a887cfc2a1a2294790cac24fad074dbbc0dc05ea48dd6c372be275f`。

唯一新标签为完整 source SHA，无latest/semver或Git tag/Release。同SHA不同内容仍拒绝覆盖。
标准 regctl 直接导入通过验证的 OCI，完整下载比对后设标签；未手写OCI、拼tar或重建冒用hash。

```text
ghcr.io/batchstream/weir@sha256:2a3ca21b950f42449b01543e96655e0484e432f8ecbf29b46c49f7fd426c229a
ghcr.io/batchstream/weir-qualification@sha256:fbef16495a37b89d44cfce51c38096e01e982ab875e3352b67c59480fdbe61a7
```

| 包 / 平台 | Binary SHA256 | Platform manifest | Config digest |
| --- | --- | --- | --- |
| weir / amd64 | `818cb4bcd25e2b08e7983f9cf673a8aeeb79d073ebbb65c33c7ca4464cdc7a30` | `sha256:98cb64175b9bc69c4d6591bd2fcc7321a6428b60141b643717a4132ac3abcdb1` | `sha256:74c445e4111ea6930b62923bb320fcec96229bd01f441b11a5efcf5ebe8c0585` |
| weir / arm64 | `f2d107995762040c55b03cb37e94dd9e6beaa14a99d563b278b4bd9c2bd4db7e` | `sha256:100102a8c319a24f571f2aaa3041d6fafc79338840829e792f404e48d117dd5e` | `sha256:52cdc38bcb31c18c139b08cd6073c7438ecd6f56fd2fa82feb0049b8feaa9fbd` |
| qualification / amd64 | `cc0116347a079396b2d0db0783b9b60ae39ae77db3de22d103c7fb8e0b6cc524` | `sha256:b7494f3c2089d39213d0725a211978274cb8b3833e7ec82d07e39b69d8559b83` | `sha256:31fe264e9ab87853a14739a85710ef7f749f63d688acb88ca2f88d0224a3387a` |
| qualification / arm64 | `d41f70ca4bbe129bff11b76f3d973cdb288d839c4df72a348cac04a233b76482` | `sha256:9eb2ff4243d7c021abe047da28867a9b3df65695660675ebcb761387b4f50351` | `sha256:063aa9659604ca3b7ab3814e31341727fe2ccce6f6758456d3f97c4b911209ee` |

基础index仍为 `sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3`；
平台base digest见 `packaging/base.json`。每平台13个既有基础层加唯一0555正式binary应用层。
全部base/app layer、config、manifest及binary hashes保存在 `verified-delivery.json` 和两份 build receipt。
产品未加入 helper/shell/source/fixture/config/额外证书，资格工具仍为独立镜像。

## 匿名验证、本地回归和保留失败

两个 CI runner 分别以新空 HOME/DOCKER_CONFIG/REGCTL_CONFIG，完整导出两包的全部双平台图，
共4次完整导出，逐blob hash、层、source labels、config和提取binary与构建receipt一致。
不是manifest-only，也不借发布job个人/登录态。原生smoke使用相应准确platform digest。
只使用CI现有 GITHUB_TOKEN 标准登录，随后logout成功；未改包/组织权限或新建凭据。

本地普通157通过（116.677秒）；优化132通过/25skip（115.066秒，外层115.192秒）。
五项pipeline合同/快照正负对照普通与优化均通过。额外干净目录的SDK路径验证首轮遗漏
公开 `deploy/kubernetes/node.example.json`，造成1项FileNotFoundError；保留原日志，只补导出fixture，
三项实际Go模板回归随后普通/优化各3通过（4.367/4.202秒），未改产品或测试断言。

本机额外匿名复核首包在180秒超时，未完成tar为13,173,250 bytes；记录与原tar保留，
不从部分归档宣称通过。CI上述4次匿名验证均已完整成功。随后只在新空客户端重试只读传输，
每包最多600秒、每包仅一次补充导出；CI与smoke期限/测试门槛不变。补充Weir完整导出在523.153秒通过，
28,468,224 bytes，源码标签再次匹配准确index；另逐blob/rootfs diffID/应用层检查并提取两个Linux binary，
hash与CI一致。资格工具补充导出在600.013秒超时，部分tar为17,015,325 bytes，未完成本地验证；
保留原结果并停止，不从部分tar推断通过。该包完整内容与提取binary的证据来自两台CI的实际完整导出，
不称本机也完成了它。未再进行额外传输重试或新CI运行。

额外本地审计首轮错误要求 `go version -m` 文本必须含linker注入的source字符串，实际输出未列该项。
原脚本/失败log保留；只修审计的证据来源：保持compiler/OS/arch/CGO检查，以已匹配的准确binary hash、
OCI source labels及同平台CI实际 `-version` 结果交叉核对源码。随后两个Weir平台通过，未改产品、镜像或正式测试。
完整输出在 `downloaded-content-audit.json`、`extracted-final/`；不将这个审计修正倒写为原审计通过。

## 证据、清理和限制

- `actions-36418991126/logs.zip` 与 `logs/`：完整原始job/step日志；run/jobs API完整状态。
- 同目录 `records.json`、`test-summary.json`、两份 `*-build-receipt.json`：实际环境、输入、产物和计数。
- `source-freeze.json`、`source-inputs.json`、`build-inputs-source.json`：source/remote及全部输入身份。
- `native-smoke-summary.json`、`verified-delivery.json`：实际原生输出与不可变镜像身份。
- `local-anonymous-failure.json`、原部分归档、新传输的plan/results/完整归档：如实分列失败与补充。
- `manifest.json`、`delivery.json`、`final-report.md`：最终证据哈希、文档delivery/remote与输入未变证明。

自有 builder `weir-m24-36418991126-1` 已回收；六个容器逐准确ID/owner执行Stop→Wait→remove，
再按ID查空，日志含6份SMOKE_CLEANUP。发布临时login已logout，托管runner生命周期结束。
没有Actions artifact/cache上传，本地未启动Docker/DB/EKS或后台fixture；所有本地命令结束后封存。
不创建其他包、tag/Release、付费runner/资源，不读取已有秘密或修改宿主/节点设置，无定时器。

M24/source `fc0eb86…` 的旧镜像与历史证据保留，不把旧digest替换成新digest冒充重新验证。
M27启动修复与M28采样代码现已随准确新镜像交付，但新制品的EKS完整资源/计时须另行安排。
M28R2发生器11–13ms及两路各5drop的NO-GO、candidate=null保持；1秒pacing不评价5ms门槛。
CNI隔离/跨节点、六平台完整原生矩阵、其他规格/后端安全、容量、过载恢复和24h仍required。
Weir auth排除、ProgramTransform V1延期/UNSUPPORTED不变。完成回调后停止，由统筹决定下一阶段。

官方行为依据：[手动workflow](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow)、
[GHCR标准token与匿名拉取](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)、
[Docker容器参数](https://docs.docker.com/reference/cli/docker/container/run/)。
