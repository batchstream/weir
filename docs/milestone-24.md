# M24 — 固定源码的 Actions / GHCR 镜像交付

2026-09-28；执行聊天 `01a0e593-317c-7431-b10f-91c7cefed657`。
基线 `d4081c449b6e80e639a1ce6930a090746bd2c813`，M23 已获统筹独立验收。
本阶段仅允许既定 public 仓库正常 fast-forward push、两个批准 GHCR 包的镜像发布和 public 可见性。
不发布 Git tag/Release、不创建 EKS/DB/业务负载、不读取既有秘密、不新增凭据或收费资源。

## 交付边界

唯一 `workflow_dispatch`，固定 batchstream/weir 的 main/source SHA、并发组防重叠，
两个标准原生 Linux 测试 runner、一个 amd64 双架构打包/发布 job、一个 arm64 匿名 smoke job，
实际共四 job，每 job 至多30分钟。最多三次有明确原因的完整尝试，约120分钟总执行/排障预算。
默认 contents:read，packages:write 仅在发布 job；GITHUB_TOKEN 仅进入标准临时 registry login，
不进入 build context、日志或 artifact。没有 artifact/cache/构建记录上传。

Go1.27.1 官方 SDK checksum、checkout v7.0.1 完整 commit、Buildx v0.37.1、
BuildKit v0.33.0 index digest、regctl v0.11.6 checksum 见 `scripts/ci-tools.json`。
基础镜像仍为 `packaging/base.json` 已审计的 distroless index 和两平台 digest。
runner 标签不是不可变 OS；实际 image版本/架构/内核/资源及 runner启动版本保存在 CI logs。

复用 `scripts/package.py`：全部 tracked 路径先检查秘密风格，按固定 Git 对象导出；
产品保持原 allowlist，资格工具仅另加自身非测试 Go 源码和专用 Dockerfile。
每轮新导出目录和空 GOCACHE；go.mod/go.sum 不变，离线只读 module、go mod verify、
CGO0/trimpath/buildvcsfalse/空 buildid/CPU baseline/sourceRevision 检查。
产品六平台双次编译与归档、工具两个 Linux 平台双次编译、两个 OCI 双次验证分列记录。
编译不能替代其他平台 native 资格。

最终镜像严格为原基础层加唯一非root二进制层。标准 regctl 直接导入已验证 OCI，
按 digest 再完整拉取核验最终 index/platform/config/layers/提取 binary hashes 后再设置完整 SHA 标签。
同 SHA 不同内容拒绝覆盖。匿名验证使用全新空 HOME/DOCKER_CONFIG/REGCTL_CONFIG，
下载两包完整双平台内容；原生 smoke 禁网、只读根、drop caps、1CPU/256MiB/128PID、20秒外层限时。
资格工具原算法和门槛不变，1秒 smoke 不评价5ms性能门槛。

## 执行证据

以下保留原失败，再列第2次实际交付；镜像source与随后文档delivery提交严格区分。
初始实现 `f9b3f82431afdf1f0b8d76870d8c7432eb216abf` 已正常push。
第1次 [36366147893](https://github.com/batchstream/weir/actions/runs/36366147893) 失败保留：
两个原生Linux test job均成功（CGO0、CGO1 race、vet/integration vet、helper race和38项Python回归），
但发布job尚未构建即因离线完整module graph缺少lazy依赖失败；清理又使用了Buildx不支持的
`inspect --format`，遮住原始stderr。未登录、未发布任何镜像，arm64镜像smoke未运行。
原始job上的builder未由脚本成功回收，随该标准托管VM结束；不称本次脚本清理通过。
空module cache独立重现原失败；准备阶段在独立临时module副本`go mod download all`，
下载可为该临时副本补充未链接依赖zip校验项，仓库go.mod/go.sum保持原样；原始文件的立即离线
list/verify已实测通过。清理使用已验证的`buildx ls --format`检查唯一自有builder，保留原始错误。
证据根 `.testdata/m24/`，忽略且不进入镜像/提交。
本地已验证 package、OCI负面/匿名身份/不可覆盖回归、容量工具离线回归；
用历史 M20 OCI 做标准 regctl 导入导出验证，index/manifest/config/layers/binary 不变，
仅用于验证转存工具，不冒用该旧镜像为本阶段产物。

## 限制与官方依据

M22R 独立50ops/s dispatch p99=8.3ms>5ms、generator_qualified=false、candidate=null 保持。
完整容量阶梯/确认/直连/过载恢复与24h未运行。Weir认证排除、ProgramTransform首版延期/UNSUPPORTED不变。
其他六平台/后端安全/部署/资源门槛仍需独立证据，不宣称整体 production ready。

2026-09-28 核验官方文档：[标准public runner免费](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)、
[GITHUB_TOKEN registry发布](https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images)、
[GHCR与匿名拉取](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)、
[包可见性独立于仓库](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility)、
[GHCR当前存储/带宽免费，Actions artifact共享额度有限](https://docs.github.com/en/billing/concepts/product-billing/github-packages)。
未变更billing、组织默认策略或其他包权限。

## 第2次实际交付

修复及准确镜像源码 **`fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`** 已正常 fast-forward push。
[第2次运行 36366671236](https://github.com/batchstream/weir/actions/runs/36366671236) 于
2026-09-28 01:37:12–01:50:24 UTC 完成，四个实际job全部成功。没有第3次尝试。
本报告收尾提交是随后仅更新文档的 delivery SHA，不能冒充已发布镜像 source SHA；
准确最终 delivery/pushed SHA 另存本地 `final-state.json` 并在统筹回调中列出。

| 检查 | 实际结果 |
| --- | --- |
| 原生 Linux amd64 / arm64 默认CGO0、独立CGO1 race | 两者均非缓存 `-count=1` 全部通过 |
| 两原生平台 vet / integration vet / helper integration race | 全部通过；integration vet仅静态，无真实DB |
| 两原生平台 Python 回归 | 各38项通过：package5、CI边界3、capacity21、入口9 |
| 产品六目标编译/归档 | 两次独立干净导出及空GOCACHE，六binary/六archive逐项相同；其他四目标仍仅编译 |
| 工具 linux/amd64、linux/arm64 | 两次独立CGO0编译一致；保持原integration helper行为 |
| 两个双架构OCI | 两轮index/platform/config/layers/提取binary一致，精确基础层+唯一应用二进制层 |
| 最终registry内容 | 标准regctl导入，无重建/手写OCI；完整重拉后逐项比对已验证产物，再设置完整SHA标签 |
| 匿名与原生smoke | 两个原生runner各完整匿名下载两包index及两平台所有blob；准确product `-version`、1秒禁网pacing均通过 |

构建步骤01:41:23–01:48:57 UTC，发布步骤01:48:58–01:49:51 UTC。
Go driver/compiler1.27.1；两原生test runner均4vCPU/约16GiB、kernel
`6.17.0-1022-azure`、Actions runner `2.337.0`；amd64 image `20260920.314.1`，
arm64 image `20260920.129.1`。这些是本次动态镜像版本，不把ubuntu标签视为不可变OS。

产品Go代码、go.mod/go.sum、base与M23基线不变；70项导出输入中仅packaging/README.md新增说明。
工具8个Go文件未变，额外导出专用Dockerfile，合计79项。两个input map已独立按Git blob重算一致。
input-map SHA256定义为 `sha256(json.dumps(inputs, sort_keys=True).encode())`：

- weir: `f4bb9ecb90d56a260f9237979b2e946575bfd9f1da49e8076dd90cf4aaa68f80`。
- qualification: `e0795944390fec13f7985d014842f055a9c20a448bb32442cfd861721a3a3edd`。

## 已发布且可匿名拉取的准确内容

唯一标签均为完整source SHA `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`，无latest/semver/release标签。
两包在干净匿名上下文实际下载成功，无需额外public UI操作；未变更组织默认策略或其他包权限。
本机已配置gh身份查询包管理API返回缺read:packages的403，原记录保留，未扩权/新设token。
公开可用结论来自真实匿名内容下载，不依赖该受限管理API、登录态或push成功。

```text
ghcr.io/batchstream/weir@sha256:cc6428d1ead507e531f95b8c45926f8bf31abf8ba9cb89cf6e8eca4a865b1f10
ghcr.io/batchstream/weir-qualification@sha256:8a5c4dbca08daae24f62798c40f1607bbfaa5362b5b86db9a5651eb5475b3057
```

| 包 / 平台 | Binary SHA256 | Platform manifest | Config digest |
| --- | --- | --- | --- |
| weir / amd64 | `8022358e061c60fe80a511b37ade60deaed00171ac141481cc4e105267f653a5` | `sha256:4e35500bd3ad092124ce64ab2c08d8f7064c314004eb651792ec0c90aab5cb54` | `sha256:05a809ccc7df9968b45013bbc7516fcabc34d5f9498a83a50ffaf50119bcab30` |
| weir / arm64 | `e152805a350d5b2968df18f736e587b649f9ef2cf3cc6c650aa44bd37cbf3b41` | `sha256:9dc3cb7fb9e69a332d10a1f49941bd80e9752778078335ac6dff7c22a6799856` | `sha256:41d400729148451fbd11846deecc4cb940c11eeaad7e4702905aa1807dfa8559` |
| qualification / amd64 | `f2231ad1ba130ec9745224d11873c19e36aed34cb24b5144e19ee7f56904a084` | `sha256:0b1033820beb5de3dde467ae56ea3c3bca43af1774af510e2bf2f2aad85c8f03` | `sha256:c28e22ab361c2691bcd02430193b1f8741bee8b14b329b484925aca2488fcc66` |
| qualification / arm64 | `fd5a5d4f1f8127a0ca30d3155d3f9170f6ab0bc70d82f9f28b01fb287e7e27fc` | `sha256:012713da8f0f0ccec1d24fe589c22b19789e177a1fc82702dd3701c190e55b05` | `sha256:7283ae02ef10d9fd28d66f651a1d7096d625d0dcabc7819cbb35a89903968205` |

Base index仍为 `sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3`，
平台base digest见 `packaging/base.json`；最终全部层digest保存在 `delivery.json`。
原生产品version准确报告该source、go1.27.1、linux/各架构、clean-commit、dirty=false。
两个纯pacing smoke各50planned/50completed/50success、0drop/UNKNOWN；没有运行或评价5ms发生器资格。

## 本地复核、证据与清理

本地Darwin arm64固定Go1.27.1：默认CGO0 62.973秒、独立CGO1 race 65.582秒、vet/integration vet、
helper integration race及38项Python回归全部通过，完整命令/exit/耗时/hash见 `local/validation.json`。
这不冒充macOS完整产品资格。模块准备补救的空缓存失败及修后原始module文件离线成功分别保留，
没有依赖升级或go.mod/go.sum变更。

本机额外创建全新空HOME/DOCKER_CONFIG/REGCTL_CONFIG，仅由固定regctl按上述index digest
下载两包及全部双平台blob，94.779秒成功；再次核对index/platform/config/基础层/源码labels/提取binary
与CI完全一致。没有用本机个人Docker登录，也没有在Darwin把跨架构执行冒充native。
`local-anonymous.log/json`、`ci/*-anonymous.json`及两个完整OCI归档保留供统筹复验。

证据根 `/Users/liran/Projects/liran/go/weir/.testdata/m24/`（权限0700、Git忽略）：

- `actions-36366147893/` 和 `actions-36366671236/`：所有尝试原始CI logs、job/step状态及SHA256SUMS；失败不倒写。
- 成功run的 `weir-build-receipt.json`、`qualification-build-receipt.json`：输入逐文件hash、binary/archive hashes、flags/Go/OCI身份；`records.json`含真实native输出。
- `source-audit.json`、`verified-delivery.json`、`attempts.json`：Git输入复算、双包交付及完整尝试链。
- `preflight/`：公开工具发行元数据/checksum与所有待公开提交的路径/祖先审计；未读取秘密文件。
- `module-repro/`：空缓存原失败、完整准备补救与原始go.mod/go.sum离线list/verify；仅准备公开固定依赖。

第2次自有builder `weir-m24-36366671236-1` 已核对唯一owner后回收，日志含BUILD_CLEANUP。
四个native smoke容器均以创建返回的准确ID及owner label核验、stop/remove；对应job在finally成功之后结束。
发布job的临时registry登录已logout；托管runner临时磁盘随job生命周期回收。
本地没有启动Docker builder/container、DB、EKS或后台测试服务，只有受控证据与公开OCI下载留存。
无Actions artifacts/cache上传，无tag/Release、AWS/Kubernetes写操作、节点/宿主设置变更或收费资源。
当前阶段交付待统筹独立验收；完成回调后停止，不自动续开EKS/容量/其他聊天。
