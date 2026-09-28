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

实现提交之后实际执行；结果、所有 Actions 尝试、准确源码和最终文档提交区别在阶段收尾补入。
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
