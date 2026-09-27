# M15：可复现本地二进制与 Linux OCI

2026-09-27/28；唯一 local main 写入者，基线
`7e7f9ffafd317eb2ecc3c5fffb4623f4d50cb776`。
本阶段交付本地产物及有限原生运行证据，不是生产发布或整体平台资格。
[M14R](milestone-14-remediation.md) 已获统筹有限独立验收；原 M14 失败历史保留。
通用 ProgramTransform 仍按用户批准首版延期并保持 UNSUPPORTED；未重建 Weir 认证。

## 来源与交付目录

实现提交 `3906b5e34b8018eb2eb247887937b6a357d8da22`，产物实际源码提交
**`95796e990cd0a9832130c128661b007a1f36f574`**（补充产物 fixture 和保留正常 help 退出）。
随后 `8c2738f` 仅按编码规则拆开两处测试清理调用；本报告及 README/readiness 同步另行提交，
所以最终 main SHA 与产物 source SHA 不同。产物使用的生产源码与打包输入未再改变。
没有把版本改成尚不存在的未来提交。最终主线独立重建会记录其自己的准确 source SHA。

交付根（明确忽略且保留）：
`/Users/liran/Projects/liran/go/weir/dist/m15/`。

- `first/`、`second/`：分别独立构建的六份归档、`binaries/`、六份 `*-linked.json`、
  `module-graph.json`、`SHA256SUMS`、`receipt.json` 和 `weir-linux.oci.tar`。
- `comparison.json`：两个不同干净导出目录及逐项相同结果。
- `audit.json`、`rootfs-*-files.txt`：交付 checksum、绝对构建路径、image 文件集合核验。
- `native-load/layout/`：从最终 OCI archive 解出的标准 layout/index，仍为交付内容。
  `native-load/weir-arm64.docker.tar` 和 `receipt.json` 是经典 Docker store 的转存件及
  原 OCI config/rootfs 一致性证据；归档解出的 Darwin binary 也保留在此处。

脱敏日志/命令/receipt 根：
`/Users/liran/Projects/liran/go/weir/.testdata/m15-evidence/`。
同一源码的两份 `receipt.json` SHA256 都为
`b5707120490a4b4c761c0bb37586745609519cdc6871f4087578ab5ff468ffec`。

## 最小入口与固定输入

`python3 scripts/package.py --output dist/NEW-DIRECTORY` 是唯一正式构建入口。
默认离线 Go 构建，不访问 DB/registry、不下载工具。`--oci --builder NAME` 显式启用
固定公开 base 的本地 BuildKit 输出，不 push，也不启动 registry。
入口要求干净 HEAD（含非忽略的 untracked）、完整 source SHA、实际 Go1.27.0；
禁用工具链自动下载、用户 Go env、工作区、实验/本机 CPU override，固定
`CGO_ENABLED=0`、`GOAMD64=v1`、`GOARM64=v8.0`、`GOPROXY=off`、`GOSUMDB=off`。
未修改 go.mod/go.sum、公共协议、生成器基线或依赖版本。

在读取任何 Git blob 前检查全部 tracked 路径；`.env*`、`.ssh`、credentials、keyfile、
`.pem/.key` 风格路径直接拒绝并报告路径。只导出 Go/module 和 packaging 允许集合，
不包含测试、实验、testutil、.git、.tools、.testdata、用户配置或未跟踪内容。
导出来自 Git 对象，构建前后核对源码 hash；`go mod verify`、`-mod=readonly`，拒绝
module replace。依赖缓存可共享，**两个 GOCACHE 从空目录独立开始**，没有复制首轮产物。

实际 flags：

```text
-trimpath -buildvcs=false -mod=readonly
-ldflags=-buildid= -X main.sourceRevision=95796e990cd0a9832130c128661b007a1f36f574
```

本轮实际入口（`package.log`）：

```sh
BUILDX_CONFIG="$PWD/.testdata/m15-evidence/docker/buildx" \
DOCKER_HOST=unix:///var/run/docker.sock PYTHONDONTWRITEBYTECODE=1 \
  python3 scripts/package.py --output dist/m15 --oci --builder weir-m15-01a0e38c
```

复验应使用新的 output 目录；入口拒绝覆盖已有交付目录。自有 builder 已清理，
OCI 重建需先按下述固定 BuildKit digest 新建自有 builder 和空白客户端配置。

`-version` 输出产品/本地版本/source/Go/target/state/dirty JSON，参数处理后、任何配置
读取或 Node/DB 初始化前退出；与其他 flags 混用及多余参数拒绝。普通开发构建保留 dev，
从标准 build info 取得可用 VCS revision/dirty，否则明确 unknown。没有身份服务或注入框架。
最终 Linux image 在 network=none、无配置下也成功查询版本，见 `image-version.json`。

归档只有 executable、README 和需显式替换 endpoint 的最小配置模板。Unix 为 tar.gz，
Windows 为 zip 和 weir.exe。文件排序、mode、owner、mtime 固定；gzip 无文件名且 mtime=0，
zip 使用 stored 以避免压缩库差异。SOURCE_DATE_EPOCH 为该提交的 **1790524768**。
Go 链接器在 Darwin arm64 自动产生必要的 ad-hoc linker signature，实测仍字节一致；
它不是 Developer ID/组织签名，未签名发布、未公证，也未制定签名政策。

模块全集有 **88 项（含主模块及 test-only 依赖）**；实际 CLI 链接模块 **21 项**。
每目标通过 `go version -m` 独立记录 Go、CGO、target、CPU baseline、settings 和链接模块；
拒绝错误 target 和实验 Lua 进入 CLI。`arnodel/golua`、`yuin/gopher-lua` 仅在模块图中，
不在六份 binary 的 linked inventory。清单不是标准 SBOM、漏洞审计或安全合格声明。

## 双目录逐项复现

实际导出（已自动回收）的目录不同：

```text
/var/folders/91/pzs4g26n4_s925wqcxc1xmd40000gn/T/weir-package-first-8bjfer_e/source
/var/folders/91/pzs4g26n4_s925wqcxc1xmd40000gn/T/weir-package-second-553451n0/source
```

每行第一/第二次二进制 SHA256 相同，归档 SHA256 也相同：

| Target | 两次 binary SHA256 | 两次 archive SHA256 |
| --- | --- | --- |
| linux/amd64 | `a3a3902ace32e6f2393152f62971c4bbbad831a4c2b36d2c5d30c804cbd0585b` | `a5a01074f8e814b6a1f3395b042ce70d09cbc6de2ec5e740cbd8b0effa1ded11` |
| linux/arm64 | `b67fb29f8a6409c8c893764b162eeead2ad4d6670c560bf3b116f90c5e5a9271` | `e7122c6e5ec1a061a629702a0d446ff7dc9b65dccf6a755e7b37e1b48e683509` |
| darwin/amd64 | `e8be6ebbaefb66c63968e3b76f29702b9154b1085e302f8680cca7b9b85e3706` | `4271f2401694d7a83e63070823a657758a18c1c0189d5a84a6c78049c59e8701` |
| darwin/arm64 | `2ddcc55005a9b87d19231ee53b2b986d2fffa1bff4269e2d200ee0aa968b0e54` | `2fd2b1a0a9fe4c589371d588f8f3bcc1d9ba7dbd9160c37e5b5d0f28245cda37` |
| windows/amd64 | `1b64806c75e6d2130ca7f6716ab99151a156ad6278606ce28cceb3a1424402f4` | `0b9dae78cfeb8ec953fd3360ce8717df77aca9af2fb686d0fe59fd6dd0fd7d50` |
| windows/arm64 | `4168e3c1ed1e0d272175f70a535bb737e258f46c1da9cb545217f9eb4a77a973` | `33e860043f770c62309e0690c1e707a2dd420b9e0f5f13b9017a87ac9cb13ae7` |

两轮各 21 个 SHA256SUMS 条目独立核对通过；所有 binary 中未找到本机源码/临时目录前缀。
这证明此固定工具链/输入下不依赖这两个构建路径或当时墙钟，不外推到任意编译器/归档工具。

## OCI 固定内容与本地装载

[distroless](https://github.com/GoogleContainerTools/distroless) static Debian13 nonroot
提供标准 CA bundle、passwd、tmp 和 timezone runtime 内容；不需要 libc、shell 或 Go 编译器。
[base 内容](https://github.com/GoogleContainerTools/distroless/blob/main/base/README.md)及
[支持政策](https://github.com/GoogleContainerTools/distroless/blob/main/SUPPORT_POLICY.md)
已复核；后续 base 更新须重新锁 digest、扫描并重验，固定 digest 不代表永久安全。

`packaging/base.json` 锁定：

```text
image: gcr.io/distroless/static-debian13
index: sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
amd64: sha256:2293b36c7c9082bf4115aab724b4d2cddec82c8eba39bf27ac0517e159acf150
arm64: sha256:dd804b6c91a33a478f9a56e5033f463007696c2934a73303eb186767226bc33d
```

Builder 是本次自有 `weir-m15-01a0e38c`，Buildx `v0.36.1-desktop.1`、BuildKit `v0.32.2`，
公开匿名获取并锁定 `moby/buildkit@sha256:28a898719c18a33f4e8000685287fa36fd0dd9560c6440227d3a732d79bb41d8`。
Docker 客户端只使用本次新建空白 auths 配置，没有读取已有 Docker credentials。
产品 Dockerfile 仅 FROM/COPY/metadata，没有 RUN，构建无需执行其他架构指令或 QEMU。
UID/GID 65532，exec ENTRYPOINT `/weir`，无内置配置/凭据/私有 CA/测试 binary。
本 profile 不需要可写临时目录，测试连 tmpfs 都未添加。

两次使用 `--no-cache --provenance=false --sbom=false --network=none`、固定
SOURCE_DATE_EPOCH 和 `rewrite-timestamp=true`；依据
[Docker 复现机制](https://docs.docker.com/build/ci/github-actions/reproducible-builds/)。
实际 image 身份比较为 index/manifest/config/layer 内容 digest，而非外层传输 tar：

| 内容 | 两轮相同 digest |
| --- | --- |
| OCI index | `sha256:67110b103c8e4cb2fde9aa13bf0a70f477a62b731342048fa9df067bd2792da8` |
| amd64 manifest | `sha256:fad492493391c3980e20ca98fdb71b803f29e8f49afedefddd1283d6df2217ce` |
| amd64 config | `sha256:c20527cecca1b34f9ae0510587434e225c712dc5a78d70ce3f086780571a0bf0` |
| arm64 manifest | `sha256:f63d5c759c3b8d7f64402188e93aa8f38b64c23650dbc4e8f31c0707a9b79294` |
| arm64 config / 实际运行 image ID | `sha256:a62ff7e1a7898a695ad4f68824812fd7df46d129b7921400002b06bbe96ccb05` |

每平台全部 14 个 layer digest、base、准确 binary hash 在两份 receipt 中；每个 blob hash
均校验，并从 image layer 提取 `/weir` 的字节核对对应交叉编译产物。文件清单中包含标准
CA bundle，没有 shell/compiler/fixture/secret；两平台文件条目分别为 1396/1394（含目录）。

当前经典 Docker store 直接加载 OCI tar 报 `invalid archive: does not contain a manifest.json`。
保留原失败，没有改 daemon。按 [Buildx OCI-layout context](https://docs.docker.com/reference/cli/docker/buildx/build/)
用临时 `FROM packaged` 仅转存最终 layout 的 arm64 分支到 Docker archive；未重编 binary，
未重建应用层。转存 config digest 和每层解压后的 rootfs diffID 与原 OCI 完全相同后才运行。
这是本地格式转换，规范产品构建仍只有 `packaging/Dockerfile`。

## 准确产物的 native 范围

预算先写入 `budget.json`：Linux 每次 2 CPU、512MiB memory+swap、96 PID、non-root、
read-only root、drop ALL capabilities、no-new-privileges，配置和 CA 只读挂载；一次仅一个
被测 image。启动 7s、RPC 8s、SIGTERM/Wait 3s、整个专项 240s。未放宽既有 Guard/drain/outcome。
宿主 macOS **26.6.2 / Darwin25.6.0 arm64**；Docker VM **7.0.12-linuxkit/aarch64/cgroup-v2**。
Mongo 是显式自有 Darwin **8.0.32** 单直接副本集、SCRAM-SHA-256、真实 requireTLS。
证书由本次 fixture 生成，SAN 增加 host.docker.internal；没有读取已有秘密。

| 真实执行 | 证据 |
| --- | --- |
| Darwin arm64 | 从交付 tar.gz 提取并校验准确 binary；version + Read/Mutate/Bulk/取消/独立 DB readback；SIGTERM/Wait 1.1525ms |
| Linux arm64 final image | 上述准确 config ID；PID1=/weir、UID65532、CapEff0、NoNewPrivs1；三轮 Read/Mutate/Bulk/取消、独立 DB readback、SIGTERM/Wait |
| Linux 三轮关闭 | 125.303875 / 128.614584 / 134.037334ms，ExitCode0、OOMKilled=false；均在原 3s 界内 |
| Linux 三轮内存 | RSS 20963328 / 21532672 / 19419136B；有限 cgroup=536870912B、valid=1、unknown=0、latch=0；正常负载短时观测 |
| backend TLS | final image 对显式自有 CA 成功；错误 CA 与 hostname 均在 serving 前拒绝，ExitCode1；未关闭验证/OCSP |
| outcome/no-replay | APPLIED、预检 NOT_STARTED；实际后端确认一条 update 后代理丢回复，准确 image 返回 UNKNOWN；wire update=1、DB effect=1、无重放，proxy sockets=0 |

`packaged-native-second.log` 为通过记录，专项20.15s；host race runner21.879s。
测试清理调用格式修正后，`packaged-native-final.log` 再次完整通过，专项19.37s、
host race runner21.100s；Darwin SIGTERM 1.578084ms，Linux 三轮132.984083/
124.748125/131.482125ms，RSS21581824/21557248/21590016B、unknown=0。
两次均使用同一准确交付 binary/image；第二次未重新编译产品。
只读独立挂载的 `app.test` 用于检查 image 内部 PID1/metrics，不在产品 image 内；实际业务进程
一直是 ENTRYPOINT 的发布 binary。内存高/中/低位与 Bulk drain 的 M14R 原断言另行原样回归，
不能把本次正常负载 image 检查外推成全拓扑内存、容量或持续负载资格。

这是 **Linux Weir + Darwin Mongo 的混合环境**。当前 Linux Mongo 的 kernel7.0.12 启动
兼容阻塞仍保留；没有再次试图绕过它。系统 roots 正向、Linux 原生 Mongo、多节点/复制故障、
Search 在新 image 上的完整资格都没有由本阶段取得。其他四目标只构建，不用 file 或模拟器冒充 native。

## 回归命令与失败记录

完整参数、exit 和耗时见 `m15-evidence/validation.json`，顺序执行入口为同目录 `verify.py`。

```sh
GOPROXY=off GOSUMDB=off go test -count=1 -timeout=180s ./...
GOPROXY=off GOSUMDB=off go test -race -count=1 -timeout=180s ./...
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
GOPROXY=off GOSUMDB=off GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go vet -tags integration ./...
python3 -m unittest discover -s scripts -p package_test.py -v
python3 -m unittest discover -s scripts -p test_memory_linux_test.py -v
WEIR_M14_INTEGRATION=1 WEIR_M14_HOST_MONGO=1 python3 scripts/test-memory-linux.py
```

产物专项（Docker 使用本次空配置，backend 仅 test fixture）：

```sh
GOPROXY=off GOSUMDB=off CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go test -tags integration -c -o .testdata/m15-evidence/app.test ./internal/app
DOCKER_CONFIG="$PWD/.testdata/m15-evidence/docker" DOCKER_HOST=unix:///var/run/docker.sock \
GOPROXY=off GOSUMDB=off WEIR_M10_INTEGRATION=1 WEIR_M15_INTEGRATION=1 \
WEIR_M15_SOURCE=95796e990cd0a9832130c128661b007a1f36f574 \
WEIR_M15_IMAGE=sha256:a62ff7e1a7898a695ad4f68824812fd7df46d129b7921400002b06bbe96ccb05 \
WEIR_M15_BINARY="$PWD/dist/m15/native-load/weir" \
WEIR_M15_HELPER="$PWD/.testdata/m15-evidence/app.test" \
  go test -race -tags integration -p 1 -count=1 -timeout=240s -v ./internal/app -run '^TestPackagedArtifacts$'
```

打包输入/归档离线测试覆盖 dirty checkout、tracked secret 风格路径（不读内容）、工具链漂移、
错误 target、误链接 Lua、归档字节/元数据/漏文件。默认 Go tests 覆盖无 DB/config 的 version
和错误 flags；默认 tests 不启动 Docker/真实后端。

| 回归 | 结果/耗时 | 原始日志 |
| --- | --- | --- |
| 全仓非缓存 test / race | PASS，63.757s / 67.067s | `default-test.log` / `default-race.log` |
| default / integration / Linux arm64 integration vet | PASS | `vet.log` / `vet-integration.log` / `vet-linux.log` |
| 全部 overload 三轮 race | PASS，3.130s | `profile-race3.log` |
| CLI/config/Bulk/Native/Scan/peer/diagnostics/admission 三轮 race | PASS，104.877s | `boundaries-race3.log`；准确 selector 在 `validation.json` |
| 打包离线测试 / 原 M14 fixture 离线测试 | 4项 / 7项 PASS | `package-offline.log` / `fixture-offline.log` |
| 真实 TLS Mongo stream 六入口 | PASS，28.866s；未启用 Search 分支为 SKIP | `tls-stream.log` |
| 真实 TLS diagnostics 两入口 | PASS，14.139s；最大固定 series=2057，SIGTERM 先 readiness=503、livez=200，再有界退出 | `tls-diagnostics.log` |
| M14R Linux Guard / CLI 各三轮 | PASS，10.016s；保留原断言与预算 | `linux-native.log` 及 `.testdata/weir-m14-4fbb0048db7c/` |

M14R 回归 Guard 高位86.072/74.693/86.474ms、低位71.898/69.463/78.255ms；CLI 高位
22.503/87.325/87.464ms，SIGTERM 28.090292/27.865667/27.388667ms。
proxy sockets=0、每轮三个 owned child PID 已退出、memory.events max/oom/oom_kill=0。
这些是原 memory fixture 的证据，与 final image 的正常负载检查分别记录。

本次新增 Go 代码的本地规则检查已无结果；该检查仍报告既有
`internal/overload/profile_identity_test.go:199` 的四参数 if 调用，非本阶段引入，未改动。
最后一次仅调整两处测试调用格式，随后 integration vet 和准确产物专项再次通过；
全仓回归后没有生产代码变更。

保留的本轮失败/限制：

- 首次 BuildKit bootstrap 拉取126.4s后 inspect 超时；后续只读 inspect 确认本次 builder
  正常，随后两轮构建成功。`builder.log` 保留原错误，没有把首次 bootstrap 当成功证据。
- 经典 store 直接 OCI load 失败见 `image-load.log`；官方格式转存后成功。
- 首次转存核验错误地把压缩 blob hash 当 rootfs diffID，触发 AssertionError；修正为
  解压后逐层比较，config从始至终相同。该核验失败使第一次 native 入口找不到尚未提取的
  Darwin binary，在启动 DB 前失败；`packaged-native.log` 和 `import-first-failure.txt` 保留。
  后续准确提取/核对后完整专项通过，没有删测试或放宽界限。
- Dockerfile 的 BASE 必须由唯一入口提供固定 digest；BuildKit 的空默认 ARG warning 保留，
  实际构建没有使用浮动 tag 或空 base。
- 最后只读清理审计首次把保留的 Mongo 轮转日志名误判为临时文件；在修改任何资源前退出。
  更正日志文件名检查后通过，原记录为 `cleanup-audit-first-failure.txt`，未放宽测试或资源预算。

## Owner 清理与保留材料

`m15-evidence/cleanup-final.json` 记录最后核验：本轮25个自有 TLS Mongo fixture
只剩 owner 和日志（包括轮转日志），生成的 CA/证书/私钥/keyfile/数据库数据目录已删除。
`weir-m15-artifact-22898`（首次失败）、`-22940` 和 `-24046` 只剩 owner/日志，
没有遗留 config 或 CA。所有对应 owner 的 Linux 容器已停止并删除。

本次 BuildKit builder `weir-m15-01a0e38c`、其唯一容器与状态卷均已删除，
见 `builder-cleanup.json`；version 专用容器也已删除。M14R 回归的
`weir-m14-4fbb0048db7c` 两个容器和网络均不存在，Darwin Mongo PID23859 已 Wait、exit0，
该 fixture 的 `cleanup.json` 为 `all_stopped=true`、`errors=[]`。
最终只读进程检查没有 mongod/weir/app.test/overload.test 遗留。

两个干净 source 导出及各自 compilation cache 已自动回收；专项 helper 和临时格式转换
Dockerfile 已按明确 owner 删除。交付六目标两份 binary/archive、OCI archive/layout、
Docker 转存件、准确提取的 Darwin binary、已加载的准确 arm64 产品 image、receipt 和日志均保留。
Docker 共享 image cache 中可保留本次固定公开 BuildKit image；没有运行中的 builder 或状态卷，
未全局 prune，也未删除未知资源或已有 fixture。自有空白 Docker 配置保留供复核，无已有 credentials。

最终提交后核验 main 干净，交付 SHA 和产物 source SHA 分开写入
`m15-evidence/submission.json` 并回调统筹。本聊天到此停止 checkout 写入/测试，不自启下一阶段。

## 平台与供应链门槛

[Go1.27](https://go.dev/doc/go1.27)与[官方最低要求](https://go.dev/wiki/MinimumRequirements)
记录编译器下限：macOS13+、Linux kernel3.2+、Windows10/Server2016+。实际 Mach-O 的
LC_BUILD_VERSION=minos13.0/sdk26.2 已检查。该表不是 Weir 在所有最低版本的运行保证；
Linux 还依赖受支持的静态可见 cgroup-v2/RSS profile，具体测试版本如上。

| 平台 | build | reproducible | native-run | resource | lifecycle | qualified |
| --- | --- | --- | --- | --- | --- | --- |
| Linux amd64 | PASS | PASS | 未验证 | 未验证 | 未验证 | 否 |
| Linux arm64 | PASS | PASS | final image，有限混合后端 | 单有限 cgroup/正常 image + M14R 回归 | image 三轮有限 SIGTERM | 否；完整后端/拓扑/部署/容量/soak 缺失 |
| macOS amd64 | PASS | PASS | 未验证 | 未验证 | 未验证 | 否 |
| macOS arm64 | PASS | PASS | 准确归档 binary，有限后端 | Go fallback；OS内存未资格 | 有限取消/SIGTERM | 否 |
| Windows amd64 | PASS | PASS | 未验证 | 未验证 | 未验证 | 否 |
| Windows arm64 | PASS | PASS | 未验证 | 未验证 | 未验证 | 否 |

[Go1.27.1 已于2026-09-01发布](https://go.dev/doc/devel/release)；按阶段要求保持已验证的
1.27.0以固定打包变量，不宣称最新/安全。完整漏洞扫描、标准 SBOM、依赖/基础镜像安全复核、
签名政策和必要有限 patch 升级仍是后续门槛。本阶段没有声称已扫描无漏洞。
未实现 Kubernetes/Helm、全平台 OS 内存、容量/soak、发布平台或自动更新；不部署、不付费。
