# M16：本地 SBOM、漏洞基线与必要补丁

2026-09-28（Asia/Shanghai）。基线 `0670f7820fa6a7f2b3c4de7c385df94f5eb7507b`；
M15 已获统筹有限独立验收。旧制品的实际 source 是
`95796e990cd0a9832130c128661b007a1f36f574`，不是该文档基线。
本轮实现和新制品 source 均为 **`84a34b38419842f3bd52993ea77450803527ce37`**；
报告/README/readiness 后续提交不改变构建输入，最终 main SHA 单独记录。

本阶段已获统筹有限独立验收，最终 main 为 `f20cff8965a3e38f74a79e5b629022ef5b6c2599`。
独立复验接受以下准确 CLI 中未链接 SSH/OpenPGP 的三个模块匹配，不是全局豁免；
历史 Search 版本风险由 [M17](milestone-17.md) 单独处理，下文仍保留本阶段实际版本。

本阶段完成有限本地供应链基线；不代表生产合格。一次必要依赖修复后，六目标函数级扫描
不再报告已知漏洞，仍有未链接包的模块匹配；旧 Search 后端有已公布版本风险。
没有忽略规则、手写 SBOM、额外扫描引擎或为清零而盲目升级。ProgramTransform 继续
UNSUPPORTED，首版延期；没有恢复 Weir 认证。未 push/PR/tag/release/部署/付费，未改系统
工具链或 Docker daemon，未读取已有秘密。下一阶段由统筹决定，本聊天停止后不自启工作。

## 交付与来源

根目录 `/Users/liran/Projects/liran/go/weir/` 下：

- `dist/m16/first/`、`second/`：六目标各两套准确 binary、archive、build info、module graph、
  OCI archive、receipt、SHA256SUMS；`comparison.json`、`audit.json` 为独立核验。
- `dist/m16/native-load/`：交付 OCI layout、原 config/rootfs 等价的 Docker 转存件、
  准确 Darwin archive binary 和转换 receipt。
- `dist/m16-scans/before/`、`after/`：各 **9 份 CycloneDX 1.6**、9 份 Syft 补充输出、
  9 份 Grype JSON；六目标源码/二进制 govulncheck JSON、实验测试扫描、完整模块图候选查询。
  `input-receipt.json`、`sbom-receipts.json`、`grype-receipts.json`、`go-scan-receipts.json`、
  `schema-validation.json` 绑定 source/Go/target/原始 bytes/退出状态/时间。
- `.testdata/m16-evidence/`：工具来源/校验、两类 DB snapshot receipt、配置和命令、
  依赖代码审计、全部测试/清理日志及本地复验脚本。`.tools/m16/` 保存公开工具、离线 DB、
  schema 和空配置；新编译器仅安装在 `.tools/go1.27.1/`。

以上目录显式忽略并保留，不把机器日志/二进制加入 Git。最终 `evidence-index.json`
记录报告、命令、schema、工具 receipt、测试及交付 receipt 的 SHA256；旧 M15 内容未改。
可复验操作和 unsigned 政策见 [supply-chain.md](supply-chain.md)。这些 receipt 是来源绑定，
**不是签名或可信发布者证明**。

## 工具与数据库

| 工具 | 固定版本/来源 | 校验与限制 |
| --- | --- | --- |
| Go | 官方 go.dev，1.27.1 darwin/arm64 | archive SHA256 `ee215d57e0ec269c60cc9ceca68e6bda321ba9ee5afe24f4b0988703c2d87d12`；本地编译器，不改系统1.27.0 |
| govulncheck | 官方 `golang.org/x/vuln` v1.8.0 | public proxy + sum.golang.org 校验；独立 GOBIN/GOPATH/GOCACHE；module sum/build info 在 tools.json |
| Syft | 官方 Anchore v1.52.0 | archive SHA256 `014d561b6d13059124155f74a6c5a9a99501f5e209313638dd884f39eb418ee6`，核对官方 checksums 后执行 |
| Grype | 官方 Anchore v0.119.0 | archive SHA256 `500c9b2b6c089d21481815f57a553fabbd441ec7d1e79d95e3aaf40c3bfc7e36`，同样校验 |
| JSON schema 校验 | jsonschema4.25.1、官方 CycloneDX1.6 schema | 独立 venv；pip receipt 记录所有下载 URL/hash；Draft7 + FormatChecker、离线引用；18份均0错误 |

Go DB `modified=2026-09-24T20:07:49Z`；保留完整 modules/index 和 M15/M16 模块并集、
stdlib/toolchain 的全部历史 OSV，共278份。最初276份；新 MVS 六个路径需要另两份历史
记录，补齐后才算完整。两份 `modified` 均早于原冻结 index；原 DB/index bytes 未改变，
旧扫描不受新路径影响。`go-db-snapshot.json` 与 `go-db-expanded-scope.json` 逐文件列 hash。

Grype DB schema `v6.1.9`，build `2026-09-27T06:30:30Z`，官方归档 checksum
`0567d832ad6f47af3f67f0d8aa36397f8a3557dc51311cff68e2bb4332ba3c56`；本地数据库 SHA256
`24b7c0db32df3a3d2417564662ab7f4745fc9896fe98675ba2b00aafcb38726c`。
前后扫描共用该文件，auto-update=false，hash 校验和默认120h新鲜度校验均开启；
status valid=true。没有把离线库的未来过期解释为安全。配置关闭远程 enrichment、外部源、
用户 ignore/VEX；空 HOME，显式 binary/OCI 输入，不扫描用户目录、checkout 或 fixtures。

## 扫描覆盖与原始结果

| 检查范围 | M15旧版 | M16新版 | 解释 |
| --- | --- | --- | --- |
| 每个平台的 Go source | 9个模块匹配；2个函数级ID | 3个模块匹配；0函数级ID | 六个平台分别扫描 `./cmd/weir`，匹配各自编译器和 GOOS/GOARCH/CGO=0 |
| 每个平台的 Go binary | 9个模块匹配；3个符号级ID | 3个模块匹配；0符号级ID | 六个准确 binary 独立扫描；符号存在不等于可达 |
| 每份 binary/OCI 的 Grype | 9条：7High/1Medium/1Unknown | 3条：2High/1Unknown | 六binary、两image原始报告均保留；未抑制 |
| 完整模块图 Grype | 17条：13High/2Medium/1Low/1Unknown | 7条：4High/1Medium/1Low/1Unknown | 87→93个版本化依赖；加主模块88→94；非产品内容清单 |
| 实验 Lua source `-test` | 0 finding | 0 finding | 本机实验测试的有限扫描，不是隔离资格或所有依赖安全承诺 |
| 两架构 OS包/stdlib | 当前库无匹配 | 当前库无匹配 | 明确检测到stdlib和6个Debian包；没有把漏扫当0 |

Go query 对模块全集返回旧17/新6个候选 OSV；这是官方查询结果，不是17/6个可达漏洞。
例如 GO-2026-5158 的旧otel1.39.0低于首次受影响1.41.0，新1.44.0已修复，均不受影响；
Grype另有 Go DB 尚无匹配记录的低危 GHSA-8wmf-6v46-5gfg。两个数据库不互相替代。
另外对toolchain@1.27.0和toolchain@1.27.1分别运行官方候选查询，均无候选；receipt单独保留。
Go输出包含很多历史 OSV 元数据，统计只使用 `finding`，不把所有 OSV 对象当漏洞。
JSON模式 exit0不表示没有漏洞；未支持或失败命令不算通过。

Syft binary目录与 Go build info 逐项相同：Linux各21个实际依赖，Darwin/Windows各20个
（少Linux专用procfs），另有主模块和stdlib；Windows另有PE文件组件。纠正M15文档将21
泛化到所有目标的表述。主模块版本 UNKNOWN 是未发布本地版本，source-version/receipt
记录准确SHA，没有伪造 semver。两个 Lua module 均只在完整图/实验中，不在任何CLI。

## 逐项处置

下表保留上游/Grype严重度；“未链接”是对这六份准确CLI的限定判断，导入/目标/配置变动
须重审。所有版本范围与别名详见冻结 OSV 和原始 Grype JSON，未写 suppressions。

| ID / 上游依据 | 旧→新 / 修复版本 | 严重度、路径及结论 |
| --- | --- | --- |
| [GO-2026-6348](https://pkg.go.dev/vuln/GO-2026-6348), CVE-2026-84304 / GHSA-vp52-pcj8-j9qc | grpc1.79.3→1.83.2；fix1.83.1 | High。HTTP/2碎片DATA使接收队列内存膨胀；共享recvBuffer用于RemoteWeir及ServeHTTP handler。源码有调用链，需修复。没有自行声称重现CVE exploit；保留上游修复和行为回归证据。 |
| [GO-2026-6061](https://pkg.go.dev/vuln/GO-2026-6061), GHSA-hrxh-6v49-42gf | grpc；fix1.82.1 | High。Rapid Reset服务端与xDS RBAC。源码报告公共transport符号，但Weir入口是标准net/http HTTP2 + grpc.ServeHTTP，不是grpc http2Server；无xDS。不能将保守trace直接当实际入口漏洞。升级一并覆盖。 |
| [GO-2026-6443](https://pkg.go.dev/vuln/GO-2026-6443), CVE-2026-84445 / GHSA-2v4p-qf9q-27wj | grpc；fix1.82.2或1.83.2 | High。缺authority/Host的xDS服务端panic；binary符号匹配，无实际xDS入口。选1.83.2而不是仍受影响的1.83.1。 |
| [GO-2026-6441](https://pkg.go.dev/vuln/GO-2026-6441), CVE-2026-84303 / GHSA-qc2q-p7wx-3px3 | grpc；fix1.83.1 | Medium。xDS RBAC filter未链接/未配置；补丁覆盖。 |
| [GO-2026-5942](https://pkg.go.dev/vuln/GO-2026-5942), CVE-2026-46600 | x/net .55→.58；fix.56 | High。x/net/dns/dnsmessage未链接；标准库vendored同名路径不同，Go1.27.0-rc.3已修复，旧1.27.0也不受影响。MVS升级覆盖外部模块版本。 |
| [GO-2026-6303](https://pkg.go.dev/vuln/GO-2026-6303), CVE-2026-56854 | x/crypto .53→.55；fix.55 | High。SSH source-address callback，SSH未链接；MVS升级覆盖。 |
| [GO-2026-6354](https://pkg.go.dev/vuln/GO-2026-6354), CVE-2026-78662 | x/crypto .55保留；fix.56 | High模块匹配仍在；SSH channel Accept/未决通道死锁。六目标无SSH包/调用链；本CLI未受影响，不为清零额外升依赖。 |
| [GO-2026-6355](https://pkg.go.dev/vuln/GO-2026-6355), CVE-2026-56855 | x/crypto .55保留；fix.56 | High模块匹配仍在；SSH已建立通道死锁。同上；若以后引入SSH，先升级并复审。 |
| [GO-2026-5932](https://pkg.go.dev/vuln/GO-2026-5932) | x/crypto/openpgp；无fixed版本 | Unknown严重度，包停止维护；未链接OpenPGP。这不是可通过整个x/crypto升级消除的条目；禁止将其标成低危或永久忽略。 |
| [GO-2026-4945](https://pkg.go.dev/vuln/GO-2026-4945), CVE-2026-34986 | go-jose/v4 4.1.3→4.1.4；fix4.1.4 | High；只在完整MVS图，JWE路径未链接；grpc选版自然覆盖。 |
| [GO-2026-5320](https://pkg.go.dev/vuln/GO-2026-5320), CVE-2026-5160 | goldmark1.4.13保留；fix1.7.17 | Medium；HTML renderer XSS，仅完整图，无CLI Markdown渲染；保留候选。 |
| [GO-2026-5506](https://pkg.go.dev/vuln/GO-2026-5506), CVE-2026-29181 | otel1.39→1.44；fix1.41 | High；baggage多值分配，仅完整图；版本已覆盖，未加入产品OTel。 |
| [GO-2026-5158](https://pkg.go.dev/vuln/GO-2026-5158), CVE-2026-41178 | 受影响1.41和1.43，fix1.42/1.44 | Go query候选；旧1.39和新1.44均不在受影响区间，且未链接；不能当升级修复数量。 |
| [GO-2026-4394](https://pkg.go.dev/vuln/GO-2026-4394), CVE-2026-24051 | otel/sdk1.39→1.44；fix1.40 | High；PATH hijack，仅完整图，未链接；版本覆盖。 |
| [GO-2026-5426](https://pkg.go.dev/vuln/GO-2026-5426), CVE-2026-39883 | otel/sdk；fix1.43 | High；BSD kenv/PATH，仅完整图，未链接；版本覆盖。 |
| [GO-2026-6179](https://pkg.go.dev/vuln/GO-2026-6179), CVE-2026-56865 | x/mod .37→.38；fix.40 | High模块图条目仍在，sumdb/tlog未链接；实际Go编译器1.27.0-rc.3已修复，不能混同工具链与外部x/mod模块。 |
| [GO-2026-6180](https://pkg.go.dev/vuln/GO-2026-6180), CVE-2026-56864 | x/mod；fix.40 | High模块图条目仍在，sumdb Lookup未链接；实际新旧Go均高于其工具链修复版本。 |
| [GHSA-8wmf-6v46-5gfg](https://github.com/open-telemetry/opentelemetry-go/security/advisories/GHSA-8wmf-6v46-5gfg), CVE-2026-81870 | otel/sdk1.39→1.44；fix1.45 | Low，Grype独有匹配；开启内部Info日志可泄露exporter endpoint；SDK/exporters未链接，无该配置。模块图保留，不冒充Go扫描覆盖。 |

实际包集合在 `dependency-audit/*-packages.txt`，与标准SBOM/build info交叉核对。
没有把“依赖模块中包含漏洞包”直接等同于“该包进入CLI”；也没有将扫描器静态能力扩展
到reflection/unsafe、全部协议组合或将来新CVE。保留的风险需要随输入变化重新评价。

## 必要改动及传输重审

只做一轮补丁：Go1.27.0→1.27.1（同步 go.mod/.go-version/打包器/bootstrap/README），
grpc1.79.3→1.83.2以及其最小版本选择结果：x/net.58、x/crypto.55、x/sync.22、x/sys.47、
x/text.41、genproto/rpc 20260526版本。`go mod tidy`把实际直接导入的x/crypto纠正为direct。
完整MVS图的其他变化逐项在新旧module-graph；不是手工批量更新。Mongo driver2.9.1、
compress1.19.2、protobuf1.36.11、实验Lua和固定distroless base保持原版本。
[Go官方patch说明](https://go.dev/doc/devel/release)不被描述成不存在的CVE修复证据。

打包器从捕获的完整SHA枚举对象，不再重新读取HEAD；新增离线Git反例移动HEAD、修改原
文件并添加新文件，仍只导出原SHA的内容；拒绝非完整SHA。没有扩大打包路径白名单。

审计保存实际依赖源码hash、关键代码和OCSP差异：

- gRPC1.83.2仍显式DisableRetry/DisableServiceConfig、WaitForReady=false、无proxy，
  固定stream/connection窗口65535、read/write buffer16KiB、retry RPC buffer0。
  `shouldRetry`在disableRetry之前仍有“尚未建立transport/明确unprocessed”的transparent
  分支；本阶段没有谎称库完全不重试。已提交/不确定后端效果仍由Weir逻辑分类并实测无重放。
- DATA buffer compaction默认true，实际调用路径使用同一recvBuffer；部署不得设置
  `GRPC_GO_EXPERIMENTAL_ENABLE_RECEIVE_BUFFER_COMPACTION=false`，否则关闭上游安全修复。
- Go1.27.1 HTTP1 Transport的重试仍检查复用连接和可重放body。Search业务请求保持非空
  非rewindable body、GetBody=nil，Native用独立新连接；未添加幂等重试header。
- Mongo仍禁用retryReads/retryWrites/adaptive retry/overload retargeting/压缩，poll监控；
  连接owner与wire guard未放宽。x/crypto OCSP变化为request签名解析拒绝/文档，response
  签名验证未删除。真实TLS/OCSP/DNS/391/丢回复、取消/关闭回归通过。

## OCI基础系统与外部后端

两个image都实际检出Debian13.7；各6个dpkg组件：base-files13.8+deb13u7、
ca-certificates20250419、media-types13.0.0、netbase6.5、tzdata及tzdata-legacy
2026c-0+deb13u1。来源为 `packaging/base.json` 的官方distroless固定index及两个平台digest，
本轮不换base。具体dpkg metadata/location在 `audit.json`，不是从浮动tag推测。
两架构CA bundle均224449B，SHA256
`714d457d580922dbf1d0be8bd35ba236a842b50b0072ae791582a19adef772a5`。
当前Grype DB对这些包无匹配；没有因此声称CA信任策略审计、私有根管理或永久安全。

外部数据库未装进Weir image，不能用image扫描替代以下官方公告复核：

| 固定后端 | 2026-09-28复核结论 | 门槛 |
| --- | --- | --- |
| MongoDB8.0.32 / Go driver2.9.1 | [官方alerts](https://www.mongodb.com/resources/products/alerts)：Server CVE-2026-89099（High7.7）受影响<8.0.32，本profile已达fix；driver Client.BulkWrite CVE-2026-81521（High7.1）受影响2.1..<2.8.2，当前2.9.1不受影响；[8.0生命周期](https://www.mongodb.com/legal/support-policy/lifecycles)仍受支持 | 这些具体项不阻塞本profile，不等于穷尽服务器组件审计；Linux native Mongo内核启动阻塞仍在 |
| Elasticsearch8.17.0 | [维护政策](https://www.elastic.co/support/eol)：8.17维护已结束；[ESA-2026-07](https://discuss.elastic.co/t/elasticsearch-8-19-10-9-1-10-9-2-4-security-update-esa-2026-07/384525) CVE-2025-66566 High8.4 transport/LZ4信息泄露，8.x fix8.19.10；[ESA-2026-184](https://discuss.elastic.co/t/elasticsearch-8-19-22-9-4-7-9-5-3-security-update-esa-2026-184/390688) CVE-2026-94408 Medium4.9资源耗尽，fix8.19.22 | **生产阻塞**。Weir发HTTP REST不是node transport，但这不能替部署隔离豁免服务器风险；需另行授权更新固定profile并重验 |
| OpenSearch2.19.0 | [维护线](https://opensearch.org/releases/)仍有2.19.6，旧patch未覆盖修复；CVE-2025-9624 / GHSA-mw3v-mmfw-3x2g High复杂query_string DoS，2.19.4修复，见[上游修复](https://github.com/opensearch-project/OpenSearch/pull/19491)及[2.19.4发布](https://github.com/opensearch-project/OpenSearch/releases/tag/2.19.4)；[FLS](https://github.com/opensearch-project/security/security/advisories/GHSA-2rjv-cv85-xhgm)与[masking](https://github.com/opensearch-project/security/security/advisories/GHSA-rrmm-wq7q-h4v5)均Moderate、fix2.19.3 | **生产阻塞**。Native/Scan可转发query_string，有限body不证明后端查询安全；fixture未配置FLS/masking，后两项有条件但旧版本仍受影响。未擅自换数据库 |

## 双轮制品与原生证据

使用固定BuildKit v0.32.2 / Buildx v0.36.1-desktop.1，自有builder和空Docker客户端配置；
唯一正式入口：

```sh
PATH="$PWD/.tools/go1.27.1/bin:$PATH" \
BUILDX_CONFIG="$PWD/.testdata/m16-evidence/docker/buildx" \
DOCKER_HOST=unix:///var/run/docker.sock PYTHONDONTWRITEBYTECODE=1 \
  python3 scripts/package.py --output dist/m16 --oci --builder weir-m16-01a0e3b9
```

双轮均从不同干净Git导出、空编译缓存开始，flags/CGO/CPU基线/时间固定，Go离线，
OCI无RUN/QEMU/push。两套各21个SHA256SUMS条目独立通过。两轮receipt SHA256均
`88b871476bd2329d632e2a4a696c6477d1f31f7e6c04b2ff5b777a9b8f8c44fb`。

| Target | 两轮相同binary SHA256 | 两轮相同archive SHA256 |
| --- | --- | --- |
| darwin-amd64 | `d9ee00c768c756733f707f8c7e3ed4ca410f68960c2a1d342bc1a35adb082145` | `f0f262d8d2d8c36987294d85d29bf49f84719670d58db23a913613033c4a0fe6` |
| darwin-arm64 | `6b87dcdde0977447d6aca5667c9d018ec25e26d45793f7030a53f9c68b5cb291` | `84afa9c8bcf339f774bcecc787079afce12576a85ffd46c30fd5158f6048e0eb` |
| linux-amd64 | `1009c0d802eaf7447548cdfc2a31da44d191af28ae165683c8e86b8b7fa865ee` | `eaf3d0d8970dd19677c49f60966df0ada6e840e8c09a4ddece499963f9c290c5` |
| linux-arm64 | `a7c1b73cb6aa74c704af3f7ef867994ae3e49a5838416e9cd0d0715bc3139acf` | `bc69450eb9e19356ad064c9b40d05fa9e91d4ee24ae6d22938db31af7ff1f9dc` |
| windows-amd64 | `3c20b57ad6942d98e113223dfc8d6fb061d93dc29ae13c082dc7d34df410dc08` | `159a99a1d2380daab60d421c7f6cad8a81ee63788466dd52333f57b62d61ee5d` |
| windows-arm64 | `86c15bfad79cfd48997354cbfe57302a52e3747f8e750bf3b70a13bd601fea68` | `4beeb845b2a413007150fedac0337a92a6c08360f04931d2258c86fd2b6b34d8` |

OCI index：`sha256:d13c0e17f38b5d032a7d02cf3e1b8d2c7a0030593875f1109dda920707d08f78`。全部14层/platform digest在receipt，
manifest/config如下；准确 `/weir` hash与交叉编译binary一致。

| Platform | Manifest | Config |
| --- | --- | --- |
| linux/amd64 | `sha256:d0fe2b6b40cf816dbcaefe152f866212cce1ef617123479267250725ccf89263` | `sha256:b517c655a610878fd29b4b5e81a2d74467c59a05ce8d939b14d2f9e69cec9063` |
| linux/arm64 | `sha256:2128f104c1a318e20b8384aef87f27ded4f9672432a9a848e0c11e976be67c85` | `sha256:e8d8721453ef0ad73756e34d071bc31a18438734213649251dcafeb093d6fe6e` |

Docker经典store的限制沿用M15证据，不再次用失败load冒充验证；通过Buildx标准
oci-layout context仅转换原arm64分支。转换后原config bytes与每层解压diffID完全相同，
核对后才加载；未重编产品。Darwin binary从新tar.gz提取并核对hash。辅助app.test仅只读
挂载测试，未装进image，现已清理。

预算先写 `budget.json`：2CPU/512MiB memory+swap/96PID、UID/GID65532、read-only root、
dropALL、no-new-privileges、无tmpfs；启动7s、RPC8s、SIGTERM/Wait3s、专项240s。
实际宿主macOS26.6.2/Darwin25.6.0 arm64，Docker Linux7.0.12-linuxkit/aarch64/cgroup-v2。
后端为本次生成证书/账号的Darwin Mongo8.0.32 TLS/SCRAM单直接副本集成员。

| 准确制品专项 | 结果 |
| --- | --- |
| Darwin三轮 | 原archive binary Read/Mutate/Bulk、取消、独立DB readback；SIGTERM/Wait 1.21825 / 1.1005 / 1.128084ms |
| Linux arm64 image三轮 | 原config ID，PID1=/weir，UID65532、CapEff0、NoNewPrivs1、只读；同样业务/取消/readback通过 |
| Linux三轮关闭 | 140.705625 / 124.707458 / 130.128666ms，exit0、OOMKilled=false，原3s界内 |
| Linux正常负载观测 | RSS21712896 / 21741568 / 19673088B；cgroup有限536870912B，valid且unknown=0；这不是容量/soak证明 |
| 真实TLS | 本次CA正向成功；错误CA和hostname均在serving前拒绝，无关闭验证 |
| 无重放 | 一次真实后端update确认后代理丢回复，准确image返回UNKNOWN；wire update=1、DB effect=1、proxy sockets=0 |

这是**Linux Weir + Darwin Mongo混合环境**，不冒充Linux原生Mongo。当前Linux Mongo内核
启动阻塞保留。系统root正向、Search在准确新image上的完整业务矩阵、其他四目标native
和多节点/生产拓扑没有新增资格。

## 回归与失败记录

`validation.json` / `artifact-validation.json` 保存完整命令、环境增量、exit和时间，
`verify.py` / `verify-artifact.py` 可查看确切selectors。全程保留原断言和预算。

| 项目 | 结果/耗时 | 日志 |
| --- | --- | --- |
| 全仓default test / race | PASS 73.844s / 78.653s | default-test.log / default-race.log |
| default / integration / Linux arm64 integration vet | PASS | vet.log / vet-integration.log / vet-linux.log |
| CLI/config/Bulk/Native/Scan/peer/diagnostics/admission/TLS边界race×3 | PASS 163.096s | boundaries-race3.log |
| 全overload race×3 | PASS 3.625s | profile-race3.log |
| package / M14 fixture离线Python测试 | 5项 / 7项PASS | package-offline.log / fixture-offline.log |
| 真实Mongo TLS/SCRAM/OCSP/wire/391/RMW/关闭 | PASS 138.190s | mongo-tls.log |
| TLS Mongo streaming/native/scan/取消/半关闭/drain/公开expression | PASS 36.799s | tls-stream.log；未启用Search子分支如实SKIP |
| TLS app装配/metrics/SIGTERM | PASS 18.106s | tls-app.log |
| Elasticsearch HTTPS与完整app装配 | PASS 93.416s | es-https.log |
| OpenSearch HTTPS与完整app装配 | PASS 80.159s | os-https.log |
| M14R Linux Guard和CLI各三轮 | PASS 13.233s | linux-native.log及`.testdata/weir-m14-7d22fc0de162/` |
| 准确制品完整专项 | PASS；race runner24.532s | packaged-native.log |
| Darwin准确制品额外两轮 | PASS；5.176s / 4.656s | packaged-darwin-second.log / packaged-darwin-third.log |

M14R仍覆盖高/中/低位、压力中拒绝/排空/恢复/SIGTERM，proxy sockets=0、每轮3个子PID
退出，memory.events max/oom/oom_kill=0。新补丁未放宽水位或资源预算；正常image观测
与M14压力fixture证据分别记录，不互相替代。测试使用新Go和新依赖；此后只有文档变化。

保留的非成功尝试与覆盖限制：

- Syft首次官方archive下载300s超时；日志保留，续传完成且核对完整官方SHA后才执行。
- govulncheck `-scan module ./...`不支持pattern；根目录无Go包的`-scan module`也失败。
  两次stderr和exit均保留。改用官方`-mode query`逐模块候选查询，加Syft/Grype完整图，
  不把失败或query模式当source reachability通过。
- 新图完整性核对发现尚缺两份新module路径的历史OSV，先失败闭合，补齐并记录单独scope
  receipt后继续；没有省略记录或将缺失当无漏洞。
- 原M14/M15失败历史保留。M16产品构建/测试没有失败后删断言、扩预算或多轮补丁试绿。
  Go1.27.1和grpc修复仅一轮；无需要第二轮的已证实产品可达High/Critical。

## 清理与剩余门槛

`cleanup-final.json`核对本轮95个新fixture目录、6个owner；Mongo只剩owner/日志（包括
轮转日志），Search只剩owner/日志，准确制品fixture只剩owner/日志；生成的配置、CA、
私钥、keyfile、DB数据已由fixture清理。M14 cleanup为all_stopped=true、errors=[]。
所有对应owner的container/network为空，宿主无mongod/weir/app.test/overload.test进程。

自有builder `weir-m16-01a0e3b9`的image/name先核对，随后只删除其container和唯一状态卷；
临时转换Dockerfile与app.test已删除。两套正式构建的临时源码/cache已自动回收。
准确产品image、归档、扫描用白名单source、公开工具/DB、日志和receipt保留；没有全局
prune或删除未知资源。最终提交后核对clean main，`submission.json`记录最终SHA/制品SHA，
通知统筹后停止checkout写入/测试。

| 门槛 | M16后状态 |
| --- | --- |
| 六目标构建/重复构建、两架构OCI内容一致 | 本地PASS；SHA绑定完善 |
| 标准SBOM/schema、冻结库漏洞扫描、限定triage | 本地完成；仍有明确未链接包匹配，待统筹独立验收 |
| Linux/Darwin arm64有限准确制品运行 | PASS；上述混合环境及边界 |
| Linux amd64、Darwin amd64、Windows双架构原生/资源/生命周期 | 未验证；不是qualified |
| 外部Search安全基线 | Elasticsearch8.17.0、OpenSearch2.19.0存在明确生产阻塞，需独立后端patch资格 |
| 发布身份/签名 | unsigned政策已说明；真实发布签名/验证流程未建立，SHA256及Darwin ad-hoc签名不替代它 |
| 完整OCI/Kubernetes、参考负载容量、持续负载、全OS内存/生产拓扑/独立安全审查 | 仍required；不因本阶段通过而关闭 |
| 整体生产qualified | **否** |
