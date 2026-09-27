# M17：Search 固定版本更新与有限重新资格

2026-09-28（Asia/Shanghai）。基线 `f20cff8965a3e38f74a79e5b629022ef5b6c2599`。
M16 已获统筹有限独立验收；该验收只接受准确 Weir CLI 中未链接 SSH/OpenPGP 包的
三个模块匹配，不豁免外部 Search 风险。本阶段只更新本地代码和自有空白 fixture，
未升级用户数据库。整体 `production qualified=false`。

## 选择与门槛

活跃 profile 仅接受 **elasticsearch-8.19.22 / opensearch-2.19.6**，严格核对实际版本和
发行版；旧 8.17.0/2.19.0 与未验证 patch 均拒绝，无别名、fallback 或版本协商。
操作语义资格与服务器安全资格分开。OS 官方 2.19.6 镜像的 JDK/插件仍有未排除的
High/Critical 候选，**OS 生产安全门槛仍阻塞**，交统筹决定有限补救，不擅自替换 JDK、
裁剪插件、跳 major 或生成自制后端镜像。

- [ES 官方 v8.19.22 release](https://github.com/elastic/elasticsearch/releases/tag/v8.19.22)
  已发布于 2026-09-23T15:20:20Z，非 draft/prerelease；实际官方镜像已匿名取得并核验。
  [8.19 文档](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/release-notes-8.19.22.html)
  仍显示 Coming in，不能单凭这处文字判定未发布；release API 与实际 registry 制品分别留证。
  [维护政策](https://www.elastic.co/support/eol)确认 8.19 是最终 8.x minor，维护至 2027-01-15，
  support 至 2027-07-15；旧 8.17 已结束维护。
- [OS 官方 2.19.6 release](https://github.com/opensearch-project/OpenSearch/releases/tag/2.19.6)
  API published_at 为 2026-07-06T18:22:45Z；[发布历史](https://opensearch.org/releases/)
  标注 July 2，两者分别记录。2.19 维护至 4.0 GA。2.19.7 仅计划 2026-10-20，
  本次查询没有已发布制品依据，不冒充可用安全补丁。

`.testdata/m17-evidence/official/` 保留原始官方响应、URL、时间和 hash；
`dist/m17-backends/` 保留准确官方 linux/arm64 镜像导出及标准工具扫描。
实际运行均为 native arm64；另一架构仅取得 registry metadata，未运行或模拟。

## 已登记风险逐项复核

| 风险 | 官方修复依据与本轮判断 |
| --- | --- |
| ES CVE-2025-66566，High8.4 | [ESA-2026-07](https://discuss.elastic.co/t/elasticsearch-8-19-10-9-1-10-9-2-4-security-update-esa-2026-07/384525)：8.x fix8.19.10，8.19.22 已覆盖。Weir 使用 REST，不使用 node transport/LZ4；这不是对部署网络风险的豁免。 |
| ES CVE-2026-94408，Medium4.9 | [ESA-2026-184](https://discuss.elastic.co/t/elasticsearch-8-19-22-9-4-7-9-5-3-security-update-esa-2026-184/390688)：fix8.19.22，覆盖。所有配置受影响，不能因未配置某插件排除。 |
| OS CVE-2025-9624 / GHSA-mw3v-mmfw-3x2g，High | [2.19.4](https://github.com/opensearch-project/OpenSearch/releases/tag/2.19.4)及[PR19491](https://github.com/opensearch-project/OpenSearch/pull/19491)修复 query_string DoS，2.19.6 覆盖。Native/Scan可转发查询，Weir body上限不是查询复杂度安全证明。 |
| OS 条件 FLS / masking，Moderate | [FLS](https://github.com/opensearch-project/security/security/advisories/GHSA-2rjv-cv85-xhgm)、[masking](https://github.com/opensearch-project/security/security/advisories/GHSA-rrmm-wq7q-h4v5)均fix2.19.3，2.19.6覆盖。fixture未配置该能力，未冒称运行其exploit。 |

安全边界测试只向本轮自有空白后端发小输入：ES min_hash hash_count10001 在构造前返回400，
固定上限10000；OS 临时将 query_string 长度设32，33字符返回400，cleanup恢复null。
不向旧实例发OOM攻击，不声称复现上述漏洞，产品不修改数据库限制。

旧里程碑仍保留其原始版本和失败；本轮全部新证据绑定新后端与以下准确制品。

## 实际后端指纹

来源为 `docker.elastic.co/elasticsearch/elasticsearch:8.19.22` 和
`docker.io/opensearchproject/opensearch:2.19.6`。tag 仅用于首次定位官方 index；fixture
固定以下 **arm64 manifest**，`--pull=never --platform=linux/arm64`，先核对版本/发行版/cluster，
再运行测试。secure fixture 额外保存 root `version.json`、实际 config ID 与过滤后的
`/_nodes/jvm,plugins` → `runtime.json`；内容不含凭据、证书私钥或用户数据。

| 指纹 | Elasticsearch 8.19.22 | OpenSearch 2.19.6 |
| --- | --- | --- |
| index | `sha256:e98f9c3b09beb2fbb9eaf667d602df3f0e00bd3644138b8458dc17ba1a675595` | `sha256:e321cb03c643874c42240458a618db56bc1e4fc143b10c41bec653542bc6851b` |
| native arm64 manifest | `sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237` | `sha256:89a402aa9132286200b8d12aa37fd5b14daa65851193d009355900c0d1d9d59c` |
| native arm64 config | `sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160` | `sha256:40cd51ca20c5ef576ef15a4476839cba1b60c0746d703f6bdc17a7ceadbe5189` |
| actual build hash | `3b2a41103de35e0af4064d647974032fcc1bcde9` | `97d3c13bf22a4a72ac11dc503fe44c97662b9161` |
| bundled JDK | Oracle `27+35-2325` | Temurin `21.0.11+10-LTS` |
| actual base | Ubuntu24.04.5 LTS | AmazonLinux2023.12.20260914 |
| amd64 manifest（仅metadata） | `sha256:f1e3d88299cb0db2131fc2ab9060faefccd88fd684d11a49256ae00d7dc3c857` | `sha256:b5facc7814bd250c11afb939bb11611f40088e2436d90f2cbdcd6d9e0b47e8d1` |
| amd64 config（仅metadata） | `sha256:5d068400eb33be90f060467b36ba31612ae488d928672a2a83b78abf334283b1` | `sha256:6e8d14afe27e30febaaa8d2e19ccff36e0fb8631b32a6c51f07a132084354308` |

镜像保存归档、原始 registry index/manifest 和 Docker inspect 分开留证；receipt 记录全部层hash。
未读取官方镜像中的 demo 私钥或任何既有 secret；组件核对只读取白名单 JAR class inventory、
JDK release、plugin descriptor、os-release。镜像有包不等于应用实际加载每个包。

## 新公告与组件安全限制

ES 当前 Medium 公告 [ESA-2026-183](https://discuss.elastic.co/t/390687)、
[180](https://discuss.elastic.co/t/390684)、[179](https://discuss.elastic.co/t/390683)、
[176](https://discuss.elastic.co/t/390682) 均由8.19.22覆盖；
[170](https://discuss.elastic.co/t/390681)由8.19.21覆盖；
[182](https://discuss.elastic.co/t/390686)仅影响9.x。官方当前 known-issues 的9.x条目不外推为8.x缺陷。
OS 当前 hostname verification [GHSA-x5hg-x4gv-j98m](https://github.com/opensearch-project/security/security/advisories/GHSA-x5hg-x4gv-j98m)、
DLS parent/child [GHSA-x83w-23jp-g6pw](https://github.com/opensearch-project/security/security/advisories/GHSA-x83w-23jp-g6pw)、
rollover [GHSA-22vx-2x23-98w6](https://github.com/opensearch-project/security/security/advisories/GHSA-22vx-2x23-98w6)
均由2.19.4覆盖。上述复核不是穷尽服务器及其所有插件的安全证明。

两个 backend image 使用 M16 固定 Syft1.52.0 / Grype0.119.0 和新鲜冻结库，0条ignore。
原始匹配数包含同一漏洞在不同嵌套位置/包的重复，不是可利用漏洞数量：

| native backend image | catalog | 原始 Grype 结果 |
| --- | --- | --- |
| ES | 706 Java、115 deb、1 binary | 78：70Medium、6Low、2Negligible；0High/Critical |
| OS | 810 Java、111 rpm、3Python、1binary | 187：14Critical、64High、106Medium、3Low |

ES 的 commons-lang3、log4j-api、jsoup、jline 和 Ubuntu 中低危候选保持原报告；没有凭包版本
自行声称 backport、全部可达或全部安全。M16登记的具体ES版本阻断已被官方修复覆盖，
这不等于所有剩余生产门槛解除。

OS 主 Netty 是4.1.135，但 security-analytics shaded JAR 中仍有4.1.130元数据及实际2490个
Netty class、Jackson2.17.1与996个class；其他插件也有Jackson2.14.1/2.18.2、httpcore5
5.3.1/5.3.4、BC1.79/1.84/bc-fips2.1.2。不能将它们一律说成旧元数据误报。
`opensearch-package-content.json`、`os-high-candidates.json`、`backend-triage.json`保存路径、
嵌套包/类、上游版本范围和保守处置；没有修改扫描器原输出。

| 重要 OS 候选 | 分层判断 |
| --- | --- |
| CVE-2026-47063，High7.5 | [OpenJDK July advisory](https://openjdk.org/groups/vulnerability/advisories/2026-07-21)的安全库修复要求21.0.12；实际运行21.0.11在受影响范围。JVM为真实运行依赖，相关功能暴露未被排除，**阻断**。同表CVE-2026-41254的2D路径有额外条件，不冒称已有利用。 |
| CVE-2026-13506 / GHSA-qp49-qgx5-5m26 | [BC官方ASN.1公告](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%9013506)：lazy depth检查绕过，fix1.85 / bc-fips2.1.3；对应类实际存在，TLS/证书相关暴露未排除，**阻断**。 |
| CVE-2026-8763 / GHSA-9pwp-9qqc-pr26 | [BC官方name-constraints公告](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%908763)：有条件，未证明应用路径已排除；保持候选。 |
| GHSA-c4c3-7fpv-j4q5 Netty SNI | [上游公告](https://github.com/netty/netty/security/advisories/GHSA-c4c3-7fpv-j4q5)需per-context SNI/mTLS fallback条件；扫描器Critical、上游High分开记录。本fixture为单证书Basic，无此配置；只对此测试配置标注条件未启用，不全局ignore。 |
| GHSA-v6w3-qrh8-qccc BCFIPS entropy | Intel特定条件不适用于本次arm64；不能据此豁免未运行的amd64。 |
| 其他 HTTP2/SPDY/压缩/解析候选 | 实际嵌套类不等于实际外部入口；也没有足够证据排除全部High/Critical。保留待有限调查的阻断候选，不将所有匹配写成已确认exploit。 |

OS2.19.6已解决M16登记的旧patch漏洞，但不能因此通过当前官方image的生产安全门槛。
没有发布更高2.x patch的证据，替换JDK/裁剪插件将产生新发行物和新资格范围，留给统筹决策。
不无限扩成第三方修复工程。

外部CDX schema限制：OS原始CycloneDX1.6校验0错误；ES有**2错误**，Syft输出的
`SMAIL-GPL` / `Artistic-dist` SPDX ID不在冻结官方1.6 schema枚举中。原始CDX/Syft及扫描均保留，
没有手改license、换成宽松schema或把失败算通过。Weir交付自身的8份CDX单独验证和记录。


## 准确 Weir 制品与内容核验

最终实现/制品 source **`1bb93fd32e18eb79ba25802809eddfe877b28a4f`**；后续收尾提交仅更改文档，
不改变构建输入。Go1.27.1、gRPC1.83.2、完整93个版本化模块和 distroless base 全部沿用M16，
没有新增依赖或顺手升级 Mongo。直接从干净本地 main 调用原唯一入口：

```sh
PATH="$PWD/.tools/go1.27.1/bin:$PATH" \
BUILDX_CONFIG="$PWD/.testdata/m17-evidence/docker/buildx" \
DOCKER_HOST=unix:///var/run/docker.sock PYTHONDONTWRITEBYTECODE=1 \
  python3 scripts/package.py --output dist/m17 --oci --builder weir-m17-01a0e3f1
```

固定 BuildKit v0.32.2 / Buildx v0.36.1-desktop.1、自有 builder、空 Docker 客户端配置。
两次不同 Git blob 导出、空 GOCACHE，Go离线、固定CGO/CPU基线/epoch、OCI无RUN或QEMU，
137.302秒完成。每套21个SHA256SUMS条目均再次验证；两套receipt相同：
`58250f3da8c375df7d3bed47b155acea02f4a88d049208cadb74c714f11b686d`。

| target | binary SHA256 | archive SHA256（双轮相同） |
| --- | --- | --- |
| darwin-amd64 | `1d5498665d7567dc8dab265eebeae0df7f6e09e69901d8084e20ad93f62f7b7e` | `bd81458a1df024603c7136b07e49815853965e56c40dc9f2aa80ef1e7977be24` |
| darwin-arm64 | `d3c1e0da522fcbeabd173b2432c157e0303f638acf78d9b4e76613a6f75e2752` | `5c99684fca36d19f720baa598851a020813215a0bdbe5d382d6222198925cf8f` |
| linux-amd64 | `09459a0c2934e0bf3259fb385c39dade5a4c7f840100ae9f441b4bd5744edda3` | `f69b9aed01f6aa8fa9426eed4b76ee89306080421515880f54f44da961a1a617` |
| linux-arm64 | `e852d44768c8ae252ac273e90e274728378af273bc0d36b120fcb273bf6c278b` | `a180e9cfeca51426c3055e58aa2402653e7166d77c30cfeaaaca43b35c70bcf7` |
| windows-amd64 | `4529c0248ce64beb3a5db2b66b62356b8227192f3211cf3b8cf1cd0e75c6f422` | `0ffcc012232513fd8eee29b41e8047e48d2ed116cf6e12d86958a382160e9421` |
| windows-arm64 | `1b768ec1ae52ba3f4ed60f31909f713cab796b29b42637af7777dda9e4e209ea` | `2a7310926bf9217bea11c0aec758f6dbd64f9c8e81672d1f63817abb93e3e7a1` |

OCI index：`sha256:7fa8f720e5c293cc810bf8422fea406c1b5cac7b41d99d52997f5f80d1c058d5`。

| platform | manifest | config |
| --- | --- | --- |
| linux/amd64 | `sha256:a1055e88906ebb17f4eaccd06c3f6ba0b4cda51ec2cbc08f6beab23842ad5bbc` | `sha256:0f493b259aae9f3e38bd266f4a2d0ec0a562e475052fa1fa3fe05a8328e56267` |
| linux/arm64 | `sha256:c6e08e6ca7dfd8ab8d3741a4369f3420b59770ce18ca5f3acdf25d0965040142` | `sha256:9512dbf5362c6e6eaa847cb6fdf2743fa875d4d99bb59f412f00b077c8537082` |

`dist/m17/first/`、`second/`保留全部制品与receipt；`audit.json`逐blob、config、rootfs diffID、
准确 `/weir`、CA bundle和dpkg核验。Linux各21个实际依赖，Darwin/Windows各20个，
另有stdlib/main；与标准Syft逐项一致，无实验Lua链接。两架构仍为Debian13.7、原6个dpkg包，
CA bundle仍224449B / `714d457d580922dbf1d0be8bd35ba236a842b50b0072ae791582a19adef772a5`。

`dist/m17/native-load/`保存标准Buildx转存：只选原OCI arm64分支，config bytes和每层
解压diffID与原OCI相同，再加载Docker经典store；没有重编产品。Darwin binary直接取自准确归档。
image probe仅作为独立只读挂载的测试helper，不进入产品image，完成后清理。

## 增量 SBOM 与漏洞复验

`dist/m17-scans/after/`的8份新CycloneDX1.6、8份Syft补充输出、8份Grype JSON均直接由新
6个binary和2个OCI生成；不是复制旧SBOM改SHA。全部8份官方schema校验0错误，和上文外部
ES清单的2个schema错误分别记录。另运行6目标source函数级与6份binary符号级govulncheck。
`input-receipt.json`把临时Go-only Git blob导出、原制品/工具链/target与hash绑定。

复用M16已验证的Syft1.52.0、Grype0.119.0、govulncheck1.8.0，执行文件重新hash校验。
Grype DB v6.1.9 build `2026-09-27T06:30:30Z`，SHA256
`24b7c0db32df3a3d2417564662ab7f4745fc9896fe98675ba2b00aafcb38726c`；默认120h内，
valid=true。Go DB最新index与冻结bytes相同，modified `2026-09-24T20:07:49Z`，
SHA256 `aad6cfbb50e7815d64ca43b7c360136cd3fee83261978b1418016a7f3346974f`。
本次不需更新数据库；后续过期仍须刷新，不把离线等同新鲜。

全部扫描命令exit0；JSON exit0并不意味着无漏洞。每份Grype仍3条（2High/1Unknown）、
ignoredMatches=0；每份Go报告仍GO-2026-5932/6354/6355三个模块匹配，**0包级/函数或符号级**。
SSH/OpenPGP仍未链接，适用范围沿用M16限定判定；不是全局ignore，改变导入/配置须重审。
完整模块图和base字节未变，未重复调查无变更实验依赖；receipt和实际binary/main source元数据
改变是新提交的正常差异。`comparison-to-m16.json`逐项比对，无新增CLI/OCI漏洞候选。

## 原语义与资源对照

产品变化仅精确版本常量/分支和默认值；Core不解析JSON/body，记录identity、单index目标、
别名/模板/default或final ingest pipeline的既有拒绝仍有效。Native保留原生escape hatch，
不等同记录API的目标/identity限制。BackendExpression继续使用后端OCC/原生原子能力；
没有隐藏metadata、通用Query/Count语言或脚本runtime。ProgramTransform仍UNSUPPORTED、V1延期；
Weir自身认证未恢复，后端standard TLS/Basic不关闭验证。

所有测试使用新空白数据，secure fixture按owner生成新CA/账号/配置，不沿用旧fixture存储。
`verify.py` / `validation.json`、`verify-extra.py` / `extra-validation.json`保留确切命令和环境。
Go固定本地1.27.1、GOENV=off/GOTOOLCHAIN=local/GOWORK=off/GOPROXY=off/GOSUMDB=off。
默认测试未因profile升级自动启动或连接外部数据库。

| 检查 | 结果与日志 |
| --- | --- |
| 完整default test / race | PASS，62.310 / 65.691s；default-test.log、default-race.log |
| default / integration / Linux arm64 integration vet | 全PASS；vet*.log；最终fixture vet也PASS |
| URI/config/peer/连接/取消/资源/Native/Scan/Bulk边界race×3 | 249项顶层、702项含子测试PASS，0SKIP/FAIL；profile-race3.log，163.485s |
| 完整Search包race×3 | PASS，35.430s；search-complete-race3.log，未只选新增版本测试 |
| ES/OS完整adapter package（race+integration） | 各51项顶层、193项含子测试PASS；22.741 / 20.973s；各2个未启用secure测试如实SKIP，随后专门HTTPS运行覆盖 |
| ES/OS stream/Native/Scan/peer/公开expression | 各19项顶层、55项含子测试PASS；16.368 / 14.545s，0SKIP/FAIL；*-streams.log。仅计实际执行子测试，不把regex未选分支算覆盖 |
| ES/OS普通Bulk/双Store/慢读背压/过载 | 各4项顶层PASS，14.116 / 14.237s；*-bulk.log |
| ES/OS提前响应与半关闭 | 各2项顶层、6项含子测试PASS，5.566 / 3.157s；*-early-response.log |
| ES/OS verified HTTPS及app direct/peer全部操作 | 各3项顶层、34项含子测试PASS，90.673 / 74.897s，无skip；*-https.log |
| ES/OS M12R进程预算race×3 | 各3轮PASS，90.588 / 103.666s；*-budget-race3.log |
| Mongo有界冒烟 | TLS/SCRAM生产Open：8.899s；普通ACK丢回复：4.863s；mongo-smoke.log、mongo-ack-smoke.log |
| 原打包Python离线测试 | 5项PASS，0.553s；package-offline.log |

adapter完整测试涵盖CRUD/missing/逐项错误、单key顺序、OCC竞争/重算/Delete-recreate、
记录identity/目标拒绝、Native真实响应/丢回复、PIT和逐页Scan/partial shard/取消/Close。
原测试断言未删除，未将版本特有解析失败降为成功；`test-summary.json`保存逐命令pass/skip计数。
secure测试涵盖CA/hostname/必要凭据正反、DNS/连接owner/Close、一派发不重放；缺系统root
正向资格仍登记，不能把显式测试CA成功外推。

M12R C=1/2/4的独立进程、3→4→3 replacement及replacement两Local分别在新ES/OS重跑。
继续记录本地硬界：每Local in-flight≤C、transport owned≤C+1；丢ACK返回UNKNOWN且独立
后端版本/派发计数为1；queued取消不落库。各轮结束所有Wait完成、proxy sockets=0、
连接账本acquired=released。远端tail依赖后端/网络条件，不宣称replicas×pool是无限网络
故障下DB端绝对连接上限。原并发、连接和关闭断言没有为了通过而放宽。


## 准确制品原生运行

本机 macOS26.6.2/Darwin25.6.0 arm64，Docker Linux7.0.12-linuxkit/aarch64/cgroup-v2。
`verify-artifact-verified.py` / `artifact-verified-validation.json`各后端三轮：新自有verified HTTPS
后端、准确Darwin归档、新原始config ID的Linux arm64 image。每轮主测试及所有子项PASS、无skip。

| 后端/轮次 | host race runner | Darwin SIGTERM/Wait | Linux SIGTERM/Wait | Linux RSS bytes |
| --- | --- | --- | --- | --- |
| elasticsearch / 1 | 29.089s | 5.467583ms | 190.131542ms | 21442560 |
| elasticsearch / 2 | 28.985s | 5.446125ms | 187.334209ms | 21426176 |
| elasticsearch / 3 | 28.744s | 5.777875ms | 192.462458ms | 21417984 |
| opensearch / 1 | 32.978s | 4.980292ms | 153.909667ms | 23523328 |
| opensearch / 2 | 33.206s | 4.594625ms | 159.395583ms | 23506944 |
| opensearch / 3 | 33.293s | 5.548416ms | 176.030917ms | 23506944 |

每轮Read/missing、Put/Create/Replace/Delete/conflict、Bulk关联End/EOF、BackendExpression精确
int64、Scan逐页End/EOF、Native bulk End/EOF及取消均使用实际产品；复用原app operations断言，
RPC预算8s。普通和Native分别在后端确认后由代理丢响应：ordinary返回UNKNOWN，Native返回
RESPONSE_INCOMPLETE与Failure；独立backend `_version=1`、dispatch/applied/dropped各1，无重放。

Linux启动维持7s上限；SIGTERM/Wait均低于原3s，exit0/OOMKilled=false，proxy sockets=0。
PID1=/weir、UID/GID65532、readonly root、CapEff0、NoNewPrivs1、2CPU、memory+swap512MiB、
96PID、无tmpfs。独立probe报告RSS/cgroup有效、limit536870912、unknown0；这是短时正常负载，
不是容量/压力/长期资源资格。镜像内未放helper或测试工具。

每轮Linux准确image分别拒绝坏CA、hostname、必要凭据、旧profile和错误产品，exit1且未serving，
日志不含本次password。正确连接用显式新CA和标准hostname验证。未把metadata检查或Go test
runner当产品native运行；也未将另一架构交叉构建当native资格。

## 失败、限制与交付边界

- ES config blob匿名HTTP首次重定向请求400；随后以官方manifest绑定config digest，并核对实际
  docker-save内config bytes；没有把失败获取算验证成功。
- 匿名GitHub advisory API达到限流后403，原始URL/错误留在image-advisory-sources.json；关键
  JDK/BC/Netty项另取primary页面，未用用户凭据绕过。BC普通连字符wiki URL跳转首页，改用官方
  Unicode连字符canonical URL；首页响应不作为具体漏洞依据。
- 外部schema初次使用不存在venv路径exit127，随后正确固定venv实际执行；ES两个SPDX错误
  仍失败，OS与8份Weir清单分开记录，不把“9/10通过”说成全部通过。
- 最初Mongo组合selector中的第二个名称不存在；该命令只覆盖生产TLS Open。另以准确
  `TestAcknowledgedOrdinaryBatchReplyLostIsNotReplayed`专项通过，不靠不存在的selector声称覆盖。
- 第一轮ES准确产品测试原始日志为PASS且owner已清理，但临时证据脚本把循环变量命名round，
  覆盖Python内置函数，写耗时收据时TypeError。原脚本/日志保留；修正为iteration，以新日志名
  完整重跑两后端各三轮，全部有exit/time/source收据。未修改产品、制品、预算或断言。
- 本轮自有单节点、零replica、短时本机证据仅重获上述有限操作范围。ES旧已登记版本漏洞已修复；
  OS bundled JDK/插件阻断未解决。两后端系统roots正向、多节点复制切换、长期网络故障、
  Kubernetes多worker、容量基准/至少24h soak仍缺；其余四格native平台和Mac/Windows完整
  OS资源资格未补。Linux Mongo旧内核启动阻塞保留，Mongo本轮仅Darwin有界冒烟。
- unsigned本地产物不是组织签名/公证或可信发布证明。ProgramTransform继续V1延期且UNSUPPORTED；
  用户排除Weir认证的边界不变。整体production仍false，M17待统筹有限独立验收。


## 清理与本地交付

`.testdata/m17-evidence/cleanup-final.json`逐个核对40个新增fixture目录、10个owner：
本次CA/私钥/临时配置/data由各fixture owner清理，仅保留owner、脱敏日志和刻意记录的
version/runtime指纹。对应container/network均不存在；专用BuildKit builder及其精确state volume
已回收。独立app.test、临时Go-only导出与新扫描cache已移除，两个包装导出目录已不存在；
无本次Weir/Mongo/helper进程。失败首轮同样已清理。未知资源、旧工作和旧证据未删除，无全局prune。

保留 `dist/m17/`、`dist/m17-scans/`、`dist/m17-backends/`、公开工具/DB及
`.testdata/m17-evidence/`的原始命令、失败、source/hash receipt。`evidence-index.json`记录
交付和报告hash，仍不构成可信发布者签名。所有改动只提交本地main，未push/PR/tag/release、
部署、付费、读取既有秘密或修改宿主工具链/daemon。完成本地收尾后停止checkout写入和测试，
按授权回调统筹；不自行开启下一阶段或定时任务。
