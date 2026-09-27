# M18：OpenSearch 组件风险边界与有限上游路线

2026-09-28（Asia/Shanghai）。基线 `2d4a76c4d96a02cde8bcb5f194d4410d01e27369`。

**结论：部分入口条件已排除，其余仍 blocked；不接受 OS2.19.6 的生产安全资格，也不推荐立即切换到 3.8.0。**
没有证实本次列出的漏洞已可经 Weir 利用。运行中的 JDK21.0.11 符合
CVE-2026-47063 版本范围，但公开资料不足以定位危险方法并排除当前调用；这是仍成立的
安全证据阻断。插件出站客户端/可选功能的若干条件亦未完成证明，不能用一次 classloader
快照将整个官方发行版放行。以下结论仅针对固定制品及明确配置，不要求扫描匹配数清零。

[M17](milestone-17.md)已由统筹完成**有限独立验收**：功能、HTTPS、UNKNOWN/无重放、
进程预算及准确 arm64 制品通过；OS 安全门槛未通过。准确 Weir 制品 source 仍是
`1bb93fd32e18eb79ba25802809eddfe877b28a4f`，M18 未重建/更名这些产物。
用户排除 Weir 认证、通用 ProgramTransform 首版延期均不变；整体 `production qualified=false`。

## 调查范围与证据身份

工作仅在本地 main；开始时基线一致、工作树干净，没有运行中的前任测试。调查预算上限约
90 分钟，未启动其他阶段。复用 M17 的完整原始扫描及统筹审计，不重跑六平台构建。

| 对象 | 固定身份 |
| --- | --- |
| 当前 OS index | `sha256:e321cb03c643874c42240458a618db56bc1e4fc143b10c41bec653542bc6851b` |
| 当前 linux/arm64 manifest | `sha256:89a402aa9132286200b8d12aa37fd5b14daa65851193d009355900c0d1d9d59c` |
| 当前 config | `sha256:40cd51ca20c5ef576ef15a4476839cba1b60c0746d703f6bdc17a7ceadbe5189` |
| 实际 core build/source | `97d3c13bf22a4a72ac11dc503fe44c97662b9161` |
| security 2.19.6.0 tag/source | `62c49afae03e12e348b5efcfbf90d44624b8e364` |
| security-analytics 2.19.6.0 tag/source | `aaba5111d7b540a56f45da86ceed3ac30578d683` |
| 本轮原生 fixture | Linux7.0.12-linuxkit/aarch64，2 CPU、1536 MiB，JDK `21.0.11+10-LTS` |

`dist/m17-backends/opensearch.grype.json` 原187条中 High64/Critical14，按公告为28项，
按公告+包+版本为46组，仍保留**全部78个位置匹配**。它们不是78个可利用漏洞。
[完整位置/版本/别名/修复范围/文件 SHA256 索引](milestone-18-findings.json)随仓库提交；
每个位置都从原报告逐项导出，没有只保留同版本的第一个 JAR。
原报告、原 Syft/CDX、M17 triage 不改写，不新增 ignore/VEX。

M18 大证据在 `.testdata/m18-evidence/`；候选在 `dist/m18-candidate/`。
`.testdata/m18-evidence/evidence-index.json`绑定源码响应、探针、扫描与复用输入的 hash；本文给出仓库相对位置和
固定上游链接，因此不依赖某一台机器的绝对路径。官方 tag 源码用于解释调用，镜像 JAR
内容另有 hash/类核对；没有声称完成第三方 JAR 可复现构建。

## 实际输入面

- Record Read/CRUD/Bulk 和有限 `{"doc":{...}}` expression；后者不接收脚本、upsert、任意
  HTTP 路由。见 [expression.go](../internal/backend/search/expression.go)。
- Native 仅具体 index 的 `GET /_doc/<id>`、`POST /_bulk`；后者只允许 index/create/delete，
  拒绝额外操作、跨 index、任意 header、压缩 header及默认/final ingest pipeline。
  见 [native.go](../internal/backend/search/native.go)的 `nativeDescriptor`、`item`、`ExecuteNative`。
- **Scan 可转发 selector 中的原生 `query` 对象**，PIT/分页/外层字段由 adapter 构造；不是任意
  HTTP API，但也不是只有 `match_all`。见 [scan.go](../internal/backend/search/scan.go):31–126。
  M17 关于“Native/Scan可转发查询”的笼统说法在此细化：原生 query 在 Scan。
  不因调查把它暗改为查询白名单；启用插件查询/模型的部署仍需单独判断。
- Go 两条后端连接均固定 HTTP/1，禁止自动压缩、重定向及隐式 mutation 重放；应用数据不能
  任意构造 TLS record、HTTP framing 或请求压缩编码。JSON 深度/字节限制是资源边界，
  **不是**脚本、查询复杂度或第三方解析器安全证明。
- 后端自身9200/transport/插件端口、运维配置、其他直接客户端和未来复制拓扑是另外的入口。
  Weir 的 HTTP 限制不能豁免这些入口；单节点 fixture 也不证明未来多节点安全。

## 加载与调用依据

### E1：当前 REST/transport pipeline

固定 core [HTTP pipeline，366–394](https://github.com/opensearch-project/OpenSearch/blob/97d3c13bf22a4a72ac11dc503fe44c97662b9161/modules/transport-netty4/src/main/java/org/opensearch/http/netty4/Netty4HttpServerTransport.java#L366)
安装 `HttpRequestDecoder`、解压器、聚合器和 pipelining handler；
[HTTPS，163–172](https://github.com/opensearch-project/OpenSearch/blob/97d3c13bf22a4a72ac11dc503fe44c97662b9161/modules/transport-netty4/src/main/java/org/opensearch/http/netty4/ssl/SecureNetty4HttpServerTransport.java#L163)
直接安装一个 `SslHandler(SSLEngine)`。
[transport，142–162](https://github.com/opensearch-project/OpenSearch/blob/97d3c13bf22a4a72ac11dc503fe44c97662b9161/modules/transport-netty4/src/main/java/org/opensearch/transport/netty4/ssl/SecureNetty4Transport.java#L142)
使用 SSL engine/自有 transport decoder；本 fixture dual mode disabled。
[安全插件 SSL context，85–123](https://github.com/opensearch-project/security/blob/62c49afae03e12e348b5efcfbf90d44624b8e364/src/main/java/org/opensearch/security/ssl/SslConfiguration.java#L85)
禁用应用协议协商，没有 per-SNI context 路由。

实际日志证实 HTTP、transport client/server provider 均为 **JDK**。新探针在 put/read 后读取
`jcmd 1 VM.classloaders show-classes=true verbose=true fold=false`：security loader 有669个
Netty类、721个BC类，包含 HTTP decoder、`LazyEncodedSequence`；bootstrap loader有
`sun.security.provider.certpath.PKIXCertPathValidator`。
没有观察到 `SniHandler`、`SslClientHelloHandler`、SPDY、Bzip2、BC PKIX validator、
`X509CRLHolder` 或 HTTP/2 decompressor。**这些负观察只说明本次负载未加载，不单独构成排除证据。**
REST 协议排除依据是上述源码/配置/实际版本的组合。

### E2：证书、CRL、Basic 与 BC

[fixture 源码](../internal/testutil/testsearch/secure.go):188–330 为每次运行生成新的根CA和
server/admin证书；没有中间CA、name constraints、CRL/OCSP地址或CRL文件。HTTP
clientauth为OPTIONAL，Basic内部用户认证；transport使用该CA和节点DN。HTTP OPTIONAL
仍允许直接客户端提交证书，不能解释成“服务器不会解析证书”。Weir自身验证服务端证书使用Go。

[SSLRequestHelper，196–272](https://github.com/opensearch-project/security/blob/62c49afae03e12e348b5efcfbf90d44624b8e364/src/main/java/org/opensearch/security/ssl/util/SSLRequestHelper.java#L196)
的CRL验证默认false，本fixture未启用；开启后才读取本地CRL/启用CRLDP或OCSP。
[TrustStoreConfiguration，60–74](https://github.com/opensearch-project/security/blob/62c49afae03e12e348b5efcfbf90d44624b8e364/src/main/java/org/opensearch/security/ssl/config/TrustStoreConfiguration.java#L60)
使用JDK默认TrustManagerFactory；
[BC注册，2233–2255](https://github.com/opensearch-project/security/blob/62c49afae03e12e348b5efcfbf90d44624b8e364/src/main/java/org/opensearch/security/OpenSearchSecurityPlugin.java#L2233)
是 `Security.addProvider`，不是把BC插到所有JDK provider之前。
BC存在及lazy类加载均不能推出正在使用BC CRL parser或BC PKIX验证。

因此，对**本次固定证书/Basic/默认CRL配置**，13506的恶意CRL来源和8763的BC受约束中间CA
路径条件不成立。不能推广到运维提供的其他CA、CRL、SAML/插件客户端、FIPS或复制节点证书。
本轮没有读取旧secret，也没有打开生成的私钥或密码配置；配置依据来自已提交生成代码。

### E3：插件、版本与 classloader

[PluginsService，709–757](https://github.com/opensearch-project/OpenSearch/blob/97d3c13bf22a4a72ac11dc503fe44c97662b9161/server/src/main/java/org/opensearch/plugins/PluginsService.java#L709)
为插件及其扩展依赖建立loader，不能把主transport的补丁版本替代插件副本。
镜像descriptor与`current-jar-audit.json`给出实际文件；运行插件清单在fixture `runtime.json`。

| 本文缩写 | 实际包位置/版本（根目录 `/usr/share/opensearch/`） |
| --- | --- |
| N130 | `plugins/opensearch-security-analytics/filtered-security-analytics-commons-1.0.0.jar`，未重命名包名的Netty4.1.130；2490个Netty类 |
| N135 | Netty4.1.135各JAR，分别在 `modules/transport-netty4/`、`plugins/opensearch-security/`、`plugins/opensearch-ml/`、`plugins/opensearch-performance-analyzer/` 及该插件下 `performance-analyzer-rca/lib/`；http2匹配只在ML/PA/RCA三处 |
| J14/J17/J18 | databind2.14.1分别在observability/reports-scheduler；2.17.1在N130同一个JAR；2.18.2在anomaly-detection |
| B179/B184 | bcprov-jdk15to18 1.79在RCA/lib，1.84在PA插件根；bcprov-jdk18on 1.84分别在security/flow-framework/sql |
| F212 | bc-fips2.1.2分别在 `lib/tools/plugin-cli/` 和 `plugins/opensearch-ml/`；两份hash相同但用途/loader不同 |
| H531/H534 | httpcore5 5.3.1及httpcore5-h2 5.3.1在ML；httpcore5 5.3.4在flow-framework |
| S/JL | skills的`spark-core_2.13-3.5.8.jar`包含重定位后的`org/sparkproject/jetty/...`9.4.58；ML的jline-builtins3.27.1 |

N130 JAR hash为 `4ca4979dd5a3f701e986a7dec8fa52969ec3ccea9b2ff0b2a38a5a8b8fe18a6d`。
实际bytecode引用包含AWS `ChannelPipelineInitializer` → `Http2FrameCodec`，所以不能因入口是
HTTP/1就声称所有HTTP/2类都是元数据误报。
[SA的S3 feed factory，36–58](https://github.com/opensearch-project/security-analytics/blob/aaba5111d7b540a56f45da86ceed3ac30578d683/src/main/java/org/opensearch/securityanalytics/services/STIX2IOCConnectorFactory.java#L36)
使用配置的S3 feed；[SA查询注册](https://github.com/opensearch-project/security-analytics/blob/aaba5111d7b540a56f45da86ceed3ac30578d683/src/main/java/org/opensearch/securityanalytics/SecurityAnalyticsPlugin.java#L471)
也确有correlation query。不能把“插件没有独立HTTP端口”当作所有插件不可达。
本次SA loader没有加载Netty类，只加载1个databind类；没有创建feed，出站具体pipeline尚未动态验证。

ML loader在运行，但本次未加载其Netty、HttpComponents或BCFIPS类；
[MLHttpClientFactory，25–35](https://github.com/opensearch-project/ml-commons/blob/7552f90b77e576c2417472a3415bb20b8a6f6e56/common/src/main/java/org/opensearch/ml/common/httpclient/MLHttpClientFactory.java#L25)
有真实AWS Netty出站客户端。镜像ML remote-metadata client亦实际引用HttpClient5。
没有模型/远程metadata配置的fixture负观察不豁免启用插件查询/外部客户端的部署。
RCA是单独agent分发目录，主JVM快照不覆盖其进程。plugin-cli的FIPS位于主`lib/*`之外，
启动脚本只在运行plugin CLI时追加该目录；**ML中的FIPS副本不能归入“仅工具”**。

### E4：JSON parser 与 databind

core的 [JsonXContent](https://github.com/opensearch-project/OpenSearch/blob/97d3c13bf22a4a72ac11dc503fe44c97662b9161/libs/x-content/src/main/java/org/opensearch/common/xcontent/json/JsonXContent.java)
创建streaming parser；记录JSON不因此成为任意Java类反序列化。
Jackson两项需要启用多态类型ID/PTV，数组项还需要`allowIfSubTypeIsArray()`。
受影响插件的已审源码未发现这些开关；镜像OpenSearch/AWS/Spark应用类常量引用扫描也未发现
这两种开关（Spark有`JsonTypeInfo`，故未据此给整个JAR豁免）。扫描不是完整反射/依赖调用图。
本次没有加载`BasicPolymorphicTypeValidator`；插件其他功能的mapper配置仍需证明或补丁。

## 完整 High/Critical 去重处置表

每行覆盖上述位置集合，精确每个包/版本/文件和范围见[索引](milestone-18-findings.json)。
“条件不成立”仅限写明的入口/fixture；“未加载”不等于永远不可达；“无法判定”保留风险候选，
不宣称成功利用。High/Critical沿用scanner；上游差异单列。所有修复指正式上游版本范围，
未发现并假定本镜像对旧版本另作backport。

| 公告与别名（主源） | 实际受影响包；修复 | 调用/输入条件及本轮处置 |
| --- | --- | --- |
| [CVE-2026-47063](https://linux.oracle.com/cve/CVE-2026-47063.html)，High7.5 | JDK21.0.11；21.0.12 | **无法判定，blocked**。OpenJDK定位`security-libs/java.security`；Oracle/CNA只说明网络输入经相关API。JDK安全库真实运行，公开资料没有给出具体危险方法/完整前提，不能以“JDK在运行”断言可利用，也不能凭Basic/TLS正常排除。 |
| [CVE-2026-41254](https://openjdk.org/groups/vulnerability/advisories/2026-07-21)，High7.5 | 同JDK；21.0.12 | LittleCMS `CubeSize`/2D颜色处理，需要攻击者数据进入颜色profile/变换；CRUD原样存JSON不解码图像，当前入口条件不成立。未穷尽ML/其他客户端图像功能，扩大到这些功能时**无法判定**；headless不是豁免。 |
| [CVE-2026-13506 / GHSA-qp49-qgx5-5m26](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%9013506)，High | B179/B184/F212；BC1.85、FIPS2.1.3 | `X509CRLHolder`/BC CertificateFactory lazy CRL →嵌套序列强制求值。E2当前证书/CRL条件不成立；BC lazy类确已加载。plugin-cli仅工具；ML/RCA/其他provider或CRL来源未被全部排除。 |
| [CVE-2026-8763 / GHSA-9pwp-9qqc-pr26](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%908763)，Critical | B179/B184/F212；BC1.85、FIPS2.1.3 | BC PKIX validator、受name-constrained中间CA控制者签发email/URI尾点SAN。E2的直接根/叶证书及JDK路径条件不成立；不是DNS hostname绕过，也不代表未来复制或插件PKI安全。 |
| [CVE-2026-8798 / GHSA-v6w3-qrh8-qccc](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%908798)，High | F212；2.1.3 | Intel RDSEED/RDRAND持续失败、native entropy重试无界。arm64架构条件不成立；plugin-cli仅工具，ML未观察加载。**amd64未获native资格，不能继承arm64排除**，须查实际provider/native entropy选择。 |
| [CVE-2026-13505 / GHSA-98j2-6v39-78w8](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%9013505)，High | F212；2.1.3 | Java>11下FIPS密钥/DRBG对象finalizer积压。plugin-cli仅工具；ML副本未加载，不是bcprov同类问题。启用FIPS使用路径/分配负载未证明，ML部分**无法判定**。 |
| [CVE-2026-5598 / GHSA-p93r-85wp-75v3](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%905598)，High | B179；1.80.2/1.81.1/1.84 | FrodoKEM decapsulation定时侧信道。E2的RSA证书/JDK TLS没有此算法，入口条件不成立；RCA独立进程的所有算法调用未调查完，不能仅靠主JVM快照排除。 |
| [CVE-2025-14813 / GHSA-574f-3g2m-x479](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902025%E2%80%9014813)，Critical | B179；上游1.84，backport1.80.2/1.81.1；scanner fix空 | `G3413CTRBlockCipher`计数器重用，需要使用GOST CTR并观察同key/IV密文。当前JDK TLS条件不成立；RCA部分同上。不能把scanner空fix写成上游无修复。 |
| [CVE-2026-75595 / GHSA-c4c3-7fpv-j4q5](https://github.com/netty/netty/security/advisories/GHSA-c4c3-7fpv-j4q5)，scanner Critical、**上游High** | N130/N135 handler；4.1.137（4.2线4.2.17） | E1直接SslHandler，无SNI handler；不存在“per-SNI REQUIRE、默认NONE/OPTIONAL、无二次验证”的组合。**当前HTTP/transport条件不成立**。Basic或OPTIONAL本身不构成豁免；插件另建SNI server未被全局证明不存在。 |
| [CVE-2026-45416 / GHSA-x4gw-5cx5-pgmh](https://github.com/netty/netty/security/advisories/GHSA-x4gw-5cx5-pgmh)，High | N130 handler；4.1.135 | SNI ClientHello预分配。主入口N135已修复且无SNI handler；SA副本未加载，不是主入口decoder，插件其他启用方式保留待证。 |
| [CVE-2026-44249 / GHSA-3qp7-7mw8-wx86](https://github.com/netty/netty/security/advisories/GHSA-3qp7-7mw8-wx86)，High | N130 handler；4.1.135 | IPv6 `IpSubnetFilterRule`错误mask。主入口已修复；本profile未用SA Netty IPv6 filter作ACL。当前条件不成立，不能推成数据库所有ACL合格。 |
| [CVE-2026-50010 / GHSA-c653-97m9-rcg9](https://github.com/netty/netty/security/advisories/GHSA-c653-97m9-rcg9)，High | N130 handler；4.1.135 | 包装普通TrustManager导致hostname校验丢失；Go后端校验不使用该库，主N135已修复。SA出站客户端trust-manager选择未取得充分证据，**无法判定**。 |
| [CVE-2026-33871 / GHSA-w9fj-cfpg-grvv](https://github.com/netty/netty/security/advisories/GHSA-w9fj-cfpg-grvv)，High | N130 http2；4.1.132 | HTTP/2 CONTINUATION计数条件。E1 HTTP/1不成立；SA内确有AWS HTTP2 pipeline类，启用出站协议及恶意响应来源未验证，不能只凭REST HTTP/1全局排除。 |
| [CVE-2026-42587 / GHSA-f6hv-jmp6-3vwv](https://github.com/netty/netty/security/advisories/GHSA-f6hv-jmp6-3vwv)，High | N130 http/http2；4.1.133 | br/zstd/snappy内容解压分配限制绕过。Weir不发压缩请求，入口N135已修；SA出站解压handler/远端输入未验证，**无法判定**。 |
| [CVE-2026-42584 / GHSA-57rv-r2g8-2cj3](https://github.com/netty/netty/security/advisories/GHSA-57rv-r2g8-2cj3)，High | N130 http；4.1.133 | HttpClientCodec的pipelining+HEAD+1xx响应错配。Weir的Go客户端不是此实现，主入口已修；SA客户端是否满足三条件未证明，保留候选。 |
| [CVE-2026-33870 / GHSA-pwqr-wmgm-9rr8](https://github.com/netty/netty/security/advisories/GHSA-pwqr-wmgm-9rr8)，High | N130 http；4.1.132 | HTTP chunk-extension解析差异。实际REST decoder为N135且已修复；用户不能经Weir指定framing。SA副本不是主server；未证明存在同版本的其他接收pipeline。 |
| [CVE-2026-56745 / GHSA-jppx-w49h-x2qq](https://github.com/netty/netty/security/advisories/GHSA-jppx-w49h-x2qq)，High | N130/N135 http；4.1.136 | `SpdyHttpDecoder` RST_STREAM释放遗漏。E1没有SPDY，**当前REST/transport条件不成立**；不把ML/PA/RCA/SA所有配置自动排除。 |
| [CVE-2026-55833 / GHSA-mvh2-crg5-v77c](https://github.com/netty/netty/security/advisories/GHSA-mvh2-crg5-v77c)，High | 同上 | SPDY zlib超限继续展开。相同协议前提/E1处置。 |
| [CVE-2026-55831 / GHSA-6jqx-86gh-f27w](https://github.com/netty/netty/security/advisories/GHSA-6jqx-86gh-f27w)，High | 同上 | SPDY SETTINGS无条目数限制。相同协议前提/E1处置。 |
| [CVE-2026-56819 / GHSA-93wv-jw9v-4972](https://github.com/netty/netty/security/advisories/GHSA-93wv-jw9v-4972)，High | N130及ML/PA/RCA的N135 http2；4.1.136 | `DelegatingDecompressorFrameListener`处理已关闭decompressor的DATA时泄漏。E1不启HTTP2；ML/SA已有真实出站客户端，是否安装此listener未证，**插件部分无法判定**。 |
| [CVE-2026-59901 / GHSA-558v-64gr-wgg4](https://github.com/netty/netty/security/advisories/GHSA-558v-64gr-wgg4)，High | N130/N135 codec；4.1.136 | Bzip2Decoder RLE死循环。E1无Bzip2 pipeline，HTTP解压不等同Bzip2；当前入口条件不成立，插件其他codec未穷尽。 |
| [CVE-2026-42583 / GHSA-mj4r-2hfc-f8p6](https://github.com/netty/netty/security/advisories/GHSA-mj4r-2hfc-f8p6)，High | N130 codec；4.1.133 | Lz4FrameDecoder按不可信长度预分配。主入口N135已修，E1也非该codec；SA副本未加载，不能把数据库其他LZ4实现当成此Netty方法。 |
| [CVE-2026-54513 / GHSA-rmj7-2vxq-3g9f](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-rmj7-2vxq-3g9f)，High | J14/J17/J18；2.18.8或2.21.4，3.x为3.1.4 | PTV数组allowlist绕过；E4普通parser条件不成立。未找到插件启用`allowIfSubTypeIsArray`，但完整依赖/反射mapper审计未完成，插件结论**无法判定**，不是“有JSON即可RCE”。 |
| [CVE-2026-54512 / GHSA-j3rv-43j4-c7qm](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-j3rv-43j4-c7qm)，High | 同上 | 多态类型ID包含泛型参数、仅验证容器类型。E4同类边界；具体mapper与可实例化类型未证，保留插件候选。 |
| [CVE-2026-54399 / GHSA-hf6x-8p5f-cgmf](https://lists.apache.org/thread/zmxh1pl2zohov5ntdh4lt85gfrlchgpy)，High | H531/H534；5.4.3 | HttpComponents HTTP/1超量header内存耗尽。不是OpenSearch REST的Netty decoder。ML remote-metadata实际引用HttpClient5，输入来自配置的远端；边界/触发功能未验证，**无法判定**。官方修复commit见E5。 |
| [CVE-2026-54428 / GHSA-v3jc-474w-2wm6](https://lists.apache.org/thread/5zjp8vczvxq19pw2rvhs21q446bhl0sd)，High | H531 h2；5.4.3 | SETTINGS ACK前HPACK解码限制未生效。E1不适用；出站是否协商h2/配置远端输入未证，**无法判定**。 |
| [CVE-2026-10050 / GHSA-2fvj-hgj9-j2gr](https://github.com/jetty/jetty.project/security/advisories/GHSA-2fvj-hgj9-j2gr)，High | S的jetty-security9.4.58；9.4.63 | Digest认证非Latin1字符损失；当前用Basic/JDK TLS，未启动Spark Digest服务。实际有重定位DigestAuthenticator类，不能说只有元数据；公告正文谈client而包范围列security，保留该差异，不据此误判已修。当前入口条件不成立，额外Spark功能未资格。 |
| [CVE-2026-77422 / GHSA-r2xf-8xr9-62gw](https://github.com/jline/jline3/security/advisories/GHSA-r2xf-8xr9-62gw)，High | JL3.27.1；3.30.15 | JLine内置grep的用户regex及输入文本；Weir没有shell/grep入口，快照未加载PosixCommands。当前条件不成立；额外远程shell不在已验证配置。 |

### E5：未得出的结论与检索限制

OpenJDK advisory经web工具读取失败，但匿名标准HTTP获取成功（200，保存原HTML），不是
使用凭据或绕过访问控制。其`java.security`分类比Oracle“Libraries”具体，仍没有危险方法名。
CVE CNA记录也只给组件/API条件；不把其他JDK CVE或不相关补丁猜成47063的修复。

HttpCore公告页面由web工具返回403，直接HTTP结果单独留在`official/httpcore-receipt.json`。
主源修复为 [HTTP/1 commit d96a00f](https://github.com/apache/httpcomponents-core/commit/d96a00fec9b2e19f8005e35681df5f6cd6e21a9e)
和 [HPACK commit 1ea1239](https://github.com/apache/httpcomponents-core/commit/1ea1239bbbe3442a8382a87279c0a8119a7e358e)；
直接HTTP取得的是200网页壳，没有公告正文；上述固定commit的内容获取成功，
对应GHSA只是辅助版本索引。LittleCMS advisory API和试探的JDK release URL返回404，
没有把失败结果当修复证据。ML源码归档超过100MiB读取上限后不完整，保留失败；随后仅获取
完整tree metadata和确切MLHttpClientFactory源码，不对部分源码树声称“全库无调用”。

## 唯一官方候选：3.8.0 不足以直接解除阻断

2026-09-28一次查询[官方release](https://github.com/opensearch-project/OpenSearch/releases/tag/3.8.0)
确认已发布、非draft/prerelease，API published_at为2026-08-05T17:20:33Z；
[官方日程](https://opensearch.org/releases/)为August4。2.19.7（October20）、3.9.0（September29）
仍是计划；本次release列表无已发布记录，不作为可用修复。
只匿名取得下列一个候选的arm64 image并离线检查；没有启动候选数据库、实现迁移或改变活跃profile。

| 3.8.0身份 | digest |
| --- | --- |
| index | `sha256:fafe3fc3587088674669235575aa166228c48bdb940294a8cdbbc1da75236a40` |
| linux/arm64 manifest | `sha256:e085eef5d92694dd98994e3daa4c50f2dc9457a7d6e3b0e48856108901a4e33d` |
| linux/arm64 config | `sha256:77b571868835ab8f1960748ae20567abfcc54414f0ee52bae9a7e4b22365cacf` |
| linux/amd64 manifest（仅metadata） | `sha256:68a688de28fb9bb66601552650b91a52a9fd5e7eac5481dd2b225ecb66fd09b0` |
| linux/amd64 config（仅metadata） | `sha256:9c4d3b54042a402a11618658d5309121a349aeb60b375deba29334d033e65fc0` |

实际archive/JAR确认：Temurin **25.0.4+7-LTS**；bc-fips **2.1.3**（core、plugin-cli、fips-demo工具）；
没有将release note中“BC1.85”直接冒称为镜像内bcprov版本。databind副本为2.21.4/2.22.1/3.2.1，
上述JDK/BC/Jackson匹配被版本更新覆盖。主Netty **4.2.16**仍在SNI公告范围；SA独立JAR含
**4.1.133、2504个真实Netty类**，SHA256
`427d4228f853aab93619130191133e224abbfd750380f6d972155e021c50e69e`。
HttpCore/httpcore-h2 **5.4**仍早于5.4.3，分布在多个插件及reindex等模块，位置完整保留在候选扫描。

同Syft1.52.0/Grype0.119.0、同M17冻结DB（2026-09-27T06:30:30Z，120h校验开启）得到
85 raw matches：6Critical/36High/43Medium，High/Critical共13公告、0ignore。
这不是“42个可利用漏洞”，也不是与旧187直接相减的安全得分。候选CDX的官方1.6 schema
校验0错误；镜像config与7层解压diffID已逐一核对，archive SHA256为
`e742ba094bb905e5f8c6fb47e90676f6a7cea72268ed51d8d35a4b85958af505`。
候选修复了最关键的JDK/BC版本缺口，但仍没有完成其自身入口/出站插件风险处置。
尤其候选实际transport JAR已有HTTP2相关类，**不能照搬2.19.6的HTTP/1服务器排除结论**。
3.8.0是可研究的官方后续候选，不是本轮合格替代方案。

[3.0 breaking changes](https://docs.opensearch.org/3.0/breaking-changes/)明确改变JDK下限、system index
访问、Bulk ID长度一致性、JSON/查询嵌套默认限制、部分安全配置及PA RCA分发。
若后续由统筹批准跨major，最小重验包括：

| Weir调用 | 必须重验的边界 |
| --- | --- |
| Read/CRUD/OCC/expression | 具体index/ID、`_source`、seq_no/primary_term、冲突重算、原子doc更新、精确整数与错误outcome |
| Bulk/Native | 512字节ID、逐项status/关联、允许header/query、无pipeline、流背压、提前响应/丢ACK后UNKNOWN且只派发一次 |
| Scan/PIT | endpoint、pit_id/creation_time、`_doc`排序/search_after、query嵌套限制、partial shard拒绝、取消和cleanup |
| 安全/连接 | 标准CA/hostname/Basic、实际TLS provider/HTTP协议、插件loader和新默认值、上游风险处置；不关闭验证 |
| 资源/制品 | 受影响的进程预算/准确Darwin与Linux arm64制品运行；其他架构仍需自身native证据 |

这些是后续范围，不是本轮已完成事项；现有profile严格版本检查仍会拒绝3.8.0。

## 最小外部依赖和下一步

1. **优先等待一个已发布官方标准发行物或具体上游适用性证据**：至少解除47063的不确定性；
   运行JDK补丁版本21.0.12或相应维护线修复可消除该版本匹配，单独证明危险API不可达也必须
   有可复核方法/输入链。没有这种证据前不放行OS2.19.6。
2. 对实际会启用的插件路径，要求官方补丁或逐条条件证明：本表对应BC1.85/FIPS2.1.3、
   Netty4.1.137/4.2.17、HttpCore5.4.3、Jackson2.18.8/2.21.4/3.1.4等。
   这不是要求所有打包库都升级到某个统一最新版本，更不是手工换JDK/JAR、删插件或禁TLS方案。
3. 若统筹希望先接受一个严格部署配置，需明确实际provider/证书/CRL、启用插件及出站功能、
   直接客户端入口和Scan query能力，并补相应证据。**任何新增“只准这些查询/禁用插件/不许其他客户端”
   都是 proposed 部署前提，尚未获接受，不能静默写成既有支持范围。**
4. 上游满足条件后，仅重做受影响的资格阶段；3.x还需上表语义/连接回归。当前没有一个可直接
   指向并宣称安全的官方替代digest。一次向统筹报告外部阻塞，停止此调查；不建立定时轮询，
   不自动开下一聊天，不拿Kubernetes/容量/soak掩盖该结论。

amd64的Intel FIPS/native资格、多节点transport/复制证书、系统roots正向资格以及原有平台/
Kubernetes/容量/24h门槛继续保留；本轮没有缩减目标。

## 复验命令、工具限制与收尾

显式探针 [component_evidence_integration_test.go](../internal/backend/search/component_evidence_integration_test.go)
只在`integration`且两个opt-in环境变量同时满足时启动新的自有fixture。
它仅运行标准HTTPS put/read并读取classloader，无OOM/证书绕过/压缩炸弹输入。

```sh
# 使用M17已核验工具链、离线module cache、自有空HOME/Docker config；完整环境在命令receipt。
WEIR_SEARCH_SECURE_INTEGRATION=opensearch WEIR_SEARCH_COMPONENT_EVIDENCE=1 \
  .tools/go1.27.1/bin/go test -race -tags=integration ./internal/backend/search \
  -run '^TestOpenSearchComponentEvidence$' -count=1 -timeout=180s -v

# 不设置上述变量时只SKIP，不连接数据库。
.tools/go1.27.1/bin/go test -race -tags=integration ./internal/backend/search \
  -run '^TestOpenSearchComponentEvidence$' -count=1 -v
.tools/go1.27.1/bin/go vet -tags=integration ./internal/backend/search

# 一次匿名registry metadata/pull/export后，扫描明确archive，不扫描HOME/fixture/daemon。
python3 .testdata/m18-evidence/registry.py
python3 .testdata/m18-evidence/candidate.py
python3 .testdata/m18-evidence/candidate-scan.py
python3 .testdata/m18-evidence/jar-audit.py
```

`candidate-scan.py`的真实工具命令、参数、时间、exit和config ID在
`dist/m18-candidate/scan-receipts.json`；`fetch.py`/`source-fetch.py`/`fetch-extra.py`及各receipt
保留公开URL、固定源码SHA和抓取失败。网络抓取/扫描是本次显式调查，未接入默认测试。

本轮HTTPS/race探针PASS（24.88s测试、26.763s包），连接账本owned=0、acquired=released=1；
disabled探针明确SKIP，integration vet PASS。未重跑M17全套功能和构建矩阵。
`final-validation.py`核对全部78个位置、46组包版本、28个公告/别名、实际JAR hash及原始修复字段；
44个相对链接、12个固定源码链接的本地对应文件/行号、gofmt和`git diff --check`均通过。
候选schema检查首轮缺少validator环境、第二轮30s超时均保留；使用M17固定venv及120s上限
完成（31.631s、exit0），没有安装新工具或改schema。
ES原始CycloneDX1.6的`SMAIL-GPL`/`Artistic-dist`两处enum失败已由统筹复现，
Weir8份SBOM通过的状态不变。它是外部SBOM表示问题；本轮不修改schema、license或包。
[官方1.6 schema](https://github.com/CycloneDX/specification/blob/1.6/schema/bom-1.6.schema.json)允许以license `name`表示非该schema枚举的名称，是可供上游采用的表达方式；**未修原报告仍失败**。

清理证据在`cleanup.json`：本轮fixture已由owner helper独立有界回收，仅保留日志、version/runtime、
loaded-classes及owner；未触碰M17产物、未知容器/网络/卷，无全局prune。
候选只作离线检查，无候选数据库进程。调查源码/扫描/日志保留；没有push/PR/tag/release、
生产部署、付费资源或系统/daemon修改。最终本地提交后停止checkout写入和全部测试，交统筹独立验收。
