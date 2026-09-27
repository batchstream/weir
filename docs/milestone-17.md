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

其余当前公告和镜像候选、准确制品及完整验证收据将在本阶段收尾时追加；本提交阶段不把
尚未完成的制品测试或扫描写成通过。旧里程碑仍保留其原始版本和失败。
