# M32R2：绝对期限比较与单次门控镜像交付

2026-09-29。基线 `3d8ac455918b4493a531da886ac355a312464f24`；实现、冻结候选与唯一 CI 受测 source `278264db2f9617ad583c6b56d19b8aaf5943e771`。最终文档 delivery/local/remote SHA 另列于 `.testdata/m32r2/delivery.json`，文档不另触发构建。

**本阶段本地/双原生CI回归及两个公开镜像交付成功，待统筹独立验收；不增加EKS或整体生产资格。**

## 根因与修改

只改三个资格脚本的原始起点与绝对期限关系。共享 `Run` 保留构造时的一次真实 `stage_started`；exec-tail prepare 冻结该值，不再从 deadline 减 900 反推，不为 prepare/execute 重开窗口。arm 直接构造 `started+60`，要求不晚于原整体截止；producer加关闭28（20+4+4）、observer加关闭18（10+4+4）和共享 CLI29（25+4）秒准入均按当前检查时刻加原固定时长，比较原截止。observer 前后使用同一整数时长求和顺序。

120/900 秒冻结计划直接检查 `deadline == started+duration`。cleanup 生产创建/执行完全不改，300 秒和 45 秒预留的相关测试按实际同方向创建关系断言。没有 epsilon、isclose、round、截断、隐式延期、Clock 接口或通用 deadline 框架。正式 role60/observer10及六样本、EOF4、Stop/Wait4、关闭4+4、120/180/300/900、namespace45/request10、合成12=4+4+4和5ms性能门槛均不变。

另增加一个直接相关的 `eks_deadline_test.py`：9个起点（4.1、4.4、8.2、124.1、212.2、212.3、1024.1、1000000.1、2**32+.1），207组控制时钟回执。arm27、observer前后54、资源CLI/一次恢复54、120/900冻结计划JSON往返54、cleanup180/300及默认180共18。`nextafter` 的前一可表示截止严格拒绝；准入的足额/后一截止通过；固定计划的前/后漂移均拒绝。首错和closing note保留；首次动作前不足时0命令/0operation，身份检查后不足不创建observer，恢复余额不足不追加CLI；计划漂移在invocation之前拒绝。prepare通过真实公共文件hash，全部远端命令被局部fixture替代。

## 原失败与本地回归

修改前重新运行统筹原探针，原始stdout/JSON及源码副本封存。起点4.1的足额60秒被原guard误拒，4.4进入identity首错；8.2+120及124.1+900的原静态计划误拒；212.2/212.3经JSON往返后deadline创建关系成立，但逆减不等于300。0外部命令。这是确定性的控制时钟证据，不是原CI现场时钟或唯一因果归因。

| 本地检查 | 实际结果 |
|---|---|
| 受影响普通/优化 | 各33通过，9.867/8.945秒；0skip |
| 完整默认普通，一次 | 264通过，0skip；356.406秒，外层356.580/360秒 |
| 完整默认优化，一次 | 236通过，28项既有优化入口限制skip；338.999秒，外层339.173/360秒 |
| 固定Go1.27.1 focused test binary | 现有缓存离线编译，11个生命周期子例通过，外层7.136秒 |
| 实际Go→Python外部pipe | 普通EKS/local/calibration分别6/71/1350；优化支持的EKS/local6/71；exit0、validate≤EOF≤Wait |

focused binary `.testdata/m32r2/testcapacity.test` SHA256 `bdecea8c710fcd57224247c6dbdd818eb4906cb79578a4244ec9a234d026c58c`；编译命令 `go test -c -tags=integration -o <本轮绝对路径>/testcapacity.test ./internal/testutil/testcapacity`。GOENVoff/GOWORKoff/GOTOOLCHAINlocal/GOPROXYoff/GOSUMDBoff/readonly/CGO0，准确PATH、GOROOT、GOMODCACHE、GOCACHE及每次测试命令见本轮 `compile.json`、`full-normal.json`、`full-optimized.json`。外部binary路径显式传入普通及优化测试，没有缺binary skip。

旧5秒/受控1.2秒启动延迟的大流反例仍失败且回收；12秒正例完整成功。超总窗、无ACK、非法控制、取消、非零、错误身份、截断、记录失败和关闭边界均保留。完整cleanup inventory、foreign/UID、同次唯一invocation及不能借45秒reserve的原回归通过。

本地为Darwin arm64、真实短pipe及合成采样时间戳，不是Linux/EKS或真实10/140/2698秒资源采样。记录的175个本地runner/owner PID均已fresh查无；102条completion OWNER全部reaped/Join/三管道关闭。其他生命周期故障注入按原独立schema记录，故意joined=false不写成全true。

## 不变输入与公开范围

206Go/module、产品70/helper82正式输入与基线逐字节相同；workflow、打包和全部pins未改。本次只有六个源码/测试文件，已逐项匹配受测快照与实现提交。fresh fetch确认origin及remote main仍为基线、祖先关系成立、main干净；审查所有新增公开路径、内容和build allowlist后正常fast-forward push。未跟踪证据/秘密风格文件，未把证据或测试binary放入产品context。

## 唯一 CI

唯一新 [run36493558434](https://github.com/batchstream/weir/actions/runs/36493558434)，attempt1、source如上，四job全部success。共同窗口2026-09-28 22:38:09.209076Z—2026-09-29 00:08:09.209076Z；run于22:38:11Z创建、23:01:14Z结束，23分03秒，未超窗、无需取消。只有一次dispatch/一个新run，没有重跑旧source或第二次run。运行中main保持原冻结source。

| 原生测试 | Job ID | Python普通 | Python优化 | Go package PASS |
|---|---|---|---|---|
| Linux amd64 | 109167737884 | 264通过/0skip，302.076秒 | 236通过/28skip，260.354秒 | 35条非缓存 |
| Linux arm64 | 109167737667 | 264通过/0skip，300.517秒 | 236通过/28skip，265.411秒 | 35条非缓存 |

两架构默认CGO0、独立CGO1race、普通/integration vet、helper聚焦race均实际执行。每架构普通三消费者6/71/1350、优化两消费者6/71真实执行；每架构新增期限控制时钟回执414组，completion OWNER 102条全部回收，仍只属于原生CI中的合成时钟/短pipe测试。独立job原始日志及解析见 `native-test-verification.json`。

publish job109171802158（22:51:34Z—23:00:54Z）与smoke-arm64 job109174598430（23:00:59Z—23:01:13Z）均成功。两原生测试环境均4 logical CPU、内核6.17.0-1022-azure；amd64 image20260920.314.1，arm64 image20260920.129.1；完整内存/工具pins见CI_ENVIRONMENT原始回执。

原M32 run36482306611、M32R run36488063884仍为failure/publish skipped/0新镜像，不以本轮成功改写。

## 准确制品与匿名验证

- `ghcr.io/batchstream/weir@sha256:aee0c24fd5a0edbe81d335522e2741b34ca267f8246c893e15d6ef0097c18bb4`
- `ghcr.io/batchstream/weir-qualification@sha256:364474990ae529650ac69a83f29e15cdbc82474c6eb5dca4ccb0d3cdf3874999`

两包唯一新标签均为完整 `278264db2f9617ad583c6b56d19b8aaf5943e771`。没有latest/semver、Git tag或Release。image source与CI source均为该实现SHA；最终文档提交不改变正式输入，也不另构建。

| 包/平台 | Manifest digest | Config digest | Binary SHA256 |
|---|---|---|---|
| weir/amd64 | `sha256:983fd8f1c4e5375bd9469fe494abd0ecf48cf7228b1feefc65471ccb255d659b` | `sha256:b18e0e4e4a901759ca4bf4397a535731e96bc62310d27fd5780da3bd689aa732` | `2561cafec9e794e893ea2d3a24218504f092a72cd4acabb212958cfefb9991dd` |
| weir/arm64 | `sha256:31a7d7a8d6db388448db51d19621a20ba93af18aff6073455f8c59121929b98c` | `sha256:041c0aa44758abf8373f1d7b6e1f7f2f69efef57c46a428e11da75a185f5e431` | `48a81d3e3ea12dc51b0ba44d8ed879a2b62c46b947afee2e8fb27db6af81bac8` |
| qualification/amd64 | `sha256:69ec7ac92f274bab48fd1b0385c5d2cbe26c13fc1d7311d023765f9a650d2542` | `sha256:c4605e5d733e557da42e8008b25a887de148d865e040a77c06a2805421c632ec` | `786b9a24f1a0b669ac9fde4bf77c07e4be7f7e74c7dabe997799de0f66c7821a` |
| qualification/arm64 | `sha256:87070a283529918acc3cd62a241135de82c90a3431ab48b2485c4e2e7f779c84` | `sha256:409a8092911ed4ba7d2b1d7901b0df121fe3ebe54705f7d7ff42d44ea7a9db69` | `2629ed0d83623c4cb0c0895386e1abe1b87f78661a42c73562ab0154e4210e99` |

产品六目标binary/archive、helper两个Linux目标binary与两包OCI各两轮一致。每包BUILD_RECEIPT与最终DELIVERY的source、输入map/hash、binary及OCI逐项匹配；base index/各平台base layers、应用层、flags和完整来源图见 `verified-delivery.json`、`ci-records.json`，70/82正式输入与本地冻结证明一致。

CI两native runner各对两个包匿名完整导出，合计4份含双架构全部内容的归档；验证index/manifest/config、每层hash、准确base、source labels和应用binary hash。不是仅manifest inspect。本机另外使用已核对固定SHA的regctl和自有临时空客户端，匿名读取两个sourceSHA标签及6个准确manifest，共8次读取；标签/index/platform/config/layers全部匹配，临时客户端已自动清理。这个本机检查仅是manifest补证，完整内容证据来自上述CI导出。

六次原生短smoke为amd64/arm64各Weir-version、qualification一秒pace、单次snapshot。准确config/binary/source、exit0、无OOM/restart、非root65532、networknone、readonly/dropALL/no-new-privileges/private-cgroup、1CPU/256MiB/128PID均经原始输出核对；pace仅50次TimingOnly，不评判5ms SLO，不是DB/资源/容量资格。

## 清理与封存

唯一自有builder `weir-m24-36493558434-1` 已删除，6个准确smoke容器ID与6条removed=true逐一对应；CI标准工具logout成功，没有读取/下载/封存登录文件。watcher已Wait/exit0；全部job终态。没有本轮本地容器、DB、EKS资源或全局Docker修改。

本轮原始40成员logs.zip为291830 bytes，SHA256 `13f3ac3fe01f9fcea587faaee840bcfe061595c99c858db5ff4dad8c7c58b3bd`。两份独立job下载与zip对应成员全文逐字一致。最终run/jobs API、唯一source-run列表、两轮复现/发布/匿名/smoke/清理回执和本地原反例/测试全部保存在 `.testdata/m32r2/`。

3937份历史封存文件前后逐项hash/size未变。`manifest.json`逐文件hash/size封存本轮证据；排除自引用的 `delivery.json` 分列baseline、implementation、CI/image source、最终docs delivery/local/remote SHA和manifest hash/size。完整本地测试在基线HEAD加最终修改上运行，随后逐文件与实现提交一致核对；原始命令环境和源码快照均保留。最终进程/CI清理与仓库状态见 `cleanup-audit.json`、`final-repository.json`。

## 限制

不改统筹state/approvedImages或EKS SOURCE/IMAGES接线。旧M29 source4abc及其准确digests在统筹接受新产物前不变；原M32/M32R失败仍为失败。resource partial/not-qualified、timing not-run、candidate null、丢字节层unknown保持；CNI、跨两worker、六原生平台、两规格/各后端、安全、容量/恢复/24h门槛均未豁免。Weir认证排除、ProgramTransform首版延期不变。

无EKS/aws/DB/负载/新本地容器/本地镜像构建、付费资源、全局变更、Git tag/Release或timer。最终自有进程/CI全部终态并封存后，一次回调统筹并停止。
