# M32R：离线测试前提补救与单次门控交付

2026-09-29。基线 `762bc7051eee20145f7a7d3752efd624467a779a`；实现、冻结候选和 CI 受测 source 为 `6e7b296d6460ccd4ed13843e2b1df35eb63caed5`。最终文档交付、本地及远端 main SHA 单独记录在 `.testdata/m32r/delivery.json`；文档不另触发构建。

**本地回归通过，唯一新 CI 因两个既有时间边界路径的新失败而 NO-GO；没有新镜像。没有第二次 run 或发布阶段追加修复。**

## 两项有界修复

`eks_exec_tail.plan_check` 保留固定 profile、arms、payload、source 文件 hash、预算及固定 kubectl hash 合同，离线检查不再读取宿主 kubectl。共享的具体 `kubectl_check` 在真实 prepare/execute 的任何命令或写入前检查工具存在和原准确 hash；缺工具明确报 `kubectl missing`，不同文件仍报 `kubectl drift`。没有修改固定 Mac 工具 hash、增加 flag 或环境绕过。回归使用自有临时程序文件和局部 PATH；真实缺工具/错 hash 均零命令、零写入，source 漂移也在 invocation 写入前拒绝。

仅合成测试 caller 的总窗固定为 **12 秒 = 接收/校验 4 秒 + 正常 EOF/排空 4 秒 + Stop/Wait 4 秒**，起点在 child 启动前，异常清理沿用同一截止。旧 5 秒仅保留为回归反例。正式 Observer.stop、三个消费者、Go 完成函数、EOF4、EKS role60/close4+4、主900/clean300/共享180、raw/FD/样本数量及5ms性能门槛全部不变。

1350 样本仍全部序列化、校验和接收；1.2 秒延迟只放在反例生产者开始输出之前，不是生产尾部 sleep。Python 流的完整 2698 秒跨度和 terminal 使用一致合成时钟，Go 联测仍以实际 Go terminal 时间满足原两秒合同；未放宽 timestamp 或资源校验。

## 原始反例和本地结果

修改前同一原测试在当前 PATH 通过，自有不同 kubectl 文件报 drift，空 PATH 报 TypeError；没有执行 kubectl。原 5 秒 consumer 的真实 pipe 无延迟通过时仅余 0.124 秒正常排空；加 1.2 秒启动延迟后在 2.074 秒失败，仍余 2.931 秒总窗、排空截止已过去 1.069 秒。子进程仍被回收。此本地窗口复现不证明它是旧 Linux CI 的唯一现场因果。

最终完整普通套件内的大流对照：

| 合成 caller 总窗 | 样本 | 接收 terminal | 校验完成 | 正常排空剩余 | 整体结果 |
| --- | --- | --- | --- | --- | --- |
| 原5秒，启动延迟1.2秒 | 1350 | 1.568s | 2.084s | -1.085s | 2.087s失败，exit-15；Wait/双EOF/Join/关闭 |
| 新12秒，同样延迟 | 1350 | 1.584s | 2.100s | 4.000s | 2.227s成功，exit0；Wait/双EOF/Join/关闭 |

raw timeline 记录接收、校验、EOF 发送、stop 入场/排空截止、双 EOF 和最终 Wait。无 ACK 的同源码 Go child 仍在 4.015 秒非零；非法控制、提前 EOF、信号、计数错误、背压、写失败仍拒绝。完整测试另覆盖超过12秒总截止、截断、非零、错误身份、额外记录、取消及记录 I/O 错误，保留第一错误并检查所有 child 回收。

| 本地检查 | 结果 |
| --- | --- |
| 受影响普通4模块 | 34通过，143.517s |
| 受影响优化4模块 | 31通过/3项 calibration 既有优化入口限制 skip，115.010s |
| 完整默认Python普通，一次 | 259通过/0skip，353.186s；外层353.344s/上限360s |
| 完整默认Python优化，一次 | 231通过/28项既有优化入口限制 skip，322.381s；外层322.570s/上限360s |
| 当前Go源码 focused binary | 已用固定Go1.27.1、已有离线缓存编译；11个生命周期子例通过，外层7.290s |
| 实际Go→Python外部pipe | 普通EKS/local/calibration分别6/71/1350；优化支持的EKS/local6/71；validate≤EOF≤Wait |

以上是 Darwin arm64 本地短 pipe/合成时间戳，不是真实10/140/2698秒采样或 Linux/EKS 资格。没有重复不变的全仓 Go 回归，没有新本地容器、镜像构建、数据库或负载。277个记录到的本地 runner/child PID 新查询均不存在；completion OWNER 全部 reaped/Join/三管道关闭。其他已有生命周期故障注入按其独立字段保留，不能把故意的 joined=false 统一写成成功。

206 个 Go/module、正式产品70/helper82项输入与基线逐字节不变，最终受测文件 hash 对应实现提交。工作流、打包代码、工具/module/base/Actions pins、两轮复现、公开范围、匿名完整导出和原生短 smoke 门禁均未修改。先 fresh fetch 确认 origin 与远端基线，再审查全部3个新增公开路径和提交；没有跟踪证据/秘密风格文件或将它们带入构建 context。普通 fast-forward push 后 API 再确认准确 source。

## 唯一 CI

唯一新 [run 36488063884](https://github.com/batchstream/weir/actions/runs/36488063884)，attempt1，source如上。共同窗口从 2026-09-28 21:44:08.705853Z 至 23:14:08.705853Z，最多一次新 run。run于21:44:11Z创建、21:58:46Z结束为failure；约14分35秒，在90分钟共同窗口内。两个测试job失败，发布和smoke依赖门禁正确跳过。本阶段按一次尝试边界停止代码补救，不追加run，镜像交付未完成。

原 M32 run36482306611/sourceb420c8ea 的两个 native Python 失败、优化 not-run、publish/smoke skipped、0新镜像原样保留。

| Job / 原生平台 | 准确ID | 实际结论 |
| --- | --- | --- |
| test / Linux amd64 | 109149710879 | failure；普通259通过/0skip（326.902s）；优化230通过/1failure/28skip（296.899s） |
| test / Linux arm64 | 109149710985 | failure；普通257通过/2failure/0skip（303.829s）；优化not-run |
| publish | 109154692934 | skipped，无steps |
| smoke-arm64 | 109154694317 | skipped，无steps |

实际runner均4 logical CPU、内核6.17.0-1022-azure；amd64 image20260920.314.1/x86_64，arm64 image20260920.129.1/aarch64。完整CI_ENVIRONMENT保留内存和工具pins。

两个平台各35条非缓存Go package PASS（默认17/race17/helper1），不是单测数量；默认CGO0、CGO1race、vet/integration vet、helper回归和focused binary编译都实际执行。没有新增外部native/DB opt-in。普通实际Go→三消费者两平台各6/71/1350通过；amd64优化的两个支持消费者也执行并通过。arm64优化不能用本地结果替代。

每个平台的1350样本旧5秒反例和12秒正例都有完整时间回执；三个实际执行的Python模式都完成超过12秒的失败回收检查。amd64有102条、arm64有63条completion OWNER，全部reaped/Join/三管道关闭；另外60条原有生命周期故障注入记录按实际schema分别保留，不声称其所有joined标志为true。


## 新失败的原始事实

arm64普通Python：259项，257通过、2failure、0error、0skip，303.829秒；优化命令因fail-fast未运行。默认CGO0、独立race、vet/integration vet、helper聚焦race和Go test binary编译已成功。

1. `AdmissionTests.test_arm_record_failure_preserves_first_error` 预期注入的 `first identity failure`，实际提前得到 `insufficient full arm budget`。原 `Run.arm` 用 `until=min(overall, started+60)` 再检查 `until-started>=60`；该次日志未记录具体started/until。源码提示浮点边界风险，但未新跑反例，不宣称唯一现场因果已证实。
2. `CleanupReplay.test_300_second_plan_keeps_original_deadline_and_single_invocation` 的原断言将两浮点时间相减严格等于整数300，实际 `299.99999999999994 != 300`。amd64优化同一测试得到 `300.0000000000001 != 300`；两个数值均来自原始traceback。没有改300秒合同或修改测试求绿。

两个失败的测试逻辑与相关guard/cleanup代码均为基线原文；本次只改变同文件内另外的离线准入测试。原M32未出现这两个错误不等于这些时间边界已稳定。两处需要统筹独立判断下一阶段，不在公开交付时追加补丁。

arm64本轮kubectl隔离/缺失/错hash/source拒绝、1350样本5秒反例与12秒正例、超过总窗负例全部通过；实际Go→EKS/local/calibration三消费者6/71/1350，stdout分别34,502/398,559/7,558,925bytes，exit0且validate≤EOF≤Wait。旧5秒带延迟失败时总窗仍余2.796秒而正常排空逾期1.204秒；12秒同样延迟在2.578秒完整成功。合成样本不等于原生资源资格。

## 制品、清理与证据

本轮新image source/digests均为null；build/two-round reproducibility、publish、匿名全内容、六个准确镜像native smoke全部not-run。publish从未启动，因此没有本轮CI builder/container或registry login待清理，也不存在本轮logout成功证据。旧M29镜像不能充作新产物。

原始run/jobs API、完整20成员logs.zip、每成员hash、独立arm64日志及其与zip全文逐字一致证明、CI回执和失败traceback保存在 `.testdata/m32r/`。zip共133,821bytes，SHA256 `38356683b17860140168db2697ba444bfce06bc82b81766ab7cd91f2c8e946c1`。`test-summary.json`分列实际普通/优化及not-run；`failure-path-lineage.json`证明四个相关原函数与基线逐字不变。预先准备的成功审核/匿名脚本以 `-not-run.py` 标明未执行。

本地watcher已Wait/exit1，未超窗口、无需cancel。全部CI job终态已确认，两runner最终清理日志存在；本阶段自有本地进程最终查询、零本地容器/远端fixture记录在 `cleanup-audit.json`。3691份历史封存文件前后逐项hash/size未变。`manifest.json`逐文件hash/size封存全部新原始证据；manifest hash、最终local/remote SHA及报告hash由排除自引用的 `delivery.json` 给出。最终只正常推送报告/readiness，不再触发workflow。

## 保留边界

本阶段不改统筹 approvedImages/state，不更新或执行 EKS SOURCE/IMAGES 接线；其现有指针仍为 M29/source `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。resource partial/not-qualified、timing not-run、candidate null及原丢字节具体层 unknown 保持。CNI、跨两worker、六原生平台、两规格/各后端、安全阻断、容量/延迟/恢复和24h门槛未由短 pipe 或镜像 smoke 通过。Auth排除、ProgramTransform首版延期不变。

无EKS/aws/DB/负载/付费资源、新本地容器、Git tag/Release或timer；本轮仅正常push既定main，两个公开GHCR包的完整sourceSHA标签发布授权未实际执行。结束后一次回调统筹并停止。
