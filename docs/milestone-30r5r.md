# M30R5R：本地 observer 无条件收尾与下一预检预算

2026-09-29。基线 `2a665ee760f942cb33f6b30a54d917c303ea80dc`。本阶段只修改共享 Python Observer、无负载预检控制流程及相关测试/文档；没有 EKS 访问（包括 GET）、新增网络/wire 实验、Docker/DB 负载、产品 Go/module/正式 helper 改动、push/Actions/registry/镜像操作。

**这里只交付本地修复。真实 M30R4 reset 发起方仍 unknown，原缺 observer_end/client、resource partial/not-qualified、timing not-run、candidate null 不变。60/300 秒是下次待原生核验的预检 profile，尚未取得 EKS 资格。**

## 两个真实 I/O 反例分别保留

直接复制统筹的 `record-failure-probe.py` 和 `io-failure-probe.py`，只操作探针自己的临时路径和子进程。原脚本字节保持一致；修前、修后各有外层 30 秒上限与 Wait 记录。

| 故障 | 原源码 | 修后 |
| --- | --- | --- |
| 初始化后把自有 weir-exec.json 换成目录 | IsADirectoryError；child 已 Wait/exit -15，但 stdout/stderr 未关闭，stopped=null；探针最后自行关闭 | 同样失败返回；child exit0、三管道关闭、stopped 已设置 |
| 把自有 weir.jsonl 建成目录 | IsADirectoryError；原实现已关闭三管道，child exit -15 | 保持失败；三管道关闭、child 已 Wait |

两者不合并成一种“磁盘异常”。修后 record 故障不再使正常 EOF 退出变成强制终止，也没有把落盘失败当成功。封存位置是 `.testdata/m30r5r/before/` 和 `after/`；原 M30R5 证据不修改。

## 收尾所有权与首个错误

Observer 在 stdin 关闭、EOF 排空、Stop/Wait、最终 poll、每个管道关闭之间明确隔开失败边界；初始化非阻塞设置/初始 record 失败也走相同 owner 收尾。缓冲管道 close 失败时关闭其自有 raw pipe，避免再次发送缓冲输入。group Stop 出错后仍单独尝试 leader kill 和 Wait；kill 失败不能跳过 Wait。

最终 record 位于进程回收和三管道关闭之后，收尾中的 poll 不再提前写 record。原 sample/nonzero/truncated/output-bound 首败优先，后续收尾/记录错误附在异常 notes；KeyboardInterrupt/SystemExit 不被关闭错误或 operation 文件落盘错误覆盖。没有引入通用资源框架、线程池、接口或可替换的生产函数变量。

`joined` 只表示完成了真实 leader Wait；`stopped` 只在 joined 且三管道均关闭时设置。它们描述本地回收状态，不表示采样/记录成功，也不证明远端 helper 的退出时刻；失败仍抛出，成功写出的记录另带 stop_error。持续注入 Wait 失败时 joined=false、stopped=null，测试在记录这一状态后自行完成真实 Wait。重复 stop 保留第一次结果，不重做 poll/record/输入写入；后续 poll 不读已关闭 FD。

测试用真实临时文件和 Popen 管道覆盖 record/data 落盘失败、stdin/stdout/stderr 关闭错误、最终排空错误、最终记录错误、初始化记录/管道设置失败、group/kill/Wait 错误、非零/sample 首败叠加 record/close 错误、取消叠加关闭和 operation 记录错误。独立 OWNER 输出保留 PID、实际退出、三管道 closed 和 waitpid 已回收证据，不依赖被破坏的 record 文件。既有真实 SIGINT/EOF 非零上传回归保持。

## 冻结 60 秒角色与 300 秒清理

使用现有 BUDGET 和 plan 的严格相等/hash/输入校验，没有动态续期层：

- 单角色从前置身份检查开始，唯一截止为 `min(主900秒截止, 开始+60秒)`；exec 建立、真实 10 秒/6 样本、EOF/Stop/Wait、后置身份复核全包含在内。主截止在返回后恢复原值，不重启计时。
- 当前置/后置身份 CLI 拥有子进程时，预留该 CLI 的 4 秒 Stop/Wait。观察期间保留 4 秒 EOF 和 4 秒 Stop/Wait。observer 已 Join 后释放其 EOF 预留；后置身份检查仍在同一 60 秒范围内。余额不足、poll 返回时越界、后置检查超时均拒绝。
- 仅无负载预检使用同一起点的 300 秒清理，最终 Pod 身份查询也计入其中，不能消费 45 秒 namespace 预留。共享 cleaner 仍默认 180 秒；已有 cleanup-only 180/300 入口及最多 5 类顺序完整盘点、准确 UID 条件删除未修改。
- 10 秒采样、6 样本、2 秒间隔、每 sample 2 秒、256KiB 字段、4096 FD、64MiB 输出、普通 GET/exec request-timeout=10s、CPU/memory/身份和无重放要求保持。prepare 的静态阶段 → 唯一 120 秒资源窗口保持。

离线回放使用保留的真实成功 GET106–108 **11.863840416 秒**、GET109–111 **8.571337417 秒**，其余明确为受控模拟：exec 建立 2 秒、完整采样 10 秒、关闭 1 秒，总计 **33.435177833 秒**。保存的原源码 30 秒路径拒绝；新 60 秒路径按相同步骤完成。模拟到达阶段截止时返回不完整观察，不能伪造完整六样本。真实本地管道的 10 秒/六样本回归另行通过。

负例包括要求超过 60 秒的样本、主 900 秒窗口余额不足、慢前后身份检查和取消；已创建的模拟 owner 均关闭，未满足余额时零创建。另一个 30 秒后置身份检查场景在 54.863840416 秒完成，证明已 Join 的 observer 不再占用 EOF 预留。实际 observer 的输出超限和磁盘故障回归均收尾。

300 秒清理通过完整六对象本地 CLI 夹具回放；已消费 255 或 300 秒时拒绝删除，45 秒预留和原截止均不移动。这些模拟和上一轮仅两个残留的真实清理都不等于下一次完整 EKS fixture 的清理资格。

## 回归与封存

实现/最终受测源码 `b61194f2f5e576798783bc0e6d17281430363c05`；4 个 Python 文件的最终 hash 与该提交及工作树一致。

| 范围 | 通过 | 既有 skip | unittest 耗时 |
| --- | --- | --- | --- |
| 受影响普通 | 63 | 0 | 22.484秒 |
| 受影响优化 | 55 | 8 | 18.986秒 |
| 全 scripts 普通 | 235 | 0 | 210.754秒 |
| 全 scripts 优化 | 210 | 25 | 208.393秒 |

聚焦模块为 observer_lifecycle_test、eks_resource_preflight_test、resource_local_test、capacity_cleanup_test；全套命令为 `python3 [-O] -m unittest discover -v -s scripts -p '*_test.py'`。每个 runner 外层不超过 360 秒、已 Wait 且输出文件句柄关闭；不重跑 Go/race/DB/镜像。旧清理、串行角色、双 EOF、正常 end、禁止重复调用、120 秒资源窗、无重放及共享 Docker 调用点回归保持。

证据根 `.testdata/m30r5r/` 包含冻结计划、原源码、独立诊断输入、修前/修后真实探针、预算回放、测试日志与 runner 生命周期、独立进程/句柄记录、输入核验、最小 diff 及逐文件 hash/size manifest。manifest 排除自身和避免自引用的 delivery.json。

M30R5 **168** 份封存文件及 manifest `6cf97cd19e30e0c1f5d537d50b0e5a4a7db6320032750190d0970c406175fb24` 已重算；此前 **1809** 份历史不变。204 Go/module、Weir70/helper81 正式输入逐文件保持 image source `4abc8761f9f0e08af978d5ae5c14176f8188cfa3`。

旧 namespace 已清理的事实保持，本轮不重建、不查询、不新增远端清理结论。Auth 排除、ProgramTransform 首版延期、CNI/多节点、六平台、规格/后端、容量/恢复/24h 门槛均不变。本地 main 提交并确认干净、自有进程/句柄收口后，向统筹发一次完成回调并停止；没有定时器或后续派发。
