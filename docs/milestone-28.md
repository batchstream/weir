# M28 — 无特权资源采样：实现交付，原生完整性 NO-GO

2026-09-28。**本阶段未取得 resource-complete。** 三次冻结的本地启动均在负载前停止；
两次允许的启动接线修正已经用完。最后发现的 JVM 参数匹配问题已修复并做离线回归，
没有第四次容器启动、原生重跑或容量试验。candidate 仍 null，计时/容量/soak 均 not-run。

## 源码与产物

- baseline：`6bdf5d230689761852742a7bb6324af8f7485821`，干净 main。
- 原生实际产物源码：`939e331ea42d408286c65721e385f446ba57d1e6`。
- 夹具接线提交：`b40f7a9`（只读根上的临时配置）、`99334a5`（尝试目录类型过滤）、
  `9564acd`（原启动预算内等待 ready）。均未改变受测 Go 输入/二进制。
- JVM 匹配离线修复：`ab5c79b4d0d42ea51f8f2db08b3270f5f4953f5e`；随后保留历史观察入口的
  1350 个样本/2698 秒上限，避免把本阶段的短窗口限制改成既有负载合同的回归。
  最终 implementation 为 `027d7c85467d5ef4da1e164b7ff81b85ed5e3070`；delivery 完整 SHA、
  源码输入核对和证据 manifest 在 `.testdata/m28/delivery.json`。
- 原始证据根：`/Users/liran/Projects/liran/go/weir/.testdata/m28/`。
  `source-inputs.json`、`build-plan.json`、`artifacts.json` 对应真正运行过的初始产物；
  后续 `final-build*/` 仅为离线修复源码的编译记录，**不是原生运行证据**。

| 实际运行的 Linux arm64 CGO0 产物 | SHA256 |
| --- | --- |
| Weir | `337f46ef3cfcfb8ba93f5d51025291e43efa71ed0dc2a2f69c912108849459e2` |
| helper/client | `2cf89c53bf8b6c45e981c6f5a2206e5618c8b437c8f8a446cdfb6b1bf0b4007d` |
| 采样测试 binary | `b0b9006c1d51b37601fa395499fccd990588795221d93e2c4e6e96fbf0e43eaf` |
| app 测试 binary | `d2df6a2c9cd3bae1e54cb6a22288e026b9c2256d4b37feef68e9ca6564f9de18` |

固定 Go1.27.1，构建在 Darwin arm64，运行在同架构 Docker VM 的 Linux7.0.12-linuxkit/aarch64。
没有把交叉编译当作 native。执行环境旧 image `sha256:ef36debc338afa91481a64a435dbe23400f6252742ff63aaca45cfeedcaebdd9`
只提供运行环境，新二进制从自有只读目录执行。ES 仍为准确 manifest
`sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`、
config `sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160`，
ES8.19.22、bundled OpenJDK27 `27+35-2325`；没有升级依赖、JDK 或 DB。

## 最小产品变化与观察边界

产品只在每个 Node 的私有 Prometheus registry 注册一次标准 Go/process collector，
不按 Store 重复注册，不连接数据库，不新增接口/配置/监听器。中英文架构同步了这个实际变化。
调度、协议、DB 语义、AIMD、resource Guard 和容量门槛均未修改。

integration helper 的原跨 `/proc/<pid>/root` observer 已替换；旧 root、共享 PID、
SYS_PTRACE/DAC_READ_SEARCH 容器启动路径被删除。现在通过准确自有容器的 `docker exec -i`
以目标相同非 root UID 执行，读取自己的 `/sys/fs/cgroup`；目标必须与 observer 同 UID、
PID/mount/cgroup/network/user namespace 和可见 cgroup。仅接受当前有限 cgroup-v2 leaf profile，
隐藏祖先保持 unknown；这不是全部 cgroup 布局或 Kubernetes 的资格。

Weir 显式 role/PID，ES 在本容器有限进程集合中按已知服务器 main marker 选择唯一 JVM；
不把 launcher 当服务器，不导出环境或任意 cmdline。PID/start ticks、binary SHA256、UID、
cgroup 和 namespace 标识在采样前后核验。退出、身份变化、权限/读取错误、重复 JVM、
计数倒退都保留错误并停止；不能复用旧样本或把未知值写成零。

| 指标来源 | 单位与语义 |
| --- | --- |
| `/proc/<pid>/{status,stat,fd}` | RSS 将明确的 kB 换成 bytes；FD/线程为 count；CPU 保留 user/system USER_HZ ticks |
| 自身 `/sys/fs/cgroup` | memory.current/max/events、cpu.max/stat（usec/throttling）、pids.current/max、可见 cpuset、io.stat；计入同容器 helper |
| 标准 Go/process collector | Go heap/goroutine 与平台进程 RSS/CPU/FD 分列；既有账本与连接指标继续保留 |
| client self sampling | 实际 trial 进程自己的 identity、资源、goroutine、GOMAXPROCS、Go heap；本轮 trial 未到达 |
| ES `_nodes/_local/stats` | JVM heap/threads、进程、线程池和 HTTP 连接；本轮 ES observer 未成功选中 JVM，未取得这些完整样本 |

不相加 RSS、Go heap 和 cgroup memory；不将 Darwin footprint 当 RSS。TCP 表属于共享的
loopback **network namespace**，不能当作单个进程独占连接。合法空 io.stat 与缺失/失败分开；
ES tmpfs 下缺少 fs.io_stats 仍是 unknown，不能伪造磁盘 I/O。CPU 百分比不能猜 USER_HZ；
fixture 的 `getconf CLK_TCK` 实测为100，原始 tick 与已核验时基分开保存。

每2秒采样，最大 gap6秒、sample2秒。单文件/HTTP256KiB、headers16KiB、FD4096、
进程目录1024、executable128MiB、单 JSON record1MiB、每输出流64MiB；HTTP固定 fixture loopback、
1秒期限/单连接、无代理/重定向。M28实际观察计划140秒/71样本，完整性入口最多450样本，
每个真实 fixture 的命令/原始证据上限沿用8MiB/128MiB。
helper软预算 RSS96MiB、最大采样区间 CPU0.25 core；开销包含在目标容器配额内，另列而不扣除。
单个历史观察调用仍有1350样本硬上限，本阶段没有运行该长窗口。

标准管道用自有 nonblocking duplicate 交给 Go poller，实现1秒输出 deadline；stdin EOF
取消 observer，Close 后 join 唯一控制 reader。没有遗留写 goroutine 来伪造超时。
/proc/cgroup 是有界本地内核文件读取，异常内核调用不具备 Go context 可中断承诺；
外层 fixture 负责独立期限和准确 owner 回收。

`resource_report.py` 独立读取原始 JSONL，核验序号/终止计数、身份、原始值/单位、计数、
各流单调时间和 UTC 一致性；跨流覆盖使用 UTC，不能相减不同进程的 epoch。
必须覆盖负载前、中、后和所有角色，否则 partial/NO-GO；峰值只能叫 sampled maximum。
采样完整性与原5ms发生器门槛、吞吐/容量判定分开。

## 三轮实际结果与不可改写失败

所有计划在 container create 前冻结 source/制品/config/命令/配额/期限，见 `fixture-{1,2,3}/plan.json`。
共用同一个900秒总窗口，清理分别受120秒限制；从首轮窗口开始到第三轮最后清理命令结束
336.871秒。没有重置总期限，也没有借重跑产品/安全/资源失败寻找通过结果。

每轮 ES 是 network-none 的 namespace owner，Weir/client 只共享其 loopback 网络。
独立 PID/mount/cgroup；ES1000:0、Weir/client65532:65532、dropALL/no-newpriv、只读根、
有限 tmpfs/pids/log、no-swap、无 publish/host 网络/外连。Weir2CPU/1GiB、ES3CPU/3GiB
（heap1GiB/data tmpfs1GiB）、client1CPU/512MiB；没有独立 observer sidecar 配额。
自身 hostname→127.0.0.1、`AWS_EC2_METADATA_DISABLED=true`、版本/settings/socket guard 保持。

| 轮次 | 结果 | fixture / cleanup 秒 |
| --- | --- | --- |
| 1 | ES 为配置目录临时 keystore 写入报只读文件系统，exit74；Weir/client未创建。唯一容器已回收；原 cleanup=false 因 network inventory ID 变化保留 | 4.220 / 0.198 |
| 2 | 固定三份公开配置复制到既有 `/tmp`，ES/guard/原生测试通过；Weir启动后约42ms即探测，早于 ready，原 probe exit1，随后自有 stop 时 Weir正常exit0；未进入负载 | 11.760 / 2.745 |
| 3 | 原5秒 startup预算内等待ready后到达观察阶段；ES observer因裸类名匹配未找到模块形式 JVM，exit1，立即取消另一observer并清理；未进入负载 | 11.951 / 3.201 |

轮1错误原文在 `fixture-1/command-0015.err`。可写配置只来自固定公开镜像的
`elasticsearch.yml`、`jvm.options`、`log4j2.properties`，逐文件限制/哈希，位于已有有限tmpfs；
保留正常 entrypoint、只读根和原 flags，没有读取/导出已有 keystore、凭据或其他秘密。
原网络快照只含 ID；host/none/既有非默认网络 ID 不变，新默认 bridge 创建时间与首次 create 重合。
这与既有工具已单列的 Docker 默认 bridge 重建行为一致，但旧快照缺名称，**不倒写原 inventory 成功**。
新快照记录名称与 ID，任何非默认网络变化仍失败；第二、三轮无变化。没有网络修改/恢复命令。
独立精确 ID 查询证明轮1自有容器消失，保留 `owned-cleanup-proof.json` 与原 failed result。

轮2之前另有一次仅入口失败：`fixture-*` 同时匹配封存 manifest 文件，未创建目录/plan/容器，
修正为只选择目录；原 `native-2.log` traceback 保留，实际第二轮在 `native-2-execution.log`。

轮3的原因有固定镜像 build revision 对应的
[Elasticsearch ServerProcessBuilder 源码](https://raw.githubusercontent.com/elastic/elasticsearch/3b2a41103de35e0af4064d647974032fcc1bcde9/distribution/tools/server-cli/src/main/java/org/elasticsearch/server/cli/ServerProcessBuilder.java)
佐证：启动参数是模块限定 main。最终 helper 接受准确 bare/main-module 两种 token，并拒绝
launcher/嵌在无关参数里的 marker。原现场未导出 cmdline，根因以源码和离线正反例交叉判断，
修复后的真实 JVM 选择仍未原生重验。第三轮 Weir 只有一份被取消的样本（57.384ms、
目标 RSS10,780,672 bytes、observer RSS11,120,640 bytes），metrics HTTP被取消，不能计为完整样本，
更不能当资源峰值或开销上界。

`operation-audit.json` 从133条全部有 exit 的实际 CLI 记录重算：
**trial/setup 命令0、seed0、planned load0、document mutation0**；仅第二、三轮各一个
空 records index 的管理 PUT。没有50ops/s计时结果、through/direct对照或吞吐结论。
由原始流计算的 `fixture-3-computed-report.json` 为 resource_evidence=partial，明确缺少 ES 流，
不是手填 complete。原有本地5ms计时失败和M26历史证据均保留，不用本轮未运行覆盖。

## 回归、清理与限制

最终源码：固定 Go1.27.1、GOPROXY=off/GOSUMDB=off/GOTOOLCHAIN=local/GOENV=off/GOWORK=off。
默认测试完全离线；真实 Linux/DB 入口需要显式 integration opt-in。

- Darwin arm64 CGO0默认、CGO1 race各220个顶层PASS；采样/解析/collector三轮race33个顶层PASS。
- 默认vet、integration vet，以及Linux/macOS/Windows × amd64/arm64 的产品cmd与integration静态编译通过。
- 受影响Python普通37通过；优化模式17通过、20个既有入口限制skip；没有重复不相关的全部历史EKS套件。
- 第二、三轮各11个原生采样/解析/权限/HTTP/真实管道测试和一个标准runtime collector测试通过。
  这是初始准确 binary 的Linux CGO0执行证据；不是Linux race、最终JVM修复、完整Weir/ES/client采样或DB负载资格。
- 开发期语法/测试配置、旧synthetic DB样本角色，以及Darwin继承管道deadline失败均有原始日志；
  后续通过不覆盖失败。非blocking duplicate 的真实子进程阻塞/Wait在Darwin race和原生Linux均验证。

第三轮两个 docker exec 都在容器 stop 前取消并Wait，退出1原样保留；随后准确ID/owner
Stop→Wait→停止确认→remove。三个轮次共7个容器，最终再次按7个ID和3个owner查询均为空；
两个已Wait的宿主exec PID也已消失。见 `final-cleanup.json`。没有新网络、volume或后台fixture残留。
最终默认/三轮race子进程由测试Wait回收；最终main干净后仅发送一次失败交付回调并停止。

后续EKS布局只能由下一独立阶段决定：测试专用只读artifact注入各容器，在其自身namespace/cgroup
按相同UID执行观察器并收集有界流；helper不进入生产镜像，不使用hostPath/hostPID/ptrace特权，
不建设远程安装器。本阶段没有EKS调用、push、CI、镜像发布、节点或全局设置修改。

公开GHCR仍是M24 source `fc0eb867ac4511a5c29dbc32b02768a3ad7a3139`；M27/M28未发布。
M27已获统筹独立有限验收。本轮M28仍NO-GO，容量校准前仍需新的独立授权阶段补全真实三角色采样。
CNI/跨节点、完整性能/六平台、其他资源规格、后端安全及24h仍required；认证排除，
通用ProgramTransform首版延期UNSUPPORTED，无定时任务恢复或自行开启下一阶段。
