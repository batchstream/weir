# M26R4 — 固定 SDK 禁用证明通过，本地 ES 启动失败

2026-09-28；执行聊天 `01a0e6d8-94e2-7151-807f-831f345f228d`。
基线 `a7d36817904ab76c7766041c9d7c296c14d815c3`，实现
`8a080b76693e4042fe0e45b900fbe9bf07283d46`；交付 SHA 记录于本地
`.testdata/m26r4/closeout.json`。任务 SHA256
`f6b44a5c80b6545f29dc758c23122f2dacf57f52e9e6da3f7e32aea589dda174`。

**ES 本地启动先决条件 NO-GO，因此没有进入 EKS。** SDK 禁用对照和离线接线通过，
不能替代真实 ES 就绪、网络边界或功能闭环。唯一 ES 尝试失败后未修改冻结计划或重跑。

实现只在共享 ES 构造增加 `AWS_EC2_METADATA_DISABLED=true`，共享 Go template 仅新增
此具名非秘密字段的投影。功能与只读入口复用同一构造，仍严格比较 env 值、类型、顺序、
容器、资源和安全字段；没有 region、凭据或线上 endpoint 设置。旧 plan 与新对象/输入不匹配，
不能冒充可重跑；历史对象仍由原 Git 提交和 hash 保留。产品 Go/helper/协议/模块/镜像/CI 未改。

固定 ES 源码的 [S3Service](https://raw.githubusercontent.com/elastic/elasticsearch/v8.19.22/modules/repository-s3/src/main/java/org/elasticsearch/repositories/s3/S3Service.java)
启动 default-region holder；[holder](https://raw.githubusercontent.com/elastic/elasticsearch/v8.19.22/modules/repository-s3/src/main/java/org/elasticsearch/repositories/s3/S3DefaultRegionHolder.java)
捕获 provider 异常，延迟到使用 region 时记录警告。这是候选探测路径，不能确定归属 M26R3
那三条连接。固定 [SDK provider](https://raw.githubusercontent.com/aws/aws-sdk-java-v2/2.31.78/core/regions/src/main/java/software/amazon/awssdk/regions/providers/InstanceProfileRegionProvider.java)
在 true 时先抛出禁用异常；以下实际镜像对照验证了该行为，未访问真实 IMDS。

证据根目录 `.testdata/m26r4/` 为新 0700 目录，两个本地 plan 均 0400。
固定 ES manifest `c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`，
实际 image/config `a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160`。
缓存命中、无 pull/build；新空 Docker 配置，没有已有秘密挂载。实际环境 Linuxkit
`7.0.12/aarch64`，Docker 29.7.2，ES 内置 OpenJDK `27+35-2325`。

SDK plan SHA256 `7640d0ec46df684601e7cf2cef4354d4724e5b0e11bab9968b1dda1de99912c2`。
`sdk/command-0004.out` 保存完整 S3 模块 jar hash、27 个 AWS jar 与固定官方校验文件匹配、
实际 provider/core CodeSource；classpath 是镜像内 `modules/repository-s3/*` 与 `lib/*`。
`regions-2.31.78.jar` SHA256 `b69dfc83837e9d6126934b7dc3375f531c216ebff494fd6d6c636f3ba6dd0057`；
`sdk-core-2.31.78.jar` SHA256 `192c9c55bd9cba7ae0ee8667e58c9edcce795b02ed4e671d45323f6d2222f0f6`。
固定源码副本及校验文件 hash 在 `sources.json`；没有替换 SDK。

单个自有容器 network=none、1CPU/512MiB、非 root、drop ALL、无提权、只读 root，
临时空间 64MiB，无 host 端口。一次 shell 中分别启动两个全新 JVM，清空继承环境，
使用新空 Java user.home；仅在此探针将 SDK endpoint 覆盖为同进程的 `127.0.0.1` 假 HTTP 服务。
false 时观察到 `PUT /latest/api/token` 和 `GET /latest/dynamic/instance-identity/document`，
共 **2 请求**，返回固定合成 region；true 时 **0 请求**，明确返回 `EC2 Metadata is disabled`。
两 JVM 均 exit0，整个命令 2.858 秒，符合各 60 秒/合计 120 秒预算。两次 stderr 都保留
hostname 解析警告；没有把 stderr 说成空白。该警告未使 SDK 探针失败。

ES plan SHA256 `4ff18be41f3159137b47550b8be4d9754d6d0034e8e36548ae9f130b96a2fd83`。
唯一真实启动保留原 ES args/user，3CPU/3072MiB、heap1024MiB、data tmpfs ≤1GiB，
network=none、无主机端口，启动配置包含准确 true。2.403 秒内发现 ES 已 exit1；
首次只读 loopback GET 的 curl exit7 亦保留。`es/command-0009.out/.err` 显示容器 hostname
无法解析，随后 `LogConfigurator.checkErrorListener` 报
`status logger logged an error before logging was configured`，ES 在 initPhase1 退出。
终止状态不是 OOM/restart。容器没有非 loopback 地址可供自身 PodIP 负向探测；HTTP version/settings、
socket 采样及完整 ES runtime 验证都未到达，不能声称它们通过或由此证明终生无外连。
没有修改 hostname 再试，也没有去线上调试此本地夹具缺口。

| 离线检查 | 结果 |
| --- | --- |
| 全 Python 普通 | 130 通过、0 skip，101.539 秒 |
| 全 Python `-O` | 130 发现、105 通过、25 既有 skip，97.131 秒 |
| 最后仅命名变量整理后的定向检查 | 普通与 `-O` 各 3 通过、0 skip |
| 真实 API 形状回放 | 两种 dry-run → create → bootstrap/main 身份与边界 → 两 trial → 六对象 UID 清理 |
| 新 flag 反例 | 两 dry-run 阶段共 20 组缺失/false/大小写/类型/空值/顺序/额外 env/字段/valueFrom，零持久 Job |
| 实际 Go template | 普通/init 投影保留 true、未知 env 值脱敏；七组投影后反例仍拒绝 |
| 失败接线 | bootstrap23、UNKNOWN、真实 SIGTERM 取消子进程、cleanup 拒绝；保留原错误/预算、停止后续 trial |

首次定向回归有一处新测试断言错误：读取不存在的 cleanup.errors；按实际 resources[].error
修正后最终全套通过。原日志 `offline/focused.log` 保留。这不是产品或原生试验失败的改写。
已有 quantity/probe/资源/UID 回归不减。实际执行 shell，未只检查模板字符串。
`replay/` 从 M26R3 原日志按长度提取四张表：完整三条连接的同一快照，以及各条独立保留的
三个快照，旧/新 shell 均 exit23、旧/新 Python 均拒绝。没有修改地址、端口、UDP、FIN_WAIT2、
零 inode 或退出规则；原 socket/bootstrap/diagnostic shell 字节不变。

本轮 EKS plan/invocation/namespace/Job/Pod、真实 Weir/helper、trial、管理写、seed、文档 mutation、
测试操作 UNKNOWN 均为 **0**；没有调用实际 kubectl/aws。原两 trial 的 6000 planned/2400 mutation
预算未启用或预留，不能将合成离线样本计作真实操作或资源资格。

SDK container `7186391be77b6593ec29c66b880e61c9b7b65890fce36bdd5de781e967ccef94`；
ES container `a89f1b5131e80d519a6da831c6ccc9c27dccd2bb6839667845bb1949d68563da`。
二者均按创建返回的精确 ID Stop → Wait → 确认 Running=false → rm；SDK exit0、ES exit1 原样保留。
两个 owner 查询均空；没有创建 Docker network，前后容器/网络 ID 集合一致。31 条本地预检/原生命令
全部有 end/exit 记录，唯一 CLI 非零是上述 curl7；没有用 CLI exit0 掩盖 ES 进程 exit1。
所有自有 fixture 与验证子进程已结束，未留后台负载。无新 EKS namespace，因此没有虚构 UID
删除或 namespace 查询证据。200 项固定产品输入与 3,120 份既有证据 hash/长度不变，见 `integrity.json`。
本轮未重跑 Go 产品资格；模板测试使用固定 Go1.27.1 的标准库，不算产品资格。

candidate=null；功能闭环仍未通过，容量/跨节点/过载/恢复/24h not-run，CNI 隔离 unqualified。
startup-signal-registration-window/exit-15 仍未解决；Weir 认证排除、通用 ProgramTransform 首版延期。
本地 main 提交干净、没有 push/CI/发布；完成回调后停止，不打开下一阶段或恢复定时任务。
