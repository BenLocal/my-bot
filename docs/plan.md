# my-bot 实施计划

> 2026-10-21 制定，依据 `docs/spec.md`（下文 §x 均指 spec 章节）与流程走查 `docs/flow.md`（F1…F12 为流程，G1…G18 为走查发现的缺口）。spec 是契约，本计划只回答「按什么顺序做、每步怎么验证」。
> **当前阶段：A 流程定稿与 P0 核验均已完成（2026-10-21），结论已写回 spec；下一步是 M0。尚未开始写正式代码。**实施中发现 spec 有误或缺口，先改 spec 再改代码。
> 每个任务完成的定义（DoD）：代码过 `gofmt -l . && go vet ./... && go test ./...`；并发相关加跑 `go test -race`；逐条对照 `docs/agents-dot-md/code-checklist.md`；涉及契约变更时 spec 已同步。

## 总览

```
A 流程定稿 ──► P0 核验 ──► M0 骨架与语义（fake 执行器） ──► M1 Pi + Codex + L1 记忆 ──► M2 k3s 常驻 + L2 记忆 ──► M3 / M4
  (≈1d)        (≈2d)          (≈2w)                           (≈2w)                         (≈1.5w)
```

工期为单人粗估，只用于排序和发现超支，不是承诺。

关键路径：P0 → T1 core → T4 store → T8 GoalWorkflow → T13 e2e。`core`（T1–T3）、`a2a`（T5）、`scheduler`（T7）可以并行。

---

## A 流程定稿（不写代码）

目的：把 `docs/flow.md` 走查出的缺口全部落进 spec，让后续每个任务都有确定的契约可依。

| 步骤 | 内容 | 产出 |
| --- | --- | --- |
| A1 ✅ | 采纳两项架构决定（2026-10-21，按建议推进，用户可否决）：**G3** 单个 `Scheduler` Actor 代替三级 SlotPool；**G8** 工作区放共享卷（单节点 hostPath）代替每 Pod PVC + 跨 Pod 产物拷贝 | 决定记录在 flow.md 与 spec §1.3 |
| A2 ✅ | 把 16 项「修正」类缺口写进 spec：G1/G2（§8 创建流程）、G4/G5/G14/G15（§5.1 改写为 Scheduler）、G6（§5.2）、G7（§2、§6、§7.1）、G9/G10（§8 请求、§7.4）、G11/G12（§5.2、§8）、G13（§7.2）、G16（§5.3）、G17/G18（§14） | spec 更新 |
| A3 ✅ | 按 A1 结果改写 spec 受影响章节：§5.1、§7.1 产物交接、§9.2 存储、§13 风险表 | spec 更新 |
| A4 ✅ | 复核：flow.md 中每个流程在更新后的 spec 里都能逐步找到对应条款；§12 验收条件补上新增行为（孤儿 Attempt、等待中取消、会话亲和、drain） | flow.md 缺口表标记「已写回」 |
| A5 ✅ | 按定稿后的 spec 修订本计划（尤其 T7、T8、T14 与 e2e 用例表） | plan.md 更新 |

A 完成标准：flow.md 的 G1–G18 全部标为「已写回」（已完成）；用户确认 spec 定稿（待确认）。

---

## P0 实现前核验（spike，代码不入主干）

目的：把 spec 中所有「实现前核验」变成确定的版本和写法，结论写回 spec 对应章节与 `tech-stack.md`。每个 spike 是放在 `spikes/<name>/` 的最小程序，验证完删除或移到 `docs/memory/` 记录结论。

| # | 核验项 | 要回答的问题 | 结论写回 |
| --- | --- | --- | --- |
| P0-1 | Dapr 版本 + Go SDK | workflow / actor / state 事务 / ETag / `ttlInSeconds` 在同一版本的 Redis 组件上能否同时使用；actor state 与普通 state 共用一个 Redis 时的 key 前缀 | §6、tech-stack |
| P0-2 | Dapr Go Workflow 细节 | ① 并发 fan-out / fan-in 写法；② `WaitForExternalEvent` 能否 0 超时非阻塞读取；③ `ContinueAsNew` 时未处理的外部事件是否保留（有无 keep-unprocessed 选项）；④ `when_any(event, timer)` 写法；⑤ Activity 的 per-call RetryPolicy | §5.2、§5.3 |
| P0-3 | a2a-go | 锁定版本；Task 型 `SendMessage`、`GetTask`、`CancelTask`、SSE 订阅；如何放 `metadata["mybot/*"]`；能否挂自定义方法 `mybot/getByAttemptKey`（不能则改为 Worker 的普通 HTTP 接口） | §7.1 |
| P0-4 | `pi --mode rpc` | 消息格式、会话续接、权限/自动批准、用量是单次还是累计、是否读取宿主配置及如何关闭、退出时 jsonl 是否已 flush | §7.2 表 |
| P0-5 | `codex app-server` | 同 P0-4；另确认一个进程多 thread 的并发行为、设备码登录与 `CODEX_HOME` | §7.2 表、§7.3 |
| P0-6 | 自托管 mem0 | REST 的 add / search / delete、`infer=false`、按 `agent_id` 与 metadata 过滤、相似度分数含义；从 k3s Pod 访问宿主机 docker 服务的地址 | §14.5 |

P0 完成标准：上述 6 项结论已写回 spec，spec 中不再有指向 P0 的「实现前核验」字样。

**状态：✅ 已完成（2026-10-21）**。锁定版本：Dapr runtime 1.18.7 / CLI 1.18.2、go-sdk v1.15.0、durabletask-go v0.12.6（随 Dapr）、a2a-go/v2 v2.6.0、pi 1.3.0、codex-cli 0.163.0。对计划影响最大的三条结论：
- Workflow 等待统一用单事件名 `signal` 的 inbox 写法，`ContinueAsNew` 带 `WithKeepUnprocessedEvents()`，不需要控制命令队列（spec §5.2）。
- Redis 上的 State 事务失败不回滚，跨键更新改为顺序写 + 幂等 + 对账（spec §6）；对已完成实例再 `ScheduleWorkflow` 会重新执行，补启动前必须先查实例（spec §8）。
- codex 的 threadId 只能由服务端生成，Recover 对非完成的轮次一律记 `interrupted`（spec §7.2）。

---

## M0 骨架与语义（只用 fake 执行器）

目标：Runtime 的状态机、调度、幂等、恢复语义全部用 fake 执行器跑通，验收 §12 第 1–5、12、13 条。

### T0 仓库骨架
- `go mod init`（module 按 §2）；目录按 §2.1 建空包；`Makefile`：`fmt`、`vet`、`test`、`test-race`、`build`、`dev-up`（起 docker Redis）、`dev-down`。
- `deploy/dapr/local/`：`statestore.yaml`（`actorStateStore: "true"`）、`pubsub.yaml`、`config.yaml`。
- `config/roles.yaml` 示例（§4.1），`roles/*.md` 占位。
- 引入首批依赖并记入 tech-stack：Dapr Go SDK、a2a-go、ULID、HTTP 路由（标准库 `net/http` 1.22+ 路由优先，不再引第三方）、日志（`log/slog`）。
- 验证：`make build` 通过；`make dev-up` 后 `dapr run` 能起一个空 Runtime。

### T1 core：模型、ID、状态机
- 文件：`internal/core/{model.go,ids.go,state.go}`。
- 模型：§4.1 全部对象、`GoalSpec`、`GoalState`（§5.2）、`Budget`、`StepState`、`StepResult`、`ArtifactRef`。
- ID：`NewGoalID()`（ULID，只在 API 层调用）；确定性派生 `AttemptKey(goal, round, step, attempt)`、`AskRequestID`、`EventID(goal, seq)`、`SessionID(goal, role)` / `SessionIDFresh(attemptKey)`（uuid5）、`MemoryEntryID(goal, round, n)`。
- 状态机：Goal / Step / Attempt 的合法转换表 + `Transition(from, event) (to, error)`。
- 测试：表驱动覆盖所有合法/非法转换；ID 派生稳定性（同输入同输出）。

### T2 core：策略与汇总
- 文件：`internal/core/{strategy.go,strategy_single.go,strategy_pipeline.go,strategy_rr.go,aggregate.go,stop.go}`。
- 实现 §5.0 的 `Strategy` 接口与三种确定性策略；`AggregateRound(steps) RoundStatus`；判停 `ShouldStop(state, now) (bool, reason)`（§5.2 第 2 步，含 FailStreak / NoProgress / 预算 / 期限）；`NoProgress` 规则（§7.4）。
- 测试：round_robin 成员顺序与轮次映射；pipeline 产物向下一角色传递；各判停条件。

### T3 core：大脑决策
- 文件：`internal/core/decision.go`、`decision_schema.go`。
- `ParseDecision(raw string) (Decision, error)`：确定性修补（去围栏、取最后一个 JSON 代码块 / 首个完整对象）后严格解码（禁止未知字段）。
- `ValidateDecision(d, ctx) []Violation`：§5.3 第 1–6 条，错误信息可直接回传大脑（规则编号 + 字段 + 期望）。
- `DecisionSchema()`：由 struct 生成 JSON Schema（库选型记入 tech-stack），供提示词使用。
- 包含 `memories` 字段的结构与基础校验（长度、topic 格式），凭据扫描放 T-M1 记忆任务中复用。
- 测试：合法 dispatch / finish / ask_user；每条校验规则的反例；围栏、前后废话、多个代码块等修补用例。

### T4 store：Dapr State 封装
- 文件：`internal/store/{store.go,index.go,records.go}`。
- 接口先定义在使用方（code-checklist G6），实现基于 Dapr client：`Get / Set / Delete`（带 ETag）、`FirstWrite(key, val, owner)`（冲突回读判 owner，§6）、`Transact(ops)`、TTL 写入。
- `MutateIndex(key, fn)`：读 ETag → 回调 → 未变不写 → 条件写；full-jitter 退避 50ms→1s，总预算 10s。
- 记录读写：goal 投影、事件（`EventID` 覆盖写）、`idem:create:*`、attempt 记录、注册表。
- 测试：单元测试用内存 fake（实现同一接口，带 ETag 语义）；集成测试（build tag `integration`，需 `make dev-up` + `dapr run`）验证真实 ETag 冲突、TTL 过期、事务。
- 索引清理任务 `PruneIndex(key, exists func(id) bool)`（§6，每 5 分钟由 Runtime 调度）。

### T5 a2a：协议封装
- 文件：`internal/a2a/{client.go,server.go,meta.go}`。
- 统一 `mybot/*` metadata 的编解码（attemptKey、goalID、round、workspace、sessionPolicy、inputs、traceparent 预留）。
- 客户端：`Submit`、`Get`、`GetByAttemptKey`、`Cancel`、`Subscribe`；超时与 Bearer token。
- 服务端适配：把 A2A 请求转成 Worker 内部接口调用。
- 测试：httptest 起服务端，客户端往返；metadata 缺失 attemptKey 时拒绝。

### T6 Worker（fake 执行器）
- 文件：`cmd/mybot-worker/main.go`、`internal/worker/{service.go,recover.go,registry.go}`、`internal/executor/{driver.go,fake/fake.go}`。
- `Driver` 接口与 `Status`（§7.2）；fake 驱动可脚本化：耗时、结果文本（含大脑决策 JSON）、失败及 `Retryable`、挂起、不发终态事件、重复回调、产物文件。脚本来源：提示正文中的指令块或按 role 的配置文件。
- `SendMessage` 流程 §7.2 第 1–3 步：FirstWrite → 会话 ID → Start → 终态先落盘再写记录 → 发布 `attempt-events`。
- `Events` 无终态即关闭 → `failed(driver_bug)`。
- 启动时 Recover（fake 驱动模拟四种判据）；注册与 15s 心跳（含 `authOK`）；产物写入共享卷并计算 sha256；下游 Step 启动前校验 `inputs` 的 sha256；Goal 级 `check`（`role=_check` 的特殊 Step，§7.4）。
- 本地开发时共享卷用一个本机目录代替，所有 fake Worker 指向同一目录。
- 优雅停机（§9.2 Worker 顺序）。
- 测试：重复 attemptKey 只启动一次；FirstWrite 冲突回读；无终态事件；Recover 四分支。

### T7 Scheduler Actor
- 文件：`internal/scheduler/{pool.go,actor.go}`。
- 纯逻辑 `pool.go`（可单测，不依赖 Dapr）：三级容量检查（全局、账号、Worker 实例）、按 attemptKey 幂等 Acquire、FIFO 队列、会话亲和（`PreferWorker` + `AffinityWait` 到期放宽并转 `fresh`）、round-robin 选实例、Cooldown、账号状态 `ok / needs_auth`、drain、lease 回收。注册表以参数传入，便于测试。
- `actor.go`：Dapr Actor 包装；每次调度读取注册表；满足排队请求时 `RaiseEvent(goalID, "slot-granted:<attemptKey>", lease)`；1 分钟 reminder（回收终态 lease、处理亲和等待到期、重试满足队列）；`Snapshot` 供 `/workers`。
- 测试：并发 Acquire 不超卖（race）；同 attemptKey 重复 Acquire；FIFO 公平；无「持有并等待」（一个 Goal 等 Codex 时不阻塞 Pi 任务）；冷却与 `needs_auth` 期间不发 lease；亲和命中 / 等待到期放宽 / 原实例注销；drain 后不再分配。

### T8 Orchestrator：GoalWorkflow 与 Activities
按以下顺序递增实现，每步都有对应测试再进入下一步：
1. `single` 策略单 Step 闭环：Scheduler `Acquire`（带 `PreferWorker`）→ `SubmitAttempt` → 等 `attempt-done` / 60s `PollAttempt` → `CollectStep` → `CollectRound` → 判停 → `ContinueAsNew`。
2. 重试与失败：Activity 分类型 RetryPolicy（§5.2 表）；不可重试错误走返回值；Step 内退避重试；`unknown` / `auth_required` → `waiting`；Role.timeout → `CancelAttempt`。
3. 等待与控制：实现 §5.2 的 inbox（单事件名 `signal`，按 `kind` 分派，非当前等待对象的信号进入待处理表）；**所有等待点**都只做 `WaitForExternalEvent("signal", 超时)`；每轮开头非阻塞读空 inbox；`ContinueAsNew` 带 `WithKeepUnprocessedEvents()`；cancel 的取消流程。
   孤儿判定：`PollAttempt` 连续失败且 Worker 注册记录过期超过 `orphanAfter` → `interrupted(worker_lost)`。
4. `brain` 策略：大脑 Step → `ParseDecision`（不重试）→ 校验失败回传重试（2 次计入 FailStreak）→ 成员 fan-out（≤ maxParallel，同 workspace 串行）→ fan-in → `LastResults`。
5. `ask_user`：确定性 requestID，等 `kind=user-input` 信号（超时 askTimeout），超时转 `paused`。
6. 投影与事件：每个 Activity 写 `goal:<id>` 与事件（`EventID` 覆盖写），事务更新 active 索引。
7. `FinishGoal`：终态投影、释放全部 lease、`goal.finished` 事件。
- 文件：`internal/orchestrator/{workflow.go,activities.go,prompt.go,retry.go}`；`prompt.go` 实现 §7.1 的两种提示词组装（M0 的 memory 两段留空）。
- 测试：Workflow 逻辑尽量抽到纯函数（下一步决策）单测；整体用集成测试 + fake Worker 验证；手工验证一次重放（重启 Runtime 后同一实例继续）。

### T9 attempt-events 订阅
- 文件：`internal/orchestrator/subscription.go`。
- 三态返回（§5.5）、死信 topic 组件配置、终态后才标记（首版无去重存储，仅保留结构）。
- 测试：解析失败 DROP；RaiseEvent 报错 RETRY；实例不存在 SUCCESS。

### T10 Runtime HTTP API
- 文件：`internal/api/{server.go,goals.go,events.go,workers.go,auth.go}`。
- §8 中 M0 范围的接口：创建（`Idempotency-Key` → `idem:create:*` FirstWrite，返回 `statusUrl`）、列表、详情、事件（JSON 与 SSE，`id:` 为 `<goalID>:<seq>`，支持 `after` 与 `Last-Event-ID`）、轮次详情、产物下载（只读挂载共享卷，校验 sha256）、pause/resume/cancel/append（统一 202，终态 409；64 KiB 上限，413；`replyTo` 409 规则）、`/workers` 与 `:drain`、`/accounts/...:ok`、探针、Bearer 鉴权。
- 创建流程按 §8：命中 `idem` 时确认 Workflow 实例存在，不存在则补启动（G1）；初始投影由 API 写（G2）。
- `DELETE`、`/memory` 留到 M1/M2。
- 测试：httptest 覆盖鉴权、幂等创建、413、409、SSE 续传。

### T11 Runtime 装配
- `cmd/mybot-runtime/main.go`：加载 `roles.yaml`（启动时一次，热更新在 M2）、注册 Workflow / Activities / Actor / 订阅、启动 HTTP、索引清理定时任务、优雅停机（§9.2 Runtime 顺序）。
- 验证：本地 `dapr run` 起 Runtime + 2 个 fake Worker。

### T12 本地联调脚本
- `scripts/dev/`：一键启动 Redis + Runtime + N 个 fake Worker（不同 app-id 与端口），以及 `curl` 示例。`mybotctl` 按 spec 放 M2；M0 用脚本和 e2e 测试代替。

### T13 M0 端到端验收
- `test/e2e/`（build tag `e2e`），由测试代码启动进程并在需要时 `kill` 进程：

| 用例 | 对应 §12 | 要点 |
| --- | --- | --- |
| 提交后断开客户端仍完成 | 1 | 客户端只发创建请求即退出，轮询结果 |
| 幂等 | 2 | 同 Idempotency-Key 两次；同 attemptKey 重复提交只产生一个 fake 会话 |
| 并发上限 | 3 | global=2，3 个 Goal；采样活跃 Attempt ≤ 2，第三个出现 `slot.waiting` |
| Runtime 重启 | 4 | 执行中 kill Runtime 再起；轮次与预算不变、无新增会话 |
| Worker 重启 | 5 | 执行中 kill Worker；Attempt 变 `interrupted` 或恢复完成 |
| brain 并行协作 | 12（fake 部分） | 大脑派 2 个成员并行，下一轮输入包含两者结果；换大脑角色的执行器配置后同一 Goal 仍完成 |
| 非法决策与 ask_user | 13 | 非成员角色 / 不存在产物 / 超 maxStepsPerRound 被拒并回传；ask_user → waiting → append 继续 |
| 创建崩溃窗口 | F1 / G1 | 在 idem 写入后注入崩溃，同 key 重试后实例被补启动 |
| 等待中取消 | F5 / G11 | Goal 在等槽位、等 Attempt、Loop 在等定时器时 cancel 立即生效 |
| 孤儿 Attempt | F8 / G13 | 永久停掉一个 fake Worker，`orphanAfter` 后 Attempt 判 `interrupted(worker_lost)` 并在其他 Worker 重试 |
| 会话亲和 | F2 / G5 | reuse 会话派回原 Worker；原 Worker 满载超过 `affinityWait` 时换实例并转 `fresh` |

M0 完成标准：上表全部通过，且 `go test -race ./internal/...` 通过。

---

## M1 Pi + Codex 真实执行 + L1 记忆

前置：P0-4、P0-5 结论已写回 spec。

### T14 工作区与产物（共享卷）
- `internal/worker/workspace.go`：共享卷上的 `/data/workspaces/<goalID>/<workspace>`；按 Goal `workspaces` 声明首次使用时克隆（git 凭据走 Secret）；同仓库并行写用 `<workspace>@<stepID>` worktree（§5.3 第 5 条）；产物写 `/data/artifacts/<goalID>/r<round>/<stepID>/` 并登记；下游校验 sha256（§7.1）。
- 本地开发时 `/data/*` 用可配置根目录替代。

### T15 Pi Driver
- `internal/executor/pi/`：按 P0-4 结论实现 RPC 会话、提示首行 `[mybot attempt <key>]`、事件映射到统一事件、用量增量、`Recover` 四判据、终态前等待 jsonl 落盘、不可重试错误分类。
- 测试：录制真实 RPC 输出作为 golden 文件做解析单测；一个需要真实模型的冒烟测试（build tag `live`，不进默认测试）。

### T16 Codex Driver
- `internal/executor/codex/`：同 T15，按 P0-5；一个进程多 thread；`CODEX_HOME` 与登录状态检测（`auth_required`）。

### T17 Loop
- every / cron（`Asia/Shanghai`）、`scheduledAt` 去重、`catchup: latest|none`（§5.4）。
- 测试：时间计算纯函数单测；e2e 用短间隔验证跨重启不重复（§12 第 8 条）。

### T18 L1 记忆
- `internal/memory/{l1.go,writer_actor.go,scan.go}`：主题文件格式（§14.2）、`MemoryWriter` Actor（临时文件 → fsync → rename、重建 `00-index.md`）、`CommitMemories` Activity（长度、topic、凭据扫描、规范化去重、`MemoryEntryID` 幂等）。
- 大脑提示词加入 `[memory index]`；Worker 把记忆目录只读挂到工作区 `memory/`（本地开发用只读 bind / 拷贝快照，k3s 用 hostPath ro）。
- `GET /memory?topic=&q=`。
- 测试：并发写同主题不交错；重试不重复追加；凭据样例被拒。

### T19 角色指令与场景
- `roles/planner.md`（含决策 schema 注入位）、`assistant.md`、`designer.md`、`coder.md`、`reviewer.md`；按执行器微调大脑指令的机制（同一角色可有 `planner.pi.md` 覆盖）。
- 场景验收（§11 M1）：
  1. 笔记摘要 Task；
  2. 每天 08:00 摘要 Loop（e2e 用短 cron 代替）；
  3. `brain` Goal：designer 出设计 → 并行 coder + reviewer → check 通过；大脑分别用 Pi、Codex 各跑一次；
  4. Goal A 写入的记忆在 Goal B 中被大脑 grep 到（§12 第 15 条的 L1 部分）。

M1 完成标准：以上场景通过；§12 第 8 条通过；真实执行器下重跑 M0 的第 4、5 条用例。

---

## M2 k3s 常驻 + L2 记忆

前置：P0-6 结论已写回 spec；k3s 与 Dapr 安装需要 root，按环境约定写成 `/tmp/*.sh` 交给用户在外部终端执行后再验证。

### T20 镜像与清单
- `build/`：`mybot-runtime`（distroless）、`mybot-worker-pi`（node + pi，npmmirror）、`mybot-worker-codex`；镜像导入 k3s（`k3s ctr images import` 或本地 registry，选一种并记入 environment）。
- `deploy/k8s/base` + `overlays/k3s`：§9.2 全部资源、Dapr Component / Configuration（含死信 topic）、Secret 模板（不含真实值）、Ingress、PVC、hostPath（用户数据 ro、记忆目录 Runtime rw / Worker ro）。
- `scripts/install-k3s.sh`、`scripts/install-dapr.sh`（交用户执行）。

### T21 运维能力
- `mybotctl`（§8 末尾命令 + `workers drain`）。
- `DELETE /goals/{id}` purge。
- webhook 通知 listener（§10）。
- `roles.yaml` 热更新（ConfigMap + fsnotify；转换 → 校验 → 失败保留旧值 → 敏感字段打码）。

### T22 可观测性与契约
- OTel OTLP：traceparent 持久化到 `GoalState` / Attempt，经 A2A metadata 传播，`gen_ai.usage.*`。
- JSON Schema 生成（决策、attempt-events、注册表记录、创建请求）+ `scripts/check-schema-compat`，接入 CI。

### T23 L2 记忆
- `internal/memory/{mem0.go,sync.go,recall.go}`：add / delete / search 客户端（2s 超时）；`memsync:*` 映射；同步任务；每 10 分钟及启动时对账；`RecallMemory` Activity（回读 L1 过滤、附出处）；`[recalled]` 注入；`memory.l2_unavailable` 事件；`/memory:recall`。
- 测试：mem0 用 httptest fake 覆盖新增 / 修改 / 删除对账；e2e 验证 §12 第 15–17 条。

### T24 扩容与 24h 浸泡
- §12 第 9 条：Pi Worker 1 → 2。
- §12 第 11 条：人为让 Codex 登录失效。
- §12 第 10 条：连续 24h，期间一个 brain Goal + 一个 cron Loop + 若干 Task；记录 Redis 内存、Pod 内存、模型调用次数（空闲期应为 0），结论写入 `docs/memory/`。
- 在 k3s 上重跑 §12 全部条目。

M2 完成标准：§12 全部通过并完成 24h 浸泡。

---

## M3 / M4（开工前再细化到 spec）

- **M3**：Claude Code Driver（参照 Dapr Agents `executors/claude.py` 的会话与审批处理）；原生 Go `llm` 执行器（只做规划的低成本大脑，评估是否用 Dapr Conversation API）；成员能力描述来自 Agent Card。验收：第三种执行器当大脑跑通 M1 的协作 Goal。
- **M4**：飞书入口（复用 pi-feishu 经验）；KEDA 评估。

---

## 风险跟踪（实施层面）

| 风险 | 早期信号 | 应对 |
| --- | --- | --- |
| inbox 写法下 Workflow 状态机复杂、重放出错 | M0 T8 | 把「收到信号后的下一步」抽成 `core` 纯函数并穷举单测；Workflow 只负责等待和调用 |
| a2a-go 不成熟或不支持扩展方法 | P0-3 | 按 attemptKey 查询改为 Worker 普通 HTTP 接口；必要时 A2A 只保留 Agent Card + SendMessage/GetTask |
| Pi / Codex 的 RPC 无法可靠判断「回答是否完整」 | P0-4/5 | Recover 退化为：有终态记录则取，否则一律 `interrupted` |
| fake 执行器掩盖真实时序问题 | M1 初期 | M1 第一周就用真实 Pi 重跑 M0 的重启用例 |
| 本地联调与 k3s 行为差异（sidecar、网络、hostPath） | M2 | M1 末尾先在 k3s 上部署一个最小 Runtime + fake Worker 冒烟 |
