# M0 开发决策与排查记录

> 记录 M0 阶段关键架构决策、踩坑与修复结论，避免后续会话重新推导。

## 双 Pool 问题根因与修复（2026-10-21）

**现象**：Workflow 直接调 `SubmitAttempt` 绕过了 Scheduler actor 的调度权威，导致并发上限不生效。

**根因**：`main.go` 中有独立的 `scheduler.NewPool()` 传给 `orchestrator.SetDeps`，同时 `SchedulerActor` 内部也有自己的 pool。两个 pool 状态不共享，Workflow 通过 actor 的 pool 拿 lease，但实际提交时用的是另一个 pool 的 Worker 信息。

**修复**：
- `SubmitAttempt` 不再操作 pool，改为接收 lease（WorkerID/Executor/Account/SessionPolicy）作为直接输入
- 新增 `AcquireSlot` / `ReleaseSlot` activity，通过 `daprClient.InvokeActor()` 调用 Scheduler actor
- `workflow.go` 实现 inbox 模式：`PendingSignals []*Signal` 切片（非 map，保证 Dapr 确定性），`drainSignals()` 每轮开头非阻塞读取
- `acquireThenSubmit` helper：AcquireSlot → 等 `slot-granted` → SubmitAttempt → PollAttempt → 终态后 ReleaseSlot

## Workflow inbox 模式设计决策（2026-10-21）

**为什么用单事件名 `signal` + inbox 切片而非 map**：Dapr Workflow 要求确定性重放。map 遍历顺序不确定，会导致重放时行为不一致。改用 `[]*Signal` 切片，按 FIFO 顺序消费，每轮开头 `drainSignals()` 非阻塞读取全部待处理信号。

**关键约束**：
- `WaitForExternalEvent("signal", 0)` = 非阻塞读取
- `WaitForExternalEvent("signal", <负数>)` = 无限等待
- `ContinueAsNew` 必须带 `workflow.WithKeepUnprocessedEvents()` 保留未处理事件
- 所有等待点统一用单事件名 `signal`，按 `kind` 字段分派

## Actor ID 与 spec 同步（2026-10-21）

**问题**：代码中 `ActorIDScheduler` 从 spec 规定的 `"global"` 改为了 `"scheduler"`，导致代码与契约不一致。

**修复**：已改回 `"global"`，与 spec §5.1 一致。

## Worker 终态落库与 pub/sub 的 context 陷阱（2026-10-21）

**现象**：Worker 处理 HTTP 请求时，请求级 context 在响应返回后被取消，导致终态写记录和 pub/sub 发布失败。

**修复**：终态写记录 / 发消息改用 `context.WithoutCancel(ctx)`，确保即使请求 context 已取消，终态仍能可靠落库并发布。

## Dapr Redis State Store 条件写入（2026-10-21 记录，2026-10-22 更正）

- 2026-10-21 的结论「Lua 脚本 ETag 比较永不相等、条件写总失败」**是错的**，已据此撤回 `writeIndex` 的无条件写。
- 2026-10-22 实测根因（daprd 1.18.7 + redis:7，HTTP 与 gRPC 对照）：Go SDK `SaveStateWithETag` 不传选项时用默认 `CONCURRENCY_LAST_WRITE`，last-write 下 Redis 组件忽略 ETag；而以 first-write 创建的键带 `first-write` 字段，之后**不带 ETag 的写（含显式 last-write）一律 `failed to set key`**，带 ETag + first-write 模式则正常，过期 ETag 返回 409/Aborted。
- 做法：`daprStore` 的 ETag 写 / 删显式 `dapr.WithConcurrency(StateConcurrencyFirstWrite)`；普通 `Set` 遇 `failed to set key` 时回读 ETag 再条件写（Worker 的 `worker:*:attempt:*` 是 first-write 创建、之后多次覆盖写，正是这里踩坑：现象为 SubmitAttempt 报 `save attempt ... user_script:14`）。`internal/store/store.go`。

## Streaming 订阅的 TopicEvent.Data 是解码后的值（2026-10-22）

- 现象：`attempt-events` 从未唤醒 Workflow，只能靠 60s 轮询兜底。
- 原因：`daprCl.SubscribeWithHandler` 对 JSON 内容把 `TopicEvent.Data` 解成 map，旧代码 `event.Data.([]byte)` 断言失败后直接 DROP。
- 做法：断言失败时用 `event.RawData`（原始字节）。`cmd/mybot-runtime/main.go`。

## 空 token 下 A2A 鉴权 401（2026-10-22）

- 现象：本地无 token 时 SubmitAttempt 一直 401。
- 原因：客户端发 `Authorization: Bearer `（末尾空格被 HTTP 栈裁掉），服务端比较 `"Bearer "+""` 永不相等。
- 做法：token 为空时客户端不发头、服务端不校验（与 `--token` 帮助文案「empty = no auth」一致）。`internal/a2a/{client,server}.go`。

## Workflow 内的阻塞与超时语义（2026-10-22）

- `WaitForExternalEvent(name, 0)` 无缓冲事件时立即取消；**负数**超时才是无限等待。旧 `waitForResume` 用 0，pause 后会被当成取消。
- Workflow 体内不能用 `time.Now()`（旧 `CheckStopConditions` 判截止时间用了它）；改为传入 `ctx.CurrentTimeUTC()`。
- `RaiseEvent` 的 payload 要传结构体：传 `[]byte` 会被 JSON 编成 base64 字符串，Workflow 解不出 Signal（旧 API 控制信号就是这样丢的，cancel 只靠「queued 直接改投影」碰巧生效）。

## Cancel while queued 的竞态修复（2026-10-21）

**问题**：Goal 在 "queued" 状态时，cancel 请求通过 `RaiseEvent` 向 workflow 实例发控制信号。但 Dapr workflow engine 启动有延迟（`DaprBuiltInActorNotFoundRetries` 重试），workflow 实例尚未创建，`RaiseEvent` 失败，cancel 信号丢失，goal 永远停留在 "queued"。

**修复**：
- `handleCancelGoal`：对 "queued" 状态直接更新投影为 "cancelled"（不等 workflow 启动），同时仍 raise 控制信号作为 best-effort
- `FinishGoal`：写入前检查当前状态，若已是 "cancelled" 则不覆盖
- `finalizeGoal`：原来总写 `GoalSucceeded`，改为根据 `StopReason` 映射正确状态（`StopFinished→GoalSucceeded`, `StopCancelled→GoalCancelled`, 其余→GoalFailed）
- Workflow 收到 "cancel" 控制信号时调用 `finalizeGoal` 写入投影，而非直接 `return nil, nil`
