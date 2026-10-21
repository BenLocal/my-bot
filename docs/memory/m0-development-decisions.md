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

## Dapr Redis State Store ETag 条件写入 bug（2026-10-21）

**现象**：`internal/store/index.go` 的 `writeIndex` 使用 `Set` with ETag 做条件写入，但 Dapr Redis state store 的 Lua 脚本中 `etag == ARGV[1]` 比较解析后的整数和原始字符串，在 Lua 中永远不相等，导致所有条件写入失败。

**影响范围**：仅影响 worker 注册索引（`registry:fake:_index`），不影响 actor state 或其他记录写入。

**修复**：`writeIndex` 改为无条件写入（`Set` without ETag），由 `MutateIndex` 的重试循环提供竞态保护。

## Cancel while queued 的竞态修复（2026-10-21）

**问题**：Goal 在 "queued" 状态时，cancel 请求通过 `RaiseEvent` 向 workflow 实例发控制信号。但 Dapr workflow engine 启动有延迟（`DaprBuiltInActorNotFoundRetries` 重试），workflow 实例尚未创建，`RaiseEvent` 失败，cancel 信号丢失，goal 永远停留在 "queued"。

**修复**：
- `handleCancelGoal`：对 "queued" 状态直接更新投影为 "cancelled"（不等 workflow 启动），同时仍 raise 控制信号作为 best-effort
- `FinishGoal`：写入前检查当前状态，若已是 "cancelled" 则不覆盖
- `finalizeGoal`：原来总写 `GoalSucceeded`，改为根据 `StopReason` 映射正确状态（`StopFinished→GoalSucceeded`, `StopCancelled→GoalCancelled`, 其余→GoalFailed）
- Workflow 收到 "cancel" 控制信号时调用 `finalizeGoal` 写入投影，而非直接 `return nil, nil`
