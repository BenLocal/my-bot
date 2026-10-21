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

## fake 驱动 Events() 流修复（2026-10-21）

**问题**：`fakeHandle.Events()` 此前返回空，导致 HTTP SSE 流只收到首个 Task 就结束。

**修复**：`fakeHandle.Events()` 现在返回后续事件流，支持完整的 Task → Events → 终态流程。
