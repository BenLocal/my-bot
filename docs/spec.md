# my-bot Spec：Dapr + Go 常驻个人 Agent

> 2026-10-21 制定。依据 Obsidian 笔记 `idea/Dapr + Go 常驻个人 Agent 构想`，以及 2026-10-16 的《基于 Go 和 Dapr 的通用 Agent Runtime 方案》（`~/Documents/agent-runtime-design.md`）。
> 本文定义跨里程碑的**硬契约**（组件边界、协议、状态键、API、调度语义）和 M0–M2 的可实现规格；M3 及以后的细节在开工前补充到本文。
> 标注「实现前核验」的条目依赖外部版本行为，开工第一步先用最小程序验证并锁定版本，再改本文。
> 2026-10-21 按流程走查 `docs/flow.md` 修订：补齐 G1–G18；采纳单一 `Scheduler` Actor（G3）与共享工作区卷（G8）。

## 1. 概述

my-bot 是一个面向个人的常驻 Agent 平台：

- **Go Runtime** 负责接收任务、调度、推进长任务（Goal / Loop）、保存进度、控制执行。
- **执行器 Worker**（Pi、Codex，后续 Claude Code）以独立服务接入，复用各自的模型与工具循环；Runtime 不自研通用 LLM 推理循环。
- **多 Agent 协作**：一个 Goal 可以指定一个**大脑（brain）角色**和若干**成员（member）角色**。大脑可以是任意已接入的执行器（Pi、Codex、Claude Code……），不绑定某个工具；它只负责规划、派活、汇总和判定进展，由 Runtime 校验后把子任务并行派给成员执行，结果回流给大脑进入下一轮。
- **长期记忆两层**：L1 是 Markdown 文件，Agent 自己 grep，是权威来源；L2 用已有的 mem0 做语义召回，是 L1 的派生索引（§14）。
- **Dapr** 提供 Workflow（持久化编排）、Actor（执行槽位）、State（记录与索引）、Pub/Sub（完成通知）。
- **k3s** 单节点起步，组件均为标准 Kubernetes 资源，可迁移到 k0s 或多节点；Worker 可横向扩容。
- 7×24 运行：空闲时只有定时器在等待，不调用模型。

调度机制参考 Dapr Agents 的 `DurableAgent.orchestration_workflow`、基于 State Store 的团队注册表和 random / round-robin / LLM 选择策略，用 Go 重新实现所需部分，不兼容其 Python API。

### 1.1 目标

1. 通过 HTTP / CLI 提交一次性 Task、长期 Goal、周期 Loop，提交端断开后后台继续执行。
2. 多个 Goal / Loop 并行推进，受全局、执行器、账号三级并发上限约束。
3. Runtime、Worker、节点重启后：已完成轮次不重跑，已派发的执行重新关联，不重复启动。
4. 每轮可查询状态、事件、产物、用量；支持暂停、恢复、取消、追加指令。
5. Worker 副本数可调，新副本自动注册并分担执行。
6. 大脑角色可换成任意执行器而不改 Runtime 代码；一个 Goal 内多个成员可并行协作，产物在成员之间交接。

### 1.2 非目标（首版不做）

自研 LLM 推理循环；任意 DAG 编排引擎（依赖关系由大脑跨轮表达，§5.3 第 4 条）；执行器内部推理步骤级恢复；多租户；可视化编辑器；数据库到 K8s 资源的配置同步控制器；自建向量库（语义检索复用已有的 mem0，§14）。

### 1.3 与已定方向的差异

| 已定方向（2026-10-16） | 本 spec | 原因 |
| --- | --- | --- |
| PostgreSQL 存业务数据与 Workflow 状态 | 首版只用 Redis（state + actor state + pubsub），PostgreSQL 留到需要关系查询时 | 用户要求尽量减少依赖；个人规模下键值 + 索引足够 |
| Run → Task → Attempt 通用流程模型 | 首版以 Goal / Loop / Task 为一等对象，固定顺序流程作为 Goal 的一种 `strategy` | 与 2026-10-20 补充的并行长任务需求对齐 |
| 首版每插件并发 1、无动态注册 | Worker 自注册 + 心跳 + 槽位 Actor | 用户要求可扩容 |
| 首版只做固定顺序流程，不做动态规划 | 首版即支持大脑角色动态派活 + 成员并行（`brain` 策略） | 用户要求任意 Agent 当大脑、多 Agent 协作 |
| （opc 设计稿）Pi 固定当 CEO 大脑 | 大脑只是一个角色，绑定哪个执行器由配置决定 | 同上 |
| （本 spec 初稿）三级 SlotPool Actor 依次获取 | 单个 `Scheduler` Actor 原子分配三级容量并选出 Worker（§5.1） | 走查 G3：依次获取会持有并等待、产生竞态与饿死 |
| （本 spec 初稿）每 Worker Pod 独立 PVC 存工作区，产物跨 Pod 拉取 | 工作区与产物放所有 Worker 共同挂载的卷（单节点 hostPath），只有执行器会话留在 Pod 本地 | 走查 G8：同一代码仓库在不同 Pod 上无法连续 |

## 2. 命名与路径契约

| 项 | 值 |
| --- | --- |
| Go module | `github.com/BenLocal/my-bot` |
| 二进制 | `mybot-runtime`、`mybot-worker`（`--executor pi\|codex\|fake`）、`mybotctl` |
| K8s namespace | `mybot` |
| Dapr app-id | `mybot-runtime`、`mybot-worker-pi`、`mybot-worker-codex-<account>` |
| Dapr 组件 | `statestore`（state.redis，`actorStateStore: "true"`）、`pubsub`（pubsub.redis） |
| Pub/Sub topic | `attempt-events` |
| 工作区（共享卷） | 节点目录 `/var/lib/mybot/workspaces/<goalID>/<workspace>/`，所有 Worker 挂载为 `/data/workspaces`（读写） |
| 产物（共享卷） | 节点目录 `/var/lib/mybot/artifacts/<goalID>/r<round>/<stepID>/<name>`，所有 Worker 挂载为 `/data/artifacts`（读写），Runtime 只读挂载 |
| 执行器会话数据 | `/data/sessions/<executor>/`（Pi session jsonl、Codex thread 数据；**每 Pod 独立 PVC**，不共享） |
| 长期记忆（L1） | 节点目录 `/var/lib/mybot/memory/`，Runtime 读写挂载为 `/data/memory`，Worker 只读挂载到工作区 `memory/`（§14） |
| 本地开发 | `dapr run` self-hosted 模式 + docker Redis，组件放 `deploy/dapr/local/` |
| ID 格式 | `g_<ulid>`（Goal/Loop/Task 共用前缀 `g_`，`kind` 字段区分），轮次 `r<n>`，尝试 `a<n>` |

### 2.1 仓库布局

```
cmd/
  mybot-runtime/      # HTTP API + Workflow/Activity/Actor 注册
  mybot-worker/       # A2A 服务 + 执行器驱动
  mybotctl/           # CLI
internal/
  api/                # Runtime HTTP handler（依赖 core，不依赖 dapr 细节）
  core/               # 领域模型、状态机、调度策略（纯 Go，无 Dapr 依赖，可单测）
  orchestrator/       # Dapr Workflow 与 Activity
  scheduler/          # Scheduler Actor（槽位、Worker 选择、账号状态）
  registry/           # Worker 注册表（State + ETag）
  store/              # 记录与索引读写（封装 Dapr State）
  a2a/                # A2A 客户端 / 服务端封装与 my-bot 扩展字段
  memory/             # L1 文件、MemoryWriter Actor、L2 同步与召回
  executor/           # Driver 接口
    pi/  codex/  fake/
deploy/
  dapr/local/  dapr/k8s/
  k8s/                # kustomize：base + overlays/k3s
docs/
```

依赖约束：`core` 不 import Dapr / A2A / HTTP；`executor/*` 不 import Dapr（Worker 侧 Dapr 访问只在 `cmd/mybot-worker` 与 `store`）。

## 3. 架构

```
           mybotctl / HTTP / (M4) 飞书
                     │ Bearer token
          ┌──────────▼───────────┐   Dapr sidecar
          │   mybot-runtime ×N   │◄──────────────┐
          │ API · Workflow ·     │  Workflow /   │
          │ Activity · Scheduler │  Actor / State│
          └──────┬────────▲──────┘               │
     A2A HTTP+SSE│        │ pubsub attempt-events │
       (直连 Pod)│        │                       │
   ┌─────────────▼──┐  ┌──┴──────────────┐   ┌────▼─────┐
   │ worker-pi ×M   │  │ worker-codex-<a>│   │  Redis   │ AOF 持久化 + PVC
   │ StatefulSet    │  │ ×1 / 每账号      │   └──────────┘
   │ pi --mode rpc  │  │ codex app-server│
   └────────────────┘  └─────────────────┘
   共享卷（hostPath）：workspaces / artifacts（Worker 读写、Runtime 只读）、memory（Runtime 读写、Worker 只读）
   Dapr 控制面：operator、sidecar-injector、placement、scheduler（各自持久化按所选版本）
```

### 3.1 职责边界

| 组件 | 负责 | 不负责 |
| --- | --- | --- |
| Runtime | Goal/Loop/Task 生命周期、选 Worker、槽位、预算、上下文组装、验收、对外 API | 启动执行器进程、持有执行器凭据 |
| Worker | 接受 A2A 请求并持久化去重映射、管理执行器进程与会话、工作区、产物发现、上报用量 | 决定下一轮做什么、跨 Goal 调度 |
| Dapr | Workflow 持久化与重放、Actor 单线程与提醒、State、Pub/Sub | 任务语义 |
| k3s | Pod 调度、PVC、Service DNS、Secret | 任务语义 |

**权威状态**：Goal 进度以 Dapr Workflow 为准；对外查询读 Runtime 写入的投影记录（§6），投影只由 Activity 写入。执行事实以 Worker 的 Attempt 记录为准。Pub/Sub 消息只用于唤醒，从不推进状态。

## 4. 领域模型

### 4.1 对象

| 对象 | 含义 | 关键字段 |
| --- | --- | --- |
| `Task` | 一次性执行（等价于只有 1 轮、无完成判定的 Goal） | 同 Goal |
| `Goal` | 长期目标，逐轮推进直到验收通过或触达上限 | `objective`、`acceptance`、`strategy`、`brain`、`members`、`workspaces`、`check`、`limits` |
| `Loop` | 按计划重复执行同一指令 | `instruction`、`schedule`、`limits` |
| `Round` | Goal/Loop 的一轮；`brain` 策略下 = 一次大脑决策 + 本轮派出的成员步骤 | `n`、`scheduledAt`、`status`、`summary`、`decision` |
| `Step` | 一轮中派给某个角色的一个子任务（大脑本身的决策也是一个 Step，`role=brain`） | `id`、`role`、`instruction`、`inputs`、`status` |
| `Attempt` | Step 的一次向 Worker 的提交（含重试） | `key`、`worker`、`remoteTaskID`、`status`、`usage` |
| `Role` | 角色定义：指令 + 执行器 + 账号引用 | `name`、`executor`、`account`、`instructions`、`timeout` |
| `WorkerRecord` | 注册表中的一个 Worker 实例 | `id`、`executor`、`account`、`endpoint`、`capacity`、`version`、`heartbeatAt`、`authOK` |
| `Workspace` | Goal 内一个具名工作目录（共享卷上） | `name`、`git`、`ref`（可选；无则为空目录） |
| `Artifact` | 产物引用 | `path`、`sha256`、`size`、`round`、`attempt` |
| `Event` | 对外事件流条目 | `seq`、`type`、`at`、`data` |

Role 首版来自配置文件 `config/roles.yaml`（ConfigMap 挂载），不做在线编辑：

```yaml
roles:
  # 大脑可以绑定任意执行器；换成 codex / claude 只改这一行
  planner:   { executor: pi,    account: default, instructions: roles/planner.md,   timeout: 15m, capabilities: [brain] }
  assistant: { executor: pi,    account: default, instructions: roles/assistant.md, timeout: 30m }
  designer:  { executor: pi,    account: default, instructions: roles/designer.md,  timeout: 30m }
  coder:     { executor: codex, account: main,    instructions: roles/coder.md,     timeout: 60m }
  reviewer:  { executor: codex, account: main,    instructions: roles/reviewer.md,  timeout: 30m }
defaultBrain: planner
limits:
  globalConcurrency: 4
  goal: { maxParallel: 2, maxStepsPerRound: 4 }   # Goal 可覆盖
  accounts:
    pi/default: { concurrency: 2 }
    codex/main: { concurrency: 1 }
```

### 4.2 状态机

Goal / Loop / Task：

```
queued ─► running ─► succeeded
   │         │  ├──► failed        （连续失败达阈值 / 预算耗尽 / 不可恢复错误）
   │         │  ├──► paused ─► running（resume）
   │         │  └──► waiting       （auth_required / unknown_outcome，需人工）
   └─────────┴─────► cancelled
```

Loop 无 `succeeded`，正常结束为 `completed`（轮数或期限到）。

Step：`pending → running → succeeded | failed | cancelled`（由其 Attempt 结果 + 验收决定）。

Attempt：`submitting → submitted → working → completed | failed | rejected | cancelled | interrupted | unknown`。

- `completed` 只表示执行器结束；Round 是否成功由 Runtime 验收（§7.4）。
- `interrupted`：Worker 无法确认或恢复执行（例如执行器进程在 Worker 重启时丢失）。
- `unknown`：提交超时且查询不到远端记录，**不得**自动新建尝试，Goal 进入 `waiting`。

## 5. 调度设计（参照 Dapr Agents）

| Dapr Agents 机制 | my-bot 对应 |
| --- | --- |
| `DurableAgent.orchestration_workflow`：取成员→计划→每轮选 Agent→派发等待→检查进度→更新计划/停止 | `GoalWorkflow`：每轮 大脑决策→校验→并行派发成员 Step→等全部完成→验收→结果回流大脑→判停 →`ContinueAsNew` |
| 团队注册表：State Store 键 `agents:<team>:_index` + `agents:<team>:<name>`，ETag 乐观并发 | Worker 注册表：`registry:<executor>:_index` + `registry:<executor>:<workerID>`，同样 ETag + 重试，心跳 TTL |
| 选择策略 random / round_robin / llm（LLM 由框架内置模型调用） | `strategy`：`single`、`pipeline`、`round_robin`（无模型参与的确定性策略），以及 `brain`（大脑角色决策，大脑由任意执行器担任） |
| `agent_to_tool` → `call_child_workflow` 跨 app 调用 | 派发走 A2A 直连 Worker Pod（执行器不跑 Dapr Workflow，不能做子工作流） |
| 每轮只等待一个 Agent 返回 | 一轮内大脑可派出多个互不依赖的 Step 并行执行（受 `maxParallel` 与槽位限制）；不同 Goal 之间也并行 |

### 5.0 协作模型：大脑 + 成员

- **大脑是角色不是工具**：任何 `capabilities` 含 `brain` 的 Role 都可担任，Goal 通过 `brain: <role>` 指定，缺省用 `roles.yaml` 的 `defaultBrain`。大脑与成员走同一套 A2A / Worker / 槽位机制，Runtime 不区分大脑背后是 Pi、Codex 还是 Claude Code。
- **大脑不常驻等待**：每次决策是一个短 Attempt，输出结构化决策后即结束；成员执行期间大脑不占槽位。下一轮把成员结果作为输入再唤醒大脑（避免 opc 设计稿中「大脑 loop 干等长任务」的问题）。大脑会话默认 `reuse`，保持跨轮上下文；会话丢失时用进展摘要 + 本 Goal 事件摘要以 `fresh` 会话继续。
- **大脑只提议，Runtime 决定**：大脑输出的派活计划必须通过 §5.3 的校验才执行；大脑不能直接调用其他 Worker、改槽位或改预算。
- **成员之间不直接对话**：协作通过产物交接与大脑汇总完成；成员看到的是大脑写的子任务说明 + 指定的上游产物，不共享彼此会话。
- **确定性策略作为退化形态**：`single`、`pipeline`、`round_robin` 不调用大脑，由 `core` 中的纯函数决定本轮 Step，适合固定流程和低成本 Loop。接口参照 Dapr Agents `orchestration/strategy.py` 的五段式：

  ```go
  type Strategy interface {
      Init(spec GoalSpec) StrategyState
      Next(st StrategyState, round int) []StepPlan          // 本轮 Step；round_robin 先对成员排序再按 (round-1)%n 选
      Absorb(st StrategyState, results []StepResult) StrategyState
      ShouldContinue(st StrategyState) bool
      Finalize(st StrategyState) RoundSummary
  }
  ```

  Step → Round → Goal 的状态汇总同样写成 `core` 里的纯函数（参照 `orchestrators/llm/utils.py` 的父状态推导），单测覆盖。

### 5.1 Scheduler Actor：三级并发与 Worker 选择

所有执行容量由**单个** Dapr Actor `Scheduler`（Actor ID 固定为 `global`）管理。Actor 单线程执行、状态存 actor state store，一次调用内原子地检查三级容量并选出 Worker，避免多 Actor 依次获取带来的「持有并等待」、竞态和饿死（flow G3）。个人规模下单 Actor 吞吐足够；成为瓶颈时再按执行器拆分。

```go
type Scheduler interface {
    // 有容量：立即返回 lease（含选中的 workerID、sessionPolicy）。
    // 无容量：把请求加入 FIFO 队列，返回 Granted=false；之后通过 RaiseEvent(instanceID, "slot-granted:<attemptKey>", lease) 交付。
    // 对同一 AttemptKey 幂等：已有 lease 直接返回，已在队列中不重复入队。
    Acquire(ctx, req AcquireReq) (AcquireResp, error)
    Release(ctx, leaseID string) error            // 释放并按 FIFO 依次尝试满足排队请求
    Cooldown(ctx, account string, until time.Time) error
    SetAccountState(ctx, account string, st AccountState) error // ok | needs_auth
    Drain(ctx, workerID string, on bool) error    // 标记 Worker 不再分配新 lease
    Snapshot(ctx) (SchedulerView, error)          // 供 GET /workers
}

type AcquireReq struct {
    WaiterInstanceID string // goalID
    AttemptKey       string
    Executor, Account string
    PreferWorker     string        // 会话亲和：同 Goal + 角色上次使用的 Worker
    AffinityWait     time.Duration // 默认 5m
}
```

分配规则（一次 `Acquire` 内完成）：
1. 账号状态必须为 `ok` 且不在冷却期。
2. 全局活跃 lease < `globalConcurrency`，且该账号活跃 lease < `accounts.<executor>/<account>.concurrency`。
3. 候选 Worker：注册表中该 executor/account 下心跳未过期、未 drain、`inflight < capacity` 的实例。Worker 注册表由 Actor 在每次调度时读取（Actor 内允许 IO）。
4. 会话亲和（flow G5）：有 `PreferWorker` 且它是候选 → 选它，`sessionPolicy=reuse`；它存在但满载 → 请求排队等待它，排队超过 `AffinityWait` 后放宽为任意候选；它已注销或已过期 → 直接选其他候选。放宽或换实例时 lease 中 `sessionPolicy=fresh`。
5. 其余情况在候选中 round-robin（按 workerID 排序后轮转）。
6. 不满足 1–3 时排队，**不算执行失败**。

其他：
- Lease 绑定 AttemptKey，记录 workerID；`inflight` 由 lease 计数得出，不依赖 Worker 自报。
- Workflow 在等待 `slot-granted:<attemptKey>` 时同时监听控制事件（§5.2 等待规则），并设 10 分钟超时后重新 `Acquire`（幂等，防丢事件）。
- 1 分钟 Actor reminder：回收对应 Attempt 或 Goal 已终态的 lease；处理 `AffinityWait` 到期的排队请求；重新尝试满足队列（注册表变化，如新 Worker 加入，也能被及时用上）。
- 公平：一轮结束立即释放；Loop 等下一轮期间不占 lease；大脑 Step 结束即释放，成员执行期间大脑不占 lease。
- 限流（flow F9）：Worker 回报 `rate_limited`（可带 `retryAfter`）→ Runtime 调 `Cooldown`；冷却期内该账号不发新 lease，本 Step 按可重试错误退避。
- 认证失效（flow G15）：Worker 回报 `auth_required` → Runtime 调 `SetAccountState(needs_auth)`，该账号不再发 lease，相关 Goal 进入 `waiting`。恢复由两种方式触发：用户 `POST /v1/accounts/<executor>/<account>:ok`，或该账号的 Worker 心跳报告 `authOK=true` 且之前为 `needs_auth`。恢复后 Goal 需用户 `resume`。
- Drain（flow G14）：`POST /v1/workers/{id}:drain` → `Drain(id, true)`；该 Worker 的在途 Attempt 跑完、`inflight=0` 后可安全缩容。

### 5.2 GoalWorkflow（一轮）

Workflow 实例 ID = Goal ID。输入是 `GoalState`（每轮 `ContinueAsNew` 时带入，控制历史长度）：

```go
type GoalState struct {
    Spec          GoalSpec   // 创建时的不可变快照（含 Role 定义快照及其 sha256）
    Round         int
    Steps         []StepState // 本轮各 Step 及其 Attempt 号、状态（成员并行时有多个）
    LastResults   []StepResult // 上一轮成员结果摘要与产物引用，供大脑下一轮输入
    Progress      string     // 上一轮的进展摘要（≤ 4 KiB）
    Artifacts     []ArtifactRef
    Budget        Budget     // 剩余轮数、截止时间、token
    FailStreak    int
    NoProgress    int
    Paused        bool
    PendingNotes  []string   // 追加指令 / ask_user 回答，下一轮生效
    PendingAsk    *AskState  // 等待中的 ask_user（requestID、question、expiresAt）
    LastWorker    map[string]string // role → workerID，用于会话亲和（§5.1）
}
```

步骤（W = Workflow 代码，A = Activity）：

1. W：处理已到达的控制事件（`pause`/`resume`/`cancel`/`append`，见下方「等待规则」）。`Paused` 时阻塞等 `resume`/`cancel`。
2. W：判停——轮数、截止时间（用 `ctx.CurrentUTCDateTime()`）、预算、`FailStreak ≥ 3`、`NoProgress ≥ 3`。命中则 A `FinishGoal(reason)` 后结束。
3. W：决定本轮 Step 列表：
   - 确定性策略：`core` 纯函数直接给出（通常 1 个 Step）。
   - `brain`：先运行一个 `role=brain` 的 Step（走下面第 4–6 步的单 Step 流程），A `ParseDecision` 解析并校验决策（§5.3）。决策为 `finish` 则跳到第 8 步做最终验收；`ask_user` 则进入 `waiting` 等 `append`/`resume`；`dispatch` 得到成员 Step 列表。校验失败把错误原因回传大脑重试，连续 2 次失败计入 `FailStreak`。
4. W：对每个 Step 向 Scheduler `Acquire`（§5.1），带 `PreferWorker = LastWorker[role]`；拿到的 lease 决定 Worker 与 `sessionPolicy`。
5. A `SubmitAttempt`：组装上下文（§7.1），用 `AttemptKey = <goalID>-r<round>-<stepID>-a<attempt>` 调 Worker A2A `SendMessage`；写投影记录。超时或网络错误时，Activity 内先按 key 对账，查到则复用。
6. W：等待完成：等 `attempt-done:<key>`，60s 无事件则 A `PollAttempt` 一次，未终态回到等待。`PollAttempt` 连续失败（Worker 不可达）且注册表中该 Worker 记录已过期超过 `orphanAfter`（默认 5 分钟）时，判 `interrupted(worker_lost)`、释放 lease，按可重试处理（flow G13；工作区在共享卷上，新尝试可在别的 Worker 上继续）。Step 超时 = Role.timeout，超时后 A `CancelAttempt` 并以 `failed(timeout)` 处理。成员 Step 并行：第 4–6 步对每个 Step 各起一个 Workflow 任务，用 `ctx` 的并发等待（fan-out / fan-in）汇合；同一 Goal 同时运行的成员 Step ≤ `maxParallel`（默认 2）。
7. A `CollectStep`：拉产物、用量，执行 Step 验收（§7.4），写事件。可重试失败且 `Attempt < 3` 的 Step 在本轮内指数退避后重试；`unknown` / `auth_required` 使 Goal 进入 `waiting`（已完成的 Step 结果保留）。
8. A `CollectRound`：汇总本轮各 Step 结果与产物；进展摘要 `Progress` 取大脑本轮决策的 `summary`（确定性策略下取成员输出的 `summary`），截断到 4 KiB，不另调模型（flow G6）；确定性策略下执行 Goal 验收，`brain` 策略下大脑 `finish` 后执行 Goal 验收。释放槽位；更新 `FailStreak/NoProgress/Budget`。
9. W：大脑 `finish`（或确定性策略的 `done=true`）后先执行 Goal 级 `check`（§7.4，作为一个特殊 Step，走同样的 Acquire / Submit / 等待流程）；通过 → A `CommitMemories`（若有，§14.3）→ `FinishGoal(succeeded)`；不通过 → 结果作为下一轮大脑输入，计入 `FailStreak`。Loop → 计算下次时间 `CreateTimer(next)`；然后 `Round+1` 并 `ContinueAsNew(state)`。`brain` 策略下本轮成员结果写入 `GoalState.LastResults`，作为下一轮大脑的输入。

**等待规则**（flow G11）：Workflow 的每一个等待点——等 `slot-granted`、等 `attempt-done`、Loop 的下次执行定时器、`ask_user`、`paused` 状态——都用 `when_any(业务事件或定时器, 控制事件 "control")` 等待。收到控制事件后：
- `cancel`：对所有运行中的 Attempt 调 `CancelAttempt`，等它们到终态（每个上限 2 分钟，超时记 `interrupted`）→ 释放全部 lease（含排队中的请求，Scheduler 会移除）→ `FinishGoal(cancelled)`。
- `pause`：有运行中的 Step 时，记下 `Paused=true`，本轮结束后停；没有则立即停。
- `append`：存进 `PendingNotes`，继续原来的等待。
- `resume`：仅在 `paused` / `waiting` 时有意义，其余忽略。

控制事件统一用一个事件名 `control`（载荷带 `op`），避免多个事件名在 `when_any` 中组合爆炸。实现前核验（P0-2）：Dapr Go SDK 中 `when_any` 的写法，以及 `ContinueAsNew` 时尚未处理的 `control` 事件是否保留；不保留时改为 API 同时把控制命令写入 `goal:<id>:control` 队列，Workflow 每次醒来由 Activity 读取。

**Activity 重试策略按类型区分**（Dapr Agents 全局只有一个 RetryPolicy，不照搬）：

| Activity | 重试 | 说明 |
| --- | --- | --- |
| `SubmitAttempt` | 3 次，5s 起、×1.5、上限 30s | 本身幂等（attemptKey），重试前先对账 |
| `PollAttempt`、`CollectStep`、`CollectRound`、投影写入 | 5 次 | 只读或幂等写 |
| `ParseDecision` | 不重试 | 解析/校验失败作为返回值交给 Workflow，按 §5.3 回传大脑 |
| `CancelAttempt` | 3 次 | 取消是请求，结果以 Poll 为准 |

不可重试的错误一律作为 Activity **返回值**（带 `Retryable=false`）而非 error 返回，避免被 Dapr 的 Activity 重试吞掉语义。

**确定性 ID**：Workflow 内所有 ID（attemptKey、事件 ID、ask 请求 ID、子实例 ID）都由 goalID、round、stepID 等推导，禁止在 Workflow 代码里生成随机 UUID（Dapr Agents `durable.py` 用 `uuid4()` 生成子实例 ID，重放时会变化，是反例）。

### 5.3 大脑决策契约与校验

大脑每次输出（最后一个 JSON 代码块）：

```json
{
  "action": "dispatch",
  "summary": "设计已完成，下一步并行实现后端与写测试",
  "steps": [
    {"id": "impl",  "role": "coder",    "instruction": "按 design.md 实现 API", "inputs": ["r1/design/design.md"], "workspace": "repo"},
    {"id": "tests", "role": "reviewer", "instruction": "为 design.md 中的接口写测试", "inputs": ["r1/design/design.md"], "workspace": "repo-tests"}
  ],
  "done": false
}
```

`action ∈ {dispatch, finish, ask_user}`；`ask_user` 时带 `question`，作为 `goal.waiting` 事件推给用户。

`ask_user` 的等待：请求 ID = `ask:<goalID>:r<round>:<n>`（确定性）；Workflow 用 `when_any(WaitForExternalEvent("user-input:<requestID>"), CreateTimer(askTimeout))` 等待，`askTimeout` 默认 24h，超时 Goal 转 `paused`（原因 `ask_timeout`），不自动替用户作答。等待期间不占槽位、不保留执行器进程。用户经 `POST /goals/{id}:append` 带 `replyTo=<requestID>` 回答；已过期或已回答的 requestID 返回 409。

Runtime 校验（不通过则不执行）：

1. `role` 必须**精确**等于本 Goal `members` 中的某一项（大小写敏感，不做模糊匹配；大脑不能派给自己，也不能派给未授权的角色）。
2. `steps` 数量 1..`maxStepsPerRound`（默认 4）；`id` 在本轮唯一，匹配 `[a-z0-9-]{1,32}`。
3. `inputs` 只能引用本 Goal 已登记的产物（`r<round>/<stepID>/<name>`）或 Goal 声明的输入挂载；`workspace` 必须是 Goal `workspaces` 中声明的名字，或新的空工作区名（匹配 `[a-z0-9-]{1,32}`）。
4. 同一轮内的 Step 视为互不依赖、并行执行；有先后依赖的工作由大脑分到不同轮次。这样无需 DAG 引擎。
5. `workspace` 相同的多个 Step 不并行：写同一代码仓库的 Step 使用不同 `workspace`（由 Worker 用独立 git worktree 实现）或被 Runtime 串行化。
6. 预估预算（Step 数 × 角色平均用量）不超过剩余预算；超出则只执行前若干个并在事件中说明。同一轮并行 Step 的实际用量可能共同超出剩余预算，**最多超支一轮**，下一轮开头判停（flow G16）；各 Goal 预算独立，不做跨 Goal 预留；用量为 `unknown` 的执行器不参与 token 预算，只受轮数与期限约束。

校验或 JSON 解析失败时，把**具体错误**（哪条规则、哪个字段）连同决策 schema 回传大脑重试，不静默跳过、也不直接让 Goal 失败。解析前只做确定性修补（去代码围栏、截取首个 JSON 对象），不让模型之外的组件「猜」字段。决策 schema 由 Go struct 生成，原样放进大脑提示词（与 §11 M2 的 schema 管理同源）。

Workflow 代码只做确定性逻辑；时间只取 `ctx.CurrentUTCDateTime()`，随机只在 Activity 内。Workflow 代码升级需兼容重放：变更逻辑时以 `Spec.Version` 分支，或等待在途实例跑完（开发期可接受清空）。

### 5.4 Loop 调度

`schedule` 二选一：`every: 30m`（上一轮结束后间隔）或 `cron: "0 8 * * *"`（时区 `Asia/Shanghai`）。

- cron 的轮次以「计划时刻」为键：`Round` 记录 `scheduledAt`，同一 `scheduledAt` 只执行一次，因此重启不会对同一天重复创建。
- Runtime 停机错过的计划时刻：只补最近一次（`catchup: latest`，默认）或全部跳过（`catchup: none`）。
- 不使用 Dapr Jobs API：Workflow 持久化定时器已覆盖需求，少一层语义。

### 5.5 完成通知

Worker 在 Attempt 到终态时发布到 `attempt-events`：`{attemptKey, goalID, status, at}`。Runtime 订阅后 `RaiseEvent(goalID, "attempt-done:<key>")`。重复或丢失都无害：Workflow 收到事件后仍以 `PollAttempt` 的结果为准，丢失则由 60s 轮询兜底。

订阅处理（参照 Dapr Agents `workflow/utils/subscription.py`）：

- 返回三态：消息解析失败 → `DROP`；`RaiseEvent` 成功或实例已结束 → `SUCCESS`；Dapr 调用出错 → `RETRY`。
- 配死信 topic `attempt-events-dead`。
- 如需去重，**只在得到 SUCCESS/DROP 之后标记已处理**；到达即标记会让 RETRY 的重投被去重吃掉。首版不设去重存储：实例 ID 就是 goalID，`RaiseEvent` 重复无害。

## 6. 存储契约（Dapr State，Redis）

| 键 | 写者 | 内容 |
| --- | --- | --- |
| `goal:<id>` | Runtime Activity | 投影：spec 摘要、status、round、progress、budget、updatedAt |
| `goal:<id>:events:<seq>` | Runtime Activity | 事件；`seq` 由 `goal:<id>` 中计数器分配（ETag 并发） |
| `goal:_index:active` / `goal:_index:all` | Runtime | ID 列表，ETag 乐观并发 + 重试（同 Dapr Agents 注册表做法） |
| `idem:create:<clientRequestID>` | Runtime | 创建接口去重 → goalID，TTL 7 天 |
| `registry:<executor>:_index`、`registry:<executor>:<workerID>` | Worker | 注册信息；心跳每 15s 更新，`ttlInSeconds=60` |
| `worker:<workerID>:attempt:<key>` | Worker | 去重映射 + Attempt 记录（远端 Task ID、会话 ID、状态、用量、产物清单） |
| `goal:<id>:artifact:r<round>/<stepID>/<name>` | Runtime Activity | 产物登记：路径（共享卷内）、sha256、size、来源 attempt |
| Actor state `Scheduler` | Dapr Actor | leases（含 workerID）、FIFO 队列、账号状态（ok / cooldown until / needs_auth）、drain 列表、round-robin 游标 |
| Actor state `MemoryWriter` | Dapr Actor | 已提交条目 ID 集合（幂等） |
| `memsync:<entryID>` | Runtime | L1 条目与 mem0 记录的映射 `{mem0ID, sha256}`（§14.5） |
| Workflow 历史 | Dapr | 平台不直接读写 |

- 跨键原子更新用 Dapr State 事务（Redis 支持）：如「写 goal 投影 + 更新 active 索引」同一事务。
- 索引（`registry:*:_index`、`goal:_index:*`）的修改统一走 `store.MutateIndex(key, func(ids) (ids, changed bool))`（参照 Dapr Agents `components.py` 的 `_mutate_team_index`）：读 ETag → 回调 → 未变化不写 → 条件写；冲突用 full-jitter 指数退避（50ms 起，上限 1s），总耗时预算 10s。
- 注册顺序：先写单条记录，再加入索引；注销反过来，先删记录再出索引。读取时跳过索引中已不存在（含 TTL 过期）的记录。
- TTL 只让单条记录过期，索引不会自动清理：Runtime 每 5 分钟用 `MutateIndex` 清掉指向不存在记录的 ID。
- 条件写（first-write）冲突时**先回读**：记录的 owner 是自己（同一 attemptKey + 同一提交者）就当作写成功，不报重复——这对应「写成功但响应丢失后重试」的情况。
- Redis 必须开 AOF（`appendonly yes`，`appendfsync everysec`）并挂 PVC。
- 大文件不进 Redis，只存引用与哈希。
- 实现前核验：Redis state store 的事务 + ETag + TTL 在所选 Dapr 版本中同时可用；actor/workflow 用同一个 Redis 实例的可行性与 key 前缀隔离。

## 7. 执行器接入

### 7.1 Runtime ↔ Worker：A2A

- 规范 A2A 1.0.1，Go SDK `github.com/a2aproject/a2a-go`；传输固定 HTTP JSON-RPC + SSE。实现前核验 SDK 发布版本与字段，锁定后写入本节。
- Runtime 直连 Worker Pod（StatefulSet headless Service：`mybot-worker-pi-0.mybot-worker-pi.mybot.svc:8080`），因为 Attempt 绑定在具体实例上。不走 Dapr service invocation（SSE 转发与超时未验证）。
- 认证：集群内共享 Bearer token（Secret `mybot-internal-token`）。

| 平台操作 | A2A | my-bot 约定 |
| --- | --- | --- |
| 发现 | Agent Card | Worker 启动时同时写注册表；Runtime 拉 Agent Card 校验 executor/version |
| 提交 | `SendMessage` | `metadata["mybot/attemptKey"]` 必填，Worker 按它去重；必须返回 Task（不返回即时 Message） |
| 查询 | `GetTask` | 恢复与对账的唯一依据；按 remoteTaskID 查，Worker 另提供按 attemptKey 查（扩展方法 `mybot/getByAttemptKey`） |
| 进展 | `SubscribeToTask`（SSE） | 只用于展示事件，断线不影响正确性 |
| 取消 | `CancelTask` | 请求语义；最终状态以 `GetTask` 为准 |
| 产物 | Task `artifacts` | `uri = mybot-artifact://<goalID>/r<round>/<stepID>/<name>` + sha256，对应共享卷 `/data/artifacts/<goalID>/r<round>/<stepID>/<name>` |

提交消息正文（`parts`）由 Runtime 组装。成员 Step：

```
[role instructions]           角色指令文件内容
[instruction]                 大脑写的子任务说明（确定性策略下为 Goal 目标或 Loop 指令）
[inputs]                      指定上游产物的路径（共享卷 /data/artifacts/...）与简介
[notes]                       用户追加指令
[output contract]             要求最后输出 JSON：{"summary": "...", "done": bool, "artifacts": [...]}
```

大脑 Step：

```
[role instructions]           大脑角色指令（规划、派活规则、决策 JSON 格式见 §5.3）
[team]                        本 Goal 可用成员：角色名、执行器、能力描述、当前是否可用/冷却
[objective / acceptance]      Goal 目标与验收条件
[progress]                    累计进展摘要
[last results]                上一轮每个成员 Step 的 summary、状态、产物清单（不含成员会话全文）
[notes]                       用户追加指令 / ask_user 的回答
[limits]                      剩余轮数、预算、maxParallel、maxStepsPerRound
[memory index]                L1 记忆索引 00-index.md（标题 + 摘要），正文在工作区 memory/ 下自行 grep（§14）
[recalled]                    L2 语义召回的 top-k 条目（含 L1 出处）
```

**产物交接**（flow G8）：工作区和产物都在共享卷上，任何 Worker 都能直接读到。Step 结束时 Worker 计算产物 sha256 并在 Task `artifacts` 中返回；Runtime 在 `CollectStep` 中登记到 `goal:<id>:artifact:*`。下游 Step 的 `metadata["mybot/inputs"]` 列出产物 URI，Worker 在 `Driver.Start` 前校验 sha256（不一致则 Step 失败、不可重试），然后把路径写进提示词，不复制文件。

**工作区**（flow G9）：Goal 的 `workspaces` 声明具名工作区，可带 `git` 与 `ref`。首次被某个 Step 使用时，由执行该 Step 的 Worker 在共享卷上创建（有 `git` 则克隆，git 凭据走 Secret、只给 Worker）；同仓库并行写的 Step 用独立 git worktree（`<workspace>@<stepID>`）。是否允许 push 由 Role 权限控制，默认不允许。§5.3 第 5 条的「同 workspace 串行」仍然有效。

`metadata` 还带：`mybot/goalID`、`mybot/round`、`mybot/workspace`（相对路径）、`mybot/sessionPolicy`（`reuse` 同 Goal 复用执行器会话 / `fresh`）。

### 7.2 Worker 内部：Driver 接口

```go
type Driver interface {
    Start(ctx context.Context, req StartReq) (Handle, error)   // 启动或续用会话，异步执行
    Status(ctx context.Context, h Handle) (Status, error)
    Events(h Handle) <-chan Event                              // 结构化事件，非终端画面
    Cancel(ctx context.Context, h Handle) error
    Recover(ctx context.Context, rec AttemptRecord) (Status, error) // Worker 重启后核实
}

type Status struct {
    Phase     Phase  // working | completed | failed | paused
    Retryable bool   // 仅 failed 有意义
    Reason    string // auth_required | rate_limited | budget_exceeded | context_too_long | timeout | crashed | ...
    Usage     Usage  // 本次 Attempt 的增量，见下
}
```

`Events` 必须以 `completed`、`failed`、`paused` 之一作为最后一个事件结束；没有终态事件就关闭通道视为 Driver 缺陷，Attempt 记 `failed(driver_bug)`。不可重试：`auth_required`、`budget_exceeded`、`context_too_long`、执行器二进制缺失、4xx 类请求错误；`rate_limited` 可重试但先触发账号冷却（§5.1）。

Worker 处理 `SendMessage`：
1. 以 attemptKey 做 `first-write`（ETag 为空的条件写）存 `worker:<id>:attempt:<key>`；已存在则直接返回已有 Task（冲突时按 §6 先回读判断 owner）。
2. **会话 ID 在启动前确定并写入记录**：`reuse` 时取同一 Goal + 角色上一次记录中的会话 ID，没有则按 `uuid5(goalID + ":" + role)` 派生；`fresh` 时按 `uuid5(attemptKey)` 派生。然后才 `Driver.Start`。
3. 终态时：**先等执行器把会话落盘**（Pi 进程退出 / jsonl flush，Codex thread 结束事件），再写终态记录 → 发布 `attempt-events`。否则下一轮 `reuse` 可能读到不完整的会话。

Worker 心跳（每 15s）同时上报 `authOK`：由 Driver 用执行器自身的状态命令检测登录是否有效（检测方式在 P0-4/5 核验），供 Scheduler 自动解除 `needs_auth`（§5.1）。

Worker 启动时扫描本实例非终态记录，逐条 `Driver.Recover`，判据（参照 Dapr Agents `executors/claude.py` 的 `_plan`，但以 attemptKey 而非提示文本为准）：
- 会话中能找到本 attemptKey 的提示且其后已有完整回答 → 直接取回答作为结果（不再调用模型），记 `completed`。
- 能找到提示但回答不完整、且本轮尚未产生外部副作用（工具调用记录为空）→ 续接会话发送「继续」提示，不重复原提示。
- 已有工具调用但无法确认结果 → 记 `interrupted`，交给 Runtime 按重试规则处理，**不自动重放工具调用**。
- 找不到会话或提示 → 记 `interrupted`。

提示正文首行带 `[mybot attempt <attemptKey>]` 标记，供 Recover 定位。

**用量按增量记账**：执行器只提供会话累计值时，Usage = 本次结束时累计值 − 本次开始前累计值；增量按 attemptKey 存，重试同一 attemptKey 不重复计费。

| 执行器 | 原生接口 | 会话 | 用量 |
| --- | --- | --- | --- |
| `pi` | `pi --mode rpc`（stdin/stdout JSON 行），每个 Goal 一个进程，cwd = 工作区 | session jsonl 存 `/data/sessions/pi/`，`reuse` 时用同一 session 继续 | 取 assistant 消息 `usage.totalTokens` |
| `codex` | `codex app-server`（JSON-RPC over stdio），一个进程多 thread | thread ID 存 Attempt 记录 | 取 token 用量事件 |
| `fake` | 进程内模拟：可配置耗时、结果、失败、挂起、重复回调 | — | 固定值 |

实现前核验：本机 pi 与 codex 版本的 RPC/app-server 消息格式、非交互权限模式（沙箱、自动批准范围）、会话续接方式；用量字段是单次还是累计；续接会话后权限模式是否需要重新传入；是否会读取宿主配置（全局指令文件、自动记忆、连接器/MCP 配置）以及如何关闭——Worker 应只使用 Role 下发的配置（Dapr Agents 的 Claude 执行器默认关闭这些，见 `claude_config.py`）。把核验结论与版本号补进本表。

### 7.3 凭据

- Pi：模型 provider key 放 Secret，挂载为 `~/.pi/agent/` 下所需文件；`settings.json` 用 ConfigMap。Pi 用 API key，可多副本共享。
- Codex：每账号一个 StatefulSet（replicas=1），`CODEX_HOME` 指向该 Pod 的 PVC，首次在 Pod 内 `codex login --device-auth` 完成登录，之后由 codex 自行刷新。不复制 OAuth 凭据到多副本；扩容 Codex = 增加账号或改用 API key。
- Runtime 和 Redis 中只存账号引用，不存令牌正文。凭据目录不挂进执行器的工具可写路径之外的位置（工作区与凭据目录分开挂载）。

### 7.4 验收

Round 成功 = 执行器 `completed` 且满足：
1. 输出含合法 output contract JSON；
2. `artifacts` 中声明的文件存在且 sha256 可算；
3. （可选）Step 级检查不做；Goal 级 `check` 见下。

Goal 级 `check`（flow G10）：`check: {command, workspace, timeout}`（`timeout` 默认 10 分钟）。在大脑 `finish` 后作为一个特殊 Step（`stepID=check`、`role=_check`，使用任意可用 Worker 的 lease，账号取 Goal 第一个成员的执行器/账号）执行：Worker 在共享卷上的该工作区内运行命令，退出码与输出作为产物返回。

Goal 完成 = 本轮成功且大脑 `finish`（确定性策略下 `done=true`）且 `check` 通过（若配置）。模型只能「提出」完成，不能单独决定完成。

`NoProgress`：本轮 `summary` 与上一轮相同（规范化后哈希相等）且无新产物时 +1。

## 8. 对外接口

Base：`/v1`，Bearer token（Secret `mybot-api-token`）。所有创建接口要求 `Idempotency-Key` 头。

创建流程（flow F1）：校验请求并固化 `Spec`（角色定义与指令文件 sha256）→ 生成 goalID → `FirstWrite idem:create:<key>`。冲突则回读已有 goalID，**确认对应 Workflow 实例存在，不存在就以该 goalID 补启动**（flow G1），返回 200；写入成功则事务写初始投影（`queued`）+ 索引，再以 goalID 为实例 ID 启动 Workflow，返回 201。初始投影由 API 写，之后投影只由 Activity 写（flow G2）。

控制接口（pause / resume / cancel / append）只向 Workflow 发 `control` 事件，返回 `202 {accepted: true}`，状态变化以事件流为准；对终态 Goal 返回 409（flow G12）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` `/goals` `/loops` | 创建，返回 `{id, status, statusUrl}`；同 key 重复返回同一 ID |
| GET | `/goals?status=active` | 列表（Task/Loop 共用，`kind` 过滤） |
| GET | `/goals/{id}` | 投影详情 |
| GET | `/goals/{id}/events?after=<seq>` | 事件；`Accept: text/event-stream` 时 SSE 跟随（`id:` 字段为 `<goalID>:<seq>`，支持 `Last-Event-ID` 续传） |
| GET | `/goals/{id}/rounds/{n}` | 轮次详情（attempts、usage、artifacts） |
| GET | `/goals/{id}/artifacts/r{round}/{stepID}/{name}` | 下载产物（Runtime 只读挂载共享卷，直接读取并校验 sha256） |
| POST | `/goals/{id}:pause` `:resume` `:cancel` | 控制；pause 在当前轮结束后生效 |
| POST | `/goals/{id}:append` | `{note, replyTo?}`，下一轮生效；`replyTo` 回答 `ask_user`（§5.3）；正文上限 64 KiB，超出返回 413 |
| DELETE | `/goals/{id}` | （M2）仅终态 Goal：清理投影、事件、索引项与 Workflow 历史（purge） |
| GET | `/workers` | 注册表、lease 与排队情况（Scheduler `Snapshot`） |
| POST | `/workers/{id}:drain` `:undrain` | 停止 / 恢复向该 Worker 分配（§5.1） |
| POST | `/accounts/{executor}/{account}:ok` | 用户重新登录后解除 `needs_auth` |
| GET | `/memory?topic=&q=` | 列出 L1 主题 / 条目，`q` 为子串过滤；`/memory:recall?q=` 走 L2 语义召回（调试用） |
| GET | `/healthz` `/readyz` | 探针 |

创建 Goal 请求：

```json
{
  "kind": "goal",
  "objective": "把 ~/notes/inbox 下的笔记分轮整理成主题索引",
  "acceptance": "index.md 覆盖 inbox 中所有笔记",
  "strategy": "brain",
  "brain": "planner",
  "members": ["designer", "coder", "reviewer"],
  "maxParallel": 2,
  "inputs": {"mounts": ["notes:/inbox:ro"]},
  "workspaces": {"out": {}, "repo": {"git": "https://example.com/me/tool.git", "ref": "main"}},
  "limits": {"maxRounds": 10, "deadline": "2026-10-28T00:00:00+08:00", "maxTokens": 2000000},
  "check": {"command": "test -f index.md", "workspace": "out", "timeout": "10m"}
}
```

`mybotctl` 是上述 API 的薄封装：`mybotctl goal create -f goal.yaml`、`mybotctl ls`、`mybotctl logs -f <id>`、`mybotctl pause|resume|cancel <id>`、`mybotctl workers [drain|undrain <id>]`、`mybotctl accounts ok <executor>/<account>`。

## 9. 部署（k3s）

### 9.1 选型

k3s（单二进制、自带 local-path PVC、Traefik、ServiceLB），单节点起步。只使用标准资源（Deployment、StatefulSet、Service、PVC、Secret、ConfigMap、Ingress），换 k0s 只需改 StorageClass 与 Ingress 类。Dapr 用官方 Helm chart 安装到 `dapr-system`。

### 9.2 资源

| 资源 | 类型 | 副本 | 存储 |
| --- | --- | --- | --- |
| `redis` | StatefulSet | 1 | PVC 10Gi，AOF |
| `mybot-runtime` | Deployment | 1（可 2） | 无 |
| `mybot-worker-pi` | StatefulSet | 1..M | 每 Pod PVC：sessions；共享 hostPath：workspaces、artifacts（rw）、memory（ro） |
| `mybot-worker-codex-<account>` | StatefulSet | 1 | 每 Pod PVC：sessions + `CODEX_HOME`；共享 hostPath 同上 |
| `mybot-runtime` 的挂载 | — | — | 共享 hostPath：memory（rw）、artifacts（ro） |
| Dapr 组件 | `Component` ×2、`Configuration`（开 tracing 采样 0，可选） | — | — |

- 共享卷：单节点用 hostPath `/var/lib/mybot/{workspaces,artifacts,memory}`；多节点时改为 RWX 存储（如 NFS）。所有 Worker 进程以同一 UID 运行，目录权限按该 UID 设置。
- 用户数据（笔记目录等）以 hostPath 只读挂入 Worker，按 Goal `inputs.mounts` 白名单映射；首版白名单写在 Worker 配置里。
- Runtime API 通过 Traefik Ingress 暴露到局域网，强制 token；不暴露 Worker 与 Redis。
- 镜像：`mybot-runtime`（distroless）；`mybot-worker-pi`（node 运行时 + pi）；`mybot-worker-codex`（codex CLI）。构建时 npm 走 npmmirror。
- Pod 资源：设 requests/limits；Worker `terminationGracePeriodSeconds: 120`，SIGTERM 时停止接新任务、注销注册、等在途执行到达可恢复点或超时。
- 优雅停机（`signal.NotifyContext`）的固定顺序：Runtime：停 HTTP 接收与订阅 → 等进行中的 Activity（上限 30s）→ 停 Workflow worker → 关 Dapr client；Worker：注销注册 → 拒绝新 `SendMessage` → 等在途执行或超时 → 写终态/保留非终态记录供 Recover → 关 client。

### 9.3 扩容

- **Worker（Pi）**：`kubectl scale sts/mybot-worker-pi --replicas=N`。新 Pod 启动即注册并按 `capacity` 参与 round-robin；缩容时最高序号 Pod 先注销，在途 Attempt 由 §7.2 的恢复与 `interrupted` 规则处理（缩容前应先 drain：`mybotctl workers drain <id>`，等 `/workers` 中该实例 `inflight=0`；工作区在共享卷上，缩容不丢工作区，只丢该 Pod 上的执行器会话）。账号级并发上限不因副本增加而提高，需同步调 `roles.yaml`。
- **Runtime**：多副本时 Workflow/Actor 由 Dapr placement 分片；HTTP 无状态。
- **自动扩缩**（M4，可选）：按排队数扩 Worker，需要 KEDA，届时再评估是否值得多一个组件。

## 10. 可观测性

- 事件类型：`goal.created`、`round.started`、`slot.waiting`、`attempt.submitted`、`attempt.progress`（节流，≤1 条/10s）、`attempt.finished`、`round.accepted|rejected`、`goal.paused|resumed|waiting|finished`、`account.cooldown|needs_auth`。
- 日志：JSON 到 stdout，字段含 `goalID`、`round`、`attemptKey`、`workerID`。
- 用量：执行器能报则记录，不能报记 `unknown`，不估算。
- 通知（M2）：`goal.finished`、`goal.waiting`、`account.needs_auth` 推到一个 webhook（如飞书机器人），URL 来自 Secret。通知实现为可插拔的事件 listener（参照 Dapr Agents `streaming/listeners.py`），与 SSE 读取同一事件源，不另起一条推送路径。
- 事件 ID 确定性：`<goalID>:<seq>`，Workflow 重放或 Activity 重试重复写同一事件时按 ID 覆盖而非追加。
- Tracing（M2）：OpenTelemetry OTLP 导出。每个 Goal 一个 trace，`traceparent` **持久化**在 `GoalState` 与 Attempt 记录中（Dapr Agents 只缓存在内存里，重启即断），经 A2A `metadata["mybot/traceparent"]` 传到 Worker；用量写 `gen_ai.usage.*` 属性。

## 11. 里程碑

| 里程碑 | 内容 | 验收 |
| --- | --- | --- |
| **M0 骨架与语义** | 仓库布局；`core` 状态机、策略接口、状态汇总、决策校验单测；本地 `dapr run` + Redis；Runtime API、GoalWorkflow（含 brain fan-out、ask_user 等待、等待点响应控制事件、孤儿判定）、Scheduler Actor（含会话亲和、drain、账号状态）、`store.MutateIndex`、订阅三态处理、优雅停机；`fake` Worker（可脚本化输出大脑决策、无终态事件、重复回调） | §12 的 1–5、12、13、18–22 在 fake 执行器上通过 |
| **M1 Pi + Codex 真实执行** | Pi Driver、Codex Driver（每账号 1 副本）；共享卷工作区（git 克隆、worktree）、产物登记与校验、Goal 级 check；验收；Loop（every + cron）；L1 记忆（目录、MemoryWriter、大脑 `memories` 写入、只读挂载） | 笔记摘要 Task 与每天 08:00 摘要 Loop；一个 `brain` Goal：大脑派 designer 出设计 → 并行派 coder 与 reviewer → check 通过；大脑分别换成 Pi 和 Codex 各跑一次 |
| **M2 k3s 常驻** | 镜像、kustomize、Dapr Helm 安装脚本、Ingress、Redis AOF；webhook 通知 listener；`mybotctl`；OTel tracing；`roles.yaml` 热更新（ConfigMap + fsnotify：转换 → 校验 → 失败保留旧值 → 敏感字段日志打码；在途 Goal 用 Spec 快照不受影响）；跨进程契约 JSON Schema 生成与兼容检查；purge 接口；L2 记忆（mem0 同步与召回、对账任务） | §12 全部条目在 k3s 上通过，并连续运行 24h |
| **M3 更多大脑与成员** | Claude Code Driver；原生 Go `llm` 执行器（直接调模型 API，只做规划，作为低成本大脑选项）；成员能力描述来自 Agent Card | 第三种执行器担任大脑跑通 M1 的协作 Goal |
| **M4 扩展入口** | 飞书入口（复用 pi-feishu 经验）；KEDA 评估 | 按需补充 |

M0 开工前先完成四项核验：Dapr Go SDK 的 workflow / actor / state 事务版本组合；a2a-go 版本与扩展 metadata 写法；`pi --mode rpc` 与 `codex app-server` 的消息格式；Dapr Go Workflow SDK 的并发 fan-out/fan-in 写法与外部事件在 `ContinueAsNew` 时的保留行为。结论写回 §6、§7。

## 12. 验收条件

1. 提交 Task 后断开客户端，任务仍完成，结果可查询。
2. 同一 `Idempotency-Key` 重复创建只得到一个 Goal；同一 attemptKey 重复提交只产生一个执行器会话。
3. 并发上限为 2 时同时创建 3 个 Goal：任何时刻活跃 Attempt ≤ 2，第三个出现 `slot.waiting` 后获得执行。
4. 执行中删除 Runtime Pod：重建后 Goal 的轮次、预算、下次执行时间不变，已提交的 Attempt 被重新关联，没有新增执行器会话。
5. 执行中删除 Worker Pod：对应 Attempt 变为 `interrupted` 或恢复后完成，状态明确；Goal 按重试规则推进，不出现静默重复执行。
6. 暂停一个 Goal 后其他 Goal 继续；恢复后从下一轮继续；取消后不再派发，当前执行终态可查询。
7. 达到轮数 / 期限 / 预算 / 连续失败 / 连续无进展任一阈值时停止推进并给出原因。
8. cron Loop 跨重启不重复执行同一计划时刻。
9. Pi Worker 从 1 扩到 2 后，新任务分配到两个实例；账号上限不被突破。
10. 连续 24h 运行：空闲期无模型调用；无遗漏、无重复轮次；Runtime/Worker 内存无持续增长。
11. 执行器认证失效时账号进入 `needs_auth`，相关 Goal `waiting` 并发出通知，重新登录后 `resume` 继续。
12. `brain` Goal 中大脑一次派出 2 个成员 Step，两者并行执行，结果与产物在下一轮出现在大脑输入中；只改 `roles.yaml` 把大脑从 Pi 换成 Codex（或反之），同一 Goal 定义仍能完成。
13. 大脑派给非成员角色、引用不存在的产物、超出 `maxStepsPerRound` 时，Runtime 拒绝执行并把原因回传大脑；大脑 `ask_user` 时 Goal 进入 `waiting`，用户 `append` 回答后继续。
14. 大脑 Step 运行期间重启 Runtime：决策不重复执行，已派出的成员 Step 不重复派发。
15. Goal A 结束时写入的记忆，在之后创建的 Goal B 中：L1 能被大脑在 `memory/` 下 grep 到，L2 能按语义（不同措辞）召回并带 L1 出处。
16. 停掉 mem0 后 Goal 照常推进，只是没有 `[recalled]`；恢复后对账任务把期间新增的 L1 条目补同步到 mem0。
17. 手工编辑或删除 L1 中的一条，对账后 mem0 中对应条目随之更新或删除；含凭据特征的记忆被拒绝写入。
18. 创建请求在写入 `idem` 后、启动 Workflow 前崩溃：同 key 重试后返回同一 ID，且 Goal 被补启动并完成。
19. Goal 在等槽位、等 Attempt 完成，Loop 在等下次执行定时器时收到 `cancel`：不等醒来，立即进入取消流程；运行中的 Attempt 被取消，lease 全部释放。
20. 永久停掉一个 Worker：`orphanAfter` 后其上的 Attempt 判为 `interrupted(worker_lost)`，Goal 在其他 Worker 上重试并继续使用同一工作区。
21. `reuse` 会话被派回原 Worker；原 Worker 满载超过 `affinityWait` 时换实例并以 `fresh` 会话继续。
22. 对一个 Worker drain 后不再收到新 lease，在途 Attempt 正常完成后 `inflight=0`。

## 13. 风险与待确认

| 项 | 说明 | 处理 |
| --- | --- | --- |
| Dapr Workflow 在 Redis 上的长期运行表现 | 历史膨胀、Redis 内存 | 每轮 `ContinueAsNew`；M2 的 24h 测试观察内存；必要时 Workflow 状态迁 PostgreSQL |
| Workflow 代码升级与重放兼容 | 改 Workflow 逻辑可能让在途实例重放失败 | `Spec.Version` 分支；开发期允许清空 |
| Pi / Codex 无人值守权限 | 自动批准范围过大有风险，过小会卡在等待输入 | 每个 Role 显式配置权限；需要输入时 Attempt 进入 `waiting` 而不是挂起 |
| 执行器会话跨 Pod 迁移 | 会话与 PVC 绑定在 Pod 上 | 会话亲和（§5.1）；该实例不可用时换实例并用 `fresh` 会话 + 进展摘要继续；工作区在共享卷上不受影响 |
| 共享卷 | 多节点需要 RWX 存储；共享目录权限与并发写 | 单节点 hostPath 起步；同 workspace 串行（§5.3）；统一 UID |
| 单一 Scheduler Actor | 所有调度经过一个 Actor，可能成为瓶颈或单点 | 个人规模可接受；Actor 状态持久化，重启后由 placement 重新激活；瓶颈出现时按执行器拆分 |
| 订阅账号额度 | 扩副本不增加额度 | 账号级并发与冷却；预算在 Runtime 统一结算 |
| 大脑决策质量 | 不同执行器当大脑时输出格式和规划能力差异大 | 决策 JSON 严格校验 + 回传错误重试；大脑角色指令按执行器微调；`maxStepsPerRound`、预算兜底 |
| 并行成员写冲突 | 两个成员改同一仓库 | 不同 `workspace` 用独立 worktree，合并由后续轮次的成员（如 reviewer）显式完成；同 workspace 串行 |
| 记忆污染 | 大脑把一次性结论或错误结论沉淀为长期记忆，后续 Goal 被误导 | 只有大脑能写、单条有长度上限、条目带来源 Goal；用户可直接编辑 / 删除 L1，对账同步到 L2；后续可加「待确认」状态由用户审批 |
| 第一个场景 | 笔记摘要是验证用例，真实高频场景待定 | M1 完成后由用户确认下一个场景 |

## 14. 记忆（两层：Markdown + grep，mem0 语义召回）

### 14.1 分层与职责

| 层 | 形态 | 谁读 | 谁写 | 地位 |
| --- | --- | --- | --- | --- |
| Goal 内上下文 | 进展摘要、`LastResults`、产物文件、执行器会话 | 本 Goal 的大脑/成员 | Runtime / Worker | 随 Goal 结束归档，不是记忆 |
| **L1 长期记忆** | `memory/<topic>.md` 纯 Markdown，一行一条 | 大脑、成员（只读挂载后自行 `grep`）、用户 | 只有 Runtime 的 `MemoryWriter` | **权威来源**，可人工编辑，可放进 git |
| **L2 语义索引** | mem0（已有的自托管服务，embedding + pgvector） | Runtime 召回后注入大脑提示词 | Runtime 同步任务 | **L1 的派生索引**，可整体重建 |

L1 与 L2 不是两份独立记忆：同一条事实只在 L1 写一次，L2 只是它的语义索引，元数据指回 L1。这避免了「两边各记一半」的问题，也让 mem0 不可用时系统退化为纯 L1 而不丢数据。

另外，L2 可选只读召回用户 mem0 个人记忆池中的条目（偏好、环境事实），配置 `memory.mem0.personalRecall`，默认关闭；这部分 my-bot 只读不写。

### 14.2 L1 格式

```
/data/memory/
  00-index.md                     Runtime 生成：每个主题文件的标题 + 摘要
  <topic>.md                      主题文件，topic 匹配 [a-z0-9-]{1,48}
```

主题文件头两行固定为 `# 标题`、`> 一句话摘要`。条目一行一条：

```
- 2026-10-21 [m_01JABC...] 现象 → 原因 → 结论/做法（来源 g_01J...，r3）
```

- 日期由 Runtime 填写（绝对日期），`m_<ulid>` 为条目 ID，供 L2 对账。
- 用户手工编辑时可以不写 ID，下一次对账由 Runtime 补上。
- 与仓库 `AGENTS.md` 的 `docs/memory/` 约定同构，便于人工维护。

### 14.3 写入

1. 大脑在 `finish` 决策（或任意一轮决策）中可附带：
   ```json
   "memories": [{"topic": "notes-workflow", "text": "inbox 中带 #draft 的笔记不进索引 → 用户约定 → 整理时跳过", "kind": "fact|pitfall|preference"}]
   ```
   成员不直接写记忆；成员的发现由大脑在汇总时决定是否沉淀。Goal 失败或取消时，若大脑的最后一次决策带了 `memories` 也照常提交（踩坑经验常来自失败），但 Runtime 不会为写记忆额外唤醒大脑（flow G18）。
2. Activity `CommitMemories` 校验：单条 ≤ 300 字；topic 合法；凭据特征扫描（私钥头、`sk-`、`AKIA`、JWT、`password=` 等）命中即拒绝并记事件；同主题内规范化文本已存在则跳过。
3. 交给 `MemoryWriter` Actor（单实例，单线程）追加到 `<topic>.md` 并重建 `00-index.md`。写文件用「写临时文件 → fsync → rename」，保证不出现半行。
4. Actor 写成功后发 `memory.written` 事件；L2 同步由 14.5 的任务异步完成，**不阻塞 Goal**。
5. 幂等：条目 ID 由 `uuid5(goalID + round + 序号)` 派生，Activity 重试不会重复追加。

### 14.4 读取

- **大脑**：提示词带 `[memory index]`（`00-index.md` 全文，上限 8 KiB）和 `[recalled]`（L2 召回结果）；正文由大脑在工作区 `memory/` 下自行 `grep` / 读文件。Runtime 不做关键词抽取（中文分词不可靠），关键词检索交给执行器自己的工具。
- **成员**：工作区同样只读挂载 `memory/`，提示词只带 `[memory index]`；大脑可在 Step 的 `inputs` 中点名相关主题文件。
- **L2 召回**：Activity `RecallMemory` 以「Goal 目标 + 本轮大脑上一次 summary」为查询，在 mem0 中检索 `agent_id=my-bot` 的条目 top-5（相似度阈值可配）。结果按条目 ID 回读 L1，只注入 L1 中仍存在的条目，并附 `memory/<topic>.md` 出处；L1 已删除的条目即使 mem0 还在也不注入。
- mem0 超时（2s）或不可用：跳过 `[recalled]`，记 `memory.l2_unavailable` 事件，Goal 照常推进。

### 14.5 L1 → L2 同步与对账

- 新条目：`MemoryWriter` 写完后，同步任务调用 mem0 add（`infer=false`，正文原样），metadata 带 `{source: "my-bot", entryID, topic, goalID}`，并记录 `memsync:<entryID> = {mem0ID, sha256}` 到 Dapr State。
- 对账（每 10 分钟，及 Runtime 启动时）：扫描 L1 全部条目，与 `memsync:*` 比较——新增的补 add；文本哈希变化的先 delete 后 add；L1 已删除的从 mem0 删除。用户直接改 Markdown 因此会自动反映到 L2。
- 实现前核验：自托管 mem0 的 REST 接口（add / search / delete、`infer=false`、按 metadata 过滤）与集群内访问地址；结论写回本节。

### 14.6 部署

- L1 目录是单节点 hostPath（`/var/lib/mybot/memory`），Runtime 以读写方式挂载，Worker 以只读方式挂载到每个工作区的 `memory/`。多节点时与工作区一样改用 RWX 存储（如 NFS）。
- Runtime 多副本时，写入串行由 `MemoryWriter` Actor 保证；文件只有一个写入方。
- 用户可以把 L1 目录做成 git 仓库自行提交；Runtime 不执行 git 操作。
- 一致性（flow G17）：同一 Step 运行期间，`memory/` 可能被其他 Goal 的写入改变；rename 保证不会读到半个文件，但前后两次读取可能不同，这一点接受。
- mem0 地址与凭据通过 Secret 下发给 Runtime；Worker 不访问 mem0。

### 14.7 与 Dapr Agents 的差异

Dapr Agents 的「记忆」本质是**对话历史存储**：默认进程内列表，可选 State Store（按 workflow 实例存整段消息数组）或向量库；Agent 之间靠广播 topic 把同伴消息写进各自记忆。它没有跨实例的知识沉淀机制，向量记忆的相似度检索在 Agent 循环里也没有被调用（见 §15）。my-bot 的记忆面向**跨 Goal 的知识**，对话历史交给执行器会话，两者不混用。

## 15. 附：参照 Dapr Agents 的取舍

依据 dapr-agents main 分支 commit `7049c0b`（2026-10-14）源码阅读。已借鉴的做法分散在 §5.0、§5.2、§5.3、§5.5、§6、§7.2、§8、§9.2、§10、§11，此处只列**刻意不照搬**的点，避免后续回退：

| Dapr Agents 现状 | my-bot 做法 | 位置 |
| --- | --- | --- |
| 编排每轮只派一个成员并阻塞等待 | 一轮内并行派多个成员 | §5.2、§5.3 |
| 全仓库无 ContinueAsNew，历史只靠 `max_iterations` 兜底 | 每轮 ContinueAsNew | §5.2 |
| 注册表无心跳/TTL、按名字而非实例、无容量 | 实例粒度 + 心跳 TTL + capacity + 索引定期清理 | §5.1、§6 |
| Workflow 内用 `uuid4()` 生成子实例 ID | 所有 ID 确定性推导 | §5.2 |
| 派发时 agent 名退化为子串模糊匹配 | 角色精确匹配 | §5.3 |
| 非法决策直接 continue 或 raise，不告诉模型原因 | 校验错误回传大脑限次重试 | §5.3 |
| 编排者不经验收就把步骤标为完成；成员结果以 `"Error:"` 开头即整体失败 | Runtime 验收，Step 级失败处理 | §5.2、§7.4 |
| 执行器在 Activity 内运行（至少一次），无超时、无取消，重试可能重复执行工具 | 执行在 Worker，Role.timeout + CancelTask，副作用不确定时记 `interrupted` | §5.2、§7.2 |
| 子工作流派发无超时、无重试策略；全局单一 RetryPolicy | 按 Activity 类型设定重试 | §5.2 |
| 对话记忆为单键 JSON 数组，读改写整组；向量记忆的相似度检索未被 Agent 循环调用；会话摘要按实例 ID 存，新实例读不到 | 对话由执行器会话负责；跨 Goal 知识走 L1 Markdown + L2 mem0 召回 | §14 |
| tracing 上下文只缓存在内存 | traceparent 持久化 | §10 |
| Runtime 内置 MCP 客户端、经 Conversation API（alpha）调模型 | 首版不在 Runtime 跑工具/模型循环；M3 的 Go `llm` 大脑再评估 | §1.2、§11 |

