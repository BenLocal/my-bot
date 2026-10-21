# 系统架构

> 组件边界、依赖方向、关键链路与设计决策；动结构、加模块、接外部依赖前先看本文。完整契约以 `docs/spec.md` 为准。

本文是 `docs/spec.md` 的导读：只摘要结构与必须遵守的边界，细节（状态键、API、决策 JSON、里程碑、验收条件）一律以 spec 为准，**不要在这里复制一份**。技术选型和构建命令在 `tech-stack.md`。

## 一、整体结构

```
 mybotctl / HTTP ──Bearer──> mybot-runtime ×N ──A2A HTTP+SSE（直连 Pod）──> mybot-worker-<executor> ×M
                              │  Dapr sidecar                                 │  Dapr sidecar
                              │  Workflow / Actor / State                     │  State / Pub/Sub
                              └────────────────────> Redis <──────────────────┘
                                                     (state + actor state + pubsub, AOF)
```

- 部署形态：k3s 单节点起步（标准 K8s 资源，可迁 k0s / 多节点）；本地开发用 `dapr run` + docker Redis。
- 独立部署单元：`mybot-runtime`（Deployment）、`mybot-worker-pi`（StatefulSet，可多副本）、`mybot-worker-codex-<account>`（每账号 1 副本）、`redis`。

## 二、模块与依赖方向

| 包 | 职责 | 禁止依赖 |
|---|---|---|
| `internal/core` | 领域模型、状态机、确定性策略、大脑决策校验（纯函数） | Dapr、A2A、HTTP、任何 IO |
| `internal/orchestrator` | Dapr Workflow 与 Activity | `executor/*` |
| `internal/scheduler` | `Scheduler` Actor：三级容量、Worker 选择、会话亲和、账号状态、drain | `executor/*` |
| `internal/memory` | L1 文件与 `MemoryWriter` Actor、L2（mem0）同步与召回 | `executor/*` |
| `internal/registry` / `internal/store` | 注册表、记录与索引（封装 Dapr State） | `api`、`orchestrator` |
| `internal/a2a` | A2A 客户端/服务端封装、`mybot/*` 扩展 metadata | `orchestrator` |
| `internal/api` | Runtime HTTP handler | Dapr 细节（经 core/store 接口） |
| `internal/executor/{pi,codex,fake}` | 执行器 Driver | Dapr |

- 依赖方向：`cmd → api/orchestrator/scheduler/memory → core`；`core` 是叶子。Worker 侧 Dapr 访问只在 `cmd/mybot-worker` 与 `store`。
- Runtime 不启动执行器进程、不持有执行器凭据；Worker 不决定下一轮做什么。

## 三、关键链路：一个 brain Goal 的一轮

1. `POST /v1/goals`（带 `Idempotency-Key`）→ 写投影 + 以 Goal ID 启动 `GoalWorkflow`。
2. Workflow 取槽位 → `SubmitAttempt` 把大脑 Step 发给大脑角色所在 Worker → 等 `attempt-done` 事件（60s 轮询兜底）。
3. `ParseDecision` 校验大脑 JSON → 对每个成员 Step 取槽位并并行提交（≤ `maxParallel`）→ fan-in。
4. `CollectStep` / `CollectRound` 拉产物、验收、写进展摘要 → 释放槽位 → `ContinueAsNew` 进入下一轮，成员结果作为大脑输入。

## 四、数据流与存储

- 权威状态：Goal 进度 = Dapr Workflow；对外查询读 Activity 写的投影 `goal:<id>`；执行事实 = Worker 的 `worker:<id>:attempt:<key>`。
- Pub/Sub `attempt-events` 只用于唤醒，从不推进状态；重复/丢失都无害。
- 大文件不进 Redis。工作区和产物在所有 Worker 共同挂载的共享卷上（单节点 hostPath），只登记路径与 sha256；执行器会话留在各 Pod 自己的 PVC。

## 五、关键设计决策

- **大脑是角色不是工具**：任意执行器可担任，Runtime 只认 Role 配置。重新评估信号：需要大脑实时打断成员时。
- **同一轮 Step 互不依赖，依赖由大脑跨轮表达**：省掉 DAG 引擎。信号：出现大量固定多步依赖流程。
- **首版只用 Redis，不上 PostgreSQL**：用户要求少依赖。信号：需要关系查询或 Workflow 历史使 Redis 内存失控。
- **派发走 A2A 直连 Pod，不走 Dapr service invocation**：Attempt 绑定实例，且 SSE 转发未验证。
- **单个 Scheduler Actor 管所有容量**：一次调用原子检查全局/账号/实例三级容量并选 Worker，避免依次获取的持有并等待（flow G3）。信号：调度成为瓶颈时按执行器拆分。
- **工作区放共享卷**：同一代码仓库可被不同执行器的成员连续使用（flow G8）。信号：上多节点时需 RWX 存储。
- **Codex 每账号 1 副本**：OAuth 凭据不复制。扩容 = 加账号或改 API key。

## 六、扩展点

- 新执行器：在 `internal/executor/<name>` 实现 `Driver`，`mybot-worker --executor <name>`，加部署清单与 Role 配置；参照 `fake`。
- 新确定性策略：`internal/core` 加纯函数 + 单测。
- 禁止：在 Workflow 代码里做 IO / 取系统时间 / 随机；Runtime 直接调执行器 CLI；把凭据写进 Redis 或日志。
