# 技术栈与实现规范

> 技术选型、存储与配置约定、构建与验证命令；写代码前先看本文，按既有方式扩展。

结构与取舍见 `architecture.md`，完整契约见 `docs/spec.md`。以下版本于 2026-10-21 经 P0 核验锁定；升级任何一项前先在 spikes 中复测相关行为。

## 一、项目概览

- 定位：个人常驻 Agent 平台，Go Runtime 编排 Pi / Codex 等执行器，大脑角色可由任意执行器担任，多 Agent 协作推进长任务。
- 仓库形态：单 Go module，三个二进制（`mybot-runtime`、`mybot-worker`、`mybotctl`）。
- 语言：Go（本机 go1.27.1）。

## 二、核心技术栈

| 类别 | 选型 | 版本 | 备注 |
|---|---|---|---|
| 语言 | Go | 1.27 | |
| 分布式运行时 | Dapr（Workflow、Actor、State、Pub/Sub） | runtime 1.18.7、CLI 1.18.2 | Go SDK `github.com/dapr/go-sdk` v1.15.0；Workflow 用 `github.com/dapr/durabletask-go/workflow`（v0.12.6，随 Dapr 1.18；不越级到 v0.15）；Actor 必须用 `service/http` |
| Agent 协议 | A2A 1.0 | a2a-go v2.6.0 | `github.com/a2aproject/a2a-go/v2`，HTTP JSON-RPC + SSE |
| 存储 / 消息 | Redis（state.redis + pubsub.redis） | 随 `dapr init` / 部署清单 | AOF 持久化；`keyPrefix: none`；首版不引入 PostgreSQL |
| 编排 | k3s | M2 安装时锁定 | 只用标准 K8s 资源；kustomize |
| 执行器 | pi（`--mode rpc`）、codex（`app-server`） | pi 1.3.0、codex-cli 0.163.0 | 只在 Worker 镜像内；协议细节见 spec §7.2 |
| 长期记忆 L2 | 自托管 mem0（REST） | 现有部署 | 见 spec §14.5 |

同类能力已有选型时不引入第二套（HTTP 路由、日志、配置库各只用一种，首次引入时记录在此）。

已引入的通用库（M0）：
- HTTP 路由：标准库 `net/http`（1.22+ 路由）。`github.com/go-chi/chi/v5` 只因 `daprhttp.NewServiceWithMux` 要求 chi Mux 而出现在 `cmd/mybot-runtime`，业务路由不用它。
- 日志：`log/slog`（Runtime 输出 JSON 到 stdout）。
- 配置解析：`gopkg.in/yaml.v3`（`config/roles.yaml`，`core.ParseRolesConfig`）。
- ID：`github.com/oklog/ulid/v2`（goalID）、`github.com/google/uuid`（uuid5 会话 ID）。

## 三、存储约定

- 所有持久化经 Dapr State API，封装在 `internal/store`；业务代码不直连 Redis。
- 键名规范见 spec §6；并发写用 ETag。Redis 上的 State 事务失败时不回滚，跨键更新用「顺序写 + 每步幂等 + 对账修复」，不依赖事务原子性。
- 首次写入去重（attemptKey、Idempotency-Key）用条件写，不先读后写。
- Redis 上带 ETag 的写 / 删必须用 `first-write` 并发模式（SDK 默认 last-write 会忽略 ETag），见 spec §6 与 `docs/memory/m0-development-decisions.md`；`internal/store` 已封装，业务代码不要直接调 SDK。
- 测试用内存实现 `store.NewMemStore()`（同样的 ETag / first-write / TTL 语义）。

## 四、配置

- Role 与并发上限：`config/roles.yaml`（K8s 中为 ConfigMap）。
- 运行参数：环境变量 / 命令行 flag。
- 凭据：K8s Secret 挂载给对应 Worker；Runtime 和 Redis 只存引用。

## 五、构建与验证命令

```bash
gofmt -l . && go vet ./...
go test ./...
go test -race ./internal/...          # 并发相关改动必跑
go build ./cmd/...

# 端到端验收（需要 dapr CLI + redis-server 或 docker，约 10 分钟）
make test-e2e                          # MYBOT_E2E_LOGDIR=<dir> 保留各进程日志

# 本地联调（Redis 容器 + sidecar + Runtime + N 个 fake Worker，组件在 deploy/dapr/local/）
./scripts/dev/dev.sh 2                 # Ctrl-C 或 ./scripts/dev/stop.sh 停止
```

## 六、部署

kustomize：`deploy/k8s/base` + `deploy/k8s/overlays/k3s`；Dapr 用官方 Helm chart 装到 `dapr-system`。细节见 spec §9 与 `environment.md`。

## 七、其他约定

- 提交：conventional commits。
- 日志：JSON 到 stdout，必带 `goalID`、`round`、`attemptKey`、`workerID`（有则带）。
- 测试：`core` 纯单测；Workflow/幂等/恢复语义先用 `fake` 执行器验证，再接真实执行器。
