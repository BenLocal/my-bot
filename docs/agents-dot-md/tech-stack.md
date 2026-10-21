# 技术栈与实现规范

> 技术选型、存储与配置约定、构建与验证命令；写代码前先看本文，按既有方式扩展。

结构与取舍见 `architecture.md`，完整契约见 `docs/spec.md`。标「待锁定」的版本在 M0 核验后回填到这里和 spec。

## 一、项目概览

- 定位：个人常驻 Agent 平台，Go Runtime 编排 Pi / Codex 等执行器，大脑角色可由任意执行器担任，多 Agent 协作推进长任务。
- 仓库形态：单 Go module，三个二进制（`mybot-runtime`、`mybot-worker`、`mybotctl`）。
- 语言：Go（本机 go1.27.1）。

## 二、核心技术栈

| 类别 | 选型 | 版本 | 备注 |
|---|---|---|---|
| 语言 | Go | 1.27 | |
| 分布式运行时 | Dapr（Workflow、Actor、State、Pub/Sub） | 待锁定 | Go SDK `github.com/dapr/go-sdk` |
| Agent 协议 | A2A 1.0.1 | SDK 待锁定 | `github.com/a2aproject/a2a-go`，HTTP JSON-RPC + SSE |
| 存储 / 消息 | Redis（state.redis + pubsub.redis） | 待锁定 | AOF 持久化；首版不引入 PostgreSQL |
| 编排 | k3s | 待锁定 | 只用标准 K8s 资源；kustomize |
| 执行器 | pi（`--mode rpc`）、codex（`app-server`） | 待核验 | 只在 Worker 镜像内 |

同类能力已有选型时不引入第二套（HTTP 路由、日志、配置库各只用一种，首次引入时记录在此）。

## 三、存储约定

- 所有持久化经 Dapr State API，封装在 `internal/store`；业务代码不直连 Redis。
- 键名规范见 spec §6；跨键原子更新用 State 事务，并发写用 ETag。
- 首次写入去重（attemptKey、Idempotency-Key）用条件写，不先读后写。

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

# 本地联调（M0 起，组件在 deploy/dapr/local/）
docker run -d --name mybot-redis -p 6379:6379 redis --appendonly yes
dapr run --app-id mybot-runtime --resources-path deploy/dapr/local -- go run ./cmd/mybot-runtime
dapr run --app-id mybot-worker-fake --resources-path deploy/dapr/local -- go run ./cmd/mybot-worker --executor fake
```

以上命令在对应代码落地后按实际情况修正。

## 六、部署

kustomize：`deploy/k8s/base` + `deploy/k8s/overlays/k3s`；Dapr 用官方 Helm chart 装到 `dapr-system`。细节见 spec §9 与 `environment.md`。

## 七、其他约定

- 提交：conventional commits。
- 日志：JSON 到 stdout，必带 `goalID`、`round`、`attemptKey`、`workerID`（有则带）。
- 测试：`core` 纯单测；Workflow/幂等/恢复语义先用 `fake` 执行器验证，再接真实执行器。
