# 代码 checklist（强制自检）

> 写完 / 改完代码必须逐条对照的硬性自检清单；新增条目直接追加本文件。

本文件由 `AGENTS.md`（即 `CLAUDE.md`）拆出，收敛「写代码时必须逐条自检」的硬性条目。顶层只留一条强制指针，条目细节都在这里；新增条目直接追加到本文件，不要写回 `AGENTS.md`。

**通用条目**是跨语言的硬红线；**语言专项**由 agents-dot-md skill 的 `scaffold.py` 按本仓库实际语言注入，是该语言公认的固定规范。落地时请按本项目实际情况增删改——不适用的直接删掉，别留着占位；踩过的坑按下面的格式追加。

## 怎么写一条 checklist

每条包含三部分，缺一不可——只写规则不给例子，下一个 agent 照样会踩：

1. **规则**：一句话说清「必须怎样 / 禁止怎样」。
2. **反例**：真实踩过的写法，标明后果（报什么错、线上出过什么事故）。
3. **正例**：照抄就对的写法。

条目来源优先级：**线上事故 > 评审反复提的意见 > 团队口头约定**。没踩过的坑不必预防性地写进来，这里只放硬性红线；软性建议归 `coding-guidelines.md`。

---

# 通用条目

## 1. Workflow 代码必须确定性

`internal/orchestrator` 里的 Workflow 函数会被 Dapr 重放。Workflow 体内禁止 IO、网络、读系统时间、随机数、遍历 map 决定顺序、启动 goroutine；这些放 Activity。时间只用 `ctx.CurrentUTCDateTime()`，等待只用 `CreateTimer` / `WaitForExternalEvent`。

- 反例：Workflow 里 `time.Now()` 判断截止时间；直接 `http.Post` 调 Worker。
- 正例：`now := ctx.CurrentUTCDateTime()`；提交走 `ctx.CallActivity(SubmitAttempt, ...)`。
- 改了 Workflow 逻辑要考虑在途实例重放（spec §5.2 末段）。

## 2. 所有提交与创建都要幂等

对外创建接口要求 `Idempotency-Key`；向 Worker 提交必须带 `mybot/attemptKey`；Worker 先条件写去重记录再启动执行器。超时后先按 key 对账，**不得**直接新建尝试；查不到时进入 `unknown`。

- 反例：`SendMessage` 超时就换个 key 重发，导致同一 Step 跑两次。
- 正例：Activity 内超时 → `GetTask`/`getByAttemptKey` 对账 → 查到则复用。

## 3. Pub/Sub 只唤醒，不推进状态

收到 `attempt-events` 后只 `RaiseEvent`，状态以 `PollAttempt` / 记录为准。不要在订阅处理函数里改 Goal 投影或释放槽位。

## 4. 对外 API 与 A2A 扩展字段是契约

`/v1` 的路径、返回字段、事件类型，以及 `mybot/*` metadata、大脑决策 JSON 格式，改动前先更新 `docs/spec.md`，再改代码；不兼容变更走新版本。鉴权：Runtime API 用 Bearer token，集群内用 `mybot-internal-token`。

## 5. 敏感信息不入库、不入 Redis、不入日志

模型 key、Codex/Claude 登录凭据、API token 不写进仓库文件、测试、注释、提交信息，也不写进 Dapr State、事件或日志；只存引用。本地凭据放 `dev-env.local.md`（见 `environment.md`）。

## 6. 沿用既有选型，不引入第二套同类方案

持久化统一经 `internal/store` 走 Dapr State，不直连 Redis；Agent 通信统一走 A2A。新增依赖前查 `tech-stack.md`，首次引入的库记录进去。

## 7. 大脑输出必须经校验才执行

任何来自执行器（尤其大脑）的结构化输出都当作不可信输入：先按 spec §5.3 校验角色、数量、产物引用、预算，再派发。模型声称 `done` 不等于完成，必须过 §7.4 验收。

---

# Go 专项

## G1. 每个 error 都要处理，并保留错误链

返回的 `error` 要么就地处理，要么向上返回；包装错误使用 `%w`，让调用方可以用 `errors.Is` / `errors.As` 判断根因。不要用字符串匹配错误，也不要用 `_` 丢掉有意义的错误。

- 反例：`data, _ := io.ReadAll(r)`、`return fmt.Errorf("读取失败: %v", err)`
- 正例：`return fmt.Errorf("读取配置 %s: %w", path, err)`

## G2. Context 沿调用链传递，创建后及时 cancel

会阻塞、访问网络或数据库的函数把 `context.Context` 作为第一个参数，并把上游 context 继续传下去。`context.WithCancel` / `WithTimeout` 返回的 `cancel` 必须及时 `defer cancel()`，避免定时器和子任务泄漏。

- 反例：下层函数重新用 `context.Background()`，或创建 timeout 后丢掉 `cancel`。
- 正例：`func Load(ctx context.Context, id string) error`；派生超时时紧接着写 `defer cancel()`。

## G3. 每个 goroutine 都必须有明确的退出路径

启动 goroutine 前写清谁关闭输入、谁取消 context、谁等待退出。禁止只启动不回收；共享状态使用 channel、mutex 或原子操作保护，并用 `go test -race ./...` 检查关键并发路径。

- 反例：后台 `go func() { for { work() } }()`，没有取消条件。
- 正例：循环同时监听工作 channel 与 `ctx.Done()`，调用方用 `WaitGroup` 等待退出。

## G4. I/O 资源必须关闭，HTTP 响应先检查状态

文件、HTTP response body、数据库 rows 等资源在成功创建后立即安排关闭，并检查 `Close` / `rows.Err()` 等延迟错误。HTTP 客户端设置超时，读取 body 前先处理非 2xx 状态。

- 反例：`client.Get(...)` 后直接读取 body，不设超时、不关闭响应体。
- 正例：检查请求错误后立即 `defer resp.Body.Close()`，再校验状态码并读取受限大小的响应体。

## G5. 公共 API 避免暴露可变内部状态

slice、map、指针传递的是共享数据；返回或保存后可能被外部修改时要复制。并发读写 map 必须同步，结构体中包含 mutex 时禁止按值复制。

- 反例：getter 直接返回结构体内部的 map，调用方可以绕过锁修改它。
- 正例：锁内复制 map / slice 后返回副本；含 mutex 的结构体只传指针。

## G6. 接口由使用方定义，保持小而具体

接口放在消费它的包中，只声明调用方真正需要的方法；函数能接收具体类型时不预先抽象。返回具体类型，让调用方按需要定义接口。

- 反例：生产者包预先定义包含十几个方法的 `Service` 接口，所有调用方被迫依赖它。
- 正例：消费方按实际调用定义 `type UserFinder interface { Find(context.Context, string) (User, error) }`。

## G7. 提交前统一格式化并跑静态检查

改动过的 Go 文件必须经过 `gofmt`；按项目约定运行 `go test ./...`、`go vet ./...`，并对并发相关改动补跑 `go test -race ./...`。生成代码按项目命令重新生成，不手改生成产物。

- 反例：只运行单个包的测试就提交跨包改动，或手改 `*.pb.go`。
- 正例：先跑项目生成命令，再运行 `gofmt`、全量测试与项目约定的静态检查。
