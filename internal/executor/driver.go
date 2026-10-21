// Package executor 定义执行器驱动的最小契约。
//
// Runtime 与 Worker 之间唯一的执行抽象是本包的 Driver 接口；具体执行器
// （pi、codex、fake 等）实现该接口，Worker 侧通过它启动、查询、取消与恢复
// 一次 Attempt。本包不依赖 Dapr / A2A / HTTP，只依赖 core 的用量模型。
package executor

import (
	"context"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
)

// Phase 是 Driver 汇报的 Attempt 生命周期阶段。
//
// 正常执行只会出现 working / completed / failed / paused；interrupted 仅供
// Recover 使用：当 Worker 无法确认或恢复执行时（工具调用副作用不确定、找不到
// 会话或提示），由 Recover 返回，交给 Runtime 按重试规则处理。
type Phase string

const (
	PhaseWorking     Phase = "working"
	PhaseCompleted   Phase = "completed"
	PhaseFailed      Phase = "failed"
	PhasePaused      Phase = "paused"
	PhaseInterrupted Phase = "interrupted"
)

// SessionPolicy 控制一次 Attempt 对执行器会话的复用策略。
type SessionPolicy string

const (
	// SessionPolicyReuse 复用同一 Goal + 角色的上一次会话。
	SessionPolicyReuse SessionPolicy = "reuse"
	// SessionPolicyFresh 为本次 Attempt 新建会话。
	SessionPolicyFresh SessionPolicy = "fresh"
)

// Input 描述一次 Step 可用的上游产物。Worker 在 Driver.Start 前校验 Sha256，
// 不一致则 Step 失败且不可重试（spec §7.1）。
type Input struct {
	Name   string
	Path   string
	Sha256 string
}

// Artifact 描述一次 Attempt 产出的文件。Worker 负责计算 Sha256 与 Size，
// Driver 返回时已填好，交由 Worker 登记到共享卷（spec §7.1）。
type Artifact struct {
	Name   string
	Path   string
	Sha256 string
	Size   int64
}

// StartReq 是 Driver.Start 的完整输入。
type StartReq struct {
	AttemptKey    string
	GoalID        string
	Round         int
	StepID        string
	Role          string
	Instruction   string
	SessionPolicy SessionPolicy
	SessionID     string
	Workspace     string
	Inputs        []Input
	Timeout       time.Duration
}

// Handle 是不透明的执行句柄。调用方不得解析或推断其内容。
type Handle string

// AttemptRecord 描述一次非终态、且可能跨越 Worker 重启的 Attempt。Worker 启动
// 时扫描这类记录并逐条调用 Driver.Recover（spec §7.2）。
type AttemptRecord struct {
	AttemptKey   string
	GoalID       string
	Round        int
	StepID       string
	Role         string
	SessionID    string
	RemoteTaskID string
	Workspace    string
	Instruction  string
	Timeout      time.Duration
}

// EventType 是结构化事件的类型。
type EventType string

const (
	// EventProgress 是中间进展事件。
	EventProgress EventType = "progress"
	// EventArtifact 是产物事件。
	EventArtifact EventType = "artifact"
	// EventTerminal 是终态事件，必须作为最后一个事件。
	EventTerminal EventType = "terminal"
)

// Event 是 Driver 通过 Events 通道发布的结构化事件。
//
// 正常执行时，最后一个事件必须是 EventTerminal，且携带 Phase / Retryable /
// Reason / Usage；没有终态事件就关闭通道视为 Driver 缺陷（spec §7.2）。
type Event struct {
	Type      EventType
	Text      string
	Artifacts []Artifact
	Phase     Phase
	Retryable bool
	Reason    string
	Usage     core.Usage
}

// Status 是一次 Attempt 的当前或终态快照。
type Status struct {
	Phase     Phase
	Retryable bool
	Reason    string
	Usage     core.Usage
}

// Driver 是执行器驱动的统一契约。实现必须是并发安全的：Worker 会在
// Start 返回后异步读取 Events、并发查询 Status、随时 Cancel。
type Driver interface {
	// Start 启动或续用会话，异步执行，立即返回 Handle。
	Start(ctx context.Context, req StartReq) (Handle, error)
	// Status 返回句柄当前的阶段快照。
	Status(ctx context.Context, h Handle) (Status, error)
	// Events 返回该句柄的结构化事件通道，终态后由 Driver 关闭。
	Events(h Handle) <-chan Event
	// Cancel 请求取消执行；最终状态以 Status 为准。
	Cancel(ctx context.Context, h Handle) error
	// Recover 在 Worker 重启后核实一条非终态记录，返回其应转成的终态/工作态。
	Recover(ctx context.Context, rec AttemptRecord) (Status, error)
}
