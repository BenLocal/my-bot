package core

import "time"

// Task, Goal, Loop share the same core structure; kind distinguishes them.
type Kind string

const (
	KindGoal Kind = "goal"
	KindLoop Kind = "loop"
	KindTask Kind = "task"
)

// GoalSpec is the immutable specification of a Goal/Loop/Task, frozen at creation.
type GoalSpec struct {
	Kind       Kind     `json:"kind"`
	Objective  string   `json:"objective"`         // Goal: 长期目标; Loop: 指令; Task: 一次性指令
	Acceptance string   `json:"acceptance"`        // 验收条件
	Strategy   string   `json:"strategy"`          // single | pipeline | round_robin | brain
	Brain      string   `json:"brain,omitempty"`   // 大脑角色名; 缺省用 defaultBrain
	Members    []string `json:"members,omitempty"` // 成员角色名列表

	MaxParallel int `json:"maxParallel,omitempty"` // 每轮最大并行 Step 数; 缺省用 limits.goal.maxParallel

	Inputs     map[string]Mount     `json:"inputs,omitempty"`
	Workspaces map[string]Workspace `json:"workspaces,omitempty"`

	Limits Limits `json:"limits"`

	// Loop 专属
	Instruction string    `json:"instruction,omitempty"`
	Schedule    *Schedule `json:"schedule,omitempty"`

	Check *Check `json:"check,omitempty"`

	// Roles 是创建时固化的角色定义快照（brain 与 members），在途 Goal 不受 roles.yaml 变更影响（spec §5.2）。
	Roles map[string]Role `json:"roles,omitempty"`

	Version string `json:"version,omitempty"` // Spec 版本，用于 Workflow 重放兼容
}

type Mount struct {
	Path string `json:"path"` // 宿主路径
	Mode string `json:"mode"` // ro | rw
}

type Workspace struct {
	Git string `json:"git,omitempty"` // git 仓库 URL
	Ref string `json:"ref,omitempty"` // 分支/tag/commit
}

type Limits struct {
	MaxRounds        int        `json:"maxRounds,omitempty"`
	Deadline         *time.Time `json:"deadline,omitempty"`
	MaxTokens        int64      `json:"maxTokens,omitempty"`
	FailStreak       int        `json:"failStreak,omitempty"` // 连续失败轮数上限
	NoProgress       int        `json:"noProgress,omitempty"` // 连续无进展轮数上限
	MaxStepsPerRound int        `json:"maxStepsPerRound,omitempty"`
	AskTimeout       string     `json:"askTimeout,omitempty"`   // ask_user 等待上限，缺省 24h
	OrphanAfter      string     `json:"orphanAfter,omitempty"`  // Worker 注册过期多久判孤儿，缺省 5m
	AffinityWait     string     `json:"affinityWait,omitempty"` // 会话亲和等待上限，缺省 5m
}

type Schedule struct {
	Every   string `json:"every,omitempty"`   // e.g. "30m"
	Cron    string `json:"cron,omitempty"`    // e.g. "0 8 * * *"
	Catchup string `json:"catchup,omitempty"` // latest | none
}

type Check struct {
	Command   string `json:"command"`
	Workspace string `json:"workspace,omitempty"`
	Timeout   string `json:"timeout,omitempty"` // e.g. "10m"
}

// Role describes a single role definition.
type Role struct {
	Name         string   `json:"name"`
	Executor     string   `json:"executor"`
	Account      string   `json:"account"`
	Instructions string   `json:"instructions"`
	Timeout      string   `json:"timeout"`
	Capabilities []string `json:"capabilities,omitempty"`

	// 创建 Goal 时固化的指令正文及其 sha256（spec §8 创建流程）。
	InstructionsText   string `json:"instructionsText,omitempty"`
	InstructionsSha256 string `json:"instructionsSha256,omitempty"`
}

// WorkerRecord is a worker instance in the registry.
type WorkerRecord struct {
	ID          string    `json:"id"`
	Executor    string    `json:"executor"`
	Account     string    `json:"account"`
	Endpoint    string    `json:"endpoint"`
	Capacity    int       `json:"capacity"`
	Version     string    `json:"version"`
	HeartbeatAt time.Time `json:"heartbeatAt"`
	AuthOK      bool      `json:"authOK"`
}

// ArtifactRef references a produced artifact.
type ArtifactRef struct {
	Path    string `json:"path"`
	Sha256  string `json:"sha256"`
	Size    int64  `json:"size"`
	Round   int    `json:"round"`
	Attempt int    `json:"attempt"`
}

// Event is an entry in the public event stream.
type Event struct {
	Seq  int         `json:"seq"`
	Type string      `json:"type"`
	At   time.Time   `json:"at"`
	Data interface{} `json:"data,omitempty"`
}

// Round represents a single round of execution.
type Round struct {
	N           int         `json:"n"`
	ScheduledAt *time.Time  `json:"scheduledAt,omitempty"`
	Status      RoundStatus `json:"status"`
	Summary     string      `json:"summary,omitempty"`
	Decision    *Decision   `json:"decision,omitempty"`
}

type RoundStatus string

const (
	RoundPending   RoundStatus = "pending"
	RoundRunning   RoundStatus = "running"
	RoundSucceeded RoundStatus = "succeeded"
	RoundFailed    RoundStatus = "failed"
	RoundRejected  RoundStatus = "rejected"
)

// Step is a single sub-task within a round.
type Step struct {
	ID          string     `json:"id"`
	Role        string     `json:"role"`
	Instruction string     `json:"instruction"`
	Inputs      []string   `json:"inputs,omitempty"`
	Status      StepStatus `json:"status"`
	Attempt     int        `json:"attempt"` // 当前第几次尝试（从 1 开始）
}

type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepCancelled StepStatus = "cancelled"
)

// Attempt records a single submission to a worker.
type Attempt struct {
	Key          string        `json:"key"`
	Worker       string        `json:"worker"`
	RemoteTaskID string        `json:"remoteTaskID,omitempty"`
	Status       AttemptStatus `json:"status"`
	Usage        Usage         `json:"usage,omitempty"`
}

type AttemptStatus string

const (
	AttemptSubmitting  AttemptStatus = "submitting"
	AttemptSubmitted   AttemptStatus = "submitted"
	AttemptWorking     AttemptStatus = "working"
	AttemptCompleted   AttemptStatus = "completed"
	AttemptFailed      AttemptStatus = "failed"
	AttemptRejected    AttemptStatus = "rejected"
	AttemptCancelled   AttemptStatus = "cancelled"
	AttemptInterrupted AttemptStatus = "interrupted"
	AttemptUnknown     AttemptStatus = "unknown"
)

// Usage tracks token usage for an attempt.
type Usage struct {
	InputTokens  int64 `json:"inputTokens,omitempty"`
	OutputTokens int64 `json:"outputTokens,omitempty"`
}

// GoalState is the projected state of a Goal, written by activities.
type GoalState struct {
	ID        string     `json:"id"`
	Kind      Kind       `json:"kind"`
	Status    GoalStatus `json:"status"`
	Round     int        `json:"round"`
	Progress  Progress   `json:"progress"`
	Budget    Budget     `json:"budget"`
	Reason    string     `json:"reason,omitempty"` // 终态 / waiting / paused 的原因
	Ask       *AskState  `json:"ask,omitempty"`    // 等待中的 ask_user
	UpdatedAt time.Time  `json:"updatedAt"`
}

// AskState 是等待中的 ask_user 请求（spec §5.3）。
type AskState struct {
	RequestID string    `json:"requestID"`
	Question  string    `json:"question"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type GoalStatus string

const (
	GoalQueued    GoalStatus = "queued"
	GoalRunning   GoalStatus = "running"
	GoalSucceeded GoalStatus = "succeeded"
	GoalFailed    GoalStatus = "failed"
	GoalPaused    GoalStatus = "paused"
	GoalWaiting   GoalStatus = "waiting"
	GoalCancelled GoalStatus = "cancelled"
)

// Loop has no GoalSucceeded; it completes as GoalFailed? No — completed.
// For Loop, normal termination is "completed". We reuse GoalStatus but add:
const (
	GoalCompleted GoalStatus = "completed" // Loop 正常结束
)

type Progress struct {
	Summary      string `json:"summary,omitempty"`
	LastDecision string `json:"lastDecision,omitempty"`
}

type Budget struct {
	TokensUsed  int64 `json:"tokensUsed"`
	TokensLimit int64 `json:"tokensLimit"`
}

// StepResult is the result of a completed step.
type StepResult struct {
	StepID    string        `json:"stepID"`
	Role      string        `json:"role"`
	Status    StepStatus    `json:"status"`
	Summary   string        `json:"summary"`
	Done      bool          `json:"done,omitempty"` // 成员 output contract 的 done（确定性策略判完成用）
	Output    string        `json:"output,omitempty"`
	Reason    string        `json:"reason,omitempty"` // 失败原因
	Attempt   int           `json:"attempt,omitempty"`
	WorkerID  string        `json:"workerID,omitempty"`
	Artifacts []ArtifactRef `json:"artifacts,omitempty"`
	Usage     Usage         `json:"usage,omitempty"`
	Decision  *Decision     `json:"decision,omitempty"` // for brain steps
}

// RoundSummary is the result of a round.
type RoundSummary struct {
	Round   int          `json:"round"`
	Status  RoundStatus  `json:"status"`
	Summary string       `json:"summary"`
	Steps   []StepResult `json:"steps"`
}

// Decision is the structured decision output by the brain.
// 字段与 spec §5.3 的决策 JSON 一一对应。
type Decision struct {
	Action   string         `json:"action"` // dispatch | finish | ask_user
	Summary  string         `json:"summary,omitempty"`
	Steps    []DispatchStep `json:"steps,omitempty"`
	Done     bool           `json:"done,omitempty"`
	Question string         `json:"question,omitempty"` // ask_user 时必填
	Memories []MemoryEntry  `json:"memories,omitempty"`
}

type DispatchStep struct {
	ID          string   `json:"id"`
	Role        string   `json:"role"`
	Instruction string   `json:"instruction"`
	Inputs      []string `json:"inputs,omitempty"` // 产物引用 r<round>/<stepID>/<name>
	Workspace   string   `json:"workspace,omitempty"`
}

type MemoryEntry struct {
	Topic string `json:"topic"`
	Text  string `json:"text"`
	Kind  string `json:"kind"` // fact | pitfall | preference
}
