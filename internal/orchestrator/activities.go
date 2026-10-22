package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	a2ago "github.com/a2aproject/a2a-go/v2/a2a"
	daprclient "github.com/dapr/go-sdk/client"

	"github.com/BenLocal/my-bot/internal/a2a"
	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/scheduler"
	"github.com/BenLocal/my-bot/internal/store"
)

// Activity names (registered in registry.go).
const (
	ActSubmitAttempt    = "SubmitAttempt"
	ActPollAttempt      = "PollAttempt"
	ActCancelAttempt    = "CancelAttempt"
	ActAcquireSlot      = "AcquireSlot"
	ActReleaseSlot      = "ReleaseSlot"
	ActReleaseWaiter    = "ReleaseWaiter"
	ActAccountState     = "AccountState"
	ActCollectStep      = "CollectStep"
	ActProject          = "Project"
	ActFinishGoal       = "FinishGoal"
	ActSaveRoundSummary = "SaveRoundSummary"
	ActRunCheck         = "RunCheck"
)

// registryStale mirrors the registry TTL: a record whose heartbeat is older
// than this is treated as expired (spec §6 心跳 TTL 60s).
const registryStale = 60 * time.Second

// ---- Worker (A2A) ----

type SubmitAttemptReq struct {
	GoalID        string
	AttemptKey    string
	Round         int
	StepID        string
	Role          string
	WorkerID      string
	Executor      string
	Account       string
	Prompt        string
	SessionPolicy string
	Workspace     string
	Inputs        []string
	Traceparent   string
}

type SubmitAttemptResp struct {
	RemoteTaskID string
}

// SubmitAttempt sends the attempt to the leased Worker via A2A. Submission is
// idempotent by attemptKey on the Worker side, so Dapr retries are safe (spec §5.2 第 5 步).
func SubmitAttempt(ctx context.Context, req SubmitAttemptReq) (SubmitAttemptResp, error) {
	client, err := workerClient(ctx, req.Executor, req.WorkerID)
	if err != nil {
		return SubmitAttemptResp{}, fmt.Errorf("SubmitAttempt: %w", err)
	}
	meta := a2a.EncodeMetadata(nil, a2a.Metadata{
		AttemptKey:    req.AttemptKey,
		GoalID:        req.GoalID,
		Round:         req.Round,
		StepID:        req.StepID,
		Role:          req.Role,
		Workspace:     req.Workspace,
		SessionPolicy: a2a.SessionPolicy(req.SessionPolicy),
		Inputs:        req.Inputs,
		Traceparent:   req.Traceparent,
	})
	task, err := client.Submit(ctx, &a2ago.SendMessageRequest{
		Message:  a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart(req.Prompt)),
		Metadata: meta,
		Config:   &a2ago.SendMessageConfig{ReturnImmediately: true},
	})
	if err != nil {
		return SubmitAttemptResp{}, fmt.Errorf("SubmitAttempt: submit: %w", err)
	}
	return SubmitAttemptResp{RemoteTaskID: string(task.ID)}, nil
}

type PollAttemptReq struct {
	GoalID     string
	AttemptKey string
	WorkerID   string
	Executor   string
	Round      int
}

type PollAttemptResp struct {
	Reachable   bool // Worker answered the query
	WorkerAlive bool // registry record present and fresh (only meaningful when !Reachable)
	Status      core.AttemptStatus
	Reason      string
	Retryable   bool
	Text        string
	Artifacts   []core.ArtifactRef
	Usage       core.Usage
}

// PollAttempt queries the attempt by attemptKey (authoritative across Worker
// restarts). Unreachable Workers are reported in the result, not as an error,
// so the workflow can apply the orphan rule (spec §5.2 第 6 步).
func PollAttempt(ctx context.Context, req PollAttemptReq) (PollAttemptResp, error) {
	if depStore == nil {
		return PollAttemptResp{}, errors.New("PollAttempt: store not configured")
	}
	rec, err := store.LoadRegistryRecord(ctx, depStore, req.Executor, req.WorkerID)
	alive := err == nil && time.Since(rec.HeartbeatAt) < registryStale
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return PollAttemptResp{}, fmt.Errorf("PollAttempt: load worker: %w", err)
	}
	if err != nil {
		return PollAttemptResp{WorkerAlive: false}, nil
	}

	client, err := a2a.NewClient(ctx, rec.Endpoint, depToken)
	if err != nil {
		return PollAttemptResp{WorkerAlive: alive}, nil
	}
	task, err := client.GetByAttemptKey(ctx, req.AttemptKey)
	if err != nil {
		if errors.Is(err, a2ago.ErrTaskNotFound) {
			// Worker 在线但没有该尝试的任何记录：无法确认，按 interrupted 处理。
			return PollAttemptResp{Reachable: true, WorkerAlive: alive, Status: core.AttemptInterrupted, Reason: "attempt_not_found", Retryable: true}, nil
		}
		return PollAttemptResp{WorkerAlive: alive}, nil
	}
	res := a2a.DecodeTaskResult(task)
	return PollAttemptResp{
		Reachable:   true,
		WorkerAlive: alive,
		Status:      res.Status,
		Reason:      res.Reason,
		Retryable:   res.Retryable,
		Text:        res.Text,
		Artifacts:   res.Artifacts,
		Usage:       res.Usage,
	}, nil
}

type CancelAttemptReq struct {
	AttemptKey string
	WorkerID   string
	Executor   string
}

// CancelAttempt asks the Worker to cancel; the final state is whatever a later
// poll reports (spec §7.1 取消是请求语义).
func CancelAttempt(ctx context.Context, req CancelAttemptReq) error {
	client, err := workerClient(ctx, req.Executor, req.WorkerID)
	if err != nil {
		return fmt.Errorf("CancelAttempt: %w", err)
	}
	task, err := client.GetByAttemptKey(ctx, req.AttemptKey)
	if err != nil {
		return fmt.Errorf("CancelAttempt: lookup: %w", err)
	}
	if task.Status.State.Terminal() {
		return nil
	}
	if _, err := client.Cancel(ctx, task.ID); err != nil {
		return fmt.Errorf("CancelAttempt: cancel: %w", err)
	}
	return nil
}

func workerClient(ctx context.Context, executor, workerID string) (*a2a.Client, error) {
	if depStore == nil {
		return nil, errors.New("store not configured")
	}
	rec, err := store.LoadRegistryRecord(ctx, depStore, executor, workerID)
	if err != nil {
		return nil, fmt.Errorf("load worker %s: %w", workerID, err)
	}
	return a2a.NewClient(ctx, rec.Endpoint, depToken)
}

// ---- Scheduler actor ----

type AcquireSlotReq struct {
	WaiterInstanceID string
	AttemptKey       string
	Executor         string
	Account          string
	PreferWorker     string
	AffinityWait     string
}

type AcquireSlotResp struct {
	Granted bool
	Lease   *LeaseInfo
}

// LeaseInfo carries the lease details from the scheduler.
type LeaseInfo struct {
	WorkerID      string `json:"workerID"`
	Executor      string `json:"executor"`
	Account       string `json:"account"`
	SessionPolicy string `json:"sessionPolicy"`
}

// AcquireSlot calls Scheduler.Acquire (spec §5.1).
func AcquireSlot(ctx context.Context, req AcquireSlotReq) (AcquireSlotResp, error) {
	var resp scheduler.AcquireRespActor
	err := invokeScheduler(ctx, "Acquire", scheduler.AcquireReqActor{
		WaiterInstanceID: req.WaiterInstanceID,
		AttemptKey:       req.AttemptKey,
		Executor:         req.Executor,
		Account:          req.Account,
		PreferWorker:     req.PreferWorker,
		AffinityWait:     req.AffinityWait,
	}, &resp)
	if err != nil {
		return AcquireSlotResp{}, fmt.Errorf("AcquireSlot: %w", err)
	}
	out := AcquireSlotResp{Granted: resp.Granted}
	if resp.Lease != nil {
		out.Lease = &LeaseInfo{
			WorkerID:      resp.Lease.WorkerID,
			Executor:      resp.Lease.Executor,
			Account:       resp.Lease.Account,
			SessionPolicy: resp.Lease.SessionPolicy,
		}
	}
	return out, nil
}

type ReleaseSlotReq struct {
	AttemptKey string
}

// ReleaseSlot releases a lease or a still-queued request (idempotent).
func ReleaseSlot(ctx context.Context, req ReleaseSlotReq) error {
	return invokeScheduler(ctx, "Release", scheduler.ReleaseReqActor{AttemptKey: req.AttemptKey}, nil)
}

type ReleaseWaiterReq struct {
	InstanceID string
}

// ReleaseWaiter releases every lease and queued request of a goal.
func ReleaseWaiter(ctx context.Context, req ReleaseWaiterReq) error {
	return invokeScheduler(ctx, "ReleaseWaiter", scheduler.ReleaseWaiterReqActor{InstanceID: req.InstanceID}, nil)
}

type AccountStateReq struct {
	Account       string // executor/account
	NeedsAuth     bool
	CooldownUntil time.Time
}

// AccountState marks an account needs_auth or puts it into cooldown (spec §5.1).
func AccountState(ctx context.Context, req AccountStateReq) error {
	if req.NeedsAuth {
		return invokeScheduler(ctx, "SetAccountState", scheduler.SetAccountStateReqActor{Account: req.Account, OK: false}, nil)
	}
	return invokeScheduler(ctx, "Cooldown", scheduler.CooldownReqActor{Account: req.Account, Until: req.CooldownUntil.Format(time.RFC3339)}, nil)
}

func invokeScheduler(ctx context.Context, method string, in, out any) error {
	if depDaprClient == nil {
		return errors.New("Dapr client not configured")
	}
	data, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", method, err)
	}
	resp, err := depDaprClient.InvokeActor(ctx, &daprclient.InvokeActorRequest{
		ActorType: scheduler.ActorTypeScheduler,
		ActorID:   scheduler.ActorIDScheduler,
		Method:    method,
		Data:      data,
	})
	if err != nil {
		return fmt.Errorf("invoke scheduler %s: %w", method, err)
	}
	if out != nil && len(resp.Data) > 0 {
		if err := json.Unmarshal(resp.Data, out); err != nil {
			return fmt.Errorf("unmarshal %s response: %w", method, err)
		}
	}
	return nil
}

// ---- Projection & records ----

type CollectStepReq struct {
	GoalID     string
	Round      int
	StepID     string
	AttemptKey string
	Artifacts  []core.ArtifactRef
}

// CollectStep registers the step's artifacts under goal:<id>:artifact:* (spec §7.1 产物交接).
func CollectStep(ctx context.Context, req CollectStepReq) error {
	if depStore == nil {
		return errors.New("CollectStep: store not configured")
	}
	for _, a := range req.Artifacts {
		name := a.Path[lastSlash(a.Path)+1:]
		if err := store.SaveArtifact(ctx, depStore, req.GoalID, req.Round, req.StepID, name, a); err != nil {
			return fmt.Errorf("CollectStep: save artifact %s: %w", a.Path, err)
		}
	}
	return nil
}

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

type ProjectReq struct {
	GoalID   string
	State    core.GoalState
	Events   []core.Event
	EventSeq int
}

// Project writes events (idempotent by seq) and then the projection. It never
// overwrites a terminal status (e.g. the API cancelled a queued goal).
func Project(ctx context.Context, req ProjectReq) error {
	if depStore == nil {
		return errors.New("Project: store not configured")
	}
	for _, ev := range req.Events {
		if err := store.SaveEvent(ctx, depStore, req.GoalID, ev); err != nil {
			return fmt.Errorf("Project: save event %d: %w", ev.Seq, err)
		}
	}
	existing, err := store.LoadGoalProjection(ctx, depStore, req.GoalID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("Project: load projection: %w", err)
	}
	proj := store.GoalProjection{GoalState: req.State, EventSeq: max(req.EventSeq, existing.EventSeq)}
	if isTerminalGoal(existing.Status) {
		proj.Status = existing.Status
		proj.Reason = existing.Reason
	}
	if err := store.SaveGoalProjection(ctx, depStore, req.GoalID, proj); err != nil {
		return fmt.Errorf("Project: save projection: %w", err)
	}
	return nil
}

type FinishGoalReq struct {
	GoalID string
	State  core.GoalState
	Events []core.Event
}

// FinishGoal releases every lease of the goal, writes the terminal projection
// and events, and moves the goal out of the active index. A goal already
// cancelled by the API keeps its cancelled status.
func FinishGoal(ctx context.Context, req FinishGoalReq) error {
	if depDaprClient != nil {
		if err := ReleaseWaiter(ctx, ReleaseWaiterReq{InstanceID: req.GoalID}); err != nil {
			return fmt.Errorf("FinishGoal: %w", err)
		}
	}
	if err := Project(ctx, ProjectReq{GoalID: req.GoalID, State: req.State, Events: req.Events, EventSeq: lastSeq(req.Events)}); err != nil {
		return fmt.Errorf("FinishGoal: %w", err)
	}
	return store.MoveGoalToFinished(ctx, depStore, req.GoalID)
}

func lastSeq(events []core.Event) int {
	if len(events) == 0 {
		return 0
	}
	return events[len(events)-1].Seq
}

func isTerminalGoal(st core.GoalStatus) bool {
	switch st {
	case core.GoalSucceeded, core.GoalFailed, core.GoalCancelled, core.GoalCompleted:
		return true
	}
	return false
}

type SaveRoundSummaryReq struct {
	GoalID  string
	Summary core.RoundSummary
}

// SaveRoundSummary persists a round summary for GET /goals/{id}/rounds/{n}.
func SaveRoundSummary(ctx context.Context, req SaveRoundSummaryReq) error {
	if depStore == nil {
		return errors.New("SaveRoundSummary: store not configured")
	}
	return store.SaveRoundSummary(ctx, depStore, req.GoalID, req.Summary)
}

type RunCheckReq struct {
	GoalID    string
	Command   string
	Workspace string
}

// RunCheck runs the goal-level check. M0 placeholder: the real check runs as
// a `_check` step on a Worker in M1 (plan T14 / spec §7.4).
func RunCheck(ctx context.Context, req RunCheckReq) (core.StepResult, error) {
	return core.StepResult{StepID: "check", Role: "_check", Status: core.StepSucceeded, Summary: "check skipped (M0)"}, nil
}
