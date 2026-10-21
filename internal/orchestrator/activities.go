package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	a2ago "github.com/a2aproject/a2a-go/v2/a2a"
	daprclient "github.com/dapr/go-sdk/client"

	"github.com/BenLocal/my-bot/internal/a2a"
	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/scheduler"
	"github.com/BenLocal/my-bot/internal/store"
	"github.com/dapr/durabletask-go/workflow"
)

// Activity function names.
const (
	ActSubmitAttempt     = "SubmitAttempt"
	ActPollAttempt       = "PollAttempt"
	ActAcquireSlot       = "AcquireSlot"
	ActReleaseSlot       = "ReleaseSlot"
	ActCollectStep       = "CollectStep"
	ActCollectRound      = "CollectRound"
	ActScheduleWorkflow  = "ScheduleWorkflow"
	ActFetchWorkflowMeta = "FetchWorkflowMetadata"
	ActFinishGoal        = "FinishGoal"
	ActCommitMemories    = "CommitMemories"
	ActRecallMemory      = "RecallMemory"
	ActRunCheck          = "RunCheck"
)

// SubmitAttempt submits a task to a worker via A2A.
// The caller must have already acquired a slot via AcquireSlot and passes
// the lease (WorkerID, Executor, Account, SessionPolicy) as direct inputs.
// This activity only loads the worker endpoint and sends the A2A message.
func SubmitAttempt(ctx context.Context, req SubmitAttemptReq) (SubmitAttemptResp, error) {
	if depStore == nil {
		return SubmitAttemptResp{}, fmt.Errorf("SubmitAttempt: store not configured")
	}

	// Load worker record to get endpoint.
	wrec, err := store.LoadRegistryRecord(ctx, depStore, req.Executor, req.WorkerID)
	if err != nil {
		return SubmitAttemptResp{}, fmt.Errorf("SubmitAttempt: load worker: %w", err)
	}

	// Create A2A client for this worker.
	client, err := a2a.NewClient(ctx, wrec.Endpoint, depToken)
	if err != nil {
		return SubmitAttemptResp{}, fmt.Errorf("SubmitAttempt: create client: %w", err)
	}

	// Build metadata.
	meta := a2a.Metadata{
		AttemptKey:    req.AttemptKey,
		GoalID:        req.GoalID,
		Round:         req.Round,
		Workspace:     req.Workspace,
		SessionPolicy: a2a.SessionPolicy(req.SessionPolicy),
		Inputs:        req.Inputs,
		Traceparent:   req.Traceparent,
	}
	metaMap := make(map[string]any)
	a2a.EncodeMetadata(metaMap, meta)

	// Build message.
	message := a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart(req.Prompt))

	// Build send request.
	sendReq := &a2ago.SendMessageRequest{
		Message:  message,
		Metadata: metaMap,
		Config:   &a2ago.SendMessageConfig{ReturnImmediately: true},
	}

	// Submit.
	task, err := client.Submit(ctx, sendReq)
	if err != nil {
		return SubmitAttemptResp{}, fmt.Errorf("SubmitAttempt: submit: %w", err)
	}

	return SubmitAttemptResp{
		RemoteTaskID: string(task.ID),
		AttemptKey:   req.AttemptKey,
		WorkerID:     req.WorkerID,
	}, nil
}

type SubmitAttemptReq struct {
	GoalID        string
	AttemptKey    string
	Round         int
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
	AttemptKey   string
	WorkerID     string
}

// AcquireSlot calls the Scheduler actor to acquire a slot.
// Returns the lease if granted, or false if no slot is available.
func AcquireSlot(ctx context.Context, req AcquireSlotReq) (AcquireSlotResp, error) {
	if depDaprClient == nil {
		return AcquireSlotResp{}, fmt.Errorf("AcquireSlot: Dapr client not configured")
	}

	actorReq := scheduler.AcquireReqActor{
		WaiterInstanceID: req.WaiterInstanceID,
		AttemptKey:       req.AttemptKey,
		Executor:         req.Executor,
		Account:          req.Account,
		PreferWorker:     req.PreferWorker,
		AffinityWait:     req.AffinityWait,
	}
	reqData, err := json.Marshal(actorReq)
	if err != nil {
		return AcquireSlotResp{}, fmt.Errorf("AcquireSlot: marshal request: %w", err)
	}

	resp, err := depDaprClient.InvokeActor(ctx, &daprclient.InvokeActorRequest{
		ActorType: scheduler.ActorTypeScheduler,
		ActorID:   scheduler.ActorIDScheduler,
		Method:    "Acquire",
		Data:      reqData,
	})
	if err != nil {
		return AcquireSlotResp{}, fmt.Errorf("AcquireSlot: invoke actor: %w", err)
	}

	var actorResp scheduler.AcquireRespActor
	if err := json.Unmarshal(resp.Data, &actorResp); err != nil {
		return AcquireSlotResp{}, fmt.Errorf("AcquireSlot: unmarshal response: %w", err)
	}

	out := AcquireSlotResp{Granted: actorResp.Granted}
	if actorResp.Lease != nil {
		out.Lease = &LeaseInfo{
			WorkerID:      actorResp.Lease.WorkerID,
			Executor:      actorResp.Lease.Executor,
			Account:       actorResp.Lease.Account,
			SessionPolicy: actorResp.Lease.SessionPolicy,
		}
	}

	return out, nil
}

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
	WorkerID      string
	Executor      string
	Account       string
	SessionPolicy string
}

// ReleaseSlot calls the Scheduler actor to release a lease.
func ReleaseSlot(ctx context.Context, req ReleaseSlotReq) error {
	if depDaprClient == nil {
		return fmt.Errorf("ReleaseSlot: Dapr client not configured")
	}

	actorReq := scheduler.ReleaseReqActor{
		AttemptKey: req.AttemptKey,
	}
	reqData, err := json.Marshal(actorReq)
	if err != nil {
		return fmt.Errorf("ReleaseSlot: marshal request: %w", err)
	}

	resp, err := depDaprClient.InvokeActor(ctx, &daprclient.InvokeActorRequest{
		ActorType: scheduler.ActorTypeScheduler,
		ActorID:   scheduler.ActorIDScheduler,
		Method:    "Release",
		Data:      reqData,
	})
	if err != nil {
		return fmt.Errorf("ReleaseSlot: invoke actor: %w", err)
	}

	// Response is not needed for M0, but we check for errors.
	var actorResp scheduler.ReleaseRespActor
	if len(resp.Data) > 0 {
		if err := json.Unmarshal(resp.Data, &actorResp); err != nil {
			return fmt.Errorf("ReleaseSlot: unmarshal response: %w", err)
		}
	}

	return nil
}

type ReleaseSlotReq struct {
	AttemptKey string
}

// PollAttempt polls the worker for attempt status.
func PollAttempt(ctx context.Context, req PollAttemptReq) (PollAttemptResp, error) {
	if depStore == nil {
		return PollAttemptResp{}, fmt.Errorf("PollAttempt: store not configured")
	}

	// Load worker record to get endpoint.
	wrec, err := store.LoadRegistryRecord(ctx, depStore, req.Executor, req.WorkerID)
	if err != nil {
		return PollAttemptResp{}, fmt.Errorf("PollAttempt: load worker: %w", err)
	}

	// Create A2A client.
	client, err := a2a.NewClient(ctx, wrec.Endpoint, depToken)
	if err != nil {
		return PollAttemptResp{}, fmt.Errorf("PollAttempt: create client: %w", err)
	}

	// Get task.
	task, err := client.Get(ctx, a2ago.TaskID(req.RemoteTaskID))
	if err != nil {
		return PollAttemptResp{}, fmt.Errorf("PollAttempt: get task: %w", err)
	}

	// Map A2A status to core.AttemptStatus.
	status := mapTaskStatus(task.Status.State)

	// Build response.
	resp := PollAttemptResp{
		Status: status,
	}

	// Extract summary from task status message if available.
	if task.Status.Message != nil {
		for _, part := range task.Status.Message.Parts {
			if text, ok := part.Content.(a2ago.Text); ok {
				resp.Summary = string(text)
				break
			}
		}
	}

	// Extract artifacts.
	for _, art := range task.Artifacts {
		for _, part := range art.Parts {
			if text, ok := part.Content.(a2ago.Text); ok {
				resp.Artifacts = append(resp.Artifacts, core.ArtifactRef{
					Path:   art.Name,
					Sha256: "", // filled by worker
					Size:   int64(len(text)),
					Round:  req.Round,
				})
			}
		}
	}

	return resp, nil
}

func mapTaskStatus(s a2ago.TaskState) core.AttemptStatus {
	switch s {
	case a2ago.TaskStateCompleted:
		return core.AttemptCompleted
	case a2ago.TaskStateFailed:
		return core.AttemptFailed
	case a2ago.TaskStateWorking:
		return core.AttemptWorking
	case a2ago.TaskStateSubmitted:
		return core.AttemptSubmitted
	case a2ago.TaskStateCanceled:
		return core.AttemptCancelled
	case a2ago.TaskStateRejected:
		return core.AttemptRejected
	case a2ago.TaskStateAuthRequired:
		return core.AttemptUnknown
	default:
		return core.AttemptUnknown
	}
}

type PollAttemptReq struct {
	GoalID       string
	AttemptKey   string
	RemoteTaskID string
	WorkerID     string
	Executor     string
	Round        int
}

type PollAttemptResp struct {
	Status    core.AttemptStatus
	Summary   string
	Artifacts []core.ArtifactRef
	Usage     core.Usage
}

// CollectStep collects the result of a single step.
// For M0, this is a placeholder — the workflow uses PollAttempt results directly.
func CollectStep(ctx context.Context, req CollectStepReq) (core.StepResult, error) {
	return core.StepResult{
		StepID:  req.StepID,
		Role:    req.Role,
		Status:  core.StepSucceeded,
		Summary: "step collected",
	}, nil
}

type CollectStepReq struct {
	GoalID string
	Round  int
	StepID string
	Role   string
}

// CollectRound collects all step results for a round.
// For M0, this is a placeholder — the workflow aggregates step results directly.
func CollectRound(ctx context.Context, req CollectRoundReq) (core.RoundSummary, error) {
	return core.RoundSummary{
		Round:   req.Round,
		Status:  core.RoundSucceeded,
		Summary: "round collected",
	}, nil
}

type CollectRoundReq struct {
	GoalID string
	Round  int
	Steps  []core.StepResult
}

// ScheduleWorkflow schedules a new workflow instance.
func ScheduleWorkflow(ctx context.Context, req ScheduleWorkflowReq) error {
	if depWorkflowClient == nil {
		return fmt.Errorf("ScheduleWorkflow: workflow client not configured")
	}
	_, err := depWorkflowClient.ScheduleWorkflow(ctx, "GoalWorkflow",
		workflow.WithInstanceID(req.GoalID),
		workflow.WithInput(req.State),
	)
	return err
}

type ScheduleWorkflowReq struct {
	GoalID string
	State  WorkflowState
}

// FetchWorkflowMetadata fetches metadata for an existing workflow instance.
func FetchWorkflowMetadata(ctx context.Context, goalID string) (bool, error) {
	if depWorkflowClient == nil {
		return false, fmt.Errorf("FetchWorkflowMetadata: workflow client not configured")
	}
	_, err := depWorkflowClient.FetchWorkflowMetadata(ctx, goalID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "no such instance") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// FinishGoal writes the final goal projection.
// Leases are reclaimed by the scheduler actor reminder on expiry (M0).
// If the goal was cancelled while the workflow was running, the cancelled
// status is preserved (spec §12.9: cancel takes effect while waiting).
func FinishGoal(ctx context.Context, req FinishGoalReq) error {
	if depStore == nil {
		return fmt.Errorf("FinishGoal: store not configured")
	}

	// Check current status: if already cancelled, don't overwrite.
	existing, err := store.LoadGoalProjection(ctx, depStore, req.GoalID)
	if err == nil && existing.Status == core.GoalCancelled {
		return nil
	}

	// Write final projection.
	proj := store.GoalProjection{
		GoalState: core.GoalState{
			ID:        req.GoalID,
			Kind:      core.KindGoal,
			Status:    req.Status,
			UpdatedAt: time.Now().UTC(),
		},
		EventSeq: existing.EventSeq,
	}
	if err := store.SaveGoalProjection(ctx, depStore, req.GoalID, proj); err != nil {
		return fmt.Errorf("FinishGoal: save projection: %w", err)
	}

	return nil
}

type FinishGoalReq struct {
	GoalID string
	Status core.GoalStatus
	Reason string
}

// RunCheck runs the goal-level check command.
// For M0, this is a placeholder.
func RunCheck(ctx context.Context, req RunCheckReq) (core.StepResult, error) {
	return core.StepResult{
		StepID:  "check",
		Role:    "_check",
		Status:  core.StepSucceeded,
		Summary: "check passed",
	}, nil
}

type RunCheckReq struct {
	GoalID    string
	Command   string
	Workspace string
	Timeout   time.Duration
}

// CommitMemories commits memory entries to L1.
// For M0, this is a placeholder.
func CommitMemories(ctx context.Context, req CommitMemoriesReq) error {
	return nil
}

type CommitMemoriesReq struct {
	GoalID   string
	Round    int
	Memories []core.MemoryEntry
}

// RecallMemory performs L2 semantic recall.
// For M0, this is a placeholder.
func RecallMemory(ctx context.Context, req RecallMemoryReq) ([]RecallMemoryResult, error) {
	return nil, nil
}

type RecallMemoryReq struct {
	GoalID string
	Query  string
}

type RecallMemoryResult struct {
	EntryID string
	Topic   string
	Text    string
	Score   float64
}

// Re-export for internal use.
var (
	_ = json.Marshal
	_ = scheduler.AcquireReq{}
)
