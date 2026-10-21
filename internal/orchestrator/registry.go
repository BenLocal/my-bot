package orchestrator

import (
	"github.com/dapr/durabletask-go/workflow"
)

// NewRegistry creates a workflow registry with all activities and workflows
// registered. The Runtime main calls this during initialization.
func NewRegistry() *workflow.Registry {
	r := workflow.NewRegistry()

	// Register workflow.
	if err := r.AddWorkflowN("GoalWorkflow", GoalWorkflow); err != nil {
		panic("orchestrator: failed to register GoalWorkflow: " + err.Error())
	}

	// Register activities.
	activities := []struct {
		name string
		fn   workflow.Activity
	}{
		{"SubmitAttempt", wrapSubmitAttempt},
		{"PollAttempt", wrapPollAttempt},
		{"AcquireSlot", wrapAcquireSlot},
		{"ReleaseSlot", wrapReleaseSlot},
		{"CollectStep", wrapCollectStep},
		{"CollectRound", wrapCollectRound},
		{"ScheduleWorkflow", wrapScheduleWorkflow},
		{"FinishGoal", wrapFinishGoal},
		{"RunCheck", wrapRunCheck},
		{"CommitMemories", wrapCommitMemories},
		{"RecallMemory", wrapRecallMemory},
	}
	for _, a := range activities {
		if err := r.AddActivityN(a.name, a.fn); err != nil {
			panic("orchestrator: failed to register activity " + a.name + ": " + err.Error())
		}
	}

	return r
}

// ---- Activity wrappers ----

func wrapSubmitAttempt(ctx workflow.ActivityContext) (any, error) {
	var req SubmitAttemptReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return SubmitAttempt(ctx.Context(), req)
}

func wrapPollAttempt(ctx workflow.ActivityContext) (any, error) {
	var req PollAttemptReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return PollAttempt(ctx.Context(), req)
}

func wrapAcquireSlot(ctx workflow.ActivityContext) (any, error) {
	var req AcquireSlotReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return AcquireSlot(ctx.Context(), req)
}

func wrapReleaseSlot(ctx workflow.ActivityContext) (any, error) {
	var req ReleaseSlotReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return nil, ReleaseSlot(ctx.Context(), req)
}

func wrapCollectStep(ctx workflow.ActivityContext) (any, error) {
	var req CollectStepReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return CollectStep(ctx.Context(), req)
}

func wrapCollectRound(ctx workflow.ActivityContext) (any, error) {
	var req CollectRoundReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return CollectRound(ctx.Context(), req)
}

func wrapScheduleWorkflow(ctx workflow.ActivityContext) (any, error) {
	var req ScheduleWorkflowReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return nil, ScheduleWorkflow(ctx.Context(), req)
}

func wrapFinishGoal(ctx workflow.ActivityContext) (any, error) {
	var req FinishGoalReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return nil, FinishGoal(ctx.Context(), req)
}

func wrapRunCheck(ctx workflow.ActivityContext) (any, error) {
	var req RunCheckReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return RunCheck(ctx.Context(), req)
}

func wrapCommitMemories(ctx workflow.ActivityContext) (any, error) {
	var req CommitMemoriesReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return nil, CommitMemories(ctx.Context(), req)
}

func wrapRecallMemory(ctx workflow.ActivityContext) (any, error) {
	var req RecallMemoryReq
	if err := ctx.GetInput(&req); err != nil {
		return nil, err
	}
	return RecallMemory(ctx.Context(), req)
}
