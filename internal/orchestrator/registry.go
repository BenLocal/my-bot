package orchestrator

import (
	"context"
	"log/slog"

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
		{ActSubmitAttempt, activity(SubmitAttempt)},
		{ActPollAttempt, activity(PollAttempt)},
		{ActCancelAttempt, activityNoResult(CancelAttempt)},
		{ActAcquireSlot, activity(AcquireSlot)},
		{ActReleaseSlot, activityNoResult(ReleaseSlot)},
		{ActReleaseWaiter, activityNoResult(ReleaseWaiter)},
		{ActAccountState, activityNoResult(AccountState)},
		{ActCollectStep, activityNoResult(CollectStep)},
		{ActProject, activityNoResult(Project)},
		{ActFinishGoal, activityNoResult(FinishGoal)},
		{ActSaveRoundSummary, activityNoResult(SaveRoundSummary)},
		{ActRunCheck, activity(RunCheck)},
	}
	for _, a := range activities {
		if err := r.AddActivityN(a.name, logged(a.name, a.fn)); err != nil {
			panic("orchestrator: failed to register activity " + a.name + ": " + err.Error())
		}
	}

	return r
}

// logged logs activity errors (they are otherwise only visible in workflow history).
func logged(name string, fn workflow.Activity) workflow.Activity {
	return func(ctx workflow.ActivityContext) (any, error) {
		out, err := fn(ctx)
		if err != nil {
			slog.Warn("activity failed", "activity", name, "error", err)
		}
		return out, err
	}
}

// activity adapts a typed activity function to workflow.Activity.
func activity[Req, Resp any](fn func(context.Context, Req) (Resp, error)) workflow.Activity {
	return func(ctx workflow.ActivityContext) (any, error) {
		var req Req
		if err := ctx.GetInput(&req); err != nil {
			return nil, err
		}
		return fn(ctx.Context(), req)
	}
}

// activityNoResult adapts a typed activity function without a result.
func activityNoResult[Req any](fn func(context.Context, Req) error) workflow.Activity {
	return func(ctx workflow.ActivityContext) (any, error) {
		var req Req
		if err := ctx.GetInput(&req); err != nil {
			return nil, err
		}
		return nil, fn(ctx.Context(), req)
	}
}
