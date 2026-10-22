package orchestrator

import (
	"time"

	"github.com/dapr/durabletask-go/workflow"
)

// activityRetryPolicy returns the per-type Activity retry policy (spec §5.2 表).
// nil means no retry. Non-retryable outcomes are returned as values, not errors.
func activityRetryPolicy(name string) *workflow.RetryPolicy {
	switch name {
	case ActSubmitAttempt:
		return &workflow.RetryPolicy{MaxAttempts: 3, InitialRetryInterval: 5 * time.Second, BackoffCoefficient: 1.5, MaxRetryInterval: 30 * time.Second}
	case ActPollAttempt, ActCollectStep, ActProject, ActSaveRoundSummary, ActFinishGoal,
		ActAcquireSlot, ActReleaseSlot, ActReleaseWaiter, ActAccountState:
		return &workflow.RetryPolicy{MaxAttempts: 5, InitialRetryInterval: time.Second, BackoffCoefficient: 2, MaxRetryInterval: 30 * time.Second}
	case ActCancelAttempt:
		return &workflow.RetryPolicy{MaxAttempts: 3, InitialRetryInterval: time.Second, BackoffCoefficient: 2, MaxRetryInterval: 10 * time.Second}
	}
	return nil
}
