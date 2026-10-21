package orchestrator

import (
	"time"

	"github.com/dapr/durabletask-go/workflow"
)

// RetryPolicy defines the retry behavior for an activity type.
type RetryPolicy struct {
	MaxAttempts      int           // 0 = no retry
	InitialInterval  time.Duration // default 1s
	BackoffFactor    float64       // default 2.0
	MaxInterval      time.Duration // default 60s
	RetryableReasons []string      // if empty, all failures retry
}

// Default retry policies per activity type (spec §5.2).
var retryPolicies = map[string]RetryPolicy{
	ActSubmitAttempt: {
		MaxAttempts:      3,
		InitialInterval:  1 * time.Second,
		BackoffFactor:    2.0,
		MaxInterval:      30 * time.Second,
		RetryableReasons: []string{"upstream_unavailable", "rate_limited", "crashed"},
	},
	ActPollAttempt: {
		MaxAttempts: 0, // polling is called repeatedly by workflow, not retried
	},
	ActCollectStep: {
		MaxAttempts:     3,
		InitialInterval: 1 * time.Second,
		BackoffFactor:   2.0,
		MaxInterval:     10 * time.Second,
	},
	ActCollectRound: {
		MaxAttempts:     2,
		InitialInterval: 2 * time.Second,
	},
	ActScheduleWorkflow: {
		MaxAttempts:     2,
		InitialInterval: 2 * time.Second,
	},
	ActFinishGoal: {
		MaxAttempts:     3,
		InitialInterval: 1 * time.Second,
	},
}

// GetRetryPolicy returns the retry policy for an activity type.
func GetRetryPolicy(activityType string) RetryPolicy {
	if p, ok := retryPolicies[activityType]; ok {
		return p
	}
	return RetryPolicy{}
}

// IsRetryableError checks if an error is retryable based on the policy.
func IsRetryableError(policy RetryPolicy, reason string) bool {
	if policy.MaxAttempts <= 0 {
		return false
	}
	if len(policy.RetryableReasons) == 0 {
		return true
	}
	for _, r := range policy.RetryableReasons {
		if r == reason {
			return true
		}
	}
	return false
}

// WithRetryPolicy wraps an activity execution with retry logic.
func WithRetryPolicy(ctx workflow.WorkflowContext, policy RetryPolicy, fn func() error) error {
	if policy.MaxAttempts <= 0 {
		return fn()
	}

	var lastErr error
	interval := policy.InitialInterval
	if interval == 0 {
		interval = time.Second
	}

	for attempt := 0; attempt <= policy.MaxAttempts; attempt++ {
		if attempt > 0 {
			// Wait before retry
			timerTask := ctx.CreateTimer(interval)
			timerTask.Await(nil)
			interval = time.Duration(float64(interval) * policy.BackoffFactor)
			if interval > policy.MaxInterval {
				interval = policy.MaxInterval
			}
		}

		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}

	return lastErr
}
