package core

import (
	"time"
)

// AggregateRound computes the overall RoundStatus from step results.
func AggregateRound(steps []StepResult) RoundStatus {
	if len(steps) == 0 {
		return RoundFailed
	}
	allSucceeded := true
	for _, s := range steps {
		if s.Status != StepSucceeded {
			allSucceeded = false
			break
		}
	}
	if allSucceeded {
		return RoundSucceeded
	}
	return RoundFailed
}

// CheckNoProgress compares current round summary with previous.
// Returns true if this round has no progress (same summary, no new artifacts).
func CheckNoProgress(prevSummary string, prevArtifactCount int, currSummary string, currArtifactCount int) bool {
	return NormalizeText(prevSummary) == NormalizeText(currSummary) && currArtifactCount <= prevArtifactCount
}

// StopReason describes why a goal stopped.
type StopReason string

const (
	StopMaxRounds  StopReason = "max_rounds"
	StopDeadline   StopReason = "deadline"
	StopBudget     StopReason = "budget"
	StopFailStreak StopReason = "fail_streak"
	StopNoProgress StopReason = "no_progress"
	StopFinished   StopReason = "finished"
	StopCancelled  StopReason = "cancelled"
	StopError      StopReason = "error"
)

func (r StopReason) String() string { return string(r) }

// Default stop thresholds (spec §5.2 step 2).
const (
	DefaultFailStreak = 3
	DefaultNoProgress = 3
)

// CheckStopConditions checks all stop conditions and returns the first triggered.
// `round` is the number of completed rounds; `now` must come from the workflow
// clock (ctx.CurrentTimeUTC) so the check is replay-safe.
func CheckStopConditions(spec GoalSpec, now time.Time, round, failStreak, noProgress int, tokensUsed int64) (bool, StopReason) {
	if spec.Limits.MaxRounds > 0 && round >= spec.Limits.MaxRounds {
		return true, StopMaxRounds
	}
	if spec.Limits.Deadline != nil && now.After(*spec.Limits.Deadline) {
		return true, StopDeadline
	}
	if spec.Limits.MaxTokens > 0 && tokensUsed >= spec.Limits.MaxTokens {
		return true, StopBudget
	}
	maxFail := spec.Limits.FailStreak
	if maxFail <= 0 {
		maxFail = DefaultFailStreak
	}
	if failStreak >= maxFail {
		return true, StopFailStreak
	}
	maxNoProgress := spec.Limits.NoProgress
	if maxNoProgress <= 0 {
		maxNoProgress = DefaultNoProgress
	}
	if noProgress >= maxNoProgress {
		return true, StopNoProgress
	}
	return false, ""
}
