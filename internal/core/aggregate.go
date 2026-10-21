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

// CheckStopConditions checks all stop conditions and returns the first triggered.
// Parameters: spec, current round, fail streak count, no-progress count,
// previous summary, previous artifact count, current summary, current artifact count.
func CheckStopConditions(
	spec GoalSpec,
	round, failStreak, noProgress int,
	prevSummary string, prevArtifacts int,
	currSummary string, currArtifacts int,
	tokensUsed int64,
) (bool, StopReason) {
	if spec.Limits.MaxRounds > 0 && round >= spec.Limits.MaxRounds {
		return true, StopMaxRounds
	}
	if spec.Limits.Deadline != nil && time.Now().After(*spec.Limits.Deadline) {
		return true, StopDeadline
	}
	if spec.Limits.MaxTokens > 0 && tokensUsed >= spec.Limits.MaxTokens {
		return true, StopBudget
	}
	if spec.Limits.FailStreak > 0 && failStreak >= spec.Limits.FailStreak {
		return true, StopFailStreak
	}
	if spec.Limits.NoProgress > 0 && noProgress >= spec.Limits.NoProgress {
		return true, StopNoProgress
	}
	return false, ""
}
