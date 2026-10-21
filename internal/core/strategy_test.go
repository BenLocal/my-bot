package core

import (
	"testing"
)

func TestSingleStrategy(t *testing.T) {
	s := NewStrategy("single")
	if s == nil {
		t.Fatal("NewStrategy(\"single\") returned nil")
	}
	spec := GoalSpec{
		Kind:     KindGoal,
		Strategy: "single",
		Members:  []string{"assistant"},
	}
	st := s.Init(spec)
	plans := s.Next(st, 1)
	if len(plans) != 1 {
		t.Errorf("expected 1 step plan, got %d", len(plans))
	}
}

func TestPipelineStrategy(t *testing.T) {
	s := NewStrategy("pipeline")
	spec := GoalSpec{
		Kind:     KindGoal,
		Strategy: "pipeline",
		Members:  []string{"designer", "coder", "reviewer"},
	}
	st := s.Init(spec)
	// First call should return step for designer
	plans := s.Next(st, 1)
	if len(plans) != 1 || plans[0].Role != "designer" {
		t.Errorf("expected designer step, got %+v", plans)
	}
	// Absorb and check second step
	results := []StepResult{{StepID: "s1", Role: "designer", Status: StepSucceeded}}
	st = s.Absorb(st, results)
	plans = s.Next(st, 2)
	if len(plans) != 1 || plans[0].Role != "coder" {
		t.Errorf("expected coder step, got %+v", plans)
	}
}

func TestRoundRobinStrategy(t *testing.T) {
	s := NewStrategy("round_robin")
	spec := GoalSpec{
		Kind:     KindGoal,
		Strategy: "round_robin",
		Members:  []string{"pi", "codex"},
	}
	st := s.Init(spec)

	// First round should go to first sorted member
	plans := s.Next(st, 1)
	if len(plans) != 1 {
		t.Fatalf("expected 1 step, got %d", len(plans))
	}
	if plans[0].Role != "codex" { // sorted: codex, pi
		t.Errorf("expected codex (first sorted), got %s", plans[0].Role)
	}

	// Absorb and check second round
	st = s.Absorb(st, []StepResult{{StepID: "s1", Status: StepSucceeded}})
	plans = s.Next(st, 2)
	if plans[0].Role != "pi" {
		t.Errorf("expected pi (second sorted), got %s", plans[0].Role)
	}

	// Third round should wrap around to codex
	st = s.Absorb(st, []StepResult{{StepID: "s2", Status: StepSucceeded}})
	plans = s.Next(st, 3)
	if plans[0].Role != "codex" {
		t.Errorf("expected codex (wrapped), got %s", plans[0].Role)
	}
}

func TestNewStrategyUnknown(t *testing.T) {
	s := NewStrategy("unknown")
	if s != nil {
		t.Errorf("expected nil for unknown strategy, got %+v", s)
	}
}

func TestAggregateRound(t *testing.T) {
	tests := []struct {
		name  string
		steps []StepResult
		want  RoundStatus
	}{
		{"all succeeded", []StepResult{
			{Status: StepSucceeded}, {Status: StepSucceeded},
		}, RoundSucceeded},
		{"one failed", []StepResult{
			{Status: StepSucceeded}, {Status: StepFailed},
		}, RoundFailed},
		{"empty", []StepResult{}, RoundFailed},
	}
	for _, tt := range tests {
		got := AggregateRound(tt.steps)
		if got != tt.want {
			t.Errorf("%s: got %s, want %s", tt.name, got, tt.want)
		}
	}
}

func TestCheckNoProgress(t *testing.T) {
	tests := []struct {
		name          string
		prevSummary   string
		prevArtifacts int
		currSummary   string
		currArtifacts int
		want          bool
	}{
		{"same summary, no new artifacts", "done", 0, "done", 0, true},
		{"different summary", "done1", 0, "done2", 0, false},
		{"new artifacts", "done", 0, "done", 1, false},
		{"case difference only", "Done", 0, "done", 0, true},
	}
	for _, tt := range tests {
		got := CheckNoProgress(tt.prevSummary, tt.prevArtifacts, tt.currSummary, tt.currArtifacts)
		if got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestCheckStopConditions(t *testing.T) {
	spec := GoalSpec{Limits: Limits{MaxRounds: 3}}
	stop, reason := CheckStopConditions(spec, 3, 0, 0, "", 0, "", 0, 0)
	if !stop || reason != StopMaxRounds {
		t.Errorf("expected max_rounds stop, got stop=%v reason=%s", stop, reason)
	}

	spec2 := GoalSpec{Limits: Limits{FailStreak: 2}}
	stop, reason = CheckStopConditions(spec2, 1, 2, 0, "", 0, "", 0, 0)
	if !stop || reason != StopFailStreak {
		t.Errorf("expected fail_streak stop, got stop=%v reason=%s", stop, reason)
	}

	spec3 := GoalSpec{Limits: Limits{NoProgress: 3}}
	stop, reason = CheckStopConditions(spec3, 1, 0, 3, "", 0, "", 0, 0)
	if !stop || reason != StopNoProgress {
		t.Errorf("expected no_progress stop, got stop=%v reason=%s", stop, reason)
	}
}
