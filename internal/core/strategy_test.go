package core

import (
	"testing"
	"time"
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
	now := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	tests := []struct {
		name       string
		spec       GoalSpec
		round      int
		failStreak int
		noProgress int
		tokens     int64
		want       StopReason
	}{
		{"max rounds", GoalSpec{Limits: Limits{MaxRounds: 3}}, 3, 0, 0, 0, StopMaxRounds},
		{"deadline", GoalSpec{Limits: Limits{Deadline: &past}}, 1, 0, 0, 0, StopDeadline},
		{"budget", GoalSpec{Limits: Limits{MaxTokens: 100}}, 1, 0, 0, 100, StopBudget},
		{"fail streak custom", GoalSpec{Limits: Limits{FailStreak: 2}}, 1, 2, 0, 0, StopFailStreak},
		{"fail streak default", GoalSpec{}, 1, 3, 0, 0, StopFailStreak},
		{"no progress default", GoalSpec{}, 1, 0, 3, 0, StopNoProgress},
		{"continue", GoalSpec{Limits: Limits{MaxRounds: 5}}, 2, 2, 2, 0, ""},
	}
	for _, tt := range tests {
		stop, reason := CheckStopConditions(tt.spec, now, tt.round, tt.failStreak, tt.noProgress, tt.tokens)
		if reason != tt.want || stop != (tt.want != "") {
			t.Errorf("%s: got stop=%v reason=%q, want %q", tt.name, stop, reason, tt.want)
		}
	}
}

func TestDeterministicDone(t *testing.T) {
	ok := []StepResult{{Status: StepSucceeded}}
	done := []StepResult{{Status: StepSucceeded, Done: true}}
	failed := []StepResult{{Status: StepFailed, Done: true}}
	pipe := GoalSpec{Strategy: "pipeline", Members: []string{"a", "b"}}
	if DeterministicDone(pipe, 1, ok) || !DeterministicDone(pipe, 2, ok) {
		t.Error("pipeline should finish after the last stage")
	}
	single := GoalSpec{Strategy: "single"}
	if DeterministicDone(single, 1, ok) || !DeterministicDone(single, 1, done) || DeterministicDone(single, 1, failed) {
		t.Error("single finishes only on a succeeded round with done=true")
	}
}

func TestParseRolesConfig(t *testing.T) {
	cfg, err := ParseRolesConfig([]byte(`
roles:
  planner: { executor: fake, account: default, instructions: roles/planner.md, timeout: 15m, capabilities: [brain] }
  coder:   { executor: fake, account: default, timeout: 1h }
defaultBrain: planner
limits:
  globalConcurrency: 2
  goal: { maxParallel: 2, maxStepsPerRound: 4 }
  accounts:
    fake/default: { concurrency: 3 }
`))
	if err != nil {
		t.Fatalf("ParseRolesConfig: %v", err)
	}
	if !cfg.Roles["planner"].HasCapability("brain") || cfg.Limits.GlobalConcurrency != 2 || cfg.AccountLimits()["fake/default"] != 3 {
		t.Errorf("unexpected config %+v", cfg)
	}
	if _, err := ParseRolesConfig([]byte("roles:\n  a: { executor: fake }\n")); err == nil {
		t.Error("expected error for missing account")
	}
	if _, err := ParseRolesConfig([]byte("roles:\n  a: { executor: fake, account: x }\ndefaultBrain: b\n")); err == nil {
		t.Error("expected error for unknown defaultBrain")
	}
}
