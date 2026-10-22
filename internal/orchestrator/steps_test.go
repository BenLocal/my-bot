package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
)

func TestWorkspaceBusy(t *testing.T) {
	a := &stepRun{Spec: core.DispatchStep{ID: "a", Workspace: "repo"}, Phase: phaseRunning}
	b := &stepRun{Spec: core.DispatchStep{ID: "b", Workspace: "repo"}, Phase: phasePending}
	c := &stepRun{Spec: core.DispatchStep{ID: "c", Workspace: "other"}, Phase: phasePending}
	d := &stepRun{Spec: core.DispatchStep{ID: "d"}, Phase: phasePending}
	steps := []*stepRun{a, b, c, d}
	if !workspaceBusy(steps, b) {
		t.Error("b shares workspace with running a: must wait")
	}
	if workspaceBusy(steps, c) || workspaceBusy(steps, d) {
		t.Error("different / empty workspace must not be serialized")
	}
	a.Phase = phaseDone
	if workspaceBusy(steps, b) {
		t.Error("b may start once a is done")
	}
}

func TestNextWakeAndStepFor(t *testing.T) {
	t0 := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	running := &stepRun{Key: "k1", Phase: phaseRunning, NextPoll: t0.Add(60 * time.Second), Deadline: t0.Add(30 * time.Minute)}
	backoff := &stepRun{Key: "k2", Phase: phaseBackoff, RetryAt: t0.Add(5 * time.Second)}
	acquiring := &stepRun{Key: "k3", Phase: phaseAcquiring, AcquireAt: t0}
	steps := []*stepRun{running, backoff, acquiring}
	if got := nextWake(steps); !got.Equal(t0.Add(5 * time.Second)) {
		t.Errorf("nextWake = %v", got)
	}
	if nextWake([]*stepRun{{Phase: phaseDone}}) != (time.Time{}) {
		t.Error("no timers for done steps")
	}

	if stepFor(steps, &Signal{Kind: SignalAttemptDone, Key: "k1"}) != running {
		t.Error("attempt-done should match the running step")
	}
	if stepFor(steps, &Signal{Kind: SignalSlotGranted, Key: "k1"}) != nil {
		t.Error("slot-granted must not match a running step")
	}
	if stepFor(steps, &Signal{Kind: SignalSlotGranted, Key: "k3"}) != acquiring {
		t.Error("slot-granted should match the acquiring step")
	}
	if stepFor(steps, &Signal{Kind: SignalAttemptDone, Key: "stale"}) != nil {
		t.Error("stale signals stay in the inbox")
	}
}

func TestCountPhases(t *testing.T) {
	steps := []*stepRun{{Phase: phaseRunning}, {Phase: phaseAcquiring}, {Phase: phaseDone}, {Phase: phaseBlocked}}
	if n := countPhases(steps, phaseRunning, phaseAcquiring); n != 2 {
		t.Errorf("countPhases = %d", n)
	}
}

func testState() *WorkflowState {
	return &WorkflowState{
		GoalID: "g_1",
		Round:  2,
		Spec: core.GoalSpec{
			Objective: "ship it", Acceptance: "tests pass", Strategy: "brain", Brain: "planner",
			Members: []string{"coder", "reviewer"},
			Limits:  core.Limits{MaxRounds: 5},
			Roles: map[string]core.Role{
				"planner":  {Name: "planner", Executor: "pi", InstructionsText: "PLAN WELL"},
				"coder":    {Name: "coder", Executor: "codex", InstructionsText: "CODE WELL"},
				"reviewer": {Name: "reviewer", Executor: "codex"},
			},
		},
		Progress:     "design done",
		PendingNotes: []string{"use postgres"},
		LastResults: []core.StepResult{{StepID: "impl", Role: "coder", Status: core.StepSucceeded, Summary: "implemented",
			Artifacts: []core.ArtifactRef{{Path: "r1/impl/main.go", Sha256: "abc"}}}},
	}
}

func TestBuildBrainPrompt(t *testing.T) {
	p := buildBrainPrompt(testState(), "[R1] steps[0].role: bad")
	for _, want := range []string{
		"[role instructions]\nPLAN WELL", `"enum": [`, "- coder (executor: codex)", "[objective]\nship it",
		"[acceptance]\ntests pass", "[progress]\ndesign done", "artifact: r1/impl/main.go",
		"[notes]\nuse postgres", "[feedback]\n[R1]", "remainingRounds=4",
	} {
		if !strings.Contains(p, want) {
			t.Errorf("brain prompt missing %q:\n%s", want, p)
		}
	}
}

func TestBuildMemberPrompt(t *testing.T) {
	p := buildMemberPrompt(testState(), core.DispatchStep{ID: "tests", Role: "coder", Instruction: "write tests", Inputs: []string{"r1/impl/main.go"}})
	for _, want := range []string{"CODE WELL", "[instruction]\nwrite tests", "/data/artifacts/g_1/r1/impl/main.go", "[notes]\nuse postgres", "[output contract]"} {
		if !strings.Contains(p, want) {
			t.Errorf("member prompt missing %q:\n%s", want, p)
		}
	}
	if strings.Contains(p, "[feedback]") {
		t.Error("member prompt must not contain brain feedback")
	}
}

func TestArtifactURIsAndMarker(t *testing.T) {
	got := artifactURIs("g_1", []string{"r1/a/x.md"})
	if len(got) != 1 || got[0] != "mybot-artifact://g_1/r1/a/x.md" {
		t.Errorf("artifactURIs = %v", got)
	}
	if attemptMarker("k") != "[mybot attempt k]\n" {
		t.Error("attempt marker must be the first prompt line (spec §7.2)")
	}
}
