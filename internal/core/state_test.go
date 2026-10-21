package core

import (
	"testing"
)

func TestGoalTransitions(t *testing.T) {
	tests := []struct {
		from   GoalStatus
		event  string
		expect GoalStatus
	}{
		{GoalQueued, "start", GoalRunning},
		{GoalRunning, "finish", GoalSucceeded},
		{GoalRunning, "fail", GoalFailed},
		{GoalRunning, "pause", GoalPaused},
		{GoalPaused, "resume", GoalRunning},
		{GoalRunning, "waiting", GoalWaiting},
		{GoalWaiting, "resume", GoalRunning},
		{GoalRunning, "cancel", GoalCancelled},
		{GoalPaused, "cancel", GoalCancelled},
		{GoalWaiting, "cancel", GoalCancelled},
		{GoalRunning, "complete", GoalCompleted},
	}
	for _, tt := range tests {
		got, err := GoalTransition(tt.from, tt.event)
		if err != nil {
			t.Errorf("GoalTransition(%s, %q): unexpected error: %v", tt.from, tt.event, err)
			continue
		}
		if got != tt.expect {
			t.Errorf("GoalTransition(%s, %q) = %s, want %s", tt.from, tt.event, got, tt.expect)
		}
	}
}

func TestGoalTransitionsIllegal(t *testing.T) {
	illegal := []struct {
		from  GoalStatus
		event string
	}{
		{GoalSucceeded, "start"},
		{GoalFailed, "start"},
		{GoalCancelled, "start"},
		{GoalQueued, "finish"},
		{GoalQueued, "pause"},
		{GoalPaused, "finish"},
		{GoalRunning, "invalid"},
	}
	for _, tt := range illegal {
		_, err := GoalTransition(tt.from, tt.event)
		if err == nil {
			t.Errorf("GoalTransition(%s, %q): expected error, got nil", tt.from, tt.event)
		}
	}
}

func TestStepTransitions(t *testing.T) {
	tests := []struct {
		from   StepStatus
		event  string
		expect StepStatus
	}{
		{StepPending, "start", StepRunning},
		{StepRunning, "succeed", StepSucceeded},
		{StepRunning, "fail", StepFailed},
		{StepRunning, "cancel", StepCancelled},
		{StepPending, "cancel", StepCancelled},
	}
	for _, tt := range tests {
		got, err := StepTransition(tt.from, tt.event)
		if err != nil {
			t.Errorf("StepTransition(%s, %q): unexpected error: %v", tt.from, tt.event, err)
			continue
		}
		if got != tt.expect {
			t.Errorf("StepTransition(%s, %q) = %s, want %s", tt.from, tt.event, got, tt.expect)
		}
	}
}

func TestAttemptTransitions(t *testing.T) {
	tests := []struct {
		from   AttemptStatus
		event  string
		expect AttemptStatus
	}{
		{AttemptSubmitting, "submitted", AttemptSubmitted},
		{AttemptSubmitted, "working", AttemptWorking},
		{AttemptWorking, "completed", AttemptCompleted},
		{AttemptWorking, "failed", AttemptFailed},
		{AttemptSubmitting, "rejected", AttemptRejected},
		{AttemptSubmitted, "rejected", AttemptRejected},
		{AttemptWorking, "cancelled", AttemptCancelled},
		{AttemptSubmitting, "interrupted", AttemptInterrupted},
		{AttemptSubmitting, "unknown", AttemptUnknown},
	}
	for _, tt := range tests {
		got, err := AttemptTransition(tt.from, tt.event)
		if err != nil {
			t.Errorf("AttemptTransition(%s, %q): unexpected error: %v", tt.from, tt.event, err)
			continue
		}
		if got != tt.expect {
			t.Errorf("AttemptTransition(%s, %q) = %s, want %s", tt.from, tt.event, got, tt.expect)
		}
	}
}

func TestNewGoalID(t *testing.T) {
	id := NewGoalID()
	if len(id) < 3 || id[:2] != "g_" {
		t.Errorf("NewGoalID() = %q, want prefix g_", id)
	}
}

func TestAttemptKeyDeterministic(t *testing.T) {
	k1 := AttemptKey("g_01ABC", 1, "s1", 1)
	k2 := AttemptKey("g_01ABC", 1, "s1", 1)
	if k1 != k2 {
		t.Errorf("AttemptKey should be deterministic: %s != %s", k1, k2)
	}

	k3 := AttemptKey("g_01ABC", 2, "s1", 1)
	if k1 == k3 {
		t.Errorf("AttemptKey should differ for different round")
	}
}

func TestSessionIDDeterministic(t *testing.T) {
	s1 := SessionID("g_01ABC", "planner")
	s2 := SessionID("g_01ABC", "planner")
	if s1 != s2 {
		t.Errorf("SessionID should be deterministic: %s != %s", s1, s2)
	}

	s3 := SessionID("g_01ABC", "coder")
	if s1 == s3 {
		t.Errorf("SessionID should differ for different roles")
	}
}

func TestEventID(t *testing.T) {
	e := EventID("g_01ABC", 5)
	if e != "g_01ABC:5" {
		t.Errorf("EventID = %q, want g_01ABC:5", e)
	}
}

func TestNormalizeText(t *testing.T) {
	tests := []struct{ in, want string }{
		{"Hello  World", "hello world"},
		{"  Hello\nWorld  ", "hello world"},
		{"Hello World!", "hello world!"},
		{"HELLO WORLD", "hello world"},
		{"", ""},
	}
	for _, tt := range tests {
		got := NormalizeText(tt.in)
		if got != tt.want {
			t.Errorf("NormalizeText(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
