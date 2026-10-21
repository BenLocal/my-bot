package e2e

import (
	"context"
	"testing"
	"time"

	a2ago "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/BenLocal/my-bot/internal/a2a"
	"github.com/BenLocal/my-bot/internal/executor"
	"github.com/BenLocal/my-bot/internal/executor/fake"
	"github.com/BenLocal/my-bot/internal/worker"
)

// TestWorkerSubmitGet verifies §12.1: submit and get a task.
func TestWorkerSubmitGet(t *testing.T) {
	ctx := context.Background()
	svc := worker.NewService()

	meta := a2a.Metadata{
		AttemptKey: "test-attempt-1",
		GoalID:     "g_test",
		Round:      1,
	}
	msg := a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("Hello, world!"))
	info := a2ago.TaskInfo{}

	res, err := svc.Submit(ctx, meta, msg, info)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Task should be in SUBMITTED state.
	if res.Task.Status.State != a2ago.TaskStateSubmitted {
		t.Fatalf("expected SUBMITTED, got %s", res.Task.Status.State)
	}

	// Get by task ID.
	task, err := svc.Get(ctx, res.Task.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if task.ID != res.Task.ID {
		t.Fatal("task ID mismatch")
	}

	// Get by attempt key.
	task2, err := svc.GetByAttemptKey(ctx, "test-attempt-1")
	if err != nil {
		t.Fatalf("GetByAttemptKey failed: %v", err)
	}
	if task2.ID != res.Task.ID {
		t.Fatal("task ID mismatch from GetByAttemptKey")
	}
}

// TestWorkerSubmitIdempotent verifies §12.2: same attemptKey returns same task.
func TestWorkerSubmitIdempotent(t *testing.T) {
	ctx := context.Background()
	svc := worker.NewService()

	meta := a2a.Metadata{
		AttemptKey: "test-attempt-idem",
		GoalID:     "g_test",
		Round:      1,
	}
	msg := a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("Hello!"))
	info := a2ago.TaskInfo{}

	res1, err := svc.Submit(ctx, meta, msg, info)
	if err != nil {
		t.Fatalf("first Submit failed: %v", err)
	}

	res2, err := svc.Submit(ctx, meta, msg, info)
	if err != nil {
		t.Fatalf("second Submit failed: %v", err)
	}

	if res1.Task.ID != res2.Task.ID {
		t.Fatal("same attemptKey should return same task")
	}
}

// TestWorkerSubmitCompleted verifies that a completed attempt returns terminal state.
func TestWorkerSubmitCompleted(t *testing.T) {
	ctx := context.Background()
	fakeDriver := fake.NewWithScript(fake.Script{
		Result: "Task completed successfully",
		Usage:  core_usage(),
	})
	svc := worker.NewService()
	svc.SetDriver(fakeDriver)

	meta := a2a.Metadata{
		AttemptKey: "test-attempt-complete",
		GoalID:     "g_test",
		Round:      1,
	}
	msg := a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("Do something"))
	info := a2ago.TaskInfo{}

	res, err := svc.Submit(ctx, meta, msg, info)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Wait for the task to reach terminal state.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task, err := svc.Get(ctx, res.Task.ID)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if task.Status.State.Terminal() {
			if task.Status.State != a2ago.TaskStateCompleted {
				t.Fatalf("expected COMPLETED, got %s", task.Status.State)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("task did not reach terminal state within 5s")
}

// TestWorkerSubmitFailed verifies that a failed attempt returns terminal state.
func TestWorkerSubmitFailed(t *testing.T) {
	ctx := context.Background()
	fakeDriver := fake.NewWithScript(fake.Script{
		Fail:      true,
		Retryable: true,
		Reason:    "test failure",
	})
	svc := worker.NewService()
	svc.SetDriver(fakeDriver)

	meta := a2a.Metadata{
		AttemptKey: "test-attempt-fail",
		GoalID:     "g_test",
		Round:      1,
	}
	msg := a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("Do something"))
	info := a2ago.TaskInfo{}

	res, err := svc.Submit(ctx, meta, msg, info)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Wait for the task to reach terminal state.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task, err := svc.Get(ctx, res.Task.ID)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if task.Status.State.Terminal() {
			if task.Status.State != a2ago.TaskStateFailed {
				t.Fatalf("expected FAILED, got %s", task.Status.State)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("task did not reach terminal state within 5s")
}

// TestWorkerCancel verifies that cancel stops an attempt.
func TestWorkerCancel(t *testing.T) {
	ctx := context.Background()
	fakeDriver := fake.NewWithScript(fake.Script{
		Hang: true, // never reaches terminal state
	})
	svc := worker.NewService()
	svc.SetDriver(fakeDriver)

	meta := a2a.Metadata{
		AttemptKey: "test-attempt-cancel",
		GoalID:     "g_test",
		Round:      1,
	}
	msg := a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("Long running task"))
	info := a2ago.TaskInfo{}

	res, err := svc.Submit(ctx, meta, msg, info)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Cancel the task.
	if err := svc.Cancel(ctx, res.Task.ID); err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	// Task should be in CANCELED state.
	task, err := svc.Get(ctx, res.Task.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if task.Status.State != a2ago.TaskStateCanceled {
		t.Fatalf("expected CANCELED, got %s", task.Status.State)
	}
}

// TestWorkerGetByAttemptKeyNotFound verifies error handling.
func TestWorkerGetByAttemptKeyNotFound(t *testing.T) {
	ctx := context.Background()
	svc := worker.NewService()

	_, err := svc.GetByAttemptKey(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent attempt key")
	}
}

// TestWorkerSubscribe verifies event streaming.
func TestWorkerSubscribe(t *testing.T) {
	ctx := context.Background()
	fakeDriver := fake.NewWithScript(fake.Script{
		Result: "Streamed result",
	})
	svc := worker.NewService()
	svc.SetDriver(fakeDriver)

	meta := a2a.Metadata{
		AttemptKey: "test-attempt-subscribe",
		GoalID:     "g_test",
		Round:      1,
	}
	msg := a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("Subscribe test"))
	info := a2ago.TaskInfo{}

	res, err := svc.Submit(ctx, meta, msg, info)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Subscribe to events.
	count := 0
	for ev, err := range svc.Subscribe(ctx, res.Task.ID) {
		if err != nil {
			t.Fatalf("Subscribe error: %v", err)
		}
		if ev != nil {
			count++
		}
		if count > 100 {
			t.Fatal("too many events")
		}
	}

	if count < 1 {
		t.Fatal("expected at least 1 event")
	}
}

// core_usage returns a default Usage for test scripts.
func core_usage() core_Usage {
	return core_Usage{}
}

// core_Usage is a local alias to avoid import cycle in test.
type core_Usage = struct {
	InputTokens  int64 `json:"inputTokens,omitempty"`
	OutputTokens int64 `json:"outputTokens,omitempty"`
}

// Ensure executor phases are available.
var _ = executor.PhaseWorking
