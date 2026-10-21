package scheduler

import (
	"testing"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
)

func TestPoolAcquireRelease(t *testing.T) {
	pool := NewPool(2)
	workers := map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "pi", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
	}
	pool.SetWorkers(workers)
	pool.SetAccountState("pi/default", true)

	resp := pool.Acquire(AcquireReq{
		AttemptKey: "a1",
		Executor:   "pi",
		Account:    "default",
	})
	if !resp.Granted {
		t.Fatal("expected grant for first acquire")
	}

	resp = pool.Acquire(AcquireReq{
		AttemptKey: "a2",
		Executor:   "pi",
		Account:    "default",
	})
	if !resp.Granted {
		t.Fatal("expected grant for second acquire")
	}

	// Third should be queued (global limit = 2)
	resp = pool.Acquire(AcquireReq{
		AttemptKey: "a3",
		Executor:   "pi",
		Account:    "default",
	})
	if resp.Granted {
		t.Fatal("expected queue for third acquire")
	}

	// Release one and check queue is satisfied
	pool.Release("a1")
	view := pool.Snapshot()
	if view.ActiveLeases != 2 {
		t.Errorf("expected 2 active leases after release (a2 + a3), got %d", view.ActiveLeases)
	}
}

func TestPoolIdempotentAcquire(t *testing.T) {
	pool := NewPool(4)
	workers := map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "pi", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
	}
	pool.SetWorkers(workers)
	pool.SetAccountState("pi/default", true)

	resp1 := pool.Acquire(AcquireReq{
		AttemptKey: "a1",
		Executor:   "pi",
		Account:    "default",
	})
	resp2 := pool.Acquire(AcquireReq{
		AttemptKey: "a1",
		Executor:   "pi",
		Account:    "default",
	})

	if !resp1.Granted || !resp2.Granted {
		t.Fatal("expected both acquires granted")
	}
	if resp1.Lease.ID != resp2.Lease.ID {
		t.Errorf("expected same lease for same attemptKey, got %s vs %s", resp1.Lease.ID, resp2.Lease.ID)
	}
}

func TestPoolFIFO(t *testing.T) {
	pool := NewPool(1)
	workers := map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "pi", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
	}
	pool.SetWorkers(workers)
	pool.SetAccountState("pi/default", true)

	// Acquire the only slot
	pool.Acquire(AcquireReq{AttemptKey: "a1", Executor: "pi", Account: "default"})

	// Queue two requests
	pool.Acquire(AcquireReq{AttemptKey: "a2", Executor: "pi", Account: "default"})
	pool.Acquire(AcquireReq{AttemptKey: "a3", Executor: "pi", Account: "default"})

	// Release and check FIFO order
	pool.Release("a1")
	view := pool.Snapshot()
	if view.ActiveLeases != 1 {
		t.Fatalf("expected 1 active lease after release, got %d", view.ActiveLeases)
	}
	// a2 should be granted (FIFO)
	lease, exists := pool.leases["a2"]
	if !exists {
		t.Error("expected a2 to be granted (FIFO order)")
	}
	_ = lease
}

func TestPoolSessionAffinity(t *testing.T) {
	pool := NewPool(4)
	workers := map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "pi", Account: "default", Capacity: 1, HeartbeatAt: time.Now(), AuthOK: true},
		"w2": {ID: "w2", Executor: "pi", Account: "default", Capacity: 1, HeartbeatAt: time.Now(), AuthOK: true},
	}
	pool.SetWorkers(workers)
	pool.SetAccountState("pi/default", true)

	// Acquire with preference for w1
	resp := pool.Acquire(AcquireReq{
		AttemptKey:   "a1",
		Executor:     "pi",
		Account:      "default",
		PreferWorker: "w1",
	})
	if !resp.Granted || resp.Lease.WorkerID != "w1" {
		t.Fatalf("expected lease on w1, got %+v", resp)
	}
	if resp.Lease.SessionPolicy != "reuse" {
		t.Errorf("expected reuse session policy, got %s", resp.Lease.SessionPolicy)
	}
}

func TestPoolDrain(t *testing.T) {
	pool := NewPool(4)
	workers := map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "pi", Account: "default", Capacity: 1, HeartbeatAt: time.Now(), AuthOK: true},
	}
	pool.SetWorkers(workers)
	pool.SetAccountState("pi/default", true)
	pool.Drain("w1", true)

	resp := pool.Acquire(AcquireReq{
		AttemptKey: "a1",
		Executor:   "pi",
		Account:    "default",
	})
	if resp.Granted {
		t.Fatal("expected no grant for drained worker")
	}
}

func TestPoolCooldown(t *testing.T) {
	pool := NewPool(4)
	workers := map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "pi", Account: "default", Capacity: 1, HeartbeatAt: time.Now(), AuthOK: true},
	}
	pool.SetWorkers(workers)
	pool.SetAccountState("pi/default", true)
	pool.Cooldown("pi/default", time.Now().Add(1*time.Hour))

	resp := pool.Acquire(AcquireReq{
		AttemptKey: "a1",
		Executor:   "pi",
		Account:    "default",
	})
	if resp.Granted {
		t.Fatal("expected no grant during cooldown")
	}
}

func TestPoolNeedsAuth(t *testing.T) {
	pool := NewPool(4)
	workers := map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "pi", Account: "default", Capacity: 1, HeartbeatAt: time.Now(), AuthOK: true},
	}
	pool.SetWorkers(workers)
	pool.SetAccountState("pi/default", false) // needs_auth

	resp := pool.Acquire(AcquireReq{
		AttemptKey: "a1",
		Executor:   "pi",
		Account:    "default",
	})
	if resp.Granted {
		t.Fatal("expected no grant when needs_auth")
	}
}

func TestPoolNoHoldAndWait(t *testing.T) {
	pool := NewPool(4)
	workers := map[string]core.WorkerRecord{
		"pi-w1": {ID: "pi-w1", Executor: "pi", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
		"cx-w1": {ID: "cx-w1", Executor: "codex", Account: "main", Capacity: 1, HeartbeatAt: time.Now(), AuthOK: true},
	}
	pool.SetWorkers(workers)
	pool.SetAccountState("pi/default", true)
	pool.SetAccountState("codex/main", true)

	// Fill Pi slots (2 of 4 global)
	pool.Acquire(AcquireReq{AttemptKey: "pi-a1", Executor: "pi", Account: "default"})
	pool.Acquire(AcquireReq{AttemptKey: "pi-a2", Executor: "pi", Account: "default"})

	// Codex should still get a slot (different account, global has room)
	resp := pool.Acquire(AcquireReq{
		AttemptKey: "cx-a1",
		Executor:   "codex",
		Account:    "main",
	})
	if !resp.Granted {
		t.Fatal("expected codex to get slot even when Pi is full")
	}
}
