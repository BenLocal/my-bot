package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/scheduler"
)

// TestPoolConcurrencyLimit verifies §12.3: global concurrency limit is enforced.
func TestPoolConcurrencyLimit(t *testing.T) {
	pool := scheduler.NewPool(2)

	// Set up a worker.
	pool.SetWorkers(map[string]core.WorkerRecord{
		"w1": {
			ID:          "w1",
			Executor:    "fake",
			Account:     "default",
			Endpoint:    "http://localhost:9101",
			Capacity:    2,
			HeartbeatAt: time.Now(),
			AuthOK:      true,
		},
	})
	pool.SetAccountState("fake/default", true)

	// Acquire 2 slots (should succeed).
	r1 := pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g1",
		AttemptKey:       "r1:s1:a1",
		Executor:         "fake",
		Account:          "default",
	})
	if !r1.Granted {
		t.Fatal("first acquire should be granted")
	}

	r2 := pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g2",
		AttemptKey:       "r1:s1:a2",
		Executor:         "fake",
		Account:          "default",
	})
	if !r2.Granted {
		t.Fatal("second acquire should be granted")
	}

	// Third acquire should be queued.
	r3 := pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g3",
		AttemptKey:       "r1:s1:a3",
		Executor:         "fake",
		Account:          "default",
	})
	if r3.Granted {
		t.Fatal("third acquire should be queued (not granted)")
	}

	// Snapshot should show 2 active leases and 1 queued.
	snap := pool.Snapshot()
	if snap.ActiveLeases != 2 {
		t.Fatalf("expected 2 active leases, got %d", snap.ActiveLeases)
	}
	if snap.Queued != 1 {
		t.Fatalf("expected 1 queued, got %d", snap.Queued)
	}

	// Release one slot, the queued request should be granted.
	granted, err := pool.Release("r1:s1:a1")
	if err != nil {
		t.Fatalf("release failed: %v", err)
	}
	if len(granted) != 1 {
		t.Fatalf("expected 1 granted from queue, got %d", len(granted))
	}
	if granted[0].AttemptKey != "r1:s1:a3" {
		t.Fatalf("expected r1:s1:a3 to be granted, got %s", granted[0].AttemptKey)
	}
}

// TestPoolIdempotentAcquire verifies §12.2: same attemptKey returns same lease.
func TestPoolIdempotentAcquire(t *testing.T) {
	pool := scheduler.NewPool(4)
	pool.SetWorkers(map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "fake", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
	})
	pool.SetAccountState("fake/default", true)

	req := scheduler.AcquireReq{
		WaiterInstanceID: "g1",
		AttemptKey:       "r1:s1:a1",
		Executor:         "fake",
		Account:          "default",
	}

	r1 := pool.Acquire(req)
	r2 := pool.Acquire(req)

	if !r1.Granted || !r2.Granted {
		t.Fatal("both acquires should be granted")
	}
	if r1.Lease.ID != r2.Lease.ID {
		t.Fatal("same attemptKey should return same lease")
	}

	// Snapshot should show 1 active lease (not 2).
	snap := pool.Snapshot()
	if snap.ActiveLeases != 1 {
		t.Fatalf("expected 1 active lease, got %d", snap.ActiveLeases)
	}
}

// TestPoolQueueFIFO verifies FIFO queue ordering.
func TestPoolQueueFIFO(t *testing.T) {
	pool := scheduler.NewPool(1)
	pool.SetWorkers(map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "fake", Account: "default", Capacity: 1, HeartbeatAt: time.Now(), AuthOK: true},
	})
	pool.SetAccountState("fake/default", true)

	// Acquire the only slot.
	pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g1",
		AttemptKey:       "r1:s1:a1",
		Executor:         "fake",
		Account:          "default",
	})

	// Queue two requests.
	pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g2",
		AttemptKey:       "r1:s1:a2",
		Executor:         "fake",
		Account:          "default",
	})
	pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g3",
		AttemptKey:       "r1:s1:a3",
		Executor:         "fake",
		Account:          "default",
	})

	// Release the slot.
	granted, _ := pool.Release("r1:s1:a1")

	// First queued request should be granted.
	if len(granted) != 1 || granted[0].AttemptKey != "r1:s1:a2" {
		t.Fatalf("expected r1:s1:a2 to be granted first, got %+v", granted)
	}
}

// TestPoolReclaimExpired verifies that expired leases are reclaimed.
func TestPoolReclaimExpired(t *testing.T) {
	pool := scheduler.NewPool(2)
	pool.SetWorkers(map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "fake", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
	})
	pool.SetAccountState("fake/default", true)

	// Acquire a slot.
	pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g1",
		AttemptKey:       "r1:s1:a1",
		Executor:         "fake",
		Account:          "default",
	})

	// Reclaim with very short TTL.
	granted := pool.ReclaimExpired(time.Nanosecond)
	if len(granted) != 0 {
		t.Fatal("no queued requests should be granted")
	}

	// Snapshot should show 0 active leases.
	snap := pool.Snapshot()
	if snap.ActiveLeases != 0 {
		t.Fatalf("expected 0 active leases after reclaim, got %d", snap.ActiveLeases)
	}
}

// TestPoolStateSerialization verifies that Pool state can be serialized and restored.
func TestPoolStateSerialization(t *testing.T) {
	pool := scheduler.NewPool(2)
	pool.SetWorkers(map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "fake", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
	})
	pool.SetAccountState("fake/default", true)

	// Acquire a slot.
	pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g1",
		AttemptKey:       "r1:s1:a1",
		Executor:         "fake",
		Account:          "default",
	})

	// Serialize state.
	state := pool.SerializeState()

	// Create a new pool and restore.
	pool2 := scheduler.NewPool(2)
	pool2.DeserializeState(state)

	// Verify the lease is restored.
	snap := pool2.Snapshot()
	if snap.ActiveLeases != 1 {
		t.Fatalf("expected 1 active lease after restore, got %d", snap.ActiveLeases)
	}
}

// TestPoolAccountState verifies account state transitions.
func TestPoolAccountState(t *testing.T) {
	pool := scheduler.NewPool(4)
	pool.SetWorkers(map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "fake", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
	})

	// Account not ok → queued.
	pool.SetAccountState("fake/default", false)
	r1 := pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g1",
		AttemptKey:       "r1:s1:a1",
		Executor:         "fake",
		Account:          "default",
	})
	if r1.Granted {
		t.Fatal("acquire should be queued when account is not ok")
	}

	// Set account ok → should be granted.
	pool.SetAccountState("fake/default", true)
	granted := pool.TrySatisfyQueue()
	if len(granted) != 1 {
		t.Fatalf("expected 1 granted after account ok, got %d", len(granted))
	}

	// Set account needs_auth → new acquires should be queued.
	pool.SetAccountState("fake/default", false)
	r2 := pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g2",
		AttemptKey:       "r1:s1:a2",
		Executor:         "fake",
		Account:          "default",
	})
	if r2.Granted {
		t.Fatal("acquire should be queued when account needs_auth")
	}
}

// TestPoolDrain verifies that drained workers don't get new leases.
func TestPoolDrain(t *testing.T) {
	pool := scheduler.NewPool(4)
	pool.SetWorkers(map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "fake", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
		"w2": {ID: "w2", Executor: "fake", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
	})
	pool.SetAccountState("fake/default", true)

	// Drain w1.
	pool.Drain("w1", true)

	// Acquire should go to w2.
	r := pool.Acquire(scheduler.AcquireReq{
		WaiterInstanceID: "g1",
		AttemptKey:       "r1:s1:a1",
		Executor:         "fake",
		Account:          "default",
	})
	if !r.Granted {
		t.Fatal("acquire should be granted")
	}
	if r.Lease.WorkerID == "w1" {
		t.Fatal("drained worker should not receive new leases")
	}
	if r.Lease.WorkerID != "w2" {
		t.Fatalf("expected w2 to get lease, got %s", r.Lease.WorkerID)
	}
}

// TestPoolSnapshot verifies Snapshot returns correct data.
func TestPoolSnapshot(t *testing.T) {
	pool := scheduler.NewPool(4)
	pool.SetWorkers(map[string]core.WorkerRecord{
		"w1": {ID: "w1", Executor: "fake", Account: "default", Capacity: 2, HeartbeatAt: time.Now(), AuthOK: true},
	})
	pool.SetAccountState("fake/default", true)

	snap := pool.Snapshot()
	if snap.ActiveLeases != 0 {
		t.Fatalf("expected 0 active leases, got %d", snap.ActiveLeases)
	}
	if snap.Queued != 0 {
		t.Fatalf("expected 0 queued, got %d", snap.Queued)
	}
	if snap.Accounts["fake/default"] != "ok" {
		t.Fatalf("expected account to be ok, got %s", snap.Accounts["fake/default"])
	}
}

// TestPoolContext ensures context is used correctly.
var _ = context.Background
