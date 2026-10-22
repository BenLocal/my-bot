package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
)

// fakeClock is an injectable clock for Pool.now.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newTestPool builds a pool with an injected clock and the given workers
// (heartbeat = now).
func newTestPool(global int, workers ...core.WorkerRecord) (*Pool, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)}
	p := NewPool(global)
	p.now = clk.now
	m := make(map[string]core.WorkerRecord)
	for _, w := range workers {
		w.HeartbeatAt = clk.t
		w.AuthOK = true
		m[w.ID] = w
	}
	p.SetWorkers(m)
	return p, clk
}

func worker(id, executor, account string, capacity int) core.WorkerRecord {
	return core.WorkerRecord{ID: id, Executor: executor, Account: account, Capacity: capacity}
}

func req(key, goal, executor, account string) AcquireReq {
	return AcquireReq{WaiterInstanceID: goal, AttemptKey: key, Executor: executor, Account: account}
}

func mustGrant(t *testing.T, resp AcquireResp, workerID, policy string) {
	t.Helper()
	if !resp.Granted || resp.Lease == nil {
		t.Fatalf("expected grant, got %+v", resp)
	}
	if workerID != "" && resp.Lease.WorkerID != workerID {
		t.Fatalf("expected worker %s, got %s", workerID, resp.Lease.WorkerID)
	}
	if policy != "" && resp.Lease.SessionPolicy != policy {
		t.Fatalf("expected sessionPolicy %s, got %s", policy, resp.Lease.SessionPolicy)
	}
}

func mustQueue(t *testing.T, resp AcquireResp) {
	t.Helper()
	if resp.Granted {
		t.Fatalf("expected queued, got lease %+v", resp.Lease)
	}
}

func grantedKeys(g []GrantedLease) []string {
	var keys []string
	for _, x := range g {
		keys = append(keys, x.AttemptKey)
	}
	return keys
}

func TestPoolGlobalLimitConcurrent(t *testing.T) {
	p, _ := newTestPool(3, worker("w1", "pi", "default", 10), worker("w2", "pi", "default", 10))
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := p.Acquire(req(fmt.Sprintf("a%d", i), "g", "pi", "default"))
			if resp.Granted {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	view := p.Snapshot()
	if granted != 3 || view.ActiveLeases != 3 || view.Queued != 47 {
		t.Fatalf("expected 3 granted / 47 queued, got granted=%d view=%d/%d", granted, view.ActiveLeases, view.Queued)
	}
}

func TestPoolAcquireIdempotent(t *testing.T) {
	p, _ := newTestPool(1, worker("w1", "pi", "default", 2))

	r1 := p.Acquire(req("a1", "g1", "pi", "default"))
	r2 := p.Acquire(req("a1", "g1", "pi", "default"))
	mustGrant(t, r1, "w1", SessionFresh)
	mustGrant(t, r2, "w1", SessionFresh)
	if r1.Lease.ID != "a1" || r2.Lease.ID != r1.Lease.ID {
		t.Fatalf("expected lease ID = attemptKey, got %s / %s", r1.Lease.ID, r2.Lease.ID)
	}

	mustQueue(t, p.Acquire(req("a2", "g2", "pi", "default")))
	mustQueue(t, p.Acquire(req("a2", "g2", "pi", "default")))
	if view := p.Snapshot(); view.ActiveLeases != 1 || view.Queued != 1 {
		t.Fatalf("expected 1 lease / 1 queued, got %d / %d", view.ActiveLeases, view.Queued)
	}
}

func TestPoolFIFO(t *testing.T) {
	p, _ := newTestPool(1, worker("w1", "pi", "default", 4))
	mustGrant(t, p.Acquire(req("a1", "g1", "pi", "default")), "w1", "")
	mustQueue(t, p.Acquire(req("a2", "g2", "pi", "default")))
	mustQueue(t, p.Acquire(req("a3", "g3", "pi", "default")))

	granted, err := p.Release("a1")
	if err != nil {
		t.Fatal(err)
	}
	if keys := grantedKeys(granted); !reflect.DeepEqual(keys, []string{"a2"}) {
		t.Fatalf("expected a2 granted first, got %v", keys)
	}
	if granted[0].InstanceID != "g2" || granted[0].Lease.WaiterInstanceID != "g2" {
		t.Fatalf("expected waiter g2, got %+v", granted[0])
	}
	granted, _ = p.Release("a2")
	if keys := grantedKeys(granted); !reflect.DeepEqual(keys, []string{"a3"}) {
		t.Fatalf("expected a3 granted next, got %v", keys)
	}
}

func TestPoolNoHeadOfLineBlocking(t *testing.T) {
	p, _ := newTestPool(2, worker("pi-w1", "pi", "default", 1), worker("cx-w1", "codex", "main", 1))
	mustGrant(t, p.Acquire(req("pi-a1", "g1", "pi", "default")), "pi-w1", "")
	mustGrant(t, p.Acquire(req("cx-a1", "g2", "codex", "main")), "cx-w1", "")

	// Global full: a codex request queues first, then a pi request.
	mustQueue(t, p.Acquire(req("cx-a2", "g3", "codex", "main")))
	mustQueue(t, p.Acquire(req("pi-a2", "g4", "pi", "default")))

	// Releasing the pi lease frees a global slot and a pi worker slot; the
	// codex request ahead cannot use it (codex worker full) and must not block pi.
	granted, _ := p.Release("pi-a1")
	if keys := grantedKeys(granted); !reflect.DeepEqual(keys, []string{"pi-a2"}) {
		t.Fatalf("expected pi-a2 granted, got %v", keys)
	}
	if view := p.Snapshot(); view.Queued != 1 || view.QueuedItems[0].AttemptKey != "cx-a2" {
		t.Fatalf("expected cx-a2 still queued, got %+v", view.QueuedItems)
	}
}

func TestPoolAccountConcurrency(t *testing.T) {
	p, _ := newTestPool(10, worker("w1", "pi", "default", 10), worker("c1", "codex", "main", 10))
	p.SetAccountLimits(map[string]int{"pi/default": 2})

	mustGrant(t, p.Acquire(req("a1", "g", "pi", "default")), "", "")
	mustGrant(t, p.Acquire(req("a2", "g", "pi", "default")), "", "")
	mustQueue(t, p.Acquire(req("a3", "g", "pi", "default")))
	// No entry for codex/main: no per-account limit.
	for i := 0; i < 3; i++ {
		mustGrant(t, p.Acquire(req(fmt.Sprintf("c%d", i), "g", "codex", "main")), "c1", "")
	}

	granted, _ := p.Release("a1")
	if keys := grantedKeys(granted); !reflect.DeepEqual(keys, []string{"a3"}) {
		t.Fatalf("expected a3 granted after release, got %v", keys)
	}
}

func TestPoolAccountStateBlocksGrants(t *testing.T) {
	tests := []struct {
		name    string
		block   func(p *Pool, clk *fakeClock)
		unblock func(p *Pool, clk *fakeClock)
	}{
		{
			name:    "cooldown",
			block:   func(p *Pool, clk *fakeClock) { p.Cooldown("pi/default", clk.t.Add(time.Minute)) },
			unblock: func(p *Pool, clk *fakeClock) { clk.advance(2 * time.Minute) },
		},
		{
			name:    "needs_auth",
			block:   func(p *Pool, _ *fakeClock) { p.SetAccountState("pi/default", false) },
			unblock: func(p *Pool, _ *fakeClock) { p.SetAccountState("pi/default", true) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, clk := newTestPool(4, worker("w1", "pi", "default", 4))
			tt.block(p, clk)
			mustQueue(t, p.Acquire(req("a1", "g1", "pi", "default")))
			if st := p.Snapshot().Accounts["pi/default"]; st != tt.name {
				t.Fatalf("expected account state %s, got %s", tt.name, st)
			}
			if g := p.TrySatisfyQueue(); len(g) != 0 {
				t.Fatalf("expected no grant while blocked, got %v", grantedKeys(g))
			}

			// Keep the worker heartbeat fresh across clock advances.
			w := p.workers["w1"]
			tt.unblock(p, clk)
			w.HeartbeatAt = clk.t
			p.SetWorkers(map[string]core.WorkerRecord{"w1": w})

			if keys := grantedKeys(p.TrySatisfyQueue()); !reflect.DeepEqual(keys, []string{"a1"}) {
				t.Fatalf("expected a1 granted after unblock, got %v", keys)
			}
		})
	}
}

func TestPoolAffinityHitReuse(t *testing.T) {
	p, _ := newTestPool(4, worker("w1", "pi", "default", 1), worker("w2", "pi", "default", 1))
	r := req("a1", "g1", "pi", "default")
	r.PreferWorker = "w2"
	mustGrant(t, p.Acquire(r), "w2", SessionReuse)
}

func TestPoolAffinityFullWaitsThenReuse(t *testing.T) {
	p, clk := newTestPool(4, worker("w1", "pi", "default", 1), worker("w2", "pi", "default", 1))
	mustGrant(t, p.Acquire(req("busy", "g0", "pi", "default")), "w1", SessionFresh)

	r := req("a1", "g1", "pi", "default")
	r.PreferWorker = "w1"
	// w2 is free, but the preferred w1 is full and alive: must wait for w1.
	mustQueue(t, p.Acquire(r))
	clk.advance(time.Minute)
	if g := p.TrySatisfyQueue(); len(g) != 0 {
		t.Fatalf("expected still waiting for w1, got %v", grantedKeys(g))
	}

	granted, _ := p.Release("busy")
	if len(granted) != 1 || granted[0].Lease.WorkerID != "w1" || granted[0].Lease.SessionPolicy != SessionReuse {
		t.Fatalf("expected a1 granted on w1 with reuse, got %+v", granted)
	}
}

func TestPoolAffinityWaitExpiry(t *testing.T) {
	tests := []struct {
		name string
		wait time.Duration
		// advance just below and just past the effective wait
		below, past time.Duration
	}{
		{name: "explicit wait", wait: 2 * time.Minute, below: time.Minute, past: 2 * time.Minute},
		{name: "default wait", wait: 0, below: 4 * time.Minute, past: DefaultAffinityWait},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, clk := newTestPool(4, worker("w1", "pi", "default", 1), worker("w2", "pi", "default", 1))
			mustGrant(t, p.Acquire(req("busy", "g0", "pi", "default")), "w1", "")

			r := req("a1", "g1", "pi", "default")
			r.PreferWorker = "w1"
			r.AffinityWait = tt.wait
			mustQueue(t, p.Acquire(r))

			// Heartbeats stay fresh: re-stamp after each advance.
			refresh := func() {
				ws := make(map[string]core.WorkerRecord)
				for id, w := range p.workers {
					w.HeartbeatAt = clk.t
					ws[id] = w
				}
				p.SetWorkers(ws)
			}
			clk.advance(tt.below)
			refresh()
			if g := p.TrySatisfyQueue(); len(g) != 0 {
				t.Fatalf("expected still waiting, got %v", grantedKeys(g))
			}
			clk.advance(tt.past - tt.below)
			refresh()
			granted := p.TrySatisfyQueue()
			if len(granted) != 1 || granted[0].Lease.WorkerID != "w2" || granted[0].Lease.SessionPolicy != SessionFresh {
				t.Fatalf("expected a1 relaxed to w2 with fresh, got %+v", granted)
			}
		})
	}
}

func TestPoolPreferredWorkerGone(t *testing.T) {
	tests := []struct {
		name  string
		setup func(p *Pool, clk *fakeClock)
	}{
		{name: "unregistered", setup: func(p *Pool, _ *fakeClock) {}},
		{name: "heartbeat expired", setup: func(p *Pool, clk *fakeClock) {
			p.workers["w9"] = core.WorkerRecord{ID: "w9", Executor: "pi", Account: "default", Capacity: 1, HeartbeatAt: clk.t.Add(-2 * heartbeatTTL)}
		}},
		{name: "drained", setup: func(p *Pool, clk *fakeClock) {
			p.workers["w9"] = core.WorkerRecord{ID: "w9", Executor: "pi", Account: "default", Capacity: 1, HeartbeatAt: clk.t}
			p.Drain("w9", true)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, clk := newTestPool(4, worker("w1", "pi", "default", 1))
			tt.setup(p, clk)
			r := req("a1", "g1", "pi", "default")
			r.PreferWorker = "w9"
			mustGrant(t, p.Acquire(r), "w1", SessionFresh)
		})
	}
}

func TestPoolDrain(t *testing.T) {
	p, _ := newTestPool(4, worker("w1", "pi", "default", 2), worker("w2", "pi", "default", 2))
	mustGrant(t, p.Acquire(req("a1", "g1", "pi", "default")), "w1", "")
	p.Drain("w1", true)

	for i := 0; i < 2; i++ {
		mustGrant(t, p.Acquire(req(fmt.Sprintf("b%d", i), "g2", "pi", "default")), "w2", "")
	}
	mustQueue(t, p.Acquire(req("c", "g3", "pi", "default")))

	view := p.Snapshot()
	if view.Inflight["w1"] != 1 || !reflect.DeepEqual(view.Drained, []string{"w1"}) {
		t.Fatalf("unexpected view %+v", view)
	}
	// Releasing the drained worker's lease must not grant a new lease on it.
	if g, _ := p.Release("a1"); len(g) != 0 {
		t.Fatalf("expected no grant on drained worker, got %v", grantedKeys(g))
	}
	if view := p.Snapshot(); view.Inflight["w1"] != 0 {
		t.Fatalf("expected drained worker inflight 0, got %d", view.Inflight["w1"])
	}
}

func TestPoolReleaseQueuedAndIdempotent(t *testing.T) {
	p, _ := newTestPool(1, worker("w1", "pi", "default", 2))
	mustGrant(t, p.Acquire(req("a1", "g1", "pi", "default")), "", "")
	mustQueue(t, p.Acquire(req("a2", "g2", "pi", "default")))
	mustQueue(t, p.Acquire(req("a3", "g3", "pi", "default")))

	// Releasing a queued request just removes it.
	if g, err := p.Release("a2"); err != nil || len(g) != 0 {
		t.Fatalf("expected no grant/no error, got %v %v", g, err)
	}
	// Unknown key is a no-op.
	if g, err := p.Release("nope"); err != nil || len(g) != 0 {
		t.Fatalf("expected no-op, got %v %v", g, err)
	}
	granted, _ := p.Release("a1")
	if keys := grantedKeys(granted); !reflect.DeepEqual(keys, []string{"a3"}) {
		t.Fatalf("expected a3 granted (a2 removed), got %v", keys)
	}
	// Second release of the same key is a no-op.
	if g, err := p.Release("a1"); err != nil || len(g) != 0 {
		t.Fatalf("expected idempotent release, got %v %v", g, err)
	}
	if view := p.Snapshot(); view.ActiveLeases != 1 || view.Queued != 0 {
		t.Fatalf("expected 1 lease / 0 queued, got %d / %d", view.ActiveLeases, view.Queued)
	}
}

func TestPoolReleaseWaiter(t *testing.T) {
	p, _ := newTestPool(2, worker("w1", "pi", "default", 4))
	mustGrant(t, p.Acquire(req("g1-a1", "g1", "pi", "default")), "", "")
	mustGrant(t, p.Acquire(req("g1-a2", "g1", "pi", "default")), "", "")
	mustQueue(t, p.Acquire(req("g1-a3", "g1", "pi", "default")))
	mustQueue(t, p.Acquire(req("g2-a1", "g2", "pi", "default")))

	if ids := p.WaiterInstanceIDs(); !reflect.DeepEqual(ids, []string{"g1", "g2"}) {
		t.Fatalf("unexpected waiters %v", ids)
	}
	granted := p.ReleaseWaiter("g1")
	if keys := grantedKeys(granted); !reflect.DeepEqual(keys, []string{"g2-a1"}) {
		t.Fatalf("expected g2-a1 granted, got %v", keys)
	}
	view := p.Snapshot()
	if view.ActiveLeases != 1 || view.Queued != 0 {
		t.Fatalf("expected only g2 lease left, got %+v", view)
	}
	if g := p.ReleaseWaiter("g1"); len(g) != 0 {
		t.Fatalf("expected idempotent ReleaseWaiter, got %v", g)
	}
}

// fakeGoalChecker reports goals in the terminal set as terminal.
type fakeGoalChecker struct {
	terminal map[string]bool
	err      map[string]error
}

func (f *fakeGoalChecker) GoalTerminal(_ context.Context, goalID string) (bool, error) {
	return f.terminal[goalID], f.err[goalID]
}

// fakeDeliverer records slot-granted deliveries.
type fakeDeliverer struct{ keys []string }

func (f *fakeDeliverer) DeliverSlotGranted(_ context.Context, _ string, attemptKey string, _ *Lease) error {
	f.keys = append(f.keys, attemptKey)
	return nil
}

func TestActorReminderReclaimsTerminalGoals(t *testing.T) {
	a := NewSchedulerActor(2, 0)
	p, _ := newTestPool(2, worker("w1", "pi", "default", 4))
	a.pool = p
	deliver := &fakeDeliverer{}
	a.SetSignalDeliverer(deliver)
	a.SetGoalStatusChecker(&fakeGoalChecker{
		terminal: map[string]bool{"done": true, "gone": true},
		err:      map[string]error{"flaky": fmt.Errorf("boom")},
	})

	mustGrant(t, p.Acquire(req("done-a1", "done", "pi", "default")), "", "")
	mustGrant(t, p.Acquire(req("flaky-a1", "flaky", "pi", "default")), "", "")
	mustQueue(t, p.Acquire(req("gone-a1", "gone", "pi", "default")))
	mustQueue(t, p.Acquire(req("live-a1", "live", "pi", "default")))

	a.ReminderCall(ReminderName, nil, "", "")

	if !reflect.DeepEqual(deliver.keys, []string{"live-a1"}) {
		t.Fatalf("expected live-a1 delivered, got %v", deliver.keys)
	}
	view := p.Snapshot()
	var keys []string
	for _, l := range view.Leases {
		keys = append(keys, l.AttemptKey)
	}
	if !reflect.DeepEqual(keys, []string{"flaky-a1", "live-a1"}) || view.Queued != 0 {
		t.Fatalf("unexpected leases %v queued %d", keys, view.Queued)
	}
	if a.leaseTTL != DefaultLeaseTTL {
		t.Fatalf("expected default lease TTL %v, got %v", DefaultLeaseTTL, a.leaseTTL)
	}
}

func TestPoolSerializeRoundTrip(t *testing.T) {
	p, clk := newTestPool(1, worker("w1", "pi", "default", 2))
	p.Cooldown("codex/main", clk.t.Add(time.Hour))
	p.Drain("w9", true)
	mustGrant(t, p.Acquire(req("a1", "g1", "pi", "default")), "w1", "")
	r := req("a2", "g2", "pi", "default")
	r.PreferWorker = "w1"
	r.AffinityWait = 3 * time.Minute
	mustQueue(t, p.Acquire(r))

	state := p.SerializeState()
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PoolState
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Leases["a1"].WaiterInstanceID != "g1" {
		t.Fatalf("expected waiterInstanceID persisted, got %+v", decoded.Leases["a1"])
	}

	p2, _ := newTestPool(1)
	p2.now = clk.now
	p2.DeserializeState(decoded)
	if got := p2.SerializeState(); !reflect.DeepEqual(got, state) {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got, state)
	}
	view := p2.Snapshot()
	if len(view.Leases) != 1 || view.Leases[0].WaiterInstanceID != "g1" ||
		len(view.QueuedItems) != 1 || view.QueuedItems[0].PreferWorker != "w1" {
		t.Fatalf("unexpected restored view %+v", view)
	}
}

func TestSchedulerViewJSON(t *testing.T) {
	p, _ := newTestPool(1, worker("w1", "pi", "default", 1))
	mustGrant(t, p.Acquire(req("a1", "g1", "pi", "default")), "", "")
	mustQueue(t, p.Acquire(req("a2", "g2", "pi", "default")))
	raw, err := json.Marshal(p.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"activeLeases", "queued", "accounts", "drained", "inflight", "leases", "queuedItems"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing JSON key %q in %s", k, raw)
		}
	}
}
