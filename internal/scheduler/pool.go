package scheduler

import (
	"sort"
	"sync"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
)

// DefaultAffinityWait is how long a request waits for its full PreferWorker
// before relaxing to any candidate (docs/spec.md §5.1 rule 4).
const DefaultAffinityWait = 5 * time.Minute

// heartbeatTTL is how long a worker heartbeat stays fresh.
const heartbeatTTL = 60 * time.Second

// Session policies carried by a lease.
const (
	SessionReuse = "reuse"
	SessionFresh = "fresh"
)

// Pool is the pure-logic scheduler pool (no Dapr dependency).
// It manages leases, FIFO queue, account state, and worker selection.
type Pool struct {
	mu sync.Mutex

	globalConcurrency int

	// per-account concurrency, keyed by "executor/account"; missing = no limit
	accountLimits map[string]int

	// account state
	accounts map[string]*accountState

	// active leases by attemptKey
	leases map[string]*Lease

	// FIFO queue of waiting requests
	queue []*queuedRequest

	// worker registry snapshot (set externally)
	workers map[string]core.WorkerRecord

	// round-robin cursor per executor/account
	cursors map[string]int

	// drain set
	drained map[string]bool

	// now is the clock; injectable for tests
	now func() time.Time
}

type accountState struct {
	ok            bool
	cooldownUntil time.Time
}

type Lease struct {
	ID               string // equals the attemptKey
	WorkerID         string
	WaiterInstanceID string
	Executor         string
	Account          string
	SessionPolicy    string // reuse | fresh
	AcquiredAt       time.Time
}

type queuedRequest struct {
	WaiterInstanceID string
	AttemptKey       string
	Executor         string
	Account          string
	PreferWorker     string
	AffinityWait     time.Duration
	EnqueuedAt       time.Time
}

// GrantedLease is returned when a queued request is granted a lease.
type GrantedLease struct {
	InstanceID string
	AttemptKey string
	Lease      *Lease
}

type AcquireReq struct {
	WaiterInstanceID  string
	AttemptKey        string
	Executor, Account string
	PreferWorker      string
	AffinityWait      time.Duration
}

type AcquireResp struct {
	Granted bool
	Lease   *Lease
	// If not granted, the request is queued and will be delivered via event.
}

// NewPool creates a new scheduler pool.
func NewPool(globalConcurrency int) *Pool {
	return &Pool{
		globalConcurrency: globalConcurrency,
		accountLimits:     make(map[string]int),
		accounts:          make(map[string]*accountState),
		leases:            make(map[string]*Lease),
		workers:           make(map[string]core.WorkerRecord),
		cursors:           make(map[string]int),
		drained:           make(map[string]bool),
		now:               time.Now,
	}
}

// SetGlobalConcurrency updates the global active lease limit.
func (p *Pool) SetGlobalConcurrency(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.globalConcurrency = n
}

// SetAccountLimits sets per-account concurrency limits keyed by
// "executor/account". Accounts without an entry have no per-account limit.
func (p *Pool) SetAccountLimits(limits map[string]int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.accountLimits = make(map[string]int, len(limits))
	for k, v := range limits {
		p.accountLimits[k] = v
	}
}

// SetWorkers updates the worker registry snapshot.
func (p *Pool) SetWorkers(workers map[string]core.WorkerRecord) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workers = workers
}

// SetAccountState sets account status to ok or needs_auth.
// The account parameter is the full key, e.g. "pi/default".
func (p *Pool) SetAccountState(account string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := account
	if _, exists := p.accounts[key]; !exists {
		p.accounts[key] = &accountState{}
	}
	p.accounts[key].ok = ok
	if ok {
		p.accounts[key].cooldownUntil = time.Time{}
	}
}

// Cooldown puts an account into cooldown until the given time.
func (p *Pool) Cooldown(account string, until time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := account
	if _, exists := p.accounts[key]; !exists {
		p.accounts[key] = &accountState{ok: true}
	}
	p.accounts[key].cooldownUntil = until
}

// Drain marks a worker as drained (no new leases).
func (p *Pool) Drain(workerID string, on bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if on {
		p.drained[workerID] = true
	} else {
		delete(p.drained, workerID)
	}
}

// Acquire attempts to get a lease. Returns Granted=true if successful,
// otherwise the request is queued. Idempotent per AttemptKey.
func (p *Pool) Acquire(req AcquireReq) AcquireResp {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if already has a lease for this attemptKey
	if lease, exists := p.leases[req.AttemptKey]; exists {
		return AcquireResp{Granted: true, Lease: lease}
	}

	// Check if already in queue
	for _, q := range p.queue {
		if q.AttemptKey == req.AttemptKey {
			return AcquireResp{Granted: false} // already queued
		}
	}

	q := &queuedRequest{
		WaiterInstanceID: req.WaiterInstanceID,
		AttemptKey:       req.AttemptKey,
		Executor:         req.Executor,
		Account:          req.Account,
		PreferWorker:     req.PreferWorker,
		AffinityWait:     req.AffinityWait,
		EnqueuedAt:       p.now(),
	}
	if lease := p.tryGrantLocked(q); lease != nil {
		return AcquireResp{Granted: true, Lease: lease}
	}
	p.queue = append(p.queue, q)
	return AcquireResp{Granted: false}
}

// Release releases the lease or removes the still-queued request for the
// attempt key, then tries to satisfy queued requests. Idempotent: an unknown
// key is a no-op. Returns the leases that were granted from the queue so the
// caller can deliver slot-granted signals. The error is always nil.
func (p *Pool) Release(attemptKey string) ([]GrantedLease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.removeQueuedLocked(func(q *queuedRequest) bool { return q.AttemptKey == attemptKey })
	if _, exists := p.leases[attemptKey]; !exists {
		return nil, nil
	}
	delete(p.leases, attemptKey)
	return p.trySatisfyQueue(), nil
}

// ReleaseWaiter releases every lease and queued request belonging to the
// given waiter (goal) instances, e.g. when a goal is cancelled or finished.
// All given waiters are dropped before the queue is retried, so none of them
// is granted a new lease. Returns the leases that were granted from the queue.
func (p *Pool) ReleaseWaiter(instanceIDs ...string) []GrantedLease {
	p.mu.Lock()
	defer p.mu.Unlock()

	ids := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		ids[id] = true
	}
	p.removeQueuedLocked(func(q *queuedRequest) bool { return ids[q.WaiterInstanceID] })
	released := false
	for key, lease := range p.leases {
		if ids[lease.WaiterInstanceID] {
			delete(p.leases, key)
			released = true
		}
	}
	if !released {
		return nil
	}
	return p.trySatisfyQueue()
}

// WaiterInstanceIDs returns the sorted distinct waiter instance IDs of all
// leases and queued requests (empty IDs excluded).
func (p *Pool) WaiterInstanceIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	seen := make(map[string]bool)
	for _, lease := range p.leases {
		if lease.WaiterInstanceID != "" {
			seen[lease.WaiterInstanceID] = true
		}
	}
	for _, q := range p.queue {
		if q.WaiterInstanceID != "" {
			seen[q.WaiterInstanceID] = true
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// removeQueuedLocked drops queued requests matching the predicate, keeping
// FIFO order. Must hold lock.
func (p *Pool) removeQueuedLocked(match func(*queuedRequest) bool) {
	remaining := p.queue[:0]
	for _, q := range p.queue {
		if !match(q) {
			remaining = append(remaining, q)
		}
	}
	for i := len(remaining); i < len(p.queue); i++ {
		p.queue[i] = nil
	}
	p.queue = remaining
}

// SchedulerView is the pool state returned for GET /workers.
type SchedulerView struct {
	ActiveLeases int               `json:"activeLeases"`
	Queued       int               `json:"queued"`
	Accounts     map[string]string `json:"accounts"` // account -> "ok" | "needs_auth" | "cooldown"
	Drained      []string          `json:"drained"`
	Inflight     map[string]int    `json:"inflight"` // workerID -> active leases
	Leases       []LeaseView       `json:"leases"`
	QueuedItems  []QueuedView      `json:"queuedItems"`
}

// LeaseView is an active lease in SchedulerView.
type LeaseView struct {
	AttemptKey       string `json:"attemptKey"`
	WorkerID         string `json:"workerID"`
	WaiterInstanceID string `json:"waiterInstanceID"`
	SessionPolicy    string `json:"sessionPolicy"`
}

// QueuedView is a queued request in SchedulerView (FIFO order).
type QueuedView struct {
	AttemptKey       string    `json:"attemptKey"`
	WaiterInstanceID string    `json:"waiterInstanceID"`
	Executor         string    `json:"executor"`
	Account          string    `json:"account"`
	PreferWorker     string    `json:"preferWorker,omitempty"`
	EnqueuedAt       time.Time `json:"enqueuedAt"`
}

// Snapshot returns the current pool state for /workers.
func (p *Pool) Snapshot() SchedulerView {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.now()
	view := SchedulerView{
		ActiveLeases: len(p.leases),
		Queued:       len(p.queue),
		Accounts:     make(map[string]string),
		Drained:      []string{},
		Inflight:     make(map[string]int),
		Leases:       []LeaseView{},
		QueuedItems:  []QueuedView{},
	}
	for key, acct := range p.accounts {
		if !acct.ok {
			view.Accounts[key] = "needs_auth"
		} else if now.Before(acct.cooldownUntil) {
			view.Accounts[key] = "cooldown"
		} else {
			view.Accounts[key] = "ok"
		}
	}
	for id := range p.drained {
		view.Drained = append(view.Drained, id)
	}
	sort.Strings(view.Drained)
	for id := range p.workers {
		view.Inflight[id] = 0
	}
	for key, lease := range p.leases {
		view.Inflight[lease.WorkerID]++
		view.Leases = append(view.Leases, LeaseView{
			AttemptKey:       key,
			WorkerID:         lease.WorkerID,
			WaiterInstanceID: lease.WaiterInstanceID,
			SessionPolicy:    lease.SessionPolicy,
		})
	}
	sort.Slice(view.Leases, func(i, j int) bool { return view.Leases[i].AttemptKey < view.Leases[j].AttemptKey })
	for _, q := range p.queue {
		view.QueuedItems = append(view.QueuedItems, QueuedView{
			AttemptKey:       q.AttemptKey,
			WaiterInstanceID: q.WaiterInstanceID,
			Executor:         q.Executor,
			Account:          q.Account,
			PreferWorker:     q.PreferWorker,
			EnqueuedAt:       q.EnqueuedAt,
		})
	}
	return view
}

// TrySatisfyQueue attempts to grant leases to queued requests.
// Returns the leases that were granted so the caller can deliver signals.
func (p *Pool) TrySatisfyQueue() []GrantedLease {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.trySatisfyQueue()
}

// trySatisfyQueue walks the queue in FIFO order and grants every request that
// can be satisfied now. A request that cannot be granted stays queued without
// blocking later requests (no head-of-line blocking). Must hold lock.
func (p *Pool) trySatisfyQueue() []GrantedLease {
	var remaining []*queuedRequest
	var granted []GrantedLease
	for _, q := range p.queue {
		if lease := p.tryGrantLocked(q); lease != nil {
			granted = append(granted, GrantedLease{
				InstanceID: q.WaiterInstanceID,
				AttemptKey: q.AttemptKey,
				Lease:      lease,
			})
		} else {
			remaining = append(remaining, q)
		}
	}
	p.queue = remaining
	return granted
}

// tryGrantLocked applies the allocation rules (docs/spec.md §5.1) to a single
// request and records the lease on success. Returns nil when the request must
// wait. Must hold lock.
func (p *Pool) tryGrantLocked(q *queuedRequest) *Lease {
	now := p.now()
	acctKey := q.Executor + "/" + q.Account

	// Rule 1: account ok and not cooling down. Unknown accounts are treated as ok.
	if acct, exists := p.accounts[acctKey]; exists {
		if !acct.ok || now.Before(acct.cooldownUntil) {
			return nil
		}
	}

	// Rule 2: global and per-account concurrency.
	if len(p.leases) >= p.globalConcurrency {
		return nil
	}
	if limit, exists := p.accountLimits[acctKey]; exists && p.countAccountInflight(q.Executor, q.Account) >= limit {
		return nil
	}

	// Rule 3: candidate workers.
	candidates := p.findCandidates(q.Executor, q.Account)

	// Rule 4: session affinity.
	workerID := ""
	sessionPolicy := SessionFresh
	if q.PreferWorker != "" {
		for _, c := range candidates {
			if c.ID == q.PreferWorker {
				workerID = c.ID
				sessionPolicy = SessionReuse
				break
			}
		}
		if workerID == "" && p.isAliveLocked(q.PreferWorker, q.Executor, q.Account, now) {
			// Preferred worker exists but is full: keep waiting for it until
			// the affinity wait expires, then relax to any candidate.
			wait := q.AffinityWait
			if wait <= 0 {
				wait = DefaultAffinityWait
			}
			if now.Sub(q.EnqueuedAt) < wait {
				return nil
			}
		}
	}

	// Rule 5: round-robin among candidates.
	if workerID == "" {
		workerID = p.selectWorker(candidates)
		if workerID == "" {
			return nil
		}
	}

	lease := &Lease{
		ID:               q.AttemptKey,
		WorkerID:         workerID,
		WaiterInstanceID: q.WaiterInstanceID,
		Executor:         q.Executor,
		Account:          q.Account,
		SessionPolicy:    sessionPolicy,
		AcquiredAt:       now,
	}
	p.leases[q.AttemptKey] = lease
	return lease
}

// isAliveLocked reports whether the worker is registered for the
// executor/account, has a fresh heartbeat and is not drained (capacity is
// not considered). Must hold lock.
func (p *Pool) isAliveLocked(workerID, executor, account string, now time.Time) bool {
	w, exists := p.workers[workerID]
	if !exists || w.Executor != executor || w.Account != account {
		return false
	}
	return !p.drained[workerID] && now.Sub(w.HeartbeatAt) <= heartbeatTTL
}

// findCandidates returns workers that are available for the given executor/account.
func (p *Pool) findCandidates(executor, account string) []core.WorkerRecord {
	now := p.now()
	var candidates []core.WorkerRecord
	for id, w := range p.workers {
		if !p.isAliveLocked(id, executor, account, now) {
			continue
		}
		// Check inflight < capacity
		if p.countInflight(id) >= w.Capacity {
			continue
		}
		candidates = append(candidates, w)
	}
	// Sort by ID for deterministic ordering
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ID < candidates[j].ID
	})
	return candidates
}

// selectWorker picks a worker from candidates round-robin.
func (p *Pool) selectWorker(candidates []core.WorkerRecord) string {
	if len(candidates) == 0 {
		return ""
	}
	key := candidates[0].Executor + "/" + candidates[0].Account
	cursor := p.cursors[key]
	selected := candidates[cursor%len(candidates)]
	p.cursors[key] = (cursor + 1) % len(candidates)
	return selected.ID
}

// countInflight counts active leases for a worker.
func (p *Pool) countInflight(workerID string) int {
	count := 0
	for _, lease := range p.leases {
		if lease.WorkerID == workerID {
			count++
		}
	}
	return count
}

// countAccountInflight counts active leases for an executor/account.
func (p *Pool) countAccountInflight(executor, account string) int {
	count := 0
	for _, lease := range p.leases {
		if lease.Executor == executor && lease.Account == account {
			count++
		}
	}
	return count
}

// ReclaimExpired reclaims leases that have exceeded the given TTL. It is only
// a safety net for leases whose goal never reached a terminal state; the
// reminder primarily reclaims leases of terminal goals via ReleaseWaiter.
func (p *Pool) ReclaimExpired(ttl time.Duration) []GrantedLease {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.now()
	reclaimed := false
	for key, lease := range p.leases {
		if now.Sub(lease.AcquiredAt) > ttl {
			delete(p.leases, key)
			reclaimed = true
		}
	}
	if reclaimed {
		return p.trySatisfyQueue()
	}
	return nil
}

// PoolState is the serializable state for actor persistence.
type PoolState struct {
	Accounts map[string]AccountStateJSON `json:"accounts,omitempty"`
	Leases   map[string]LeaseJSON        `json:"leases,omitempty"`
	Queue    []QueuedRequestJSON         `json:"queue,omitempty"`
	Cursors  map[string]int              `json:"cursors,omitempty"`
	Drained  []string                    `json:"drained,omitempty"`
}

type AccountStateJSON struct {
	OK            bool      `json:"ok"`
	CooldownUntil time.Time `json:"cooldownUntil"`
}

type LeaseJSON struct {
	ID               string    `json:"id"`
	WorkerID         string    `json:"workerID"`
	WaiterInstanceID string    `json:"waiterInstanceID,omitempty"`
	Executor         string    `json:"executor"`
	Account          string    `json:"account"`
	SessionPolicy    string    `json:"sessionPolicy"`
	AcquiredAt       time.Time `json:"acquiredAt"`
}

type QueuedRequestJSON struct {
	WaiterInstanceID string        `json:"waiterInstanceID"`
	AttemptKey       string        `json:"attemptKey"`
	Executor         string        `json:"executor"`
	Account          string        `json:"account"`
	PreferWorker     string        `json:"preferWorker"`
	AffinityWait     time.Duration `json:"affinityWait"`
	EnqueuedAt       time.Time     `json:"enqueuedAt"`
}

// SerializeState returns the current pool state as JSON-serializable struct.
func (p *Pool) SerializeState() PoolState {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := PoolState{
		Accounts: make(map[string]AccountStateJSON),
		Leases:   make(map[string]LeaseJSON),
		Cursors:  make(map[string]int, len(p.cursors)),
	}
	for key, c := range p.cursors {
		state.Cursors[key] = c
	}
	for key, acct := range p.accounts {
		state.Accounts[key] = AccountStateJSON{
			OK:            acct.ok,
			CooldownUntil: acct.cooldownUntil,
		}
	}
	for key, lease := range p.leases {
		state.Leases[key] = LeaseJSON{
			ID:               lease.ID,
			WorkerID:         lease.WorkerID,
			WaiterInstanceID: lease.WaiterInstanceID,
			Executor:         lease.Executor,
			Account:          lease.Account,
			SessionPolicy:    lease.SessionPolicy,
			AcquiredAt:       lease.AcquiredAt,
		}
	}
	for _, q := range p.queue {
		state.Queue = append(state.Queue, QueuedRequestJSON{
			WaiterInstanceID: q.WaiterInstanceID,
			AttemptKey:       q.AttemptKey,
			Executor:         q.Executor,
			Account:          q.Account,
			PreferWorker:     q.PreferWorker,
			AffinityWait:     q.AffinityWait,
			EnqueuedAt:       q.EnqueuedAt,
		})
	}
	for id := range p.drained {
		state.Drained = append(state.Drained, id)
	}
	sort.Strings(state.Drained)
	return state
}

// DeserializeState restores pool state from a serialized struct.
// Workers map is not persisted — it is set externally via SetWorkers.
// Limits are configuration, not state — they are set via the setters.
func (p *Pool) DeserializeState(s PoolState) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.accounts = make(map[string]*accountState)
	for key, acct := range s.Accounts {
		p.accounts[key] = &accountState{
			ok:            acct.OK,
			cooldownUntil: acct.CooldownUntil,
		}
	}
	p.leases = make(map[string]*Lease)
	for key, lj := range s.Leases {
		p.leases[key] = &Lease{
			ID:               lj.ID,
			WorkerID:         lj.WorkerID,
			WaiterInstanceID: lj.WaiterInstanceID,
			Executor:         lj.Executor,
			Account:          lj.Account,
			SessionPolicy:    lj.SessionPolicy,
			AcquiredAt:       lj.AcquiredAt,
		}
	}
	p.queue = nil
	for _, q := range s.Queue {
		p.queue = append(p.queue, &queuedRequest{
			WaiterInstanceID: q.WaiterInstanceID,
			AttemptKey:       q.AttemptKey,
			Executor:         q.Executor,
			Account:          q.Account,
			PreferWorker:     q.PreferWorker,
			AffinityWait:     q.AffinityWait,
			EnqueuedAt:       q.EnqueuedAt,
		})
	}
	p.cursors = make(map[string]int, len(s.Cursors))
	for key, c := range s.Cursors {
		p.cursors[key] = c
	}
	p.drained = make(map[string]bool)
	for _, id := range s.Drained {
		p.drained[id] = true
	}
}
