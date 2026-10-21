package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
)

// Pool is the pure-logic scheduler pool (no Dapr dependency).
// It manages leases, FIFO queue, account state, and worker selection.
type Pool struct {
	mu sync.Mutex

	globalConcurrency int

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
}

type accountState struct {
	ok            bool
	cooldownUntil time.Time
}

type Lease struct {
	ID            string
	WorkerID      string
	Executor      string
	Account       string
	SessionPolicy string // reuse | fresh
	AcquiredAt    time.Time
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
		accounts:          make(map[string]*accountState),
		leases:            make(map[string]*Lease),
		workers:           make(map[string]core.WorkerRecord),
		cursors:           make(map[string]int),
		drained:           make(map[string]bool),
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
		p.accounts[key] = &accountState{}
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
// otherwise the request is queued.
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

	// Check account state
	acctKey := req.Executor + "/" + req.Account
	acct, exists := p.accounts[acctKey]
	if !exists || !acct.ok {
		p.queue = append(p.queue, &queuedRequest{
			WaiterInstanceID: req.WaiterInstanceID,
			AttemptKey:       req.AttemptKey,
			Executor:         req.Executor,
			Account:          req.Account,
			PreferWorker:     req.PreferWorker,
			AffinityWait:     req.AffinityWait,
			EnqueuedAt:       time.Now(),
		})
		return AcquireResp{Granted: false}
	}
	if time.Now().Before(acct.cooldownUntil) {
		p.queue = append(p.queue, &queuedRequest{
			WaiterInstanceID: req.WaiterInstanceID,
			AttemptKey:       req.AttemptKey,
			Executor:         req.Executor,
			Account:          req.Account,
			PreferWorker:     req.PreferWorker,
			AffinityWait:     req.AffinityWait,
			EnqueuedAt:       time.Now(),
		})
		return AcquireResp{Granted: false}
	}

	// Check global concurrency
	if len(p.leases) >= p.globalConcurrency {
		p.queue = append(p.queue, &queuedRequest{
			WaiterInstanceID: req.WaiterInstanceID,
			AttemptKey:       req.AttemptKey,
			Executor:         req.Executor,
			Account:          req.Account,
			PreferWorker:     req.PreferWorker,
			AffinityWait:     req.AffinityWait,
			EnqueuedAt:       time.Now(),
		})
		return AcquireResp{Granted: false}
	}

	// Find candidate workers
	candidates := p.findCandidates(req.Executor, req.Account, req.PreferWorker)
	if len(candidates) == 0 {
		p.queue = append(p.queue, &queuedRequest{
			WaiterInstanceID: req.WaiterInstanceID,
			AttemptKey:       req.AttemptKey,
			Executor:         req.Executor,
			Account:          req.Account,
			PreferWorker:     req.PreferWorker,
			AffinityWait:     req.AffinityWait,
			EnqueuedAt:       time.Now(),
		})
		return AcquireResp{Granted: false}
	}

	// Select worker
	workerID := p.selectWorker(candidates, req.PreferWorker)
	if workerID == "" {
		p.queue = append(p.queue, &queuedRequest{
			WaiterInstanceID: req.WaiterInstanceID,
			AttemptKey:       req.AttemptKey,
			Executor:         req.Executor,
			Account:          req.Account,
			PreferWorker:     req.PreferWorker,
			AffinityWait:     req.AffinityWait,
			EnqueuedAt:       time.Now(),
		})
		return AcquireResp{Granted: false}
	}

	sessionPolicy := "fresh"
	if workerID == req.PreferWorker && req.PreferWorker != "" {
		sessionPolicy = "reuse"
	}

	lease := &Lease{
		ID:            fmt.Sprintf("lease_%d", time.Now().UnixNano()),
		WorkerID:      workerID,
		Executor:      req.Executor,
		Account:       req.Account,
		SessionPolicy: sessionPolicy,
		AcquiredAt:    time.Now(),
	}
	p.leases[req.AttemptKey] = lease
	return AcquireResp{Granted: true, Lease: lease}
}

// Release releases a lease and tries to satisfy queued requests.
// Returns the leases that were granted from the queue so the caller can
// deliver slot-granted signals.
func (p *Pool) Release(attemptKey string) ([]GrantedLease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.leases[attemptKey]
	if !exists {
		return nil, fmt.Errorf("no lease for attempt key %s", attemptKey)
	}
	delete(p.leases, attemptKey)
	granted := p.trySatisfyQueue()
	return granted, nil
}

// Snapshot returns the current pool state for /workers.
type SchedulerView struct {
	ActiveLeases int
	Queued       int
	Accounts     map[string]string // account -> "ok" | "needs_auth" | "cooldown"
	Drained      []string
}

func (p *Pool) Snapshot() SchedulerView {
	p.mu.Lock()
	defer p.mu.Unlock()

	view := SchedulerView{
		ActiveLeases: len(p.leases),
		Queued:       len(p.queue),
		Accounts:     make(map[string]string),
	}
	for key, acct := range p.accounts {
		if !acct.ok {
			view.Accounts[key] = "needs_auth"
		} else if time.Now().Before(acct.cooldownUntil) {
			view.Accounts[key] = "cooldown"
		} else {
			view.Accounts[key] = "ok"
		}
	}
	for id := range p.drained {
		view.Drained = append(view.Drained, id)
	}
	sort.Strings(view.Drained)
	return view
}

// TrySatisfyQueue attempts to grant leases to queued requests.
// Returns the leases that were granted so the caller can deliver signals.
func (p *Pool) TrySatisfyQueue() []GrantedLease {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.trySatisfyQueue()
}

// trySatisfyQueue attempts to grant leases to queued requests. Must hold lock.
func (p *Pool) trySatisfyQueue() []GrantedLease {
	var remaining []*queuedRequest
	var granted []GrantedLease
	for _, q := range p.queue {
		resp := p.tryGrantLocked(q)
		if resp.Granted {
			granted = append(granted, GrantedLease{
				InstanceID: q.WaiterInstanceID,
				AttemptKey: q.AttemptKey,
				Lease:      resp.Lease,
			})
		} else {
			remaining = append(remaining, q)
		}
	}
	p.queue = remaining
	return granted
}

// tryGrantLocked tries to grant a single queued request. Must hold lock.
func (p *Pool) tryGrantLocked(q *queuedRequest) AcquireResp {
	// Check account state
	acctKey := q.Executor + "/" + q.Account
	acct, exists := p.accounts[acctKey]
	if !exists || !acct.ok || time.Now().Before(acct.cooldownUntil) {
		return AcquireResp{Granted: false}
	}

	// Check global concurrency
	if len(p.leases) >= p.globalConcurrency {
		return AcquireResp{Granted: false}
	}

	candidates := p.findCandidates(q.Executor, q.Account, q.PreferWorker)
	if len(candidates) == 0 {
		return AcquireResp{Granted: false}
	}

	workerID := p.selectWorker(candidates, q.PreferWorker)
	if workerID == "" {
		return AcquireResp{Granted: false}
	}

	sessionPolicy := "fresh"
	if workerID == q.PreferWorker && q.PreferWorker != "" {
		sessionPolicy = "reuse"
	}

	lease := &Lease{
		ID:            fmt.Sprintf("lease_%d", time.Now().UnixNano()),
		WorkerID:      workerID,
		Executor:      q.Executor,
		Account:       q.Account,
		SessionPolicy: sessionPolicy,
		AcquiredAt:    time.Now(),
	}
	p.leases[q.AttemptKey] = lease
	return AcquireResp{Granted: true, Lease: lease}
}

// findCandidates returns workers that are available for the given executor/account.
func (p *Pool) findCandidates(executor, account, preferWorker string) []core.WorkerRecord {
	var candidates []core.WorkerRecord
	for id, w := range p.workers {
		if w.Executor != executor || w.Account != account {
			continue
		}
		if p.drained[id] {
			continue
		}
		// Check heartbeat freshness (60s TTL)
		if time.Since(w.HeartbeatAt) > 60*time.Second {
			continue
		}
		// Check inflight < capacity
		inflight := p.countInflight(id)
		if inflight >= w.Capacity {
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

// selectWorker picks a worker from candidates.
func (p *Pool) selectWorker(candidates []core.WorkerRecord, preferWorker string) string {
	if len(candidates) == 0 {
		return ""
	}
	// Check if preferred worker is available
	if preferWorker != "" {
		for _, c := range candidates {
			if c.ID == preferWorker {
				return c.ID
			}
		}
	}
	// Round-robin
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

// ReclaimExpired reclaims leases that have exceeded the given TTL.
// This is called by the actor reminder to clean up stuck leases.
func (p *Pool) ReclaimExpired(ttl time.Duration) []GrantedLease {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	var expiredKeys []string
	for key, lease := range p.leases {
		if now.Sub(lease.AcquiredAt) > ttl {
			expiredKeys = append(expiredKeys, key)
		}
	}
	for _, key := range expiredKeys {
		delete(p.leases, key)
	}
	if len(expiredKeys) > 0 {
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
	ID            string    `json:"id"`
	WorkerID      string    `json:"workerID"`
	Executor      string    `json:"executor"`
	Account       string    `json:"account"`
	SessionPolicy string    `json:"sessionPolicy"`
	AcquiredAt    time.Time `json:"acquiredAt"`
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
		Cursors:  p.cursors,
	}
	for key, acct := range p.accounts {
		state.Accounts[key] = AccountStateJSON{
			OK:            acct.ok,
			CooldownUntil: acct.cooldownUntil,
		}
	}
	for key, lease := range p.leases {
		state.Leases[key] = LeaseJSON{
			ID:            lease.ID,
			WorkerID:      lease.WorkerID,
			Executor:      lease.Executor,
			Account:       lease.Account,
			SessionPolicy: lease.SessionPolicy,
			AcquiredAt:    lease.AcquiredAt,
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
			ID:            lj.ID,
			WorkerID:      lj.WorkerID,
			Executor:      lj.Executor,
			Account:       lj.Account,
			SessionPolicy: lj.SessionPolicy,
			AcquiredAt:    lj.AcquiredAt,
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
	p.cursors = s.Cursors
	if p.cursors == nil {
		p.cursors = make(map[string]int)
	}
	p.drained = make(map[string]bool)
	for _, id := range s.Drained {
		p.drained[id] = true
	}
}
