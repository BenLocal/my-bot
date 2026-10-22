package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/store"
	"github.com/dapr/go-sdk/actor"
)

// ActorTypeScheduler is the Dapr actor type name for the scheduler.
const ActorTypeScheduler = "Scheduler"

// ActorIDScheduler is the fixed actor ID for the singleton scheduler actor.
const ActorIDScheduler = "global"

// DefaultLeaseTTL is the safety-net TTL for reclaiming stuck leases. Leases of
// terminal goals are reclaimed via GoalStatusChecker; the TTL only covers
// leases whose goal never terminates, so it must exceed any role timeout.
const DefaultLeaseTTL = 6 * time.Hour

// ReminderName is the name of the periodic reminder.
const ReminderName = "scheduler-reminder"

// ReminderPeriod is the reminder period (1 minute).
const ReminderPeriod = time.Minute

// knownExecutors enumerates the executor registry indexes the actor can read.
// The store has no global executor index (docs/spec.md §6); M0 hardcodes the
// three supported executors.
var knownExecutors = []string{"pi", "codex", "fake"}

// SignalDeliverer delivers slot-granted signals to workflow instances.
type SignalDeliverer interface {
	DeliverSlotGranted(ctx context.Context, instanceID string, attemptKey string, lease *Lease) error
}

// WorkerProvider returns the current worker registry snapshot.
type WorkerProvider interface {
	Workers(ctx context.Context) (map[string]core.WorkerRecord, error)
}

// GoalStatusChecker reports whether a goal (waiter instance) is terminal, so
// the reminder can reclaim its leases and queued requests (docs/spec.md §5.1).
type GoalStatusChecker interface {
	GoalTerminal(ctx context.Context, goalID string) (bool, error)
}

// SchedulerActor is the Dapr actor wrapping the Pool.
type SchedulerActor struct {
	actor.ServerImplBaseCtx

	pool              *Pool
	globalConcurrency int
	accountLimits     map[string]int
	store             store.Store
	deliver           SignalDeliverer
	workers           WorkerProvider
	goals             GoalStatusChecker
	leaseTTL          time.Duration
	logger            *slog.Logger
}

// NewSchedulerActor creates a new scheduler actor. The pool is created lazily
// by ensurePool on first use so it can be restored from the actor state store
// on process restart.
func NewSchedulerActor(globalConcurrency int, leaseTTL time.Duration) *SchedulerActor {
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	return &SchedulerActor{
		globalConcurrency: globalConcurrency,
		leaseTTL:          leaseTTL,
		logger:            slog.Default(),
	}
}

// Type returns the actor type name.
func (a *SchedulerActor) Type() string {
	return ActorTypeScheduler
}

// SetStore sets the Dapr State store used to load the worker registry.
func (a *SchedulerActor) SetStore(s store.Store) {
	a.store = s
}

// SetLogger sets the actor logger.
func (a *SchedulerActor) SetLogger(l *slog.Logger) {
	a.logger = l
}

// SetSignalDeliverer sets the callback for delivering slot-granted signals.
func (a *SchedulerActor) SetSignalDeliverer(d SignalDeliverer) {
	a.deliver = d
}

// SetWorkerProvider sets the callback for getting the worker registry.
func (a *SchedulerActor) SetWorkerProvider(w WorkerProvider) {
	a.workers = w
}

// SetGoalStatusChecker sets the checker used by the reminder to reclaim
// leases of terminal goals.
func (a *SchedulerActor) SetGoalStatusChecker(c GoalStatusChecker) {
	a.goals = c
}

// SetGlobalConcurrency sets the global active lease limit.
func (a *SchedulerActor) SetGlobalConcurrency(n int) {
	a.globalConcurrency = n
	if a.pool != nil {
		a.pool.SetGlobalConcurrency(n)
	}
}

// SetAccountLimits sets per-account concurrency limits keyed by
// "executor/account"; accounts without an entry have no per-account limit.
func (a *SchedulerActor) SetAccountLimits(limits map[string]int) {
	a.accountLimits = make(map[string]int, len(limits))
	for k, v := range limits {
		a.accountLimits[k] = v
	}
	if a.pool != nil {
		a.pool.SetAccountLimits(a.accountLimits)
	}
}

// refreshWorkers loads the worker registry into the pool.
func (a *SchedulerActor) refreshWorkers(ctx context.Context) {
	if a.workers != nil {
		workers, err := a.workers.Workers(ctx)
		if err != nil {
			a.logWarn("refresh workers failed", "error", err)
			return
		}
		a.pool.SetWorkers(workers)
		return
	}
	if a.store == nil {
		return
	}
	a.pool.SetWorkers(a.loadWorkersFromStore(ctx))
}

// loadWorkersFromStore reads every worker registry record the actor can enumerate.
func (a *SchedulerActor) loadWorkersFromStore(ctx context.Context) map[string]core.WorkerRecord {
	workers := make(map[string]core.WorkerRecord)
	for _, executor := range knownExecutors {
		var ids []string
		if err := store.MutateIndex(ctx, a.store, registryIndexKey(executor), func(existing []string) ([]string, bool) {
			ids = existing
			return existing, false
		}); err != nil {
			a.logWarn("load registry index failed", "executor", executor, "error", err)
			continue
		}
		for _, id := range ids {
			rec, err := store.LoadRegistryRecord(ctx, a.store, executor, id)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					continue
				}
				a.logWarn("load registry record failed", "executor", executor, "workerID", id, "error", err)
				continue
			}
			workers[rec.ID] = rec
		}
	}
	return workers
}

// ensurePool loads pool state from the actor state store if not yet loaded.
func (a *SchedulerActor) ensurePool(ctx context.Context) {
	if a.pool != nil {
		return
	}
	p := NewPool(a.globalConcurrency)
	p.SetAccountLimits(a.accountLimits)
	a.pool = p
	sm := a.GetStateManager()
	if sm == nil {
		return
	}
	var state PoolState
	if err := sm.Get(ctx, "pool", &state); err != nil {
		return
	}
	p.DeserializeState(state)
}

// saveState serializes and saves the pool state.
func (a *SchedulerActor) saveState(ctx context.Context) {
	if a.pool == nil {
		return
	}
	sm := a.GetStateManager()
	if sm == nil {
		return
	}
	state := a.pool.SerializeState()
	_ = sm.Set(ctx, "pool", state)
}

// ---- Actor methods ----

// AcquireReqActor is the JSON request for the Acquire actor method.
type AcquireReqActor struct {
	WaiterInstanceID string `json:"waiterInstanceID"`
	AttemptKey       string `json:"attemptKey"`
	Executor         string `json:"executor"`
	Account          string `json:"account"`
	PreferWorker     string `json:"preferWorker"`
	AffinityWait     string `json:"affinityWait"` // duration string, e.g. "5m"
}

// AcquireRespActor is the JSON response for the Acquire actor method.
type AcquireRespActor struct {
	Granted bool            `json:"granted"`
	Lease   *LeaseActorJSON `json:"lease,omitempty"`
}

// LeaseActorJSON is the JSON representation of a lease.
type LeaseActorJSON struct {
	ID               string `json:"id"`
	WorkerID         string `json:"workerID"`
	WaiterInstanceID string `json:"waiterInstanceID,omitempty"`
	Executor         string `json:"executor"`
	Account          string `json:"account"`
	SessionPolicy    string `json:"sessionPolicy"`
	AcquiredAt       string `json:"acquiredAt"` // RFC3339
}

// Acquire is the Dapr actor method for acquiring a slot.
func (a *SchedulerActor) Acquire(ctx context.Context, req AcquireReqActor) (AcquireRespActor, error) {
	a.ensurePool(ctx)
	a.refreshWorkers(ctx)

	var affinityWait time.Duration
	if req.AffinityWait != "" {
		affinityWait, _ = time.ParseDuration(req.AffinityWait)
	}

	resp := a.pool.Acquire(AcquireReq{
		WaiterInstanceID: req.WaiterInstanceID,
		AttemptKey:       req.AttemptKey,
		Executor:         req.Executor,
		Account:          req.Account,
		PreferWorker:     req.PreferWorker,
		AffinityWait:     affinityWait,
	})

	out := AcquireRespActor{Granted: resp.Granted}
	if resp.Lease != nil {
		out.Lease = leaseToActorJSON(resp.Lease)
	}

	a.saveState(ctx)
	return out, nil
}

// ReleaseReqActor is the JSON request for the Release actor method.
type ReleaseReqActor struct {
	AttemptKey string `json:"attemptKey"`
}

// ReleaseRespActor is the JSON response for the Release actor method.
type ReleaseRespActor struct {
	GrantedLeases []GrantedLeaseActorJSON `json:"grantedLeases,omitempty"`
}

// GrantedLeaseActorJSON is the JSON representation of a granted lease.
type GrantedLeaseActorJSON struct {
	InstanceID string          `json:"instanceID"`
	AttemptKey string          `json:"attemptKey"`
	Lease      *LeaseActorJSON `json:"lease,omitempty"`
}

// Release is the Dapr actor method for releasing a lease or dropping a
// still-queued request. Idempotent: an unknown attempt key is a no-op.
func (a *SchedulerActor) Release(ctx context.Context, req ReleaseReqActor) (ReleaseRespActor, error) {
	a.ensurePool(ctx)
	a.refreshWorkers(ctx)

	granted, err := a.pool.Release(req.AttemptKey)
	if err != nil {
		return ReleaseRespActor{}, err
	}
	a.deliverGrants(ctx, granted)

	a.saveState(ctx)
	return grantsToActorJSON(granted), nil
}

// ReleaseWaiterReqActor is the JSON request for the ReleaseWaiter actor method.
type ReleaseWaiterReqActor struct {
	InstanceID string `json:"instanceID"`
}

// ReleaseWaiter is the Dapr actor method that releases every lease and queued
// request of a goal (used on cancel / finish).
func (a *SchedulerActor) ReleaseWaiter(ctx context.Context, req ReleaseWaiterReqActor) (ReleaseRespActor, error) {
	a.ensurePool(ctx)
	a.refreshWorkers(ctx)

	granted := a.pool.ReleaseWaiter(req.InstanceID)
	a.deliverGrants(ctx, granted)

	a.saveState(ctx)
	return grantsToActorJSON(granted), nil
}

// grantsToActorJSON converts granted leases to the Release response.
func grantsToActorJSON(granted []GrantedLease) ReleaseRespActor {
	out := ReleaseRespActor{}
	for _, g := range granted {
		out.GrantedLeases = append(out.GrantedLeases, GrantedLeaseActorJSON{
			InstanceID: g.InstanceID,
			AttemptKey: g.AttemptKey,
			Lease:      leaseToActorJSON(g.Lease),
		})
	}
	return out
}

// CooldownReqActor is the JSON request for the Cooldown actor method.
type CooldownReqActor struct {
	Account string `json:"account"`
	Until   string `json:"until"` // RFC3339
}

// Cooldown is the Dapr actor method for putting an account into cooldown.
func (a *SchedulerActor) Cooldown(ctx context.Context, req CooldownReqActor) error {
	a.ensurePool(ctx)

	until, err := time.Parse(time.RFC3339, req.Until)
	if err != nil {
		return fmt.Errorf("invalid until time: %w", err)
	}
	a.pool.Cooldown(req.Account, until)
	a.saveState(ctx)
	return nil
}

// SetAccountStateReqActor is the JSON request for the SetAccountState actor method.
type SetAccountStateReqActor struct {
	Account string `json:"account"`
	OK      bool   `json:"ok"`
}

// SetAccountState is the Dapr actor method for setting account status.
func (a *SchedulerActor) SetAccountState(ctx context.Context, req SetAccountStateReqActor) error {
	a.ensurePool(ctx)
	a.pool.SetAccountState(req.Account, req.OK)
	a.saveState(ctx)
	return nil
}

// DrainReqActor is the JSON request for the Drain actor method.
type DrainReqActor struct {
	WorkerID string `json:"workerID"`
	On       bool   `json:"on"`
}

// Drain is the Dapr actor method for marking a worker as drained.
func (a *SchedulerActor) Drain(ctx context.Context, req DrainReqActor) error {
	a.ensurePool(ctx)
	a.pool.Drain(req.WorkerID, req.On)
	a.saveState(ctx)
	return nil
}

// Snapshot is the Dapr actor method for returning the current pool state.
func (a *SchedulerActor) Snapshot(ctx context.Context) (SchedulerView, error) {
	a.ensurePool(ctx)
	a.refreshWorkers(ctx)
	return a.pool.Snapshot(), nil
}

// RaiseEventReq carries a slot-granted delivery request.
type RaiseEventReq struct {
	InstanceID string          `json:"instanceID"`
	AttemptKey string          `json:"attemptKey"`
	Lease      *LeaseActorJSON `json:"lease,omitempty"`
}

// RaiseEvent delivers a slot-granted signal for the given attempt key to the
// waiting workflow instance.
func (a *SchedulerActor) RaiseEvent(ctx context.Context, req RaiseEventReq) error {
	if a.deliver == nil {
		return errors.New("scheduler: signal deliverer not configured")
	}
	lease := leaseFromActorJSON(req.Lease)
	return a.deliver.DeliverSlotGranted(ctx, req.InstanceID, req.AttemptKey, lease)
}

// ReminderCall is the Dapr actor reminder callback.
// The period parameter is empty in Dapr's ReminderCall; we track it ourselves.
func (a *SchedulerActor) ReminderCall(name string, state []byte, dueTime string, period string) {
	ctx := context.Background()
	a.ensurePool(ctx)
	a.refreshWorkers(ctx)

	a.reclaimTerminalGoals(ctx)

	granted := a.pool.ReclaimExpired(a.leaseTTL)
	a.deliverGrants(ctx, granted)

	queueGranted := a.pool.TrySatisfyQueue()
	a.deliverGrants(ctx, queueGranted)

	a.saveState(ctx)
}

// reclaimTerminalGoals releases leases and queued requests of goals that are
// already terminal. Queued requests whose affinity wait expired are relaxed by
// the pool itself when the queue is retried.
func (a *SchedulerActor) reclaimTerminalGoals(ctx context.Context) {
	if a.goals == nil {
		return
	}
	var terminalIDs []string
	for _, id := range a.pool.WaiterInstanceIDs() {
		terminal, err := a.goals.GoalTerminal(ctx, id)
		if err != nil {
			a.logWarn("check goal status failed", "goalID", id, "error", err)
			continue
		}
		if terminal {
			terminalIDs = append(terminalIDs, id)
		}
	}
	if len(terminalIDs) > 0 {
		a.deliverGrants(ctx, a.pool.ReleaseWaiter(terminalIDs...))
	}
}

// deliverGrants sends slot-granted signals for newly granted queued requests.
func (a *SchedulerActor) deliverGrants(ctx context.Context, grants []GrantedLease) {
	for _, g := range grants {
		if a.deliver == nil {
			break
		}
		if err := a.deliver.DeliverSlotGranted(ctx, g.InstanceID, g.AttemptKey, g.Lease); err != nil {
			a.logWarn("deliver slot-granted failed", "instanceID", g.InstanceID, "attemptKey", g.AttemptKey, "error", err)
		}
	}
}

// leaseToActorJSON converts a Pool Lease to the actor JSON representation.
func leaseToActorJSON(l *Lease) *LeaseActorJSON {
	if l == nil {
		return nil
	}
	return &LeaseActorJSON{
		ID:               l.ID,
		WorkerID:         l.WorkerID,
		WaiterInstanceID: l.WaiterInstanceID,
		Executor:         l.Executor,
		Account:          l.Account,
		SessionPolicy:    l.SessionPolicy,
		AcquiredAt:       l.AcquiredAt.Format(time.RFC3339),
	}
}

// leaseFromActorJSON converts the actor JSON representation back to a Pool Lease.
func leaseFromActorJSON(l *LeaseActorJSON) *Lease {
	if l == nil {
		return nil
	}
	acquired, _ := time.Parse(time.RFC3339, l.AcquiredAt)
	return &Lease{
		ID:               l.ID,
		WorkerID:         l.WorkerID,
		WaiterInstanceID: l.WaiterInstanceID,
		Executor:         l.Executor,
		Account:          l.Account,
		SessionPolicy:    l.SessionPolicy,
		AcquiredAt:       acquired,
	}
}

// registryIndexKey mirrors store's unexported key builder for reading the
// per-executor worker registry index.
func registryIndexKey(executor string) string {
	return "registry:" + executor + ":_index"
}

// logWarn logs a warning without panicking when the logger is nil.
func (a *SchedulerActor) logWarn(msg string, args ...any) {
	if a.logger != nil {
		a.logger.Warn(msg, args...)
	}
}

// Factory returns a Dapr actor factory that constructs a SchedulerActor. Register
// it on the HTTP app channel (the only channel that supports actors):
//
//	svc.RegisterActorImplFactoryContext(scheduler.Factory(globalMax, leaseTTL))
//
// For the singleton pattern used by mybot-runtime, create the actor once, wire
// its dependencies via setters, and register a factory that returns that same
// instance:
//
//	actor := scheduler.NewSchedulerActor(globalMax, leaseTTL)
//	actor.SetStore(st)
//	actor.SetLogger(logger)
//	svc.RegisterActorImplFactoryContext(func() actor.ServerContext { return actor })
func Factory(globalConcurrency int, leaseTTL time.Duration) actor.FactoryContext {
	return func() actor.ServerContext {
		return NewSchedulerActor(globalConcurrency, leaseTTL)
	}
}

// Ensure interface compliance.
var (
	_ actor.ServerContext  = (*SchedulerActor)(nil)
	_ actor.ReminderCallee = (*SchedulerActor)(nil)
)
