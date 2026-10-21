// Package worker implements the a2a.Worker interface using a fake driver
// for local development and end-to-end testing.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	a2ago "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/BenLocal/my-bot/internal/a2a"
	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/executor"
	"github.com/BenLocal/my-bot/internal/executor/fake"
	"github.com/BenLocal/my-bot/internal/store"

	daprclient "github.com/dapr/go-sdk/client"
	"github.com/google/uuid"
)

// attemptEventsTopic is the pub/sub topic for terminal attempt notifications
// (docs/spec.md §5.5).
const attemptEventsTopic = "attempt-events"

// PubSubPublisher is the minimal pub/sub interface used by the worker.
type PubSubPublisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
}

// attemptEvent is the terminal-attempt notification payload (docs/spec.md §5.5).
type attemptEvent struct {
	AttemptKey string    `json:"attemptKey"`
	GoalID     string    `json:"goalID"`
	Status     string    `json:"status"`
	At         time.Time `json:"at"`
}

// Service implements a2a.Worker using a fake driver.
type Service struct {
	mu       sync.Mutex
	driver   *fake.Fake
	tasks    map[a2ago.TaskID]*taskEntry
	byKey    map[string]*taskEntry
	handles  map[a2ago.TaskID]executor.Handle
	store    store.Store
	workerID string
	pubsub   PubSubPublisher
	logger   *slog.Logger

	draining atomic.Bool
	inflight sync.WaitGroup
}

type taskEntry struct {
	task       *a2ago.Task
	attemptKey string
	goalID     string
	sessionID  string
	events     chan executor.Event
}

// Option configures a Service.
type Option func(*Service)

// WithFakeOptions applies fake driver options.
func WithFakeOptions(opts ...fake.Option) Option {
	return func(s *Service) { s.driver = fake.New(opts...) }
}

// WithStore sets the storage backend.
func WithStore(st store.Store) Option {
	return func(s *Service) { s.store = st }
}

// WithWorkerID sets the worker instance ID.
func WithWorkerID(id string) Option {
	return func(s *Service) { s.workerID = id }
}

// WithPubSub sets the pub/sub publisher.
func WithPubSub(p PubSubPublisher) Option {
	return func(s *Service) { s.pubsub = p }
}

// NewService creates a new worker service with a fake driver.
func NewService(opts ...Option) *Service {
	s := &Service{
		driver:  fake.New(),
		tasks:   make(map[a2ago.TaskID]*taskEntry),
		byKey:   make(map[string]*taskEntry),
		handles: make(map[a2ago.TaskID]executor.Handle),
		logger:  slog.Default(),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// SetDriver replaces the fake driver (for testing).
func (s *Service) SetDriver(d *fake.Fake) {
	s.mu.Lock()
	s.driver = d
	s.mu.Unlock()
}

// Submit starts an attempt using the fake driver.
func (s *Service) Submit(ctx context.Context, meta a2a.Metadata, message *a2ago.Message, info a2ago.TaskInfo) (a2a.SubmitResult, error) {
	if s.draining.Load() {
		return a2a.SubmitResult{}, fmt.Errorf("worker: draining, not accepting new attempts")
	}

	s.mu.Lock()

	// Fast path: in-memory dedup for same-process retries (§7.2 step 1).
	if existing, ok := s.byKey[meta.AttemptKey]; ok {
		s.mu.Unlock()
		return s.submitResult(existing), nil
	}

	// Storage-backed first-write dedup (§7.2 step 1, §6). The session ID is
	// decided before execution and persisted with the record (§7.2 step 2).
	sessionID := s.sessionIDFor(meta)
	if s.store != nil {
		rec := store.AttemptRecord{
			SessionID: sessionID,
			Status:    core.AttemptSubmitting,
		}
		created, err := store.CreateAttempt(ctx, s.store, s.workerID, meta.AttemptKey, rec)
		if err != nil {
			s.mu.Unlock()
			return a2a.SubmitResult{}, fmt.Errorf("worker: create attempt: %w", err)
		}
		if !created {
			// Owner-matched retry ("write succeeded but response was lost").
			// The record is already in the store; return the in-memory task when
			// present, otherwise the full task body is no longer available.
			if _, lerr := store.LoadAttempt(ctx, s.store, s.workerID, meta.AttemptKey); lerr != nil {
				s.mu.Unlock()
				return a2a.SubmitResult{}, fmt.Errorf("worker: load existing attempt: %w", lerr)
			}
			if entry, ok := s.byKey[meta.AttemptKey]; ok {
				s.mu.Unlock()
				return s.submitResult(entry), nil
			}
			s.mu.Unlock()
			return a2a.SubmitResult{}, fmt.Errorf("worker: attempt %q already exists for worker %q", meta.AttemptKey, s.workerID)
		}
	}

	// Create a submitted task.
	task := a2ago.NewSubmittedTask(info, message)
	task.Metadata = a2a.EncodeMetadata(task.Metadata, meta)

	// Build the StartReq for the fake driver.
	instruction := extractInstruction(message)
	sessionPolicy := executor.SessionPolicy(meta.SessionPolicy)
	if meta.SessionPolicy == "" {
		sessionPolicy = executor.SessionPolicyFresh
	}

	startReq := executor.StartReq{
		AttemptKey:    meta.AttemptKey,
		GoalID:        meta.GoalID,
		Round:         meta.Round,
		StepID:        "step",
		Role:          "role",
		Instruction:   instruction,
		SessionPolicy: sessionPolicy,
		SessionID:     sessionID,
		Workspace:     meta.Workspace,
	}

	// Persist the task ID and session ID before starting execution (§7.2 step 2).
	if s.store != nil {
		rec := store.AttemptRecord{
			SessionID:    sessionID,
			RemoteTaskID: string(task.ID),
			Status:       core.AttemptSubmitted,
		}
		if err := store.SaveAttempt(ctx, s.store, s.workerID, meta.AttemptKey, rec); err != nil {
			s.mu.Unlock()
			return a2a.SubmitResult{}, fmt.Errorf("worker: save attempt: %w", err)
		}
	}

	// Start the fake driver.
	handle, err := s.driver.Start(ctx, startReq)
	if err != nil {
		s.mu.Unlock()
		return a2a.SubmitResult{}, fmt.Errorf("worker: start driver: %w", err)
	}

	// Store the task entry.
	entry := &taskEntry{
		task:       task,
		attemptKey: meta.AttemptKey,
		goalID:     meta.GoalID,
		sessionID:  sessionID,
		events:     make(chan executor.Event, 16),
	}
	s.tasks[task.ID] = entry
	s.byKey[meta.AttemptKey] = entry
	s.handles[task.ID] = handle
	s.inflight.Add(1)
	s.mu.Unlock()

	// Start a goroutine to consume driver events and update task status.
	go s.consumeEvents(ctx, task.ID, handle)

	return s.submitResult(entry), nil
}

func (s *Service) submitResult(entry *taskEntry) a2a.SubmitResult {
	s.mu.Lock()
	task := copyTask(entry.task)
	s.mu.Unlock()
	return a2a.SubmitResult{
		Task:   task,
		Handle: handleFromEntry(s, entry),
	}
}

// Get returns a task by ID.
func (s *Service) Get(ctx context.Context, taskID a2ago.TaskID) (*a2ago.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.tasks[taskID]
	if !ok {
		return nil, a2ago.ErrTaskNotFound
	}
	// Return a copy to avoid data races.
	return copyTask(entry.task), nil
}

// GetByAttemptKey returns a task by attempt key.
func (s *Service) GetByAttemptKey(ctx context.Context, attemptKey string) (*a2ago.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.byKey[attemptKey]
	if !ok {
		return nil, a2ago.ErrTaskNotFound
	}
	return copyTask(entry.task), nil
}

// Cancel cancels an attempt.
func (s *Service) Cancel(ctx context.Context, taskID a2ago.TaskID) error {
	s.mu.Lock()
	handle, ok := s.handles[taskID]
	entry, okEntry := s.tasks[taskID]
	s.mu.Unlock()

	if !ok {
		return a2ago.ErrTaskNotFound
	}

	if err := s.driver.Cancel(ctx, handle); err != nil {
		return fmt.Errorf("worker: cancel driver: %w", err)
	}

	if okEntry {
		s.mu.Lock()
		entry.task.Status.State = a2ago.TaskStateCanceled
		s.mu.Unlock()
	}

	return nil
}

// Subscribe returns an event stream for a task.
func (s *Service) Subscribe(ctx context.Context, taskID a2ago.TaskID) iter.Seq2[a2ago.Event, error] {
	return func(yield func(a2ago.Event, error) bool) {
		s.streamEvents(ctx, taskID, true, yield)
	}
}

// streamEvents yields a task (when includeTask is true) followed by its
// subsequent driver events. It is shared by Subscribe and the a2a.Handle
// returned from Submit so both the direct API and HTTP streaming observe the
// same events.
func (s *Service) streamEvents(ctx context.Context, taskID a2ago.TaskID, includeTask bool, yield func(a2ago.Event, error) bool) {
	s.mu.Lock()
	entry, ok := s.tasks[taskID]
	s.mu.Unlock()
	if !ok {
		yield(nil, a2ago.ErrTaskNotFound)
		return
	}

	if includeTask {
		task, err := s.Get(ctx, taskID)
		if err != nil {
			yield(nil, err)
			return
		}
		if !yield(task, nil) {
			return
		}
	}

	// Stream events from the per-task broadcast channel. The background
	// consumeEvents goroutine is the sole reader of the driver channel.
	for ev := range entry.events {
		a2aEvent, err := s.convertEvent(ctx, taskID, ev)
		if err != nil {
			yield(nil, err)
			return
		}
		if a2aEvent == nil {
			continue
		}
		if !yield(a2aEvent, nil) {
			return
		}
	}
}

// consumeEvents reads driver events and updates task status. It is the sole
// reader of the driver event channel; Subscribe consumes the broadcast channel
// attached to the task entry instead, so the two never race on the driver
// channel.
func (s *Service) consumeEvents(ctx context.Context, taskID a2ago.TaskID, handle executor.Handle) {
	defer s.inflight.Done()

	s.mu.Lock()
	entry := s.tasks[taskID]
	s.mu.Unlock()
	if entry == nil {
		return
	}

	// Terminal persistence and pub/sub must not be tied to the request-scoped
	// context, which is canceled once the HTTP response returns. Detach
	// cancellation while preserving values for the terminal write.
	termCtx := context.WithoutCancel(ctx)

	events := s.driver.Events(handle)
	var terminalEv executor.Event
	terminalReceived := false
	for ev := range events {
		// Broadcast to any SSE subscriber; drop if none is reading.
		select {
		case entry.events <- ev:
		default:
		}
		if ev.Type == executor.EventTerminal {
			terminalEv = ev
			terminalReceived = true
			break
		}
	}
	close(entry.events)

	if terminalReceived {
		s.handleTerminal(termCtx, taskID, terminalEv)
	} else {
		// The driver closed the channel without a terminal event: driver bug.
		s.handleDriverBug(termCtx, taskID)
	}
}

// handleTerminal persists the terminal record and publishes attempt-events.
func (s *Service) handleTerminal(ctx context.Context, taskID a2ago.TaskID, ev executor.Event) {
	phase := ev.Phase
	status := mapPhaseToAttemptStatus(phase)

	s.mu.Lock()
	entry := s.tasks[taskID]
	if entry != nil {
		entry.task.Status.State = mapPhaseToState(phase)
		if ev.Text != "" {
			entry.task.Status.Message = a2ago.NewMessage(a2ago.MessageRoleAgent,
				a2ago.NewTextPart(ev.Text))
		}
	}
	goalID, attemptKey, sessionID := "", "", ""
	if entry != nil {
		goalID, attemptKey, sessionID = entry.goalID, entry.attemptKey, entry.sessionID
	}
	s.mu.Unlock()

	if s.store != nil {
		rec := store.AttemptRecord{
			SessionID:    sessionID,
			RemoteTaskID: string(taskID),
			Status:       status,
			Usage:        ev.Usage,
		}
		if err := store.SaveAttempt(ctx, s.store, s.workerID, attemptKey, rec); err != nil {
			s.logger.Error("worker: save terminal attempt record",
				"error", err, "attemptKey", attemptKey, "taskID", taskID)
		}
	}

	s.publishAttemptEvent(ctx, attemptKey, goalID, status)

	s.logger.Info("worker: attempt terminal",
		"attemptKey", attemptKey,
		"taskID", taskID,
		"phase", phase,
		"retryable", ev.Retryable,
	)
}

// handleDriverBug marks an attempt failed when the driver closes events without
// a terminal event (docs/spec.md §7.2).
func (s *Service) handleDriverBug(ctx context.Context, taskID a2ago.TaskID) {
	s.mu.Lock()
	entry := s.tasks[taskID]
	alreadyTerminal := entry != nil && entry.task.Status.State.Terminal()
	if entry != nil && !alreadyTerminal {
		entry.task.Status.State = a2ago.TaskStateFailed
		entry.task.Status.Message = a2ago.NewMessage(a2ago.MessageRoleAgent,
			a2ago.NewTextPart("driver bug: events closed without terminal event"))
	}
	goalID, attemptKey, sessionID := "", "", ""
	if entry != nil {
		goalID, attemptKey, sessionID = entry.goalID, entry.attemptKey, entry.sessionID
	}
	s.mu.Unlock()

	if alreadyTerminal {
		return
	}

	if s.store != nil {
		rec := store.AttemptRecord{
			SessionID:    sessionID,
			RemoteTaskID: string(taskID),
			Status:       core.AttemptFailed,
		}
		if err := store.SaveAttempt(ctx, s.store, s.workerID, attemptKey, rec); err != nil {
			s.logger.Error("worker: save driver-bug attempt record",
				"error", err, "attemptKey", attemptKey, "taskID", taskID)
		}
	}

	s.publishAttemptEvent(ctx, attemptKey, goalID, core.AttemptFailed)

	s.logger.Warn("worker: attempt closed without terminal event",
		"attemptKey", attemptKey, "taskID", taskID)
}

// publishAttemptEvent publishes a terminal attempt-events notification.
func (s *Service) publishAttemptEvent(ctx context.Context, attemptKey, goalID string, status core.AttemptStatus) {
	if s.pubsub == nil {
		return
	}
	payload, err := json.Marshal(attemptEvent{
		AttemptKey: attemptKey,
		GoalID:     goalID,
		Status:     string(status),
		At:         time.Now().UTC(),
	})
	if err != nil {
		s.logger.Error("worker: marshal attempt event", "error", err, "attemptKey", attemptKey)
		return
	}
	if err := s.pubsub.Publish(ctx, attemptEventsTopic, payload); err != nil {
		s.logger.Error("worker: publish attempt-events", "error", err, "attemptKey", attemptKey)
	}
}

// convertEvent converts a driver event to an a2a event.
func (s *Service) convertEvent(ctx context.Context, taskID a2ago.TaskID, ev executor.Event) (a2ago.Event, error) {
	taskInfo := a2ago.TaskInfo{TaskID: taskID}

	switch ev.Type {
	case executor.EventProgress:
		msg := a2ago.NewMessage(a2ago.MessageRoleAgent, a2ago.NewTextPart(ev.Text))
		return a2ago.NewStatusUpdateEvent(taskInfo, a2ago.TaskStateWorking, msg), nil

	case executor.EventArtifact:
		parts := make([]*a2ago.Part, 0, len(ev.Artifacts))
		for _, art := range ev.Artifacts {
			parts = append(parts, a2ago.NewTextPart(art.Path))
		}
		return a2ago.NewArtifactEvent(taskInfo, parts...), nil

	case executor.EventTerminal:
		state := mapPhaseToState(ev.Phase)
		msg := a2ago.NewMessage(a2ago.MessageRoleAgent, a2ago.NewTextPart(ev.Text))
		return a2ago.NewStatusUpdateEvent(taskInfo, state, msg), nil

	default:
		return nil, nil
	}
}

// Drain stops accepting new attempts.
func (s *Service) Drain() {
	s.draining.Store(true)
}

// WaitInflight waits until all running attempts finish or the context is done.
func (s *Service) WaitInflight(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sessionIDFor derives the session ID for an attempt (§7.2 step 2). The fake
// driver does not truly manage sessions; reuse is simplified to an empty ID.
func (s *Service) sessionIDFor(meta a2a.Metadata) string {
	if meta.SessionPolicy == a2a.SessionPolicyReuse {
		return ""
	}
	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte(meta.AttemptKey)).String()
}

// extractInstruction extracts the instruction text from the message.
func extractInstruction(msg *a2ago.Message) string {
	if msg == nil || len(msg.Parts) == 0 {
		return ""
	}
	return msg.Parts[0].Text()
}

// mapPhaseToState maps an executor phase to an a2a task state.
func mapPhaseToState(phase executor.Phase) a2ago.TaskState {
	switch phase {
	case executor.PhaseCompleted:
		return a2ago.TaskStateCompleted
	case executor.PhaseFailed:
		return a2ago.TaskStateFailed
	case executor.PhasePaused:
		return a2ago.TaskStateInputRequired
	default:
		return a2ago.TaskStateWorking
	}
}

// mapPhaseToAttemptStatus maps an executor phase to a core.AttemptStatus.
func mapPhaseToAttemptStatus(phase executor.Phase) core.AttemptStatus {
	switch phase {
	case executor.PhaseCompleted:
		return core.AttemptCompleted
	case executor.PhaseFailed:
		return core.AttemptFailed
	case executor.PhaseInterrupted:
		return core.AttemptInterrupted
	case executor.PhasePaused:
		return core.AttemptRejected
	case executor.PhaseWorking:
		return core.AttemptWorking
	default:
		return core.AttemptUnknown
	}
}

// copyTask returns a shallow copy of a task to avoid data races.
func copyTask(t *a2ago.Task) *a2ago.Task {
	if t == nil {
		return nil
	}
	cp := *t
	if t.Status.Message != nil {
		msg := *t.Status.Message
		cp.Status.Message = &msg
	}
	cp.Artifacts = make([]*a2ago.Artifact, len(t.Artifacts))
	for i, art := range t.Artifacts {
		if art != nil {
			a := *art
			cp.Artifacts[i] = &a
		}
	}
	cp.History = make([]*a2ago.Message, len(t.History))
	for i, msg := range t.History {
		if msg != nil {
			m := *msg
			cp.History[i] = &m
		}
	}
	return &cp
}

// fakeHandle wraps a task ID and its service for the a2a.Handle interface.
type fakeHandle struct {
	svc    *Service
	taskID a2ago.TaskID
}

// Events returns the subsequent event stream for this handle. The initial task
// is not repeated: the A2A executor already yields it before calling Events.
func (h *fakeHandle) Events() iter.Seq2[a2ago.Event, error] {
	return func(yield func(a2ago.Event, error) bool) {
		if h.svc == nil {
			return
		}
		h.svc.streamEvents(context.Background(), h.taskID, false, yield)
	}
}

// Cancel cancels this handle's task.
func (h *fakeHandle) Cancel(ctx context.Context) error {
	if h.svc == nil {
		return nil
	}
	return h.svc.Cancel(ctx, h.taskID)
}

// handleFromEntry creates a Handle from a task entry.
func handleFromEntry(s *Service, entry *taskEntry) a2a.Handle {
	return &fakeHandle{svc: s, taskID: entry.task.ID}
}

// daprPublisher publishes through a Dapr sidecar.
type daprPublisher struct {
	client     daprclient.Client
	pubsubName string
}

// NewDaprPublisher creates a Dapr-backed pub/sub publisher.
func NewDaprPublisher(c daprclient.Client, pubsubName string) PubSubPublisher {
	return &daprPublisher{client: c, pubsubName: pubsubName}
}

func (p *daprPublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	if p.client == nil {
		return errors.New("worker: dapr publisher: nil client")
	}
	return p.client.PublishEvent(ctx, p.pubsubName, topic, payload,
		daprclient.PublishEventWithContentType("application/json"))
}

// noopPublisher logs but does not publish (local development without Dapr).
type noopPublisher struct {
	logger *slog.Logger
}

// NewNoopPublisher creates a no-op publisher for local development.
func NewNoopPublisher(logger *slog.Logger) PubSubPublisher {
	return &noopPublisher{logger: logger}
}

func (p *noopPublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	if p.logger != nil {
		p.logger.Debug("worker: noop publish", "topic", topic, "payload", string(payload))
	}
	return nil
}

// Ensure interface compliance.
var (
	_ a2a.Worker = (*Service)(nil)
)
