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
	"maps"
	"path"
	"slices"
	"strings"
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

	// indexMu 串行化本进程对 attempt 索引的修改：store.MutateIndex 当前是
	// 无条件写（见 store/index.go），同进程并发修改会互相覆盖。
	indexMu sync.Mutex

	draining atomic.Bool
	inflight sync.WaitGroup
}

type taskEntry struct {
	task       *a2ago.Task
	attemptKey string
	goalID     string
	round      int
	stepID     string
	role       string
	sessionID  string
	events     chan a2ago.Event
	artifacts  []core.ArtifactRef

	// cancelRequested 在 Cancel 调用 driver.Cancel 前置位，使「事件通道无终态
	// 关闭」被识别为取消而不是 driver_bug。
	cancelRequested bool
	// finalized 表示终态已被认领（首个调用 finish 者生效）。
	finalized bool
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

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(s *Service) {
		if l != nil {
			s.logger = l
		}
	}
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

// attemptIndexKey 是本 Worker 非终态 attempt 的索引键，供重启后 Recover 扫描。
func attemptIndexKey(workerID string) string {
	return "worker:" + workerID + ":attempts:_index"
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
	sessionID := sessionIDFor(meta)
	if s.store != nil {
		rec := store.AttemptRecord{
			SessionID: sessionID,
			Status:    core.AttemptSubmitting,
			GoalID:    meta.GoalID,
			Round:     meta.Round,
			StepID:    meta.StepID,
			Role:      meta.Role,
		}
		created, err := store.CreateAttempt(ctx, s.store, s.workerID, meta.AttemptKey, rec)
		if err != nil {
			s.mu.Unlock()
			return a2a.SubmitResult{}, fmt.Errorf("worker: create attempt: %w", err)
		}
		if !created {
			// Owner-matched retry ("write succeeded but response was lost"). The
			// record exists but this process has no live entry for it (e.g. after
			// a restart); callers reconcile via GetByAttemptKey instead.
			s.mu.Unlock()
			return a2a.SubmitResult{}, fmt.Errorf("worker: attempt %q already exists for worker %q", meta.AttemptKey, s.workerID)
		}
		// 先写记录、再进索引（§6）。索引写失败只影响重启后的 Recover，不阻断执行。
		if err := s.addToIndex(ctx, meta.AttemptKey); err != nil {
			s.logger.Error("worker: add attempt index", "error", err, "attemptKey", meta.AttemptKey)
		}
	}

	// Create a submitted task.
	task := a2ago.NewSubmittedTask(info, message)
	task.Metadata = a2a.EncodeMetadata(task.Metadata, meta)
	task.Metadata = a2a.EncodeResultMeta(task.Metadata, core.AttemptSubmitted, "", false, core.Usage{})

	sessionPolicy := executor.SessionPolicy(meta.SessionPolicy)
	if meta.SessionPolicy == "" {
		sessionPolicy = executor.SessionPolicyFresh
	}

	startReq := executor.StartReq{
		AttemptKey:    meta.AttemptKey,
		GoalID:        meta.GoalID,
		Round:         meta.Round,
		StepID:        meta.StepID,
		Role:          meta.Role,
		Instruction:   extractInstruction(message),
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
			GoalID:       meta.GoalID,
			Round:        meta.Round,
			StepID:       meta.StepID,
			Role:         meta.Role,
		}
		if err := store.SaveAttempt(ctx, s.store, s.workerID, meta.AttemptKey, rec); err != nil {
			s.mu.Unlock()
			return a2a.SubmitResult{}, fmt.Errorf("worker: save attempt: %w", err)
		}
	}

	handle, err := s.driver.Start(ctx, startReq)
	if err != nil {
		s.mu.Unlock()
		return a2a.SubmitResult{}, fmt.Errorf("worker: start driver: %w", err)
	}

	entry := &taskEntry{
		task:       task,
		attemptKey: meta.AttemptKey,
		goalID:     meta.GoalID,
		round:      meta.Round,
		stepID:     meta.StepID,
		role:       meta.Role,
		sessionID:  sessionID,
		events:     make(chan a2ago.Event, 64),
	}
	s.tasks[task.ID] = entry
	s.byKey[meta.AttemptKey] = entry
	s.handles[task.ID] = handle
	s.inflight.Add(1)
	s.mu.Unlock()

	// Execution outlives the request: consumeEvents detaches cancellation.
	go s.consumeEvents(ctx, entry, handle)

	return s.submitResult(entry), nil
}

func (s *Service) submitResult(entry *taskEntry) a2a.SubmitResult {
	s.mu.Lock()
	task := copyTask(entry.task)
	s.mu.Unlock()
	return a2a.SubmitResult{
		Task:   task,
		Handle: &fakeHandle{svc: s, taskID: task.ID},
	}
}

// Get returns a task by ID. Only attempts started by this process are known;
// after a restart callers use GetByAttemptKey.
func (s *Service) Get(ctx context.Context, taskID a2ago.TaskID) (*a2ago.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.tasks[taskID]
	if !ok {
		return nil, a2ago.ErrTaskNotFound
	}
	return copyTask(entry.task), nil
}

// GetByAttemptKey returns a task by attempt key, falling back to the stored
// attempt record when this process has no live entry (e.g. after a restart).
func (s *Service) GetByAttemptKey(ctx context.Context, attemptKey string) (*a2ago.Task, error) {
	s.mu.Lock()
	entry, ok := s.byKey[attemptKey]
	var task *a2ago.Task
	if ok {
		task = copyTask(entry.task)
	}
	s.mu.Unlock()
	if ok {
		return task, nil
	}
	if s.store == nil {
		return nil, a2ago.ErrTaskNotFound
	}
	rec, err := store.LoadAttempt(ctx, s.store, s.workerID, attemptKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, a2ago.ErrTaskNotFound
		}
		return nil, fmt.Errorf("worker: load attempt %q: %w", attemptKey, err)
	}
	return taskFromRecord(rec), nil
}

// Cancel cancels an attempt; the attempt ends as cancelled. Cancelling an
// attempt that already reached a terminal state is a no-op.
func (s *Service) Cancel(ctx context.Context, taskID a2ago.TaskID) error {
	s.mu.Lock()
	handle, ok := s.handles[taskID]
	entry := s.tasks[taskID]
	if !ok || entry == nil {
		s.mu.Unlock()
		return a2ago.ErrTaskNotFound
	}
	if entry.finalized {
		s.mu.Unlock()
		return nil
	}
	entry.cancelRequested = true
	s.mu.Unlock()

	if err := s.driver.Cancel(ctx, handle); err != nil {
		// 句柄可能已结束；取消语义仍以记录为准。
		s.logger.Warn("worker: driver cancel", "error", err, "taskID", taskID)
	}
	s.finish(context.WithoutCancel(ctx), entry, core.AttemptCancelled, "", false, "", core.Usage{})
	return nil
}

// Subscribe returns an event stream for a task.
func (s *Service) Subscribe(ctx context.Context, taskID a2ago.TaskID) iter.Seq2[a2ago.Event, error] {
	return func(yield func(a2ago.Event, error) bool) {
		s.streamEvents(ctx, taskID, true, yield)
	}
}

// streamEvents yields a task (when includeTask is true) followed by its
// subsequent events. It is shared by Subscribe and the a2a.Handle returned
// from Submit so both the direct API and HTTP streaming observe the same events.
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

	for ev := range entry.events {
		if !yield(ev, nil) {
			return
		}
	}
}

// broadcast forwards an event to the stream consumer; dropped if the buffer is
// full (streaming is display-only, §7.1).
func broadcast(entry *taskEntry, ev a2ago.Event) {
	select {
	case entry.events <- ev:
	default:
	}
}

// consumeEvents is the sole reader of the driver event channel. It updates the
// task, finalizes the attempt and closes the per-task event channel.
func (s *Service) consumeEvents(ctx context.Context, entry *taskEntry, handle executor.Handle) {
	defer s.inflight.Done()
	defer close(entry.events)

	// Terminal persistence and pub/sub must not be tied to the request-scoped
	// context, which is canceled once the HTTP response returns.
	termCtx := context.WithoutCancel(ctx)
	info := entry.task.TaskInfo()

	var terminalEv executor.Event
	terminalReceived := false
	for ev := range s.driver.Events(handle) {
		switch ev.Type {
		case executor.EventProgress:
			msg := a2ago.NewMessage(a2ago.MessageRoleAgent, a2ago.NewTextPart(ev.Text))
			up := a2ago.NewStatusUpdateEvent(info, a2ago.TaskStateWorking, msg)
			s.mu.Lock()
			if !entry.finalized {
				entry.task.Status.State = a2ago.TaskStateWorking
				entry.task.Metadata = cloneMeta(entry.task.Metadata)
				entry.task.Metadata[a2a.MetaAttemptStatus] = string(core.AttemptWorking)
			}
			s.mu.Unlock()
			up.Metadata = map[string]any{a2a.MetaAttemptStatus: string(core.AttemptWorking)}
			broadcast(entry, up)
		case executor.EventArtifact:
			for _, art := range ev.Artifacts {
				a2aArt := a2a.NewArtifact(entry.goalID, entry.round, entry.stepID, art.Name, art.Path, art.Sha256, art.Size)
				ref := core.ArtifactRef{
					Path:   a2a.ArtifactRelPath(entry.round, entry.stepID, art.Name),
					Sha256: art.Sha256,
					Size:   art.Size,
					Round:  entry.round,
				}
				s.mu.Lock()
				entry.task.Artifacts = append(entry.task.Artifacts, a2aArt)
				entry.artifacts = append(entry.artifacts, ref)
				s.mu.Unlock()
				ae := &a2ago.TaskArtifactUpdateEvent{
					TaskID:    info.TaskID,
					ContextID: info.ContextID,
					Artifact:  a2aArt,
					LastChunk: true,
				}
				broadcast(entry, ae)
			}
		case executor.EventTerminal:
			terminalEv = ev
			terminalReceived = true
		}
		if terminalReceived {
			break
		}
	}

	s.mu.Lock()
	cancelled := entry.cancelRequested
	s.mu.Unlock()

	switch {
	case terminalReceived:
		status := mapPhaseToAttemptStatus(terminalEv.Phase)
		if ev, ok := s.finish(termCtx, entry, status, terminalEv.Reason, terminalEv.Retryable, terminalEv.Text, terminalEv.Usage); ok {
			broadcast(entry, ev)
		}
	case cancelled:
		// SDK 的取消流程自己写入 CANCELED，这里不再推送事件。
		s.finish(termCtx, entry, core.AttemptCancelled, "", false, "", core.Usage{})
	default:
		// The driver closed the channel without a terminal event: driver bug (§7.2).
		if ev, ok := s.finish(termCtx, entry, core.AttemptFailed, "driver_bug", false,
			"driver bug: events closed without terminal event", core.Usage{}); ok {
			broadcast(entry, ev)
		}
	}
}

// finish claims the terminal state for an attempt (first caller wins), updates
// the in-memory task, persists the record, drops it from the attempt index and
// publishes attempt-events. It returns the terminal status-update event for the
// stream and whether this call performed the finalization.
func (s *Service) finish(ctx context.Context, entry *taskEntry, status core.AttemptStatus, reason string, retryable bool, text string, usage core.Usage) (a2ago.Event, bool) {
	state := attemptStatusToState(status)
	var msg *a2ago.Message
	if text != "" {
		msg = a2ago.NewMessage(a2ago.MessageRoleAgent, a2ago.NewTextPart(text))
	}

	s.mu.Lock()
	if entry.finalized {
		s.mu.Unlock()
		return nil, false
	}
	entry.finalized = true
	entry.task.Status = a2ago.TaskStatus{State: state, Message: msg}
	entry.task.Metadata = a2a.EncodeResultMeta(cloneMeta(entry.task.Metadata), status, reason, retryable, usage)
	artifacts := append([]core.ArtifactRef(nil), entry.artifacts...)
	info := entry.task.TaskInfo()
	s.mu.Unlock()

	ev := a2ago.NewStatusUpdateEvent(info, state, msg)
	ev.Metadata = a2a.EncodeResultMeta(nil, status, reason, retryable, usage)

	if s.store != nil {
		rec := store.AttemptRecord{
			SessionID:    entry.sessionID,
			RemoteTaskID: string(info.TaskID),
			Status:       status,
			Usage:        usage,
			Artifacts:    artifacts,
			GoalID:       entry.goalID,
			Round:        entry.round,
			StepID:       entry.stepID,
			Role:         entry.role,
			Reason:       reason,
			Retryable:    retryable,
			Text:         text,
		}
		if err := store.SaveAttempt(ctx, s.store, s.workerID, entry.attemptKey, rec); err != nil {
			s.logger.Error("worker: save terminal attempt record",
				"error", err, "attemptKey", entry.attemptKey, "taskID", info.TaskID)
		} else if isTerminalAttemptStatus(status) {
			if err := s.removeFromIndex(ctx, entry.attemptKey); err != nil {
				s.logger.Error("worker: remove attempt index", "error", err, "attemptKey", entry.attemptKey)
			}
		}
	}

	s.publishAttemptEvent(ctx, entry.attemptKey, entry.goalID, status)

	s.logger.Info("worker: attempt terminal",
		"attemptKey", entry.attemptKey,
		"taskID", info.TaskID,
		"status", status,
		"reason", reason,
		"retryable", retryable,
	)
	return ev, true
}

// addToIndex adds attemptKey to this worker's attempt index.
func (s *Service) addToIndex(ctx context.Context, attemptKey string) error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	return store.MutateIndex(ctx, s.store, attemptIndexKey(s.workerID), func(ids []string) ([]string, bool) {
		if slices.Contains(ids, attemptKey) {
			return ids, false
		}
		return append(ids, attemptKey), true
	})
}

// removeFromIndex removes attemptKey from this worker's attempt index.
func (s *Service) removeFromIndex(ctx context.Context, attemptKey string) error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	return store.MutateIndex(ctx, s.store, attemptIndexKey(s.workerID), func(ids []string) ([]string, bool) {
		i := slices.Index(ids, attemptKey)
		if i < 0 {
			return ids, false
		}
		return slices.Delete(ids, i, i+1), true
	})
}

// listIndex returns the attempt keys currently in this worker's index.
func (s *Service) listIndex(ctx context.Context) ([]string, error) {
	var keys []string
	err := store.MutateIndex(ctx, s.store, attemptIndexKey(s.workerID), func(ids []string) ([]string, bool) {
		keys = append([]string(nil), ids...)
		return ids, false
	})
	return keys, err
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

// taskFromRecord synthesizes a Task from a stored attempt record (used after a
// restart). The context ID and absolute artifact paths are not persisted and
// are absent.
func taskFromRecord(rec store.AttemptRecord) *a2ago.Task {
	meta := map[string]any{a2a.MetaAttemptKey: rec.Key}
	if rec.GoalID != "" {
		meta[a2a.MetaGoalID] = rec.GoalID
	}
	if rec.Round != 0 {
		meta[a2a.MetaRound] = rec.Round
	}
	if rec.StepID != "" {
		meta[a2a.MetaStepID] = rec.StepID
	}
	if rec.Role != "" {
		meta[a2a.MetaRole] = rec.Role
	}
	task := &a2ago.Task{
		ID:       a2ago.TaskID(rec.RemoteTaskID),
		Status:   a2ago.TaskStatus{State: attemptStatusToState(rec.Status)},
		Metadata: a2a.EncodeResultMeta(meta, rec.Status, rec.Reason, rec.Retryable, rec.Usage),
	}
	if rec.Text != "" {
		task.Status.Message = a2ago.NewMessage(a2ago.MessageRoleAgent, a2ago.NewTextPart(rec.Text))
	}
	for _, ref := range rec.Artifacts {
		uri := a2a.ArtifactURIScheme + rec.GoalID + "/" + ref.Path
		task.Artifacts = append(task.Artifacts, &a2ago.Artifact{
			ID:    a2ago.NewArtifactID(),
			Name:  path.Base(ref.Path),
			Parts: []*a2ago.Part{a2ago.NewTextPart(uri)},
			Metadata: map[string]any{
				a2a.MetaArtifactURI:    uri,
				a2a.MetaArtifactSha256: ref.Sha256,
				a2a.MetaArtifactSize:   ref.Size,
			},
		})
	}
	return task
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

// sessionIDFor derives the session ID for an attempt (§7.2 step 2): reuse →
// uuid5(goalID + ":" + role); fresh → uuid5(attemptKey).
func sessionIDFor(meta a2a.Metadata) string {
	if meta.SessionPolicy == a2a.SessionPolicyReuse {
		return core.SessionID(meta.GoalID, meta.Role)
	}
	return core.SessionIDFresh(meta.AttemptKey)
}

// extractInstruction joins all text parts of the message.
func extractInstruction(msg *a2ago.Message) string {
	if msg == nil {
		return ""
	}
	var texts []string
	for _, p := range msg.Parts {
		if p == nil {
			continue
		}
		if t := p.Text(); t != "" {
			texts = append(texts, t)
		}
	}
	return strings.Join(texts, "\n")
}

// attemptStatusToState maps an attempt status to an a2a task state. interrupted
// has no A2A equivalent and maps to FAILED; the precise status travels in
// MetaAttemptStatus.
func attemptStatusToState(status core.AttemptStatus) a2ago.TaskState {
	switch status {
	case core.AttemptSubmitting, core.AttemptSubmitted:
		return a2ago.TaskStateSubmitted
	case core.AttemptWorking:
		return a2ago.TaskStateWorking
	case core.AttemptCompleted:
		return a2ago.TaskStateCompleted
	case core.AttemptFailed, core.AttemptInterrupted:
		return a2ago.TaskStateFailed
	case core.AttemptRejected:
		return a2ago.TaskStateRejected
	case core.AttemptCancelled:
		return a2ago.TaskStateCanceled
	default:
		return a2ago.TaskStateUnspecified
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

// cloneMeta returns a shallow copy of a metadata map (never nil). Values stored
// in metadata are treated as immutable, so a top-level copy is enough.
func cloneMeta(m map[string]any) map[string]any {
	out := make(map[string]any, len(m)+4)
	maps.Copy(out, m)
	return out
}

// copyTask returns a copy of a task that is safe to hand out while the entry
// keeps being updated under s.mu.
func copyTask(t *a2ago.Task) *a2ago.Task {
	if t == nil {
		return nil
	}
	cp := *t
	if t.Status.Message != nil {
		msg := *t.Status.Message
		cp.Status.Message = &msg
	}
	if t.Metadata != nil {
		cp.Metadata = cloneMeta(t.Metadata)
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
