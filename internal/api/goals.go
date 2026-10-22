package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/workflow"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/orchestrator"
	"github.com/BenLocal/my-bot/internal/store"
)

const maxBodyBytes = 64 * 1024

type createGoalResponse struct {
	ID        string          `json:"id"`
	Status    core.GoalStatus `json:"status"`
	StatusURL string          `json:"statusUrl"`
}

// handleCreateGoal implements the creation flow of spec §8 / flow F1:
// freeze spec → idem first-write → spec + projection(queued) + goal.created +
// indexes → schedule workflow. Every step after the idem write is idempotent,
// so a duplicate request re-runs them and re-schedules a missing instance (G1).
func (s *Server) handleCreateGoal(w http.ResponseWriter, r *http.Request) {
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var spec core.GoalSpec
	if err := decodeJSONBody(w, r, &spec); err != nil {
		if isBodyTooLarge(err) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if err := s.freezeSpec(&spec); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	goalID := core.NewGoalID()
	existing, created, err := store.CreateIdem(r.Context(), s.store, idemKey, goalID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	status := http.StatusCreated
	if !created {
		goalID = existing
		status = http.StatusOK
		running, err := s.workflowExists(r, goalID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if running {
			proj, _ := store.LoadGoalProjection(r.Context(), s.store, goalID)
			writeJSON(w, status, createGoalResponse{ID: goalID, Status: proj.Status, StatusURL: "/v1/goals/" + goalID})
			return
		}
		// 崩溃窗口（flow G1）：idem 已写但实例不存在，用已固化的 spec（没有则用本次请求的）补启动。
		if saved, err := store.LoadGoalSpec(r.Context(), s.store, goalID); err == nil {
			spec = saved
		} else if !errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.logger.Warn("idempotent create: workflow instance missing, re-scheduling", "goalID", goalID)
	}

	if err := s.startGoal(r, goalID, spec, created); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, status, createGoalResponse{ID: goalID, Status: core.GoalQueued, StatusURL: "/v1/goals/" + goalID})
}

// startGoal writes the creation records and schedules the workflow. The
// initial projection is written only for a fresh create (flow G2).
func (s *Server) startGoal(r *http.Request, goalID string, spec core.GoalSpec, fresh bool) error {
	ctx := r.Context()
	if err := store.SaveGoalSpec(ctx, s.store, goalID, spec); err != nil {
		return fmt.Errorf("save spec: %w", err)
	}
	if _, err := store.LoadGoalProjection(ctx, s.store, goalID); fresh || errors.Is(err, store.ErrNotFound) {
		now := time.Now().UTC()
		proj := store.GoalProjection{
			GoalState: core.GoalState{
				ID:        goalID,
				Kind:      spec.Kind,
				Status:    core.GoalQueued,
				UpdatedAt: now,
				Budget:    core.Budget{TokensLimit: spec.Limits.MaxTokens},
			},
			EventSeq: 1,
		}
		if err := store.SaveEvent(ctx, s.store, goalID, core.Event{Seq: 1, Type: "goal.created", At: now, Data: map[string]any{"kind": spec.Kind, "strategy": spec.Strategy}}); err != nil {
			return fmt.Errorf("save goal.created: %w", err)
		}
		if err := store.SaveGoalProjection(ctx, s.store, goalID, proj); err != nil {
			return fmt.Errorf("save projection: %w", err)
		}
	}
	if err := store.AddGoalToIndexes(ctx, s.store, goalID); err != nil {
		return err
	}
	if s.wfClient == nil {
		return nil
	}
	_, err := s.wfClient.ScheduleWorkflow(ctx, "GoalWorkflow",
		workflow.WithInstanceID(goalID),
		workflow.WithInput(orchestrator.WorkflowState{GoalID: goalID, Spec: spec, EventSeq: 1}),
	)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("schedule workflow: %w", err)
	}
	s.logger.Info("goal workflow scheduled", "goalID", goalID)
	return nil
}

// workflowExists checks the instance before re-scheduling: scheduling an
// already completed instance ID would re-run the whole goal (spec §8).
func (s *Server) workflowExists(r *http.Request, goalID string) (bool, error) {
	if s.wfClient == nil {
		return true, nil
	}
	if _, err := s.wfClient.FetchWorkflowMetadata(r.Context(), goalID); err != nil {
		if errors.Is(err, api.ErrInstanceNotFound) || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "no such instance") {
			return false, nil
		}
		return false, fmt.Errorf("fetch workflow metadata: %w", err)
	}
	return true, nil
}

// freezeSpec validates the request and snapshots role definitions with their
// instruction text and sha256 (spec §8 创建流程).
func (s *Server) freezeSpec(spec *core.GoalSpec) error {
	if spec.Kind == "" {
		spec.Kind = core.KindGoal
	}
	switch spec.Kind {
	case core.KindGoal:
	case core.KindTask:
		spec.Limits.MaxRounds = 1
	default:
		return fmt.Errorf("kind %q is not supported yet", spec.Kind)
	}
	if strings.TrimSpace(spec.Objective) == "" {
		return errors.New("objective is required")
	}
	if spec.Strategy == "" {
		spec.Strategy = "single"
	}
	if spec.Strategy != "brain" && core.NewStrategy(spec.Strategy) == nil {
		return fmt.Errorf("unknown strategy %q", spec.Strategy)
	}
	cfg := s.roles
	if spec.Strategy == "brain" && spec.Brain == "" {
		spec.Brain = cfg.DefaultBrain
	}
	if spec.Strategy == "single" && len(spec.Members) == 0 && spec.Brain == "" {
		return errors.New("single strategy needs members[0] or brain")
	}
	if spec.Strategy == "brain" {
		if spec.Brain == "" {
			return errors.New("brain strategy needs brain (no defaultBrain configured)")
		}
		if rc, ok := cfg.Roles[spec.Brain]; !ok || !rc.HasCapability("brain") {
			return fmt.Errorf("role %q cannot be a brain (needs capability brain)", spec.Brain)
		}
		if len(spec.Members) == 0 {
			return errors.New("brain strategy needs members")
		}
	}
	if spec.MaxParallel <= 0 {
		spec.MaxParallel = cfg.Limits.Goal.MaxParallel
	}
	if spec.Limits.MaxStepsPerRound <= 0 {
		spec.Limits.MaxStepsPerRound = cfg.Limits.Goal.MaxStepsPerRound
	}

	names := append([]string(nil), spec.Members...)
	if spec.Brain != "" {
		names = append(names, spec.Brain)
	}
	spec.Roles = make(map[string]core.Role, len(names))
	for _, name := range names {
		rc, ok := cfg.Roles[name]
		if !ok {
			return fmt.Errorf("unknown role %q", name)
		}
		role := core.Role{Name: name, Executor: rc.Executor, Account: rc.Account, Instructions: rc.Instructions, Timeout: rc.Timeout, Capabilities: rc.Capabilities}
		if rc.Instructions != "" {
			text, err := os.ReadFile(filepath.Join(s.rolesDir, rc.Instructions))
			if err != nil {
				return fmt.Errorf("role %q instructions: %w", name, err)
			}
			role.InstructionsText = string(text)
			role.InstructionsSha256 = core.HashSHA256(text)
		}
		spec.Roles[name] = role
	}
	return nil
}

func (s *Server) handleListGoals(w http.ResponseWriter, r *http.Request) {
	activeOnly := r.URL.Query().Get("status") != "all"
	kind := core.Kind(r.URL.Query().Get("kind"))
	ids, err := store.ListGoalIDs(r.Context(), s.store, activeOnly)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	projections := make([]store.GoalProjection, 0, len(ids))
	for _, id := range ids {
		proj, err := store.LoadGoalProjection(r.Context(), s.store, id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if kind != "" && proj.Kind != kind {
			continue
		}
		projections = append(projections, proj)
	}
	writeJSON(w, http.StatusOK, projections)
}

func (s *Server) handleGetGoal(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	proj, err := store.LoadGoalProjection(r.Context(), s.store, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "goal not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, proj)
}

func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	goalID := r.PathValue("id")

	after := 0
	if raw := r.URL.Query().Get("after"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid after parameter")
			return
		}
		after = n
	}

	proj, err := store.LoadGoalProjection(r.Context(), s.store, goalID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "goal not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	events := make([]core.Event, 0)
	for seq := after + 1; seq <= proj.EventSeq; seq++ {
		ev, err := store.LoadEvent(r.Context(), s.store, goalID, seq)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, ev)
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) handleEventStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	goalID := r.PathValue("id")
	after, ok := parseAfterParam(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid after parameter")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	lastSeq := after
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		proj, err := store.LoadGoalProjection(ctx, s.store, goalID)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			s.logger.Warn("event stream: load projection", "goalID", goalID, "err", err)
			return
		}
		if err == nil && proj.EventSeq > lastSeq {
			for seq := lastSeq + 1; seq <= proj.EventSeq; seq++ {
				ev, lerr := store.LoadEvent(ctx, s.store, goalID, seq)
				if lerr != nil {
					if !errors.Is(lerr, store.ErrNotFound) {
						s.logger.Warn("event stream: load event", "goalID", goalID, "seq", seq, "err", lerr)
						return
					}
					continue
				}
				if werr := writeSSEEvent(w, flusher, goalID, ev); werr != nil {
					return
				}
				lastSeq = seq
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) handlePauseGoal(w http.ResponseWriter, r *http.Request) {
	s.controlGoal(w, r, "pause")
}

func (s *Server) handleResumeGoal(w http.ResponseWriter, r *http.Request) {
	s.controlGoal(w, r, "resume")
}

func (s *Server) handleCancelGoal(w http.ResponseWriter, r *http.Request) {
	proj, ok := s.loadActiveGoal(w, r)
	if !ok {
		return
	}
	goalID := proj.ID
	// queued 时 Workflow 可能尚未启动、收不到事件：直接把投影改为 cancelled，
	// Workflow 起来后读到 cancel 信号会走取消流程且不会覆盖终态（Project 不覆盖终态）。
	if proj.Status == core.GoalQueued {
		proj.Status = core.GoalCancelled
		proj.Reason = string(core.StopCancelled)
		proj.UpdatedAt = time.Now().UTC()
		if err := store.SaveGoalProjection(r.Context(), s.store, goalID, proj); err != nil {
			s.logger.Warn("cancel goal: direct projection update failed", "goalID", goalID, "error", err)
		}
	}
	if err := s.raiseSignal(r, goalID, orchestrator.Signal{Kind: orchestrator.SignalControl, Data: mustJSON(orchestrator.ControlSignal{Action: "cancel"})}); err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.logger.Info("cancel goal requested", "goalID", goalID, "status", proj.Status)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// handleAppendGoal: {note, replyTo?}. With replyTo it answers the pending
// ask_user (409 when expired / already answered), spec §5.3.
func (s *Server) handleAppendGoal(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Note    string `json:"note"`
		ReplyTo string `json:"replyTo,omitempty"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		if isBodyTooLarge(err) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	proj, ok := s.loadActiveGoal(w, r)
	if !ok {
		return
	}

	sig := orchestrator.Signal{Kind: orchestrator.SignalControl, Data: mustJSON(orchestrator.ControlSignal{Action: "append", Note: req.Note})}
	if req.ReplyTo != "" {
		if proj.Ask == nil || proj.Ask.RequestID != req.ReplyTo || time.Now().After(proj.Ask.ExpiresAt) {
			writeError(w, http.StatusConflict, "ask_user request "+req.ReplyTo+" is not pending")
			return
		}
		sig = orchestrator.Signal{Kind: orchestrator.SignalUserInput, Key: req.ReplyTo, Data: mustJSON(orchestrator.UserInput{Note: req.Note})}
	}
	if err := s.raiseSignal(r, proj.ID, sig); err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.logger.Info("append note to goal", "goalID", proj.ID, "noteLen", len(req.Note), "replyTo", req.ReplyTo)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// controlGoal handles pause/resume: 404 unknown, 409 terminal, else raise a control signal.
func (s *Server) controlGoal(w http.ResponseWriter, r *http.Request, action string) {
	proj, ok := s.loadActiveGoal(w, r)
	if !ok {
		return
	}
	if err := s.raiseSignal(r, proj.ID, orchestrator.Signal{Kind: orchestrator.SignalControl, Data: mustJSON(orchestrator.ControlSignal{Action: action})}); err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.logger.Info("goal control requested", "goalID", proj.ID, "action", action)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// loadActiveGoal loads the projection, writing 404 / 409 (terminal, flow G12) on failure.
func (s *Server) loadActiveGoal(w http.ResponseWriter, r *http.Request) (store.GoalProjection, bool) {
	goalID := r.PathValue("id")
	proj, err := store.LoadGoalProjection(r.Context(), s.store, goalID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "goal not found")
			return proj, false
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return proj, false
	}
	if isTerminalStatus(proj.Status) {
		writeError(w, http.StatusConflict, "goal already in terminal state")
		return proj, false
	}
	proj.ID = goalID
	return proj, true
}

// raiseSignal sends a signal to the workflow instance (single event name, spec §5.2 inbox).
func (s *Server) raiseSignal(r *http.Request, goalID string, sig orchestrator.Signal) error {
	if s.wfClient == nil {
		return nil
	}
	if err := s.wfClient.RaiseEvent(r.Context(), goalID, "signal", workflow.WithEventPayload(sig)); err != nil {
		s.logger.Warn("raise signal failed", "goalID", goalID, "kind", sig.Kind, "error", err)
		return fmt.Errorf("raise signal: %w", err)
	}
	return nil
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	return json.NewDecoder(r.Body).Decode(v)
}

func isBodyTooLarge(err error) bool {
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr)
}

func parseAfterParam(r *http.Request) (int, bool) {
	after := 0
	if raw := r.URL.Query().Get("after"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return 0, false
		}
		after = n
	}
	if last := r.Header.Get("Last-Event-ID"); last != "" {
		if idx := strings.LastIndex(last, ":"); idx >= 0 {
			if n, err := strconv.Atoi(last[idx+1:]); err == nil {
				after = n
			}
		}
	}
	return after, true
}

func isTerminalStatus(st core.GoalStatus) bool {
	switch st {
	case core.GoalSucceeded, core.GoalFailed, core.GoalCancelled, core.GoalCompleted:
		return true
	}
	return false
}

// handleRoundDetail returns the summary for a specific round.
func (s *Server) handleRoundDetail(w http.ResponseWriter, r *http.Request) {
	goalID := r.PathValue("id")
	roundStr := r.PathValue("n")
	round, err := strconv.Atoi(roundStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid round number")
		return
	}

	rs, err := store.LoadRoundSummary(r.Context(), s.store, goalID, round)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "round not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rs)
}

// handleListRounds returns all round summaries for a goal.
func (s *Server) handleListRounds(w http.ResponseWriter, r *http.Request) {
	goalID := r.PathValue("id")
	rounds, err := store.ListRoundSummaries(r.Context(), s.store, goalID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rounds)
}

// handleArtifactDownload serves a single artifact file from the shared volume.
func (s *Server) handleArtifactDownload(w http.ResponseWriter, r *http.Request) {
	goalID := r.PathValue("id")
	roundStr := r.PathValue("round")
	stepID := r.PathValue("stepID")
	name := r.PathValue("name")

	round, err := strconv.Atoi(roundStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid round number")
		return
	}

	ref, err := store.LoadArtifact(r.Context(), s.store, goalID, round, stepID, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "artifact not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if ref.Sha256 == "" {
		writeError(w, http.StatusInternalServerError, "artifact has no sha256")
		return
	}

	if s.artifactDir == "" {
		writeError(w, http.StatusServiceUnavailable, "artifact directory not configured")
		return
	}
	// ref.Path 是 r<round>/<stepID>/<name>；共享卷布局 <root>/<goalID>/r<round>/<stepID>/<name>（spec §2）。
	data, err := os.ReadFile(filepath.Join(s.artifactDir, goalID, filepath.Clean("/"+ref.Path)))
	if err != nil {
		writeError(w, http.StatusNotFound, "artifact file not accessible")
		return
	}

	// Verify sha256 if the ref provides one.
	if computed := core.HashSHA256(data); computed != ref.Sha256 {
		writeError(w, http.StatusInternalServerError, "artifact sha256 mismatch")
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
