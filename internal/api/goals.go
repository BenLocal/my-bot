package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/durabletask-go/workflow"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/orchestrator"
	"github.com/BenLocal/my-bot/internal/store"
)

const (
	maxBodyBytes       = 64 * 1024
	goalActiveIndexKey = "goal:_index:active"
)

type createGoalResponse struct {
	GoalID    string `json:"goalID"`
	StatusURL string `json:"statusUrl"`
}

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

	if spec.Kind == "" {
		spec.Kind = core.KindGoal
	}

	goalID := core.NewGoalID()
	existing, created, err := store.CreateIdem(r.Context(), s.store, idemKey, goalID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !created {
		writeJSON(w, http.StatusOK, createGoalResponse{
			GoalID:    existing,
			StatusURL: "/goals/" + existing,
		})
		return
	}

	proj := store.GoalProjection{
		GoalState: core.GoalState{
			ID:        goalID,
			Kind:      spec.Kind,
			Status:    core.GoalQueued,
			UpdatedAt: time.Now().UTC(),
			Budget:    core.Budget{TokensLimit: spec.Limits.MaxTokens},
		},
	}
	if err := store.SaveGoalProjection(r.Context(), s.store, goalID, proj); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := store.MutateIndex(r.Context(), s.store, goalActiveIndexKey, func(ids []string) ([]string, bool) {
		for _, id := range ids {
			if id == goalID {
				return ids, false
			}
		}
		return append(ids, goalID), true
	}); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Schedule the workflow instance.
	if s.wfClient != nil {
		wfState := orchestrator.WorkflowState{
			GoalID: goalID,
			Spec:   spec,
			Round:  0,
		}
		if _, err := s.wfClient.ScheduleWorkflow(r.Context(), "GoalWorkflow",
			workflow.WithInstanceID(goalID),
			workflow.WithInput(wfState),
		); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to schedule workflow: "+err.Error())
			return
		}
		s.logger.Info("goal workflow scheduled", "goalID", goalID)
	} else {
		s.logger.Info("scheduling goal workflow (no workflow client)", "goalID", goalID)
	}

	writeJSON(w, http.StatusCreated, createGoalResponse{
		GoalID:    goalID,
		StatusURL: "/goals/" + goalID,
	})
}

func (s *Server) handleListGoals(w http.ResponseWriter, r *http.Request) {
	var ids []string
	err := store.MutateIndex(r.Context(), s.store, goalActiveIndexKey, func(existing []string) ([]string, bool) {
		ids = existing
		return existing, false
	})
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
	goalID := r.PathValue("id")
	proj, err := store.LoadGoalProjection(r.Context(), s.store, goalID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "goal not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if isTerminalStatus(proj.Status) {
		writeError(w, http.StatusConflict, "goal already in terminal state")
		return
	}

	// If the goal is still queued, update the projection directly so the
	// cancel takes effect without waiting for the workflow engine to start.
	// The control signal is still raised as a best-effort for the case where
	// the workflow has already started processing.
	if proj.Status == core.GoalQueued {
		cancelledProj := store.GoalProjection{
			GoalState: core.GoalState{
				ID:        goalID,
				Kind:      proj.Kind,
				Status:    core.GoalCancelled,
				UpdatedAt: time.Now().UTC(),
				Budget:    proj.Budget,
			},
			EventSeq: proj.EventSeq,
		}
		if err := store.SaveGoalProjection(r.Context(), s.store, goalID, cancelledProj); err != nil {
			s.logger.Warn("cancel goal: direct projection update failed", "goalID", goalID, "error", err)
		}
	}
	s.raiseControlSignal(r, goalID, "cancel", "")
	s.logger.Info("cancel goal requested", "goalID", goalID, "status", proj.Status)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func (s *Server) handleAppendGoal(w http.ResponseWriter, r *http.Request) {
	goalID := r.PathValue("id")
	if _, err := store.LoadGoalProjection(r.Context(), s.store, goalID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "goal not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var req struct {
		Note string `json:"note"`
	}
	if err := decodeJSONBody(w, r, &req); err != nil {
		if isBodyTooLarge(err) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	s.raiseControlSignal(r, goalID, "append", req.Note)
	s.logger.Info("append note to goal", "goalID", goalID, "noteLen", len(req.Note))
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// controlGoal handles pause/resume: verify the goal exists, then raise a control
// signal to the workflow.
func (s *Server) controlGoal(w http.ResponseWriter, r *http.Request, action string) {
	goalID := r.PathValue("id")
	if _, err := store.LoadGoalProjection(r.Context(), s.store, goalID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "goal not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.raiseControlSignal(r, goalID, action, "")
	s.logger.Info("goal control requested", "goalID", goalID, "action", action)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// raiseControlSignal sends a control signal to the workflow instance.
func (s *Server) raiseControlSignal(r *http.Request, goalID, action, note string) {
	if s.wfClient == nil {
		return
	}
	ctrl := orchestrator.ControlSignal{Action: action, Note: note}
	ctrlData, _ := json.Marshal(ctrl)
	signal := orchestrator.Signal{
		Kind: orchestrator.SignalControl,
		Data: ctrlData,
	}
	payload, _ := json.Marshal(signal)
	if err := s.wfClient.RaiseEvent(r.Context(), goalID, "signal", workflow.WithEventPayload(payload)); err != nil {
		s.logger.Warn("raise control signal failed", "goalID", goalID, "action", action, "error", err)
	}
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
