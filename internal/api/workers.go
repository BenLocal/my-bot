package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	daprclient "github.com/dapr/go-sdk/client"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/scheduler"
	"github.com/BenLocal/my-bot/internal/store"
)

// knownExecutors is the M0 approximation of the set of executors whose worker
// registry entries can be listed. The store contract only exposes per-executor
// registry indexes (registry:<executor>:_index), with no global executor index.
var knownExecutors = []string{"pi", "codex", "fake"}

type workersSnapshot struct {
	Workers   []core.WorkerRecord      `json:"workers"`
	Scheduler *scheduler.SchedulerView `json:"scheduler,omitempty"`
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	workers := make([]core.WorkerRecord, 0)
	for _, executor := range knownExecutors {
		var ids []string
		err := store.MutateIndex(r.Context(), s.store, registryIndexKey(executor), func(existing []string) ([]string, bool) {
			ids = existing
			return existing, false
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for _, id := range ids {
			rec, err := store.LoadRegistryRecord(r.Context(), s.store, executor, id)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					continue
				}
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			workers = append(workers, rec)
		}
	}
	out := workersSnapshot{Workers: workers}
	if s.daprCl != nil {
		resp, err := s.daprCl.InvokeActor(r.Context(), &daprclient.InvokeActorRequest{
			ActorType: scheduler.ActorTypeScheduler,
			ActorID:   scheduler.ActorIDScheduler,
			Method:    "Snapshot",
		})
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "scheduler snapshot: "+err.Error())
			return
		}
		var view scheduler.SchedulerView
		if err := json.Unmarshal(resp.Data, &view); err != nil {
			writeError(w, http.StatusInternalServerError, "decode scheduler snapshot: "+err.Error())
			return
		}
		out.Scheduler = &view
	}
	writeJSON(w, http.StatusOK, out)
}

// handleWorkerAction dispatches the worker control sub-routes. Go 1.22
// ServeMux wildcards must be full path segments, so ":drain"/":ok" cannot be
// part of the registered pattern; the suffix is parsed here instead.
func (s *Server) handleWorkerAction(w http.ResponseWriter, r *http.Request) {
	executor := r.PathValue("executor")
	rawID := r.PathValue("id")

	switch {
	case strings.HasSuffix(rawID, ":drain"):
		s.handleDrainWorker(w, r, executor, strings.TrimSuffix(rawID, ":drain"), true)
	case strings.HasSuffix(rawID, ":undrain"):
		s.handleDrainWorker(w, r, executor, strings.TrimSuffix(rawID, ":undrain"), false)
	case strings.HasSuffix(rawID, ":ok"):
		s.handleWorkerOK(w, r, executor, strings.TrimSuffix(rawID, ":ok"))
	default:
		writeError(w, http.StatusNotFound, "unknown worker action")
	}
}

func (s *Server) handleDrainWorker(w http.ResponseWriter, r *http.Request, executor, id string, on bool) {
	if s.daprCl != nil {
		req := scheduler.DrainReqActor{WorkerID: id, On: on}
		reqData, _ := json.Marshal(req)
		if _, err := s.daprCl.InvokeActor(r.Context(), &daprclient.InvokeActorRequest{
			ActorType: scheduler.ActorTypeScheduler,
			ActorID:   scheduler.ActorIDScheduler,
			Method:    "Drain",
			Data:      reqData,
		}); err != nil {
			s.logger.Warn("invoke Drain actor failed", "workerID", id, "error", err)
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	s.logger.Info("drain worker requested", "executor", executor, "workerID", id, "on", on)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func (s *Server) handleWorkerOK(w http.ResponseWriter, r *http.Request, executor, id string) {
	if s.daprCl != nil {
		req := scheduler.SetAccountStateReqActor{Account: executor + "/" + id, OK: true}
		reqData, _ := json.Marshal(req)
		if _, err := s.daprCl.InvokeActor(r.Context(), &daprclient.InvokeActorRequest{
			ActorType: scheduler.ActorTypeScheduler,
			ActorID:   scheduler.ActorIDScheduler,
			Method:    "SetAccountState",
			Data:      reqData,
		}); err != nil {
			s.logger.Warn("invoke SetAccountState actor failed", "workerID", id, "error", err)
		}
	}
	s.logger.Info("mark worker healthy", "executor", executor, "workerID", id)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// handleAccountAction dispatches account-level control actions (:ok).
func (s *Server) handleAccountAction(w http.ResponseWriter, r *http.Request) {
	executor := r.PathValue("executor")
	rawAccount := r.PathValue("account")

	switch {
	case strings.HasSuffix(rawAccount, ":ok"):
		s.handleAccountOK(w, r, executor, strings.TrimSuffix(rawAccount, ":ok"))
	default:
		writeError(w, http.StatusNotFound, "unknown account action")
	}
}

// handleAccountOK marks an account as healthy, clearing needs_auth.
func (s *Server) handleAccountOK(w http.ResponseWriter, r *http.Request, executor, account string) {
	if s.daprCl != nil {
		req := scheduler.SetAccountStateReqActor{Account: executor + "/" + account, OK: true}
		reqData, _ := json.Marshal(req)
		if _, err := s.daprCl.InvokeActor(r.Context(), &daprclient.InvokeActorRequest{
			ActorType: scheduler.ActorTypeScheduler,
			ActorID:   scheduler.ActorIDScheduler,
			Method:    "SetAccountState",
			Data:      reqData,
		}); err != nil {
			s.logger.Warn("invoke SetAccountState actor failed", "account", account, "error", err)
		}
	}
	s.logger.Info("mark account healthy", "executor", executor, "account", account)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func registryIndexKey(executor string) string {
	return "registry:" + executor + ":_index"
}
