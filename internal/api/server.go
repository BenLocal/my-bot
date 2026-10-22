// Package api implements the Runtime HTTP API over the store layer.
// It translates HTTP requests into store reads/writes and workflow/actor calls.
package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/dapr/durabletask-go/workflow"
	daprclient "github.com/dapr/go-sdk/client"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/store"
)

// Server exposes the Runtime HTTP API. It wraps a store and a logger and
// holds the bearer token used by the auth middleware. The workflow client
// and Dapr client are optional — when set, handlers can schedule workflows,
// raise control signals, and invoke actor methods.
type Server struct {
	store    store.Store
	logger   *slog.Logger
	token    atomic.Value // string
	wfClient *workflow.Client
	daprCl   daprclient.Client
	roles    core.RolesConfig
	rolesDir string // 指令文件路径的根目录

	artifactDir string // 共享卷产物根目录（只读挂载）
}

// SetArtifactDir sets the shared-volume artifact root used by artifact downloads.
func (s *Server) SetArtifactDir(dir string) {
	s.artifactDir = dir
}

// SetRoles sets the roles.yaml config used to freeze goal specs; rolesDir is
// the base directory that instruction paths are relative to.
func (s *Server) SetRoles(cfg core.RolesConfig, rolesDir string) {
	s.roles = cfg
	s.rolesDir = rolesDir
}

// NewServer creates an API server. A nil logger falls back to slog.Default.
func NewServer(s store.Store, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{store: s, logger: logger}
}

// SetToken configures the bearer token used by the auth middleware. An empty
// token means auth is disabled (development convenience).
func (s *Server) SetToken(token string) {
	s.token.Store(token)
}

// SetWorkflowClient sets the Dapr workflow client for scheduling and signals.
func (s *Server) SetWorkflowClient(c *workflow.Client) {
	s.wfClient = c
}

// SetDaprClient sets the Dapr client for invoking actor methods.
func (s *Server) SetDaprClient(c daprclient.Client) {
	s.daprCl = c
}

func (s *Server) tokenValue() string {
	v, _ := s.token.Load().(string)
	return v
}

// Handler returns the fully wired route handler. Health probes are public;
// every other route is wrapped by the bearer-token middleware. Routes are
// served both under /v1 (spec §8) and at the root (back-compat).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleHealth)

	auth := func(h http.HandlerFunc) http.Handler { return s.requireAuth(h) }
	mux.Handle("POST /goals", auth(s.handleCreateGoal))
	mux.Handle("POST /tasks", auth(s.handleCreateTask))
	mux.Handle("GET /goals", auth(s.handleListGoals))
	mux.Handle("GET /goals/{id}", auth(s.handleGetGoal))
	mux.Handle("GET /goals/{id}/events", auth(s.handleGetEvents))
	mux.Handle("GET /goals/{id}/events/stream", auth(s.handleEventStream))
	// spec 形式 POST /goals/{id}:pause；ServeMux 通配符必须是整段，后缀在 handleGoalAction 里解析。
	mux.Handle("POST /goals/{idAction}", auth(s.handleGoalAction))
	mux.Handle("POST /goals/{id}/pause", auth(s.handlePauseGoal))
	mux.Handle("POST /goals/{id}/resume", auth(s.handleResumeGoal))
	mux.Handle("POST /goals/{id}/cancel", auth(s.handleCancelGoal))
	mux.Handle("POST /goals/{id}/append", auth(s.handleAppendGoal))

	mux.Handle("GET /goals/{id}/rounds", auth(s.handleListRounds))
	mux.Handle("GET /goals/{id}/rounds/{n}", auth(s.handleRoundDetail))
	mux.Handle("GET /goals/{id}/artifacts/{round}/{stepID}/{name}", auth(s.handleArtifactDownload))

	mux.Handle("GET /workers", auth(s.handleListWorkers))
	mux.Handle("POST /workers/{id}", auth(s.handleWorkerAction))
	mux.Handle("POST /workers/{executor}/{id}", auth(s.handleWorkerAction))
	mux.Handle("POST /accounts/{executor}/{account}", auth(s.handleAccountAction))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			r2 := r.Clone(r.Context())
			r2.URL.Path = strings.TrimPrefix(r.URL.Path, "/v1")
			r2.URL.RawPath = ""
			mux.ServeHTTP(w, r2)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// handleGoalAction dispatches POST /goals/{id}:pause|:resume|:cancel|:append.
func (s *Server) handleGoalAction(w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("idAction")
	id, action, ok := strings.Cut(raw, ":")
	if !ok {
		writeError(w, http.StatusNotFound, "unknown goal action")
		return
	}
	r.SetPathValue("id", id)
	switch action {
	case "pause":
		s.handlePauseGoal(w, r)
	case "resume":
		s.handleResumeGoal(w, r)
	case "cancel":
		s.handleCancelGoal(w, r)
	case "append":
		s.handleAppendGoal(w, r)
	default:
		writeError(w, http.StatusNotFound, "unknown goal action")
	}
}

// handleCreateTask is POST /tasks: a goal with kind=task.
func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var raw map[string]any
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	raw["kind"] = string(core.KindTask)
	r.Body = io.NopCloser(bytes.NewReader(mustJSON(raw)))
	s.handleCreateGoal(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
