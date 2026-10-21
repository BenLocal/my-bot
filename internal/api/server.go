// Package api implements the Runtime HTTP API over the store layer.
// It translates HTTP requests into store reads/writes and workflow/actor calls.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/dapr/durabletask-go/workflow"
	daprclient "github.com/dapr/go-sdk/client"

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

// Handler returns the fully wired route handler. The health check is public;
// every other route is wrapped by the bearer-token middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)

	mux.Handle("POST /goals", s.requireAuth(http.HandlerFunc(s.handleCreateGoal)))
	mux.Handle("GET /goals", s.requireAuth(http.HandlerFunc(s.handleListGoals)))
	mux.Handle("GET /goals/{id}", s.requireAuth(http.HandlerFunc(s.handleGetGoal)))
	mux.Handle("GET /goals/{id}/events", s.requireAuth(http.HandlerFunc(s.handleGetEvents)))
	mux.Handle("GET /goals/{id}/events/stream", s.requireAuth(http.HandlerFunc(s.handleEventStream)))
	mux.Handle("POST /goals/{id}/pause", s.requireAuth(http.HandlerFunc(s.handlePauseGoal)))
	mux.Handle("POST /goals/{id}/resume", s.requireAuth(http.HandlerFunc(s.handleResumeGoal)))
	mux.Handle("POST /goals/{id}/cancel", s.requireAuth(http.HandlerFunc(s.handleCancelGoal)))
	mux.Handle("POST /goals/{id}/append", s.requireAuth(http.HandlerFunc(s.handleAppendGoal)))

	mux.Handle("GET /workers", s.requireAuth(http.HandlerFunc(s.handleListWorkers)))
	mux.Handle("POST /workers/{executor}/{id}", s.requireAuth(http.HandlerFunc(s.handleWorkerAction)))

	return mux
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
