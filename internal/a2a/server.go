package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"sync"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// Worker 是 Worker 侧提供给 a2a.Server 的内部执行契约。
// Server 负责把 A2A 请求转成这些调用；具体执行器（pi/codex/fake）由 Worker 自行实现。
type Worker interface {
	// Submit 启动一次 Attempt，返回已提交的 Task 与执行句柄。
	// Task 必须携带 attemptKey metadata，且 ID/ContextID 与 info 一致，
	// 供去重、按 attemptKey 查询以及 SDK 的 Store 校验使用。
	Submit(ctx context.Context, meta Metadata, message *a2a.Message, info a2a.TaskInfo) (SubmitResult, error)
	// Get 按 remoteTaskID 查询 Task。
	Get(ctx context.Context, taskID a2a.TaskID) (*a2a.Task, error)
	// GetByAttemptKey 按 attemptKey 查询 Task；找不到返回 a2a.ErrTaskNotFound。
	GetByAttemptKey(ctx context.Context, attemptKey string) (*a2a.Task, error)
	// Cancel 请求取消一次 Attempt；最终状态以 Get 为准。
	Cancel(ctx context.Context, taskID a2a.TaskID) error
	// Subscribe 返回该 Task 的后续事件流（SSE 由 SDK 订阅机制消费）。
	Subscribe(ctx context.Context, taskID a2a.TaskID) iter.Seq2[a2a.Event, error]
}

// SubmitResult 是 Worker.Submit 的返回值。
type SubmitResult struct {
	// Task 是已提交（SUBMITTED）状态的 Task。
	Task *a2a.Task
	// Handle 是本次 Attempt 的执行句柄，用于流式获取后续事件与取消。
	Handle Handle
}

// Handle 是一次执行的句柄。调用方不得解析其内部状态。
type Handle interface {
	// Events 返回执行过程中的 A2A 事件（working / artifact / terminal）。
	Events() iter.Seq2[a2a.Event, error]
	// Cancel 请求取消本次执行；最终状态以 Worker.Get 为准。
	Cancel(ctx context.Context) error
}

// Server 把 A2A 请求适配成 Worker 内部接口调用，并组装 HTTP 端点。
type Server struct {
	worker Worker
	card   *a2a.AgentCard
	token  string
}

// NewServer 创建 A2A 服务端适配器。
func NewServer(worker Worker, card *a2a.AgentCard, token string) *Server {
	return &Server{worker: worker, card: card, token: token}
}

// Handler 返回组装好的 HTTP 处理器：
//   - POST /a2a                    A2A JSON-RPC（Bearer 保护）
//   - GET  /.well-known/agent-card.json（公开）
//   - GET  /mybot/attempts/{key}   my-bot 私有查询（Bearer 保护）
func (s *Server) Handler() http.Handler {
	exec := &executor{worker: s.worker}
	base := a2asrv.NewHandler(exec,
		a2asrv.WithCallInterceptors(authInterceptor{token: s.token}))
	handler := &dedupeHandler{RequestHandler: base, worker: s.worker}

	mux := http.NewServeMux()
	mux.Handle("/a2a", bearerMiddleware(s.token, a2asrv.NewJSONRPCHandler(handler)))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(s.card))
	mux.Handle("GET /mybot/attempts/{key}", bearerMiddleware(s.token, http.HandlerFunc(s.handleGetByAttemptKey)))
	return mux
}

// handleGetByAttemptKey 处理 GET /mybot/attempts/{key}，返回 a2a.Task JSON。
func (s *Server) handleGetByAttemptKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	task, err := s.worker.GetByAttemptKey(r.Context(), key)
	if err != nil {
		if errors.Is(err, a2a.ErrTaskNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(task)
}

// executor 实现 a2asrv.AgentExecutor，把 Execute/Cancel 转成 Worker 调用。
type executor struct {
	worker Worker
}

func (e *executor) Execute(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		meta, err := DecodeMetadata(ec.Metadata)
		if err != nil {
			yield(nil, err)
			return
		}
		res, err := e.worker.Submit(ctx, meta, ec.Message, ec.TaskInfo())
		if err != nil {
			yield(nil, err)
			return
		}
		// 先 yield 已提交的 Task，触发 SDK 的 Store.Create；
		// 再流式输出执行过程中的后续事件。
		if !yield(res.Task, nil) {
			return
		}
		for ev, err := range res.Handle.Events() {
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(ev, nil) {
				return
			}
		}
	}
}

func (e *executor) Cancel(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		if err := e.worker.Cancel(ctx, ec.TaskID); err != nil {
			yield(nil, err)
			return
		}
		yield(a2a.NewStatusUpdateEvent(ec, a2a.TaskStateCanceled, nil), nil)
	}
}

// dedupeHandler 包装 SDK RequestHandler，实现按 attemptKey 去重，
// 并在进入 SDK 前强制 configuration.returnImmediately=true。
type dedupeHandler struct {
	a2asrv.RequestHandler
	worker Worker
	locks  sync.Map // attemptKey -> *sync.Mutex（生产中由 Dapr first-write 承担）
}

func (h *dedupeHandler) SendMessage(ctx context.Context, req *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	key, _ := req.Metadata[MetaAttemptKey].(string)
	if key == "" {
		return nil, fmt.Errorf("%s required: %w", MetaAttemptKey, a2a.ErrInvalidParams)
	}

	mu, _ := h.locks.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	// 已存在则直接返回已有 Task（first-write 去重）。
	if task, err := h.worker.GetByAttemptKey(ctx, key); err == nil && task != nil {
		return task, nil
	} else if err != nil && !errors.Is(err, a2a.ErrTaskNotFound) {
		return nil, err
	}

	if req.Config == nil {
		req.Config = &a2a.SendMessageConfig{}
	}
	req.Config.ReturnImmediately = true
	return h.RequestHandler.SendMessage(ctx, req)
}

// authInterceptor 在服务端校验 Bearer token，并设置已认证用户。
type authInterceptor struct {
	a2asrv.PassthroughCallInterceptor
	token string
}

func (a authInterceptor) Before(ctx context.Context, cc *a2asrv.CallContext, req *a2asrv.Request) (context.Context, any, error) {
	if v, ok := cc.ServiceParams().Get("Authorization"); ok && len(v) > 0 && v[0] == "Bearer "+a.token {
		cc.User = a2asrv.NewAuthenticatedUser("mybot-runtime", nil)
		return ctx, nil, nil
	}
	return ctx, nil, a2a.ErrUnauthenticated
}

// bearerMiddleware 在 HTTP 层校验 Bearer token，缺失或错误时返回 401。
func bearerMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
