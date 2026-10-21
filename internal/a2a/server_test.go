package a2a

import (
	"context"
	"encoding/json"
	"iter"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

const testToken = "s3cret-internal-token"

// ---------------------------------------------------------------------------
// Fake Worker
// ---------------------------------------------------------------------------

type fakeHandle struct {
	events chan a2a.Event
	cancel context.CancelFunc
}

func (h *fakeHandle) Events() iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		for ev := range h.events {
			if !yield(ev, nil) {
				return
			}
		}
	}
}

func (h *fakeHandle) Cancel(ctx context.Context) error {
	h.cancel()
	return nil
}

type fakeWorker struct {
	mu      sync.Mutex
	tasks   map[a2a.TaskID]*a2a.Task
	byKey   map[string]a2a.TaskID
	handles map[a2a.TaskID]*fakeHandle
	delay   time.Duration
}

func newFakeWorker(delay time.Duration) *fakeWorker {
	return &fakeWorker{
		tasks:   map[a2a.TaskID]*a2a.Task{},
		byKey:   map[string]a2a.TaskID{},
		handles: map[a2a.TaskID]*fakeHandle{},
		delay:   delay,
	}
}

func (w *fakeWorker) Submit(ctx context.Context, meta Metadata, message *a2a.Message, info a2a.TaskInfo) (SubmitResult, error) {
	w.mu.Lock()
	task := &a2a.Task{
		ID:        info.TaskID,
		ContextID: info.ContextID,
		Status:    a2a.TaskStatus{State: a2a.TaskStateSubmitted},
		History:   []*a2a.Message{message},
	}
	task.SetMeta(MetaAttemptKey, meta.AttemptKey)
	task.SetMeta(MetaGoalID, meta.GoalID)
	if meta.Round != 0 {
		task.SetMeta(MetaRound, meta.Round)
	}
	w.tasks[task.ID] = task
	w.byKey[meta.AttemptKey] = task.ID

	events := make(chan a2a.Event)
	cctx, cancel := context.WithCancel(ctx)
	h := &fakeHandle{events: events, cancel: cancel}
	w.handles[task.ID] = h
	w.mu.Unlock()

	go w.run(cctx, h, task)
	return SubmitResult{Task: task, Handle: h}, nil
}

func (w *fakeWorker) run(ctx context.Context, h *fakeHandle, task *a2a.Task) {
	defer close(h.events)

	select {
	case <-ctx.Done():
		return
	case <-time.After(w.delay):
	}

	select {
	case h.events <- a2a.NewStatusUpdateEvent(task, a2a.TaskStateWorking, nil):
	case <-ctx.Done():
		return
	}

	art := a2a.NewArtifactEvent(task, a2a.NewTextPart("report"))
	art.Artifact.Name = "report.md"
	art.LastChunk = true
	select {
	case h.events <- art:
	case <-ctx.Done():
		return
	}

	select {
	case h.events <- a2a.NewStatusUpdateEvent(task, a2a.TaskStateCompleted, nil):
	case <-ctx.Done():
		return
	}
}

func (w *fakeWorker) Get(ctx context.Context, taskID a2a.TaskID) (*a2a.Task, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	t, ok := w.tasks[taskID]
	if !ok {
		return nil, a2a.ErrTaskNotFound
	}
	return t, nil
}

func (w *fakeWorker) GetByAttemptKey(ctx context.Context, attemptKey string) (*a2a.Task, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	id, ok := w.byKey[attemptKey]
	if !ok {
		return nil, a2a.ErrTaskNotFound
	}
	return w.tasks[id], nil
}

func (w *fakeWorker) Cancel(ctx context.Context, taskID a2a.TaskID) error {
	w.mu.Lock()
	h, ok := w.handles[taskID]
	w.mu.Unlock()
	if ok {
		h.cancel()
	}
	return nil
}

func (w *fakeWorker) Subscribe(ctx context.Context, taskID a2a.TaskID) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {}
}

// ---------------------------------------------------------------------------

func newAgentCard(baseURL string) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:                 "mybot-worker-pi",
		Description:          "my-bot worker (executor=pi)",
		Version:              "0.0.1",
		SupportedInterfaces:  []*a2a.AgentInterface{a2a.NewAgentInterface(baseURL+"/a2a", a2a.TransportProtocolJSONRPC)},
		Capabilities:         a2a.AgentCapabilities{Streaming: true},
		DefaultInputModes:    []string{"text/plain"},
		DefaultOutputModes:   []string{"text/plain"},
		Skills:               []a2a.AgentSkill{{ID: "pi", Name: "pi executor", Description: "runs pi", Tags: []string{"executor:pi"}}},
		SecuritySchemes:      a2a.NamedSecuritySchemes{"mybotInternal": a2a.HTTPAuthSecurityScheme{Scheme: "Bearer"}},
		SecurityRequirements: a2a.SecurityRequirementsOptions{{"mybotInternal": {}}},
	}
}

func newTestServer(t *testing.T, worker Worker) (*httptest.Server, *Client) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	baseURL := "http://" + ln.Addr().String()

	srv := httptest.NewUnstartedServer(NewServer(worker, newAgentCard(baseURL), testToken).Handler())
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)

	client, err := NewClient(context.Background(), srv.URL, testToken)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return srv, client
}

func submitReq(key string) *a2a.SendMessageRequest {
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("instruction"))
	req := &a2a.SendMessageRequest{Message: msg}
	req.SetMeta(MetaAttemptKey, key)
	req.SetMeta(MetaGoalID, "g1")
	req.SetMeta(MetaRound, 1)
	return req
}

func TestRoundTrip(t *testing.T) {
	worker := newFakeWorker(30 * time.Millisecond)
	_, client := newTestServer(t, worker)

	task, err := client.Submit(context.Background(), submitReq("g1/r1/s1/a1"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if task.Status.State == a2a.TaskStateCompleted {
		t.Fatalf("expected non-terminal submitted task, got %s", task.Status.State)
	}

	// GetByAttemptKey via plain HTTP.
	byKey, err := client.GetByAttemptKey(context.Background(), "g1/r1/s1/a1")
	if err != nil {
		t.Fatalf("GetByAttemptKey: %v", err)
	}
	if byKey.ID != task.ID {
		t.Fatalf("GetByAttemptKey returned %s, want %s", byKey.ID, task.ID)
	}

	// GetTask after completion.
	waitForCompleted(t, client, task.ID)

	got, err := client.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("Get state = %s, want COMPLETED", got.Status.State)
	}
	if len(got.Artifacts) != 1 || got.Artifacts[0].Name != "report.md" {
		t.Fatalf("expected one artifact report.md, got %+v", got.Artifacts)
	}
}

func waitForCompleted(t *testing.T, client *Client, taskID a2a.TaskID) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := client.Get(context.Background(), taskID)
		if err != nil {
			t.Fatalf("Get during wait: %v", err)
		}
		if got.Status.State == a2a.TaskStateCompleted {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not complete in time", taskID)
}

func TestMissingAttemptKeyRejected(t *testing.T) {
	worker := newFakeWorker(30 * time.Millisecond)
	srv, client := newTestServer(t, worker)

	req := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("x"))}
	_, err := client.Submit(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for missing attemptKey")
	}

	// Also verify the server rejects it at the JSON-RPC layer (dedup handler).
	body := `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"text":"x"}]}}}`
	httpReq, err := http.NewRequest("POST", srv.URL+"/a2a", strings.NewReader(body))
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+testToken)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request: %v", err)
	}
	defer resp.Body.Close()

	var rpcResp struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      any             `json:"id"`
		Error   json.RawMessage `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		t.Fatalf("decode JSON-RPC response: %v", err)
	}
	if len(rpcResp.Error) == 0 {
		t.Fatal("expected JSON-RPC error for missing attemptKey")
	}
}

func TestCancel(t *testing.T) {
	worker := newFakeWorker(300 * time.Millisecond)
	_, client := newTestServer(t, worker)

	task, err := client.Submit(context.Background(), submitReq("g1/r1/s2/a1"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	canceled, err := client.Cancel(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if canceled.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("Cancel state = %s, want CANCELED", canceled.Status.State)
	}

	got, err := client.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("Get after cancel: %v", err)
	}
	if got.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("Get after cancel state = %s, want CANCELED", got.Status.State)
	}
}

func TestSubscribeStream(t *testing.T) {
	worker := newFakeWorker(200 * time.Millisecond)
	_, client := newTestServer(t, worker)

	task, err := client.Submit(context.Background(), submitReq("g1/r1/s3/a1"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	sawWorking := false
	for ev, err := range client.Subscribe(context.Background(), task.ID) {
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if v, ok := ev.(*a2a.TaskStatusUpdateEvent); ok && v.Status.State == a2a.TaskStateWorking {
			sawWorking = true
		}
	}
	if !sawWorking {
		t.Fatal("subscribe stream did not see working status")
	}
}

func TestAgentCardPublic(t *testing.T) {
	worker := newFakeWorker(30 * time.Millisecond)
	srv, _ := newTestServer(t, worker)

	resp, err := http.Get(srv.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil {
		t.Fatalf("GET agent card: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("agent card status = %d, want 200", resp.StatusCode)
	}
}

func TestBearerRejected(t *testing.T) {
	worker := newFakeWorker(30 * time.Millisecond)
	srv, _ := newTestServer(t, worker)

	resp, err := http.Post(srv.URL+"/a2a", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}
