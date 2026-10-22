package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	a2ago "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/BenLocal/my-bot/internal/a2a"
	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/executor/fake"
	"github.com/BenLocal/my-bot/internal/store"
)

const testToken = "test-token"

// memStore 是测试用的进程内 store.Store，FirstWrite 的 owner 语义与 store 层一致。
type memStore struct {
	mu    sync.Mutex
	items map[string][]byte
	seq   int
	etags map[string]string
}

func newMemStore() *memStore {
	return &memStore{items: map[string][]byte{}, etags: map[string]string{}}
}

func (m *memStore) Get(ctx context.Context, key string) (*store.Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.items[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &store.Item{Value: append([]byte(nil), v...), ETag: m.etags[key]}, nil
}

func (m *memStore) Set(ctx context.Context, key string, value []byte, etag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if etag != "" && m.etags[key] != etag {
		return store.ErrConflict
	}
	m.seq++
	m.items[key] = append([]byte(nil), value...)
	m.etags[key] = strconv.Itoa(m.seq)
	return nil
}

func (m *memStore) SetTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return m.Set(ctx, key, value, "")
}

func (m *memStore) Delete(ctx context.Context, key string, etag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
	delete(m.etags, key)
	return nil
}

func (m *memStore) FirstWrite(ctx context.Context, key string, value []byte, owner string) (bool, error) {
	m.mu.Lock()
	if v, ok := m.items[key]; ok {
		m.mu.Unlock()
		var probe struct {
			Owner string `json:"owner"`
		}
		if err := json.Unmarshal(v, &probe); err != nil || probe.Owner != owner {
			return false, store.ErrConflict
		}
		return false, nil
	}
	m.mu.Unlock()
	return true, m.Set(ctx, key, value, "")
}

func (m *memStore) Transact(ctx context.Context, ops []store.Op) error {
	return errors.New("memStore: Transact not supported")
}

// recPublisher 记录发布的 attempt-events。
type recPublisher struct {
	mu     sync.Mutex
	events []attemptEvent
}

func (p *recPublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	var ev attemptEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		return err
	}
	p.mu.Lock()
	p.events = append(p.events, ev)
	p.mu.Unlock()
	return nil
}

func (p *recPublisher) statuses(key string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, ev := range p.events {
		if ev.AttemptKey == key {
			out = append(out, ev.Status)
		}
	}
	return out
}

func newTestService(st store.Store, pub PubSubPublisher, opts ...fake.Option) *Service {
	return NewService(
		WithWorkerID("w1"),
		WithStore(st),
		WithPubSub(pub),
		WithFakeOptions(append([]fake.Option{fake.WithWorkerID("w1")}, opts...)...),
	)
}

func waitTerminal(t *testing.T, svc *Service, key string) *a2ago.Task {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		task, err := svc.GetByAttemptKey(context.Background(), key)
		if err != nil {
			t.Fatalf("GetByAttemptKey: %v", err)
		}
		if task.Status.State.Terminal() {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("attempt %s did not reach terminal state", key)
	return nil
}

func indexKeys(t *testing.T, svc *Service) []string {
	t.Helper()
	keys, err := svc.listIndex(context.Background())
	if err != nil {
		t.Fatalf("listIndex: %v", err)
	}
	return keys
}

func TestCompletedCarriesResultAndArtifacts(t *testing.T) {
	ctx := context.Background()
	st, pub, root := newMemStore(), &recPublisher{}, t.TempDir()
	svc := newTestService(st, pub, fake.WithArtifactRoot(root), fake.WithDefaultScript(fake.Script{
		Result:    `{"summary":"ok","done":true}`,
		Usage:     core.Usage{InputTokens: 10, OutputTokens: 5},
		Artifacts: []fake.ArtifactSpec{{Name: "report.md", Content: "hello"}},
	}))

	meta := a2a.Metadata{AttemptKey: "k1", GoalID: "g1", Round: 1, StepID: "s1", Role: "dev"}
	res, err := svc.Submit(ctx, meta, a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("do it")), a2ago.TaskInfo{})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := a2a.DecodeTaskResult(res.Task).Status; got != core.AttemptSubmitted {
		t.Fatalf("submit status = %s", got)
	}

	task := waitTerminal(t, svc, "k1")
	r := a2a.DecodeTaskResult(task)
	if r.Status != core.AttemptCompleted || r.Text != `{"summary":"ok","done":true}` || r.Usage.InputTokens != 10 {
		t.Fatalf("unexpected result: %+v", r)
	}
	if len(r.Artifacts) != 1 || r.Artifacts[0].Path != "r1/s1/report.md" || r.Artifacts[0].Round != 1 ||
		r.Artifacts[0].Sha256 != core.HashString("hello") || r.Artifacts[0].Size != 5 {
		t.Fatalf("unexpected artifacts: %+v", r.Artifacts)
	}
	art := task.Artifacts[0]
	wantPath := filepath.Join(root, "g1", "r1", "s1", "report.md")
	if art.Name != "report.md" || art.Metadata[a2a.MetaArtifactPath] != wantPath ||
		art.Metadata[a2a.MetaArtifactURI] != "mybot-artifact://g1/r1/s1/report.md" {
		t.Fatalf("unexpected artifact: %+v", art)
	}
	if b, err := os.ReadFile(wantPath); err != nil || string(b) != "hello" {
		t.Fatalf("artifact file: %q %v", b, err)
	}

	rec, err := store.LoadAttempt(ctx, st, "w1", "k1")
	if err != nil {
		t.Fatalf("LoadAttempt: %v", err)
	}
	if rec.Status != core.AttemptCompleted || len(rec.Artifacts) != 1 || rec.Artifacts[0].Path != "r1/s1/report.md" {
		t.Fatalf("unexpected record: %+v", rec)
	}
	if keys := indexKeys(t, svc); len(keys) != 0 {
		t.Fatalf("terminal attempt should leave the index, got %v", keys)
	}
	if got := pub.statuses("k1"); len(got) != 1 || got[0] != "completed" {
		t.Fatalf("published = %v", got)
	}

	// 重启后按 attemptKey 查询，结果正文、产物 URI（含 goalID）仍可从记录取回。
	svc2 := newTestService(st, pub)
	after, err := svc2.GetByAttemptKey(ctx, "k1")
	if err != nil {
		t.Fatal(err)
	}
	r2 := a2a.DecodeTaskResult(after)
	if r2.Status != core.AttemptCompleted || r2.Text != r.Text || r2.Usage != r.Usage ||
		len(r2.Artifacts) != 1 || r2.Artifacts[0] != r.Artifacts[0] {
		t.Fatalf("after restart: %+v", r2)
	}
	if after.Artifacts[0].Metadata[a2a.MetaArtifactURI] != "mybot-artifact://g1/r1/s1/report.md" ||
		after.Metadata[a2a.MetaGoalID] != "g1" || after.Metadata[a2a.MetaStepID] != "s1" {
		t.Fatalf("after restart metadata: %+v / %+v", after.Metadata, after.Artifacts[0].Metadata)
	}
}

func TestSessionIDPolicy(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	svc := newTestService(st, nil, fake.WithDefaultScript(fake.Script{Hang: true}))
	msg := a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("x"))

	if _, err := svc.Submit(ctx, a2a.Metadata{AttemptKey: "kr", GoalID: "g1", Role: "dev", SessionPolicy: a2a.SessionPolicyReuse}, msg, a2ago.TaskInfo{}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Submit(ctx, a2a.Metadata{AttemptKey: "kf", GoalID: "g1", Role: "dev", SessionPolicy: a2a.SessionPolicyFresh}, msg, a2ago.TaskInfo{}); err != nil {
		t.Fatal(err)
	}
	rr, _ := store.LoadAttempt(ctx, st, "w1", "kr")
	rf, _ := store.LoadAttempt(ctx, st, "w1", "kf")
	if rr.SessionID != core.SessionID("g1", "dev") {
		t.Fatalf("reuse session = %s", rr.SessionID)
	}
	if rf.SessionID != core.SessionIDFresh("kf") {
		t.Fatalf("fresh session = %s", rf.SessionID)
	}
}

func TestCancelEndsCancelled(t *testing.T) {
	ctx := context.Background()
	st, pub := newMemStore(), &recPublisher{}
	svc := newTestService(st, pub, fake.WithDefaultScript(fake.Script{Hang: true}))

	res, err := svc.Submit(ctx, a2a.Metadata{AttemptKey: "kc", GoalID: "g1"}, a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("x")), a2ago.TaskInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Cancel(ctx, res.Task.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := svc.WaitInflight(ctx); err != nil {
		t.Fatal(err)
	}
	task, _ := svc.Get(ctx, res.Task.ID)
	if task.Status.State != a2ago.TaskStateCanceled || a2a.DecodeTaskResult(task).Status != core.AttemptCancelled {
		t.Fatalf("task after cancel: %s %+v", task.Status.State, task.Metadata)
	}
	rec, _ := store.LoadAttempt(ctx, st, "w1", "kc")
	if rec.Status != core.AttemptCancelled {
		t.Fatalf("record status = %s", rec.Status)
	}
	// 再次取消是空操作，不重复发布。
	if err := svc.Cancel(ctx, res.Task.ID); err != nil {
		t.Fatalf("second Cancel: %v", err)
	}
	if got := pub.statuses("kc"); len(got) != 1 || got[0] != "cancelled" {
		t.Fatalf("published = %v", got)
	}
}

func TestRestartGetByAttemptKeyAndRecover(t *testing.T) {
	ctx := context.Background()
	st, pub := newMemStore(), &recPublisher{}
	svc1 := newTestService(st, pub, fake.WithDefaultScript(fake.Script{Hang: true}))
	res, err := svc1.Submit(ctx, a2a.Metadata{AttemptKey: "kh", GoalID: "g1", Round: 1}, a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("x")), a2ago.TaskInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if keys := indexKeys(t, svc1); len(keys) != 1 || keys[0] != "kh" {
		t.Fatalf("index = %v", keys)
	}

	// 模拟重启：新 Service 共享同一存储，内存表为空。
	svc2 := newTestService(st, pub)
	_, client := newHTTPServer(t, svc2)

	task, err := client.GetByAttemptKey(ctx, "kh")
	if err != nil {
		t.Fatalf("GetByAttemptKey after restart: %v", err)
	}
	if task.ID != res.Task.ID || a2a.DecodeTaskResult(task).Status != core.AttemptSubmitted {
		t.Fatalf("synthesized task: id=%s meta=%+v", task.ID, task.Metadata)
	}
	if _, err := svc2.Get(ctx, res.Task.ID); !errors.Is(err, a2ago.ErrTaskNotFound) {
		t.Fatalf("Get after restart should be not-found, got %v", err)
	}

	if err := svc2.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	task, err = client.GetByAttemptKey(ctx, "kh")
	if err != nil {
		t.Fatal(err)
	}
	if r := a2a.DecodeTaskResult(task); r.Status != core.AttemptInterrupted || task.Status.State != a2ago.TaskStateFailed {
		t.Fatalf("after recover: %s %+v", task.Status.State, r)
	}
	if keys := indexKeys(t, svc2); len(keys) != 0 {
		t.Fatalf("index after recover = %v", keys)
	}
	if got := pub.statuses("kh"); len(got) != 1 || got[0] != "interrupted" {
		t.Fatalf("published = %v", got)
	}
	pub.mu.Lock()
	gotGoal := pub.events[len(pub.events)-1].GoalID
	pub.mu.Unlock()
	if gotGoal != "g1" {
		t.Fatalf("recover event goalID = %q", gotGoal)
	}

	if _, err := client.GetByAttemptKey(ctx, "missing"); !errors.Is(err, a2ago.ErrTaskNotFound) {
		t.Fatalf("missing key: %v", err)
	}
}

func TestHTTPRoundTripResultMetadata(t *testing.T) {
	ctx := context.Background()
	st, root := newMemStore(), t.TempDir()
	svc := newTestService(st, nil, fake.WithArtifactRoot(root), fake.WithDefaultScript(fake.Script{
		Delay:     fake.Duration(20 * time.Millisecond),
		Result:    "done",
		Usage:     core.Usage{InputTokens: 3, OutputTokens: 4},
		Artifacts: []fake.ArtifactSpec{{Name: "out.txt", Content: "abc"}},
	}))
	_, client := newHTTPServer(t, svc)

	req := &a2ago.SendMessageRequest{Message: a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("go"))}
	req.Metadata = a2a.EncodeMetadata(nil, a2a.Metadata{AttemptKey: "kx", GoalID: "g9", Round: 2, StepID: "s1", Role: "dev"})
	task, err := client.Submit(ctx, req)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	var got *a2ago.Task
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err = client.Get(ctx, task.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Status.State.Terminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	r := a2a.DecodeTaskResult(got)
	if r.Status != core.AttemptCompleted || r.Text != "done" || r.Usage.OutputTokens != 4 {
		t.Fatalf("GetTask result: %+v", r)
	}
	if len(r.Artifacts) != 1 || r.Artifacts[0].Path != "r2/s1/out.txt" {
		t.Fatalf("GetTask artifacts: %+v", r.Artifacts)
	}

	byKey, err := client.GetByAttemptKey(ctx, "kx")
	if err != nil {
		t.Fatal(err)
	}
	if rk := a2a.DecodeTaskResult(byKey); rk.Status != core.AttemptCompleted || len(rk.Artifacts) != 1 {
		t.Fatalf("GetByAttemptKey result: %+v", rk)
	}

	// 对已完成任务取消是空操作，返回原任务。
	c, err := client.Cancel(ctx, task.ID)
	if err != nil {
		t.Fatalf("Cancel terminal: %v", err)
	}
	if c.Status.State != a2ago.TaskStateCompleted {
		t.Fatalf("Cancel terminal state = %s", c.Status.State)
	}
}

func TestHTTPCancel(t *testing.T) {
	ctx := context.Background()
	st, pub := newMemStore(), &recPublisher{}
	svc := newTestService(st, pub, fake.WithDefaultScript(fake.Script{Hang: true}))
	_, client := newHTTPServer(t, svc)

	req := &a2ago.SendMessageRequest{Message: a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("go"))}
	req.Metadata = a2a.EncodeMetadata(nil, a2a.Metadata{AttemptKey: "kq", GoalID: "g1", Round: 1})
	task, err := client.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.Cancel(ctx, task.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if c.Status.State != a2ago.TaskStateCanceled {
		t.Fatalf("cancel state = %s", c.Status.State)
	}
	byKey, err := client.GetByAttemptKey(ctx, "kq")
	if err != nil {
		t.Fatal(err)
	}
	if a2a.DecodeTaskResult(byKey).Status != core.AttemptCancelled {
		t.Fatalf("byKey after cancel: %+v", byKey.Metadata)
	}
	rec, _ := store.LoadAttempt(ctx, st, "w1", "kq")
	if rec.Status != core.AttemptCancelled {
		t.Fatalf("record = %s", rec.Status)
	}
}

func TestDrainRejectsSubmit(t *testing.T) {
	svc := newTestService(newMemStore(), nil)
	svc.Drain()
	_, err := svc.Submit(context.Background(), a2a.Metadata{AttemptKey: "kd"}, a2ago.NewMessage(a2ago.MessageRoleUser, a2ago.NewTextPart("x")), a2ago.TaskInfo{})
	if err == nil {
		t.Fatal("draining worker must reject Submit")
	}
}

func newHTTPServer(t *testing.T, svc *Service) (*httptest.Server, *a2a.Client) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	baseURL := "http://" + ln.Addr().String()
	card := &a2ago.AgentCard{
		Name:                "test-worker",
		Version:             "0.0.1",
		SupportedInterfaces: []*a2ago.AgentInterface{a2ago.NewAgentInterface(baseURL+"/a2a", a2ago.TransportProtocolJSONRPC)},
		Capabilities:        a2ago.AgentCapabilities{Streaming: true},
		DefaultInputModes:   []string{"text/plain"},
		DefaultOutputModes:  []string{"text/plain"},
	}
	srv := httptest.NewUnstartedServer(a2a.NewServer(svc, card, testToken).Handler())
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)
	client, err := a2a.NewClient(context.Background(), baseURL, testToken)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return srv, client
}
