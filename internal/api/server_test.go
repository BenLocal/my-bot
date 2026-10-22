package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/store"
)

const testRoles = `
roles:
  planner:   { executor: fake, account: default, instructions: planner.md, timeout: 15m, capabilities: [brain] }
  assistant: { executor: fake, account: default, timeout: 30m }
  coder:     { executor: fake, account: default, timeout: 60m }
defaultBrain: planner
limits:
  globalConcurrency: 2
  goal: { maxParallel: 2, maxStepsPerRound: 4 }
`

// newTestServer builds a Server over an in-memory store without Dapr: the
// workflow client is nil, so creation records are written but nothing is scheduled.
func newTestServer(t *testing.T, token string) (*httptest.Server, store.Store) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "planner.md"), []byte("PLAN"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := core.ParseRolesConfig([]byte(testRoles))
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewMemStore()
	s := NewServer(st, nil)
	s.SetRoles(cfg, dir)
	s.SetToken(token)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts, st
}

func do(t *testing.T, ts *httptest.Server, method, path, body string, headers map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func TestAuth(t *testing.T) {
	ts, _ := newTestServer(t, "secret")
	if resp, _ := do(t, ts, "GET", "/v1/goals", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: %d", resp.StatusCode)
	}
	if resp, _ := do(t, ts, "GET", "/v1/goals", "", map[string]string{"Authorization": "Bearer wrong"}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", resp.StatusCode)
	}
	if resp, _ := do(t, ts, "GET", "/v1/goals", "", map[string]string{"Authorization": "Bearer secret"}); resp.StatusCode != http.StatusOK {
		t.Fatalf("good token: %d", resp.StatusCode)
	}
	if resp, _ := do(t, ts, "GET", "/healthz", "", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz must be public: %d", resp.StatusCode)
	}
}

func TestCreateIdempotentAndFrozenSpec(t *testing.T) {
	ts, st := newTestServer(t, "")
	body := `{"objective":"plan it","strategy":"brain","members":["coder"]}`
	h := map[string]string{"Idempotency-Key": "k1"}
	r1, o1 := do(t, ts, "POST", "/v1/goals", body, h)
	r2, o2 := do(t, ts, "POST", "/v1/goals", body, h)
	if r1.StatusCode != http.StatusCreated || r2.StatusCode != http.StatusOK || o1["id"] != o2["id"] || o1["statusUrl"] == "" {
		t.Fatalf("idempotent create: %d %v / %d %v", r1.StatusCode, o1, r2.StatusCode, o2)
	}
	id := o1["id"].(string)
	spec, err := store.LoadGoalSpec(context.Background(), st, id)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Brain != "planner" || spec.Roles["planner"].InstructionsText != "PLAN" || spec.Roles["planner"].InstructionsSha256 != core.HashString("PLAN") || spec.MaxParallel != 2 {
		t.Fatalf("spec not frozen with role snapshot / defaults: %+v", spec)
	}
	proj, _ := store.LoadGoalProjection(context.Background(), st, id)
	if proj.Status != core.GoalQueued || proj.EventSeq != 1 {
		t.Fatalf("initial projection: %+v", proj)
	}
	if ev, err := store.LoadEvent(context.Background(), st, id, 1); err != nil || ev.Type != "goal.created" {
		t.Fatalf("goal.created event: %+v %v", ev, err)
	}
}

func TestCreateValidation(t *testing.T) {
	ts, st := newTestServer(t, "")
	cases := map[string]string{
		"missing key":        "",
		"unknown role":       `{"objective":"x","members":["ghost"]}`,
		"brain not capable":  `{"objective":"x","strategy":"brain","brain":"coder","members":["assistant"]}`,
		"brain needs member": `{"objective":"x","strategy":"brain"}`,
		"no objective":       `{"members":["coder"]}`,
		"loop not yet":       `{"kind":"loop","objective":"x","members":["coder"]}`,
	}
	for name, body := range cases {
		h := map[string]string{"Idempotency-Key": name}
		if name == "missing key" {
			h = nil
			body = `{"objective":"x","members":["coder"]}`
		}
		if resp, out := do(t, ts, "POST", "/v1/goals", body, h); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d %v", name, resp.StatusCode, out)
		}
	}

	resp, out := do(t, ts, "POST", "/v1/tasks", `{"objective":"once","members":["assistant"]}`, map[string]string{"Idempotency-Key": "task"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("task: %d %v", resp.StatusCode, out)
	}
	spec, _ := store.LoadGoalSpec(context.Background(), st, out["id"].(string))
	if spec.Kind != core.KindTask || spec.Limits.MaxRounds != 1 {
		t.Fatalf("task spec: %+v", spec)
	}
}

func TestControlRules(t *testing.T) {
	ts, st := newTestServer(t, "")
	_, out := do(t, ts, "POST", "/v1/goals", `{"objective":"x","members":["coder"]}`, map[string]string{"Idempotency-Key": "c"})
	id := out["id"].(string)

	big := `{"note":"` + strings.Repeat("a", 70*1024) + `"}`
	if resp, _ := do(t, ts, "POST", "/v1/goals/"+id+":append", big, nil); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("64KiB limit: %d", resp.StatusCode)
	}
	if resp, _ := do(t, ts, "POST", "/v1/goals/"+id+":append", `{"note":"n","replyTo":"ask:none"}`, nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("replyTo without pending ask: %d", resp.StatusCode)
	}
	for _, action := range []string{"pause", "resume", "append"} {
		if resp, _ := do(t, ts, "POST", "/v1/goals/"+id+":"+action, `{"note":"n"}`, nil); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("%s: %d", action, resp.StatusCode)
		}
	}
	// queued 的 cancel 直接生效。
	if resp, _ := do(t, ts, "POST", "/v1/goals/"+id+":cancel", "", nil); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel: %d", resp.StatusCode)
	}
	if proj, _ := store.LoadGoalProjection(context.Background(), st, id); proj.Status != core.GoalCancelled {
		t.Fatalf("queued cancel not applied: %+v", proj)
	}
	for _, action := range []string{"pause", "resume", "cancel", "append"} {
		if resp, _ := do(t, ts, "POST", "/v1/goals/"+id+":"+action, `{"note":"n"}`, nil); resp.StatusCode != http.StatusConflict {
			t.Errorf("%s on terminal goal: expected 409, got %d", action, resp.StatusCode)
		}
	}
	if resp, _ := do(t, ts, "POST", "/v1/goals/g_missing:pause", "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown goal: %d", resp.StatusCode)
	}
}

func TestEventStreamResume(t *testing.T) {
	ts, st := newTestServer(t, "")
	ctx := context.Background()
	const id = "g_sse"
	for seq := 1; seq <= 3; seq++ {
		if err := store.SaveEvent(ctx, st, id, core.Event{Seq: seq, Type: "round.started", At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SaveGoalProjection(ctx, st, id, store.GoalProjection{GoalState: core.GoalState{ID: id, Status: core.GoalRunning}, EventSeq: 3}); err != nil {
		t.Fatal(err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(reqCtx, "GET", ts.URL+"/v1/goals/"+id+"/events/stream", nil)
	req.Header.Set("Last-Event-ID", id+":1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var ids []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() && len(ids) < 2 {
		if line := sc.Text(); strings.HasPrefix(line, "id: ") {
			ids = append(ids, strings.TrimPrefix(line, "id: "))
		}
	}
	if strings.Join(ids, ",") != id+":2,"+id+":3" {
		t.Fatalf("SSE resume ids = %v", ids)
	}

	// JSON 形式的 after 参数。
	r, _ := http.Get(ts.URL + "/v1/goals/" + id + "/events?after=2")
	var evs []core.Event
	_ = json.NewDecoder(r.Body).Decode(&evs)
	r.Body.Close()
	if len(evs) != 1 || evs[0].Seq != 3 {
		t.Fatalf("events after=2: %+v", evs)
	}
}
