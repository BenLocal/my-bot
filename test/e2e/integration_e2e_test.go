//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// §12.1：提交 Task 后客户端即断开，任务仍完成，结果可查询。
func TestE2E_TaskCompletesWithoutClient(t *testing.T) {
	e := startEnv(t, envOpts{workers: 2})
	code, id := e.create("/tasks", map[string]any{"objective": "summarise notes", "members": []string{"assistant"}}, "")
	if code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", code)
	}
	g := e.waitStatus(id, 60*time.Second, "succeeded", "failed")
	if g.Status != "succeeded" || g.Round != 1 {
		t.Fatalf("task: status=%s round=%d reason=%s", g.Status, g.Round, g.Reason)
	}
	var round struct {
		Status string `json:"status"`
		Steps  []struct {
			Summary string `json:"summary"`
		} `json:"steps"`
	}
	if e.getJSON("/goals/"+id+"/rounds/1", &round) != http.StatusOK || round.Status != "succeeded" || len(round.Steps) != 1 || round.Steps[0].Summary != "ok" {
		t.Fatalf("round 1 detail: %+v", round)
	}
	evs := e.events(id)
	for _, typ := range []string{"goal.created", "round.started", "attempt.submitted", "attempt.finished", "goal.finished"} {
		if countEvents(evs, typ) == 0 {
			t.Errorf("missing event %s in %+v", typ, evs)
		}
	}
}

// §12.2：同一 Idempotency-Key 只得到一个 Goal；同一 Step 只提交一次。
func TestE2E_Idempotency(t *testing.T) {
	e := startEnv(t, envOpts{workers: 1})
	spec := map[string]any{"objective": "idempotent", "members": []string{"assistant"}}
	c1, id1 := e.create("/tasks", spec, "idem-1")
	c2, id2 := e.create("/tasks", spec, "idem-1")
	if c1 != http.StatusCreated || c2 != http.StatusOK || id1 != id2 {
		t.Fatalf("idempotent create: (%d,%s) (%d,%s)", c1, id1, c2, id2)
	}
	e.waitStatus(id1, 60*time.Second, "succeeded")
	if n := countEvents(e.events(id1), "attempt.submitted"); n != 1 {
		t.Fatalf("expected exactly 1 attempt.submitted, got %d", n)
	}
	var all []goalView
	e.getJSON("/goals?status=all", &all)
	if len(all) != 1 {
		t.Fatalf("expected 1 goal in index, got %d", len(all))
	}
}

// §12.3：并发上限 2（roles.fake.yaml）时 3 个 Goal：活跃 lease ≤ 2，第三个出现 slot.waiting 后完成。
func TestE2E_ConcurrencyLimit(t *testing.T) {
	e := startEnv(t, envOpts{workers: 2, capacity: 4})
	slow := fakeObjective("slow task", map[string]any{"delay": "6s", "result": memberResult("ok", true)})
	var ids []string
	for i := 0; i < 3; i++ {
		_, id := e.create("/tasks", map[string]any{"objective": slow, "members": []string{"assistant"}}, "")
		ids = append(ids, id)
	}
	deadline := time.Now().Add(90 * time.Second)
	maxActive := 0
	for time.Now().Before(deadline) {
		var ws workersView
		if e.getJSON("/workers", &ws) == http.StatusOK && ws.Scheduler != nil && ws.Scheduler.ActiveLeases > maxActive {
			maxActive = ws.Scheduler.ActiveLeases
		}
		done := 0
		for _, id := range ids {
			if s := e.goal(id).Status; s == "succeeded" || s == "failed" {
				done++
			}
		}
		if done == len(ids) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if maxActive > 2 {
		t.Fatalf("active leases exceeded global limit: %d", maxActive)
	}
	waited := 0
	for _, id := range ids {
		if g := e.goal(id); g.Status != "succeeded" {
			t.Fatalf("goal %s: %s (%s)", id, g.Status, g.Reason)
		}
		waited += countEvents(e.events(id), "slot.waiting")
	}
	if waited == 0 {
		t.Fatal("expected at least one slot.waiting event")
	}
}

// §12.4：执行中重启 Runtime：轮次与预算不变，Attempt 被重新关联，不产生新会话。
func TestE2E_RuntimeRestart(t *testing.T) {
	e := startEnv(t, envOpts{workers: 1})
	obj := fakeObjective("long task", map[string]any{"delay": "15s", "result": memberResult("ok", true), "usage": map[string]any{"inputTokens": 10, "outputTokens": 5}})
	_, id := e.create("/tasks", map[string]any{"objective": obj, "members": []string{"assistant"}}, "")
	e.waitEvent(id, "attempt.submitted", 30*time.Second)
	e.restartRuntime()
	g := e.waitStatus(id, 90*time.Second, "succeeded", "failed")
	if g.Status != "succeeded" || g.Round != 1 || g.Budget.TokensUsed != 15 {
		t.Fatalf("after restart: %+v", g)
	}
	if n := countEvents(e.events(id), "attempt.submitted"); n != 1 {
		t.Fatalf("expected 1 attempt.submitted (no new session), got %d", n)
	}
}

// §12.5：执行中重启 Worker：Attempt 记 interrupted，按重试规则在本轮内重试并完成。
func TestE2E_WorkerRestart(t *testing.T) {
	e := startEnv(t, envOpts{workers: 1})
	obj := fakeObjective("worker restart", map[string]any{"delay": "15s", "result": memberResult("ok", true)})
	_, id := e.create("/tasks", map[string]any{"objective": obj, "members": []string{"assistant"}}, "")
	e.waitEvent(id, "attempt.submitted", 30*time.Second)
	e.restartWorker("w1")
	g := e.waitStatus(id, 120*time.Second, "succeeded", "failed")
	if g.Status != "succeeded" {
		t.Fatalf("goal after worker restart: %+v", g)
	}
	evs := e.events(id)
	interrupted := false
	for _, ev := range eventsOf(evs, "attempt.finished") {
		if ev.Data["status"] == "interrupted" {
			interrupted = true
		}
	}
	if !interrupted || countEvents(evs, "attempt.submitted") != 2 {
		t.Fatalf("expected interrupted attempt then a retry, events: %+v", evs)
	}
}

// brainScript builds a byRound fake script for the brain.
func brainScript(rounds map[int]any) map[string]any {
	byRound := map[string]any{}
	for n, decision := range rounds {
		byRound[fmt.Sprint(n)] = map[string]any{"delay": "1s", "result": mustJSON(decision)}
	}
	return map[string]any{"byRound": byRound}
}

func memberStep(id, role string, delay string, artifact string) map[string]any {
	script := map[string]any{"delay": delay, "result": memberResult(id+" done", true)}
	if artifact != "" {
		script["artifacts"] = []map[string]any{{"name": artifact, "content": id + " output"}}
	}
	return map[string]any{"id": id, "role": role, "instruction": fakeObjective("do "+id, script)}
}

// §12.12：大脑一次派出 2 个成员并行执行，结果与产物在下一轮可见。
func TestE2E_BrainParallel(t *testing.T) {
	e := startEnv(t, envOpts{workers: 2})
	brain := brainScript(map[int]any{
		1: map[string]any{"action": "dispatch", "summary": "design and test in parallel", "steps": []any{
			memberStep("impl", "coder", "4s", "impl.txt"),
			memberStep("tests", "reviewer", "4s", "tests.txt"),
		}},
		2: map[string]any{"action": "finish", "done": true, "summary": "all done"},
	})
	_, id := e.create("/goals", map[string]any{
		"objective": fakeObjective("build it", brain),
		"strategy":  "brain", "brain": "planner", "members": []string{"coder", "reviewer"},
		"limits": map[string]any{"maxRounds": 4},
	}, "")
	g := e.waitStatus(id, 90*time.Second, "succeeded", "failed")
	if g.Status != "succeeded" || g.Round != 2 {
		t.Fatalf("brain goal: %+v", g)
	}
	// 并行：两个成员都在任一成员结束前提交。
	evs := e.events(id)
	submittedMembers, firstMemberFinish := 0, -1
	for i, ev := range evs {
		step, _ := ev.Data["stepID"].(string)
		if step != "impl" && step != "tests" {
			continue
		}
		if ev.Type == "attempt.submitted" && firstMemberFinish < 0 {
			submittedMembers++
		}
		if ev.Type == "attempt.finished" && firstMemberFinish < 0 {
			firstMemberFinish = i
		}
	}
	if submittedMembers != 2 {
		t.Fatalf("members did not run in parallel: %+v", evs)
	}
	// 产物已登记并可下载（sha256 校验）。
	for _, art := range []string{"r1/impl/impl.txt", "r1/tests/tests.txt"} {
		code, body := e.do("GET", "/goals/"+id+"/artifacts/"+strings.Replace(art, "r1/", "1/", 1), nil, nil)
		if code != http.StatusOK || !bytes.Contains(body, []byte("output")) {
			t.Fatalf("artifact %s: %d %s", art, code, body)
		}
	}
}

// §12.13：非成员角色 / 不存在的产物 / 超出 maxStepsPerRound 被拒绝并把原因回传大脑。
func TestE2E_InvalidDecisionRejected(t *testing.T) {
	e := startEnv(t, envOpts{workers: 1})
	bad := map[string]any{"action": "dispatch", "summary": "bad", "steps": []any{
		map[string]any{"id": "a", "role": "hacker", "instruction": "x"},
		map[string]any{"id": "b", "role": "coder", "instruction": "x", "inputs": []string{"r9/nope/file.md"}},
		map[string]any{"id": "c", "role": "coder", "instruction": "x"},
	}}
	brain := brainScript(map[int]any{1: bad, 2: map[string]any{"action": "finish", "done": true, "summary": "ok"}})
	_, id := e.create("/goals", map[string]any{
		"objective": fakeObjective("validate", brain),
		"strategy":  "brain", "brain": "planner", "members": []string{"coder"},
		"limits": map[string]any{"maxRounds": 4, "maxStepsPerRound": 2},
	}, "")
	g := e.waitStatus(id, 90*time.Second, "succeeded", "failed")
	evs := e.events(id)
	rejected := eventsOf(evs, "decision.rejected")
	if len(rejected) != 2 {
		t.Fatalf("expected 2 decision.rejected (2 tries in round 1), got %d: %+v", len(rejected), evs)
	}
	fb, _ := rejected[0].Data["feedback"].(string)
	for _, rule := range []string{"[R1]", "[R2]", "[R3]"} {
		if !strings.Contains(fb, rule) {
			t.Errorf("feedback missing %s: %s", rule, fb)
		}
	}
	if countEvents(evs, "attempt.submitted") != 3 { // brain, brain-2, round-2 brain；成员从未派发
		t.Errorf("unexpected submissions: %+v", eventsOf(evs, "attempt.submitted"))
	}
	if g.Status != "succeeded" {
		t.Fatalf("goal: %+v", g)
	}
}

// §12.13：大脑 ask_user → waiting；错误的 replyTo 返回 409；正确回答后继续完成。
func TestE2E_AskUser(t *testing.T) {
	e := startEnv(t, envOpts{workers: 1})
	brain := brainScript(map[int]any{
		1: map[string]any{"action": "ask_user", "question": "which db?", "summary": "need input"},
		2: map[string]any{"action": "finish", "done": true, "summary": "done"},
	})
	_, id := e.create("/goals", map[string]any{
		"objective": fakeObjective("ask", brain),
		"strategy":  "brain", "brain": "planner", "members": []string{"coder"},
		"limits": map[string]any{"maxRounds": 4},
	}, "")
	g := e.waitStatus(id, 60*time.Second, "waiting")
	if g.Ask == nil || g.Ask.Question != "which db?" {
		t.Fatalf("waiting without ask: %+v", g)
	}
	if code, _ := e.do("POST", "/goals/"+id+":append", map[string]any{"note": "x", "replyTo": "ask:wrong"}, nil); code != http.StatusConflict {
		t.Fatalf("wrong replyTo: expected 409, got %d", code)
	}
	if code, body := e.do("POST", "/goals/"+id+":append", map[string]any{"note": "postgres", "replyTo": g.Ask.RequestID}, nil); code != http.StatusAccepted {
		t.Fatalf("reply: %d %s", code, body)
	}
	if g = e.waitStatus(id, 60*time.Second, "succeeded", "failed"); g.Status != "succeeded" {
		t.Fatalf("after reply: %+v", g)
	}
	if code, _ := e.do("POST", "/goals/"+id+":append", map[string]any{"note": "late"}, nil); code != http.StatusConflict {
		t.Fatalf("append on terminal goal: expected 409, got %d", code)
	}
}

// §12.18：idem 写入后、启动 Workflow 前崩溃：同 key 重试返回同一 ID，并补启动完成。
func TestE2E_CreateCrashWindow(t *testing.T) {
	e := startEnv(t, envOpts{workers: 1})
	const goalID = "g_01CRASHWINDOWE2E000000000"
	// 模拟崩溃：只有 idem 记录（与 CreateIdem 一样以 first-write 写入，值为 JSON 字符串 goalID），没有投影 / 实例。
	body := fmt.Sprintf(`[{"key":"idem:create:crash-1","value":%q,"options":{"concurrency":"first-write"}}]`, goalID)
	resp, err := http.Post(daprHTTPBase+"/v1.0/state/statestore", "application/json", strings.NewReader(body))
	if err != nil || resp.StatusCode/100 != 2 {
		t.Fatalf("seed idem record: %v %v", err, resp)
	}
	resp.Body.Close()
	code, id := e.create("/tasks", map[string]any{"objective": "after crash", "members": []string{"assistant"}}, "crash-1")
	if code != http.StatusOK || id != goalID {
		t.Fatalf("expected 200 with %s, got %d %s", goalID, code, id)
	}
	if g := e.waitStatus(id, 60*time.Second, "succeeded", "failed"); g.Status != "succeeded" {
		t.Fatalf("re-scheduled goal: %+v", g)
	}
}

// §12.19：等槽位、等 Attempt 时 cancel 立即生效，运行中的 Attempt 被取消，lease 全部释放。
func TestE2E_CancelWhileWaiting(t *testing.T) {
	e := startEnv(t, envOpts{workers: 1, capacity: 4})
	hang := fakeObjective("hang", map[string]any{"hang": true})
	var running []string
	for i := 0; i < 2; i++ {
		_, id := e.create("/tasks", map[string]any{"objective": hang, "members": []string{"assistant"}}, "")
		running = append(running, id)
		e.waitEvent(id, "attempt.submitted", 30*time.Second)
	}
	_, queued := e.create("/tasks", map[string]any{"objective": hang, "members": []string{"assistant"}}, "")
	e.waitEvent(queued, "slot.waiting", 30*time.Second)

	start := time.Now()
	for _, id := range append([]string{queued}, running...) {
		if code, body := e.do("POST", "/goals/"+id+":cancel", nil, nil); code != http.StatusAccepted {
			t.Fatalf("cancel %s: %d %s", id, code, body)
		}
	}
	for _, id := range append([]string{queued}, running...) {
		e.waitStatus(id, 20*time.Second, "cancelled")
	}
	if time.Since(start) > 20*time.Second {
		t.Fatalf("cancel took %v", time.Since(start))
	}
	eventually(t, 20*time.Second, func() bool {
		var ws workersView
		return e.getJSON("/workers", &ws) == http.StatusOK && ws.Scheduler != nil && ws.Scheduler.ActiveLeases == 0 && ws.Scheduler.Queued == 0
	}, "all leases released")
}

// §12.20：永久停掉一个 Worker：orphanAfter 后 Attempt 判 interrupted(worker_lost)，在另一个 Worker 上重试。
func TestE2E_OrphanAttempt(t *testing.T) {
	e := startEnv(t, envOpts{workers: 2})
	obj := fakeObjective("orphan", map[string]any{"delay": "20s", "result": memberResult("ok", true)})
	_, id := e.create("/tasks", map[string]any{"objective": obj, "members": []string{"assistant"}, "limits": map[string]any{"orphanAfter": "10s"}}, "")
	sub := e.waitEvent(id, "attempt.submitted", 30*time.Second)
	victim, _ := sub.Data["workerID"].(string)
	e.killWorker(victim, true)

	g := e.waitStatus(id, 4*time.Minute, "succeeded", "failed")
	evs := e.events(id)
	lost := false
	for _, ev := range eventsOf(evs, "attempt.finished") {
		if ev.Data["reason"] == "worker_lost" {
			lost = true
		}
	}
	subs := eventsOf(evs, "attempt.submitted")
	if !lost || len(subs) != 2 || subs[1].Data["workerID"] == victim || g.Status != "succeeded" {
		t.Fatalf("orphan handling: status=%s events=%+v", g.Status, evs)
	}
}

// §12.21（reuse 部分）：大脑第二轮被派回同一 Worker 并复用会话；满载超时放宽见 scheduler 单测。
func TestE2E_SessionAffinity(t *testing.T) {
	e := startEnv(t, envOpts{workers: 2})
	brain := brainScript(map[int]any{
		1: map[string]any{"action": "dispatch", "summary": "step 1", "steps": []any{memberStep("a", "coder", "1s", "")}},
		2: map[string]any{"action": "dispatch", "summary": "step 2", "steps": []any{memberStep("b", "coder", "1s", "")}},
		3: map[string]any{"action": "finish", "done": true, "summary": "done"},
	})
	_, id := e.create("/goals", map[string]any{
		"objective": fakeObjective("affinity", brain),
		"strategy":  "brain", "brain": "planner", "members": []string{"coder"},
		"limits": map[string]any{"maxRounds": 5},
	}, "")
	e.waitStatus(id, 90*time.Second, "succeeded")
	var brainSubs []eventView
	for _, ev := range eventsOf(e.events(id), "attempt.submitted") {
		if ev.Data["stepID"] == "brain" {
			brainSubs = append(brainSubs, ev)
		}
	}
	if len(brainSubs) != 3 {
		t.Fatalf("expected 3 brain submissions, got %+v", brainSubs)
	}
	for _, ev := range brainSubs[1:] {
		if ev.Data["workerID"] != brainSubs[0].Data["workerID"] || ev.Data["sessionPolicy"] != "reuse" {
			t.Fatalf("brain not routed back with reuse: %+v", brainSubs)
		}
	}
}

// §12.22：drain 后该 Worker 不再收到新 lease，inflight 回到 0。
func TestE2E_Drain(t *testing.T) {
	e := startEnv(t, envOpts{workers: 2})
	if code, body := e.do("POST", "/workers/w1:drain", nil, nil); code != http.StatusAccepted {
		t.Fatalf("drain: %d %s", code, body)
	}
	var ids []string
	for i := 0; i < 3; i++ {
		_, id := e.create("/tasks", map[string]any{"objective": "after drain", "members": []string{"assistant"}}, "")
		ids = append(ids, id)
	}
	for _, id := range ids {
		e.waitStatus(id, 90*time.Second, "succeeded")
		for _, ev := range eventsOf(e.events(id), "attempt.submitted") {
			if ev.Data["workerID"] == "w1" {
				t.Fatalf("drained worker got a lease: %+v", ev)
			}
		}
	}
	var ws workersView
	if e.getJSON("/workers", &ws) != http.StatusOK || ws.Scheduler == nil || ws.Scheduler.Inflight["w1"] != 0 {
		t.Fatalf("scheduler view: %+v", ws.Scheduler)
	}
}
