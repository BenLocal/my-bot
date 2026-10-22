//go:build e2e

// Package e2e contains end-to-end tests that start Redis, a Dapr sidecar, the
// Runtime and fake Workers as real processes and verify the M0 acceptance
// cases of docs/spec.md §12 (plan T13). Run with:
//
//	go test -tags e2e -timeout 30m ./test/e2e/
//
// Requires redis-server, dapr CLI (initialised: `dapr init --slim` or full) on PATH.
// Each test gets a fresh Redis (data dir under t.TempDir()); processes are
// started in their own process group and stopped by PID, never by pattern.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	apiBase      = "http://localhost:8080/v1"
	daprHTTPBase = "http://localhost:3500"
	repoRoot     = "../.."
)

// okScript is the default member script: valid output contract, done=true.
const okScript = `{"delay":"2s","result":"{\"summary\":\"ok\",\"done\":true}"}`

var (
	binDir         string
	useDockerRedis bool // 本机无 redis-server 时用一次性 docker 容器（redis:7-alpine）
)

func TestMain(m *testing.M) {
	if _, err := exec.LookPath("dapr"); err != nil {
		fmt.Println("skipping e2e: dapr not on PATH")
		os.Exit(0)
	}
	if _, err := exec.LookPath("redis-server"); err != nil {
		if _, derr := exec.LookPath("docker"); derr != nil {
			fmt.Println("skipping e2e: neither redis-server nor docker on PATH")
			os.Exit(0)
		}
		useDockerRedis = true
	}
	dir, err := os.MkdirTemp("", "mybot-e2e-bin-")
	if err != nil {
		panic(err)
	}
	binDir = dir
	for _, name := range []string{"mybot-runtime", "mybot-worker"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(binDir, name), "./cmd/"+name)
		cmd.Dir = repoRoot
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Printf("build %s: %v\n%s", name, err, out)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(binDir)
	os.Exit(code)
}

type workerOpts struct {
	id       string
	port     int
	capacity int
	script   string
}

type env struct {
	t           *testing.T
	dir         string
	artifactDir string
	procs       map[string]*exec.Cmd // name → process (group leader)
	workers     map[string]workerOpts
	runtimeArgs []string

	redisContainer string
	appID          string // 每个用例独立：Workflow / Actor 定时器存在共享的 dapr scheduler 中，按 app-id 隔离
}

type envOpts struct {
	workers      int
	capacity     int
	workerScript string
	runtimeArgs  []string
}

// startEnv launches Redis, the Runtime with its Dapr sidecar, and N fake Workers (w1..wN).
func startEnv(t *testing.T, o envOpts) *env {
	t.Helper()
	if o.capacity == 0 {
		o.capacity = 2
	}
	if o.workerScript == "" {
		o.workerScript = okScript
	}
	dir := t.TempDir()
	if keep := os.Getenv("MYBOT_E2E_LOGDIR"); keep != "" { // 保留日志便于排查
		d, err := os.MkdirTemp(keep, t.Name()+"-")
		if err != nil {
			t.Fatal(err)
		}
		dir = d
	}
	e := &env{t: t, dir: dir, procs: map[string]*exec.Cmd{}, workers: map[string]workerOpts{}, runtimeArgs: o.runtimeArgs}
	e.artifactDir = filepath.Join(e.dir, "artifacts")
	e.appID = fmt.Sprintf("mybot-e2e-%d", time.Now().UnixNano())
	t.Cleanup(e.stopAll)

	for _, port := range []string{"6379", "3500", "50001", "8080"} {
		if conn, err := net.DialTimeout("tcp", "localhost:"+port, 200*time.Millisecond); err == nil {
			conn.Close()
			t.Fatalf("port %s already in use; stop the local dev stack before running e2e", port)
		}
	}

	e.startRedis()

	e.start("daprd", "dapr", "run",
		"--app-id", e.appID,
		"--app-port", "8080",
		"--app-protocol", "http",
		"--dapr-http-port", "3500",
		"--dapr-grpc-port", "50001",
		"--resources-path", "deploy/dapr/local",
		"--config", "deploy/dapr/local/config.yaml",
	)
	waitForPort(t, "localhost:50001", 30*time.Second)

	e.startRuntime()
	for i := 1; i <= o.workers; i++ {
		e.startWorker(workerOpts{id: fmt.Sprintf("w%d", i), port: 9100 + i, capacity: o.capacity, script: o.workerScript})
	}
	e.waitWorkers(o.workers)
	return e
}

func (e *env) startRedis() {
	e.t.Helper()
	if !useDockerRedis {
		e.start("redis", "redis-server", "--port", "6379", "--dir", e.dir, "--save", "", "--appendonly", "no")
		waitForPort(e.t, "localhost:6379", 10*time.Second)
		return
	}
	e.redisContainer = fmt.Sprintf("mybot-e2e-redis-%d", time.Now().UnixNano())
	out, err := exec.Command("docker", "run", "-d", "--rm", "--name", e.redisContainer, "-p", "127.0.0.1:6379:6379",
		"redis:7-alpine", "redis-server", "--save", "", "--appendonly", "no").CombinedOutput()
	if err != nil {
		e.t.Fatalf("docker run redis: %v: %s", err, out)
	}
	eventually(e.t, 20*time.Second, func() bool {
		return exec.Command("docker", "exec", e.redisContainer, "redis-cli", "ping").Run() == nil
	}, "redis container ready")
}

func (e *env) start(name string, bin string, args ...string) {
	e.t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = repoRoot
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	logFile, err := os.Create(filepath.Join(e.dir, name+".log"))
	if err != nil {
		e.t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		e.t.Fatalf("start %s: %v", name, err)
	}
	e.procs[name] = cmd
}

// kill stops a process group started by start (SIGKILL when hard, else SIGTERM + wait).
func (e *env) kill(name string, hard bool) {
	cmd, ok := e.procs[name]
	if !ok {
		return
	}
	delete(e.procs, name)
	sig := syscall.SIGTERM
	if hard {
		sig = syscall.SIGKILL
	}
	_ = syscall.Kill(-cmd.Process.Pid, sig)
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
}

func (e *env) stopAll() {
	if e.t.Failed() {
		if _, ok := e.procs["runtime"]; ok {
			var goals []goalView
			e.getJSON("/goals?status=all", &goals)
			for _, g := range goals {
				_, evs := e.do("GET", "/goals/"+g.ID+"/events", nil, nil)
				_, rounds := e.do("GET", "/goals/"+g.ID+"/rounds", nil, nil)
				e.t.Logf("goal %s status=%s reason=%s\nevents: %s\nrounds: %s", g.ID, g.Status, g.Reason, evs, rounds)
			}
		}
	}
	for _, name := range []string{"runtime"} {
		e.kill(name, false)
	}
	for name := range e.procs {
		if strings.HasPrefix(name, "worker-") {
			e.kill(name, false)
		}
	}
	e.kill("daprd", false)
	e.kill("redis", false)
	if e.redisContainer != "" {
		_ = exec.Command("docker", "rm", "-f", e.redisContainer).Run()
	}
	if e.t.Failed() {
		for _, name := range []string{"daprd", "runtime", "worker-w1", "worker-w2"} {
			if b, err := os.ReadFile(filepath.Join(e.dir, name+".log")); err == nil {
				e.t.Logf("---- %s.log (warnings/errors, tail) ----\n%s", name, tail(problems(string(b)), 40))
			}
		}
	}
}

func (e *env) startRuntime() {
	e.t.Helper()
	args := append([]string{
		"--port", "8080",
		"--dapr-port", "50001",
		"--roles", "config/roles.fake.yaml",
		"--roles-dir", ".",
		"--artifact-dir", e.artifactDir,
	}, e.runtimeArgs...)
	e.start("runtime", filepath.Join(binDir, "mybot-runtime"), args...)
	waitHTTP(e.t, "http://localhost:8080/healthz", 30*time.Second)
}

func (e *env) restartRuntime() {
	e.kill("runtime", true)
	e.startRuntime()
}

func (e *env) startWorker(o workerOpts) {
	e.t.Helper()
	e.workers[o.id] = o
	e.start("worker-"+o.id, filepath.Join(binDir, "mybot-worker"),
		"--addr", fmt.Sprintf(":%d", o.port),
		"--worker-id", o.id,
		"--executor", "fake",
		"--account", "default",
		"--capacity", fmt.Sprint(o.capacity),
		"--store-addr", "localhost:50001",
		"--store-name", "statestore",
		"--pubsub-name", "pubsub",
		"--artifact-dir", e.artifactDir,
		"--fake-script", o.script,
	)
	waitForPort(e.t, fmt.Sprintf("localhost:%d", o.port), 15*time.Second)
}

func (e *env) killWorker(id string, hard bool) { e.kill("worker-"+id, hard) }

func (e *env) restartWorker(id string) {
	e.killWorker(id, true)
	e.startWorker(e.workers[id])
}

func (e *env) waitWorkers(n int) {
	e.t.Helper()
	eventually(e.t, 30*time.Second, func() bool {
		var ws workersView
		return e.getJSON("/workers", &ws) == http.StatusOK && len(ws.Workers) >= n
	}, "workers registered")
}

// ---- API helpers ----

type workersView struct {
	Workers []struct {
		ID string `json:"id"`
	} `json:"workers"`
	Scheduler *struct {
		ActiveLeases int            `json:"activeLeases"`
		Queued       int            `json:"queued"`
		Inflight     map[string]int `json:"inflight"`
		Drained      []string       `json:"drained"`
	} `json:"scheduler"`
}

type goalView struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Round  int    `json:"round"`
	Reason string `json:"reason"`
	Ask    *struct {
		RequestID string `json:"requestID"`
		Question  string `json:"question"`
	} `json:"ask"`
	Budget struct {
		TokensUsed int64 `json:"tokensUsed"`
	} `json:"budget"`
}

type eventView struct {
	Seq  int            `json:"seq"`
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
}

func (e *env) do(method, path string, body any, headers map[string]string) (int, []byte) {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, apiBase+path, rd)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, []byte(err.Error())
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func (e *env) getJSON(path string, v any) int {
	code, body := e.do("GET", path, nil, nil)
	if code == http.StatusOK {
		_ = json.Unmarshal(body, v)
	}
	return code
}

// create posts a goal/task and returns (status code, id).
func (e *env) create(path string, spec map[string]any, idemKey string) (int, string) {
	e.t.Helper()
	if idemKey == "" {
		idemKey = fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	}
	code, body := e.do("POST", path, spec, map[string]string{"Idempotency-Key": idemKey})
	if code != http.StatusCreated && code != http.StatusOK {
		e.t.Fatalf("create %s: status=%d body=%s", path, code, body)
	}
	var resp struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(body, &resp)
	return code, resp.ID
}

func (e *env) goal(id string) goalView {
	var g goalView
	if code := e.getJSON("/goals/"+id, &g); code != http.StatusOK {
		e.t.Fatalf("get goal %s: status %d", id, code)
	}
	return g
}

func (e *env) events(id string) []eventView {
	var evs []eventView
	e.getJSON("/goals/"+id+"/events", &evs)
	return evs
}

func (e *env) waitStatus(id string, timeout time.Duration, statuses ...string) goalView {
	e.t.Helper()
	var g goalView
	eventually(e.t, timeout, func() bool {
		g = e.goal(id)
		for _, s := range statuses {
			if g.Status == s {
				return true
			}
		}
		return false
	}, fmt.Sprintf("goal %s status in %v (last %q, reason %q)", id, statuses, g.Status, g.Reason))
	return g
}

func (e *env) waitEvent(id, typ string, timeout time.Duration) eventView {
	e.t.Helper()
	var found eventView
	eventually(e.t, timeout, func() bool {
		for _, ev := range e.events(id) {
			if ev.Type == typ {
				found = ev
				return true
			}
		}
		return false
	}, fmt.Sprintf("event %s on goal %s", typ, id))
	return found
}

func countEvents(evs []eventView, typ string) int {
	n := 0
	for _, ev := range evs {
		if ev.Type == typ {
			n++
		}
	}
	return n
}

func eventsOf(evs []eventView, typ string) []eventView {
	var out []eventView
	for _, ev := range evs {
		if ev.Type == typ {
			out = append(out, ev)
		}
	}
	return out
}

// fakeObjective embeds a fake-script block (fake.Script JSON) into a prompt text.
func fakeObjective(text string, script any) string {
	b, err := json.Marshal(script)
	if err != nil {
		panic(err)
	}
	return text + "\n```fake-script\n" + string(b) + "\n```\n"
}

// memberResult is a member output contract.
func memberResult(summary string, done bool) string {
	b, _ := json.Marshal(map[string]any{"summary": summary, "done": done})
	return string(b)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func waitForPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, func() bool {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	}, "port "+addr)
}

func waitHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, url)
}

func tail(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// problems keeps only warning / error log lines.
func problems(log string) string {
	var out []string
	for _, l := range strings.Split(log, "\n") {
		ll := strings.ToLower(l)
		if strings.Contains(ll, "warn") || strings.Contains(ll, "error") || strings.Contains(ll, "fatal") || strings.Contains(ll, "panic") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
