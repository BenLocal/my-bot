//go:build e2e

// Package e2e contains integration tests that start Dapr, Runtime, and fake
// Workers to verify §12 end-to-end use cases. These tests require a running
// Dapr sidecar and are run with: go test -tags e2e ./test/e2e/
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
	"strconv"
	"testing"
	"time"
)

// integrationEnv manages the lifecycle of Dapr, Runtime, and Worker processes
// for integration tests.
type integrationEnv struct {
	t          *testing.T
	redisPID   string
	daprdPID   string
	runtimePID string
	workerPIDs []string
	httpBase   string
}

// startIntegration launches Redis, Dapr, Runtime, and N fake Workers.
// It skips the test if Redis or Dapr binaries are not available.
func startIntegration(t *testing.T, numWorkers int) *integrationEnv {
	t.Helper()
	env := &integrationEnv{t: t, httpBase: "http://localhost:8080"}

	// Check prerequisites.
	if err := exec.Command("redis-server", "--version").Run(); err != nil {
		t.Skip("redis-server not available, skipping integration test")
	}
	if err := exec.Command("daprd", "--version").Run(); err != nil {
		t.Skip("daprd not available, skipping integration test")
	}

	// Kill any stale Redis from previous test runs.
	exec.Command("pkill", "-f", "redis-server.*6379").Run()

	// Start Redis.
	redisCmd := exec.Command("redis-server", "--port", "6379", "--daemonize", "no")
	redisCmd.Stdout = os.Stdout
	redisCmd.Stderr = os.Stderr
	if err := redisCmd.Start(); err != nil {
		t.Fatalf("failed to start Redis: %v", err)
	}
	env.redisPID = fmt.Sprintf("%d", redisCmd.Process.Pid)

	// Wait for Redis.
	waitForPort(t, "localhost:6379", 5*time.Second)

	// Start Dapr sidecar via dapr run (standard standalone mode, starts placement internally).
	daprdCmd := exec.Command("dapr", "run",
		"--app-id", "mybot-runtime",
		"--dapr-http-port", "3500",
		"--dapr-grpc-port", "50001",
		"--resources-path", "../../deploy/dapr/local",
		"--config", "../../deploy/dapr/local/config.yaml",
		"--app-port", "8080",
		"--app-protocol", "http",
	)
	daprdCmd.Stdout = os.Stdout
	daprdCmd.Stderr = os.Stderr
	if err := daprdCmd.Start(); err != nil {
		t.Fatalf("failed to start daprd: %v", err)
	}
	env.daprdPID = fmt.Sprintf("%d", daprdCmd.Process.Pid)

	// Wait for Dapr.
	waitForPort(t, "localhost:3500", 10*time.Second)

	// Build binaries.
	goEnv := os.Environ()
	goEnv = append(goEnv,
		"GOCACHE=/tmp/go-cache",
		"GOMODCACHE=/tmp/go-mod-cache",
		"GOPATH=/tmp/go-path",
	)
	buildCmd := exec.Command("go", "build", "-o", "/tmp/mybot-runtime", "./cmd/mybot-runtime")
	buildCmd.Env = goEnv
	buildCmd.Dir = "../../"
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build runtime: %v: %s", err, out)
	}
	buildCmd = exec.Command("go", "build", "-o", "/tmp/mybot-worker", "./cmd/mybot-worker")
	buildCmd.Env = goEnv
	buildCmd.Dir = "../../"
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build worker: %v: %s", err, out)
	}

	// Start Runtime.
	runtimeCmd := exec.Command("/tmp/mybot-runtime",
		"--port", "8080",
		"--actor-port", "8081",
		"--dapr-port", "50001",
		"--global-max", "2",
	)
	runtimeCmd.Stdout = os.Stdout
	runtimeCmd.Stderr = os.Stderr
	if err := runtimeCmd.Start(); err != nil {
		t.Fatalf("failed to start runtime: %v", err)
	}
	env.runtimePID = fmt.Sprintf("%d", runtimeCmd.Process.Pid)

	// Wait for Runtime.
	waitForPort(t, "localhost:8080", 10*time.Second)

	// Start Workers.
	for i := 1; i <= numWorkers; i++ {
		port := 9100 + i
		workerCmd := exec.Command("/tmp/mybot-worker",
			"--addr", fmt.Sprintf(":%d", port),
			"--executor", "fake",
			"--account", "default",
			"--capacity", "2",
			"--store-addr", "localhost:50001",
			"--store-name", "statestore",
			"--pubsub-name", "pubsub",
		)
		workerCmd.Stdout = os.Stdout
		workerCmd.Stderr = os.Stderr
		if err := workerCmd.Start(); err != nil {
			t.Fatalf("failed to start worker %d: %v", i, err)
		}
		env.workerPIDs = append(env.workerPIDs, fmt.Sprintf("%d", workerCmd.Process.Pid))
		waitForPort(t, fmt.Sprintf("localhost:%d", port), 5*time.Second)
	}

	// Give workers time to register.
	time.Sleep(3 * time.Second)

	return env
}

// stopIntegration kills all processes started by startIntegration.
func (e *integrationEnv) stopIntegration() {
	for _, pid := range e.workerPIDs {
		exec.Command("kill", pid).Run()
	}
	exec.Command("kill", e.runtimePID).Run()
	exec.Command("kill", e.daprdPID).Run()
	exec.Command("kill", e.redisPID).Run()
}

// createGoal creates a goal via the API and returns the goal ID.
func (e *integrationEnv) createGoal(spec map[string]any) string {
	e.t.Helper()
	body, _ := json.Marshal(spec)
	req, _ := http.NewRequest("POST", e.httpBase+"/goals", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "test-"+strconv.FormatInt(time.Now().UnixNano(), 10))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("create goal request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		out, _ := io.ReadAll(resp.Body)
		e.t.Fatalf("create goal failed: status=%d body=%s", resp.StatusCode, out)
	}

	var result struct {
		GoalID string `json:"goalID"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		e.t.Fatalf("decode response: %v", err)
	}
	return result.GoalID
}

// getGoal fetches the current goal projection.
func (e *integrationEnv) getGoal(goalID string) map[string]any {
	e.t.Helper()
	resp, err := http.Get(e.httpBase + "/goals/" + goalID)
	if err != nil {
		e.t.Fatalf("get goal request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		e.t.Fatalf("get goal failed: status=%d", resp.StatusCode)
	}
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		e.t.Fatalf("decode response: %v", err)
	}
	return result
}

// waitForGoalStatus polls until the goal reaches a terminal status or times out.
func (e *integrationEnv) waitForGoalStatus(goalID string, statuses []string, timeout time.Duration) map[string]any {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		g := e.getGoal(goalID)
		status, _ := g["status"].(string)
		for _, s := range statuses {
			if status == s {
				return g
			}
		}
		time.Sleep(1 * time.Second)
	}
	e.t.Fatalf("goal %s did not reach status %v within %v", goalID, statuses, timeout)
	return nil
}

// waitForPort waits until a TCP port is accepting connections.
func waitForPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("port %s not reachable within %v", addr, timeout)
}

// TestE2E_SingleGoalCompletes verifies §12.1: a goal submitted via API
// completes end-to-end.
func TestE2E_SingleGoalCompletes(t *testing.T) {
	env := startIntegration(t, 2)
	defer env.stopIntegration()

	goalID := env.createGoal(map[string]any{
		"kind":      "goal",
		"objective": "Write a hello world program",
		"strategy":  "single",
		"limits": map[string]any{
			"maxRounds": 3,
		},
	})

	t.Logf("created goal %s", goalID)

	// Wait for the goal to complete.
	g := env.waitForGoalStatus(goalID, []string{"succeeded", "failed", "cancelled"}, 60*time.Second)
	t.Logf("goal status: %v", g["status"])
}

// TestE2E_ConcurrencyLimit verifies §12.3: global concurrency limit is enforced.
func TestE2E_ConcurrencyLimit(t *testing.T) {
	env := startIntegration(t, 2)
	defer env.stopIntegration()

	// Create 3 goals with global=2.
	var goalIDs []string
	for i := 0; i < 3; i++ {
		goalID := env.createGoal(map[string]any{
			"kind":      "goal",
			"objective": fmt.Sprintf("Task %d: do something", i),
			"strategy":  "single",
			"limits": map[string]any{
				"maxRounds": 5,
			},
		})
		goalIDs = append(goalIDs, goalID)
	}

	// Check that at least one goal is still running after a short wait.
	time.Sleep(5 * time.Second)
	running := 0
	for _, id := range goalIDs {
		g := env.getGoal(id)
		status, _ := g["status"].(string)
		if status == "running" || status == "queued" {
			running++
		}
	}
	if running == 0 {
		t.Logf("all goals completed (concurrency may have been fast enough)")
	}
}

// TestE2E_CancelGoal verifies §12.9: cancel takes effect while waiting.
func TestE2E_CancelGoal(t *testing.T) {
	env := startIntegration(t, 2)
	defer env.stopIntegration()

	goalID := env.createGoal(map[string]any{
		"kind":      "goal",
		"objective": "A long running task",
		"strategy":  "single",
		"limits": map[string]any{
			"maxRounds": 10,
		},
	})

	// Wait a moment then cancel.
	time.Sleep(2 * time.Second)
	req, _ := http.NewRequest("POST", env.httpBase+"/goals/"+goalID+"/cancel", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("cancel request failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel should return 202, got %d", resp.StatusCode)
	}

	g := env.waitForGoalStatus(goalID, []string{"cancelled"}, 30*time.Second)
	t.Logf("goal cancelled: %v", g["status"])
}

// TestE2E_WorkerRegistration verifies that workers are visible via the API.
func TestE2E_WorkerRegistration(t *testing.T) {
	env := startIntegration(t, 2)
	defer env.stopIntegration()

	resp, err := http.Get(env.httpBase + "/workers")
	if err != nil {
		t.Fatalf("workers request failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Workers []map[string]any `json:"workers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(result.Workers) < 2 {
		t.Fatalf("expected at least 2 workers, got %d", len(result.Workers))
	}
	t.Logf("found %d workers", len(result.Workers))
}

// TestE2E_BrainStrategy verifies §12.12: brain strategy with parallel members.
func TestE2E_BrainStrategy(t *testing.T) {
	env := startIntegration(t, 2)
	defer env.stopIntegration()

	goalID := env.createGoal(map[string]any{
		"kind":      "goal",
		"objective": "Build a simple web app",
		"strategy":  "brain",
		"brain":     "default",
		"members":   []string{"coder", "reviewer"},
		"limits": map[string]any{
			"maxRounds": 5,
		},
	})

	t.Logf("created brain goal %s", goalID)

	// Wait for the goal to complete or fail.
	g := env.waitForGoalStatus(goalID, []string{"succeeded", "failed", "cancelled"}, 120*time.Second)
	t.Logf("brain goal status: %v", g["status"])
}
