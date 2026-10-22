// mybot-worker is a fake A2A worker for local development and testing.
// It implements the a2a.Worker interface using a fake driver.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	a2ago "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/BenLocal/my-bot/internal/a2a"
	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/executor/fake"
	"github.com/BenLocal/my-bot/internal/store"
	"github.com/BenLocal/my-bot/internal/worker"

	daprclient "github.com/dapr/go-sdk/client"
)

func main() {
	var (
		httpAddr    = flag.String("addr", ":9001", "HTTP listen address")
		executor    = flag.String("executor", "fake", "Executor name (fake)")
		account     = flag.String("account", "default", "Account name")
		capacity    = flag.Int("capacity", 2, "Worker capacity (max concurrent attempts)")
		token       = flag.String("token", "", "Bearer token for A2A auth (empty = no auth)")
		artifactDir = flag.String("artifact-dir", filepath.Join(os.TempDir(), "mybot-artifacts"), "Shared artifact root; files go to <root>/<goalID>/r<round>/<stepID>/<name>")
		advertise   = flag.String("advertise-url", "", "Base URL the Runtime uses to reach this worker (default: derived from --addr, e.g. http://localhost:9001)")
		fakeScript  = flag.String("fake-script", "", "Default fake script JSON (result text, fail, delay, etc.)")
		workerID    = flag.String("worker-id", "", "Worker instance ID (default: fake-<executor>-<account>-<port>)")
		storeAddr   = flag.String("store-addr", "", "Dapr sidecar gRPC address (empty = in-memory store)")
		storeName   = flag.String("store-name", "mybot-state", "Dapr state store name")
		pubsubName  = flag.String("pubsub-name", "mybot-pubsub", "Dapr pubsub component name")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if *workerID == "" {
		*workerID = deriveWorkerID(*executor, *account, *httpAddr)
	}

	if *advertise == "" {
		*advertise = deriveAdvertiseURL(*httpAddr)
	}
	*advertise = strings.TrimRight(*advertise, "/")

	artifactRoot, err := filepath.Abs(*artifactDir)
	if err != nil {
		logger.Error("invalid --artifact-dir", "error", err)
		os.Exit(1)
	}

	// Create fake driver options.
	fakeOpts := []fake.Option{fake.WithArtifactRoot(artifactRoot), fake.WithWorkerID(*workerID)}
	if *fakeScript != "" {
		var script fake.Script
		if err := json.Unmarshal([]byte(*fakeScript), &script); err != nil {
			logger.Error("invalid --fake-script JSON", "error", err)
			os.Exit(1)
		}
		fakeOpts = append(fakeOpts, fake.WithDefaultScript(script))
	}

	// Create storage backend. Without --store-addr we fall back to an in-memory
	// store for local development.
	var daprClient daprclient.Client
	var st store.Store = store.NewMemStore()
	if *storeAddr != "" {
		c, err := daprclient.NewClientWithAddress(*storeAddr)
		if err != nil {
			logger.Error("worker: create dapr client", "error", err)
			os.Exit(1)
		}
		daprClient = c
		st = store.NewDaprStore(c, *storeName)
	}

	// Create pub/sub publisher (Dapr when connected, no-op otherwise).
	var pubsub worker.PubSubPublisher = worker.NewNoopPublisher(logger)
	if daprClient != nil {
		pubsub = worker.NewDaprPublisher(daprClient, *pubsubName)
	}

	// Create worker service.
	var opts []worker.Option
	opts = append(opts, worker.WithWorkerID(*workerID), worker.WithStore(st), worker.WithPubSub(pubsub),
		worker.WithLogger(logger), worker.WithFakeOptions(fakeOpts...))
	svc := worker.NewService(opts...)

	// Startup recovery: reconcile non-terminal attempts left by a previous run.
	rctx, rcancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := svc.Recover(rctx); err != nil {
		logger.Error("worker: recover failed", "error", err)
	}
	rcancel()

	// Register and start heartbeats.
	reg := worker.NewRegistry(st, core.WorkerRecord{
		ID:          *workerID,
		Executor:    *executor,
		Account:     *account,
		Endpoint:    *advertise,
		Capacity:    *capacity,
		Version:     "0.1.0",
		HeartbeatAt: time.Now().UTC(),
		AuthOK:      true,
	}, logger)

	regCtx, regCancel := context.WithCancel(context.Background())
	defer regCancel()

	if err := reg.Register(regCtx); err != nil {
		logger.Error("worker: register failed", "error", err)
		os.Exit(1)
	}
	reg.StartHeartbeat(regCtx, 15*time.Second)

	// Create A2A server.
	card := &a2ago.AgentCard{
		Name:    fmt.Sprintf("mybot-worker-%s-%s", *executor, *account),
		Version: "0.1.0",
		Capabilities: a2ago.AgentCapabilities{
			Streaming:         true,
			PushNotifications: false,
		},
		SupportedInterfaces: []*a2ago.AgentInterface{
			a2ago.NewAgentInterface(*advertise+"/a2a", a2ago.TransportProtocolJSONRPC),
		},
		DefaultInputModes:    []string{"text/plain"},
		DefaultOutputModes:   []string{"text/plain"},
		SecuritySchemes:      a2ago.NamedSecuritySchemes{"mybotInternal": a2ago.HTTPAuthSecurityScheme{Scheme: "Bearer"}},
		SecurityRequirements: a2ago.SecurityRequirementsOptions{{"mybotInternal": {}}},
	}

	server := a2a.NewServer(svc, card, *token)
	handler := server.Handler()

	// Start HTTP server.
	httpServer := &http.Server{
		Addr:    *httpAddr,
		Handler: handler,
	}

	go func() {
		logger.Info("worker: starting HTTP server",
			"addr", *httpAddr,
			"executor", *executor,
			"account", *account,
			"capacity", *capacity,
			"workerID", *workerID,
			"advertiseURL", *advertise,
			"artifactRoot", artifactRoot,
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("worker: HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("worker: shutdown signal received")

	// 优雅停机顺序（spec §9.2）：注销注册 → 拒绝新 SendMessage → 等在途执行或
	// 超时 → 非终态记录保留给下次启动的 Recover → 关闭 HTTP 与 client。
	// 等待期间 HTTP 继续服务，Runtime 仍可按 attemptKey 查询。
	regCancel() // 停止心跳，避免注销后又被写回。
	unCtx, unCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := reg.Unregister(unCtx); err != nil {
		logger.Error("worker: unregister failed", "error", err)
	}
	unCancel()

	svc.Drain()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := svc.WaitInflight(drainCtx); err != nil {
		logger.Warn("worker: drain timed out; non-terminal attempts kept for Recover", "error", err)
	}
	drainCancel()

	// Shut down HTTP server.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("worker: HTTP server shutdown error", "error", err)
	}

	if daprClient != nil {
		_ = daprClient.Shutdown(context.Background())
	}

	logger.Info("worker: stopped")
}

// deriveWorkerID builds a stable default worker ID from executor/account/port.
func deriveWorkerID(executor, account, addr string) string {
	port := strings.TrimPrefix(addr, ":")
	if port == "" {
		port = "0"
	}
	return fmt.Sprintf("fake-%s-%s-%s", executor, account, port)
}

// deriveAdvertiseURL turns a listen address into a base URL reachable from the
// same host: ":9101" / "0.0.0.0:9101" → "http://localhost:9101".
func deriveAdvertiseURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port)
}
