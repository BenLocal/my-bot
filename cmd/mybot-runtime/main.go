// mybot-runtime is the Runtime entry point: it wires together the Dapr
// workflow worker, the HTTP API, the Scheduler actor, and the orchestrator
// dependencies.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dapr/durabletask-go/workflow"
	dapractor "github.com/dapr/go-sdk/actor"
	daprclient "github.com/dapr/go-sdk/client"
	daprcommon "github.com/dapr/go-sdk/service/common"
	daprhttp "github.com/dapr/go-sdk/service/http"
	"github.com/go-chi/chi/v5"

	"github.com/BenLocal/my-bot/internal/api"
	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/orchestrator"
	"github.com/BenLocal/my-bot/internal/scheduler"
	"github.com/BenLocal/my-bot/internal/store"
)

func main() {
	var (
		httpPort  = flag.Int("port", 8080, "HTTP API listen port")
		daprPort  = flag.String("dapr-port", "50001", "Dapr gRPC port")
		rolesPath = flag.String("roles", "config/roles.yaml", "roles.yaml path")
		rolesDir  = flag.String("roles-dir", ".", "base directory of role instruction files")
		globalMax = flag.Int("global-max", 0, "global concurrency limit (0 = roles.yaml limits.globalConcurrency)")
		token     = flag.String("token", "", "Bearer token for API auth (empty = no auth)")
		artifacts = flag.String("artifact-dir", "/data/artifacts", "shared artifact root (read-only)")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	rolesData, err := os.ReadFile(*rolesPath)
	if err != nil {
		logger.Error("read roles config", "path", *rolesPath, "error", err)
		os.Exit(1)
	}
	roles, err := core.ParseRolesConfig(rolesData)
	if err != nil {
		logger.Error("invalid roles config", "path", *rolesPath, "error", err)
		os.Exit(1)
	}
	if *globalMax <= 0 {
		*globalMax = roles.Limits.GlobalConcurrency
	}
	if *globalMax <= 0 {
		*globalMax = 4
	}

	daprCl, err := daprclient.NewClientWithPort(*daprPort)
	if err != nil {
		logger.Error("failed to create Dapr client", "error", err)
		os.Exit(1)
	}
	st := store.NewDaprStore(daprCl, "statestore")

	// Scheduler actor: single instance, registry read from the store for every executor.
	actor := scheduler.NewSchedulerActor(*globalMax, 0)
	actor.SetStore(st)
	actor.SetLogger(logger)
	actor.SetAccountLimits(roles.AccountLimits())
	actor.SetGoalStatusChecker(goalStatusChecker{store: st})

	wfClient := workflow.NewClient(daprCl.GrpcClientConn())
	orchestrator.SetWorkflowClient(wfClient)
	actor.SetSignalDeliverer(&wfSignalDeliverer{wfClient: wfClient, logger: logger})

	orchestrator.SetDeps(st, nil, *token)
	orchestrator.SetDaprClient(daprCl)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	wfCtx, wfCancel := context.WithCancel(context.Background())
	defer wfCancel()

	wfDone := make(chan struct{})
	go func() {
		defer close(wfDone)
		logger.Info("starting workflow worker")
		if err := wfClient.StartWorker(wfCtx, orchestrator.NewRegistry()); err != nil {
			logger.Error("workflow worker exited with error", "error", err)
			stop()
		}
	}()

	apiServer := api.NewServer(st, logger)
	apiServer.SetWorkflowClient(wfClient)
	apiServer.SetDaprClient(daprCl)
	apiServer.SetRoles(roles, *rolesDir)
	apiServer.SetArtifactDir(*artifacts)
	if *token != "" {
		apiServer.SetToken(*token)
	}

	// Actor 只能挂在 HTTP app channel，且 Dapr 只在主端口查询 /dapr/config，
	// 所以 API 与 Actor 共用一个 HTTP service（spec §5.1 实现约束）。
	mux := chi.NewMux()
	mux.Mount("/", apiServer.Handler())
	combinedSvc := daprhttp.NewServiceWithMux(fmt.Sprintf(":%d", *httpPort), mux)
	combinedSvc.RegisterActorImplFactoryContext(func() dapractor.ServerContext { return actor })

	go func() {
		logger.Info("starting combined HTTP+actor service", "port", *httpPort)
		if err := combinedSvc.Start(); err != nil {
			logger.Error("combined HTTP service error", "error", err)
			stop()
		}
	}()

	// 1-minute scheduler reminder (persisted by dapr scheduler; re-registering is idempotent).
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
		if err := daprCl.RegisterActorReminder(ctx, &daprclient.RegisterActorReminderRequest{
			ActorType: scheduler.ActorTypeScheduler,
			ActorID:   scheduler.ActorIDScheduler,
			Name:      scheduler.ReminderName,
			DueTime:   "0s",
			Period:    "60s",
		}); err != nil {
			logger.Warn("failed to register actor reminder", "error", err)
		}
	}()

	go pruneIndexesLoop(ctx, st, logger)

	subHandler := orchestrator.NewSubscriptionHandler(wfClient, logger)
	subCloser, err := daprCl.SubscribeWithHandler(ctx, daprclient.SubscriptionOptions{
		PubsubName: "pubsub",
		Topic:      "attempt-events",
	}, func(event *daprcommon.TopicEvent) daprcommon.SubscriptionResponseStatus {
		data, ok := event.Data.([]byte)
		if !ok {
			data = event.RawData
		}
		switch subHandler.Handle(ctx, event.Topic, data) {
		case orchestrator.SubscriptionDrop:
			return daprcommon.SubscriptionResponseStatusDrop
		case orchestrator.SubscriptionRetry:
			return daprcommon.SubscriptionResponseStatusRetry
		default:
			return daprcommon.SubscriptionResponseStatusSuccess
		}
	})
	if err != nil {
		logger.Warn("failed to subscribe to attempt-events", "error", err)
	}

	<-ctx.Done()
	logger.Info("shutdown signal received, stopping...")

	// 优雅停机顺序（spec §9.2）：停 HTTP 与订阅 → 等进行中的 Activity（上限 30s）→ 停 Workflow worker → 关 Dapr client。
	if err := combinedSvc.GracefulStop(); err != nil {
		logger.Error("HTTP service shutdown error", "error", err)
	}
	if subCloser != nil {
		_ = subCloser()
	}
	wfCancel()
	select {
	case <-wfDone:
	case <-time.After(30 * time.Second):
		logger.Warn("workflow worker did not stop within 30s")
	}
	daprCl.Close()
	logger.Info("Runtime stopped")
}

// wfSignalDeliverer delivers slot-granted signals to workflow instances.
type wfSignalDeliverer struct {
	wfClient *workflow.Client
	logger   *slog.Logger
}

func (d *wfSignalDeliverer) DeliverSlotGranted(ctx context.Context, instanceID string, attemptKey string, lease *scheduler.Lease) error {
	signal := map[string]any{
		"kind": "slot-granted",
		"key":  attemptKey,
		"data": map[string]any{
			"lease": map[string]any{
				"workerID":      lease.WorkerID,
				"executor":      lease.Executor,
				"account":       lease.Account,
				"sessionPolicy": lease.SessionPolicy,
			},
		},
	}
	err := d.wfClient.RaiseEvent(ctx, instanceID, "signal", workflow.WithEventPayload(signal))
	if err != nil {
		d.logger.Warn("signal deliverer: RaiseEvent failed",
			"instanceID", instanceID, "attemptKey", attemptKey, "error", err)
	}
	return err
}

// goalStatusChecker lets the scheduler reminder reclaim leases of terminal goals.
type goalStatusChecker struct {
	store store.Store
}

func (c goalStatusChecker) GoalTerminal(ctx context.Context, goalID string) (bool, error) {
	proj, err := store.LoadGoalProjection(ctx, c.store, goalID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// 投影总在 Workflow 启动前写入；没有投影的 lease 只可能是残留（如被清空的环境），按已结束回收。
			return true, nil
		}
		return false, err
	}
	switch proj.Status {
	case core.GoalSucceeded, core.GoalFailed, core.GoalCancelled, core.GoalCompleted:
		return true, nil
	}
	return false, nil
}

// pruneIndexesLoop removes index entries pointing at expired records every 5 minutes (spec §6).
func pruneIndexesLoop(ctx context.Context, st store.Store, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, executor := range []string{"pi", "codex", "fake"} {
			key := "registry:" + executor + ":_index"
			err := store.PruneIndex(ctx, st, key, func(id string) bool {
				_, err := store.LoadRegistryRecord(ctx, st, executor, id)
				return !errors.Is(err, store.ErrNotFound)
			})
			if err != nil {
				logger.Warn("prune registry index failed", "key", key, "error", err)
			}
		}
	}
}
