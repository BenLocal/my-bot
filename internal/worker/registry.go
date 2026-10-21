package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/store"
)

// Registry manages a worker's registration record and periodic heartbeats
// (docs/spec.md §5.1, §6).
type Registry struct {
	store  store.Store
	logger *slog.Logger

	mu  sync.Mutex
	rec core.WorkerRecord
}

// NewRegistry creates a registry for one worker instance.
func NewRegistry(st store.Store, rec core.WorkerRecord, logger *slog.Logger) *Registry {
	if logger == nil {
		logger = slog.Default()
	}
	rec.HeartbeatAt = time.Now().UTC()
	rec.AuthOK = true
	return &Registry{store: st, rec: rec, logger: logger}
}

// Register saves the registry record and adds it to the executor index.
func (r *Registry) Register(ctx context.Context) error {
	if r.store == nil {
		return nil
	}
	if err := store.SaveRegistryRecord(ctx, r.store, r.snapshot()); err != nil {
		return fmt.Errorf("worker: register save record: %w", err)
	}
	if err := store.AddRegistryIndex(ctx, r.store, r.rec.Executor, r.rec.ID); err != nil {
		return fmt.Errorf("worker: register add index: %w", err)
	}
	return nil
}

// Heartbeat updates heartbeatAt and authOK, then re-saves the record with a
// fresh TTL.
func (r *Registry) Heartbeat(ctx context.Context) error {
	if r.store == nil {
		return nil
	}
	r.mu.Lock()
	r.rec.HeartbeatAt = time.Now().UTC()
	r.rec.AuthOK = true
	rec := r.rec
	r.mu.Unlock()

	if err := store.SaveRegistryRecord(ctx, r.store, rec); err != nil {
		return fmt.Errorf("worker: heartbeat save record: %w", err)
	}
	return nil
}

// Unregister removes the registry record first, then the index entry.
func (r *Registry) Unregister(ctx context.Context) error {
	if r.store == nil {
		return nil
	}
	r.mu.Lock()
	executor, id := r.rec.Executor, r.rec.ID
	r.mu.Unlock()

	if err := store.DeleteRegistryRecord(ctx, r.store, executor, id); err != nil {
		return fmt.Errorf("worker: unregister delete record: %w", err)
	}
	if err := store.RemoveRegistryIndex(ctx, r.store, executor, id); err != nil {
		return fmt.Errorf("worker: unregister remove index: %w", err)
	}
	return nil
}

// StartHeartbeat launches the periodic heartbeat goroutine. It stops when ctx is
// done. The caller should not call it with a context that is canceled before
// the heartbeat interval elapses if liveness matters.
func (r *Registry) StartHeartbeat(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.Heartbeat(ctx); err != nil {
					r.logger.Error("worker: heartbeat failed", "error", err, "workerID", r.rec.ID)
				}
			}
		}
	}()
}

// snapshot returns a copy of the current record.
func (r *Registry) snapshot() core.WorkerRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rec
}
