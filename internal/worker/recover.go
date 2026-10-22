package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/executor"
	"github.com/BenLocal/my-bot/internal/store"
)

// Recover reconciles this worker's non-terminal attempt records left by a
// previous process (docs/spec.md §7.2). It scans the store-backed attempt index
// worker:<id>:attempts:_index, calls driver.Recover for each non-terminal
// record, saves the resulting status and publishes attempt-events. Records
// that are terminal or missing are dropped from the index.
func (s *Service) Recover(ctx context.Context) error {
	if s.store == nil {
		return nil
	}
	keys, err := s.listIndex(ctx)
	if err != nil {
		return fmt.Errorf("worker: recover list index: %w", err)
	}

	for _, key := range keys {
		rec, err := store.LoadAttempt(ctx, s.store, s.workerID, key)
		if errors.Is(err, store.ErrNotFound) {
			s.dropIndex(ctx, key)
			continue
		}
		if err != nil {
			s.logger.Error("worker: recover load attempt", "attemptKey", key, "error", err)
			continue
		}
		if isTerminalAttemptStatus(rec.Status) {
			s.dropIndex(ctx, key)
			continue
		}

		st, err := s.driver.Recover(ctx, executor.AttemptRecord{
			AttemptKey:   key,
			GoalID:       rec.GoalID,
			Round:        rec.Round,
			StepID:       rec.StepID,
			Role:         rec.Role,
			SessionID:    rec.SessionID,
			RemoteTaskID: rec.RemoteTaskID,
		})
		if err != nil {
			s.logger.Error("worker: recover attempt failed", "attemptKey", key, "error", err)
			continue
		}

		status := mapPhaseToAttemptStatus(st.Phase)
		if !isTerminalAttemptStatus(status) {
			// 续接执行需要真实会话；fake 无法续跑，保留记录等待下次处理。
			s.logger.Warn("worker: recover left attempt non-terminal",
				"attemptKey", key, "phase", st.Phase)
			continue
		}

		rec.Status = status
		rec.Usage = st.Usage
		rec.Reason = st.Reason
		rec.Retryable = st.Retryable
		if err := store.SaveAttempt(ctx, s.store, s.workerID, key, rec); err != nil {
			s.logger.Error("worker: recover save attempt failed", "attemptKey", key, "error", err)
			continue
		}
		s.dropIndex(ctx, key)
		s.publishAttemptEvent(ctx, key, rec.GoalID, status)
		s.logger.Info("worker: recovered attempt",
			"attemptKey", key,
			"status", status,
			"reason", st.Reason,
			"retryable", st.Retryable,
		)
	}
	return nil
}

func (s *Service) dropIndex(ctx context.Context, key string) {
	if err := s.removeFromIndex(ctx, key); err != nil {
		s.logger.Error("worker: recover remove index", "attemptKey", key, "error", err)
	}
}

// isTerminalAttemptStatus reports whether an attempt status is terminal.
func isTerminalAttemptStatus(status core.AttemptStatus) bool {
	switch status {
	case core.AttemptCompleted, core.AttemptFailed, core.AttemptRejected,
		core.AttemptCancelled, core.AttemptInterrupted:
		return true
	default:
		return false
	}
}
