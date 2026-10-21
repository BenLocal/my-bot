package worker

import (
	"context"

	"github.com/BenLocal/my-bot/internal/core"
	"github.com/BenLocal/my-bot/internal/executor"
	"github.com/BenLocal/my-bot/internal/store"
)

// Recover scans this worker's non-terminal attempt records and reconciles each
// one via driver.Recover (docs/spec.md §7.2).
//
// The store has no List method, so this implementation scans the in-memory
// attempt-key set maintained by Submit. That covers attempts started in the
// current process; a full cross-restart sweep would need a store-backed
// attempt index (see the task brief's simplification note).
func (s *Service) Recover(ctx context.Context) error {
	if s.store == nil {
		return nil
	}

	s.mu.Lock()
	keys := make([]string, 0, len(s.byKey))
	for k := range s.byKey {
		keys = append(keys, k)
	}
	s.mu.Unlock()

	for _, key := range keys {
		rec, err := store.LoadAttempt(ctx, s.store, s.workerID, key)
		if err != nil {
			// Missing records are expected for expired/removed entries; skip.
			continue
		}
		if isTerminalAttemptStatus(rec.Status) {
			continue
		}

		// The fake driver only keys its recover script on AttemptKey; the other
		// fields are left empty because store.AttemptRecord does not persist
		// them.
		ar := executor.AttemptRecord{
			AttemptKey:   rec.Key,
			SessionID:    rec.SessionID,
			RemoteTaskID: rec.RemoteTaskID,
		}
		st, err := s.driver.Recover(ctx, ar)
		if err != nil {
			s.logger.Error("worker: recover attempt failed",
				"attemptKey", rec.Key, "error", err)
			continue
		}

		rec.Status = mapPhaseToAttemptStatus(st.Phase)
		rec.Usage = st.Usage
		if err := store.SaveAttempt(ctx, s.store, s.workerID, key, rec); err != nil {
			s.logger.Error("worker: recover save attempt failed",
				"attemptKey", rec.Key, "error", err)
		}
		s.logger.Info("worker: recovered attempt",
			"attemptKey", rec.Key,
			"phase", st.Phase,
			"retryable", st.Retryable,
		)
	}
	return nil
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
