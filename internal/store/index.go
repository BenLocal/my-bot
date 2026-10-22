package store

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"time"
)

const (
	// mutateMinBackoff and mutateMaxBackoff bound the full-jitter backoff used
	// by MutateIndex (docs/spec.md §6).
	mutateMinBackoff = 50 * time.Millisecond
	mutateMaxBackoff = 1 * time.Second
	// mutateTotalBudget caps the total time spent retrying an index mutation.
	mutateTotalBudget = 10 * time.Second
)

// MutateIndex applies fn to the JSON-encoded []string stored at key, then
// conditionally writes the result back using the ETag read before the callback.
// If fn reports changed=false, nothing is written. On ETag conflict it retries
// with full-jitter exponential backoff (50ms→1s) within a 10s total budget.
func MutateIndex(ctx context.Context, s Store, key string, fn func(ids []string) ([]string, bool)) error {
	deadline := time.Now().Add(mutateTotalBudget)
	for {
		item, err := s.Get(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}

		var ids []string
		if err == nil {
			if uerr := json.Unmarshal(item.Value, &ids); uerr != nil {
				return uerr
			}
		}

		newIDs, changed := fn(ids)
		if !changed {
			return nil
		}
		data, err := json.Marshal(newIDs)
		if err != nil {
			return err
		}

		if werr := writeIndex(ctx, s, key, data, item); werr == nil {
			return nil
		} else if !isConflict(werr) {
			return werr
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ErrConflict
		}
		delay := fullJitter(mutateMinBackoff, mutateMaxBackoff)
		if delay > remaining {
			delay = remaining
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// writeIndex writes the new index value conditionally (spec §6): with the
// ETag read before the callback, or as a first-write when the index did not
// exist yet. A lost race surfaces as a conflict and MutateIndex retries.
func writeIndex(ctx context.Context, s Store, key string, data []byte, cur *Item) error {
	if cur != nil {
		return s.Set(ctx, key, data, cur.ETag)
	}
	created, err := s.FirstWrite(ctx, key, data, "")
	if err != nil {
		return err
	}
	if !created {
		return ErrConflict
	}
	return nil
}

// fullJitter returns a random duration in [min, max).
func fullJitter(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

// PruneIndex removes IDs for which exists returns false. It is a thin wrapper
// over MutateIndex used by the periodic reconciliation task (docs/spec.md §6).
func PruneIndex(ctx context.Context, s Store, key string, exists func(id string) bool) error {
	return MutateIndex(ctx, s, key, func(ids []string) ([]string, bool) {
		kept := make([]string, 0, len(ids))
		changed := false
		for _, id := range ids {
			if exists(id) {
				kept = append(kept, id)
			} else {
				changed = true
			}
		}
		return kept, changed
	})
}
