package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
)

// registryTTL is the heartbeat TTL for worker registry records (docs/spec.md §6).
const registryTTL = 60 * time.Second

// ----- key builders (docs/spec.md §6) -----

func goalStateKey(id string) string { return "goal:" + id }

func goalEventKey(goalID string, seq int) string {
	return fmt.Sprintf("goal:%s:events:%d", goalID, seq)
}

func goalActiveIndexKey() string { return "goal:_index:active" }
func goalAllIndexKey() string    { return "goal:_index:all" }

func idemKey(clientRequestID string) string { return "idem:create:" + clientRequestID }

func registryIndexKey(executor string) string { return "registry:" + executor + ":_index" }

func registryRecordKey(executor, workerID string) string {
	return "registry:" + executor + ":" + workerID
}

func attemptRecordKey(workerID, attemptKey string) string {
	return "worker:" + workerID + ":attempt:" + attemptKey
}

func attemptOwner(workerID, attemptKey string) string {
	return attemptKey + "|" + workerID
}

func artifactKey(goalID string, round int, stepID, name string) string {
	return fmt.Sprintf("goal:%s:artifact:r%d/%s/%s", goalID, round, stepID, name)
}

// GoalProjection is the projected Goal record plus the event-sequence counter.
// docs/spec.md §6 states that event seq is allocated from the counter stored
// inside goal:<id>, so the counter travels with the projection.
type GoalProjection struct {
	core.GoalState
	EventSeq int `json:"eventSeq,omitempty"`
}

// SaveGoalProjection writes the projection. It is a last-write write; callers
// that need the counter to move atomically use AppendEvent instead.
func SaveGoalProjection(ctx context.Context, s Store, id string, p GoalProjection) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return s.Set(ctx, goalStateKey(id), data, "")
}

// LoadGoalProjection reads the projection.
func LoadGoalProjection(ctx context.Context, s Store, id string) (GoalProjection, error) {
	item, err := s.Get(ctx, goalStateKey(id))
	if err != nil {
		return GoalProjection{}, err
	}
	var p GoalProjection
	if err := json.Unmarshal(item.Value, &p); err != nil {
		return GoalProjection{}, err
	}
	return p, nil
}

// AppendEvent appends an event to the goal event stream, allocating its sequence
// from the counter in goal:<id>. Event writes are idempotent by (goalID, seq):
// a replay or retry overwrites the same key rather than appending (docs/spec.md
// §10). The projection counter is updated with an ETag conditional write and
// retried with full-jitter backoff on conflict.
//
// This method assumes a single writer per goal (the orchestrator's per-round
// Activity), which is how the spec's "sequential write + idempotent + reconcile"
// model is meant to operate.
func AppendEvent(ctx context.Context, s Store, goalID string, ev core.Event) error {
	key := goalStateKey(goalID)
	deadline := time.Now().Add(mutateTotalBudget)
	for {
		item, err := s.Get(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}

		var p GoalProjection
		if err == nil {
			if uerr := json.Unmarshal(item.Value, &p); uerr != nil {
				return uerr
			}
		}

		seq := p.EventSeq + 1
		ev.Seq = seq
		ev.At = time.Now().UTC()
		eventData, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		if err := s.Set(ctx, goalEventKey(goalID, seq), eventData, ""); err != nil {
			return err
		}

		p.EventSeq = seq
		projData, err := json.Marshal(p)
		if err != nil {
			return err
		}

		var writeErr error
		if item != nil && item.ETag != "" {
			writeErr = s.Set(ctx, key, projData, item.ETag)
		} else {
			_, writeErr = s.FirstWrite(ctx, key, projData, "")
		}
		if writeErr == nil {
			return nil
		}
		if !isConflict(writeErr) {
			return writeErr
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

// SaveEvent writes a single event by its sequence. It is idempotent: repeated
// writes with the same seq overwrite.
func SaveEvent(ctx context.Context, s Store, goalID string, ev core.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return s.Set(ctx, goalEventKey(goalID, ev.Seq), data, "")
}

// LoadEvent reads a single event by its sequence.
func LoadEvent(ctx context.Context, s Store, goalID string, seq int) (core.Event, error) {
	item, err := s.Get(ctx, goalEventKey(goalID, seq))
	if err != nil {
		return core.Event{}, err
	}
	var ev core.Event
	if err := json.Unmarshal(item.Value, &ev); err != nil {
		return core.Event{}, err
	}
	return ev, nil
}

// CreateIdem performs the create-dedup for clientRequestID (docs/spec.md §8). On
// a duplicate it reads back and returns the existing goal ID with created=false.
func CreateIdem(ctx context.Context, s Store, clientRequestID, goalID string) (existing string, created bool, err error) {
	key := idemKey(clientRequestID)
	data, err := json.Marshal(goalID)
	if err != nil {
		return "", false, err
	}

	created, err = s.FirstWrite(ctx, key, data, "")
	if err != nil && !errors.Is(err, ErrConflict) {
		return "", false, err
	}
	if created {
		return "", true, nil
	}

	// Duplicate request (or owner-matched retry): read back the existing goal ID.
	item, gerr := s.Get(ctx, key)
	if gerr != nil {
		return "", false, gerr
	}
	var existingGoal string
	if uerr := json.Unmarshal(item.Value, &existingGoal); uerr != nil {
		return "", false, uerr
	}
	return existingGoal, false, nil
}

// AttemptRecord is the worker-side attempt record stored under
// worker:<workerID>:attempt:<key> (docs/spec.md §6, §7.2).
type AttemptRecord struct {
	Owner        string             `json:"owner"`
	Key          string             `json:"key"`
	Worker       string             `json:"worker"`
	RemoteTaskID string             `json:"remoteTaskID,omitempty"`
	SessionID    string             `json:"sessionID,omitempty"`
	Status       core.AttemptStatus `json:"status"`
	Usage        core.Usage         `json:"usage,omitempty"`
	Artifacts    []core.ArtifactRef `json:"artifacts,omitempty"`
	UpdatedAt    time.Time          `json:"updatedAt"`
}

// CreateAttempt performs the first-write dedup for a new attempt. On success it
// returns created=true. If the attempt already exists and its owner matches (same
// attemptKey + worker), it returns created=false with no error so the caller can
// read the existing record back.
func CreateAttempt(ctx context.Context, s Store, workerID, attemptKey string, rec AttemptRecord) (bool, error) {
	rec.Owner = attemptOwner(workerID, attemptKey)
	rec.Worker = workerID
	rec.Key = attemptKey
	rec.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(rec)
	if err != nil {
		return false, err
	}
	return s.FirstWrite(ctx, attemptRecordKey(workerID, attemptKey), data, rec.Owner)
}

// SaveAttempt updates an existing attempt record (non-conditional last write).
func SaveAttempt(ctx context.Context, s Store, workerID, attemptKey string, rec AttemptRecord) error {
	rec.Owner = attemptOwner(workerID, attemptKey)
	rec.Worker = workerID
	rec.Key = attemptKey
	rec.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.Set(ctx, attemptRecordKey(workerID, attemptKey), data, "")
}

// LoadAttempt reads an attempt record.
func LoadAttempt(ctx context.Context, s Store, workerID, attemptKey string) (AttemptRecord, error) {
	item, err := s.Get(ctx, attemptRecordKey(workerID, attemptKey))
	if err != nil {
		return AttemptRecord{}, err
	}
	var rec AttemptRecord
	if err := json.Unmarshal(item.Value, &rec); err != nil {
		return AttemptRecord{}, err
	}
	return rec, nil
}

// SaveRegistryRecord writes a worker registry record with a 60s heartbeat TTL.
func SaveRegistryRecord(ctx context.Context, s Store, rec core.WorkerRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.SetTTL(ctx, registryRecordKey(rec.Executor, rec.ID), data, registryTTL)
}

// LoadRegistryRecord reads a worker registry record.
func LoadRegistryRecord(ctx context.Context, s Store, executor, workerID string) (core.WorkerRecord, error) {
	item, err := s.Get(ctx, registryRecordKey(executor, workerID))
	if err != nil {
		return core.WorkerRecord{}, err
	}
	var rec core.WorkerRecord
	if err := json.Unmarshal(item.Value, &rec); err != nil {
		return core.WorkerRecord{}, err
	}
	return rec, nil
}

// DeleteRegistryRecord removes a worker registry record.
func DeleteRegistryRecord(ctx context.Context, s Store, executor, workerID string) error {
	return s.Delete(ctx, registryRecordKey(executor, workerID), "")
}

// AddRegistryIndex adds workerID to the executor registry index (idempotent).
func AddRegistryIndex(ctx context.Context, s Store, executor, workerID string) error {
	return MutateIndex(ctx, s, registryIndexKey(executor), func(ids []string) ([]string, bool) {
		for _, id := range ids {
			if id == workerID {
				return ids, false
			}
		}
		return append(ids, workerID), true
	})
}

// RemoveRegistryIndex removes workerID from the executor registry index.
func RemoveRegistryIndex(ctx context.Context, s Store, executor, workerID string) error {
	return MutateIndex(ctx, s, registryIndexKey(executor), func(ids []string) ([]string, bool) {
		kept := make([]string, 0, len(ids))
		changed := false
		for _, id := range ids {
			if id == workerID {
				changed = true
				continue
			}
			kept = append(kept, id)
		}
		return kept, changed
	})
}

// SaveArtifact registers an artifact under
// goal:<id>:artifact:r<round>/<stepID>/<name>.
func SaveArtifact(ctx context.Context, s Store, goalID string, round int, stepID, name string, ref core.ArtifactRef) error {
	data, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	return s.Set(ctx, artifactKey(goalID, round, stepID, name), data, "")
}

// LoadArtifact reads an artifact registration.
func LoadArtifact(ctx context.Context, s Store, goalID string, round int, stepID, name string) (core.ArtifactRef, error) {
	item, err := s.Get(ctx, artifactKey(goalID, round, stepID, name))
	if err != nil {
		return core.ArtifactRef{}, err
	}
	var ref core.ArtifactRef
	if err := json.Unmarshal(item.Value, &ref); err != nil {
		return core.ArtifactRef{}, err
	}
	return ref, nil
}
