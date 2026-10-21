package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/BenLocal/my-bot/internal/core"
)

func sampleGoalState(id string) core.GoalState {
	return core.GoalState{
		ID:        id,
		Kind:      core.KindGoal,
		Status:    core.GoalQueued,
		Round:     0,
		UpdatedAt: time.Now().UTC(),
	}
}

func TestSaveLoadGoalProjection(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	p := GoalProjection{GoalState: sampleGoalState("g_1"), EventSeq: 3}
	if err := SaveGoalProjection(ctx, s, "g_1", p); err != nil {
		t.Fatalf("SaveGoalProjection: %v", err)
	}
	got, err := LoadGoalProjection(ctx, s, "g_1")
	if err != nil {
		t.Fatalf("LoadGoalProjection: %v", err)
	}
	if got.ID != "g_1" || got.EventSeq != 3 || got.Status != core.GoalQueued {
		t.Fatalf("projection=%+v", got)
	}
}

func TestAppendEvent(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	// First event on a brand-new goal.
	if err := AppendEvent(ctx, s, "g_1", core.Event{Type: "goal.created"}); err != nil {
		t.Fatalf("AppendEvent #1: %v", err)
	}
	// Second event allocates seq=2.
	if err := AppendEvent(ctx, s, "g_1", core.Event{Type: "round.started"}); err != nil {
		t.Fatalf("AppendEvent #2: %v", err)
	}

	ev1, err := LoadEvent(ctx, s, "g_1", 1)
	if err != nil || ev1.Seq != 1 || ev1.Type != "goal.created" {
		t.Fatalf("event1=%+v err=%v", ev1, err)
	}
	ev2, err := LoadEvent(ctx, s, "g_1", 2)
	if err != nil || ev2.Seq != 2 || ev2.Type != "round.started" {
		t.Fatalf("event2=%+v err=%v", ev2, err)
	}

	p, err := LoadGoalProjection(ctx, s, "g_1")
	if err != nil {
		t.Fatalf("LoadGoalProjection: %v", err)
	}
	if p.EventSeq != 2 {
		t.Fatalf("EventSeq=%d", p.EventSeq)
	}
}

func TestAppendEventAfterProjection(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	// Pre-seed a projection with EventSeq=5.
	p := GoalProjection{GoalState: sampleGoalState("g_2"), EventSeq: 5}
	if err := SaveGoalProjection(ctx, s, "g_2", p); err != nil {
		t.Fatalf("SaveGoalProjection: %v", err)
	}

	if err := AppendEvent(ctx, s, "g_2", core.Event{Type: "round.started"}); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	ev, err := LoadEvent(ctx, s, "g_2", 6)
	if err != nil || ev.Seq != 6 {
		t.Fatalf("event=%+v err=%v", ev, err)
	}
	got, _ := LoadGoalProjection(ctx, s, "g_2")
	if got.EventSeq != 6 {
		t.Fatalf("EventSeq=%d", got.EventSeq)
	}
}

func TestSaveLoadEvent(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	ev := core.Event{Seq: 7, Type: "attempt.finished", At: time.Now().UTC(), Data: map[string]string{"x": "y"}}
	if err := SaveEvent(ctx, s, "g_3", ev); err != nil {
		t.Fatalf("SaveEvent: %v", err)
	}
	got, err := LoadEvent(ctx, s, "g_3", 7)
	if err != nil {
		t.Fatalf("LoadEvent: %v", err)
	}
	if got.Type != "attempt.finished" {
		t.Fatalf("event=%+v", got)
	}
}

func TestCreateIdem(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	existing, created, err := CreateIdem(ctx, s, "key-1", "g_1")
	if err != nil || !created || existing != "" {
		t.Fatalf("CreateIdem #1: existing=%q created=%v err=%v", existing, created, err)
	}

	// Duplicate returns the original goal ID.
	existing, created, err = CreateIdem(ctx, s, "key-1", "g_2")
	if err != nil || created || existing != "g_1" {
		t.Fatalf("CreateIdem #2: existing=%q created=%v err=%v", existing, created, err)
	}
}

func TestAttemptRecord(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	rec := AttemptRecord{Status: core.AttemptSubmitting}
	created, err := CreateAttempt(ctx, s, "w1", "r1:s1:a1:hash", rec)
	if err != nil || !created {
		t.Fatalf("CreateAttempt #1: created=%v err=%v", created, err)
	}

	// Owner-matched retry is a no-op success.
	created, err = CreateAttempt(ctx, s, "w1", "r1:s1:a1:hash", rec)
	if err != nil || created {
		t.Fatalf("CreateAttempt #2: created=%v err=%v", created, err)
	}

	loaded, err := LoadAttempt(ctx, s, "w1", "r1:s1:a1:hash")
	if err != nil {
		t.Fatalf("LoadAttempt: %v", err)
	}
	if loaded.Owner != "r1:s1:a1:hash|w1" || loaded.Worker != "w1" || loaded.Status != core.AttemptSubmitting {
		t.Fatalf("record=%+v", loaded)
	}

	loaded.Status = core.AttemptCompleted
	if err := SaveAttempt(ctx, s, "w1", "r1:s1:a1:hash", loaded); err != nil {
		t.Fatalf("SaveAttempt: %v", err)
	}
	got, err := LoadAttempt(ctx, s, "w1", "r1:s1:a1:hash")
	if err != nil || got.Status != core.AttemptCompleted {
		t.Fatalf("after save: %+v err=%v", got, err)
	}
}

func TestRegistryRecord(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	rec := core.WorkerRecord{
		ID:          "w1",
		Executor:    "pi",
		Account:     "default",
		Endpoint:    "http://worker:8080",
		Capacity:    2,
		Version:     "1.0",
		HeartbeatAt: time.Now().UTC(),
		AuthOK:      true,
	}
	if err := SaveRegistryRecord(ctx, s, rec); err != nil {
		t.Fatalf("SaveRegistryRecord: %v", err)
	}
	got, err := LoadRegistryRecord(ctx, s, "pi", "w1")
	if err != nil || got.ID != "w1" || got.Executor != "pi" {
		t.Fatalf("LoadRegistryRecord: %+v err=%v", got, err)
	}

	if err := AddRegistryIndex(ctx, s, "pi", "w1"); err != nil {
		t.Fatalf("AddRegistryIndex: %v", err)
	}
	idxItem, err := s.Get(ctx, "registry:pi:_index")
	if err != nil {
		t.Fatalf("Get index: %v", err)
	}
	var ids []string
	_ = json.Unmarshal(idxItem.Value, &ids)
	if len(ids) != 1 || ids[0] != "w1" {
		t.Fatalf("index=%v", ids)
	}

	if err := RemoveRegistryIndex(ctx, s, "pi", "w1"); err != nil {
		t.Fatalf("RemoveRegistryIndex: %v", err)
	}
	idxItem, _ = s.Get(ctx, "registry:pi:_index")
	_ = json.Unmarshal(idxItem.Value, &ids)
	if len(ids) != 0 {
		t.Fatalf("index after remove=%v", ids)
	}

	if err := DeleteRegistryRecord(ctx, s, "pi", "w1"); err != nil {
		t.Fatalf("DeleteRegistryRecord: %v", err)
	}
	if _, err := LoadRegistryRecord(ctx, s, "pi", "w1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load after delete: want ErrNotFound, got %v", err)
	}
}

func TestArtifact(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	ref := core.ArtifactRef{
		Path:    "mybot-artifact://g_1/r1/s1:coder:1/out.md",
		Sha256:  "abc123",
		Size:    42,
		Round:   1,
		Attempt: 1,
	}
	if err := SaveArtifact(ctx, s, "g_1", 1, "s1:coder:1", "out.md", ref); err != nil {
		t.Fatalf("SaveArtifact: %v", err)
	}
	got, err := LoadArtifact(ctx, s, "g_1", 1, "s1:coder:1", "out.md")
	if err != nil {
		t.Fatalf("LoadArtifact: %v", err)
	}
	if got.Sha256 != "abc123" || got.Path != ref.Path {
		t.Fatalf("artifact=%+v", got)
	}
}
