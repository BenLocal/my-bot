package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestMutateIndexAdd(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	err := MutateIndex(ctx, s, "registry:pi:_index", func(ids []string) ([]string, bool) {
		return append(ids, "w1"), true
	})
	if err != nil {
		t.Fatalf("MutateIndex add: %v", err)
	}

	item, err := s.Get(ctx, "registry:pi:_index")
	if err != nil {
		t.Fatalf("Get index: %v", err)
	}
	var ids []string
	if err := json.Unmarshal(item.Value, &ids); err != nil {
		t.Fatalf("unmarshal index: %v", err)
	}
	if len(ids) != 1 || ids[0] != "w1" {
		t.Fatalf("index=%v", ids)
	}
}

func TestMutateIndexNoChangeNoWrite(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	if err := s.Set(ctx, "idx", []byte(`["w1"]`), ""); err != nil {
		t.Fatalf("Set: %v", err)
	}
	before, _ := s.Get(ctx, "idx")

	err := MutateIndex(ctx, s, "idx", func(ids []string) ([]string, bool) {
		return ids, false
	})
	if err != nil {
		t.Fatalf("MutateIndex no-change: %v", err)
	}
	after, _ := s.Get(ctx, "idx")
	if before.ETag != after.ETag {
		t.Fatalf("no-change should not rewrite: etag %q -> %q", before.ETag, after.ETag)
	}
}

func TestMutateIndexIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	// Add w1, then add w1 again — no duplicate.
	if err := MutateIndex(ctx, s, "idx", func(ids []string) ([]string, bool) {
		return append(ids, "w1"), true
	}); err != nil {
		t.Fatalf("add w1: %v", err)
	}
	if err := MutateIndex(ctx, s, "idx", func(ids []string) ([]string, bool) {
		for _, id := range ids {
			if id == "w1" {
				return ids, false
			}
		}
		return append(ids, "w1"), true
	}); err != nil {
		t.Fatalf("add w1 again: %v", err)
	}

	item, _ := s.Get(ctx, "idx")
	var ids []string
	_ = json.Unmarshal(item.Value, &ids)
	if len(ids) != 1 {
		t.Fatalf("ids=%v", ids)
	}
}

func TestMutateIndexConflictRetries(t *testing.T) {
	ctx := context.Background()
	base := newFakeStore()

	// Pre-seed a value so a conditional write can conflict.
	if err := base.Set(ctx, "idx", []byte(`["w1"]`), ""); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Wrap the store so the first conditional Set on "idx" fails once, then
	// MutateIndex must retry with backoff and succeed.
	s := &conflictOnceStore{fakeStore: base, key: "idx"}

	attempts := 0
	err := MutateIndex(ctx, s, "idx", func(ids []string) ([]string, bool) {
		attempts++
		return append(ids, "w2"), true
	})
	if err != nil {
		t.Fatalf("MutateIndex conflict retry: %v", err)
	}
	if attempts < 2 {
		t.Fatalf("expected at least one retry, got %d attempts", attempts)
	}

	item, _ := base.Get(ctx, "idx")
	var ids []string
	_ = json.Unmarshal(item.Value, &ids)
	if len(ids) != 2 || ids[0] != "w1" || ids[1] != "w2" {
		t.Fatalf("ids=%v", ids)
	}
}

// conflictOnceStore injects a single conditional-write conflict on one key.
type conflictOnceStore struct {
	*fakeStore
	key  string
	done bool
}

func (c *conflictOnceStore) Set(ctx context.Context, key string, value []byte, etag string) error {
	if key == c.key && !c.done {
		c.done = true
		return ErrConflict
	}
	return c.fakeStore.Set(ctx, key, value, etag)
}

func TestPruneIndex(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	if err := s.Set(ctx, "idx", []byte(`["w1","w2","w3"]`), ""); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// w2 does not exist.
	err := PruneIndex(ctx, s, "idx", func(id string) bool { return id != "w2" })
	if err != nil {
		t.Fatalf("PruneIndex: %v", err)
	}

	item, _ := s.Get(ctx, "idx")
	var ids []string
	_ = json.Unmarshal(item.Value, &ids)
	if len(ids) != 2 || ids[0] != "w1" || ids[1] != "w3" {
		t.Fatalf("ids=%v", ids)
	}
}

func TestPruneIndexAllMissing(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	if err := s.Set(ctx, "idx", []byte(`["w1","w2"]`), ""); err != nil {
		t.Fatalf("Set: %v", err)
	}
	err := PruneIndex(ctx, s, "idx", func(string) bool { return false })
	if err != nil {
		t.Fatalf("PruneIndex: %v", err)
	}
	item, _ := s.Get(ctx, "idx")
	var ids []string
	_ = json.Unmarshal(item.Value, &ids)
	if len(ids) != 0 {
		t.Fatalf("ids=%v", ids)
	}
	// Empty list is stored as an empty array (not a missing key).
	if string(item.Value) != "[]" {
		t.Fatalf("value=%q", item.Value)
	}
}

func TestMutateIndexNotFoundAndEmpty(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	// Key missing + no changes should not create the key.
	err := MutateIndex(ctx, s, "idx", func(ids []string) ([]string, bool) {
		return ids, false
	})
	if err != nil {
		t.Fatalf("MutateIndex missing no-change: %v", err)
	}
	if _, err := s.Get(ctx, "idx"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing index: want ErrNotFound, got %v", err)
	}
}
