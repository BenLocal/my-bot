package store

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeStore is an in-memory Store implementation with ETag and TTL semantics,
// used to unit-test the store package without Dapr.
type fakeStore struct {
	mu    sync.Mutex
	items map[string]fakeItem
	seq   int64
}

type fakeItem struct {
	value     []byte
	etag      string
	expiresAt time.Time
}

func newFakeStore() *fakeStore {
	return &fakeStore{items: map[string]fakeItem{}}
}

func (f *fakeStore) nextETag() string {
	f.seq++
	return strconv.FormatInt(f.seq, 10)
}

// liveItem returns the current item, deleting it if it has expired.
func (f *fakeStore) liveItem(key string) (fakeItem, bool) {
	it, ok := f.items[key]
	if !ok {
		return fakeItem{}, false
	}
	if !it.expiresAt.IsZero() && time.Now().After(it.expiresAt) {
		delete(f.items, key)
		return fakeItem{}, false
	}
	return it, true
}

func (f *fakeStore) Get(ctx context.Context, key string) (*Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.liveItem(key)
	if !ok {
		return nil, ErrNotFound
	}
	return &Item{Value: it.value, ETag: it.etag}, nil
}

func (f *fakeStore) Set(ctx context.Context, key string, value []byte, etag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.liveItem(key)
	if ok && etag != "" && cur.etag != etag {
		return ErrConflict
	}
	f.items[key] = fakeItem{value: value, etag: f.nextETag()}
	return nil
}

func (f *fakeStore) SetTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[key] = fakeItem{value: value, etag: f.nextETag(), expiresAt: time.Now().Add(ttl)}
	return nil
}

func (f *fakeStore) Delete(ctx context.Context, key string, etag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.liveItem(key)
	if ok && etag != "" && cur.etag != etag {
		return ErrConflict
	}
	delete(f.items, key)
	return nil
}

func (f *fakeStore) FirstWrite(ctx context.Context, key string, value []byte, owner string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if it, ok := f.liveItem(key); ok {
		return false, ownerCheck(it.value, owner)
	}
	f.items[key] = fakeItem{value: value, etag: f.nextETag()}
	return true, nil
}

func (f *fakeStore) Transact(ctx context.Context, ops []Op) error {
	return runOps(ctx, f, ops)
}

func TestGetSetDelete(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	if _, err := s.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: want ErrNotFound, got %v", err)
	}

	if err := s.Set(ctx, "k", []byte("v1"), ""); err != nil {
		t.Fatalf("Set: %v", err)
	}
	item, err := s.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(item.Value) != "v1" || item.ETag == "" {
		t.Fatalf("Get: value=%q etag=%q", item.Value, item.ETag)
	}

	if err := s.Delete(ctx, "k", ""); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: want ErrNotFound, got %v", err)
	}
}

func TestETagConditionalWrite(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	if err := s.Set(ctx, "k", []byte("v1"), ""); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	item, _ := s.Get(ctx, "k")

	// Stale etag must conflict.
	err := s.Set(ctx, "k", []byte("v2"), item.ETag+"x")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("Set stale etag: want ErrConflict, got %v", err)
	}

	// Correct etag succeeds.
	if err := s.Set(ctx, "k", []byte("v2"), item.ETag); err != nil {
		t.Fatalf("Set current etag: %v", err)
	}
	got, _ := s.Get(ctx, "k")
	if string(got.Value) != "v2" {
		t.Fatalf("value after etag write: %q", got.Value)
	}

	// Delete with stale etag conflicts.
	if err := s.Delete(ctx, "k", item.ETag); !errors.Is(err, ErrConflict) {
		t.Fatalf("Delete stale etag: want ErrConflict, got %v", err)
	}
}

func TestFirstWrite(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	// First create succeeds.
	created, err := s.FirstWrite(ctx, "idem:k", []byte(`"g_1"`), "")
	if err != nil || !created {
		t.Fatalf("FirstWrite #1: created=%v err=%v", created, err)
	}

	// Duplicate with no owner in the stored value conflicts.
	created, err = s.FirstWrite(ctx, "idem:k", []byte(`"g_2"`), "")
	if err == nil || !errors.Is(err, ErrConflict) || created {
		t.Fatalf("FirstWrite #2: created=%v err=%v", created, err)
	}
}

func TestFirstWriteOwnerMatch(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	rec := []byte(`{"owner":"r1:s1:a1|w1","key":"r1:s1:a1"}`)
	created, err := s.FirstWrite(ctx, "attempt:k", rec, "r1:s1:a1|w1")
	if err != nil || !created {
		t.Fatalf("FirstWrite #1: created=%v err=%v", created, err)
	}

	// Same owner is treated as success (response-lost retry).
	created, err = s.FirstWrite(ctx, "attempt:k", []byte(`{"owner":"r1:s1:a1|w1"}`), "r1:s1:a1|w1")
	if err != nil || created {
		t.Fatalf("FirstWrite owner-match: created=%v err=%v", created, err)
	}

	// Different owner conflicts.
	created, err = s.FirstWrite(ctx, "attempt:k", []byte(`{"owner":"other"}`), "r1:s1:a1|w2")
	if err == nil || !errors.Is(err, ErrConflict) || created {
		t.Fatalf("FirstWrite other-owner: created=%v err=%v", created, err)
	}
}

func TestSetTTLExpiry(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	if err := s.SetTTL(ctx, "k", []byte("v"), 100*time.Millisecond); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}
	if _, err := s.Get(ctx, "k"); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}

	// Force expiry deterministically.
	s.mu.Lock()
	item := s.items["k"]
	item.expiresAt = time.Now().Add(-time.Second)
	s.items["k"] = item
	s.mu.Unlock()

	if _, err := s.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after expiry: want ErrNotFound, got %v", err)
	}
}

func TestTransact(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()

	err := s.Transact(ctx, []Op{
		UpsertOp{Key: "a", Value: []byte("A")},
		UpsertOp{Key: "b", Value: []byte("B"), TTL: time.Minute},
		DeleteOp{Key: "missing"},
	})
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}

	a, err := s.Get(ctx, "a")
	if err != nil || string(a.Value) != "A" {
		t.Fatalf("a=%q err=%v", a.Value, err)
	}
	b, err := s.Get(ctx, "b")
	if err != nil || string(b.Value) != "B" {
		t.Fatalf("b=%q err=%v", b.Value, err)
	}
}

func TestTransactUnknownOp(t *testing.T) {
	ctx := context.Background()
	s := newFakeStore()
	err := s.Transact(ctx, []Op{struct{}{}})
	if err == nil {
		t.Fatal("Transact unknown op: want error")
	}
}

func TestJSONRoundTripOwnerFromValue(t *testing.T) {
	owner, ok := ownerFromValue([]byte(`{"owner":"abc"}`))
	if !ok || owner != "abc" {
		t.Fatalf("ownerFromValue: owner=%q ok=%v", owner, ok)
	}
	if _, ok := ownerFromValue([]byte(`"plain"`)); ok {
		t.Fatalf("ownerFromValue plain: want ok=false")
	}
	if _, ok := ownerFromValue([]byte(`{"key":"x"}`)); ok {
		t.Fatalf("ownerFromValue no owner: want ok=false")
	}
}

func TestIsConflictStringForms(t *testing.T) {
	// Dapr Redis first-write collision and ETag mismatch surface as gRPC
	// errors. The store normalizes them to ErrConflict.
	if !isConflict(errFromString("failed to set key statetest||x")) {
		t.Fatal("isConflict: failed to set key should be true")
	}
	if !isConflict(errFromString("possible etag mismatch")) {
		t.Fatal("isConflict: possible etag mismatch should be true")
	}
	if isConflict(errFromString("some other error")) {
		t.Fatal("isConflict: other error should be false")
	}
}

type stringErr string

func (e stringErr) Error() string { return string(e) }

func errFromString(s string) error { return stringErr(s) }
