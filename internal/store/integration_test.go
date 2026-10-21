//go:build integration

// Integration tests run against a live Dapr sidecar + Redis. Start them with:
//
//	docker run -d --name mybot-redis -p 6379:6379 redis --appendonly yes
//	dapr run --app-id mybot-runtime -- resources... && go test -tags integration ./internal/store/
package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	dapr "github.com/dapr/go-sdk/client"
)

func integrationStore(t *testing.T) Store {
	t.Helper()
	c, err := dapr.NewClient()
	if err != nil {
		t.Fatalf("dapr.NewClient: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return NewDaprStore(c, "statestore")
}

func TestIntegrationETag(t *testing.T) {
	ctx := context.Background()
	s := integrationStore(t)

	key := "it:etag"
	_ = s.Delete(ctx, key, "")

	if err := s.Set(ctx, key, []byte("v1"), ""); err != nil {
		t.Fatalf("Set: %v", err)
	}
	item, err := s.Get(ctx, key)
	if err != nil || item.ETag == "" {
		t.Fatalf("Get: %+v err=%v", item, err)
	}

	if err := s.Set(ctx, key, []byte("v2"), item.ETag); err != nil {
		t.Fatalf("Set with etag: %v", err)
	}
	got, _ := s.Get(ctx, key)
	if string(got.Value) != "v2" {
		t.Fatalf("value=%q", got.Value)
	}

	if err := s.Delete(ctx, key, item.ETag); !errors.Is(err, ErrConflict) {
		t.Fatalf("Delete stale etag: want ErrConflict, got %v", err)
	}
}

func TestIntegrationFirstWrite(t *testing.T) {
	ctx := context.Background()
	s := integrationStore(t)

	key := "it:fw"
	_ = s.Delete(ctx, key, "")

	created, err := s.FirstWrite(ctx, key, []byte(`{"owner":"o1"}`), "o1")
	if err != nil || !created {
		t.Fatalf("FirstWrite #1: created=%v err=%v", created, err)
	}
	created, err = s.FirstWrite(ctx, key, []byte(`{"owner":"o1"}`), "o1")
	if err != nil || created {
		t.Fatalf("FirstWrite #2: created=%v err=%v", created, err)
	}
}

func TestIntegrationTTL(t *testing.T) {
	ctx := context.Background()
	s := integrationStore(t)

	key := "it:ttl"
	_ = s.Delete(ctx, key, "")
	if err := s.SetTTL(ctx, key, []byte("x"), 2*time.Second); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}
	if _, err := s.Get(ctx, key); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}
	time.Sleep(3 * time.Second)
	if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after expiry: want ErrNotFound, got %v", err)
	}
}

func TestIntegrationTransact(t *testing.T) {
	ctx := context.Background()
	s := integrationStore(t)

	_ = s.Delete(ctx, "it:tx:a", "")
	_ = s.Delete(ctx, "it:tx:b", "")
	err := s.Transact(ctx, []Op{
		UpsertOp{Key: "it:tx:a", Value: []byte("A")},
		UpsertOp{Key: "it:tx:b", Value: []byte("B")},
		DeleteOp{Key: "it:tx:missing"},
	})
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}
	a, _ := s.Get(ctx, "it:tx:a")
	b, _ := s.Get(ctx, "it:tx:b")
	if string(a.Value) != "A" || string(b.Value) != "B" {
		t.Fatalf("a=%q b=%q", a.Value, b.Value)
	}
}

func TestIntegrationMutateIndex(t *testing.T) {
	ctx := context.Background()
	s := integrationStore(t)

	key := "it:idx"
	_ = s.Delete(ctx, key, "")

	err := MutateIndex(ctx, s, key, func(ids []string) ([]string, bool) {
		return append(ids, "w1"), true
	})
	if err != nil {
		t.Fatalf("MutateIndex: %v", err)
	}
	item, _ := s.Get(ctx, key)
	var ids []string
	if err := json.Unmarshal(item.Value, &ids); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(ids) != 1 || ids[0] != "w1" {
		t.Fatalf("ids=%v", ids)
	}
}
