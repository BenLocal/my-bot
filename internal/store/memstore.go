package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// memStore is an in-memory Store with ETag / first-write / TTL semantics. Used by
// the worker when no Dapr sidecar is configured (local development) and by tests.
type memStore struct {
	mu    sync.Mutex
	items map[string]memItem
	seq   int64
}

type memItem struct {
	value     []byte
	etag      string
	expiresAt time.Time
}

// NewMemStore creates an in-memory Store.
func NewMemStore() Store {
	return &memStore{items: map[string]memItem{}}
}

func (m *memStore) nextETag() string {
	m.seq++
	return strconv.FormatInt(m.seq, 10)
}

func (m *memStore) liveItem(key string) (memItem, bool) {
	it, ok := m.items[key]
	if !ok {
		return memItem{}, false
	}
	if !it.expiresAt.IsZero() && time.Now().After(it.expiresAt) {
		delete(m.items, key)
		return memItem{}, false
	}
	return it, true
}

func (m *memStore) Get(ctx context.Context, key string) (*Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	it, ok := m.liveItem(key)
	if !ok {
		return nil, ErrNotFound
	}
	return &Item{Value: it.value, ETag: it.etag}, nil
}

func (m *memStore) Set(ctx context.Context, key string, value []byte, etag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.liveItem(key)
	if ok && etag != "" && cur.etag != etag {
		return ErrConflict
	}
	m.items[key] = memItem{value: value, etag: m.nextETag()}
	return nil
}

func (m *memStore) SetTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = memItem{value: value, etag: m.nextETag(), expiresAt: time.Now().Add(ttl)}
	return nil
}

func (m *memStore) Delete(ctx context.Context, key string, etag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.liveItem(key)
	if ok && etag != "" && cur.etag != etag {
		return ErrConflict
	}
	delete(m.items, key)
	return nil
}

func (m *memStore) FirstWrite(ctx context.Context, key string, value []byte, owner string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if it, ok := m.liveItem(key); ok {
		return false, memOwnerCheck(it.value, owner)
	}
	m.items[key] = memItem{value: value, etag: m.nextETag()}
	return true, nil
}

func (m *memStore) Transact(ctx context.Context, ops []Op) error {
	for _, op := range ops {
		switch o := op.(type) {
		case UpsertOp:
			if o.TTL > 0 {
				if err := m.SetTTL(ctx, o.Key, o.Value, o.TTL); err != nil {
					return err
				}
			} else {
				if err := m.Set(ctx, o.Key, o.Value, o.ETag); err != nil {
					return err
				}
			}
		case DeleteOp:
			if err := m.Delete(ctx, o.Key, o.ETag); err != nil {
				return err
			}
		default:
			return fmt.Errorf("store: memStore: unknown operation type %T", op)
		}
	}
	return nil
}

// memOwnerCheck mirrors ownerCheck: only an exact owner match is treated
// as success.
func memOwnerCheck(existingValue []byte, owner string) error {
	var probe struct {
		Owner string `json:"owner"`
	}
	if err := json.Unmarshal(existingValue, &probe); err != nil {
		return ErrConflict
	}
	if probe.Owner != "" && probe.Owner == owner {
		return nil
	}
	return ErrConflict
}
