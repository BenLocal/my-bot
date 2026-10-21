// Package store wraps Dapr State with ETag-based optimistic concurrency,
// first-write dedup, sequential (non-atomic) transactions, TTL writes and
// index mutation with full-jitter backoff. See docs/spec.md §6.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	dapr "github.com/dapr/go-sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Sentinel errors returned by Store methods.
var (
	// ErrNotFound means the requested key does not exist (or has expired).
	ErrNotFound = errors.New("store: key not found")
	// ErrConflict means a conditional write (ETag or first-write) lost a
	// concurrent race and must be retried.
	ErrConflict = errors.New("store: optimistic concurrency conflict")
)

// Item is a stored value together with its ETag.
type Item struct {
	Value []byte
	ETag  string
}

// Op is a single operation used by Transact. Transact is intentionally not
// atomic on Redis (docs/spec.md §6): operations are applied sequentially and
// each must be individually idempotent.
type Op interface{}

// UpsertOp writes a value (with optional conditional ETag and TTL).
type UpsertOp struct {
	Key   string
	Value []byte
	ETag  string
	TTL   time.Duration
}

// DeleteOp deletes a key (with optional conditional ETag).
type DeleteOp struct {
	Key  string
	ETag string
}

// Store is the Dapr State abstraction used across the project.
type Store interface {
	// Get returns the value and ETag, or ErrNotFound.
	Get(ctx context.Context, key string) (*Item, error)
	// Set writes value. If etag is non-empty the write is conditional on that
	// exact ETag.
	Set(ctx context.Context, key string, value []byte, etag string) error
	// SetTTL writes value with a TTL (rounded up to at least one second).
	SetTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Delete removes key. If etag is non-empty the delete is conditional.
	Delete(ctx context.Context, key string, etag string) error
	// FirstWrite performs a create-if-absent write. If the key already exists it
	// reads back the existing value and reports whether the stored owner equals
	// owner (owner-matched retry is treated as success). Returns created=true
	// only when the key was newly created.
	FirstWrite(ctx context.Context, key string, value []byte, owner string) (bool, error)
	// Transact applies ops sequentially.
	Transact(ctx context.Context, ops []Op) error
}

// NewDaprStore wraps an existing Dapr client with a state store name.
func NewDaprStore(c dapr.Client, storeName string) Store {
	return &daprStore{client: c, storeName: storeName}
}

type daprStore struct {
	client    dapr.Client
	storeName string
}

func (s *daprStore) Get(ctx context.Context, key string) (*Item, error) {
	item, err := s.client.GetState(ctx, s.storeName, key, nil)
	if err != nil {
		return nil, err
	}
	if item == nil || item.Etag == "" {
		return nil, ErrNotFound
	}
	return &Item{Value: item.Value, ETag: item.Etag}, nil
}

func (s *daprStore) Set(ctx context.Context, key string, value []byte, etag string) error {
	if etag != "" {
		return s.client.SaveStateWithETag(ctx, s.storeName, key, value, etag, nil)
	}
	return s.client.SaveState(ctx, s.storeName, key, value, nil)
}

func (s *daprStore) SetTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	secs := int64(ttl / time.Second)
	if secs < 1 {
		secs = 1
	}
	return s.client.SaveState(ctx, s.storeName, key, value,
		map[string]string{"ttlInSeconds": strconv.FormatInt(secs, 10)})
}

func (s *daprStore) Delete(ctx context.Context, key string, etag string) error {
	if etag != "" {
		return s.client.DeleteStateWithETag(ctx, s.storeName, key,
			&dapr.ETag{Value: etag}, nil, nil)
	}
	return s.client.DeleteState(ctx, s.storeName, key, nil)
}

func (s *daprStore) FirstWrite(ctx context.Context, key string, value []byte, owner string) (bool, error) {
	err := s.client.SaveState(ctx, s.storeName, key, value, nil,
		dapr.WithConcurrency(dapr.StateConcurrencyFirstWrite))
	if err == nil {
		return true, nil
	}
	if !isConflict(err) {
		return false, err
	}

	// Conflict: the key already exists. Read it back and apply the owner rule
	// from docs/spec.md §6 ("写成功但响应丢失后重试").
	existing, gerr := s.client.GetState(ctx, s.storeName, key, nil)
	if gerr != nil {
		return false, gerr
	}
	if existing == nil || existing.Etag == "" {
		return false, ErrNotFound
	}
	return false, ownerCheck(existing.Value, owner)
}

func (s *daprStore) Transact(ctx context.Context, ops []Op) error {
	return runOps(ctx, s, ops)
}

// runOps applies ops sequentially. It is shared by the Dapr and fake
// implementations because the semantics are the same.
func runOps(ctx context.Context, s Store, ops []Op) error {
	for _, op := range ops {
		switch o := op.(type) {
		case UpsertOp:
			if o.TTL > 0 {
				if err := s.SetTTL(ctx, o.Key, o.Value, o.TTL); err != nil {
					return err
				}
			} else {
				if err := s.Set(ctx, o.Key, o.Value, o.ETag); err != nil {
					return err
				}
			}
		case DeleteOp:
			if err := s.Delete(ctx, o.Key, o.ETag); err != nil {
				return err
			}
		default:
			return fmt.Errorf("store: unknown operation type %T", op)
		}
	}
	return nil
}

// isConflict normalizes the Dapr/Redis conflict signals into ErrConflict.
// ETag mismatches surface as gRPC Aborted ("possible etag mismatch"); first-write
// collisions surface as Internal ("failed to set key"). Both are conflicts.
func isConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrConflict) {
		return true
	}
	if status.Code(err) == codes.Aborted {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "failed to set key") ||
		strings.Contains(msg, "possible etag mismatch")
}

// ownerFromValue extracts the "owner" field from a JSON record. Records without
// an owner field (e.g. idem values) report hasOwner=false.
func ownerFromValue(value []byte) (owner string, hasOwner bool) {
	var probe struct {
		Owner string `json:"owner"`
	}
	if err := json.Unmarshal(value, &probe); err != nil {
		return "", false
	}
	if probe.Owner == "" {
		return "", false
	}
	return probe.Owner, true
}

// ownerCheck applies the first-write owner rule.
func ownerCheck(existingValue []byte, owner string) error {
	existingOwner, hasOwner := ownerFromValue(existingValue)
	if hasOwner && existingOwner == owner {
		return nil
	}
	return ErrConflict
}
