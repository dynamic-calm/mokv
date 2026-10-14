package kv

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	// ErrInvalidTTL is returned when a TTL is not positive.
	ErrInvalidTTL = errors.New("ttl must be positive")
	// ErrNotLeader is returned when a write reaches a node that is not the
	// leader.
	ErrNotLeader = errors.New("not the leader")
)

// Replicator replicates commands to the Store of every node in the cluster.
type Replicator interface {
	// Propose replicates cmds and returns once they are applied on this
	// node. It returns an error wrapping ErrNotLeader if this node is not
	// the leader.
	Propose(ctx context.Context, cmds ...Command) error
	// IsLeader reports whether this node is the leader.
	IsLeader() bool
}

// Service is the key-value store. It reads from the local Store and
// replicates writes through a Replicator, which applies them to the Store.
type Service struct {
	store      *Store
	replicator Replicator
	now        func() time.Time
}

// NewService returns a Service that reads from store and replicates writes
// through r. The clock now is used to compute and check expiry deadlines.
func NewService(store *Store, r Replicator, now func() time.Time) *Service {
	return &Service{store: store, replicator: r, now: now}
}

// Get returns the value stored under key on this node. Reads are not
// replicated, so a follower may return a value the leader has already
// changed. It returns ErrNotFound if the key does not exist or has expired.
func (s *Service) Get(key string) ([]byte, error) {
	value, err := s.store.Get(key, s.now())
	if err != nil {
		return nil, fmt.Errorf("getting key %q: %w", key, err)
	}
	return value, nil
}

// Set stores value under key for ttl. It returns ErrInvalidTTL if ttl is not
// positive.
func (s *Service) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	cmd := Set{Key: key, Value: value, ExpiresAt: s.now().Add(ttl)}
	if err := s.replicator.Propose(ctx, cmd); err != nil {
		return fmt.Errorf("replicating set of key %q: %w", key, err)
	}
	return nil
}

// Delete removes key. Deleting a key that does not exist is not an error.
func (s *Service) Delete(ctx context.Context, key string) error {
	if err := s.replicator.Propose(ctx, Delete{Key: key}); err != nil {
		return fmt.Errorf("replicating delete of key %q: %w", key, err)
	}
	return nil
}

// List returns an iterator over the unexpired entries on this node.
func (s *Service) List() iter.Seq[Entry] {
	return s.store.List(s.now())
}

// Expire replicates the removal of every expired entry. Only the leader
// expires entries, so on other nodes it does nothing. Each entry is removed
// with an Expire command, so an entry overwritten in the meantime survives.
func (s *Service) Expire(ctx context.Context) error {
	if !s.replicator.IsLeader() {
		return nil
	}
	expired := s.store.Expired(s.now())
	if len(expired) == 0 {
		return nil
	}
	cmds := make([]Command, len(expired))
	for i, e := range expired {
		cmds[i] = Expire{Key: e.Key, ExpiresAt: e.ExpiresAt}
	}
	if err := s.replicator.Propose(ctx, cmds...); err != nil {
		return fmt.Errorf("replicating expiry of %d keys: %w", len(cmds), err)
	}
	log.Debug().Int("count", len(cmds)).Msg("expired keys")
	return nil
}

// RunExpiry calls Expire every interval until ctx is done.
func (s *Service) RunExpiry(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Expire(ctx); err != nil {
				log.Error().Err(err).Msg("failed to expire keys")
			}
		}
	}
}
