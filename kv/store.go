package kv

import (
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"
)

// ErrNotFound is returned when a key does not exist or has expired.
var ErrNotFound = errors.New("key not found")

// Entry is a value stored under a key.
type Entry struct {
	Key   string
	Value []byte
	// ExpiresAt is when the entry expires. A zero ExpiresAt means it never
	// expires.
	ExpiresAt time.Time
}

// Expired reports whether the entry has expired as of now.
func (e Entry) Expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt)
}

// Store is the replicated state: an in-memory set of entries that changes
// only through Apply. It never reads the clock, so every node that applies
// the same commands holds the same entries. It is safe for concurrent use.
type Store struct {
	entries sync.Map
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{}
}

// Apply applies cmd to the store.
func (s *Store) Apply(cmd Command) error {
	switch c := cmd.(type) {
	case Set:
		s.entries.Store(c.Key, &Entry{Key: c.Key, Value: c.Value, ExpiresAt: c.ExpiresAt})
	case Delete:
		s.entries.Delete(c.Key)
	case Expire:
		if e, ok := s.load(c.Key); ok && e.ExpiresAt.Equal(c.ExpiresAt) {
			s.entries.CompareAndDelete(c.Key, e)
		}
	default:
		return fmt.Errorf("unknown command %T", cmd)
	}
	return nil
}

// Get returns the value stored under key. It returns ErrNotFound if the key
// does not exist or has expired as of now.
func (s *Store) Get(key string, now time.Time) ([]byte, error) {
	e, ok := s.load(key)
	if !ok || e.Expired(now) {
		return nil, ErrNotFound
	}
	return e.Value, nil
}

// List returns an iterator over the entries that have not expired as of now.
func (s *Store) List(now time.Time) iter.Seq[Entry] {
	return func(yield func(Entry) bool) {
		for e := range s.All() {
			if !e.Expired(now) && !yield(e) {
				return
			}
		}
	}
}

// Expired returns the entries that have expired as of now.
func (s *Store) Expired(now time.Time) []Entry {
	var expired []Entry
	for e := range s.All() {
		if e.Expired(now) {
			expired = append(expired, e)
		}
	}
	return expired
}

// All returns an iterator over every entry, including expired ones.
func (s *Store) All() iter.Seq[Entry] {
	return func(yield func(Entry) bool) {
		s.entries.Range(func(_, v any) bool {
			return yield(*v.(*Entry))
		})
	}
}

// Reset removes every entry.
func (s *Store) Reset() {
	s.entries.Clear()
}

func (s *Store) load(key string) (*Entry, bool) {
	v, ok := s.entries.Load(key)
	if !ok {
		return nil, false
	}
	return v.(*Entry), true
}
