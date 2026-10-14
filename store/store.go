package store

import (
	"errors"
	"iter"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("item not found")
	ErrNotBytes = errors.New("value is not of type []byte")
)

// entry is a stored value with its expiration deadline.
// A zero expiresAt means the entry never expires.
type entry struct {
	value     []byte
	expiresAt time.Time
}

func (e *entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// Store is a concurrent, in-memory implementation of Storer using a sync.Map.
type Store struct {
	m sync.Map
}

// New initializes and returns a new empty Store.
func New() *Store {
	return &Store{}
}

// Get retrieves the value associated with the given key.
// It returns ErrNotFound if the key does not exist or has expired.
func (s *Store) Get(key string) ([]byte, error) {
	e, err := s.load(key)
	if err != nil {
		return nil, err
	}
	if e.expired(time.Now()) {
		return nil, ErrNotFound
	}
	return e.value, nil
}

// Set stores or updates the value for the given key. The entry expires at
// expiresAt; a zero expiresAt means it never expires.
func (s *Store) Set(key string, value []byte, expiresAt time.Time) error {
	s.m.Store(key, &entry{value: value, expiresAt: expiresAt})
	return nil
}

// Delete removes the key from the store.
// It does not return an error if the key does not exist.
func (s *Store) Delete(key string) error {
	s.m.Delete(key)
	return nil
}

// DeleteExpired removes the key only if its current expiration deadline equals
// expiresAt. This keeps an entry that was overwritten after it was found
// expired from being deleted.
func (s *Store) DeleteExpired(key string, expiresAt time.Time) error {
	e, err := s.load(key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if e.expiresAt.Equal(expiresAt) {
		s.m.CompareAndDelete(key, e)
	}
	return nil
}

// Expired returns the keys that have expired as of now, mapped to their
// expiration deadline.
func (s *Store) Expired(now time.Time) map[string]time.Time {
	expired := make(map[string]time.Time)
	s.m.Range(func(key any, val any) bool {
		e, ok := val.(*entry)
		if ok && e.expired(now) {
			expired[key.(string)] = e.expiresAt
		}
		return true
	})
	return expired
}

// Range calls fn for every entry, including expired ones, until fn returns false.
func (s *Store) Range(fn func(key string, value []byte, expiresAt time.Time) bool) {
	s.m.Range(func(key any, val any) bool {
		e, ok := val.(*entry)
		if !ok {
			return true
		}
		return fn(key.(string), e.value, e.expiresAt)
	})
}

// List returns an iterator over all unexpired values currently in the store.
func (s *Store) List() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		now := time.Now()
		s.m.Range(func(_, val any) bool {
			e, ok := val.(*entry)
			if !ok || e.expired(now) {
				return true
			}
			return yield(e.value)
		})
	}
}

func (s *Store) load(key string) (*entry, error) {
	val, ok := s.m.Load(key)
	if !ok {
		return nil, ErrNotFound
	}
	e, ok := val.(*entry)
	if !ok {
		return nil, ErrNotBytes
	}
	return e, nil
}
