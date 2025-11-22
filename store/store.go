package store

import (
	"errors"
	"sync"
)

// Storer defines the interface for a the key-value storage.
type Storer interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	List() <-chan []byte
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
// It returns ErrNotFound if the key does not exist.
func (s *Store) Get(key string) ([]byte, error) {
	value, ok := s.m.Load(key)
	if !ok {
		return nil, errors.New("not found")
	}

	bytes, ok := value.([]byte)
	if !ok {
		return nil, errors.New("value is not of type []byte")
	}
	return bytes, nil
}

// Set stores or updates the value for the given key.
func (s *Store) Set(key string, value []byte) error {
	s.m.Store(key, value)
	return nil
}

// Delete removes the key from the store.
// It does not return an error if the key does not exist.
func (s *Store) Delete(key string) error {
	s.m.Delete(key)
	return nil
}

// List returns a channel that streams all values currently in the store.
// The channel is closed once iteration is complete.
func (s *Store) List() <-chan []byte {
	c := make(chan []byte)
	go func() {
		defer close(c)
		s.m.Range(func(key any, val any) bool {
			bytes, _ := val.([]byte)
			c <- bytes
			return true
		})
	}()
	return c
}
