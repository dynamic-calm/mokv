package store

import (
	"errors"
	"sync"
)

type Storer interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	List() <-chan []byte
}

type Store struct {
	db map[string][]byte
	mu sync.RWMutex
}

func New() *Store {
	return &Store{db: map[string][]byte{}}
}

func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.db[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return value, nil
}

func (s *Store) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db[key] = value
	return nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.db[key]
	if !ok {
		return errors.New("not found")
	}
	delete(s.db, key)
	return nil
}

func (s *Store) List() <-chan []byte {
	c := make(chan []byte)
	s.mu.RLock()
	go func() {
		defer s.mu.RUnlock()
		defer close(c)
		for _, val := range s.db {
			c <- val
		}
	}()
	return c
}
