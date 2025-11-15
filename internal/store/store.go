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
	m sync.Map
}

func New() *Store {
	return &Store{m: sync.Map{}}
}

func (s *Store) Get(key string) ([]byte, error) {
	value, ok := s.m.Load(key)
	if !ok {
		return nil, errors.New("not found")
	}

	bytes, ok := value.([]byte)
	if !ok {
		return nil, errors.New("not bytes")
	}
	return bytes, nil
}

func (s *Store) Set(key string, value []byte) error {
	s.m.Store(key, value)
	return nil
}

func (s *Store) Delete(key string) error {
	s.m.Delete(key)
	return nil
}

func (s *Store) List() <-chan []byte {
	c := make(chan []byte)
	go func() {
		defer close(c)
		s.m.Range(func(key any, val any) bool {
			bytes, ok := val.([]byte)
			if !ok {
				return false
			}
			c <- bytes
			return true
		})
	}()
	return c
}
