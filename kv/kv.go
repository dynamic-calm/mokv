package kv

import "github.com/mateopresacastro/mokv/kv/store"

type KV interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	List() <-chan []byte
}

type kv struct {
	store store.Store
}

// These are all shallow methods since I haven't developed the persistance layer for now.
func New(store store.Store) KV {
	return &kv{store: store}
}

func (kv *kv) Get(key string) ([]byte, error) {
	return kv.store.Get(key)
}

func (kv *kv) Set(key string, value []byte) error {
	return kv.store.Set(key, value)
}

func (kv *kv) Delete(key string) error {
	return kv.store.Delete(key)
}

func (kv *kv) List() <-chan []byte {
	return kv.store.List()
}
