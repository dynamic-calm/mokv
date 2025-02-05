package kv

type kv struct {
	store Store
}

// These are all shallow methods since I haven't developed the persistance layer for now.
func New(store Store) KV {
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
