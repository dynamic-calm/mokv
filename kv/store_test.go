package kv_test

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/kv"
)

func mustApply(t *testing.T, s *kv.Store, cmd kv.Command) {
	t.Helper()
	if err := s.Apply(cmd); err != nil {
		t.Fatalf("Apply(%+v) = %v, want nil", cmd, err)
	}
}

func TestStoreSetAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		value []byte
	}{
		{name: "simple string value", key: "key1", value: []byte("value1")},
		{name: "empty value", key: "key2", value: []byte{}},
		{name: "nil value", key: "key3", value: nil},
		{name: "binary data", key: "key4", value: []byte{0x00, 0xFF, 0x10, 0xAB}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := kv.NewStore()
			mustApply(t, s, kv.Set{Key: tt.key, Value: tt.value})

			got, err := s.Get(tt.key, time.Now())
			if err != nil {
				t.Fatalf("Get(%q) error = %v", tt.key, err)
			}
			if !bytes.Equal(got, tt.value) {
				t.Errorf("Get(%q) = %v, want %v", tt.key, got, tt.value)
			}
		})
	}
}

func TestStoreGetNotFound(t *testing.T) {
	t.Parallel()

	s := kv.NewStore()
	if _, err := s.Get("nonexistent", time.Now()); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("Get(%q) error = %v, want %v", "nonexistent", err, kv.ErrNotFound)
	}
}

func TestStoreUpdate(t *testing.T) {
	t.Parallel()

	s := kv.NewStore()
	mustApply(t, s, kv.Set{Key: "key", Value: []byte("initial")})
	mustApply(t, s, kv.Set{Key: "key", Value: []byte("updated")})

	got, err := s.Get("key", time.Now())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if want := []byte("updated"); !bytes.Equal(got, want) {
		t.Errorf("Get() = %q, want %q", got, want)
	}
}

func TestStoreDelete(t *testing.T) {
	t.Parallel()

	t.Run("delete existing key", func(t *testing.T) {
		s := kv.NewStore()
		mustApply(t, s, kv.Set{Key: "key", Value: []byte("value")})
		mustApply(t, s, kv.Delete{Key: "key"})

		if _, err := s.Get("key", time.Now()); !errors.Is(err, kv.ErrNotFound) {
			t.Errorf("Get() after delete error = %v, want %v", err, kv.ErrNotFound)
		}
	})

	t.Run("delete nonexistent key", func(t *testing.T) {
		mustApply(t, kv.NewStore(), kv.Delete{Key: "nonexistent"})
	})
}

func TestStoreList(t *testing.T) {
	t.Parallel()

	t.Run("empty store", func(t *testing.T) {
		var count int
		for range kv.NewStore().List(time.Now()) {
			count++
		}
		if count != 0 {
			t.Errorf("List() returned %d entries, want 0", count)
		}
	})

	t.Run("multiple entries", func(t *testing.T) {
		s := kv.NewStore()
		want := map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"}
		for k, v := range want {
			mustApply(t, s, kv.Set{Key: k, Value: []byte(v)})
		}

		got := make(map[string]string)
		for e := range s.List(time.Now()) {
			got[e.Key] = string(e.Value)
		}
		if len(got) != len(want) {
			t.Errorf("List() returned %d entries, want %d", len(got), len(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("List() entry %q = %q, want %q", k, got[k], v)
			}
		}
	})
}

func TestStoreConcurrent(t *testing.T) {
	t.Parallel()

	s := kv.NewStore()
	const goroutines = 100
	const operations = 100

	var wg sync.WaitGroup
	for i := range goroutines {
		key := string(rune('A' + (i % 26)))
		wg.Go(func() {
			for j := range operations {
				if err := s.Apply(kv.Set{Key: key, Value: []byte{byte(i), byte(j)}}); err != nil {
					t.Errorf("Apply() error = %v", err)
				}
			}
		})
		wg.Go(func() {
			for range operations {
				if _, err := s.Get(key, time.Now()); err != nil && !errors.Is(err, kv.ErrNotFound) {
					t.Errorf("Get(%q) error = %v, want nil or %v", key, err, kv.ErrNotFound)
				}
			}
		})
		wg.Go(func() {
			for range s.List(time.Now()) {
			}
		})
		if i%2 == 0 {
			wg.Go(func() {
				for range operations {
					if err := s.Apply(kv.Delete{Key: key}); err != nil {
						t.Errorf("Apply() error = %v", err)
					}
				}
			})
		}
	}
	wg.Wait()
}

func TestStoreExpiry(t *testing.T) {
	t.Parallel()

	now := time.Now()

	t.Run("expired key is hidden and reported", func(t *testing.T) {
		s := kv.NewStore()
		expiresAt := now.Add(-time.Second)
		mustApply(t, s, kv.Set{Key: "old", Value: []byte("old"), ExpiresAt: expiresAt})
		mustApply(t, s, kv.Set{Key: "fresh", Value: []byte("fresh"), ExpiresAt: now.Add(time.Hour)})

		if _, err := s.Get("old", now); !errors.Is(err, kv.ErrNotFound) {
			t.Errorf("Get(%q) error = %v, want %v", "old", err, kv.ErrNotFound)
		}
		for e := range s.List(now) {
			if e.Key == "old" {
				t.Error("List() returned an expired entry")
			}
		}

		expired := s.Expired(now)
		if len(expired) != 1 || expired[0].Key != "old" || !expired[0].ExpiresAt.Equal(expiresAt) {
			t.Errorf("Expired() = %v, want only old at %v", expired, expiresAt)
		}
	})

	t.Run("expire removes matching deadline", func(t *testing.T) {
		s := kv.NewStore()
		expiresAt := now.Add(-time.Second)
		mustApply(t, s, kv.Set{Key: "key", Value: []byte("value"), ExpiresAt: expiresAt})
		mustApply(t, s, kv.Expire{Key: "key", ExpiresAt: expiresAt})

		if got := s.Expired(now); len(got) != 0 {
			t.Errorf("Expired() after Expire = %v, want empty", got)
		}
	})

	t.Run("expire keeps overwritten key", func(t *testing.T) {
		s := kv.NewStore()
		expiresAt := now.Add(-time.Second)
		mustApply(t, s, kv.Set{Key: "key", Value: []byte("old"), ExpiresAt: expiresAt})
		mustApply(t, s, kv.Set{Key: "key", Value: []byte("new"), ExpiresAt: now.Add(time.Hour)})
		mustApply(t, s, kv.Expire{Key: "key", ExpiresAt: expiresAt})

		got, err := s.Get("key", now)
		if err != nil || !bytes.Equal(got, []byte("new")) {
			t.Errorf("Get() = %q, %v, want %q, nil", got, err, "new")
		}
	})
}

func TestStoreReset(t *testing.T) {
	t.Parallel()

	s := kv.NewStore()
	mustApply(t, s, kv.Set{Key: "key", Value: []byte("value")})
	s.Reset()

	for e := range s.All() {
		t.Errorf("All() after Reset returned %q", e.Key)
	}
}
