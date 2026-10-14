package kv

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/store"
	"github.com/hashicorp/raft"
)

type testSink struct {
	bytes.Buffer
}

func (s *testSink) ID() string    { return "test" }
func (s *testSink) Cancel() error { return nil }
func (s *testSink) Close() error  { return nil }

func TestSnapshotRestore(t *testing.T) {
	expiresAt := time.Unix(0, time.Now().Add(time.Hour).UnixNano())
	expiredAt := time.Unix(0, time.Now().Add(-time.Hour).UnixNano())
	want := map[string]struct {
		value     []byte
		expiresAt time.Time
	}{
		"expiring": {[]byte("a"), expiresAt},
		"expired":  {[]byte("b"), expiredAt},
		"forever":  {[]byte("c"), time.Time{}},
	}

	src := store.New()
	for k, v := range want {
		if err := src.Set(k, v.value, v.expiresAt); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
	}

	snap, err := (&fsm{store: src}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	var sink testSink
	if err := snap.Persist(&sink); err != nil {
		t.Fatalf("Persist() error = %v", err)
	}

	dst := store.New()
	_ = dst.Set("stale", []byte("stale"), time.Time{})
	if err := (&fsm{store: dst}).Restore(io.NopCloser(&sink)); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	got := 0
	dst.Range(func(key string, value []byte, expiresAt time.Time) bool {
		got++
		w, ok := want[key]
		if !ok {
			t.Errorf("Restore() kept unexpected key %q", key)
			return true
		}
		if !bytes.Equal(value, w.value) || !expiresAt.Equal(w.expiresAt) {
			t.Errorf("key %q = (%q, %v), want (%q, %v)", key, value, expiresAt, w.value, w.expiresAt)
		}
		return true
	})
	if got != len(want) {
		t.Errorf("Restore() restored %d keys, want %d", got, len(want))
	}
}

var _ raft.SnapshotSink = (*testSink)(nil)
