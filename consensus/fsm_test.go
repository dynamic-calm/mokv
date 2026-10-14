package consensus

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/kv"
	"github.com/hashicorp/raft"
)

type testSink struct {
	bytes.Buffer
}

var _ raft.SnapshotSink = (*testSink)(nil)

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

	src := kv.NewStore()
	for k, v := range want {
		if err := src.Apply(kv.Set{Key: k, Value: v.value, ExpiresAt: v.expiresAt}); err != nil {
			t.Fatalf("Apply() error = %v", err)
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

	dst := kv.NewStore()
	if err := dst.Apply(kv.Set{Key: "stale", Value: []byte("stale")}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if err := (&fsm{store: dst}).Restore(io.NopCloser(&sink)); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	got := 0
	for e := range dst.All() {
		got++
		w, ok := want[e.Key]
		if !ok {
			t.Errorf("Restore() kept unexpected key %q", e.Key)
			continue
		}
		if !bytes.Equal(e.Value, w.value) || !e.ExpiresAt.Equal(w.expiresAt) {
			t.Errorf("key %q = (%q, %v), want (%q, %v)", e.Key, e.Value, e.ExpiresAt, w.value, w.expiresAt)
		}
	}
	if got != len(want) {
		t.Errorf("Restore() restored %d keys, want %d", got, len(want))
	}
}
