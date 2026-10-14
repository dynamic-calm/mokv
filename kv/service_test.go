package kv_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/kv"
	"github.com/dynamic-calm/mokv/kv/kvtest"
)

// clock is a fake clock that only moves when advanced.
type clock struct {
	now time.Time
}

func newClock() *clock { return &clock{now: time.Unix(1_000_000, 0)} }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newService(r *kvtest.Replicator, c *clock) *kv.Service {
	return kv.NewService(r.Store, r, c.Now)
}

func TestServiceSetGetDelete(t *testing.T) {
	t.Parallel()

	r := &kvtest.Replicator{Store: kv.NewStore()}
	svc := newService(r, newClock())
	ctx := t.Context()

	if err := svc.Set(ctx, "key", []byte("value"), time.Minute); err != nil {
		t.Fatalf("Set() = %v, want nil", err)
	}
	got, err := svc.Get("key")
	if err != nil || !bytes.Equal(got, []byte("value")) {
		t.Errorf("Get(%q) = %q, %v, want %q, nil", "key", got, err, "value")
	}

	if err := svc.Delete(ctx, "key"); err != nil {
		t.Fatalf("Delete() = %v, want nil", err)
	}
	if _, err := svc.Get("key"); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("Get(%q) after Delete error = %v, want %v", "key", err, kv.ErrNotFound)
	}
}

func TestServiceSetInvalidTTL(t *testing.T) {
	t.Parallel()

	svc := newService(&kvtest.Replicator{Store: kv.NewStore()}, newClock())
	for _, ttl := range []time.Duration{0, -time.Second} {
		if err := svc.Set(t.Context(), "key", nil, ttl); !errors.Is(err, kv.ErrInvalidTTL) {
			t.Errorf("Set(ttl=%v) = %v, want %v", ttl, err, kv.ErrInvalidTTL)
		}
	}
}

func TestServiceSetOnFollower(t *testing.T) {
	t.Parallel()

	svc := newService(&kvtest.Replicator{Store: kv.NewStore(), Follower: true}, newClock())
	if err := svc.Set(t.Context(), "key", nil, time.Minute); !errors.Is(err, kv.ErrNotLeader) {
		t.Errorf("Set() on follower = %v, want %v", err, kv.ErrNotLeader)
	}
}

func TestServiceExpiry(t *testing.T) {
	t.Parallel()

	r := &kvtest.Replicator{Store: kv.NewStore()}
	c := newClock()
	svc := newService(r, c)
	ctx := t.Context()

	if err := svc.Set(ctx, "short", []byte("a"), time.Second); err != nil {
		t.Fatalf("Set() = %v, want nil", err)
	}
	if err := svc.Set(ctx, "long", []byte("b"), time.Hour); err != nil {
		t.Fatalf("Set() = %v, want nil", err)
	}
	c.Advance(time.Second)

	if _, err := svc.Get("short"); !errors.Is(err, kv.ErrNotFound) {
		t.Errorf("Get(%q) after ttl error = %v, want %v", "short", err, kv.ErrNotFound)
	}
	for e := range svc.List() {
		if e.Key == "short" {
			t.Errorf("List() returned expired key %q", e.Key)
		}
	}

	r.Follower = true
	if err := svc.Expire(ctx); err != nil {
		t.Fatalf("Expire() on follower = %v, want nil", err)
	}
	if got := len(r.Store.Expired(c.Now())); got != 1 {
		t.Fatalf("follower Expire() left %d expired keys, want 1 untouched", got)
	}

	r.Follower = false
	if err := svc.Expire(ctx); err != nil {
		t.Fatalf("Expire() = %v, want nil", err)
	}
	var keys []string
	for e := range r.Store.All() {
		keys = append(keys, e.Key)
	}
	if len(keys) != 1 || keys[0] != "long" {
		t.Errorf("keys after Expire() = %v, want [long]", keys)
	}
}
