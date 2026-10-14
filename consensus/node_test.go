package consensus_test

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/consensus"
	"github.com/dynamic-calm/mokv/kv"
)

type testNode struct {
	*consensus.Node
	store *kv.Store
	addr  string
}

func newNode(t *testing.T, id string, bootstrap bool) *testNode {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Setup: net.Listen() for %s: %v", id, err)
	}
	store := kv.NewStore()
	node, err := consensus.New(store, consensus.Config{
		ID:        id,
		Addr:      ln.Addr().String(),
		Listener:  ln,
		DataDir:   t.TempDir(),
		Bootstrap: bootstrap,
	})
	if err != nil {
		t.Fatalf("Setup: consensus.New() for %s: %v", id, err)
	}
	t.Cleanup(func() { _ = node.Close() })
	return &testNode{Node: node, store: store, addr: ln.Addr().String()}
}

func TestReplication(t *testing.T) {
	node1 := newNode(t, "node-1", true)
	node2 := newNode(t, "node-2", false)

	if err := node1.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("node1.WaitForLeader() = %v, want nil", err)
	}
	if err := node1.Join("node-2", node2.addr); err != nil {
		t.Fatalf("node1.Join(%q) = %v, want nil", "node-2", err)
	}

	if err := node1.Propose(t.Context(), kv.Set{Key: "test-key", Value: []byte("test-value")}); err != nil {
		t.Fatalf("node1.Propose() = %v, want nil", err)
	}
	waitForValue(t, node2.store, "test-key", "test-value")

	servers, err := node1.Servers()
	if err != nil {
		t.Fatalf("node1.Servers() = %v, want nil", err)
	}
	if len(servers) != 2 {
		t.Fatalf("len(node1.Servers()) = %d, want 2", len(servers))
	}
	if !servers[0].IsLeader {
		t.Errorf("servers[0].IsLeader = false, want true")
	}
	if servers[1].IsLeader {
		t.Errorf("servers[1].IsLeader = true, want false")
	}

	if err := node1.Leave("node-2"); err != nil {
		t.Fatalf("node1.Leave(%q) = %v, want nil", "node-2", err)
	}
	waitFor(t, "node-2 to leave", func() bool {
		servers, err := node1.Servers()
		return err == nil && len(servers) == 1
	})

	node3 := newNode(t, "node-3", false)
	if err := node1.Join("node-3", node3.addr); err != nil {
		t.Fatalf("node1.Join(%q) = %v, want nil", "node-3", err)
	}
	if err := node1.Propose(t.Context(), kv.Set{Key: "test-key2", Value: []byte("test-value2")}); err != nil {
		t.Fatalf("node1.Propose() = %v, want nil", err)
	}
	waitForValue(t, node3.store, "test-key2", "test-value2")

	err = node1.Propose(t.Context(), kv.Set{
		Key:       "ttl-key",
		Value:     []byte("ttl-value"),
		ExpiresAt: time.Now().Add(time.Second),
	})
	if err != nil {
		t.Fatalf("node1.Propose() = %v, want nil", err)
	}
	waitFor(t, "ttl-key to replicate", func() bool { return hasKey(node3.store, "ttl-key") })
	svc := kv.NewService(node1.store, node1, time.Now)
	waitFor(t, "ttl-key to expire", func() bool {
		if err := svc.Expire(t.Context()); err != nil {
			t.Fatalf("Expire() = %v, want nil", err)
		}
		return !hasKey(node1.store, "ttl-key") && !hasKey(node3.store, "ttl-key")
	})

	if err := node3.Propose(t.Context(), kv.Delete{Key: "test-key2"}); !errors.Is(err, kv.ErrNotLeader) {
		t.Errorf("node3.Propose() = %v, want %v", err, kv.ErrNotLeader)
	}
	if err := node3.Join("node-4", "127.0.0.1:1"); !errors.Is(err, kv.ErrNotLeader) {
		t.Errorf("node3.Join() = %v, want %v", err, kv.ErrNotLeader)
	}

	if value, err := node2.store.Get("test-key2", time.Now()); err == nil {
		t.Errorf("node2 Get(%q) = %q, nil, want error", "test-key2", value)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitForValue(t *testing.T, st *kv.Store, key, want string) {
	t.Helper()
	waitFor(t, key+" to replicate", func() bool {
		got, err := st.Get(key, time.Now())
		return err == nil && string(got) == want
	})
}

func hasKey(st *kv.Store, key string) bool {
	for e := range st.All() {
		if e.Key == key {
			return true
		}
	}
	return false
}
