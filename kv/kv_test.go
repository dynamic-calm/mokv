package kv_test

import (
	"net"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/kv"
	"github.com/soheilhy/cmux"
)

func TestDistributedKVReplication(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	store1 := kv.NewStore()
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener for node 1: %v", err)
	}
	mux1 := cmux.New(ln1)
	raftLn1 := mux1.Match(cmux.Any()) // For testing, accept any connection as Raft

	cfg1 := &kv.Config{
		DataDir: dir1,
	}
	cfg1.Raft.BindAddr = ln1.Addr().String()
	cfg1.Raft.RPCPort = "3000"
	cfg1.Raft.LocalID = "node-1"
	cfg1.Raft.Bootstrap = true
	cfg1.Raft.StreamLayer = kv.NewStreamLayer(raftLn1)

	node1, err := kv.New(store1, cfg1)
	if err != nil {
		t.Fatalf("failed to create node 1: %v", err)
	}
	t.Cleanup(func() { _ = node1.Close() })

	go func() {
		if err := mux1.Serve(); err != nil {
			t.Logf("mux1 serve error: %v", err)
		}
	}()

	// Setup second node
	store2 := kv.NewStore()
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener for node 2: %v", err)
	}
	mux2 := cmux.New(ln2)
	raftLn2 := mux2.Match(cmux.Any())

	cfg2 := &kv.Config{
		DataDir: dir2,
	}
	cfg2.Raft.BindAddr = ln2.Addr().String()
	cfg2.Raft.RPCPort = "3001"
	cfg2.Raft.LocalID = "node-2"
	cfg2.Raft.Bootstrap = false
	cfg2.Raft.StreamLayer = kv.NewStreamLayer(raftLn2)

	node2, err := kv.New(store2, cfg2)
	if err != nil {
		t.Fatalf("failed to create node 2: %v", err)
	}
	t.Cleanup(func() { _ = node2.Close() })

	go func() {
		if err := mux2.Serve(); err != nil {
			t.Logf("mux2 serve error: %v", err)
		}
	}()

	if err := node1.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("node1.WaitForLeader() = %v, want nil", err)
	}

	// Join node2 to the cluster
	err = node1.Join("node-2", cfg2.Raft.BindAddr)
	if err != nil {
		t.Fatalf("failed to join node 2 to cluster: %v", err)
	}

	// Write data to node 1
	err = node1.Set(t.Context(), "test-key", []byte("test-value"), time.Time{})
	if err != nil {
		t.Fatalf("failed to write to node 1: %v", err)
	}

	// Read from node 2
	waitForValue(t, node2, "test-key", "test-value")

	// Test leader
	servers, err := node1.GetServers()
	if err != nil {
		t.Fatalf("node1.GetServers() = %v, want nil", err)
	}
	if len(servers) != 2 {
		t.Fatalf("len(node1.GetServers()) = %d, want 2", len(servers))
	}
	if !servers[0].IsLeader {
		t.Errorf("servers[0].IsLeader = false, want true")
	}
	if servers[1].IsLeader {
		t.Errorf("servers[1].IsLeader = true, want false")
	}

	// Leave node 2
	if err := node1.Leave("node-2"); err != nil {
		t.Fatalf("node1.Leave(%q) = %v, want nil", "node-2", err)
	}
	waitFor(t, "node-2 to leave", func() bool {
		servers, err := node1.GetServers()
		return err == nil && len(servers) == 1
	})

	// Setup third node
	dir3 := t.TempDir()

	store3 := kv.NewStore()
	ln3, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener for node 3: %v", err)
	}
	mux3 := cmux.New(ln3)
	raftLn3 := mux3.Match(cmux.Any())

	cfg3 := &kv.Config{
		DataDir: dir3,
	}
	cfg3.Raft.BindAddr = ln3.Addr().String()
	cfg3.Raft.RPCPort = "3002"
	cfg3.Raft.LocalID = "node-3"
	cfg3.Raft.Bootstrap = false
	cfg3.Raft.StreamLayer = kv.NewStreamLayer(raftLn3)

	node3, err := kv.New(store3, cfg3)
	if err != nil {
		t.Fatalf("failed to create node 3: %v", err)
	}
	t.Cleanup(func() { _ = node3.Close() })

	go func() {
		if err := mux3.Serve(); err != nil {
			t.Logf("mux3 serve error: %v", err)
		}
	}()

	// Join node3 to the cluster
	err = node1.Join("node-3", cfg3.Raft.BindAddr)
	if err != nil {
		t.Fatalf("failed to join node 3 to cluster: %v", err)
	}

	// Write data to node 1
	err = node1.Set(t.Context(), "test-key2", []byte("test-value2"), time.Time{})
	if err != nil {
		t.Fatalf("failed to write to node 1: %v", err)
	}

	// Read from node3
	waitForValue(t, node3, "test-key2", "test-value2")

	// Write an expiring key and check the leader removes it on every node
	err = node1.Set(t.Context(), "ttl-key", []byte("ttl-value"), time.Now().Add(2*time.Second))
	if err != nil {
		t.Fatalf("failed to write expiring key to node 1: %v", err)
	}
	waitForValue(t, node3, "ttl-key", "ttl-value")
	for name, st := range map[string]*kv.Store{"node1": store1, "node3": store3} {
		waitFor(t, name+" to remove ttl-key", func() bool {
			return !hasKey(st, "ttl-key")
		})
	}

	// Read from node2. Since it left no data should be replicated.
	if value, err := node2.Get("test-key2"); err == nil {
		t.Errorf("node2.Get(%q) = %q, nil, want error", "test-key2", value)
	}
}

// waitFor polls cond until it returns true or the test times out.
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

// waitForValue waits until node returns want for key.
func waitForValue(t *testing.T, node *kv.KV, key, want string) {
	t.Helper()
	waitFor(t, key+" to replicate", func() bool {
		got, err := node.Get(key)
		return err == nil && string(got) == want
	})
}

// hasKey reports whether st holds key, including expired entries.
func hasKey(st *kv.Store, key string) bool {
	for e := range st.All() {
		if e.Key == key {
			return true
		}
	}
	return false
}
