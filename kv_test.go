package mokv_test

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mateopresacastro/mokv"
	"github.com/mateopresacastro/mokv/config"
	"github.com/mateopresacastro/mokv/internal/store"
	"github.com/soheilhy/cmux"
)

func TestDistributedKVReplication(t *testing.T) {
	dir1 := filepath.Join(os.TempDir(), "node-1")
	dir2 := filepath.Join(os.TempDir(), "node-2")
	os.MkdirAll(dir1, 0755)
	os.MkdirAll(dir2, 0755)
	defer os.RemoveAll(dir1)
	defer os.RemoveAll(dir2)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	if err != nil {
		t.Fatalf("failed to setup server TLS: %v", err)
	}

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
	})
	if err != nil {
		t.Fatalf("failed to setup peer TLS: %v", err)
	}

	store1 := store.New()
	ln1, err := net.Listen("tcp", "127.0.0.1:3001")
	if err != nil {
		t.Fatalf("failed to create listener for node 1: %v", err)
	}
	mux1 := cmux.New(ln1)
	raftLn1 := mux1.Match(cmux.Any()) // For testing, accept any connection as Raft

	cfg1 := &mokv.KVConfig{
		DataDir: dir1,
	}
	cfg1.Raft.BindAddr = "127.0.0.1:3001"
	cfg1.Raft.RPCPort = "3000"
	cfg1.Raft.LocalID = "node-1"
	cfg1.Raft.Bootstrap = true
	cfg1.Raft.StreamLayer = *mokv.NewStreamLayer(
		raftLn1,
		serverTLSConfig,
		peerTLSConfig,
	)

	node1, err := mokv.NewKV(store1, cfg1)
	if err != nil {
		t.Fatalf("failed to create node 1: %v", err)
	}

	go func() {
		if err := mux1.Serve(); err != nil {
			t.Logf("mux1 serve error: %v", err)
		}
	}()

	// Setup second node
	store2 := store.New()
	ln2, err := net.Listen("tcp", "127.0.0.1:3002")
	if err != nil {
		t.Fatalf("failed to create listener for node 2: %v", err)
	}
	mux2 := cmux.New(ln2)
	raftLn2 := mux2.Match(cmux.Any())

	cfg2 := &mokv.KVConfig{
		DataDir: dir2,
	}
	cfg2.Raft.BindAddr = "127.0.0.1:3002"
	cfg2.Raft.RPCPort = "3001"
	cfg2.Raft.LocalID = "node-2"
	cfg2.Raft.Bootstrap = false
	cfg2.Raft.StreamLayer = *mokv.NewStreamLayer(
		raftLn2,
		serverTLSConfig,
		peerTLSConfig,
	)

	node2, err := mokv.NewKV(store2, cfg2)
	if err != nil {
		t.Fatalf("failed to create node 2: %v", err)
	}

	go func() {
		if err := mux2.Serve(); err != nil {
			t.Logf("mux2 serve error: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)

	// Join node2 to the cluster
	dkv1 := node1
	err = dkv1.Join("node-2", "127.0.0.1:3002")
	if err != nil {
		t.Fatalf("failed to join node 2 to cluster: %v", err)
	}

	// Write data to node 1
	err = node1.Set("test-key", []byte("test-value"))
	if err != nil {
		t.Fatalf("failed to write to node 1: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Read from node 2
	value, err := node2.Get("test-key")
	if err != nil {
		t.Fatalf("failed to read from node 2: %v", err)
	}
	if string(value) != "test-value" {
		t.Errorf("replication failed: got %s, want test-value", string(value))
	}

	// Test leader
	servers, err := dkv1.GetServers()
	if err != nil {
		t.Fatalf("failed to get servers: %v", err)
	}
	if len(servers) != 2 {
		t.Fatal("we should have two servers")
	}

	if !servers[0].IsLeader {
		t.Fatalf("the node 1 should be the leader: %v", err)
	}

	if servers[1].IsLeader {
		t.Fatalf("the node 2 should not be the leader: %v", err)
	}

	// Leave node 2
	dkv1.Leave("node-2")
	time.Sleep(1 * time.Second)
	servers, err = dkv1.GetServers()
	if err != nil {
		t.Fatalf("failed to get servers: %v", err)
	}
	if len(servers) != 1 {
		t.Fatal("we should have one server")
	}

	// Setup third node
	dir3 := filepath.Join(os.TempDir(), "node-3")
	os.MkdirAll(dir3, 0755)
	defer os.RemoveAll(dir3)

	store3 := store.New()
	ln3, err := net.Listen("tcp", "127.0.0.1:3003")
	if err != nil {
		t.Fatalf("failed to create listener for node 3: %v", err)
	}
	mux3 := cmux.New(ln3)
	raftLn3 := mux3.Match(cmux.Any())

	cfg3 := &mokv.KVConfig{
		DataDir: dir3,
	}
	cfg3.Raft.BindAddr = "127.0.0.1:3003"
	cfg3.Raft.RPCPort = "3002"
	cfg3.Raft.LocalID = "node-3"
	cfg3.Raft.Bootstrap = false
	cfg3.Raft.StreamLayer = *mokv.NewStreamLayer(
		raftLn3,
		serverTLSConfig,
		peerTLSConfig,
	)

	node3, err := mokv.NewKV(store3, cfg3)
	if err != nil {
		t.Fatalf("failed to create node 2: %v", err)
	}

	go func() {
		if err := mux3.Serve(); err != nil {
			t.Logf("mux3 serve error: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	// Join node3 to the cluster
	err = dkv1.Join("node-3", "127.0.0.1:3003")
	if err != nil {
		t.Fatalf("failed to join node3  to cluster: %v", err)
	}

	// Write data to node 1
	err = node1.Set("test-key2", []byte("test-value2"))
	if err != nil {
		t.Fatalf("failed to write to node 1: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Read from node3
	value, err = node3.Get("test-key2")
	if err != nil {
		t.Fatalf("failed to read from node 3: %v", err)
	}
	if string(value) != "test-value2" {
		t.Errorf("replication failed: got %s, want test-value2", string(value))
	}

	// Read from node2. Since it left no data should be replicated.
	value, err = node2.Get("test-key2")
	if err == nil {
		t.Fatalf("we should no have be able to read the data from a node that left the cluster: %v", err)
	}

	if string(value) == "test-value2" {
		t.Errorf("we read the data from a node not in the cluser")
	}

}
