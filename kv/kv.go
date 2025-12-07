package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/dynamic-calm/mokv/api"
	"github.com/dynamic-calm/mokv/logger"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

const (
	// RaftRPC is the first byte sent on a connection to identify it as a Raft command
	// for the connection multiplexer.
	RaftRPC = 1

	// RetainSnapshotCount defines how many snapshots to retain.
	RetainSnapshotCount = 2

	// RaftTimeout defines the generic timeout for Raft operations.
	RaftTimeout = 10 * time.Second

	lenWidth = 8
)

var (
	ErrLeaderTimeout = errors.New("timed out waiting for leader")
	ErrNotRaftRPC    = errors.New("not a raft rpc connection")
)

// RequestType identifies the type of operation being applied to the Raft log.
type RequestType uint8

const (
	RequestTypeSet RequestType = iota
	RequestTypeDelete
)

type BatchOperation struct {
	reqType RequestType
	key     string
	value   []byte
	result  chan error
}

// RaftConfig holds configuration specific to the Raft consensus mechanism.
type RaftConfig struct {
	raft.Config
	BindAddr    string
	StreamLayer *StreamLayer
	Bootstrap   bool
	RPCPort     string
}

// KVConfig holds the general configuration for the KV store.
type KVConfig struct {
	Raft    RaftConfig
	DataDir string
}

// Storer defines the interface for a the key-value storage.
type Storer interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	List() <-chan []byte
}

// KV is a distributed key-value store implementation using Raft consensus.
type KV struct {
	cfg   *KVConfig
	store Storer
	raft  *raft.Raft
}

// New creates and initializes a new distributed KV.
func New(store Storer, cfg *KVConfig) (*KV, error) {
	kv := &KV{
		cfg:   cfg,
		store: store,
	}
	if err := kv.setupRaft(kv.cfg.DataDir); err != nil {
		log.Error().Err(err).Msg("failed setting up raft")
		return nil, err
	}
	return kv, nil
}

// Get retrieves a value from the local store.
func (kv *KV) Get(key string) ([]byte, error) {
	return kv.store.Get(key)
}

// Set applies a set operation to the distributed store via Raft.
func (kv *KV) Set(key string, value []byte) error {
	_, err := kv.apply(RequestTypeSet, &api.SetRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf(
			"failed to apply replication setting key: %s, val: %s, from kv: %w",
			key, string(value), err,
		)
	}
	return nil
}

// Delete applies a delete operation to the distributed store via Raft.
func (kv *KV) Delete(key string) error {
	// Replicate Delete
	_, err := kv.apply(RequestTypeDelete, &api.DeleteRequest{Key: key})
	if err != nil {
		return fmt.Errorf("failed to apply replication deleting key: %s from kv: %w", key, err)
	}
	return nil
}

// List returns a channel that streams all values from the local store.
func (kv *KV) List() <-chan []byte {
	return kv.store.List()
}

// GetServers retrieves the current list of servers in the Raft configuration.
func (kv *KV) GetServers() ([]*api.Server, error) {
	future := kv.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("failed on get raft configuration future: %w", err)
	}
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		_, leaderID := kv.raft.LeaderWithID()

		log.Debug().
			Str("server address", string(server.Address)).
			Str("server.ID", string(server.ID)).
			Str("leaderID", string(leaderID)).
			Msg("got server from list")

		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: leaderID == server.ID,
		})
	}
	return servers, nil
}

// Join adds a new node (voter) to the Raft cluster.
// This operation must be executed on the cluster leader.
func (kv *KV) Join(id, addr string) error {
	log.Info().
		Str("id", id).
		Str("addr", addr).
		Msg("attempting to join")

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	isLeader := kv.raft.State() == raft.Leader
	log.Info().
		Bool("am_i_leader", isLeader).
		Msg("join request received")

	if !isLeader {
		leaderAddr := kv.raft.Leader()
		return fmt.Errorf("not the leader, please retry join request with leader at %s", leaderAddr)
	}

	configFuture := kv.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %w", err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		log.Info().
			Str("id", string(srv.ID)).
			Str("addr", string(srv.Address)).
			Msg("existing server")

		if srv.ID == serverID && srv.Address == serverAddr {
			log.Info().
				Str("id", id).
				Msg("server already joined")
			return nil
		}
	}

	addFuture := kv.raft.AddVoter(serverID, serverAddr, 0, RaftTimeout)
	if err := addFuture.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	log.Info().Str("id", id).Msg("successfully added voter")
	return nil
}

// Leave removes a node from the Raft cluster.
func (kv *KV) Leave(id string) error {
	future := kv.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return future.Error()
}

// Close gracefully shuts down the Raft node.
func (kv *KV) Close() error {
	if kv.isLeader() {
		future := kv.raft.LeadershipTransfer()
		if err := future.Error(); err != nil {
			return err
		}
	}
	future := kv.raft.Shutdown()
	return future.Error()
}

// WaitForLeader blocks until the Raft node detects a leader in the cluster or times out.
func (kv *KV) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return ErrLeaderTimeout
		case <-ticker.C:
			if l := kv.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}
func (kv *KV) isLeader() bool {
	return kv.raft.State() == raft.Leader
}

// apply encodes the request and submits it to Raft.
func (kv *KV) apply(reqType RequestType, req proto.Message) (any, error) {
	var buf bytes.Buffer
	// Write the reqType byte to the buffer to differentiate from other requests
	// later on
	if _, err := buf.Write([]byte{byte(reqType)}); err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	// Add the actual request after that first byte
	if _, err := buf.Write(b); err != nil {
		return nil, err
	}

	future := kv.raft.Apply(buf.Bytes(), RaftTimeout)
	if err := future.Error(); err != nil {
		return nil, err
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

func (kv *KV) setupRaft(dataDir string) error {
	fsm := &fsm{kv: kv.store, dataDir: dataDir}

	raftDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return fmt.Errorf("failed to create raft directory: %w", err)
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "log"))
	if err != nil {
		return fmt.Errorf("failed to create log store: %w", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable"))
	if err != nil {
		return err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		RetainSnapshotCount,
		os.Stderr,
	)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		kv.cfg.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	raftLogger := log.With().Str("component", "raft").Logger()
	config.Logger = logger.NewHCLogWrapper(raftLogger)
	config.LocalID = kv.cfg.Raft.LocalID
	config.HeartbeatTimeout = 1 * time.Second
	config.ElectionTimeout = 3 * time.Second
	config.LeaderLeaseTimeout = 500 * time.Millisecond
	config.CommitTimeout = 500 * time.Millisecond
	config.SnapshotInterval = 120 * time.Second
	config.SnapshotThreshold = 8192
	config.MaxAppendEntries = 64
	config.BatchApplyCh = true

	kv.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)

	if err != nil {
		return fmt.Errorf("failed to create new raft: %w", err)
	}

	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)

	if err != nil {
		return fmt.Errorf("failed to check if raft has exisiting state: %w", err)
	}

	if kv.cfg.Raft.Bootstrap && !hasState {
		log.Info().
			Str("id", string(config.LocalID)).
			Str("addr", string(transport.LocalAddr())).
			Msg("bootstrapping first node")

		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: raft.ServerAddress(kv.cfg.Raft.BindAddr),
				},
			},
		}
		err = kv.raft.BootstrapCluster(config).Error()
		if err != nil {
			return fmt.Errorf("failed to bootstrap: %w", err)
		}
		log.Info().Msg("bootstrap successful")
	}

	return nil
}

type fsm struct {
	kv      Storer
	dataDir string
}

// Finite State Machine
var _ raft.FSM = (*fsm)(nil)

// Apply applies a Raft log entry to the local KV store.
// This will get called on every node in the cluster
func (fsm *fsm) Apply(log *raft.Log) any {
	reqType := RequestType(log.Data[0])
	switch reqType {
	case RequestTypeSet:
		return fsm.applySet(log.Data[1:])
	case RequestTypeDelete:
		return fsm.applyDelete(log.Data[1:])
	}
	return nil
}

func (fsm *fsm) applySet(b []byte) any {
	var req api.SetRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}

	err = fsm.kv.Set(req.Key, req.Value)
	if err != nil {
		return err
	}
	return &api.SetResponse{Ok: true}
}

func (fsm *fsm) applyDelete(b []byte) any {
	var req api.DeleteRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	err = fsm.kv.Delete(req.Key)
	if err != nil {
		return err
	}
	return &api.DeleteResponse{Ok: true}
}

// Snapshot creates a snapshot of the current state of the store.
func (fsm *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := chanToReader(fsm.kv.List())
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

// Restore restores the store state from a snapshot.
func (fsm *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(binary.BigEndian.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		element := &api.GetResponse{}
		if err = proto.Unmarshal(buf.Bytes(), element); err != nil {
			return err
		}
		fsm.kv.Set(element.Key, element.Value)
		buf.Reset()
	}
	return nil
}

// Stream layer
var _ raft.StreamLayer = (*StreamLayer)(nil)

// StreamLayer implements raft.StreamLayer to allow using a custom listener
// (e.g., from cmux) with the Hashicorp Raft library.
type StreamLayer struct {
	ln net.Listener
}

func NewStreamLayer(ln net.Listener) *StreamLayer {
	return &StreamLayer{
		ln: ln,
	}
}

// Dial establishes an outgoing connection to another Raft node.
// It prepends the RaftRPC byte to identify the protocol to the remote multiplexer.
func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// Identify to mux this is a raft RPC
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	return conn, err
}

// Accept accepts a new incoming connection.
// It consumes the first byte (the multiplexing header) to verify the protocol
// and provides the clean connection to the Raft library.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read raft identifier: %w", err)

	}
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		conn.Close()
		return nil, ErrNotRaftRPC
	}
	return conn, nil
}

// Close closes the listener.
func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

// Addr returns the listener's network address.
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}

func chanToReader(ch <-chan []byte) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		for data := range ch {
			w.Write(data)
		}
	}()
	return r
}
