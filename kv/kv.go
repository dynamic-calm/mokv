// Package kv implements a key-value store replicated with Raft.
package kv

import (
	"bytes"
	"context"
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
	// ErrLeaderTimeout is returned when no leader is elected in time.
	ErrLeaderTimeout = errors.New("timed out waiting for leader")
	// ErrNotRaftRPC is returned when a connection does not start with RaftRPC.
	ErrNotRaftRPC = errors.New("not a raft rpc connection")
)

// RequestType identifies the type of operation being applied to the Raft log.
type RequestType uint8

const (
	// RequestTypeSet is a set without expiry, kept for old log entries.
	RequestTypeSet RequestType = iota
	// RequestTypeDelete deletes a key.
	RequestTypeDelete
	// RequestTypeSetWithExpiry is a set whose entry carries an absolute
	// expiration deadline, so every node expires it at the same time.
	RequestTypeSetWithExpiry
	// RequestTypeExpire deletes a key only if its deadline still matches,
	// so a key overwritten after it was found expired survives.
	RequestTypeExpire
)

// RaftConfig holds configuration specific to the Raft consensus mechanism.
type RaftConfig struct {
	raft.Config
	BindAddr    string
	StreamLayer *StreamLayer
	Bootstrap   bool
	RPCPort     string
}

// Config holds the general configuration for the KV store.
type Config struct {
	Raft    RaftConfig
	DataDir string
}

// KV is a Raft node that replicates commands to the Store of every node.
type KV struct {
	cfg  *Config
	raft *raft.Raft
}

var _ Replicator = (*KV)(nil)

// New creates a Raft node that applies replicated commands to store.
func New(store *Store, cfg *Config) (*KV, error) {
	kv := &KV{cfg: cfg}
	if err := kv.setupRaft(store, kv.cfg.DataDir); err != nil {
		return nil, fmt.Errorf("failed to set up raft: %w", err)
	}
	return kv, nil
}

// Propose replicates cmds and returns once they are applied on this node.
// All commands are submitted before waiting so Raft can batch them. The Raft
// enqueue timeout is bounded by the context deadline, if any.
func (kv *KV) Propose(ctx context.Context, cmds ...Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !kv.IsLeader() {
		return kv.notLeader()
	}
	entries := make([][]byte, len(cmds))
	for i, cmd := range cmds {
		b, err := encodeCommand(cmd)
		if err != nil {
			return err
		}
		entries[i] = b
	}

	timeout := RaftTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	futures := make([]raft.ApplyFuture, len(entries))
	for i, b := range entries {
		futures[i] = kv.raft.Apply(b, timeout)
	}

	var errs []error
	for _, f := range futures {
		if err := f.Error(); errors.Is(err, raft.ErrNotLeader) {
			errs = append(errs, kv.notLeader())
		} else if err != nil {
			errs = append(errs, err)
		} else if err, ok := f.Response().(error); ok {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// IsLeader reports whether this node is the Raft leader.
func (kv *KV) IsLeader() bool {
	return kv.raft.State() == raft.Leader
}

func (kv *KV) notLeader() error {
	return fmt.Errorf("%w: leader is %q", ErrNotLeader, kv.raft.Leader())
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

	if !kv.IsLeader() {
		return kv.notLeader()
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
	if kv.IsLeader() {
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

// encodeCommand encodes cmd as a Raft log entry.
func encodeCommand(cmd Command) ([]byte, error) {
	switch c := cmd.(type) {
	case Set:
		return encodeRequest(RequestTypeSetWithExpiry, &api.SetRequest{Key: c.Key, Value: c.Value}, c.ExpiresAt)
	case Delete:
		return encodeRequest(RequestTypeDelete, &api.DeleteRequest{Key: c.Key}, time.Time{})
	case Expire:
		return encodeRequest(RequestTypeExpire, &api.DeleteRequest{Key: c.Key}, c.ExpiresAt)
	}
	return nil, fmt.Errorf("unknown command %T", cmd)
}

// encodeRequest builds a Raft log entry: the reqType byte to differentiate
// requests later on, the expiry deadline for types that carry one, and then
// the actual request.
func encodeRequest(reqType RequestType, req proto.Message, expiresAt time.Time) ([]byte, error) {
	buf := []byte{byte(reqType)}
	if hasExpiry(reqType) {
		buf = binary.BigEndian.AppendUint64(buf, uint64(unixNano(expiresAt)))
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return append(buf, b...), nil
}

// decodeExpiry splits the expiry deadline from the rest of the entry.
func decodeExpiry(b []byte) (time.Time, []byte, error) {
	if len(b) < lenWidth {
		return time.Time{}, nil, errors.New("log entry too short for expiry")
	}
	n := int64(binary.BigEndian.Uint64(b[:lenWidth]))
	if n == 0 {
		return time.Time{}, b[lenWidth:], nil
	}
	return time.Unix(0, n), b[lenWidth:], nil
}

func hasExpiry(reqType RequestType) bool {
	return reqType == RequestTypeSetWithExpiry || reqType == RequestTypeExpire
}

// unixNano encodes a zero time as 0, meaning no expiry.
func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func (kv *KV) setupRaft(store *Store, dataDir string) error {
	f := &fsm{store: store}

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
		return fmt.Errorf("failed to create stable store: %w", err)
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
		f,
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

		bootstrapCfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: raft.ServerAddress(kv.cfg.Raft.BindAddr),
				},
			},
		}
		err = kv.raft.BootstrapCluster(bootstrapCfg).Error()
		if err != nil {
			return fmt.Errorf("failed to bootstrap: %w", err)
		}
		log.Info().Msg("bootstrap successful")
	}

	return nil
}

type fsm struct {
	store *Store
}

var _ raft.FSM = (*fsm)(nil)

// Apply applies a Raft log entry to the local KV store.
// It is called on every node in the cluster.
func (f *fsm) Apply(l *raft.Log) any {
	if len(l.Data) == 0 {
		return errors.New("empty log entry")
	}
	reqType := RequestType(l.Data[0])
	data := l.Data[1:]
	var expiresAt time.Time
	if hasExpiry(reqType) {
		var err error
		if expiresAt, data, err = decodeExpiry(data); err != nil {
			return err
		}
	}

	cmd, err := decodeCommand(reqType, data, expiresAt)
	if err != nil {
		return err
	}
	return f.store.Apply(cmd)
}

// decodeCommand decodes the request in b into the Command of reqType.
func decodeCommand(reqType RequestType, b []byte, expiresAt time.Time) (Command, error) {
	switch reqType {
	case RequestTypeSet, RequestTypeSetWithExpiry:
		var req api.SetRequest
		if err := proto.Unmarshal(b, &req); err != nil {
			return nil, err
		}
		return Set{Key: req.Key, Value: req.Value, ExpiresAt: expiresAt}, nil
	case RequestTypeDelete, RequestTypeExpire:
		var req api.DeleteRequest
		if err := proto.Unmarshal(b, &req); err != nil {
			return nil, err
		}
		if reqType == RequestTypeExpire {
			return Expire{Key: req.Key, ExpiresAt: expiresAt}, nil
		}
		return Delete{Key: req.Key}, nil
	}
	return nil, fmt.Errorf("unknown request type %d", reqType)
}

// Snapshot creates a snapshot of the current state of the store.
// Each entry is written as a length-prefixed set log entry, so Restore can
// replay it through Apply with its expiry intact. Expired entries are kept so
// the snapshot matches the replicated state exactly.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	var buf bytes.Buffer
	for e := range f.store.All() {
		b, err := encodeCommand(Set(e))
		if err != nil {
			return nil, fmt.Errorf("failed to encode snapshot entry: %w", err)
		}
		buf.Write(binary.BigEndian.AppendUint64(nil, uint64(len(b))))
		buf.Write(b)
	}
	return &snapshot{data: buf.Bytes()}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	data []byte
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

// Restore replaces the store state with the one in the snapshot.
func (f *fsm) Restore(r io.ReadCloser) error {
	defer func() { _ = r.Close() }()

	// Discard the previous state, as required by raft.FSM.
	f.store.Reset()

	b := make([]byte, lenWidth)
	for {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		entry := make([]byte, binary.BigEndian.Uint64(b))
		if _, err = io.ReadFull(r, entry); err != nil {
			return err
		}
		if err, ok := f.Apply(&raft.Log{Data: entry}).(error); ok {
			return err
		}
	}
	return nil
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// StreamLayer implements raft.StreamLayer to allow using a custom listener
// (e.g., from cmux) with the Hashicorp Raft library.
type StreamLayer struct {
	ln net.Listener
}

// NewStreamLayer returns a StreamLayer that accepts connections from ln.
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
	if _, err := conn.Write([]byte{byte(RaftRPC)}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
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
		_ = conn.Close()
		return nil, fmt.Errorf("failed to read raft identifier: %w", err)

	}
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		_ = conn.Close()
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
