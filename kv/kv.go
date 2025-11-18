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
	"github.com/dynamic-calm/mokv/store"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

type KVI interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	List() <-chan []byte
}

type ServerProvider interface {
	GetServers() ([]*api.Server, error)
}

type RequestType uint8

const (
	RequestTypeSet RequestType = iota
	RequestTypeDelete
	lenWidth = 8
)

type BatchOperation struct {
	reqType RequestType
	key     string
	value   []byte
	result  chan error
}

type RaftConfig struct {
	raft.Config
	BindAddr    string
	StreamLayer *StreamLayer
	Bootstrap   bool
	RPCPort     string
}

type KVConfig struct {
	Raft    RaftConfig
	DataDir string
}

type KV struct {
	cfg   *KVConfig
	store store.Storer
	raft  *raft.Raft
}

func New(store store.Storer, cfg *KVConfig) (*KV, error) {
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

func (kv *KV) Set(key string, value []byte) error {
	// Replicate Set
	_, err := kv.apply(RequestTypeSet, &api.SetRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf(
			"failed to apply replication setting key: %s, val: %s, from kv: %w",
			key, string(value), err,
		)
	}
	return nil
}

func (kv *KV) Delete(key string) error {
	// Replicate Delete
	_, err := kv.apply(RequestTypeDelete, &api.DeleteRequest{Key: key})
	if err != nil {
		return fmt.Errorf("failed to apply replication deleting key: %s from kv: %w", key, err)
	}
	return nil
}

func (kv *KV) Get(key string) ([]byte, error) {
	return kv.store.Get(key)
}

func (kv *KV) List() <-chan []byte {
	return kv.store.List()
}

func (kv *KV) apply(reqType RequestType, req proto.Message) (any, error) {
	var buf bytes.Buffer
	// Write the reqType byte to the buffer to differentiate from other requests
	// later on
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	// Add the actual request after that first byte
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	future := kv.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func (kv *KV) GetServers() ([]*api.Server, error) {
	future := kv.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("failed on get raft configuration future: %w", err)
	}
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: kv.raft.Leader() == server.Address,
		})
	}
	return servers, nil
}

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

	log.Info().
		Str("id", id).
		Str("addr", addr).
		Msg("adding voter to cluster")

	addFuture := kv.raft.AddVoter(serverID, serverAddr, 0, time.Second*10)
	if err := addFuture.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	log.Info().
		Str("id", id).
		Msg("successfully added voter")
	return nil
}

func (kv *KV) Leave(id string) error {
	removeFuture := kv.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

func (kv *KV) Close() error {
	f := kv.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return nil
}

func (kv *KV) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return errors.New("timed out")
		case <-ticker.C:
			if l := kv.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

type fsm struct {
	kv      store.Storer
	dataDir string
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
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
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

// Finite State Machine
var _ raft.FSM = (*fsm)(nil)

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

const RaftRPC = 1

type StreamLayer struct {
	ln net.Listener
}

func NewStreamLayer(ln net.Listener) *StreamLayer {
	return &StreamLayer{
		ln: ln,
	}
}

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

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		conn.Close()
		return nil, errors.New("not a raft rpc")
	}
	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

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
