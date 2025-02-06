package mokv

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/mateopresacastro/mokv/api"
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
	SetRequestType    RequestType = 0
	DeleteRequestType RequestType = 1
	lenWidth                      = 8
)

type KVConfig struct {
	Raft struct {
		raft.Config
		StreamLayer
		Bootstrap bool
		BindAddr  string
		RPCPort   string
	}
	DataDir string
}

type KV struct {
	cfg   *KVConfig
	store Store
	raft  *raft.Raft
}

func NewKV(store Store, cfg *KVConfig) (*KV, error) {
	kv := &KV{cfg: cfg, store: store}
	if err := kv.setupRaft(kv.cfg.DataDir); err != nil {
		slog.Error("failed setting up raft", "err", err)
		return nil, err
	}
	return kv, nil
}

func (kv *KV) Set(key string, value []byte) error {
	err := kv.store.Set(key, value)
	if err != nil {
		return fmt.Errorf("failed to set key: %s, val: %s from kv: %w", key, string(value), err)
	}
	// Replicate Set
	_, err = kv.apply(SetRequestType, &api.SetRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("failed to apply raft set: %w", err)
	}
	return nil
}

func (kv *KV) Delete(key string) error {
	err := kv.store.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete key: %s from kv: %w", key, err)
	}
	// Replicate Delete
	_, err = kv.apply(DeleteRequestType, &api.DeleteRequest{Key: key})
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
	slog.Info("attempting to join", "id", id, "addr", addr)

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	isLeader := kv.raft.State() == raft.Leader
	slog.Info("join request received", "am_i_leader", isLeader)

	if !isLeader {
		slog.Info("not leader, forwarding join request", "leader_addr", kv.raft.Leader())
		return fmt.Errorf("not the leader, cannot process join")
	}

	slog.Info("checking configuration")
	configFuture := kv.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %w", err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		slog.Info("existing server", "id", srv.ID, "addr", srv.Address)
		if srv.ID == serverID && srv.Address == serverAddr {
			slog.Info("server already joined", "id", id)
			return nil
		}
	}

	slog.Info("adding voter to cluster", "id", id, "addr", addr)
	addFuture := kv.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	if err := kv.WaitForLeader(3 * time.Second); err != nil {
		return fmt.Errorf("failed waiting for leader after join: %w", err)
	}

	slog.Info("successfully added voter", "id", id)
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

func (kv *KV) setupRaft(dataDir string) error {
	fsm := &fsm{kv: kv.store}

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
		&kv.cfg.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	config.LocalID = kv.cfg.Raft.LocalID

	// To override in tests
	if kv.cfg.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = kv.cfg.Raft.HeartbeatTimeout
	}
	if kv.cfg.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = kv.cfg.Raft.ElectionTimeout
	}
	if kv.cfg.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = kv.cfg.Raft.LeaderLeaseTimeout
	}
	if kv.cfg.Raft.CommitTimeout != 0 {
		config.CommitTimeout = kv.cfg.Raft.CommitTimeout
	}

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
		return fmt.Errorf("failed to if raft has exisiting state: %w", err)
	}

	if kv.cfg.Raft.Bootstrap && !hasState {
		slog.Info("bootstrapping first node",
			"id", config.LocalID,
			"addr", transport.LocalAddr())

		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		err = kv.raft.BootstrapCluster(config).Error()
		if err != nil {
			return fmt.Errorf("failed to bootstrap: %w", err)
		}
		slog.Info("bootstrap successful")
	}

	return nil
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

	// Here we call the actual raft Apply method
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

// Finite State Machine
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	kv Store
}

// This will get called on every node in the cluster
func (fsm *fsm) Apply(log *raft.Log) any {
	buf := log.Data
	// Get the reqType from the first byte of the buffer
	reqType := RequestType(buf[0])
	switch reqType {
	case SetRequestType:
		return fsm.applySet(buf[1:])
	case DeleteRequestType:
		return fsm.applyDelete(buf[1:])
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
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
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
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
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
		return nil, err
	}
	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, errors.New("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
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
