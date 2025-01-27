package kv

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/mateopresacastro/mokv/api"
	"github.com/mateopresacastro/mokv/kv/store"
	"google.golang.org/protobuf/proto"
)

type RequestType uint8

const (
	SetRequestType    RequestType = 0
	DeleteRequestType RequestType = 1
	lenWidth                      = 8
)

type Config struct {
	Raft struct {
		raft.Config
		StreamLayer
		Bootstrap bool
	}
	dataDir string
}

type DistributedKV struct {
	cfg  Config
	kv   KV
	raft *raft.Raft
}

func NewDistributedKV(store store.Store, cfg Config) (KV, error) {
	kv := New(store)
	dkv := &DistributedKV{cfg: cfg, kv: kv}
	if err := dkv.setupRaft(dkv.cfg.dataDir); err != nil {
		return nil, err
	}
	return dkv, nil
}

func (dkv *DistributedKV) Set(key string, value []byte) error {
	var rt RequestType
	// Replicate Set
	_, err := dkv.apply(rt, &api.SetRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	return nil
}

func (dkv *DistributedKV) Delete(key string) error {
	var rt RequestType
	// Replicate Delete
	_, err := dkv.apply(rt, &api.DeleteRequest{Key: key})
	if err != nil {
		return err
	}
	return nil
}

func (dkv *DistributedKV) Get(key string) ([]byte, error) {
	return dkv.kv.Get(key)
}

func (dkv *DistributedKV) List() <-chan []byte {
	return dkv.kv.List()
}

func (dkv *DistributedKV) setupRaft(dataDir string) error {
	fsm := &fsm{kv: dkv.kv}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "log"))
	if err != nil {
		return err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
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
		return err
	}
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		&dkv.cfg.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)
	config := raft.DefaultConfig()
	config.LocalID = dkv.cfg.Raft.LocalID

	// To override in tests
	if dkv.cfg.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = dkv.cfg.Raft.HeartbeatTimeout
	}
	if dkv.cfg.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = dkv.cfg.Raft.ElectionTimeout
	}
	if dkv.cfg.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = dkv.cfg.Raft.LeaderLeaseTimeout
	}
	if dkv.cfg.Raft.CommitTimeout != 0 {
		config.CommitTimeout = dkv.cfg.Raft.CommitTimeout
	}

	dkv.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}

	if dkv.cfg.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		err = dkv.raft.BootstrapCluster(config).Error()
	}

	return err
}

func (dkv *DistributedKV) apply(reqType RequestType, req proto.Message) (any, error) {
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
	future := dkv.raft.Apply(buf.Bytes(), timeout)
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
	kv KV
}

func (fsm *fsm) Apply(record *raft.Log) any {
	buf := record.Data
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
