package consensus

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

const raftHeader byte = 1

var errNotRaft = errors.New("not a raft connection")

// Match reports whether the connection that r reads from is a Raft
// connection. It is meant for a connection multiplexer such as cmux.
func Match(r io.Reader) bool {
	b := make([]byte, 1)
	if _, err := io.ReadFull(r, b); err != nil {
		return false
	}
	return b[0] == raftHeader
}

type streamLayer struct {
	ln net.Listener
}

var _ raft.StreamLayer = (*streamLayer)(nil)

func (s *streamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte{raftHeader}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (s *streamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(conn, b); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("reading raft header: %w", err)
	}
	if b[0] != raftHeader {
		_ = conn.Close()
		return nil, errNotRaft
	}
	return conn, nil
}

func (s *streamLayer) Close() error {
	return s.ln.Close()
}

func (s *streamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
