package consensus

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dynamic-calm/mokv/kv"
	"github.com/hashicorp/raft"
)

// lenWidth is the width of the length prefix of each snapshot entry.
const lenWidth = 8

// fsm applies committed Raft log entries to a kv.Store.
type fsm struct {
	store *kv.Store
}

var _ raft.FSM = (*fsm)(nil)

// Apply applies a committed log entry. It runs on every node, and its result
// is the error, if any, returned to the proposer.
func (f *fsm) Apply(l *raft.Log) any {
	cmd, err := decode(l.Data)
	if err != nil {
		return err
	}
	return f.store.Apply(cmd)
}

// Snapshot captures every entry, including expired ones, so the snapshot
// matches the replicated state exactly. Each entry is a length-prefixed Set
// log entry, so Restore can replay it with its expiry intact.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	var buf bytes.Buffer
	for e := range f.store.All() {
		b, err := encode(kv.Set(e))
		if err != nil {
			return nil, fmt.Errorf("failed to encode snapshot entry: %w", err)
		}
		buf.Write(binary.BigEndian.AppendUint64(nil, uint64(len(b))))
		buf.Write(b)
	}
	return &snapshot{data: buf.Bytes()}, nil
}

// Restore replaces the store's entries with the ones in the snapshot.
func (f *fsm) Restore(r io.ReadCloser) error {
	defer func() { _ = r.Close() }()

	// Discard the previous state, as required by raft.FSM.
	f.store.Reset()

	b := make([]byte, lenWidth)
	for {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		entry := make([]byte, binary.BigEndian.Uint64(b))
		if _, err := io.ReadFull(r, entry); err != nil {
			return err
		}
		if err, ok := f.Apply(&raft.Log{Data: entry}).(error); ok {
			return err
		}
	}
}

type snapshot struct {
	data []byte
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
