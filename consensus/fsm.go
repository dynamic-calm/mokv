package consensus

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/dynamic-calm/mokv/kv"
	"github.com/hashicorp/raft"
)

const lenWidth = 8

type fsm struct {
	store *kv.Store
}

var _ raft.FSM = (*fsm)(nil)

func (f *fsm) Apply(l *raft.Log) any {
	cmd, err := decode(l.Data)
	if err != nil {
		return err
	}
	return f.store.Apply(cmd)
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	var buf bytes.Buffer
	for e := range f.store.All() {
		b, err := encode(kv.Set(e))
		if err != nil {
			return nil, fmt.Errorf("encoding snapshot entry %q: %w", e.Key, err)
		}
		buf.Write(binary.BigEndian.AppendUint64(nil, uint64(len(b))))
		buf.Write(b)
	}
	return &snapshot{data: buf.Bytes()}, nil
}

func (f *fsm) Restore(r io.ReadCloser) error {
	defer func() { _ = r.Close() }()
	f.store.Reset()

	b := make([]byte, lenWidth)
	for {
		_, err := io.ReadFull(r, b)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading snapshot entry length: %w", err)
		}
		entry := make([]byte, binary.BigEndian.Uint64(b))
		if _, err := io.ReadFull(r, entry); err != nil {
			return fmt.Errorf("reading snapshot entry: %w", err)
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
