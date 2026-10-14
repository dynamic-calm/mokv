package consensus

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/api"
	"github.com/dynamic-calm/mokv/kv"
	"google.golang.org/protobuf/proto"
)

func TestCodecRoundTrip(t *testing.T) {
	t.Parallel()

	expiresAt := time.Unix(0, time.Now().Add(time.Hour).UnixNano())
	for _, cmd := range []kv.Command{
		kv.Set{Key: "key", Value: []byte("value"), ExpiresAt: expiresAt},
		kv.Set{Key: "forever", Value: []byte("value")},
		kv.Set{Key: "empty"},
		kv.Delete{Key: "key"},
		kv.Expire{Key: "key", ExpiresAt: expiresAt},
	} {
		b, err := encode(cmd)
		if err != nil {
			t.Fatalf("encode(%+v) error = %v", cmd, err)
		}
		got, err := decode(b)
		if err != nil {
			t.Fatalf("decode(encode(%+v)) error = %v", cmd, err)
		}
		if !reflect.DeepEqual(got, cmd) {
			t.Errorf("decode(encode(%+v)) = %+v, want unchanged", cmd, got)
		}
	}
}

// TestCodecCompatible checks entries match the format written before this
// package owned it, when they were the api request messages.
func TestCodecCompatible(t *testing.T) {
	t.Parallel()

	expiresAt := time.Unix(0, 1_700_000_000_000_000_000)
	deadline := binary.BigEndian.AppendUint64(nil, uint64(expiresAt.UnixNano()))
	legacy := func(typ entryType, deadline []byte, msg proto.Message) []byte {
		b, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("proto.Marshal() error = %v", err)
		}
		return append(append([]byte{byte(typ)}, deadline...), b...)
	}

	tests := []struct {
		cmd    kv.Command
		legacy []byte
	}{
		{
			cmd:    kv.Set{Key: "key", Value: []byte("value"), ExpiresAt: expiresAt},
			legacy: legacy(entrySetWithExpiry, deadline, &api.SetRequest{Key: "key", Value: []byte("value")}),
		},
		{
			cmd:    kv.Delete{Key: "key"},
			legacy: legacy(entryDelete, nil, &api.DeleteRequest{Key: "key"}),
		},
		{
			cmd:    kv.Expire{Key: "key", ExpiresAt: expiresAt},
			legacy: legacy(entryExpire, deadline, &api.DeleteRequest{Key: "key"}),
		},
	}
	for _, tt := range tests {
		got, err := encode(tt.cmd)
		if err != nil {
			t.Fatalf("encode(%+v) error = %v", tt.cmd, err)
		}
		if !bytes.Equal(got, tt.legacy) {
			t.Errorf("encode(%+v) = %x, want %x", tt.cmd, got, tt.legacy)
		}
	}

	old := legacy(entrySet, nil, &api.SetRequest{Key: "key", Value: []byte("value")})
	got, err := decode(old)
	if err != nil {
		t.Fatalf("decode(%x) error = %v", old, err)
	}
	if want := (kv.Set{Key: "key", Value: []byte("value")}); !reflect.DeepEqual(got, want) {
		t.Errorf("decode(%x) = %+v, want %+v", old, got, want)
	}
}

func TestDecodeInvalid(t *testing.T) {
	t.Parallel()

	for _, b := range [][]byte{
		nil,
		{byte(entryExpire), 0, 0},
		{byte(entryDelete), 0x0a, 0x05, 'k'},
		{99},
	} {
		if cmd, err := decode(b); err == nil {
			t.Errorf("decode(%x) = %+v, nil, want error", b, cmd)
		}
	}
}

func TestMatch(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		in   string
		want bool
	}{
		{in: string([]byte{raftHeader}) + "rest", want: true},
		{in: "PRI * HTTP/2.0", want: false},
		{in: "", want: false},
	} {
		if got := Match(strings.NewReader(tt.in)); got != tt.want {
			t.Errorf("Match(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
