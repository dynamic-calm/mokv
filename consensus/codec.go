package consensus

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/dynamic-calm/mokv/kv"
	"google.golang.org/protobuf/encoding/protowire"
)

type entryType uint8

const (
	entrySet entryType = iota
	entryDelete
	entrySetWithExpiry
	entryExpire
)

const (
	fieldKey      protowire.Number = 1
	fieldValue    protowire.Number = 2
	deadlineWidth                  = 8
)

func encode(cmd kv.Command) ([]byte, error) {
	switch c := cmd.(type) {
	case kv.Set:
		b := appendDeadline([]byte{byte(entrySetWithExpiry)}, c.ExpiresAt)
		return appendFields(b, c.Key, c.Value), nil
	case kv.Delete:
		return appendFields([]byte{byte(entryDelete)}, c.Key, nil), nil
	case kv.Expire:
		b := appendDeadline([]byte{byte(entryExpire)}, c.ExpiresAt)
		return appendFields(b, c.Key, nil), nil
	}
	return nil, fmt.Errorf("unknown command %T", cmd)
}

func decode(b []byte) (kv.Command, error) {
	if len(b) == 0 {
		return nil, errors.New("empty log entry")
	}
	typ, b := entryType(b[0]), b[1:]

	var expiresAt time.Time
	if typ == entrySetWithExpiry || typ == entryExpire {
		if len(b) < deadlineWidth {
			return nil, errors.New("log entry too short for expiry")
		}
		if n := int64(binary.BigEndian.Uint64(b)); n != 0 {
			expiresAt = time.Unix(0, n)
		}
		b = b[deadlineWidth:]
	}

	key, value, err := consumeFields(b)
	if err != nil {
		return nil, fmt.Errorf("decoding log entry: %w", err)
	}
	switch typ {
	case entrySet, entrySetWithExpiry:
		return kv.Set{Key: key, Value: value, ExpiresAt: expiresAt}, nil
	case entryDelete:
		return kv.Delete{Key: key}, nil
	case entryExpire:
		return kv.Expire{Key: key, ExpiresAt: expiresAt}, nil
	}
	return nil, fmt.Errorf("unknown log entry type %d", typ)
}

func appendDeadline(b []byte, t time.Time) []byte {
	var n int64
	if !t.IsZero() {
		n = t.UnixNano()
	}
	return binary.BigEndian.AppendUint64(b, uint64(n))
}

func appendFields(b []byte, key string, value []byte) []byte {
	if key != "" {
		b = protowire.AppendTag(b, fieldKey, protowire.BytesType)
		b = protowire.AppendString(b, key)
	}
	if len(value) > 0 {
		b = protowire.AppendTag(b, fieldValue, protowire.BytesType)
		b = protowire.AppendBytes(b, value)
	}
	return b
}

func consumeFields(b []byte) (key string, value []byte, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", nil, protowire.ParseError(n)
		}
		b = b[n:]

		if typ != protowire.BytesType || (num != fieldKey && num != fieldValue) {
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return "", nil, protowire.ParseError(n)
			}
			b = b[n:]
			continue
		}

		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return "", nil, protowire.ParseError(n)
		}
		b = b[n:]
		if num == fieldKey {
			key = string(v)
		} else {
			value = bytes.Clone(v)
		}
	}
	return key, value, nil
}
