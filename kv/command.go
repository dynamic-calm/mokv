package kv

import "time"

// Command is a change to a Store. Commands are replicated to every node and
// applied in the same order, so applying one must be deterministic.
type Command interface {
	command()
}

// Set stores Value under Key until ExpiresAt. A zero ExpiresAt means the
// entry never expires.
type Set struct {
	Key       string
	Value     []byte
	ExpiresAt time.Time
}

// Delete removes Key.
type Delete struct {
	Key string
}

// Expire removes Key only if it still expires at ExpiresAt, so an entry
// overwritten after it was found expired survives.
type Expire struct {
	Key       string
	ExpiresAt time.Time
}

func (Set) command()    {}
func (Delete) command() {}
func (Expire) command() {}
