// Package kvtest provides test doubles for package kv.
package kvtest

import (
	"context"
	"errors"

	"github.com/dynamic-calm/mokv/kv"
)

// Replicator is a kv.Replicator that applies commands straight to Store, as
// the only node of a cluster would. It is a follower if Follower is set.
type Replicator struct {
	Store    *kv.Store
	Follower bool
}

var _ kv.Replicator = (*Replicator)(nil)

// Propose applies cmds to r.Store. It returns kv.ErrNotLeader if r is a
// follower.
func (r *Replicator) Propose(ctx context.Context, cmds ...kv.Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Follower {
		return kv.ErrNotLeader
	}
	var errs []error
	for _, cmd := range cmds {
		errs = append(errs, r.Store.Apply(cmd))
	}
	return errors.Join(errs...)
}

// IsLeader reports whether r is not a follower.
func (r *Replicator) IsLeader() bool {
	return !r.Follower
}
