// Package consensus replicates kv commands across the cluster with Raft.
//
// Commands are persisted in the Raft log and in snapshots in a binary format
// owned by this package. Changes to the format must keep existing logs and
// snapshots readable.
package consensus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/dynamic-calm/mokv/kv"
	"github.com/dynamic-calm/mokv/logger"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rs/zerolog/log"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	maxPool             = 5
)

// ErrLeaderTimeout is returned when no leader is elected in time.
var ErrLeaderTimeout = errors.New("timed out waiting for leader")

// Config configures a Node.
type Config struct {
	// ID uniquely identifies the node in the cluster.
	ID string
	// Addr is the address other nodes reach the node's Raft listener at.
	Addr string
	// Listener accepts the node's Raft connections. Each connection must
	// start with the byte Match looks for.
	Listener net.Listener
	// DataDir is where the Raft log and snapshots are stored.
	DataDir string
	// Bootstrap makes the node start a new cluster if it has no state.
	Bootstrap bool
}

// Node is a Raft node that applies replicated commands to a kv.Store.
type Node struct {
	id   raft.ServerID
	raft *raft.Raft
}

var _ kv.Replicator = (*Node)(nil)

// New starts a Raft node that applies replicated commands to store.
func New(store *kv.Store, cfg Config) (*Node, error) {
	r, err := newRaft(&fsm{store: store}, cfg)
	if err != nil {
		return nil, err
	}
	return &Node{id: raft.ServerID(cfg.ID), raft: r}, nil
}

// Propose replicates cmds and returns once they are applied on this node.
// All commands are submitted before waiting so Raft can batch them. The Raft
// enqueue timeout is bounded by the context deadline, if any.
func (n *Node) Propose(ctx context.Context, cmds ...kv.Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !n.IsLeader() {
		return n.notLeader()
	}
	entries := make([][]byte, len(cmds))
	for i, cmd := range cmds {
		b, err := encode(cmd)
		if err != nil {
			return err
		}
		entries[i] = b
	}

	timeout := raftTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	futures := make([]raft.ApplyFuture, len(entries))
	for i, b := range entries {
		futures[i] = n.raft.Apply(b, timeout)
	}

	var errs []error
	for _, f := range futures {
		err := f.Error()
		switch {
		case errors.Is(err, raft.ErrNotLeader):
			errs = append(errs, n.notLeader())
		case err != nil:
			errs = append(errs, err)
		default:
			if err, ok := f.Response().(error); ok {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// IsLeader reports whether this node is the Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// Servers returns the servers in the Raft configuration.
func (n *Node) Servers() ([]kv.Server, error) {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("getting raft configuration: %w", err)
	}
	_, leaderID := n.raft.LeaderWithID()
	var servers []kv.Server
	for _, s := range future.Configuration().Servers {
		servers = append(servers, kv.Server{
			ID:       string(s.ID),
			Addr:     string(s.Address),
			IsLeader: s.ID == leaderID,
		})
	}
	return servers, nil
}

// Join adds the node with id at addr to the cluster as a voter. It must run
// on the leader; elsewhere it returns an error wrapping kv.ErrNotLeader.
func (n *Node) Join(id, addr string) error {
	log.Info().Str("id", id).Str("addr", addr).Msg("attempting to join")

	if !n.IsLeader() {
		return n.notLeader()
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return fmt.Errorf("getting raft configuration: %w", err)
	}
	for _, s := range future.Configuration().Servers {
		if s.ID == serverID && s.Address == serverAddr {
			log.Info().Str("id", id).Msg("server already joined")
			return nil
		}
	}

	if err := n.raft.AddVoter(serverID, serverAddr, 0, raftTimeout).Error(); err != nil {
		return fmt.Errorf("adding voter %q: %w", id, err)
	}
	log.Info().Str("id", id).Msg("successfully added voter")
	return nil
}

// Leave removes the node with id from the cluster.
func (n *Node) Leave(id string) error {
	return n.raft.RemoveServer(raft.ServerID(id), 0, 0).Error()
}

// WaitForLeader blocks until the node sees a leader or timeout passes.
func (n *Node) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return ErrLeaderTimeout
		case <-ticker.C:
			if n.raft.Leader() != "" {
				return nil
			}
		}
	}
}

// Close hands leadership over if this node leads other voters, then shuts
// the node down. The node is shut down even if the handover fails.
func (n *Node) Close() error {
	var errs []error
	if n.IsLeader() {
		if err := n.transferLeadership(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := n.raft.Shutdown().Error(); err != nil {
		errs = append(errs, fmt.Errorf("shutting down raft: %w", err))
	}
	return errors.Join(errs...)
}

func (n *Node) transferLeadership() error {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return fmt.Errorf("getting raft configuration: %w", err)
	}
	for _, s := range future.Configuration().Servers {
		if s.ID != n.id && s.Suffrage == raft.Voter {
			if err := n.raft.LeadershipTransfer().Error(); err != nil {
				return fmt.Errorf("transferring leadership: %w", err)
			}
			return nil
		}
	}
	return nil
}

func (n *Node) notLeader() error {
	return fmt.Errorf("%w: leader is %q", kv.ErrNotLeader, n.raft.Leader())
}

func newRaft(f raft.FSM, cfg Config) (*raft.Raft, error) {
	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating raft directory: %w", err)
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "log"))
	if err != nil {
		return nil, fmt.Errorf("creating log store: %w", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable"))
	if err != nil {
		return nil, fmt.Errorf("creating stable store: %w", err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating snapshot store: %w", err)
	}
	transport := raft.NewNetworkTransport(&streamLayer{ln: cfg.Listener}, maxPool, raftTimeout, os.Stderr)

	config := raft.DefaultConfig()
	config.Logger = logger.NewHCLogWrapper(log.With().Str("component", "raft").Logger())
	config.LocalID = raft.ServerID(cfg.ID)
	config.HeartbeatTimeout = 1 * time.Second
	config.ElectionTimeout = 3 * time.Second
	config.LeaderLeaseTimeout = 500 * time.Millisecond
	config.CommitTimeout = 500 * time.Millisecond
	config.SnapshotInterval = 120 * time.Second
	config.SnapshotThreshold = 8192
	config.MaxAppendEntries = 64
	config.BatchApplyCh = true

	r, err := raft.NewRaft(config, f, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("creating raft: %w", err)
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("checking for existing raft state: %w", err)
	}
	if cfg.Bootstrap && !hasState {
		log.Info().Str("id", cfg.ID).Str("addr", cfg.Addr).Msg("bootstrapping first node")
		bootstrapCfg := raft.Configuration{
			Servers: []raft.Server{{ID: config.LocalID, Address: raft.ServerAddress(cfg.Addr)}},
		}
		if err := r.BootstrapCluster(bootstrapCfg).Error(); err != nil {
			return nil, fmt.Errorf("bootstrapping cluster: %w", err)
		}
		log.Info().Msg("bootstrap successful")
	}
	return r, nil
}
