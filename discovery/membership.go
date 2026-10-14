// Package discovery handles cluster membership with Serf.
package discovery

import (
	stdlog "log"
	"net"
	"sync"

	"github.com/dynamic-calm/mokv/logger"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/rs/zerolog/log"
)

const rpcAddrTag = "rpc_addr"

// Handler is notified when members join or leave the cluster.
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// Config configures a Membership.
type Config struct {
	// NodeName uniquely identifies the node in the cluster.
	NodeName string
	// BindAddr is the address Serf listens on.
	BindAddr string
	// RPCAddr is the address the node serves RPCs and Raft on. It is passed
	// to the Handler of other members when this node joins.
	RPCAddr string
	// StartJoinAddrs are the Serf addresses of existing members to join.
	StartJoinAddrs []string
}

// Membership tracks the members of the cluster and notifies a Handler when
// they join or leave.
type Membership struct {
	serf    *serf.Serf
	handler Handler
	events  chan serf.Event
	done    chan struct{}
	leave   sync.Once
	err     error
}

// New starts Serf and joins the cluster through cfg.StartJoinAddrs, if any.
// Other members joining and leaving are reported to h until Leave is called.
func New(h Handler, cfg Config) (*Membership, error) {
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, err
	}
	stdLogger := stdlog.New(logger.NewZerologWriter(log.With().Str("component", "serf").Logger()), "", 0)

	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Logger = stdLogger
	mlConfig.BindAddr = addr.IP.String()
	mlConfig.BindPort = addr.Port
	mlConfig.AdvertisePort = addr.Port

	m := &Membership{
		handler: h,
		events:  make(chan serf.Event),
		done:    make(chan struct{}),
	}
	config := serf.DefaultConfig()
	config.Init()
	config.Logger = stdLogger
	config.MemberlistConfig = mlConfig
	config.EventCh = m.events
	config.Tags = map[string]string{rpcAddrTag: cfg.RPCAddr}
	config.NodeName = cfg.NodeName

	m.serf, err = serf.Create(config)
	if err != nil {
		return nil, err
	}
	go m.handleEvents()

	if len(cfg.StartJoinAddrs) > 0 {
		if _, err := m.serf.Join(cfg.StartJoinAddrs, true); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Members returns the current members of the cluster.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave gracefully leaves the cluster and shuts down Serf. Calls after the
// first return the first result.
func (m *Membership) Leave() error {
	m.leave.Do(func() {
		defer close(m.done)
		if m.err = m.serf.Leave(); m.err != nil {
			return
		}
		m.err = m.serf.Shutdown()
	})
	return m.err
}

func (m *Membership) handleEvents() {
	for {
		var e serf.Event
		select {
		case <-m.done:
			return
		case e = <-m.events:
		}

		var handle func(serf.Member)
		switch e.EventType() {
		case serf.EventMemberJoin:
			handle = m.handleJoin
		case serf.EventMemberLeave:
			handle = m.handleLeave
		default:
			continue
		}
		for _, member := range e.(serf.MemberEvent).Members {
			if member.Name != m.serf.LocalMember().Name {
				handle(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	log.Info().Str("member", member.Name).Str("addr", member.Addr.String()).Msg("member joined")
	if err := m.handler.Join(member.Name, member.Tags[rpcAddrTag]); err != nil {
		log.Error().Err(err).Str("member", member.Name).Msg("failed to handle join")
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	log.Info().Str("member", member.Name).Str("addr", member.Addr.String()).Msg("member left")
	if err := m.handler.Leave(member.Name); err != nil {
		log.Error().Err(err).Str("member", member.Name).Msg("failed to handle leave")
	}
}
