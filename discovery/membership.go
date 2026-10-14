// Package discovery handles cluster membership with Serf.
package discovery

import (
	stdlog "log"
	"net"
	"sync"

	"github.com/dynamic-calm/mokv/logger"
	"github.com/rs/zerolog/log"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

// Handler is notified when members join or leave the cluster.
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// rpcAddrTag is the Serf tag that carries a member's RPC address.
const rpcAddrTag = "rpc_addr"

// MembershipConfig holds the configuration for the Serf-based discovery mechanism.
type MembershipConfig struct {
	NodeName string
	// BindAddr is the address Serf listens on.
	BindAddr string
	// RPCAddr is the address the node serves RPCs and Raft on. It is passed
	// to the Handler of other members when this node joins.
	RPCAddr        string
	StartJoinAddrs []string
}

// Membership manages the Serf cluster membership and handles node events.
type Membership struct {
	MembershipConfig
	serf    *serf.Serf
	handler Handler
	events  chan serf.Event
	done    chan struct{}
	leave   sync.Once
	err     error
}

// NewMembership creates a Membership, starts Serf, and joins the cluster
// if seed addresses are provided.
func NewMembership(h Handler, cfg MembershipConfig) (*Membership, error) {
	m := &Membership{handler: h, MembershipConfig: cfg}
	if err := m.setupSerf(); err != nil {
		return nil, err
	}

	return m, nil
}

// Members returns the list of current members in the Serf cluster.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave gracefully causes the local node to leave the Serf cluster and
// shuts down Serf. Calls after the first return the first result.
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

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	// Setup logger
	serfLogger := log.With().Str("component", "serf").Logger()
	stdLogger := stdlog.New(logger.NewZerologWriter(serfLogger), "", 0)

	// Serf config
	config := serf.DefaultConfig()
	config.Init()
	config.Logger = stdLogger

	// Memberlist config
	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Logger = stdLogger
	mlConfig.BindAddr = addr.IP.String()
	mlConfig.BindPort = addr.Port
	mlConfig.AdvertisePort = addr.Port
	config.MemberlistConfig = mlConfig

	m.events = make(chan serf.Event)
	m.done = make(chan struct{})
	config.EventCh = m.events
	config.Tags = map[string]string{rpcAddrTag: m.RPCAddr}
	config.NodeName = m.NodeName

	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()

	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Membership) eventHandler() {
	for {
		var e serf.Event
		select {
		case <-m.done:
			return
		case e = <-m.events:
		}
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	log.Info().
		Str("member_name", member.Name).
		Str("member_addr", member.Addr.String()).
		Msg("handling join event")

	err := m.handler.Join(member.Name, member.Tags[rpcAddrTag])
	if err != nil {
		log.Error().Err(err).Msg("failed to join")
		return
	}
	log.Info().Str("member", member.Name).Msg("successfully handled join")
}

func (m *Membership) handleLeave(member serf.Member) {
	log.Info().
		Str("member_name", member.Name).
		Str("member_addr", member.Addr.String()).
		Msg("handling leave event")

	err := m.handler.Leave(member.Name)
	if err != nil {
		log.Error().Err(err).Str("member", member.Name).Msg("failed to process leave")
	}
}
