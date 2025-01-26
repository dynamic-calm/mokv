package discovery

import (
	"log/slog"
	"net"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

type Membership struct {
	Config
	serf    *serf.Serf
	handler Handler
	events  chan serf.Event
}

func New(h Handler, cfg Config) (*Membership, error) {
	c := &Membership{handler: h, Config: cfg}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}

	return c, nil
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()

	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.BindAddr = addr.IP.String()
	mlConfig.BindPort = addr.Port

	config.MemberlistConfig = mlConfig
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName

	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go func() {
		defer close(m.events)
		m.eventHandler()
	}()

	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Membership) eventHandler() {
	for e := range m.events {
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
	err := m.handler.Join(member.Name, member.Tags["rpc_addr"])
	if err != nil {
		slog.Error("failed to join", "member", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	err := m.handler.Leave(member.Name)
	if err != nil {
		slog.Error("failed to leave", "member", member)
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *Membership) Leave() error {
	return m.serf.Leave()
}
