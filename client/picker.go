package client

import (
	"sync/atomic"

	"github.com/dynamic-calm/mokv/api"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

func init() {
	balancer.Register(base.NewBalancerBuilder(Scheme, &pickerBuilder{}, base.Config{}))
}

type pickerBuilder struct{}

var _ base.PickerBuilder = (*pickerBuilder)(nil)

func (b *pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	log.Debug().Int("ready_count", len(info.ReadySCs)).Msg("building picker")

	p := &picker{}
	for sc, scInfo := range info.ReadySCs {
		if isLeader, _ := scInfo.Address.Attributes.Value(isLeaderAttr).(bool); isLeader {
			p.leader = sc
			continue
		}
		p.followers = append(p.followers, sc)
	}
	return p
}

type picker struct {
	leader    balancer.SubConn
	followers []balancer.SubConn
	next      atomic.Uint64
}

var _ balancer.Picker = (*picker)(nil)

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	method := info.FullMethodName
	if method == api.KV_Set_FullMethodName || method == api.KV_Delete_FullMethodName {
		if p.leader == nil {
			log.Warn().Str("method", method).Msg("no leader for write")
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}
		return balancer.PickResult{SubConn: p.leader}, nil
	}

	if len(p.followers) > 0 {
		i := p.next.Add(1) % uint64(len(p.followers))
		return balancer.PickResult{SubConn: p.followers[i]}, nil
	}
	if p.leader != nil {
		return balancer.PickResult{SubConn: p.leader}, nil
	}
	return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
}
