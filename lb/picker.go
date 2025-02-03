package lb

import (
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

type Builder struct{}

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Builder{}, base.Config{}),
	)
}

var _ base.PickerBuilder = (*Builder)(nil)

func (b *Builder) Build(info base.PickerBuildInfo) balancer.Picker {
	var leader balancer.SubConn
	var followers []balancer.SubConn

	for sc, scInfo := range info.ReadySCs {
		isLeader := scInfo.Address.Attributes.Value("is_leader").(bool)
		if isLeader {
			leader = sc
			continue
		}
		followers = append(followers, sc)
	}

	return &Picker{
		leader:    leader,
		followers: followers,
	}
}

type Picker struct {
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

var _ balancer.Picker = (*Picker)(nil)

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var result balancer.PickResult
	if p.leader == nil && len(p.followers) == 0 {
		return result, balancer.ErrNoSubConnAvailable
	}

	if strings.Contains(info.FullMethodName, "Set") ||
		strings.Contains(info.FullMethodName, "Delete") {
		if p.leader == nil {
			return result, balancer.ErrNoSubConnAvailable
		}
		result.SubConn = p.leader
		return result, nil
	}

	if len(p.followers) > 0 {
		result.SubConn = p.nextFollower()
		return result, nil
	}

	if p.leader != nil {
		result.SubConn = p.leader
		return result, nil
	}

	return result, balancer.ErrNoSubConnAvailable
}

func (p *Picker) nextFollower() balancer.SubConn {
	numFollowers := len(p.followers)
	if numFollowers == 0 {
		return nil
	}
	cur := atomic.AddUint64(&p.current, uint64(1))
	idx := int(cur % uint64(numFollowers))
	return p.followers[idx]
}
