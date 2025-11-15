package discovery

import (
	"log/slog"
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
	slog.Info("building picker", "ready_count", len(info.ReadySCs))
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

	methodName := info.FullMethodName
	isWrite := strings.Contains(methodName, "Set") ||
		strings.Contains(methodName, "Delete")

	// No available connections
	if p.leader == nil && len(p.followers) == 0 {
		slog.Debug("pick failed: no available subconns",
			"method", methodName)
		return result, balancer.ErrNoSubConnAvailable
	}

	// Write operations must go to leader
	if isWrite {
		if p.leader == nil {
			slog.Debug("pick failed: write operation but no leader",
				"method", methodName)
			return result, balancer.ErrNoSubConnAvailable
		}
		result.SubConn = p.leader
		slog.Debug("picked leader for write", "method", methodName)
		return result, nil
	}

	// Read operations prefer followers for load distribution
	if len(p.followers) > 0 {
		result.SubConn = p.nextFollower()
		slog.Debug("picked follower for read",
			"method", methodName,
			"follower_index", p.current%uint64(len(p.followers)))
		return result, nil
	}

	// Fall back to leader for reads if no followers available
	if p.leader != nil {
		result.SubConn = p.leader
		slog.Debug("picked leader for read (no followers)", "method", methodName)
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
