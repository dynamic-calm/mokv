package client

import (
	"sync/atomic"

	"github.com/dynamic-calm/mokv/api"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Scheme, &pickerBuilder{}, base.Config{}),
	)
}

var _ base.PickerBuilder = (*pickerBuilder)(nil)

// pickerBuilder builds a picker from the resolved addresses.
type pickerBuilder struct{}

// Build creates a new picker using the provided build information.
// It segregates sub-connections into leader and followers based on attributes.
func (b *pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	log.Info().
		Int("ready_count", len(info.ReadySCs)).
		Msg("building picker")

	var leader balancer.SubConn
	var followers []balancer.SubConn

	for sc, scInfo := range info.ReadySCs {
		isLeader := scInfo.Address.Attributes.Value(isLeaderAttr).(bool)
		if isLeader {
			leader = sc
			continue
		}
		followers = append(followers, sc)
	}

	return &picker{
		leader:    leader,
		followers: followers,
	}
}

// picker implements balancer.Picker to handle RPC load balancing logic.
// It routes write operations to the leader and read operations to followers
// in a round-robin fashion.
type picker struct {
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   atomic.Uint64
}

var _ balancer.Picker = (*picker)(nil)

// Pick selects the appropriate sub-connection for the RPC.
// It inspects the method name to determine if the operation is a
// write (Set/Delete) or a read.
func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var result balancer.PickResult

	methodName := info.FullMethodName

	// No available connections
	if p.leader == nil && len(p.followers) == 0 {
		log.Debug().
			Str("method", methodName).
			Msg("pick failed: no available subconns")
		return result, balancer.ErrNoSubConnAvailable
	}

	isWrite := methodName == api.KV_Set_FullMethodName ||
		methodName == api.KV_Delete_FullMethodName

	// Write operations must go to leader
	if isWrite {
		if p.leader == nil {
			log.Warn().
				Str("method", methodName).
				Msg("pick failed: write operation but no leader")
			return result, balancer.ErrNoSubConnAvailable
		}
		result.SubConn = p.leader
		log.Debug().
			Str("method", methodName).
			Msg("picked leader for write")
		return result, nil
	}

	// Read operations prefer followers for load distribution
	if len(p.followers) > 0 {
		result.SubConn = p.nextFollower()
		log.Debug().
			Str("method", methodName).
			Uint64("follower_index", p.current.Load()%uint64(len(p.followers))).
			Msg("picked follower for read")
		return result, nil
	}

	// Fall back to leader for reads if no followers available
	if p.leader != nil {
		result.SubConn = p.leader
		log.Debug().
			Str("method", methodName).
			Msg("picked leader for read (no followers)")
		return result, nil
	}

	return result, balancer.ErrNoSubConnAvailable
}

func (p *picker) nextFollower() balancer.SubConn {
	numFollowers := len(p.followers)
	if numFollowers == 0 {
		return nil
	}
	cur := p.current.Add(1)
	idx := int(cur % uint64(numFollowers))
	return p.followers[idx]
}
