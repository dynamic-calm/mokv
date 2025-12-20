package discovery

import (
	"strings"
	"sync/atomic"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Builder{}, base.Config{}),
	)
}

var _ base.PickerBuilder = (*Builder)(nil)

// Builder implements base.PickerBuilder to construct a custom Picker.
type Builder struct{}

// Build creates a new Picker instance using the provided build information.
// It segregates sub-connections into leader and followers based on attributes.
func (b *Builder) Build(info base.PickerBuildInfo) balancer.Picker {
	log.Info().
		Int("ready_count", len(info.ReadySCs)).
		Msg("building picker")

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

// Picker implements balancer.Picker to handle RPC load balancing logic.
// It routes write operations to the leader and read operations to followers
// in a round-robin fashion.
type Picker struct {
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

var _ balancer.Picker = (*Picker)(nil)

// Pick selects the appropriate sub-connection for the RPC.
// It inspects the method name to determine if the operation is a
// write (Set/Delete) or a read.
func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var result balancer.PickResult

	if p.leader == nil && len(p.followers) == 0 {
		return result, balancer.ErrNoSubConnAvailable
	}

	methodName := info.FullMethodName

	// No available connections
	if p.leader == nil && len(p.followers) == 0 {
		log.Debug().
			Str("method", methodName).
			Msg("pick failed: no available subconns")
		return result, balancer.ErrNoSubConnAvailable
	}

	isWrite := strings.Contains(methodName, "Set") ||
		strings.Contains(methodName, "Delete")

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
			Uint64("follower_index", p.current%uint64(len(p.followers))).
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

func (p *Picker) nextFollower() balancer.SubConn {
	numFollowers := len(p.followers)
	if numFollowers == 0 {
		return nil
	}
	cur := atomic.AddUint64(&p.current, uint64(1))
	idx := int(cur % uint64(numFollowers))
	return p.followers[idx]
}
