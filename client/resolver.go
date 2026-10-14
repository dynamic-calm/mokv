package client

import (
	"context"
	"sync"
	"time"

	"github.com/dynamic-calm/mokv/api"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Scheme is the target scheme that resolves to the cluster servers and
// balances requests across them, as in "mokv://host:port".
const Scheme = "mokv"

const (
	serviceConfig     = `{"loadBalancingConfig":[{"` + Scheme + `":{}}]}`
	isLeaderAttr      = "is_leader"
	reResolveInterval = 2 * time.Second
	resolveTimeout    = 5 * time.Second
)

func init() {
	resolver.Register(&resolverBuilder{})
}

type resolverBuilder struct{}

var _ resolver.Builder = (*resolverBuilder)(nil)

func (b *resolverBuilder) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	conn, err := grpc.NewClient(target.URL.Host, dialOpts...)
	if err != nil {
		return nil, err
	}
	r := &clusterResolver{
		cc:            cc,
		conn:          conn,
		api:           api.NewKVClient(conn),
		serviceConfig: cc.ParseServiceConfig(serviceConfig),
		done:          make(chan struct{}),
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	go r.resolvePeriodically()
	return r, nil
}

func (b *resolverBuilder) Scheme() string {
	return Scheme
}

type clusterResolver struct {
	mu            sync.Mutex
	cc            resolver.ClientConn
	conn          *grpc.ClientConn
	api           api.KVClient
	serviceConfig *serviceconfig.ParseResult
	done          chan struct{}
}

var _ resolver.Resolver = (*clusterResolver)(nil)

func (r *clusterResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), resolveTimeout)
	defer cancel()
	res, err := r.api.GetServers(ctx, &emptypb.Empty{})
	if err != nil {
		log.Error().Err(err).Msg("failed to resolve servers")
		r.cc.ReportError(err)
		return
	}
	var addrs []resolver.Address
	for _, s := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr:       s.RpcAddr,
			Attributes: attributes.New(isLeaderAttr, s.IsLeader),
		})
	}
	if err := r.cc.UpdateState(resolver.State{Addresses: addrs, ServiceConfig: r.serviceConfig}); err != nil {
		log.Error().Err(err).Msg("failed to update resolver state")
	}
}

func (r *clusterResolver) Close() {
	close(r.done)
	if err := r.conn.Close(); err != nil {
		log.Error().Err(err).Msg("failed to close resolver connection")
	}
}

func (r *clusterResolver) resolvePeriodically() {
	ticker := time.NewTicker(reResolveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.ResolveNow(resolver.ResolveNowOptions{})
		case <-r.done:
			return
		}
	}
}
