package discovery

import (
	"context"
	"fmt"
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

const Name = "mokv"

type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
}

func init() {
	resolver.Register(&Resolver{})
	log.Info().Msg("registered mokv resolver")
}

var _ resolver.Builder = (*Resolver)(nil)

func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	log.Info().
		Str("target", target.URL.Host).
		Msg("building resolver")

	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)
	var err error
	r.resolverConn, err = grpc.NewClient(target.URL.Host, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (r *Resolver) Scheme() string {
	return Name
}

var _ resolver.Resolver = (*Resolver)(nil)

func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewKVClient(r.resolverConn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := client.GetServers(ctx, &emptypb.Empty{})
	if err != nil {
		log.Error().
			Err(err).
			Msg("failed to resolve server")
		r.clientConn.ReportError(err)
		return
	}
	var addrs []resolver.Address
	for _, server := range res.Servers {
		log.Info().
			Interface("server", server).
			Msg("got server")
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,
			Attributes: attributes.New(
				"is_leader",
				server.IsLeader,
			),
		})
	}
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		log.Error().
			Err(err).
			Msg("failed to close connection")
		return
	}
}
