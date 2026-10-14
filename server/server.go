// Package server implements the mokv gRPC service.
package server

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/dynamic-calm/mokv/api"
	"github.com/dynamic-calm/mokv/kv"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// KV is the key-value store the server exposes.
type KV interface {
	Get(key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	List() iter.Seq[kv.Entry]
}

// Cluster lists the servers in the cluster.
type Cluster interface {
	Servers() ([]kv.Server, error)
}

type kvServer struct {
	api.UnimplementedKVServer
	svc     KV
	cluster Cluster
}

// New returns a gRPC server that serves svc and cluster through the KV API,
// along with health checks. Requests are logged to logger.
func New(svc KV, cluster Cluster, logger zerolog.Logger, opts ...grpc.ServerOption) *grpc.Server {
	logOpts := []logging.Option{
		logging.WithLogOnEvents(logging.FinishCall),
		logging.WithLevels(logging.DefaultServerCodeToLevel),
	}
	opts = append(opts,
		grpc.ChainStreamInterceptor(
			selector.StreamServerInterceptor(
				logging.StreamServerInterceptor(interceptorLogger(logger), logOpts...),
				selector.MatchFunc(skipHealthAndReflectionRequests),
			),
		),
		grpc.ChainUnaryInterceptor(
			selector.UnaryServerInterceptor(
				logging.UnaryServerInterceptor(interceptorLogger(logger), logOpts...),
				selector.MatchFunc(skipHealthAndReflectionRequests),
			),
		),
	)
	s := grpc.NewServer(opts...)

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, healthSrv)

	api.RegisterKVServer(s, &kvServer{svc: svc, cluster: cluster})
	return s
}

func (s *kvServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	value, err := s.svc.Get(req.Key)
	if err != nil {
		return nil, toStatus(err)
	}
	return &api.GetResponse{Key: req.Key, Value: value}, nil
}

func (s *kvServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	if !req.Ttl.IsValid() {
		return nil, status.Error(codes.InvalidArgument, "ttl must be a valid duration")
	}
	if err := s.svc.Set(ctx, req.Key, req.Value, req.Ttl.AsDuration()); err != nil {
		return nil, toStatus(err)
	}
	return &api.SetResponse{Ok: true}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	if err := s.svc.Delete(ctx, req.Key); err != nil {
		return nil, toStatus(err)
	}
	return &api.DeleteResponse{Ok: true}, nil
}

func (s *kvServer) List(req *emptypb.Empty, stream grpc.ServerStreamingServer[api.GetResponse]) error {
	for e := range s.svc.List() {
		if err := stream.Context().Err(); err != nil {
			return toStatus(err)
		}
		if err := stream.Send(&api.GetResponse{Key: e.Key, Value: e.Value}); err != nil {
			return err
		}
	}
	return nil
}

func (s *kvServer) GetServers(ctx context.Context, req *emptypb.Empty) (*api.GetServersResponse, error) {
	servers, err := s.cluster.Servers()
	if err != nil {
		return nil, toStatus(err)
	}
	res := &api.GetServersResponse{Servers: make([]*api.Server, len(servers))}
	for i, srv := range servers {
		res.Servers[i] = toServer(srv)
	}
	return res, nil
}

func toServer(s kv.Server) *api.Server {
	return &api.Server{Id: s.ID, RpcAddr: s.Addr, IsLeader: s.IsLeader}
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, kv.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, kv.ErrInvalidTTL):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, kv.ErrNotLeader):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Internal, err.Error())
}

func interceptorLogger(l zerolog.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		var event *zerolog.Event
		switch lvl {
		case logging.LevelDebug:
			event = l.Debug()
		case logging.LevelWarn:
			event = l.Warn()
		case logging.LevelError:
			event = l.Error()
		default:
			event = l.Info()
		}
		event.Fields(fields).Msg(msg)
	})
}

func skipHealthAndReflectionRequests(_ context.Context, c interceptors.CallMeta) bool {
	return c.FullMethod() != "/grpc.health.v1.Health/Check" &&
		c.FullMethod() != "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
}
