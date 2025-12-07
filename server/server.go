package server

import (
	"context"
	"fmt"

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
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type kvServer struct {
	api.KVServer
	KV           kv.KVI
	serverGetter kv.ServerProvider
	logger       zerolog.Logger
}

func NewServerGetter(kv kv.KVI) kv.ServerProvider {
	return &kvServerGetter{kv: kv}
}

type kvServerGetter struct {
	kv kv.KVI
}

func (kg *kvServerGetter) GetServers() ([]*api.Server, error) {
	if provider, ok := kg.kv.(kv.ServerProvider); ok {
		return provider.GetServers()
	}
	return nil, fmt.Errorf("kv store does not support getting servers")
}

// New creates and configures a new gRPC server instance with logging middleware,
// health checks, and the registered KV service.
func New(KV kv.KVI, logger zerolog.Logger, opts ...grpc.ServerOption) *grpc.Server {
	logOpts := []logging.Option{
		logging.WithLogOnEvents(logging.FinishCall),
		logging.WithLevels(logging.DefaultServerCodeToLevel),
	}

	// Middleware for streaming and unary requests
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

	serverGetter := NewServerGetter(KV)
	srv := &kvServer{
		KV:           KV,
		serverGetter: serverGetter,
	}
	api.RegisterKVServer(s, srv)
	return s
}

// Get retrieves a value for a specific key from the store.
func (s *kvServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	value, err := s.KV.Get(req.Key)
	if err != nil {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err()
	}
	return &api.GetResponse{Value: value, Key: req.Key}, nil
}

// Set stores a key-value pair in the store.
func (s *kvServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	if err := s.KV.Set(req.Key, req.Value); err != nil {
		s.logger.Error().Err(err).Str("key", req.Key).Msg("set operation failed")
		return &api.SetResponse{Ok: false}, status.Errorf(codes.Internal, "failed to set key: %v", err)
	}
	return &api.SetResponse{Ok: true}, nil
}

// Delete removes a key from the store.
func (s *kvServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	if err := s.KV.Delete(req.Key); err != nil {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err()
	}
	return &api.DeleteResponse{Ok: true}, nil
}

// List streams all existing key-value pairs from the store to the client.
func (s *kvServer) List(req *emptypb.Empty, stream grpc.ServerStreamingServer[api.GetResponse]) error {
	for value := range s.KV.List() {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			if err := stream.Send(&api.GetResponse{Value: value}); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetServers returns the list of nodes in the cluster.
func (s *kvServer) GetServers(ctx context.Context, req *emptypb.Empty) (*api.GetServersResponse, error) {
	servers, err := s.serverGetter.GetServers()
	if err != nil {
		return nil, err
	}
	return &api.GetServersResponse{Servers: servers}, nil
}

func (s *kvServer) notFoundMsg(key string) string {
	return fmt.Sprintf("no value for key: %s", key)
}

func interceptorLogger(l zerolog.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		var event *zerolog.Event
		switch lvl {
		case logging.LevelDebug:
			event = l.Debug()
		case logging.LevelInfo:
			event = l.Info()
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
