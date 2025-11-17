package server

import (
	"context"
	"fmt"
	"os"

	"github.com/dynamic-calm/mokv/api"
	"github.com/dynamic-calm/mokv/kv"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
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

func New(KV kv.KVI, opts ...grpc.ServerOption) *grpc.Server {
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	logOpts := []logging.Option{
		logging.WithLogOnEvents(logging.FinishCall),
		logging.WithLevels(logging.DefaultServerCodeToLevel),
	}

	// Middleware for streaming and unary requests
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			logging.StreamServerInterceptor(interceptorLogger(logger), logOpts...),
		),
	), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		logging.UnaryServerInterceptor(interceptorLogger(logger), logOpts...),
	)))
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

func (s *kvServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	value, err := s.KV.Get(req.Key)
	if err != nil {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err()
	}
	return &api.GetResponse{Value: value, Key: req.Key}, nil
}

func (s *kvServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	err := s.KV.Set(req.Key, req.Value)
	if err != nil {
		s.logger.Error().Err(err).Str("key", req.Key).Msg("set operation failed")
		return &api.SetResponse{Ok: false}, status.Errorf(codes.Internal, "failed to set key: %v", err)
	}
	return &api.SetResponse{Ok: true}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	err := s.KV.Delete(req.Key)
	if err != nil {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err()
	}
	return &api.DeleteResponse{Ok: true}, nil
}

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
