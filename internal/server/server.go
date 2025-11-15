package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/dynamic-calm/mokv/internal/api"
	"github.com/dynamic-calm/mokv/internal/kv"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type kvServer struct {
	api.KVServer
	KV           kv.KVI
	serverGetter kv.ServerProvider
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
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	logOpts := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
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
		slog.Error("set operation failed", "key", req.Key, "error", err)
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

func interceptorLogger(l *slog.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		l.Log(ctx, slog.Level(lvl), msg, fields...)
	})
}
