package server

import (
	"context"
	"fmt"

	"github.com/mateopresacastro/mokv/api"
	"github.com/mateopresacastro/mokv/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type kvServer struct {
	api.KVServer
	store store.Store
}

func New(store store.Store, opts ...grpc.ServerOption) *grpc.Server {
	s := grpc.NewServer(opts...)
	srv := &kvServer{store: store}
	api.RegisterKVServer(s, srv)
	return s
}

func (s *kvServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	value, err := s.store.Get(req.Key)
	if err != nil {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err() // TODO improve error handling. Do boundary layers.
	}
	return &api.GetResponse{Value: value}, nil
}

func (s *kvServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	err := s.store.Set(req.Key, req.Value)
	if err != nil {
		return &api.SetResponse{Ok: false}, status.New(codes.Internal, "something went wrong storing data").Err()
	}
	return &api.SetResponse{Ok: true}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	err := s.store.Delete(req.Key)
	if err != nil {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err()
	}
	return &api.DeleteResponse{Ok: true}, nil
}

func (s *kvServer) List(req *api.Empty, stream grpc.ServerStreamingServer[api.GetResponse]) error {
	for value := range s.store.List() {
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

func (s *kvServer) HealthCheck(ctx context.Context, req *api.Empty) (*api.HealthCheckResponse, error) {
	return &api.HealthCheckResponse{Ok: true}, nil
}

func (s *kvServer) notFoundMsg(key string) string {
	return fmt.Sprintf("no value for key: %s", key)
}
