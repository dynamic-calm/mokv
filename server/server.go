package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/mateopresacastro/kv/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type kvServer struct {
	api.KVServer
	store map[string][]byte
	mu    sync.RWMutex
}

func New() *grpc.Server {
	s := grpc.NewServer()
	srv := &kvServer{store: map[string][]byte{}}
	api.RegisterKVServer(s, srv)
	return s
}

func (s *kvServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.store[req.Key]
	if !ok {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err()
	}
	return &api.GetResponse{Value: value}, nil
}

func (s *kvServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[req.Key] = req.Value
	return &api.SetResponse{Ok: true}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.store[req.Key]
	if !ok {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err()
	}
	delete(s.store, req.Key)
	return &api.DeleteResponse{Ok: true}, nil
}

func (s *kvServer) List(req *api.Empty, stream grpc.ServerStreamingServer[api.GetResponse]) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, value := range s.store {
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
