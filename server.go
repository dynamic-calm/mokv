package kv

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type kvServer struct {
	KVServer
	store map[string][]byte
	mu    sync.RWMutex
}

func NewKVServer() *grpc.Server {
	s := grpc.NewServer()
	srv := &kvServer{store: map[string][]byte{}}
	RegisterKVServer(s, srv)
	return s
}

func (s *kvServer) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.store[req.Key]
	if !ok {
		return nil, status.New(codes.NotFound, fmt.Sprintf("no value for key: %s", req.Key)).Err()
	}
	return &GetResponse{Value: value}, nil
}

func (s *kvServer) Set(ctx context.Context, req *SetRequest) (*SetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[req.Key] = req.Value
	return &SetResponse{Ok: true}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.store[req.Key]
	if !ok {
		return nil, status.New(codes.NotFound, fmt.Sprintf("no value for key: %s", req.Key)).Err()
	}
	delete(s.store, req.Key)
	return &DeleteResponse{Ok: true}, nil
}

func (s *kvServer) List(req *Empty, stream KV_ListServer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, value := range s.store {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			if err := stream.Send(&GetResponse{Value: value}); err != nil {
				return err
			}
		}
	}

	return nil
}
