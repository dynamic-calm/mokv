package kv

import (
	"context"

	"google.golang.org/grpc"
)

type kvServer struct {
	UnimplementedKVServer
}

func NewKVServer() *grpc.Server {
	s := grpc.NewServer()
	srv := &kvServer{}
	RegisterKVServer(s, srv)
	return s
}

func (s *kvServer) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	return &GetResponse{Value: []byte("ok")}, nil
}
