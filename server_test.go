package kv

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServer(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	defer listener.Close()
	if err != nil {
		t.Fatalf("%s", err)
	}
	creds := insecure.NewCredentials()
	opts := grpc.WithTransportCredentials(creds)
	clientOpts := []grpc.DialOption{opts}
	cc, err := grpc.NewClient(listener.Addr().String(), clientOpts...)
	defer cc.Close()
	if err != nil {
		t.Fatalf("%s", err)
	}
	server := NewKVServer()
	client := NewKVClient(cc)
	go func() {
		defer server.Stop()
		server.Serve(listener)
	}()

	ctx := context.Background()
	result := &GetResponse{Value: []byte("ok")}
	req := &GetRequest{Key: []byte("test-key")}
	res, err := client.Get(ctx, req)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if string(res.Value) != string(result.Value) {
		t.Fatalf("the values should be equal")
	}
}
