package kv

import (
	"bytes"
	context "context"
	"io"
	"net"
	"testing"
	"time"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
)

func TestAPI(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	defer listener.Close()
	if err != nil {
		t.Fatalf("%s", err)
	}

	server := NewKVServer()
	ready := make(chan bool)
	go func() {
		defer close(ready)
		ready <- true
		if err := server.Serve(listener); err != nil {
			t.Errorf("server error: %v", err)
		}
	}()
	defer server.Stop()

	<-ready

	creds := insecure.NewCredentials()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	clientConn, err := grpc.NewClient(listener.Addr().String(), opts...)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer clientConn.Close()

	client := NewKVClient(clientConn)
	ctx := context.Background()

	expected := []byte("test_value")
	setReq := &SetRequest{Key: "test_key", Value: expected}
	setRes, err := client.Set(ctx, setReq)
	if err != nil {
		t.Fatalf("%s", err)
	}
	if !setRes.Ok {
		t.Fatalf("set res not ok")
	}

	getReq := &GetRequest{Key: "test_key"}
	getRes, err := client.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if string(getRes.Value) != string(expected) {
		t.Fatalf("the values should be equal")
	}

	getRes, err = client.Get(ctx, &GetRequest{Key: "unknown"})
	if err == nil {
		t.Fatal("we should have an error when getting not existing key")
	}

}
func TestStream(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	defer listener.Close()
	if err != nil {
		t.Fatalf("%s", err)
	}

	server := NewKVServer()
	ready := make(chan bool)
	go func() {
		defer close(ready)
		ready <- true
		if err := server.Serve(listener); err != nil {
			t.Errorf("server error: %v", err)
		}
	}()
	defer server.Stop()

	<-ready

	creds := insecure.NewCredentials()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	clientConn, err := grpc.NewClient(listener.Addr().String(), opts...)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer clientConn.Close()

	client := NewKVClient(clientConn)
	ctx := context.Background()

	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for k, v := range testData {
		_, err := client.Set(ctx, &SetRequest{Key: k, Value: v})
		if err != nil {
			t.Fatalf("failed to set test data: %v", err)
		}
	}

	stream, err := client.List(ctx, &Empty{})
	if err != nil {
		t.Fatalf("failed to start list stream: %v", err)
	}

	received := make(map[string]bool)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error receiving from stream: %v", err)
		}

		found := false
		for _, expected := range testData {
			if bytes.Equal(resp.Value, expected) {
				found = true
				received[string(resp.Value)] = true
				break
			}
		}
		if !found {
			t.Errorf("received unexpected value: %s", string(resp.Value))
		}
	}

	for _, v := range testData {
		if !received[string(v)] {
			t.Errorf("missing expected value: %s", string(v))
		}
	}

}

func TestListErrors(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	defer listener.Close()
	if err != nil {
		t.Fatalf("%s", err)
	}
	server := NewKVServer()
	ready := make(chan bool)
	go func() {
		defer close(ready)
		ready <- true
		if err := server.Serve(listener); err != nil {
			t.Errorf("server error: %v", err)
		}
	}()
	defer server.Stop()
	<-ready

	creds := insecure.NewCredentials()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	clientConn, err := grpc.NewClient(listener.Addr().String(), opts...)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer clientConn.Close()

	client := NewKVClient(clientConn)

	t.Run("context cancellation", func(t *testing.T) {
		ctx := context.Background()
		_, err := client.Set(ctx, &SetRequest{
			Key:   "test_key",
			Value: []byte("test_value"),
		})
		if err != nil {
			t.Fatalf("failed to set test data: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.List(ctx, &Empty{})
		if err != nil {
			t.Fatalf("failed to start stream: %v", err)
		}

		// Cancel context before receiving
		cancel()

		// Attempt to receive - should fail with cancelled context
		_, err = stream.Recv()
		if status.Code(err) != codes.Canceled {
			t.Errorf("expected canceled error, got: %v", err)
		}
	})

	t.Run("server shutdown", func(t *testing.T) {
		stream, err := client.List(context.Background(), &Empty{})
		if err != nil {
			t.Fatalf("failed to start stream: %v", err)
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			server.Stop()
		}()

		for {
			_, err := stream.Recv()
			if err != nil {
				code := status.Code(err)
				if code != codes.Unavailable && err != io.EOF {
					t.Errorf("unexpected error: %v", err)
				}
				break
			}
		}
	})
}
