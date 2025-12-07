package server_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/api"
	"github.com/dynamic-calm/mokv/server"
	"github.com/dynamic-calm/mokv/store"
	"github.com/rs/zerolog"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// setupTestServer creates and starts a test server, returning cleanup function
func setupTestServer(t *testing.T) (api.KVClient, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	st := store.New()
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	srv := server.New(st, logger)

	ready := make(chan bool)
	go func() {
		defer close(ready)
		ready <- true
		if err := srv.Serve(listener); err != nil {
			t.Errorf("server error: %v", err)
		}
	}()

	<-ready

	clientConn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create grpc client: %v", err)
	}

	client := api.NewKVClient(clientConn)

	cleanup := func() {
		clientConn.Close()
		srv.Stop()
		listener.Close()
	}

	return client, cleanup
}

func TestAPI(t *testing.T) {
	t.Parallel()

	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("set and get value", func(t *testing.T) {
		expected := []byte("test_value")
		setReq := &api.SetRequest{Key: "test_key", Value: expected}

		setRes, err := client.Set(ctx, setReq)
		if err != nil {
			t.Fatalf("Set() error = %v", err)
		}
		if !setRes.Ok {
			t.Fatal("Set() returned Ok = false, want true")
		}

		getReq := &api.GetRequest{Key: "test_key"}
		getRes, err := client.Get(ctx, getReq)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		if !bytes.Equal(getRes.Value, expected) {
			t.Errorf("Get() = %q, want %q", getRes.Value, expected)
		}
	})

	t.Run("get non-existent key returns error", func(t *testing.T) {
		_, err := client.Get(ctx, &api.GetRequest{Key: "unknown"})
		if err == nil {
			t.Error("Get() with non-existent key: expected error, got nil")
		}
	})
}

func TestStream(t *testing.T) {
	t.Parallel()

	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	// Populate test data
	for k, v := range testData {
		_, err := client.Set(ctx, &api.SetRequest{Key: k, Value: v})
		if err != nil {
			t.Fatalf("failed to set test data: %v", err)
		}
	}

	// Start stream
	stream, err := client.List(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	// Collect all streamed values
	received := make(map[string]bool)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream.Recv() error = %v", err)
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
			t.Errorf("received unexpected value: %q", resp.Value)
		}
	}

	// Verify all expected values were received
	for _, v := range testData {
		if !received[string(v)] {
			t.Errorf("missing expected value: %q", v)
		}
	}
}

func TestListErrors(t *testing.T) {
	t.Parallel()

	client, cleanup := setupTestServer(t)
	defer cleanup()

	t.Run("context cancellation", func(t *testing.T) {
		ctx := context.Background()

		// Set up test data
		_, err := client.Set(ctx, &api.SetRequest{
			Key:   "test_key",
			Value: []byte("test_value"),
		})
		if err != nil {
			t.Fatalf("failed to set test data: %v", err)
		}

		// Create cancellable context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.List(ctx, &emptypb.Empty{})
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}

		// Cancel context before receiving
		cancel()

		// Attempt to receive - should fail with cancelled context
		_, err = stream.Recv()
		if status.Code(err) != codes.Canceled {
			t.Errorf("stream.Recv() after cancel: got code %v, want %v", status.Code(err), codes.Canceled)
		}
	})

	t.Run("server shutdown during stream", func(t *testing.T) {
		// Note: This test reuses the parent server which will be cleaned up
		// This is a limitation but maintains the original logic
		stream, err := client.List(context.Background(), &emptypb.Empty{})
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}

		// Trigger cleanup in background
		go func() {
			time.Sleep(100 * time.Millisecond)
			cleanup()
		}()

		// Expect error when server shuts down
		for {
			_, err := stream.Recv()
			if err != nil {
				code := status.Code(err)
				if code != codes.Unavailable && err != io.EOF {
					t.Errorf("stream.Recv() after shutdown: got code %v, want %v or EOF", code, codes.Unavailable)
				}
				break
			}
		}
	})
}

func TestConcurrency(t *testing.T) {
	t.Parallel()

	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	expected := []byte("test_value")
	const goroutines = 100

	t.Run("concurrent sets", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := range goroutines {
			go func(i int) {
				defer wg.Done()

				setReq := &api.SetRequest{
					Key:   "test_key" + strconv.Itoa(i),
					Value: expected,
				}
				setRes, err := client.Set(ctx, setReq)
				if err != nil {
					t.Errorf("Set() error = %v", err)
					return
				}
				if !setRes.Ok {
					t.Errorf("Set() returned Ok = false, want true")
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("concurrent gets", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := range goroutines {
			go func(i int) {
				defer wg.Done()

				getReq := &api.GetRequest{Key: "test_key" + strconv.Itoa(i)}
				getRes, err := client.Get(ctx, getReq)
				if err != nil {
					t.Errorf("Get() error = %v", err)
					return
				}
				if !bytes.Equal(getRes.Value, expected) {
					t.Errorf("Get() = %q, want %q", getRes.Value, expected)
				}
			}(i)
		}

		wg.Wait()
	})
}
