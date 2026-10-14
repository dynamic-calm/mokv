package server_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/api"
	"github.com/dynamic-calm/mokv/kv"
	"github.com/dynamic-calm/mokv/kv/kvtest"
	"github.com/dynamic-calm/mokv/server"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type cluster struct{}

func (cluster) Servers() ([]kv.Server, error) { return nil, nil }

func setupTestServer(t *testing.T) (api.KVClient, func()) {
	t.Helper()
	return startServer(t, &kvtest.Replicator{Store: kv.NewStore()})
}

func startServer(t *testing.T, r *kvtest.Replicator) (api.KVClient, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Setup: net.Listen(): %v", err)
	}

	svc := kv.NewService(r.Store, r, time.Now)
	srv := server.New(svc, cluster{}, log.Logger)

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
		t.Fatalf("Setup: grpc.NewClient(): %v", err)
	}

	client := api.NewKVClient(clientConn)

	cleanup := func() {
		_ = clientConn.Close()
		srv.Stop()
		_ = listener.Close()
	}

	return client, cleanup
}

func TestAPI(t *testing.T) {
	t.Parallel()

	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := t.Context()

	t.Run("set and get value", func(t *testing.T) {
		expected := []byte("test_value")
		setReq := &api.SetRequest{Key: "test_key", Value: expected, Ttl: durationpb.New(time.Second * 5)}

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

	t.Run("get non-existent key returns not found", func(t *testing.T) {
		_, err := client.Get(ctx, &api.GetRequest{Key: "unknown"})
		if status.Code(err) != codes.NotFound {
			t.Errorf("Get(%q) error = %v, want code %v", "unknown", err, codes.NotFound)
		}
	})

	t.Run("fails on missing ttl", func(t *testing.T) {
		setReq := &api.SetRequest{
			Key:   "test_key",
			Value: []byte("test_value"),
		}
		_, err := client.Set(ctx, setReq)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("Set() without ttl error = %v, want code %v", err, codes.InvalidArgument)
		}
	})

	t.Run("fails on non-positive ttl", func(t *testing.T) {
		for _, ttl := range []time.Duration{0, -time.Second} {
			setReq := &api.SetRequest{
				Key:   "test_key",
				Value: []byte("test_value"),
				Ttl:   durationpb.New(ttl),
			}
			_, err := client.Set(ctx, setReq)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("Set(ttl=%v) error = %v, want code %v", ttl, err, codes.InvalidArgument)
			}
		}
	})
}

func TestWriteOnFollower(t *testing.T) {
	t.Parallel()

	client, cleanup := startServer(t, &kvtest.Replicator{Store: kv.NewStore(), Follower: true})
	defer cleanup()

	_, err := client.Set(t.Context(), &api.SetRequest{Key: "key", Ttl: durationpb.New(time.Minute)})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Set() on follower error = %v, want code %v", err, codes.Unavailable)
	}
	_, err = client.Delete(t.Context(), &api.DeleteRequest{Key: "key"})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("Delete() on follower error = %v, want code %v", err, codes.Unavailable)
	}
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

	for k, v := range testData {
		_, err := client.Set(ctx, &api.SetRequest{Key: k, Value: v, Ttl: durationpb.New(time.Second * 5)})
		if err != nil {
			t.Fatalf("Setup: Set(): %v", err)
		}
	}

	stream, err := client.List(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

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

		_, err := client.Set(ctx, &api.SetRequest{
			Key:   "test_key",
			Value: []byte("test_value"),
			Ttl:   durationpb.New(time.Second * 5),
		})
		if err != nil {
			t.Fatalf("Setup: Set(): %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.List(ctx, &emptypb.Empty{})
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}

		cancel()

		_, err = stream.Recv()
		if status.Code(err) != codes.Canceled {
			t.Errorf("stream.Recv() after cancel: got code %v, want %v", status.Code(err), codes.Canceled)
		}
	})

	t.Run("server shutdown during stream", func(t *testing.T) {
		stream, err := client.List(context.Background(), &emptypb.Empty{})
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			cleanup()
		}()

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

		for i := range goroutines {
			wg.Go(func() {
				setReq := &api.SetRequest{
					Key:   "test_key" + strconv.Itoa(i),
					Value: expected,
					Ttl:   durationpb.New(time.Second * 5),
				}
				setRes, err := client.Set(ctx, setReq)
				if err != nil {
					t.Errorf("Set() error = %v", err)
					return
				}
				if !setRes.Ok {
					t.Errorf("Set() returned Ok = false, want true")
				}
			})
		}

		wg.Wait()
	})

	t.Run("concurrent gets", func(t *testing.T) {
		var wg sync.WaitGroup

		for i := range goroutines {
			wg.Go(func() {
				getReq := &api.GetRequest{Key: "test_key" + strconv.Itoa(i)}
				getRes, err := client.Get(ctx, getReq)
				if err != nil {
					t.Errorf("Get() error = %v", err)
					return
				}
				if !bytes.Equal(getRes.Value, expected) {
					t.Errorf("Get() = %q, want %q", getRes.Value, expected)
				}
			})
		}

		wg.Wait()
	})
}
