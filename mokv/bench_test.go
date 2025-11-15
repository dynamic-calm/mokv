package mokv_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/api"
	_ "github.com/dynamic-calm/mokv/discovery"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupClient(b *testing.B) api.KVClient {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Helper()
	conn, err := grpc.NewClient(
		"mokv://127.0.0.1:8400",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{
			"loadBalancingConfig": [{"mokv": {}}]
		}`),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(10*1024*1024),
			grpc.MaxCallSendMsgSize(10*1024*1024),
		),
	)
	if err != nil {
		b.Fatalf("failed to connect: %v", err)
	}

	b.Cleanup(func() {
		conn.Close()
	})

	client := api.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = client.Set(ctx, &api.SetRequest{
		Key:   "bench-key",
		Value: []byte("bench-value"),
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to pre-populate bench-key: %v", err))
	}

	time.Sleep(500 * time.Millisecond)
	return client
}

// Benchmark single-threaded writes (measures raw latency)
func BenchmarkSet(b *testing.B) {
	client := setupClient(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		_, err := client.Set(ctx, &api.SetRequest{
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte("benchmark-value"),
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark parallel writes (measures throughput)
func BenchmarkSetParallel(b *testing.B) {
	client := setupClient(b)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, err := client.Set(ctx, &api.SetRequest{
				Key:   fmt.Sprintf("key-%d", i),
				Value: []byte("benchmark-value"),
			})
			if err != nil {
				b.Error(err)
			}
			i++
		}
	})
}

// Benchmark single-threaded reads (measures raw latency)
func BenchmarkGet(b *testing.B) {
	client := setupClient(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		_, err := client.Get(ctx, &api.GetRequest{Key: "bench-key"})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark parallel reads (measures throughput)
func BenchmarkGetParallel(b *testing.B) {
	client := setupClient(b)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Get(ctx, &api.GetRequest{Key: "bench-key"})
			if err != nil {
				b.Error(err)
			}
		}
	})
}
