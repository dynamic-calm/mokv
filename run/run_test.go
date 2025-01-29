package run_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/mateopresacastro/mokv/api"
	"github.com/mateopresacastro/mokv/config"
	"github.com/mateopresacastro/mokv/run"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestRunE2E(t *testing.T) {
	getenv := func(key string) string {
		switch key {
		case "PORT":
			return "3000"
		case "METRICS_PORT":
			return "4000"
		default:
			return ""
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		errChan <- run.Run(ctx, getenv)
	}()

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "localhost",
	})
	if err != nil {
		t.Fatalf("client TLS setup failed: %s", err)
	}

	clientCreds := credentials.NewTLS(clientTLSConfig)
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", 3000),
		grpc.WithTransportCredentials(clientCreds),
	)
	if err != nil {
		t.Fatalf("failed to connect: %s", err)
	}
	defer conn.Close()

	client := api.NewKVClient(conn)

	value := []byte("test")
	_, err = client.Set(ctx, &api.SetRequest{Key: "test", Value: value})
	if err != nil {
		t.Fatalf("failed to set: %s", err)
	}

	resp, err := client.Get(ctx, &api.GetRequest{Key: "test"})
	if err != nil {
		t.Fatalf("failed to get: %s", err)
	}

	if !bytes.Equal(resp.Value, value) {
		t.Fatalf("got wrong value back")
	}

	metricsResp, err := http.Get("http://localhost:4000/metrics")
	if err != nil {
		t.Fatalf("failed to get metrics: %s", err)
	}
	defer metricsResp.Body.Close()

	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics endpoint returned %d", metricsResp.StatusCode)
	}

	cancel()

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("server error: %s", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server didn't shut down")
	}
}

// func TestReplication(t *testing.T) {
// 	// Start first node (bootstrap node)
// 	getenv := func(key string) string {
// 		switch key {
// 		case "PORT":
// 			return "3000"
// 		case "METRICS_PORT":
// 			return "4000"
// 		default:
// 			return ""
// 		}
// 	}

// 	ctx1, cancel1 := context.WithCancel(context.Background())
// 	defer cancel1()
// 	go func() {
// 		if err := run.Run(ctx1, getenv); err != nil {
// 			t.Errorf("node 1 failed: %v", err)
// 		}
// 	}()

// 	// Start second node
// 	getenv = func(key string) string {
// 		switch key {
// 		case "PORT":
// 			return "3007"
// 		case "METRICS_PORT":
// 			return "4001"
// 		default:
// 			return ""
// 		}
// 	}

// 	ctx2, cancel2 := context.WithCancel(context.Background())
// 	defer cancel2()
// 	go func() {
// 		if err := run.Run(ctx2, getenv); err != nil {
// 			t.Errorf("node 2 failed: %v", err)
// 		}
// 	}()

// 	// Wait for nodes to start and form cluster
// 	time.Sleep(4 * time.Second)

// 	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
// 		CertFile:      config.RootClientCertFile,
// 		KeyFile:       config.RootClientKeyFile,
// 		CAFile:        config.CAFile,
// 		ServerAddress: "localhost",
// 	})
// 	if err != nil {
// 		t.Fatalf("client TLS setup failed: %s", err)
// 	}

// 	// Create client to connect to first node
// 	clientCreds := credentials.NewTLS(clientTLSConfig)
// 	conn, err := grpc.NewClient(
// 		fmt.Sprintf("localhost:%d", 3000),
// 		grpc.WithTransportCredentials(clientCreds),
// 	)
// 	if err != nil {
// 		t.Fatalf("failed to connect for node 1: %s", err)
// 	}
// 	defer conn.Close()
// 	client1 := api.NewKVClient(conn)

// 	// Create client to connect to second node
// 	conn2, err := grpc.NewClient(
// 		fmt.Sprintf("localhost:%d", 3000),
// 		grpc.WithTransportCredentials(clientCreds),
// 	)
// 	if err != nil {
// 		t.Fatalf("failed to connect for node 2: %s", err)
// 	}
// 	defer conn2.Close()
// 	client2 := api.NewKVClient(conn2)

// 	// Write data to first node
// 	ctx := context.Background()
// 	key := "test-key"
// 	value := []byte("test-value")

// 	_, err = client1.Set(ctx, &api.SetRequest{Key: key, Value: value})
// 	if err != nil {
// 		t.Fatalf("failed to set value on node 1: %v", err)
// 	}

// 	// Wait for replication
// 	time.Sleep(1 * time.Second)

// 	// Read from second node to verify replication
// 	resp, err := client2.Get(ctx, &api.GetRequest{Key: key})
// 	if err != nil {
// 		t.Fatalf("failed to get value from node 2: %v", err)
// 	}

// 	if string(resp.Value) != string(value) {
// 		t.Errorf("replication failed: expected %s, got %s", value, resp.Value)
// 	}
// }
