package runner_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"testing"
	"time"

	"github.com/mateopresacastro/mokv/api"
	"github.com/mateopresacastro/mokv/config"
	"github.com/mateopresacastro/mokv/lb"
	"github.com/mateopresacastro/mokv/runner"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestRunE2E(t *testing.T) {
	// Create context with timeout
	ctx := context.Background()

	// Setup test data directory
	testDir := path.Join(os.TempDir(), fmt.Sprintf("mokv-test-%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}

	// Setup Server TLS
	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: "127.0.0.1",
			Server:        true,
		},
	)
	if err != nil {
		t.Fatalf("server TLS setup failed: %s", err)
	}

	// Setup Peer TLS
	peerTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: "127.0.0.1",
		},
	)
	if err != nil {
		t.Fatalf("peer TLS setup failed: %s", err)
	}

	// Create runner config
	cfg := &runner.Config{
		DataDir:         testDir,
		NodeName:        hostname,
		BindAddr:        "127.0.0.1:8401",
		RPCPort:         8400,
		Bootstrap:       true,
		ACLModelFile:    config.ACLModelFile,
		ACLPolicyFile:   config.ACLPolicyFile,
		ServerTLSConfig: serverTLSConfig,
		MetricsPort:     4000,
		PeerTLSConfig:   peerTLSConfig,
	}

	// Create and start runner
	r := runner.New(cfg, os.Getenv)
	go func() {
		r.Run(ctx)
	}()

	time.Sleep(3 * time.Second)
	// Setup client TLS
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
	})
	if err != nil {
		t.Fatalf("client TLS setup failed: %s", err)
	}

	// Setup client connection
	clientCreds := credentials.NewTLS(clientTLSConfig)
	rpcAddr := "127.0.0.1"
	conn, err := grpc.NewClient(
		fmt.Sprintf("%s:///%s", lb.Name, rpcAddr),
		grpc.WithTransportCredentials(clientCreds),
	)
	if err != nil {
		t.Fatalf("failed to connect: %s", err)
	}
	defer conn.Close()

	// Create client and run tests
	slog.Info("creating client")
	_ = api.NewKVClient(conn)

	// Test Set operation
	// value := []byte("test")
	// _, err = client.Set(ctx, &api.SetRequest{Key: "test", Value: value})
	// if err != nil {
	// 	t.Fatalf("failed to set: %s", err)
	// }

	// // Test Get operation
	// resp, err := client.Get(ctx, &api.GetRequest{Key: "test"})
	// if err != nil {
	// 	t.Fatalf("failed to get: %s", err)
	// }
	// if !bytes.Equal(resp.Value, value) {
	// 	t.Fatalf("got wrong value back: got %v, want %v", resp.Value, value)
	// }

	// // Test GetServers operation
	// getServersRes, err := client.GetServers(ctx, &emptypb.Empty{})
	// if err != nil {
	// 	t.Fatalf("failed to get servers: %s", err)
	// }
	// if len(getServersRes.Servers) < 1 {
	// 	t.Fatal("we must have at least one server")
	// }

	// // Test metrics endpoint
	// metricsResp, err := http.Get("http://localhost:4000/metrics")
	// if err != nil {
	// 	t.Fatalf("failed to get metrics: %s", err)
	// }
	// defer metricsResp.Body.Close()
	// if metricsResp.StatusCode != http.StatusOK {
	// 	t.Fatalf("metrics endpoint returned %d", metricsResp.StatusCode)
	// }

	// // Initiate graceful shutdown
	// cancel()

	// // Wait for runner to shutdown gracefully
	// select {
	// case err := <-runnerErr:
	// 	if err != nil && err != context.Canceled {
	// 		t.Errorf("runner failed to shutdown gracefully: %v", err)
	// 	}
	// case <-time.After(5 * time.Second):
	// 	t.Error("runner failed to shutdown within timeout")
	// }
}
