package runner_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
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
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRunE2E(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

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

	cfg := &runner.Config{
		DataDir:         path.Join(os.TempDir(), "mokv-test"),
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
	r := runner.New(cfg, os.Getenv)

	go func() {
		r.Run(ctx)
	}()

	time.Sleep(3 * time.Second)

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
		fmt.Sprintf("%s:///%s", lb.Name, "localhost:3000"),
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

	getServersRes, err := client.GetServers(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("failed to get servers: %s", err)
	}

	if len(getServersRes.Servers) < 1 {
		t.Fatal("we must have at least one server")
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
}
