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
	"github.com/mateopresacastro/mokv/lb"
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

	getServersRes, err := client.GetServers(ctx, &api.Empty{})
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

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("server error: %s", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server didn't shut down")
	}
}
