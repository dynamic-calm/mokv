package run_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/mateopresacastro/mokv/api"
	"github.com/mateopresacastro/mokv/config"
	"github.com/mateopresacastro/mokv/run"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestRunE2E(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to create listener: %s", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	os.Setenv("PORT", strconv.Itoa(port))
	os.Setenv("METRICS_PORT", "4000")

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		errChan <- run.Run(ctx)
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
		fmt.Sprintf("localhost:%d", port),
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
