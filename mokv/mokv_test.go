package mokv_test

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/api"
	"github.com/dynamic-calm/mokv/client"
	"github.com/dynamic-calm/mokv/mokv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRunE2E(t *testing.T) {
	ctx := t.Context()

	// Setup test data directory
	testDir := t.TempDir()

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}

	cfg := &mokv.Config{
		DataDir:     testDir,
		NodeName:    hostname,
		BindAddr:    "127.0.0.1:8401",
		RPCPort:     8400,
		MetricsPort: 4000,
		Bootstrap:   true,
	}

	m, err := mokv.New(cfg)
	if err != nil {
		t.Fatalf("failed to create new mokv: %s", err)
	}

	go func() {
		_ = m.Listen(ctx)
	}()

	// Setup client connection
	rpcAddr := "127.0.0.1:" + strconv.Itoa(cfg.RPCPort)
	conn, err := grpc.NewClient(
		fmt.Sprintf("%s://%s", client.Scheme, rpcAddr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to connect: %s", err)
	}
	defer func() { _ = conn.Close() }()

	client := api.NewKVClient(conn)

	// Test Set operation
	value := []byte("test")
	_, err = client.Set(ctx, &api.SetRequest{Key: "test", Value: value, Ttl: durationpb.New(time.Second * 5)})
	if err != nil {
		t.Fatalf("failed to set: %s", err)
	}

	// Test Get operation
	resp, err := client.Get(ctx, &api.GetRequest{Key: "test"})
	if err != nil {
		t.Fatalf("failed to get: %s", err)
	}
	if !bytes.Equal(resp.Value, value) {
		t.Fatalf("got wrong value back: got %v, want %v", resp.Value, value)
	}

	// Test GetServers operation
	getServersRes, err := client.GetServers(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("failed to get servers: %s", err)
	}
	if len(getServersRes.Servers) < 1 {
		t.Fatalf("len(GetServers().Servers) = %d, want >= 1", len(getServersRes.Servers))
	}

	// Test metrics endpoint
	metricsResp, err := http.Get("http://localhost:4000/metrics")
	if err != nil {
		t.Fatalf("failed to get metrics: %s", err)
	}
	defer func() { _ = metricsResp.Body.Close() }()
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", metricsResp.StatusCode, http.StatusOK)
	}
}
