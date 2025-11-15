package mokv_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	mokv "github.com/dynamic-calm/mokv/internal"
	"github.com/dynamic-calm/mokv/internal/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRunE2E(t *testing.T) {
	ctx := context.Background()

	// Setup test data directory
	testDir := path.Join(os.TempDir(), fmt.Sprintf("mokv-test-%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

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

	m, err := mokv.New(cfg, os.Getenv)
	if err != nil {
		t.Fatalf("failed to creating new mokv: %s", err)
	}

	go func() {
		m.Listen(ctx)
	}()

	time.Sleep(3 * time.Second)

	// Setup client connection
	rpcAddr := "127.0.0.1:" + strconv.Itoa(cfg.RPCPort)
	conn, err := grpc.NewClient(
		fmt.Sprintf("mokv:///%s", rpcAddr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to connect: %s", err)
	}
	defer conn.Close()

	client := api.NewKVClient(conn)

	// Test Set operation
	value := []byte("test")
	_, err = client.Set(ctx, &api.SetRequest{Key: "test", Value: value})
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
		t.Fatal("we must have at least one server")
	}

	// Test metrics endpoint
	metricsResp, err := http.Get("http://localhost:4000/metrics")
	if err != nil {
		t.Fatalf("failed to get metrics: %s", err)
	}
	defer metricsResp.Body.Close()
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics endpoint returned %d", metricsResp.StatusCode)
	}
}
