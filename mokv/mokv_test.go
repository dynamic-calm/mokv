package mokv_test

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/client"
	"github.com/dynamic-calm/mokv/mokv"
)

func TestRunE2E(t *testing.T) {
	ctx := t.Context()

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Setup: os.Hostname(): %v", err)
	}
	cfg := &mokv.Config{
		DataDir:     t.TempDir(),
		NodeName:    hostname,
		BindAddr:    "127.0.0.1:8401",
		RPCPort:     8400,
		MetricsPort: 4000,
		Bootstrap:   true,
	}
	m, err := mokv.New(cfg)
	if err != nil {
		t.Fatalf("mokv.New() = %v, want nil", err)
	}
	go func() { _ = m.Listen(ctx) }()

	c, err := client.New(fmt.Sprintf("%s://127.0.0.1:%d", client.Scheme, cfg.RPCPort))
	if err != nil {
		t.Fatalf("client.New() = %v, want nil", err)
	}
	defer func() { _ = c.Close() }()

	value := []byte("test")
	if err := c.Set(ctx, "test", value, 5*time.Second); err != nil {
		t.Fatalf("Set() = %v, want nil", err)
	}
	got, err := c.Get(ctx, "test")
	if err != nil {
		t.Fatalf("Get(%q) error = %v", "test", err)
	}
	if !bytes.Equal(got, value) {
		t.Errorf("Get(%q) = %q, want %q", "test", got, value)
	}

	servers, err := c.Servers(ctx)
	if err != nil {
		t.Fatalf("Servers() error = %v", err)
	}
	if len(servers) != 1 {
		t.Errorf("len(Servers()) = %d, want 1", len(servers))
	}

	res, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", cfg.MetricsPort))
	if err != nil {
		t.Fatalf("GET /metrics error = %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		t.Errorf("GET /metrics status = %d, want %d", res.StatusCode, http.StatusOK)
	}
}
