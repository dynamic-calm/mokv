package run

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mateopresacastro/mokv/auth"
	"github.com/mateopresacastro/mokv/config"
	"github.com/mateopresacastro/mokv/discovery"
	"github.com/mateopresacastro/mokv/kv"
	"github.com/mateopresacastro/mokv/kv/store"
	"github.com/mateopresacastro/mokv/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/stats/opentelemetry"
)

type Agent struct {
	kv *kv.DistributedKV
}

func Run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	provider, err := setupMetricsServer(ctx)
	if err != nil {
		return err
	}

	kv, errChan, err := setupGRPCServer(ctx, provider)
	if err != nil {
		return err
	}
	shutdown, err := setupMemership(kv)
	if err != nil {
		return err
	}
	defer shutdown()

	<-ctx.Done()

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

func setupMetricsServer(ctx context.Context) (*metric.MeterProvider, error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to start prometheus exporter: %w", err)
	}

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "4000"
		slog.Warn("METRICS_PORT not set", "using", metricsPort)
	}

	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	defer provider.Shutdown(ctx)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			slog.Info("metrics server listening...", "port", metricsPort)
			if err := http.ListenAndServe(":"+metricsPort, promhttp.Handler()); err != nil && err != http.ErrServerClosed {
				slog.Error("metrics server failed", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					continue
				}
			}
			return
		}
	}()

	return provider, nil
}

func setupGRPCServer(ctx context.Context, provider *metric.MeterProvider) (kv.KV, chan error, error) {
	_, cancel := context.WithCancel(ctx)

	defer cancel()
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
		slog.Warn("PORT not set", "using", port)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}

	mux := cmux.New(listener)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: listener.Addr().String(),
		Server:        true,
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to setup TLS: %w", err)
	}

	serverOpts := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(serverTLSConfig)),
		opentelemetry.ServerOption(
			opentelemetry.Options{
				MetricsOptions: opentelemetry.MetricsOptions{
					MeterProvider: provider,
				},
			},
		),
	}

	store := store.New()
	hostname, err := os.Hostname()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get hostname: %w", err)
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse port: %w", err)
	}
	raftPort := strconv.Itoa(parsedPort + 1000)
	dataDir := filepath.Join("data", fmt.Sprintf("node-%s", port))

	cfg := &kv.Config{
		DataDir: dataDir,
	}
	cfg.Raft.BindAddr = fmt.Sprintf("127.0.0.1:%s", raftPort)
	cfg.Raft.RPCPort = port
	cfg.Raft.LocalID = raft.ServerID(fmt.Sprintf("%s-%s", hostname, port))
	cfg.Raft.Bootstrap = port == "3000"

	// Create a Raft listener for incoming connections that
	// have RaftRPC as first byte. This way we can multiplex
	// on the same port regular connections and raft ones.
	raftLn := mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(kv.RaftRPC)}) == 0
	})

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "localhost",
	})

	cfg.Raft.StreamLayer = *kv.NewStreamLayer(
		raftLn,
		serverTLSConfig,
		peerTLSConfig,
	)

	dkv, err := kv.NewDistributedKV(store, cfg)
	if err != nil {
		return nil, nil, err
	}

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	server := server.New(dkv, authorizer, serverOpts...)
	grpcLn := mux.Match(cmux.Any())
	srvErrChan := make(chan error, 1)
	go func() {
		defer func() {
			close(srvErrChan)
			server.GracefulStop()
			listener.Close()
			grpcLn.Close()
			mux.Close()
		}()
		slog.Info("multiplexer listening...")
		if err := mux.Serve(); err != nil {
			slog.Error("mux error", "err", err)
			srvErrChan <- err
			cancel()
		}
	}()

	go func() {
		defer func() {
			close(srvErrChan)
			server.GracefulStop()
			listener.Close()
			grpcLn.Close()
			mux.Close()
		}()
		slog.Info("gRPC server listening...", "addr", listener.Addr())
		if err := server.Serve(grpcLn); err != nil {
			slog.Error("server error", "err", err)
			srvErrChan <- err
			cancel()
		}
	}()

	return dkv, srvErrChan, nil
}

func setupMemership(dkv kv.KV) (func(), error) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	nodePort := fmt.Sprintf("%d", 8400+func() int {
		p, _ := strconv.Atoi(port)
		return p - 3000
	}())

	hostname, _ := os.Hostname()
	nodeName := fmt.Sprintf("%s-%s", hostname, port)

	var startJoinAddrs []string
	if port != "3000" {
		startJoinAddrs = []string{"127.0.0.1:8400"}
	}

	distributedKV, ok := dkv.(*kv.DistributedKV)
	if !ok {
		return nil, fmt.Errorf("failed to convert kv to *kv.DistributedKV")
	}

	membership, err := discovery.New(distributedKV, discovery.Config{
		NodeName: nodeName,
		BindAddr: fmt.Sprintf("127.0.0.1:%s", nodePort),
		Tags: map[string]string{
			"rpc_addr": fmt.Sprintf("127.0.0.1:%s", port),
		},
		StartJoinAddrs: startJoinAddrs,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create membership: %w", err)
	}

	shutdown := func() {
		if membership != nil {
			if err := membership.Leave(); err != nil {
				slog.Error("failed to leave", "error", err)
			}
		}
	}

	return shutdown, nil
}
