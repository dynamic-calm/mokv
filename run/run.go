package run

import (
	"bytes"
	"context"
	"crypto/tls"
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

type Config struct {
	DataDir         string
	NodeName        string
	BindAddr        string
	RPCPort         int
	MetricsPort     int
	StartJoinAddrs  []string
	Bootstrap       bool
	ACLModelFile    string
	ACLPolicyFile   string
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
}

type GetEnv func(string) string

func Run(ctx context.Context, getenv GetEnv, cfg Config) error {

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	provider, err := setupMetricsServer(ctx, getenv)
	if err != nil {
		return err
	}

	kv, errChan, err := setupGRPCServer(ctx, getenv, provider, cfg)
	if err != nil {
		return err
	}
	shutdown, err := setupMemership(kv, getenv, cfg)
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

func setupMetricsServer(ctx context.Context, getenv GetEnv) (*metric.MeterProvider, error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to start prometheus exporter: %w", err)
	}

	metricsPort := getenv("METRICS_PORT")
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

func setupGRPCServer(ctx context.Context, getenv GetEnv, provider *metric.MeterProvider, cfg Config) (kv.KV, chan error, error) {
	_, cancel := context.WithCancel(ctx)

	defer cancel()
	port := strconv.Itoa(cfg.RPCPort)
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}

	mux := cmux.New(listener)

	serverOpts := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(cfg.ServerTLSConfig)),
		opentelemetry.ServerOption(
			opentelemetry.Options{
				MetricsOptions: opentelemetry.MetricsOptions{
					MeterProvider: provider,
				},
			},
		),
	}

	store := store.New()
	dataDir := filepath.Join("data", fmt.Sprintf("node-%s", port))
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get absolute path: %v", err)
	}
	dataDir = absDataDir

	kvCFG := &kv.Config{
		DataDir: dataDir,
	}
	kvCFG.Raft.BindAddr = cfg.BindAddr
	kvCFG.Raft.RPCPort = port
	kvCFG.Raft.LocalID = raft.ServerID(cfg.NodeName)
	kvCFG.Raft.Bootstrap = cfg.Bootstrap

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

	kvCFG.Raft.StreamLayer = *kv.NewStreamLayer(
		raftLn,
		cfg.ServerTLSConfig,
		cfg.PeerTLSConfig,
	)

	dkv, err := kv.NewDistributedKV(store, kvCFG)
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
		}()
		slog.Info("gRPC server listening...", "addr", listener.Addr())
		if err := server.Serve(grpcLn); err != nil {
			slog.Error("server error", "err", err)
			srvErrChan <- err
			cancel()
		}
	}()

	go func() {
		defer func() {
			close(srvErrChan)
			mux.Close()
		}()
		slog.Info("multiplexer listening...")
		if err := mux.Serve(); err != nil {
			slog.Error("mux error", "err", err)
			srvErrChan <- err
			cancel()
		}
	}()

	return dkv, srvErrChan, nil
}

func setupMemership(dkv kv.KV, getenv GetEnv, cfg Config) (func(), error) {
	port := getenv("PORT")
	if port == "" {
		port = "3000"
	}

	distributedKV, ok := dkv.(*kv.DistributedKV)
	if !ok {
		return nil, fmt.Errorf("failed to convert kv to *kv.DistributedKV")
	}

	membership, err := discovery.New(distributedKV, discovery.Config{
		NodeName: cfg.NodeName,
		BindAddr: cfg.BindAddr,
		Tags: map[string]string{
			"rpc_addr": cfg.BindAddr,
		},
		StartJoinAddrs: cfg.StartJoinAddrs,
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
