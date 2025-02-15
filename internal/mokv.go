package mokv

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
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/dynamic-calm/mokv/internal/auth"
	"github.com/dynamic-calm/mokv/internal/config"
	"github.com/dynamic-calm/mokv/internal/discovery"
	"github.com/dynamic-calm/mokv/internal/kv"
	"github.com/dynamic-calm/mokv/internal/server"
	"github.com/dynamic-calm/mokv/internal/store"
	"github.com/hashicorp/raft"
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

type MOKV struct {
	cfg            *Config
	getEnv         GetEnv
	kv             kv.KVI
	meterProvider  *metric.MeterProvider
	grpcServer     *grpc.Server
	mertricsServer *http.Server
	grpcLn         net.Listener
	cmux           cmux.CMux
	membership     *discovery.Membership
}

func New(cfg *Config, getEnv GetEnv) (*MOKV, error) {
	exp, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to start prometheus exporter: %w", err)
	}

	provider := metric.NewMeterProvider(metric.WithReader(exp))
	port := strconv.Itoa(cfg.RPCPort)
	kvCFG := &kv.KVConfig{DataDir: cfg.DataDir}
	kvCFG.Raft.BindAddr = cfg.BindAddr
	kvCFG.Raft.RPCPort = port
	kvCFG.Raft.LocalID = raft.ServerID(cfg.NodeName)
	kvCFG.Raft.Bootstrap = cfg.Bootstrap

	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	myCmux := cmux.New(listener)
	grpcLn := myCmux.Match(cmux.Any())

	raftLn := myCmux.Match(func(reader io.Reader) bool {
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

	store := store.New()
	kv, err := kv.New(store, kvCFG)
	if err != nil {
		return nil, err
	}

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

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	grpcServer := server.New(kv, authorizer, serverOpts...)

	rpcAddr := fmt.Sprintf("127.0.0.1:%d", cfg.RPCPort)
	membership, err := discovery.NewMembership(kv, discovery.MembershipConfig{
		NodeName: cfg.NodeName,
		BindAddr: cfg.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: cfg.StartJoinAddrs,
	})

	metricServer := &http.Server{
		Addr:    ":" + strconv.Itoa(cfg.MetricsPort),
		Handler: promhttp.Handler(),
	}

	return &MOKV{
		cfg:            cfg,
		getEnv:         getEnv,
		meterProvider:  provider,
		kv:             kv,
		grpcServer:     grpcServer,
		mertricsServer: metricServer,
		grpcLn:         grpcLn,
		membership:     membership,
		cmux:           myCmux,
	}, nil
}

func (r *MOKV) Run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start metrics server
	go func() {
		slog.Info("metrics server listening...", "port", r.cfg.MetricsPort)
		if err := r.mertricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Errorf("metrics server failed: %w", err)
		}
	}()

	// Start gRPC server
	go func() {
		slog.Info("gRPC server listening...", "addr", r.grpcLn.Addr())
		if err := r.grpcServer.Serve(r.grpcLn); err != nil {
			fmt.Errorf("gRPC server error: %w", err)
		}
	}()

	// Start Multiplexer
	go func() {
		slog.Info("multiplexer listening...")
		if err := r.cmux.Serve(); err != nil {
			fmt.Errorf("multiplexer server error: %w", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		shutdownCtx := context.Background()
		shutdownCtx, cancel := context.WithTimeout(shutdownCtx, 10*time.Second)
		defer cancel()
		if err := r.mertricsServer.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "error shutting down http server: %s\n", err)
		}

		if err := r.membership.Leave(); err != nil {
			slog.Error("failed to leave", "error", err)
		}

		r.grpcServer.GracefulStop()
	}()

	wg.Wait()

	return nil
}
