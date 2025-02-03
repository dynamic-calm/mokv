package runner

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
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

type Runner struct {
	cfg           Config
	getEnv        GetEnv
	dkv           kv.KV
	meterProvider *metric.MeterProvider
}

func New(cfg Config, getEnv GetEnv) *Runner {
	return &Runner{cfg: cfg, getEnv: getEnv}
}

func (r *Runner) Run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	// This deferred cancel function will signal all goroutines to exit
	// on all return paths.
	defer cancel()

	mErrc, err := r.setupMetricsServer(ctx)
	if err != nil {
		return err
	}
	grpcErrc, err := r.setupGRPCServer(ctx)
	if err != nil {
		return err
	}
	err = r.setupMemership(ctx)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-mErrc:
		return err // TODO: think about what to do when metrics server crashes.
	case err := <-grpcErrc:
		return err
	}
}

func (r *Runner) setupMetricsServer(ctx context.Context) (<-chan error, error) {
	errc := make(chan error, 1)
	exp, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to start prometheus exporter: %w", err)
	}
	provider := metric.NewMeterProvider(metric.WithReader(exp))
	// We need to do this outside of the goroutine so it happends syncronously.
	// The gRPC server depends on this provider.
	r.meterProvider = provider
	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(r.cfg.MetricsPort),
		Handler: promhttp.Handler(),
	}
	go func() {
		defer close(errc)
		defer provider.Shutdown(ctx)

		go func() {
			slog.Info("metrics server listening...", "port", r.cfg.MetricsPort)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errc <- fmt.Errorf("metrics server failed: %w", err)
			}
		}()

		// Wait for context cancelation. Then do shutdown.
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("error shutting down metrics server", "err", err)
		}
	}()
	return errc, nil
}

func (r *Runner) setupGRPCServer(ctx context.Context) (<-chan error, error) {
	port := strconv.Itoa(r.cfg.RPCPort)
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	serverOpts := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(r.cfg.ServerTLSConfig)),
		opentelemetry.ServerOption(
			opentelemetry.Options{
				MetricsOptions: opentelemetry.MetricsOptions{
					MeterProvider: r.meterProvider,
				},
			},
		),
	}

	mux := cmux.New(listener)
	kvCFG := &kv.Config{DataDir: r.cfg.DataDir}
	kvCFG.Raft.BindAddr = r.cfg.BindAddr
	kvCFG.Raft.RPCPort = port
	kvCFG.Raft.LocalID = raft.ServerID(r.cfg.NodeName)
	kvCFG.Raft.Bootstrap = r.cfg.Bootstrap

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
		r.cfg.ServerTLSConfig,
		r.cfg.PeerTLSConfig,
	)

	store := store.New()
	dkv, err := kv.NewDistributedKV(store, kvCFG)
	if err != nil {
		return nil, err
	}

	r.dkv = dkv

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	server := server.New(dkv, authorizer, serverOpts...)
	grpcLn := mux.Match(cmux.Any())

	errc := make(chan error, 1)

	go func() {
		defer close(errc)
		go func() {
			slog.Info("gRPC server listening...", "addr", listener.Addr())
			if err := server.Serve(grpcLn); err != nil {
				errc <- fmt.Errorf("gRPC server error: %w", err)
			}
		}()

		go func() {
			slog.Info("multiplexer listening...")
			if err := mux.Serve(); err != nil {
				errc <- fmt.Errorf("multiplexer server error: %w", err)
			}
		}()

		<-ctx.Done()
		// The context is done when user stops the process or there is an error
		// on the errc channel. The context gets cancelled on the defer
		// of the Run function.
		server.GracefulStop()
		listener.Close()
		grpcLn.Close()
		mux.Close()
	}()

	return errc, nil
}

func (r *Runner) setupMemership(ctx context.Context) error {
	distributedKV, ok := r.dkv.(*kv.DistributedKV)
	if !ok {
		return fmt.Errorf("failed to convert kv to *kv.DistributedKV")
	}

	membership, err := discovery.New(distributedKV, discovery.Config{
		NodeName: r.cfg.NodeName,
		BindAddr: r.cfg.BindAddr,
		Tags: map[string]string{
			"rpc_addr": r.cfg.BindAddr,
		},
		StartJoinAddrs: r.cfg.StartJoinAddrs,
	})
	if err != nil {
		return fmt.Errorf("failed to create membership: %w", err)
	}

	go func() {
		<-ctx.Done()
		if err := membership.Leave(); err != nil {
			slog.Error("failed to leave", "error", err)
		}
	}()

	return nil
}
