// Package mokv runs a mokv node and provides a client for it.
package mokv

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/dynamic-calm/mokv/consensus"
	"github.com/dynamic-calm/mokv/discovery"
	"github.com/dynamic-calm/mokv/kv"
	"github.com/dynamic-calm/mokv/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats/opentelemetry"
)

// expireInterval is how often the leader looks for expired keys.
const expireInterval = time.Second

// Config holds the configuration parameters for the MOKV server node.
type Config struct {
	DataDir        string
	NodeName       string
	BindAddr       string
	RPCPort        int
	MetricsPort    int
	StartJoinAddrs []string
	Bootstrap      bool
	LogLevel       string
}

// GetEnv defines a function signature for retrieving environment variables.
type GetEnv func(string) string

// MOKV represents the main server instance orchestrating gRPC, Raft, and Discovery services.
type MOKV struct {
	cfg           *Config
	getEnv        GetEnv
	node          *consensus.Node
	svc           *kv.Service
	meterProvider *metric.MeterProvider
	grpcServer    *grpc.Server
	metricsServer *http.Server
	grpcLn        net.Listener
	cmux          cmux.CMux
	membership    *discovery.Membership
}

// New initializes a new MOKV server instance with the provided configuration.
func New(cfg *Config, getEnv GetEnv) (*MOKV, error) {
	host, _, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bind address: %w", err)
	}

	rpcAddr := fmt.Sprintf(":%d", cfg.RPCPort)
	listener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %w", err)
	}

	raftAdvertiseAddr := fmt.Sprintf("%s:%d", host, cfg.RPCPort)

	myCmux := cmux.New(listener)

	// Raft connections start with a header byte; the rest are gRPC. Raft is
	// matched first so it takes precedence.
	raftLn := myCmux.Match(consensus.Match)
	grpcLn := myCmux.Match(cmux.Any())

	store := kv.NewStore()
	node, err := consensus.New(store, consensus.Config{
		ID:        cfg.NodeName,
		Addr:      raftAdvertiseAddr,
		Listener:  raftLn,
		DataDir:   cfg.DataDir,
		Bootstrap: cfg.Bootstrap,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}

	// Initialize Prometheus exporter
	exp, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to start prometheus exporter: %w", err)
	}

	meterProvider := metric.NewMeterProvider(metric.WithReader(exp))

	// Configure gRPC server
	serverOpts := []grpc.ServerOption{
		opentelemetry.ServerOption(
			opentelemetry.Options{
				MetricsOptions: opentelemetry.MetricsOptions{
					MeterProvider: meterProvider,
				},
			},
		),
	}

	svc := kv.NewService(store, node, time.Now)
	grpcServer := server.New(svc, node, log.Logger, serverOpts...)

	// Initialize membership
	membership, err := discovery.NewMembership(node, discovery.MembershipConfig{
		NodeName:       cfg.NodeName,
		BindAddr:       cfg.BindAddr,
		RPCAddr:        raftAdvertiseAddr,
		StartJoinAddrs: cfg.StartJoinAddrs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create membership: %w", err)
	}

	// Setup metrics server
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: promhttp.Handler(),
	}

	return &MOKV{
		node:          node,
		svc:           svc,
		cfg:           cfg,
		getEnv:        getEnv,
		grpcLn:        grpcLn,
		cmux:          myCmux,
		grpcServer:    grpcServer,
		membership:    membership,
		meterProvider: meterProvider,
		metricsServer: metricsServer,
	}, nil
}

// Listen serves gRPC, Raft and metrics until ctx is done or a signal is
// received, then shuts the node down.
func (m *MOKV) Listen(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// Start metrics server
	g.Go(func() error {
		log.Info().Str("addr", m.metricsServer.Addr).Msg("metrics server listening...")
		if err := m.metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("metrics server failed: %w", err)
		}
		return nil
	})

	// Start gRPC server though gprcLn
	g.Go(func() error {
		log.Info().Str("addr", m.grpcLn.Addr().String()).Msg("gRPC server listening...")
		if err := m.grpcServer.Serve(m.grpcLn); err != nil {
			return fmt.Errorf("gRPC server error: %w", err)
		}
		return nil
	})

	// Expire keys while this node is the leader.
	g.Go(func() error {
		m.svc.RunExpiry(ctx, expireInterval)
		return nil
	})

	// Start Multiplexer
	g.Go(func() error {
		log.Info().Msg("multiplexer (RAFT, gRPC) listening...")
		if err := m.cmux.Serve(); err != nil {
			return fmt.Errorf("multiplexer server error: %w", err)
		}
		return nil
	})

	// Handle shutdown
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()

		if err := m.close(shutdownCtx); err != nil {
			log.Error().Err(err).Msg("shutdown error")
		}
	}()

	return g.Wait()
}

func (r *MOKV) close(ctx context.Context) error {
	var errs []error

	if err := r.metricsServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("metrics server shutdown error: %w", err))
	}

	if err := r.membership.Leave(); err != nil {
		errs = append(errs, fmt.Errorf("membership leave error: %w", err))
	}

	if err := r.node.Close(); err != nil {
		errs = append(errs, fmt.Errorf("raft node close error: %w", err))
	}

	r.grpcServer.GracefulStop()

	if err := r.meterProvider.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("meter provider shutdown error: %w", err))
	}

	return errors.Join(errs...)
}
