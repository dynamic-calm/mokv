// Package mokv wires the parts of a mokv node together and runs them.
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

	"github.com/dynamic-calm/mokv/consensus"
	"github.com/dynamic-calm/mokv/discovery"
	"github.com/dynamic-calm/mokv/kv"
	"github.com/dynamic-calm/mokv/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
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
}

// MOKV is a mokv node: the key-value service served over gRPC, replicated
// with Raft, with cluster membership through Serf.
type MOKV struct {
	svc           *kv.Service
	node          *consensus.Node
	membership    *discovery.Membership
	grpcServer    *grpc.Server
	grpcLn        net.Listener
	cmux          cmux.CMux
	meterProvider *metric.MeterProvider
	metricsServer *http.Server
}

// New creates a node from cfg. gRPC and Raft share the RPC port.
func New(cfg *Config) (_ *MOKV, err error) {
	host, _, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bind address: %w", err)
	}
	rpcAddr := fmt.Sprintf("%s:%d", host, cfg.RPCPort)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.RPCPort))
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %w", err)
	}
	defer func() {
		if err != nil {
			_ = ln.Close()
		}
	}()

	// Raft connections start with a header byte; the rest are gRPC. Raft is
	// matched first so it takes precedence.
	mux := cmux.New(ln)
	raftLn := mux.Match(consensus.Match)
	grpcLn := mux.Match(cmux.Any())

	store := kv.NewStore()
	node, err := consensus.New(store, consensus.Config{
		ID:        cfg.NodeName,
		Addr:      rpcAddr,
		Listener:  raftLn,
		DataDir:   cfg.DataDir,
		Bootstrap: cfg.Bootstrap,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}
	defer func() {
		if err != nil {
			_ = node.Close()
		}
	}()
	svc := kv.NewService(store, node, time.Now)

	exp, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to start prometheus exporter: %w", err)
	}
	meterProvider := metric.NewMeterProvider(metric.WithReader(exp))
	grpcServer := server.New(svc, node, log.Logger, opentelemetry.ServerOption(opentelemetry.Options{
		MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: meterProvider},
	}))

	membership, err := discovery.NewMembership(node, discovery.MembershipConfig{
		NodeName:       cfg.NodeName,
		BindAddr:       cfg.BindAddr,
		RPCAddr:        rpcAddr,
		StartJoinAddrs: cfg.StartJoinAddrs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create membership: %w", err)
	}

	return &MOKV{
		svc:           svc,
		node:          node,
		membership:    membership,
		grpcServer:    grpcServer,
		grpcLn:        grpcLn,
		cmux:          mux,
		meterProvider: meterProvider,
		metricsServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
			Handler: promhttp.Handler(),
		},
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

	// Start gRPC server
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

func (m *MOKV) close(ctx context.Context) error {
	var errs []error

	if err := m.metricsServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("metrics server shutdown error: %w", err))
	}

	if err := m.membership.Leave(); err != nil {
		errs = append(errs, fmt.Errorf("membership leave error: %w", err))
	}

	if err := m.node.Close(); err != nil {
		errs = append(errs, fmt.Errorf("raft node close error: %w", err))
	}

	m.grpcServer.GracefulStop()

	if err := m.meterProvider.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("meter provider shutdown error: %w", err))
	}

	return errors.Join(errs...)
}
