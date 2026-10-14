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

const (
	expireInterval  = time.Second
	shutdownTimeout = 25 * time.Second
)

// Config configures a node.
type Config struct {
	// DataDir is where the Raft log and snapshots are stored.
	DataDir string
	// NodeName uniquely identifies the node in the cluster.
	NodeName string
	// BindAddr is the address Serf listens on. Its host is also the host
	// other nodes reach the RPC port at.
	BindAddr string
	// RPCPort is the port gRPC and Raft are served on.
	RPCPort int
	// MetricsPort is the port Prometheus metrics are served on.
	MetricsPort int
	// StartJoinAddrs are the Serf addresses of existing members to join.
	StartJoinAddrs []string
	// Bootstrap makes the node start a new cluster if it has no state.
	Bootstrap bool
}

// Node is a mokv node: the key-value service served over gRPC, replicated
// with Raft, with cluster membership through Serf.
type Node struct {
	svc           *kv.Service
	node          *consensus.Node
	membership    *discovery.Membership
	grpcServer    *grpc.Server
	grpcLn        net.Listener
	cmux          cmux.CMux
	meterProvider *metric.MeterProvider
	metricsServer *http.Server
}

// New creates a node from cfg and joins the cluster. Serving starts with
// Listen.
func New(cfg *Config) (_ *Node, err error) {
	host, _, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("parsing bind address: %w", err)
	}
	rpcAddr := fmt.Sprintf("%s:%d", host, cfg.RPCPort)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.RPCPort))
	if err != nil {
		return nil, fmt.Errorf("listening on RPC port: %w", err)
	}
	defer func() {
		if err != nil {
			_ = ln.Close()
		}
	}()

	mux := cmux.New(ln)
	raftLn := sharedListener{mux.Match(consensus.Match)}
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
		return nil, fmt.Errorf("starting raft: %w", err)
	}
	defer func() {
		if err != nil {
			_ = node.Close()
		}
	}()
	svc := kv.NewService(store, node, time.Now)

	exp, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("creating prometheus exporter: %w", err)
	}
	meterProvider := metric.NewMeterProvider(metric.WithReader(exp))
	grpcServer := server.New(svc, node, log.Logger, opentelemetry.ServerOption(opentelemetry.Options{
		MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: meterProvider},
	}))

	membership, err := discovery.New(node, discovery.Config{
		NodeName:       cfg.NodeName,
		BindAddr:       cfg.BindAddr,
		RPCAddr:        rpcAddr,
		StartJoinAddrs: cfg.StartJoinAddrs,
	})
	if err != nil {
		return nil, fmt.Errorf("joining cluster: %w", err)
	}

	return &Node{
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
func (m *Node) Listen(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Info().Str("addr", m.metricsServer.Addr).Msg("metrics server listening...")
		if err := m.metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serving metrics: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		log.Info().Str("addr", m.grpcLn.Addr().String()).Msg("gRPC server listening...")
		if err := m.grpcServer.Serve(m.grpcLn); err != nil {
			return fmt.Errorf("serving gRPC: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		m.svc.RunExpiry(ctx, expireInterval)
		return nil
	})

	g.Go(func() error {
		log.Info().Msg("multiplexer (RAFT, gRPC) listening...")
		if err := m.cmux.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("serving multiplexer: %w", err)
		}
		return nil
	})

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := m.close(shutdownCtx); err != nil {
			log.Error().Err(err).Msg("shutdown error")
		}
	}()

	return g.Wait()
}

func (m *Node) close(ctx context.Context) error {
	var errs []error

	if err := m.metricsServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("shutting down metrics server: %w", err))
	}

	if err := m.membership.Leave(); err != nil {
		errs = append(errs, fmt.Errorf("leaving cluster: %w", err))
	}

	if err := m.node.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing raft: %w", err))
	}

	m.grpcServer.GracefulStop()

	if err := m.meterProvider.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("shutting down meter provider: %w", err))
	}

	return errors.Join(errs...)
}

type sharedListener struct {
	net.Listener
}

func (sharedListener) Close() error {
	return nil
}
