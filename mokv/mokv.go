package mokv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/dynamic-calm/mokv/discovery"
	"github.com/dynamic-calm/mokv/kv"
	"github.com/dynamic-calm/mokv/server"
	"github.com/dynamic-calm/mokv/store"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats/opentelemetry"
)

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

// Storer defines the interface for a the key-value storage.
type Storer interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	List() <-chan []byte
	Close() error
}

// GetEnv defines a function signature for retrieving environment variables.
type GetEnv func(string) string

// MOKV represents the main server instance orchestrating gRPC, Raft, and Discovery services.
type MOKV struct {
	cfg           *Config
	getEnv        GetEnv
	kv            Storer
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

	// Configure KV store
	rpcPort := strconv.Itoa(cfg.RPCPort)
	kvCFG := &kv.KVConfig{DataDir: cfg.DataDir}
	kvCFG.Raft.BindAddr = raftAdvertiseAddr
	kvCFG.Raft.RPCPort = rpcPort
	kvCFG.Raft.LocalID = raft.ServerID(cfg.NodeName)
	kvCFG.Raft.Bootstrap = cfg.Bootstrap

	// Configure Raft listener.
	// If the first byte of the incoming connection is our RaftRPC constant,
	// we know the call was initiated by Raft, and not a regular operation: get, set, etc.
	raftLn := myCmux.Match(func(r io.Reader) bool {
		b := make([]byte, 1)
		if _, err := r.Read(b); err != nil {
			return false
		}

		return bytes.Equal(b, []byte{byte(kv.RaftRPC)})
	})

	// The rest of connections are regular gRPC calls. Since this is set
	// below, the Raft listener has precedence.
	grpcLn := myCmux.Match(cmux.Any())

	// Setup Raft stream layer
	kvCFG.Raft.StreamLayer = kv.NewStreamLayer(raftLn)

	// Initialize store and KV
	store := store.New()
	kv, err := kv.New(store, kvCFG)
	if err != nil {
		return nil, fmt.Errorf("failed to create KV store: %w", err)
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

	grpcServer := server.New(kv, log.Logger, serverOpts...)

	// Initialize membership
	membership, err := discovery.NewMembership(kv, discovery.MembershipConfig{
		NodeName: cfg.NodeName,
		BindAddr: cfg.BindAddr,
		Tags: map[string]string{
			"rpc_addr": raftAdvertiseAddr,
		},
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
		kv:            kv,
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

	if err := r.kv.Close(); err != nil {
		errs = append(errs, fmt.Errorf("KV close error: %w", err))
	}

	r.grpcServer.GracefulStop()

	if err := r.meterProvider.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("meter provider shutdown error: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}

	return nil
}
