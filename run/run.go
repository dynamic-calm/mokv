package run

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mateopresacastro/mokv/api"
	"github.com/mateopresacastro/mokv/auth"
	"github.com/mateopresacastro/mokv/config"
	"github.com/mateopresacastro/mokv/discovery"
	"github.com/mateopresacastro/mokv/kv"
	"github.com/mateopresacastro/mokv/kv/store"
	"github.com/mateopresacastro/mokv/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func Run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	// provider, err := setupMetricsServer(ctx)
	// if err != nil {
	// 	return err
	// }
	errChan, err := setupGRPCServer(ctx, nil)
	if err != nil {
		return err
	}
	shutdown, err := setupMemership()
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

func setupGRPCServer(ctx context.Context, provider *metric.MeterProvider) (chan error, error) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
		slog.Warn("PORT not set", "using", port)
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: listener.Addr().String(),
		Server:        true,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to setup TLS: %w", err)
	}

	serverOpts := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(serverTLSConfig)),
		// opentelemetry.ServerOption(
		// 	opentelemetry.Options{
		// 		MetricsOptions: opentelemetry.MetricsOptions{
		// 			MeterProvider: provider,
		// 		},
		// 	},
		// ),
	}

	store := store.New()
	cfg := &kv.Config{
		DataDir: "data",
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %w", err)
	}
	isFirstNode := port == "3000"
	cfg.Raft.LocalID = raft.ServerID(fmt.Sprintf("%s-%s", hostname, port))
	cfg.Raft.Bootstrap = isFirstNode
	raftLn, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, fmt.Errorf("failed to create raft listener: %w", err)
	}

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

	kv, err := kv.NewDistributedKV(store, cfg)
	if err != nil {
		return nil, err
	}
	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	server := server.New(kv, authorizer, serverOpts...)

	srvErrChan := make(chan error, 1)

	go func() {
		defer func() {
			close(srvErrChan)
			server.GracefulStop()
			listener.Close()
		}()
		slog.Info("grpc server listening...", "addr", listener.Addr())
		if err := server.Serve(listener); err != nil {
			slog.Error("server error", "err", err)
			srvErrChan <- err
			cancel()
		}
	}()

	return srvErrChan, nil
}

func setupMemership() (func(), error) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "localhost",
	})
	creds := credentials.NewTLS(peerTLSConfig)
	opts := grpc.WithTransportCredentials(creds)
	cc, err := grpc.NewClient(":"+port, opts)
	if err != nil {
		return nil, err
	}

	client := api.NewKVClient(cc)
	replicator := &kv.Replicator{
		DialOptions: []grpc.DialOption{opts},
		LocalServer: client,
	}

	nodePort := fmt.Sprintf("%d", 8400+func() int {
		p, _ := strconv.Atoi(port)
		return p - 3000 + 1
	}())

	hostname, _ := os.Hostname()
	nodeName := fmt.Sprintf("%s-%s", hostname, port)

	var startJoinAddrs []string
	if port != "3000" {
		startJoinAddrs = []string{"127.0.0.1:8401"}
	}

	membership, err := discovery.New(replicator, discovery.Config{
		NodeName: nodeName,
		BindAddr: fmt.Sprintf("127.0.0.1:%s", nodePort),
		Tags: map[string]string{
			"rpc_addr": ":" + port,
		},
		StartJoinAddrs: startJoinAddrs,
	})

	shutdown := func() {
		err = membership.Leave()
		if err != nil {
			fmt.Println(err)
		}
		err = replicator.Close()
		if err != nil {
			fmt.Println(err)
		}
	}

	return shutdown, nil
}
