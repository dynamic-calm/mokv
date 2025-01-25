package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/mateopresacastro/kv/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc/stats/opentelemetry"
)

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
		slog.Warn("PORT not set", "using", port)
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	defer listener.Close()

	exporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("failed to start prometheus exporter: %w", err)
	}
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	so := opentelemetry.ServerOption(
		opentelemetry.Options{MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: provider}},
	)

	server := server.New(so)
	errChan := make(chan error, 1)

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "4000"
		slog.Warn("METRICS_PORT not set", "using", metricsPort)
	}

	go func() {
		defer close(errChan)
		go func() {
			slog.Info("metrics listening...", "port", metricsPort)
			if err := http.ListenAndServe(":"+metricsPort, promhttp.Handler()); err != nil {
				slog.Error("metrics error", "err", err)
				errChan <- err
				cancel()
			}
		}()

		slog.Info("server listening...", "addr", listener.Addr())
		if err := server.Serve(listener); err != nil {
			slog.Error("server error", "err", err)
			errChan <- err
			cancel()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down server...")
	server.GracefulStop()
	slog.Info("server stopped gracefully")

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}
