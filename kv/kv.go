package kv

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/mateopresacastro/mokv/server"
	"github.com/mateopresacastro/mokv/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc/stats/opentelemetry"
)

func Run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
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
	defer provider.Shutdown(ctx)

	so := opentelemetry.ServerOption(
		opentelemetry.Options{MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: provider}},
	)

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "4000"
		slog.Warn("METRICS_PORT not set", "using", metricsPort)
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			slog.Info("metrics server listening...", "port", metricsPort)
			if err := http.ListenAndServe(":"+metricsPort, promhttp.Handler()); err != nil && err != http.ErrServerClosed {
				slog.Error("metrics server failed", "err", err) // If the metrics server fails restart it after 5 seconds
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

	store := store.New()
	server := server.New(so, store)
	srvErrChan := make(chan error, 1)

	go func() {
		defer close(srvErrChan)
		slog.Info("grpc server listening...", "addr", listener.Addr())
		if err := server.Serve(listener); err != nil {
			slog.Error("server error", "err", err)
			srvErrChan <- err
			cancel()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down server...")
	server.GracefulStop()
	slog.Info("server stopped gracefully")

	select {
	case err := <-srvErrChan:
		return err
	default:
		return nil
	}
}
