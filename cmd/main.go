package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"

	"github.com/mateopresacastro/kv"
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

	server := kv.NewServer()
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		slog.Info("listening...", "addr", listener.Addr())
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
