package logger

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Config holds logger configuration
type Config struct {
	Level       string
	Development bool
	ServiceName string
}

// Setup configures the global logger.
func Setup(cfg Config, output io.Writer) zerolog.Logger {
	// Pretty console output for development
	if cfg.Development {
		output = zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.RFC3339,
			NoColor:    false,
		}
	}

	level, err := zerolog.ParseLevel(strings.ToLower(cfg.Level))
	if err != nil {
		level = zerolog.InfoLevel
	}

	// Set global level (affects all loggers)
	zerolog.SetGlobalLevel(level)

	// Configure global settings
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.DurationFieldUnit = time.Millisecond

	// Build logger with default fields
	logger := zerolog.New(output).
		With().
		Timestamp().
		Str("service", cfg.ServiceName).
		Logger()

	// Set as global logger
	log.Logger = logger

	return logger
}
