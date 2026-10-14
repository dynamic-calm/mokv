// Package logger configures zerolog and adapts it for other log interfaces.
package logger

import (
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Config configures the global logger.
type Config struct {
	// Level is the minimum level logged, such as "debug" or "INFO". An
	// unknown level means info.
	Level string
	// Development makes the output human-readable instead of JSON.
	Development bool
	// ServiceName is added to every entry as the service field.
	ServiceName string
}

// Setup configures the global zerolog logger and level from cfg, writing to
// output, and returns the logger.
func Setup(cfg Config, output io.Writer) zerolog.Logger {
	if cfg.Development {
		output = zerolog.ConsoleWriter{Out: output, TimeFormat: time.RFC3339}
	}

	level, err := zerolog.ParseLevel(strings.ToLower(cfg.Level))
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.DurationFieldUnit = time.Millisecond

	logger := zerolog.New(output).With().Timestamp().Str("service", cfg.ServiceName).Logger()
	log.Logger = logger
	return logger
}
