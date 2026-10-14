package logger

import (
	"strings"

	"github.com/rs/zerolog"
)

// ZerologWriter adapts zerolog to io.Writer for the standard library log package.
type ZerologWriter struct {
	logger zerolog.Logger
}

// NewZerologWriter returns a ZerologWriter that writes to logger.
func NewZerologWriter(logger zerolog.Logger) *ZerologWriter {
	return &ZerologWriter{logger}
}

// Write logs p at the level found in its [LEVEL] prefix, defaulting to info.
func (w *ZerologWriter) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))

	switch {
	case strings.Contains(msg, "[ERR]") || strings.Contains(msg, "[ERROR]"):
		w.logger.Error().Msg(msg)
	case strings.Contains(msg, "[WARN]"):
		w.logger.Warn().Msg(msg)
	case strings.Contains(msg, "[DEBUG]"):
		w.logger.Debug().Msg(msg)
	default:
		w.logger.Info().Msg(msg)
	}

	return len(p), nil
}
