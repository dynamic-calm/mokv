package logger

import (
	"io"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/rs/zerolog"
)

// DefaultNameField is the field a [Logger] writes its [hclog.Logger] name to.
// hclog prefixes messages with the logger name, while zerolog adds context as
// fields, so the name becomes a field. [NewWithCustomNameField] uses another
// field.
const DefaultNameField = "hclog_name"

// Logger adapts a [zerolog.Logger] to the [hclog.Logger] interface. It is
// based on https://github.com/psapezhka/hclog-zerolog.
type Logger struct {
	logger    zerolog.Logger
	nameField string
	name      string
}

// NewHCLogWrapper returns a [Logger] that writes to logger. For example, to
// use the global zerolog logger for [hashicorp/raft]:
//
//	raftLogger := log.With().Str("component", "raft").Logger()
//	config := raft.DefaultConfig()
//	config.Logger = logger.NewHCLogWrapper(raftLogger)
//
// [hashicorp/raft]: https://pkg.go.dev/github.com/hashicorp/raft#Config
func NewHCLogWrapper(logger zerolog.Logger) *Logger {
	return &Logger{
		logger:    logger.With().Str(DefaultNameField, "").Logger(),
		nameField: DefaultNameField,
		name:      "",
	}
}

// NewWithCustomNameField is like [NewHCLogWrapper], but writes the
// [hclog.Logger] name to nameField instead of [DefaultNameField].
func NewWithCustomNameField(logger zerolog.Logger, nameField string) *Logger {
	return &Logger{
		logger:    logger.With().Str(nameField, "").Logger(),
		nameField: nameField,
		name:      "",
	}
}

func (l *Logger) Log(level hclog.Level, msg string, args ...any) {
	switch level {
	case hclog.Trace:
		l.logger.Trace().Fields(args).Msg(msg)
	case hclog.Debug:
		l.logger.Debug().Fields(args).Msg(msg)
	case hclog.Info:
		l.logger.Info().Fields(args).Msg(msg)
	case hclog.Warn:
		l.logger.Warn().Fields(args).Msg(msg)
	case hclog.Error:
		l.logger.Error().Fields(args).Msg(msg)
	case hclog.NoLevel:
		l.logger.Log().Fields(args).Msg(msg)
	case hclog.Off:
	default:
		l.logger.Error().Msgf("Unknown log level: %s", level)
	}
}

func (l *Logger) Trace(format string, args ...any) {
	l.logger.Trace().Fields(args).Msg(format)
}

func (l *Logger) Debug(format string, args ...any) {
	l.logger.Debug().Fields(args).Msg(format)
}

func (l *Logger) Info(format string, args ...any) {
	l.logger.Info().Fields(args).Msg(format)
}

func (l *Logger) Warn(format string, args ...any) {
	l.logger.Warn().Fields(args).Msg(format)
}

func (l *Logger) Error(format string, args ...any) {
	l.logger.Error().Fields(args).Msg(format)
}

func (l *Logger) IsTrace() bool {
	return l.logger.GetLevel() == zerolog.TraceLevel
}

func (l *Logger) IsDebug() bool {
	return l.logger.GetLevel() == zerolog.DebugLevel
}

func (l *Logger) IsInfo() bool {
	return l.logger.GetLevel() == zerolog.InfoLevel
}

func (l *Logger) IsWarn() bool {
	return l.logger.GetLevel() == zerolog.WarnLevel
}

func (l *Logger) IsError() bool {
	return l.logger.GetLevel() == zerolog.ErrorLevel
}

func (l *Logger) ImpliedArgs() []any {
	return nil
}

func (l *Logger) With(args ...any) hclog.Logger {
	return &Logger{l.logger.With().Fields(args).Logger(), l.nameField, l.name}
}

func (l *Logger) Name() string {
	return l.name
}

func (l *Logger) Named(name string) hclog.Logger {
	var newName string
	if l.name == "" {
		newName = name
	} else {
		newName = l.name + "." + name
	}

	return &Logger{l.logger.With().Str(l.nameField, newName).Logger(), l.nameField, newName}
}

func (l *Logger) ResetNamed(name string) hclog.Logger {
	return &Logger{l.logger.With().Str(l.nameField, name).Logger(), l.nameField, name}
}

func (l *Logger) SetLevel(level hclog.Level) {
	switch level {
	case hclog.Trace:
		l.logger = l.logger.Level(zerolog.TraceLevel)
	case hclog.Debug:
		l.logger = l.logger.Level(zerolog.DebugLevel)
	case hclog.Info:
		l.logger = l.logger.Level(zerolog.InfoLevel)
	case hclog.Warn:
		l.logger = l.logger.Level(zerolog.WarnLevel)
	case hclog.Error:
		l.logger = l.logger.Level(zerolog.ErrorLevel)
	case hclog.Off:
		l.logger = l.logger.Level(zerolog.Disabled)
	case hclog.NoLevel:
		l.logger = l.logger.Level(zerolog.NoLevel)
	default:
		l.logger.Error().Msgf("Unknown log level: %s", level)
	}
}

func (l *Logger) GetLevel() hclog.Level {
	switch l.logger.GetLevel() {
	case zerolog.TraceLevel:
		return hclog.Trace
	case zerolog.DebugLevel:
		return hclog.Debug
	case zerolog.InfoLevel:
		return hclog.Info
	case zerolog.WarnLevel:
		return hclog.Warn
	case zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel:
		return hclog.Error
	case zerolog.Disabled:
		return hclog.Off
	case zerolog.NoLevel:
		return hclog.NoLevel
	default:
		l.logger.Error().Msgf("Unknown log level: %s", l.logger.GetLevel())

		return hclog.NoLevel
	}
}

func (l *Logger) StandardLogger(_ *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(l.logger, "", 0)
}

func (l *Logger) StandardWriter(_ *hclog.StandardLoggerOptions) io.Writer {
	return l.logger
}
