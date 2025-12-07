// Taken from https://github.com/psapezhka/hclog-zerolog/blob/main/wrapper_test.go
package logger

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/rs/zerolog"
)

const (
	customFieldName  = "custom_field"
	customFieldValue = "value"
	messageToLog     = "test message"
)

type message struct {
	Level       string `json:"level"`
	Message     string `json:"message"`
	HCLogName   string `json:"hclog_name"`
	CustomField string `json:"custom_field"`
}

func (m message) Equal(other message) bool {
	return m.Level == other.Level &&
		m.Message == other.Message &&
		m.HCLogName == other.HCLogName &&
		m.CustomField == other.CustomField
}

func TestNew(t *testing.T) {
	buf := &bytes.Buffer{}
	hclogLogger := NewHCLogWrapper(zerolog.New(buf))

	if hclogLogger.nameField != DefaultNameField {
		t.Errorf("expected nameField to be %q, got %q", DefaultNameField, hclogLogger.nameField)
	}

	if hclogLogger.name != "" {
		t.Errorf("expected name to be empty, got %q", hclogLogger.name)
	}
}

func TestNewWithCustomNameField(t *testing.T) {
	buf := &bytes.Buffer{}
	customNameField := "custom_name"

	hclogLogger := NewWithCustomNameField(zerolog.New(buf), customNameField)

	if hclogLogger.nameField != customNameField {
		t.Errorf("expected nameField to be %q, got %q", customNameField, hclogLogger.nameField)
	}

	if hclogLogger.name != "" {
		t.Errorf("expected name to be empty, got %q", hclogLogger.name)
	}
}

func TestLog(t *testing.T) {
	t.Run("logs messages at level", func(t *testing.T) {
		logLevels := []struct {
			level         hclog.Level
			expectedLevel string
		}{
			{hclog.Trace, "trace"},
			{hclog.Debug, "debug"},
			{hclog.Info, "info"},
			{hclog.Warn, "warn"},
			{hclog.Error, "error"},
			{hclog.NoLevel, ""},
		}

		for _, tt := range logLevels {
			t.Run(tt.expectedLevel, func(t *testing.T) {
				buf := &bytes.Buffer{}
				msg := &message{}
				logger := zerolog.New(buf)
				hclogLogger := NewHCLogWrapper(logger)

				hclogLogger.Log(tt.level, messageToLog, customFieldName, customFieldValue)

				if err := json.Unmarshal(buf.Bytes(), msg); err != nil {
					t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
				}

				wantedMessage := &message{
					Level:       tt.expectedLevel,
					Message:     messageToLog,
					HCLogName:   "",
					CustomField: customFieldValue,
				}

				if !msg.Equal(*wantedMessage) {
					t.Errorf("expected message to be\n %+v\n got\n %+v", wantedMessage, msg)
				}
			})
		}
	})

	t.Run("logs messages with unknown level", func(t *testing.T) {
		buf := &bytes.Buffer{}
		msg := &message{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		hclogLogger.Log(hclog.Level(999), messageToLog, customFieldName, customFieldValue)

		if err := json.Unmarshal(buf.Bytes(), msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		wantedMessage := &message{
			Level:       "error",
			Message:     "Unknown log level: unknown",
			HCLogName:   "",
			CustomField: "",
		}

		if !msg.Equal(*wantedMessage) {
			t.Errorf("expected message to be\n %+v\n got\n %+v", wantedMessage, msg)
		}
	})
}

func TestTrace(t *testing.T) {
	t.Run("logs trace level messages", func(t *testing.T) {
		buf := &bytes.Buffer{}
		msg := &message{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		hclogLogger.Trace(messageToLog, customFieldName, customFieldValue)

		if err := json.Unmarshal(buf.Bytes(), msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		wantedMessage := &message{
			Level:       "trace",
			Message:     messageToLog,
			HCLogName:   "",
			CustomField: customFieldValue,
		}

		if !msg.Equal(*wantedMessage) {
			t.Errorf("expected message to be\n %+v\n got\n %+v", wantedMessage, msg)
		}
	})
}

func TestDebug(t *testing.T) {
	t.Run("logs debug level messages", func(t *testing.T) {
		buf := &bytes.Buffer{}
		msg := &message{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		hclogLogger.Debug(messageToLog, customFieldName, customFieldValue)

		if err := json.Unmarshal(buf.Bytes(), msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		wantedMessage := &message{
			Level:       "debug",
			Message:     messageToLog,
			HCLogName:   "",
			CustomField: customFieldValue,
		}

		if !msg.Equal(*wantedMessage) {
			t.Errorf("expected message to be\n %+v\n got\n %+v", wantedMessage, msg)
		}
	})
}

func TestInfo(t *testing.T) {
	t.Run("logs info level messages", func(t *testing.T) {
		buf := &bytes.Buffer{}
		msg := &message{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		hclogLogger.Info(messageToLog, customFieldName, customFieldValue)

		if err := json.Unmarshal(buf.Bytes(), msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		wantedMessage := &message{
			Level:       "info",
			Message:     messageToLog,
			HCLogName:   "",
			CustomField: customFieldValue,
		}

		if !msg.Equal(*wantedMessage) {
			t.Errorf("expected message to be\n %+v\n got\n %+v", wantedMessage, msg)
		}
	})
}

func TestWarn(t *testing.T) {
	t.Run("logs warn level messages", func(t *testing.T) {
		buf := &bytes.Buffer{}
		msg := &message{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		hclogLogger.Warn(messageToLog, customFieldName, customFieldValue)

		if err := json.Unmarshal(buf.Bytes(), msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		wantedMessage := &message{
			Level:       "warn",
			Message:     messageToLog,
			HCLogName:   "",
			CustomField: customFieldValue,
		}

		if !msg.Equal(*wantedMessage) {
			t.Errorf("expected message to be\n %+v\n got\n %+v", wantedMessage, msg)
		}
	})
}

func TestError(t *testing.T) {
	t.Run("logs error level messages", func(t *testing.T) {
		buf := &bytes.Buffer{}
		msg := &message{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		hclogLogger.Error(messageToLog, customFieldName, customFieldValue)

		if err := json.Unmarshal(buf.Bytes(), msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		wantedMessage := &message{
			Level:       "error",
			Message:     messageToLog,
			HCLogName:   "",
			CustomField: customFieldValue,
		}

		if !msg.Equal(*wantedMessage) {
			t.Errorf("expected message to be\n %+v\n got\n %+v", wantedMessage, msg)
		}
	})
}

func TestIsTrace(t *testing.T) {
	t.Run("returns true when logger level is Trace", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.TraceLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if !hclogLogger.IsTrace() {
			t.Errorf("expected IsTrace to return true, got false")
		}
	})

	t.Run("returns false when logger level is not Trace", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.InfoLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if hclogLogger.IsTrace() {
			t.Errorf("expected IsTrace to return false, got true")
		}
	})
}

func TestIsDebug(t *testing.T) {
	t.Run("returns true when logger level is Debug", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.DebugLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if !hclogLogger.IsDebug() {
			t.Errorf("expected IsDebug to return true, got false")
		}
	})

	t.Run("returns false when logger level is not Debug", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.InfoLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if hclogLogger.IsDebug() {
			t.Errorf("expected IsDebug to return false, got true")
		}
	})
}

func TestIsInfo(t *testing.T) {
	t.Run("returns true when logger level is Info", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.InfoLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if !hclogLogger.IsInfo() {
			t.Errorf("expected IsInfo to return true, got false")
		}
	})

	t.Run("returns false when logger level is not Info", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.DebugLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if hclogLogger.IsInfo() {
			t.Errorf("expected IsInfo to return false, got true")
		}
	})
}

func TestIsWarn(t *testing.T) {
	t.Run("returns true when logger level is Warn", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.WarnLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if !hclogLogger.IsWarn() {
			t.Errorf("expected IsWarn to return true, got false")
		}
	})

	t.Run("returns false when logger level is not Warn", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.InfoLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if hclogLogger.IsWarn() {
			t.Errorf("expected IsWarn to return false, got true")
		}
	})
}

func TestIsError(t *testing.T) {
	t.Run("returns true when logger level is Error", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.ErrorLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if !hclogLogger.IsError() {
			t.Errorf("expected IsError to return true, got false")
		}
	})

	t.Run("returns false when logger level is not Error", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{}).Level(zerolog.WarnLevel)
		hclogLogger := NewHCLogWrapper(logger)

		if hclogLogger.IsError() {
			t.Errorf("expected IsError to return false, got true")
		}
	})
}

func TestImpliedArgs(t *testing.T) {
	t.Run("returns nil for implied args", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{})
		hclogLogger := NewHCLogWrapper(logger)

		impliedArgs := hclogLogger.ImpliedArgs()

		if impliedArgs != nil {
			t.Errorf("expected impliedArgs to be nil, got %v", impliedArgs)
		}
	})
}

func TestWith(t *testing.T) {
	t.Run("returns a new logger with additional fields", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		key := "key"
		value := "value"
		newLogger := hclogLogger.With(key, value)

		// Log a message using the new logger
		newLogger.Info("test message")

		// Parse the logged message
		var msg map[string]any
		if err := json.Unmarshal(buf.Bytes(), &msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		if msg[key] != value {
			t.Errorf("expected field %q to have value %q, got %q", key, value, msg[key])
		}

		if msg["message"] != "test message" {
			t.Errorf("expected message to be %q, got %q", "test message", msg["message"])
		}
	})
}

func TestName(t *testing.T) {
	t.Run("returns the logger's name", func(t *testing.T) {
		logger := zerolog.New(&bytes.Buffer{})
		hclogLogger := NewHCLogWrapper(logger)

		if hclogLogger.Name() != "" {
			t.Errorf("expected name to be empty, got %q", hclogLogger.Name())
		}

		namedLogger := hclogLogger.Named("test")
		if namedLogger.Name() != "test" {
			t.Errorf("expected name to be %q, got %q", ".test", namedLogger.Name())
		}

		nestedLogger := namedLogger.Named("nested")
		if nestedLogger.Name() != "test.nested" {
			t.Errorf("expected name to be %q, got %q", ".test.nested", nestedLogger.Name())
		}
	})
}

func TestResetNamed(t *testing.T) {
	t.Run("resets the logger name", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger).Named("test")

		newName := "reset"
		resetLogger := hclogLogger.ResetNamed(newName)

		if resetLogger.Name() != newName {
			t.Errorf("expected name to be %q, got %q", newName, resetLogger.Name())
		}

		resetLogger.Info("test message")

		var msg map[string]any
		if err := json.Unmarshal(buf.Bytes(), &msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		if msg[DefaultNameField] != newName {
			t.Errorf("expected %q field to be %q, got %q", DefaultNameField, newName, msg[DefaultNameField])
		}

		if msg["message"] != "test message" {
			t.Errorf("expected message to be %q, got %q", "test message", msg["message"])
		}
	})
}

func TestSetLevel(t *testing.T) {
	t.Run("sets the logger level correctly", func(t *testing.T) {
		tests := []struct {
			hclogLevel    hclog.Level
			expectedLevel zerolog.Level
		}{
			{hclog.Trace, zerolog.TraceLevel},
			{hclog.Debug, zerolog.DebugLevel},
			{hclog.Info, zerolog.InfoLevel},
			{hclog.Warn, zerolog.WarnLevel},
			{hclog.Error, zerolog.ErrorLevel},
			{hclog.Off, zerolog.Disabled},
			{hclog.NoLevel, zerolog.NoLevel},
		}

		for _, tt := range tests {
			t.Run(tt.hclogLevel.String(), func(t *testing.T) {
				logger := zerolog.New(&bytes.Buffer{})
				hclogLogger := NewHCLogWrapper(logger)

				hclogLogger.SetLevel(tt.hclogLevel)

				if hclogLogger.logger.GetLevel() != tt.expectedLevel {
					t.Errorf("expected logger level to be %v, got %v", tt.expectedLevel, hclogLogger.logger.GetLevel())
				}
			})
		}
	})

	t.Run("handles unknown log levels gracefully", func(t *testing.T) {
		buf := &bytes.Buffer{}
		msg := &message{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		hclogLogger.SetLevel(hclog.Level(999))

		if err := json.Unmarshal(buf.Bytes(), msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		wantedMessage := &message{
			Level:       "error",
			Message:     "Unknown log level: unknown",
			HCLogName:   "",
			CustomField: "",
		}

		if !msg.Equal(*wantedMessage) {
			t.Errorf("expected message to be\n %+v\n got\n %+v", wantedMessage, msg)
		}
	})
}

func TestGetLevel(t *testing.T) {
	tests := []struct {
		zerologLevel  zerolog.Level
		expectedLevel hclog.Level
	}{
		{zerolog.TraceLevel, hclog.Trace},
		{zerolog.DebugLevel, hclog.Debug},
		{zerolog.InfoLevel, hclog.Info},
		{zerolog.WarnLevel, hclog.Warn},
		{zerolog.ErrorLevel, hclog.Error},
		{zerolog.FatalLevel, hclog.Error},
		{zerolog.PanicLevel, hclog.Error},
		{zerolog.Disabled, hclog.Off},
		{zerolog.NoLevel, hclog.NoLevel},
	}

	for _, tt := range tests {
		t.Run(tt.zerologLevel.String(), func(t *testing.T) {
			logger := zerolog.New(&bytes.Buffer{}).Level(tt.zerologLevel)
			hclogLogger := NewHCLogWrapper(logger)

			level := hclogLogger.GetLevel()

			if level != tt.expectedLevel {
				t.Errorf("expected level to be %v, got %v", tt.expectedLevel, level)
			}
		})
	}

	t.Run("handles unknown zerolog levels gracefully", func(t *testing.T) {
		buf := &bytes.Buffer{}
		msg := &message{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		hclogLogger.logger = hclogLogger.logger.Level(zerolog.Level(-2))

		level := hclogLogger.GetLevel()

		if level != hclog.NoLevel {
			t.Errorf("expected level to be %v for unknown zerolog level, got %v", hclog.NoLevel, level)
		}

		if err := json.Unmarshal(buf.Bytes(), msg); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		wantedMessage := &message{
			Level:       "error",
			Message:     "Unknown log level: -2",
			HCLogName:   "",
			CustomField: "",
		}

		if !msg.Equal(*wantedMessage) {
			t.Errorf("expected message to be\n %+v\n got\n %+v", wantedMessage, msg)
		}
	})
}

func TestStandardLogger(t *testing.T) {
	t.Run("returns a standard logger that writes to zerolog", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		stdLogger := hclogLogger.StandardLogger(nil)

		message := "standard logger message"
		stdLogger.Println(message)

		var loggedMessage map[string]any
		if err := json.Unmarshal(buf.Bytes(), &loggedMessage); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		if loggedMessage["message"] != message {
			t.Errorf("expected message to be %q, got %q", message, loggedMessage["message"])
		}
	})
}

func TestStandardWriter(t *testing.T) {
	t.Run("returns a writer that writes to zerolog", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := zerolog.New(buf)
		hclogLogger := NewHCLogWrapper(logger)

		writer := hclogLogger.StandardWriter(nil)

		message := "standard writer message\n"

		_, err := writer.Write([]byte(message))
		if err != nil {
			t.Fatalf("expected no error while writing, got: %v", err)
		}

		var loggedMessage map[string]any
		if err := json.Unmarshal(buf.Bytes(), &loggedMessage); err != nil {
			t.Fatalf("Expected log output to be a valid JSON, got: %s", buf.String())
		}

		if loggedMessage["message"] != "standard writer message" {
			t.Errorf("expected message to be %q, got %q", "standard writer message", loggedMessage["message"])
		}
	})
}
