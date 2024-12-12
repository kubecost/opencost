package log

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestGetLogger(t *testing.T) {
	initialLogger := GetLogger()
	if initialLogger == nil {
		t.Error("GetLogger() returned nil")
	}

	secondLogger := GetLogger()
	if initialLogger != secondLogger {
		t.Error("GetLogger() returned different loggers on subsequent calls")
	}
}

func TestSetLogger(t *testing.T) {
	var buf bytes.Buffer
	newLogger := zerolog.New(&buf).With().Str("test", "value").Logger()
	SetLogger(&newLogger)

	// Log a message using the global logger
	Infof("Test message")

	// Parse the logged message
	loggedData := parseLogMessage(t, buf.String())

	// Check if the "test" field is present in the logged message
	if value, exists := loggedData["test"]; !exists || value != "value" {
		t.Error("SetLogger() did not set the logger with expected context")
	}
}

func TestLoggerConsistency(t *testing.T) {
	var buf bytes.Buffer
	newLogger := zerolog.New(&buf).With().Str("test", "consistency").Logger()
	SetLogger(&newLogger)

	// Log a message using the global logger
	Infof("Consistency test message")

	// Parse the logged message
	loggedData := parseLogMessage(t, buf.String())

	// Check if the "test" field is present in the logged message
	if value, exists := loggedData["test"]; !exists || value != "consistency" {
		t.Error("Logger inconsistency: Updated logger does not have expected context")
	}
}

func parseLogMessage(t *testing.T, logMessage string) map[string]interface{} {
	var loggedData map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(logMessage)), &loggedData); err != nil {
		t.Fatalf("Failed to parse logged message: %v", err)
	}
	return loggedData
}
