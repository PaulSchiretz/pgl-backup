package plog

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestPlogLevels(t *testing.T) {
	// --- Setup: Redirect plog output to capture log output ---
	var logBuf bytes.Buffer
	SetOutput(&logBuf)
	t.Cleanup(func() { SetOutput(os.Stderr) }) // Restore original output after test.

	t.Run("Logs Debug and Info when not quiet", func(t *testing.T) {
		logBuf.Reset()
		SetLevel(LevelDebug)

		Debug("debug message", "key", "val1")
		Info("info message", "key", "val2")
		Warn("warn message") // Should be in the buffer now, as SetOutput captures all levels.

		output := logBuf.String()

		if !strings.Contains(output, "level=DEBUG msg=\"debug message\" key=val1") {
			t.Errorf("expected debug message to be logged, but it wasn't. Got: %s", output)
		}
		if !strings.Contains(output, "level=INFO msg=\"info message\" key=val2") {
			t.Errorf("expected info message to be logged, but it wasn't. Got: %s", output)
		}
		if !strings.Contains(output, "level=WARN msg=\"warn message\"") {
			t.Errorf("expected warn message to be logged, but it wasn't. Got: %s", output)
		}
	})

	t.Run("Suppresses Debug and Info when quiet", func(t *testing.T) {
		logBuf.Reset()
		SetLevel(LevelWarn) // Set level to Warn, which should suppress Debug and Info

		Debug("debug message")
		Info("info message")

		output := logBuf.String()

		if strings.Contains(output, "level=DEBUG") || strings.Contains(output, "level=INFO") {
			t.Errorf("expected no debug or info output at warn level, but got: %s", output)
		}
	})
}
