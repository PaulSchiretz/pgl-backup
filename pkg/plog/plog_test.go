package plog

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestLevelDispatchHandler(t *testing.T) {
	var stdoutBuf, stderrBuf bytes.Buffer

	// Create a test logger that writes to our buffers instead of os.Stdout/Stderr
	testHandler := &LevelDispatchHandler{
		stdoutHandler: slog.NewTextHandler(&stdoutBuf, &slog.HandlerOptions{Level: slog.LevelInfo}),
		stderrHandler: slog.NewTextHandler(&stderrBuf, &slog.HandlerOptions{Level: slog.LevelWarn}),
	}
	testLogger := slog.New(testHandler)

	t.Run("Info logs to stdout", func(t *testing.T) {
		stdoutBuf.Reset()
		stderrBuf.Reset()

		testLogger.Info("info message", "key", "value")

		if !strings.Contains(stdoutBuf.String(), "level=INFO msg=\"info message\" key=value") {
			t.Errorf("expected info message in stdout, but got: %q", stdoutBuf.String())
		}
		if stderrBuf.Len() > 0 {
			t.Errorf("expected stderr to be empty, but got: %q", stderrBuf.String())
		}
	})

	t.Run("Warn logs to stderr", func(t *testing.T) {
		stdoutBuf.Reset()
		stderrBuf.Reset()

		testLogger.Warn("warn message", "key", "value")

		if stdoutBuf.Len() > 0 {
			t.Errorf("expected stdout to be empty, but got: %q", stdoutBuf.String())
		}
		if !strings.Contains(stderrBuf.String(), "level=WARN msg=\"warn message\" key=value") {
			t.Errorf("expected warn message in stderr, but got: %q", stderrBuf.String())
		}
	})

	t.Run("Error logs to stderr", func(t *testing.T) {
		stdoutBuf.Reset()
		stderrBuf.Reset()

		testLogger.Error("error message", "key", "value")

		if stdoutBuf.Len() > 0 {
			t.Errorf("expected stdout to be empty, but got: %q", stdoutBuf.String())
		}
		if !strings.Contains(stderrBuf.String(), "level=ERROR msg=\"error message\" key=value") {
			t.Errorf("expected error message in stderr, but got: %q", stderrBuf.String())
		}
	})

	t.Run("WithAttrs propagates to both handlers", func(t *testing.T) {
		stdoutBuf.Reset()
		stderrBuf.Reset()

		loggerWithAttr := testLogger.With("attr_key", "attr_value")

		// Test info log
		loggerWithAttr.Info("info with attr")
		if !strings.Contains(stdoutBuf.String(), "attr_key=attr_value") {
			t.Error("expected attribute in stdout log")
		}

		// Test warn log
		loggerWithAttr.Warn("warn with attr")
		if !strings.Contains(stderrBuf.String(), "attr_key=attr_value") {
			t.Error("expected attribute in stderr log")
		}
	})

	t.Run("WithGroup propagates to both handlers", func(t *testing.T) {
		stdoutBuf.Reset()
		stderrBuf.Reset()

		loggerWithGroup := testLogger.WithGroup("my_group")

		// Test info log
		loggerWithGroup.Info("info in group", "inner_key", "inner_value")
		if !strings.Contains(stdoutBuf.String(), "my_group.inner_key=inner_value") {
			t.Errorf("expected grouped attribute in stdout log, got: %q", stdoutBuf.String())
		}

		// Test warn log
		loggerWithGroup.Warn("warn in group", "inner_key", "inner_value")
		if !strings.Contains(stderrBuf.String(), "my_group.inner_key=inner_value") {
			t.Errorf("expected grouped attribute in stderr log, got: %q", stderrBuf.String())
		}
	})

	t.Run("Enabled method works correctly", func(t *testing.T) {
		ctx := context.Background()

		if !testHandler.Enabled(ctx, slog.LevelInfo) {
			t.Error("expected handler to be enabled for INFO level")
		}
		if !testHandler.Enabled(ctx, slog.LevelWarn) {
			t.Error("expected handler to be enabled for WARN level")
		}
		if !testHandler.Enabled(ctx, slog.LevelError) {
			t.Error("expected handler to be enabled for ERROR level")
		}

		// Based on the handler setup, DEBUG should be disabled.
		if testHandler.Enabled(ctx, slog.LevelDebug) {
			t.Error("expected handler to be disabled for DEBUG level")
		}
	})
}
