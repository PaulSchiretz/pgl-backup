package plog

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// LevelDispatchHandler is a slog.Handler that writes log records to different
// handlers based on the record's level. INFO and below go to one handler,
// while WARNING and above go to another.
type LevelDispatchHandler struct {
	stdoutHandler slog.Handler
	stderrHandler slog.Handler
}

// Enabled checks if the level is enabled for either of the underlying handlers.
func (h *LevelDispatchHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.stdoutHandler.Enabled(ctx, level) || h.stderrHandler.Enabled(ctx, level)
}

// Handle dispatches the record to the appropriate handler.
func (h *LevelDispatchHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		return h.stderrHandler.Handle(ctx, r)
	}
	return h.stdoutHandler.Handle(ctx, r)
}

// WithAttrs returns a new LevelDispatchHandler with the given attributes added.
func (h *LevelDispatchHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LevelDispatchHandler{
		stdoutHandler: h.stdoutHandler.WithAttrs(attrs),
		stderrHandler: h.stderrHandler.WithAttrs(attrs),
	}
}

// WithGroup returns a new LevelDispatchHandler with the given group.
func (h *LevelDispatchHandler) WithGroup(name string) slog.Handler {
	return &LevelDispatchHandler{
		stdoutHandler: h.stdoutHandler.WithGroup(name),
		stderrHandler: h.stderrHandler.WithGroup(name),
	}
}

var defaultLogger *slog.Logger
var level = new(slog.LevelVar) // Use LevelVar for atomic, dynamic level changes.

// Level is an alias for slog.Level to avoid exposing slog directly to consumers of this package.
type Level slog.Level

// Log level constants, mirroring slog's levels.
const (
	LevelDebug  Level = Level(slog.LevelDebug)
	LevelNotice Level = Level(slog.LevelInfo - 2) // Between Debug (-4) and Info (0)
	LevelInfo   Level = Level(slog.LevelInfo)
	LevelWarn   Level = Level(slog.LevelWarn)
	LevelError  Level = Level(slog.LevelError)
)

// LevelFromString parses a string and returns the corresponding slog.Level.
// It defaults to slog.LevelInfo if the string is invalid.
func LevelFromString(levelStr string) Level {
	switch levelStr {
	case "debug":
		return LevelDebug
	case "notice":
		return LevelNotice
	case "info":
		return LevelInfo
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

// SetOutput allows redirecting the logger's output, primarily for testing.
func SetOutput(w io.Writer) {
	// Recreate the dual-handler setup, but point both to the test writer.
	// This ensures the test captures all output (stdout/stderr) and respects all log levels.
	// Crucially, we use the global 'level' variable here so that tests can change it.
	testHandler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})
	// Default to Debug level for tests to capture all potential output unless overridden.
	level.Set(slog.LevelDebug)
	defaultLogger = slog.New(&LevelDispatchHandler{
		stdoutHandler: testHandler,
		stderrHandler: testHandler,
	})
}

// SetLevel sets the global log level for the application.
func SetLevel(l Level) {
	level.Set(slog.Level(l))
}

// Default returns the default logger instance.
func Default() *slog.Logger {
	return defaultLogger
}

func init() {
	level.Set(slog.LevelInfo) // Default log level.
	// Handler for info-level logs (and below) to stdout
	stdoutHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})

	// Handler for warning/error-level logs to stderr
	stderrHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})

	defaultLogger = slog.New(&LevelDispatchHandler{
		stdoutHandler: stdoutHandler,
		stderrHandler: stderrHandler,
	})
}

// Info logs an informational message.
func Info(msg string, args ...any) {
	defaultLogger.Info(msg, args...)
}

// Notice logs a notice-level message.
func Notice(msg string, args ...any) {
	defaultLogger.Log(context.Background(), slog.Level(LevelNotice), msg, args...)
}

// Debug logs a debug message. It is suppressed when quiet mode is active.
func Debug(msg string, args ...any) {
	defaultLogger.Debug(msg, args...)
}

// Warn logs a warning message.
func Warn(msg string, args ...any) {
	defaultLogger.Warn(msg, args...)
}

// Error logs an error message.
func Error(msg string, args ...any) {
	defaultLogger.Error(msg, args...)
}
