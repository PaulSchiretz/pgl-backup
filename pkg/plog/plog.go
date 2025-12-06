package plog

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
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
var quietMode atomic.Bool // Use an atomic bool for safe concurrent reads.

// SetOutput allows redirecting the logger's output, primarily for testing.
func SetOutput(w io.Writer) {
	// When redirecting output for tests, ensure quiet mode is off
	// so that all levels are written to the provided writer.
	quietMode.Store(false)
	defaultLogger = slog.New(slog.NewTextHandler(w, nil))
}

// SetQuiet enables or disables quiet mode for the global logger.
// In quiet mode, INFO level logs are suppressed.
func SetQuiet(quiet bool) {
	quietMode.Store(quiet)
}

// IsQuiet returns true if the global logger is in quiet mode.
func IsQuiet() bool {
	return quietMode.Load()
}

func init() {
	// Handler for info-level logs (and below) to stdout
	stdoutHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
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
	if quietMode.Load() {
		return
	}
	defaultLogger.Info(msg, args...)
}

// Warn logs a warning message.
func Warn(msg string, args ...any) {
	defaultLogger.Warn(msg, args...)
}

// Error logs an error message.
func Error(msg string, args ...any) {
	defaultLogger.Error(msg, args...)
}
