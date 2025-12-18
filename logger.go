package filesql

import (
	"context"
	"log/slog"
)

// Logger defines the interface for logging within filesql.
// Users can implement this interface to use their own logging solution.
// The interface is designed to be compatible with slog.Logger.
type Logger interface {
	// Debug logs a debug message with optional key-value pairs
	Debug(msg string, args ...any)
	// Info logs an info message with optional key-value pairs
	Info(msg string, args ...any)
	// Warn logs a warning message with optional key-value pairs
	Warn(msg string, args ...any)
	// Error logs an error message with optional key-value pairs
	Error(msg string, args ...any)
	// With returns a new Logger with the given key-value pairs added to the context
	With(args ...any) Logger
}

// SlogAdapter wraps slog.Logger to implement the Logger interface
type SlogAdapter struct {
	logger *slog.Logger
}

// NewSlogAdapter creates a new SlogAdapter wrapping the given slog.Logger
func NewSlogAdapter(logger *slog.Logger) *SlogAdapter {
	return &SlogAdapter{logger: logger}
}

// Debug logs a debug message
func (s *SlogAdapter) Debug(msg string, args ...any) {
	s.logger.Debug(msg, args...)
}

// Info logs an info message
func (s *SlogAdapter) Info(msg string, args ...any) {
	s.logger.Info(msg, args...)
}

// Warn logs a warning message
func (s *SlogAdapter) Warn(msg string, args ...any) {
	s.logger.Warn(msg, args...)
}

// Error logs an error message
func (s *SlogAdapter) Error(msg string, args ...any) {
	s.logger.Error(msg, args...)
}

// With returns a new Logger with the given key-value pairs added
func (s *SlogAdapter) With(args ...any) Logger {
	return &SlogAdapter{logger: s.logger.With(args...)}
}

// ContextLogger extends Logger with context-aware logging methods
type ContextLogger interface {
	Logger
	// DebugContext logs a debug message with context
	DebugContext(ctx context.Context, msg string, args ...any)
	// InfoContext logs an info message with context
	InfoContext(ctx context.Context, msg string, args ...any)
	// WarnContext logs a warning message with context
	WarnContext(ctx context.Context, msg string, args ...any)
	// ErrorContext logs an error message with context
	ErrorContext(ctx context.Context, msg string, args ...any)
}

// SlogContextAdapter wraps slog.Logger to implement the ContextLogger interface
type SlogContextAdapter struct {
	SlogAdapter
}

// NewSlogContextAdapter creates a new SlogContextAdapter wrapping the given slog.Logger
func NewSlogContextAdapter(logger *slog.Logger) *SlogContextAdapter {
	return &SlogContextAdapter{SlogAdapter: SlogAdapter{logger: logger}}
}

// DebugContext logs a debug message with context
func (s *SlogContextAdapter) DebugContext(ctx context.Context, msg string, args ...any) {
	s.logger.DebugContext(ctx, msg, args...)
}

// InfoContext logs an info message with context
func (s *SlogContextAdapter) InfoContext(ctx context.Context, msg string, args ...any) {
	s.logger.InfoContext(ctx, msg, args...)
}

// WarnContext logs a warning message with context
func (s *SlogContextAdapter) WarnContext(ctx context.Context, msg string, args ...any) {
	s.logger.WarnContext(ctx, msg, args...)
}

// ErrorContext logs an error message with context
func (s *SlogContextAdapter) ErrorContext(ctx context.Context, msg string, args ...any) {
	s.logger.ErrorContext(ctx, msg, args...)
}

// With returns a new ContextLogger with the given key-value pairs added
func (s *SlogContextAdapter) With(args ...any) Logger {
	return &SlogContextAdapter{SlogAdapter: SlogAdapter{logger: s.logger.With(args...)}}
}

// nopLogger is a no-op logger that discards all log messages
type nopLogger struct{}

// newNopLogger creates a new no-op logger
func newNopLogger() Logger {
	return &nopLogger{}
}

// Debug does nothing
func (n *nopLogger) Debug(_ string, _ ...any) {}

// Info does nothing
func (n *nopLogger) Info(_ string, _ ...any) {}

// Warn does nothing
func (n *nopLogger) Warn(_ string, _ ...any) {}

// Error does nothing
func (n *nopLogger) Error(_ string, _ ...any) {}

// With returns the same no-op logger
func (n *nopLogger) With(_ ...any) Logger {
	return n
}
