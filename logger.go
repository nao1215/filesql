package filesql

import "log/slog"

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
