package filesql

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNopLogger(t *testing.T) {
	t.Parallel()

	t.Run("nopLogger implements Logger interface", func(t *testing.T) {
		t.Parallel()
		logger := newNopLogger()
		// Verify it implements Logger interface by using interface methods
		assert.NotNil(t, logger)
		assert.NotPanics(t, func() { logger.Info("test") })
	})

	t.Run("nopLogger methods do not panic", func(t *testing.T) {
		t.Parallel()
		logger := newNopLogger()

		assert.NotPanics(t, func() {
			logger.Debug("debug message", "key", "value")
			logger.Info("info message", "key", "value")
			logger.Warn("warn message", "key", "value")
			logger.Error("error message", "key", "value")
		})
	})

	t.Run("nopLogger With returns same logger", func(t *testing.T) {
		t.Parallel()
		logger := newNopLogger()
		withLogger := logger.With("key", "value")
		assert.NotNil(t, withLogger)
	})
}

func TestSlogAdapter(t *testing.T) {
	t.Parallel()

	t.Run("SlogAdapter implements Logger interface", func(t *testing.T) {
		t.Parallel()
		slogLogger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
		var logger Logger = NewSlogAdapter(slogLogger)
		assert.NotNil(t, logger)
	})

	t.Run("SlogAdapter logs messages", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logger := NewSlogAdapter(slogLogger)

		logger.Debug("debug message", "key", "value1")
		logger.Info("info message", "key", "value2")
		logger.Warn("warn message", "key", "value3")
		logger.Error("error message", "key", "value4")

		output := buf.String()
		assert.Contains(t, output, "debug message")
		assert.Contains(t, output, "info message")
		assert.Contains(t, output, "warn message")
		assert.Contains(t, output, "error message")
		assert.Contains(t, output, "value1")
		assert.Contains(t, output, "value2")
		assert.Contains(t, output, "value3")
		assert.Contains(t, output, "value4")
	})

	t.Run("SlogAdapter With adds context", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logger := NewSlogAdapter(slogLogger)

		withLogger := logger.With("component", "test")
		withLogger.Info("test message")

		output := buf.String()
		assert.Contains(t, output, "component")
		assert.Contains(t, output, "test")
	})
}

func TestSlogContextAdapter(t *testing.T) {
	t.Parallel()

	t.Run("SlogContextAdapter implements ContextLogger interface", func(t *testing.T) {
		t.Parallel()
		slogLogger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
		var logger ContextLogger = NewSlogContextAdapter(slogLogger)
		assert.NotNil(t, logger)
	})

	t.Run("SlogContextAdapter logs with context", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logger := NewSlogContextAdapter(slogLogger)

		ctx := context.Background()
		logger.DebugContext(ctx, "debug message", "key", "value1")
		logger.InfoContext(ctx, "info message", "key", "value2")
		logger.WarnContext(ctx, "warn message", "key", "value3")
		logger.ErrorContext(ctx, "error message", "key", "value4")

		output := buf.String()
		assert.Contains(t, output, "debug message")
		assert.Contains(t, output, "info message")
		assert.Contains(t, output, "warn message")
		assert.Contains(t, output, "error message")
	})

	t.Run("SlogContextAdapter With returns ContextLogger-compatible Logger", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logger := NewSlogContextAdapter(slogLogger)

		withLogger := logger.With("component", "test")
		assert.NotNil(t, withLogger)
		withLogger.Info("test message")

		output := buf.String()
		assert.Contains(t, output, "component")
	})
}

func TestDBBuilderWithLogger(t *testing.T) {
	t.Parallel()

	t.Run("WithLogger sets custom logger", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logger := NewSlogAdapter(slogLogger)

		builder := NewBuilder().WithLogger(logger)
		assert.NotNil(t, builder)
	})

	t.Run("WithLogger with nil does not change logger", func(t *testing.T) {
		t.Parallel()
		builder := NewBuilder().WithLogger(nil)
		assert.NotNil(t, builder)
	})

	t.Run("logging during Build and Open", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logger := NewSlogAdapter(slogLogger)

		// Create a temp CSV file
		tempDir := t.TempDir()
		csvFile := filepath.Join(tempDir, "test.csv")
		require.NoError(t, os.WriteFile(csvFile, []byte("a,b\n1,2\n3,4"), 0600))

		ctx := context.Background()
		builder := NewBuilder().
			WithLogger(logger).
			AddPath(csvFile)

		validatedBuilder, err := builder.Build(ctx)
		require.NoError(t, err)

		db, err := validatedBuilder.Open(ctx)
		require.NoError(t, err)
		defer db.Close()

		output := buf.String()
		// Check that logging occurred
		assert.Contains(t, output, "starting build")
		assert.Contains(t, output, "build completed")
		assert.Contains(t, output, "opening database")
		assert.Contains(t, output, "database opened successfully")
	})

	t.Run("logging with reader input", func(t *testing.T) {
		t.Parallel()
		buf := &bytes.Buffer{}
		slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logger := NewSlogAdapter(slogLogger)

		ctx := context.Background()
		builder := NewBuilder().
			WithLogger(logger).
			AddReader(strings.NewReader("a,b\n1,2"), "test_table", FileTypeCSV)

		validatedBuilder, err := builder.Build(ctx)
		require.NoError(t, err)

		db, err := validatedBuilder.Open(ctx)
		require.NoError(t, err)
		defer db.Close()

		output := buf.String()
		assert.Contains(t, output, "starting reader streaming")
		assert.Contains(t, output, "test_table")
	})
}

func TestStreamProcessorLogger(t *testing.T) {
	t.Parallel()

	t.Run("setLogger updates logger", func(t *testing.T) {
		t.Parallel()
		sp := newStreamProcessor(1000)

		buf := &bytes.Buffer{}
		slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logger := NewSlogAdapter(slogLogger)

		sp.setLogger(logger)
		sp.logger.Info("test message")

		assert.Contains(t, buf.String(), "test message")
	})

	t.Run("setLogger with nil does not change logger", func(t *testing.T) {
		t.Parallel()
		sp := newStreamProcessor(1000)
		originalLogger := sp.logger

		sp.setLogger(nil)
		assert.Equal(t, originalLogger, sp.logger)
	})
}

func BenchmarkNopLogger(b *testing.B) {
	logger := newNopLogger()
	b.ResetTimer()
	for range b.N {
		logger.Debug("debug message", "key1", "value1", "key2", 123)
		logger.Info("info message", "key1", "value1", "key2", 123)
	}
}

func BenchmarkSlogAdapter(b *testing.B) {
	buf := &bytes.Buffer{}
	slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger := NewSlogAdapter(slogLogger)
	b.ResetTimer()
	for range b.N {
		logger.Debug("debug message", "key1", "value1", "key2", 123)
		logger.Info("info message", "key1", "value1", "key2", 123)
	}
}

func BenchmarkSlogAdapterDiscardLevel(b *testing.B) {
	buf := &bytes.Buffer{}
	// Set level to Info, so Debug messages are discarded
	slogLogger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger := NewSlogAdapter(slogLogger)
	b.ResetTimer()
	for range b.N {
		logger.Debug("debug message", "key1", "value1", "key2", 123)
		logger.Info("info message", "key1", "value1", "key2", 123)
	}
}
