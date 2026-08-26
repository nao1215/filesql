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

// debugLogger is a logger that keeps every record, including debug ones, in buf.
// It is what a test reads the package's own reporting out of.
func debugLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestNopLogger(t *testing.T) {
	t.Parallel()

	t.Run("it writes nothing and never panics", func(t *testing.T) {
		t.Parallel()

		logger := newNopLogger()
		require.NotNil(t, logger)

		assert.NotPanics(t, func() {
			logger.Debug("debug message", "key", "value")
			logger.Info("info message", "key", "value")
			logger.Warn("warn message", "key", "value")
			logger.Error("error message", "key", "value")
			logger.With("key", "value").Info("with message")
		})
	})

	t.Run("it reports every level as disabled", func(t *testing.T) {
		t.Parallel()

		// A discarding handler that answered Enabled with true would still make
		// every call build its record, which is the cost the default is meant to
		// avoid.
		logger := newNopLogger()
		for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
			assert.False(t, logger.Enabled(context.Background(), level), "level %v", level)
		}
	})
}

func TestDBBuilderWithLogger(t *testing.T) {
	t.Parallel()

	t.Run("a nil logger leaves the current one in place", func(t *testing.T) {
		t.Parallel()

		builder := NewBuilder()
		original := builder.logger

		assert.Same(t, original, builder.WithLogger(nil).logger)
		assert.NotPanics(t, func() { builder.logger.Info("still usable") })
	})

	t.Run("the builder reports what it loaded from a path", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		dir := t.TempDir()
		csvFile := filepath.Join(dir, "test.csv")
		require.NoError(t, os.WriteFile(csvFile, []byte("a,b\n1,2\n3,4"), 0o600))

		ctx := context.Background()
		validated, err := NewBuilder().
			WithLogger(debugLogger(buf)).
			AddPath(csvFile).
			Build(ctx)
		require.NoError(t, err)

		db, err := validated.Open(ctx)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		output := buf.String()
		assert.Contains(t, output, "starting build")
		assert.Contains(t, output, "build completed")
		assert.Contains(t, output, "opening database")
		assert.Contains(t, output, "database opened successfully")
	})

	t.Run("the builder reports what it loaded from a reader", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		ctx := context.Background()
		validated, err := NewBuilder().
			WithLogger(debugLogger(buf)).
			AddReader(strings.NewReader("a,b\n1,2"), "test_table", FileTypeCSV).
			Build(ctx)
		require.NoError(t, err)

		db, err := validated.Open(ctx)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		output := buf.String()
		assert.Contains(t, output, "starting reader streaming")
		assert.Contains(t, output, "test_table")
	})

	t.Run("the handler decides which levels are kept", func(t *testing.T) {
		t.Parallel()

		// Levels are the handler's business rather than this package's, so a
		// logger that keeps only warnings has to drop the debug and info
		// records the load emits.
		buf := &bytes.Buffer{}
		logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

		ctx := context.Background()
		validated, err := NewBuilder().
			WithLogger(logger).
			AddReader(strings.NewReader("a,b\n1,2"), "quiet", FileTypeCSV).
			Build(ctx)
		require.NoError(t, err)

		db, err := validated.Open(ctx)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		assert.Empty(t, buf.String())
	})
}

func TestStreamProcessorLogger(t *testing.T) {
	t.Parallel()

	t.Run("setLogger updates logger", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sp := newStreamProcessor(1000)
		sp.setLogger(debugLogger(buf))
		sp.logger.Info("test message")

		assert.Contains(t, buf.String(), "test message")
	})

	t.Run("setLogger with nil does not change logger", func(t *testing.T) {
		t.Parallel()

		sp := newStreamProcessor(1000)
		originalLogger := sp.logger

		sp.setLogger(nil)
		assert.Same(t, originalLogger, sp.logger)
	})
}

func TestFileProcessorLogger(t *testing.T) {
	t.Parallel()

	t.Run("setLogger updates logger", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		fp := newFileProcessor()
		fp.setLogger(debugLogger(buf))
		fp.logger.Info("test message")

		assert.Contains(t, buf.String(), "test message")
	})

	t.Run("setLogger with nil does not change logger", func(t *testing.T) {
		t.Parallel()

		fp := newFileProcessor()
		originalLogger := fp.logger

		fp.setLogger(nil)
		assert.Same(t, originalLogger, fp.logger)
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

func BenchmarkTextLogger(b *testing.B) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	b.ResetTimer()
	for range b.N {
		logger.Debug("debug message", "key1", "value1", "key2", 123)
		logger.Info("info message", "key1", "value1", "key2", 123)
	}
}

func BenchmarkTextLoggerDiscardLevel(b *testing.B) {
	buf := &bytes.Buffer{}
	// Level Info, so the debug message is dropped by the handler.
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	b.ResetTimer()
	for range b.N {
		logger.Debug("debug message", "key1", "value1", "key2", 123)
		logger.Info("info message", "key1", "value1", "key2", 123)
	}
}
