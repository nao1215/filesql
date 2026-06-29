package filesql

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOpenConcurrentQueries verifies that a database returned by Open can be
// queried from multiple goroutines without data races or errors.
//
// The in-memory database is backed by a single SQLite connection that is reused
// for every pooled connection, so the pool must be pinned to one connection
// (SetMaxOpenConns(1)) to serialize access. Run with -race to detect regressions.
func TestOpenConcurrentQueries(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const goroutines = 16
	const iterations = 50

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iterations)
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				var count int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count); err != nil {
					errCh <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
}

// TestOpenConcurrentNestedQueries verifies that issuing a query while iterating
// an open *sql.Rows works from multiple goroutines. This needs more than one
// real connection per goroutine, so it would deadlock if the pool were pinned
// to a single connection. Run with -race to detect data races.
func TestOpenConcurrentNestedQueries(t *testing.T) {
	ctx := context.Background()

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const goroutines = 8

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for range goroutines {
		wg.Go(func() {
			rows, err := db.QueryContext(ctx, "SELECT name FROM test")
			if err != nil {
				errCh <- err
				return
			}
			defer rows.Close()
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					errCh <- err
					return
				}
				// Nested query while the outer rows are still open.
				var count int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test WHERE name = ?", name).Scan(&count); err != nil {
					errCh <- err
					return
				}
			}
			if err := rows.Err(); err != nil {
				errCh <- err
			}
		})
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
}
