package filesql

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestOpenConcurrentQueries verifies that a database returned by Open can be
// queried from multiple goroutines without data races or errors.
//
// The in-memory database uses a uniquely named shared-cache DSN, so pooled
// connections can access the same in-memory database without pinning the pool
// to one connection. Run with -race to detect regressions.
func TestOpenConcurrentQueries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

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
// to a single connection. A timeout context makes a deadlock regression fail
// fast instead of hanging until the global test timeout. Run with -race to
// detect data races.
func TestOpenConcurrentNestedQueries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	db, err := OpenContext(ctx, filepath.Join("testdata", "test.csv"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

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
