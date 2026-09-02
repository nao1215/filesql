package filesql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqlite3 "modernc.org/sqlite/lib"
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

	db, err := Open(ctx, filepath.Join("testdata", "test.csv"))
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

	db, err := Open(ctx, filepath.Join("testdata", "test.csv"))
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

// TestAutoSaveConcurrentQueries verifies that a database opened with auto-save
// is as safe to share across goroutines as one opened without it, and that the
// save still runs once when it is closed.
//
// It was not: the auto-save connector handed every pooled connection a wrapper
// around one shared driver.Conn, so concurrent queries reached the same SQLite
// connection at the same time. Run with -race to detect regressions.
func TestAutoSaveConcurrentQueries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	path := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(path, []byte("id,name\n1,alice\n"), 0o600))

	validated, err := buildForTest(ctx, NewBuilder().AddPath(path).EnableAutoSave(""))
	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)

	const goroutines = 8
	const iterations = 20

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iterations)
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				var count int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
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

	_, err = db.ExecContext(ctx, "UPDATE users SET name = 'bob'")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	require.Equal(t, "id,name\n1,bob\n", string(saved))
}

// TestAutoSaveOnCommitConcurrentCommits verifies that committing from several
// goroutines saves once per commit and returns. Every commit writes the whole
// database out through the connector's own connection, so the saves have to be
// serialized: two of them running at once drove statements on one SQLite
// connection with nothing between them, which hung and sometimes faulted.
func TestAutoSaveOnCommitConcurrentCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	path := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(path, []byte("id,name\n1,alice\n"), 0o600))

	outputDir := t.TempDir()
	validated, err := buildForTest(ctx, NewBuilder().AddPath(path).EnableAutoSaveOnCommit(outputDir))
	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const goroutines = 4
	const iterations = 25

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iterations)
	for g := range goroutines {
		wg.Go(func() {
			for i := range iterations {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					errCh <- err
					return
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO users VALUES (?, ?)", g*iterations+i+2, "bob"); err != nil {
					errCh <- err
					errCh <- tx.Rollback()
					return
				}
				if err := tx.Commit(); err != nil {
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

	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count))
	require.Equal(t, 1+goroutines*iterations, count)

	saved, err := os.ReadFile(filepath.Join(outputDir, "users.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	require.Equal(t, 1+goroutines*iterations+1, len(strings.Split(strings.TrimRight(string(saved), "\n"), "\n")))
}

// TestAutoSaveOnCommitConcurrentQueriesAndCommits mixes reads with committing
// writers, which is the shape that faulted rather than hung.
func TestAutoSaveOnCommitConcurrentQueriesAndCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	path := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(path, []byte("id,name\n1,alice\n"), 0o600))

	validated, err := buildForTest(ctx, NewBuilder().AddPath(path).EnableAutoSaveOnCommit(t.TempDir()))
	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const goroutines = 8
	const iterations = 10

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iterations)
	for g := range goroutines {
		wg.Go(func() {
			for i := range iterations {
				if g%2 == 0 {
					var count int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
						errCh <- err
						return
					}
					continue
				}
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					errCh <- err
					return
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO users VALUES (?, ?)", g*iterations+i+2, "bob"); err != nil {
					errCh <- err
					errCh <- tx.Rollback()
					return
				}
				if err := tx.Commit(); err != nil {
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

// TestOpenConcurrentWrites verifies that the database is as safe to write to
// from several goroutines as it is to read from. README promises the *sql.DB
// itself, not a read-only view of it, and a shared-cache SQLite database
// serializes writers with a lock a second writer can be refused by, so the
// promise is only worth what a concurrent INSERT does. Run with -race.
func TestOpenConcurrentWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	path := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(path, []byte("id,name\n1,alice\n"), 0o600))

	db, err := Open(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	const goroutines = 8
	const iterations = 25

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iterations)
	for g := range goroutines {
		wg.Go(func() {
			for i := range iterations {
				if _, err := db.ExecContext(ctx, "INSERT INTO users VALUES (?, ?)", g*iterations+i+2, "bob"); err != nil {
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

	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count))
	require.Equal(t, 1+goroutines*iterations, count)
}

// TestAutoSaveSavesOnceUnderConcurrentClose verifies the other half of the
// auto-save promise: the save runs when Close returns, and it runs once. The
// connector takes its anchor connection and its armed flag away under a mutex
// before saving, so a second Close finds nothing to save; nothing pinned that,
// and a save that ran twice would write the destination through the staged
// file a second time while the first was still there. Run with -race.
func TestAutoSaveSavesOnceUnderConcurrentClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	path := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(path, []byte("id,name\n1,alice\n"), 0o600))

	validated, err := buildForTest(ctx, NewBuilder().AddPath(path).EnableAutoSave(""))
	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "UPDATE users SET name = 'bob'")
	require.NoError(t, err)

	const closers = 8

	var wg sync.WaitGroup
	errCh := make(chan error, closers)
	for range closers {
		wg.Go(func() { errCh <- db.Close() })
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	require.Equal(t, "id,name\n1,bob\n", string(saved))

	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 1, "the save left a staged file behind: %v", entries)
}

// TestConcurrentLoadInto verifies that loading into one database from several
// goroutines lands every table.
//
// It did not: creating a table takes a lock on the schema, and SQLite refuses a
// second writer rather than queueing it, so a load that met another load's lock
// came back with `database schema is locked` on a shared-cache database or
// `database is locked` on a file, its table not created. Sixteen concurrent
// loads into a file database left the database empty and sixteen errors behind.
// The load now waits the lock out. Run with -race.
func TestConcurrentLoadInto(t *testing.T) {
	t.Parallel()

	source := func(t *testing.T, n int) []string {
		t.Helper()
		dir := t.TempDir()
		paths := make([]string, n)
		for i := range n {
			paths[i] = filepath.Join(dir, fmt.Sprintf("t%d.csv", i))
			require.NoError(t, os.WriteFile(paths[i], []byte("id,name\n1,alice\n"), 0o600))
		}
		return paths
	}

	// The three shapes a caller's database can have, plus this package's own.
	// They fail differently: a file database answers SQLITE_BUSY, a shared-cache
	// one answers SQLITE_LOCKED, and a pinned ":memory:" pool cannot collide at
	// all because every load goes through the one connection.
	for _, tc := range []struct {
		name string
		open func(t *testing.T) *sql.DB
	}{
		{
			name: "a pinned in-memory pool",
			open: func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := sql.Open("sqlite", ":memory:")
				require.NoError(t, err)
				db.SetMaxOpenConns(1)
				return db
			},
		},
		{
			name: "a file database",
			open: func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "load.db"))
				require.NoError(t, err)
				return db
			},
		},
		{
			name: "a database this package opened",
			open: func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := Open(t.Context(), source(t, 1)[0])
				require.NoError(t, err)
				return db
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const goroutines = 8

			db := tc.open(t)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			paths := source(t, goroutines)

			var wg sync.WaitGroup
			errs := make([]error, goroutines)
			for i := range goroutines {
				wg.Go(func() { errs[i] = LoadInto(t.Context(), db, paths[i]) })
			}
			wg.Wait()

			for i, err := range errs {
				require.NoErrorf(t, err, "load %d", i)
			}
			for i := range goroutines {
				var count int
				require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM t%d", i)).Scan(&count))
				assert.Equal(t, 1, count, "table t%d", i)
			}
		})
	}
}

// TestConcurrentLoadIntoFromReaders is the same for inputs that cannot be read
// twice. A reader is spent by the attempt that reads it, so only the step
// before the reading is tried again -- which on a shared-cache database is
// where the lock is taken, since that is where the schema lock lands.
func TestConcurrentLoadIntoFromReaders(t *testing.T) {
	t.Parallel()

	seed := filepath.Join(t.TempDir(), "seed.csv")
	require.NoError(t, os.WriteFile(seed, []byte("id,name\n1,alice\n"), 0o600))
	db, err := Open(t.Context(), seed)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	const goroutines = 8

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Go(func() {
			builder, buildErr := buildForTest(

				t.Context(), NewBuilder().
					AddReader(strings.NewReader("id,name\n1,alice\n"), fmt.Sprintf("r%d", i), FileTypeCSV))

			if buildErr != nil {
				errs[i] = buildErr
				return
			}
			errs[i] = builder.LoadInto(t.Context(), db)
		})
	}
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "load %d", i)
	}
	for i := range goroutines {
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM r%d", i)).Scan(&count))
		assert.Equal(t, 1, count, "table r%d", i)
	}
}

// lockedStep answers with the error a write meets while another connection
// holds the database. The error has to be the driver's own, since that is what
// the retry reads to tell "someone else holds it" from "the input is wrong",
// and the driver does not export a way to build one.
func lockedStep(t *testing.T) func() error {
	t.Helper()

	path := filepath.Join(t.TempDir(), "locked.db")
	holder, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	_, err = holder.ExecContext(t.Context(), "CREATE TABLE keep(x)")
	require.NoError(t, err)
	tx, err := holder.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "INSERT INTO keep VALUES (1)")
	require.NoError(t, err)
	// The transaction is bound to the test's context, which is canceled before
	// the cleanup runs, so database/sql may have rolled it back already.
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back the holding transaction: %v", err)
		}
	})

	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// The step runs under a context of its own so that a caller's canceled one
	// turns the answer into a context error rather than the lock being tested.
	return func() error {
		_, execErr := db.ExecContext(context.Background(), "CREATE TABLE mine(x)")
		require.Error(t, execErr)
		return execErr
	}
}

// TestLoadRetryBudgetIsSpentOnWaiting pins what the five seconds bound. They
// bound how long a load waits for another load to let go, not how long its
// attempts take: an attempt reads the input and infers its column types before
// it reaches the statement that needs the write lock, so a wall-clock budget was
// consumed by work that was then thrown away and the load failed with the
// database's own error while it had barely waited at all. Thirty-two goroutines
// loading a 200000-row CSV each lost twelve of the thirty-two that way.
func TestLoadRetryBudgetIsSpentOnWaiting(t *testing.T) {
	t.Parallel()

	t.Run("an attempt that takes longer than the budget does not use it up", func(t *testing.T) {
		t.Parallel()

		locked := lockedStep(t)
		attempts := 0
		err := retryWhileLockedFor(t.Context(), 30*time.Millisecond, time.Minute, func() error {
			attempts++
			// Longer than the whole budget, the way reading a large input is.
			time.Sleep(20 * time.Millisecond)
			if attempts < 4 {
				return locked()
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 4, attempts, "the time an attempt takes is not time spent waiting")
	})

	t.Run("the waiting itself is still bounded", func(t *testing.T) {
		t.Parallel()

		locked := lockedStep(t)
		attempts := 0
		start := time.Now()
		err := retryWhileLockedFor(t.Context(), 30*time.Millisecond, time.Minute, func() error {
			attempts++
			return locked()
		})
		elapsed := time.Since(start)

		require.Error(t, err)
		assert.True(t, lockedByAnotherConnection(err), "the database's own answer comes back once the wait is over")
		assert.Less(t, elapsed, time.Second, "a lock nobody lets go of is waited on for the budget, not forever")
		assert.Greater(t, attempts, 1)
	})

	t.Run("retrying something expensive is bounded by the clock too", func(t *testing.T) {
		t.Parallel()

		locked := lockedStep(t)
		attempts := 0
		start := time.Now()
		// Waiting is cheap here and the attempt is not, which is the workbook's
		// shape: without a bound of its own the retry would keep re-parsing.
		//
		// The budget is twelve times one attempt rather than three. At three
		// this test failed under -race on a loaded machine: one 20ms sleep can
		// take longer than a 60ms budget when every core is busy, and then only
		// one attempt fits and the assertion below has nothing to stand on. The
		// ratio is what the test is about, so widening it costs a fifth of a
		// second and takes the wall clock out of the result.
		err := retryWhileLockedFor(t.Context(), time.Minute, 240*time.Millisecond, func() error {
			attempts++
			time.Sleep(20 * time.Millisecond)
			return locked()
		})
		elapsed := time.Since(start)

		require.Error(t, err)
		assert.True(t, lockedByAnotherConnection(err))
		assert.Less(t, elapsed, 2*time.Second, "the attempts have to stop even while there is waiting left")
		assert.Greater(t, attempts, 1)
	})

	t.Run("a failure that is not a lock is not waited on", func(t *testing.T) {
		t.Parallel()

		attempts := 0
		err := retryWhileLockedFor(t.Context(), time.Minute, time.Minute, func() error {
			attempts++
			return errors.New("disk full")
		})
		require.Error(t, err)
		assert.Equal(t, 1, attempts)
	})
}

// TestLoadRetryReportsWhyItStopped covers the load that ends because its
// context ended. It reported the lock it was waiting for and nothing else, so
// errors.Is(err, context.DeadlineExceeded) was false for an operation that
// ended for exactly that reason and a caller could not tell a deadline of their
// own from a bad input.
func TestLoadRetryReportsWhyItStopped(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		with func(context.Context) (context.Context, context.CancelFunc)
		want error
	}{
		{
			name: "canceled",
			with: func(ctx context.Context) (context.Context, context.CancelFunc) {
				return context.WithCancel(ctx)
			},
			want: context.Canceled,
		},
		{
			name: "deadline exceeded",
			with: func(ctx context.Context) (context.Context, context.CancelFunc) {
				return context.WithTimeout(ctx, 20*time.Millisecond)
			},
			want: context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name+" is what the load reports", func(t *testing.T) {
			t.Parallel()

			ctx, cancel := tc.with(t.Context())
			defer cancel()
			if errors.Is(tc.want, context.Canceled) {
				time.AfterFunc(20*time.Millisecond, cancel)
			}

			locked := lockedStep(t)
			err := retryWhileLockedFor(ctx, time.Minute, time.Minute, locked)
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.want, "the reason the wait stopped has to be reachable")
			assert.True(t, lockedByAnotherConnection(err), "what it was waiting for is still worth keeping")
		})
	}

	t.Run("a context already done still gets one attempt", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		attempts := 0
		err := retryWhileLockedFor(ctx, time.Minute, time.Minute, func() error {
			attempts++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, attempts, "the operation answers for itself before the context does")
	})
}

// TestLoadRetryOnlyWaitsForLocks keeps the waiting from spreading: only the two
// answers SQLite gives when someone else holds what a load needs are worth
// waiting on, and everything else has to come back at once. A bad input that
// became a slow bad input would be a worse trade than the one this makes.
func TestLoadRetryOnlyWaitsForLocks(t *testing.T) {
	t.Parallel()

	t.Run("the codes that mean someone else holds it", func(t *testing.T) {
		t.Parallel()

		// The extended codes are the ones a caller actually meets: 262 is what a
		// shared-cache table lock answers, and 517 is what a write into a stale
		// snapshot answers. Both carry their primary code in the low byte.
		for _, code := range []int{
			sqlite3.SQLITE_BUSY,
			sqlite3.SQLITE_LOCKED,
			sqlite3.SQLITE_LOCKED_SHAREDCACHE,
			sqlite3.SQLITE_BUSY_SNAPSHOT,
		} {
			assert.Truef(t, isLockCode(code), "code %d", code)
		}
	})

	t.Run("the codes that mean the input is wrong", func(t *testing.T) {
		t.Parallel()

		for _, code := range []int{
			sqlite3.SQLITE_OK,
			sqlite3.SQLITE_ERROR,
			sqlite3.SQLITE_CONSTRAINT,
			sqlite3.SQLITE_FULL,
			sqlite3.SQLITE_READONLY,
		} {
			assert.Falsef(t, isLockCode(code), "code %d", code)
		}
	})

	t.Run("anything else comes back at once", func(t *testing.T) {
		t.Parallel()

		assert.False(t, lockedByAnotherConnection(nil))
		assert.False(t, lockedByAnotherConnection(errors.New("disk full")))
		assert.False(t, lockedByAnotherConnection(fmt.Errorf("%w: no such table", ErrDatabaseOperation)))
	})

	t.Run("a load that fails for its own reasons fails fast", func(t *testing.T) {
		t.Parallel()

		db, err := sql.Open("sqlite", ":memory:")
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, db.Close()) })

		path := filepath.Join(t.TempDir(), "dup.csv")
		require.NoError(t, os.WriteFile(path, []byte("id,id\n1,2\n"), 0o600))

		start := time.Now()
		loadErr := LoadInto(t.Context(), db, path)
		elapsed := time.Since(start)

		require.Error(t, loadErr)
		assert.Less(t, elapsed, time.Second, "a duplicate column was waited on as though it were a lock")
	})
}
