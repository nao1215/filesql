package filesql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nao1215/filesql/dialect"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errStub is the failure a stub connection reports when a test asks it to fail.
var errStub = errors.New("stub failure")

// plainConn implements only what driver.Conn requires. A driver this small is
// what the fallbacks in the auto-save wrapper exist for: the wrapper cannot
// assume the connection it wraps implements the context-aware interfaces.
type plainConn struct {
	closeErr error
	beginErr error
	begun    bool
}

func (c *plainConn) Prepare(string) (driver.Stmt, error) { return nil, errStub }
func (c *plainConn) Close() error                        { return c.closeErr }

func (c *plainConn) Begin() (driver.Tx, error) {
	if c.beginErr != nil {
		return nil, c.beginErr
	}
	c.begun = true
	return stubTx{}, nil
}

// legacyConn adds the pre-context Execer and Queryer interfaces, which is the
// other shape the wrapper has to handle.
type legacyConn struct {
	plainConn
	execCalled  bool
	queryCalled bool
	lastArgs    []driver.Value
}

func (c *legacyConn) Exec(_ string, args []driver.Value) (driver.Result, error) {
	c.execCalled = true
	c.lastArgs = args
	return driver.RowsAffected(1), nil
}

func (c *legacyConn) Query(_ string, args []driver.Value) (driver.Rows, error) {
	c.queryCalled = true
	c.lastArgs = args
	return stubRows{}, nil
}

// stubTx is a transaction that accepts both outcomes.
type stubTx struct{}

func (stubTx) Commit() error   { return nil }
func (stubTx) Rollback() error { return nil }

// failingCommitTx is a transaction whose commit fails, which is the outcome
// that leaves database/sql calling neither Commit again nor Rollback.
type failingCommitTx struct{ stubTx }

func (failingCommitTx) Commit() error { return errStub }

// stubRows is an empty result set.
type stubRows struct{}

func (stubRows) Columns() []string          { return nil }
func (stubRows) Close() error               { return nil }
func (stubRows) Next([]driver.Value) error  { return errStub }
func (stubRows) ColumnTypeScanType(int) any { return nil }
func (stubRows) ColumnTypeDatabaseTypeName(int) string {
	return ""
}

// TestGuardedConn_BeginTxFallsBackToBegin covers a wrapped driver that
// predates ConnBeginTx. Without the fallback such a driver could not start a
// transaction at all once auto-save wrapped it.
func TestGuardedConn_BeginTxFallsBackToBegin(t *testing.T) {
	t.Parallel()

	t.Run("the legacy Begin is used", func(t *testing.T) {
		t.Parallel()

		inner := &plainConn{}
		conn := &guardedConn{conn: inner}

		tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
		require.NoError(t, err)
		assert.IsType(t, &guardedTx{}, tx, "the transaction stays wrapped so a commit can still auto-save")
		assert.True(t, inner.begun, "the legacy Begin is what starts the transaction")
	})

	t.Run("a refused Begin is reported", func(t *testing.T) {
		t.Parallel()

		conn := &guardedConn{conn: &plainConn{beginErr: errStub}}

		_, err := conn.BeginTx(context.Background(), driver.TxOptions{})
		assert.ErrorIs(t, err, errStub)
	})

	t.Run("the deprecated Begin goes through BeginTx", func(t *testing.T) {
		t.Parallel()

		inner := &plainConn{}
		conn := &guardedConn{conn: inner}

		tx, err := conn.Begin()
		require.NoError(t, err)
		assert.IsType(t, &guardedTx{}, tx)
		assert.True(t, inner.begun)
	})
}

// TestGuardedConn_LegacyExecAndQuery covers the pre-context statement
// interfaces. A driver that implements only those still has to be usable, and
// the named arguments it cannot take have to be converted rather than dropped.
func TestGuardedConn_LegacyExecAndQuery(t *testing.T) {
	t.Parallel()

	t.Run("exec", func(t *testing.T) {
		t.Parallel()

		inner := &legacyConn{}
		conn := &guardedConn{conn: inner}

		_, err := conn.ExecContext(context.Background(), "UPDATE t SET a = ?", []driver.NamedValue{{Ordinal: 1, Value: int64(7)}})
		require.NoError(t, err)
		assert.True(t, inner.execCalled)
		assert.Equal(t, []driver.Value{int64(7)}, inner.lastArgs, "the named values must reach the legacy driver as plain ones")
	})

	t.Run("query", func(t *testing.T) {
		t.Parallel()

		inner := &legacyConn{}
		conn := &guardedConn{conn: inner}

		_, err := conn.QueryContext(context.Background(), "SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: "x"}})
		require.NoError(t, err)
		assert.True(t, inner.queryCalled)
		assert.Equal(t, []driver.Value{"x"}, inner.lastArgs)
	})

	t.Run("a connection with neither interface asks database/sql to take over", func(t *testing.T) {
		t.Parallel()

		conn := &guardedConn{conn: &plainConn{}}

		_, err := conn.ExecContext(context.Background(), "UPDATE t SET a = 1", nil)
		assert.ErrorIs(t, err, driver.ErrSkip, "database/sql falls back to Prepare when the driver skips")

		_, err = conn.QueryContext(context.Background(), "SELECT 1", nil)
		assert.ErrorIs(t, err, driver.ErrSkip)
	})
}

// TestGuardedConn_Prepare checks that preparing is handed straight to the
// wrapped connection.
func TestGuardedConn_Prepare(t *testing.T) {
	t.Parallel()

	conn := &guardedConn{conn: &plainConn{}}

	_, err := conn.Prepare("SELECT 1")
	assert.ErrorIs(t, err, errStub)
}

// TestGuardedTx_CommitReportsAFailedSave covers a commit that
// succeeded followed by a save that did not. The rows are already committed, so
// the caller has to be told that only the file is out of date.
func TestGuardedTx_CommitReportsAFailedSave(t *testing.T) {
	t.Parallel()

	tx := &guardedTx{
		tx: stubTx{},
		conn: &guardedConn{
			conn: &plainConn{},
			tracker: &autoSaveConnector{
				autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnCommit},
				anchor:         &plainConn{},
			},
		},
	}

	err := tx.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction committed successfully")
}

// TestGuardedConn_StatementCommitReportsAFailedSave covers the same failure on
// the other spelling. A commit-time save that failed after a COMMIT run as a
// statement leaves the rows in the database and the file behind them, which is
// only something the caller can act on if the statement says so.
func TestGuardedConn_StatementCommitReportsAFailedSave(t *testing.T) {
	t.Parallel()

	conn := &guardedConn{
		conn: &plainConn{},
		tracker: &autoSaveConnector{
			autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnCommit},
			anchor:         &plainConn{},
		},
	}
	require.NoError(t, conn.openTx(context.Background()))

	_, err := conn.runExec(context.Background(), "COMMIT", func(context.Context) (driver.Result, error) {
		return driver.RowsAffected(0), nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction committed successfully")
	assert.False(t, conn.inTx, "the transaction is over whether or not the save worked")
}

// TestGuardedTx_CommitThatFailedStopsCountingAsOpen covers the
// transaction a caller has no way to finish. database/sql does not call
// Rollback after a Commit that returned an error, and the driver has already
// rolled the connection back, so a transaction left in the connector's count
// there would make every later close refuse a save that had nothing to wait
// for.
func TestGuardedTx_CommitThatFailedStopsCountingAsOpen(t *testing.T) {
	t.Parallel()

	connector := &autoSaveConnector{
		autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnClose},
	}
	conn := &guardedConn{conn: &plainConn{}, tracker: connector}
	require.NoError(t, conn.openTx(context.Background()))
	tx := &guardedTx{tx: failingCommitTx{}, conn: conn}

	require.ErrorIs(t, tx.Commit(), errStub)

	connector.mu.Lock()
	open := connector.openTx
	connector.mu.Unlock()
	assert.Zero(t, open, "a transaction that cannot be committed is still over")
}

// TestGuardedTx_RollbackNeverSaves checks that a rollback reaches the
// wrapped transaction and does not run the save a commit would.
func TestGuardedTx_RollbackNeverSaves(t *testing.T) {
	t.Parallel()

	tx := &guardedTx{
		tx: stubTx{},
		conn: &guardedConn{
			conn: &plainConn{},
			tracker: &autoSaveConnector{
				autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnCommit},
				anchor:         &plainConn{},
			},
		},
	}

	assert.NoError(t, tx.Rollback())
}

// TestGuardedConn_TransactionOptions covers the sql.TxOptions a caller passes.
// SQLite has no read-only transaction and no isolation level other than
// serializable, and the driver took a request for either without saying it
// could not give it: a read-only transaction went on to accept writes, and a
// level SQLite does not implement was silently downgraded.
func TestGuardedConn_TransactionOptions(t *testing.T) {
	t.Parallel()

	open := map[string]func(ctx context.Context, path string) (*sql.DB, error){
		"plain": func(ctx context.Context, path string) (*sql.DB, error) {
			return NewBuilder().AddPath(path).Open(ctx)
		},
		"auto-save": func(ctx context.Context, path string) (*sql.DB, error) {
			return NewBuilder().AddPath(path).EnableAutoSave("").Open(ctx)
		},
		"dialect": func(ctx context.Context, path string) (*sql.DB, error) {
			return NewBuilder().AddPath(path).WithDialect(dialect.MySQL).Open(ctx)
		},
		"read-only": func(ctx context.Context, path string) (*sql.DB, error) {
			return NewBuilder().AddPath(path).OpenReadOnly(ctx)
		},
	}

	setup := func(t *testing.T, name string) (*sql.DB, string) {
		t.Helper()

		dir := t.TempDir()
		src := filepath.Join(dir, "users.csv")
		require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))
		db, err := open[name](t.Context(), src)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db, src
	}

	for _, name := range []string{"plain", "auto-save", "dialect", "read-only"} {
		t.Run("a read-only transaction reads and refuses to write ("+name+")", func(t *testing.T) {
			t.Parallel()

			db, src := setup(t, name)
			tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
			require.NoError(t, err)

			var n int
			require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&n))
			assert.Equal(t, 1, n, "a read-only transaction still reads")

			_, err = tx.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
			require.Error(t, err, "a transaction the caller declared read-only must refuse a write")
			require.NoError(t, tx.Rollback())

			got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
			require.NoError(t, readErr)
			assert.Equal(t, "id,name\n1,alice\n", string(got))
		})

		t.Run("only the levels SQLite gives are taken ("+name+")", func(t *testing.T) {
			t.Parallel()

			db, _ := setup(t, name)
			for _, tt := range []struct {
				level sql.IsolationLevel
				ok    bool
			}{
				{level: sql.LevelDefault, ok: true},
				{level: sql.LevelSerializable, ok: true},
				{level: sql.LevelReadUncommitted},
				{level: sql.LevelReadCommitted},
				{level: sql.LevelWriteCommitted},
				{level: sql.LevelRepeatableRead},
				{level: sql.LevelSnapshot},
				{level: sql.LevelLinearizable},
			} {
				tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: tt.level})
				if tt.ok {
					require.NoError(t, err, tt.level.String())
					require.NoError(t, tx.Rollback())
					continue
				}
				require.Error(t, err, tt.level.String())
				assert.ErrorIs(t, err, ErrDatabaseOperation)
				assert.Contains(t, err.Error(), tt.level.String(), "the refusal has to name the level it could not give")
			}
		})
	}

	t.Run("a writable handle takes writes again once the read-only transaction ends", func(t *testing.T) {
		t.Parallel()

		// The pragma is per connection and the connection goes back to the
		// pool, so a transaction that did not give the permission back would
		// leave the next caller of that connection unable to write.
		db, _ := setup(t, "plain")
		for range 4 {
			tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())

			_, err = db.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
			require.NoError(t, err)
			_, err = db.ExecContext(t.Context(), "DELETE FROM users WHERE id = 2")
			require.NoError(t, err)
		}
	})

	t.Run("a read-only handle stays read-only after one", func(t *testing.T) {
		t.Parallel()

		db, _ := setup(t, "read-only")
		tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		_, err = db.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
		assert.Error(t, err, "clearing the pragma at the end of the transaction must not unlock the handle")
	})
}

// TestReadTxStatement covers the reading that tells a statement which opens a
// transaction from one which closes it. ROLLBACK TO a savepoint is the case
// worth naming: it leaves the transaction around it open, so reading it as an
// end would drop the count while work is still uncommitted. A savepoint is read
// with the name it takes or releases, because whether a RELEASE ends the
// transaction depends on which savepoint opened it, which only the connection
// knows.
func TestReadTxStatement(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		query string
		want  txStatement
	}{
		{query: "BEGIN", want: txStatement{effect: txEffectBegin}},
		{query: "begin", want: txStatement{effect: txEffectBegin}},
		{query: "  \n\tBEGIN TRANSACTION", want: txStatement{effect: txEffectBegin}},
		{query: "BEGIN IMMEDIATE", want: txStatement{effect: txEffectBegin}},
		{query: "BEGIN DEFERRED", want: txStatement{effect: txEffectBegin}},
		{query: "BEGIN EXCLUSIVE", want: txStatement{effect: txEffectBegin}},
		{query: "COMMIT", want: txStatement{effect: txEffectCommit}},
		{query: "commit transaction", want: txStatement{effect: txEffectCommit}},
		{query: "END", want: txStatement{effect: txEffectCommit}},
		{query: "END TRANSACTION", want: txStatement{effect: txEffectCommit}},
		{query: "ROLLBACK", want: txStatement{effect: txEffectRollback}},
		{query: "rollback transaction", want: txStatement{effect: txEffectRollback}},
		{query: "ROLLBACK TO SAVEPOINT s", want: txStatement{}},
		{query: "rollback to s", want: txStatement{}},
		{query: "ROLLBACK TRANSACTION TO SAVEPOINT s", want: txStatement{}},
		{query: "rollback transaction to s", want: txStatement{}},
		{query: "SELECT 1", want: txStatement{}},
		{query: "SELECT 1 AS beginning", want: txStatement{}},
		{query: "UPDATE t SET commits = 1", want: txStatement{}},
		{query: "", want: txStatement{}},
		{query: "-- BEGIN", want: txStatement{}},
		{query: "CREATE TRIGGER t AFTER INSERT ON u BEGIN SELECT 1; END", want: txStatement{}},

		// A comment is not part of the statement, so what follows one is read
		// as though it were written alone. Everything a caller can put in front
		// of the keyword has to be skipped, including several comments in a row
		// and one that never ends.
		{query: "/* batch */ BEGIN", want: txStatement{effect: txEffectBegin}},
		{query: "-- batch\nBEGIN", want: txStatement{effect: txEffectBegin}},
		{query: "-- one\n /* two */\tCOMMIT", want: txStatement{effect: txEffectCommit}},
		{query: "/* a */-- b\n/* c */ROLLBACK", want: txStatement{effect: txEffectRollback}},
		{query: "/* c */ ROLLBACK TO s", want: txStatement{}},
		{query: "/* never closed BEGIN", want: txStatement{}},
		{query: "--", want: txStatement{}},
		{query: "/**/COMMIT", want: txStatement{effect: txEffectCommit}},
		{query: "SELECT '/* not a comment */ COMMIT'", want: txStatement{}},

		// A savepoint taken outside a transaction opens one and releasing it
		// closes one, so both are read, with the name that says which.
		{query: "SAVEPOINT batch", want: txStatement{effect: txEffectBegin, savepoint: "batch"}},
		{query: "savepoint BATCH", want: txStatement{effect: txEffectBegin, savepoint: "BATCH"}},
		{query: "SAVEPOINT", want: txStatement{}},
		{query: `SAVEPOINT "the batch"`, want: txStatement{effect: txEffectBegin, savepoint: "the batch"}},
		{query: "SAVEPOINT `the batch`", want: txStatement{effect: txEffectBegin, savepoint: "the batch"}},
		{query: `SAVEPOINT [the batch]`, want: txStatement{effect: txEffectBegin, savepoint: "the batch"}},
		{query: `SAVEPOINT "say ""hi"""`, want: txStatement{effect: txEffectBegin, savepoint: `say "hi"`}},
		{query: `SAVEPOINT "never closed`, want: txStatement{}},
		{query: "RELEASE batch", want: txStatement{effect: txEffectCommit, savepoint: "batch"}},
		{query: "RELEASE SAVEPOINT batch", want: txStatement{effect: txEffectCommit, savepoint: "batch"}},
		{query: "release savepoint BATCH", want: txStatement{effect: txEffectCommit, savepoint: "BATCH"}},
		// "savepoint" is a name a caller may have taken, so the keyword is only
		// dropped when another name follows it.
		{query: "RELEASE savepoint", want: txStatement{effect: txEffectCommit, savepoint: "savepoint"}},
		{query: "RELEASE", want: txStatement{}},
		{query: "SAVEPOINTS", want: txStatement{}},
		{query: "RELEASED batch", want: txStatement{}},
	} {
		assert.Equal(t, tt.want, readTxStatement(tt.query), tt.query)
	}
}

// TestGuardedConn_OptionalInterfaces covers what the wrapper forwards. A driver
// connection that implements none of the context-aware interfaces still has to
// work through it, and one this package could not put back the way it found it
// has to leave the pool rather than be handed to the next caller.
func TestGuardedConn_OptionalInterfaces(t *testing.T) {
	t.Parallel()

	t.Run("a connection with no context interfaces still prepares and pings", func(t *testing.T) {
		t.Parallel()

		conn := &guardedConn{conn: &plainConn{}}
		_, err := conn.PrepareContext(context.Background(), "SELECT 1")
		assert.ErrorIs(t, err, errStub, "preparing falls back to the interface the connection has")
		assert.NoError(t, conn.Ping(context.Background()), "a connection that cannot be pinged is taken as reachable")
		assert.NoError(t, conn.ResetSession(context.Background()))
		assert.True(t, conn.IsValid())
	})

	t.Run("a connection that could not be restored leaves the pool", func(t *testing.T) {
		t.Parallel()

		// plainConn has no Execer, so the pragma cannot be run on it, which is
		// the failure allowWrites is there for.
		conn := &guardedConn{conn: &plainConn{}}
		conn.allowWrites(context.Background())
		assert.False(t, conn.IsValid(), "a connection still refusing writes must not be reused")
	})
}

// TestCheckIsolation covers a level outside the ones database/sql names, which
// is what a caller reaches by converting an integer of their own.
func TestCheckIsolation(t *testing.T) {
	t.Parallel()

	require.NoError(t, checkIsolation(driver.IsolationLevel(sql.LevelDefault)))
	err := checkIsolation(driver.IsolationLevel(99))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
	assert.Contains(t, err.Error(), "99")
}

// TestTransactionsQueueForEachOther covers the wait a second transaction used to
// spend inside the driver. SQLite runs one write transaction at a time and the
// driver waits for its turn on a mutex of its own, with no context in the path,
// so a caller with a deadline could not fail the work and the goroutine stayed
// there until the first transaction ended.
func TestTransactionsQueueForEachOther(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *sql.DB {
		t.Helper()

		dir := t.TempDir()
		src := filepath.Join(dir, "users.csv")
		require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n2,bob\n"), 0o600))
		db, err := NewBuilder().AddPath(src).Open(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	t.Run("a second transaction ends at its deadline", func(t *testing.T) {
		t.Parallel()

		db := setup(t)
		held, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = held.ExecContext(t.Context(), "UPDATE users SET name='held' WHERE id=1")
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			_, err := db.BeginTx(ctx, nil)
			done <- err
		}()

		select {
		case err := <-done:
			require.Error(t, err, "the second transaction must not begin while the first is open")
			assert.ErrorIs(t, err, context.DeadlineExceeded, "the wait has to end at the caller's deadline")
		case <-time.After(30 * time.Second):
			t.Fatal("the second transaction never returned: its wait is outside the caller's reach")
		}
		require.NoError(t, held.Rollback())
	})

	t.Run("the second transaction runs once the first is over", func(t *testing.T) {
		t.Parallel()

		db := setup(t)
		first, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = first.ExecContext(t.Context(), "UPDATE users SET name='first' WHERE id=1")
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() {
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				done <- err
				return
			}
			if _, err := tx.ExecContext(t.Context(), "UPDATE users SET name='second' WHERE id=2"); err != nil {
				done <- errors.Join(err, tx.Rollback())
				return
			}
			done <- tx.Commit()
		}()

		require.NoError(t, first.Commit())
		select {
		case err := <-done:
			require.NoError(t, err, "the queued transaction has to run once the gate is free")
		case <-time.After(30 * time.Second):
			t.Fatal("the queued transaction never ran after the first committed")
		}

		var name string
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT name FROM users WHERE id=2").Scan(&name))
		assert.Equal(t, "second", name)
	})

	t.Run("a refused nested BEGIN keeps the first transaction queued", func(t *testing.T) {
		t.Parallel()

		// A BEGIN inside a transaction is a mistake SQLite refuses, and the
		// transaction underneath it is untouched. Reading the refusal as the
		// end of a transaction gave the gate away while the first was still
		// open, so a second transaction ran beside it.
		db := setup(t)
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		_, err = conn.ExecContext(t.Context(), "BEGIN")
		require.NoError(t, err)
		_, err = conn.ExecContext(t.Context(), "UPDATE users SET name='held' WHERE id=1")
		require.NoError(t, err)
		_, err = conn.ExecContext(t.Context(), "BEGIN")
		require.Error(t, err, "SQLite has no nested transaction")

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, err = db.BeginTx(ctx, nil)
		require.Error(t, err, "the first transaction is still open, so the second must wait for it")
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		_, err = conn.ExecContext(t.Context(), "ROLLBACK")
		require.NoError(t, err)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err, "the gate has to be free once the first transaction ends")
		require.NoError(t, tx.Rollback())
	})

	t.Run("a savepoint releases the gate only when the outermost one is released", func(t *testing.T) {
		t.Parallel()

		db := setup(t)
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		for _, stmt := range []string{"SAVEPOINT outer", "SAVEPOINT inner", "UPDATE users SET name='held' WHERE id=1", "RELEASE inner"} {
			_, err = conn.ExecContext(t.Context(), stmt)
			require.NoError(t, err, stmt)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, err = db.BeginTx(ctx, nil)
		require.Error(t, err, "releasing a nested savepoint leaves the transaction around it open")
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		_, err = conn.ExecContext(t.Context(), "RELEASE outer")
		require.NoError(t, err)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err, "releasing the savepoint that opened the transaction ends it")
		require.NoError(t, tx.Rollback())
	})

	t.Run("a reused savepoint name holds the gate until the outermost is released", func(t *testing.T) {
		t.Parallel()

		// SQLite lets one name be taken twice and releases the innermost, so
		// matching the name alone read the first RELEASE as the end of the
		// transaction and gave the gate away while it was still open.
		db := setup(t)
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		for _, stmt := range []string{"SAVEPOINT s", "SAVEPOINT s", "UPDATE users SET name='held' WHERE id=1", "RELEASE s"} {
			_, err = conn.ExecContext(t.Context(), stmt)
			require.NoError(t, err, stmt)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, err = db.BeginTx(ctx, nil)
		require.Error(t, err, "the outer savepoint still holds the transaction open")
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		_, err = conn.ExecContext(t.Context(), "RELEASE s")
		require.NoError(t, err)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err, "releasing the outermost savepoint ends the transaction")
		require.NoError(t, tx.Rollback())
	})

	t.Run("a COMMIT with nothing open leaves the next transaction alone", func(t *testing.T) {
		t.Parallel()

		db := setup(t)
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		_, err = conn.ExecContext(t.Context(), "COMMIT")
		require.Error(t, err, "there is no transaction to commit")

		_, err = conn.ExecContext(t.Context(), "BEGIN")
		require.NoError(t, err)
		_, err = conn.ExecContext(t.Context(), "COMMIT")
		require.NoError(t, err, "the refused COMMIT must not have left the count out of step")

		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err, "the gate has to be free")
		require.NoError(t, tx.Rollback())
	})

	t.Run("a statement beside an open transaction still runs", func(t *testing.T) {
		t.Parallel()

		// Only transactions queue. This package cannot tell a transaction that
		// has written from one that has only read, so making statements wait
		// would deadlock the ordinary shape of holding a transaction open and
		// querying the same database beside it.
		db := setup(t)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		done := make(chan error, 1)
		go func() {
			var n int
			done <- db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&n)
		}()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("a query beside an open transaction must not wait for it")
		}
	})
}

// TestGuardedConn_TransactionKeywordAsAQuery covers a transaction keyword sent
// through Query rather than Exec, which is legal and reaches a different path.
func TestGuardedConn_TransactionKeywordAsAQuery(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "users.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))
	db, err := NewBuilder().AddPath(src).EnableAutoSave("").Open(t.Context())
	require.NoError(t, err)

	rows, err := db.QueryContext(t.Context(), "BEGIN")
	require.NoError(t, err)
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	_, err = db.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
	require.NoError(t, err)

	err = db.Close()
	require.Error(t, err, "a BEGIN counts however it was sent")
	assert.ErrorIs(t, err, ErrDatabaseOperation)

	got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
	require.NoError(t, readErr)
	assert.Equal(t, "id,name\n1,alice\n", string(got))
}

// TestTxGate covers the gate on its own, including the states a database is
// awkward to drive into.
func TestTxGate(t *testing.T) {
	t.Parallel()

	t.Run("one holder at a time, and the next one after a release", func(t *testing.T) {
		t.Parallel()

		g := newTxGate()
		require.NoError(t, g.acquire(t.Context()))

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		assert.ErrorIs(t, g.acquire(ctx), context.DeadlineExceeded)

		g.release()
		require.NoError(t, g.acquire(t.Context()))
	})

	t.Run("a canceled context is refused before the wait", func(t *testing.T) {
		t.Parallel()

		g := newTxGate()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		assert.ErrorIs(t, g.acquire(ctx), context.Canceled)
	})

	t.Run("releasing a gate nobody holds is a no-op", func(t *testing.T) {
		t.Parallel()

		g := newTxGate()
		g.release()
		g.release()
		require.NoError(t, g.acquire(t.Context()))
	})
}

// TestReadTxStatementsReadsEveryStatement holds the reading of a query to what
// SQLite does with it. database/sql hands the whole string to the driver and
// SQLite runs every statement in it, so a reading that stopped at the first
// left a transaction opened by a later one invisible: an auto-save then ran
// over work the connection went on to discard, and Close answered nil.
func TestReadTxStatementsReadsEveryStatement(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		query string
		want  []txEffect
	}{
		{"one statement that opens", "BEGIN", []txEffect{txEffectBegin}},
		{"one that does nothing", "SELECT 1", nil},
		{"an opener that is not first", "SELECT 1; BEGIN", []txEffect{txEffectBegin}},
		{"an opener after a newline", "SELECT 1;\nBEGIN", []txEffect{txEffectBegin}},
		{"a pair that opens and closes", "BEGIN; COMMIT", []txEffect{txEffectBegin, txEffectCommit}},
		{"a savepoint that is not first", "SELECT 1; SAVEPOINT sp", []txEffect{txEffectBegin}},
		{"a savepoint and its release", "SAVEPOINT sp; RELEASE sp", []txEffect{txEffectBegin, txEffectCommit}},
		{"a rollback that is not first", "SELECT 1; ROLLBACK", []txEffect{txEffectRollback}},
		{"a trailing semicolon", "BEGIN;", []txEffect{txEffectBegin}},
		{"nothing but semicolons", ";;;", nil},
		// A semicolon inside a string, an identifier or a comment ends no
		// statement, so what follows it is not one.
		{"a semicolon inside a string", "SELECT ';BEGIN'", nil},
		{"a semicolon inside a quoted name", `SELECT "a;BEGIN" FROM t`, nil},
		{"a semicolon inside a bracketed name", `SELECT [a;BEGIN] FROM t`, nil},
		{"a semicolon inside a backticked name", "SELECT `a;BEGIN` FROM t", nil},
		{"a semicolon inside a line comment", "SELECT 1 -- ;BEGIN", nil},
		{"a semicolon inside a block comment", "SELECT 1 /* ;BEGIN */", nil},
		{"a doubled quote inside a string", "SELECT 'it''s;BEGIN'", nil},
		// A comment before the keyword does not hide it, in any position.
		{"a comment before a later opener", "SELECT 1; /* hi */ BEGIN", []txEffect{txEffectBegin}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := readTxStatements(tt.query)
			effects := make([]txEffect, 0, len(got))
			for _, stmt := range got {
				effects = append(effects, stmt.effect)
			}
			if len(effects) != len(tt.want) {
				t.Fatalf("readTxStatements(%q) = %v, want %v", tt.query, effects, tt.want)
			}
			for i := range effects {
				if effects[i] != tt.want[i] {
					t.Errorf("readTxStatements(%q) = %v, want %v", tt.query, effects, tt.want)
					break
				}
			}
		})
	}
}

// TestSplitStatements covers the cut on its own, since what it must not do is
// find a boundary inside a value.
func TestSplitStatements(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		query string
		want  int
	}{
		{"SELECT 1", 1},
		{"SELECT 1;", 2}, // The empty statement after the semicolon.
		{"SELECT 1; SELECT 2", 2},
		{"SELECT ';'", 1},
		{`SELECT "a;b"`, 1},
		{"SELECT `a;b`", 1},
		{"SELECT [a;b]", 1},
		{"SELECT 'it''s;here'", 1},
		// The semicolon is inside the comment, so it ends nothing.
		{"SELECT 1 -- ;\nSELECT 2", 1},
		{"SELECT 1 /* ; */; SELECT 2", 2},
		{"SELECT 'unterminated;", 1},
		{"SELECT 1 /* unterminated;", 1},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			if got := len(splitStatements(tt.query)); got != tt.want {
				t.Errorf("splitStatements(%q) gave %d statements, want %d", tt.query, got, tt.want)
			}
		})
	}
}
