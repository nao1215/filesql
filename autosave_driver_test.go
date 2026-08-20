package filesql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"testing"

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

// stubRows is an empty result set.
type stubRows struct{}

func (stubRows) Columns() []string          { return nil }
func (stubRows) Close() error               { return nil }
func (stubRows) Next([]driver.Value) error  { return errStub }
func (stubRows) ColumnTypeScanType(int) any { return nil }
func (stubRows) ColumnTypeDatabaseTypeName(int) string {
	return ""
}

// TestAutoSaveConnection_BeginTxFallsBackToBegin covers a wrapped driver that
// predates ConnBeginTx. Without the fallback such a driver could not start a
// transaction at all once auto-save wrapped it.
func TestAutoSaveConnection_BeginTxFallsBackToBegin(t *testing.T) {
	t.Parallel()

	t.Run("the legacy Begin is used", func(t *testing.T) {
		t.Parallel()

		inner := &plainConn{}
		conn := &autoSaveConnection{conn: inner}

		tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
		require.NoError(t, err)
		assert.IsType(t, &autoSaveTransaction{}, tx, "the transaction stays wrapped so a commit can still auto-save")
		assert.True(t, inner.begun, "the legacy Begin is what starts the transaction")
	})

	t.Run("a refused Begin is reported", func(t *testing.T) {
		t.Parallel()

		conn := &autoSaveConnection{conn: &plainConn{beginErr: errStub}}

		_, err := conn.BeginTx(context.Background(), driver.TxOptions{})
		assert.ErrorIs(t, err, errStub)
	})

	t.Run("the deprecated Begin goes through BeginTx", func(t *testing.T) {
		t.Parallel()

		inner := &plainConn{}
		conn := &autoSaveConnection{conn: inner}

		tx, err := conn.Begin()
		require.NoError(t, err)
		assert.IsType(t, &autoSaveTransaction{}, tx)
		assert.True(t, inner.begun)
	})
}

// TestAutoSaveConnection_LegacyExecAndQuery covers the pre-context statement
// interfaces. A driver that implements only those still has to be usable, and
// the named arguments it cannot take have to be converted rather than dropped.
func TestAutoSaveConnection_LegacyExecAndQuery(t *testing.T) {
	t.Parallel()

	t.Run("exec", func(t *testing.T) {
		t.Parallel()

		inner := &legacyConn{}
		conn := &autoSaveConnection{conn: inner}

		_, err := conn.ExecContext(context.Background(), "UPDATE t SET a = ?", []driver.NamedValue{{Ordinal: 1, Value: int64(7)}})
		require.NoError(t, err)
		assert.True(t, inner.execCalled)
		assert.Equal(t, []driver.Value{int64(7)}, inner.lastArgs, "the named values must reach the legacy driver as plain ones")
	})

	t.Run("query", func(t *testing.T) {
		t.Parallel()

		inner := &legacyConn{}
		conn := &autoSaveConnection{conn: inner}

		_, err := conn.QueryContext(context.Background(), "SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: "x"}})
		require.NoError(t, err)
		assert.True(t, inner.queryCalled)
		assert.Equal(t, []driver.Value{"x"}, inner.lastArgs)
	})

	t.Run("a connection with neither interface asks database/sql to take over", func(t *testing.T) {
		t.Parallel()

		conn := &autoSaveConnection{conn: &plainConn{}}

		_, err := conn.ExecContext(context.Background(), "UPDATE t SET a = 1", nil)
		assert.ErrorIs(t, err, driver.ErrSkip, "database/sql falls back to Prepare when the driver skips")

		_, err = conn.QueryContext(context.Background(), "SELECT 1", nil)
		assert.ErrorIs(t, err, driver.ErrSkip)
	})
}

// TestAutoSaveConnection_Prepare checks that preparing is handed straight to the
// wrapped connection.
func TestAutoSaveConnection_Prepare(t *testing.T) {
	t.Parallel()

	conn := &autoSaveConnection{conn: &plainConn{}}

	_, err := conn.Prepare("SELECT 1")
	assert.ErrorIs(t, err, errStub)
}

// TestAutoSaveConnector_CloseReportsBothFailures covers a close where the save
// and the close itself both fail. The save error is the one a caller acts on, so
// it leads, but a connection that also failed to close is worth saying.
func TestAutoSaveConnector_CloseReportsBothFailures(t *testing.T) {
	t.Parallel()

	connector := &autoSaveConnector{
		// Overwrite mode with no original paths: the save has nowhere to write, so
		// it fails without touching the filesystem.
		autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnClose},
		anchor:         &plainConn{closeErr: errStub},
		armed:          true,
	}

	err := connector.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auto-save failed")
	assert.Contains(t, err.Error(), "also failed to close connection", "a connection left open is worth reporting too")
}

// TestAutoSaveConnector_CloseBeforeArmingDoesNotSave checks that a setup which
// fails after opening the connector does not write out what it is discarding.
func TestAutoSaveConnector_CloseBeforeArmingDoesNotSave(t *testing.T) {
	t.Parallel()

	connector := &autoSaveConnector{
		autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnClose},
		anchor:         &plainConn{},
	}

	assert.NoError(t, connector.Close(), "an unarmed connector closes without saving")
	assert.NoError(t, connector.Close(), "closing twice is a no-op")
}

// TestAutoSaveTransaction_CommitReportsAFailedSave covers a commit that
// succeeded followed by a save that did not. The rows are already committed, so
// the caller has to be told that only the file is out of date.
func TestAutoSaveTransaction_CommitReportsAFailedSave(t *testing.T) {
	t.Parallel()

	tx := &autoSaveTransaction{
		tx: stubTx{},
		conn: &autoSaveConnection{
			conn: &plainConn{},
			connector: &autoSaveConnector{
				autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnCommit},
				anchor:         &plainConn{},
			},
		},
	}

	err := tx.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction committed successfully")
}

// TestAutoSaveTransaction_RollbackNeverSaves checks that a rollback reaches the
// wrapped transaction and does not run the save a commit would.
func TestAutoSaveTransaction_RollbackNeverSaves(t *testing.T) {
	t.Parallel()

	tx := &autoSaveTransaction{
		tx: stubTx{},
		conn: &autoSaveConnection{
			conn: &plainConn{},
			connector: &autoSaveConnector{
				autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnCommit},
				anchor:         &plainConn{},
			},
		},
	}

	assert.NoError(t, tx.Rollback())
}

// TestSave_DisabledDoesNothing covers the two states in which a close has
// nothing to save.
func TestSave_DisabledDoesNothing(t *testing.T) {
	t.Parallel()

	t.Run("no configuration", func(t *testing.T) {
		t.Parallel()
		connector := &autoSaveConnector{}
		assert.NoError(t, connector.save(&plainConn{}))
	})

	t.Run("configuration turned off", func(t *testing.T) {
		t.Parallel()
		connector := &autoSaveConnector{autoSaveConfig: &autoSaveConfig{enabled: false}}
		assert.NoError(t, connector.save(&plainConn{}))
	})
}

// autoSaveSource writes a one-row CSV and returns its path.
func autoSaveSource(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// openAutoSave opens path with auto-save configured by configure.
func openAutoSave(t *testing.T, path string, configure func(*DBBuilder) *DBBuilder) *sql.DB {
	t.Helper()

	validated, err := configure(NewBuilder().AddPath(path)).Build(t.Context())
	require.NoError(t, err)
	db, err := validated.Open(t.Context())
	require.NoError(t, err)
	return db
}

// TestAutoSaveSurvivesAPoolWithMoreThanOneConnection pins that an auto-save
// database behaves like any other pooled database: a second connection is a
// second connection, and closing the pool saves once.
//
// It did not: every pooled connection wrapped one shared driver.Conn, so the
// first wrapper the pool closed ran the save and closed that connection, and the
// next wrapper ran the save against a connection that was already gone. The
// crash was a SIGSEGV inside the SQLite driver, which a caller cannot recover
// from, and it took the save that was meant to persist their work with it.
func TestAutoSaveSurvivesAPoolWithMoreThanOneConnection(t *testing.T) {
	t.Parallel()

	t.Run("the pool trims the connection a write just used", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSave("") })
		db.SetMaxIdleConns(0)

		_, err := db.ExecContext(t.Context(), "UPDATE users SET name = 'bob'")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n", string(saved))
	})

	t.Run("a query issued while rows are open", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n2,carol\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSave("") })

		rows, err := db.QueryContext(t.Context(), "SELECT id FROM users")
		require.NoError(t, err)
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&count))
		assert.Equal(t, 2, count)
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		_, err = db.ExecContext(t.Context(), "UPDATE users SET name = 'bob' WHERE id = 1")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n2,carol\n", string(saved))
	})
}

// TestAutoSaveWritesEachTableOnce pins that closing a database that has held
// several connections leaves one file per table holding the final rows, not one
// save per connection the pool happened to open.
func TestAutoSaveWritesEachTableOnce(t *testing.T) {
	t.Parallel()

	path := autoSaveSource(t, "id,name\n1,alice\n")
	outputDir := t.TempDir()
	db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSave(outputDir) })

	// A second pooled connection, so the pool holds more than one when it closes.
	rows, err := db.QueryContext(t.Context(), "SELECT id FROM users")
	require.NoError(t, err)
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&count))
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	_, err = db.ExecContext(t.Context(), "UPDATE users SET name = 'bob'")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	assert.Equal(t, []string{"users.csv"}, dirEntries(t, outputDir))
	saved, err := os.ReadFile(filepath.Join(outputDir, "users.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(saved))
}

// TestAutoSaveOnCommitSavesWhatNoTransactionWrapped pins that a change which
// survives to a clean Close is on disk whichever auto-save timing was chosen.
//
// It did not: the commit-timing save lived only in the transaction wrapper, so a
// statement run outside an explicit transaction — committed as far as SQLite was
// concerned — never reached the file, and Close did not save either. The change
// was lost with no error to say so.
func TestAutoSaveOnCommitSavesWhatNoTransactionWrapped(t *testing.T) {
	t.Parallel()

	t.Run("overwrite mode", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit("") })

		_, err := db.ExecContext(t.Context(), "UPDATE users SET name = 'bob'")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n", string(saved))
	})

	t.Run("export mode", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n")
		outputDir := t.TempDir()
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit(outputDir) })

		_, err := db.ExecContext(t.Context(), "UPDATE users SET name = 'bob'")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(filepath.Join(outputDir, "users.csv")) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n", string(saved))
	})

	t.Run("a commit and a bare statement both land", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n2,carol\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit("") })

		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "UPDATE users SET name = 'bob' WHERE id = 1")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		committed, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n2,carol\n", string(committed), "the commit saves immediately")

		_, err = db.ExecContext(t.Context(), "UPDATE users SET name = 'dave' WHERE id = 2")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n2,dave\n", string(saved))
	})

	t.Run("a rolled back transaction stays out", func(t *testing.T) {
		t.Parallel()

		path := autoSaveSource(t, "id,name\n1,alice\n2,carol\n")
		db := openAutoSave(t, path, func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit("") })

		_, err := db.ExecContext(t.Context(), "UPDATE users SET name = 'bob' WHERE id = 1")
		require.NoError(t, err)

		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "UPDATE users SET name = 'dave' WHERE id = 2")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		require.NoError(t, db.Close())

		saved, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,bob\n2,carol\n", string(saved))
	})
}
