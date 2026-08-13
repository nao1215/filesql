package filesql

import (
	"context"
	"database/sql/driver"
	"errors"
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

// TestAutoSaveConnection_CloseReportsBothFailures covers a close where the save
// and the close itself both fail. The save error is the one a caller acts on, so
// it leads, but a connection that also failed to close is worth saying.
func TestAutoSaveConnection_CloseReportsBothFailures(t *testing.T) {
	t.Parallel()

	inner := &plainConn{closeErr: errStub}
	conn := &autoSaveConnection{
		conn: inner,
		// Overwrite mode with no original paths: the save has nowhere to write, so
		// it fails without touching the filesystem.
		autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnClose},
	}

	err := conn.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auto-save failed")
	assert.Contains(t, err.Error(), "also failed to close connection", "a connection left open is worth reporting too")
}

// TestAutoSaveTransaction_CommitReportsAFailedSave covers a commit that
// succeeded followed by a save that did not. The rows are already committed, so
// the caller has to be told that only the file is out of date.
func TestAutoSaveTransaction_CommitReportsAFailedSave(t *testing.T) {
	t.Parallel()

	tx := &autoSaveTransaction{
		tx: stubTx{},
		conn: &autoSaveConnection{
			conn:           &plainConn{},
			autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnCommit},
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
			conn:           &plainConn{},
			autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnCommit},
		},
	}

	assert.NoError(t, tx.Rollback())
}

// TestPerformAutoSave_DisabledDoesNothing covers the two states in which a close
// has nothing to save.
func TestPerformAutoSave_DisabledDoesNothing(t *testing.T) {
	t.Parallel()

	t.Run("no configuration", func(t *testing.T) {
		t.Parallel()
		conn := &autoSaveConnection{conn: &plainConn{}}
		assert.NoError(t, conn.performAutoSave())
	})

	t.Run("configuration turned off", func(t *testing.T) {
		t.Parallel()
		conn := &autoSaveConnection{conn: &plainConn{}, autoSaveConfig: &autoSaveConfig{enabled: false}}
		assert.NoError(t, conn.performAutoSave())
	})
}
