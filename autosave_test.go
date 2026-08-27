package filesql

import (
	"compress/gzip"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/nao1215/filesql/internal/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
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

// TestAutoSaveTransaction_CommitThatFailedStopsCountingAsOpen covers the
// transaction a caller has no way to finish. database/sql does not call
// Rollback after a Commit that returned an error, and the driver has already
// rolled the connection back, so a transaction left in the connector's count
// there would make every later close refuse a save that had nothing to wait
// for.
func TestAutoSaveTransaction_CommitThatFailedStopsCountingAsOpen(t *testing.T) {
	t.Parallel()

	connector := &autoSaveConnector{
		autoSaveConfig: &autoSaveConfig{enabled: true, timing: autoSaveOnClose},
	}
	conn := &autoSaveConnection{conn: &plainConn{}, connector: connector}
	tx := conn.wrapTx(failingCommitTx{})

	require.ErrorIs(t, tx.Commit(), errStub)

	connector.mu.Lock()
	open := connector.openTx
	connector.mu.Unlock()
	assert.Zero(t, open, "a transaction that cannot be committed is still over")
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

// TestAutoSaveOverwriteKeepsLineEnding pins that a save in place writes back the
// terminator the file already used.
//
// It did not: every record was written with "\n" whatever the source used, so a
// CRLF file came back LF throughout. A caller who edited one row got a file
// whose every line had changed — a whole-file diff in a repository configured
// for CRLF, and a file the tools that read it no longer saw as they had.
func TestAutoSaveOverwriteKeepsLineEnding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
		update  string
		want    string
	}{
		{
			name:    "CSV keeps CRLF",
			file:    "crlf.csv",
			content: "id,v\r\n1,a\r\n2,b\r\n",
			update:  "UPDATE crlf SET v='x' WHERE id=1",
			want:    "id,v\r\n1,x\r\n2,b\r\n",
		},
		{
			name:    "CSV keeps LF",
			file:    "lf.csv",
			content: "id,v\n1,a\n2,b\n",
			update:  "UPDATE lf SET v='x' WHERE id=1",
			want:    "id,v\n1,x\n2,b\n",
		},
		{
			name:    "TSV keeps CRLF",
			file:    "crlf.tsv",
			content: "id\tv\r\n1\ta\r\n2\tb\r\n",
			update:  "UPDATE crlf SET v='x' WHERE id=1",
			want:    "id\tv\r\n1\tx\r\n2\tb\r\n",
		},
		{
			name:    "LTSV keeps CRLF",
			file:    "crlf.ltsv",
			content: "id:1\tv:a\r\nid:2\tv:b\r\n",
			update:  "UPDATE crlf SET v='x' WHERE id=1",
			want:    "id:1\tv:x\r\nid:2\tv:b\r\n",
		},
		{
			// The parser reads a CR-terminated file as lines rather than as one
			// very long line, so the save has to be able to put CR back. It could
			// not: the count only looked for "\n", so a file with none came back
			// rewritten line by line.
			name:    "CSV keeps a lone CR",
			file:    "cr.csv",
			content: "id,v\r1,a\r2,b\r",
			update:  "UPDATE cr SET v='x' WHERE id=1",
			want:    "id,v\r1,x\r2,b\r",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{path}, tt.update))

			got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got), "only the edited row may differ from what was there")
		})
	}
}

// TestAutoSaveOverwriteKeepsLineEndingUnderCompression checks that the
// terminator is read from the bytes inside the codec, not from the archive.
func TestAutoSaveOverwriteKeepsLineEndingUnderCompression(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "crlf.csv.gz")
	file, err := os.Create(path) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	gz := gzip.NewWriter(file)
	_, err = gz.Write([]byte("id,v\r\n1,a\r\n2,b\r\n"))
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.NoError(t, file.Close())

	require.NoError(t, autoSaveOverwrite(t, []string{path}, "UPDATE crlf SET v='x' WHERE id=1"))

	reopened, err := os.Open(path) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	defer reopened.Close()
	reader, err := gzip.NewReader(reopened)
	require.NoError(t, err)
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "id,v\r\n1,x\r\n2,b\r\n", string(decompressed))
}

// TestDumpDatabase_WithLineEnding covers the option on a dump to a new
// destination, where there is no existing file to take the terminator from.
func TestDumpDatabase_WithLineEnding(t *testing.T) {
	t.Parallel()

	source := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(source, []byte("id,v\n1,a\n"), 0o600))

	db, err := Open(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	outputDir := t.TempDir()
	require.NoError(t, DumpDatabase(db, outputDir, NewDumpOptions().WithLineEnding(LineEndingCRLF)))

	got, err := os.ReadFile(filepath.Join(outputDir, "users.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,v\r\n1,a\r\n", string(got))
}

// TestNewDumpOptions_DefaultsToLF pins the default, which is what a save wrote
// before the option existed.
func TestNewDumpOptions_DefaultsToLF(t *testing.T) {
	t.Parallel()

	assert.Equal(t, LineEndingLF, NewDumpOptions().LineEnding)
	assert.Equal(t, LineEndingCRLF, NewDumpOptions().WithLineEnding(LineEndingCRLF).LineEnding)
}

// TestSaveLineEndingByDestination pins which save reads a source's terminator
// and which does not, for every way of writing back over the file a table was
// loaded from.
//
// Only the in-place mode reads it. A dump is an export: it writes the same bytes
// whatever already sits in the destination, so pointing one at the source
// directory replaces a CRLF file with an LF copy. That is a defensible split —
// an export whose output depended on the destination's contents would write
// different bytes on its second run — but it is not what a caller expects from
// "save it back where it came from", so the three are pinned side by side here
// and the README names the mode rather than describing every overwrite.
func TestSaveLineEndingByDestination(t *testing.T) {
	t.Parallel()

	const source = "id,v\r\n1,a\r\n2,b\r\n"
	const update = "UPDATE crlf SET v='x' WHERE id=1"

	newSource := func(t *testing.T) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "crlf.csv")
		require.NoError(t, os.WriteFile(path, []byte(source), 0o600))
		return path
	}

	t.Run("in place keeps CRLF", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		require.NoError(t, autoSaveOverwrite(t, []string{path}, update))

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\r\n1,x\r\n2,b\r\n", string(got))
	})

	t.Run("auto-save into the source directory writes LF", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		ctx := t.Context()
		validated, err := NewBuilder().AddPath(path).EnableAutoSave(filepath.Dir(path)).Build(ctx)
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, update)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\n1,x\n2,b\n", string(got))
	})

	t.Run("dump into the source directory writes LF", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		ctx := t.Context()
		db, err := Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		_, err = db.ExecContext(ctx, update)
		require.NoError(t, err)
		require.NoError(t, DumpDatabase(db, filepath.Dir(path)))

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\n1,x\n2,b\n", string(got))
	})

	t.Run("auto-save into a directory is told the terminator instead", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		ctx := t.Context()
		validated, err := NewBuilder().
			AddPath(path).
			EnableAutoSave(filepath.Dir(path), NewDumpOptions().WithLineEnding(LineEndingCRLF)).
			Build(ctx)
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, update)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\r\n1,x\r\n2,b\r\n", string(got))
	})

	t.Run("an export is told the terminator instead", func(t *testing.T) {
		t.Parallel()

		path := newSource(t)
		ctx := t.Context()
		db, err := Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		_, err = db.ExecContext(ctx, update)
		require.NoError(t, err)
		require.NoError(t, DumpDatabase(db, filepath.Dir(path), NewDumpOptions().WithLineEnding(LineEndingCRLF)))

		got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id,v\r\n1,x\r\n2,b\r\n", string(got))
	})
}

// TestAutoSaveOverwriteWithNoStatementIsByteIdentical pins the property the
// terminator detection exists for, stated as an invariant rather than as one
// terminator at a time: a database nobody wrote to has nothing to change on
// disk, so the file has to come back exactly as it was.
func TestAutoSaveOverwriteWithNoStatementIsByteIdentical(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
	}{
		{name: "LF", file: "lf.csv", content: "id,v\n1,a\n2,b\n"},
		{name: "CRLF", file: "crlf.csv", content: "id,v\r\n1,a\r\n2,b\r\n"},
		{name: "lone CR", file: "cr.csv", content: "id,v\r1,a\r2,b\r"},
		{name: "quoted CR is data, not a terminator", file: "quoted.csv", content: "id,v\n1,\"a\rb\"\n2,c\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{path}))

			got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.content, string(got))
		})
	}
}

// autoSaveOverwrite opens path with auto-save in overwrite mode, runs stmts, and
// closes, returning the close error. Closing is what performs the save.
func autoSaveOverwrite(t *testing.T, paths []string, stmts ...string) error {
	t.Helper()

	ctx := t.Context()
	builder := NewBuilder()
	for _, p := range paths {
		builder = builder.AddPath(p)
	}
	validated, err := builder.EnableAutoSave("").Build(ctx)
	require.NoError(t, err)

	db, err := validated.Open(ctx)
	require.NoError(t, err)

	for _, stmt := range stmts {
		_, execErr := db.ExecContext(ctx, stmt)
		require.NoError(t, execErr)
	}
	return db.Close()
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// TestAutoSaveOverwriteKeepsSourceFormat pins that overwrite mode writes each
// table back to the file it came from, in that file's own format.
//
// It did not: overwrite mode handed the whole database to DumpDatabase with the
// output format from the auto-save options, which defaults to CSV. A .tsv source
// therefore got a new .csv beside it holding the change, while the .tsv the
// caller had asked to overwrite still held the old rows — the save went to a file
// nobody named, and the file that was named went stale.
func TestAutoSaveOverwriteKeepsSourceFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
		want    string
	}{
		{name: "csv", file: "data.csv", content: "id,name\n1,alice\n", want: "id,name\n1,bob\n"},
		{name: "tsv", file: "data.tsv", content: "id\tname\n1\talice\n", want: "id\tname\n1\tbob\n"},
		{name: "ltsv", file: "data.ltsv", content: "id:1\tname:alice\n", want: "id:1\tname:bob\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			src := filepath.Join(dir, tt.file)
			require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE data SET name = 'bob'"))

			assert.Equal(t, []string{tt.file}, dirEntries(t, dir), "overwrite mode writes no file the caller did not open")

			got, err := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestAutoSaveOverwriteKeepsCompression pins that a compressed source is written
// back compressed, and in place. A .csv.gz source used to get a plain .csv beside
// it while the archive kept the old rows.
func TestAutoSaveOverwriteKeepsCompression(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	dir := t.TempDir()

	// Build the fixture through the dump path so it is a real archive.
	plain := filepath.Join(dir, "seed.csv")
	require.NoError(t, os.WriteFile(plain, []byte("id,name\n1,alice\n"), 0o600))
	seedDB, err := OpenContext(ctx, plain)
	require.NoError(t, err)
	gzDir := filepath.Join(dir, "gz")
	require.NoError(t, DumpDatabase(seedDB, gzDir, NewDumpOptions().WithCompression(CompressionGZ)))
	require.NoError(t, seedDB.Close())

	src := filepath.Join(gzDir, "seed.csv.gz")
	require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE seed SET name = 'bob'"))

	assert.Equal(t, []string{"seed.csv.gz"}, dirEntries(t, gzDir), "the archive is replaced, not sidestepped")

	// Reading it back is what proves it is still a gzip archive holding the change.
	reloaded, err := OpenContext(ctx, src)
	require.NoError(t, err)
	defer reloaded.Close()

	var name string
	require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM seed").Scan(&name))
	assert.Equal(t, "bob", name)
}

// TestAutoSaveOverwriteAcrossDirectories pins that each source is written back to
// its own directory. The output directory was taken from the first source path, so
// every table landed next to whichever file happened to be loaded first.
func TestAutoSaveOverwriteAcrossDirectories(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirA := filepath.Join(root, "a")
	dirB := filepath.Join(root, "b")
	require.NoError(t, os.MkdirAll(dirA, 0o750))
	require.NoError(t, os.MkdirAll(dirB, 0o750))

	srcA := filepath.Join(dirA, "x.csv")
	srcB := filepath.Join(dirB, "y.csv")
	require.NoError(t, os.WriteFile(srcA, []byte("id,name\n1,alice\n"), 0o600))
	require.NoError(t, os.WriteFile(srcB, []byte("id,name\n2,carol\n"), 0o600))

	require.NoError(t, autoSaveOverwrite(t, []string{srcA, srcB},
		"UPDATE x SET name = 'bob'", "UPDATE y SET name = 'dave'"))

	assert.Equal(t, []string{"x.csv"}, dirEntries(t, dirA))
	assert.Equal(t, []string{"y.csv"}, dirEntries(t, dirB))

	gotA, err := os.ReadFile(srcA) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(gotA))

	gotB, err := os.ReadFile(srcB) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n2,dave\n", string(gotB))
}

// TestAutoSaveOverwriteLeavesNewTablesAlone pins that a table the caller created
// is not written anywhere. Overwrite mode is defined by the files that were
// opened, and a new table is not one of them; it used to appear as a new file in
// the source directory.
func TestAutoSaveOverwriteLeavesNewTablesAlone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "data.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

	require.NoError(t, autoSaveOverwrite(t, []string{src},
		"CREATE TABLE scratch (a TEXT)",
		"INSERT INTO scratch VALUES ('temporary')",
		"UPDATE data SET name = 'bob'"))

	assert.Equal(t, []string{"data.csv"}, dirEntries(t, dir))

	got, err := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(got))
}

// TestAutoSaveOverwriteRefusesFormatItCannotWrite pins that a source in a format
// with no writer fails the save instead of quietly becoming a CSV beside it.
func TestAutoSaveOverwriteRefusesFormatItCannotWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
	}{
		{name: "json", file: "records.json", content: `[{"id":1}]`},
		{name: "jsonl", file: "records.jsonl", content: "{\"id\":1}\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			src := filepath.Join(dir, tt.file)
			require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

			// The extension is the whole of the answer, so Build is where the
			// caller hears it: no database is opened and no file is touched.
			_, err := NewBuilder().AddPath(src).EnableAutoSave("").Build(t.Context())
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Contains(t, err.Error(), tt.file)

			assert.Equal(t, []string{tt.file}, dirEntries(t, dir), "nothing else may be written")

			got, readErr := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, readErr)
			assert.Equal(t, tt.content, string(got), "the source is left as it was")
		})
	}
}

// TestAutoSaveOverwriteXLSX pins the two shapes an Excel source can have. A
// workbook of one sheet is written back to itself. A workbook of several sheets
// became one CSV per sheet next to it, which is not the file the caller opened;
// it now fails and says so, because the XLSX writer holds one sheet per file.
func TestAutoSaveOverwriteXLSX(t *testing.T) {
	t.Parallel()

	t.Run("a workbook of one sheet is written back to itself", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()

		// Build a single-sheet workbook through the dump path.
		plain := filepath.Join(dir, "book.csv")
		require.NoError(t, os.WriteFile(plain, []byte("id,name\n1,alice\n"), 0o600))
		seedDB, err := OpenContext(ctx, plain)
		require.NoError(t, err)
		bookDir := filepath.Join(dir, "book")
		require.NoError(t, DumpDatabase(seedDB, bookDir, NewDumpOptions().WithFormat(OutputFormatXLSX)))
		require.NoError(t, seedDB.Close())

		src := filepath.Join(bookDir, "book.xlsx")
		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book SET name = 'bob'"))

		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, bookDir))

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()

		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book").Scan(&name))
		assert.Equal(t, "bob", name)
	})

	t.Run("a sheet keeps its name across a round trip", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book_Orders SET name = 'bob'"))

		assert.Equal(t, []string{"Orders"}, workbookSheets(t, src),
			"overwriting a workbook in place must not rename its sheet")

		// The name has to survive repeatedly, not just once: a prefix added on
		// every save accumulates until Excel's 31-rune sheet name limit truncates it.
		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book_Orders SET name = 'carol'"))
		assert.Equal(t, []string{"Orders"}, workbookSheets(t, src))

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		assert.Equal(t, "carol", name)
	})

	t.Run("a workbook of several sheets is written back to itself", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders":    {{"id", "name"}, {"1", "alice"}},
			"Customers": {{"id", "city"}, {"1", "tokyo"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{src},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_Customers SET city = 'osaka'"))

		assert.Equal(t, []string{"Customers", "Orders"}, workbookSheets(t, src),
			"every sheet has to come back, under its own name")
		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, dir), "nothing else may be written")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name, city string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT city FROM book_Customers").Scan(&city))
		assert.Equal(t, "bob", name)
		assert.Equal(t, "osaka", city)
	})

	t.Run("a workbook keeps the sheets of a sibling whose name it prefixes out", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		book := filepath.Join(dir, "book.xlsx")
		sibling := filepath.Join(dir, "book_v2.xlsx")
		writeWorkbook(t, book, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})
		writeWorkbook(t, sibling, map[string][][]string{
			"Orders": {{"id", "name"}, {"2", "dave"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{book, sibling},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_v2_Orders SET name = 'erin'"))

		assert.Equal(t, []string{"Orders"}, workbookSheets(t, book),
			"book.xlsx holds its own sheet only: book_v2.xlsx's tables are named inside book's prefix space, but they are not book's")
		assert.Equal(t, []string{"Orders"}, workbookSheets(t, sibling))

		reloaded, err := OpenContext(ctx, book, sibling)
		require.NoError(t, err)
		defer reloaded.Close()

		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		assert.Equal(t, "bob", name)
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_v2_Orders").Scan(&name))
		assert.Equal(t, "erin", name)
	})

	t.Run("a workbook keeps out a sibling of another format whose name it prefixes", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		book := filepath.Join(dir, "book.xlsx")
		sibling := filepath.Join(dir, "book_v2.csv")
		writeWorkbook(t, book, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})
		require.NoError(t, os.WriteFile(sibling, []byte("id,name\n2,dave\n"), 0o600))

		require.NoError(t, autoSaveOverwrite(t, []string{book, sibling},
			"UPDATE book_Orders SET name = 'bob'"))

		assert.Equal(t, []string{"Orders"}, workbookSheets(t, book),
			"a CSV sibling loads as one table named inside the workbook's prefix space, and it is not the workbook's either")
	})

	t.Run("a compressed workbook of several sheets round-trips", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		plain := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, plain, map[string][][]string{
			"Orders":    {{"id", "name"}, {"1", "alice"}},
			"Customers": {{"id", "city"}, {"1", "tokyo"}},
		})

		// A compressed source has to be written back through its own codec, and
		// the workbook still has to arrive whole on the other side of it.
		raw, err := os.ReadFile(plain) //nolint:gosec // plain is under t.TempDir()
		require.NoError(t, err)
		require.NoError(t, os.Remove(plain))

		src := filepath.Join(dir, "book.xlsx.gz")
		out, err := os.Create(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, err)
		gz := gzip.NewWriter(out)
		_, err = gz.Write(raw)
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, out.Close())

		require.NoError(t, autoSaveOverwrite(t, []string{src},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_Customers SET city = 'osaka'"))

		assert.Equal(t, []string{"book.xlsx.gz"}, dirEntries(t, dir), "nothing else may be written")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name, city string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT city FROM book_Customers").Scan(&city))
		assert.Equal(t, "bob", name)
		assert.Equal(t, "osaka", city)
	})

	t.Run("two tables that would share a sheet name are refused", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})
		before, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, err)

		validated, err := NewBuilder().AddPath(src).EnableAutoSave("").Build(ctx)
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)

		// Excel caps a sheet name at 31 runes, so two tables of this workbook
		// whose names agree for the first 31 and differ after map to one sheet.
		// excelize's NewSheet returns the existing index rather than erroring, so
		// the second table used to overwrite the first's sheet and one table's
		// rows vanished while the save reported success.
		stem := strings.Repeat("a", excelSheetNameMaxLen)
		for _, suffix := range []string{stem + "X", stem + "Y"} {
			_, execErr := db.ExecContext(ctx, "CREATE TABLE `book_"+suffix+"` (id TEXT)")
			require.NoError(t, execErr)
		}

		err = db.Close()
		require.Error(t, err, "a save that cannot keep both tables must not report success")
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		// Both table names, not just the sheet: the error's job is to say which
		// two tables collided, and asserting only the sheet would pass an error
		// that named neither.
		assert.Contains(t, err.Error(), "book_"+stem+"X")
		assert.Contains(t, err.Error(), "book_"+stem+"Y")
		assert.Contains(t, err.Error(), stem, "the error names the sheet the two tables collide on")

		after, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, before, after, "the workbook must be left as it was")
	})

	t.Run("a workbook read from a fixture round-trips whole", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		data, err := os.ReadFile(filepath.Join("testdata", "excel", "sample.xlsx"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(src, data, 0o600)) //nolint:gosec // src is under t.TempDir()

		before := workbookSheets(t, src)
		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book_Sheet1 SET name = 'bob'"))

		assert.Equal(t, before, workbookSheets(t, src), "the sheets have to come back as they were")
		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, dir), "nothing else may be written")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Sheet1").Scan(&name))
		assert.Equal(t, "bob", name)
	})
}

// writeWorkbook builds an xlsx at path holding the given sheets. Each sheet's
// first row is its header.
func writeWorkbook(t *testing.T, path string, sheets map[string][][]string) {
	t.Helper()

	f := excelize.NewFile()
	defer func() {
		_ = f.Close()
	}()

	names := make([]string, 0, len(sheets))
	for name := range sheets {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if _, err := f.NewSheet(name); err != nil {
			t.Fatal(err)
		}
		for r, row := range sheets[name] {
			for c, value := range row {
				cell, err := excelize.CoordinatesToCellName(c+1, r+1)
				require.NoError(t, err)
				require.NoError(t, f.SetCellValue(name, cell, value))
			}
		}
	}
	require.NoError(t, f.DeleteSheet(defaultSheetName))
	require.NoError(t, f.SaveAs(path))
}

// workbookSheets returns the sheet names of the workbook at path, sorted.
func workbookSheets(t *testing.T, path string) []string {
	t.Helper()

	f, err := excelize.OpenFile(path)
	require.NoError(t, err)
	defer func() {
		_ = f.Close()
	}()

	names := f.GetSheetList()
	sort.Strings(names)
	return names
}

// TestAutoSaveOverwriteKeepsTheFileItWasGiven pins overwrite mode's core
// promise from the file's side: the bytes go back into the path that was
// opened, under the name it already had, or the save fails and the file is
// left alone. Nothing covered either half for a name that is not already a
// valid SQL identifier, and the table name is derived from the file name by a
// mapping that is not reversible.
func TestAutoSaveOverwriteKeepsTheFileItWasGiven(t *testing.T) {
	t.Parallel()

	// Each name loads as a table spelled differently from the file: "my-data"
	// becomes my_data, "sales report" becomes sales_report, and a name starting
	// with a digit gains a prefix. The file must keep its own spelling.
	names := []string{
		"my-data.csv",
		"sales report.csv",
		"2024.q1.csv",
		"café.csv",
	}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			dir := t.TempDir()
			src := filepath.Join(dir, name)
			require.NoError(t, os.WriteFile(src, []byte("id,v\n1,a\n"), 0o600))

			validated, err := NewBuilder().AddPath(src).EnableAutoSave("").Build(ctx)
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)

			tables, err := getSQLiteTableNames(context.Background(), db)
			require.NoError(t, err)
			require.Len(t, tables, 1)

			//nolint:gosec // the table name comes from the file this test just wrote
			_, err = db.ExecContext(ctx, "UPDATE `"+tables[0]+"` SET v = 'b'")
			require.NoError(t, err)
			require.NoError(t, db.Close())

			assert.Equal(t, []string{name}, dirEntries(t, dir),
				"the save goes back to the file that was opened, under its own name")

			content, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, "id,v\n1,b\n", string(content))
		})
	}
}

// TestAutoSaveOverwriteRefusesCodecItCannotWrite pins the other half: bzip2 is
// read but has no writer in this library, so a .bz2 source cannot be written
// back. The save has to say so and leave the file untouched rather than report
// success over a file it never wrote.
func TestAutoSaveOverwriteRefusesCodecItCannotWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "products.tsv.bz2")
	fixture, err := os.ReadFile(filepath.Join("testdata", "products.tsv.bz2"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(src, fixture, 0o600)) //nolint:gosec // src is under t.TempDir()

	_, err = NewBuilder().AddPath(src).EnableAutoSave("").Build(t.Context())
	require.Error(t, err, "a codec this package cannot write must not report a successful save")
	assert.Contains(t, err.Error(), "bzip2")
	// The codec is read off the name, so the refusal comes from Build, before
	// there is a database to change or a file to replace. ErrUnsupportedFormat
	// is the sentinel it carries, the same one the writer reports when a dump
	// asks for bzip2; TestDumpDatabase_RefusesACodecItCannotWrite covers the
	// rest of that chain.
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.ErrorIs(t, err, codec.ErrNoBZ2Writer)

	after, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, fixture, after, "the source must be left byte for byte as it was")
	assert.Equal(t, []string{"products.tsv.bz2"}, dirEntries(t, dir), "nothing else may be written")
}

// TestAutoSaveOverwriteLongSourceName pins the auto-save form of the staged-name
// bug. Overwrite mode is where the failure costs the caller their edit: the save
// runs from Close, after the change is in the database and with nowhere else for
// it to go.
func TestAutoSaveOverwriteLongSourceName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := strings.Repeat("s", 246) + ".csv"
	src := filepath.Join(dir, base)
	if err := os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600); err != nil {
		t.Skipf("this filesystem does not accept a %d-byte name: %v", len(base), err)
	}

	table := sanitizeTableName(tableFromFilePath(src))
	require.NoError(t, autoSaveOverwrite(t, []string{src}, `UPDATE "`+table+`" SET name = 'bob'`))

	got, err := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(got))
	assert.Equal(t, []string{base}, dirEntries(t, dir), "no staged file may be left beside the source")
}

// TestAutoSaveCloseWithAnOpenTransaction pins that closing a database with a
// transaction still open returns. It did not: the save reads every table
// through the connector's own connection, an uncommitted write holds the lock
// on the table it touched, and the driver waits for that lock with no deadline
// and no context. The only goroutine that could release it was the one inside
// Close, so a caller whose error path forgot a rollback did not leak a
// connection -- their process stopped.
func TestAutoSaveCloseWithAnOpenTransaction(t *testing.T) {
	t.Parallel()

	// closeWithin runs Close on a goroutine so a Close that never returns fails
	// the test instead of hanging the run until the package timeout.
	closeWithin := func(t *testing.T, db *sql.DB) error {
		t.Helper()

		done := make(chan error, 1)
		go func() { done <- db.Close() }()
		select {
		case err := <-done:
			return err
		case <-time.After(30 * time.Second):
			t.Fatal("db.Close did not return; the save is waiting on a lock it cannot get")
			return nil
		}
	}

	setup := func(t *testing.T, enable func(*DBBuilder) *DBBuilder) (*sql.DB, string) {
		t.Helper()

		dir := t.TempDir()
		src := filepath.Join(dir, "users.csv")
		require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

		validated, err := enable(NewBuilder().AddPath(src)).Build(t.Context())
		require.NoError(t, err)
		db, err := validated.Open(t.Context())
		require.NoError(t, err)
		return db, src
	}

	onClose := func(b *DBBuilder) *DBBuilder { return b.EnableAutoSave("") }
	onCommit := func(b *DBBuilder) *DBBuilder { return b.EnableAutoSaveOnCommit("") }

	for _, tt := range []struct {
		name   string
		enable func(*DBBuilder) *DBBuilder
	}{
		{name: "save on close", enable: onClose},
		{name: "save on commit", enable: onCommit},
	} {
		t.Run("a write left uncommitted stops the save ("+tt.name+")", func(t *testing.T) {
			t.Parallel()

			db, src := setup(t, tt.enable)
			tx, err := db.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			_, err = tx.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
			require.NoError(t, err)

			err = closeWithin(t, db)
			require.Error(t, err, "a save that was skipped must be reported, not passed off as done")
			assert.ErrorIs(t, err, ErrDatabaseOperation)
			assert.Contains(t, err.Error(), "transaction")

			got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
			require.NoError(t, readErr)
			assert.Equal(t, "id,name\n1,alice\n", string(got), "nothing uncommitted may reach the file")
		})
	}

	t.Run("a transaction that only read is refused the same way", func(t *testing.T) {
		t.Parallel()

		// Reading takes no write lock, so this one never hung. It is refused
		// all the same: a transaction still open at Close is a caller who is
		// not done with the database, and one rule is easier to rely on than a
		// rule that depends on what the transaction happened to run.
		db, src := setup(t, onClose)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		var n int
		require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&n))
		require.Equal(t, 1, n)

		err = closeWithin(t, db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction")

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n", string(got))
	})

	t.Run("a committed transaction saves", func(t *testing.T) {
		t.Parallel()

		db, src := setup(t, onClose)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		require.NoError(t, closeWithin(t, db))

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got))
	})

	t.Run("a rolled back transaction saves what the rollback left", func(t *testing.T) {
		t.Parallel()

		db, src := setup(t, onClose)
		_, err := db.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
		require.NoError(t, err)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "INSERT INTO users VALUES (3,'carol')")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		require.NoError(t, closeWithin(t, db))

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got))
	})

	t.Run("an unclosed rows iterator still saves", func(t *testing.T) {
		t.Parallel()

		// Rows hold a pooled connection but no transaction and no lock the save
		// waits on, so this has to keep working: a fix that refused whenever a
		// connection was still checked out would break it silently.
		db, src := setup(t, onClose)
		_, err := db.ExecContext(t.Context(), "INSERT INTO users VALUES (2,'bob')")
		require.NoError(t, err)
		rows, err := db.QueryContext(t.Context(), "SELECT * FROM users")
		require.NoError(t, err)
		require.True(t, rows.Next())

		require.NoError(t, closeWithin(t, db))
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		got, readErr := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got))
	})
}

// TestAutoSaveOverwriteFollowsASymlink pins that a source reached through a
// symbolic link is written back through it. The staged file was renamed onto
// the link itself, so the link became a regular file holding the change and the
// file it named still held the old rows: the save reported success while the
// data the caller meant to update never moved.
func TestAutoSaveOverwriteFollowsASymlink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "real.csv")
	require.NoError(t, os.WriteFile(target, []byte("id,name\n1,alice\n"), 0o600))
	link := filepath.Join(dir, "users.csv")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("this platform does not allow a symlink to be created: %v", err)
	}

	require.NoError(t, autoSaveOverwrite(t, []string{link}, "INSERT INTO users VALUES (2,'bob')"))

	info, err := os.Lstat(link)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "the link must survive the save")

	got, err := os.ReadFile(target) //nolint:gosec // target is under t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got), "the file the link names is what receives the row")
	assert.Equal(t, []string{"real.csv", "users.csv"}, dirEntries(t, dir), "no staged file may be left behind")
}

// TestAutoSaveOverwriteRefusesASourceItCannotWriteBeforeOpening pins where a
// source that overwrite mode can never write back is reported. It was reported
// from Close, one file at a time, so a set holding such a source had its earlier
// files replaced before the caller heard about it: half the directory held the
// session's rows and half held the old ones, with nothing on disk saying which
// was which. The extension decides the answer, so Build knows it before any
// database exists and refuses there.
func TestAutoSaveOverwriteRefusesASourceItCannotWriteBeforeOpening(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "aaa.csv")
	require.NoError(t, os.WriteFile(csvPath, []byte("id,name\n1,alice\n"), 0o600))
	jsonPath := filepath.Join(dir, "zzz.json")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`[{"id":1}]`), 0o600))

	_, err := NewBuilder().AddPath(csvPath).AddPath(jsonPath).EnableAutoSave("").Build(t.Context())
	require.Error(t, err, "a set that cannot be saved must be refused before it is loaded")
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.Contains(t, err.Error(), "zzz.json")

	got, readErr := os.ReadFile(csvPath) //nolint:gosec // csvPath is under t.TempDir()
	require.NoError(t, readErr)
	assert.Equal(t, "id,name\n1,alice\n", string(got), "no file may be replaced by a save that cannot finish")

	t.Run("an output directory is unaffected", func(t *testing.T) {
		t.Parallel()

		// Export mode writes what DumpOptions says into a directory of its own,
		// so a source with no writer is read and written out as CSV and no
		// source file is replaced. The refusal is about overwrite mode only.
		out := filepath.Join(t.TempDir(), "out")
		validated, buildErr := NewBuilder().AddPath(csvPath).AddPath(jsonPath).EnableAutoSave(out).Build(t.Context())
		require.NoError(t, buildErr)
		db, openErr := validated.Open(t.Context())
		require.NoError(t, openErr)
		require.NoError(t, db.Close())

		assert.Equal(t, []string{"aaa.csv", "zzz.csv"}, dirEntries(t, out))
	})
}
