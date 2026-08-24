package filesql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// plainExecutor is a DBTX that is neither *sql.DB nor *sql.Tx. A load needs one
// of those two to run its input under a transaction or a savepoint, so a
// caller's own implementation has to be refused by name rather than crashing on
// a type assertion.
type plainExecutor struct {
	db *sql.DB
}

func (e plainExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return e.db.ExecContext(ctx, query, args...)
}

func (e plainExecutor) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return e.db.QueryContext(ctx, query, args...)
}

func (e plainExecutor) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return e.db.QueryRowContext(ctx, query, args...)
}

func (e plainExecutor) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return e.db.PrepareContext(ctx, query)
}

// openTestTx returns a transaction on an empty test database, which is the
// scope runInputScope hands every load. A test that calls one of the loading
// functions directly opens its own, so what it exercises is what a load
// exercises.
func openTestTx(t *testing.T) *sql.Tx {
	t.Helper()

	tx, err := openTestDB(t).BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		// A test that ended its own transaction leaves nothing to roll back.
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("could not roll back the test transaction: %v", err)
		}
	})
	return tx
}

// failingCloser reports a failure when the loader closes a reader it opened.
type failingCloser struct{ closed bool }

func (c *failingCloser) Close() error {
	c.closed = true
	return errStub
}

// TestStreamFileToDatabase_UnsupportedFormat covers the refusal of a file whose
// extension names no format this package reads.
func TestStreamFileToDatabase_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "notes.docx")
	require.NoError(t, os.WriteFile(path, []byte("content"), 0o600))

	err := newStreamProcessor(100).streamFileToDatabase(context.Background(), openTestTx(t), path)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
}

// TestStreamWriteBackFormatFiles_Failures covers the two formats that are read
// from a path rather than through the chunk loader. Both are opened and measured
// before parsing, so a missing or empty file is reported as such instead of as a
// parse failure with nothing in it.
func TestStreamWriteBackFormatFiles_Failures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name string
		ext  string
	}{
		{"ACH input", extACH},
		{"Fedwire", extFED},
	}
	for _, tt := range tests {
		t.Run(tt.name+" file that is not there", func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "missing"+tt.ext)
			err := newStreamProcessor(100).streamFileToDatabase(ctx, openTestTx(t), path)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrIOOperation)
		})

		t.Run(tt.name+" file with no bytes in it", func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "empty"+tt.ext)
			require.NoError(t, os.WriteFile(path, nil, 0o600))

			err := newStreamProcessor(100).streamFileToDatabase(ctx, openTestTx(t), path)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrEmptyData)
		})
	}
}

// TestRunInputScope_UnsupportedExecutor covers a DBTX an input cannot be scoped
// on. It is refused with the type in the message, because a caller who passed
// their own wrapper has no other way to tell what was wrong.
func TestRunInputScope_UnsupportedExecutor(t *testing.T) {
	t.Parallel()

	loaded := false
	err := newStreamProcessor(100).runInputScope(context.Background(), plainExecutor{db: openTestDB(t)}, func(*sql.Tx) error {
		loaded = true
		return nil
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
	assert.Contains(t, err.Error(), "unsupported database executor")
	assert.False(t, loaded, "an input with nowhere to be undone must not be loaded at all")
}

// TestStreamReaderToDatabase_UnusableDatabase covers the check for a table of
// the same name, which is the first thing a load asks the database.
func TestStreamReaderToDatabase_UnusableDatabase(t *testing.T) {
	t.Parallel()

	tx := openTestTx(t)
	require.NoError(t, tx.Rollback())

	err := newStreamProcessor(100).streamReaderToDatabase(context.Background(), tx, readerInput{
		reader:    strings.NewReader("id,name\n1,Alice\n"),
		tableName: "users",
		fileType:  FileTypeCSV,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
}

// TestStreamReaderToDatabase_ReservedTableName pins that the reserved namespace
// is refused for readers too, not only for paths.
func TestStreamReaderToDatabase_ReservedTableName(t *testing.T) {
	t.Parallel()

	err := newStreamProcessor(100).streamReaderToDatabase(context.Background(), openTestTx(t), readerInput{
		reader:    strings.NewReader("id\n1\n"),
		tableName: sourceTablePrefix + "report",
		fileType:  FileTypeCSV,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrReservedTableName)
}

// TestCloseReaderInput_ReportsNothingToTheCaller checks that a reader this
// package opened itself is closed, and that a failure to close it does not fail
// the load: the rows are already in the database by then.
func TestCloseReaderInput_ReportsNothingToTheCaller(t *testing.T) {
	t.Parallel()

	closer := &failingCloser{}
	newStreamProcessor(100).closeReaderInput(readerInput{tableName: "users", closer: closer})
	assert.True(t, closer.closed, "a reader opened by this package must be closed")
}

// TestDropIfReplacing covers the drop that lets a reload install its own schema.
func TestDropIfReplacing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("does nothing in open mode", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		// A closed database would fail any statement, so a successful call proves
		// none was sent.
		assert.NoError(t, newStreamProcessor(100).dropIfReplacing(ctx, db, "users"))
	})

	t.Run("reports a drop the database refused", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		sp := newStreamProcessor(100)
		sp.replaceExisting = true

		err := sp.dropIfReplacing(ctx, db, "users")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})
}

// TestLoadTyped_ReadAgain covers the path a file takes when a later chunk
// widens a column: the first attempt is dropped and the file is read again
// under the types the whole of it calls for.
func TestLoadTyped_ReadAgain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const body = "v\n1\n2.50\nabc\n"

	newSource := func(reread func(emit chunkProcessor) (columnInfoList, error)) tableSource {
		return tableSource{
			read: func(emit chunkProcessor) (columnInfoList, error) {
				return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader(body), emit)
			},
			reread: reread,
		}
	}

	t.Run("a second read that does not match the first is refused", func(t *testing.T) {
		t.Parallel()

		tx := openTestTx(t)
		changed := func(emit chunkProcessor) (columnInfoList, error) {
			return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader("v\n1\n2\n"), emit)
		}
		// Through the scope, because undoing a refused load is what the scope is
		// for: loadTable itself only reports.
		sp := newStreamProcessor(1)
		err := sp.runInputScope(ctx, tx, func(scope *sql.Tx) error {
			return sp.loadTable(ctx, scope, "t", newSource(changed))
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrParsing)
		assert.Contains(t, err.Error(), "changed while it was being read")

		var tables int
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table'`).Scan(&tables))
		assert.Equal(t, 0, tables, "a refused load leaves no table behind")
	})

	t.Run("a source that cannot be opened again reports why", func(t *testing.T) {
		t.Parallel()

		failing := func(chunkProcessor) (columnInfoList, error) { return nil, errStub }
		err := newStreamProcessor(1).loadTable(ctx, openTestTx(t), "t", newSource(failing))
		require.ErrorIs(t, err, errStub)
	})

	t.Run("a second read stores the file's text at every row", func(t *testing.T) {
		t.Parallel()

		tx := openTestTx(t)
		again := func(emit chunkProcessor) (columnInfoList, error) {
			return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader(body), emit)
		}
		require.NoError(t, newStreamProcessor(1).loadTable(ctx, tx, "t", newSource(again)))

		rows, err := tx.QueryContext(ctx, `SELECT v FROM t ORDER BY rowid`)
		require.NoError(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var v string
			require.NoError(t, rows.Scan(&v))
			got = append(got, v)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"1", "2.50", "abc"}, got)
	})
}

// TestRunInputScope_CallerTransaction covers a failed load inside a transaction
// the caller owns: rolling back to the savepoint takes the staging table and
// every other trace of the input with it, leaves what the caller had, and
// leaves the transaction itself for the caller to end.
func TestRunInputScope_CallerTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := openTestTx(t)
	_, err := tx.ExecContext(ctx, `CREATE TABLE keep (v)`)
	require.NoError(t, err)

	source := tableSource{read: func(emit chunkProcessor) (columnInfoList, error) {
		return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader("v\n1\n2\nx,y\n"), emit)
	}}
	sp := newStreamProcessor(1)
	err = sp.runInputScope(ctx, tx, func(scope *sql.Tx) error {
		return sp.loadTable(ctx, scope, "t", source)
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrColumnMismatch)

	var left []string
	rows, err := tx.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	require.NoError(t, err, "the caller's transaction is still usable")
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		left = append(left, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	assert.Equal(t, []string{"keep"}, left, "the failed input left nothing in the caller's transaction")
}

// TestUndoInput_TransactionAlreadyEnded covers the undo of an input whose
// transaction is already gone, which is what a load cancelled through the
// context the caller's transaction was built on finds: database/sql has rolled
// the whole transaction back, taking the savepoint with it, and that is the undo
// having happened rather than a failure to report.
func TestUndoInput_TransactionAlreadyEnded(t *testing.T) {
	t.Parallel()

	tx := openTestTx(t)
	require.NoError(t, tx.Rollback())

	assert.NoError(t, undoInput(context.Background(), tx))
}

// TestLoadStaged_TypesTheTableOnce covers the two ways a staged table is
// declared: renamed when every column is TEXT, copied when one is not.
func TestLoadStaged_TypesTheTableOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name     string
		body     string
		wantType string
		wantRows []string
	}{
		{"all text is renamed in place", "a,b\nx,y\n", "TEXT", []string{"x"}},
		{"a numeric column is copied into its type", "a,b\n1,y\n2,z\n", "INTEGER", []string{"1", "2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tx := openTestTx(t)
			source := tableSource{read: func(emit chunkProcessor) (columnInfoList, error) {
				return newStreamingParser(FileTypeCSV, CompressionNone, "t", 1).ProcessInChunks(strings.NewReader(tt.body), emit)
			}}
			require.NoError(t, newStreamProcessor(1).loadTable(ctx, tx, "t", source))

			var declared string
			require.NoError(t, tx.QueryRowContext(ctx, `SELECT type FROM pragma_table_info('t') WHERE name = 'a'`).Scan(&declared))
			assert.Equal(t, tt.wantType, declared)

			var names []string
			rows, err := tx.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table'`)
			require.NoError(t, err)
			for rows.Next() {
				var name string
				require.NoError(t, rows.Scan(&name))
				names = append(names, name)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			assert.Equal(t, []string{"t"}, names, "the staging table is gone")

			rows, err = tx.QueryContext(ctx, `SELECT a FROM t ORDER BY rowid`)
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var a string
				require.NoError(t, rows.Scan(&a))
				got = append(got, a)
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, tt.wantRows, got)
		})
	}
}

// TestLoadTable_SourceWithoutChunks covers a source that returns without
// emitting a chunk, which every reader in this package is written not to do.
func TestLoadTable_SourceWithoutChunks(t *testing.T) {
	t.Parallel()

	silent := func(chunkProcessor) (columnInfoList, error) { return nil, nil }
	for name, source := range map[string]tableSource{
		"once":  {read: silent},
		"twice": {read: silent, reread: silent},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := newStreamProcessor(1).loadTable(context.Background(), openTestTx(t), "t", source)
			require.ErrorIs(t, err, ErrEmptyData)
		})
	}
}

// TestAddFS_ReadsAFileAgainWhenAColumnWidens covers the reopen an fs.FS input
// carries: a file whose column widens late is read twice and stored as written.
func TestAddFS_ReadsAFileAgainWhenAColumnWidens(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockFS := fstest.MapFS{"t.csv": &fstest.MapFile{Data: []byte("v\n1\n2.50\nabc\n")}}
	built, err := NewBuilder().AddFS(mockFS).SetDefaultChunkSize(1).Build(ctx)
	require.NoError(t, err)
	db, err := built.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.QueryContext(ctx, `SELECT v FROM t ORDER BY rowid`)
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		got = append(got, v)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"1", "2.50", "abc"}, got)
}
