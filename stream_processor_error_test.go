package filesql

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// plainExecutor is a DBTX that is neither *sql.DB nor *sql.Tx. The chunk loader
// needs one of those two to open its own transaction, so a caller's own
// implementation has to be refused by name rather than crashing on a type
// assertion.
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

	err := newStreamProcessor(100).streamFileToDatabase(context.Background(), openTestDB(t), path)
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
			err := newStreamProcessor(100).streamFileToDatabase(ctx, openTestDB(t), path)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrIOOperation)
		})

		t.Run(tt.name+" file with no bytes in it", func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "empty"+tt.ext)
			require.NoError(t, os.WriteFile(path, nil, 0o600))

			err := newStreamProcessor(100).streamFileToDatabase(ctx, openTestDB(t), path)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrEmptyData)
		})
	}
}

// TestStreamReaderToDatabase_UnsupportedExecutor covers a DBTX the loader cannot
// start a transaction on. It is refused with the type in the message, because a
// caller who passed their own wrapper has no other way to tell what was wrong.
func TestStreamReaderToDatabase_UnsupportedExecutor(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	err := newStreamProcessor(100).streamReaderToDatabase(context.Background(), plainExecutor{db: db}, readerInput{
		reader:    strings.NewReader("id,name\n1,Alice\n"),
		tableName: "users",
		fileType:  FileTypeCSV,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
	assert.Contains(t, err.Error(), "unsupported database executor")
}

// TestStreamReaderToDatabase_UnusableDatabase covers the check for a table of
// the same name, which is the first thing a load asks the database.
func TestStreamReaderToDatabase_UnusableDatabase(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	require.NoError(t, db.Close())

	err := newStreamProcessor(100).streamReaderToDatabase(context.Background(), db, readerInput{
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

	err := newStreamProcessor(100).streamReaderToDatabase(context.Background(), openTestDB(t), readerInput{
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

// TestCreateEmptyTable covers the header-only file, which is a valid input that
// produces a table with no rows.
func TestCreateEmptyTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("creates the columns the header names", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		input := readerInput{
			reader:    strings.NewReader("id,name\n"),
			tableName: "users",
			fileType:  FileTypeCSV,
		}
		require.NoError(t, newStreamProcessor(100).createEmptyTable(ctx, db, input))

		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&count))
		assert.Equal(t, 0, count, "a header-only file loads as a table with no rows")

		rows, err := db.QueryContext(ctx, `SELECT * FROM users`)
		require.NoError(t, err)
		defer rows.Close()
		columns, err := rows.Columns()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, columns)
		require.NoError(t, rows.Err())
	})

	t.Run("keeps a duplicate column refusal", func(t *testing.T) {
		t.Parallel()

		input := readerInput{
			reader:    strings.NewReader("id,id\n"),
			tableName: "users",
			fileType:  FileTypeCSV,
		}
		err := newStreamProcessor(100).createEmptyTable(ctx, openTestDB(t), input)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column name", "the parser's own refusal must not be replaced by a fallback table")
	})

	t.Run("reports a database that cannot take the table", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		input := readerInput{
			reader:    strings.NewReader("id,name\n"),
			tableName: "users",
			fileType:  FileTypeCSV,
		}
		err := newStreamProcessor(100).createEmptyTable(ctx, db, input)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})
}

// TestCreateTableFromHeaders covers the fallback used when the header cannot be
// parsed at all: the file still becomes a table, so a later query names a table
// that exists instead of failing on a missing one.
func TestCreateTableFromHeaders(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("creates a single-column table", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		input := readerInput{tableName: "users", fileType: FileTypeCSV}
		require.NoError(t, newStreamProcessor(100).createTableFromHeaders(ctx, db, input))

		var name string
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name='users'`).Scan(&name))
		assert.Equal(t, "users", name)
	})

	t.Run("reports a database that cannot take the table", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		input := readerInput{tableName: "users", fileType: FileTypeCSV}
		err := newStreamProcessor(100).createTableFromHeaders(ctx, db, input)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})
}
