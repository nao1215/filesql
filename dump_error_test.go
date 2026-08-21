package filesql

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDumpDatabase_UnusableConnection covers the first thing a dump asks for.
// Without a connection there is nothing to read, and the caller's output
// directory must be left as it was.
func TestDumpDatabase_UnusableConnection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	require.NoError(t, db.Close())

	outputDir := filepath.Join(t.TempDir(), "out")
	err := DumpDatabase(db, outputDir)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
	assert.NoDirExists(t, outputDir, "a dump that never started must not leave a directory behind")
}

// TestDumpSQLiteDatabase_Failures covers the steps between the connection and
// the first table.
func TestDumpSQLiteDatabase_Failures(t *testing.T) {
	t.Parallel()

	t.Run("the output directory cannot be created", func(t *testing.T) {
		t.Parallel()

		blocked := filepath.Join(t.TempDir(), "in-the-way")
		require.NoError(t, os.WriteFile(blocked, nil, 0o600))

		// The directory is created once there is something to write, so the
		// database needs a table for this path to be reached at all.
		db := openTestDB(t)
		_, err := db.ExecContext(context.Background(), `CREATE TABLE t (a TEXT)`)
		require.NoError(t, err)

		err = dumpSQLiteDatabase(db, filepath.Join(blocked, "out"), NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrIOOperation)
	})

	t.Run("a database with no tables leaves no directory", func(t *testing.T) {
		t.Parallel()

		outputDir := filepath.Join(t.TempDir(), "out")
		err := dumpSQLiteDatabase(openTestDB(t), outputDir, NewDumpOptions())
		require.ErrorIs(t, err, ErrNoTables)
		assert.NoDirExists(t, outputDir, "a dump with nothing to write must not leave a directory behind")
	})

	t.Run("the tables cannot be listed", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		err := dumpSQLiteDatabase(db, filepath.Join(t.TempDir(), "out"), NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("a database with no table", func(t *testing.T) {
		t.Parallel()

		err := dumpSQLiteDatabase(openTestDB(t), filepath.Join(t.TempDir(), "out"), NewDumpOptions())
		assert.ErrorIs(t, err, ErrNoTables)
	})
}

// TestDumpSQLiteDatabase_WriteBackSourceIsGone covers a dump of a database whose
// ACH or Fedwire source file has disappeared since the load. Those files are
// rebuilt from the original, so the dump fails naming the format rather than
// writing a file with the fields only the original carries left empty.
func TestDumpSQLiteDatabase_WriteBackSourceIsGone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name   string
		source string
		format sourceFormat
		want   error
	}{
		{"ACH source", "payment.ach", sourceFormatACH, ErrACH},
		{"Fedwire", "payment.fed", sourceFormatFedWire, ErrWire},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			_, err := db.ExecContext(ctx, `CREATE TABLE payment_entries (id TEXT)`)
			require.NoError(t, err)
			require.NoError(t, recordFileSource(ctx, db, "payment", filepath.Join(t.TempDir(), tt.source), tt.format))

			err = dumpSQLiteDatabase(db, filepath.Join(t.TempDir(), "out"), NewDumpOptions())
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.want)
		})
	}
}

// TestDumpSQLiteTable_UnreadableTable covers the per-table step of a dump.
func TestDumpSQLiteTable_UnreadableTable(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	require.NoError(t, db.Close())

	err := dumpSQLiteTable(db, "users", t.TempDir(), NewDumpOptions())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
}
