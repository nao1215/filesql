package filesql

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openTestDB opens an empty on-disk database for a test and closes it afterwards.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestRecordFileSource_ReportsAnUnusableDatabase covers the two writes the
// bookkeeping makes. They run on the caller's own dbtx, so a database that
// cannot take them has to be reported rather than leaving tables whose source is
// silently unrecorded — a later dump would then refuse with a puzzling
// "no source recorded".
func TestRecordFileSource_ReportsAnUnusableDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("the source table cannot be created", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		err := recordFileSource(ctx, db, "payment", "payment.ach", sourceFormatACH)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("the row cannot be inserted", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		// A table of that name with other columns is left alone by CREATE TABLE IF
		// NOT EXISTS, so the insert is what fails.
		_, err := db.ExecContext(ctx, `CREATE TABLE "`+sourceTableName+`" (unrelated TEXT)`)
		require.NoError(t, err)

		err = recordFileSource(ctx, db, "payment", "payment.ach", sourceFormatACH)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("a reader load records nothing", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, recordFileSource(ctx, db, "payment", "", sourceFormatACH))

		_, ok := fileSourcePath(ctx, db, "payment", sourceFormatACH)
		assert.False(t, ok, "a load with no file behind it has no source to go back to")
	})
}

// TestFileSourceBaseNames_UnreadableRows checks the listing used by a dump of
// every loaded file. A row it cannot read means the set of files to write is
// unknown, so it answers with nothing rather than a partial set that would dump
// some files and silently skip others.
func TestFileSourceBaseNames_UnreadableRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	_, err := db.ExecContext(ctx, `CREATE TABLE "`+sourceTableName+`" (base_table_name TEXT, source_path TEXT, format TEXT)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO "`+sourceTableName+`" VALUES (NULL, '/tmp/payment.ach', 'ach')`)
	require.NoError(t, err)

	assert.Nil(t, fileSourceBaseNames(ctx, db, sourceFormatACH), "a row that cannot be read yields no names")
}

// TestTableSetForDump_UnparsableSource covers the reread a write-back format
// depends on. The tables alone cannot rebuild the file, so a source that no
// longer parses has to be reported instead of writing a file built from
// whatever was salvageable.
func TestTableSetForDump_UnparsableSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("ACH reread", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		path := filepath.Join(t.TempDir(), "payment.ach")
		require.NoError(t, os.WriteFile(path, []byte("this is not an ACH file"), 0o600))
		require.NoError(t, recordFileSource(ctx, db, "payment", path, sourceFormatACH))

		_, err := achTableSetForDump(ctx, db, "payment")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrACH)
	})

	t.Run("Fedwire", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		path := filepath.Join(t.TempDir(), "payment.fed")
		require.NoError(t, os.WriteFile(path, []byte("this is not a Fedwire file"), 0o600))
		require.NoError(t, recordFileSource(ctx, db, "payment", path, sourceFormatFedWire))

		_, err := wireTableSetForDump(ctx, db, "payment")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrWire)
	})

	t.Run("the recorded file is gone", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		path := filepath.Join(t.TempDir(), "payment.ach")
		require.NoError(t, recordFileSource(ctx, db, "payment", path, sourceFormatACH))

		_, err := achTableSetForDump(ctx, db, "payment")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSourceUnavailable)
	})
}
