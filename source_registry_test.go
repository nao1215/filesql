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

// TestSourceTableIsHiddenFromCallers pins that the reserved table holding
// write-back metadata is neither listed as a user table nor dumped as one.
func TestSourceTableIsHiddenFromCallers(t *testing.T) {
	ctx := context.Background()

	db, err := Open(ctx, copyACHFixture(t, "ppd-debit.ach"))
	require.NoError(t, err)
	defer db.Close()

	names, err := getSQLiteTableNames(context.Background(), db)
	require.NoError(t, err)
	assert.NotContains(t, names, sourceTableName, "the reserved table must not be listed as a user table")

	outputDir := t.TempDir()
	require.NoError(t, DumpDatabase(context.Background(), db, outputDir))

	entries, err := os.ReadDir(outputDir)
	require.NoError(t, err)
	for _, entry := range entries {
		assert.False(t, strings.HasPrefix(entry.Name(), "_filesql_"),
			"the reserved table must not be dumped as a file: %s", entry.Name())
	}
}

// TestACHAndFedwireShareABaseNameInOneDatabase pins that payment.ach and
// payment.fed can live in one database and each still be dumped. Their SQL
// tables do not collide, so both load; keying the source metadata by name alone
// would let the second load erase the first's source.
func TestACHAndFedwireShareABaseNameInOneDatabase(t *testing.T) {
	ctx := context.Background()

	db, err := Open(ctx, copyACHFixture(t, "ppd-debit.ach"), copyWireFixture(t, "customer-transfer.fed"))
	require.NoError(t, err)
	defer db.Close()

	outDir := t.TempDir()
	require.NoError(t, DumpACH(ctx, db, "payment", filepath.Join(outDir, "out.ach")))
	require.NoError(t, DumpFedWire(ctx, db, "payment", filepath.Join(outDir, "out.fed")))

	assert.NotEmpty(t, readFileString(t, filepath.Join(outDir, "out.ach")))
	assert.Contains(t, readFileString(t, filepath.Join(outDir, "out.fed")), "{1500}")
}

// TestReservedTableNameIsRefused pins that the reserved prefix is reserved in
// both directions. Hiding _filesql_ tables from dumps and listings while still
// loading a file named _filesql_report.csv into one would leave that table
// queryable but absent from everything that enumerates tables, so the load is
// refused instead, the way SQLite refuses its own sqlite_ prefix.
func TestReservedTableNameIsRefused(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	csvPath := filepath.Join(dir, "_filesql_report.csv")
	require.NoError(t, os.WriteFile(csvPath, []byte("id,v\n1,a\n"), 0o600))

	_, err := Open(ctx, csvPath)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrReservedTableName)
	assert.Contains(t, err.Error(), "_filesql_")

	// A reader names its own table, so it can reach the namespace too.
	builder, err := buildForTest(

		ctx, NewBuilder().
			AddReader(strings.NewReader("id\n1\n"), "_filesql_sources", FileTypeCSV))

	require.NoError(t, err)
	_, err = builder.Open(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrReservedTableName)

	// The LIKE that hides these tables folds ASCII case, so the refusal must
	// too: an upper-case spelling used to load and then vanish from every
	// listing while still answering queries.
	upperPath := filepath.Join(dir, "_FILESQL_report.csv")
	require.NoError(t, os.WriteFile(upperPath, []byte("id,v\n1,a\n"), 0o600))
	_, err = Open(ctx, upperPath)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrReservedTableName)

	// A name that merely resembles the prefix is a normal table.
	okPath := filepath.Join(dir, "filesql_report.csv")
	require.NoError(t, os.WriteFile(okPath, []byte("id,v\n1,a\n"), 0o600))
	db, err := Open(ctx, okPath)
	require.NoError(t, err)
	defer db.Close()

	names, err := getSQLiteTableNames(context.Background(), db)
	require.NoError(t, err)
	assert.Contains(t, names, "filesql_report")
}

// TestSQLitePrefixIsRefusedTheSameWay pins the other reserved namespace. SQLite
// keeps sqlite_ for itself, and a file named for it used to reach CREATE TABLE
// and come back as that library's "object name reserved for internal use"
// wrapped in a database-operation error: unmatchable, silent about what to do,
// and raised only after the file had been read.
func TestSQLitePrefixIsRefusedTheSameWay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	for _, name := range []string{"sqlite_stat1.csv", "SQLite_Notes.csv"} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte("id,v\n1,a\n"), 0o600))

		db, err := Open(ctx, path)
		if db != nil {
			assert.NoError(t, db.Close())
		}
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrReservedTableName)
		assert.Contains(t, err.Error(), "sqlite_")
	}

	// A reader names its own table, so it can reach that namespace too.
	builder, err := buildForTest(

		ctx, NewBuilder().
			AddReader(strings.NewReader("id\n1\n"), "sqlite_x", FileTypeCSV))

	require.NoError(t, err)
	db, err := builder.Open(ctx)
	if db != nil {
		assert.NoError(t, db.Close())
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrReservedTableName)

	// The prefix is sqlite_, not sqlite, so a name that merely starts with the
	// letters is a normal table.
	okPath := filepath.Join(dir, "sqliteish.csv")
	require.NoError(t, os.WriteFile(okPath, []byte("id,v\n1,a\n"), 0o600))
	ok, err := Open(ctx, okPath)
	require.NoError(t, err)
	defer ok.Close()

	names, err := getSQLiteTableNames(context.Background(), ok)
	require.NoError(t, err)
	assert.Contains(t, names, "sqliteish")
}

// TestSourceMetadataRolledBackWithTransaction pins that metadata written by a
// load shares the fate of the tables it describes. A rolled-back load must not
// leave a row pointing at tables that do not exist.
func TestSourceMetadataRolledBackWithTransaction(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "rollback.db"))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)

	builder, err := buildForTest(ctx, NewBuilder().AddPath(copyACHFixture(t, "ppd-debit.ach")))
	require.NoError(t, err)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, builder.LoadIntoTx(ctx, tx))
	require.NoError(t, tx.Rollback())

	var count int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", sourceTableName).Scan(&count)
	require.NoError(t, err)
	assert.Zero(t, count, "a rolled-back load must leave no write-back metadata behind")
}

// TestSourceMetadataSurvivesCommit is the other half: a committed load can be
// dumped, because its metadata was committed with the tables.
func TestSourceMetadataSurvivesCommit(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "commit.db"))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)

	builder, err := buildForTest(ctx, NewBuilder().AddPath(copyACHFixture(t, "ppd-debit.ach")))
	require.NoError(t, err)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, builder.LoadIntoTx(ctx, tx))
	require.NoError(t, tx.Commit())

	out := filepath.Join(t.TempDir(), "out.ach")
	require.NoError(t, DumpACH(ctx, db, "payment", out))
	assert.NotEmpty(t, readFileString(t, out))
}

// TestAutoSaveWritesACHFromSourceMetadata pins that auto-save keeps working
// through the metadata table: it has to find the ACH files a connection loaded
// without a process-global list of them.
func TestAutoSaveWritesACHFromSourceMetadata(t *testing.T) {
	ctx := context.Background()

	outputDir := t.TempDir()
	builder, err := buildForTest(

		ctx, NewBuilder().
			AddPath(copyACHFixture(t, "ppd-debit.ach")).
			EnableAutoSave(outputDir, NewDumpOptions().WithFormat(OutputFormatACH)))

	require.NoError(t, err)

	db, err := builder.Open(ctx)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "UPDATE payment_entries SET amount = 100")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	saved := filepath.Join(outputDir, "payment.ach")
	assert.NotEmpty(t, readFileString(t, saved), "auto-save must write the ACH file back")
}

// TestReaderLoadedACHCannotBeDumped pins the documented limit of write-back:
// the ACH writer edits a copy of the original file, so a load with no file
// behind it has nothing to edit. The failure names the base table and points at
// the function that takes a TableSet directly.
func TestReaderLoadedACHCannotBeDumped(t *testing.T) {
	ctx := context.Background()

	content, err := os.ReadFile(filepath.Join("testdata", "ppd-debit.ach"))
	require.NoError(t, err)

	builder, err := buildForTest(

		ctx, NewBuilder().
			AddReader(strings.NewReader(string(content)), "payment", FileTypeACH))

	require.NoError(t, err)

	db, err := builder.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	err = DumpACH(ctx, db, "payment", filepath.Join(t.TempDir(), "out.ach"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payment")
	assert.Contains(t, err.Error(), "DumpACHWithSource")
}

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
