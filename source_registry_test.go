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

	db, err := OpenContext(ctx, copyACHFixture(t, "ppd-debit.ach"))
	require.NoError(t, err)
	defer db.Close()

	names, err := getSQLiteTableNames(db)
	require.NoError(t, err)
	assert.NotContains(t, names, sourceTableName, "the reserved table must not be listed as a user table")

	outputDir := t.TempDir()
	require.NoError(t, DumpDatabase(db, outputDir))

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

	db, err := OpenContext(ctx, copyACHFixture(t, "ppd-debit.ach"), copyWireFixture(t, "customer-transfer.fed"))
	require.NoError(t, err)
	defer db.Close()

	outDir := t.TempDir()
	require.NoError(t, DumpACH(ctx, db, "payment", filepath.Join(outDir, "out.ach")))
	require.NoError(t, DumpFedWire(ctx, db, "payment", filepath.Join(outDir, "out.fed")))

	assert.NotEmpty(t, readFileString(t, filepath.Join(outDir, "out.ach")))
	assert.Contains(t, readFileString(t, filepath.Join(outDir, "out.fed")), "{1500}")
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

	builder, err := NewBuilder().AddPath(copyACHFixture(t, "ppd-debit.ach")).Build(ctx)
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

	builder, err := NewBuilder().AddPath(copyACHFixture(t, "ppd-debit.ach")).Build(ctx)
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
	builder, err := NewBuilder().
		AddPath(copyACHFixture(t, "ppd-debit.ach")).
		EnableAutoSave(outputDir, NewDumpOptions().WithFormat(OutputFormatACH)).
		Build(ctx)
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

	builder, err := NewBuilder().
		AddReader(strings.NewReader(string(content)), "payment", FileTypeACH).
		Build(ctx)
	require.NoError(t, err)

	db, err := builder.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	err = DumpACH(ctx, db, "payment", filepath.Join(t.TempDir(), "out.ach"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payment")
	assert.Contains(t, err.Error(), "DumpACHWithTableSet")
}
