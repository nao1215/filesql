package filesql

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nao1215/filesql/dialect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
	_ "modernc.org/sqlite"
)

// newCallerDB returns an in-memory SQLite database owned by the test, standing
// in for a caller-managed database. ":memory:" is private per connection, so the
// pool is pinned to one connection; otherwise tables created on one connection
// would be invisible to the next.
func newCallerDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	return db
}

func listTables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY name`)
	require.NoError(t, err)
	defer func() { assert.NoError(t, rows.Close()) }()
	var names []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	require.NoError(t, rows.Err())
	return names
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM "`+table+`"`).Scan(&n))
	return n
}

func writeTempCSV(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestLoadInto_PackageFunc_CSV(t *testing.T) {
	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, filepath.Join("testdata", "sample.csv")))

	assert.Equal(t, []string{"sample"}, listTables(t, db))
	assert.Equal(t, 3, countRows(t, db, "sample"))

	var name string
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT name FROM sample WHERE id = 1`).Scan(&name))
	assert.Equal(t, "John Doe", name)
}

func TestDBBuilder_LoadIntoTxUsesCallerTransaction(t *testing.T) {
	dir := t.TempDir()
	path := writeTempCSV(t, dir, "tx.csv", "id\n1\n")
	db := newCallerDB(t)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	builder, err := buildForTest(context.Background(), NewBuilder().AddPath(path))
	require.NoError(t, err)
	require.NoError(t, builder.LoadIntoTx(context.Background(), tx))

	var count int
	require.NoError(t, tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM tx`).Scan(&count))
	assert.Equal(t, 1, count)
	// The API must not commit the caller's transaction itself.
	assert.NoError(t, tx.Rollback())
	_, err = db.ExecContext(context.Background(), `SELECT * FROM tx`)
	assert.Error(t, err)
}

func TestDBBuilder_LoadIntoTxACHIsDumpableOnlyAfterCommit(t *testing.T) {
	db := newCallerDB(t)
	builder, err := buildForTest(context.Background(), NewBuilder().AddPath(filepath.Join("testdata", "ppd-debit.ach")))
	require.NoError(t, err)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	require.NoError(t, builder.LoadIntoTx(context.Background(), tx))
	require.NoError(t, tx.Commit())

	out := filepath.Join(t.TempDir(), "out.ach")
	require.NoError(t, DumpACH(context.Background(), db, "ppd_debit", out))
}

func TestDBBuilder_LoadIntoTxDiscardsACHMetadataOnFailure(t *testing.T) {
	dir := t.TempDir()
	bad := writeTempCSV(t, dir, "broken.json", "[")
	db := newCallerDB(t)
	builder, err := buildForTest(

		context.Background(), NewBuilder().
			AddPath(filepath.Join("testdata", "ppd-debit.ach")).
			AddPath(bad))

	require.NoError(t, err)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.Error(t, builder.LoadIntoTx(context.Background(), tx))
	require.NoError(t, tx.Rollback())
	assert.Empty(t, listTables(t, db), "failed load must be rolled back")
}

func TestDBBuilder_LoadIntoTxDiscardsFedwireMetadataOnFailure(t *testing.T) {
	dir := t.TempDir()
	bad := writeTempCSV(t, dir, "broken.json", "[")
	db := newCallerDB(t)
	builder, err := buildForTest(

		context.Background(), NewBuilder().
			AddPath(filepath.Join("testdata", "customer-transfer.fed")).
			AddPath(bad))

	require.NoError(t, err)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.Error(t, builder.LoadIntoTx(context.Background(), tx))
	require.NoError(t, tx.Rollback())
	assert.Empty(t, listTables(t, db), "failed load must be rolled back")
}

func TestLoadInto_PreservesExistingCallerTables(t *testing.T) {
	db := newCallerDB(t)
	_, err := db.ExecContext(context.Background(), `CREATE TABLE app (k TEXT); INSERT INTO app VALUES ('keep')`)
	require.NoError(t, err)

	require.NoError(t, LoadInto(context.Background(), db, filepath.Join("testdata", "sample.csv")))

	assert.Equal(t, []string{"app", "sample"}, listTables(t, db))
	assert.Equal(t, 1, countRows(t, db, "app")) // caller's table is untouched
	assert.Equal(t, 3, countRows(t, db, "sample"))
}

func TestLoadInto_ReplacesSameNamedTable(t *testing.T) {
	db := newCallerDB(t)
	// Pre-create a table named like the file; loading must replace, not append.
	_, err := db.ExecContext(context.Background(), `CREATE TABLE sample (x TEXT); INSERT INTO sample VALUES ('old1'); INSERT INTO sample VALUES ('old2')`)
	require.NoError(t, err)

	require.NoError(t, LoadInto(context.Background(), db, filepath.Join("testdata", "sample.csv")))

	assert.Equal(t, 3, countRows(t, db, "sample")) // file rows, not 2 old + 3 new
	// The replaced table has the file's schema (id column exists).
	var id int
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT id FROM sample ORDER BY id LIMIT 1`).Scan(&id))
	assert.Equal(t, 1, id)
}

func TestLoadInto_IncrementalAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	a := writeTempCSV(t, dir, "left.csv", "id,word\n1,hello\n")
	b := writeTempCSV(t, dir, "right.csv", "id,word\n1,world\n")

	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, a))
	require.NoError(t, LoadInto(context.Background(), db, b))

	assert.Equal(t, []string{"left", "right"}, listTables(t, db))

	// Cross-source JOIN proves both live in one database.
	var joined string
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT left.word || ' ' || right.word FROM left JOIN right ON left.id = right.id`,
	).Scan(&joined))
	assert.Equal(t, "hello world", joined)
}

func TestLoadInto_LastWinsWithinOneCall(t *testing.T) {
	dir := t.TempDir()
	sub1 := filepath.Join(dir, "d1")
	sub2 := filepath.Join(dir, "d2")
	require.NoError(t, os.MkdirAll(sub1, 0o750))
	require.NoError(t, os.MkdirAll(sub2, 0o750))
	first := writeTempCSV(t, sub1, "users.csv", "id\n1\n2\n")           // 2 rows
	second := writeTempCSV(t, sub2, "users.csv", "id\n9\n8\n7\n6\n5\n") // 5 rows

	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, first, second))

	assert.Equal(t, []string{"users"}, listTables(t, db))
	assert.Equal(t, 5, countRows(t, db, "users")) // last file wins
}

func TestLoadInto_Directory(t *testing.T) {
	dir := t.TempDir()
	writeTempCSV(t, dir, "one.csv", "a\n1\n")
	writeTempCSV(t, dir, "two.csv", "b\n2\n")

	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, dir))

	assert.Equal(t, []string{"one", "two"}, listTables(t, db))
}

func TestLoadInto_CompressedCSV(t *testing.T) {
	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, filepath.Join("testdata", "sample.csv.gz")))
	assert.Equal(t, []string{"sample"}, listTables(t, db))
	assert.Positive(t, countRows(t, db, "sample"))
}

func TestLoadInto_Parquet(t *testing.T) {
	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, filepath.Join("testdata", "products.parquet")))
	assert.Equal(t, []string{"products"}, listTables(t, db))
	assert.Positive(t, countRows(t, db, "products"))
}

func TestLoadInto_JSONL(t *testing.T) {
	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, filepath.Join("testdata", "sample.jsonl")))
	assert.Equal(t, []string{"sample"}, listTables(t, db))
	assert.Positive(t, countRows(t, db, "sample"))
}

func TestLoadInto_Excel(t *testing.T) {
	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, filepath.Join("testdata", "excel", "sample.xlsx")))
	tables := listTables(t, db)
	require.NotEmpty(t, tables)
	for _, name := range tables {
		assert.True(t, strings.HasPrefix(name, "sample_"), "excel sheet table %q should be prefixed with the file name", name)
	}
}

func TestLoadInto_ACH_MultiTableAndReplaceOnReload(t *testing.T) {
	db := newCallerDB(t)
	ach := filepath.Join("testdata", "ppd-debit.ach")

	require.NoError(t, LoadInto(context.Background(), db, ach))
	first := listTables(t, db)
	require.GreaterOrEqual(t, len(first), 2, "ACH file should produce multiple tables")

	// Re-loading the same ACH file must replace its tables, not error with
	// ErrDuplicateTable.
	require.NoError(t, LoadInto(context.Background(), db, ach))
	second := listTables(t, db)
	assert.Equal(t, first, second)
}

func TestLoadInto_Fedwire(t *testing.T) {
	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, filepath.Join("testdata", "customer-transfer.fed")))
	assert.NotEmpty(t, listTables(t, db))
}

func TestLoadInto_PreservesTypeInference(t *testing.T) {
	dir := t.TempDir()
	csv := writeTempCSV(t, dir, "typed.csv", "n,label\n1,a\n2,b\n")

	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, csv))

	types := map[string]string{}
	rows, err := db.QueryContext(context.Background(), `PRAGMA table_info("typed")`)
	require.NoError(t, err)
	defer func() { assert.NoError(t, rows.Close()) }()
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt sql.NullString
		require.NoError(t, rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk))
		types[name] = typ
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, "INTEGER", types["n"], "integer column should keep INTEGER affinity")
}

func TestLoadInto_BuilderReaders(t *testing.T) {
	db := newCallerDB(t)
	builder, err := buildForTest(

		context.Background(), NewBuilder().
			AddReader(strings.NewReader("id,v\n1,x\n2,y\n"), "from_reader", FileTypeCSV))

	require.NoError(t, err)
	require.NoError(t, builder.LoadInto(context.Background(), db))

	assert.Equal(t, []string{"from_reader"}, listTables(t, db))
	assert.Equal(t, 2, countRows(t, db, "from_reader"))
}

func TestLoadInto_Errors(t *testing.T) {
	t.Run("nil database", func(t *testing.T) {
		err := LoadInto(context.Background(), nil, filepath.Join("testdata", "sample.csv"))
		require.Error(t, err)
	})

	t.Run("nonexistent path", func(t *testing.T) {
		db := newCallerDB(t)
		err := LoadInto(context.Background(), db, filepath.Join("testdata", "does_not_exist.csv"))
		require.Error(t, err)
	})

	t.Run("unsupported format", func(t *testing.T) {
		dir := t.TempDir()
		bad := filepath.Join(dir, "data.unknown")
		require.NoError(t, os.WriteFile(bad, []byte("x"), 0o600))
		db := newCallerDB(t)
		err := LoadInto(context.Background(), db, bad)
		require.Error(t, err)
	})

	t.Run("canceled context", func(t *testing.T) {
		db := newCallerDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := LoadInto(ctx, db, filepath.Join("testdata", "sample.csv"))
		require.Error(t, err)
	})

	t.Run("auto-save configured is rejected", func(t *testing.T) {
		db := newCallerDB(t)
		builder, err := buildForTest(

			context.Background(), NewBuilder().
				AddPath(filepath.Join("testdata", "sample.csv")).
				EnableAutoSave(t.TempDir()))

		require.NoError(t, err)
		err = builder.LoadInto(context.Background(), db)
		require.Error(t, err)
	})
}

// TestLoadIntoRefusesADialect holds the rule that an option which cannot reach
// the caller's database is refused rather than dropped. The dialect is a
// connector wrapping the database this package returns, so it cannot wrap one
// the caller opened -- the same reason auto-save cannot, and auto-save has
// always been refused by name. The dialect was accepted and then did nothing,
// and the caller's first dialect query answered SQLite's tokenizer error about a
// token they had not written.
func TestLoadIntoRefusesADialect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := filepath.Join("testdata", "sample.csv")

	for _, d := range []dialect.Dialect{dialect.MySQL, dialect.PostgreSQL, dialect.GoogleSQL} {
		t.Run(string(d), func(t *testing.T) {
			t.Parallel()

			t.Run("LoadInto", func(t *testing.T) {
				db := newCallerDB(t)
				err := NewBuilder().AddPath(src).WithDialect(d).LoadInto(ctx, db)

				require.Error(t, err)
				assert.ErrorIs(t, err, ErrDatabaseOperation)
				assert.Contains(t, err.Error(), string(d), "the error must name the dialect")
				assert.Empty(t, listTables(t, db), "nothing may be loaded into the caller's database")
			})

			t.Run("LoadIntoTx", func(t *testing.T) {
				db := newCallerDB(t)
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer func() { _ = tx.Rollback() }()

				err = NewBuilder().AddPath(src).WithDialect(d).LoadIntoTx(ctx, tx)

				require.Error(t, err)
				assert.ErrorIs(t, err, ErrDatabaseOperation)
				assert.Contains(t, err.Error(), string(d))
			})
		})
	}

	t.Run("SQLite asks for no translation and loads", func(t *testing.T) {
		t.Parallel()

		for _, d := range []dialect.Dialect{"", dialect.SQLite} {
			db := newCallerDB(t)
			require.NoError(t, NewBuilder().AddPath(src).WithDialect(d).LoadInto(ctx, db))
			assert.NotEmpty(t, listTables(t, db))
		}
	})
}

func TestLoadInto_DoesNotLeakReplaceModeToBuilder(t *testing.T) {
	db := newCallerDB(t)
	builder, err := buildForTest(

		context.Background(), NewBuilder().
			AddPath(filepath.Join("testdata", "sample.csv")))

	require.NoError(t, err)
	require.NoError(t, builder.LoadInto(context.Background(), db))

	// Replace mode must not persist on the builder; a later Open must see the
	// default (non-replace) behavior.
	assert.False(t, builder.streamProcessor.replaceExisting,
		"LoadInto must reset replaceExisting so reusing the builder is unaffected")
}

func TestLoadInto_DoesNotCloseCallerDB(t *testing.T) {
	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, filepath.Join("testdata", "sample.csv")))
	// The caller still owns the connection; it must remain usable.
	require.NoError(t, db.PingContext(context.Background()))

	got := listTables(t, db)
	sort.Strings(got)
	assert.Equal(t, []string{"sample"}, got)
}

// TestLoadInto_DumpDatabaseOnCallerDB runs the cycle this package documents for
// a caller-managed database from end to end: load files into it, query it, then
// write it back out.
//
// The last step used to hang. DumpDatabase took a connection out of the pool
// that it never read from and held it for the whole dump, so on the pool this
// package tells the caller to pin to one connection every query the dump made
// waited for the connection the dump itself was sitting on. The call returned
// no error and never timed out; a caller who ran it in a goroutine leaked one.
// The timeout below is a guard, not a measurement: a regression must fail the
// test rather than hang the suite.
func TestLoadInto_DumpDatabaseOnCallerDB(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	source := filepath.Join(dir, "users.csv")
	require.NoError(t, os.WriteFile(source, []byte("id,name\n1,alice\n2,bob\n"), 0o600))

	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, source))
	require.Equal(t, 2, countRows(t, db, "users"))

	outputDir := filepath.Join(dir, "out")
	done := make(chan error, 1)
	go func() { done <- DumpDatabase(db, outputDir) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("DumpDatabase did not return: it is waiting for a connection it is holding itself")
	}

	got, err := os.ReadFile(filepath.Join(outputDir, "users.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,alice\n2,bob\n", string(got))
}

// TestLoadInto_FailedLoadLeavesTheDatabaseAsItWas pins that a load which fails
// partway does not take the caller's data with it.
//
// The table was created, and an existing one of the same name dropped, outside
// the transaction the rows were inserted in. So a failed load rolled back the
// rows and kept the empty table: a reload that failed left the caller holding a
// table that answers queries and returns nothing, where their rows had been.
// The error said the load failed and the database said the file was empty.
func TestLoadInto_FailedLoadLeavesTheDatabaseAsItWas(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "big.csv")
	require.NoError(t, os.WriteFile(path, []byte("id,payload\n1,first load\n2,second row\n"), 0o600))

	db := newCallerDB(t)
	require.NoError(t, LoadInto(context.Background(), db, path))
	require.Equal(t, 2, countRows(t, db, "big"))

	// The same table name, reloaded from a file large enough that the load is
	// cut short. 200000 rows is well past what a five-millisecond budget reaches.
	var body strings.Builder
	body.WriteString("id,payload\n")
	for i := range 200000 {
		body.WriteString(strconv.Itoa(i))
		body.WriteString(",")
		body.WriteString(strings.Repeat("x", 200))
		body.WriteString("\n")
	}
	require.NoError(t, os.WriteFile(path, []byte(body.String()), 0o600))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	require.Error(t, LoadInto(ctx, db, path), "the load has to fail for this test to say anything")

	assert.Equal(t, 2, countRows(t, db, "big"), "a failed reload must not take the rows that were there")
}

// TestLoadInto_FailedFirstLoadLeavesNoTable is the other half: a load that
// fails while creating a table the database did not have leaves nothing behind,
// rather than an empty table named after the file.
func TestLoadInto_FailedFirstLoadLeavesNoTable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "fresh.csv")

	var body strings.Builder
	body.WriteString("id,payload\n")
	for i := range 200000 {
		body.WriteString(strconv.Itoa(i))
		body.WriteString(",")
		body.WriteString(strings.Repeat("x", 200))
		body.WriteString("\n")
	}
	require.NoError(t, os.WriteFile(path, []byte(body.String()), 0o600))

	db := newCallerDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	require.Error(t, LoadInto(ctx, db, path))

	assert.Empty(t, listTables(t, db), "a load that failed created nothing the caller can query")
}

// writeTwoSheetWorkbook writes a workbook whose first sheet loads and whose
// second is refused for naming one column twice, so a load of the file fails
// after the first sheet's table was already made.
func writeTwoSheetWorkbook(t *testing.T, dir string) string {
	t.Helper()
	f := excelize.NewFile()
	require.NoError(t, f.SetSheetName("Sheet1", "good"))
	require.NoError(t, f.SetSheetRow("good", "A1", &[]any{"a", "b"}))
	require.NoError(t, f.SetSheetRow("good", "A2", &[]any{1, "x"}))
	_, err := f.NewSheet("bad")
	require.NoError(t, err)
	require.NoError(t, f.SetSheetRow("bad", "A1", &[]any{"id", "id"}))
	require.NoError(t, f.SetSheetRow("bad", "A2", &[]any{1, 2}))
	path := filepath.Join(dir, "book.xlsx")
	require.NoError(t, f.SaveAs(path))
	return path
}

// TestLoadInto_FailedXLSXLeavesTheDatabaseAsItWas pins the documented per-input
// atomicity onto a file that maps to several tables: a workbook whose later
// sheet fails must leave none of its sheets loaded and must leave a table it
// was replacing exactly as it was.
func TestLoadInto_FailedXLSXLeavesTheDatabaseAsItWas(t *testing.T) {
	t.Parallel()
	path := writeTwoSheetWorkbook(t, t.TempDir())

	t.Run("no prior table", func(t *testing.T) {
		t.Parallel()
		db := newCallerDB(t)
		builder, err := buildForTest(context.Background(), NewBuilder().AddPath(path))
		require.NoError(t, err)
		require.Error(t, builder.LoadInto(context.Background(), db))
		assert.Empty(t, listTables(t, db), "a workbook that failed partway left a sheet's table behind")
	})

	t.Run("replacing a caller table", func(t *testing.T) {
		t.Parallel()
		db := newCallerDB(t)
		_, err := db.ExecContext(context.Background(), `CREATE TABLE book_good (v)`)
		require.NoError(t, err)
		_, err = db.ExecContext(context.Background(), `INSERT INTO book_good VALUES ('precious')`)
		require.NoError(t, err)
		builder, err := buildForTest(context.Background(), NewBuilder().AddPath(path))
		require.NoError(t, err)
		require.Error(t, builder.LoadInto(context.Background(), db))
		var kept string
		require.NoError(t, db.QueryRowContext(context.Background(), `SELECT v FROM book_good`).Scan(&kept),
			"the table the workbook was replacing must be as it was")
		assert.Equal(t, "precious", kept)
	})

	t.Run("inside a caller transaction", func(t *testing.T) {
		t.Parallel()
		db := newCallerDB(t)
		_, err := db.ExecContext(context.Background(), `CREATE TABLE book_good (v)`)
		require.NoError(t, err)
		_, err = db.ExecContext(context.Background(), `INSERT INTO book_good VALUES ('precious')`)
		require.NoError(t, err)
		builder, err := buildForTest(context.Background(), NewBuilder().AddPath(path))
		require.NoError(t, err)
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		require.Error(t, builder.LoadIntoTx(context.Background(), tx))
		var kept string
		require.NoError(t, tx.QueryRowContext(context.Background(), `SELECT v FROM book_good`).Scan(&kept),
			"the caller's table must be as it was inside the still-open transaction")
		assert.Equal(t, "precious", kept)
	})
}

// TestLoadIntoTx_FailedInputLeavesCallersTable pins that a failed input leaves
// the caller's still-open transaction as that input found it, for both kinds of
// input. The path input is read in typed chunks and the reader input is staged,
// and the two failure paths must not differ in what they leave behind.
func TestLoadIntoTx_FailedInputLeavesCallersTable(t *testing.T) {
	t.Parallel()

	// A file that fails at row 3001 under the default MalformedRowStop policy,
	// long after rows were inserted.
	var body strings.Builder
	body.WriteString("a,b\n")
	for i := range 3000 {
		body.WriteString(strconv.Itoa(i))
		body.WriteString(",value\n")
	}
	body.WriteString("badrow-only-one-field\n")
	content := body.String()

	for _, tc := range []struct {
		name      string
		chunkSize int
		viaReader bool
	}{
		{name: "path input many chunks", chunkSize: 100},
		{name: "path input one chunk", chunkSize: 100000},
		{name: "reader input many chunks", chunkSize: 100, viaReader: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newCallerDB(t)
			_, err := db.ExecContext(context.Background(), `CREATE TABLE tbl (old)`)
			require.NoError(t, err)
			_, err = db.ExecContext(context.Background(), `INSERT INTO tbl VALUES ('keep-me')`)
			require.NoError(t, err)

			builder := NewBuilder().SetDefaultChunkSize(tc.chunkSize)
			if tc.viaReader {
				builder = builder.AddReader(strings.NewReader(content), "tbl", FileTypeCSV)
			} else {
				builder = builder.AddPath(writeTempCSV(t, t.TempDir(), "tbl.csv", content))
			}
			built, err := buildForTest(context.Background(), builder)
			require.NoError(t, err)

			tx, err := db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			require.Error(t, built.LoadIntoTx(context.Background(), tx))

			var kept string
			require.NoError(t, tx.QueryRowContext(context.Background(), `SELECT old FROM tbl LIMIT 1`).Scan(&kept),
				"the caller's table must keep its schema and rows inside the transaction")
			assert.Equal(t, "keep-me", kept)
			var stagingCount int
			require.NoError(t, tx.QueryRowContext(context.Background(),
				`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE '\_filesql\_%' ESCAPE '\'`).Scan(&stagingCount))
			assert.Zero(t, stagingCount, "no staging table may be left for the caller to find")
		})
	}
}

// TestLoadIntoTx_CanceledLoadLeavesCallersTable is the cancellation half of the
// same guarantee. The caller's transaction is not built on the context the load
// runs under, so canceling the load leaves the transaction alive, and undoing
// the input has to happen even though the context that failed it is done.
func TestLoadIntoTx_CanceledLoadLeavesCallersTable(t *testing.T) {
	t.Parallel()

	var body strings.Builder
	body.WriteString("a,b\n")
	for i := range 200000 {
		body.WriteString(strconv.Itoa(i))
		body.WriteString(",")
		body.WriteString(strings.Repeat("x", 200))
		body.WriteString("\n")
	}
	path := writeTempCSV(t, t.TempDir(), "tbl.csv", body.String())

	db := newCallerDB(t)
	_, err := db.ExecContext(context.Background(), `CREATE TABLE tbl (old)`)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `INSERT INTO tbl VALUES ('keep-me')`)
	require.NoError(t, err)

	builder, err := buildForTest(context.Background(), NewBuilder().AddPath(path).SetDefaultChunkSize(100))
	require.NoError(t, err)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	require.Error(t, builder.LoadIntoTx(ctx, tx), "the load has to fail for this test to say anything")

	var kept string
	require.NoError(t, tx.QueryRowContext(context.Background(), `SELECT old FROM tbl LIMIT 1`).Scan(&kept),
		"a canceled load must leave the caller's table and transaction as they were")
	assert.Equal(t, "keep-me", kept)
}

// TestLoadIntoTx_CanceledContextTheTransactionWasBuiltOn is the other half of
// cancellation: when the caller's transaction is built on the context the load
// runs under, the load fails and nothing of it reaches the database, because
// database/sql ends the transaction the context belonged to.
//
// Which error the load reports is not pinned. Ending a transaction closes the
// statements prepared on it, so a load that is inserting when the context
// expires reports either the context or the statement it can no longer use,
// depending on which of the two the race goes to.
func TestLoadIntoTx_CanceledContextTheTransactionWasBuiltOn(t *testing.T) {
	t.Parallel()

	var body strings.Builder
	body.WriteString("a,b\n")
	for i := range 200000 {
		body.WriteString(strconv.Itoa(i))
		body.WriteString(",")
		body.WriteString(strings.Repeat("x", 200))
		body.WriteString("\n")
	}
	path := writeTempCSV(t, t.TempDir(), "tbl.csv", body.String())

	db := newCallerDB(t)
	builder, err := buildForTest(context.Background(), NewBuilder().AddPath(path).SetDefaultChunkSize(100))
	require.NoError(t, err)

	// The context is cancelled after the transaction has begun rather than by a
	// deadline set before it: a deadline short enough to land inside the load
	// can expire during BeginTx itself on a slow machine, where the driver
	// reports an interrupted connection and the test fails at its setup.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	require.Error(t, builder.LoadIntoTx(ctx, tx), "the load has to fail for this test to say anything")

	// Ending the transaction here rather than in a defer frees the one pooled
	// connection the listing below needs. What Rollback reports is the driver's
	// business: a cancelled context may have ended the transaction already, or
	// interrupted the connection it was on.
	if err := tx.Rollback(); err != nil {
		t.Logf("rollback after the canceled load: %v", err)
	}
	assert.Empty(t, listTables(t, db), "nothing of the canceled load reached the database")
}
