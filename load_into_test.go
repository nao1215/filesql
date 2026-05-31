package filesql

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	rows, err := db.QueryContext(context.Background(), `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
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
	builder, err := NewBuilder().
		AddReader(strings.NewReader("id,v\n1,x\n2,y\n"), "from_reader", FileTypeCSV).
		Build(context.Background())
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
		builder, err := NewBuilder().
			AddPath(filepath.Join("testdata", "sample.csv")).
			EnableAutoSave(t.TempDir()).
			Build(context.Background())
		require.NoError(t, err)
		err = builder.LoadInto(context.Background(), db)
		require.Error(t, err)
	})
}

func TestLoadInto_DoesNotLeakReplaceModeToBuilder(t *testing.T) {
	db := newCallerDB(t)
	builder, err := NewBuilder().
		AddPath(filepath.Join("testdata", "sample.csv")).
		Build(context.Background())
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
