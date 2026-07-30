package filesql

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openWithTable seeds a database from a file so the dump has something to walk,
// then replaces it with a table built by ddl. The dump reads whatever SQLite
// holds, and a table a caller created carries value types a loaded CSV never
// produces.
func openWithTable(t *testing.T, ddl string, inserts ...string) *sql.DB {
	t.Helper()

	src := filepath.Join(t.TempDir(), "seed.csv")
	require.NoError(t, os.WriteFile(src, []byte("a\n1\n"), 0o600))

	db, err := OpenContext(t.Context(), src)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(t.Context(), "DROP TABLE seed")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), ddl)
	require.NoError(t, err)
	for _, ins := range inserts {
		_, err = db.ExecContext(t.Context(), ins)
		require.NoError(t, err)
	}
	return db
}

// dumpToString dumps table "t" from db and returns the file's contents.
func dumpToString(t *testing.T, db *sql.DB, opts DumpOptions) string {
	t.Helper()

	outDir := t.TempDir()
	require.NoError(t, DumpDatabase(db, outDir, opts))
	got, err := os.ReadFile(filepath.Join(outDir, "t"+opts.FileExtension())) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	return string(got)
}

// TestDumpValueFormatting pins how each value type SQLite hands back is written.
// A dump used fmt's %v as its catch-all, which prints a Go value rather than a
// data value: a BLOB came out as the decimal bytes of a Go slice, and a column
// declared DATETIME — which the driver converts to time.Time — came out in Go's
// default time layout, neither of which reads back as what was stored.
func TestDumpValueFormatting(t *testing.T) {
	t.Parallel()

	t.Run("a BLOB is written as its bytes, not as a Go slice", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (name TEXT, payload BLOB)",
			"INSERT INTO t VALUES ('a', CAST('hello' AS BLOB))")

		assert.Equal(t, "name,payload\na,hello\n", dumpToString(t, db, NewDumpOptions()))
	})

	t.Run("a DATETIME column keeps the text that was stored", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (id INTEGER, created DATETIME)",
			"INSERT INTO t VALUES (1, '2026-07-30')",
			"INSERT INTO t VALUES (2, '2026-07-30 12:34:56')")

		assert.Equal(t,
			"id,created\n1,2026-07-30\n2,2026-07-30 12:34:56\n",
			dumpToString(t, db, NewDumpOptions()))
	})

	t.Run("a BOOLEAN column keeps the integer that was stored", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (flag BOOLEAN)",
			"INSERT INTO t VALUES (1)",
			"INSERT INTO t VALUES (0)")

		assert.Equal(t, "flag\n1\n0\n", dumpToString(t, db, NewDumpOptions()))
	})

	t.Run("a REAL keeps its shortest exact form", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (v REAL)",
			"INSERT INTO t VALUES (0.1)",
			"INSERT INTO t VALUES (1e21)",
			"INSERT INTO t VALUES (1.0)")

		assert.Equal(t, "v\n0.1\n1e+21\n1\n", dumpToString(t, db, NewDumpOptions()))
	})

	t.Run("NULL and the empty string stay distinguishable in LTSV", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (a TEXT, b TEXT)",
			"INSERT INTO t VALUES (NULL, '')")

		assert.Equal(t, "a:\tb:\n", dumpToString(t, db, NewDumpOptions().WithFormat(OutputFormatLTSV)))
	})

	t.Run("a BLOB survives a dump and reload round-trip", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (payload BLOB)",
			"INSERT INTO t VALUES (CAST('hello' AS BLOB))")

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions()))

		reloaded, err := OpenContext(t.Context(), filepath.Join(outDir, "t.csv"))
		require.NoError(t, err)
		defer reloaded.Close()

		var got string
		require.NoError(t, reloaded.QueryRowContext(t.Context(), "SELECT payload FROM t").Scan(&got))
		assert.Equal(t, "hello", got)
	})

	t.Run("every format agrees on how a value is written", func(t *testing.T) {
		t.Parallel()

		formats := []struct {
			name   string
			format OutputFormat
			want   string
		}{
			{name: "csv", format: OutputFormatCSV, want: "name,payload\na,hello\n"},
			{name: "tsv", format: OutputFormatTSV, want: "name\tpayload\na\thello\n"},
			{name: "ltsv", format: OutputFormatLTSV, want: "name:a\tpayload:hello\n"},
		}

		for _, f := range formats {
			t.Run(f.name, func(t *testing.T) {
				t.Parallel()

				db := openWithTable(t,
					"CREATE TABLE t (name TEXT, payload BLOB)",
					"INSERT INTO t VALUES ('a', CAST('hello' AS BLOB))")

				assert.Equal(t, f.want, dumpToString(t, db, NewDumpOptions().WithFormat(f.format)))
			})
		}
	})
}
