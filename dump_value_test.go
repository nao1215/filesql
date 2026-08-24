package filesql

import (
	"database/sql"
	"math"
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

	// A whole number keeps a decimal point because that is what makes the file
	// read back as REAL. Written as "1", it reloaded as an INTEGER column, and
	// integer division then answered a different question than the one the
	// database being dumped would have answered.
	t.Run("a REAL keeps its shortest exact form and its decimal point", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (v REAL)",
			"INSERT INTO t VALUES (0.1)",
			"INSERT INTO t VALUES (1e21)",
			"INSERT INTO t VALUES (1.0)")

		assert.Equal(t, "v\n0.1\n1e+21\n1.0\n", dumpToString(t, db, NewDumpOptions()))
	})

	// An INTEGER column is written bare: the suffix above belongs to the values
	// SQLite hands back as floats, and nothing else.
	t.Run("an INTEGER column is written without one", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (v INTEGER)",
			"INSERT INTO t VALUES (1)",
			"INSERT INTO t VALUES (-10)")

		assert.Equal(t, "v\n1\n-10\n", dumpToString(t, db, NewDumpOptions()))
	})

	// SQLite has no spelling of infinity that its own REAL affinity accepts, so
	// a literal that overflows to one is what the file carries. Written "+Inf",
	// the value reloaded as the text of that word inside a REAL column.
	t.Run("an infinity is written as a literal that overflows to it", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (v REAL)",
			"INSERT INTO t VALUES (9e999)",
			"INSERT INTO t VALUES (-9e999)")

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir))
		assert.Equal(t, "v\n9e999\n-9e999\n", readFileString(t, filepath.Join(outDir, "t.csv")))

		// Read back through this package the column is REAL again and holds the
		// infinities: the inference calls a saturating spelling a float because
		// SQLite's affinity saturates the same text to the same value. This is
		// not the overflow-integer rule — there TEXT preserves digits a float64
		// would lose, while here the value is the infinity, which float64 holds
		// exactly and a TEXT column would replace with a five-byte word.
		back, err := OpenContext(t.Context(), filepath.Join(outDir, "t.csv"))
		require.NoError(t, err)
		defer back.Close()

		rows, err := back.QueryContext(t.Context(), "SELECT typeof(v), v FROM t")
		require.NoError(t, err)
		defer rows.Close()

		reloaded := make([]float64, 0, 2)
		for rows.Next() {
			var kind string
			var value float64
			require.NoError(t, rows.Scan(&kind, &value))
			assert.Equal(t, "real", kind)
			reloaded = append(reloaded, value)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []float64{math.Inf(1), math.Inf(-1)}, reloaded)
	})

	// An auto-save writes through the same formatting, and it is the path where
	// a caller sees the change without asking for a dump: the file they loaded
	// is the file that gets rewritten.
	t.Run("a REAL column is still REAL after an auto-save and a load", func(t *testing.T) {
		t.Parallel()

		source := filepath.Join(t.TempDir(), "m.csv")
		require.NoError(t, os.WriteFile(source, []byte("amount\n10.00\n5.00\n"), 0o600))
		require.NoError(t, autoSaveOverwrite(t, []string{source}, "UPDATE m SET amount = amount WHERE 1"))

		back, err := OpenContext(t.Context(), source)
		require.NoError(t, err)
		defer back.Close()

		var kind, quarter string
		require.NoError(t, back.QueryRowContext(t.Context(),
			"SELECT typeof(amount), amount/4 FROM m LIMIT 1").Scan(&kind, &quarter))
		assert.Equal(t, "real", kind)
		assert.Equal(t, "2.5", quarter)
	})

	t.Run("a REAL column is still REAL after a save and a load", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (amount REAL)",
			"INSERT INTO t VALUES (10.0)",
			"INSERT INTO t VALUES (5.0)")

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir))

		back, err := OpenContext(t.Context(), filepath.Join(outDir, "t.csv"))
		require.NoError(t, err)
		defer back.Close()

		var kind, quarter string
		require.NoError(t, back.QueryRowContext(t.Context(),
			"SELECT typeof(amount), amount/4 FROM t LIMIT 1").Scan(&kind, &quarter))
		assert.Equal(t, "real", kind)
		assert.Equal(t, "2.5", quarter, "a saved and reloaded REAL column divides as a REAL column")
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
