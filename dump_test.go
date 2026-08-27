package filesql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDumpEmptyTable pins what a table with no rows dumps to, and what reading
// that dump back gives.
//
// A table can be emptied by the query that was the point of the session — a
// DELETE that removed everything, a filtered load that matched nothing — and a
// dump of it has to say so. Refusing to write means an auto-save silently keeps
// the rows the caller deleted, and a save that reports failure after the
// transaction committed leaves the file disagreeing with the database.
func TestDumpEmptyTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		format      OutputFormat
		compression CompressionType
	}{
		{name: "csv", format: OutputFormatCSV, compression: CompressionNone},
		{name: "csv gz", format: OutputFormatCSV, compression: CompressionGZ},
		{name: "tsv", format: OutputFormatTSV, compression: CompressionNone},
		{name: "ltsv", format: OutputFormatLTSV, compression: CompressionNone},
		{name: "parquet", format: OutputFormatParquet, compression: CompressionNone},
		{name: "xlsx", format: OutputFormatXLSX, compression: CompressionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "people.csv")
			require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer db.Close()

			_, err = db.ExecContext(ctx, "DELETE FROM people")
			require.NoError(t, err)

			opts := NewDumpOptions().WithFormat(tt.format).WithCompression(tt.compression)
			outDir := t.TempDir()
			require.NoError(t, DumpDatabase(db, outDir, opts), "an emptied table must be dumpable")

			entries, err := os.ReadDir(outDir)
			require.NoError(t, err)
			require.Len(t, entries, 1, "no staged file may be left behind: %v", entries)
			assert.Equal(t, "people"+opts.FileExtension(), entries[0].Name())
		})
	}
}

// TestDumpEmptyTableRoundTrip pins that the dump of an emptied table reads back
// as the same table with no rows.
//
// LTSV is the exception and is covered separately: it carries a label on every
// row and so has no header, which leaves an emptied table nothing to write and
// nothing to read back.
func TestDumpEmptyTableRoundTrip(t *testing.T) {
	t.Parallel()

	formats := []struct {
		name   string
		format OutputFormat
	}{
		{name: "csv", format: OutputFormatCSV},
		{name: "tsv", format: OutputFormatTSV},
		{name: "parquet", format: OutputFormatParquet},
		{name: "xlsx", format: OutputFormatXLSX},
	}

	for _, tt := range formats {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "people.csv")
			require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer db.Close()

			_, err = db.ExecContext(ctx, "DELETE FROM people")
			require.NoError(t, err)

			opts := NewDumpOptions().WithFormat(tt.format)
			outDir := t.TempDir()
			require.NoError(t, DumpDatabase(db, outDir, opts))

			reloaded, err := OpenContext(ctx, filepath.Join(outDir, "people"+opts.FileExtension()))
			require.NoError(t, err)
			defer reloaded.Close()

			var count int
			require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT COUNT(*) FROM people").Scan(&count))
			assert.Equal(t, 0, count)

			rows, err := reloaded.QueryContext(ctx, "SELECT * FROM people")
			require.NoError(t, err)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, []string{"id", "name"}, cols, "an emptied table keeps its columns")
			require.NoError(t, rows.Err())
		})
	}

	t.Run("ltsv has no header, so an emptied table cannot be read back", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		src := filepath.Join(t.TempDir(), "people.csv")
		require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

		db, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.ExecContext(ctx, "DELETE FROM people")
		require.NoError(t, err)

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatLTSV)))

		// The empty file is a correct empty LTSV; there is simply no column list in
		// it to rebuild the table from.
		_, err = OpenContext(ctx, filepath.Join(outDir, "people.ltsv"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEmptyData)
	})
}

// TestDumpEmptyTableColumnsSurvive pins that a dump of an emptied table still
// carries its columns, for the formats that have somewhere to put them. LTSV
// writes one label per row and so has nowhere; an empty LTSV file is all it can
// be.
func TestDumpEmptyTableColumnsSurvive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{name: "csv keeps its header", format: OutputFormatCSV, want: "id,name\n"},
		{name: "tsv keeps its header", format: OutputFormatTSV, want: "id\tname\n"},
		{name: "ltsv has nowhere to put one", format: OutputFormatLTSV, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "people.csv")
			require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer db.Close()

			_, err = db.ExecContext(ctx, "DELETE FROM people")
			require.NoError(t, err)

			opts := NewDumpOptions().WithFormat(tt.format)
			outDir := t.TempDir()
			require.NoError(t, DumpDatabase(db, outDir, opts))

			got, err := os.ReadFile(filepath.Join(outDir, "people"+opts.FileExtension())) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

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

		err = dumpSQLiteDatabase(context.Background(), db, filepath.Join(blocked, "out"), NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrIOOperation)
	})

	t.Run("a database with no tables leaves no directory", func(t *testing.T) {
		t.Parallel()

		outputDir := filepath.Join(t.TempDir(), "out")
		err := dumpSQLiteDatabase(context.Background(), openTestDB(t), outputDir, NewDumpOptions())
		require.ErrorIs(t, err, ErrNoTables)
		assert.NoDirExists(t, outputDir, "a dump with nothing to write must not leave a directory behind")
	})

	t.Run("the tables cannot be listed", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		require.NoError(t, db.Close())

		err := dumpSQLiteDatabase(context.Background(), db, filepath.Join(t.TempDir(), "out"), NewDumpOptions())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDatabaseOperation)
	})

	t.Run("a database with no table", func(t *testing.T) {
		t.Parallel()

		err := dumpSQLiteDatabase(context.Background(), openTestDB(t), filepath.Join(t.TempDir(), "out"), NewDumpOptions())
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

			err = dumpSQLiteDatabase(context.Background(), db, filepath.Join(t.TempDir(), "out"), NewDumpOptions())
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

	err := dumpSQLiteTable(context.Background(), db, "users", t.TempDir(), NewDumpOptions())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseOperation)
}

// TestDumpLoneEmptyField pins that a row whose only column is empty survives a
// dump and a reload.
//
// In CSV a record of one empty field, written plainly, is a blank line, and a
// blank line is not a record — a reader skips it. A one-column table of five
// rows, two of them empty, came back with three: the rows were gone and the dump
// reported success. Quoting the field says "one field, empty" and cannot be read
// as anything else. A record of several columns is unaffected, because the
// delimiters already say how many fields there are.
//
// TSV has no quoting to say that with, so the two halves agree the other way:
// the blank line is the value, and the reader takes it back as that column's
// empty field.
func TestDumpLoneEmptyField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{name: "csv", format: OutputFormatCSV, want: "v\nalice\n\"\"\nbob\n"},
		{name: "tsv", format: OutputFormatTSV, want: "v\nalice\n\nbob\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openWithTable(t,
				"CREATE TABLE t (v TEXT)",
				"INSERT INTO t VALUES ('alice')",
				"INSERT INTO t VALUES ('')",
				"INSERT INTO t VALUES ('bob')")

			assert.Equal(t, tt.want, dumpToString(t, db, NewDumpOptions().WithFormat(tt.format)))

			// The two spellings have to read back as the same three values, or
			// one of the formats is losing the row rather than writing it
			// differently.
			outDir := t.TempDir()
			require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(tt.format)))
			reloaded, err := OpenContext(t.Context(), filepath.Join(outDir, "t"+tt.format.Extension()))
			require.NoError(t, err)
			defer reloaded.Close()

			rows, err := reloaded.QueryContext(t.Context(), `SELECT v FROM t`)
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var value string
				require.NoError(t, rows.Scan(&value))
				got = append(got, value)
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, []string{"alice", "", "bob"}, got)
		})
	}

	t.Run("every row comes back, empty ones included", func(t *testing.T) {
		t.Parallel()

		stored := []string{"alice", "", "bob", "", "carol"}

		ctx := t.Context()
		src := filepath.Join(t.TempDir(), "seed.csv")
		require.NoError(t, os.WriteFile(src, []byte("a\n1\n"), 0o600))

		db, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.ExecContext(ctx, "DROP TABLE seed")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "CREATE TABLE names (v TEXT)")
		require.NoError(t, err)
		for _, v := range stored {
			_, err = db.ExecContext(ctx, "INSERT INTO names VALUES (?)", v)
			require.NoError(t, err)
		}

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions()))

		reloaded, err := OpenContext(ctx, filepath.Join(outDir, "names.csv"))
		require.NoError(t, err)
		defer reloaded.Close()

		rows, err := reloaded.QueryContext(ctx, "SELECT v FROM names")
		require.NoError(t, err)
		defer rows.Close()
		got := make([]string, 0, len(stored))
		for rows.Next() {
			var v string
			require.NoError(t, rows.Scan(&v))
			got = append(got, v)
		}
		require.NoError(t, rows.Err())

		assert.Equal(t, stored, got)
	})

	t.Run("a multi-column row with every field empty is unaffected", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (a TEXT, b TEXT)",
			"INSERT INTO t VALUES ('', '')")

		assert.Equal(t, "a,b\n,\n", dumpToString(t, db, NewDumpOptions()))
	})

	t.Run("a lone NULL is written the same way, since neither format has a NULL", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (v TEXT)",
			"INSERT INTO t VALUES (NULL)")

		assert.Equal(t, "v\n\"\"\n", dumpToString(t, db, NewDumpOptions()))
	})
}

// TestDumpLTSVUnrepresentableValues pins that a value LTSV cannot hold is
// refused rather than written.
//
// LTSV separates fields with a tab and records with a newline, and the format
// gives no way to escape either one. Writing them anyway produced a file that
// parses as something else: a tab inside a value split it into a second field,
// which has no label and which the reader drops without a word, and a newline
// split the record in two. The value was gone and nothing said so.
func TestDumpLTSVUnrepresentableValues(t *testing.T) {
	t.Parallel()

	ltsv := NewDumpOptions().WithFormat(OutputFormatLTSV)

	tests := []struct {
		name    string
		insert  string
		wantErr string
	}{
		{
			name:    "a tab in a value would open a second field",
			insert:  "INSERT INTO t VALUES ('x\ty', 'z')",
			wantErr: `column "a" holds a tab`,
		},
		{
			name:    "a newline in a value would end the record",
			insert:  "INSERT INTO t VALUES ('x\ny', 'z')",
			wantErr: `column "a" holds a newline`,
		},
		{
			name:    "a carriage return in a value would end the record",
			insert:  "INSERT INTO t VALUES ('x\ry', 'z')",
			wantErr: `column "a" holds a carriage return`,
		},
		{
			name:    "the offending column is named even when it is not the first",
			insert:  "INSERT INTO t VALUES ('x', 'y\tz')",
			wantErr: `column "b" holds a tab`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openWithTable(t, "CREATE TABLE t (a TEXT, b TEXT)", tt.insert)

			outDir := t.TempDir()
			dest := filepath.Join(outDir, "t.ltsv")
			original := []byte("a:untouched\n")
			require.NoError(t, os.WriteFile(dest, original, 0o600))

			err := DumpDatabase(db, outDir, ltsv)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Contains(t, err.Error(), tt.wantErr)

			after, readErr := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, readErr)
			assert.Equal(t, original, after, "a refused dump leaves the destination as it was")
		})
	}

	t.Run("a colon in a value is written as it is", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, "CREATE TABLE t (a TEXT)", "INSERT INTO t VALUES ('12:34:56')")
		assert.Equal(t, "a:12:34:56\n", dumpToString(t, db, ltsv))
	})

	t.Run("a column name that would be read as a label and a value is refused", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE t ("a:b" TEXT)`, "INSERT INTO t VALUES ('v')")

		err := DumpDatabase(db, t.TempDir(), ltsv)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), `column "a:b" holds a colon`)
	})

	t.Run("a column name the reader would trim is refused", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE t (" a" TEXT)`, "INSERT INTO t VALUES ('v')")

		err := DumpDatabase(db, t.TempDir(), ltsv)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), `column " a" would be read back as "a"`)
	})

	t.Run("a column name LTSV can hold round-trips unchanged", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE t ("a b" TEXT, "日付" TEXT, "" TEXT)`, "INSERT INTO t VALUES ('x', 'y', 'z')")

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir, ltsv))

		reloaded, err := OpenContext(t.Context(), filepath.Join(outDir, "t.ltsv"))
		require.NoError(t, err)
		defer reloaded.Close()

		var columns []string
		rows, err := reloaded.QueryContext(t.Context(), `SELECT name FROM pragma_table_info('t')`)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			columns = append(columns, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"a b", "日付", ""}, columns)
	})

	t.Run("a value that survives LTSV round-trips unchanged", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t,
			"CREATE TABLE t (a TEXT, b TEXT)",
			"INSERT INTO t VALUES ('plain', 'with space')")

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir, ltsv))

		reloaded, err := OpenContext(t.Context(), filepath.Join(outDir, "t.ltsv"))
		require.NoError(t, err)
		defer reloaded.Close()

		var a, b string
		require.NoError(t, reloaded.QueryRowContext(t.Context(), "SELECT a, b FROM t").Scan(&a, &b))
		assert.Equal(t, "plain", a)
		assert.Equal(t, "with space", b)
	})
}

// TestDumpUnrepresentableSentinels pins what a dump reports when the output
// format cannot hold a value, for both formats that can refuse one.
//
// Two things are pinned. Every such failure carries ErrUnsupportedFormat, which
// says the table is fine and the format is not; TSV used to carry only
// parser.ErrTSVUnrepresentable, so the two formats answered the same fault in
// two ways. And that sentinel is still on a TSV failure, because a caller may
// have been matching it since before this package had one of its own.
//
// The advice names CSV in every case. LTSV used to answer "CSV or TSV", which
// is wrong for all three characters it forbids in a value: TSV forbids the same
// three, having no quoting to hold them in.
func TestDumpUnrepresentableSentinels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		format      OutputFormat
		insert      string
		wantTSVUnre bool
	}{
		{
			name:        "a tab is the delimiter TSV separates fields with",
			format:      OutputFormatTSV,
			insert:      "INSERT INTO t VALUES ('x\ty')",
			wantTSVUnre: true,
		},
		{
			name:        "a newline ends a TSV record",
			format:      OutputFormatTSV,
			insert:      "INSERT INTO t VALUES ('x\ny')",
			wantTSVUnre: true,
		},
		{
			name:   "a tab is the delimiter LTSV separates fields with",
			format: OutputFormatLTSV,
			insert: "INSERT INTO t VALUES ('x\ty')",
		},
		{
			name:   "a newline ends an LTSV record",
			format: OutputFormatLTSV,
			insert: "INSERT INTO t VALUES ('x\ny')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openWithTable(t, "CREATE TABLE t (a TEXT)", tt.insert)

			err := DumpDatabase(db, t.TempDir(), NewDumpOptions().WithFormat(tt.format))

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Equal(t, tt.wantTSVUnre, errors.Is(err, parser.ErrTSVUnrepresentable),
				"parser.ErrTSVUnrepresentable should be on a TSV failure and only on one")
			assert.Contains(t, err.Error(), "dump this table as CSV instead")
		})
	}
}

// TestDumpDatabaseStaysInOutputDir pins that a dump writes only into the
// directory it was given.
//
// A table name is an arbitrary SQL identifier, so it can carry a path separator
// or a parent reference, and the output path was built by joining it to the
// output directory. filepath.Join resolves those, so a table created as
// "../escaped" had its dump written next to the directory instead of in it —
// past whatever the caller had decided the dump was allowed to touch.
func TestDumpDatabaseStaysInOutputDir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table string
	}{
		{name: "a parent reference", table: "../escaped"},
		{name: "a parent reference in the middle", table: "sub/../../escaped"},
		{name: "a subdirectory that does not exist", table: "sub/nested"},
		{name: "an absolute path", table: "/escaped"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openWithTable(t,
				`CREATE TABLE "`+tt.table+`" (a TEXT)`,
				`INSERT INTO "`+tt.table+`" VALUES ('leaked')`)

			root := t.TempDir()
			outDir := filepath.Join(root, "out")

			err := DumpDatabase(db, outDir, NewDumpOptions())
			require.Error(t, err, "a table whose name is not a file name must be refused")
			assert.ErrorIs(t, err, ErrInvalidData)
			assert.Contains(t, err.Error(), tt.table)

			// Nothing may exist above the output directory, and the output
			// directory itself may hold no staged leftovers.
			entries, readErr := os.ReadDir(root)
			require.NoError(t, readErr)
			for _, e := range entries {
				assert.Equal(t, "out", e.Name(), "the dump wrote outside its output directory")
			}
			outEntries, readErr := os.ReadDir(outDir)
			require.NoError(t, readErr)
			assert.Empty(t, outEntries, "no staged file may be left behind: %v", outEntries)
		})
	}

	t.Run("an ordinary table name is written into the output directory", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, "CREATE TABLE people (a TEXT)", "INSERT INTO people VALUES ('kept')")

		outDir := filepath.Join(t.TempDir(), "out")
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions()))

		got, err := os.ReadFile(filepath.Join(outDir, "people.csv")) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Contains(t, string(got), "kept")
	})

	// A dot is a file name every platform holds, so this is refused for the
	// other reason: a load of "a.b.csv" names the table a_b, and a dump nobody
	// can read back under the name they dumped is not a dump.
	t.Run("a table name a load would spell differently is refused", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE "a.b" (x TEXT)`, `INSERT INTO "a.b" VALUES ('kept')`)

		outDir := filepath.Join(t.TempDir(), "out")
		err := DumpDatabase(db, outDir, NewDumpOptions())

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidData)
		assert.Contains(t, err.Error(), "a.b")
		assert.Contains(t, err.Error(), "a_b", "the error must name what a load would call the table")
	})
}

// TestDumpWritesWhatItCanReadBack holds the rule that a dump is a file this
// package can load again under the name it was dumped from. A table name is an
// arbitrary SQL identifier and a table name derived from a file is not: the
// load spells a space, a hyphen and a dot as an underscore and drops what is
// left, so a table named "with space" was written to "with space.csv" and came
// back as with_space, and two tables named "a b" and "a-b" were written to two
// files that could not be loaded together at all.
func TestDumpWritesWhatItCanReadBack(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("a table a load would rename is refused", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE "with space" (id INTEGER)`, `INSERT INTO "with space" VALUES (1)`)

		outDir := filepath.Join(t.TempDir(), "out")
		err := DumpDatabase(db, outDir, NewDumpOptions())

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidData)
		assert.Contains(t, err.Error(), "with space")
		assert.Contains(t, err.Error(), "with_space")
	})

	t.Run("two tables a load would give one name are refused", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE "a b" (id INTEGER)`, `INSERT INTO "a b" VALUES (1)`)
		_, err := db.ExecContext(ctx, `CREATE TABLE "a-b" (id INTEGER)`)
		require.NoError(t, err)

		outDir := filepath.Join(t.TempDir(), "out")
		require.Error(t, DumpDatabase(db, outDir, NewDumpOptions()),
			"a dump whose files cannot be loaded together must be refused")
	})

	t.Run("an ordinary table dumps and loads under its own name", func(t *testing.T) {
		t.Parallel()

		for _, name := range []string{"orders", "sales_2026", "売上"} {
			db := openWithTable(t, "CREATE TABLE "+quoteIdentifier(name)+" (id INTEGER)",
				"INSERT INTO "+quoteIdentifier(name)+" VALUES (7)")

			outDir := filepath.Join(t.TempDir(), "out")
			require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions()))
			require.NoError(t, db.Close())

			back, err := OpenContext(ctx, filepath.Join(outDir, name+".csv"))
			require.NoError(t, err)

			var got int
			require.NoError(t, back.QueryRowContext(ctx, "SELECT id FROM "+quoteIdentifier(name)).Scan(&got))
			assert.Equal(t, 7, got)
			require.NoError(t, back.Close())
		}
	})
}

// TestDumpXLSXRefusesWhatXMLCannotHold holds the rule that a dump does not
// change a value it cannot write. A worksheet is XML, and XML 1.0 has no way to
// spell most of the control characters, so the library writing the workbook
// replaced each of them with U+FFFD: a cell holding a NUL was dumped and read
// back as "a�b" under a dump that reported success.
func TestDumpXLSXRefusesWhatXMLCannotHold(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		value string
	}{
		{name: "a NUL", value: "a\x00b"},
		{name: "a start of heading", value: "a\x01b"},
		{name: "a vertical tab", value: "a\x0bb"},
		{name: "an escape", value: "a\x1bb"},
		{name: "a unit separator", value: "a\x1fb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openWithTable(t, "CREATE TABLE t (value TEXT)", "")
			_, err := db.ExecContext(ctx, "INSERT INTO t VALUES (?)", tc.value)
			require.NoError(t, err)

			outDir := filepath.Join(t.TempDir(), "out")
			err = DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatXLSX))

			require.Error(t, err, "a value XML cannot hold must be refused, not rewritten")
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Contains(t, err.Error(), "value", "the error must name the column")
		})
	}

	t.Run("the three control characters XML admits still round-trip", func(t *testing.T) {
		t.Parallel()

		const value = "a\tb\nc\rd"
		db := openWithTable(t, "CREATE TABLE t (value TEXT)", "")
		_, err := db.ExecContext(ctx, "INSERT INTO t VALUES (?)", value)
		require.NoError(t, err)

		outDir := filepath.Join(t.TempDir(), "out")
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatXLSX)))
		require.NoError(t, db.Close())

		back, err := OpenContext(ctx, filepath.Join(outDir, "t.xlsx"))
		require.NoError(t, err)
		defer func() { _ = back.Close() }()

		var got string
		require.NoError(t, back.QueryRowContext(ctx, "SELECT value FROM t").Scan(&got))
		assert.Equal(t, value, got)
	})

	t.Run("the same value dumps as CSV, which is what the error says to do", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, "CREATE TABLE t (value TEXT)", "")
		_, err := db.ExecContext(ctx, "INSERT INTO t VALUES (?)", "a\x00b")
		require.NoError(t, err)

		outDir := filepath.Join(t.TempDir(), "out")
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions()))
		require.NoError(t, db.Close())

		back, err := OpenContext(ctx, filepath.Join(outDir, "t.csv"))
		require.NoError(t, err)
		defer func() { _ = back.Close() }()

		var got string
		require.NoError(t, back.QueryRowContext(ctx, "SELECT value FROM t").Scan(&got))
		assert.Equal(t, "a\x00b", got)
	})
}

func TestDumpFilePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		outputDir string
		table     string
		ext       string
		want      string
		wantErr   bool
	}{
		{name: "a plain name", outputDir: "out", table: "people", ext: ".csv", want: filepath.Join("out", "people.csv")},
		{name: "a trailing separator on the directory", outputDir: "out" + string(filepath.Separator), table: "people", ext: ".csv", want: filepath.Join("out", "people.csv")},
		{name: "a relative directory", outputDir: filepath.Join(".", "out"), table: "people", ext: ".csv", want: filepath.Join("out", "people.csv")},
		{name: "a compressed extension", outputDir: "out", table: "people", ext: ".csv.gz", want: filepath.Join("out", "people.csv.gz")},
		{name: "a dot inside the name", outputDir: "out", table: "a.b", ext: ".csv", wantErr: true},
		{name: "a name a load would spell with an underscore", outputDir: "out", table: "with space", ext: ".csv", wantErr: true},
		{name: "a name holding a bracket", outputDir: "out", table: "a[b]", ext: ".csv", wantErr: true},
		{name: "a name that is a file name of its own", outputDir: "out", table: "people.csv", ext: ".csv", wantErr: true},
		{name: "a non-Latin name", outputDir: "out", table: "売上", ext: ".csv", want: filepath.Join("out", "売上.csv")},
		{name: "a parent reference", outputDir: "out", table: "../escaped", ext: ".csv", wantErr: true},
		{name: "a separator", outputDir: "out", table: "sub/x", ext: ".csv", wantErr: true},
		{name: "a backslash", outputDir: "out", table: `sub\x`, ext: ".csv", wantErr: true},
		{name: "an absolute name", outputDir: "out", table: string(filepath.Separator) + "escaped", ext: ".csv", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := dumpFilePath(tt.outputDir, tt.table, tt.ext)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidData)
				assert.Empty(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestDumpKeepsTablesThatOnlyLookReserved pins that the filter hiding SQLite's
// own tables hides nothing else. It was written as NOT LIKE 'sqlite_%', where
// LIKE reads a bare underscore as any one character, so a table named sqliteish
// or sqlite2024 loaded and answered queries but appeared in no listing and in no
// dump: a save wrote the database out without it and said nothing.
func TestDumpKeepsTablesThatOnlyLookReserved(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	for _, name := range []string{"sqliteish.csv", "sqlite2024.csv", "sqlite.csv", "plain.csv"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("a,b\n1,2\n"), 0o600))
	}

	db, err := OpenContext(ctx, dir)
	require.NoError(t, err)
	defer db.Close()

	names, err := getSQLiteTableNames(context.Background(), db)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"plain", "sqlite", "sqlite2024", "sqliteish"}, names)

	// SQLite creates this one itself, and it stays hidden: the escape narrows
	// the filter to the names the database reserves rather than removing it.
	_, err = db.ExecContext(ctx, `ANALYZE`)
	require.NoError(t, err)
	names, err = getSQLiteTableNames(context.Background(), db)
	require.NoError(t, err)
	assert.NotContains(t, names, "sqlite_stat1")

	out := t.TempDir()
	require.NoError(t, DumpDatabase(db, out))
	entries, err := os.ReadDir(out)
	require.NoError(t, err)
	written := make([]string, 0, len(entries))
	for _, entry := range entries {
		written = append(written, entry.Name())
	}
	assert.ElementsMatch(t, []string{"plain.csv", "sqlite.csv", "sqlite2024.csv", "sqliteish.csv"}, written)
}

// TestDumpDatabase_RoundTripPerFormat dumps the same table in every format the
// dump supports and reads the result back. It is the check that was missing when
// the tabular dump moved to a staged file: the staged name carries a temporary
// suffix, and a writer that decides anything from the file name it is handed —
// Excel picks both its container format and its sheet name that way — produced a
// broken file or none at all while the dump reported success.
func TestDumpDatabase_RoundTripPerFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		format      OutputFormat
		compression CompressionType
	}{
		{name: "csv", format: OutputFormatCSV, compression: CompressionNone},
		{name: "csv gz", format: OutputFormatCSV, compression: CompressionGZ},
		{name: "tsv", format: OutputFormatTSV, compression: CompressionNone},
		{name: "ltsv", format: OutputFormatLTSV, compression: CompressionNone},
		{name: "parquet", format: OutputFormatParquet, compression: CompressionNone},
		{name: "xlsx", format: OutputFormatXLSX, compression: CompressionNone},
		{name: "xlsx gz", format: OutputFormatXLSX, compression: CompressionGZ},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			srcDir := t.TempDir()
			src := filepath.Join(srcDir, "people.csv")
			require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n2,bob\n"), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer db.Close()

			opts := NewDumpOptions().WithFormat(tt.format).WithCompression(tt.compression)
			outDir := t.TempDir()
			require.NoError(t, DumpDatabase(db, outDir, opts))

			entries, err := os.ReadDir(outDir)
			require.NoError(t, err)
			require.Len(t, entries, 1, "no staged file may be left behind: %v", entries)
			assert.Equal(t, "people"+opts.FileExtension(), entries[0].Name())

			// Reading the dump back is what catches a file that was written in the
			// wrong shape: the table name comes from the sheet or file name, and the
			// rows from the payload.
			reloaded, err := OpenContext(ctx, filepath.Join(outDir, entries[0].Name()))
			require.NoError(t, err)
			defer reloaded.Close()

			var count int
			require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT COUNT(*) FROM people").Scan(&count))
			assert.Equal(t, 2, count)

			rows, err := reloaded.QueryContext(ctx, "SELECT name FROM people ORDER BY name")
			require.NoError(t, err)
			defer rows.Close()
			names := make([]string, 0, 2)
			for rows.Next() {
				var name string
				require.NoError(t, rows.Scan(&name))
				names = append(names, name)
			}
			require.NoError(t, rows.Err())
			sort.Strings(names)
			assert.Equal(t, []string{"alice", "bob"}, names)
		})
	}
}

// TestDumpDatabase_RealColumnWithInfinityRoundTrip pins the read side of the
// SQLiteFloatText contract. The dump spells an infinity 9e999 because that is
// the spelling SQLite's affinity converts back to the value, but the
// inference refused it — ParseFloat answers the saturated value beside
// ErrRange — so the whole reloaded column, finite values included, was TEXT.
func TestDumpDatabase_RealColumnWithInfinityRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	dir := t.TempDir()
	src, err := sql.Open("sqlite", filepath.Join(dir, "src.db"))
	require.NoError(t, err)
	defer func() { _ = src.Close() }()
	_, err = src.ExecContext(ctx, `CREATE TABLE m (v REAL);`)
	require.NoError(t, err)
	_, err = src.ExecContext(ctx, `INSERT INTO m VALUES (9e999), (-9e999), (2.5);`)
	require.NoError(t, err)

	out := filepath.Join(dir, "out")
	require.NoError(t, DumpDatabase(src, out))

	reloaded, err := OpenContext(ctx, filepath.Join(out, "m.csv"))
	require.NoError(t, err)
	defer reloaded.Close()

	var kinds, values string
	require.NoError(t, reloaded.QueryRowContext(ctx,
		`SELECT group_concat(typeof(v)), group_concat(quote(v)) FROM m`).Scan(&kinds, &values))
	assert.Equal(t, "real,real,real", kinds)
	assert.Equal(t, "9.0e+999,-9.0e+999,2.5", values)
}

// TestDumpDatabase_TSVQuoteRoundTrip is the metamorphic half of TSV taking its
// fields literally: what a dump writes is what a load reads back. A CSV writer
// would quote a value holding a quote, and the literal reader would hand those
// quotes back as part of the value.
func TestDumpDatabase_TSVQuoteRoundTrip(t *testing.T) {
	t.Parallel()

	stored := []string{`5'9" tall`, `said "hi" loudly`, `"quoted"`, `a""b`, "plain"}

	ctx := t.Context()
	src := filepath.Join(t.TempDir(), "seed.csv")
	require.NoError(t, os.WriteFile(src, []byte("a\n1\n"), 0o600))

	db, err := OpenContext(ctx, src)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, "CREATE TABLE notes (v TEXT)")
	require.NoError(t, err)
	for _, v := range stored {
		_, err = db.ExecContext(ctx, "INSERT INTO notes VALUES (?)", v)
		require.NoError(t, err)
	}

	outDir := t.TempDir()
	require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatTSV)))

	reloaded, err := OpenContext(ctx, filepath.Join(outDir, "notes.tsv"))
	require.NoError(t, err)
	defer reloaded.Close()

	rows, err := reloaded.QueryContext(ctx, "SELECT v FROM notes")
	require.NoError(t, err)
	defer rows.Close()
	got := make([]string, 0, len(stored))
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		got = append(got, v)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, stored, got)
}

// roundTripCells are the values a generated cell is drawn from: the boundaries
// type inference decides on, the spellings that are numbers in Go and not in
// SQL, and text that has to survive quoting.
var roundTripCells = []string{
	"", "0", "1", "-1", "007", "1.0", "2.50", "1e3", "-0", "0.0",
	"9223372036854775807", "9223372036854775808", "-9223372036854775808",
	"true", "TRUE", "false", "null", "NULL", "NA", "N/A", "nan", "inf", "-inf",
	"2024-01-02", "2024-01-02 03:04:05", "03:04:05", "20240102",
	"a", " a ", "a,b", "a\"b", "a\nb", "日本語", "0x10", "+1", "1_000", ".5", "5.",
	"1e400", "-1e400", "0.1", "1e21", "1e-7",
}

// TestDumpAndLoadIsIdentityOverGeneratedTables is the property the per-format
// round trips above check one table at a time: loading a file, dumping it, and
// loading the dump gives back the same columns holding the same values of the
// same types. It generates tables from the cells type inference decides on,
// under a fixed seed so a failure is reproducible.
//
// The property failed on whole numbers in a REAL column. A column of 10.00 and
// 5.00 loaded as REAL, dumped as "10" and "5" because that is the shortest form
// of the float, and reloaded as INTEGER, so amount/4 answered 2 where it had
// answered 2.5 -- a different number out of the same SQL, with nothing said.
func TestDumpAndLoadIsIdentityOverGeneratedTables(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	random := rand.New(rand.NewPCG(2026, 821)) //nolint:gosec // A fixed seed makes a failure reproducible; this is not cryptography.

	for iteration := range 200 {
		columns := 1 + random.IntN(3)
		header := make([]string, columns)
		for i := range header {
			header[i] = fmt.Sprintf("c%d", i)
		}
		records := 1 + random.IntN(4)
		lines := make([]string, 0, records+1)
		lines = append(lines, strings.Join(header, ","))
		for range records {
			record := make([]string, columns)
			for i := range record {
				record[i] = quoteCSVField(roundTripCells[random.IntN(len(roundTripCells))])
			}
			lines = append(lines, strings.Join(record, ","))
		}
		body := strings.Join(lines, "\n") + "\n"

		source := filepath.Join(t.TempDir(), "t.csv")
		require.NoError(t, os.WriteFile(source, []byte(body), 0o600))

		loaded, err := OpenContext(ctx, source)
		require.NoErrorf(t, err, "iteration %d: loading %q", iteration, body)
		before := describeTable(t, loaded, "t")

		outDir := t.TempDir()
		require.NoErrorf(t, DumpDatabase(loaded, outDir), "iteration %d: dumping %q", iteration, body)
		require.NoError(t, loaded.Close())

		reloaded, err := OpenContext(ctx, filepath.Join(outDir, "t.csv"))
		require.NoErrorf(t, err, "iteration %d: reloading the dump of %q", iteration, body)
		after := describeTable(t, reloaded, "t")
		require.NoError(t, reloaded.Close())

		assert.Equalf(t, before, after, "iteration %d: the round trip changed the table loaded from %q", iteration, body)
	}
}

// quoteCSVField wraps a generated cell the way a CSV writer would.
func quoteCSVField(cell string) string {
	if !strings.ContainsAny(cell, ",\"\n") {
		return cell
	}
	return `"` + strings.ReplaceAll(cell, `"`, `""`) + `"`
}

// describeTable renders a table's columns and every value with its Go type, so
// a comparison catches a REAL that came back as an INTEGER as readily as a value
// that changed.
func describeTable(t *testing.T, db *sql.DB, table string) string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), `SELECT * FROM "`+table+`"`) //nolint:gosec // The table name is a constant from this test.
	require.NoError(t, err)
	defer rows.Close()

	columns, err := rows.Columns()
	require.NoError(t, err)

	var described strings.Builder
	described.WriteString(strings.Join(columns, "|"))
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		require.NoError(t, rows.Scan(pointers...))
		described.WriteString(" // ")
		for i, value := range values {
			if i > 0 {
				described.WriteString("|")
			}
			fmt.Fprintf(&described, "%T:%v", value, value)
		}
	}
	require.NoError(t, rows.Err())
	return described.String()
}

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

// TestDumpDatabaseFollowsASymlink pins that a destination which already exists
// as a symbolic link is followed. Dumping into a directory whose entries are
// links into a shared location wrote regular files over the links, so the files
// the links named kept their old contents and the layout that made the
// directory work was gone -- all while DumpDatabase reported success.
func TestDumpDatabaseFollowsASymlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	src := filepath.Join(root, "users.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

	target := filepath.Join(root, "shared.csv")
	require.NoError(t, os.WriteFile(target, []byte("old\n"), 0o600))
	out := filepath.Join(root, "out")
	require.NoError(t, os.Mkdir(out, 0o750))
	link := filepath.Join(out, "users.csv")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("this platform does not allow a symlink to be created: %v", err)
	}

	validated, err := buildForTest(t.Context(), NewBuilder().AddPath(src))
	require.NoError(t, err)
	db, err := validated.Open(t.Context())
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, DumpDatabase(db, out))

	info, err := os.Lstat(link)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "the link must survive the dump")

	got, err := os.ReadFile(target) //nolint:gosec // target is under t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,alice\n", string(got), "the file the link names is what receives the dump")
}

// TestBlankCellInNumericColumnSurvivesARoundTrip pins that a missing number
// stays missing through a dump and back, in every format that can carry it.
//
// The loader used to store that cell as the empty string while the Parquet
// exporter wrote it as a null, so the two disagreed and a round trip through
// Parquet changed the value. They agree now, and this is what keeps them
// agreeing.
func TestBlankCellInNumericColumnSurvivesARoundTrip(t *testing.T) {
	t.Parallel()

	formats := []struct {
		name   string
		format OutputFormat
		ext    string
	}{
		{name: "csv", format: OutputFormatCSV, ext: ".csv"},
		{name: "tsv", format: OutputFormatTSV, ext: ".tsv"},
		{name: "parquet", format: OutputFormatParquet, ext: ".parquet"},
	}

	for _, tt := range formats {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "sales.csv")
			require.NoError(t, os.WriteFile(src, []byte("region,amount\nnorth,10\nsouth,\neast,30\n"), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			out := filepath.Join(t.TempDir(), "out")
			require.NoError(t, DumpDatabase(db, out, NewDumpOptions().WithFormat(tt.format)))
			require.NoError(t, db.Close())

			back, err := OpenContext(ctx, filepath.Join(out, "sales"+tt.ext))
			require.NoError(t, err)
			defer back.Close()

			var missing, present int
			var largest int64
			require.NoError(t, back.QueryRowContext(ctx,
				`SELECT SUM(amount IS NULL), COUNT(amount), MAX(amount) FROM sales`).Scan(&missing, &present, &largest))
			assert.Equal(t, 1, missing, "the row with no amount is still the row with no amount")
			assert.Equal(t, 2, present)
			assert.Equal(t, int64(30), largest)
		})
	}
}

// TestDumpRefusesNamesNoPlatformCanHold pins the rule dumpFilePath's comment
// states: a table name that cannot be a file name is refused on every platform,
// not only the one the running OS objects to, so the same database dumped on
// Linux and on Windows agrees about which tables it can write.
//
// It refused the two path separators and nothing else, so a table named CON,
// NUL or a:b was written on Linux under a name Windows reserves or forbids.
// AddReader keeps the name the caller gives it, so a name that came from user
// input reached the dump unaltered.
func TestDumpRefusesNamesNoPlatformCanHold(t *testing.T) {
	t.Parallel()

	refused := []struct {
		name  string
		table string
	}{
		{name: "a forward slash", table: "a/b"},
		{name: "a backslash", table: `a\b`},
		{name: "a colon, which names an alternate stream on Windows", table: "a:b"},
		{name: "a pipe", table: "a|b"},
		{name: "an asterisk", table: "a*b"},
		{name: "a question mark", table: "a?b"},
		{name: "a quotation mark", table: `a"b`},
		{name: "a less-than sign", table: "a<b"},
		{name: "a greater-than sign", table: "a>b"},
		{name: "a control byte", table: "a\x01b"},
		{name: "the console device", table: "CON"},
		{name: "the console device in lower case", table: "con"},
		{name: "the null device", table: "NUL"},
		{name: "a serial port", table: "COM1"},
		{name: "a printer port", table: "LPT9"},
		{name: "the auxiliary device", table: "aux"},
		{name: "a device name with an extension in front of the dump's own", table: "CON.backup"},
	}

	for _, tt := range refused {
		t.Run("refused: "+tt.name, func(t *testing.T) {
			t.Parallel()

			db, out := loadOneTable(t, tt.table)
			err := DumpDatabase(db, out)
			require.Error(t, err, "a name no platform can hold must not be written")
			assert.ErrorIs(t, err, ErrInvalidData)
			assert.Contains(t, err.Error(), tt.table)
			assert.Empty(t, dirEntriesOrNone(t, out), "nothing may be written for a refused table")
		})
	}

	kept := []struct {
		name  string
		table string
		file  string
	}{
		{name: "a name that is not ASCII", table: "日本語", file: "日本語.csv"},
		{name: "a word that contains a device name", table: "console", file: "console.csv"},
		{name: "another word that contains one", table: "nullable", file: "nullable.csv"},
	}

	for _, tt := range kept {
		t.Run("kept: "+tt.name, func(t *testing.T) {
			t.Parallel()

			db, out := loadOneTable(t, tt.table)
			require.NoError(t, DumpDatabase(db, out))
			assert.Equal(t, []string{tt.file}, dirEntriesOrNone(t, out))
		})
	}

	// These are file names every platform holds -- the dump appends its own
	// extension, so a name ending in a dot or a space does not make a file name
	// that ends in one -- and they are refused for the other reason: a load
	// spells a dot and a space as an underscore, so none of them would come
	// back under the name it was dumped from.
	renamed := []struct {
		name   string
		table  string
		loaded string
	}{
		{name: "a dot inside the name", table: "a.b", loaded: "a_b"},
		{name: "a space inside the name", table: "a b", loaded: "a_b"},
		{name: "a trailing dot", table: "a.", loaded: "a_"},
		{name: "a trailing space", table: "a ", loaded: "a_"},
	}

	for _, tt := range renamed {
		t.Run("a load would rename: "+tt.name, func(t *testing.T) {
			t.Parallel()

			db, out := loadOneTable(t, tt.table)
			err := DumpDatabase(db, out)

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidData)
			assert.Contains(t, err.Error(), tt.loaded)
			assert.Empty(t, dirEntriesOrNone(t, out), "nothing may be written for a refused table")
		})
	}
}

// TestUsableAsFileNameRejectsWhatTheDumpCannotReach pins the two rules a dump
// cannot exercise, because it appends its own extension and so never asks about
// a name that ends in a dot or a space. Windows strips both from the last
// component, which turns such a name into a different file and lets two of them
// land on one.
func TestUsableAsFileNameRejectsWhatTheDumpCannotReach(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"", "a.", "a ", "a..", "a  "} {
		assert.False(t, usableAsFileName(name), "%q is not a file name every platform can hold", name)
	}
	for _, name := range []string{"a", "a.b", ".a", " a", "a.csv", "console.csv"} {
		assert.True(t, usableAsFileName(name), "%q is a file name every platform can hold", name)
	}
}

// loadOneTable builds a database holding one table of the given name, and the
// directory a dump of it is written to.
func loadOneTable(t *testing.T, table string) (*sql.DB, string) {
	t.Helper()

	validated, err := buildForTest(

		t.Context(), NewBuilder().
			AddReader(strings.NewReader("v\n1\n"), table, FileTypeCSV))

	require.NoError(t, err)
	db, err := validated.Open(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, filepath.Join(t.TempDir(), "out")
}

// dirEntriesOrNone is the names in dir, or none when the dump never made it.
func dirEntriesOrNone(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// TestDumpDatabaseContext_StopsWhenTheContextEnds pins that an export can be
// canceled, which is what a download endpoint needs when its client goes away.
//
// DumpDatabase takes no context and builds context.Background() for the calls
// underneath that do take one, so an export ran to completion however long ago
// the request behind it was abandoned.
func TestDumpDatabaseContext_StopsWhenTheContextEnds(t *testing.T) {
	t.Parallel()

	newDB := func(t *testing.T, rows int) *sql.DB {
		t.Helper()
		path := filepath.Join(t.TempDir(), "big.csv")
		var body strings.Builder
		body.WriteString("id,name\n")
		for i := range rows {
			fmt.Fprintf(&body, "%d,customer%d\n", i, i)
		}
		if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		db, err := OpenContext(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	t.Run("a context that is already done writes nothing", func(t *testing.T) {
		t.Parallel()

		db := newDB(t, 100)
		out := filepath.Join(t.TempDir(), "out")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := DumpDatabaseContext(ctx, db, out)

		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.NoDirExists(t, out, "a dump that never started must not create the output directory")
	})

	t.Run("a deadline that expires during the export leaves no partial file", func(t *testing.T) {
		t.Parallel()

		db := newDB(t, 200000)
		out := filepath.Join(t.TempDir(), "out")
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		err := DumpDatabaseContext(ctx, db, out)
		if err == nil {
			t.Skip("the export beat the deadline on this machine")
		}
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		entries, readErr := os.ReadDir(out)
		if readErr == nil {
			for _, e := range entries {
				t.Errorf("a canceled export left %s behind", e.Name())
			}
		}
	})

	t.Run("a live context exports everything", func(t *testing.T) {
		t.Parallel()

		db := newDB(t, 100)
		out := filepath.Join(t.TempDir(), "out")

		require.NoError(t, DumpDatabaseContext(context.Background(), db, out))

		body, err := os.ReadFile(filepath.Join(out, "big.csv")) //nolint:gosec // Path built from the test's own temporary directory.
		require.NoError(t, err)
		assert.Equal(t, 101, strings.Count(string(body), "\n"))
	})

	t.Run("DumpDatabase still exports without a context", func(t *testing.T) {
		t.Parallel()

		db := newDB(t, 100)
		out := filepath.Join(t.TempDir(), "out")

		require.NoError(t, DumpDatabase(db, out))

		_, err := os.Stat(filepath.Join(out, "big.csv"))
		assert.NoError(t, err)
	})
}

// TestDumpRefusesColumnsTheLoadWouldRefuse is the column half of the rule that
// a dump is a file this package can read again. SQLite tells "a" from "a " and
// this package does not: a table built from both would answer a query about "a"
// with a column the caller did not mean, so the load refuses it. The dump wrote
// the two names into one header and said nothing, and the caller found out when
// they tried to read their own dump.
func TestDumpRefusesColumnsTheLoadWouldRefuse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	colliding := []struct {
		name    string
		columns string
	}{
		{name: "a trailing space", columns: `"a" TEXT, "a " TEXT`},
		{name: "a leading space", columns: `"a" TEXT, " a" TEXT`},
		{name: "a trailing tab", columns: "\"a\" TEXT, \"a\t\" TEXT"},
	}

	formats := []struct {
		name string
		f    OutputFormat
	}{
		{name: "csv", f: OutputFormatCSV},
		{name: "parquet", f: OutputFormatParquet},
	}

	for _, tt := range colliding {
		for _, format := range formats {
			t.Run(tt.name+"/"+format.name, func(t *testing.T) {
				t.Parallel()

				db := openWithTable(t, `CREATE TABLE t (`+tt.columns+`)`, `INSERT INTO t VALUES ('p', 'q')`)

				outDir := filepath.Join(t.TempDir(), "out")
				err := DumpDatabase(db, outDir, NewDumpOptions().WithFormat(format.f))

				require.Error(t, err, "a table the load would refuse must not be dumped in silence")
				assert.ErrorIs(t, err, ErrDuplicateColumn)
				assert.Contains(t, err.Error(), "t", "the error must name the table")
			})
		}
	}

	t.Run("ordinary column names still dump and load", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE t (a TEXT, b TEXT)`, `INSERT INTO t VALUES ('p', 'q')`)

		outDir := filepath.Join(t.TempDir(), "out")
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions()))
		require.NoError(t, db.Close())

		back, err := OpenContext(ctx, filepath.Join(outDir, "t.csv"))
		require.NoError(t, err)
		defer func() { _ = back.Close() }()

		var a, b string
		require.NoError(t, back.QueryRowContext(ctx, "SELECT a, b FROM t").Scan(&a, &b))
		assert.Equal(t, "p", a)
		assert.Equal(t, "q", b)
	})
}

// TestDumpXLSXRefusesAnUnnamedLastColumn pins that a workbook this package
// writes is one it can read. A worksheet does not store a trailing empty cell,
// so a header ending in a column with no name came back one cell short of the
// rows under it, and the load refused the workbook the dump had just written.
func TestDumpXLSXRefusesAnUnnamedLastColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("the last column has no name", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE t ("a" TEXT, "" TEXT)`, `INSERT INTO t VALUES ('p', 'q')`)

		outDir := filepath.Join(t.TempDir(), "out")
		err := DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatXLSX))

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), "CSV", "the error must say what to dump instead")
	})

	t.Run("an unnamed column that is not the last still round-trips", func(t *testing.T) {
		t.Parallel()

		// A worksheet stores an empty cell that has a cell after it, so this
		// header comes back whole.
		db := openWithTable(t, `CREATE TABLE t ("a" TEXT, "" TEXT, "b" TEXT)`, `INSERT INTO t VALUES ('p', 'q', 'r')`)

		outDir := filepath.Join(t.TempDir(), "out")
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatXLSX)))
		require.NoError(t, db.Close())

		back, err := OpenContext(ctx, filepath.Join(outDir, "t.xlsx"))
		require.NoError(t, err)
		defer func() { _ = back.Close() }()

		var middle string
		require.NoError(t, back.QueryRowContext(ctx, `SELECT "" FROM t`).Scan(&middle))
		assert.Equal(t, "q", middle)
	})

	t.Run("a last column named with a space still round-trips", func(t *testing.T) {
		t.Parallel()

		// That cell is not empty, so the workbook stores it. This is the
		// boundary between what looks blank and what a worksheet drops.
		db := openWithTable(t, `CREATE TABLE t ("a" TEXT, " " TEXT)`, `INSERT INTO t VALUES ('p', 'q')`)

		outDir := filepath.Join(t.TempDir(), "out")
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatXLSX)))
		require.NoError(t, db.Close())

		back, err := OpenContext(ctx, filepath.Join(outDir, "t.xlsx"))
		require.NoError(t, err)
		defer func() { _ = back.Close() }()

		var last string
		require.NoError(t, back.QueryRowContext(ctx, `SELECT " " FROM t`).Scan(&last))
		assert.Equal(t, "q", last)
	})
}
