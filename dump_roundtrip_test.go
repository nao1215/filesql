package filesql

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
