package filesql

import (
	"os"
	"path/filepath"
	"testing"

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
