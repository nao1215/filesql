package filesql

import (
	"os"
	"path/filepath"
	"sort"
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
