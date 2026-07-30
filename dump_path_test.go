package filesql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	t.Run("a table name with a dot is still a file name", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE "a.b" (x TEXT)`, `INSERT INTO "a.b" VALUES ('kept')`)

		outDir := filepath.Join(t.TempDir(), "out")
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions()))

		got, err := os.ReadFile(filepath.Join(outDir, "a.b.csv")) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Contains(t, string(got), "kept")
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
		{name: "a dot inside the name", outputDir: "out", table: "a.b", ext: ".csv", want: filepath.Join("out", "a.b.csv")},
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
