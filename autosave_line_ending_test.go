package filesql

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAutoSaveOverwriteKeepsLineEnding pins that a save in place writes back the
// terminator the file already used.
//
// It did not: every record was written with "\n" whatever the source used, so a
// CRLF file came back LF throughout. A caller who edited one row got a file
// whose every line had changed — a whole-file diff in a repository configured
// for CRLF, and a file the tools that read it no longer saw as they had.
func TestAutoSaveOverwriteKeepsLineEnding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
		update  string
		want    string
	}{
		{
			name:    "CSV keeps CRLF",
			file:    "crlf.csv",
			content: "id,v\r\n1,a\r\n2,b\r\n",
			update:  "UPDATE crlf SET v='x' WHERE id=1",
			want:    "id,v\r\n1,x\r\n2,b\r\n",
		},
		{
			name:    "CSV keeps LF",
			file:    "lf.csv",
			content: "id,v\n1,a\n2,b\n",
			update:  "UPDATE lf SET v='x' WHERE id=1",
			want:    "id,v\n1,x\n2,b\n",
		},
		{
			name:    "TSV keeps CRLF",
			file:    "crlf.tsv",
			content: "id\tv\r\n1\ta\r\n2\tb\r\n",
			update:  "UPDATE crlf SET v='x' WHERE id=1",
			want:    "id\tv\r\n1\tx\r\n2\tb\r\n",
		},
		{
			name:    "LTSV keeps CRLF",
			file:    "crlf.ltsv",
			content: "id:1\tv:a\r\nid:2\tv:b\r\n",
			update:  "UPDATE crlf SET v='x' WHERE id=1",
			want:    "id:1\tv:x\r\nid:2\tv:b\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), tt.file)
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{path}, tt.update))

			got, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got), "only the edited row may differ from what was there")
		})
	}
}

// TestAutoSaveOverwriteKeepsLineEndingUnderCompression checks that the
// terminator is read from the bytes inside the codec, not from the archive.
func TestAutoSaveOverwriteKeepsLineEndingUnderCompression(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "crlf.csv.gz")
	file, err := os.Create(path) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	gz := gzip.NewWriter(file)
	_, err = gz.Write([]byte("id,v\r\n1,a\r\n2,b\r\n"))
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.NoError(t, file.Close())

	require.NoError(t, autoSaveOverwrite(t, []string{path}, "UPDATE crlf SET v='x' WHERE id=1"))

	reopened, err := os.Open(path) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	defer reopened.Close()
	reader, err := gzip.NewReader(reopened)
	require.NoError(t, err)
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "id,v\r\n1,x\r\n2,b\r\n", string(decompressed))
}

// TestDumpDatabase_WithLineEnding covers the option on a dump to a new
// destination, where there is no existing file to take the terminator from.
func TestDumpDatabase_WithLineEnding(t *testing.T) {
	t.Parallel()

	source := filepath.Join(t.TempDir(), "users.csv")
	require.NoError(t, os.WriteFile(source, []byte("id,v\n1,a\n"), 0o600))

	db, err := Open(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	outputDir := t.TempDir()
	require.NoError(t, DumpDatabase(db, outputDir, NewDumpOptions().WithLineEnding(LineEndingCRLF)))

	got, err := os.ReadFile(filepath.Join(outputDir, "users.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,v\r\n1,a\r\n", string(got))
}

// TestNewDumpOptions_DefaultsToLF pins the default, which is what a save wrote
// before the option existed.
func TestNewDumpOptions_DefaultsToLF(t *testing.T) {
	t.Parallel()

	assert.Equal(t, LineEndingLF, NewDumpOptions().LineEnding)
	assert.Equal(t, LineEndingCRLF, NewDumpOptions().WithLineEnding(LineEndingCRLF).LineEnding)
}
