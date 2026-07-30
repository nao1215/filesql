package filesql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
