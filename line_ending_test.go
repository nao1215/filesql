package filesql

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLineEnding_String pins the names of the terminators.
func TestLineEnding_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "lf", LineEndingLF.String())
	assert.Equal(t, "crlf", LineEndingCRLF.String())
	assert.Equal(t, unknownName, LineEnding(9).String())
}

// TestLineEnding_Terminator covers the bytes each value writes, including a
// value from outside the set: a save must still terminate its records, so an
// unknown one writes the default rather than nothing.
func TestLineEnding_Terminator(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "\n", LineEndingLF.terminator())
	assert.Equal(t, "\r\n", LineEndingCRLF.terminator())
	assert.Equal(t, "\n", LineEnding(9).terminator())
}

// TestDominantLineEnding covers the rule that decides a file's terminator. The
// majority wins so that a file with one stray ending keeps the one the rest of
// its lines use — rewriting those lines is the loss this whole feature exists to
// prevent.
func TestDominantLineEnding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sample string
		want   LineEnding
	}{
		{"all CRLF", "id,v\r\n1,a\r\n2,b\r\n", LineEndingCRLF},
		{"all LF", "id,v\n1,a\n2,b\n", LineEndingLF},
		{"mostly CRLF", "id,v\r\n1,a\r\n2,b\n", LineEndingCRLF},
		{"mostly LF", "id,v\n1,a\n2,b\r\n", LineEndingLF},
		{"a tie keeps LF", "id,v\r\n1,a\n", LineEndingLF},
		{"no line ending at all", "id,v", LineEndingLF},
		{"nothing at all", "", LineEndingLF},
		{"a lone carriage return is not a terminator", "id,v\r1,a\n", LineEndingLF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, dominantLineEnding([]byte(tt.sample)))
		})
	}
}

// TestDetectLineEnding covers reading the terminator off a file, which is what
// an in-place save does before it rewrites one.
func TestDetectLineEnding(t *testing.T) {
	t.Parallel()

	t.Run("a plain file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		crlf := filepath.Join(dir, "crlf.csv")
		lf := filepath.Join(dir, "lf.csv")
		require.NoError(t, os.WriteFile(crlf, []byte("id,v\r\n1,a\r\n"), 0o600))
		require.NoError(t, os.WriteFile(lf, []byte("id,v\n1,a\n"), 0o600))

		assert.Equal(t, LineEndingCRLF, detectLineEnding(crlf))
		assert.Equal(t, LineEndingLF, detectLineEnding(lf))
	})

	t.Run("a compressed file is read through its codec", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "crlf.csv.gz")
		file, err := os.Create(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		gz := gzip.NewWriter(file)
		_, err = gz.Write([]byte("id,v\r\n1,a\r\n"))
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, file.Close())

		assert.Equal(t, LineEndingCRLF, detectLineEnding(path),
			"the terminator is in the compressed bytes, so the codec has to be undone to see it")
	})

	t.Run("a file that cannot be read answers with the default", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, LineEndingLF, detectLineEnding(filepath.Join(t.TempDir(), "missing.csv")),
			"detection is on the destination's behalf and must not fail the save")
	})
}
