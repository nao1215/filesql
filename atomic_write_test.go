package filesql

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteFileAtomically(t *testing.T) {
	t.Parallel()

	t.Run("creates a new file", func(t *testing.T) {
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "out.txt")
		err := writeFileAtomically(dest, func(w io.Writer) error {
			_, err := io.WriteString(w, "hello")
			return err
		})
		require.NoError(t, err)

		got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "hello", string(got))
	})

	t.Run("replaces an existing file", func(t *testing.T) {
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "out.txt")
		require.NoError(t, os.WriteFile(dest, []byte("old content that is longer"), 0o600))

		err := writeFileAtomically(dest, func(w io.Writer) error {
			_, err := io.WriteString(w, "new")
			return err
		})
		require.NoError(t, err)

		got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "new", string(got), "the old content must not survive as a tail")
	})

	t.Run("a failed write leaves the destination and the directory untouched", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "out.txt")
		require.NoError(t, os.WriteFile(dest, []byte("original"), 0o600))

		sentinel := errors.New("encoder rejected the data")
		err := writeFileAtomically(dest, func(w io.Writer) error {
			// Write some bytes first, the way an encoder that validates midway does.
			if _, err := io.WriteString(w, "partial"); err != nil {
				return err
			}
			return sentinel
		})
		require.ErrorIs(t, err, sentinel, "the writer's own error must reach the caller unwrapped")

		got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "original", string(got))

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Len(t, entries, 1, "the staged file must be removed: %v", entries)
	})

	t.Run("a failed write does not create a destination that was absent", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "out.txt")

		err := writeFileAtomically(dest, func(io.Writer) error {
			return errors.New("nope")
		})
		require.Error(t, err)

		_, statErr := os.Stat(dest)
		assert.ErrorIs(t, statErr, os.ErrNotExist)

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Empty(t, entries, "the staged file must be removed: %v", entries)
	})

	t.Run("an existing file keeps its permissions", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Windows does not model Unix permission bits")
		}
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "out.txt")
		require.NoError(t, os.WriteFile(dest, []byte("old"), 0o640)) //nolint:gosec // The 0640 mode is the subject of this test

		err := writeFileAtomically(dest, func(w io.Writer) error {
			_, err := io.WriteString(w, "new")
			return err
		})
		require.NoError(t, err)

		info, err := os.Stat(dest)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o640), info.Mode().Perm())
	})

	t.Run("a new file is readable, not the 0600 of a temp file", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Windows does not model Unix permission bits")
		}
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "out.txt")
		require.NoError(t, writeFileAtomically(dest, func(w io.Writer) error {
			_, err := io.WriteString(w, "new")
			return err
		}))

		info, err := os.Stat(dest)
		require.NoError(t, err)
		assert.Equal(t, defaultOutputFileMode, info.Mode().Perm())
	})

	t.Run("reports a destination directory that does not exist", func(t *testing.T) {
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "missing", "out.txt")
		err := writeFileAtomically(dest, func(w io.Writer) error {
			_, err := io.WriteString(w, "hello")
			return err
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrIOOperation)
	})
}
