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

// TestDumpDatabase_FailedWriteLeavesDestinationIntact pins that the tabular dump
// path is staged as well as the financial ones. A format the writer rejects
// reaches its switch after the destination has been opened, which used to
// truncate whatever was already there — the source file itself when the dump is
// a write-back over it.
func TestDumpDatabase_FailedWriteLeavesDestinationIntact(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "data.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

	db, err := OpenContext(ctx, src)
	require.NoError(t, err)
	defer db.Close()

	// An out-of-range format is rejected by the writer's switch, past the point
	// where the destination used to be truncated.
	opts := NewDumpOptions().WithFormat(OutputFormat(9999))

	outDir := t.TempDir()
	dest := filepath.Join(outDir, "data"+opts.FileExtension())
	original := []byte("this content must survive\n")
	require.NoError(t, os.WriteFile(dest, original, 0o600))

	require.Error(t, DumpDatabase(db, outDir, opts))

	after, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, original, after, "a rejected dump must leave the destination unchanged")

	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "no staged file may be left behind: %v", entries)
}

// TestDumpDatabase_SucceedsThroughStaging keeps the ordinary path honest: the
// staging must not change what a working dump produces.
func TestDumpDatabase_SucceedsThroughStaging(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "data.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

	db, err := OpenContext(ctx, src)
	require.NoError(t, err)
	defer db.Close()

	outDir := t.TempDir()
	require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatCSV)))

	got, err := os.ReadFile(filepath.Join(outDir, "data.csv")) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Contains(t, string(got), "alice")

	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "no staged file may be left behind: %v", entries)
}

// TestCommitStagedFile covers the commit half, including the path taken when the
// plain rename is refused. Windows refuses to rename over a destination another
// handle still has open, which is exactly a save that overwrites a file this
// package is streaming from, so the fallback is not a rare path there.
func TestCommitStagedFile(t *testing.T) {
	t.Parallel()

	t.Run("replaces an existing destination", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		staged := filepath.Join(dir, "staged")
		dest := filepath.Join(dir, "dest")
		require.NoError(t, os.WriteFile(staged, []byte("new"), 0o600))
		require.NoError(t, os.WriteFile(dest, []byte("old content that is longer"), 0o600))

		require.NoError(t, commitStagedFile(staged, dest))

		got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "new", string(got), "no tail of the old content may survive")

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Len(t, entries, 1, "neither the staged nor the moved-aside file may be left: %v", entries)
	})

	t.Run("creates a destination that does not exist", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		staged := filepath.Join(dir, "staged")
		dest := filepath.Join(dir, "dest")
		require.NoError(t, os.WriteFile(staged, []byte("new"), 0o600))

		require.NoError(t, commitStagedFile(staged, dest))

		got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "new", string(got))
	})

	// The move-aside fallback, which is what runs on Windows whenever the
	// destination is open. It is driven directly here because a plain rename
	// succeeds on Unix, so commitStagedFile never reaches it on this platform.
	t.Run("the move-aside fallback replaces the destination", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		staged := filepath.Join(dir, "staged")
		dest := filepath.Join(dir, "dest")
		require.NoError(t, os.WriteFile(staged, []byte("new"), 0o600))
		require.NoError(t, os.WriteFile(dest, []byte("old content that is longer"), 0o600))

		require.NoError(t, commitByMovingAside(staged, dest))

		got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "new", string(got))

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Len(t, entries, 1, "the moved-aside file must not be left behind: %v", entries)
	})

	t.Run("the move-aside fallback restores the destination when it cannot finish", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "dest")
		require.NoError(t, os.WriteFile(dest, []byte("precious"), 0o600))

		// A staged file that is not there makes the second rename fail, after the
		// destination has already been moved aside.
		require.Error(t, commitByMovingAside(filepath.Join(dir, "missing"), dest))

		got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "precious", string(got), "a refused commit must put the destination back")

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Len(t, entries, 1, "the moved-aside file must not be left behind: %v", entries)
	})

	t.Run("reports a staged file that is gone", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.Error(t, commitStagedFile(filepath.Join(dir, "missing"), filepath.Join(dir, "dest")))
	})
}
