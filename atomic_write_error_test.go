package filesql

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteFileAtomically_ReportsARefusedCommit drives the failure that the
// staging exists for: everything is written, and only the last step — putting
// the staged file where the caller asked — is refused. The destination here is a
// directory, which no rename can replace.
func TestWriteFileAtomically_ReportsARefusedCommit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dest := filepath.Join(dir, "a-directory")
	require.NoError(t, os.Mkdir(dest, 0o750))

	err := writeFileAtomically(dest, func(w io.Writer) error {
		_, writeErr := w.Write([]byte("payload"))
		return writeErr
	})
	require.Error(t, err, "a destination that cannot be replaced must be reported")
	assert.ErrorIs(t, err, ErrIOOperation)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "the staged file must not be left behind: %v", entries)
}

// TestCommitStagedFile_FallsBackWhenTheDestinationIsInTheWay covers the branch
// that tells the two rename failures apart. A destination that is still there
// after a failed rename is the Windows case the copy fallback exists for, so the
// fallback runs and reports its own failure; a destination that is not there
// cannot be helped by copying, and the rename error is returned as is.
func TestCommitStagedFile_FallsBackWhenTheDestinationIsInTheWay(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dest := filepath.Join(dir, "dest")
	require.NoError(t, os.WriteFile(dest, []byte("precious"), 0o600))

	err := commitStagedFile(filepath.Join(dir, "missing"), dest)
	require.Error(t, err, "a staged file that is gone cannot be committed by either route")

	got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "precious", string(got), "the destination must survive a refused commit")
}

// TestCommitByCopy_ReportsAnUnbackupableDestination covers the first step of the
// fallback. Without a backup there is nothing to restore from, so the copy must
// not start at all.
func TestCommitByCopy_ReportsAnUnbackupableDestination(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	staged := filepath.Join(dir, "staged")
	require.NoError(t, os.WriteFile(staged, []byte("new"), 0o600))

	err := commitByCopy(staged, filepath.Join(dir, "no-such-directory", "dest"))
	assert.Error(t, err, "a destination whose directory does not exist cannot be backed up")
}

// TestCopyToBackup covers the two answers of the backup step.
func TestCopyToBackup(t *testing.T) {
	t.Parallel()

	t.Run("copies the file beside itself", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "dest")
		require.NoError(t, os.WriteFile(path, []byte("content"), 0o600))

		backup, err := copyToBackup(path)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, os.Remove(backup)) })

		assert.Equal(t, dir, filepath.Dir(backup), "the backup belongs in the same directory as the file")
		got, err := os.ReadFile(backup) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "content", string(got))
	})

	t.Run("reports a file that cannot be read", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		_, err := copyToBackup(filepath.Join(dir, "missing"))
		require.Error(t, err)

		assertNoBackupLeft(t, dir)
	})

	t.Run("reports a directory it cannot write the backup into", func(t *testing.T) {
		t.Parallel()

		_, err := copyToBackup(filepath.Join(t.TempDir(), "no-such-directory", "dest"))
		assert.Error(t, err)
	})
}

// TestCopyOnto covers the failures of the copy itself, which is what the
// fallback restores from.
func TestCopyOnto(t *testing.T) {
	t.Parallel()

	t.Run("reports a destination that cannot be opened for writing", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := filepath.Join(dir, "src")
		require.NoError(t, os.WriteFile(src, []byte("content"), 0o600))

		assert.Error(t, copyOnto(src, dir), "a directory cannot be opened as an output file")
	})

	t.Run("reports a source it cannot read", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Windows refuses to open a directory as a file, so the read never starts")
		}
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "dest")
		require.NoError(t, os.WriteFile(dest, []byte("old"), 0o600))

		assert.Error(t, copyOnto(dir, dest), "reading a directory as a file must be reported")
	})
}
