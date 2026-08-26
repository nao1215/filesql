package atomicwrite

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dirEntries lists dir's entries by name, sorted, so a test can say exactly what
// a directory holds after a call.
func dirEntries(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	slices.Sort(names)
	return names
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
		assert.Len(t, entries, 1, "neither the staged file nor a backup may be left: %v", entries)
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

	// The copy fallback, which is what runs on Windows whenever the destination is
	// open. It is driven directly here because a plain rename succeeds on Unix, so
	// commitStagedFile never reaches it on this platform.
	t.Run("the copy fallback replaces the destination", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		staged := filepath.Join(dir, "staged")
		dest := filepath.Join(dir, "dest")
		require.NoError(t, os.WriteFile(staged, []byte("new"), 0o600))
		require.NoError(t, os.WriteFile(dest, []byte("old content that is longer"), 0o600))

		require.NoError(t, commitByCopy(staged, dest))

		got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "new", string(got))

		// The staged file is still there: unlike a rename, a copy does not consume
		// it, and its caller removes it. What must be gone is the backup.
		assertNoBackupLeft(t, dir)
	})

	t.Run("the copy fallback restores the destination when it cannot finish", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "dest")
		require.NoError(t, os.WriteFile(dest, []byte("precious"), 0o600))

		// A staged file that is not there makes the copy fail, after the backup has
		// been taken.
		require.Error(t, commitByCopy(filepath.Join(dir, "missing"), dest))

		got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "precious", string(got), "a refused commit must put the destination back")

		assertNoBackupLeft(t, dir)
	})

	t.Run("reports a staged file that is gone", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.Error(t, commitStagedFile(filepath.Join(dir, "missing"), filepath.Join(dir, "dest")))
	})
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

// TestCommitByCopy_ReplacesTheDestination is the success half of the fallback,
// exercised deliberately here because the only platform that reaches it on its
// own is Windows.
func TestCommitByCopy_ReplacesTheDestination(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dest := filepath.Join(dir, "dest.csv")
	require.NoError(t, os.WriteFile(dest, []byte("old content that is longer"), 0o600))
	staged := filepath.Join(dir, "staged")
	require.NoError(t, os.WriteFile(staged, []byte("new"), 0o600))

	require.NoError(t, commitByCopy(staged, dest))

	got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "new", string(got), "the old content must not survive as a tail")
	assert.Equal(t, []string{"dest.csv", "staged"}, dirEntries(t, dir), "the backup must not be left behind")
}

// TestCommitByCopy_KeepsTheDestinationWhenTheCopyFails pins what stands in for
// the atomicity a plain rename gives. The copy truncates the destination before
// it can fail, so the backup taken first is what puts the original bytes back —
// best effort, since that restore is itself a copy, which is why the copy error
// stays the one reported.
func TestCommitByCopy_KeepsTheDestinationWhenTheCopyFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dest := filepath.Join(dir, "dest.csv")
	require.NoError(t, os.WriteFile(dest, []byte("precious"), 0o600))

	// A staged path that is a directory cannot be copied from, so the copy
	// fails after the backup was taken.
	staged := filepath.Join(dir, "staged")
	require.NoError(t, os.Mkdir(staged, 0o750))

	require.Error(t, commitByCopy(staged, dest))

	got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "precious", string(got), "a refused copy must leave the destination as it was")
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

// assertNoBackupLeft fails when the commit's own backup file survived the call.
func assertNoBackupLeft(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".bak", "the backup must not be left behind: %s", e.Name())
	}
}
