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

// TestWriteFileAtomically_ReportsAStagedFileItCannotRemove pins the contract the
// deferred cleanup states: a staged file left in the caller's directory is
// reported rather than dropped.
//
// It was dropped. The result was unnamed, so the join inside the deferred
// closure wrote to a local nothing read afterwards, and a save that left a hidden
// .tmp file beside the caller's data returned no sign of it.
func TestWriteFileAtomically_ReportsAStagedFileItCannotRemove(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory permissions do not stop a rename on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	t.Parallel()

	dir := t.TempDir()
	dest := filepath.Join(dir, "out.csv")
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o700) //nolint:errcheck,gosec // Test cleanup so t.TempDir can remove the directory
	})

	err := writeFileAtomically(dest, func(w io.Writer) error {
		if _, writeErr := w.Write([]byte("id\n1\n")); writeErr != nil {
			return writeErr
		}
		// Take the directory's write bit away, so both the rename and the
		// staged file's removal are refused.
		return os.Chmod(dir, 0o500) //nolint:gosec // A directory mode, deliberately read-only for this test
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIOOperation, "the failed replace stays the primary error")
	assert.ErrorIs(t, err, ErrCleanup, "the staged file that is still there must be reported")
}

// TestWriteFileAtomically_ReportsNoCleanupWhenNothingIsLeft is the sibling that
// keeps the fix honest: the rename consumes the staged file, and that is not a
// cleanup failure.
func TestWriteFileAtomically_ReportsNoCleanupWhenNothingIsLeft(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dest := filepath.Join(dir, "out.csv")
	require.NoError(t, writeFileAtomically(dest, func(w io.Writer) error {
		_, writeErr := w.Write([]byte("id\n1\n"))
		return writeErr
	}))
	assert.Equal(t, []string{"out.csv"}, dirEntries(t, dir))
}
