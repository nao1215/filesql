package atomicwrite

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWrite_ZeroOptions covers the wording a caller with no sentinel of its own
// gets. frame passes a zero Options, so these are the errors ToCSV and ToTSV
// report, and nothing else in this module exercises the defaults.
func TestWrite_ZeroOptions(t *testing.T) {
	t.Parallel()

	t.Run("a staging failure is reported in plain words", func(t *testing.T) {
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "no-such-directory", "out.csv")

		err := Write(dest, func(io.Writer) error { return nil }, Options{})
		require.Error(t, err, "a directory that does not exist has nowhere to stage")
		assert.Contains(t, err.Error(), "failed to create a temporary file next to")
		assert.Contains(t, err.Error(), dest)
	})

	t.Run("the caller's own error is passed through untouched", func(t *testing.T) {
		t.Parallel()

		refused := errors.New("this value cannot be represented")
		dir := t.TempDir()
		dest := filepath.Join(dir, "out.tsv")
		require.NoError(t, os.WriteFile(dest, []byte("precious"), 0o600))

		err := Write(dest, func(w io.Writer) error {
			// Write something first: the destination must survive a write that
			// fails after it has begun, which is the whole point.
			if _, writeErr := w.Write([]byte("partial")); writeErr != nil {
				return writeErr
			}
			return refused
		}, Options{})
		require.ErrorIs(t, err, refused, "the write's own error is the one to report")

		got, err := os.ReadFile(dest) //nolint:gosec // path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "precious", string(got))
		assert.Equal(t, []string{"out.tsv"}, dirEntries(t, dir), "the staged file must not be left behind")
	})

	t.Run("a cleanup failure is joined onto the error being returned", func(t *testing.T) {
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

		err := Write(dest, func(w io.Writer) error {
			if _, writeErr := w.Write([]byte("id\n1\n")); writeErr != nil {
				return writeErr
			}
			// Take the directory's write bit away, so both the rename and the
			// staged file's removal are refused.
			return os.Chmod(dir, 0o500) //nolint:gosec // A directory mode, deliberately read-only for this test
		}, Options{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "remove the staged file", "a leftover in the caller's directory must be reported")
	})

	t.Run("a success reports no cleanup at all", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "out.csv")

		require.NoError(t, Write(dest, func(w io.Writer) error {
			_, writeErr := w.Write([]byte("id\n1\n"))
			return writeErr
		}, Options{}))

		got, err := os.ReadFile(dest) //nolint:gosec // path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "id\n1\n", string(got))
		assert.Equal(t, []string{"out.csv"}, dirEntries(t, dir))
	})
}

// TestWrite_OptionsAreUsed pins that a caller's own wording reaches the error,
// which is what keeps filesql's ErrIOOperation and ErrCleanup on the errors
// DumpDatabase reports while frame gets plain ones.
func TestWrite_OptionsAreUsed(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("caller's sentinel")
	dest := filepath.Join(t.TempDir(), "no-such-directory", "out.csv")

	err := Write(dest, func(io.Writer) error { return nil }, Options{
		FailIO: func(what string, err error) error {
			return errors.Join(sentinel, errors.New(what), err)
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel, "the caller's wording must be what comes back")
	assert.True(t, strings.Contains(err.Error(), "failed to create a temporary file next to"))
}

// TestWrite_KeepsTheDestinationsMode pins that replacing a file does not reset
// its permissions to the ones a new file gets.
func TestWrite_KeepsTheDestinationsMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not carry Unix permission bits")
	}
	t.Parallel()

	t.Run("an existing destination keeps its own", func(t *testing.T) {
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "out.csv")
		require.NoError(t, os.WriteFile(dest, []byte("old"), 0o600))

		require.NoError(t, Write(dest, func(w io.Writer) error {
			_, err := w.Write([]byte("new"))
			return err
		}, Options{}))

		info, err := os.Stat(dest)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	})

	t.Run("a new destination gets the default", func(t *testing.T) {
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "out.csv")

		require.NoError(t, Write(dest, func(w io.Writer) error {
			_, err := w.Write([]byte("new"))
			return err
		}, Options{}))

		info, err := os.Stat(dest)
		require.NoError(t, err)
		assert.Equal(t, DefaultFileMode, info.Mode().Perm())
	})
}
