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

// TestWrite_FollowsASymlink pins that a destination which is a symbolic link is
// followed rather than replaced. A rename puts the staged file where the link
// itself was, so the link disappeared and the file it named still held the old
// rows: the caller's edit landed at a path they never asked for, and the tool
// reading the real file saw no change at all.
func TestWrite_FollowsASymlink(t *testing.T) {
	t.Parallel()

	newContents := func(w io.Writer) error {
		_, err := w.Write([]byte("new"))
		return err
	}

	t.Run("a link beside its target", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		target := filepath.Join(dir, "target.csv")
		require.NoError(t, os.WriteFile(target, []byte("old"), 0o600))
		link := filepath.Join(dir, "link.csv")
		requireSymlink(t, target, link)

		require.NoError(t, Write(link, newContents, Options{}))

		assertSymlink(t, link, target)
		assert.Equal(t, "new", readFile(t, target), "the target is what receives the write")
		assert.Equal(t, []string{"link.csv", "target.csv"}, dirEntries(t, dir))
	})

	t.Run("a link into another directory", func(t *testing.T) {
		t.Parallel()

		// The staging has to move with the target, or the rename crosses from
		// the link's directory into the target's; a link beside its target
		// cannot tell the two apart.
		root := t.TempDir()
		data := filepath.Join(root, "data")
		require.NoError(t, os.Mkdir(data, 0o750))
		target := filepath.Join(data, "target.csv")
		require.NoError(t, os.WriteFile(target, []byte("old"), 0o600))
		work := filepath.Join(root, "work")
		require.NoError(t, os.Mkdir(work, 0o750))
		link := filepath.Join(work, "users.csv")
		requireSymlink(t, target, link)

		require.NoError(t, Write(link, newContents, Options{}))

		assertSymlink(t, link, target)
		assert.Equal(t, "new", readFile(t, target))
		assert.Equal(t, []string{"users.csv"}, dirEntries(t, work), "no staged file may be left beside the link")
		assert.Equal(t, []string{"target.csv"}, dirEntries(t, data), "nor beside the target")
	})

	t.Run("a relative link", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		target := filepath.Join(dir, "target.csv")
		require.NoError(t, os.WriteFile(target, []byte("old"), 0o600))
		link := filepath.Join(dir, "link.csv")
		if err := os.Symlink("target.csv", link); err != nil {
			t.Skipf("this platform does not allow a symlink to be created: %v", err)
		}

		require.NoError(t, Write(link, newContents, Options{}))

		assertSymlink(t, link, target)
		assert.Equal(t, "new", readFile(t, target))
	})

	t.Run("a chain of links", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		target := filepath.Join(dir, "target.csv")
		require.NoError(t, os.WriteFile(target, []byte("old"), 0o600))
		middle := filepath.Join(dir, "middle.csv")
		requireSymlink(t, target, middle)
		link := filepath.Join(dir, "link.csv")
		requireSymlink(t, middle, link)

		require.NoError(t, Write(link, newContents, Options{}))

		assertSymlink(t, link, middle)
		assertSymlink(t, middle, target)
		assert.Equal(t, "new", readFile(t, target))
	})

	t.Run("a dangling link is written at its own path", func(t *testing.T) {
		t.Parallel()

		// There is no file to follow, so the link is where the contents go; the
		// alternative is creating a file the caller never named.
		dir := t.TempDir()
		link := filepath.Join(dir, "link.csv")
		requireSymlink(t, filepath.Join(dir, "gone.csv"), link)

		require.NoError(t, Write(link, newContents, Options{}))

		assert.Equal(t, "new", readFile(t, link))
		assert.Equal(t, []string{"link.csv"}, dirEntries(t, dir), "the link's own path is the only thing written")
	})

	t.Run("the target's mode is the one that is kept", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Windows does not carry Unix permission bits")
		}
		t.Parallel()

		dir := t.TempDir()
		target := filepath.Join(dir, "target.csv")
		require.NoError(t, os.WriteFile(target, []byte("old"), 0o600))
		link := filepath.Join(dir, "link.csv")
		requireSymlink(t, target, link)

		require.NoError(t, Write(link, newContents, Options{}))

		info, err := os.Stat(target)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "the target's mode, not the mode a new file gets")
	})
}

// requireSymlink creates link pointing at target, or skips the test where the
// platform refuses: Windows needs a privilege for it that a test run may lack.
func requireSymlink(t *testing.T, target, link string) {
	t.Helper()

	if err := os.Symlink(target, link); err != nil {
		t.Skipf("this platform does not allow a symlink to be created: %v", err)
	}
}

// assertSymlink asserts link is still a symbolic link naming target.
func assertSymlink(t *testing.T, link, target string) {
	t.Helper()

	info, err := os.Lstat(link)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink, "%s must still be a symbolic link", link)
	resolved, err := filepath.EvalSymlinks(link)
	require.NoError(t, err)
	wantResolved, err := filepath.EvalSymlinks(target)
	require.NoError(t, err)
	assert.Equal(t, wantResolved, resolved)
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	got, err := os.ReadFile(path) //nolint:gosec // path from t.TempDir()
	require.NoError(t, err)
	return string(got)
}
