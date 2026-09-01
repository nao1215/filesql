package atomicwrite

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWrite_ZeroOptions covers the wording a caller with no sentinel of its own
// gets. The only caller in this module passes one, so nothing else exercises
// the defaults and this is what keeps them working for the next caller.
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
// DumpDatabase reports.
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

// makeFIFO creates a named pipe at path, or skips the test where the platform
// has none. syscall.Mkfifo is not on every platform this package builds for, so
// the pipe is made with the tool that is.
func makeFIFO(t *testing.T, path string) {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("windows has no named pipes in the filesystem")
	}
	if err := exec.CommandContext(t.Context(), "mkfifo", path).Run(); err != nil { //nolint:gosec // the command is fixed and the path is under t.TempDir()
		t.Skipf("this platform cannot make a named pipe here: %v", err)
	}
}

// TestWrite_RefusesADestinationThatIsNotAFile pins that a write replaces a file
// and nothing else.
//
// It replaced whatever was there. A named pipe became a regular file with no
// error, so a dump into a directory someone else had set up destroyed the pipe
// that was in it and reported success; and a directory in the way failed from
// inside the backup step, reporting the name of a file this package invented
// ("write /out/.users.csv.bak264152240: copy_file_range: is a directory")
// rather than saying that the destination is a directory.
func TestWrite_RefusesADestinationThatIsNotAFile(t *testing.T) {
	t.Parallel()

	newContents := func(w io.Writer) error {
		_, err := w.Write([]byte("new"))
		return err
	}

	t.Run("a directory in the way is named as one", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "users.csv")
		require.NoError(t, os.Mkdir(dest, 0o750))

		err := Write(dest, newContents, Options{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), dest)
		assert.Contains(t, err.Error(), "a directory")
		assert.NotContains(t, err.Error(), backupSuffix, "a caller never named the backup file")
		assert.Equal(t, []string{"users.csv"}, dirEntries(t, dir), "nothing may be left beside it")
	})

	t.Run("a named pipe is left where it was", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "users.csv")
		makeFIFO(t, dest)

		err := Write(dest, newContents, Options{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), dest)
		assert.Contains(t, err.Error(), "a named pipe")

		info, statErr := os.Lstat(dest)
		require.NoError(t, statErr)
		assert.NotZero(t, info.Mode()&os.ModeNamedPipe, "the pipe has to still be a pipe")
	})

	t.Run("a regular file is still replaced", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "users.csv")
		require.NoError(t, os.WriteFile(dest, []byte("old"), 0o600))

		require.NoError(t, Write(dest, newContents, Options{}))
		assert.Equal(t, "new", readFile(t, dest))
	})

	t.Run("a destination that is not there yet is still written", func(t *testing.T) {
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "users.csv")
		require.NoError(t, Write(dest, newContents, Options{}))
		assert.Equal(t, "new", readFile(t, dest))
	})
}

// TestDescribeFileMode covers the naming on its own, for the kinds a platform
// may not let a test create. It is what both the load side and the write side
// call, so a kind is named one way wherever it is refused.
func TestDescribeFileMode(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		mode os.FileMode
		want string
	}{
		{name: "a directory", mode: os.ModeDir | 0o755, want: "a directory"},
		{name: "a named pipe", mode: os.ModeNamedPipe | 0o644, want: "a named pipe"},
		{name: "a socket", mode: os.ModeSocket | 0o644, want: "a socket"},
		{name: "a character device", mode: os.ModeDevice | os.ModeCharDevice | 0o644, want: "a character device"},
		{name: "a block device", mode: os.ModeDevice | 0o644, want: "a block device"},
		{name: "a kind the system does not name", mode: os.ModeIrregular | 0o644, want: "not a regular file"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, DescribeFileMode(tt.mode))
		})
	}
}

// TestStage_HoldsEveryFileBackUntilCommit pins the two-phase form: a staged
// file has touched nothing at its destination, a discard leaves it that way,
// and a commit puts every one of them in place. It is what lets a caller
// replacing several files replace all of them or none.
func TestStage_HoldsEveryFileBackUntilCommit(t *testing.T) {
	t.Parallel()

	t.Run("a staged file leaves the destination alone", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "out.txt")
		require.NoError(t, os.WriteFile(dest, []byte("old"), 0o600))

		staged, err := Stage(dest, func(w io.Writer) error {
			_, writeErr := io.WriteString(w, "new")
			return writeErr
		}, Options{})
		require.NoError(t, err)

		body, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "old", string(body), "nothing is put in place until Commit")

		require.NoError(t, staged.Commit())
		body, err = os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "new", string(body))
		assert.Equal(t, []string{"out.txt"}, dirEntries(t, dir), "a committed file leaves no staged one")
	})

	t.Run("a discarded set leaves every destination alone", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		first := filepath.Join(dir, "first.txt")
		second := filepath.Join(dir, "second.txt")
		require.NoError(t, os.WriteFile(first, []byte("old first"), 0o600))
		require.NoError(t, os.WriteFile(second, []byte("old second"), 0o600))

		dests := []string{first, second}
		staged := make([]*Staged, 0, len(dests))
		for _, dest := range dests {
			one, err := Stage(dest, func(w io.Writer) error {
				_, writeErr := io.WriteString(w, "new")
				return writeErr
			}, Options{})
			require.NoError(t, err)
			staged = append(staged, one)
		}
		for _, one := range staged {
			require.NoError(t, one.Discard())
		}

		assert.Equal(t, []string{"first.txt", "second.txt"}, dirEntries(t, dir), "no staged file may be left behind")
		for dest, want := range map[string]string{first: "old first", second: "old second"} {
			body, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, want, string(body))
		}
	})

	t.Run("a refused write stages nothing", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "out.txt")
		require.NoError(t, os.WriteFile(dest, []byte("old"), 0o600))

		refused := errors.New("the encoder refused this value")
		staged, err := Stage(dest, func(io.Writer) error { return refused }, Options{})
		require.ErrorIs(t, err, refused, "the write error is the one to report")
		assert.Nil(t, staged)

		body, readErr := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "old", string(body))
		assert.Equal(t, []string{"out.txt"}, dirEntries(t, dir), "no staged file may be left behind")
	})

	t.Run("discarding twice is a no-op", func(t *testing.T) {
		t.Parallel()

		dest := filepath.Join(t.TempDir(), "out.txt")
		staged, err := Stage(dest, func(w io.Writer) error {
			_, writeErr := io.WriteString(w, "new")
			return writeErr
		}, Options{})
		require.NoError(t, err)

		require.NoError(t, staged.Discard())
		assert.NoError(t, staged.Discard(), "a staged file already gone is not a cleanup failure")
		assert.NoFileExists(t, dest)
	})
}

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
