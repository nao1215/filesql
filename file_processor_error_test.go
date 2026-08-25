package filesql

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollectFilesFromPaths_UnsupportedFile covers a named file whose extension
// is not a format this package reads. Naming a file explicitly is a request to
// load it, so it is refused rather than skipped the way an unrelated file inside
// a directory is.
func TestCollectFilesFromPaths_UnsupportedFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "notes.docx")
	require.NoError(t, os.WriteFile(path, []byte("content"), 0o600))

	_, err := newFileProcessor().collectFilesFromPaths([]string{path})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
}

// TestCollectFilesFromDirectory_UnreadableDirectory covers a directory the
// process cannot walk. The load stops rather than returning the files it managed
// to reach, because a partial set of tables is worse than no load at all.
func TestCollectFilesFromDirectory_UnreadableDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory permissions do not stop a walk on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root reads a directory whatever its mode says")
	}
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "closed")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "users.csv"), []byte("id\n1\n"), 0o600))
	require.NoError(t, os.Chmod(dir, 0o000))
	// The directory has to be traversable again, or the temporary directory it
	// lives in cannot be removed.
	t.Cleanup(func() { require.NoError(t, os.Chmod(dir, 0o700)) }) //nolint:gosec // A directory needs its execute bit to be walked and removed

	_, err := newFileProcessor().collectFilesFromPaths([]string{dir})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIOOperation)
}

// TestProcessFSToReaders_FindsFilesInSubdirectories covers the walk that runs
// after the glob. A glob pattern matches one directory level, so a workbook or
// CSV one directory down is only found by the walk.
func TestProcessFSToReaders_FindsFilesInSubdirectories(t *testing.T) {
	t.Parallel()

	filesystem := fstest.MapFS{
		"top.csv":            &fstest.MapFile{Data: []byte("id\n1\n")},
		"nested/deep/in.csv": &fstest.MapFile{Data: []byte("id\n2\n")},
		"nested/notes.txt":   &fstest.MapFile{Data: []byte("ignored")},
	}

	readers, err := newFileProcessor().processFSToReaders(context.Background(), filesystem)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, r := range readers {
			if r.closer != nil {
				_ = r.closer.Close()
			}
		}
	})

	names := make([]string, 0, len(readers))
	for _, r := range readers {
		names = append(names, r.tableName)
	}
	assert.ElementsMatch(t, []string{"top", "in"}, names, "the file one directory down belongs in the load too")
}

// TestProcessFilesystemsToReaders_NilFilesystem covers the argument check. A nil
// filesystem is a caller mistake that would otherwise surface as a panic deep in
// the walk.
func TestProcessFilesystemsToReaders_NilFilesystem(t *testing.T) {
	t.Parallel()

	_, err := newFileProcessor().processFilesystemsToReaders(context.Background(), []fs.FS{nil})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilInput)
}

// TestCollectFilesFromDirectory_LoadsEveryFileWhateverItIsNamed pins that a
// directory scan interprets nothing out of a file's name.
//
// It interpreted one thing: a fixture filter written for this repository's own
// testdata skipped any file whose base name contained "duplicate_columns", so an
// ordinary CSV named that way vanished from a directory load with no error and
// no table, while the same file named explicitly loaded fine.
func TestCollectFilesFromDirectory_LoadsEveryFileWhateverItIsNamed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	names := []string{
		"sales.csv",
		"duplicate_columns.csv",
		"report_duplicate_columns_2026.csv",
		"malformed_rows.csv",
		"invalid.tsv",
		"broken_input.ltsv",
	}
	for _, name := range names {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("id\n1\n"), 0o600))
	}

	collected, err := newFileProcessor().collectFilesFromPaths([]string{dir})
	require.NoError(t, err)

	got := make([]string, 0, len(collected))
	for _, p := range collected {
		got = append(got, filepath.Base(p))
	}
	assert.ElementsMatch(t, names, got, "a directory scan must not read meaning out of a file name")
}

// TestCollectFilesFromFS_LoadsEveryFileWhateverItIsNamed is the sibling of the
// test above on the other collector. The two walk different trees through
// different APIs and must agree on which files count, which they did not while
// only one of them carried the fixture filter.
func TestCollectFilesFromFS_LoadsEveryFileWhateverItIsNamed(t *testing.T) {
	t.Parallel()

	filesystem := fstest.MapFS{
		"sales.csv":                         {Data: []byte("id\n1\n")},
		"duplicate_columns.csv":             {Data: []byte("id\n1\n")},
		"report_duplicate_columns_2026.csv": {Data: []byte("id\n1\n")},
	}

	readers, err := newFileProcessor().processFSToReaders(t.Context(), filesystem)
	require.NoError(t, err)

	got := make([]string, 0, len(readers))
	for _, r := range readers {
		got = append(got, r.tableName)
		if r.closer != nil {
			require.NoError(t, r.closer.Close())
		}
	}
	assert.ElementsMatch(t, []string{"sales", "duplicate_columns", "report_duplicate_columns_2026"}, got)
}

// TestCollectFilesFromPaths_FollowsASymlinkToADirectory pins that a link
// standing in for a data directory loads what the directory holds.
//
// os.Stat follows a link and reported the input as a directory; filepath.WalkDir
// lstats its root and saw the link itself, so the walk visited one entry that was
// not a supported file and the load failed with "no supported files found in
// directory" for a directory that was full of them.
func TestCollectFilesFromPaths_FollowsASymlinkToADirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "real")
	require.NoError(t, os.Mkdir(target, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(target, "users.csv"), []byte("id\n1\n"), 0o600))

	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("this process cannot create a symlink here: %v", err)
	}

	collected, err := newFileProcessor().collectFilesFromPaths([]string{link})
	require.NoError(t, err)
	require.Len(t, collected, 1)
	assert.Equal(t, "users.csv", filepath.Base(collected[0]))
}

// TestCollectFilesFromPaths_FollowsASymlinkToAFile is the case that already
// worked, kept beside its directory sibling so a fix for one cannot break the
// other.
func TestCollectFilesFromPaths_FollowsASymlinkToAFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "users.csv")
	require.NoError(t, os.WriteFile(target, []byte("id\n1\n"), 0o600))

	link := filepath.Join(root, "link.csv")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("this process cannot create a symlink here: %v", err)
	}

	collected, err := newFileProcessor().collectFilesFromPaths([]string{link})
	require.NoError(t, err)
	require.Len(t, collected, 1)
	assert.Equal(t, "link.csv", filepath.Base(collected[0]), "a linked file keeps the name the caller gave")
}

// TestCollectFilesFromDirectory_DoesNotDescendIntoALinkedSubdirectory pins the
// limit deliberately left in place: only the root the caller named is resolved.
// Descending into every link found during a walk is what makes a link cycle hang
// a scan.
func TestCollectFilesFromDirectory_DoesNotDescendIntoALinkedSubdirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "real")
	require.NoError(t, os.Mkdir(target, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(target, "hidden.csv"), []byte("id\n1\n"), 0o600))

	scan := filepath.Join(root, "scan")
	require.NoError(t, os.Mkdir(scan, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(scan, "top.csv"), []byte("id\n9\n"), 0o600))
	if err := os.Symlink(target, filepath.Join(scan, "sub")); err != nil {
		t.Skipf("this process cannot create a symlink here: %v", err)
	}

	collected, err := newFileProcessor().collectFilesFromPaths([]string{scan})
	require.NoError(t, err)
	require.Len(t, collected, 1)
	assert.Equal(t, "top.csv", filepath.Base(collected[0]))
}

// TestCollectFilesFromPaths_TerminatesOnALinkToItsOwnAncestor is the failure a
// fix that resolves links too eagerly introduces: a link whose target contains
// it must end the walk rather than run forever.
func TestCollectFilesFromPaths_TerminatesOnALinkToItsOwnAncestor(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "top.csv"), []byte("id\n1\n"), 0o600))
	if err := os.Symlink(root, filepath.Join(root, "loop")); err != nil {
		t.Skipf("this process cannot create a symlink here: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		collected, err := newFileProcessor().collectFilesFromPaths([]string{filepath.Join(root, "loop")})
		assert.NoError(t, err)
		assert.Len(t, collected, 1)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a link cycle must not hold the walk open")
	}
}
