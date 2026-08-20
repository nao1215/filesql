package filesql

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"testing/fstest"

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
