package filesql

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nao1215/filesql/internal/atomicwrite"
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
		assert.Equal(t, atomicwrite.DefaultFileMode, info.Mode().Perm())
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

// TestWriteFileAtomically_LongDestinationName pins that a destination the
// filesystem accepts can be written.
//
// The staged file's name was the destination's with a dot in front and
// ".tmp<random>" behind, up to fifteen bytes longer, so a legal file name close
// to the 255-byte component limit could be loaded and queried but never saved:
// the save failed with "file name too long" naming a path the caller had not
// chosen.
func TestWriteFileAtomically_LongDestinationName(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		base string
	}{
		{name: "ascii", base: strings.Repeat("a", 246) + ".csv"},
		// Multi-byte runes, so a fix that cuts by bytes cannot split one.
		{name: "multibyte", base: strings.Repeat("あ", 82) + ".csv"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			dest := filepath.Join(dir, tc.base)
			if err := os.WriteFile(dest, []byte("placeholder"), 0o600); err != nil {
				t.Skipf("this filesystem does not accept a %d-byte name: %v", len(tc.base), err)
			}

			err := writeFileAtomically(dest, func(w io.Writer) error {
				_, writeErr := io.WriteString(w, "saved")
				return writeErr
			})
			require.NoError(t, err, "a destination the filesystem holds must be writable")

			got, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, "saved", string(got))

			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			assert.Len(t, entries, 1, "no staged file may be left behind: %v", entries)
		})
	}
}

// TestDumpDatabase_LongOutputFileName is the caller-visible form of the same
// bug: the output file name is the table's, so a long table name is what makes
// the destination long.
func TestDumpDatabase_LongOutputFileName(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	tableName := strings.Repeat("t", 246)
	validated, err := buildForTest(

		ctx, NewBuilder().
			AddReader(strings.NewReader("id,name\n1,alice\n"), tableName, FileTypeCSV))

	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, tableName+".csv"), nil, 0o600); err != nil {
		t.Skipf("this filesystem does not accept a %d-byte name: %v", len(tableName)+4, err)
	}
	require.NoError(t, DumpDatabase(db, out, NewDumpOptions()))

	reloaded, err := OpenContext(ctx, filepath.Join(out, tableName+".csv"))
	require.NoError(t, err)
	defer reloaded.Close()

	var name string
	require.NoError(t, reloaded.QueryRowContext(ctx, `SELECT name FROM "`+tableName+`"`).Scan(&name))
	assert.Equal(t, "alice", name)
}

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

// TestWriteTogether pins that a set of files is replaced together: a produce
// that fails replaces none of them, and a nil set is one file written straight
// through.
func TestWriteTogether(t *testing.T) {
	t.Parallel()

	newFile := func(w io.Writer) error {
		_, err := io.WriteString(w, "new")
		return err
	}

	t.Run("every file is put in place", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		first := filepath.Join(dir, "first.txt")
		second := filepath.Join(dir, "second.txt")

		require.NoError(t, writeTogether(func(set *writeSet) error {
			if err := set.write(first, newFile); err != nil {
				return err
			}
			return set.write(second, newFile)
		}))

		for _, path := range []string{first, second} {
			body, err := os.ReadFile(path) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, "new", string(body))
		}
		assert.Equal(t, []string{"first.txt", "second.txt"}, dirEntries(t, dir), "no staged file may be left behind")
	})

	t.Run("a failure after a staged file replaces nothing", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		first := filepath.Join(dir, "first.txt")
		require.NoError(t, os.WriteFile(first, []byte("old"), 0o600))

		refused := errors.New("the second file could not be produced")
		err := writeTogether(func(set *writeSet) error {
			if writeErr := set.write(first, newFile); writeErr != nil {
				return writeErr
			}
			return refused
		})
		require.ErrorIs(t, err, refused)

		body, readErr := os.ReadFile(first) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "old", string(body), "a file staged before the failure is not put in place")
		assert.Equal(t, []string{"first.txt"}, dirEntries(t, dir), "no staged file may be left behind")
	})

	// The commit is the one step this cannot make all-or-nothing: each file is
	// renamed over its destination, and an operating system that refuses one of
	// those after an earlier one has gone through leaves the files before it
	// replaced. What is pinned here is that this is what happens and that the
	// caller is told which destination it was.
	t.Run("a commit that fails discards what is left and says so", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		first := filepath.Join(dir, "first.txt")
		second := filepath.Join(dir, "second.txt")

		set := &writeSet{}
		require.NoError(t, set.write(first, newFile))
		require.NoError(t, set.write(second, newFile))

		// A directory where the first file goes is a destination the commit
		// cannot replace, which is the operating system refusing a file already
		// written rather than an encoder refusing a value.
		require.NoError(t, os.Mkdir(first, 0o750))

		err := set.commit()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrIOOperation)
		assert.Contains(t, err.Error(), first, "the error names the destination it would not replace")
		assert.DirExists(t, first, "the destination the commit could not replace is left as it was")
		assert.NoFileExists(t, second, "a file still staged when the commit fails is discarded")
		assert.Equal(t, []string{"first.txt"}, dirEntries(t, dir), "no staged file may be left behind")
	})

	t.Run("a commit that fails after one has gone through leaves that one replaced", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		first := filepath.Join(dir, "first.txt")
		second := filepath.Join(dir, "second.txt")
		require.NoError(t, os.WriteFile(first, []byte("old"), 0o600))

		set := &writeSet{}
		require.NoError(t, set.write(first, newFile))
		require.NoError(t, set.write(second, newFile))
		require.NoError(t, os.Mkdir(second, 0o750))

		err := set.commit()
		require.Error(t, err)
		assert.Contains(t, err.Error(), second, "the error names the destination it would not replace")

		body, readErr := os.ReadFile(first) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, readErr)
		assert.Equal(t, "new", string(body), "a file the commit had already renamed stays renamed")
		assert.Equal(t, []string{"first.txt", "second.txt"}, dirEntries(t, dir), "no staged file may be left behind")
	})

	t.Run("a nil set writes one file straight through", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dest := filepath.Join(dir, "out.txt")

		var set *writeSet
		require.NoError(t, set.write(dest, newFile))

		body, err := os.ReadFile(dest) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, "new", string(body))
	})
}
