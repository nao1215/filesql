package filesql

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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

// Deduplication exists for one case: the same dataset offered both plain and
// compressed, where reading both would build one table from one place twice.
// It used to be keyed on the table name instead of the place, which made
// "a/users.csv" and "b/users.csv" look like the same input. One of them was
// dropped, nothing was said about it, and which one survived came out of a Go
// map — so the same command could lose a different file on different runs.
//
// These tests pin both halves of the repair: what counts as one source, and
// that the surviving order is the order the caller gave.

// TestSourceIdentity covers the rule the deduplication is built on, directly.
func TestSourceIdentity(t *testing.T) {
	t.Parallel()

	t.Run("a codec suffix does not change which source a path names", func(t *testing.T) {
		t.Parallel()
		for _, compressed := range []string{
			"dir/users.csv.gz", "dir/users.csv.bz2", "dir/users.csv.xz",
			"dir/users.csv.zst", "dir/users.csv.z", "dir/users.csv.snappy",
			"dir/users.csv.s2", "dir/users.csv.lz4",
		} {
			if got, want := sourceIdentity(compressed), sourceIdentity("dir/users.csv"); got != want {
				t.Errorf("sourceIdentity(%q) = %q, want %q", compressed, got, want)
			}
		}
	})

	t.Run("the same file written two ways is one source", func(t *testing.T) {
		t.Parallel()
		if got, want := sourceIdentity("./dir/users.csv"), sourceIdentity("dir/users.csv"); got != want {
			t.Errorf("sourceIdentity of an equivalent path = %q, want %q", got, want)
		}
		if got, want := sourceIdentity("dir/../dir/users.csv"), sourceIdentity("dir/users.csv"); got != want {
			t.Errorf("sourceIdentity of an equivalent path = %q, want %q", got, want)
		}
	})

	t.Run("files in different directories are different sources", func(t *testing.T) {
		t.Parallel()
		pairs := [][2]string{
			{"a/users.csv", "b/users.csv"},
			{"a/users.csv", "b/users.csv.gz"},
			{"a/book.xlsx.gz", "b/book.xlsx.gz"},
			{"users.csv", "nested/users.csv"},
		}
		for _, pair := range pairs {
			if sourceIdentity(pair[0]) == sourceIdentity(pair[1]) {
				t.Errorf("%q and %q share a source identity; they are different files", pair[0], pair[1])
			}
		}
	})

	t.Run("different files in one directory are different sources", func(t *testing.T) {
		t.Parallel()
		// These sanitize to the same table name. That is a collision for whoever
		// creates the tables to report, and no business of deduplication's.
		if sourceIdentity("dir/user-data.csv") == sourceIdentity("dir/user data.csv") {
			t.Error("two differently named files share a source identity")
		}
		if sourceIdentity("dir/products.tsv") == sourceIdentity("dir/products.parquet") {
			t.Error("two formats of different data share a source identity")
		}
	})
}

// TestDeduplicateCompressedFiles asserts the whole result slice, in order,
// rather than membership. Order is the half of the contract a set comparison
// cannot see, and it is the half everything downstream reads: which input a
// last-wins load leaves in place, which malformed file a failing load names,
// and what order the collision check works through.
func TestDeduplicateCompressedFiles(t *testing.T) {
	t.Parallel()

	fp := newFileProcessor()

	tests := []struct {
		name  string
		files []string
		want  []string
	}{
		{
			name:  "a plain file wins over its own compressed twin",
			files: []string{"dir/users.csv", "dir/users.csv.gz"},
			want:  []string{"dir/users.csv"},
		},
		{
			name: "and wins from behind, too",
			// The plain file is second here. Preferring it must not depend on it
			// being seen first.
			files: []string{"dir/users.csv.gz", "dir/users.csv"},
			want:  []string{"dir/users.csv"},
		},
		{
			name:  "the same name in two directories is two sources",
			files: []string{"a/users.csv", "b/users.csv"},
			want:  []string{"a/users.csv", "b/users.csv"},
		},
		{
			name: "a plain file does not displace a compressed file somewhere else",
			// The bug this replaces: both map to table "users", so one vanished.
			files: []string{"a/users.csv", "b/users.csv.gz"},
			want:  []string{"a/users.csv", "b/users.csv.gz"},
		},
		{
			name:  "two compressed workbooks in different directories both survive",
			files: []string{"a/book.xlsx.gz", "b/book.xlsx.gz"},
			want:  []string{"a/book.xlsx.gz", "b/book.xlsx.gz"},
		},
		{
			name: "files that only share a sanitized name both survive",
			// "user-data" and "user data" both sanitize to user_data, and
			// products.tsv and products.parquet both to products. Whether that
			// can be loaded is the table layer's decision, made out loud.
			files: []string{"dir/user-data.csv", "dir/user data.csv", "dir/products.tsv", "dir/products.parquet"},
			want:  []string{"dir/user-data.csv", "dir/user data.csv", "dir/products.tsv", "dir/products.parquet"},
		},
		{
			name: "order survives a longer list",
			files: []string{
				"z/last.csv", "a/first.csv", "m/middle.csv.gz",
				"a/first.csv.gz", "q/other.tsv",
			},
			want: []string{"z/last.csv", "a/first.csv", "m/middle.csv.gz", "q/other.tsv"},
		},
		{
			name:  "the same source named twice is read once",
			files: []string{"./dir/users.csv", "dir/users.csv"},
			want:  []string{"./dir/users.csv"},
		},
		{
			name:  "an exact repeat is read once",
			files: []string{"dir/users.csv", "dir/users.csv"},
			want:  []string{"dir/users.csv"},
		},
		{
			name:  "a lone compressed file is kept",
			files: []string{"dir/users.csv.gz"},
			want:  []string{"dir/users.csv.gz"},
		},
		{
			name:  "nothing in, nothing out",
			files: []string{},
			want:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := fp.deduplicateCompressedFiles(tt.files)
			if strings.Join(got, "|") != strings.Join(tt.want, "|") {
				t.Errorf("deduplicateCompressedFiles(%v)\n got %v\nwant %v", tt.files, got, tt.want)
			}
		})
	}
}

// TestDeduplicateCompressedFilesIsStable checks the result is a function of the
// input and nothing else. The old implementation built its result by ranging
// over a map, so the answer was free to change between calls in one process —
// which is exactly the kind of failure that reproduces once and then hides.
func TestDeduplicateCompressedFilesIsStable(t *testing.T) {
	t.Parallel()

	fp := newFileProcessor()
	files := []string{
		"a/users.csv", "b/users.csv.gz", "c/users.csv", "a/users.csv.gz",
		"d/orders.tsv", "e/orders.tsv", "f/logs.ltsv.xz",
	}
	want := strings.Join(fp.deduplicateCompressedFiles(files), "|")

	for i := range 50 {
		got := strings.Join(fp.deduplicateCompressedFiles(files), "|")
		if got != want {
			t.Fatalf("call %d returned %q, want %q; the result depends on something other than the input", i, got, want)
		}
	}
	// And the answer itself, so a stable-but-wrong result is not mistaken for
	// success.
	if expected := "a/users.csv|b/users.csv.gz|c/users.csv|d/orders.tsv|e/orders.tsv|f/logs.ltsv.xz"; want != expected {
		t.Errorf("result = %q, want %q", want, expected)
	}
}

// writeCSVFile writes a one-column CSV whose single row carries value.
func writeCSVFile(t *testing.T, path, value string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("v\n"+value+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// sameNamePair writes a/book.csv and b/book.csv, each carrying its own value.
// Both want the table "book", so what each API does with them is the question
// these tests ask.
func sameNamePair(t *testing.T) (first, second string) {
	t.Helper()
	dir := t.TempDir()
	return writeCSVFile(t, filepath.Join(dir, "a", "book.csv"), "from-a"),
		writeCSVFile(t, filepath.Join(dir, "b", "book.csv"), "from-b")
}

// TestOpenContextRefusesTwoSourcesWantingOneTable pins Open's side of the
// contract. Open builds a fresh database, so two files asking for one table
// cannot both be honored, and the answer is a refusal that names the file —
// deterministically, on every run. What it must never be is one of the two
// loading and the other disappearing.
func TestOpenContextRefusesTwoSourcesWantingOneTable(t *testing.T) {
	t.Parallel()
	first, second := sameNamePair(t)

	db, err := OpenContext(context.Background(), first, second)
	if err == nil {
		_ = db.Close()
		t.Fatal("OpenContext succeeded on two files that both want the table 'book'")
	}
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("error = %v, want ErrDuplicateTable", err)
	}
	// The second file is the one that found the name taken, so it is the one
	// named. The path is compared as the platform writes it — normalizing it
	// here would have passed on Windows whatever the message said.
	if !strings.Contains(err.Error(), second) {
		t.Errorf("error %q should name the file that collided, %q", err, second)
	}

	reversed, err := OpenContext(context.Background(), second, first)
	if err == nil {
		_ = reversed.Close()
		t.Fatal("OpenContext succeeded with the arguments reversed")
	}
	if !strings.Contains(err.Error(), first) {
		t.Errorf("with the arguments reversed the error %q should name %q", err, first)
	}
}

// TestLoadIntoKeepsItsLastWinsContract pins the other API's side, and pins that
// this change did not move it. LoadInto loads into a database the caller owns,
// where replacing a same-named table is the useful behavior and has been the
// documented one; what changed is only that both inputs are now read at all.
// Before, one of them was discarded before any of this was reached.
func TestLoadIntoKeepsItsLastWinsContract(t *testing.T) {
	t.Parallel()
	first, second := sameNamePair(t)

	for _, tt := range []struct {
		name  string
		paths []string
		want  string
	}{
		{name: "the later input wins", paths: []string{first, second}, want: "from-b"},
		{name: "and reversing the order reverses the winner", paths: []string{second, first}, want: "from-a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := newMemoryDB(t)
			ctx := context.Background()
			if err := LoadInto(ctx, db, tt.paths...); err != nil {
				t.Fatalf("LoadInto: %v", err)
			}
			var got string
			if err := db.QueryRowContext(ctx, "SELECT v FROM book").Scan(&got); err != nil {
				t.Fatalf("query book: %v", err)
			}
			if got != tt.want {
				t.Errorf("book holds %q, want %q; input order decides last-wins", got, tt.want)
			}
		})
	}
}

// TestLoadIntoTxKeepsInputOrder is the same question for the transactional
// entry point, where the caller owns the transaction as well.
func TestLoadIntoTxKeepsInputOrder(t *testing.T) {
	t.Parallel()
	first, second := sameNamePair(t)

	for _, tt := range []struct {
		name  string
		paths []string
		want  string
	}{
		{name: "the later input wins", paths: []string{first, second}, want: "from-b"},
		{name: "and reversing the order reverses the winner", paths: []string{second, first}, want: "from-a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := newMemoryDB(t)
			ctx := context.Background()

			builder, err := buildForTest(ctx, NewBuilder().AddPaths(tt.paths...))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			if err := builder.LoadIntoTx(ctx, tx); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Errorf("rollback after a failed load: %v", rollbackErr)
				}
				t.Fatalf("LoadIntoTx: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			var got string
			if err := db.QueryRowContext(ctx, "SELECT v FROM book").Scan(&got); err != nil {
				t.Fatalf("query book: %v", err)
			}
			if got != tt.want {
				t.Errorf("book holds %q, want %q; input order decides last-wins", got, tt.want)
			}
		})
	}
}

// TestLoadReadsEveryDistinctSource is the plainest statement of what the repair
// bought: files that merely share a base name all arrive.
func TestLoadReadsEveryDistinctSource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	paths := []string{
		writeCSVFile(t, filepath.Join(dir, "a", "users.csv"), "a"),
		writeCSVFile(t, filepath.Join(dir, "b", "orders.csv"), "b"),
		writeCSVFile(t, filepath.Join(dir, "c", "logs.csv"), "c"),
	}

	db := newMemoryDB(t)
	ctx := context.Background()
	if err := LoadInto(ctx, db, paths...); err != nil {
		t.Fatalf("LoadInto: %v", err)
	}
	for table, want := range map[string]string{"users": "a", "orders": "b", "logs": "c"} {
		var got string
		if err := db.QueryRowContext(ctx, "SELECT v FROM "+table).Scan(&got); err != nil {
			t.Fatalf("query %s: %v", table, err)
		}
		if got != want {
			t.Errorf("%s holds %q, want %q", table, got, want)
		}
	}
}

// TestOpenDirectoryWithCollidingBasenamesFails is the test the repointed
// fixtures above refer to. A directory tree holding two files of the same base
// name in different subdirectories cannot become one database, and saying so is
// the whole point: the previous behavior was to load one of them and leave the
// other out, without a word and without deciding which.
func TestOpenDirectoryWithCollidingBasenamesFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCSVFile(t, filepath.Join(dir, "a", "book.csv"), "from-a")
	writeCSVFile(t, filepath.Join(dir, "b", "book.csv"), "from-b")

	db, err := Open(dir)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open succeeded on a tree with two files that both want the table 'book'")
	}
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("error = %v, want ErrDuplicateTable", err)
	}
}

// TestOpenDirectoryStillPrefersThePlainSibling keeps the case deduplication is
// actually for: one dataset, offered twice in one place.
func TestOpenDirectoryStillPrefersThePlainSibling(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCSVFile(t, filepath.Join(dir, "users.csv"), "plain")
	gzipFile(t, filepath.Join(dir, "users.csv"), filepath.Join(dir, "users.csv.gz"))

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var rows int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM users").Scan(&rows); err != nil {
		t.Fatalf("query users: %v", err)
	}
	if rows != 1 {
		t.Errorf("users holds %d rows, want 1; the compressed twin was loaded as well", rows)
	}
}

// newMemoryDB opens a caller-owned in-memory database, pinned to one connection
// so every query reaches the same one.
func newMemoryDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestOpenRefusesTwoSourcesWhoseNamesDifferOnlyInCase is the same question with
// the names spelled differently. SQLite folds ASCII case when it compares
// identifiers, so "Users" and "users" are one table there whatever the file
// names were; the check that finds the clash has to fold the same way, or the
// second file's rows land in the first file's table by position and the columns
// they were written under are gone.
func TestOpenRefusesTwoSourcesWhoseNamesDifferOnlyInCase(t *testing.T) {
	t.Parallel()
	first, second := caseDifferingPair(t)

	db, err := Open(first, second)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open succeeded on two files that both want one table, spelled in two cases")
	}
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("error = %v, want ErrDuplicateTable", err)
	}
}

// caseDifferingPair writes a/Users.csv and b/users.csv, each with its own
// columns. They go in separate directories because macOS and Windows fold case
// in file names: side by side, the second write would replace the first and
// there would be nothing to collide.
func caseDifferingPair(t *testing.T) (first, second string) {
	t.Helper()
	dir := t.TempDir()
	first = filepath.Join(dir, "a", "Users.csv")
	second = filepath.Join(dir, "b", "users.csv")
	for path, body := range map[string]string{first: "a,b\n1,2\n", second: "c,d\n3,4\n"} {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return first, second
}

// TestBuilderRefusesTwoReadersWhoseNamesDifferOnlyInCase asks it of the API
// where the caller picks the names outright, which no filesystem constrains.
func TestBuilderRefusesTwoReadersWhoseNamesDifferOnlyInCase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	validated, err := buildForTest(

		ctx, NewBuilder().
			AddReader(strings.NewReader("a,b\n1,2\n"), "Users", FileTypeCSV).
			AddReader(strings.NewReader("c,d\n3,4\n"), "users", FileTypeCSV))

	if err != nil {
		if !errors.Is(err, ErrDuplicateTable) {
			t.Errorf("Build error = %v, want ErrDuplicateTable", err)
		}
		return
	}

	db, err := validated.Open(ctx)
	if err == nil {
		_ = db.Close()
		t.Fatal("two readers named Users and users both loaded")
	}
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("error = %v, want ErrDuplicateTable", err)
	}
}

// TestReplacingKeepsTheLaterSpellingsColumns pins what the clash looks like when
// replacing is asked for: one table, holding the second file's columns and rows
// rather than the second file's rows under the first file's headers.
func TestReplacingKeepsTheLaterSpellingsColumns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	first, second := caseDifferingPair(t)

	db := newMemoryDB(t)
	if err := LoadInto(ctx, db, first, second); err != nil {
		t.Fatalf("LoadInto: %v", err)
	}

	var tables int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE '\_filesql\_%' ESCAPE '\'`,
	).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 1 {
		t.Errorf("database holds %d tables, want 1", tables)
	}

	var columns string
	if err := db.QueryRowContext(ctx,
		`SELECT group_concat(name) FROM pragma_table_info('users')`,
	).Scan(&columns); err != nil {
		t.Fatalf("read columns: %v", err)
	}
	if columns != "c,d" {
		t.Errorf("the surviving table has columns %q, want %q; the later file's rows are stored under the earlier file's headers", columns, "c,d")
	}
}

// makeFIFO creates a named pipe at path, or skips the test where the platform
// has none. syscall.Mkfifo is not on every platform this package builds for, so
// the pipe is made with the tool that is, and a platform without that one skips.
func makeFIFO(t *testing.T, path string) {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("windows has no named pipes in the filesystem")
	}
	if err := exec.CommandContext(t.Context(), "mkfifo", path).Run(); err != nil { //nolint:gosec // the command is fixed and the path is under t.TempDir()
		t.Skipf("this platform cannot make a named pipe here: %v", err)
	}
}

// TestCollectFilesFromPaths_RefusesASourceThatIsNotAFile pins that a path
// carrying a supported extension is refused unless it is a regular file.
//
// It was not checked, and opening a named pipe for reading blocks until a
// writer opens the other end: a directory holding one entry called "pipe.csv"
// made Open block inside the os.Open syscall, where the context cannot reach it,
// so a caller with a deadline waited for the life of the process rather than
// getting an error. One such entry anywhere under a scanned directory was
// enough. A character device did not block, but only because it reports a size
// of zero and the emptiness check ran first.
func TestCollectFilesFromPaths_RefusesASourceThatIsNotAFile(t *testing.T) {
	t.Parallel()

	t.Run("a pipe found in a directory", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "real.csv"), []byte("id\n1\n"), 0o600))
		makeFIFO(t, filepath.Join(root, "pipe.csv"))

		_, err := newFileProcessor().collectFilesFromPaths([]string{root})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), "pipe.csv", "the refusal has to name the entry that caused it")
	})

	t.Run("a pipe named by the caller", func(t *testing.T) {
		t.Parallel()

		pipe := filepath.Join(t.TempDir(), "pipe.csv")
		makeFIFO(t, pipe)

		_, err := newFileProcessor().collectFilesFromPaths([]string{pipe})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), "pipe.csv")
	})

	t.Run("a load of a directory holding a pipe returns", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "real.csv"), []byte("id\n1\n"), 0o600))
		makeFIFO(t, filepath.Join(root, "pipe.csv"))

		// Open on a goroutine: a block here has no deadline of its own, so a
		// test that waited for it inline would take the package timeout rather
		// than fail.
		done := make(chan error, 1)
		go func() {
			db, err := OpenContext(t.Context(), root)
			if db != nil {
				_ = db.Close()
			}
			done <- err
		}()
		select {
		case err := <-done:
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
		case <-time.After(30 * time.Second):
			t.Fatal("Open did not return: it is waiting for a writer on the pipe, which no context ends")
		}
	})
}

// TestRefuseIrregularSource covers the reading on its own, for the kinds a
// platform may not let a test create.
func TestRefuseIrregularSource(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		mode    os.FileMode
		refused bool
		says    string
	}{
		{name: "a regular file", mode: 0o644},
		{name: "a named pipe", mode: os.ModeNamedPipe | 0o644, refused: true, says: "named pipe"},
		{name: "a character device", mode: os.ModeDevice | os.ModeCharDevice | 0o644, refused: true, says: "device"},
		{name: "a block device", mode: os.ModeDevice | 0o644, refused: true, says: "device"},
		{name: "a socket", mode: os.ModeSocket | 0o644, refused: true, says: "socket"},
		{name: "a kind the system does not name", mode: os.ModeIrregular | 0o644, refused: true, says: "not a regular file"},
		{name: "a directory", mode: os.ModeDir | 0o755, refused: true, says: "directory"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := refuseIrregularSource("users.csv", tt.mode)
			if !tt.refused {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Contains(t, err.Error(), "users.csv")
			assert.Contains(t, err.Error(), tt.says)
		})
	}
}
