package filesql

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// autoSaveOverwrite opens path with auto-save in overwrite mode, runs stmts, and
// closes, returning the close error. Closing is what performs the save.
func autoSaveOverwrite(t *testing.T, paths []string, stmts ...string) error {
	t.Helper()

	ctx := t.Context()
	builder := NewBuilder()
	for _, p := range paths {
		builder = builder.AddPath(p)
	}
	validated, err := builder.EnableAutoSave("").Build(ctx)
	require.NoError(t, err)

	db, err := validated.Open(ctx)
	require.NoError(t, err)

	for _, stmt := range stmts {
		_, execErr := db.ExecContext(ctx, stmt)
		require.NoError(t, execErr)
	}
	return db.Close()
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// TestAutoSaveOverwriteKeepsSourceFormat pins that overwrite mode writes each
// table back to the file it came from, in that file's own format.
//
// It did not: overwrite mode handed the whole database to DumpDatabase with the
// output format from the auto-save options, which defaults to CSV. A .tsv source
// therefore got a new .csv beside it holding the change, while the .tsv the
// caller had asked to overwrite still held the old rows — the save went to a file
// nobody named, and the file that was named went stale.
func TestAutoSaveOverwriteKeepsSourceFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
		want    string
	}{
		{name: "csv", file: "data.csv", content: "id,name\n1,alice\n", want: "id,name\n1,bob\n"},
		{name: "tsv", file: "data.tsv", content: "id\tname\n1\talice\n", want: "id\tname\n1\tbob\n"},
		{name: "ltsv", file: "data.ltsv", content: "id:1\tname:alice\n", want: "id:1\tname:bob\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			src := filepath.Join(dir, tt.file)
			require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

			require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE data SET name = 'bob'"))

			assert.Equal(t, []string{tt.file}, dirEntries(t, dir), "overwrite mode writes no file the caller did not open")

			got, err := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestAutoSaveOverwriteKeepsCompression pins that a compressed source is written
// back compressed, and in place. A .csv.gz source used to get a plain .csv beside
// it while the archive kept the old rows.
func TestAutoSaveOverwriteKeepsCompression(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	dir := t.TempDir()

	// Build the fixture through the dump path so it is a real archive.
	plain := filepath.Join(dir, "seed.csv")
	require.NoError(t, os.WriteFile(plain, []byte("id,name\n1,alice\n"), 0o600))
	seedDB, err := OpenContext(ctx, plain)
	require.NoError(t, err)
	gzDir := filepath.Join(dir, "gz")
	require.NoError(t, DumpDatabase(seedDB, gzDir, NewDumpOptions().WithCompression(CompressionGZ)))
	require.NoError(t, seedDB.Close())

	src := filepath.Join(gzDir, "seed.csv.gz")
	require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE seed SET name = 'bob'"))

	assert.Equal(t, []string{"seed.csv.gz"}, dirEntries(t, gzDir), "the archive is replaced, not sidestepped")

	// Reading it back is what proves it is still a gzip archive holding the change.
	reloaded, err := OpenContext(ctx, src)
	require.NoError(t, err)
	defer reloaded.Close()

	var name string
	require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM seed").Scan(&name))
	assert.Equal(t, "bob", name)
}

// TestAutoSaveOverwriteAcrossDirectories pins that each source is written back to
// its own directory. The output directory was taken from the first source path, so
// every table landed next to whichever file happened to be loaded first.
func TestAutoSaveOverwriteAcrossDirectories(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirA := filepath.Join(root, "a")
	dirB := filepath.Join(root, "b")
	require.NoError(t, os.MkdirAll(dirA, 0o750))
	require.NoError(t, os.MkdirAll(dirB, 0o750))

	srcA := filepath.Join(dirA, "x.csv")
	srcB := filepath.Join(dirB, "y.csv")
	require.NoError(t, os.WriteFile(srcA, []byte("id,name\n1,alice\n"), 0o600))
	require.NoError(t, os.WriteFile(srcB, []byte("id,name\n2,carol\n"), 0o600))

	require.NoError(t, autoSaveOverwrite(t, []string{srcA, srcB},
		"UPDATE x SET name = 'bob'", "UPDATE y SET name = 'dave'"))

	assert.Equal(t, []string{"x.csv"}, dirEntries(t, dirA))
	assert.Equal(t, []string{"y.csv"}, dirEntries(t, dirB))

	gotA, err := os.ReadFile(srcA) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(gotA))

	gotB, err := os.ReadFile(srcB) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n2,dave\n", string(gotB))
}

// TestAutoSaveOverwriteLeavesNewTablesAlone pins that a table the caller created
// is not written anywhere. Overwrite mode is defined by the files that were
// opened, and a new table is not one of them; it used to appear as a new file in
// the source directory.
func TestAutoSaveOverwriteLeavesNewTablesAlone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "data.csv")
	require.NoError(t, os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600))

	require.NoError(t, autoSaveOverwrite(t, []string{src},
		"CREATE TABLE scratch (a TEXT)",
		"INSERT INTO scratch VALUES ('temporary')",
		"UPDATE data SET name = 'bob'"))

	assert.Equal(t, []string{"data.csv"}, dirEntries(t, dir))

	got, err := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(got))
}

// TestAutoSaveOverwriteRefusesFormatItCannotWrite pins that a source in a format
// with no writer fails the save instead of quietly becoming a CSV beside it.
func TestAutoSaveOverwriteRefusesFormatItCannotWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		content string
	}{
		{name: "json", file: "records.json", content: `[{"id":1}]`},
		{name: "jsonl", file: "records.jsonl", content: "{\"id\":1}\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			src := filepath.Join(dir, tt.file)
			require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

			// A JSON source becomes one table with a single "data" column holding the
			// raw value, which json_extract reads into.
			err := autoSaveOverwrite(t, []string{src}, `UPDATE records SET data = '{"id":2}'`)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Contains(t, err.Error(), tt.file)

			assert.Equal(t, []string{tt.file}, dirEntries(t, dir), "nothing else may be written")

			got, readErr := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
			require.NoError(t, readErr)
			assert.Equal(t, tt.content, string(got), "the source is left as it was")
		})
	}
}

// TestAutoSaveOverwriteXLSX pins the two shapes an Excel source can have. A
// workbook of one sheet is written back to itself. A workbook of several sheets
// became one CSV per sheet next to it, which is not the file the caller opened;
// it now fails and says so, because the XLSX writer holds one sheet per file.
func TestAutoSaveOverwriteXLSX(t *testing.T) {
	t.Parallel()

	t.Run("a workbook of one sheet is written back to itself", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()

		// Build a single-sheet workbook through the dump path.
		plain := filepath.Join(dir, "book.csv")
		require.NoError(t, os.WriteFile(plain, []byte("id,name\n1,alice\n"), 0o600))
		seedDB, err := OpenContext(ctx, plain)
		require.NoError(t, err)
		bookDir := filepath.Join(dir, "book")
		require.NoError(t, DumpDatabase(seedDB, bookDir, NewDumpOptions().WithFormat(OutputFormatXLSX)))
		require.NoError(t, seedDB.Close())

		src := filepath.Join(bookDir, "book.xlsx")
		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book SET name = 'bob'"))

		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, bookDir))

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()

		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book").Scan(&name))
		assert.Equal(t, "bob", name)
	})

	t.Run("a workbook of several sheets is refused", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		data, err := os.ReadFile(filepath.Join("testdata", "excel", "sample.xlsx"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(src, data, 0o600)) //nolint:gosec // src is under t.TempDir()

		err = autoSaveOverwrite(t, []string{src}, "UPDATE book_Sheet1 SET name = 'bob'")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), "book.xlsx")

		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, dir), "nothing else may be written")
	})
}
