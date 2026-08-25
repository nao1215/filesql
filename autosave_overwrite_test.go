package filesql

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
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

	t.Run("a sheet keeps its name across a round trip", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book_Orders SET name = 'bob'"))

		assert.Equal(t, []string{"Orders"}, workbookSheets(t, src),
			"overwriting a workbook in place must not rename its sheet")

		// The name has to survive repeatedly, not just once: a prefix added on
		// every save accumulates until Excel's 31-rune sheet name limit truncates it.
		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book_Orders SET name = 'carol'"))
		assert.Equal(t, []string{"Orders"}, workbookSheets(t, src))

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		assert.Equal(t, "carol", name)
	})

	t.Run("a workbook of several sheets is written back to itself", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders":    {{"id", "name"}, {"1", "alice"}},
			"Customers": {{"id", "city"}, {"1", "tokyo"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{src},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_Customers SET city = 'osaka'"))

		assert.Equal(t, []string{"Customers", "Orders"}, workbookSheets(t, src),
			"every sheet has to come back, under its own name")
		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, dir), "nothing else may be written")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name, city string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT city FROM book_Customers").Scan(&city))
		assert.Equal(t, "bob", name)
		assert.Equal(t, "osaka", city)
	})

	t.Run("a workbook keeps the sheets of a sibling whose name it prefixes out", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		book := filepath.Join(dir, "book.xlsx")
		sibling := filepath.Join(dir, "book_v2.xlsx")
		writeWorkbook(t, book, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})
		writeWorkbook(t, sibling, map[string][][]string{
			"Orders": {{"id", "name"}, {"2", "dave"}},
		})

		require.NoError(t, autoSaveOverwrite(t, []string{book, sibling},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_v2_Orders SET name = 'erin'"))

		assert.Equal(t, []string{"Orders"}, workbookSheets(t, book),
			"book.xlsx holds its own sheet only: book_v2.xlsx's tables are named inside book's prefix space, but they are not book's")
		assert.Equal(t, []string{"Orders"}, workbookSheets(t, sibling))

		reloaded, err := OpenContext(ctx, book, sibling)
		require.NoError(t, err)
		defer reloaded.Close()

		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		assert.Equal(t, "bob", name)
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_v2_Orders").Scan(&name))
		assert.Equal(t, "erin", name)
	})

	t.Run("a workbook keeps out a sibling of another format whose name it prefixes", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		book := filepath.Join(dir, "book.xlsx")
		sibling := filepath.Join(dir, "book_v2.csv")
		writeWorkbook(t, book, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})
		require.NoError(t, os.WriteFile(sibling, []byte("id,name\n2,dave\n"), 0o600))

		require.NoError(t, autoSaveOverwrite(t, []string{book, sibling},
			"UPDATE book_Orders SET name = 'bob'"))

		assert.Equal(t, []string{"Orders"}, workbookSheets(t, book),
			"a CSV sibling loads as one table named inside the workbook's prefix space, and it is not the workbook's either")
	})

	t.Run("a compressed workbook of several sheets round-trips", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		plain := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, plain, map[string][][]string{
			"Orders":    {{"id", "name"}, {"1", "alice"}},
			"Customers": {{"id", "city"}, {"1", "tokyo"}},
		})

		// A compressed source has to be written back through its own codec, and
		// the workbook still has to arrive whole on the other side of it.
		raw, err := os.ReadFile(plain) //nolint:gosec // plain is under t.TempDir()
		require.NoError(t, err)
		require.NoError(t, os.Remove(plain))

		src := filepath.Join(dir, "book.xlsx.gz")
		out, err := os.Create(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, err)
		gz := gzip.NewWriter(out)
		_, err = gz.Write(raw)
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, out.Close())

		require.NoError(t, autoSaveOverwrite(t, []string{src},
			"UPDATE book_Orders SET name = 'bob'",
			"UPDATE book_Customers SET city = 'osaka'"))

		assert.Equal(t, []string{"book.xlsx.gz"}, dirEntries(t, dir), "nothing else may be written")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name, city string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Orders").Scan(&name))
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT city FROM book_Customers").Scan(&city))
		assert.Equal(t, "bob", name)
		assert.Equal(t, "osaka", city)
	})

	t.Run("two tables that would share a sheet name are refused", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		writeWorkbook(t, src, map[string][][]string{
			"Orders": {{"id", "name"}, {"1", "alice"}},
		})
		before, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, err)

		validated, err := NewBuilder().AddPath(src).EnableAutoSave("").Build(ctx)
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)

		// Excel caps a sheet name at 31 runes, so two tables of this workbook
		// whose names agree for the first 31 and differ after map to one sheet.
		// excelize's NewSheet returns the existing index rather than erroring, so
		// the second table used to overwrite the first's sheet and one table's
		// rows vanished while the save reported success.
		stem := strings.Repeat("a", excelSheetNameMaxLen)
		for _, suffix := range []string{stem + "X", stem + "Y"} {
			_, execErr := db.ExecContext(ctx, "CREATE TABLE `book_"+suffix+"` (id TEXT)")
			require.NoError(t, execErr)
		}

		err = db.Close()
		require.Error(t, err, "a save that cannot keep both tables must not report success")
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		// Both table names, not just the sheet: the error's job is to say which
		// two tables collided, and asserting only the sheet would pass an error
		// that named neither.
		assert.Contains(t, err.Error(), "book_"+stem+"X")
		assert.Contains(t, err.Error(), "book_"+stem+"Y")
		assert.Contains(t, err.Error(), stem, "the error names the sheet the two tables collide on")

		after, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
		require.NoError(t, err)
		assert.Equal(t, before, after, "the workbook must be left as it was")
	})

	t.Run("a workbook read from a fixture round-trips whole", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()
		src := filepath.Join(dir, "book.xlsx")
		data, err := os.ReadFile(filepath.Join("testdata", "excel", "sample.xlsx"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(src, data, 0o600)) //nolint:gosec // src is under t.TempDir()

		before := workbookSheets(t, src)
		require.NoError(t, autoSaveOverwrite(t, []string{src}, "UPDATE book_Sheet1 SET name = 'bob'"))

		assert.Equal(t, before, workbookSheets(t, src), "the sheets have to come back as they were")
		assert.Equal(t, []string{"book.xlsx"}, dirEntries(t, dir), "nothing else may be written")

		reloaded, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer reloaded.Close()
		var name string
		require.NoError(t, reloaded.QueryRowContext(ctx, "SELECT name FROM book_Sheet1").Scan(&name))
		assert.Equal(t, "bob", name)
	})
}

// writeWorkbook builds an xlsx at path holding the given sheets. Each sheet's
// first row is its header.
func writeWorkbook(t *testing.T, path string, sheets map[string][][]string) {
	t.Helper()

	f := excelize.NewFile()
	defer func() {
		_ = f.Close()
	}()

	names := make([]string, 0, len(sheets))
	for name := range sheets {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if _, err := f.NewSheet(name); err != nil {
			t.Fatal(err)
		}
		for r, row := range sheets[name] {
			for c, value := range row {
				cell, err := excelize.CoordinatesToCellName(c+1, r+1)
				require.NoError(t, err)
				require.NoError(t, f.SetCellValue(name, cell, value))
			}
		}
	}
	require.NoError(t, f.DeleteSheet(defaultSheetName))
	require.NoError(t, f.SaveAs(path))
}

// workbookSheets returns the sheet names of the workbook at path, sorted.
func workbookSheets(t *testing.T, path string) []string {
	t.Helper()

	f, err := excelize.OpenFile(path)
	require.NoError(t, err)
	defer func() {
		_ = f.Close()
	}()

	names := f.GetSheetList()
	sort.Strings(names)
	return names
}

// TestAutoSaveOverwriteKeepsTheFileItWasGiven pins overwrite mode's core
// promise from the file's side: the bytes go back into the path that was
// opened, under the name it already had, or the save fails and the file is
// left alone. Nothing covered either half for a name that is not already a
// valid SQL identifier, and the table name is derived from the file name by a
// mapping that is not reversible.
func TestAutoSaveOverwriteKeepsTheFileItWasGiven(t *testing.T) {
	t.Parallel()

	// Each name loads as a table spelled differently from the file: "my-data"
	// becomes my_data, "sales report" becomes sales_report, and a name starting
	// with a digit gains a prefix. The file must keep its own spelling.
	names := []string{
		"my-data.csv",
		"sales report.csv",
		"2024.q1.csv",
		"café.csv",
	}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			dir := t.TempDir()
			src := filepath.Join(dir, name)
			require.NoError(t, os.WriteFile(src, []byte("id,v\n1,a\n"), 0o600))

			validated, err := NewBuilder().AddPath(src).EnableAutoSave("").Build(ctx)
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)

			tables, err := getSQLiteTableNames(db)
			require.NoError(t, err)
			require.Len(t, tables, 1)

			//nolint:gosec // the table name comes from the file this test just wrote
			_, err = db.ExecContext(ctx, "UPDATE `"+tables[0]+"` SET v = 'b'")
			require.NoError(t, err)
			require.NoError(t, db.Close())

			assert.Equal(t, []string{name}, dirEntries(t, dir),
				"the save goes back to the file that was opened, under its own name")

			content, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
			require.NoError(t, err)
			assert.Equal(t, "id,v\n1,b\n", string(content))
		})
	}
}

// TestAutoSaveOverwriteRefusesCodecItCannotWrite pins the other half: bzip2 is
// read but has no writer in this library, so a .bz2 source cannot be written
// back. The save has to say so and leave the file untouched rather than report
// success over a file it never wrote.
func TestAutoSaveOverwriteRefusesCodecItCannotWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "products.tsv.bz2")
	fixture, err := os.ReadFile(filepath.Join("testdata", "products.tsv.bz2"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(src, fixture, 0o600)) //nolint:gosec // src is under t.TempDir()

	err = autoSaveOverwrite(t, []string{src}, "UPDATE products SET price = 1")
	require.Error(t, err, "a codec this package cannot write must not report a successful save")
	assert.Contains(t, err.Error(), "bzip2")
	// Every sentinel the failure passed through is reachable, so a caller can
	// tell "this codec cannot be written" from "the compressor failed" without
	// matching on the message. ErrUnsupportedFormat used to be text only,
	// because the writer flattened the inner error with %s; see #216.
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.ErrorIs(t, err, ErrCompression)
	assert.ErrorIs(t, err, ErrIOOperation)

	after, err := os.ReadFile(src) //nolint:gosec // src is under t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, fixture, after, "the source must be left byte for byte as it was")
	assert.Equal(t, []string{"products.tsv.bz2"}, dirEntries(t, dir), "nothing else may be written")
}

// TestAutoSaveOverwriteLongSourceName pins the auto-save form of the staged-name
// bug. Overwrite mode is where the failure costs the caller their edit: the save
// runs from Close, after the change is in the database and with nowhere else for
// it to go.
func TestAutoSaveOverwriteLongSourceName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := strings.Repeat("s", 246) + ".csv"
	src := filepath.Join(dir, base)
	if err := os.WriteFile(src, []byte("id,name\n1,alice\n"), 0o600); err != nil {
		t.Skipf("this filesystem does not accept a %d-byte name: %v", len(base), err)
	}

	table := sanitizeTableName(tableFromFilePath(src))
	require.NoError(t, autoSaveOverwrite(t, []string{src}, `UPDATE "`+table+`" SET name = 'bob'`))

	got, err := os.ReadFile(src) //nolint:gosec // Test path from t.TempDir()
	require.NoError(t, err)
	assert.Equal(t, "id,name\n1,bob\n", string(got))
	assert.Equal(t, []string{base}, dirEntries(t, dir), "no staged file may be left beside the source")
}
