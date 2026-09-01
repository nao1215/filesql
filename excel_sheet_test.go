package filesql

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// A workbook can hide a sheet two ways: "hidden", which a reader can undo from
// the sheet tabs, and "very hidden", which only the VBA editor can. excelize
// reports one boolean covering both, so these tests build both and assert the
// same outcome for each — a policy that told them apart would be claiming
// knowledge the library does not supply.

// sheetVisibility is how a fixture workbook stores one sheet.
type sheetVisibility int

const (
	sheetVisible sheetVisibility = iota
	sheetHidden
	sheetVeryHidden
)

// sheetSpec is one sheet of a fixture workbook.
type sheetSpec struct {
	name       string
	visibility sheetVisibility
}

// visibilityWorkbook writes a workbook holding exactly these sheets, in this
// order, each with one header cell and one data row carrying the sheet's name.
//
// The fixture is generated rather than committed so the visibility every test
// depends on is stated in the test itself. A workbook checked in as bytes could
// be claimed to hold a very hidden sheet and hold nothing of the sort, and
// nothing in the suite would notice.
func visibilityWorkbook(t *testing.T, path string, specs ...sheetSpec) string {
	t.Helper()
	if len(specs) == 0 {
		t.Fatal("a fixture workbook needs at least one sheet")
	}

	f := excelize.NewFile()
	for _, spec := range specs {
		if spec.name != defaultSheetName {
			if _, err := f.NewSheet(spec.name); err != nil {
				t.Fatalf("new sheet %q: %v", spec.name, err)
			}
		}
		if err := f.SetCellValue(spec.name, "A1", "v"); err != nil {
			t.Fatalf("write header of %q: %v", spec.name, err)
		}
		if err := f.SetCellValue(spec.name, "A2", spec.name); err != nil {
			t.Fatalf("write row of %q: %v", spec.name, err)
		}
	}
	if specs[0].name != defaultSheetName {
		if err := f.DeleteSheet(defaultSheetName); err != nil {
			t.Fatalf("delete the default sheet: %v", err)
		}
	}

	// Excel refuses to hide every sheet, and so does excelize, so the active
	// sheet is moved onto one that stays shown before anything is hidden.
	active := -1
	for _, spec := range specs {
		if spec.visibility != sheetVisible {
			continue
		}
		index, err := f.GetSheetIndex(spec.name)
		if err != nil {
			t.Fatalf("index of %q: %v", spec.name, err)
		}
		active = index
		break
	}
	if active < 0 {
		t.Fatal("a fixture workbook needs at least one visible sheet")
	}
	f.SetActiveSheet(active)

	for _, spec := range specs {
		switch spec.visibility {
		case sheetVisible:
		case sheetHidden:
			if err := f.SetSheetVisible(spec.name, false); err != nil {
				t.Fatalf("hide %q: %v", spec.name, err)
			}
		case sheetVeryHidden:
			if err := f.SetSheetVisible(spec.name, false, true); err != nil {
				t.Fatalf("very-hide %q: %v", spec.name, err)
			}
		}
	}

	if err := f.SaveAs(path); err != nil {
		t.Fatalf("save %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
	return path
}

// mixedVisibilityWorkbook is the workbook these tests share: one shown sheet,
// one hidden, one very hidden, one shown again, in that order. The last one is
// there so "order is preserved" can fail if filtering reorders anything.
func mixedVisibilityWorkbook(t *testing.T, dir string) string {
	t.Helper()
	return visibilityWorkbook(t, filepath.Join(dir, "book.xlsx"),
		sheetSpec{"Shown", sheetVisible},
		sheetSpec{"Hidden", sheetHidden},
		sheetSpec{"Secret", sheetVeryHidden},
		sheetSpec{"AlsoShown", sheetVisible},
	)
}

// tableNames returns the loaded table names, sorted.
func tableNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("list tables: %v", err)
	}
	sort.Strings(names)
	return names
}

func assertTables(t *testing.T, db *sql.DB, want ...string) {
	t.Helper()
	sort.Strings(want)
	got := tableNames(t, db)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("tables = %v, want %v", got, want)
	}
}

// TestExcelSheetsInFile pins the reporting helper a caller uses to explain a
// load: every sheet, in workbook order, with the visibility that decided
// whether it was loaded.
func TestExcelSheetsInFile(t *testing.T) {
	t.Parallel()
	path := mixedVisibilityWorkbook(t, t.TempDir())

	sheets, err := ExcelSheetsInFile(path)
	if err != nil {
		t.Fatalf("ExcelSheetsInFile: %v", err)
	}
	want := []ExcelSheet{
		{Name: "Shown", Visible: true},
		{Name: "Hidden", Visible: false},
		{Name: "Secret", Visible: false},
		{Name: "AlsoShown", Visible: true},
	}
	if len(sheets) != len(want) {
		t.Fatalf("got %d sheets, want %d: %+v", len(sheets), len(want), sheets)
	}
	for i := range want {
		if sheets[i] != want[i] {
			t.Errorf("sheet %d = %+v, want %+v", i, sheets[i], want[i])
		}
	}
}

// TestExcelSheetsInFileCompressed pins that the helper answers for a workbook
// behind a codec, because a load accepts one there too.
func TestExcelSheetsInFileCompressed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	plain := mixedVisibilityWorkbook(t, dir)
	gzPath := gzipFile(t, plain, filepath.Join(dir, "book.xlsx.gz"))

	sheets, err := ExcelSheetsInFile(gzPath)
	if err != nil {
		t.Fatalf("ExcelSheetsInFile: %v", err)
	}
	if len(sheets) != 4 {
		t.Fatalf("got %d sheets, want 4: %+v", len(sheets), sheets)
	}
	if sheets[1].Name != "Hidden" || sheets[1].Visible {
		t.Errorf("second sheet = %+v, want a hidden %q", sheets[1], "Hidden")
	}
}

func TestExcelSheetsInReader(t *testing.T) {
	t.Parallel()
	path := mixedVisibilityWorkbook(t, t.TempDir())
	data, err := os.ReadFile(path) //nolint:gosec // a workbook this test just wrote
	if err != nil {
		t.Fatal(err)
	}

	sheets, err := ExcelSheetsInReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ExcelSheetsInReader: %v", err)
	}
	if len(sheets) != 4 {
		t.Fatalf("got %d sheets, want 4: %+v", len(sheets), sheets)
	}
	if !sheets[3].Visible || sheets[3].Name != "AlsoShown" {
		t.Errorf("last sheet = %+v, want a visible %q", sheets[3], "AlsoShown")
	}
}

func TestExcelSheetsInFileRejectsUnreadableWorkbook(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "broken.xlsx")
	if err := os.WriteFile(path, []byte("not a zip archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ExcelSheetsInFile(path); !errors.Is(err, ErrParsing) {
		t.Fatalf("error = %v, want ErrParsing", err)
	}
}

// TestOpenAppliesTheDefaultSheetPolicy is the compatibility guard: a caller
// that names no policy still gets every sheet, exactly as before the policy
// existed. Changing this silently would drop tables out of existing programs.
// TestExcelSheetsInFileRefusesWhatIsNotAFile pins the exported call a caller
// reaches first, before they have decided to load anything: it opens the path
// it is given, so a named pipe made it block for the life of the process.
func TestExcelSheetsInFileRefusesWhatIsNotAFile(t *testing.T) {
	t.Parallel()

	pipe := filepath.Join(t.TempDir(), "book.xlsx")
	makeFIFO(t, pipe)

	done := make(chan error, 1)
	go func() {
		_, err := ExcelSheetsInFile(pipe)
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), "a named pipe")
	case <-time.After(30 * time.Second):
		t.Fatal("ExcelSheetsInFile did not return: it is waiting for a writer on the pipe")
	}
}

func TestOpenAppliesTheDefaultSheetPolicy(t *testing.T) {
	t.Parallel()
	path := mixedVisibilityWorkbook(t, t.TempDir())

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	assertTables(t, db, "book_Shown", "book_Hidden", "book_Secret", "book_AlsoShown")
}

// TestBuilderSheetPolicyFromPath drives the policy through the path load, which
// is the one that turns each sheet into its own table.
func TestBuilderSheetPolicyFromPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy ExcelSheetPolicy
		want   []string
	}{
		{
			name:   "the all policy loads the hidden and very hidden sheets too",
			policy: ExcelSheetPolicyAll,
			want:   []string{"book_Shown", "book_Hidden", "book_Secret", "book_AlsoShown"},
		},
		{
			name:   "the visible-only policy loads neither the hidden nor the very hidden sheet",
			policy: ExcelSheetPolicyVisibleOnly,
			want:   []string{"book_Shown", "book_AlsoShown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := mixedVisibilityWorkbook(t, t.TempDir())

			db := openWithSheetPolicy(t, tt.policy, path)
			defer func() { _ = db.Close() }()
			assertTables(t, db, tt.want...)
		})
	}
}

// TestBuilderSheetPolicyFromCompressedPath pins that a codec around the
// workbook does not lose the policy: the file is unwrapped and then read by the
// same path, so a policy applied only to plain workbooks would show up here.
func TestBuilderSheetPolicyFromCompressedPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	plain := mixedVisibilityWorkbook(t, dir)
	gzPath := gzipFile(t, plain, filepath.Join(dir, "packed.xlsx.gz"))

	db := openWithSheetPolicy(t, ExcelSheetPolicyVisibleOnly, gzPath)
	defer func() { _ = db.Close() }()
	assertTables(t, db, "packed_Shown", "packed_AlsoShown")
}

// TestBuilderSheetPolicyFromReader covers the reader load, which takes one
// sheet rather than all of them. Under the visible-only policy the sheet it
// takes must be the first one shown, not the first one stored.
func TestBuilderSheetPolicyFromReader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy ExcelSheetPolicy
		want   []string
	}{
		{
			name:   "the all policy takes every sheet, hidden ones included",
			policy: ExcelSheetPolicyAll,
			want:   []string{"book_Buried", "book_Shown"},
		},
		{
			name:   "the visible-only policy leaves the hidden sheet out",
			policy: ExcelSheetPolicyVisibleOnly,
			want:   []string{"book_Shown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The hidden sheet is stored first on purpose: the two policies then
			// disagree about which sheets the reader load takes.
			path := visibilityWorkbook(t, filepath.Join(t.TempDir(), "book.xlsx"),
				sheetSpec{"Buried", sheetHidden},
				sheetSpec{"Shown", sheetVisible},
			)
			data, err := os.ReadFile(path) //nolint:gosec // a workbook this test just wrote
			if err != nil {
				t.Fatal(err)
			}

			builder, err := buildForTest(

				context.Background(), NewBuilder().
					AddReader(bytes.NewReader(data), "book", FileTypeXLSX).
					WithExcelSheetPolicy(tt.policy))

			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			db, err := builder.Open(context.Background())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			assert.Equal(t, tt.want, loadedTables(t, db), "the policy decides which sheets a reader load takes")
		})
	}
}

// TestBuilderSheetPolicyFromFS covers the embedded-filesystem load, which
// reaches the same reader path through a different entry point.
func TestBuilderSheetPolicyFromFS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	visibilityWorkbook(t, filepath.Join(dir, "book.xlsx"),
		sheetSpec{"Buried", sheetHidden},
		sheetSpec{"Shown", sheetVisible},
	)

	builder, err := buildForTest(

		context.Background(), NewBuilder().
			AddFS(os.DirFS(dir)).
			WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly))

	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	db, err := builder.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	assert.Equal(t, []string{"book_Shown"}, loadedTables(t, db),
		"the policy decides which sheets a filesystem load takes")
}

// TestSheetPolicyKeepsWorkbookOrder pins that filtering removes sheets without
// reordering the ones it keeps. Table names are derived per sheet, so a reorder
// would attach the wrong rows to the wrong name.
func TestSheetPolicyKeepsWorkbookOrder(t *testing.T) {
	t.Parallel()
	path := visibilityWorkbook(t, filepath.Join(t.TempDir(), "book.xlsx"),
		sheetSpec{"A", sheetVisible},
		sheetSpec{"B", sheetHidden},
		sheetSpec{"C", sheetVisible},
		sheetSpec{"D", sheetVeryHidden},
		sheetSpec{"E", sheetVisible},
	)

	db := openWithSheetPolicy(t, ExcelSheetPolicyVisibleOnly, path)
	defer func() { _ = db.Close() }()

	for table, want := range map[string]string{"book_A": "A", "book_C": "C", "book_E": "E"} {
		var got string
		if err := db.QueryRowContext(context.Background(), "SELECT v FROM "+table).Scan(&got); err != nil {
			t.Fatalf("query %s: %v", table, err)
		}
		if got != want {
			t.Errorf("%s holds %q, want %q; the rows moved between sheets", table, got, want)
		}
	}
}

// TestSheetPolicyDecidesCollisionsAfterFiltering is the rule the two features
// have to agree on: a sheet nobody loads cannot make a loaded sheet fail.
func TestSheetPolicyDecidesCollisionsAfterFiltering(t *testing.T) {
	t.Parallel()

	// "Q1 sales" and "Q1.sales" both sanitize to book_Q1_sales, so the workbook
	// is refused when both are loaded and accepted when only one is.
	newBook := func(t *testing.T) string {
		t.Helper()
		return visibilityWorkbook(t, filepath.Join(t.TempDir(), "book.xlsx"),
			sheetSpec{"Q1 sales", sheetVisible},
			sheetSpec{"Q1.sales", sheetHidden},
		)
	}

	t.Run("a hidden sheet that is not loaded does not collide with a shown one", func(t *testing.T) {
		t.Parallel()
		db := openWithSheetPolicy(t, ExcelSheetPolicyVisibleOnly, newBook(t))
		defer func() { _ = db.Close() }()

		var got string
		if err := db.QueryRowContext(context.Background(), "SELECT v FROM book_Q1_sales").Scan(&got); err != nil {
			t.Fatalf("query book_Q1_sales: %v", err)
		}
		if got != "Q1 sales" {
			t.Errorf("book_Q1_sales holds %q, want the shown sheet's row", got)
		}
	})

	t.Run("the same two sheets collide when the policy loads both", func(t *testing.T) {
		t.Parallel()
		_, err := openWithSheetPolicyErr(ExcelSheetPolicyAll, newBook(t))
		if !errors.Is(err, ErrDuplicateTable) {
			t.Fatalf("error = %v, want ErrDuplicateTable", err)
		}
		for _, want := range []string{"Q1 sales", "Q1.sales", "book_Q1_sales"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should name %s", err, want)
			}
		}
	})

	t.Run("a very hidden sheet is left out of the collision check too", func(t *testing.T) {
		t.Parallel()
		path := visibilityWorkbook(t, filepath.Join(t.TempDir(), "book.xlsx"),
			sheetSpec{"Q1 sales", sheetVisible},
			sheetSpec{"Q1.sales", sheetVeryHidden},
		)
		db := openWithSheetPolicy(t, ExcelSheetPolicyVisibleOnly, path)
		defer func() { _ = db.Close() }()
		assertTables(t, db, "book_Q1_sales")
	})
}

// TestSheetPolicyDecidesCollisionsAcrossWorkbooks applies the same rule to two
// files loaded together: the sheets that decide table names are the loaded
// ones, per workbook, and a sheet the policy leaves out contributes nothing to
// any of it.
func TestSheetPolicyDecidesCollisionsAcrossWorkbooks(t *testing.T) {
	t.Parallel()

	// Both workbooks hold a sheet called "Shared"; in the first it is hidden.
	// Their tables are named after their files, so what the policy changes is
	// which of the four tables exist at all.
	newPair := func(t *testing.T) (first, second string) {
		t.Helper()
		dir := t.TempDir()
		first = visibilityWorkbook(t, filepath.Join(dir, "left.xlsx"),
			sheetSpec{"Data", sheetVisible},
			sheetSpec{"Shared", sheetHidden},
		)
		second = visibilityWorkbook(t, filepath.Join(dir, "right.xlsx"),
			sheetSpec{"Data", sheetVisible},
			sheetSpec{"Shared", sheetVisible},
		)
		return first, second
	}

	t.Run("the visible-only policy leaves out only the workbook that hid the sheet", func(t *testing.T) {
		t.Parallel()
		first, second := newPair(t)
		db := openWithSheetPolicy(t, ExcelSheetPolicyVisibleOnly, first, second)
		defer func() { _ = db.Close() }()

		assertTables(t, db, "left_Data", "right_Data", "right_Shared")
		var got string
		if err := db.QueryRowContext(context.Background(), "SELECT v FROM right_Shared").Scan(&got); err != nil {
			t.Fatalf("query right_Shared: %v", err)
		}
		if got != "Shared" {
			t.Errorf("right_Shared holds %q, want the second workbook's own row", got)
		}
	})

	t.Run("the all policy loads the hidden sheet as its own workbook's table", func(t *testing.T) {
		t.Parallel()
		first, second := newPair(t)
		db := openWithSheetPolicy(t, ExcelSheetPolicyAll, first, second)
		defer func() { _ = db.Close() }()
		assertTables(t, db, "left_Data", "left_Shared", "right_Data", "right_Shared")
	})

	t.Run("a workbook whose loaded sheets collide with each other is still refused", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		clash := visibilityWorkbook(t, filepath.Join(dir, "book.xlsx"),
			sheetSpec{"Q1 sales", sheetVisible},
			sheetSpec{"Q1.sales", sheetVisible},
		)
		other := visibilityWorkbook(t, filepath.Join(dir, "other.xlsx"),
			sheetSpec{"Data", sheetVisible},
		)
		if _, err := openWithSheetPolicyErr(ExcelSheetPolicyVisibleOnly, other, clash); !errors.Is(err, ErrDuplicateTable) {
			t.Fatalf("error = %v, want ErrDuplicateTable", err)
		}
	})
}

// TestVisibleOnlyPolicyOnAnAllHiddenWorkbook pins that "everything was filtered
// out" is reported as the policy's doing rather than as a workbook with no
// sheets, because the two need different things done about them.
func TestVisibleOnlyPolicyOnAnAllHiddenWorkbook(t *testing.T) {
	t.Parallel()
	// The workbook has to keep one shown sheet for Excel's sake, so the reader
	// path is used with a workbook whose only other sheet is hidden, and the
	// shown one is deleted from the sheet list by naming a policy that skips it.
	path := visibilityWorkbook(t, filepath.Join(t.TempDir(), "book.xlsx"),
		sheetSpec{"Shown", sheetVisible},
		sheetSpec{"Buried", sheetHidden},
	)
	sheets, err := ExcelSheetsInFile(path)
	if err != nil {
		t.Fatalf("ExcelSheetsInFile: %v", err)
	}
	if len(sheets) != 2 {
		t.Fatalf("got %d sheets, want 2", len(sheets))
	}

	// A source whose every sheet is hidden cannot be written by excelize, so the
	// "nothing left" message is checked against the helper that builds it.
	err = noExcelSheetsError(fakeAllHiddenWorkbook{}, ExcelSheetPolicyVisibleOnly)
	if !errors.Is(err, ErrEmptyData) {
		t.Fatalf("error = %v, want ErrEmptyData", err)
	}
	if !strings.Contains(err.Error(), "no visible sheets") {
		t.Errorf("error %q should say the policy left nothing, not that the file has no sheets", err)
	}
	err = noExcelSheetsError(fakeAllHiddenWorkbook{empty: true}, ExcelSheetPolicyVisibleOnly)
	if !strings.Contains(err.Error(), "no sheets found") {
		t.Errorf("error %q should say the workbook holds no sheets at all", err)
	}
}

// fakeAllHiddenWorkbook stands in for a workbook Excel will not write: one with
// no sheet that is shown.
type fakeAllHiddenWorkbook struct{ empty bool }

func (f fakeAllHiddenWorkbook) GetSheetList() []string {
	if f.empty {
		return nil
	}
	return []string{"Buried"}
}

func (f fakeAllHiddenWorkbook) GetSheetVisible(string) (bool, error) { return false, nil }

// TestSheetPolicyReportsAnUnreadableVisibility pins that the load fails rather
// than assuming a visibility, on the path that turns sheets into tables.
func TestSheetPolicyReportsAnUnreadableVisibility(t *testing.T) {
	t.Parallel()
	boom := errors.New("sheet index out of range")
	_, _, err := selectExcelSheets(failingVisibilityWorkbook{err: boom}, ExcelSheetPolicyVisibleOnly)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap %v", err, boom)
	}
	if !errors.Is(err, ErrParsing) {
		t.Errorf("error = %v, want it to carry ErrParsing", err)
	}
}

type failingVisibilityWorkbook struct{ err error }

func (f failingVisibilityWorkbook) GetSheetList() []string { return []string{"Sheet1"} }

func (f failingVisibilityWorkbook) GetSheetVisible(string) (bool, error) { return false, f.err }

// openWithSheetPolicy loads paths under policy and returns the open database.
func openWithSheetPolicy(t *testing.T, policy ExcelSheetPolicy, paths ...string) *sql.DB {
	t.Helper()
	db, err := openWithSheetPolicyErr(policy, paths...)
	if err != nil {
		t.Fatalf("open under the %s policy: %v", policy, err)
	}
	return db
}

func openWithSheetPolicyErr(policy ExcelSheetPolicy, paths ...string) (*sql.DB, error) {
	ctx := context.Background()
	builder, err := buildForTest(

		ctx, NewBuilder().
			AddPaths(paths...).
			WithExcelSheetPolicy(policy))

	if err != nil {
		return nil, err
	}
	return builder.Open(ctx)
}

// gzipFile writes src's bytes to dst through gzip and returns dst.
func gzipFile(t *testing.T, src, dst string) string {
	t.Helper()
	data, err := os.ReadFile(src) //nolint:gosec // a file this test just wrote
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(dst) //nolint:gosec // a path under the test's temp directory
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(out)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return dst
}

// TestLoadIntoAppliesSheetPolicy covers the other load entry point: loading
// into a caller-owned database rather than opening a new one.
func TestLoadIntoAppliesSheetPolicy(t *testing.T) {
	t.Parallel()
	path := mixedVisibilityWorkbook(t, t.TempDir())

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	builder, err := buildForTest(

		ctx, NewBuilder().
			AddPath(path).
			WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly))

	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := builder.LoadInto(ctx, db); err != nil {
		t.Fatalf("LoadInto: %v", err)
	}
	assertTables(t, db, "book_Shown", "book_AlsoShown")
}

// TestBuilderSheetPolicyDefaultIsAll states the default in the one place a
// reader looks for it, so a change to the zero value cannot pass unnoticed.
func TestBuilderSheetPolicyDefaultIsAll(t *testing.T) {
	t.Parallel()
	if got := NewBuilder().excelSheetPolicy; got != ExcelSheetPolicyAll {
		t.Errorf("a new builder loads with the %s policy, want %s", got, ExcelSheetPolicyAll)
	}
	if got := newStreamProcessor(1).excelSheetPolicy; got != ExcelSheetPolicyAll {
		t.Errorf("a new stream processor loads with the %s policy, want %s", got, ExcelSheetPolicyAll)
	}
}

// TestSaveKeepsTheSheetsThePolicyDidNotLoad pins that a write-back does not
// delete what it did not read. The policy says which sheets to load; the save
// used to rebuild the workbook from the tables it held, so a sheet the caller
// had asked to ignore was removed from their file, with nothing said and no
// query having to run.
func TestSaveKeepsTheSheetsThePolicyDidNotLoad(t *testing.T) {
	t.Parallel()

	for _, hidden := range []sheetVisibility{sheetHidden, sheetVeryHidden} {
		t.Run(map[sheetVisibility]string{sheetHidden: "hidden", sheetVeryHidden: "very hidden"}[hidden], func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			path := visibilityWorkbook(t, filepath.Join(t.TempDir(), "book.xlsx"),
				sheetSpec{name: defaultSheetName},
				sheetSpec{name: "Secret", visibility: hidden},
			)

			validated, err := buildForTest(

				ctx, NewBuilder().AddPath(path).
					WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly).
					EnableAutoSave(""))

			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			// The save happens on close, with nothing edited.
			require.NoError(t, db.Close())

			after, err := excelize.OpenFile(path)
			require.NoError(t, err)
			defer after.Close()

			assert.Contains(t, after.GetSheetList(), "Secret", "the sheet the policy skipped was deleted from the workbook")
			rows, err := after.GetRows("Secret")
			require.NoError(t, err)
			assert.Equal(t, [][]string{{"v"}, {"Secret"}}, rows, "the skipped sheet kept its rows")

			visible, err := after.GetSheetVisible("Secret")
			require.NoError(t, err)
			assert.False(t, visible, "the skipped sheet is still hidden")
		})
	}
}

// TestSaveKeepsWhatItDidNotWrite pins the rest of what a workbook carries. The
// save rebuilt the file from values, so a column width, a merged range and a
// comment were gone after a save that changed nothing.
func TestSaveKeepsWhatItDidNotWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "styled.xlsx")

	book := excelize.NewFile()
	require.NoError(t, book.SetCellValue(defaultSheetName, "A1", "n"))
	require.NoError(t, book.SetCellValue(defaultSheetName, "A2", 3))
	require.NoError(t, book.SetColWidth(defaultSheetName, "A", "A", 30))
	style, err := book.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true}})
	require.NoError(t, err)
	require.NoError(t, book.SetCellStyle(defaultSheetName, "A2", "A2", style))
	require.NoError(t, book.MergeCell(defaultSheetName, "D1", "E1"))
	require.NoError(t, book.AddComment(defaultSheetName, excelize.Comment{Cell: "A1", Text: "note"}))
	require.NoError(t, book.SaveAs(path))
	require.NoError(t, book.Close())

	validated, err := buildForTest(ctx, NewBuilder().AddPath(path).EnableAutoSave(""))
	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	after, err := excelize.OpenFile(path)
	require.NoError(t, err)
	defer after.Close()

	// A cell the save does rewrite keeps the style it carried: the value is
	// written over it rather than the cell being built again.
	written, err := after.GetCellStyle(defaultSheetName, "A2")
	require.NoError(t, err)
	assert.NotZero(t, written, "the style of a rewritten cell was dropped")

	width, err := after.GetColWidth(defaultSheetName, "A")
	require.NoError(t, err)
	assert.InDelta(t, 30.0, width, 0.001, "the column width was reset")

	merged, err := after.GetMergeCells(defaultSheetName)
	require.NoError(t, err)
	assert.Len(t, merged, 1, "the merged range was dropped")

	comments, err := after.GetComments(defaultSheetName)
	require.NoError(t, err)
	assert.Len(t, comments, 1, "the comment was dropped")

	rows, err := after.GetRows(defaultSheetName)
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"n"}, {"3"}}, rows, "the rows are still the table's")
}

// TestSaveAndAFormulaCell pins the last thing a rebuilt workbook lost. A
// formula is content rather than presentation: the cell that held one came back
// empty, so the workbook no longer carried the rule that produced its numbers,
// and a spreadsheet opening the file showed a blank column where a computed one
// had been.
//
// A workbook stores a formula and the value it last evaluated to. filesql reads
// the value, so writing that same value back says nothing the cell did not
// already say -- and writing it is what took the formula. A cell whose value the
// caller did change is the other half of the rule: it holds what they set, and a
// formula that no longer produces it cannot stay.
func TestSaveAndAFormulaCell(t *testing.T) {
	t.Parallel()

	// writeWorkbook makes a workbook whose second column is computed from its
	// first, with no value cached for the computed cell.
	writeWorkbook := func(t *testing.T, path string) {
		t.Helper()
		book := excelize.NewFile()
		require.NoError(t, book.SetCellValue(defaultSheetName, "A1", "n"))
		require.NoError(t, book.SetCellValue(defaultSheetName, "B1", "double"))
		require.NoError(t, book.SetCellValue(defaultSheetName, "A2", 3))
		require.NoError(t, book.SetCellFormula(defaultSheetName, "B2", "A2*2"))
		// A row whose last cell is empty is stored short, which is the shape a
		// blank cell arrives in and has to be recognized as unchanged too.
		require.NoError(t, book.SetCellValue(defaultSheetName, "A3", 5))
		require.NoError(t, book.SaveAs(path))
		require.NoError(t, book.Close())
	}

	tests := []struct {
		name string
		// edit runs before the save, or is empty for a save that changes nothing.
		edit string
		// wantFormula is what B2 holds afterwards.
		wantFormula string
		// wantValue is what B2 shows afterwards.
		wantValue string
		// wantBlank is what B3, the cell the source workbook stored nothing
		// for, shows afterwards.
		wantBlank string
	}{
		{
			name:        "a save that changed nothing keeps the formula",
			wantFormula: "A2*2",
		},
		{
			name:        "an edited cell holds the value the caller set",
			edit:        "UPDATE calc_Sheet1 SET double = '99' WHERE n = '3'",
			wantFormula: "",
			wantValue:   "99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "calc.xlsx")
			writeWorkbook(t, path)

			validated, err := buildForTest(ctx, NewBuilder().AddPath(path).EnableAutoSave(""))
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			if tt.edit != "" {
				_, err = db.ExecContext(ctx, tt.edit)
				require.NoError(t, err)
			}
			require.NoError(t, db.Close()) // The save happens here.

			after, err := excelize.OpenFile(path)
			require.NoError(t, err)
			defer after.Close()

			formula, err := after.GetCellFormula(defaultSheetName, "B2")
			require.NoError(t, err)
			assert.Equal(t, tt.wantFormula, formula)
			value, err := after.GetCellValue(defaultSheetName, "B2")
			require.NoError(t, err)
			assert.Equal(t, tt.wantValue, value)
			blank, err := after.GetCellValue(defaultSheetName, "B3")
			require.NoError(t, err)
			assert.Equal(t, tt.wantBlank, blank, "the cell the table has nothing for")
		})
	}
}

// TestSaveKeepsADateCellADate pins the same rule for the other thing a cell
// holds beyond its text. A workbook stores a date as a serial number and a
// number format; filesql reads it as the ISO 8601 the datetime inference wants,
// and writing that string back turned a date cell into text, so the sheet that
// showed 03-15-23 showed 2023-03-15 left-aligned and no longer sorted or
// calculated as a date.
func TestSaveKeepsADateCellADate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dated.xlsx")

	book := excelize.NewFile()
	require.NoError(t, book.SetCellValue(defaultSheetName, "A1", "when"))
	require.NoError(t, book.SetCellValue(defaultSheetName, "A2", time.Date(2023, 3, 15, 0, 0, 0, 0, time.UTC)))
	style, err := book.NewStyle(&excelize.Style{NumFmt: 14})
	require.NoError(t, err)
	require.NoError(t, book.SetCellStyle(defaultSheetName, "A2", "A2", style))
	require.NoError(t, book.SaveAs(path))
	require.NoError(t, book.Close())

	before, err := excelize.OpenFile(path)
	require.NoError(t, err)
	serial, err := before.GetCellValue(defaultSheetName, "A2", excelize.Options{RawCellValue: true})
	require.NoError(t, err)
	shown, err := before.GetCellValue(defaultSheetName, "A2")
	require.NoError(t, err)
	require.NoError(t, before.Close())

	validated, err := buildForTest(ctx, NewBuilder().AddPath(path).EnableAutoSave(""))
	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	after, err := excelize.OpenFile(path)
	require.NoError(t, err)
	defer after.Close()

	stored, err := after.GetCellValue(defaultSheetName, "A2", excelize.Options{RawCellValue: true})
	require.NoError(t, err)
	assert.Equal(t, serial, stored, "the date became text instead of staying a serial number")
	rendered, err := after.GetCellValue(defaultSheetName, "A2")
	require.NoError(t, err)
	assert.Equal(t, shown, rendered, "the sheet no longer shows the date the way it was formatted")
}

// TestSaveShrinksASheetTheTableNoLongerFills pins the other half of writing onto
// an existing workbook. Cells are written over rather than the sheet being
// cleared first, which is what keeps the styles and the merges, so the rows and
// columns a deletion left behind have to be removed afterwards; without that a
// sheet would keep the rows the table no longer has and the save would report
// success over a file holding deleted data.
func TestSaveShrinksASheetTheTableNoLongerFills(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "roster.xlsx")

	book := excelize.NewFile()
	for cell, value := range map[string]any{
		"A1": "name", "B1": "team", "C1": "seat",
		"A2": "ada", "B2": "core", "C2": "1",
		"A3": "bob", "B3": "core", "C3": "2",
		"A4": "cyd", "B4": "edge", "C4": "3",
	} {
		require.NoError(t, book.SetCellValue(defaultSheetName, cell, value))
	}
	require.NoError(t, book.SaveAs(path))
	require.NoError(t, book.Close())

	validated, err := buildForTest(ctx, NewBuilder().AddPath(path).EnableAutoSave(""))
	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "DELETE FROM roster_Sheet1 WHERE team = 'core'")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "ALTER TABLE roster_Sheet1 DROP COLUMN seat")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	after, err := excelize.OpenFile(path)
	require.NoError(t, err)
	defer after.Close()

	rows, err := after.GetRows(defaultSheetName)
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"name", "team"}, {"cyd", "edge"}}, rows)
}

// TestExcelSheetName pins how a table name is adapted to Excel's worksheet-name
// rules. A table name comes from a file name, so one that is long or punctuated
// is ordinary input rather than a mistake: the dump used to hand it to Excel
// as-is and fail, so a table named after monthly_sales_report_2026_q3_final.csv
// could not be exported to XLSX at all.
func TestExcelSheetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table string
		want  string
	}{
		{name: "a short name is unchanged", table: "sales", want: "sales"},
		{name: "exactly the limit is unchanged", table: strings.Repeat("a", 31), want: strings.Repeat("a", 31)},
		{name: "one over the limit is cut to it", table: strings.Repeat("a", 32), want: strings.Repeat("a", 31)},
		{name: "the limit counts runes, not bytes", table: strings.Repeat("売", 32), want: strings.Repeat("売", 31)},
		{name: "a bracket becomes an underscore", table: "sales[2026]", want: "sales_2026_"},
		{name: "a question mark becomes an underscore", table: "what?", want: "what_"},
		{name: "a star becomes an underscore", table: "a*b", want: "a_b"},
		{name: "a colon becomes an underscore", table: "12:34", want: "12_34"},
		{name: "a slash becomes an underscore", table: "a/b", want: "a_b"},
		{name: "a backslash becomes an underscore", table: `a\b`, want: "a_b"},
		{name: "surrounding apostrophes are dropped", table: "'sales'", want: "sales"},
		{name: "a name that leaves nothing usable falls back", table: "'", want: defaultSheetName},
		{name: "an empty name falls back", table: "", want: defaultSheetName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, excelSheetName(tt.table))
		})
	}
}

// TestDumpXLSXAdaptsSheetName pins that a dump of a table Excel cannot name as
// a sheet succeeds and comes back under the table's own name. The sheet is
// spelled the way Excel allows -- at most 31 characters, without the seven it
// forbids -- and a sheet named after the file is not repeated in the table
// name, so a 32-character table used to come back as 64.
func TestDumpXLSXAdaptsSheetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		table     string
		wantSheet string
	}{
		{name: "a long name", table: "monthly_sales_report_2026_q3_final", wantSheet: "monthly_sales_report_2026_q3_fi"},
		{name: "a name at the limit", table: "quarterly_revenue_by_region_202", wantSheet: "quarterly_revenue_by_region_202"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openWithTable(t,
				`CREATE TABLE "`+tt.table+`" (x TEXT)`,
				`INSERT INTO "`+tt.table+`" VALUES ('kept')`)

			outDir := t.TempDir()
			require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatXLSX)))

			assert.Equal(t, tt.wantSheet, excelSheetName(tt.table), "the sheet is spelled the way Excel allows")

			reloaded, err := OpenContext(t.Context(), filepath.Join(outDir, tt.table+".xlsx"))
			require.NoError(t, err)
			defer reloaded.Close()

			rows, err := reloaded.QueryContext(t.Context(), "SELECT name FROM sqlite_master WHERE type='table'")
			require.NoError(t, err)
			defer rows.Close()
			names := make([]string, 0, 1)
			for rows.Next() {
				var n string
				require.NoError(t, rows.Scan(&n))
				names = append(names, n)
			}
			require.NoError(t, rows.Err())

			// The sheet is named after the file, in the spelling Excel allows,
			// so the table keeps the name it was dumped from.
			require.Len(t, names, 1)
			assert.Equal(t, tt.table, names[0])

			var kept string
			require.NoError(t, reloaded.QueryRowContext(t.Context(),
				"SELECT x FROM "+quoteIdentifier(names[0])).Scan(&kept))
			assert.Equal(t, "kept", kept)
		})
	}

	t.Run("a name a load would spell differently is refused", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, `CREATE TABLE "sales[2026]" (x TEXT)`, `INSERT INTO "sales[2026]" VALUES ('kept')`)

		err := DumpDatabase(db, t.TempDir(), NewDumpOptions().WithFormat(OutputFormatXLSX))

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidData)
		assert.Contains(t, err.Error(), "sales2026", "the error must name what a load would call the table")
	})
}

// TestXLSXDateCellsImportAsISO pins what a date cell holds against how it is
// shown. A workbook stores a serial and a number format, and GetRows applies the
// format: the same day arrived as "03-15-23" under format 14, so ORDER BY sorted
// the column lexically and a comparison against an ISO literal never matched.
func TestXLSXDateCellsImportAsISO(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "d.xlsx")

	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	require.NoError(t, f.SetSheetRow("Sheet1", "A1", &[]any{"date", "shown", "n"}))
	// 45000 is 2023-03-15. Column A is formatted mm-dd-yy, column B as a plain
	// number, and column C holds text that looks like a date.
	require.NoError(t, f.SetCellValue("Sheet1", "A2", 45000))
	require.NoError(t, f.SetCellValue("Sheet1", "B2", 45000))
	require.NoError(t, f.SetCellValue("Sheet1", "C2", "03-15-23"))
	style, err := f.NewStyle(&excelize.Style{NumFmt: 14})
	require.NoError(t, err)
	require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))
	require.NoError(t, f.SaveAs(path))

	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var date, shown, n string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT "date", shown, n FROM d_Sheet1`).Scan(&date, &shown, &n))

	assert.Equal(t, "2023-03-15", date, "a date cell holds a day, whatever format shows it")
	assert.Equal(t, "45000", shown, "a number formatted as a number is a number")
	assert.Equal(t, "03-15-23", n, "text that looks like a date is text, and is left as it is")
}

// TestXLSXDateTimeCellKeepsItsTime covers the other half: a cell whose serial
// carries a time of day keeps it, rather than being cut back to the date.
func TestXLSXDateTimeCellKeepsItsTime(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dt.xlsx")

	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	require.NoError(t, f.SetSheetRow("Sheet1", "A1", &[]any{"at"}))
	// Half a day past the date is noon.
	require.NoError(t, f.SetCellValue("Sheet1", "A2", 45000.5))
	style, err := f.NewStyle(&excelize.Style{NumFmt: 22})
	require.NoError(t, err)
	require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))
	require.NoError(t, f.SaveAs(path))

	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var at string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT at FROM dt_Sheet1`).Scan(&at))

	assert.Equal(t, "2023-03-15 12:00:00", at)
}

// TestXLSXDateHonorsTheWorkbookEpoch covers the other calendar. A workbook
// written on a Mac before 2016 counts its serials from 1904, and reading them
// against 1900 puts every date in the file four years and a day early.
func TestXLSXDateHonorsTheWorkbookEpoch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mac.xlsx")

	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	date1904 := true
	require.NoError(t, f.SetWorkbookProps(&excelize.WorkbookPropsOptions{Date1904: &date1904}))
	require.NoError(t, f.SetSheetRow("Sheet1", "A1", &[]any{"date"}))
	require.NoError(t, f.SetCellValue("Sheet1", "A2", 45000))
	style, err := f.NewStyle(&excelize.Style{NumFmt: 14})
	require.NoError(t, err)
	require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))
	require.NoError(t, f.SaveAs(path))

	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var date string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT "date" FROM mac_Sheet1`).Scan(&date))

	// The same serial is 2023-03-15 under the 1900 epoch and 2027-03-16 here.
	assert.Equal(t, "2027-03-16", date)
}

// TestXLSXElapsedDurationIsNotADate pins what must not be converted. An elapsed
// duration counts hours, not days from an epoch: [h]:mm of 1.5 is 36 hours, and
// reading it as a calendar datetime invents a date the cell never held.
func TestXLSXElapsedDurationIsNotADate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "elapsed.xlsx")

	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	require.NoError(t, f.SetSheetRow("Sheet1", "A1", &[]any{"worked"}))
	require.NoError(t, f.SetCellValue("Sheet1", "A2", 1.5))
	style, err := f.NewStyle(&excelize.Style{NumFmt: 46}) // [h]:mm:ss
	require.NoError(t, err)
	require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))
	require.NoError(t, f.SaveAs(path))

	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var worked string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT worked FROM elapsed_Sheet1`).Scan(&worked))

	assert.NotContains(t, worked, "1900-", "36 hours is not a day in January 1900")
	assert.NotContains(t, worked, "1899-")
}

// TestXLSXLocalizedDateFormatImportsAsISO covers the date formats a workbook
// written in Japanese, Chinese, or Korean uses. They are ordinary built-in
// formats with their own IDs, and recognizing only the English-stable ones left
// those files with the format-dependent text this conversion exists to remove.
func TestXLSXLocalizedDateFormatImportsAsISO(t *testing.T) {
	t.Parallel()

	// 30 is "m/d/yy" in the East Asian tables, 27 "yyyy年m月", 36 a era date.
	for _, numFmt := range []int{27, 30, 36} {
		t.Run(fmt.Sprintf("number format %d", numFmt), func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "jp.xlsx")

			f := excelize.NewFile()
			defer func() { _ = f.Close() }()
			require.NoError(t, f.SetSheetRow("Sheet1", "A1", &[]any{"date"}))
			require.NoError(t, f.SetCellValue("Sheet1", "A2", 45000))
			style, err := f.NewStyle(&excelize.Style{NumFmt: numFmt})
			require.NoError(t, err)
			require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))
			require.NoError(t, f.SaveAs(path))

			db, err := OpenContext(ctx, path)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()

			var date string
			require.NoError(t, db.QueryRowContext(ctx, `SELECT "date" FROM jp_Sheet1`).Scan(&date))

			assert.Equal(t, "2023-03-15", date)
		})
	}
}

// dateCellWorkbook writes a workbook holding serial twice: once under numFmt,
// which is the cell a test loads, and once under an ISO format, which is what
// the workbook shows for that serial. It returns both renderings, so a test can
// assert a load against the file rather than against a constant of its own.
func dateCellWorkbook(t *testing.T, path string, serial float64, numFmt int, date1904 bool) (shown, iso string) {
	t.Helper()

	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	if date1904 {
		yes := true
		require.NoError(t, f.SetWorkbookProps(&excelize.WorkbookPropsOptions{Date1904: &yes}))
	}
	require.NoError(t, f.SetSheetRow("Sheet1", "A1", &[]any{"at", "oracle"}))
	require.NoError(t, f.SetCellValue("Sheet1", "A2", serial))
	require.NoError(t, f.SetCellValue("Sheet1", "B2", serial))
	style, err := f.NewStyle(&excelize.Style{NumFmt: numFmt})
	require.NoError(t, err)
	require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))
	isoFormat := "yyyy-mm-dd hh:mm:ss"
	oracle, err := f.NewStyle(&excelize.Style{CustomNumFmt: &isoFormat})
	require.NoError(t, err)
	require.NoError(t, f.SetCellStyle("Sheet1", "B2", "B2", oracle))
	require.NoError(t, f.SaveAs(path))

	shownFile, err := excelize.OpenFile(path)
	require.NoError(t, err)
	defer func() { _ = shownFile.Close() }()
	rows, err := shownFile.GetRows("Sheet1")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Len(t, rows[1], 2)
	// A cell at midnight holds a day and no time, which is how a date cell
	// loads.
	return rows[1][0], strings.TrimSuffix(rows[1][1], " 00:00:00")
}

// loadDateCell is the value a workbook's first date cell loads as.
func loadDateCell(t *testing.T, path, table string) string {
	t.Helper()

	ctx := context.Background()
	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var at string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT at FROM `+table+`_Sheet1`).Scan(&at))
	return at
}

// TestXLSX1900SerialsMatchTheWorkbook pins a date cell to the day the workbook
// shows. The 1900 date system counts serial 1 as January 1, 1900 and keeps a
// February 29, 1900 that never existed, so a conversion that counts plain days
// from 1899-12-30 is a day early for every date before March 1900 — a whole
// column of historical records read as the day before the one in the file.
func TestXLSX1900SerialsMatchTheWorkbook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		serial float64
		want   string
	}{
		{name: "the first day of the system", serial: 1, want: "1900-01-01"},
		{name: "the second day", serial: 2, want: "1900-01-02"},
		{name: "the day before the phantom leap day", serial: 59, want: "1900-02-28"},
		{name: "the day after it", serial: 61, want: "1900-03-01"},
		{name: "the day after that", serial: 62, want: "1900-03-02"},
		{name: "a modern day", serial: 45000, want: "2023-03-15"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "d.xlsx")
			// Number format 14 renders mm-dd-yy, which is the same day in
			// another order.
			_, iso := dateCellWorkbook(t, path, tt.serial, 14, false)
			require.Equal(t, tt.want, iso, "the workbook shows a different day than the test expects")

			assert.Equal(t, iso, loadDateCell(t, path, "d"))
		})
	}
}

// TestXLSXSerialWithoutADayStaysAsShown covers the two serials the 1900 system
// has no calendar day for. Serial 60 is the February 29, 1900 Excel keeps for
// compatibility and no calendar has, and a serial below 1 is before the system
// starts; converting either invents a day, and converting both onto their
// neighbors makes two different cells the same date.
func TestXLSXSerialWithoutADayStaysAsShown(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		serial float64
	}{
		{name: "the phantom leap day", serial: 60},
		{name: "day zero", serial: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "d.xlsx")
			shown, _ := dateCellWorkbook(t, path, tt.serial, 14, false)

			assert.Equal(t, shown, loadDateCell(t, path, "d"),
				"a serial with no calendar day keeps the text the workbook shows")
		})
	}
}

// TestXLSX1900SerialKeepsItsTimeOfDay pins that the correction moves the day
// and leaves the clock alone, on both sides of the boundary it turns on at.
func TestXLSX1900SerialKeepsItsTimeOfDay(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		serial float64
		want   string
	}{
		{name: "before the phantom leap day", serial: 59.25, want: "1900-02-28 06:00:00"},
		{name: "after it", serial: 61.5, want: "1900-03-01 12:00:00"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "d.xlsx")
			// Number format 22 renders a date and a time of day.
			_, iso := dateCellWorkbook(t, path, tt.serial, 22, false)
			require.Equal(t, tt.want, iso, "the workbook shows a different time than the test expects")

			assert.Equal(t, iso, loadDateCell(t, path, "d"))
		})
	}
}

// TestXLSX1904SerialsAreUntouched keeps the correction confined to the calendar
// that needs it. The 1904 system starts at serial 0 and has no phantom day, so
// nothing about it is off by one.
//
// The expected days here are Excel's documented meaning rather than the
// workbook's own rendering, which the other tests in this file assert against:
// the renderer shows the 1900 system's day-zero placeholder for serial 0 even
// in a 1904 workbook, and that serial is the boundary worth pinning.
func TestXLSX1904SerialsAreUntouched(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		serial float64
		want   string
	}{
		{name: "the first day of the system", serial: 0, want: "1904-01-01"},
		{name: "the second day", serial: 1, want: "1904-01-02"},
		{name: "a modern day", serial: 45000, want: "2027-03-16"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "mac1904.xlsx")
			dateCellWorkbook(t, path, tt.serial, 14, true)

			assert.Equal(t, tt.want, loadDateCell(t, path, "mac1904"))
		})
	}
}

// TestXLSXColoredDateFormatImportsAsISO pins that a cell's color has nothing to
// do with whether it holds a date. A custom format may start with a color, and
// two of Excel's color names hold a letter that also names an elapsed unit, so
// the same day in the same column arrived as a date in one row and as
// format-dependent text in the next.
func TestXLSXColoredDateFormatImportsAsISO(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "colored.xlsx")

	formats := []string{"mm/dd/yy", "[Red]mm/dd/yy", "[Magenta]mm/dd/yy", "[White]mm/dd/yy"}
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	require.NoError(t, f.SetSheetRow("Sheet1", "A1", &[]any{"plain", "red", "magenta", "white"}))
	for i := range formats {
		axis, err := excelize.CoordinatesToCellName(i+1, 2)
		require.NoError(t, err)
		require.NoError(t, f.SetCellValue("Sheet1", axis, 45000))
		style, err := f.NewStyle(&excelize.Style{CustomNumFmt: &formats[i]})
		require.NoError(t, err)
		require.NoError(t, f.SetCellStyle("Sheet1", axis, axis, style))
	}
	require.NoError(t, f.SaveAs(path))

	db, err := OpenContext(ctx, path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var plain, red, magenta, white string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT plain, red, magenta, white FROM colored_Sheet1`).Scan(&plain, &red, &magenta, &white))

	assert.Equal(t, "2023-03-15", plain)
	assert.Equal(t, "2023-03-15", red)
	assert.Equal(t, "2023-03-15", magenta, "a color name holding an m is a color, not an elapsed unit")
	assert.Equal(t, "2023-03-15", white, "a color name holding an h is a color, not an elapsed unit")
}

// TestXLSXDateCellsImportAsISOThroughAddReader is the same workbook handed to
// the builder as bytes rather than named by path. The two took different code
// paths, and only the path one rewrote a date cell into ISO 8601: the same file
// gave a datetime column when opened by name and format-dependent text when
// read from an io.Reader.
func TestXLSXDateCellsImportAsISOThroughAddReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	require.NoError(t, f.SetSheetRow("Sheet1", "A1", &[]any{"date"}))
	// 45000 is 2023-03-15, formatted mm-dd-yy so the shown text is not ISO.
	require.NoError(t, f.SetCellValue("Sheet1", "A2", 45000))
	style, err := f.NewStyle(&excelize.Style{NumFmt: 14})
	require.NoError(t, err)
	require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))

	var book bytes.Buffer
	require.NoError(t, f.Write(&book))

	db, err := buildForTest(

		ctx, NewBuilder().
			AddReader(bytes.NewReader(book.Bytes()), "book", FileTypeXLSX))

	require.NoError(t, err)
	sqlDB, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() { _ = sqlDB.Close() }()

	var date string
	require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT "date" FROM book_Sheet1`).Scan(&date))
	assert.Equal(t, "2023-03-15", date, "a date cell holds a day whether the workbook came by path or by reader")
}

// Two sheets of one workbook can ask for the same table. "Q1 sales" and
// "Q1.sales" sanitize to the same string, and "x(1)" and "x1" do too, because
// the sanitizer turns a space and a dot into "_" and drops the brackets.
// Loading them in turn used to leave only the last one, with the rows of the
// other gone and nothing said about it. These tests pin the refusal.
//
// Sheets differing only in case are not among the cases: Excel compares sheet
// names case-insensitively itself, so a workbook cannot hold both "Data" and
// "data" in the first place. The key below folds case anyway, because SQLite
// does when it compares table names, and the sanitizer preserves it.

// collidingWorkbook builds a workbook whose sheets are named exactly as given,
// in that order, each with one column and one row.
func collidingWorkbook(t *testing.T, path string, sheets ...string) string {
	t.Helper()

	f := excelize.NewFile()
	for _, sheet := range sheets {
		if sheet != defaultSheetName {
			if _, err := f.NewSheet(sheet); err != nil {
				t.Fatalf("new sheet %q: %v", sheet, err)
			}
		}
		if err := f.SetCellValue(sheet, "A1", "v"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue(sheet, "A2", sheet); err != nil {
			t.Fatal(err)
		}
	}
	if sheets[0] != defaultSheetName {
		if err := f.DeleteSheet(defaultSheetName); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.SaveAs(path); err != nil {
		t.Fatalf("save %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExcelSheetTableNames(t *testing.T) {
	t.Parallel()

	t.Run("maps each sheet to its own table", func(t *testing.T) {
		t.Parallel()
		got, err := ExcelSheetTableNames("book.xlsx", []string{"Sheet1", "Second", "Q3 actuals"})
		if err != nil {
			t.Fatalf("ExcelSheetTableNames: %v", err)
		}
		want := []string{"book_Sheet1", "book_Second", "book_Q3_actuals"}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("table %d = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("rejects sheets that sanitize to the same name", func(t *testing.T) {
		t.Parallel()
		_, err := ExcelSheetTableNames("book.xlsx", []string{"Q1 sales", "Q1.sales"})
		if !errors.Is(err, ErrDuplicateTable) {
			t.Fatalf("error = %v, want ErrDuplicateTable", err)
		}
		for _, want := range []string{`"Q1 sales"`, `"Q1.sales"`, "book.xlsx", "book_Q1_sales"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should name %s", err, want)
			}
		}
	})

	t.Run("rejects sheets whose punctuation is dropped into the same name", func(t *testing.T) {
		t.Parallel()
		_, err := ExcelSheetTableNames("book.xlsx", []string{"x(1)", "x1"})
		if !errors.Is(err, ErrDuplicateTable) {
			t.Fatalf("error = %v, want ErrDuplicateTable", err)
		}
		if !strings.Contains(err.Error(), "book_x1") {
			t.Errorf("error %q should name the shared table", err)
		}
	})

	t.Run("folds case the way SQLite does", func(t *testing.T) {
		t.Parallel()
		// Excel will not hold both of these, but the comparison must fold case
		// regardless: SQLite treats book_Data and book_data as one table, so a
		// workbook produced by something other than Excel cannot slip past.
		if _, err := ExcelSheetTableNames("book.xlsx", []string{"Data", "data"}); !errors.Is(err, ErrDuplicateTable) {
			t.Fatalf("error = %v, want ErrDuplicateTable", err)
		}
	})

	t.Run("keeps a sheet named after its file on the base table", func(t *testing.T) {
		t.Parallel()
		got, err := ExcelSheetTableNames("people.xlsx", []string{"people"})
		if err != nil {
			t.Fatalf("ExcelSheetTableNames: %v", err)
		}
		if got[0] != "people" {
			t.Errorf("table = %q, want %q", got[0], "people")
		}
	})
}

// TestOpenXLSXWithCollidingSheetsFails drives a real workbook through the
// loader. The unit test above pins the rule; this one pins that the loader
// actually applies it, which is what stops the silent overwrite.
func TestOpenXLSXWithCollidingSheetsFails(t *testing.T) {
	t.Parallel()
	path := collidingWorkbook(t, filepath.Join(t.TempDir(), "book.xlsx"), "Q1 sales", "Q1.sales")

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open succeeded on a workbook whose sheets share a table name")
	}
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("error = %v, want ErrDuplicateTable", err)
	}
	for _, want := range []string{"Q1 sales", "Q1.sales", "book_Q1_sales"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should name %s", err, want)
		}
	}
}

// TestOpenXLSXWithDistinctSheetsStillWorks is the guard on the guard: the check
// must not refuse an ordinary workbook.
func TestOpenXLSXWithDistinctSheetsStillWorks(t *testing.T) {
	t.Parallel()
	path := collidingWorkbook(t, filepath.Join(t.TempDir(), "book.xlsx"), "Alpha", "Beta")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for table, want := range map[string]string{"book_Alpha": "Alpha", "book_Beta": "Beta"} {
		var got string
		if err := db.QueryRowContext(context.Background(), "SELECT v FROM "+table).Scan(&got); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				t.Errorf("%s has no rows", table)
				continue
			}
			t.Fatalf("query %s: %v", table, err)
		}
		if got != want {
			t.Errorf("%s holds %q, want %q", table, got, want)
		}
	}
}

// TestOpenXLSXRefusesARowWiderThanItsHeader pins that a workbook row carrying
// more cells than the header names is refused rather than truncated.
//
// It was truncated: the extra cell was dropped with no error, no skipped-row
// count, and nothing else to say it had happened — silent data loss, and the
// opposite of what MalformedRowFill documents for a long record. A short row is
// still padded, because a workbook stores no cell for a trailing empty one.
func TestOpenXLSXRefusesARowWiderThanItsHeader(t *testing.T) {
	t.Parallel()

	t.Run("a row wider than the header", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "wide.xlsx")
		writeWorkbook(t, path, map[string][][]string{
			"wide": {{"a", "b"}, {"1", "2", "3"}},
		})

		db, err := OpenContext(context.Background(), path)
		if db != nil {
			defer func() { _ = db.Close() }()
		}
		if err == nil {
			t.Fatal("OpenContext accepted a row whose last cell has no column")
		}
		if !errors.Is(err, ErrParsing) {
			t.Errorf("error = %v, want it to match ErrParsing", err)
		}
		if !strings.Contains(err.Error(), "3 cells") {
			t.Errorf("error = %v, want it to say how many cells the row has", err)
		}
	})

	t.Run("a row shorter than the header is padded", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "short.xlsx")
		writeWorkbook(t, path, map[string][][]string{
			"short": {{"a", "b", "c"}, {"1", "2"}},
		})

		db, err := OpenContext(context.Background(), path)
		if err != nil {
			t.Fatalf("OpenContext: %v", err)
		}
		defer func() { _ = db.Close() }()

		var c string
		if err := db.QueryRowContext(context.Background(), "SELECT c FROM short").Scan(&c); err != nil {
			t.Fatalf("query: %v", err)
		}
		if c != "" {
			t.Errorf("c = %q, want the empty padding", c)
		}
	})
}

// TestExcelSheetsInFile_UnreadableWorkbook covers the two ways the sheet listing
// can fail before it has a workbook to read: no file at that path, and a file
// that is not a workbook.
func TestExcelSheetsInFile_UnreadableWorkbook(t *testing.T) {
	t.Parallel()

	t.Run("a file that is not there", func(t *testing.T) {
		t.Parallel()

		_, err := ExcelSheetsInFile(filepath.Join(t.TempDir(), "missing.xlsx"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrIOOperation)
	})

	t.Run("a file that is not a workbook", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "fake.xlsx")
		require.NoError(t, os.WriteFile(path, []byte("id,name\n1,Alice\n"), 0o600))

		_, err := ExcelSheetsInFile(path)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrParsing)
	})
}

// TestExcelSheetsInReader_NotAWorkbook is the same refusal for a workbook that
// has no path.
func TestExcelSheetsInReader_NotAWorkbook(t *testing.T) {
	t.Parallel()

	_, err := ExcelSheetsInReader(strings.NewReader("id,name\n1,Alice\n"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrParsing)
}

// TestBlankSheetCellInNumericColumnIsNull pins that a workbook's blank cell
// reaches the database as the missing number it is, the way a blank cell in a
// delimited file does. The sheet reader is a different one, so its answer is
// worth pinning next to the readers that share a path.
func TestBlankSheetCellInNumericColumnIsNull(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "sales.xlsx")
	book := excelize.NewFile()
	t.Cleanup(func() { _ = book.Close() })
	for row, cells := range [][]string{
		{"region", "amount"},
		{"north", "10"},
		{"south", ""},
		{"east", "30"},
	} {
		for col, value := range cells {
			name, err := excelize.CoordinatesToCellName(col+1, row+1)
			if err != nil {
				t.Fatalf("cell name: %v", err)
			}
			if err := book.SetCellStr("Sheet1", name, value); err != nil {
				t.Fatalf("set cell: %v", err)
			}
		}
	}
	if err := book.SaveAs(path); err != nil {
		t.Fatalf("save workbook: %v", err)
	}

	db, err := OpenContext(t.Context(), path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var missing, present int
	var largest int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT SUM(amount IS NULL), COUNT(amount), MAX(amount) FROM sales_Sheet1`).
		Scan(&missing, &present, &largest); err != nil {
		t.Fatalf("query: %v", err)
	}
	if missing != 1 || present != 2 || largest != 30 {
		t.Errorf("missing=%d present=%d max=%d, want 1, 2 and 30", missing, present, largest)
	}
}

// TestSaveKeepsACellsStorageType pins the last thing a save that changed
// nothing could take from a workbook: what the cell is, as opposed to what it
// shows.
//
// A cell holds a number, a boolean or a string, and a sheet is worth more as
// numbers than as the text of them -- a spreadsheet sums a number column and
// leaves a text one alone. Every cell here is written back as text, so what
// keeps the sheet's types is the check that a cell whose value did not change
// is not written at all. That check compared two spellings: the loaded value of
// a REAL column carries a decimal point the sheet never showed, so every whole
// number in such a column was rewritten as text, and a large one was rewritten
// as the exponent spelling of itself.
// TestAFormattedNumberColumnIsANumber covers the symptom a caller meets. A
// sheet of percentages, of thousands-separated amounts, of accounting figures
// or of fractions came back as text, so SUM answered 0, AVG answered nothing
// and ORDER BY sorted lexically -- a format says how a spreadsheet paints a
// number, and the number is what a query is about.
func TestAFormattedNumberColumnIsANumber(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for _, tt := range []struct {
		name   string
		numFmt int
		values []float64
		want   float64
	}{
		{"percentages", 9, []float64{0.5, 0.25}, 0.75},
		{"thousands separators", 3, []float64{1234.5, 1000}, 2234.5},
		{"accounting figures", 44, []float64{1234.5, 1000}, 2234.5},
		{"fractions", 12, []float64{1234.5, 0.5}, 1235},
		{"a whole-number format", 1, []float64{1234.5, 1000}, 2234.5},
		{"a scientific format", 11, []float64{1234.5, 1000}, 2234.5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "rates.xlsx")
			book := excelize.NewFile()
			require.NoError(t, book.SetCellStr(defaultSheetName, "A1", "rate"))
			style, err := book.NewStyle(&excelize.Style{NumFmt: tt.numFmt})
			require.NoError(t, err)
			for i, value := range tt.values {
				axis, err := excelize.CoordinatesToCellName(1, i+2)
				require.NoError(t, err)
				require.NoError(t, book.SetCellValue(defaultSheetName, axis, value))
				require.NoError(t, book.SetCellStyle(defaultSheetName, axis, axis, style))
			}
			require.NoError(t, book.SaveAs(path))
			require.NoError(t, book.Close())

			db, err := OpenContext(ctx, path)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()

			var kind string
			require.NoError(t, db.QueryRowContext(ctx, `SELECT typeof(rate) FROM rates_Sheet1 LIMIT 1`).Scan(&kind))
			assert.Equal(t, "real", kind, "a column of numbers is a number column")

			var total float64
			require.NoError(t, db.QueryRowContext(ctx, `SELECT SUM(rate) FROM rates_Sheet1`).Scan(&total))
			assert.InDelta(t, tt.want, total, 1e-9, "SUM over a text column answers 0")
		})
	}
}

func TestSaveKeepsACellsStorageType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// values are the cells of the sheet's one column, below its header.
		values []any
		// styled says the cells wear a number format that renders a date, which
		// is what a boolean or a string has to survive being formatted as.
		styled bool
	}{
		{name: "whole numbers", values: []any{1.0, 2.0, 3.0}},
		{name: "one decimal makes the column REAL", values: []any{1.5, 2.0, 3.0}},
		{name: "a large integer beside a decimal", values: []any{1.5, 1e15}},
		{name: "text", values: []any{"a", "b"}},
		{name: "booleans", values: []any{true, false}},
		{name: "booleans formatted as dates", values: []any{true, false}, styled: true},
		{name: "text formatted as dates", values: []any{"45001", "45002"}, styled: true},
		{name: "numbers formatted as dates", values: []any{45000.0, 45001.0}, styled: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "book.xlsx")
			book := excelize.NewFile()
			require.NoError(t, book.SetCellValue(defaultSheetName, "A1", "v"))
			for i, value := range tt.values {
				cell, err := excelize.CoordinatesToCellName(1, i+2)
				require.NoError(t, err)
				require.NoError(t, book.SetCellValue(defaultSheetName, cell, value))
			}
			if tt.styled {
				style, err := book.NewStyle(&excelize.Style{NumFmt: 14}) // m/d/yy
				require.NoError(t, err)
				last, err := excelize.CoordinatesToCellName(1, len(tt.values)+1)
				require.NoError(t, err)
				require.NoError(t, book.SetCellStyle(defaultSheetName, "A2", last, style))
			}
			require.NoError(t, book.SaveAs(path))
			require.NoError(t, book.Close())

			before := storedCells(t, path, len(tt.values))

			// Nothing is edited: the database is opened and closed, and the
			// close is what saves.
			require.NoError(t, autoSaveOverwrite(t, []string{path}))

			assert.Equal(t, before, storedCells(t, path, len(tt.values)),
				"a save that changed nothing leaves every cell as it found it")
		})
	}

	t.Run("an edited number is written as a number", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "book.xlsx")
		book := excelize.NewFile()
		require.NoError(t, book.SetCellValue(defaultSheetName, "A1", "v"))
		require.NoError(t, book.SetCellValue(defaultSheetName, "A2", 1.5))
		require.NoError(t, book.SetCellValue(defaultSheetName, "A3", 2.0))
		require.NoError(t, book.SaveAs(path))
		require.NoError(t, book.Close())

		require.NoError(t, autoSaveOverwrite(t, []string{path},
			"UPDATE book_Sheet1 SET v = 7 WHERE v = 2.0"))

		// The cell holds the number the caller set, stored as a number rather
		// than as text: a spreadsheet sums a column whose one edited cell is
		// text one row short. It is stored with the decimal point this
		// package's own spelling of a REAL carries, "7.0", which is what makes
		// a load read the column back as REAL rather than as INTEGER.
		assert.Equal(t, []string{"A2=0/1.5", "A3=0/7.0"}, storedCells(t, path, 2))
	})
}

// TestSameCellValue covers where the line between the two comparisons falls.
// Two spellings of one number are one value, so a cell holding it is untouched;
// text is compared as text, because a value this package keeps as text is not a
// number here either.
func TestSameCellValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		held  string
		value string
		want  bool
	}{
		{name: "the same text", held: "abc", value: "abc", want: true},
		{name: "a whole number a REAL column spells with a point", held: "2", value: "2.0", want: true},
		{name: "a large integer against its exponent spelling", held: "1000000000000000", value: "1e+15", want: true},
		{name: "zero against a signed zero", held: "0", value: "-0.0", want: true},
		{name: "trailing zeros of a decimal", held: "1.50", value: "1.5", want: true},
		{name: "a different number", held: "2", value: "3", want: false},
		{name: "a zero-padded code is text, and not the number", held: "007", value: "7", want: false},
		{name: "a literal past int64 is text, and not the number", held: "11040320260000000000", value: "1.104032026e+19", want: false},
		{name: "a padded number is text", held: " 5 ", value: "5", want: false},
		{name: "an empty cell against a number", held: "", value: "0", want: false},
		{name: "text against a number", held: "TRUE", value: "1", want: false},
		{name: "both empty", held: "", value: "", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, sameCellValue(tt.held, tt.value))
		})
	}
}

// storedCells is what each cell of the sheet's one column holds: its storage
// type and the value stored under it, which is what a rewrite would change even
// when the cell goes on showing the same thing.
func storedCells(t *testing.T, path string, rows int) []string {
	t.Helper()

	book, err := excelize.OpenFile(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, book.Close()) }()

	held := make([]string, 0, rows)
	for i := range rows {
		cell, err := excelize.CoordinatesToCellName(1, i+2)
		require.NoError(t, err)
		kind, err := book.GetCellType(defaultSheetName, cell)
		require.NoError(t, err)
		stored, err := book.GetCellValue(defaultSheetName, cell, excelize.Options{RawCellValue: true})
		require.NoError(t, err)
		held = append(held, fmt.Sprintf("%s=%v/%s", cell, kind, stored))
	}
	return held
}

// TestEverySheetIsLoadedByEveryRoute holds what README says twice: every sheet
// of a workbook becomes a table, and the sheet policy "applies to every source
// -- a path, a directory, an embedded filesystem, a reader, and a compressed
// workbook alike".
//
// It did not. A path loaded one table per sheet and a reader loaded the first
// sheet alone, so a workbook of four sheets came back as one table with the
// other three neither loaded nor mentioned. `ExcelSheetsInReader` on the same
// bytes reported all four, so the package would say the workbook had four and
// then load one.
func TestEverySheetIsLoadedByEveryRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	data := multiSheetWorkbook(t, "Sheet1", "Orders", "Customers")
	path := filepath.Join(t.TempDir(), "book.xlsx")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	want := []string{"book_Customers", "book_Orders", "book_Sheet1"}

	t.Run("a path", func(t *testing.T) {
		t.Parallel()

		db, err := OpenContext(ctx, path)
		require.NoError(t, err)
		defer db.Close()
		assert.Equal(t, want, loadedTables(t, db))
	})

	t.Run("a reader", func(t *testing.T) {
		t.Parallel()

		validated, err := buildForTest(
			ctx, NewBuilder().
				AddReader(bytes.NewReader(data), "book", FileTypeXLSX))

		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)
		defer db.Close()
		assert.Equal(t, want, loadedTables(t, db))
	})

	t.Run("an embedded filesystem", func(t *testing.T) {
		t.Parallel()

		validated, err := buildForTest(
			ctx, NewBuilder().
				AddFS(fstest.MapFS{"book.xlsx": &fstest.MapFile{Data: data}}))

		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)
		defer db.Close()
		assert.Equal(t, want, loadedTables(t, db))
	})

	t.Run("a compressed reader", func(t *testing.T) {
		t.Parallel()

		var squeezed bytes.Buffer
		zw := gzip.NewWriter(&squeezed)
		_, err := zw.Write(data)
		require.NoError(t, err)
		require.NoError(t, zw.Close())

		// A reader carries no path, so the codec is named through the option
		// rather than guessed from bytes.
		compressed, err := buildForTest(

			ctx, NewBuilder().
				AddReader(bytes.NewReader(squeezed.Bytes()), "book", FileTypeXLSX, WithCompression(CompressionGZ)))

		require.NoError(t, err)
		db, err := compressed.Open(ctx)
		require.NoError(t, err)
		defer db.Close()
		assert.Equal(t, want, loadedTables(t, db))
	})

	t.Run("a reader refuses two sheets that map to one table", func(t *testing.T) {
		t.Parallel()

		colliding := multiSheetWorkbook(t, "Q1 sales", "Q1.sales")
		validated, err := buildForTest(
			ctx, NewBuilder().
				AddReader(bytes.NewReader(colliding), "book", FileTypeXLSX))

		require.NoError(t, err)
		_, err = validated.Open(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDuplicateTable,
			"the check that stops one sheet loading over another belongs to the workbook, not to how its bytes arrived")
	})
}

// multiSheetWorkbook builds a workbook of the named sheets, each holding one
// column and one row.
func multiSheetWorkbook(t *testing.T, sheets ...string) []byte {
	t.Helper()

	book := excelize.NewFile()
	for i, sheet := range sheets {
		if i == 0 {
			require.NoError(t, book.SetSheetName(defaultSheetName, sheet))
		} else {
			_, err := book.NewSheet(sheet)
			require.NoError(t, err)
		}
		require.NoError(t, book.SetCellValue(sheet, "A1", "v"))
		require.NoError(t, book.SetCellValue(sheet, "A2", sheet))
	}
	var out bytes.Buffer
	require.NoError(t, book.Write(&out))
	require.NoError(t, book.Close())
	return out.Bytes()
}

// loadedTables names the tables a load made, in order.
func loadedTables(t *testing.T, db *sql.DB) []string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(),
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '_filesql_%' ORDER BY name`)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

// TestASheetWhoseFirstRowIsBlankKeepsItsRows pins that a workbook holding data
// loads it. A sheet whose first row held cells that were all empty -- which is
// what a cleared or formatted top row leaves -- was taken for an empty sheet
// and passed over, so the workbook opened as a database with no tables and no
// error at all, and every row in it was lost in silence.
func TestASheetWhoseFirstRowIsBlankKeepsItsRows(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "book.xlsx")
	f := excelize.NewFile()
	// Cells that exist and hold nothing, which is not the same as no cells.
	if err := f.SetCellStr("Sheet1", "A1", ""); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := f.SetCellStr("Sheet1", "B1", ""); err != nil {
		t.Fatalf("set: %v", err)
	}
	for row, values := range map[int][2]any{2: {"a", "b"}, 3: {"1", "2"}} {
		for col, value := range values {
			cell, err := excelize.CoordinatesToCellName(col+1, row)
			if err != nil {
				t.Fatalf("cell: %v", err)
			}
			if err := f.SetCellValue("Sheet1", cell, value); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
	}
	if err := f.SaveAs(path); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err := OpenContext(t.Context(), path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(t.Context(), "SELECT a, b FROM book_Sheet1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var got [][2]string
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, [2]string{a, b})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if want := [][2]string{{"1", "2"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
}

// TestASheetWithNothingInItStillMakesNoTable pins the rule the skip must not
// take with it.
func TestASheetWithNothingInItStillMakesNoTable(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "book.xlsx")
	f := excelize.NewFile()
	if _, err := f.NewSheet("Empty"); err != nil {
		t.Fatalf("new sheet: %v", err)
	}
	if err := f.SetCellValue("Sheet1", "A1", "a"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := f.SetCellValue("Sheet1", "A2", 1); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := f.SaveAs(path); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err := OpenContext(t.Context(), path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(t.Context(), "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if want := []string{"book_Sheet1"}; !reflect.DeepEqual(tables, want) {
		t.Errorf("tables = %q, want %q", tables, want)
	}
}
