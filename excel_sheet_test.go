package filesql

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

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
		want   string
	}{
		{
			name:   "the all policy takes the first stored sheet even when it is hidden",
			policy: ExcelSheetPolicyAll,
			want:   "Buried",
		},
		{
			name:   "the visible-only policy takes the first shown sheet",
			policy: ExcelSheetPolicyVisibleOnly,
			want:   "Shown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The hidden sheet is stored first on purpose: the two policies then
			// disagree about which sheet the reader load takes.
			path := visibilityWorkbook(t, filepath.Join(t.TempDir(), "book.xlsx"),
				sheetSpec{"Buried", sheetHidden},
				sheetSpec{"Shown", sheetVisible},
			)
			data, err := os.ReadFile(path) //nolint:gosec // a workbook this test just wrote
			if err != nil {
				t.Fatal(err)
			}

			builder, err := NewBuilder().
				AddReader(bytes.NewReader(data), "book", FileTypeXLSX).
				WithExcelSheetPolicy(tt.policy).
				Build(context.Background())
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			db, err := builder.Open(context.Background())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			var got string
			if err := db.QueryRowContext(context.Background(), "SELECT v FROM book").Scan(&got); err != nil {
				t.Fatalf("query book: %v", err)
			}
			if got != tt.want {
				t.Errorf("the reader load took sheet %q, want %q", got, tt.want)
			}
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

	builder, err := NewBuilder().
		AddFS(os.DirFS(dir)).
		WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	db, err := builder.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var got string
	if err := db.QueryRowContext(context.Background(), "SELECT v FROM book").Scan(&got); err != nil {
		t.Fatalf("query book: %v", err)
	}
	if got != "Shown" {
		t.Errorf("the filesystem load took sheet %q, want %q", got, "Shown")
	}
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
	builder, err := NewBuilder().
		AddPaths(paths...).
		WithExcelSheetPolicy(policy).
		Build(ctx)
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
	builder, err := NewBuilder().
		AddPath(path).
		WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly).
		Build(ctx)
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

			validated, err := NewBuilder().AddPath(path).
				WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly).
				EnableAutoSave("").Build(ctx)
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

	validated, err := NewBuilder().AddPath(path).EnableAutoSave("").Build(ctx)
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
