package filesql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

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
