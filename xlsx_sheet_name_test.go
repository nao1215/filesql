package filesql

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

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

// TestDumpXLSXAdaptsSheetName pins that a dump of such a table succeeds and can
// be read back.
func TestDumpXLSXAdaptsSheetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		table     string
		wantSheet string
	}{
		{name: "a long name", table: "monthly_sales_report_2026_q3_final", wantSheet: "monthly_sales_report_2026_q3_fi"},
		{name: "a punctuated name", table: "sales[2026]", wantSheet: "sales_2026_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openWithTable(t,
				`CREATE TABLE "`+tt.table+`" (x TEXT)`,
				`INSERT INTO "`+tt.table+`" VALUES ('kept')`)

			outDir := t.TempDir()
			require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatXLSX)))

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

			// The sheet name is what a reader turns into a table name, and Excel
			// cannot hold the original, so the reloaded name is the adapted one.
			require.Len(t, names, 1)
			assert.Contains(t, names[0], sanitizeTableName(tt.wantSheet))
		})
	}
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
