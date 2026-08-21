package filesql

import (
	"context"
	"fmt"
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
