package reader

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// TestIsDateNumberFormat covers what makes a custom number format a date. The
// format language spells dates and times with y, m, d, h and s, and everything
// inside quotes or brackets is literal text or a condition — a currency format
// quoting a word with a "d" in it is not a date.
func TestIsDateNumberFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format string
		want   bool
	}{
		{name: "an ISO date", format: "yyyy-mm-dd", want: true},
		{name: "a month and year", format: "mmm yyyy", want: true},
		{name: "a date and time", format: "yyyy-mm-dd hh:mm", want: true},
		// A time of day names no day, and an elapsed duration names none
		// either: "[h]:mm" of 1.5 is 36 hours, not a day and a half after the
		// epoch. Reading either as a calendar datetime invents a date.
		{name: "a time of day", format: "hh:mm:ss", want: false},
		{name: "minutes and seconds", format: "mm:ss", want: false},
		{name: "an elapsed time in brackets", format: "[h]:mm", want: false},
		{name: "an elapsed time beside a date token", format: "[h]:mm dd", want: false},
		{name: "a plain number", format: "#,##0.00", want: false},
		{name: "a currency", format: `"$"#,##0.00`, want: false},
		{name: "a quoted word holding date letters", format: `#,##0" days"`, want: false},
		{name: "a percentage", format: "0.0%", want: false},
		// A backslash, an underscore, and an asterisk each draw the character
		// after them as itself, so a date letter there is literal text.
		{name: "a backslash-escaped date letter", format: `0 \d`, want: false},
		{name: "an underscore-escaped date letter", format: "0_y", want: false},
		{name: "an asterisk-escaped date letter", format: "0*d", want: false},
		{name: "an escape before a real date token", format: `\d yyyy`, want: true},
		// A bracket holds a color, a condition, or a locale as well as an
		// elapsed unit, and only the elapsed unit says the value is not a day.
		// Excel writes an elapsed unit as one letter repeated and nothing else,
		// so "Magenta" and "White" are colors that happen to share a letter
		// with one.
		{name: "a color before a date", format: "[Red]yyyy-mm-dd", want: true},
		{name: "a color whose name holds an m", format: "[Magenta]mm/dd/yy", want: true},
		{name: "a color whose name holds an h", format: "[White]yyyy-mm-dd", want: true},
		{name: "a locale before a date", format: "[$-409]d-mmm-yy", want: true},
		{name: "a condition before a date", format: "[>0]yyyy-mm-dd;@", want: true},
		{name: "an empty bracket before a date", format: "[]yyyy", want: true},
		{name: "elapsed hours", format: "[hh]:mm:ss", want: false},
		{name: "elapsed minutes", format: "[mm]:ss", want: false},
		{name: "elapsed seconds beside a date token", format: "[ss] dd", want: false},
		{name: "a bracket of mixed letters is not an elapsed unit", format: "[hm] dd", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isDateNumberFormat(tt.format); got != tt.want {
				t.Errorf("isDateNumberFormat(%q) = %v, want %v", tt.format, got, tt.want)
			}
		})
	}
}

// TestDefinesADateStyle covers the question that is asked before any cell is,
// and the reason it is asked first: a cell is a date because of its style, so a
// workbook whose style table holds no date format has no date cells. The style
// table is a part of the file on its own and costs nothing to read, while
// asking about one cell's style makes the library build the whole sheet as
// objects -- 24 MB against 1470 MB for an 18.5 MB workbook of 200,000 rows.
// TestIsClockNumberFormat covers what makes a custom number format draw a
// moment rather than a quantity. An hour or a second token says so, and so does
// an elapsed unit in a bracket; a minute on its own does not, since "m" is a
// month as often as a minute and a format holding only months is a date.
func TestIsClockNumberFormat(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		format string
		want   bool
	}{
		{name: "a time of day", format: "hh:mm:ss", want: true},
		{name: "minutes and seconds", format: "mm:ss", want: true},
		{name: "an elapsed hour", format: "[h]:mm", want: true},
		{name: "an elapsed minute", format: "[mm]:ss", want: true},
		{name: "a date and a time", format: "yyyy-mm-dd hh:mm", want: true},
		{name: "a plain number", format: "#,##0.00", want: false},
		{name: "a percentage", format: "0.0%", want: false},
		{name: "a thousands separator", format: "#,##0", want: false},
		{name: "an accounting figure", format: `_-"$"* #,##0.00_-`, want: false},
		{name: "a date", format: "yyyy-mm-dd", want: false},
		{name: "a month and year", format: "mmm yyyy", want: false},
		{name: "a colored number", format: "[Red]#,##0", want: false},
		{name: "a quoted word holding clock letters", format: `#,##0" hours"`, want: false},
		{name: "an escaped clock letter", format: `0 \s`, want: false},
		{name: "an underscore-escaped clock letter", format: "0_h", want: false},
		{name: "an asterisk-escaped clock letter", format: "0*s", want: false},
		{name: "nothing at all", format: "", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isClockNumberFormat(tt.format), "format %q", tt.format)
		})
	}
}

func TestDefinesADateStyle(t *testing.T) {
	t.Parallel()

	t.Run("a workbook of plain values defines none", func(t *testing.T) {
		t.Parallel()

		f := excelize.NewFile()
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		require.NoError(t, f.SetCellStr("Sheet1", "A1", "name"))
		require.NoError(t, f.SetCellValue("Sheet1", "A2", 42))

		assert.False(t, definesADateStyle(f))
	})

	t.Run("a built-in date format is one", func(t *testing.T) {
		t.Parallel()

		f := excelize.NewFile()
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		style, err := f.NewStyle(&excelize.Style{NumFmt: 15}) // d-mmm-yy
		require.NoError(t, err)
		require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))

		assert.True(t, definesADateStyle(f))
	})

	t.Run("a custom date format is one", func(t *testing.T) {
		t.Parallel()

		f := excelize.NewFile()
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		custom := "yyyy-mm-dd"
		style, err := f.NewStyle(&excelize.Style{CustomNumFmt: &custom})
		require.NoError(t, err)
		require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))

		assert.True(t, definesADateStyle(f))
	})

	t.Run("a format that is not a date is not one", func(t *testing.T) {
		t.Parallel()

		f := excelize.NewFile()
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		custom := `#,##0.00" days"`
		style, err := f.NewStyle(&excelize.Style{CustomNumFmt: &custom})
		require.NoError(t, err)
		require.NoError(t, f.SetCellStyle("Sheet1", "A2", "A2", style))

		assert.False(t, definesADateStyle(f))
	})

	t.Run("a workbook with no date style leaves its rows alone", func(t *testing.T) {
		t.Parallel()

		f := excelize.NewFile()
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		require.NoError(t, f.SetCellStr("Sheet1", "A1", "when"))
		require.NoError(t, f.SetCellValue("Sheet1", "A2", 45000))

		rows := [][]string{{"when"}, {"45000"}}

		assert.Equal(t, rows, normalizeXLSXDates(f, "Sheet1", rows))
	})
}

// TestTheTwoDateReadingsAgree holds the sentence this package's date
// normalization rests on: the two ways of finding a sheet's date cells "differ
// only in how they reach a cell's style".
//
// One reads the sheet's own XML, which the loader uses because it costs what
// the date cells cost; the other asks the library cell by cell, which is what
// NormalizeXLSXDates does and what the XLSX save reads a sheet with. They have
// to answer the same, or the same workbook read for loading and read for
// saving disagrees about which cells are dates, and the save rewrites a cell
// nothing edited.
func TestTheTwoCellReadingsAgree(t *testing.T) {
	t.Parallel()

	const sheet = "Sheet1"
	f := excelize.NewFile()
	style, err := f.NewStyle(&excelize.Style{NumFmt: 14}) // m/d/yy, a builtin date format
	require.NoError(t, err)
	percent, err := f.NewStyle(&excelize.Style{NumFmt: 10}) // 0.00%
	require.NoError(t, err)
	clock, err := f.NewStyle(&excelize.Style{NumFmt: 46}) // [h]:mm:ss
	require.NoError(t, err)

	// One row of cells, every one of them wearing the same date format, each
	// stored as a different type. Only the number is a serial.
	require.NoError(t, f.SetCellValue(sheet, "A1", "number"))
	require.NoError(t, f.SetCellValue(sheet, "B1", "boolean"))
	require.NoError(t, f.SetCellValue(sheet, "C1", "string spelling a number"))
	require.NoError(t, f.SetCellValue(sheet, "D1", "text"))
	require.NoError(t, f.SetCellValue(sheet, "A2", 45000.0))
	require.NoError(t, f.SetCellBool(sheet, "B2", true))
	require.NoError(t, f.SetCellStr(sheet, "C2", "45001"))
	require.NoError(t, f.SetCellStr(sheet, "D2", "not a date"))
	require.NoError(t, f.SetCellStyle(sheet, "A2", "D2", style))

	// And a second row of the formats that are not dates: a quantity the sheet
	// redraws, a moment it draws instead of the serial, and a number nothing
	// formats.
	require.NoError(t, f.SetCellValue(sheet, "E1", "percentage"))
	require.NoError(t, f.SetCellValue(sheet, "F1", "elapsed"))
	require.NoError(t, f.SetCellValue(sheet, "G1", "plain"))
	require.NoError(t, f.SetCellValue(sheet, "E2", 0.5))
	require.NoError(t, f.SetCellValue(sheet, "F2", 1.5))
	require.NoError(t, f.SetCellValue(sheet, "G2", 2.25))
	require.NoError(t, f.SetCellStyle(sheet, "E2", "E2", percent))
	require.NoError(t, f.SetCellStyle(sheet, "F2", "F2", clock))

	var buffer bytes.Buffer
	require.NoError(t, f.Write(&buffer))
	require.NoError(t, f.Close())

	data := buffer.Bytes()
	book, err := excelize.OpenReader(bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, book.Close()) })

	rows, err := book.GetRows(sheet)
	require.NoError(t, err)

	styles, _, complete := numberFormatStyleIDs(book)
	require.True(t, complete, "the style table is short enough to walk")
	require.NotEmpty(t, styles.dates)
	values, ok := cellValuesFromXML(data, sheet, styles, false)
	require.True(t, ok, "the workbook's parts say where the sheet is")

	fromXML := copyRows(rows)
	for cell, text := range values {
		fromXML[cell.row-1][cell.col-1] = text
	}
	fromLibrary := normalizeXLSXDates(book, sheet, normalizeXLSXNumbers(book, sheet, copyRows(rows)))

	assert.Equal(t, fromXML, fromLibrary,
		"the two readings of the same sheet have to answer the same for every cell")
	assert.Equal(t, []string{"2023-03-15", "1", "45001", "not a date", "0.5", "36:00:00", "2.25"}, fromLibrary[1],
		"a boolean is the number it stores; a percentage is the number behind it; an elapsed duration is what the sheet drew")
}

// copyRows is rows with each row copied, so a normalization that writes in
// place leaves the caller's rows alone.
func copyRows(rows [][]string) [][]string {
	out := make([][]string, len(rows))
	for i, row := range rows {
		out[i] = append([]string(nil), row...)
	}
	return out
}
