package reader

import (
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

		assert.Equal(t, rows, NormalizeXLSXDates(f, "Sheet1", rows))
	})
}
