package parser

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseXLSX(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"

	t.Run("parses sample.xlsx from testdata", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "excel", "sample.xlsx"))
		if os.IsNotExist(err) {
			t.Skip("testdata/excel/sample.xlsx not found")
		}
		require.NoError(t, err)
		defer f.Close()

		result, err := parseXLSX(f, ExcelSheetPolicyAll)

		require.NoError(t, err)
		assert.Greater(t, len(result.Headers), 0)
		assert.Greater(t, len(result.Records), 0)
	})

	t.Run("returns error for empty data", func(t *testing.T) {
		t.Parallel()

		reader := bytes.NewReader([]byte{})

		_, err := parseXLSX(reader, ExcelSheetPolicyAll)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open XLSX")
	})

	t.Run("returns error for invalid xlsx data", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("not an xlsx file")

		_, err := parseXLSX(reader, ExcelSheetPolicyAll)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open XLSX")
	})
}

func TestParseXLSX_ErrorCases(t *testing.T) {
	t.Parallel()

	t.Run("handles xlsx with no sheets", func(t *testing.T) {
		t.Parallel()
		// This test verifies error handling for empty workbook
		// Note: Creating an actual XLSX file with no sheets is complex
		// We primarily test the error path through invalid data
		reader := bytes.NewReader([]byte{0x50, 0x4B, 0x03, 0x04}) // ZIP magic bytes but not valid XLSX

		_, err := parseXLSX(reader, ExcelSheetPolicyAll)

		// Should fail during XLSX parsing
		assert.Error(t, err)
	})
}

func TestParse_XLSX_FromTestdata(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"

	t.Run("parses xlsx through Parse function", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "excel", "sample.xlsx"))
		if os.IsNotExist(err) {
			t.Skip("testdata/excel/sample.xlsx not found")
		}
		require.NoError(t, err)
		defer f.Close()

		result, err := Parse(f, XLSX)

		require.NoError(t, err)
		assert.Greater(t, len(result.Headers), 0)
		assert.Greater(t, len(result.Records), 0)
		assert.Equal(t, len(result.Headers), len(result.ColumnTypes))
	})
}

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
