package reader

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// numericWorkbook returns a workbook whose one data cell holds value under the
// given number format, so a case can say what the file stores and what a sheet
// would draw.
func numericWorkbook(t *testing.T, numFmt int, value any) []byte {
	t.Helper()

	const sheet = "people"
	f := excelize.NewFile()
	defer func() { require.NoError(t, f.Close()) }()
	index, err := f.NewSheet(sheet)
	require.NoError(t, err)
	f.SetActiveSheet(index)
	require.NoError(t, f.DeleteSheet("Sheet1"))

	require.NoError(t, f.SetCellStr(sheet, "A1", "amount"))
	require.NoError(t, f.SetCellValue(sheet, "A2", value))
	if numFmt != 0 {
		styleID, err := f.NewStyle(&excelize.Style{NumFmt: numFmt})
		require.NoError(t, err)
		require.NoError(t, f.SetCellStyle(sheet, "A2", "A2", styleID))
	}

	var buf bytes.Buffer
	require.NoError(t, f.Write(&buf))
	return buf.Bytes()
}

// TestReadXLSXKeepsTheStoredNumber holds a numeric cell to the number the file
// stores. A sheet draws a number the way its format says, and the drawing
// carries fifteen significant digits, rounds to the decimals the format asks
// for, and can be written in exponent form -- none of which is the value.
func TestReadXLSXKeepsTheStoredNumber(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		numFmt int
		value  any
		want   string
	}{
		{"an integer past what fifteen digits spell", 0, int64(1234567890123456789), "1234567890123456789"},
		{"the largest int64", 0, int64(9223372036854775807), "9223372036854775807"},
		{"the smallest int64", 0, int64(-9223372036854775808), "-9223372036854775808"},
		{"an integer a double holds exactly", 0, int64(9007199254740992), "9007199254740992"},
		{"a double of seventeen digits", 0, 3.141592653589793, "3.141592653589793"},
		{"a whole number format rounds the drawing", 1, 1234.5, "1234.5"},
		{"a two-decimal format pads the drawing", 2, 1234.5, "1234.5"},
		{"a scientific format shortens the drawing", 11, 1234.5, "1234.5"},
		{"a small integer is drawn as it is stored", 0, int64(42), "42"},
		{"a small double is drawn as it is stored", 0, 1.5, "1.5"},
		// A format that draws the number as something other than digits is
		// presentation too: the cell holds one number whatever the sheet paints.
		{"a percentage", 9, 0.5, "0.5"},
		{"a percentage with decimals", 10, 0.5, "0.5"},
		{"a thousands separator", 3, 1234.5, "1234.5"},
		{"a thousands separator with decimals", 4, 1234.5, "1234.5"},
		{"a currency", 7, 1234.5, "1234.5"},
		{"an accounting figure", 44, 1234.5, "1234.5"},
		{"a fraction", 12, 1234.5, "1234.5"},
		{"a fraction of two digits", 13, 1234.5, "1234.5"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, records, err := readSheet(t, numericWorkbook(t, tt.numFmt, tt.value))
			require.NoError(t, err)
			require.Len(t, records, 1)
			assert.Equal(t, []string{tt.want}, records[0])
		})
	}
}

// TestReadXLSXKeepsWhatIsNotAStoredNumber pins the other half of the rule. A
// cell that does not store a number, and a number whose format draws a moment
// rather than a quantity, keep what the sheet says: a boolean is TRUE, a time
// of day is a clock reading, and an elapsed duration is the hours it counts,
// none of which the serial behind it means on its own.
func TestReadXLSXKeepsWhatIsNotAStoredNumber(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		numFmt int
		value  any
		want   string
	}{
		{"a boolean", 0, true, "1"},
		{"a boolean under a number format", 9, false, "0"},
		{"a time of day", 21, 0.5, "12:00:00"},
		{"an elapsed duration", 46, 1.5, "36:00:00"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, records, err := readSheet(t, numericWorkbook(t, tt.numFmt, tt.value))
			require.NoError(t, err)
			require.Len(t, records, 1)
			assert.Equal(t, []string{tt.want}, records[0])
		})
	}

	t.Run("a date is still rewritten into ISO 8601", func(t *testing.T) {
		t.Parallel()

		_, records, err := readSheet(t, numericWorkbook(t, 22, 45293.5))
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, []string{"2024-01-02 12:00:00"}, records[0])
	})

	t.Run("a text cell holding digits keeps them", func(t *testing.T) {
		t.Parallel()

		_, records, err := readSheet(t, workbookOf(t, [][]string{{"code"}, {"007"}}))
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, []string{"007"}, records[0])
	})
}

// TestIsPlainNumber pins what counts as a drawing worth replacing. The check
// stands between a stored number and a text cell that merely reads like one, so
// the forms Go's own parser takes and a sheet never draws have to be refused.
func TestIsPlainNumber(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		text string
		want bool
	}{
		{"0", true},
		{"42", true},
		{"-42", true},
		{"+42", true},
		{"1234.5", true},
		{"-0.5", true},
		{".5", true},
		{"5.", true},
		{"1e3", true},
		{"1E+3", true},
		{"1.5e-3", true},
		{"", false},
		{"+", false},
		{"-", false},
		{".", false},
		{"1.2.3", false},
		{"1e", false},
		{"1e+", false},
		{"1e2e3", false},
		{"e3", false},
		{"Inf", false},
		{"NaN", false},
		{"0x1p4", false},
		{"1_000", false},
		{"1,234", false},
		{"50%", false},
		{" 42", false},
		{"42 ", false},
		{"1234 1/2", false},
		{"TRUE", false},
		{"#DIV/0!", false},
		{"2024-01-02", false},
	} {
		t.Run(tt.text, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isPlainNumber(tt.text))
		})
	}
}
