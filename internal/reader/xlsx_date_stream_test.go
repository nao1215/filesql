package reader

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// TestCellRef covers the reference a sheet gives each cell, which is what says
// where a date belongs among the rows a read produced.
func TestCellRef(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		ref  string
		col  int
		row  int
		okay bool
	}{
		{ref: "A1", col: 1, row: 1, okay: true},
		{ref: "B2", col: 2, row: 2, okay: true},
		{ref: "Z9", col: 26, row: 9, okay: true},
		{ref: "AA1", col: 27, row: 1, okay: true},
		{ref: "AB12", col: 28, row: 12, okay: true},
		{ref: "XFD1048576", col: 16384, row: 1048576, okay: true},
		// Neither half may be missing, and nothing else belongs in one.
		{ref: "A", okay: false},
		{ref: "1", okay: false},
		{ref: "", okay: false},
		{ref: "A1B", okay: false},
		{ref: "A-1", okay: false},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			t.Parallel()

			col, row, ok := cellRef(tc.ref)

			assert.Equal(t, tc.okay, ok)
			if tc.okay {
				assert.Equal(t, tc.col, col)
				assert.Equal(t, tc.row, row)
			}
		})
	}
}

// TestScanSheetDates covers the walk over a sheet's XML on its own, including
// the shapes a writer other than excelize produces: a row or a cell without its
// reference, which follows the one before it.
func TestScanSheetDates(t *testing.T) {
	t.Parallel()

	dateStyles := map[int]bool{3: true}

	t.Run("a styled numeric cell is a date", func(t *testing.T) {
		t.Parallel()

		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="3"><v>45000</v></c></row>
		</sheetData></worksheet>`

		dates := map[datedCell]string{}
		require.NoError(t, scanSheetDates(strings.NewReader(sheet), dateStyles, false, dates))
		assert.Equal(t, map[datedCell]string{{row: 2, col: 1}: "2023-03-15"}, dates)
	})

	t.Run("a cell wearing another style is not", func(t *testing.T) {
		t.Parallel()

		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="1"><v>45000</v></c></row>
		</sheetData></worksheet>`

		dates := map[datedCell]string{}
		require.NoError(t, scanSheetDates(strings.NewReader(sheet), dateStyles, false, dates))
		assert.Empty(t, dates)
	})

	t.Run("a shared or inline string is text whatever its style", func(t *testing.T) {
		t.Parallel()

		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="3" t="s"><v>0</v></c><c r="B2" s="3" t="str"><v>45000</v></c></row>
		</sheetData></worksheet>`

		dates := map[datedCell]string{}
		require.NoError(t, scanSheetDates(strings.NewReader(sheet), dateStyles, false, dates))
		assert.Empty(t, dates)
	})

	t.Run("rows and cells without a reference follow the ones before", func(t *testing.T) {
		t.Parallel()

		sheet := `<worksheet><sheetData>
			<row><c s="0"><v>1</v></c></row>
			<row><c s="0"><v>2</v></c><c s="3"><v>45000</v></c></row>
		</sheetData></worksheet>`

		dates := map[datedCell]string{}
		require.NoError(t, scanSheetDates(strings.NewReader(sheet), dateStyles, false, dates))
		assert.Equal(t, map[datedCell]string{{row: 2, col: 2}: "2023-03-15"}, dates)
	})

	t.Run("a serial the 1900 system does not turn into a day is left alone", func(t *testing.T) {
		t.Parallel()

		// Serial 60 is a February 29, 1900 no calendar has, and 0 is before the
		// system starts.
		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="3"><v>60</v></c><c r="B2" s="3"><v>0</v></c><c r="C2" s="3"><v>abc</v></c></row>
		</sheetData></worksheet>`

		dates := map[datedCell]string{}
		require.NoError(t, scanSheetDates(strings.NewReader(sheet), dateStyles, false, dates))
		assert.Empty(t, dates)
	})

	t.Run("XML that ends in the middle is an error", func(t *testing.T) {
		t.Parallel()

		dates := map[datedCell]string{}
		err := scanSheetDates(strings.NewReader(`<worksheet><sheetData><row r="2"><c r="A2" s="3">`), dateStyles, false, dates)
		require.Error(t, err)
	})
}

// TestDateCellsFromXML covers finding the sheet in the archive, which follows
// the workbook's relationships: a workbook whose first sheet was deleted does
// not name its parts in the order its sheets appear.
func TestDateCellsFromXML(t *testing.T) {
	t.Parallel()

	const sheet = "people"
	f := excelize.NewFile()
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	index, err := f.NewSheet(sheet)
	require.NoError(t, err)
	f.SetActiveSheet(index)
	require.NoError(t, f.DeleteSheet("Sheet1"))

	style, err := f.NewStyle(&excelize.Style{NumFmt: 14})
	require.NoError(t, err)
	require.NoError(t, f.SetCellStr(sheet, "A1", "when"))
	require.NoError(t, f.SetCellValue(sheet, "A2", 45000))
	require.NoError(t, f.SetCellStyle(sheet, "A2", "A2", style))

	var buf bytes.Buffer
	require.NoError(t, f.Write(&buf))
	data := buf.Bytes()

	dated, complete := dateStyleIDs(f)
	require.True(t, complete)
	require.NotEmpty(t, dated)

	t.Run("the sheet is found through the workbook's relationships", func(t *testing.T) {
		t.Parallel()

		dates, ok := dateCellsFromXML(data, sheet, dated, false)

		require.True(t, ok)
		assert.Equal(t, map[datedCell]string{{row: 2, col: 1}: "2023-03-15"}, dates)
	})

	t.Run("a sheet the workbook does not have is not found", func(t *testing.T) {
		t.Parallel()

		_, ok := dateCellsFromXML(data, "no such sheet", dated, false)

		assert.False(t, ok)
	})

	t.Run("bytes that are not an archive are not found", func(t *testing.T) {
		t.Parallel()

		_, ok := dateCellsFromXML([]byte("not a zip"), sheet, dated, false)

		assert.False(t, ok)
	})
}
