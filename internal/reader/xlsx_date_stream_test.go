package reader

import (
	"bytes"
	"fmt"
	"slices"
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

// TestScanSheetRowsReadsTheRowsThatHoldCells covers the byte scanner that says
// which rows of a sheet hold a cell. The question cannot be asked of the library:
// it drops a cell whose value is the empty string, so a row of empty cells and a
// row that is not in the file both come back with no cells, and the two mean
// opposite things.
func TestScanSheetRowsReadsTheRowsThatHoldCells(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		sheet string
		want  []int
	}{
		{
			name:  "a row element carries its number",
			sheet: `<sheetData><row r="1"><c r="A1"><v>1</v></c></row><row r="3"><c r="A3"/></row></sheetData>`,
			want:  []int{1, 3},
		},
		{
			name:  "a row without a number follows the one before",
			sheet: `<sheetData><row><c/></row><row><c/></row></sheetData>`,
			want:  []int{1, 2},
		},
		{
			name:  "a cell reference says which row it is in",
			sheet: `<sheetData><row><c r="A7"/></row></sheetData>`,
			want:  []int{7},
		},
		{
			name:  "a row holding no cell is not held",
			sheet: `<sheetData><row r="1"><c r="A1"/></row><row r="2"/><row r="3"><c r="A3"/></row></sheetData>`,
			want:  []int{1, 3},
		},
		{
			name:  "a tag whose name begins with one of these is not one of these",
			sheet: `<rowBreaks count="1"/><sheetData><row r="2"><c r="A2"/></row></sheetData><cols><col min="1"/></cols>`,
			want:  []int{2},
		},
		{
			name:  "a reference that is not one is passed over",
			sheet: `<sheetData><row r="zzz"><c r="!!"/></row></sheetData>`,
			want:  []int{1},
		},
		{
			name:  "a sheet with no rows holds none",
			sheet: `<sheetData/>`,
			want:  nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := &rowSet{}
			if err := scanSheetRows(strings.NewReader(tt.sheet), rows); err != nil {
				t.Fatalf("scanSheetRows: %v", err)
			}
			var got []int
			for row := 1; row <= rows.lastRow(); row++ {
				if rows.has(row) {
					got = append(got, row)
				}
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("rows holding cells = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestScanSheetRowsCrossesTheWindow covers a sheet longer than the scanner's
// buffer, where a tag lands on the boundary between two reads. Reading it as two
// halves would lose the row it names.
func TestScanSheetRowsCrossesTheWindow(t *testing.T) {
	t.Parallel()

	// Every cell carries a comment long enough that the rows land at every
	// offset within the buffer rather than at a repeating one.
	var sheet strings.Builder
	sheet.WriteString("<sheetData>")
	const rows = 4000
	for row := 1; row <= rows; row++ {
		fmt.Fprintf(&sheet, `<row r="%d"><c r="A%d" t="s"><v>%d</v></c></row>`, row, row, row%7)
	}
	sheet.WriteString("</sheetData>")

	held := &rowSet{}
	if err := scanSheetRows(strings.NewReader(sheet.String()), held); err != nil {
		t.Fatalf("scanSheetRows: %v", err)
	}
	if held.lastRow() != rows {
		t.Fatalf("last row = %d, want %d", held.lastRow(), rows)
	}
	for row := 1; row <= rows; row++ {
		if !held.has(row) {
			t.Fatalf("row %d is not held, and every row here holds a cell", row)
		}
	}
	if held.has(rows + 1) {
		t.Error("a row past the last one is held")
	}
}

// TestRowSetIsEmptyWhenNil covers the set a workbook whose bytes are not there
// answers with, which is the reading that came before it could be asked.
func TestRowSetIsEmptyWhenNil(t *testing.T) {
	t.Parallel()

	var rows *rowSet
	if rows.has(1) || rows.has(0) || rows.lastRow() != 0 {
		t.Error("a nil set holds a row")
	}
	held := &rowSet{}
	held.add(0)
	held.add(-1)
	if held.lastRow() != 0 {
		t.Error("a row number below one was recorded")
	}
}
