package reader

import (
	"archive/zip"
	"bytes"
	"fmt"
	"regexp"
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
func TestScanSheetValues(t *testing.T) {
	t.Parallel()

	styles := numberFormatStyles{dates: map[int]bool{3: true}, clocks: map[int]bool{4: true}}

	t.Run("a styled numeric cell is a date", func(t *testing.T) {
		t.Parallel()

		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="3"><v>45000</v></c></row>
		</sheetData></worksheet>`

		dates := map[sheetCell]string{}
		require.NoError(t, scanSheetValues(strings.NewReader(sheet), styles, false, dates))
		assert.Equal(t, map[sheetCell]string{{row: 2, col: 1}: "2023-03-15"}, dates)
	})

	t.Run("a cell wearing another style keeps its number", func(t *testing.T) {
		t.Parallel()

		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="1"><v>45000</v></c></row>
		</sheetData></worksheet>`

		dates := map[sheetCell]string{}
		require.NoError(t, scanSheetValues(strings.NewReader(sheet), styles, false, dates))
		assert.Equal(t, map[sheetCell]string{{row: 2, col: 1}: "45000"}, dates)
	})

	t.Run("a cell wearing a clock style keeps what the sheet drew", func(t *testing.T) {
		t.Parallel()

		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="4"><v>0.5</v></c></row>
		</sheetData></worksheet>`

		dates := map[sheetCell]string{}
		require.NoError(t, scanSheetValues(strings.NewReader(sheet), styles, false, dates))
		assert.Empty(t, dates)
	})

	t.Run("a cell that does not store a number keeps what it is", func(t *testing.T) {
		t.Parallel()

		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="3" t="s"><v>0</v></c><c r="B2" s="3" t="str"><v>45000</v></c>` +
			`<c r="C2" s="3" t="inlineStr"><is><t>45000</t></is></c></row>
		</sheetData></worksheet>`

		dates := map[sheetCell]string{}
		require.NoError(t, scanSheetValues(strings.NewReader(sheet), styles, false, dates))
		assert.Empty(t, dates)
	})

	t.Run("a boolean loads as the number it stores", func(t *testing.T) {
		t.Parallel()

		// A boolean is stored as 1 or 0 and drawn TRUE or FALSE, whatever the
		// style says, so both a plain one and one wearing a date style load as
		// the number rather than the word.
		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="1" t="b"><v>1</v></c><c r="B2" s="0" t="b"><v>0</v></c></row>
		</sheetData></worksheet>`

		values := map[sheetCell]string{}
		require.NoError(t, scanSheetValues(strings.NewReader(sheet), styles, false, values))
		assert.Equal(t, map[sheetCell]string{{row: 2, col: 1}: "1", {row: 2, col: 2}: "0"}, values)
	})

	t.Run("rows and cells without a reference follow the ones before", func(t *testing.T) {
		t.Parallel()

		sheet := `<worksheet><sheetData>
			<row><c s="0"><v>1</v></c></row>
			<row><c s="0"><v>2</v></c><c s="3"><v>45000</v></c></row>
		</sheetData></worksheet>`

		dates := map[sheetCell]string{}
		require.NoError(t, scanSheetValues(strings.NewReader(sheet), styles, false, dates))
		assert.Equal(t, map[sheetCell]string{
			{row: 1, col: 1}: "1",
			{row: 2, col: 1}: "2",
			{row: 2, col: 2}: "2023-03-15",
		}, dates)
	})

	t.Run("a serial the 1900 system does not turn into a day is left alone", func(t *testing.T) {
		t.Parallel()

		// Serial 60 is a February 29, 1900 no calendar has, and 0 is before the
		// system starts.
		sheet := `<worksheet><sheetData>
			<row r="2"><c r="A2" s="3"><v>60</v></c><c r="B2" s="3"><v>0</v></c><c r="C2" s="3"><v>abc</v></c></row>
		</sheetData></worksheet>`

		dates := map[sheetCell]string{}
		require.NoError(t, scanSheetValues(strings.NewReader(sheet), styles, false, dates))
		assert.Empty(t, dates)
	})

	t.Run("XML that ends in the middle is an error", func(t *testing.T) {
		t.Parallel()

		dates := map[sheetCell]string{}
		err := scanSheetValues(strings.NewReader(`<worksheet><sheetData><row r="2"><c r="A2" s="3">`), styles, false, dates)
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

	styles, formats, complete := numberFormatStyleIDs(f)
	require.True(t, complete)
	require.True(t, formats)
	require.NotEmpty(t, styles.dates)

	t.Run("the sheet is found through the workbook's relationships", func(t *testing.T) {
		t.Parallel()

		dates, ok := cellValuesFromXML(data, sheet, styles, false)

		require.True(t, ok)
		assert.Equal(t, map[sheetCell]string{{row: 2, col: 1}: "2023-03-15"}, dates)
	})

	t.Run("a sheet the workbook does not have is not found", func(t *testing.T) {
		t.Parallel()

		_, ok := cellValuesFromXML(data, "no such sheet", styles, false)

		assert.False(t, ok)
	})

	t.Run("bytes that are not an archive are not found", func(t *testing.T) {
		t.Parallel()

		_, ok := cellValuesFromXML([]byte("not a zip"), sheet, styles, false)

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
		{
			name:  "a comment, a declaration and an end tag open nothing",
			sheet: `<?xml version="1.0"?><!-- <row r="9"> --><sheetData><row r="2"><c r="A2"/></row></sheetData>`,
			want:  []int{2},
		},
		{
			name:  "an attribute the parser would refuse is passed over",
			sheet: `<sheetData><row r=1><c r="A1/></row><row r="3"><c r="A3"/></row></sheetData>`,
			want:  []int{1, 3},
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

// TestScanSheetRowsOutlivesATagLongerThanItsBuffer covers a tag that runs past
// the scanner's buffer without closing: a comment of that length, or a file
// damaged in the middle of one. The unfinished tag used to be carried whole into
// the next read, which left the buffer no room for it and made the read panic.
func TestScanSheetRowsOutlivesATagLongerThanItsBuffer(t *testing.T) {
	t.Parallel()

	var sheet strings.Builder
	sheet.WriteString(`<sheetData><row r="1"><c r="A1"/></row><!-- `)
	sheet.WriteString(strings.Repeat("x", 200<<10))
	sheet.WriteString(` --><row r="3"><c r="A3"/></row></sheetData>`)

	rows := &rowSet{}
	require.NoError(t, scanSheetRows(strings.NewReader(sheet.String()), rows))
	assert.True(t, rows.has(1))
	assert.True(t, rows.has(3))
	assert.False(t, rows.has(2))
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

// TestScanSheetRowsReadsEverySpelling holds the byte scanner to what the XML
// parser beside it accepts, for the constructs it looks at. A writer other than
// Excel may put every element behind a namespace prefix, quote an attribute
// with single quotes, or break a line before an attribute and space out its
// equals sign; the library that opens the workbook reads all of those, and a
// scan that stands in for it has to as well, or the rows it answers with are
// not the rows the sheet holds.
func TestScanSheetRowsReadsEverySpelling(t *testing.T) {
	t.Parallel()

	// Rows 1, 3 and 4 hold a cell; row 2 is not in the sheet.
	want := []int{1, 3, 4}
	for _, tt := range []struct {
		name  string
		sheet string
	}{
		{
			name:  "as Excel writes it",
			sheet: `<sheetData><row r="1"><c r="A1"><v>1</v></c></row><row r="3"><c r="A3"/></row><row r="4"><c r="A4"/></row></sheetData>`,
		},
		{
			name:  "elements behind a namespace prefix",
			sheet: `<x:sheetData><x:row r="1"><x:c r="A1"><x:v>1</x:v></x:c></x:row><x:row r="3"><x:c r="A3"/></x:row><x:row r="4"><x:c r="A4"/></x:row></x:sheetData>`,
		},
		{
			name:  "attributes in single quotes",
			sheet: `<sheetData><row r='1'><c r='A1'><v>1</v></c></row><row r='3'><c r='A3'/></row><row r='4'><c r='A4'/></row></sheetData>`,
		},
		{
			name:  "a line break before the attribute and spaces around the equals sign",
			sheet: "<sheetData><row\n\tr = \"1\"><c\n\tr = \"A1\"><v>1</v></c></row><row\n\tr = \"3\"><c\n\tr = \"A3\"/></row><row\n\tr=\"4\"><c\n\tr=\"A4\"/></row></sheetData>",
		},
		{
			name:  "the reference behind other attributes",
			sheet: `<sheetData><row spans="1:1" r="1"><c s="1" t="n" r="A1"><v>1</v></c></row><row ht="15" r="3"><c t='s' r='A3'/></row><row r="4"><c r="A4"/></row></sheetData>`,
		},
		{
			name:  "a cell reference in lower case",
			sheet: `<sheetData><row><c r="a1"/></row><row><c r="a3"/></row><row><c r="a4"/></row></sheetData>`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := &rowSet{}
			require.NoError(t, scanSheetRows(strings.NewReader(tt.sheet), rows))
			var got []int
			for row := 1; row <= rows.lastRow(); row++ {
				if rows.has(row) {
					got = append(got, row)
				}
			}
			assert.Equal(t, want, got)
		})
	}
}

// TestTheTwoScansAgreeOnTheRows holds the byte scan and the XML scan of one
// sheet to the same rows. The XML scan visits every cell that stores a number,
// through encoding/xml, and the byte scan answers which rows hold a cell; over a
// sheet whose every cell stores a number the two have to name the same rows,
// whatever spelling the sheet is written in, so the byte scan cannot accept
// less than the parser does without this saying so.
func TestTheTwoScansAgreeOnTheRows(t *testing.T) {
	t.Parallel()

	body := `<row r="1"><c r="A1"><v>1</v></c><c r="B1"><v>2</v></c></row>` +
		`<row r="3"><c r="A3"><v>3</v></c></row>` +
		`<row r="6"><c r="B6"><v>6</v></c></row>` +
		`<row><c><v>7</v></c></row>`
	for name, spell := range map[string]func(string) string{
		"as Excel writes it": func(s string) string { return s },
		"elements prefixed": func(s string) string {
			return regexp.MustCompile(`<(/?)([A-Za-z])`).ReplaceAllString(s, "<${1}x:${2}")
		},
		"attributes single-quoted": func(s string) string {
			return regexp.MustCompile(`="([^"]*)"`).ReplaceAllString(s, "='${1}'")
		},
		"references on their own lines": func(s string) string {
			return strings.ReplaceAll(s, ` r="`, "\n\tr = \"")
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sheet := spell(`<worksheet><sheetData>` + body + `</sheetData></worksheet>`)

			held := &rowSet{}
			require.NoError(t, scanSheetRows(strings.NewReader(sheet), held))
			values := map[sheetCell]string{}
			require.NoError(t, scanSheetValues(strings.NewReader(sheet), numberFormatStyles{}, false, values))

			fromValues := map[int]bool{}
			for cell := range values {
				fromValues[cell.row] = true
			}
			fromBytes := map[int]bool{}
			for row := 1; row <= held.lastRow(); row++ {
				if held.has(row) {
					fromBytes[row] = true
				}
			}
			assert.Equal(t, map[int]bool{1: true, 3: true, 6: true, 7: true}, fromValues)
			assert.Equal(t, fromValues, fromBytes)
		})
	}
}

// archiveOf returns a zip holding the given parts, which is enough of a
// workbook for the part lookup to be asked where a sheet is.
func archiveOf(t *testing.T, parts map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	out := zip.NewWriter(&buf)
	for name, body := range parts {
		w, err := out.Create(name)
		require.NoError(t, err)
		_, err = w.Write([]byte(body))
		require.NoError(t, err)
	}
	require.NoError(t, out.Close())
	return buf.Bytes()
}

// TestSheetPartFollowsThePackage covers finding a sheet's part through the
// package's own bookkeeping: the root relationships say where the main part
// is, the main part's relationships say where each sheet is, and a sheet's
// target is written relative to the main part unless it begins at the root.
// The main part used to be looked for at xl/workbook.xml by name.
func TestSheetPartFollowsThePackage(t *testing.T) {
	t.Parallel()

	rootRels := func(target string) string {
		return `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
			`<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="` + target + `"/>` +
			`</Relationships>`
	}
	workbook := `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">` +
		`<sheets><sheet name="data" sheetId="1" r:id="rId1"/></sheets></workbook>`
	workbookRels := func(target string) string {
		return `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
			`<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="` + target + `"/>` +
			`</Relationships>`
	}

	for _, tt := range []struct {
		name  string
		parts map[string]string
		want  string
	}{
		{
			name: "the main part where Excel puts it",
			parts: map[string]string{
				"_rels/.rels":                rootRels("xl/workbook.xml"),
				"xl/workbook.xml":            workbook,
				"xl/_rels/workbook.xml.rels": workbookRels("worksheets/sheet1.xml"),
				"xl/worksheets/sheet1.xml":   "<worksheet/>",
			},
			want: "xl/worksheets/sheet1.xml",
		},
		{
			name: "the main part under another name",
			parts: map[string]string{
				"_rels/.rels":              rootRels("/xl/book.xml"),
				"xl/book.xml":              workbook,
				"xl/_rels/book.xml.rels":   workbookRels("worksheets/sheet1.xml"),
				"xl/worksheets/sheet1.xml": "<worksheet/>",
			},
			want: "xl/worksheets/sheet1.xml",
		},
		{
			name: "the main part in another directory",
			parts: map[string]string{
				"_rels/.rels":            rootRels("book/wb.xml"),
				"book/wb.xml":            workbook,
				"book/_rels/wb.xml.rels": workbookRels("sheets/s.xml"),
				"book/sheets/s.xml":      "<worksheet/>",
			},
			want: "book/sheets/s.xml",
		},
		{
			name: "the main part at the root",
			parts: map[string]string{
				"_rels/.rels":       rootRels("wb.xml"),
				"wb.xml":            workbook,
				"_rels/wb.xml.rels": workbookRels("sheets/s.xml"),
				"sheets/s.xml":      "<worksheet/>",
			},
			want: "sheets/s.xml",
		},
		{
			name: "a sheet target written from the root",
			parts: map[string]string{
				"_rels/.rels":                rootRels("xl/workbook.xml"),
				"xl/workbook.xml":            workbook,
				"xl/_rels/workbook.xml.rels": workbookRels("/xl/worksheets/sheet1.xml"),
				"xl/worksheets/sheet1.xml":   "<worksheet/>",
			},
			want: "xl/worksheets/sheet1.xml",
		},
		{
			name: "a sheet target that begins with a dot",
			parts: map[string]string{
				"_rels/.rels":                rootRels("xl/workbook.xml"),
				"xl/workbook.xml":            workbook,
				"xl/_rels/workbook.xml.rels": workbookRels("./worksheets/sheet1.xml"),
				"xl/worksheets/sheet1.xml":   "<worksheet/>",
			},
			want: "xl/worksheets/sheet1.xml",
		},
		{
			name: "no root relationships at all falls back to where Excel puts it",
			parts: map[string]string{
				"xl/workbook.xml":            workbook,
				"xl/_rels/workbook.xml.rels": workbookRels("worksheets/sheet1.xml"),
				"xl/worksheets/sheet1.xml":   "<worksheet/>",
			},
			want: "xl/worksheets/sheet1.xml",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data := archiveOf(t, tt.parts)
			archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
			require.NoError(t, err)

			part, ok := sheetPart(archive, "data")

			require.True(t, ok, "the sheet is found")
			assert.Equal(t, tt.want, part.Name)
		})
	}

	t.Run("root relationships that are not XML fall back to where Excel puts it", func(t *testing.T) {
		t.Parallel()

		data := archiveOf(t, map[string]string{
			"_rels/.rels":                "not xml",
			"xl/workbook.xml":            workbook,
			"xl/_rels/workbook.xml.rels": workbookRels("worksheets/sheet1.xml"),
			"xl/worksheets/sheet1.xml":   "<worksheet/>",
		})
		archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err)

		part, ok := sheetPart(archive, "data")

		require.True(t, ok)
		assert.Equal(t, "xl/worksheets/sheet1.xml", part.Name)
	})

	t.Run("a main part the root relationships name but the archive lacks is not found", func(t *testing.T) {
		t.Parallel()

		data := archiveOf(t, map[string]string{
			"_rels/.rels":              rootRels("xl/book.xml"),
			"xl/workbook.xml":          workbook,
			"xl/worksheets/sheet1.xml": "<worksheet/>",
		})
		archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err)

		_, ok := sheetPart(archive, "data")

		assert.False(t, ok)
	})
}
