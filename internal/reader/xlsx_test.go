package reader

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"path"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// workbookOf writes rows onto one sheet and returns the workbook's bytes, so a
// case can state the sheet it means as a table.
func workbookOf(t *testing.T, rows [][]string) []byte {
	t.Helper()

	const sheet = "people"
	f := excelize.NewFile()
	defer func() { require.NoError(t, f.Close()) }()
	index, err := f.NewSheet(sheet)
	require.NoError(t, err)
	f.SetActiveSheet(index)
	require.NoError(t, f.DeleteSheet("Sheet1"))

	for r, row := range rows {
		for c, cell := range row {
			axis, err := excelize.CoordinatesToCellName(c+1, r+1)
			require.NoError(t, err)
			require.NoError(t, f.SetCellStr(sheet, axis, cell))
		}
	}

	var buf bytes.Buffer
	require.NoError(t, f.Write(&buf))
	return buf.Bytes()
}

// readSheet reads a workbook's only sheet whole.
func readSheet(t *testing.T, data []byte) (Result, [][]string, error) {
	t.Helper()

	var records [][]string
	result, err := Read(bytes.NewReader(data), FormatXLSX, Options{}, func(chunk *Chunk) error {
		records = append(records, chunk.Records...)
		return nil
	})
	return result, records, err
}

func TestReadXLSX_SheetShape(t *testing.T) {
	t.Parallel()

	t.Run("the first row names the columns and the rest are records", func(t *testing.T) {
		t.Parallel()

		data := workbookOf(t, [][]string{
			{"Name", "Age", "City"},
			{"Alice", "25", "Tokyo"},
			{"Bob", "30", "Osaka"},
		})

		result, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, []string{"Name", "Age", "City"}, result.Header)
		assert.Equal(t, [][]string{{"Alice", "25", "Tokyo"}, {"Bob", "30", "Osaka"}}, records)
	})

	t.Run("a sheet that is nothing but a header still names its columns", func(t *testing.T) {
		t.Parallel()

		data := workbookOf(t, [][]string{{"Name", "Age"}})

		result, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, []string{"Name", "Age"}, result.Header)
		assert.Empty(t, records)
	})

	t.Run("a short row is padded", func(t *testing.T) {
		t.Parallel()

		// A workbook stores no cell for a trailing empty one, so a row ending in
		// blanks arrives short and the padding says what it means.
		data := workbookOf(t, [][]string{
			{"Name", "Age", "City"},
			{"Alice", "25"},
		})

		_, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"Alice", "25", ""}}, records)
	})

	t.Run("a row wider than its header is refused", func(t *testing.T) {
		t.Parallel()

		// It used to be truncated: the extra cell was dropped with no error and
		// no count, which is data in a column the header does not name being
		// discarded silently.
		data := workbookOf(t, [][]string{
			{"Name", "Age", "City"},
			{"Bob", "30", "Osaka", "Extra"},
		})

		_, _, err := readSheet(t, data)

		require.Error(t, err)
		var readErr *Error
		require.ErrorAs(t, err, &readErr)
		assert.Equal(t, KindParse, readErr.Kind)
		assert.Contains(t, err.Error(), "row 2 has 4 cells where the header has 3")
	})

	t.Run("a header naming one column twice is refused", func(t *testing.T) {
		t.Parallel()

		data := workbookOf(t, [][]string{
			{"Name", "name"},
			{"Alice", "Bob"},
		})

		_, _, err := readSheet(t, data)

		require.Error(t, err)
		var readErr *Error
		require.ErrorAs(t, err, &readErr)
		assert.Equal(t, KindDuplicateColumn, readErr.Kind)
	})

	t.Run("a sheet holding no cell at all is empty", func(t *testing.T) {
		t.Parallel()

		data := workbookOf(t, nil)

		_, _, err := readSheet(t, data)

		require.Error(t, err)
		var readErr *Error
		require.ErrorAs(t, err, &readErr)
		assert.Equal(t, KindEmpty, readErr.Kind)
	})

	t.Run("a row holding no cell at all is not a record", func(t *testing.T) {
		t.Parallel()

		// A blank line is not a record in any other format this package reads,
		// and a sheet's blank row is the same thing: encoding/csv skips one, and
		// so do the LTSV and JSONL readers.
		data := workbookOf(t, [][]string{
			{"Name", "Age"},
			{"Alice", "25"},
			{},
			{"Bob", "30"},
		})

		result, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"Alice", "25"}, {"Bob", "30"}}, records)
		assert.Equal(t, 2, result.Rows)
	})

	t.Run("a row whose leading cells are empty keeps them", func(t *testing.T) {
		t.Parallel()

		// The row above holds nothing and is dropped; this one holds a value in
		// its second column and nothing in its first, which is a record whose
		// first field is empty. A workbook writes no cell for an empty value, so
		// the two shapes are told apart by whether the row reaches any column at
		// all.
		data := workbookOf(t, [][]string{
			{"Name", "Age"},
			{"", "30"},
		})

		_, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"", "30"}}, records)
	})

	t.Run("rows are handed out a chunk at a time", func(t *testing.T) {
		t.Parallel()

		data := workbookOf(t, [][]string{
			{"i"}, {"1"}, {"2"}, {"3"}, {"4"}, {"5"},
		})

		var sizes []int
		result, err := Read(bytes.NewReader(data), FormatXLSX, Options{ChunkSize: 2}, func(chunk *Chunk) error {
			sizes = append(sizes, len(chunk.Records))
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, []int{2, 2, 1}, sizes)
		assert.Equal(t, 5, result.Rows)
	})
}

// TestReadXLSXGapRowsCostNothing pins the property the gap-row rule exists for:
// a workbook whose used range reaches far down the sheet costs what it holds
// rather than what its range spans. Padding every row of the range to the
// header's width made a file of a few kilobytes allocate gigabytes and put a
// million rows of empty strings into the table, and the allocation is what
// there is to assert, since the row count alone would pass a fix that only
// dropped the rows after building them.
func TestReadXLSXGapRowsCostNothing(t *testing.T) {
	// Not parallel: the measurement is this process's total allocation, so
	// anything running beside it is counted in.

	const columns = 20
	const farRow = 200000

	f := excelize.NewFile()
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	for c := 1; c <= columns; c++ {
		axis, err := excelize.CoordinatesToCellName(c, 1)
		require.NoError(t, err)
		require.NoError(t, f.SetCellStr("Sheet1", axis, fmt.Sprintf("c%d", c)))
	}
	far, err := excelize.CoordinatesToCellName(columns, farRow)
	require.NoError(t, err)
	require.NoError(t, f.SetCellStr("Sheet1", far, "x"))

	var buf bytes.Buffer
	require.NoError(t, f.Write(&buf))
	data := buf.Bytes()
	require.Less(t, len(data), 64*1024, "the workbook itself has to stay small for the ratio to mean anything")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, records, err := readSheet(t, data)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)

	want := make([]string, columns)
	want[columns-1] = "x"
	assert.Equal(t, [][]string{want}, records)

	// Generous next to the 6 KiB the file is, and well below the 119 MiB that
	// padding 200000 rows of 20 columns cost before.
	const ceiling = 32 << 20
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > ceiling {
		t.Errorf("reading a %d-byte workbook allocated %d MiB", len(data), allocated>>20)
	}
}

// TestReadXLSXDates covers the date normalization where a load meets it: the
// rows a sheet read produces, rather than the exported helper. A workbook
// stores a date as a serial number and a number format, so what a cell looks
// like depends on how the sheet was formatted, and only one of those forms
// sorts chronologically.
//
// The sheet these build is named "people" with "Sheet1" deleted, which is also
// the case that says the sheet's part cannot be guessed from its position: the
// styles are read out of the sheet's own XML, and finding that XML follows the
// workbook's relationships.
func TestReadXLSXDates(t *testing.T) {
	t.Parallel()

	// 45000 is 2023-03-15, and 45000.5 is midday on it.
	for _, tc := range []struct {
		name   string
		numFmt int
		custom string
		value  any
		want   string
	}{
		{name: "the American built-in date format", numFmt: 14, value: 45000, want: "2023-03-15"},
		{name: "a day and abbreviated month", numFmt: 16, value: 45000, want: "2023-03-15"},
		{name: "a date and time", numFmt: 22, value: 45000.5, want: "2023-03-15 12:00:00"},
		{name: "a Japanese locale date format", numFmt: 27, value: 45000, want: "2023-03-15"},
		{name: "a custom ISO format", custom: "yyyy-mm-dd", value: 45000, want: "2023-03-15"},
		{name: "a custom format wearing a color", custom: "[Magenta]yyyy-mm-dd", value: 45000, want: "2023-03-15"},
		// A cell the workbook calls a quantity loads the number it stores,
		// whatever the format would draw; one it draws as a moment keeps the
		// drawing, since the serial behind a clock is not what it means.
		{name: "a thousands-separated number", numFmt: 3, value: 45000, want: "45000"},
		{name: "a percentage", numFmt: 10, value: 0.5, want: "0.5"},
		{name: "a time of day", numFmt: 18, value: 45000.5, want: "12:00 PM"},
		{name: "an elapsed duration", numFmt: 46, value: 1.5, want: "36:00:00"},
		// Serial 60 is a February 29, 1900 no calendar has, kept so files count
		// days the way Lotus 1-2-3 did. It names no day, so it is not converted.
		{name: "the phantom day of the 1900 system", numFmt: 14, value: 60, want: "02-29-00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := datedWorkbook(t, tc.numFmt, tc.custom, tc.value, false)

			_, records, err := readSheet(t, data)

			require.NoError(t, err)
			require.Len(t, records, 1)
			assert.Equal(t, tc.want, records[0][0])
		})
	}

	t.Run("text that merely looks like a date is untouched", func(t *testing.T) {
		t.Parallel()

		data := workbookOf(t, [][]string{{"when"}, {"2023-03-15"}, {"15-Mar-23"}})

		_, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"2023-03-15"}, {"15-Mar-23"}}, records)
	})

	t.Run("a workbook counting from 1904 converts against 1904", func(t *testing.T) {
		t.Parallel()

		// The 1904 system counts from a day 1462 days later, so the same serial
		// names a day four years and a day further on.
		data := datedWorkbook(t, 14, "", 45000, true)

		_, records, err := readSheet(t, data)

		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, "2027-03-16", records[0][0])
	})

	t.Run("a date beside values that are not dates", func(t *testing.T) {
		t.Parallel()

		const sheet = "people"
		f := excelize.NewFile()
		defer func() { require.NoError(t, f.Close()) }()
		index, err := f.NewSheet(sheet)
		require.NoError(t, err)
		f.SetActiveSheet(index)
		require.NoError(t, f.DeleteSheet("Sheet1"))

		style, err := f.NewStyle(&excelize.Style{NumFmt: 14})
		require.NoError(t, err)
		require.NoError(t, f.SetCellStr(sheet, "A1", "name"))
		require.NoError(t, f.SetCellStr(sheet, "B1", "joined"))
		require.NoError(t, f.SetCellStr(sheet, "C1", "score"))
		require.NoError(t, f.SetCellStr(sheet, "A2", "alice"))
		require.NoError(t, f.SetCellValue(sheet, "B2", 45000))
		require.NoError(t, f.SetCellStyle(sheet, "B2", "B2", style))
		require.NoError(t, f.SetCellValue(sheet, "C2", 42))
		require.NoError(t, f.SetCellStr(sheet, "A3", "bob"))
		require.NoError(t, f.SetCellValue(sheet, "B3", 45001))
		require.NoError(t, f.SetCellStyle(sheet, "B3", "B3", style))
		require.NoError(t, f.SetCellValue(sheet, "C3", 7))

		var buf bytes.Buffer
		require.NoError(t, f.Write(&buf))

		_, records, err := readSheet(t, buf.Bytes())

		require.NoError(t, err)
		assert.Equal(t, [][]string{
			{"alice", "2023-03-15", "42"},
			{"bob", "2023-03-16", "7"},
		}, records)
	})
}

// datedWorkbook returns a workbook whose one data cell holds value under the
// given number format, on a sheet that is not the one the file was created
// with.
func datedWorkbook(t *testing.T, numFmt int, custom string, value any, date1904 bool) []byte {
	t.Helper()

	const sheet = "people"
	f := excelize.NewFile()
	defer func() { require.NoError(t, f.Close()) }()
	index, err := f.NewSheet(sheet)
	require.NoError(t, err)
	f.SetActiveSheet(index)
	require.NoError(t, f.DeleteSheet("Sheet1"))
	if date1904 {
		require.NoError(t, f.SetWorkbookProps(&excelize.WorkbookPropsOptions{Date1904: &date1904}))
	}

	style := &excelize.Style{}
	if custom != "" {
		style.CustomNumFmt = &custom
	} else {
		style.NumFmt = numFmt
	}
	styleID, err := f.NewStyle(style)
	require.NoError(t, err)

	require.NoError(t, f.SetCellStr(sheet, "A1", "when"))
	require.NoError(t, f.SetCellValue(sheet, "A2", value))
	require.NoError(t, f.SetCellStyle(sheet, "A2", "A2", styleID))

	var buf bytes.Buffer
	require.NoError(t, f.Write(&buf))
	return buf.Bytes()
}

// hiddenWorkbook writes header and records onto a sheet named "data", with the
// cells of one column wearing ";;;", the format a spreadsheet user picks to hide
// a cell's content while keeping its value. A column hidden that way draws
// nothing, and the library that reads the rows drops a cell drawn as nothing.
func hiddenWorkbook(t *testing.T, header []string, records [][]any, hiddenColumn int) []byte {
	t.Helper()

	const sheet = "data"
	f := excelize.NewFile()
	defer func() { require.NoError(t, f.Close()) }()
	require.NoError(t, f.SetSheetName("Sheet1", sheet))
	hidden := ";;;"
	style, err := f.NewStyle(&excelize.Style{CustomNumFmt: &hidden})
	require.NoError(t, err)

	for c, name := range header {
		axis, err := excelize.CoordinatesToCellName(c+1, 1)
		require.NoError(t, err)
		require.NoError(t, f.SetCellStr(sheet, axis, name))
	}
	for r, record := range records {
		for c, value := range record {
			axis, err := excelize.CoordinatesToCellName(c+1, r+2)
			require.NoError(t, err)
			require.NoError(t, f.SetCellValue(sheet, axis, value))
			if c+1 == hiddenColumn || hiddenColumn == 0 {
				require.NoError(t, f.SetCellStyle(sheet, axis, axis, style))
			}
		}
	}

	var buf bytes.Buffer
	require.NoError(t, f.Write(&buf))
	return buf.Bytes()
}

// TestReadXLSXHiddenNumbersLoadWhereverTheySit holds a number whose format
// draws nothing to the rule every other number format follows: the cell loads
// as the number the file stores. The rule held in a middle column, where the
// cells after the hidden one padded it back into the row, and broke in the last
// column, where the library returned the row one cell short and the value had
// nowhere to land.
func TestReadXLSXHiddenNumbersLoadWhereverTheySit(t *testing.T) {
	t.Parallel()

	t.Run("in the last column", func(t *testing.T) {
		t.Parallel()

		data := hiddenWorkbook(t, []string{"id", "name", "secret"}, [][]any{{1, "alice", 99}, {2, "bob", 98}}, 3)

		result, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"1", "alice", "99"}, {"2", "bob", "98"}}, records)
		assert.Equal(t, infer.Integer, result.Types[2])
	})

	t.Run("in a middle column", func(t *testing.T) {
		t.Parallel()

		data := hiddenWorkbook(t, []string{"id", "secret", "name"}, [][]any{{1, 99, "alice"}, {2, 98, "bob"}}, 2)

		_, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"1", "99", "alice"}, {"2", "98", "bob"}}, records)
	})

	t.Run("in every column of a row", func(t *testing.T) {
		t.Parallel()

		// The library returns no cell at all for such a row, which is the
		// shape of a record of empties; the file says what the cells hold.
		data := hiddenWorkbook(t, []string{"a", "b"}, [][]any{{1, 2}, {3, 4}}, 0)

		_, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"1", "2"}, {"3", "4"}}, records)
	})

	t.Run("a hidden text cell still draws nothing", func(t *testing.T) {
		t.Parallel()

		// A string is text whatever its format says, and the format says to
		// draw nothing: the rule that a number loads as the number stored does
		// not reach a cell that stores no number.
		data := hiddenWorkbook(t, []string{"id", "secret"}, [][]any{{1, "shh"}}, 2)

		_, records, err := readSheet(t, data)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"1", ""}}, records)
	})
}

// rewritePart returns the workbook with one part of the archive rewritten by fn.
func rewritePart(t *testing.T, data []byte, part string, fn func(string) string) []byte {
	t.Helper()

	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	var buf bytes.Buffer
	out := zip.NewWriter(&buf)
	for _, file := range archive.File {
		body, err := file.Open()
		require.NoError(t, err)
		content, err := io.ReadAll(body)
		require.NoError(t, err)
		require.NoError(t, body.Close())
		if file.Name == part {
			content = []byte(fn(string(content)))
		}
		w, err := out.Create(file.Name)
		require.NoError(t, err)
		_, err = w.Write(content)
		require.NoError(t, err)
	}
	require.NoError(t, out.Close())
	return buf.Bytes()
}

// spellings are the ways a writer other than Excel may spell a worksheet's XML,
// each a rewrite of the sheet excelize writes: every element behind a namespace
// prefix, as the Open XML SDK writes them; attributes in single quotes, which
// XML allows; and each reference attribute on a line of its own behind a tab
// with spaces around the equals sign, as a pretty-printing writer emits.
var spellings = map[string]func(string) string{
	"as excelize writes it": func(s string) string { return s },
	"elements prefixed": func(s string) string {
		head, body, _ := strings.Cut(s, "?>")
		body = regexp.MustCompile(`<(/?)([A-Za-z])`).ReplaceAllString(body, "<${1}x:${2}")
		return head + "?>" + strings.Replace(body,
			`xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"`,
			`xmlns:x="http://schemas.openxmlformats.org/spreadsheetml/2006/main"`, 1)
	},
	"attributes single-quoted": func(s string) string {
		head, body, _ := strings.Cut(s, "?>")
		return head + "?>" + regexp.MustCompile(`="([^"]*)"`).ReplaceAllString(body, "='${1}'")
	},
	"references on their own lines": func(s string) string {
		head, body, _ := strings.Cut(s, "?>")
		return head + "?>" + strings.ReplaceAll(body, ` r="`, "\n\tr = \"")
	},
}

// TestReadXLSXSpellingsOfTheSheet holds a sheet to the same table however its
// XML is spelled. The library that opens the workbook is an XML parser and
// reads every spelling alike; the byte scan that says which rows hold a cell
// recognized only Excel's, and a spelling it did not recognize either dropped
// every record of empty cells or numbered the rows in sequence, which put a
// record where the sheet holds no row and lost the one at the end.
func TestReadXLSXSpellingsOfTheSheet(t *testing.T) {
	t.Parallel()

	// Row 3 and row 7 hold the empty string in every cell and are records; row
	// 5 is not in the sheet and is not one.
	const sheet = "data"
	f := excelize.NewFile()
	require.NoError(t, f.SetSheetName("Sheet1", sheet))
	require.NoError(t, f.SetSheetRow(sheet, "A1", &[]any{"id", "name"}))
	require.NoError(t, f.SetSheetRow(sheet, "A2", &[]any{1, "alice"}))
	require.NoError(t, f.SetSheetRow(sheet, "A3", &[]any{"", ""}))
	require.NoError(t, f.SetSheetRow(sheet, "A4", &[]any{2, "bob"}))
	require.NoError(t, f.SetSheetRow(sheet, "A6", &[]any{3, "carol"}))
	require.NoError(t, f.SetSheetRow(sheet, "A7", &[]any{"", ""}))
	var buf bytes.Buffer
	require.NoError(t, f.Write(&buf))
	require.NoError(t, f.Close())

	want := [][]string{{"1", "alice"}, {"", ""}, {"2", "bob"}, {"3", "carol"}, {"", ""}}
	for name, spell := range spellings {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			data := rewritePart(t, buf.Bytes(), "xl/worksheets/sheet1.xml", spell)

			_, records, err := readSheet(t, data)

			require.NoError(t, err)
			assert.Equal(t, want, records)
		})
	}
}

// relocateWorkbookPart returns the workbook with its main part moved from
// xl/workbook.xml to another name, and the two references to it -- the content
// types and the package's root relationships -- pointed at the new name. The
// package format lets the main part sit anywhere, and the library follows the
// root relationships to it.
func relocateWorkbookPart(t *testing.T, data []byte, to string) []byte {
	t.Helper()

	const from = "xl/workbook.xml"
	dir, base := path.Split(to)
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	var buf bytes.Buffer
	out := zip.NewWriter(&buf)
	for _, file := range archive.File {
		body, err := file.Open()
		require.NoError(t, err)
		content, err := io.ReadAll(body)
		require.NoError(t, err)
		require.NoError(t, body.Close())
		name := file.Name
		switch name {
		case from:
			name = to
		case "xl/_rels/workbook.xml.rels":
			// The targets are relative to the main part, which moves; written
			// from the root they still name the parts that stay where they are.
			name = dir + "_rels/" + base + ".rels"
			content = regexp.MustCompile(`Target="([^/"][^"]*)"`).ReplaceAll(content, []byte(`Target="/xl/${1}"`))
		case "[Content_Types].xml", "_rels/.rels":
			content = bytes.ReplaceAll(content, []byte(from), []byte(to))
		}
		w, err := out.Create(name)
		require.NoError(t, err)
		_, err = w.Write(content)
		require.NoError(t, err)
	}
	require.NoError(t, out.Close())
	return buf.Bytes()
}

// TestReadXLSXWorkbookPartElsewhere holds a workbook to the same table wherever
// its main part sits. The sheet's XML was looked for under xl/workbook.xml by
// name, and a workbook whose main part was elsewhere fell back to the reading
// that asks the library cell by cell, which loaded a hidden number as nothing
// and passed over a record of empty cells.
func TestReadXLSXWorkbookPartElsewhere(t *testing.T) {
	t.Parallel()

	data := hiddenWorkbook(t, []string{"id", "secret", "name"}, [][]any{{1, 99, "alice"}, {"", "", ""}, {2, 98, "bob"}}, 2)
	want := [][]string{{"1", "99", "alice"}, {"", "", ""}, {"2", "98", "bob"}}

	for _, to := range []string{"xl/workbook.xml", "xl/book.xml", "book/wb.xml"} {
		t.Run(to, func(t *testing.T) {
			t.Parallel()

			_, records, err := readSheet(t, relocateWorkbookPart(t, data, to))

			require.NoError(t, err)
			assert.Equal(t, want, records)
		})
	}
}
