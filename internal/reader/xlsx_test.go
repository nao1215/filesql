package reader

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

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
	t.Parallel()

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
