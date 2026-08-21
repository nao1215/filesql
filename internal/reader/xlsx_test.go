package reader

import (
	"bytes"
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
