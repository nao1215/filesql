package parser

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
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

		result, err := Parse(f, XLSX, WithExcelSheetPolicy(ExcelSheetPolicyAll))

		require.NoError(t, err)
		assert.Greater(t, len(result.Headers), 0)
		assert.Greater(t, len(result.Records), 0)
	})

	t.Run("returns error for empty data", func(t *testing.T) {
		t.Parallel()

		reader := bytes.NewReader([]byte{})

		_, err := Parse(reader, XLSX, WithExcelSheetPolicy(ExcelSheetPolicyAll))

		assert.Error(t, err)
		// A file with no bytes is named as such rather than reported as the zip
		// library's "not a valid zip file", which says nothing about why.
		assert.Contains(t, err.Error(), "empty XLSX file")
	})

	t.Run("returns error for invalid xlsx data", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("not an xlsx file")

		_, err := Parse(reader, XLSX, WithExcelSheetPolicy(ExcelSheetPolicyAll))

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

		_, err := Parse(reader, XLSX, WithExcelSheetPolicy(ExcelSheetPolicyAll))

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

// sheetSource is a workbook as sheet selection sees it, which is the whole
// reason ExcelSheetSource is an interface: the order the names come back in and
// the failure to read a visibility are both stated here rather than coaxed out
// of a real file.
type sheetSource struct {
	names   []string
	visible map[string]bool
	err     map[string]error
}

func (s sheetSource) GetSheetList() []string { return s.names }

func (s sheetSource) GetSheetVisible(sheet string) (bool, error) {
	if err, ok := s.err[sheet]; ok {
		return false, err
	}
	return s.visible[sheet], nil
}

func TestExcelSheets(t *testing.T) {
	t.Parallel()

	t.Run("sheets come back in workbook order with their visibility", func(t *testing.T) {
		t.Parallel()

		sheets, err := ExcelSheets(sheetSource{
			names:   []string{"summary", "scratch", "data"},
			visible: map[string]bool{"summary": true, "scratch": false, "data": true},
		})

		require.NoError(t, err)
		assert.Equal(t, []ExcelSheet{
			{Name: "summary", Visible: true},
			{Name: "scratch", Visible: false},
			{Name: "data", Visible: true},
		}, sheets)
	})

	t.Run("a workbook with no sheets reports none", func(t *testing.T) {
		t.Parallel()

		sheets, err := ExcelSheets(sheetSource{})

		require.NoError(t, err)
		assert.Empty(t, sheets)
	})

	t.Run("a visibility that cannot be read is an error, not an assumption", func(t *testing.T) {
		t.Parallel()

		_, err := ExcelSheets(sheetSource{
			names: []string{"data"},
			err:   map[string]error{"data": errors.New("no such sheet")},
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), `sheet "data"`)
	})
}

func TestSelectExcelSheets(t *testing.T) {
	t.Parallel()

	book := sheetSource{
		names:   []string{"summary", "scratch", "data"},
		visible: map[string]bool{"summary": true, "scratch": false, "data": true},
	}

	t.Run("the default policy takes every sheet and skips none", func(t *testing.T) {
		t.Parallel()

		loaded, skipped, err := SelectExcelSheets(book, ExcelSheetPolicyAll)

		require.NoError(t, err)
		assert.Equal(t, []string{"summary", "scratch", "data"}, loaded)
		assert.Empty(t, skipped)
	})

	t.Run("the visible-only policy names what it left out", func(t *testing.T) {
		t.Parallel()

		loaded, skipped, err := SelectExcelSheets(book, ExcelSheetPolicyVisibleOnly)

		require.NoError(t, err)
		assert.Equal(t, []string{"summary", "data"}, loaded)
		assert.Equal(t, []string{"scratch"}, skipped)
	})

	t.Run("a workbook whose sheets are all hidden loads none under visible-only", func(t *testing.T) {
		t.Parallel()

		loaded, skipped, err := SelectExcelSheets(sheetSource{
			names:   []string{"a", "b"},
			visible: map[string]bool{"a": false, "b": false},
		}, ExcelSheetPolicyVisibleOnly)

		require.NoError(t, err)
		assert.Empty(t, loaded)
		assert.Equal(t, []string{"a", "b"}, skipped)
	})

	t.Run("a visibility that cannot be read stops the selection", func(t *testing.T) {
		t.Parallel()

		_, _, err := SelectExcelSheets(sheetSource{
			names: []string{"data"},
			err:   map[string]error{"data": errors.New("no such sheet")},
		}, ExcelSheetPolicyVisibleOnly)

		require.Error(t, err)
	})
}

func TestNormalizeXLSXDates(t *testing.T) {
	t.Parallel()

	t.Run("a cell the workbook calls a date is rewritten into ISO 8601", func(t *testing.T) {
		t.Parallel()

		const sheet = "Sheet1"
		f := excelize.NewFile()
		t.Cleanup(func() { require.NoError(t, f.Close()) })

		// A date cell beside text that looks like one: what GetRows renders for
		// the date depends on the sheet's number format, and the d-mmm-yy form
		// below does not sort chronologically.
		style, err := f.NewStyle(&excelize.Style{NumFmt: 15}) // d-mmm-yy
		require.NoError(t, err)
		require.NoError(t, f.SetCellStr(sheet, "A1", "when"))
		require.NoError(t, f.SetCellStr(sheet, "B1", "label"))
		require.NoError(t, f.SetCellValue(sheet, "A2", time.Date(2023, 3, 15, 0, 0, 0, 0, time.UTC)))
		require.NoError(t, f.SetCellStyle(sheet, "A2", "A2", style))
		require.NoError(t, f.SetCellStr(sheet, "B2", "15-Mar-23"))

		rows, err := f.GetRows(sheet)
		require.NoError(t, err)

		normalized := NormalizeXLSXDates(f, sheet, rows)

		require.Len(t, normalized, 2)
		assert.Equal(t, []string{"when", "label"}, normalized[0])
		assert.Equal(t, "2023-03-15", normalized[1][0])
		// Text that merely looks like a date is not a date, so it is untouched.
		assert.Equal(t, "15-Mar-23", normalized[1][1])
	})

	t.Run("a sheet with no rows comes back with no rows", func(t *testing.T) {
		t.Parallel()

		f := excelize.NewFile()
		t.Cleanup(func() { require.NoError(t, f.Close()) })

		assert.Empty(t, NormalizeXLSXDates(f, "Sheet1", nil))
	})

	t.Run("a sheet the workbook does not have leaves the rows alone", func(t *testing.T) {
		t.Parallel()

		f := excelize.NewFile()
		t.Cleanup(func() { require.NoError(t, f.Close()) })

		rows := [][]string{{"a"}, {"1"}}

		assert.Equal(t, rows, NormalizeXLSXDates(f, "no such sheet", rows))
	})
}
