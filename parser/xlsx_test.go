package parser

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// TestParseTakesOneSheetFromAWorkbook pins what the documentation promises: a
// TableData is one table, so a workbook contributes the first sheet the policy
// admits and the rest are not read.
func TestParseTakesOneSheetFromAWorkbook(t *testing.T) {
	t.Parallel()

	// The hidden sheet is stored first on purpose, so the two policies choose
	// differently.
	book := excelize.NewFile()
	require.NoError(t, book.SetSheetName("Sheet1", "Buried"))
	require.NoError(t, book.SetCellValue("Buried", "A1", "v"))
	require.NoError(t, book.SetCellValue("Buried", "A2", "Buried"))
	for _, name := range []string{"Shown", "Third"} {
		_, err := book.NewSheet(name)
		require.NoError(t, err)
		require.NoError(t, book.SetCellValue(name, "A1", "v"))
		require.NoError(t, book.SetCellValue(name, "A2", name))
	}
	// Hidden last, and not while it is the active sheet: a workbook cannot hide
	// its only sheet or the one it opens on, and excelize declines either
	// without saying so.
	shown, err := book.GetSheetIndex("Shown")
	require.NoError(t, err)
	book.SetActiveSheet(shown)
	require.NoError(t, book.SetSheetVisible("Buried", false))
	var out bytes.Buffer
	require.NoError(t, book.Write(&out))
	require.NoError(t, book.Close())

	tests := []struct {
		name   string
		option []ParseOption
		want   string
	}{
		{name: "the default takes the first stored sheet", want: "Buried"},
		{
			name:   "the visible-only policy takes the first shown sheet",
			option: []ParseOption{WithExcelSheetPolicy(ExcelSheetPolicyVisibleOnly)},
			want:   "Shown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := Parse(bytes.NewReader(out.Bytes()), XLSX, tt.option...)
			require.NoError(t, err)
			assert.Equal(t, [][]string{{tt.want}}, result.Records,
				"one sheet, and the rest are not read")
		})
	}
}
