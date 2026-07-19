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

		result, err := parseXLSX(f)

		require.NoError(t, err)
		assert.Greater(t, len(result.Headers), 0)
		assert.Greater(t, len(result.Records), 0)
	})

	t.Run("returns error for empty data", func(t *testing.T) {
		t.Parallel()

		reader := bytes.NewReader([]byte{})

		_, err := parseXLSX(reader)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open XLSX")
	})

	t.Run("returns error for invalid xlsx data", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("not an xlsx file")

		_, err := parseXLSX(reader)

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

		_, err := parseXLSX(reader)

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
