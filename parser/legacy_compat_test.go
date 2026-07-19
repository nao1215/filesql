package parser_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	legacyparser "github.com/nao1215/fileparser"
	"github.com/nao1215/filesql/parser"
)

type comparableTableData struct {
	Headers     []string
	Records     [][]string
	ColumnTypes []string
}

func normalizeCurrentTableData(data *parser.TableData) comparableTableData {
	columnTypes := make([]string, len(data.ColumnTypes))
	for i, ct := range data.ColumnTypes {
		columnTypes[i] = ct.String()
	}

	return comparableTableData{
		Headers:     data.Headers,
		Records:     data.Records,
		ColumnTypes: columnTypes,
	}
}

func normalizeLegacyTableData(data *legacyparser.TableData) comparableTableData {
	columnTypes := make([]string, len(data.ColumnTypes))
	for i, ct := range data.ColumnTypes {
		columnTypes[i] = ct.String()
	}

	return comparableTableData{
		Headers:     data.Headers,
		Records:     data.Records,
		ColumnTypes: columnTypes,
	}
}

func readLegacyCompatFixture(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path) //nolint:gosec // fixed in-repo fixture path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	return data
}

func compareLegacyAndCurrentParseResult(
	t *testing.T,
	input []byte,
	currentType parser.FileType,
	legacyType legacyparser.FileType,
) {
	t.Helper()

	currentResult, currentErr := parser.Parse(bytes.NewReader(input), currentType)
	legacyResult, legacyErr := legacyparser.Parse(bytes.NewReader(input), legacyType)

	if (currentErr != nil) != (legacyErr != nil) {
		t.Fatalf("error presence mismatch: current=%v legacy=%v", currentErr, legacyErr)
	}
	if currentErr != nil {
		if currentErr.Error() != legacyErr.Error() {
			t.Fatalf("error mismatch:\ncurrent: %s\nlegacy:  %s", currentErr.Error(), legacyErr.Error())
		}
		return
	}

	if diff := cmp.Diff(
		normalizeLegacyTableData(legacyResult),
		normalizeCurrentTableData(currentResult),
	); diff != "" {
		t.Fatalf("parse result mismatch (-legacy +current):\n%s", diff)
	}
}

func TestLegacyCompatibility_ParseRepresentativeFormats(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		path        string
		currentType parser.FileType
		legacyType  legacyparser.FileType
	}{
		{
			name:        "csv",
			path:        filepath.Join("testdata", "sample.csv"),
			currentType: parser.CSV,
			legacyType:  legacyparser.CSV,
		},
		{
			name:        "json",
			path:        filepath.Join("testdata", "sample.json"),
			currentType: parser.JSON,
			legacyType:  legacyparser.JSON,
		},
		{
			name:        "jsonl",
			path:        filepath.Join("testdata", "sample.jsonl"),
			currentType: parser.JSONL,
			legacyType:  legacyparser.JSONL,
		},
		{
			name:        "xlsx",
			path:        filepath.Join("testdata", "excel", "sample.xlsx"),
			currentType: parser.XLSX,
			legacyType:  legacyparser.XLSX,
		},
		{
			name:        "parquet",
			path:        filepath.Join("testdata", "products.parquet"),
			currentType: parser.Parquet,
			legacyType:  legacyparser.Parquet,
		},
		{
			name:        "compressed csv gzip",
			path:        filepath.Join("testdata", "sample.csv.gz"),
			currentType: parser.CSVGZ,
			legacyType:  legacyparser.CSVGZ,
		},
		{
			name:        "compressed json zstd",
			path:        filepath.Join("testdata", "sample.json.zst"),
			currentType: parser.JSONZSTD,
			legacyType:  legacyparser.JSONZSTD,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			input := readLegacyCompatFixture(t, tc.path)
			compareLegacyAndCurrentParseResult(t, input, tc.currentType, tc.legacyType)
		})
	}
}

func TestLegacyCompatibility_ParseRepresentativeErrors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		input       []byte
		path        string
		currentType parser.FileType
		legacyType  legacyparser.FileType
	}{
		{
			name:        "empty csv",
			input:       []byte(""),
			currentType: parser.CSV,
			legacyType:  legacyparser.CSV,
		},
		{
			name:        "duplicate csv columns",
			path:        filepath.Join("testdata", "duplicate_columns.csv"),
			currentType: parser.CSV,
			legacyType:  legacyparser.CSV,
		},
		{
			name:        "invalid json",
			input:       []byte(`{"name":`),
			currentType: parser.JSON,
			legacyType:  legacyparser.JSON,
		},
		{
			name:        "invalid jsonl",
			input:       []byte("{\"ok\":true}\nnot-json\n"),
			currentType: parser.JSONL,
			legacyType:  legacyparser.JSONL,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			input := tc.input
			if tc.path != "" {
				input = readLegacyCompatFixture(t, tc.path)
			}

			compareLegacyAndCurrentParseResult(t, input, tc.currentType, tc.legacyType)
		})
	}
}
