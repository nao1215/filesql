package frame

import (
	"testing"

	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
)

func TestDelimiter(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		fileType FileType
		expected rune
	}{
		{"CSV uses comma", CSV, ','},
		{"TSV uses tab", TSV, '\t'},
		{"LTSV uses comma", LTSV, ','},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := delimiter(tc.fileType)

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBaseType(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		fileType FileType
		expected FileType
	}{
		{"CSV returns CSV", CSV, CSV},
		{"TSV returns TSV", TSV, TSV},
		{"LTSV returns LTSV", LTSV, LTSV},
		{"Parquet returns Parquet", Parquet, Parquet},
		{"XLSX returns XLSX", XLSX, XLSX},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := baseType(tc.fileType)

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBaseTypeUnsupported(t *testing.T) {
	t.Parallel()

	// Test with an invalid FileType value
	result := baseType(FileType(999))

	assert.Equal(t, parser.Unsupported, result)
}

func TestNewCompressionFormats(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		path         string
		expectedRows int
		expectedCols int
	}{
		{"CSV ZLIB", "testdata/sample.csv.z", 3, 4},
		{"CSV Snappy", "testdata/sample.csv.snappy", 3, 4},
		{"CSV S2", "testdata/sample.csv.s2", 3, 4},
		{"CSV LZ4", "testdata/sample.csv.lz4", 3, 4},
		{"TSV ZLIB", "testdata/products.tsv.z", 3, 3},
		{"TSV Snappy", "testdata/products.tsv.snappy", 3, 3},
		{"TSV S2", "testdata/products.tsv.s2", 3, 3},
		{"TSV LZ4", "testdata/products.tsv.lz4", 3, 3},
		{"LTSV ZLIB", "testdata/logs.ltsv.z", 3, 3},
		{"LTSV Snappy", "testdata/logs.ltsv.snappy", 3, 3},
		{"LTSV S2", "testdata/logs.ltsv.s2", 3, 3},
		{"LTSV LZ4", "testdata/logs.ltsv.lz4", 3, 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			df, err := NewDataFrameFromPath(tc.path)
			assert.NoError(t, err)
			assert.NotNil(t, df)
			assert.Equal(t, tc.expectedRows, df.Len(), "row count mismatch")
			assert.Equal(t, tc.expectedCols, len(df.Columns()), "column count mismatch")
		})
	}
}
