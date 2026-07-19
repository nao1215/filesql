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
		{"CSVGZ uses comma", CSVGZ, ','},
		{"CSVBZ2 uses comma", CSVBZ2, ','},
		{"CSVXZ uses comma", CSVXZ, ','},
		{"CSVZSTD uses comma", CSVZSTD, ','},
		{"CSVZLIB uses comma", CSVZLIB, ','},
		{"CSVSNAPPY uses comma", CSVSNAPPY, ','},
		{"CSVS2 uses comma", CSVS2, ','},
		{"CSVLZ4 uses comma", CSVLZ4, ','},
		{"TSVGZ uses tab", TSVGZ, '\t'},
		{"TSVBZ2 uses tab", TSVBZ2, '\t'},
		{"TSVXZ uses tab", TSVXZ, '\t'},
		{"TSVZSTD uses tab", TSVZSTD, '\t'},
		{"TSVZLIB uses tab", TSVZLIB, '\t'},
		{"TSVSNAPPY uses tab", TSVSNAPPY, '\t'},
		{"TSVS2 uses tab", TSVS2, '\t'},
		{"TSVLZ4 uses tab", TSVLZ4, '\t'},
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
		{"CSVGZ returns CSV", CSVGZ, CSV},
		{"CSVBZ2 returns CSV", CSVBZ2, CSV},
		{"CSVXZ returns CSV", CSVXZ, CSV},
		{"CSVZSTD returns CSV", CSVZSTD, CSV},
		{"CSVZLIB returns CSV", CSVZLIB, CSV},
		{"CSVSNAPPY returns CSV", CSVSNAPPY, CSV},
		{"CSVS2 returns CSV", CSVS2, CSV},
		{"CSVLZ4 returns CSV", CSVLZ4, CSV},
		{"TSV returns TSV", TSV, TSV},
		{"TSVGZ returns TSV", TSVGZ, TSV},
		{"TSVBZ2 returns TSV", TSVBZ2, TSV},
		{"TSVXZ returns TSV", TSVXZ, TSV},
		{"TSVZSTD returns TSV", TSVZSTD, TSV},
		{"TSVZLIB returns TSV", TSVZLIB, TSV},
		{"TSVSNAPPY returns TSV", TSVSNAPPY, TSV},
		{"TSVS2 returns TSV", TSVS2, TSV},
		{"TSVLZ4 returns TSV", TSVLZ4, TSV},
		{"LTSV returns LTSV", LTSV, LTSV},
		{"LTSVGZ returns LTSV", LTSVGZ, LTSV},
		{"LTSVZLIB returns LTSV", LTSVZLIB, LTSV},
		{"LTSVSNAPPY returns LTSV", LTSVSNAPPY, LTSV},
		{"LTSVS2 returns LTSV", LTSVS2, LTSV},
		{"LTSVLZ4 returns LTSV", LTSVLZ4, LTSV},
		{"Parquet returns Parquet", Parquet, Parquet},
		{"ParquetGZ returns Parquet", ParquetGZ, Parquet},
		{"ParquetZLIB returns Parquet", ParquetZLIB, Parquet},
		{"ParquetSNAPPY returns Parquet", ParquetSNAPPY, Parquet},
		{"ParquetS2 returns Parquet", ParquetS2, Parquet},
		{"ParquetLZ4 returns Parquet", ParquetLZ4, Parquet},
		{"XLSX returns XLSX", XLSX, XLSX},
		{"XLSXGZ returns XLSX", XLSXGZ, XLSX},
		{"XLSXZLIB returns XLSX", XLSXZLIB, XLSX},
		{"XLSXSNAPPY returns XLSX", XLSXSNAPPY, XLSX},
		{"XLSXS2 returns XLSX", XLSXS2, XLSX},
		{"XLSXLZ4 returns XLSX", XLSXLZ4, XLSX},
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
