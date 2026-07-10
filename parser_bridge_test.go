package filesql

import (
	"errors"
	"testing"

	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
)

func TestParserFileType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    FileType
		expected parser.FileType
	}{
		// Base types
		{"CSV", FileTypeCSV, parser.CSV},
		{"TSV", FileTypeTSV, parser.TSV},
		{"LTSV", FileTypeLTSV, parser.LTSV},
		{"Parquet", FileTypeParquet, parser.Parquet},
		{"XLSX", FileTypeXLSX, parser.XLSX},

		// GZ compressed
		{"CSV GZ", FileTypeCSVGZ, parser.CSVGZ},
		{"TSV GZ", FileTypeTSVGZ, parser.TSVGZ},
		{"LTSV GZ", FileTypeLTSVGZ, parser.LTSVGZ},
		{"Parquet GZ", FileTypeParquetGZ, parser.ParquetGZ},
		{"XLSX GZ", FileTypeXLSXGZ, parser.XLSXGZ},

		// BZ2 compressed
		{"CSV BZ2", FileTypeCSVBZ2, parser.CSVBZ2},
		{"TSV BZ2", FileTypeTSVBZ2, parser.TSVBZ2},
		{"LTSV BZ2", FileTypeLTSVBZ2, parser.LTSVBZ2},
		{"Parquet BZ2", FileTypeParquetBZ2, parser.ParquetBZ2},
		{"XLSX BZ2", FileTypeXLSXBZ2, parser.XLSXBZ2},

		// XZ compressed
		{"CSV XZ", FileTypeCSVXZ, parser.CSVXZ},
		{"TSV XZ", FileTypeTSVXZ, parser.TSVXZ},
		{"LTSV XZ", FileTypeLTSVXZ, parser.LTSVXZ},
		{"Parquet XZ", FileTypeParquetXZ, parser.ParquetXZ},
		{"XLSX XZ", FileTypeXLSXXZ, parser.XLSXXZ},

		// ZSTD compressed
		{"CSV ZSTD", FileTypeCSVZSTD, parser.CSVZSTD},
		{"TSV ZSTD", FileTypeTSVZSTD, parser.TSVZSTD},
		{"LTSV ZSTD", FileTypeLTSVZSTD, parser.LTSVZSTD},
		{"Parquet ZSTD", FileTypeParquetZSTD, parser.ParquetZSTD},
		{"XLSX ZSTD", FileTypeXLSXZSTD, parser.XLSXZSTD},

		// ZLIB compressed
		{"CSV ZLIB", FileTypeCSVZLIB, parser.CSVZLIB},
		{"TSV ZLIB", FileTypeTSVZLIB, parser.TSVZLIB},
		{"LTSV ZLIB", FileTypeLTSVZLIB, parser.LTSVZLIB},
		{"Parquet ZLIB", FileTypeParquetZLIB, parser.ParquetZLIB},
		{"XLSX ZLIB", FileTypeXLSXZLIB, parser.XLSXZLIB},

		// SNAPPY compressed
		{"CSV SNAPPY", FileTypeCSVSNAPPY, parser.CSVSNAPPY},
		{"TSV SNAPPY", FileTypeTSVSNAPPY, parser.TSVSNAPPY},
		{"LTSV SNAPPY", FileTypeLTSVSNAPPY, parser.LTSVSNAPPY},
		{"Parquet SNAPPY", FileTypeParquetSNAPPY, parser.ParquetSNAPPY},
		{"XLSX SNAPPY", FileTypeXLSXSNAPPY, parser.XLSXSNAPPY},

		// S2 compressed
		{"CSV S2", FileTypeCSVS2, parser.CSVS2},
		{"TSV S2", FileTypeTSVS2, parser.TSVS2},
		{"LTSV S2", FileTypeLTSVS2, parser.LTSVS2},
		{"Parquet S2", FileTypeParquetS2, parser.ParquetS2},
		{"XLSX S2", FileTypeXLSXS2, parser.XLSXS2},

		// LZ4 compressed
		{"CSV LZ4", FileTypeCSVLZ4, parser.CSVLZ4},
		{"TSV LZ4", FileTypeTSVLZ4, parser.TSVLZ4},
		{"LTSV LZ4", FileTypeLTSVLZ4, parser.LTSVLZ4},
		{"Parquet LZ4", FileTypeParquetLZ4, parser.ParquetLZ4},
		{"XLSX LZ4", FileTypeXLSXLZ4, parser.XLSXLZ4},

		// JSON base types
		{"JSON", FileTypeJSON, parser.JSON},
		{"JSONL", FileTypeJSONL, parser.JSONL},

		// JSON compressed
		{"JSON GZ", FileTypeJSONGZ, parser.JSONGZ},
		{"JSON BZ2", FileTypeJSONBZ2, parser.JSONBZ2},
		{"JSON XZ", FileTypeJSONXZ, parser.JSONXZ},
		{"JSON ZSTD", FileTypeJSONZSTD, parser.JSONZSTD},
		{"JSON ZLIB", FileTypeJSONZLIB, parser.JSONZLIB},
		{"JSON SNAPPY", FileTypeJSONSNAPPY, parser.JSONSNAPPY},
		{"JSON S2", FileTypeJSONS2, parser.JSONS2},
		{"JSON LZ4", FileTypeJSONLZ4, parser.JSONLZ4},

		// JSONL compressed
		{"JSONL GZ", FileTypeJSONLGZ, parser.JSONLGZ},
		{"JSONL BZ2", FileTypeJSONLBZ2, parser.JSONLBZ2},
		{"JSONL XZ", FileTypeJSONLXZ, parser.JSONLXZ},
		{"JSONL ZSTD", FileTypeJSONLZSTD, parser.JSONLZSTD},
		{"JSONL ZLIB", FileTypeJSONLZLIB, parser.JSONLZLIB},
		{"JSONL SNAPPY", FileTypeJSONLSNAPPY, parser.JSONLSNAPPY},
		{"JSONL S2", FileTypeJSONLS2, parser.JSONLS2},
		{"JSONL LZ4", FileTypeJSONLLZ4, parser.JSONLLZ4},

		// Unsupported / default case
		{"Unsupported", FileTypeUnsupported, parser.Unsupported},
		{"Unknown type", FileType(9999), parser.Unsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parserFileType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParserColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    parser.ColumnType
		expected columnType
	}{
		{"Integer", parser.TypeInteger, columnTypeInteger},
		{"Real", parser.TypeReal, columnTypeReal},
		{"Datetime", parser.TypeDatetime, columnTypeDatetime},
		{"Text", parser.TypeText, columnTypeText},
		{"Unknown defaults to Text", parser.ColumnType(9999), columnTypeText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parserColumnType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertParserError(t *testing.T) {
	t.Parallel()

	t.Run("nil error returns nil", func(t *testing.T) {
		result := convertParserError(nil)
		assert.NoError(t, result)
	})

	t.Run("duplicate column name with column specified", func(t *testing.T) {
		err := errors.New("duplicate column name: foo")
		result := convertParserError(err)
		assert.Error(t, result)
		assert.ErrorIs(t, result, errDuplicateColumnName)
		assert.Contains(t, result.Error(), "foo")
	})

	t.Run("duplicate column name without column specified", func(t *testing.T) {
		err := errors.New("duplicate column name")
		result := convertParserError(err)
		assert.Error(t, result)
		assert.Equal(t, errDuplicateColumnName, result)
	})

	t.Run("other error passes through", func(t *testing.T) {
		err := errors.New("some other error")
		result := convertParserError(err)
		assert.Equal(t, err, result)
	})
}
