package filesql

import (
	"errors"
	"testing"

	"github.com/nao1215/fileparser"
	"github.com/stretchr/testify/assert"
)

func TestParserFileType(t *testing.T) {
	tests := []struct {
		name     string
		input    FileType
		expected fileparser.FileType
	}{
		// Base types
		{"CSV", FileTypeCSV, fileparser.CSV},
		{"TSV", FileTypeTSV, fileparser.TSV},
		{"LTSV", FileTypeLTSV, fileparser.LTSV},
		{"Parquet", FileTypeParquet, fileparser.Parquet},
		{"XLSX", FileTypeXLSX, fileparser.XLSX},

		// GZ compressed
		{"CSV GZ", FileTypeCSVGZ, fileparser.CSVGZ},
		{"TSV GZ", FileTypeTSVGZ, fileparser.TSVGZ},
		{"LTSV GZ", FileTypeLTSVGZ, fileparser.LTSVGZ},
		{"Parquet GZ", FileTypeParquetGZ, fileparser.ParquetGZ},
		{"XLSX GZ", FileTypeXLSXGZ, fileparser.XLSXGZ},

		// BZ2 compressed
		{"CSV BZ2", FileTypeCSVBZ2, fileparser.CSVBZ2},
		{"TSV BZ2", FileTypeTSVBZ2, fileparser.TSVBZ2},
		{"LTSV BZ2", FileTypeLTSVBZ2, fileparser.LTSVBZ2},
		{"Parquet BZ2", FileTypeParquetBZ2, fileparser.ParquetBZ2},
		{"XLSX BZ2", FileTypeXLSXBZ2, fileparser.XLSXBZ2},

		// XZ compressed
		{"CSV XZ", FileTypeCSVXZ, fileparser.CSVXZ},
		{"TSV XZ", FileTypeTSVXZ, fileparser.TSVXZ},
		{"LTSV XZ", FileTypeLTSVXZ, fileparser.LTSVXZ},
		{"Parquet XZ", FileTypeParquetXZ, fileparser.ParquetXZ},
		{"XLSX XZ", FileTypeXLSXXZ, fileparser.XLSXXZ},

		// ZSTD compressed
		{"CSV ZSTD", FileTypeCSVZSTD, fileparser.CSVZSTD},
		{"TSV ZSTD", FileTypeTSVZSTD, fileparser.TSVZSTD},
		{"LTSV ZSTD", FileTypeLTSVZSTD, fileparser.LTSVZSTD},
		{"Parquet ZSTD", FileTypeParquetZSTD, fileparser.ParquetZSTD},
		{"XLSX ZSTD", FileTypeXLSXZSTD, fileparser.XLSXZSTD},

		// Unsupported / default case
		{"Unsupported", FileTypeUnsupported, fileparser.Unsupported},
		{"Unknown type", FileType(9999), fileparser.Unsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parserFileType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParserColumnType(t *testing.T) {
	tests := []struct {
		name     string
		input    fileparser.ColumnType
		expected columnType
	}{
		{"Integer", fileparser.TypeInteger, columnTypeInteger},
		{"Real", fileparser.TypeReal, columnTypeReal},
		{"Datetime", fileparser.TypeDatetime, columnTypeDatetime},
		{"Text", fileparser.TypeText, columnTypeText},
		{"Unknown defaults to Text", fileparser.ColumnType(9999), columnTypeText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parserColumnType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertParserError(t *testing.T) {
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
