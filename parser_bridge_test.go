package filesql

import (
	"testing"

	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
)

// fileTypePair is one format and the parser constant that names it. Only
// formats appear here: FileType no longer has a constant for a codec.
type fileTypePair struct {
	name    string
	filesql FileType
	parser  parser.FileType
}

func parserBackedFileTypePairs() []fileTypePair {
	return []fileTypePair{
		{"CSV", FileTypeCSV, parser.CSV},
		{"TSV", FileTypeTSV, parser.TSV},
		{"LTSV", FileTypeLTSV, parser.LTSV},
		{"Parquet", FileTypeParquet, parser.Parquet},
		{"XLSX", FileTypeXLSX, parser.XLSX},
		{"JSON", FileTypeJSON, parser.JSON},
		{"JSONL", FileTypeJSONL, parser.JSONL},
	}
}

// compressedParserFileTypes lists every fused constant the parser still has,
// paired with the format it folds to. filesqlFileType has to drop the codec:
// the bridge is where the two enums stop agreeing, because parser.FileType is
// the lower level and keeps naming the combination.
func compressedParserFileTypes() []struct {
	name    string
	parser  parser.FileType
	filesql FileType
} {
	return []struct {
		name    string
		parser  parser.FileType
		filesql FileType
	}{
		{"CSV GZ", parser.CSVGZ, FileTypeCSV},
		{"TSV GZ", parser.TSVGZ, FileTypeTSV},
		{"LTSV GZ", parser.LTSVGZ, FileTypeLTSV},
		{"Parquet GZ", parser.ParquetGZ, FileTypeParquet},
		{"XLSX GZ", parser.XLSXGZ, FileTypeXLSX},
		{"CSV BZ2", parser.CSVBZ2, FileTypeCSV},
		{"TSV BZ2", parser.TSVBZ2, FileTypeTSV},
		{"LTSV BZ2", parser.LTSVBZ2, FileTypeLTSV},
		{"Parquet BZ2", parser.ParquetBZ2, FileTypeParquet},
		{"XLSX BZ2", parser.XLSXBZ2, FileTypeXLSX},
		{"CSV XZ", parser.CSVXZ, FileTypeCSV},
		{"TSV XZ", parser.TSVXZ, FileTypeTSV},
		{"LTSV XZ", parser.LTSVXZ, FileTypeLTSV},
		{"Parquet XZ", parser.ParquetXZ, FileTypeParquet},
		{"XLSX XZ", parser.XLSXXZ, FileTypeXLSX},
		{"CSV ZSTD", parser.CSVZSTD, FileTypeCSV},
		{"TSV ZSTD", parser.TSVZSTD, FileTypeTSV},
		{"LTSV ZSTD", parser.LTSVZSTD, FileTypeLTSV},
		{"Parquet ZSTD", parser.ParquetZSTD, FileTypeParquet},
		{"XLSX ZSTD", parser.XLSXZSTD, FileTypeXLSX},
		{"CSV ZLIB", parser.CSVZLIB, FileTypeCSV},
		{"TSV ZLIB", parser.TSVZLIB, FileTypeTSV},
		{"LTSV ZLIB", parser.LTSVZLIB, FileTypeLTSV},
		{"Parquet ZLIB", parser.ParquetZLIB, FileTypeParquet},
		{"XLSX ZLIB", parser.XLSXZLIB, FileTypeXLSX},
		{"CSV SNAPPY", parser.CSVSNAPPY, FileTypeCSV},
		{"TSV SNAPPY", parser.TSVSNAPPY, FileTypeTSV},
		{"LTSV SNAPPY", parser.LTSVSNAPPY, FileTypeLTSV},
		{"Parquet SNAPPY", parser.ParquetSNAPPY, FileTypeParquet},
		{"XLSX SNAPPY", parser.XLSXSNAPPY, FileTypeXLSX},
		{"CSV S2", parser.CSVS2, FileTypeCSV},
		{"TSV S2", parser.TSVS2, FileTypeTSV},
		{"LTSV S2", parser.LTSVS2, FileTypeLTSV},
		{"Parquet S2", parser.ParquetS2, FileTypeParquet},
		{"XLSX S2", parser.XLSXS2, FileTypeXLSX},
		{"CSV LZ4", parser.CSVLZ4, FileTypeCSV},
		{"TSV LZ4", parser.TSVLZ4, FileTypeTSV},
		{"LTSV LZ4", parser.LTSVLZ4, FileTypeLTSV},
		{"Parquet LZ4", parser.ParquetLZ4, FileTypeParquet},
		{"XLSX LZ4", parser.XLSXLZ4, FileTypeXLSX},
		{"JSON GZ", parser.JSONGZ, FileTypeJSON},
		{"JSON BZ2", parser.JSONBZ2, FileTypeJSON},
		{"JSON XZ", parser.JSONXZ, FileTypeJSON},
		{"JSON ZSTD", parser.JSONZSTD, FileTypeJSON},
		{"JSON ZLIB", parser.JSONZLIB, FileTypeJSON},
		{"JSON SNAPPY", parser.JSONSNAPPY, FileTypeJSON},
		{"JSON S2", parser.JSONS2, FileTypeJSON},
		{"JSON LZ4", parser.JSONLZ4, FileTypeJSON},
		{"JSONL GZ", parser.JSONLGZ, FileTypeJSONL},
		{"JSONL BZ2", parser.JSONLBZ2, FileTypeJSONL},
		{"JSONL XZ", parser.JSONLXZ, FileTypeJSONL},
		{"JSONL ZSTD", parser.JSONLZSTD, FileTypeJSONL},
		{"JSONL ZLIB", parser.JSONLZLIB, FileTypeJSONL},
		{"JSONL SNAPPY", parser.JSONLSNAPPY, FileTypeJSONL},
		{"JSONL S2", parser.JSONLS2, FileTypeJSONL},
		{"JSONL LZ4", parser.JSONLLZ4, FileTypeJSONL},
	}
}

func TestParserFileType(t *testing.T) {
	t.Parallel()

	for _, tt := range parserBackedFileTypePairs() {
		t.Run(tt.name, func(t *testing.T) {
			result := parserFileType(tt.filesql)
			assert.Equal(t, tt.parser, result)
		})
	}

	t.Run("Unsupported", func(t *testing.T) {
		result := parserFileType(FileTypeUnsupported)
		assert.Equal(t, parser.Unsupported, result)
	})

	t.Run("Unknown type", func(t *testing.T) {
		result := parserFileType(FileType(9999))
		assert.Equal(t, parser.Unsupported, result)
	})

	// ACH and Fedwire are filesql's own formats; the parser does not handle
	// them, so the bridge has nothing to hand it.
	for _, ft := range []FileType{FileTypeACH, FileTypeFedWire} {
		t.Run(ft.String(), func(t *testing.T) {
			assert.Equal(t, parser.Unsupported, parserFileType(ft))
		})
	}
}

// TestParserFileType_RoundTrip pins that a format survives the trip through
// the parser's enum and back.
func TestParserFileType_RoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range parserBackedFileTypePairs() {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.filesql, filesqlFileType(parserFileType(tt.filesql)))
		})
	}
}

func TestFilesqlFileType(t *testing.T) {
	t.Parallel()

	for _, tt := range parserBackedFileTypePairs() {
		t.Run(tt.name, func(t *testing.T) {
			result := filesqlFileType(tt.parser)
			assert.Equal(t, tt.filesql, result)
		})
	}

	t.Run("Unsupported", func(t *testing.T) {
		result := filesqlFileType(parser.Unsupported)
		assert.Equal(t, FileTypeUnsupported, result)
	})

	t.Run("Unknown type", func(t *testing.T) {
		result := filesqlFileType(parser.FileType(9999))
		assert.Equal(t, FileTypeUnsupported, result)
	})
}

// TestFilesqlFileType_FoldsCompression checks the property the fused map used
// to state entry by entry: a compressed parser constant answers its format.
func TestFilesqlFileType_FoldsCompression(t *testing.T) {
	t.Parallel()

	for _, tt := range compressedParserFileTypes() {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.filesql, filesqlFileType(tt.parser))
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
