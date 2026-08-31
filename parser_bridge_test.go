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
