package filesql

import (
	"github.com/nao1215/filesql/parser"
)

// filesqlToParserFileTypes maps a format to the parser's constant for it.
// Only formats appear here: the parser's compressed constants are folded back
// to their base by filesqlFileType.
var filesqlToParserFileTypes = map[FileType]parser.FileType{
	FileTypeCSV:     parser.CSV,
	FileTypeTSV:     parser.TSV,
	FileTypeLTSV:    parser.LTSV,
	FileTypeParquet: parser.Parquet,
	FileTypeXLSX:    parser.XLSX,
	FileTypeJSON:    parser.JSON,
	FileTypeJSONL:   parser.JSONL,
}

var parserToFilesqlFileTypes = reverseParserFileTypes(filesqlToParserFileTypes)

func reverseParserFileTypes(source map[FileType]parser.FileType) map[parser.FileType]FileType {
	reversed := make(map[parser.FileType]FileType, len(source))
	for filesqlType, parserType := range source {
		reversed[parserType] = filesqlType
	}
	return reversed
}

// parserFileType converts filesql.FileType to parser.FileType
func parserFileType(ft FileType) parser.FileType {
	if parserType, ok := filesqlToParserFileTypes[ft]; ok {
		return parserType
	}

	return parser.Unsupported
}

// filesqlFileType converts parser.FileType to filesql.FileType, folding any
// compression the parser's constant carries away: parser.CSVGZ answers
// FileTypeCSV, because FileType names the format only.
func filesqlFileType(ft parser.FileType) FileType {
	if filesqlType, ok := parserToFilesqlFileTypes[parser.BaseFileType(ft)]; ok {
		return filesqlType
	}

	return FileTypeUnsupported
}

// parserColumnType converts parser.ColumnType to filesql.columnType
func parserColumnType(ct parser.ColumnType) columnType {
	switch ct {
	case parser.TypeInteger:
		return columnTypeInteger
	case parser.TypeReal:
		return columnTypeReal
	case parser.TypeDatetime:
		return columnTypeDatetime
	default:
		return columnTypeText
	}
}
