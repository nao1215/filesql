package filesql

import (
	"fmt"
	"io"
	"strings"

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

// parseWithParser uses the parser package to parse data and returns a filesql table.
// This function bridges the parser package with filesql's internal table structure.
func parseWithParser(reader io.Reader, fileType FileType, tableName string, excelSheetPolicy ExcelSheetPolicy) (*table, error) {
	// Strip a Unicode BOM (and transcode UTF-16) before the text parser sees it,
	// matching the streaming path; binary formats keep their raw bytes.
	if isTextBaseType(fileType) {
		reader = decodeTextReader(reader)
	}
	result, err := parser.Parse(reader, parserFileType(fileType), parser.WithExcelSheetPolicy(excelSheetPolicy))
	if err != nil {
		// Convert parser errors to filesql errors for compatibility
		return nil, convertParserError(err)
	}

	// Convert parser.TableData to filesql table structure
	headers := newHeader(result.Headers)

	records := make([]record, len(result.Records))
	for i, rec := range result.Records {
		records[i] = newRecord(rec)
	}

	// Convert column types
	columnInfos := make([]columnInfo, len(result.Headers))
	for i, name := range result.Headers {
		columnInfos[i] = columnInfo{
			Name: name,
			Type: parserColumnType(result.ColumnTypes[i]),
		}
	}

	return &table{
		name:       newTableName(tableName),
		header:     headers,
		records:    records,
		columnInfo: columnInfos,
	}, nil
}

// convertParserError converts parser package errors to filesql errors for compatibility.
func convertParserError(err error) error {
	if err == nil {
		return nil
	}

	errStr := err.Error()

	// Check for duplicate column name error
	if strings.Contains(errStr, "duplicate column name") {
		// Extract the column name from the error message
		// Format: "duplicate column name: <name>"
		parts := strings.SplitN(errStr, ": ", 2)
		if len(parts) == 2 {
			return fmt.Errorf("%w: %s", errDuplicateColumnName, parts[1])
		}
		return errDuplicateColumnName
	}

	return err
}
