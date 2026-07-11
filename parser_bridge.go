package filesql

import (
	"fmt"
	"io"
	"strings"

	"github.com/nao1215/filesql/parser"
)

var filesqlToParserFileTypes = map[FileType]parser.FileType{
	FileTypeCSV:           parser.CSV,
	FileTypeTSV:           parser.TSV,
	FileTypeLTSV:          parser.LTSV,
	FileTypeParquet:       parser.Parquet,
	FileTypeXLSX:          parser.XLSX,
	FileTypeCSVGZ:         parser.CSVGZ,
	FileTypeTSVGZ:         parser.TSVGZ,
	FileTypeLTSVGZ:        parser.LTSVGZ,
	FileTypeParquetGZ:     parser.ParquetGZ,
	FileTypeCSVBZ2:        parser.CSVBZ2,
	FileTypeTSVBZ2:        parser.TSVBZ2,
	FileTypeLTSVBZ2:       parser.LTSVBZ2,
	FileTypeParquetBZ2:    parser.ParquetBZ2,
	FileTypeCSVXZ:         parser.CSVXZ,
	FileTypeTSVXZ:         parser.TSVXZ,
	FileTypeLTSVXZ:        parser.LTSVXZ,
	FileTypeParquetXZ:     parser.ParquetXZ,
	FileTypeCSVZSTD:       parser.CSVZSTD,
	FileTypeTSVZSTD:       parser.TSVZSTD,
	FileTypeLTSVZSTD:      parser.LTSVZSTD,
	FileTypeParquetZSTD:   parser.ParquetZSTD,
	FileTypeXLSXGZ:        parser.XLSXGZ,
	FileTypeXLSXBZ2:       parser.XLSXBZ2,
	FileTypeXLSXXZ:        parser.XLSXXZ,
	FileTypeXLSXZSTD:      parser.XLSXZSTD,
	FileTypeCSVZLIB:       parser.CSVZLIB,
	FileTypeTSVZLIB:       parser.TSVZLIB,
	FileTypeLTSVZLIB:      parser.LTSVZLIB,
	FileTypeParquetZLIB:   parser.ParquetZLIB,
	FileTypeXLSXZLIB:      parser.XLSXZLIB,
	FileTypeCSVSNAPPY:     parser.CSVSNAPPY,
	FileTypeTSVSNAPPY:     parser.TSVSNAPPY,
	FileTypeLTSVSNAPPY:    parser.LTSVSNAPPY,
	FileTypeParquetSNAPPY: parser.ParquetSNAPPY,
	FileTypeXLSXSNAPPY:    parser.XLSXSNAPPY,
	FileTypeCSVS2:         parser.CSVS2,
	FileTypeTSVS2:         parser.TSVS2,
	FileTypeLTSVS2:        parser.LTSVS2,
	FileTypeParquetS2:     parser.ParquetS2,
	FileTypeXLSXS2:        parser.XLSXS2,
	FileTypeCSVLZ4:        parser.CSVLZ4,
	FileTypeTSVLZ4:        parser.TSVLZ4,
	FileTypeLTSVLZ4:       parser.LTSVLZ4,
	FileTypeParquetLZ4:    parser.ParquetLZ4,
	FileTypeXLSXLZ4:       parser.XLSXLZ4,
	FileTypeJSON:          parser.JSON,
	FileTypeJSONL:         parser.JSONL,
	FileTypeJSONGZ:        parser.JSONGZ,
	FileTypeJSONBZ2:       parser.JSONBZ2,
	FileTypeJSONXZ:        parser.JSONXZ,
	FileTypeJSONZSTD:      parser.JSONZSTD,
	FileTypeJSONZLIB:      parser.JSONZLIB,
	FileTypeJSONSNAPPY:    parser.JSONSNAPPY,
	FileTypeJSONS2:        parser.JSONS2,
	FileTypeJSONLZ4:       parser.JSONLZ4,
	FileTypeJSONLGZ:       parser.JSONLGZ,
	FileTypeJSONLBZ2:      parser.JSONLBZ2,
	FileTypeJSONLXZ:       parser.JSONLXZ,
	FileTypeJSONLZSTD:     parser.JSONLZSTD,
	FileTypeJSONLZLIB:     parser.JSONLZLIB,
	FileTypeJSONLSNAPPY:   parser.JSONLSNAPPY,
	FileTypeJSONLS2:       parser.JSONLS2,
	FileTypeJSONLLZ4:      parser.JSONLLZ4,
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

// filesqlFileType converts parser.FileType to filesql.FileType.
func filesqlFileType(ft parser.FileType) FileType {
	if filesqlType, ok := parserToFilesqlFileTypes[ft]; ok {
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

// parseWithParser uses the fileparser package to parse data and returns a filesql table.
// This function bridges the fileparser package with filesql's internal table structure.
func parseWithParser(reader io.Reader, fileType FileType, tableName string) (*table, error) {
	// Strip a Unicode BOM (and transcode UTF-16) before the text parser sees it,
	// matching the streaming path; binary formats keep their raw bytes.
	if isTextBaseType(fileType.baseType()) {
		reader = decodeTextReader(reader)
	}
	result, err := parser.Parse(reader, parserFileType(fileType))
	if err != nil {
		// Convert parser errors to filesql errors for compatibility
		return nil, convertParserError(err)
	}

	// Convert parser.TableData to filesql table structure
	headers := newHeader(result.Headers)

	records := make([]Record, len(result.Records))
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
		name:       NewTableName(tableName),
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
