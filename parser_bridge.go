package filesql

import (
	"fmt"
	"io"
	"strings"

	"github.com/nao1215/filesql/parser"
)

// parserFileType converts filesql.FileType to parser.FileType
func parserFileType(ft FileType) parser.FileType {
	switch ft {
	case FileTypeCSV:
		return parser.CSV
	case FileTypeTSV:
		return parser.TSV
	case FileTypeLTSV:
		return parser.LTSV
	case FileTypeParquet:
		return parser.Parquet
	case FileTypeXLSX:
		return parser.XLSX
	case FileTypeCSVGZ:
		return parser.CSVGZ
	case FileTypeTSVGZ:
		return parser.TSVGZ
	case FileTypeLTSVGZ:
		return parser.LTSVGZ
	case FileTypeParquetGZ:
		return parser.ParquetGZ
	case FileTypeCSVBZ2:
		return parser.CSVBZ2
	case FileTypeTSVBZ2:
		return parser.TSVBZ2
	case FileTypeLTSVBZ2:
		return parser.LTSVBZ2
	case FileTypeParquetBZ2:
		return parser.ParquetBZ2
	case FileTypeCSVXZ:
		return parser.CSVXZ
	case FileTypeTSVXZ:
		return parser.TSVXZ
	case FileTypeLTSVXZ:
		return parser.LTSVXZ
	case FileTypeParquetXZ:
		return parser.ParquetXZ
	case FileTypeCSVZSTD:
		return parser.CSVZSTD
	case FileTypeTSVZSTD:
		return parser.TSVZSTD
	case FileTypeLTSVZSTD:
		return parser.LTSVZSTD
	case FileTypeParquetZSTD:
		return parser.ParquetZSTD
	case FileTypeXLSXGZ:
		return parser.XLSXGZ
	case FileTypeXLSXBZ2:
		return parser.XLSXBZ2
	case FileTypeXLSXXZ:
		return parser.XLSXXZ
	case FileTypeXLSXZSTD:
		return parser.XLSXZSTD
	default:
		return parser.Unsupported
	}
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
func parseWithParser(reader io.Reader, fileType FileType, tableName string) (*table, error) {
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
