package filesql

import (
	"fmt"
	"io"
	"strings"

	"github.com/nao1215/fileparser"
)

// parserFileType converts filesql.FileType to fileparser.FileType
func parserFileType(ft FileType) fileparser.FileType {
	switch ft {
	case FileTypeCSV:
		return fileparser.CSV
	case FileTypeTSV:
		return fileparser.TSV
	case FileTypeLTSV:
		return fileparser.LTSV
	case FileTypeParquet:
		return fileparser.Parquet
	case FileTypeXLSX:
		return fileparser.XLSX
	case FileTypeCSVGZ:
		return fileparser.CSVGZ
	case FileTypeTSVGZ:
		return fileparser.TSVGZ
	case FileTypeLTSVGZ:
		return fileparser.LTSVGZ
	case FileTypeParquetGZ:
		return fileparser.ParquetGZ
	case FileTypeCSVBZ2:
		return fileparser.CSVBZ2
	case FileTypeTSVBZ2:
		return fileparser.TSVBZ2
	case FileTypeLTSVBZ2:
		return fileparser.LTSVBZ2
	case FileTypeParquetBZ2:
		return fileparser.ParquetBZ2
	case FileTypeCSVXZ:
		return fileparser.CSVXZ
	case FileTypeTSVXZ:
		return fileparser.TSVXZ
	case FileTypeLTSVXZ:
		return fileparser.LTSVXZ
	case FileTypeParquetXZ:
		return fileparser.ParquetXZ
	case FileTypeCSVZSTD:
		return fileparser.CSVZSTD
	case FileTypeTSVZSTD:
		return fileparser.TSVZSTD
	case FileTypeLTSVZSTD:
		return fileparser.LTSVZSTD
	case FileTypeParquetZSTD:
		return fileparser.ParquetZSTD
	case FileTypeXLSXGZ:
		return fileparser.XLSXGZ
	case FileTypeXLSXBZ2:
		return fileparser.XLSXBZ2
	case FileTypeXLSXXZ:
		return fileparser.XLSXXZ
	case FileTypeXLSXZSTD:
		return fileparser.XLSXZSTD
	default:
		return fileparser.Unsupported
	}
}

// parserColumnType converts fileparser.ColumnType to filesql.columnType
func parserColumnType(ct fileparser.ColumnType) columnType {
	switch ct {
	case fileparser.TypeInteger:
		return columnTypeInteger
	case fileparser.TypeReal:
		return columnTypeReal
	case fileparser.TypeDatetime:
		return columnTypeDatetime
	default:
		return columnTypeText
	}
}

// parseWithParser uses the fileparser package to parse data and returns a filesql table.
// This function bridges the fileparser package with filesql's internal table structure.
func parseWithParser(reader io.Reader, fileType FileType, tableName string) (*table, error) {
	result, err := fileparser.Parse(reader, parserFileType(fileType))
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
