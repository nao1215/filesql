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
	case FileTypeXLSXZLIB:
		return fileparser.XLSXZLIB
	case FileTypeXLSXSNAPPY:
		return fileparser.XLSXSNAPPY
	case FileTypeXLSXS2:
		return fileparser.XLSXS2
	case FileTypeXLSXLZ4:
		return fileparser.XLSXLZ4
	case FileTypeCSVZLIB:
		return fileparser.CSVZLIB
	case FileTypeTSVZLIB:
		return fileparser.TSVZLIB
	case FileTypeLTSVZLIB:
		return fileparser.LTSVZLIB
	case FileTypeParquetZLIB:
		return fileparser.ParquetZLIB
	case FileTypeCSVSNAPPY:
		return fileparser.CSVSNAPPY
	case FileTypeTSVSNAPPY:
		return fileparser.TSVSNAPPY
	case FileTypeLTSVSNAPPY:
		return fileparser.LTSVSNAPPY
	case FileTypeParquetSNAPPY:
		return fileparser.ParquetSNAPPY
	case FileTypeCSVS2:
		return fileparser.CSVS2
	case FileTypeTSVS2:
		return fileparser.TSVS2
	case FileTypeLTSVS2:
		return fileparser.LTSVS2
	case FileTypeParquetS2:
		return fileparser.ParquetS2
	case FileTypeCSVLZ4:
		return fileparser.CSVLZ4
	case FileTypeTSVLZ4:
		return fileparser.TSVLZ4
	case FileTypeLTSVLZ4:
		return fileparser.LTSVLZ4
	case FileTypeParquetLZ4:
		return fileparser.ParquetLZ4
	case FileTypeJSON:
		return fileparser.JSON
	case FileTypeJSONL:
		return fileparser.JSONL
	case FileTypeJSONGZ:
		return fileparser.JSONGZ
	case FileTypeJSONBZ2:
		return fileparser.JSONBZ2
	case FileTypeJSONXZ:
		return fileparser.JSONXZ
	case FileTypeJSONZSTD:
		return fileparser.JSONZSTD
	case FileTypeJSONZLIB:
		return fileparser.JSONZLIB
	case FileTypeJSONSNAPPY:
		return fileparser.JSONSNAPPY
	case FileTypeJSONS2:
		return fileparser.JSONS2
	case FileTypeJSONLZ4:
		return fileparser.JSONLZ4
	case FileTypeJSONLGZ:
		return fileparser.JSONLGZ
	case FileTypeJSONLBZ2:
		return fileparser.JSONLBZ2
	case FileTypeJSONLXZ:
		return fileparser.JSONLXZ
	case FileTypeJSONLZSTD:
		return fileparser.JSONLZSTD
	case FileTypeJSONLZLIB:
		return fileparser.JSONLZLIB
	case FileTypeJSONLSNAPPY:
		return fileparser.JSONLSNAPPY
	case FileTypeJSONLS2:
		return fileparser.JSONLS2
	case FileTypeJSONLLZ4:
		return fileparser.JSONLLZ4
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
