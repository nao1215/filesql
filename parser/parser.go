// Package parser provides file parsing functionality for various tabular data formats.
// It supports CSV, TSV, LTSV, XLSX, and Parquet files, with optional compression
// (gzip, bzip2, xz, zstd).
//
// This package is used internally by filesql but can also be used independently
// when you need direct access to parsed data without SQL functionality.
//
// # Memory Considerations
//
// All parsing functions in this package load the entire dataset into memory.
// This design is intentional for simplicity and compatibility with formats that
// require random access (Parquet, XLSX), but has implications for large files:
//
//   - CSV/TSV/LTSV: Entire file content is read into memory
//   - XLSX: Entire workbook is loaded (Excel files can be large even with few rows)
//   - Parquet: Entire file is read into memory for random access
//
// For files larger than available memory, consider:
//   - Using filesql's streaming API (ProcessInChunks) for CSV/TSV
//   - Pre-filtering or splitting large files before processing
//   - Increasing available memory for the process
//
// # Example usage
//
//	f, _ := os.Open("data.csv")
//	defer f.Close()
//	result, err := parser.Parse(f, parser.CSV)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Columns:", result.Headers)
//	fmt.Println("Rows:", len(result.Records))
//
// # Type Conversion
//
// Use [ParseValue] to convert string records to typed Go values based on [ColumnType].
package parser

import (
	"compress/bzip2"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// FileType represents supported file types including compression variants.
type FileType int

const (
	// CSV represents CSV file type.
	CSV FileType = iota
	// TSV represents TSV file type.
	TSV
	// LTSV represents LTSV (Labeled Tab-Separated Values) file type.
	LTSV
	// Parquet represents Apache Parquet file type.
	Parquet
	// XLSX represents Excel XLSX file type.
	XLSX

	// CSVGZ represents gzip-compressed CSV file type.
	CSVGZ
	// CSVBZ2 represents bzip2-compressed CSV file type.
	CSVBZ2
	// CSVXZ represents xz-compressed CSV file type.
	CSVXZ
	// CSVZSTD represents zstd-compressed CSV file type.
	CSVZSTD

	// TSVGZ represents gzip-compressed TSV file type.
	TSVGZ
	// TSVBZ2 represents bzip2-compressed TSV file type.
	TSVBZ2
	// TSVXZ represents xz-compressed TSV file type.
	TSVXZ
	// TSVZSTD represents zstd-compressed TSV file type.
	TSVZSTD

	// LTSVGZ represents gzip-compressed LTSV file type.
	LTSVGZ
	// LTSVBZ2 represents bzip2-compressed LTSV file type.
	LTSVBZ2
	// LTSVXZ represents xz-compressed LTSV file type.
	LTSVXZ
	// LTSVZSTD represents zstd-compressed LTSV file type.
	LTSVZSTD

	// ParquetGZ represents gzip-compressed Parquet file type.
	ParquetGZ
	// ParquetBZ2 represents bzip2-compressed Parquet file type.
	ParquetBZ2
	// ParquetXZ represents xz-compressed Parquet file type.
	ParquetXZ
	// ParquetZSTD represents zstd-compressed Parquet file type.
	ParquetZSTD

	// XLSXGZ represents gzip-compressed XLSX file type.
	XLSXGZ
	// XLSXBZ2 represents bzip2-compressed XLSX file type.
	XLSXBZ2
	// XLSXXZ represents xz-compressed XLSX file type.
	XLSXXZ
	// XLSXZSTD represents zstd-compressed XLSX file type.
	XLSXZSTD

	// Unsupported represents unsupported file type.
	Unsupported
)

// String returns a human-readable string representation of the FileType.
func (ft FileType) String() string {
	switch ft {
	case CSV:
		return "CSV"
	case TSV:
		return "TSV"
	case LTSV:
		return "LTSV"
	case Parquet:
		return "Parquet"
	case XLSX:
		return "XLSX"
	case CSVGZ:
		return "CSV (gzip)"
	case CSVBZ2:
		return "CSV (bzip2)"
	case CSVXZ:
		return "CSV (xz)"
	case CSVZSTD:
		return "CSV (zstd)"
	case TSVGZ:
		return "TSV (gzip)"
	case TSVBZ2:
		return "TSV (bzip2)"
	case TSVXZ:
		return "TSV (xz)"
	case TSVZSTD:
		return "TSV (zstd)"
	case LTSVGZ:
		return "LTSV (gzip)"
	case LTSVBZ2:
		return "LTSV (bzip2)"
	case LTSVXZ:
		return "LTSV (xz)"
	case LTSVZSTD:
		return "LTSV (zstd)"
	case ParquetGZ:
		return "Parquet (gzip)"
	case ParquetBZ2:
		return "Parquet (bzip2)"
	case ParquetXZ:
		return "Parquet (xz)"
	case ParquetZSTD:
		return "Parquet (zstd)"
	case XLSXGZ:
		return "XLSX (gzip)"
	case XLSXBZ2:
		return "XLSX (bzip2)"
	case XLSXXZ:
		return "XLSX (xz)"
	case XLSXZSTD:
		return "XLSX (zstd)"
	default:
		return "Unsupported"
	}
}

// ColumnType represents the inferred type of a column.
type ColumnType int

const (
	// TypeText represents text/string column type.
	TypeText ColumnType = iota
	// TypeInteger represents integer column type.
	TypeInteger
	// TypeReal represents floating-point column type.
	TypeReal
	// TypeDatetime represents datetime column type.
	TypeDatetime
)

// String returns the string representation of ColumnType.
func (ct ColumnType) String() string {
	switch ct {
	case TypeText:
		return "TEXT"
	case TypeInteger:
		return "INTEGER"
	case TypeReal:
		return "REAL"
	case TypeDatetime:
		return "DATETIME"
	default:
		return "TEXT"
	}
}

// TableData contains the parsed data from a file.
type TableData struct {
	// Headers contains the column names in order.
	Headers []string
	// Records contains the data rows. Each record is a slice of string values.
	Records [][]string
	// ColumnTypes contains the inferred types for each column.
	// The length matches Headers.
	ColumnTypes []ColumnType
}

// Parse reads data from an io.Reader and returns parsed results.
// The fileType parameter specifies the format and compression of the data.
//
// Example:
//
//	f, _ := os.Open("data.csv.gz")
//	defer f.Close()
//	result, err := parser.Parse(f, parser.CSVGZ)
func Parse(reader io.Reader, fileType FileType) (result *TableData, err error) {
	if reader == nil {
		return nil, errors.New("reader cannot be nil")
	}

	// Handle decompression
	decompressedReader, closeFunc, decompErr := createDecompressedReader(reader, fileType)
	if decompErr != nil {
		return nil, fmt.Errorf("failed to decompress: %w", decompErr)
	}
	if closeFunc != nil {
		defer func() {
			if closeErr := closeFunc(); closeErr != nil && err == nil {
				err = fmt.Errorf("failed to close decompressor: %w", closeErr)
			}
		}()
	}

	// Parse based on base file type
	baseType := BaseFileType(fileType)
	switch baseType {
	case CSV:
		return parseDelimited(decompressedReader, ',', "CSV")
	case TSV:
		return parseDelimited(decompressedReader, '\t', "TSV")
	case LTSV:
		return parseLTSV(decompressedReader)
	case Parquet:
		return parseParquet(decompressedReader)
	case XLSX:
		return parseXLSX(decompressedReader)
	default:
		return nil, errors.New("unsupported file type")
	}
}

// File extensions
const (
	ExtCSV     = ".csv"
	ExtTSV     = ".tsv"
	ExtLTSV    = ".ltsv"
	ExtParquet = ".parquet"
	ExtXLSX    = ".xlsx"
	ExtGZ      = ".gz"
	ExtBZ2     = ".bz2"
	ExtXZ      = ".xz"
	ExtZSTD    = ".zst"
)

// Compression type identifiers
const (
	compGZ   = "gz"
	compBZ2  = "bz2"
	compXZ   = "xz"
	compZSTD = "zstd"
)

// DetectFileType detects file type from path extension, including compression variants.
func DetectFileType(path string) FileType {
	basePath := path
	var compressionType string

	// Remove compression extensions
	switch {
	case strings.HasSuffix(strings.ToLower(path), ExtGZ):
		basePath = path[:len(path)-len(ExtGZ)]
		compressionType = compGZ
	case strings.HasSuffix(strings.ToLower(path), ExtBZ2):
		basePath = path[:len(path)-len(ExtBZ2)]
		compressionType = compBZ2
	case strings.HasSuffix(strings.ToLower(path), ExtXZ):
		basePath = path[:len(path)-len(ExtXZ)]
		compressionType = compXZ
	case strings.HasSuffix(strings.ToLower(path), ExtZSTD):
		basePath = path[:len(path)-len(ExtZSTD)]
		compressionType = compZSTD
	}

	ext := strings.ToLower(filepath.Ext(basePath))
	switch ext {
	case ExtCSV:
		switch compressionType {
		case compGZ:
			return CSVGZ
		case compBZ2:
			return CSVBZ2
		case compXZ:
			return CSVXZ
		case compZSTD:
			return CSVZSTD
		default:
			return CSV
		}
	case ExtTSV:
		switch compressionType {
		case compGZ:
			return TSVGZ
		case compBZ2:
			return TSVBZ2
		case compXZ:
			return TSVXZ
		case compZSTD:
			return TSVZSTD
		default:
			return TSV
		}
	case ExtLTSV:
		switch compressionType {
		case compGZ:
			return LTSVGZ
		case compBZ2:
			return LTSVBZ2
		case compXZ:
			return LTSVXZ
		case compZSTD:
			return LTSVZSTD
		default:
			return LTSV
		}
	case ExtParquet:
		switch compressionType {
		case compGZ:
			return ParquetGZ
		case compBZ2:
			return ParquetBZ2
		case compXZ:
			return ParquetXZ
		case compZSTD:
			return ParquetZSTD
		default:
			return Parquet
		}
	case ExtXLSX:
		switch compressionType {
		case compGZ:
			return XLSXGZ
		case compBZ2:
			return XLSXBZ2
		case compXZ:
			return XLSXXZ
		case compZSTD:
			return XLSXZSTD
		default:
			return XLSX
		}
	default:
		return Unsupported
	}
}

// BaseFileType returns the base file type without compression.
func BaseFileType(ft FileType) FileType {
	switch ft {
	case CSV, CSVGZ, CSVBZ2, CSVXZ, CSVZSTD:
		return CSV
	case TSV, TSVGZ, TSVBZ2, TSVXZ, TSVZSTD:
		return TSV
	case LTSV, LTSVGZ, LTSVBZ2, LTSVXZ, LTSVZSTD:
		return LTSV
	case Parquet, ParquetGZ, ParquetBZ2, ParquetXZ, ParquetZSTD:
		return Parquet
	case XLSX, XLSXGZ, XLSXBZ2, XLSXXZ, XLSXZSTD:
		return XLSX
	default:
		return Unsupported
	}
}

// createDecompressedReader wraps the reader with appropriate decompression.
func createDecompressedReader(reader io.Reader, fileType FileType) (io.Reader, func() error, error) {
	switch fileType {
	case CSVGZ, TSVGZ, LTSVGZ, XLSXGZ, ParquetGZ:
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzReader, func() error { return gzReader.Close() }, nil

	case CSVBZ2, TSVBZ2, LTSVBZ2, XLSXBZ2, ParquetBZ2:
		bz2Reader := bzip2.NewReader(reader)
		return bz2Reader, nil, nil

	case CSVXZ, TSVXZ, LTSVXZ, XLSXXZ, ParquetXZ:
		xzReader, err := xz.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create xz reader: %w", err)
		}
		return xzReader, nil, nil

	case CSVZSTD, TSVZSTD, LTSVZSTD, XLSXZSTD, ParquetZSTD:
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		return decoder, func() error { decoder.Close(); return nil }, nil

	default:
		// No compression
		return reader, nil, nil
	}
}

// parseDelimited parses CSV or TSV data.
func parseDelimited(reader io.Reader, delimiter rune, fileTypeName string) (*TableData, error) {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = delimiter

	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", fileTypeName, err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("empty %s data", fileTypeName)
	}

	headers := records[0]
	if err := validateColumnNames(headers); err != nil {
		return nil, err
	}

	dataRecords := make([][]string, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		dataRecords = append(dataRecords, records[i])
	}

	// Infer column types
	columnTypes := inferColumnTypes(headers, dataRecords)

	return &TableData{
		Headers:     headers,
		Records:     dataRecords,
		ColumnTypes: columnTypes,
	}, nil
}

// parseLTSV parses LTSV (Labeled Tab-Separated Values) data.
// Column order is preserved as first-seen order for deterministic output.
func parseLTSV(reader io.Reader) (*TableData, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read LTSV: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return nil, errors.New("empty LTSV data")
	}

	// Use slice to preserve first-seen order
	var headers []string
	headerSeen := make(map[string]bool)
	var parsedRecords []map[string]string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		recordMap := make(map[string]string)
		pairs := strings.Split(line, "\t")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				recordMap[key] = value
				// Track headers in first-seen order
				if !headerSeen[key] {
					headerSeen[key] = true
					headers = append(headers, key)
				}
			}
		}
		if len(recordMap) > 0 {
			parsedRecords = append(parsedRecords, recordMap)
		}
	}

	if len(parsedRecords) == 0 {
		return nil, errors.New("no valid LTSV records found")
	}

	// Convert to records using first-seen header order
	records := make([][]string, 0, len(parsedRecords))
	for _, recordMap := range parsedRecords {
		row := make([]string, len(headers))
		for i, key := range headers {
			if val, exists := recordMap[key]; exists {
				row[i] = val
			} else {
				row[i] = ""
			}
		}
		records = append(records, row)
	}

	// Infer column types
	columnTypes := inferColumnTypes(headers, records)

	return &TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}, nil
}

// validateColumnNames checks for duplicate column names.
func validateColumnNames(columns []string) error {
	seen := make(map[string]bool, len(columns))
	for _, col := range columns {
		if seen[col] {
			return fmt.Errorf("duplicate column name: %s", col)
		}
		seen[col] = true
	}
	return nil
}
