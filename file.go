package filesql

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
	"github.com/xuri/excelize/v2"
)

// FileType represents supported file types including compression variants
type FileType int

const (
	// FileTypeCSV represents CSV file type
	FileTypeCSV FileType = iota
	// FileTypeTSV represents TSV file type
	FileTypeTSV
	// FileTypeLTSV represents LTSV file type
	FileTypeLTSV
	// FileTypeParquet represents Parquet file type
	FileTypeParquet
	// FileTypeXLSX represents Excel XLSX file type
	FileTypeXLSX
	// FileTypeCSVGZ represents gzip-compressed CSV file type
	FileTypeCSVGZ
	// FileTypeTSVGZ represents gzip-compressed TSV file type
	FileTypeTSVGZ
	// FileTypeLTSVGZ represents gzip-compressed LTSV file type
	FileTypeLTSVGZ
	// FileTypeParquetGZ represents gzip-compressed Parquet file type
	FileTypeParquetGZ
	// FileTypeCSVBZ2 represents bzip2-compressed CSV file type
	FileTypeCSVBZ2
	// FileTypeTSVBZ2 represents bzip2-compressed TSV file type
	FileTypeTSVBZ2
	// FileTypeLTSVBZ2 represents bzip2-compressed LTSV file type
	FileTypeLTSVBZ2
	// FileTypeParquetBZ2 represents bzip2-compressed Parquet file type
	FileTypeParquetBZ2
	// FileTypeCSVXZ represents xz-compressed CSV file type
	FileTypeCSVXZ
	// FileTypeTSVXZ represents xz-compressed TSV file type
	FileTypeTSVXZ
	// FileTypeLTSVXZ represents xz-compressed LTSV file type
	FileTypeLTSVXZ
	// FileTypeParquetXZ represents xz-compressed Parquet file type
	FileTypeParquetXZ
	// FileTypeCSVZSTD represents zstd-compressed CSV file type
	FileTypeCSVZSTD
	// FileTypeTSVZSTD represents zstd-compressed TSV file type
	FileTypeTSVZSTD
	// FileTypeLTSVZSTD represents zstd-compressed LTSV file type
	FileTypeLTSVZSTD
	// FileTypeParquetZSTD represents zstd-compressed Parquet file type
	FileTypeParquetZSTD
	// FileTypeXLSXGZ represents gzip-compressed Excel XLSX file type
	FileTypeXLSXGZ
	// FileTypeXLSXBZ2 represents bzip2-compressed Excel XLSX file type
	FileTypeXLSXBZ2
	// FileTypeXLSXXZ represents xz-compressed Excel XLSX file type
	FileTypeXLSXXZ
	// FileTypeXLSXZSTD represents zstd-compressed Excel XLSX file type
	FileTypeXLSXZSTD
	// FileTypeUnsupported represents unsupported file type
	FileTypeUnsupported
)

// File extensions
const (
	// extCSV is the CSV file extension
	extCSV = ".csv"
	// extTSV is the TSV file extension
	extTSV = ".tsv"
	// extLTSV is the LTSV file extension
	extLTSV = ".ltsv"
	// extParquet is the Parquet file extension
	extParquet = ".parquet"
	// extXLSX is the Excel XLSX file extension
	extXLSX = ".xlsx"
	// extGZ is the gzip compression extension
	extGZ = ".gz"
	// extBZ2 is the bzip2 compression extension
	extBZ2 = ".bz2"
	// extXZ is the xz compression extension
	extXZ = ".xz"
	// extZSTD is the zstd compression extension
	extZSTD = ".zst"
)

// file represents a file that can be converted to table
type file struct {
	path     string
	fileType FileType
}

// tableChunk represents a chunk of table data for streaming processing
type tableChunk struct {
	tableName  string
	headers    header
	records    []record
	columnInfo []columnInfo
}

// getTableName returns the name of the table
func (tc *tableChunk) getTableName() string {
	return tc.tableName
}

// getHeaders returns the table headers
func (tc *tableChunk) getHeaders() header {
	return tc.headers
}

// getRecords returns the records in this chunk
func (tc *tableChunk) getRecords() []record {
	return tc.records
}

// getColumnInfo returns the column information with inferred types
func (tc *tableChunk) getColumnInfo() []columnInfo {
	return tc.columnInfo
}

// chunkProcessor is a function type for processing table chunks
type chunkProcessor func(chunk *tableChunk) error

// streamingParser represents a parser that can read from io.Reader directly
type streamingParser struct {
	fileType  FileType
	tableName string
	chunkSize ChunkSize
}

// newFile creates a new file
func newFile(path string) *file {
	return &file{
		path:     path,
		fileType: detectFileType(path),
	}
}

// supportedFileExtPatterns returns all supported file patterns for glob matching
func supportedFileExtPatterns() []string {
	baseExts := []string{extCSV, extTSV, extLTSV, extParquet, extXLSX}
	compressionExts := []string{"", extGZ, extBZ2, extXZ, extZSTD}

	var patterns []string
	for _, baseExt := range baseExts {
		for _, compressionExt := range compressionExts {
			pattern := "*" + baseExt + compressionExt
			patterns = append(patterns, pattern)
		}
	}
	return patterns
}

// isSupportedFile checks if the file has a supported extension
func isSupportedFile(fileName string) bool {
	fileName = strings.ToLower(fileName)

	// Remove compression extensions
	for _, ext := range []string{extGZ, extBZ2, extXZ, extZSTD} {
		if strings.HasSuffix(fileName, ext) {
			fileName = strings.TrimSuffix(fileName, ext)
			break
		}
	}

	// Check for supported file extensions
	return strings.HasSuffix(fileName, extCSV) ||
		strings.HasSuffix(fileName, extTSV) ||
		strings.HasSuffix(fileName, extLTSV) ||
		strings.HasSuffix(fileName, extParquet) ||
		strings.HasSuffix(fileName, extXLSX)
}

// isSupportedExtension checks if the given extension is supported
// The extension should start with a dot (e.g., ".csv", ".tsv.gz")
func isSupportedExtension(ext string) bool {
	ext = strings.ToLower(ext)

	// Check if it's a simple extension or has compression
	return isSupportedFile("file" + ext)
}

// extension returns the file extension for the FileType
func (ft FileType) extension() string {
	switch ft {
	case FileTypeCSV:
		return extCSV
	case FileTypeTSV:
		return extTSV
	case FileTypeLTSV:
		return extLTSV
	case FileTypeParquet:
		return extParquet
	case FileTypeXLSX:
		return extXLSX
	case FileTypeCSVGZ:
		return extCSV + extGZ
	case FileTypeTSVGZ:
		return extTSV + extGZ
	case FileTypeLTSVGZ:
		return extLTSV + extGZ
	case FileTypeParquetGZ:
		return extParquet + extGZ
	case FileTypeCSVBZ2:
		return extCSV + extBZ2
	case FileTypeTSVBZ2:
		return extTSV + extBZ2
	case FileTypeLTSVBZ2:
		return extLTSV + extBZ2
	case FileTypeParquetBZ2:
		return extParquet + extBZ2
	case FileTypeCSVXZ:
		return extCSV + extXZ
	case FileTypeTSVXZ:
		return extTSV + extXZ
	case FileTypeLTSVXZ:
		return extLTSV + extXZ
	case FileTypeParquetXZ:
		return extParquet + extXZ
	case FileTypeCSVZSTD:
		return extCSV + extZSTD
	case FileTypeTSVZSTD:
		return extTSV + extZSTD
	case FileTypeLTSVZSTD:
		return extLTSV + extZSTD
	case FileTypeParquetZSTD:
		return extParquet + extZSTD
	case FileTypeXLSXGZ:
		return extXLSX + extGZ
	case FileTypeXLSXBZ2:
		return extXLSX + extBZ2
	case FileTypeXLSXXZ:
		return extXLSX + extXZ
	case FileTypeXLSXZSTD:
		return extXLSX + extZSTD
	default:
		return ""
	}
}

// baseType returns the base file type without compression
func (ft FileType) baseType() FileType {
	switch ft {
	case FileTypeCSV, FileTypeCSVGZ, FileTypeCSVBZ2, FileTypeCSVXZ, FileTypeCSVZSTD:
		return FileTypeCSV
	case FileTypeTSV, FileTypeTSVGZ, FileTypeTSVBZ2, FileTypeTSVXZ, FileTypeTSVZSTD:
		return FileTypeTSV
	case FileTypeLTSV, FileTypeLTSVGZ, FileTypeLTSVBZ2, FileTypeLTSVXZ, FileTypeLTSVZSTD:
		return FileTypeLTSV
	case FileTypeParquet, FileTypeParquetGZ, FileTypeParquetBZ2, FileTypeParquetXZ, FileTypeParquetZSTD:
		return FileTypeParquet
	case FileTypeXLSX, FileTypeXLSXGZ, FileTypeXLSXBZ2, FileTypeXLSXXZ, FileTypeXLSXZSTD:
		return FileTypeXLSX
	default:
		return FileTypeUnsupported
	}
}

// getFileExtension returns the file extension for a given FileType
// Deprecated: Use FileType.extension() method instead
func getFileExtension(fileType FileType) string {
	return fileType.extension()
}

// getBaseFileType returns the base file type without compression
// Deprecated: Use FileType.baseType() method instead
func getBaseFileType(fileType FileType) FileType {
	return fileType.baseType()
}

// getPath returns file path
func (f *file) getPath() string {
	return f.path
}

// getFileType returns file type
func (f *file) getFileType() FileType {
	return f.fileType
}

// isCSV returns true if the file is CSV format
func (f *file) isCSV() bool {
	return f.getFileType().baseType() == FileTypeCSV
}

// isTSV returns true if the file is TSV format
func (f *file) isTSV() bool {
	return f.getFileType().baseType() == FileTypeTSV
}

// isLTSV returns true if the file is LTSV format
func (f *file) isLTSV() bool {
	return f.getFileType().baseType() == FileTypeLTSV
}

// isXLSX returns true if the file is XLSX format
func (f *file) isXLSX() bool {
	return f.getFileType().baseType() == FileTypeXLSX
}

// isCompressed returns true if file is compressed
func (f *file) isCompressed() bool {
	return f.isGZ() || f.isBZ2() || f.isXZ() || f.isZSTD()
}

// isGZ returns true if file is gzip compressed
func (f *file) isGZ() bool {
	return strings.HasSuffix(f.path, extGZ)
}

// isBZ2 returns true if file is bzip2 compressed
func (f *file) isBZ2() bool {
	return strings.HasSuffix(f.path, extBZ2)
}

// isXZ returns true if file is xz compressed
func (f *file) isXZ() bool {
	return strings.HasSuffix(f.path, extXZ)
}

// isZSTD returns true if file is zstd compressed
func (f *file) isZSTD() bool {
	return strings.HasSuffix(f.path, extZSTD)
}

// toTable converts file to table structure
func (f *file) toTable() (*table, error) {
	switch f.getFileType().baseType() {
	case FileTypeCSV:
		return f.parseCSV()
	case FileTypeTSV:
		return f.parseTSV()
	case FileTypeLTSV:
		return f.parseLTSV()
	case FileTypeParquet:
		return f.parseParquet()
	case FileTypeXLSX:
		return f.parseXLSX()
	default:
		return nil, fmt.Errorf("unsupported file type: %s", f.getPath())
	}
}

// detectFileType detects file type from extension, considering compressed files
func detectFileType(path string) FileType {
	basePath := path
	var compressionType string

	// Remove compression extensions
	if strings.HasSuffix(path, extGZ) {
		basePath = strings.TrimSuffix(path, extGZ)
		compressionType = compressionGZStr
	} else if strings.HasSuffix(path, extBZ2) {
		basePath = strings.TrimSuffix(path, extBZ2)
		compressionType = compressionBZ2Str
	} else if strings.HasSuffix(path, extXZ) {
		basePath = strings.TrimSuffix(path, extXZ)
		compressionType = compressionXZStr
	} else if strings.HasSuffix(path, extZSTD) {
		basePath = strings.TrimSuffix(path, extZSTD)
		compressionType = compressionZSTDStr
	}

	ext := strings.ToLower(filepath.Ext(basePath))
	switch ext {
	case extCSV:
		switch compressionType {
		case compressionGZStr:
			return FileTypeCSVGZ
		case compressionBZ2Str:
			return FileTypeCSVBZ2
		case compressionXZStr:
			return FileTypeCSVXZ
		case compressionZSTDStr:
			return FileTypeCSVZSTD
		default:
			return FileTypeCSV
		}
	case extTSV:
		switch compressionType {
		case compressionGZStr:
			return FileTypeTSVGZ
		case compressionBZ2Str:
			return FileTypeTSVBZ2
		case compressionXZStr:
			return FileTypeTSVXZ
		case compressionZSTDStr:
			return FileTypeTSVZSTD
		default:
			return FileTypeTSV
		}
	case extLTSV:
		switch compressionType {
		case compressionGZStr:
			return FileTypeLTSVGZ
		case compressionBZ2Str:
			return FileTypeLTSVBZ2
		case compressionXZStr:
			return FileTypeLTSVXZ
		case compressionZSTDStr:
			return FileTypeLTSVZSTD
		default:
			return FileTypeLTSV
		}
	case extParquet:
		switch compressionType {
		case compressionGZStr:
			return FileTypeParquetGZ
		case compressionBZ2Str:
			return FileTypeParquetBZ2
		case compressionXZStr:
			return FileTypeParquetXZ
		case compressionZSTDStr:
			return FileTypeParquetZSTD
		default:
			return FileTypeParquet
		}
	case extXLSX:
		switch compressionType {
		case compressionGZStr:
			return FileTypeXLSXGZ
		case compressionBZ2Str:
			return FileTypeXLSXBZ2
		case compressionXZStr:
			return FileTypeXLSXXZ
		case compressionZSTDStr:
			return FileTypeXLSXZSTD
		default:
			return FileTypeXLSX
		}
	default:
		return FileTypeUnsupported
	}
}

// openReader opens file and returns a reader that handles compression
func (f *file) openReader() (io.Reader, func() error, error) {
	file, err := os.Open(f.path)
	if err != nil {
		return nil, nil, err
	}

	var reader io.Reader = file
	closer := file.Close

	if f.isGZ() {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			_ = file.Close() // Ignore close error during error handling
			return nil, nil, err
		}
		reader = gzReader
		closer = func() error {
			_ = gzReader.Close() // Ignore close error in cleanup
			return file.Close()
		}
	} else if f.isBZ2() {
		reader = bzip2.NewReader(file)
		closer = file.Close
	} else if f.isXZ() {
		xzReader, err := xz.NewReader(file)
		if err != nil {
			_ = file.Close() // Ignore close error during error handling
			return nil, nil, err
		}
		reader = xzReader
		closer = file.Close
	} else if f.isZSTD() {
		decoder, err := zstd.NewReader(file)
		if err != nil {
			_ = file.Close() // Ignore close error during error handling
			return nil, nil, err
		}
		reader = decoder
		closer = func() error {
			decoder.Close()
			return file.Close()
		}
	}

	return reader, closer, nil
}

// parseDelimitedFile parses CSV or TSV files with specified delimiter
func (f *file) parseDelimitedFile(delimiter rune) (*table, error) {
	reader, closer, err := f.openReader()
	if err != nil {
		return nil, err
	}
	defer closer()

	csvReader := csv.NewReader(reader)
	csvReader.Comma = delimiter
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("empty file: %s", f.path)
	}

	header := newHeader(records[0])
	// Check for duplicate column names
	if err := validateColumnNames(records[0]); err != nil {
		return nil, err
	}

	tableRecords := make([]record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		tableRecords = append(tableRecords, newRecord(records[i]))
	}

	tableName := tableFromFilePath(f.path)
	return newTable(tableName, header, tableRecords), nil
}

// parseCSV parses CSV file with compression support
func (f *file) parseCSV() (*table, error) {
	return f.parseDelimitedFile(csvDelimiter)
}

// parseTSV parses TSV file with compression support
func (f *file) parseTSV() (*table, error) {
	return f.parseDelimitedFile(tsvDelimiter)
}

// parseLTSV parses LTSV file with compression support
func (f *file) parseLTSV() (*table, error) {
	reader, closer, err := f.openReader()
	if err != nil {
		return nil, err
	}
	defer closer()

	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty file: %s", f.path)
	}

	headerMap := make(map[string]bool)
	var records []map[string]string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		record := make(map[string]string)
		pairs := strings.Split(line, "\t")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				record[key] = value
				headerMap[key] = true
			}
		}
		if len(record) > 0 {
			records = append(records, record)
		}
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no valid records found: %s", f.path)
	}

	var header header
	for key := range headerMap {
		header = append(header, key)
	}

	tableRecords := make([]record, 0, len(records))
	for _, recordMap := range records {
		var row record
		for _, key := range header {
			if val, exists := recordMap[key]; exists {
				row = append(row, val)
			} else {
				row = append(row, "")
			}
		}
		tableRecords = append(tableRecords, row)
	}

	tableName := tableFromFilePath(f.path)
	return newTable(tableName, header, tableRecords), nil
}

// parseXLSX parses XLSX file with compression support
// Only supports single-sheet files for single table parsing.
// For multiple sheets, use filesql.Open() or filesql.OpenContext() for 1-sheet-1-table approach.
func (f *file) parseXLSX() (*table, error) {
	reader, closer, err := f.openReader()
	if err != nil {
		return nil, err
	}
	defer closer()

	// For XLSX files, we need to handle them specially since excelize needs a file path or bytes
	// If it's compressed, we need to read all data into memory first
	var xlsxFile *excelize.File

	if f.isCompressed() {
		// Read all data into memory for compressed files
		data, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		xlsxFile, err = excelize.OpenReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
	} else {
		// For uncompressed files, open directly
		xlsxFile, err = excelize.OpenFile(f.path)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		_ = xlsxFile.Close() // Ignore close error
	}()

	// Get all sheet names
	sheetNames := xlsxFile.GetSheetList()
	if len(sheetNames) == 0 {
		return nil, fmt.Errorf("no sheets found in Excel file: %s", f.path)
	}

	// With the new 1-sheet-1-table approach, we only parse the first sheet for single table parsing
	// For multiple sheets, we process only the first sheet (single table parsing limitation)
	// Users should use Open/OpenContext for full multi-sheet support with separate tables

	// Process the first sheet
	sheetName := sheetNames[0]
	rows, err := xlsxFile.GetRows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("failed to read sheet %s: %w", sheetName, err)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("sheet %s is empty in Excel file: %s", sheetName, f.path)
	}

	// Convert to standard table format
	headers, records := convertXLSXRowsToTable(rows)

	tableName := tableFromFilePath(f.path)
	return newTable(tableName, headers, records), nil
}

// convertXLSXRowsToTable converts XLSX rows to table headers and records
// First row becomes headers, remaining rows become records with padding
func convertXLSXRowsToTable(rows [][]string) (header, []record) {
	var headers header
	var records []record

	// First row as headers
	if len(rows) > 0 {
		headers = make(header, len(rows[0]))
		copy(headers, rows[0])
	}

	// Remaining rows as records
	if len(rows) > 1 {
		records = make([]record, len(rows)-1)
		for i, row := range rows[1:] {
			record := make(record, len(headers))
			for j := range headers {
				if j < len(row) {
					record[j] = row[j]
				} else {
					record[j] = "" // Pad with empty string if row is shorter
				}
			}
			records[i] = record
		}
	}

	return headers, records
}
