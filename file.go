package filesql

import (
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
	// FileTypeCSVGZ represents gzip-compressed CSV file type
	FileTypeCSVGZ
	// FileTypeTSVGZ represents gzip-compressed TSV file type
	FileTypeTSVGZ
	// FileTypeLTSVGZ represents gzip-compressed LTSV file type
	FileTypeLTSVGZ
	// FileTypeCSVBZ2 represents bzip2-compressed CSV file type
	FileTypeCSVBZ2
	// FileTypeTSVBZ2 represents bzip2-compressed TSV file type
	FileTypeTSVBZ2
	// FileTypeLTSVBZ2 represents bzip2-compressed LTSV file type
	FileTypeLTSVBZ2
	// FileTypeCSVXZ represents xz-compressed CSV file type
	FileTypeCSVXZ
	// FileTypeTSVXZ represents xz-compressed TSV file type
	FileTypeTSVXZ
	// FileTypeLTSVXZ represents xz-compressed LTSV file type
	FileTypeLTSVXZ
	// FileTypeCSVZSTD represents zstd-compressed CSV file type
	FileTypeCSVZSTD
	// FileTypeTSVZSTD represents zstd-compressed TSV file type
	FileTypeTSVZSTD
	// FileTypeLTSVZSTD represents zstd-compressed LTSV file type
	FileTypeLTSVZSTD
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
	chunkSize int
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
	baseExts := []string{extCSV, extTSV, extLTSV}
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
		strings.HasSuffix(fileName, extLTSV)
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
	case FileTypeCSVGZ:
		return extCSV + extGZ
	case FileTypeTSVGZ:
		return extTSV + extGZ
	case FileTypeLTSVGZ:
		return extLTSV + extGZ
	case FileTypeCSVBZ2:
		return extCSV + extBZ2
	case FileTypeTSVBZ2:
		return extTSV + extBZ2
	case FileTypeLTSVBZ2:
		return extLTSV + extBZ2
	case FileTypeCSVXZ:
		return extCSV + extXZ
	case FileTypeTSVXZ:
		return extTSV + extXZ
	case FileTypeLTSVXZ:
		return extLTSV + extXZ
	case FileTypeCSVZSTD:
		return extCSV + extZSTD
	case FileTypeTSVZSTD:
		return extTSV + extZSTD
	case FileTypeLTSVZSTD:
		return extLTSV + extZSTD
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
	return f.getFileType() == FileTypeCSV
}

// isTSV returns true if the file is TSV format
func (f *file) isTSV() bool {
	return f.getFileType() == FileTypeTSV
}

// isLTSV returns true if the file is LTSV format
func (f *file) isLTSV() bool {
	return f.getFileType() == FileTypeLTSV
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
	switch f.getFileType() {
	case FileTypeCSV:
		return f.parseCSV()
	case FileTypeTSV:
		return f.parseTSV()
	case FileTypeLTSV:
		return f.parseLTSV()
	default:
		return nil, fmt.Errorf("unsupported file type: %s", f.getPath())
	}
}

// detectFileType detects file type from extension, considering compressed files
func detectFileType(path string) FileType {
	basePath := path

	// Remove compression extensions
	if strings.HasSuffix(path, extGZ) {
		basePath = strings.TrimSuffix(path, extGZ)
	} else if strings.HasSuffix(path, extBZ2) {
		basePath = strings.TrimSuffix(path, extBZ2)
	} else if strings.HasSuffix(path, extXZ) {
		basePath = strings.TrimSuffix(path, extXZ)
	} else if strings.HasSuffix(path, extZSTD) {
		basePath = strings.TrimSuffix(path, extZSTD)
	}

	ext := strings.ToLower(filepath.Ext(basePath))
	switch ext {
	case extCSV:
		return FileTypeCSV
	case extTSV:
		return FileTypeTSV
	case extLTSV:
		return FileTypeLTSV
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

// parseCSV parses CSV file with compression support
func (f *file) parseCSV() (*table, error) {
	reader, closer, err := f.openReader()
	if err != nil {
		return nil, err
	}
	defer closer()

	csvReader := csv.NewReader(reader)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("empty file: %s", f.path)
	}

	header := newHeader(records[0])
	// Check for duplicate column names
	columnsSeen := make(map[string]bool)
	for _, col := range records[0] {
		if columnsSeen[col] {
			return nil, fmt.Errorf("%w: %s", errDuplicateColumnName, col)
		}
		columnsSeen[col] = true
	}

	tableRecords := make([]record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		tableRecords = append(tableRecords, newRecord(records[i]))
	}

	tableName := tableFromFilePath(f.path)
	return newTable(tableName, header, tableRecords), nil
}

// parseTSV parses TSV file with compression support
func (f *file) parseTSV() (*table, error) {
	reader, closer, err := f.openReader()
	if err != nil {
		return nil, err
	}
	defer closer()

	csvReader := csv.NewReader(reader)
	csvReader.Comma = TSVDelimiter
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("empty file: %s", f.path)
	}

	header := newHeader(records[0])
	// Check for duplicate column names
	columnsSeen := make(map[string]bool)
	for _, col := range records[0] {
		if columnsSeen[col] {
			return nil, fmt.Errorf("%w: %s", errDuplicateColumnName, col)
		}
		columnsSeen[col] = true
	}

	tableRecords := make([]record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		tableRecords = append(tableRecords, newRecord(records[i]))
	}

	tableName := tableFromFilePath(f.path)
	return newTable(tableName, header, tableRecords), nil
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
