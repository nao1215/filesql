package model

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
	// ExtCSV is the CSV file extension
	ExtCSV = ".csv"
	// ExtTSV is the TSV file extension
	ExtTSV = ".tsv"
	// ExtLTSV is the LTSV file extension
	ExtLTSV = ".ltsv"
	// ExtGZ is the gzip compression extension
	ExtGZ = ".gz"
	// ExtBZ2 is the bzip2 compression extension
	ExtBZ2 = ".bz2"
	// ExtXZ is the xz compression extension
	ExtXZ = ".xz"
	// ExtZSTD is the zstd compression extension
	ExtZSTD = ".zst"
)

// File represents a file that can be converted to Table
type File struct {
	path     string
	fileType FileType
}

// TableChunk represents a chunk of table data for streaming processing
type TableChunk struct {
	tableName  string
	headers    Header
	records    []Record
	columnInfo []ColumnInfo
}

// TableName returns the name of the table
func (tc *TableChunk) TableName() string {
	return tc.tableName
}

// Headers returns the table headers
func (tc *TableChunk) Headers() Header {
	return tc.headers
}

// Records returns the records in this chunk
func (tc *TableChunk) Records() []Record {
	return tc.records
}

// ColumnInfo returns the column information with inferred types
func (tc *TableChunk) ColumnInfo() []ColumnInfo {
	return tc.columnInfo
}

// ChunkProcessor is a function type for processing table chunks
type ChunkProcessor func(chunk *TableChunk) error

// StreamingParser represents a parser that can read from io.Reader directly
type StreamingParser struct {
	fileType  FileType
	tableName string
	chunkSize int
}

// NewFile creates a new File
func NewFile(path string) *File {
	return &File{
		path:     path,
		fileType: detectFileType(path),
	}
}

// SupportedFileExtPatterns returns all supported file patterns for glob matching
func SupportedFileExtPatterns() []string {
	baseExts := []string{ExtCSV, ExtTSV, ExtLTSV}
	compressionExts := []string{"", ExtGZ, ExtBZ2, ExtXZ, ExtZSTD}

	var patterns []string
	for _, baseExt := range baseExts {
		for _, compressionExt := range compressionExts {
			pattern := "*" + baseExt + compressionExt
			patterns = append(patterns, pattern)
		}
	}
	return patterns
}

// IsSupportedFile checks if the file has a supported extension
func IsSupportedFile(fileName string) bool {
	fileName = strings.ToLower(fileName)

	// Remove compression extensions
	for _, ext := range []string{ExtGZ, ExtBZ2, ExtXZ, ExtZSTD} {
		if strings.HasSuffix(fileName, ext) {
			fileName = strings.TrimSuffix(fileName, ext)
			break
		}
	}

	// Check for supported file extensions
	return strings.HasSuffix(fileName, ExtCSV) ||
		strings.HasSuffix(fileName, ExtTSV) ||
		strings.HasSuffix(fileName, ExtLTSV)
}

// IsSupportedExtension checks if the given extension is supported
// The extension should start with a dot (e.g., ".csv", ".tsv.gz")
func IsSupportedExtension(ext string) bool {
	ext = strings.ToLower(ext)

	// Check if it's a simple extension or has compression
	return IsSupportedFile("file" + ext)
}

// Extension returns the file extension for the FileType
func (ft FileType) Extension() string {
	switch ft {
	case FileTypeCSV:
		return ExtCSV
	case FileTypeTSV:
		return ExtTSV
	case FileTypeLTSV:
		return ExtLTSV
	case FileTypeCSVGZ:
		return ExtCSV + ExtGZ
	case FileTypeTSVGZ:
		return ExtTSV + ExtGZ
	case FileTypeLTSVGZ:
		return ExtLTSV + ExtGZ
	case FileTypeCSVBZ2:
		return ExtCSV + ExtBZ2
	case FileTypeTSVBZ2:
		return ExtTSV + ExtBZ2
	case FileTypeLTSVBZ2:
		return ExtLTSV + ExtBZ2
	case FileTypeCSVXZ:
		return ExtCSV + ExtXZ
	case FileTypeTSVXZ:
		return ExtTSV + ExtXZ
	case FileTypeLTSVXZ:
		return ExtLTSV + ExtXZ
	case FileTypeCSVZSTD:
		return ExtCSV + ExtZSTD
	case FileTypeTSVZSTD:
		return ExtTSV + ExtZSTD
	case FileTypeLTSVZSTD:
		return ExtLTSV + ExtZSTD
	default:
		return ""
	}
}

// BaseType returns the base file type without compression
func (ft FileType) BaseType() FileType {
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

// GetFileExtension returns the file extension for a given FileType
// Deprecated: Use FileType.Extension() method instead
func GetFileExtension(fileType FileType) string {
	return fileType.Extension()
}

// GetBaseFileType returns the base file type without compression
// Deprecated: Use FileType.BaseType() method instead
func GetBaseFileType(fileType FileType) FileType {
	return fileType.BaseType()
}

// Path returns file path
func (f *File) Path() string {
	return f.path
}

// Type returns file type
func (f *File) Type() FileType {
	return f.fileType
}

// IsCSV returns true if the file is CSV format
func (f *File) IsCSV() bool {
	return f.fileType == FileTypeCSV
}

// IsTSV returns true if the file is TSV format
func (f *File) IsTSV() bool {
	return f.fileType == FileTypeTSV
}

// IsLTSV returns true if the file is LTSV format
func (f *File) IsLTSV() bool {
	return f.fileType == FileTypeLTSV
}

// IsCompressed returns true if file is compressed
func (f *File) IsCompressed() bool {
	return f.IsGZ() || f.IsBZ2() || f.IsXZ() || f.IsZSTD()
}

// IsGZ returns true if file is gzip compressed
func (f *File) IsGZ() bool {
	return strings.HasSuffix(f.path, ExtGZ)
}

// IsBZ2 returns true if file is bzip2 compressed
func (f *File) IsBZ2() bool {
	return strings.HasSuffix(f.path, ExtBZ2)
}

// IsXZ returns true if file is xz compressed
func (f *File) IsXZ() bool {
	return strings.HasSuffix(f.path, ExtXZ)
}

// IsZSTD returns true if file is zstd compressed
func (f *File) IsZSTD() bool {
	return strings.HasSuffix(f.path, ExtZSTD)
}

// ToTable converts file to Table structure
func (f *File) ToTable() (*Table, error) {
	switch f.fileType {
	case FileTypeCSV:
		return f.parseCSV()
	case FileTypeTSV:
		return f.parseTSV()
	case FileTypeLTSV:
		return f.parseLTSV()
	default:
		return nil, fmt.Errorf("unsupported file type: %s", f.path)
	}
}

// detectFileType detects file type from extension, considering compressed files
func detectFileType(path string) FileType {
	basePath := path

	// Remove compression extensions
	if strings.HasSuffix(path, ExtGZ) {
		basePath = strings.TrimSuffix(path, ExtGZ)
	} else if strings.HasSuffix(path, ExtBZ2) {
		basePath = strings.TrimSuffix(path, ExtBZ2)
	} else if strings.HasSuffix(path, ExtXZ) {
		basePath = strings.TrimSuffix(path, ExtXZ)
	} else if strings.HasSuffix(path, ExtZSTD) {
		basePath = strings.TrimSuffix(path, ExtZSTD)
	}

	ext := strings.ToLower(filepath.Ext(basePath))
	switch ext {
	case ExtCSV:
		return FileTypeCSV
	case ExtTSV:
		return FileTypeTSV
	case ExtLTSV:
		return FileTypeLTSV
	default:
		return FileTypeUnsupported
	}
}

// openReader opens file and returns a reader that handles compression
func (f *File) openReader() (io.Reader, func() error, error) {
	file, err := os.Open(f.path)
	if err != nil {
		return nil, nil, err
	}

	var reader io.Reader = file
	closer := file.Close

	if f.IsGZ() {
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
	} else if f.IsBZ2() {
		reader = bzip2.NewReader(file)
		closer = file.Close
	} else if f.IsXZ() {
		xzReader, err := xz.NewReader(file)
		if err != nil {
			_ = file.Close() // Ignore close error during error handling
			return nil, nil, err
		}
		reader = xzReader
		closer = file.Close
	} else if f.IsZSTD() {
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
func (f *File) parseCSV() (*Table, error) {
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

	header := NewHeader(records[0])
	// Check for duplicate column names
	columnsSeen := make(map[string]bool)
	for _, col := range records[0] {
		if columnsSeen[col] {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateColumnName, col)
		}
		columnsSeen[col] = true
	}

	tableRecords := make([]Record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		tableRecords = append(tableRecords, NewRecord(records[i]))
	}

	tableName := TableFromFilePath(f.path)
	return NewTable(tableName, header, tableRecords), nil
}

// parseTSV parses TSV file with compression support
func (f *File) parseTSV() (*Table, error) {
	reader, closer, err := f.openReader()
	if err != nil {
		return nil, err
	}
	defer closer()

	csvReader := csv.NewReader(reader)
	csvReader.Comma = '\t'
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("empty file: %s", f.path)
	}

	header := NewHeader(records[0])
	// Check for duplicate column names
	columnsSeen := make(map[string]bool)
	for _, col := range records[0] {
		if columnsSeen[col] {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateColumnName, col)
		}
		columnsSeen[col] = true
	}

	tableRecords := make([]Record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		tableRecords = append(tableRecords, NewRecord(records[i]))
	}

	tableName := TableFromFilePath(f.path)
	return NewTable(tableName, header, tableRecords), nil
}

// parseLTSV parses LTSV file with compression support
func (f *File) parseLTSV() (*Table, error) {
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

	var header Header
	for key := range headerMap {
		header = append(header, key)
	}

	tableRecords := make([]Record, 0, len(records))
	for _, record := range records {
		var row Record
		for _, key := range header {
			if val, exists := record[key]; exists {
				row = append(row, val)
			} else {
				row = append(row, "")
			}
		}
		tableRecords = append(tableRecords, row)
	}

	tableName := TableFromFilePath(f.path)
	return NewTable(tableName, header, tableRecords), nil
}
