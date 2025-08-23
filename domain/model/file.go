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

// FileType represents supported file types
type FileType int

const (
	FileTypeCSV FileType = iota
	FileTypeTSV
	FileTypeLTSV
	FileTypeUnsupported
)

// File represents a file that can be converted to Table
type File struct {
	path     string
	fileType FileType
}

// NewFile creates a new File
func NewFile(path string) *File {
	return &File{
		path:     path,
		fileType: detectFileType(path),
	}
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
	return strings.HasSuffix(f.path, ".gz")
}

// IsBZ2 returns true if file is bzip2 compressed
func (f *File) IsBZ2() bool {
	return strings.HasSuffix(f.path, ".bz2")
}

// IsXZ returns true if file is xz compressed
func (f *File) IsXZ() bool {
	return strings.HasSuffix(f.path, ".xz")
}

// IsZSTD returns true if file is zstd compressed
func (f *File) IsZSTD() bool {
	return strings.HasSuffix(f.path, ".zst")
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
	if strings.HasSuffix(path, ".gz") {
		basePath = strings.TrimSuffix(path, ".gz")
	} else if strings.HasSuffix(path, ".bz2") {
		basePath = strings.TrimSuffix(path, ".bz2")
	} else if strings.HasSuffix(path, ".xz") {
		basePath = strings.TrimSuffix(path, ".xz")
	} else if strings.HasSuffix(path, ".zst") {
		basePath = strings.TrimSuffix(path, ".zst")
	}
	
	ext := strings.ToLower(filepath.Ext(basePath))
	switch ext {
	case ".csv":
		return FileTypeCSV
	case ".tsv":
		return FileTypeTSV
	case ".ltsv":
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
			file.Close()
			return nil, nil, err
		}
		reader = gzReader
		closer = func() error {
			gzReader.Close()
			return file.Close()
		}
	} else if f.IsBZ2() {
		reader = bzip2.NewReader(file)
		closer = file.Close
	} else if f.IsXZ() {
		xzReader, err := xz.NewReader(file)
		if err != nil {
			file.Close()
			return nil, nil, err
		}
		reader = xzReader
		closer = file.Close
	} else if f.IsZSTD() {
		decoder, err := zstd.NewReader(file)
		if err != nil {
			file.Close()
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
	var tableRecords []Record
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
	var tableRecords []Record
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

	var tableRecords []Record
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