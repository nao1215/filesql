// Package model provides domain model for filesql
package model

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ErrDuplicateColumnName is returned when a file contains duplicate column names
var ErrDuplicateColumnName = errors.New("duplicate column name")

// Header is file header.
type Header []string

// NewHeader create new Header.
func NewHeader(h []string) Header {
	return Header(h)
}

// Equal compare Header.
func (h Header) Equal(h2 Header) bool {
	if len(h) != len(h2) {
		return false
	}
	for i, v := range h {
		if v != h2[i] {
			return false
		}
	}
	return true
}

// Record is file records.
type Record []string

// NewRecord create new Record.
func NewRecord(r []string) Record {
	return Record(r)
}

// Equal compare Record.
func (r Record) Equal(r2 Record) bool {
	if len(r) != len(r2) {
		return false
	}
	for i, v := range r {
		if v != r2[i] {
			return false
		}
	}
	return true
}

// ColumnType represents the SQL column type
type ColumnType int

const (
	// ColumnTypeText represents TEXT column type
	ColumnTypeText ColumnType = iota
	// ColumnTypeInteger represents INTEGER column type
	ColumnTypeInteger
	// ColumnTypeReal represents REAL column type
	ColumnTypeReal
	// ColumnTypeDatetime represents datetime stored as TEXT in ISO8601 format
	ColumnTypeDatetime
)

const (
	// SQLTypeText is the SQL TEXT type string
	sqlTypeText = "TEXT"
	// SQLTypeInteger is the SQL INTEGER type string
	sqlTypeInteger = "INTEGER"
	// SQLTypeReal is the SQL REAL type string
	sqlTypeReal = "REAL"
)

// String returns the SQL column type string
func (ct ColumnType) String() string {
	switch ct {
	case ColumnTypeText:
		return sqlTypeText
	case ColumnTypeInteger:
		return sqlTypeInteger
	case ColumnTypeReal:
		return sqlTypeReal
	case ColumnTypeDatetime:
		return sqlTypeText // SQLite stores datetime as TEXT in ISO8601 format
	default:
		return sqlTypeText
	}
}

// ColumnInfo represents column information with name and inferred type
type ColumnInfo struct {
	Name string
	Type ColumnType
}

// Common datetime patterns to detect
var datetimePatterns = []struct {
	pattern *regexp.Regexp
	formats []string // Multiple formats for the same pattern
}{
	// ISO8601 formats with timezone
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`),
		[]string{time.RFC3339, time.RFC3339Nano},
	},
	// ISO8601 formats without timezone
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$`),
		[]string{"2006-01-02T15:04:05", "2006-01-02T15:04:05.000"},
	},
	// ISO8601 date and time with space
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$`),
		[]string{"2006-01-02 15:04:05", "2006-01-02 15:04:05.000"},
	},
	// ISO8601 date only
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`),
		[]string{"2006-01-02"},
	},
	// US formats
	{
		regexp.MustCompile(`^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}( (AM|PM))?$`),
		[]string{"1/2/2006 15:04:05", "1/2/2006 3:04:05 PM", "01/02/2006 15:04:05"},
	},
	{
		regexp.MustCompile(`^\d{1,2}/\d{1,2}/\d{4}$`),
		[]string{"1/2/2006", "01/02/2006"},
	},
	// European formats
	{
		regexp.MustCompile(`^\d{1,2}\.\d{1,2}\.\d{4} \d{1,2}:\d{2}:\d{2}$`),
		[]string{"2.1.2006 15:04:05", "02.01.2006 15:04:05"},
	},
	{
		regexp.MustCompile(`^\d{1,2}\.\d{1,2}\.\d{4}$`),
		[]string{"2.1.2006", "02.01.2006"},
	},
	// Time only
	{
		regexp.MustCompile(`^\d{1,2}:\d{2}:\d{2}(\.\d+)?$`),
		[]string{"15:04:05", "15:04:05.000", "3:04:05"},
	},
	{
		regexp.MustCompile(`^\d{1,2}:\d{2}$`),
		[]string{"15:04", "3:04"},
	},
}

// isDatetime checks if a string value represents a datetime
func isDatetime(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}

	for _, dp := range datetimePatterns {
		if dp.pattern.MatchString(value) {
			// Try each format for this pattern
			for _, format := range dp.formats {
				if _, err := time.Parse(format, value); err == nil {
					return true
				}
			}
		}
	}

	return false
}

// InferColumnType infers the SQL column type from a slice of string values
func InferColumnType(values []string) ColumnType {
	if len(values) == 0 {
		return ColumnTypeText
	}

	hasDatetime := false
	hasReal := false
	hasInteger := false
	hasText := false

	for _, value := range values {
		// Skip empty values for type inference
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		// Check if it's a datetime first (before checking numbers)
		if isDatetime(value) {
			hasDatetime = true
			continue
		}

		// Try to parse as integer
		if _, err := strconv.ParseInt(value, 10, 64); err == nil {
			hasInteger = true
			continue
		}

		// Try to parse as float
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			hasReal = true
			continue
		}

		// If it's not a number or datetime, it's text
		hasText = true
		break // If any value is text, the whole column is text
	}

	// Determine the most appropriate type
	// Priority: TEXT > DATETIME > REAL > INTEGER
	if hasText {
		return ColumnTypeText
	}
	if hasDatetime {
		return ColumnTypeDatetime
	}
	if hasReal {
		return ColumnTypeReal
	}
	if hasInteger {
		return ColumnTypeInteger
	}

	// Default to TEXT if no values were found
	return ColumnTypeText
}

// InferColumnsInfo infers column information from header and data records
func InferColumnsInfo(header Header, records []Record) []ColumnInfo {
	columnCount := len(header)
	if columnCount == 0 {
		return nil
	}

	columns := make([]ColumnInfo, columnCount)

	// Initialize column info with headers
	for i, name := range header {
		columns[i] = ColumnInfo{
			Name: name,
			Type: ColumnTypeText, // Default to TEXT
		}
	}

	// If no records, return with TEXT types
	if len(records) == 0 {
		return columns
	}

	// Collect values for each column
	for i := range columnCount {
		var values []string
		for _, record := range records {
			if i < len(record) {
				values = append(values, record[i])
			}
		}

		// Infer type from values
		columns[i].Type = InferColumnType(values)
	}

	return columns
}

// OutputFormat represents the output file format
type OutputFormat int

const (
	// OutputFormatCSV represents CSV output format
	OutputFormatCSV OutputFormat = iota
	// OutputFormatTSV represents TSV output format
	OutputFormatTSV
	// OutputFormatLTSV represents LTSV output format
	OutputFormatLTSV
)

// String returns the string representation of OutputFormat
func (f OutputFormat) String() string {
	switch f {
	case OutputFormatCSV:
		return "csv"
	case OutputFormatTSV:
		return "tsv"
	case OutputFormatLTSV:
		return "ltsv"
	default:
		return "csv"
	}
}

// Extension returns the file extension for the format
func (f OutputFormat) Extension() string {
	switch f {
	case OutputFormatCSV:
		return ".csv"
	case OutputFormatTSV:
		return ".tsv"
	case OutputFormatLTSV:
		return ".ltsv"
	default:
		return ".csv"
	}
}

// CompressionType represents the compression type
type CompressionType int

const (
	// CompressionNone represents no compression
	CompressionNone CompressionType = iota
	// CompressionGZ represents gzip compression
	CompressionGZ
	// CompressionBZ2 represents bzip2 compression
	CompressionBZ2
	// CompressionXZ represents xz compression
	CompressionXZ
	// CompressionZSTD represents zstd compression
	CompressionZSTD
)

// String returns the string representation of CompressionType
func (c CompressionType) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionGZ:
		return "gz"
	case CompressionBZ2:
		return "bz2"
	case CompressionXZ:
		return "xz"
	case CompressionZSTD:
		return "zstd"
	default:
		return "none"
	}
}

// Extension returns the file extension for the compression type
func (c CompressionType) Extension() string {
	switch c {
	case CompressionNone:
		return ""
	case CompressionGZ:
		return ".gz"
	case CompressionBZ2:
		return ".bz2"
	case CompressionXZ:
		return ".xz"
	case CompressionZSTD:
		return ".zst"
	default:
		return ""
	}
}

// DumpOptions represents options for dumping database
type DumpOptions struct {
	// Format specifies the output file format
	Format OutputFormat
	// Compression specifies the compression type
	Compression CompressionType
}

// NewDumpOptions creates new DumpOptions with default values (CSV format, no compression)
func NewDumpOptions() DumpOptions {
	return DumpOptions{
		Format:      OutputFormatCSV,
		Compression: CompressionNone,
	}
}

// WithFormat sets the output format
func (o DumpOptions) WithFormat(format OutputFormat) DumpOptions {
	o.Format = format
	return o
}

// WithCompression sets the compression type
func (o DumpOptions) WithCompression(compression CompressionType) DumpOptions {
	o.Compression = compression
	return o
}

// FileExtension returns the complete file extension including compression
func (o DumpOptions) FileExtension() string {
	baseExt := o.Format.Extension()
	compExt := o.Compression.Extension()
	return baseExt + compExt
}
