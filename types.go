package filesql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Processing constants (rows-based)
const (
	// DefaultRowsPerChunk is the default number of rows per chunk
	DefaultRowsPerChunk = 1000
	// DefaultChunkSize is the default chunk size (rows); alias for clarity
	DefaultChunkSize = DefaultRowsPerChunk
	// MinChunkSize is the minimum allowed rows per chunk
	MinChunkSize = 1
	// ValidationPeekSize is the size used for validation peek operations
	ValidationPeekSize = 1
)

// Character validation constants
const (
	// firstDigitChar represents the first numeric character
	firstDigitChar = '0'
	// lastDigitChar represents the last numeric character
	lastDigitChar = '9'
	// firstLowerChar represents the first lowercase letter
	firstLowerChar = 'a'
	// lastLowerChar represents the last lowercase letter
	lastLowerChar = 'z'
	// firstUpperChar represents the first uppercase letter
	firstUpperChar = 'A'
	// lastUpperChar represents the last uppercase letter
	lastUpperChar = 'Z'
	// underscoreChar represents the underscore character
	underscoreChar = '_'
)

// File format delimiters
const (
	// csvDelimiter is the delimiter for CSV files
	csvDelimiter = ','
	// tsvDelimiter is the delimiter for TSV files
	tsvDelimiter = '\t'
)

// TableName represents a table name with validation
type TableName struct {
	value string
}

// NewTableName creates a new TableName with validation
func NewTableName(name string) TableName {
	// Basic validation - table name cannot be empty
	if strings.TrimSpace(name) == "" {
		return TableName{value: "table"}
	}
	return TableName{value: strings.TrimSpace(name)}
}

// String returns the string representation of TableName
func (tn TableName) String() string {
	return tn.value
}

// Equal compares two table names
func (tn TableName) Equal(other TableName) bool {
	return tn.value == other.value
}

// Sanitize returns a sanitized version of the table name
func (tn TableName) Sanitize() TableName {
	return TableName{value: tn.sanitizeString()}
}

// sanitizeString removes invalid characters from table names
func (tn TableName) sanitizeString() string {
	// Replace spaces and invalid characters with underscores
	result := strings.ReplaceAll(tn.value, " ", "_")
	result = strings.ReplaceAll(result, "-", "_")
	result = strings.ReplaceAll(result, ".", "_")

	// Remove any non-alphanumeric characters except underscore
	var sanitized strings.Builder
	for _, r := range result {
		if (r >= firstLowerChar && r <= lastLowerChar) ||
			(r >= firstUpperChar && r <= lastUpperChar) ||
			(r >= firstDigitChar && r <= lastDigitChar) ||
			r == underscoreChar {
			sanitized.WriteRune(r)
		}
	}

	finalResult := sanitized.String()

	// Ensure it doesn't start with a number
	if len(finalResult) > 0 && finalResult[0] >= firstDigitChar && finalResult[0] <= lastDigitChar {
		finalResult = "table_" + finalResult
	}

	// Ensure it's not empty
	if finalResult == "" {
		finalResult = "table"
	}

	return finalResult
}

// header is file header.
type header []string

// newHeader create new header.
func newHeader(h []string) header {
	return header(h)
}

// equal compare header.
func (h header) equal(h2 header) bool {
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

// record is file records.
type record []string

// newRecord create new record.
func newRecord(r []string) record {
	return record(r)
}

// equal compare record.
func (r record) equal(r2 record) bool {
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

// columnType represents the SQL column type
type columnType int

const (
	// columnTypeText represents TEXT column type
	columnTypeText columnType = iota
	// columnTypeInteger represents INTEGER column type
	columnTypeInteger
	// columnTypeReal represents REAL column type
	columnTypeReal
	// columnTypeDatetime represents datetime stored as TEXT in ISO8601 format
	columnTypeDatetime
)

const (
	// SQLTypeText is the SQL TEXT type string
	sqlTypeText = "TEXT"
	// SQLTypeInteger is the SQL INTEGER type string
	sqlTypeInteger = "INTEGER"
	// SQLTypeReal is the SQL REAL type string
	sqlTypeReal = "REAL"
)

// string returns the SQL column type string
func (ct columnType) string() string {
	switch ct {
	case columnTypeText:
		return sqlTypeText
	case columnTypeInteger:
		return sqlTypeInteger
	case columnTypeReal:
		return sqlTypeReal
	case columnTypeDatetime:
		return sqlTypeText // SQLite stores datetime as TEXT in ISO8601 format
	default:
		return sqlTypeText
	}
}

// String returns the SQL column type string (public method)
func (ct columnType) String() string {
	return ct.string()
}

// validateColumnNames checks for duplicate column names and returns error if found.
// Column name comparison is case-sensitive to maintain backward compatibility.
func validateColumnNames(columns []string) error {
	columnsSeen := make(map[string]bool)
	for _, col := range columns {
		trimmedCol := strings.TrimSpace(col)
		if columnsSeen[trimmedCol] {
			return fmt.Errorf("%w: %s", errDuplicateColumnName, col)
		}
		columnsSeen[trimmedCol] = true
	}
	return nil
}

// ChunkSize represents a chunk size with validation
type ChunkSize int

// NewChunkSize creates a new ChunkSize with validation
func NewChunkSize(size int) ChunkSize {
	if size < MinChunkSize {
		return ChunkSize(DefaultRowsPerChunk)
	}
	return ChunkSize(size)
}

// Int returns the int value of ChunkSize
func (cs ChunkSize) Int() int {
	return int(cs)
}

// String returns the string representation of ChunkSize
func (cs ChunkSize) String() string {
	return strconv.Itoa(int(cs))
}

// IsValid checks if the chunk size is valid
func (cs ChunkSize) IsValid() bool {
	return int(cs) >= MinChunkSize
}

// columnInfo represents column information with name and inferred type
type columnInfo struct {
	Name string
	Type columnType
}

// newColumnInfo creates a new columnInfo with the given name and inferred type from values
func newColumnInfo(name string, values []string) columnInfo {
	return columnInfo{
		Name: name,
		Type: inferColumnType(values),
	}
}

// newColumnInfoWithType creates a new columnInfo with explicit type
func newColumnInfoWithType(name string, colType columnType) columnInfo {
	return columnInfo{
		Name: name,
		Type: colType,
	}
}

// columnInfoList represents a collection of column information
type columnInfoList []columnInfo

// newColumnInfoList creates column info list from header and records
func newColumnInfoList(header header, records []record) columnInfoList {
	columnCount := len(header)
	if columnCount == 0 {
		return nil
	}

	columns := make(columnInfoList, columnCount)

	// Initialize column info with headers
	for i, name := range header {
		columns[i] = columnInfo{
			Name: name,
			Type: columnTypeText, // Default to TEXT
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
		columns[i] = newColumnInfo(header[i], values)
	}

	return columns
}

// newColumnInfoListFromValues creates column info list from header and column values
func newColumnInfoListFromValues(header header, columnValues [][]string) columnInfoList {
	if len(columnValues) == 0 {
		// No data to infer from, use default TEXT type
		columnInfos := make(columnInfoList, len(header))
		for i, name := range header {
			columnInfos[i] = newColumnInfoWithType(name, columnTypeText)
		}
		return columnInfos
	}

	columnInfos := make(columnInfoList, len(header))
	for i, name := range header {
		var values []string
		if i < len(columnValues) {
			values = columnValues[i]
		}
		columnInfos[i] = newColumnInfo(name, values)
	}
	return columnInfos
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

// inferColumnType infers the SQL column type from a slice of string values
func inferColumnType(values []string) columnType {
	if len(values) == 0 {
		return columnTypeText
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
		return columnTypeText
	}
	if hasDatetime {
		return columnTypeDatetime
	}
	if hasReal {
		return columnTypeReal
	}
	if hasInteger {
		return columnTypeInteger
	}

	// Default to TEXT if no values were found
	return columnTypeText
}

// inferColumnsInfo infers column information from header and data records
func inferColumnsInfo(header header, records []record) []columnInfo {
	columnCount := len(header)
	if columnCount == 0 {
		return nil
	}

	columns := make([]columnInfo, columnCount)

	// Initialize column info with headers
	for i, name := range header {
		columns[i] = columnInfo{
			Name: name,
			Type: columnTypeText, // Default to TEXT
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
		columns[i].Type = inferColumnType(values)
	}

	return columns
}
