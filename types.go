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

// Record represents file records as a slice of string fields.
// This type was changed from unexported 'record' to exported 'Record' in v0.5.0
// to fix lint issues with exported methods returning unexported types.
//
// Breaking change: Code that previously imported and used the unexported 'record'
// type will need to be updated to use 'Record'.
type Record []string

// newRecord create new record.
func newRecord(r []string) Record {
	return Record(r)
}

// equal compare record.
func (r Record) equal(r2 Record) bool {
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
func newColumnInfoList(header header, records []Record) columnInfoList {
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

// datetimePattern represents a cached datetime pattern with compiled regex
type datetimePattern struct {
	pattern *regexp.Regexp
	formats []string // Multiple formats for the same pattern
}

// Cached datetime patterns for better performance
var cachedDatetimePatterns = []datetimePattern{
	// ISO8601 formats with timezone (most common first for early termination)
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

// Type inference constants
const (
	// MaxSampleSize limits how many values to sample for type inference
	MaxSampleSize = 1000
	// MinConfidenceThreshold is the minimum percentage of values that must match a type
	MinConfidenceThreshold = 0.8
	// EarlyTerminationThreshold is the percentage of text values that triggers early termination
	EarlyTerminationThreshold = 0.5
	// MinDatetimeLength is the minimum reasonable length for datetime values
	MinDatetimeLength = 4
	// MaxDatetimeLength is the maximum reasonable length for datetime values
	MaxDatetimeLength = 35
	// SamplingStratificationFactor determines when to use stratified vs simple sampling
	SamplingStratificationFactor = 3
	// MinRealThreshold is the minimum percentage of real values needed to classify as REAL
	MinRealThreshold = 0.1
)

// isDatetime checks if a string value represents a datetime with optimized pattern matching
func isDatetime(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}

	// Quick length-based filtering to avoid regex on obviously non-datetime values
	valueLen := len(value)
	if valueLen < MinDatetimeLength || valueLen > MaxDatetimeLength {
		return false
	}

	// Quick character check - datetime must contain at least one digit and separator
	hasDigit := false
	hasSeparator := false
	for _, r := range value {
		if r >= '0' && r <= '9' {
			hasDigit = true
		} else if r == '-' || r == '/' || r == '.' || r == ':' || r == 'T' || r == ' ' {
			hasSeparator = true
		}
		if hasDigit && hasSeparator {
			break
		}
	}
	if !hasDigit || !hasSeparator {
		return false
	}

	// Test patterns with early termination
	for _, dp := range cachedDatetimePatterns {
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

// inferColumnType infers the SQL column type from a slice of string values with optimized sampling
func inferColumnType(values []string) columnType {
	if len(values) == 0 {
		return columnTypeText
	}

	// Use sampling for large datasets to improve performance
	sampleValues := getSampleValues(values)

	// Track type counts for confidence-based inference
	typeCounts := map[columnType]int{
		columnTypeText:     0,
		columnTypeDatetime: 0,
		columnTypeReal:     0,
		columnTypeInteger:  0,
	}

	nonEmptyCount := 0

	for _, value := range sampleValues {
		// Skip empty values for type inference
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		nonEmptyCount++

		// Determine the type of this value
		valueType := classifyValue(value)
		typeCounts[valueType]++

		// Early termination: if too many text values, it's definitely text
		if typeCounts[columnTypeText] > 0 && float64(typeCounts[columnTypeText])/float64(nonEmptyCount) > EarlyTerminationThreshold {
			return columnTypeText
		}
	}

	if nonEmptyCount == 0 {
		return columnTypeText
	}

	// Determine the most appropriate type based on confidence thresholds
	return selectColumnType(typeCounts, nonEmptyCount)
}

// getSampleValues returns a sample of values for type inference to improve performance
// Uses stratified sampling to ensure better representation across the dataset
func getSampleValues(values []string) []string {
	if len(values) <= MaxSampleSize {
		return values
	}

	sampleSize := MaxSampleSize
	samples := make([]string, 0, sampleSize)

	// For very small datasets relative to sample size, fall back to simple sampling
	if len(values) < sampleSize*SamplingStratificationFactor {
		step := max(1, len(values)/sampleSize)
		for i := 0; i < len(values) && len(samples) < sampleSize; i += step {
			samples = append(samples, values[i])
		}
		return samples
	}

	// Stratified sampling: divide into 3 sections for better representation
	sectionSize := len(values) / SamplingStratificationFactor
	if sectionSize == 0 {
		// If section size is 0, fall back to simple sampling
		step := max(1, len(values)/sampleSize)
		for i := 0; i < len(values) && len(samples) < sampleSize; i += step {
			samples = append(samples, values[i])
		}
		return samples
	}

	samplesPerSection := sampleSize / SamplingStratificationFactor
	remainder := sampleSize % SamplingStratificationFactor

	// Ensure each section gets at least one sample if possible
	if samplesPerSection == 0 {
		samplesPerSection = 1
		remainder = max(0, sampleSize-SamplingStratificationFactor)
	}

	// Sample from beginning section with bounds checking
	beginSamples := samplesPerSection
	if remainder > 0 {
		beginSamples++
		remainder--
	}
	if beginSamples > 0 {
		step := max(1, sectionSize/beginSamples)
		for i := 0; i < sectionSize && len(samples) < beginSamples && i < len(values); i += step {
			samples = append(samples, values[i])
		}
	}

	// Sample from middle section with bounds checking
	middleSamples := samplesPerSection
	if remainder > 0 {
		middleSamples++
	}
	if middleSamples > 0 {
		startMiddle := sectionSize
		step := max(1, sectionSize/middleSamples)
		targetSamples := len(samples) + middleSamples
		for i := 0; i < sectionSize && len(samples) < targetSamples; i += step {
			idx := startMiddle + i
			if idx < len(values) {
				samples = append(samples, values[idx])
			}
		}
	}

	// Sample from end section with bounds checking
	endSamples := sampleSize - len(samples)
	if endSamples > 0 {
		startEnd := 2 * sectionSize
		if startEnd < len(values) {
			endSectionSize := len(values) - startEnd
			step := max(1, endSectionSize/endSamples)
			for i := 0; i < endSectionSize && len(samples) < sampleSize; i += step {
				idx := startEnd + i
				if idx < len(values) {
					samples = append(samples, values[idx])
				}
			}
		}
	}

	return samples
}

// classifyValue determines the type of a single value
func classifyValue(value string) columnType {
	// Check if it's a datetime first (before checking numbers)
	if isDatetime(value) {
		return columnTypeDatetime
	}

	// Check for integer first to avoid redundant parsing
	if isInteger(value) {
		return columnTypeInteger
	}

	// Then check for float (covers non-integer numbers)
	if isFloat(value) {
		return columnTypeReal
	}

	return columnTypeText
}

// isInteger checks if a value is an integer with optimized parsing
func isInteger(value string) bool {
	// Quick pre-check: must start with digit or sign
	if len(value) == 0 {
		return false
	}
	first := value[0]
	if first != '+' && first != '-' && (first < '0' || first > '9') {
		return false
	}

	_, err := strconv.ParseInt(value, 10, 64)
	return err == nil
}

// isFloat checks if a value is a float with optimized parsing
func isFloat(value string) bool {
	// Quick pre-check: must contain digits
	hasDigit := false
	for _, r := range value {
		if r >= '0' && r <= '9' {
			hasDigit = true
			break
		}
	}
	if !hasDigit {
		return false
	}

	_, err := strconv.ParseFloat(value, 64)
	return err == nil
}

// selectColumnType selects the best column type based on confidence analysis
func selectColumnType(typeCounts map[columnType]int, totalCount int) columnType {
	// If any text values exist with reasonable confidence, choose text
	if typeCounts[columnTypeText] > 0 {
		return columnTypeText
	}

	// Calculate confidence for each type
	datetimeConfidence := float64(typeCounts[columnTypeDatetime]) / float64(totalCount)
	realConfidence := float64(typeCounts[columnTypeReal]) / float64(totalCount)
	integerConfidence := float64(typeCounts[columnTypeInteger]) / float64(totalCount)

	// Choose type with highest confidence above threshold
	if datetimeConfidence >= MinConfidenceThreshold {
		return columnTypeDatetime
	}
	// For mixed numeric types, prefer REAL if there are significant real values
	// Only classify as REAL if real values make up a reasonable portion
	if realConfidence >= MinRealThreshold && (realConfidence+integerConfidence) >= MinConfidenceThreshold {
		return columnTypeReal
	}

	if integerConfidence >= MinConfidenceThreshold {
		return columnTypeInteger
	}

	// If no type has sufficient confidence, choose the most appropriate numeric type
	if realConfidence > 0 {
		return columnTypeReal
	}
	if integerConfidence > 0 {
		return columnTypeInteger
	}
	if datetimeConfidence > 0 {
		return columnTypeDatetime
	}

	// Default to text if nothing else matches
	return columnTypeText
}

// inferColumnsInfo infers column information from header and data records
func inferColumnsInfo(header header, records []Record) []columnInfo {
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
