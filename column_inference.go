package filesql

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

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
