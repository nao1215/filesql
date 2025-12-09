package parser

import (
	"strconv"
	"strings"
	"time"
)

// Type inference constants
const (
	maxSampleSize          = 1000
	minConfidenceThreshold = 0.8
	minDatetimeLength      = 4
	maxDatetimeLength      = 35
)

// inferColumnTypes infers the type of each column based on the data.
func inferColumnTypes(headers []string, records [][]string) []ColumnType {
	columnTypes := make([]ColumnType, len(headers))

	for i := range headers {
		columnTypes[i] = inferColumnType(records, i)
	}

	return columnTypes
}

// inferColumnType infers the type of a single column.
func inferColumnType(records [][]string, colIndex int) ColumnType {
	if len(records) == 0 {
		return TypeText
	}

	// Collect non-empty values for this column
	var values []string
	sampleSize := min(len(records), maxSampleSize)
	for i := range sampleSize {
		if colIndex < len(records[i]) {
			val := strings.TrimSpace(records[i][colIndex])
			if val != "" {
				values = append(values, val)
			}
		}
	}

	if len(values) == 0 {
		return TypeText
	}

	// Count types
	var intCount, floatCount, datetimeCount int
	for _, val := range values {
		switch classifyValue(val) {
		case TypeInteger:
			intCount++
		case TypeReal:
			floatCount++
		case TypeDatetime:
			datetimeCount++
		}
	}

	total := len(values)

	// Determine type based on majority
	if float64(intCount)/float64(total) >= minConfidenceThreshold {
		return TypeInteger
	}
	if float64(intCount+floatCount)/float64(total) >= minConfidenceThreshold {
		return TypeReal
	}
	if float64(datetimeCount)/float64(total) >= minConfidenceThreshold {
		return TypeDatetime
	}

	return TypeText
}

// classifyValue determines the type of a single value.
func classifyValue(value string) ColumnType {
	if value == "" {
		return TypeText
	}

	// Check integer
	if isInteger(value) {
		return TypeInteger
	}

	// Check float
	if isFloat(value) {
		return TypeReal
	}

	// Check datetime
	if isDatetime(value) {
		return TypeDatetime
	}

	return TypeText
}

// isInteger checks if the string represents an integer.
func isInteger(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}

	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

// isFloat checks if the string represents a floating-point number.
func isFloat(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}

	// Must contain decimal point or scientific notation
	if !strings.Contains(s, ".") && !strings.ContainsAny(s, "eE") {
		return false
	}

	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// isDatetime checks if the string represents a datetime value.
func isDatetime(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) < minDatetimeLength || len(s) > maxDatetimeLength {
		return false
	}

	// Common datetime formats
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006/01/02",
		"2006/01/02 15:04:05",
		"01/02/2006",
		"01-02-2006",
		"02/01/2006",
		"02-01-2006",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05-07:00",
		"Jan 2, 2006",
		"January 2, 2006",
		"02 Jan 2006",
	}

	for _, format := range formats {
		if _, err := time.Parse(format, s); err == nil {
			return true
		}
	}

	return false
}

// ParseValue converts a string value to the appropriate Go type based on ColumnType.
// This function is useful for converting string records from TableData to typed values.
//
// Conversion rules:
//   - TypeInteger: returns int64, or original string if parsing fails
//   - TypeReal: returns float64, or original string if parsing fails
//   - TypeDatetime: returns string (caller can parse with time.Parse if needed)
//   - TypeText: returns string as-is
//   - Empty values return nil
func ParseValue(value string, colType ColumnType) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	switch colType {
	case TypeInteger:
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
		return value
	case TypeReal:
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
		return value
	case TypeDatetime:
		// Return as string for now; caller can parse if needed
		return value
	default:
		return value
	}
}
