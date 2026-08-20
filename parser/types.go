package parser

import (
	"strconv"
	"strings"
	"time"
)

// Type inference constants
const (
	minDatetimeLength = 4
	maxDatetimeLength = 35
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
//
// Every record is read and every kind of value present counts. Reading a sample
// and taking the majority let a type be chosen against the values a column holds
// least often, which is exactly the set of values that type cannot store: a
// column of integers with one text value among them came out INTEGER, and the
// text value stayed text inside it. It also made the answer depend on where in
// the column a value sat, since only the head of it was read.
//
// The types form a chain — an integer is held by REAL, and anything is held by
// TEXT — so the answer is the highest link any value reaches.
func inferColumnType(records [][]string, colIndex int) ColumnType {
	var evidence columnTypeEvidence
	for _, r := range records {
		if colIndex < len(r) {
			evidence.add(r[colIndex])
		}
	}
	return evidence.columnType()
}

// columnTypeEvidence records what the values of one column require of its type.
// It mirrors the rule the filesql package applies to the same question, so a
// table read through this package and the same table loaded into SQLite are
// typed alike.
type columnTypeEvidence struct {
	// forcedText marks a value that only text stores as written.
	forcedText bool
	text       bool
	datetime   bool
	real       bool
	integer    bool
	// nonEmpty marks that some value was seen at all.
	nonEmpty bool
}

// add folds one value into the evidence.
func (e *columnTypeEvidence) add(value string) {
	if e.forcedText || e.text {
		// Text already holds anything a later value could ask for.
		return
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		// An empty cell says nothing about the type it belongs to.
		return
	}
	e.nonEmpty = true
	// Surrounding whitespace on a value that would otherwise be a number is lost
	// by every numeric type, and a fixed-width padded code is what it costs.
	if trimmed != value && (isInteger(trimmed) || isFloat(trimmed)) {
		e.forcedText = true
		return
	}
	switch classifyValue(trimmed) {
	case TypeDatetime:
		e.datetime = true
	case TypeReal:
		e.real = true
	case TypeInteger:
		e.integer = true
	default:
		e.text = true
	}
}

// columnType reports the narrowest type holding every value that was folded in.
func (e columnTypeEvidence) columnType() ColumnType {
	switch {
	case !e.nonEmpty:
		return TypeText
	case e.forcedText || e.text:
		return TypeText
	case e.datetime && (e.integer || e.real):
		// A datetime is kept as text, so a column that also holds a number has no
		// type covering both.
		return TypeText
	case e.datetime:
		return TypeDatetime
	case e.real:
		return TypeReal
	default:
		return TypeInteger
	}
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

	// A zero-padded literal is not one. The leading zero is what the value means
	// — a ZIP code, a product ID — and every numeric type drops it, so the only
	// form that holds the value is text.
	if isZeroPaddedIntegerLiteral(s) {
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

	if isZeroPaddedIntegerLiteral(s) {
		return false
	}

	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// isZeroPaddedIntegerLiteral reports whether s is an integer literal with a
// redundant leading zero, such as "007" or "02134".
//
// Decimal scale is deliberately not treated the same way. "1.50" is the real
// 1.5 here as it is everywhere else in filesql: the quantity survives and only
// the way it was written does not, while a leading zero is the value itself.
// A lone "0" is an ordinary integer.
func isZeroPaddedIntegerLiteral(s string) bool {
	digits := s
	if len(digits) > 0 && (digits[0] == '+' || digits[0] == '-') {
		digits = digits[1:]
	}
	if len(digits) < 2 || digits[0] != '0' {
		return false
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return false
		}
	}
	return true
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
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}

	switch colType {
	case TypeInteger:
		if i, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return i
		}
		return value
	case TypeReal:
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
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
