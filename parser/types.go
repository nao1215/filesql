package parser

import (
	"strconv"
	"strings"

	"github.com/nao1215/filesql/internal/infer"
)

// columnTypesOf names a read's inferred types in this package's vocabulary.
func columnTypesOf(types []infer.Type) []ColumnType {
	columnTypes := make([]ColumnType, len(types))
	for i, t := range types {
		columnTypes[i] = columnTypeOf(t)
	}
	return columnTypes
}

// columnTypeOf names an inferred type in this package's vocabulary.
func columnTypeOf(t infer.Type) ColumnType {
	switch t {
	case infer.Integer:
		return TypeInteger
	case infer.Real:
		return TypeReal
	case infer.Datetime:
		return TypeDatetime
	default:
		return TypeText
	}
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
