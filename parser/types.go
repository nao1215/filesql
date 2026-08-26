package parser

import "github.com/nao1215/filesql/internal/infer"

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
