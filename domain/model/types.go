// Package model provides domain model for filesql
package model

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
