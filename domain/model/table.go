package model

import (
	"path/filepath"
	"strings"
)

// Table represents file contents as database table structure.
type Table struct {
	// Name is table name derived from file path.
	name string
	// Header is table header.
	header Header
	// Records is table records.
	records []Record
	// ColumnInfo contains inferred type information for each column
	columnInfo []ColumnInfo
}

// NewTable create new Table.
func NewTable(
	name string,
	header Header,
	records []Record,
) *Table {
	// Infer column types from data
	columnInfo := InferColumnsInfo(header, records)

	return &Table{
		name:       name,
		header:     header,
		records:    records,
		columnInfo: columnInfo,
	}
}

// Name return table name.
func (t *Table) Name() string {
	return t.name
}

// Header return table header.
func (t *Table) Header() Header {
	return t.header
}

// Records return table records.
func (t *Table) Records() []Record {
	return t.records
}

// ColumnInfo returns column information with inferred types
func (t *Table) ColumnInfo() []ColumnInfo {
	return t.columnInfo
}

// Equal compare Table.
func (t *Table) Equal(t2 *Table) bool {
	if t.Name() != t2.Name() {
		return false
	}
	if !t.header.Equal(t2.header) {
		return false
	}
	if len(t.Records()) != len(t2.Records()) {
		return false
	}
	for i, record := range t.Records() {
		if !record.Equal(t2.Records()[i]) {
			return false
		}
	}
	return true
}

// TableFromFilePath creates table name from file path
func TableFromFilePath(filePath string) string {
	fileName := filepath.Base(filePath)
	// Remove compression extensions first
	for _, ext := range []string{ExtGZ, ExtBZ2, ExtXZ, ExtZSTD} {
		if strings.HasSuffix(fileName, ext) {
			fileName = strings.TrimSuffix(fileName, ext)
			break
		}
	}
	// Then remove the file type extension
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}
