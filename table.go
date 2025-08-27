package filesql

import (
	"path/filepath"
	"strings"
)

// table represents file contents as database table structure.
type table struct {
	// Name is table name derived from file path.
	name string
	// header is table header.
	header header
	// records is table records.
	records []record
	// columnInfo contains inferred type information for each column
	columnInfo []columnInfo
}

// newTable create new table.
func newTable(
	name string,
	header header,
	records []record,
) *table {
	// Infer column types from data
	columnInfo := inferColumnsInfo(header, records)

	return &table{
		name:       name,
		header:     header,
		records:    records,
		columnInfo: columnInfo,
	}
}

// getName return table name.
func (t *table) getName() string {
	return t.name
}

// getHeader return table header.
func (t *table) getHeader() header {
	return t.header
}

// getRecords return table records.
func (t *table) getRecords() []record {
	return t.records
}

// equal compare table.
func (t *table) equal(t2 *table) bool {
	if t.getName() != t2.getName() {
		return false
	}
	if !t.header.equal(t2.header) {
		return false
	}
	if len(t.getRecords()) != len(t2.getRecords()) {
		return false
	}
	for i, record := range t.getRecords() {
		if !record.equal(t2.getRecords()[i]) {
			return false
		}
	}
	return true
}

// tableFromFilePath creates table name from file path
func tableFromFilePath(filePath string) string {
	fileName := filepath.Base(filePath)
	// Remove compression extensions first
	for _, ext := range []string{extGZ, extBZ2, extXZ, extZSTD} {
		if strings.HasSuffix(fileName, ext) {
			fileName = strings.TrimSuffix(fileName, ext)
			break
		}
	}
	// Then remove the file type extension
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}
