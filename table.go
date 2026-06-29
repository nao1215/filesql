package filesql

import (
	"path/filepath"
	"strings"
)

// sheetFallbackName is the table name used when a sheet name sanitises to empty.
const sheetFallbackName = "sheet"

// table represents file contents as database table structure.
type table struct {
	// Name is table name derived from file path.
	name TableName
	// header is table header.
	header header
	// records is table records.
	records []Record
	// columnInfo contains inferred type information for each column
	columnInfo []columnInfo
}

// newTable create new table.
func newTable(
	name string,
	header header,
	records []Record,
) *table {
	// Infer column types from data
	columnInfo := newColumnInfoList(header, records)

	return &table{
		name:       NewTableName(name),
		header:     header,
		records:    records,
		columnInfo: columnInfo,
	}
}

// getName return table name.
func (t *table) getName() string {
	return t.name.String()
}

// getHeader return table header.
func (t *table) getHeader() header {
	return t.header
}

// getRecords return table records.
func (t *table) getRecords() []Record {
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
	lowerFileName := strings.ToLower(fileName)
	// Remove compression extensions first (case-insensitive)
	for _, ext := range []string{extGZ, extBZ2, extXZ, extZSTD, extZLIB, extSNAPPY, extS2, extLZ4} {
		if strings.HasSuffix(lowerFileName, ext) {
			fileName = fileName[:len(fileName)-len(ext)]
			break
		}
	}
	// Then remove the file type extension
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

// sanitizeTableName removes invalid characters from table names and ensures SQL-safe identifiers.
// This function is automatically applied to all table names generated from file paths to prevent
// SQL syntax errors caused by special characters like hyphens, spaces, and other symbols.
//
// Transformations applied:
//   - Replaces spaces, hyphens (-), and dots (.) with underscores (_)
//   - Removes any non-alphanumeric characters except underscores
//   - Adds "sheet_" prefix if the name starts with a number
//   - Returns "sheet" as fallback for empty names
//
// Example:
//
//	sanitizeTableName("with-hyphens") // returns "with_hyphens"
//	sanitizeTableName("data.backup")  // returns "data_backup"
//	sanitizeTableName("test@#$%")     // returns "test_"
func sanitizeTableName(name string) string {
	// Replace spaces and invalid characters with underscores
	result := strings.ReplaceAll(name, " ", "_")
	result = strings.ReplaceAll(result, "-", "_")
	result = strings.ReplaceAll(result, ".", "_")

	// Remove any non-alphanumeric characters except underscore
	var sanitized strings.Builder
	for _, r := range result {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			sanitized.WriteRune(r)
		}
	}

	finalResult := sanitized.String()

	// Ensure it doesn't start with a number
	if len(finalResult) > 0 && finalResult[0] >= '0' && finalResult[0] <= '9' {
		finalResult = "sheet_" + finalResult
	}

	// Ensure it's not empty
	if finalResult == "" {
		finalResult = sheetFallbackName
	}

	return finalResult
}
