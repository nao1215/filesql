package filesql

import (
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"
)

// sheetFallbackName is the table name used when a sheet name sanitizes to empty.
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
//   - Removes any character that is not a letter, a digit, a combining mark, or an underscore
//   - Adds "sheet_" prefix if the name starts with a digit
//   - Returns "sheet" as fallback for empty names
//
// Letters and digits are judged by Unicode category, not by the ASCII range: a
// table name is always emitted double-quoted, so every letter SQLite accepts
// inside quotes is kept. Restricting the set to ASCII erased a Japanese,
// Chinese, Korean, Cyrillic, or accented-Latin file name down to the fallback,
// which also made two such files collide on one table name.
//
// Example:
//
//	sanitizeTableName("with-hyphens") // returns "with_hyphens"
//	sanitizeTableName("data.backup")  // returns "data_backup"
//	sanitizeTableName("test@#$%")     // returns "test"
//	sanitizeTableName("売上")          // returns "売上"
func sanitizeTableName(name string) string {
	finalResult := identifierRunes(name)

	// Ensure it doesn't start with a digit
	if first, _ := utf8.DecodeRuneInString(finalResult); unicode.IsDigit(first) {
		finalResult = "sheet_" + finalResult
	}

	// Ensure it's not empty
	if finalResult == "" {
		finalResult = sheetFallbackName
	}

	return finalResult
}

// identifierRunes maps a derived name onto the characters an identifier may
// carry: spaces, hyphens, and dots become underscores, and every remaining
// character that is not a letter, a digit, a combining mark, or an underscore is
// dropped. Letters and digits are judged by Unicode category so a name written
// in any script survives; a combining mark is kept so a decomposed accent stays
// with its base letter. Underscore is punctuation category-wise, so it is kept
// by name. This is the one place both table-name sanitizers agree on the
// character set, so a name derived from a file path and one built through
// TableName cannot disagree.
func identifierRunes(name string) string {
	result := strings.ReplaceAll(name, " ", "_")
	result = strings.ReplaceAll(result, "-", "_")
	result = strings.ReplaceAll(result, ".", "_")

	var sanitized strings.Builder
	for _, r := range result {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r) || r == '_' {
			sanitized.WriteRune(r)
		}
	}
	return sanitized.String()
}
