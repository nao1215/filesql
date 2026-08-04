package filesql

import (
	"fmt"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"
)

// sheetFallbackName is the table name used when a sheet name sanitizes to empty.
const sheetFallbackName = "sheet"

// defaultSheetName is the sheet a new Excel workbook is created with.
const defaultSheetName = "Sheet1"

// excelSheetNameMaxLen is the longest worksheet name Excel accepts, in runes.
const excelSheetNameMaxLen = 31

// excelForbiddenSheetChars are the characters Excel rejects in a worksheet name.
const excelForbiddenSheetChars = `:\/?*[]`

// excelSheetName adapts a table name to Excel's worksheet-name rules.
//
// A table name comes from a file name, so one that is long or punctuated is
// ordinary input rather than a mistake — and handing it to Excel as-is failed the
// whole dump, so a table named after monthly_sales_report_2026_q3_final.csv could
// not be exported to XLSX at all. The forbidden characters become underscores,
// the name is cut to 31 runes, and apostrophes are trimmed from the edges, which
// Excel also disallows; a name that leaves nothing usable falls back.
//
// The adapted name is what a reader turns back into a table name, so a table
// whose name Excel cannot hold does not survive a round trip under it. There is
// no name that would: the alternative is refusing to write the file.
func excelSheetName(name string) string {
	var b strings.Builder
	for _, r := range name {
		if strings.ContainsRune(excelForbiddenSheetChars, r) {
			b.WriteByte('_')
			continue
		}
		b.WriteRune(r)
	}

	sheet := []rune(b.String())
	if len(sheet) > excelSheetNameMaxLen {
		sheet = sheet[:excelSheetNameMaxLen]
	}

	trimmed := strings.Trim(string(sheet), "'")
	if trimmed == "" {
		return defaultSheetName
	}
	return trimmed
}

// table represents file contents as database table structure.
type table struct {
	// Name is table name derived from file path.
	name tableName
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
	columnInfo := newColumnInfoList(header, records)

	return &table{
		name:       newTableName(name),
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

// xlsxSheetTableName is the table name a sheet is loaded under. The sheet name
// is appended to the file's name because one workbook holds several sheets and
// each needs a table of its own.
//
// It is not appended when it would only repeat the file name. That is the shape
// a dump produces — one table per file, in a sheet named after the table — so
// without this a dumped table read back gained a suffix ("people" became
// "people_people") and a save that overwrote its own source stopped matching it.
// A workbook whose sheet name differs from its file name is unaffected.
func xlsxSheetTableName(baseTableName, sheetName string) string {
	sanitized := sanitizeTableName(sheetName)
	if sanitized == baseTableName {
		return baseTableName
	}
	return baseTableName + "_" + sanitized
}

// ExcelSheetTableNames maps every sheet of a workbook to the table it is loaded
// as, and reports the sheets that would share one table.
//
// A workbook can name two sheets "Data" and "data", or "Q1 sales" and
// "Q1.sales". Sanitizing makes the first pair identical in case only and the
// second identical outright, and SQLite compares table names case-insensitively
// for ASCII, so all four end up asking for the same table. Loading them in turn
// means the last sheet read is the only one that survives, and the rows of the
// others are gone with nothing said about it.
//
// This is the check that turns that into an error. It is exported because a
// caller that wants to refuse such a workbook before loading anything — rather
// than partway through — needs the same rule filesql applies, and a caller
// reimplementing the sanitizer would drift from it.
//
// tables is parallel to sheetNames. err is non-nil when any two sheets collide,
// and names both the sheets and the table they would share.
func ExcelSheetTableNames(filePath string, sheetNames []string) (tables []string, err error) {
	base := sanitizeTableName(tableFromFilePath(filePath))
	tables = make([]string, len(sheetNames))
	// SQLite folds ASCII case when it compares identifiers, so the key does too:
	// "Data" and "data" are one table there, whatever the sanitizer preserved.
	claimed := make(map[string]string, len(sheetNames))
	for i, sheet := range sheetNames {
		table := xlsxSheetTableName(base, sheet)
		tables[i] = table
		key := strings.ToLower(table)
		if first, taken := claimed[key]; taken {
			return nil, fmt.Errorf("%w: sheets %q and %q of %s both map to table %q; rename one of them",
				ErrDuplicateTable, first, sheet, filepath.Base(filePath), table)
		}
		claimed[key] = sheet
	}
	return tables, nil
}

// xlsxSheetNameForTable undoes xlsxSheetTableName: it is the sheet a table of
// this workbook goes back into when the workbook is overwritten in place.
//
// Naming the sheet after the table instead prefixed the file's name onto it on
// every save. A workbook "book.xlsx" holding a sheet "Orders" loads as the table
// "book_Orders", and writing that table's name back made the sheet "book_Orders";
// loading again gave "book_book_Orders", and the prefix accumulated on each round
// until Excel's 31-rune sheet name limit truncated it away.
//
// The reverse is not exact, because sanitizeTableName is not injective — a sheet
// named "Q1 Sales" and one named "Q1-Sales" both load as "Q1_Sales", and only the
// sanitized form can be recovered. What it does guarantee is that the mapping is
// stable: the name a save writes is the one the next load reads back to the same
// table, so nothing accumulates and no sheet is lost.
func xlsxSheetNameForTable(baseTableName, tableName string) string {
	if tableName == baseTableName {
		return excelSheetName(baseTableName)
	}
	if suffix, ok := strings.CutPrefix(tableName, baseTableName+"_"); ok && suffix != "" {
		return excelSheetName(suffix)
	}
	// A table that is not this workbook's is not this function's to rename.
	return excelSheetName(tableName)
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
