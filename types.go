package filesql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/nao1215/filesql/internal/infer"
)

// defaultTableName is the table name used when a derived name is empty.
const defaultTableName = "table"

// Processing constants (rows-based)
const (
	// DefaultChunkSize is the default number of rows read per chunk.
	DefaultChunkSize = 1000
)

// File format delimiters
const (
	// csvDelimiter is the delimiter for CSV files
	csvDelimiter = ','
	// tsvDelimiter is the delimiter for TSV files
	tsvDelimiter = '\t'
)

// tableName represents a table name with validation
type tableName struct {
	value string
}

// newTableName creates a new tableName with validation
func newTableName(name string) tableName {
	// Basic validation - table name cannot be empty
	if strings.TrimSpace(name) == "" {
		return tableName{value: defaultTableName}
	}
	return tableName{value: strings.TrimSpace(name)}
}

// String returns the string representation of tableName
func (tn tableName) String() string {
	return tn.value
}

// Equal compares two table names
func (tn tableName) Equal(other tableName) bool {
	return tn.value == other.value
}

// sanitize returns a sanitized version of the table name
func (tn tableName) sanitize() tableName {
	return tableName{value: tn.sanitizeString()}
}

// sanitizeString removes invalid characters from table names. It keeps the same
// characters as the file-path sanitizer (see identifierRunes), so a name written
// in a non-Latin script survives here too, and differs only in the fallback and
// prefix it uses when the result is empty or starts with a digit.
func (tn tableName) sanitizeString() string {
	finalResult := identifierRunes(tn.value)

	// Ensure it doesn't start with a digit
	if first, _ := utf8.DecodeRuneInString(finalResult); unicode.IsDigit(first) {
		finalResult = "table_" + finalResult
	}

	// Ensure it's not empty
	if finalResult == "" {
		finalResult = defaultTableName
	}

	return finalResult
}

// header is file header.
type header []string

// newHeader create new header.
func newHeader(h []string) header {
	return header(h)
}

// equal compare header.
func (h header) equal(h2 header) bool {
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

// record is one row of a file, as its fields.
//
// It was exported as Record in v0.5.0 to quiet a lint warning about an exported
// method returning an unexported type. No exported method returns it any more,
// so the warning is gone and the type is internal again — nothing a caller can
// reach ever takes or returns one.
type record []string

// newRecord create new record.
func newRecord(r []string) record {
	return record(r)
}

// equal compare record.
func (r record) equal(r2 record) bool {
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

// columnType represents the SQL column type
type columnType int

const (
	// columnTypeText represents TEXT column type
	columnTypeText columnType = iota
	// columnTypeInteger represents INTEGER column type
	columnTypeInteger
	// columnTypeReal represents REAL column type
	columnTypeReal
	// columnTypeDatetime represents datetime stored as TEXT in ISO8601 format
	columnTypeDatetime
)

const (
	// sqlTypeText is the SQL TEXT type string
	sqlTypeText = "TEXT"
	// sqlTypeInteger is the SQL INTEGER type string
	sqlTypeInteger = "INTEGER"
	// sqlTypeReal is the SQL REAL type string
	sqlTypeReal = "REAL"
)

// string returns the SQL column type string
func (ct columnType) string() string {
	switch ct {
	case columnTypeText:
		return sqlTypeText
	case columnTypeInteger:
		return sqlTypeInteger
	case columnTypeReal:
		return sqlTypeReal
	case columnTypeDatetime:
		return sqlTypeText // SQLite stores datetime as TEXT in ISO8601 format
	default:
		return sqlTypeText
	}
}

// String returns the SQL column type string (public method)
func (ct columnType) String() string {
	return ct.string()
}

// validateColumnNames checks for duplicate column names and returns error if found.
//
// The message quotes the name and gives its 1-based position, because a header
// can duplicate the empty name — two unnamed columns — and an unquoted empty
// name printed nothing at all after the colon.
// Two names are the same column if either comparison says so, and the two are
// kept apart rather than combined into one key. Whitespace is filesql's own
// rule — " name " and "name" are one name typed twice — while case is SQLite's,
// because SQLite is what ends up holding the columns. Folding a trimmed name
// would apply both at once and refuse " A" beside "a", which neither rule
// refuses on its own and which SQLite keeps as two columns.
func validateColumnNames(columns []string) error {
	trimmed := make(map[string]bool, len(columns))
	folded := make(map[string]bool, len(columns))
	for i, col := range columns {
		trimmedName := strings.TrimSpace(col)
		foldedName := asciiFold(col)
		if trimmed[trimmedName] || folded[foldedName] {
			return fmt.Errorf("%w: %q (column %d)", errDuplicateColumnName, col, i+1)
		}
		trimmed[trimmedName] = true
		folded[foldedName] = true
	}
	return nil
}

// ltsvLabelKey is how two LTSV labels are compared for being one column.
//
// LTSV carries its labels on every record rather than in a header, so the
// duplicate check runs per record and had its own comparison, which was exact.
// A record holding "A:1\ta:2" therefore reached SQLite, which folds ASCII case,
// and failed as a raw CREATE TABLE error with no ErrDuplicateColumn to match —
// the outcome the check exists to replace, left in the one format whose labels
// do not go through validateColumnNames.
func ltsvLabelKey(label string) string {
	return asciiFold(strings.TrimSpace(label))
}

// asciiFold lowercases the ASCII letters in s and leaves every other byte as it
// is, which is how SQLite compares two column names: its default case folding
// stops at ASCII, so "ä" and "Ä" stay two names. Folding with strings.ToLower
// would make them one and refuse a header SQLite accepts.
//
// Case has to be folded somewhere, because leaving it out did not make "ID" and
// "id" two columns: it moved the refusal to SQLite, which reported it as a
// failed CREATE TABLE wrapped in a database-operation error — no
// ErrDuplicateColumn to match and no column position, which is the outcome this
// check exists to replace.
func asciiFold(s string) string {
	var folded []byte
	for i := range len(s) {
		c := s[i]
		if c < 'A' || c > 'Z' {
			continue
		}
		if folded == nil {
			folded = []byte(s)
		}
		folded[i] = c + ('a' - 'A')
	}
	if folded == nil {
		return s
	}
	return string(folded)
}

// chunkSizeValue represents a chunk size with validation. The name carries the
// "Value" suffix because "chunkSize" is taken by the many variables and fields
// that hold one.
type chunkSizeValue int

// newChunkSize creates a new chunkSizeValue with validation
func newChunkSize(size int) chunkSizeValue {
	// A chunk of no rows would read a file forever.
	if size < 1 {
		return chunkSizeValue(DefaultChunkSize)
	}
	return chunkSizeValue(size)
}

// Int returns the int value of chunkSizeValue
func (cs chunkSizeValue) Int() int {
	return int(cs)
}

// String returns the string representation of chunkSizeValue
func (cs chunkSizeValue) String() string {
	return strconv.Itoa(int(cs))
}

// columnInfo represents column information with name and inferred type
type columnInfo struct {
	Name string
	Type columnType
}

// newJSONDataColumn returns the single column a JSON or JSONL table has: the
// raw JSON of each element, held as text for json_extract() to read.
func newJSONDataColumn() columnInfo {
	return columnInfo{
		Name: jsonDataHeader,
		Type: columnTypeText,
	}
}

// columnInfoList represents a collection of column information
type columnInfoList []columnInfo

// newColumnInfoList names the columns of a header and gives each the type the
// records require, folding every row in rather than sampling them.
func newColumnInfoList(header header, records []record) columnInfoList {
	if len(header) == 0 {
		return nil
	}

	evidence := newColumnEvidenceList(len(header))
	evidence.addRecords(records)
	return evidence.columnInfos(header)
}

// columnEvidenceList holds the evidence for each column of a table.
type columnEvidenceList []infer.Evidence

// newColumnEvidenceList returns evidence for a table of the given width.
func newColumnEvidenceList(columnCount int) columnEvidenceList {
	return make(columnEvidenceList, columnCount)
}

// addRecord folds one row into the evidence of the columns it covers. A row
// shorter than the header leaves the columns it does not reach alone, which is
// what a missing cell means.
func (c columnEvidenceList) addRecord(r record) {
	for i, value := range r {
		if i >= len(c) {
			return
		}
		c[i].Add(value)
	}
}

// addRecords folds a chunk of rows in.
func (c columnEvidenceList) addRecords(records []record) {
	for _, r := range records {
		c.addRecord(r)
	}
}

// columnInfos names the columns and gives each the type its evidence requires.
func (c columnEvidenceList) columnInfos(h header) columnInfoList {
	infos := make(columnInfoList, len(h))
	for i, name := range h {
		info := columnInfo{Name: name, Type: columnTypeText}
		if i < len(c) {
			info.Type = columnTypeOf(c[i].Type())
		}
		infos[i] = info
	}
	return infos
}

// columnTypeOf names the SQLite column an inferred type is declared as.
func columnTypeOf(t infer.Type) columnType {
	switch t {
	case infer.Integer:
		return columnTypeInteger
	case infer.Real:
		return columnTypeReal
	case infer.Datetime:
		return columnTypeDatetime
	default:
		return columnTypeText
	}
}

// allText reports whether every column is declared TEXT, datetimes included.
func (c columnInfoList) allText() bool {
	for _, col := range c {
		if col.Type.string() != sqlTypeText {
			return false
		}
	}
	return true
}

// equalTypes reports whether both lists declare the same column types.
func (c columnInfoList) equalTypes(other columnInfoList) bool {
	if len(c) != len(other) {
		return false
	}
	for i := range c {
		if c[i].Type != other[i].Type {
			return false
		}
	}
	return true
}
