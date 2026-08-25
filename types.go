package filesql

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/nao1215/filesql/internal/reader"
)

// defaultTableName is the table name used when a derived name is empty.
const defaultTableName = "table"

// jsonDataHeader is the single column a JSON or JSONL table has.
const jsonDataHeader = reader.JSONDataColumn

// Processing constants (rows-based)
const (
	// DefaultChunkSize is the default number of rows read per chunk.
	DefaultChunkSize = 1000
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

// columnInfos names the columns of a header and gives each the type a read
// found it to require.
func columnInfos(header []string, types []infer.Type) columnInfoList {
	infos := make(columnInfoList, len(header))
	for i, name := range header {
		info := columnInfo{Name: name, Type: columnTypeText}
		if i < len(types) {
			info.Type = columnTypeOf(types[i])
		}
		infos[i] = info
	}
	return infos
}

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
