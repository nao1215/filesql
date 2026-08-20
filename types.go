package filesql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
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

// columnTypeEvidence records what the values of one column require of its type.
// It accumulates, so a value read in an early chunk still counts when a later
// one is judged: a column ends up with the type every value it holds can be
// stored as, not the one its first chunk suggested.
//
// The types form a chain — an integer is held by REAL, and anything is held by
// TEXT — so folding in another value can only move the answer along it. That is
// what lets a table widen while it loads: a column never has to narrow, which a
// table already holding rows could not do.
type columnTypeEvidence struct {
	// forcedText marks a value that only TEXT stores as written; see mustStayText.
	forcedText bool
	text       bool
	datetime   bool
	real       bool
	integer    bool
	// nonEmpty marks that some value was seen at all.
	nonEmpty bool
}

// add folds one value into the evidence.
func (e *columnTypeEvidence) add(value string) {
	if e.forcedText || e.text {
		// TEXT already holds anything a later value could ask for.
		return
	}
	if mustStayText(value) {
		e.forcedText = true
		e.nonEmpty = true
		return
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		// An empty cell says nothing about the type it belongs to.
		return
	}
	e.nonEmpty = true
	switch classifyValue(trimmed) {
	case columnTypeDatetime:
		e.datetime = true
	case columnTypeReal:
		e.real = true
	case columnTypeInteger:
		e.integer = true
	default:
		e.text = true
	}
}

// columnType reports the narrowest type holding every value that was folded in.
func (e columnTypeEvidence) columnType() columnType {
	switch {
	case !e.nonEmpty:
		// Nothing was seen, and TEXT is the only type no later value is damaged by.
		return columnTypeText
	case e.forcedText || e.text:
		return columnTypeText
	case e.datetime && (e.integer || e.real):
		// A datetime is stored as text, so a column that also holds a number has
		// no type covering both. Picking the numeric one declared INTEGER or REAL
		// over values SQLite then stored as text, leaving the schema and typeof()
		// disagreeing.
		return columnTypeText
	case e.datetime:
		return columnTypeDatetime
	case e.real:
		// One decimal is enough. Deciding this by how many decimals the column
		// happens to hold left an INTEGER column that rewrote 4.0 to 4 and stored
		// 2.5 against its own declared type, and adding one more decimal row
		// changed the arithmetic of every row already there.
		return columnTypeReal
	default:
		return columnTypeInteger
	}
}

// columnEvidenceList holds the evidence for each column of a table.
type columnEvidenceList []columnTypeEvidence

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
		c[i].add(value)
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
			info.Type = c[i].columnType()
		}
		infos[i] = info
	}
	return infos
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

// clone returns a copy that later promotions cannot reach, so a caller can hold
// on to the types a table was actually created with.
func (c columnInfoList) clone() columnInfoList {
	return append(columnInfoList(nil), c...)
}

// datetimePattern represents a cached datetime pattern with compiled regex
type datetimePattern struct {
	pattern *regexp.Regexp
	formats []string // Multiple formats for the same pattern
}

// Cached datetime patterns for better performance
var cachedDatetimePatterns = []datetimePattern{
	// ISO8601 formats with timezone (most common first for early termination)
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`),
		[]string{time.RFC3339, time.RFC3339Nano},
	},
	// ISO8601 formats without timezone
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$`),
		[]string{"2006-01-02T15:04:05", "2006-01-02T15:04:05.000"},
	},
	// ISO8601 date and time with space
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$`),
		[]string{"2006-01-02 15:04:05", "2006-01-02 15:04:05.000"},
	},
	// ISO8601 date only
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`),
		[]string{"2006-01-02"},
	},
	// US formats
	{
		regexp.MustCompile(`^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}( (AM|PM))?$`),
		[]string{"1/2/2006 15:04:05", "1/2/2006 3:04:05 PM", "01/02/2006 15:04:05"},
	},
	{
		regexp.MustCompile(`^\d{1,2}/\d{1,2}/\d{4}$`),
		[]string{"1/2/2006", "01/02/2006"},
	},
	// European formats
	{
		regexp.MustCompile(`^\d{1,2}\.\d{1,2}\.\d{4} \d{1,2}:\d{2}:\d{2}$`),
		[]string{"2.1.2006 15:04:05", "02.01.2006 15:04:05"},
	},
	{
		regexp.MustCompile(`^\d{1,2}\.\d{1,2}\.\d{4}$`),
		[]string{"2.1.2006", "02.01.2006"},
	},
	// Time only
	{
		regexp.MustCompile(`^\d{1,2}:\d{2}:\d{2}(\.\d+)?$`),
		[]string{"15:04:05", "15:04:05.000", "3:04:05"},
	},
	{
		regexp.MustCompile(`^\d{1,2}:\d{2}$`),
		[]string{"15:04", "3:04"},
	},
}

// Type inference constants
const (
	// minDatetimeLength is the minimum reasonable length for datetime values
	minDatetimeLength = 4
	// maxDatetimeLength is the maximum reasonable length for datetime values
	maxDatetimeLength = 35
)

// isDatetime checks if a string value represents a datetime with optimized pattern matching
func isDatetime(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}

	// Quick length-based filtering to avoid regex on obviously non-datetime values
	valueLen := len(value)
	if valueLen < minDatetimeLength || valueLen > maxDatetimeLength {
		return false
	}

	// Quick character check - datetime must contain at least one digit and separator
	hasDigit := false
	hasSeparator := false
	for _, r := range value {
		if r >= '0' && r <= '9' {
			hasDigit = true
		} else if r == '-' || r == '/' || r == '.' || r == ':' || r == 'T' || r == ' ' {
			hasSeparator = true
		}
		if hasDigit && hasSeparator {
			break
		}
	}
	if !hasDigit || !hasSeparator {
		return false
	}

	// Test patterns with early termination
	for _, dp := range cachedDatetimePatterns {
		if dp.pattern.MatchString(value) {
			// Try each format for this pattern
			for _, format := range dp.formats {
				if _, err := time.Parse(format, value); err == nil {
					return true
				}
			}
		}
	}

	return false
}

// inferColumnType infers the SQL column type from a slice of string values.
//
// Every value is folded in, not a sample of them: which type a column gets has
// to follow from what the column holds, and a sample made the answer depend on
// which values the sampler happened to look at.
func inferColumnType(values []string) columnType {
	var evidence columnTypeEvidence
	for _, value := range values {
		evidence.add(value)
	}
	return evidence.columnType()
}

// classifyValue determines the type of a single value
func classifyValue(value string) columnType {
	// Check if it's a datetime first (before checking numbers)
	if isDatetime(value) {
		return columnTypeDatetime
	}

	// Check for integer first to avoid redundant parsing
	if isInteger(value) {
		return columnTypeInteger
	}

	// Then check for float (covers non-integer numbers)
	if isFloat(value) {
		return columnTypeReal
	}

	return columnTypeText
}

// isInteger checks if a value is an integer with optimized parsing
func isInteger(value string) bool {
	// Quick pre-check: must start with digit or sign
	if len(value) == 0 {
		return false
	}
	first := value[0]
	if first != '+' && first != '-' && (first < '0' || first > '9') {
		return false
	}

	// A zero-padded literal such as "007" or "02134" is not an integer: the
	// leading zero is significant (ZIP codes, product IDs), and SQLite INTEGER
	// would drop it. Preserve it as TEXT instead.
	if isZeroPaddedIntegerLiteral(value) {
		return false
	}

	_, err := strconv.ParseInt(value, 10, 64)
	return err == nil
}

// isZeroPaddedIntegerLiteral reports whether value is an integer literal
// (optional leading '+'/'-' followed solely by ASCII digits) with a redundant
// leading zero, such as "007" or "02134". A leading zero is semantically
// significant, and both SQLite INTEGER and float64 would drop it, so the only
// lossless representation is TEXT. A lone "0" is a normal integer and is
// excluded.
func isZeroPaddedIntegerLiteral(value string) bool {
	digits := value
	if len(digits) > 0 && (digits[0] == '+' || digits[0] == '-') {
		digits = digits[1:]
	}
	if len(digits) < 2 || digits[0] != '0' {
		return false
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return false
		}
	}
	return true
}

// isFloat checks if a value is a float with optimized parsing
func isFloat(value string) bool {
	// Quick pre-check: must contain digits
	hasDigit := false
	for _, r := range value {
		if r >= '0' && r <= '9' {
			hasDigit = true
			break
		}
	}
	if !hasDigit {
		return false
	}

	// An integer-looking string (all digits with an optional sign) that does
	// not fit in int64 must NOT be treated as a float. strconv.ParseFloat would
	// succeed for such a value but silently lose precision and render it in
	// scientific notation (e.g. "11040320260000000000" -> 1.104032026e+19).
	// SQLite's INTEGER type also cannot hold values beyond int64, so the only
	// lossless representation is TEXT. Returning false here lets classifyValue
	// fall through to columnTypeText and preserve the exact digits.
	if isIntegerLiteralOverflowingInt64(value) {
		return false
	}

	// A zero-padded integer literal ("007") is not a float either: float64 would
	// drop the leading zero just as INTEGER does. Keep it as TEXT.
	if isZeroPaddedIntegerLiteral(value) {
		return false
	}

	// strconv.ParseFloat accepts Go source syntax: digit-separating underscores
	// ("1_000") and hexadecimal floats ("0x1p4"). SQLite's numeric affinity
	// converts neither, so calling them numeric declared a REAL column whose
	// values it then stored as text, leaving the schema and typeof() disagreeing.
	if hasGoOnlyNumericSyntax(value) {
		return false
	}

	// Decimal spelling is deliberately not guarded the way the three cases above
	// are. "2.50" loads as the REAL 2.5 and "1e3" as 1000: the quantity survives
	// and the way it was written does not. Keeping the spelling would mean a
	// TEXT column, and SQLite compares a TEXT column against a number as text —
	// "WHERE amount > 9.5" over "9.00" and "10.00" then matches nothing at all.
	// A column of money is worth more as numbers than as the strings it was
	// typed as, so the trailing zeros go and the arithmetic stays.
	_, err := strconv.ParseFloat(value, 64)
	return err == nil
}

// mustStayText reports whether a numeric column would damage this value, so the
// column holding it has to be TEXT.
//
// These are the three cases the classifier already refuses to call numeric, and
// they differ from the rest of inference in kind rather than degree. Whether a
// column is INTEGER or REAL is a judgement about the column, and a sample is a
// reasonable way to make it. Whether a cell survives the load is not a
// judgement: a zero-padded code, a literal past int64, or Go-only numeric
// syntax is rewritten by SQLite's affinity the moment it reaches a numeric
// column, and no later inspection can recover what it said. So every value is
// asked this question, and only the leftovers are sampled.
func mustStayText(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	// Surrounding whitespace on a value that would otherwise be a number is the
	// same kind of loss. SQLite's numeric affinity converts " 5 " to 5 and the
	// spaces are gone, while the text column beside it keeps its own — so the
	// same input was preserved or rewritten depending on what it looked like.
	// A fixed-width padded code ("  42") is the case that costs.
	if trimmed != value && (isInteger(trimmed) || isFloat(trimmed)) {
		return true
	}
	return isZeroPaddedIntegerLiteral(trimmed) ||
		isIntegerLiteralOverflowingInt64(trimmed) ||
		hasGoOnlyNumericSyntax(trimmed)
}

// hasGoOnlyNumericSyntax reports whether value uses numeric syntax that Go's
// parsers accept and SQLite's numeric affinity does not convert: an underscore
// separator, or the "0x" prefix that introduces a hexadecimal (and possibly
// p-exponent) literal.
func hasGoOnlyNumericSyntax(value string) bool {
	digits := strings.TrimSpace(value)
	digits = strings.TrimPrefix(strings.TrimPrefix(digits, "+"), "-")
	if strings.Contains(digits, "_") {
		return true
	}
	return len(digits) > 1 && digits[0] == '0' && (digits[1] == 'x' || digits[1] == 'X')
}

// isIntegerLiteralOverflowingInt64 reports whether value is an integer literal
// (optional leading '+'/'-' followed solely by ASCII digits) whose magnitude
// exceeds the range representable by int64. Such values can only be stored
// losslessly as TEXT, since both SQLite INTEGER (int64) and float64 would lose
// information.
func isIntegerLiteralOverflowingInt64(value string) bool {
	digits := value
	if len(digits) > 0 && (digits[0] == '+' || digits[0] == '-') {
		digits = digits[1:]
	}
	if len(digits) == 0 {
		return false
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			// Contains a non-digit (decimal point, exponent, etc.), so it is
			// not a plain integer literal and should be handled by ParseFloat.
			return false
		}
	}

	// It is a pure integer literal. If it fits in int64 it was already handled
	// as an integer upstream; if ParseInt fails it overflows int64.
	_, err := strconv.ParseInt(value, 10, 64)
	return err != nil
}
