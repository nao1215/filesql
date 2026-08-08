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
	// DefaultRowsPerChunk is the default number of rows per chunk
	DefaultRowsPerChunk = 1000
	// DefaultChunkSize is the default chunk size (rows); alias for clarity
	DefaultChunkSize = DefaultRowsPerChunk
	// MinChunkSize is the minimum allowed rows per chunk
	MinChunkSize = 1
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
	if size < MinChunkSize {
		return chunkSizeValue(DefaultRowsPerChunk)
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

// isValid checks if the chunk size is valid
func (cs chunkSizeValue) isValid() bool {
	return int(cs) >= MinChunkSize
}

// columnInfo represents column information with name and inferred type
type columnInfo struct {
	Name string
	Type columnType
}

// newColumnInfo creates a new columnInfo with the given name and inferred type from values
func newColumnInfo(name string, values []string) columnInfo {
	return columnInfo{
		Name: name,
		Type: inferColumnType(values),
	}
}

// newColumnInfoWithType creates a new columnInfo with TEXT type.
// This is used when the column type is known to be TEXT (e.g., JSON data columns,
// Parquet schema columns in streaming mode).
func newColumnInfoWithType(name string) columnInfo {
	return columnInfo{
		Name: name,
		Type: columnTypeText,
	}
}

// columnInfoList represents a collection of column information
type columnInfoList []columnInfo

// newColumnInfoList creates column info list from header and records
func newColumnInfoList(header header, records []record) columnInfoList {
	columnCount := len(header)
	if columnCount == 0 {
		return nil
	}

	columns := make(columnInfoList, columnCount)

	// Initialize column info with headers
	for i, name := range header {
		columns[i] = columnInfo{
			Name: name,
			Type: columnTypeText, // Default to TEXT
		}
	}

	// If no records, return with TEXT types
	if len(records) == 0 {
		return columns
	}

	// Pre-allocate column values slices to avoid repeated allocations
	columnValues := make([][]string, columnCount)
	for i := range columnValues {
		columnValues[i] = make([]string, 0, len(records))
	}

	// Collect values for all columns in a single pass through records
	for _, record := range records {
		for i := range columnCount {
			if i < len(record) {
				columnValues[i] = append(columnValues[i], record[i])
			}
		}
	}

	// Infer type for each column
	for i := range columnCount {
		columns[i] = newColumnInfo(header[i], columnValues[i])
	}

	return columns
}

// newColumnInfoListFromValues creates column info list from header and column values
func newColumnInfoListFromValues(header header, columnValues [][]string) columnInfoList {
	if len(columnValues) == 0 {
		// No data to infer from, use default TEXT type
		columnInfos := make(columnInfoList, len(header))
		for i, name := range header {
			columnInfos[i] = newColumnInfoWithType(name)
		}
		return columnInfos
	}

	columnInfos := make(columnInfoList, len(header))
	for i, name := range header {
		var values []string
		if i < len(columnValues) {
			values = columnValues[i]
		}
		columnInfos[i] = newColumnInfo(name, values)
	}
	return columnInfos
}

// promoteForRecords widens any numeric column that these records show cannot
// hold its values losslessly.
//
// Types are inferred once, from the first chunk, and a file is not obliged to
// introduce its awkward values early. A zero-padded code or an int64-overflowing
// literal arriving in a later chunk met a column that was already INTEGER, and
// SQLite's affinity rewrote it on the way in — the same loss the guards prevent
// in the first chunk, reached by arriving late. Only widening to TEXT is needed:
// nothing here can make a TEXT column numeric again.
func (c columnInfoList) promoteForRecords(records []record) {
	for _, r := range records {
		for i, value := range r {
			if i >= len(c) || c[i].Type == columnTypeText {
				continue
			}
			if mustStayText(value) {
				c[i].Type = columnTypeText
			}
		}
	}
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
	// maxSampleSize limits how many values to sample for type inference
	maxSampleSize = 1000
	// minConfidenceThreshold is the minimum percentage of values that must match a type
	minConfidenceThreshold = 0.8
	// earlyTerminationThreshold is the percentage of text values that triggers early termination
	earlyTerminationThreshold = 0.5
	// minDatetimeLength is the minimum reasonable length for datetime values
	minDatetimeLength = 4
	// maxDatetimeLength is the maximum reasonable length for datetime values
	maxDatetimeLength = 35
	// samplingStratificationFactor determines when to use stratified vs simple sampling
	samplingStratificationFactor = 3
	// minRealThreshold is the minimum percentage of real values needed to classify as REAL
	minRealThreshold = 0.1
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

// inferColumnType infers the SQL column type from a slice of string values with optimized sampling
func inferColumnType(values []string) columnType {
	if len(values) == 0 {
		return columnTypeText
	}

	// Asked of every value, not of the sample: see mustStayText.
	for _, value := range values {
		if mustStayText(value) {
			return columnTypeText
		}
	}

	// Use sampling for large datasets to improve performance
	sampleValues := getSampleValues(values)

	// Track type counts for confidence-based inference
	typeCounts := map[columnType]int{
		columnTypeText:     0,
		columnTypeDatetime: 0,
		columnTypeReal:     0,
		columnTypeInteger:  0,
	}

	nonEmptyCount := 0

	for _, value := range sampleValues {
		// Skip empty values for type inference
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		nonEmptyCount++

		// Determine the type of this value
		valueType := classifyValue(value)
		typeCounts[valueType]++

		// Early termination: if too many text values, it's definitely text
		if typeCounts[columnTypeText] > 0 && float64(typeCounts[columnTypeText])/float64(nonEmptyCount) > earlyTerminationThreshold {
			return columnTypeText
		}
	}

	if nonEmptyCount == 0 {
		return columnTypeText
	}

	// Determine the most appropriate type based on confidence thresholds
	return selectColumnType(typeCounts, nonEmptyCount)
}

// getSampleValues returns a sample of values for type inference to improve performance
// Uses stratified sampling to ensure better representation across the dataset
func getSampleValues(values []string) []string {
	if len(values) <= maxSampleSize {
		return values
	}

	sampleSize := maxSampleSize
	samples := make([]string, 0, sampleSize)

	// For very small datasets relative to sample size, fall back to simple sampling
	if len(values) < sampleSize*samplingStratificationFactor {
		step := max(1, len(values)/sampleSize)
		for i := 0; i < sampleSize && i*step < len(values); i++ {
			samples = append(samples, values[i*step])
		}
		return samples
	}

	// Stratified sampling: divide into 3 sections for better representation
	sectionSize := len(values) / samplingStratificationFactor
	if sectionSize == 0 {
		// If section size is 0, fall back to simple sampling
		step := max(1, len(values)/sampleSize)
		for i := 0; i < sampleSize && i*step < len(values); i++ {
			samples = append(samples, values[i*step])
		}
		return samples
	}

	samplesPerSection := sampleSize / samplingStratificationFactor
	remainder := sampleSize % samplingStratificationFactor

	// Ensure each section gets at least one sample if possible
	if samplesPerSection == 0 {
		samplesPerSection = 1
		remainder = max(0, sampleSize-samplingStratificationFactor)
	}

	// Sample from beginning section with bounds checking
	beginSamples := samplesPerSection
	if remainder > 0 {
		beginSamples++
		remainder--
	}
	if beginSamples > 0 {
		step := max(1, sectionSize/beginSamples)
		for j := range beginSamples {
			idx := j * step
			if idx >= sectionSize || idx >= len(values) {
				break
			}
			samples = append(samples, values[idx])
		}
	}

	// Sample from middle section with bounds checking
	middleSamples := samplesPerSection
	if remainder > 0 {
		middleSamples++
	}
	if middleSamples > 0 {
		startMiddle := sectionSize
		step := max(1, sectionSize/middleSamples)
		targetSamples := len(samples) + middleSamples
		for i := 0; i < sectionSize && len(samples) < targetSamples; i += step {
			idx := startMiddle + i
			if idx < len(values) {
				samples = append(samples, values[idx])
			}
		}
	}

	// Sample from end section with bounds checking
	endSamples := sampleSize - len(samples)
	if endSamples > 0 {
		startEnd := 2 * sectionSize
		if startEnd < len(values) {
			endSectionSize := len(values) - startEnd
			step := max(1, endSectionSize/endSamples)
			for i := 0; i < endSectionSize && len(samples) < sampleSize; i += step {
				idx := startEnd + i
				if idx < len(values) {
					samples = append(samples, values[idx])
				}
			}
		}
	}

	return samples
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
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	return isZeroPaddedIntegerLiteral(value) ||
		isIntegerLiteralOverflowingInt64(value) ||
		hasGoOnlyNumericSyntax(value)
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

// selectColumnType selects the best column type based on confidence analysis
func selectColumnType(typeCounts map[columnType]int, totalCount int) columnType {
	// If any text values exist with reasonable confidence, choose text
	if typeCounts[columnTypeText] > 0 {
		return columnTypeText
	}

	// A datetime is stored as text, so a column that also holds a number has no
	// type covering both, exactly as a column holding text does. Picking the
	// numeric one declared INTEGER or REAL over values SQLite then stored as
	// text, leaving the schema and typeof() disagreeing.
	if typeCounts[columnTypeDatetime] > 0 &&
		typeCounts[columnTypeInteger]+typeCounts[columnTypeReal] > 0 {
		return columnTypeText
	}

	// Calculate confidence for each type
	datetimeConfidence := float64(typeCounts[columnTypeDatetime]) / float64(totalCount)
	realConfidence := float64(typeCounts[columnTypeReal]) / float64(totalCount)
	integerConfidence := float64(typeCounts[columnTypeInteger]) / float64(totalCount)

	// Choose type with highest confidence above threshold
	if datetimeConfidence >= minConfidenceThreshold {
		return columnTypeDatetime
	}
	// For mixed numeric types, prefer REAL if there are significant real values
	// Only classify as REAL if real values make up a reasonable portion
	if realConfidence >= minRealThreshold && (realConfidence+integerConfidence) >= minConfidenceThreshold {
		return columnTypeReal
	}

	if integerConfidence >= minConfidenceThreshold {
		return columnTypeInteger
	}

	// If no type has sufficient confidence, choose the most appropriate numeric type
	if realConfidence > 0 {
		return columnTypeReal
	}
	if integerConfidence > 0 {
		return columnTypeInteger
	}
	if datetimeConfidence > 0 {
		return columnTypeDatetime
	}

	// Default to text if nothing else matches
	return columnTypeText
}

// inferColumnsInfo infers column information from header and data records
func inferColumnsInfo(header header, records []record) []columnInfo {
	columnCount := len(header)
	if columnCount == 0 {
		return nil
	}

	columns := make([]columnInfo, columnCount)

	// Initialize column info with headers
	for i, name := range header {
		columns[i] = columnInfo{
			Name: name,
			Type: columnTypeText, // Default to TEXT
		}
	}

	// If no records, return with TEXT types
	if len(records) == 0 {
		return columns
	}

	// Pre-allocate column values slices to avoid repeated allocations
	// This reduces memory allocations significantly for large datasets
	columnValues := make([][]string, columnCount)
	for i := range columnValues {
		columnValues[i] = make([]string, 0, len(records))
	}

	// Collect values for all columns in a single pass through records
	for _, record := range records {
		for i := range columnCount {
			if i < len(record) {
				columnValues[i] = append(columnValues[i], record[i])
			}
		}
	}

	// Infer type for each column
	for i := range columnCount {
		columns[i].Type = inferColumnType(columnValues[i])
	}

	return columns
}
