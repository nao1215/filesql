// Package infer decides what type a column of text values can be stored as.
//
// It is the one rule the module types a column by. The root package calls it
// to declare SQLite columns and the parser package calls it to report a
// column's type to callers, so a file read through either is typed alike.
// It is internal because it is a rule and not an API: the packages that expose
// types name them in their own vocabulary.
package infer

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Type is what a column's values can all be stored as.
type Type int

const (
	// Text holds anything, and is the answer when nothing narrower does.
	Text Type = iota
	// Integer holds values strconv.ParseInt accepts, spelled canonically.
	Integer
	// Real holds decimal and exponent spellings of numbers.
	Real
	// Datetime holds values in one of the recognized date and time layouts.
	// SQLite has no such storage class; it is stored as TEXT and reported so
	// that a caller knows what the column was recognized as.
	Datetime
)

// String names the type the way the parser package reports it.
func (t Type) String() string {
	switch t {
	case Integer:
		return "INTEGER"
	case Real:
		return "REAL"
	case Datetime:
		return "DATETIME"
	default:
		return "TEXT"
	}
}

// Evidence records what the values of one column require of its type.
// It accumulates, so a value read early still counts when a later one is
// judged: a column ends up with the type every value it holds can be stored
// as, not the one its first rows suggested.
//
// The types form a chain — an integer is held by REAL, and anything is held by
// TEXT — so folding in another value can only move the answer along it. The
// zero value has seen nothing and answers Text.
type Evidence struct {
	// forcedText marks a value that only TEXT stores as written; see MustStayText.
	forcedText bool
	text       bool
	datetime   bool
	real       bool
	integer    bool
	// nonEmpty marks that some value was seen at all.
	nonEmpty bool
}

// Add folds one value into the evidence.
func (e *Evidence) Add(value string) {
	if e.forcedText || e.text {
		// TEXT already holds anything a later value could ask for.
		return
	}
	if MustStayText(value) {
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
	switch Classify(trimmed) {
	case Datetime:
		e.datetime = true
	case Real:
		e.real = true
	case Integer:
		e.integer = true
	default:
		e.text = true
	}
}

// Type reports the narrowest type holding every value that was folded in.
func (e Evidence) Type() Type {
	switch {
	case !e.nonEmpty:
		// Nothing was seen, and TEXT is the only type no later value is damaged by.
		return Text
	case e.forcedText || e.text:
		return Text
	case e.datetime && (e.integer || e.real):
		// A datetime is stored as text, so a column that also holds a number has
		// no type covering both. Picking the numeric one declared INTEGER or REAL
		// over values SQLite then stored as text, leaving the schema and typeof()
		// disagreeing.
		return Text
	case e.datetime:
		return Datetime
	case e.real:
		// One decimal is enough. Deciding this by how many decimals the column
		// happens to hold left an INTEGER column that rewrote 4.0 to 4 and stored
		// 2.5 against its own declared type, and adding one more decimal row
		// changed the arithmetic of every row already there.
		return Real
	default:
		return Integer
	}
}

// Column reports the type holding every value in the column.
//
// Every value is folded in, not a sample of them: which type a column gets has
// to follow from what the column holds, and a sample made the answer depend on
// which values the sampler happened to look at.
func Column(values []string) Type {
	var e Evidence
	for _, v := range values {
		e.Add(v)
	}
	return e.Type()
}

// Classify reports the type of a single value, which is already trimmed.
func Classify(value string) Type {
	// No recognized layout is also a number, so the order is the loader's
	// historical one and changes nothing about the answer.
	if IsDatetime(value) {
		return Datetime
	}
	if IsInteger(value) {
		return Integer
	}
	if IsFloat(value) {
		return Real
	}
	return Text
}

// IsInteger reports whether value is an integer literal int64 holds as written.
func IsInteger(value string) bool {
	if len(value) == 0 {
		return false
	}
	first := value[0]
	if first != '+' && first != '-' && (first < '0' || first > '9') {
		return false
	}
	// A zero-padded literal such as "007" is not an integer: the leading zero
	// is what the value means — a ZIP code, a product ID — and every numeric
	// type drops it, so the only form that holds the value is text.
	if IsZeroPaddedIntegerLiteral(value) {
		return false
	}
	_, err := strconv.ParseInt(value, 10, 64)
	return err == nil
}

// IsFloat reports whether value is a decimal or exponent spelling of a number
// that SQLite's numeric affinity converts the same way strconv does.
func IsFloat(value string) bool {
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
	// An integer-looking string that does not fit in int64 is not a float:
	// strconv.ParseFloat would accept it and silently lose precision, rendering
	// 11040320260000000000 as 1.104032026e+19. SQLite's INTEGER cannot hold it
	// either, so the only lossless representation is TEXT.
	if IsIntegerLiteralOverflowingInt64(value) {
		return false
	}
	// A zero-padded integer literal ("007") is not a float either: float64 would
	// drop the leading zero just as INTEGER does.
	if IsZeroPaddedIntegerLiteral(value) {
		return false
	}
	// strconv.ParseFloat accepts Go source syntax: digit-separating underscores
	// ("1_000") and hexadecimal floats ("0x1p4"). SQLite's numeric affinity
	// converts neither, so calling them numeric declared a REAL column whose
	// values it then stored as text, leaving the schema and typeof() disagreeing.
	if HasGoOnlyNumericSyntax(value) {
		return false
	}
	// Decimal spelling is deliberately not guarded the way the three cases above
	// are. "2.50" loads as the REAL 2.5 and "1e3" as 1000: the quantity survives
	// and the way it was written does not. Keeping the spelling would mean a
	// TEXT column, and SQLite compares a TEXT column against a number as text —
	// "WHERE amount > 9.5" over "9.00" and "10.00" then matches nothing at all.
	_, err := strconv.ParseFloat(value, 64)
	return err == nil
}

// MustStayText reports whether a numeric column would damage this value, so
// the column holding it has to be TEXT.
//
// These differ from the rest of inference in kind rather than degree. Whether
// a column is INTEGER or REAL is a judgement about the column. Whether a cell
// survives the load is not: a zero-padded code, a literal past int64, or
// Go-only numeric syntax is rewritten by SQLite's affinity the moment it
// reaches a numeric column, and no later inspection can recover what it said.
func MustStayText(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	// Surrounding whitespace on a value that would otherwise be a number is the
	// same kind of loss. SQLite's numeric affinity converts " 5 " to 5 and the
	// spaces are gone, while the text column beside it keeps its own. A
	// fixed-width padded code ("  42") is the case that costs.
	if trimmed != value && (IsInteger(trimmed) || IsFloat(trimmed)) {
		return true
	}
	return IsZeroPaddedIntegerLiteral(trimmed) ||
		IsIntegerLiteralOverflowingInt64(trimmed) ||
		HasGoOnlyNumericSyntax(trimmed)
}

// IsZeroPaddedIntegerLiteral reports whether value is an integer literal
// (optional sign, then ASCII digits) with a redundant leading zero, such as
// "007" or "02134". A lone "0" is an ordinary integer.
//
// Decimal scale is deliberately not treated the same way. "1.50" is the real
// 1.5 here as it is everywhere else in filesql: the quantity survives and only
// the way it was written does not, while a leading zero is the value itself.
func IsZeroPaddedIntegerLiteral(value string) bool {
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

// HasGoOnlyNumericSyntax reports whether value uses numeric syntax that Go's
// parsers accept and SQLite's numeric affinity does not convert: an underscore
// separator, or the "0x" prefix of a hexadecimal literal.
func HasGoOnlyNumericSyntax(value string) bool {
	digits := strings.TrimSpace(value)
	digits = strings.TrimPrefix(strings.TrimPrefix(digits, "+"), "-")
	if strings.Contains(digits, "_") {
		return true
	}
	return len(digits) > 1 && digits[0] == '0' && (digits[1] == 'x' || digits[1] == 'X')
}

// IsIntegerLiteralOverflowingInt64 reports whether value is an integer literal
// (optional sign, then ASCII digits) whose magnitude int64 cannot hold. Such a
// value is only stored losslessly as TEXT, since float64 would lose digits too.
func IsIntegerLiteralOverflowingInt64(value string) bool {
	digits := value
	if len(digits) > 0 && (digits[0] == '+' || digits[0] == '-') {
		digits = digits[1:]
	}
	if len(digits) == 0 {
		return false
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return false
		}
	}
	_, err := strconv.ParseInt(value, 10, 64)
	return err != nil
}

// datetimePattern pairs a shape a value must have with the layouts that shape
// can be read by. The regular expression is a gate, so time.Parse — which is
// slow and forgiving — runs only on values that look like the layout.
type datetimePattern struct {
	pattern *regexp.Regexp
	formats []string
}

//nolint:gochecknoglobals // Compiled once; the patterns are constants in all but type.
var datetimePatterns = []datetimePattern{
	// ISO 8601 with a zone, first because it is the most common.
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`),
		[]string{time.RFC3339, time.RFC3339Nano},
	},
	// ISO 8601 without a zone.
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$`),
		[]string{"2006-01-02T15:04:05", "2006-01-02T15:04:05.000"},
	},
	// ISO 8601 date and time with a space.
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$`),
		[]string{"2006-01-02 15:04:05", "2006-01-02 15:04:05.000"},
	},
	// ISO 8601 date only.
	{
		regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`),
		[]string{"2006-01-02"},
	},
	// Year first with slashes, with and without a time of day.
	{
		regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$`),
		[]string{"2006/01/02 15:04:05"},
	},
	{
		regexp.MustCompile(`^\d{4}/\d{2}/\d{2}$`),
		[]string{"2006/01/02"},
	},
	// US layouts.
	{
		regexp.MustCompile(`^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}( (AM|PM))?$`),
		[]string{"1/2/2006 15:04:05", "1/2/2006 3:04:05 PM", "01/02/2006 15:04:05"},
	},
	{
		regexp.MustCompile(`^\d{1,2}/\d{1,2}/\d{4}$`),
		[]string{"1/2/2006", "01/02/2006"},
	},
	// European layouts.
	{
		regexp.MustCompile(`^\d{1,2}\.\d{1,2}\.\d{4} \d{1,2}:\d{2}:\d{2}$`),
		[]string{"2.1.2006 15:04:05", "02.01.2006 15:04:05"},
	},
	{
		regexp.MustCompile(`^\d{1,2}\.\d{1,2}\.\d{4}$`),
		[]string{"2.1.2006", "02.01.2006"},
	},
	// Time of day.
	{
		regexp.MustCompile(`^\d{1,2}:\d{2}:\d{2}(\.\d+)?$`),
		[]string{"15:04:05", "15:04:05.000", "3:04:05"},
	},
	{
		regexp.MustCompile(`^\d{1,2}:\d{2}$`),
		[]string{"15:04", "3:04"},
	},
}

const (
	// minDatetimeLength is the shortest value any layout above admits.
	minDatetimeLength = 4
	// maxDatetimeLength is the longest value any layout above admits.
	maxDatetimeLength = 35
)

// IsDatetime reports whether value is written in one of the recognized date
// and time layouts.
func IsDatetime(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) < minDatetimeLength || len(value) > maxDatetimeLength {
		return false
	}
	// A datetime has at least one digit and one separator, which rules out most
	// values before a regular expression runs.
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
	for _, dp := range datetimePatterns {
		if !dp.pattern.MatchString(value) {
			continue
		}
		for _, format := range dp.formats {
			if _, err := time.Parse(format, value); err == nil {
				return true
			}
		}
	}
	return false
}
