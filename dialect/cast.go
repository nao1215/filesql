package dialect

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// This file implements CAST with the source dialect's semantics instead of
// SQLite's.
//
// Mapping a cast onto SQLite's own CAST loses the parts of the operation users
// rely on, and loses them silently. SQLite truncates 1.9 to 1 where all three
// dialects round it to 2; it answers 0 for CAST('abc' AS INTEGER) where
// PostgreSQL and GoogleSQL raise; it passes an invalid date straight through;
// and it drops the length and scale of CHAR(n) and DECIMAL(p,s). A query that
// casts to validate its input therefore reports success on exactly the rows it
// was written to reject.
//
// Each dialect's rewrite pass emits a call to its own helper here, carrying the
// original target type text so parameters survive. GoogleSQL's SAFE_CAST shares
// the same conversion and turns an error into NULL, which is its whole purpose.

// ErrInvalidCast indicates a value the target type cannot represent, in a
// dialect that treats that as an error rather than a coerced fallback.
var ErrInvalidCast = errors.New("dialect: invalid cast")

// sqliteInteger is both a source-dialect type name and SQLite's own INTEGER
// cast target, which MySQL's DIV rewrite needs for its truncating division.
const sqliteInteger = "INTEGER"

// Output layouts for the date and time cast targets.
const (
	layoutDateOnly = "2006-01-02"
	layoutTimeOnly = "15:04:05"
)

// castKind is the conversion a target type name asks for. Several spellings map
// to one kind (INT64, SIGNED, and int4 are all castInt).
type castKind int

const (
	castInt castKind = iota
	castFloat
	castDecimal
	castText
	castBool
	castDate
	castTime
	castTimestamp
	castUUID
	castJSON
	castBlob
	castYear
)

// commonCastKinds holds the type names shared by the dialects. Each dialect map
// below holds only the names unique to it and falls back to this table.
var commonCastKinds = map[string]castKind{
	"SMALLINT":    castInt,
	"INT":         castInt,
	sqliteInteger: castInt,
	"BIGINT":      castInt,
	"TINYINT":     castInt,
	"BOOL":        castBool,
	"BOOLEAN":     castBool,
	"REAL":        castFloat,
	"FLOAT":       castFloat,
	"DOUBLE":      castFloat,
	"NUMERIC":     castDecimal,
	"DECIMAL":     castDecimal,
	"CHAR":        castText,
	"VARCHAR":     castText,
	"TEXT":        castText,
	"JSON":        castJSON,
	typeDate:      castDate,
	typeDatetime:  castTimestamp,
	typeTime:      castTime,
	typeTimestamp: castTimestamp,
	"BLOB":        castBlob,
}

var mysqlCastKinds = map[string]castKind{
	"SIGNED":    castInt,
	"UNSIGNED":  castInt,
	"MEDIUMINT": castInt,
	"NCHAR":     castText,
	"NVARCHAR":  castText,
	"DEC":       castDecimal,
	"YEAR":      castYear,
	"BINARY":    castBlob,
	"VARBINARY": castBlob,
}

var pgCastKinds = map[string]castKind{
	"INT2":        castInt,
	"INT4":        castInt,
	"INT8":        castInt,
	"SERIAL":      castInt,
	"BIGSERIAL":   castInt,
	"FLOAT4":      castFloat,
	"FLOAT8":      castFloat,
	"MONEY":       castFloat,
	"CHARACTER":   castText,
	"BPCHAR":      castText,
	"NAME":        castText,
	"UUID":        castUUID,
	"JSONB":       castJSON,
	"TIMESTAMPTZ": castTimestamp,
	"INTERVAL":    castText,
	"BYTEA":       castBlob,
}

var googlesqlCastKinds = map[string]castKind{
	"INT64":      castInt,
	"BYTEINT":    castInt,
	"FLOAT64":    castFloat,
	"BIGNUMERIC": castDecimal,
	"BIGDECIMAL": castDecimal,
	"STRING":     castText,
	"BYTES":      castBlob,
}

// castKindsFor returns the dialect-specific type table.
func castKindsFor(d Dialect) map[string]castKind {
	switch d {
	case MySQL:
		return mysqlCastKinds
	case PostgreSQL:
		return pgCastKinds
	case GoogleSQL:
		return googlesqlCastKinds
	default:
		return nil
	}
}

// lookupCastKind resolves a source-dialect type name to a conversion kind,
// checking the dialect table first and then the shared one.
func lookupCastKind(d Dialect, name string) (castKind, bool) {
	upper := strings.ToUpper(name)
	if kind, ok := castKindsFor(d)[upper]; ok {
		return kind, true
	}
	kind, ok := commonCastKinds[upper]
	return kind, ok
}

// parseCastTarget splits a target type text such as "decimal(10,2)" into its
// name and integer parameters. A malformed parameter list is ignored, since the
// name alone still selects the conversion.
func parseCastTarget(target string) (string, []int) {
	target = strings.TrimSpace(target)
	open := strings.IndexByte(target, '(')
	if open < 0 || !strings.HasSuffix(target, ")") {
		return target, nil
	}
	name := strings.TrimSpace(target[:open])
	var params []int
	for _, field := range strings.Split(target[open+1:len(target)-1], ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil {
			return name, nil
		}
		params = append(params, n)
	}
	return name, params
}

// castValue converts v to the target type using dialect d's rules. It is the
// single implementation behind every dialect's cast helper.
//
// The dialects split into two camps for a value the target cannot represent.
// PostgreSQL and GoogleSQL raise, so the caller learns the data is bad. MySQL
// coerces instead: a non-numeric string becomes 0 (or its leading numeric
// prefix) and an invalid date becomes NULL, matching what MySQL itself returns
// alongside a warning.
func castValue(d Dialect, target string, v driver.Value) (driver.Value, error) {
	if v == nil {
		return nil, nil
	}
	name, params := parseCastTarget(target)
	kind, ok := lookupCastKind(d, name)
	if !ok {
		// The rewrite only emits a helper call for names it recognizes, so an
		// unknown one here means the value should pass through untouched.
		return v, nil
	}
	strict := d != MySQL

	switch kind {
	case castInt:
		return castToInt(d, v, strict)
	case castFloat:
		return castToFloat(v, strict)
	case castDecimal:
		return castToDecimal(d, v, params, strict)
	case castText:
		return castToText(v, params)
	case castBool:
		return castToBool(d, v, strict)
	case castDate:
		return castToTimeString(v, layoutDateOnly, strict)
	case castTime:
		return castToTimeValue(d, v, strict)
	case castYear:
		return castToYear(v)
	case castTimestamp:
		return castToTimeString(v, layoutDateTime, strict)
	case castUUID:
		return castToUUID(v)
	case castJSON:
		return castToJSON(v)
	case castBlob:
		return castToBlob(v)
	default:
		return v, nil
	}
}

// castToInt rounds rather than truncates: every dialect here rounds a
// fractional value on the way to an integer type, while SQLite truncates toward
// zero. PostgreSQL rounds halves to even; MySQL and GoogleSQL round them away
// from zero. A string is not a fractional value but a literal to be read, and
// the dialects part company there: PostgreSQL and GoogleSQL raise for text that
// is not an integer, and MySQL takes the digits at the front of it.
func castToInt(d Dialect, v driver.Value, strict bool) (driver.Value, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case bool:
		return boolToInt(x), nil
	case float64:
		return roundForDialect(d, x, strict)
	}
	s, _ := toString(v)
	text := strings.TrimSpace(s)
	// GoogleSQL reads a hexadecimal string, which is how a column of
	// hexadecimal identifiers becomes numbers. The other two dialects do not:
	// PostgreSQL raises for it and MySQL reads the leading digits.
	if d == GoogleSQL {
		if n, ok, err := hexadecimalInt(text, strict); ok {
			return n, err
		}
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err == nil {
		return n, nil
	}
	// A well-formed integer outside the range is answered here rather than
	// through a float, which cannot tell -2^63-1 from -2^63: both are the same
	// float64, so the range check would let the first one through as the second.
	if errors.Is(err, strconv.ErrRange) {
		return outOfRangeInt(strings.HasPrefix(text, "-"), strict, text)
	}
	if strict {
		// PostgreSQL and GoogleSQL read the string as an integer literal, so
		// "1.5" and "1e3" are not smaller integers but values the type cannot
		// hold: both engines raise there, and GoogleSQL's SAFE_CAST answers NULL.
		// Rounding them instead answered 2 and 1000 for text neither engine
		// accepts. A number past the float64 range is still reported as out of
		// range rather than as a parse failure.
		if f, ferr := strconv.ParseFloat(text, 64); errors.Is(ferr, strconv.ErrRange) && math.IsInf(f, 0) {
			return outOfRangeInt(math.Signbit(f), strict, text)
		}
		return nil, fmt.Errorf("%w: %q is not an integer", ErrInvalidCast, s)
	}
	// MySQL reads the leading run of digits and stops at the first character
	// that cannot be part of an integer, so "1.5" is 1 and "1e3" is 1. Its
	// coercion in a numeric context reads the whole number instead, which is what
	// numericPrefix is for; a cast to an integer type is not that context.
	return mysqlIntegerPrefix(text), nil
}

// hexadecimalInt reads GoogleSQL's hexadecimal string form -- an optional sign
// then "0x" or "0X" then hexadecimal digits. It reports whether the text was
// written that way at all, so a decimal string falls through to the reader
// below rather than being refused here.
func hexadecimalInt(text string, strict bool) (driver.Value, bool, error) {
	digits, negative := text, false
	if rest, found := strings.CutPrefix(digits, "-"); found {
		digits, negative = rest, true
	} else if rest, found := strings.CutPrefix(digits, "+"); found {
		digits = rest
	}
	rest, found := strings.CutPrefix(digits, "0x")
	if !found {
		if rest, found = strings.CutPrefix(digits, "0X"); !found {
			return nil, false, nil
		}
	}
	if rest == "" {
		return nil, true, fmt.Errorf("%w: %q is not an integer", ErrInvalidCast, text)
	}
	n, err := strconv.ParseUint(rest, 16, 64)
	switch {
	case errors.Is(err, strconv.ErrRange):
		out, rangeErr := outOfRangeInt(negative, strict, text)
		return out, true, rangeErr
	case err != nil:
		return nil, true, fmt.Errorf("%w: %q is not an integer", ErrInvalidCast, text)
	}
	if negative {
		if n > 1<<63 {
			out, rangeErr := outOfRangeInt(true, strict, text)
			return out, true, rangeErr
		}
		return -int64(n), true, nil //nolint:gosec // the bound above keeps the negation inside an int64
	}
	if n > math.MaxInt64 {
		out, rangeErr := outOfRangeInt(false, strict, text)
		return out, true, rangeErr
	}
	return int64(n), true, nil
}

// mysqlIntegerPrefix returns the integer MySQL reads at the front of text when
// it casts a string to an integer type: an optional sign followed by digits,
// stopping at the first character that is neither. A string with no digits at
// the front is 0, and a run too large for an int64 clamps to the bound of the
// type, which is what MySQL answers alongside a warning.
func mysqlIntegerPrefix(text string) int64 {
	end := 0
	negative := false
	if end < len(text) && (text[end] == '+' || text[end] == '-') {
		negative = text[end] == '-'
		end++
	}
	digits := end
	for end < len(text) && text[end] >= '0' && text[end] <= '9' {
		end++
	}
	if end == digits {
		return 0
	}
	n, err := strconv.ParseInt(text[:end], 10, 64)
	if err != nil {
		if negative {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	return n
}

// intRangeAsFloat is the int64 range measured in float64. The upper bound is 2^63
// rather than the largest integer, because no float64 holds that one: the
// nearest float above the range is 2^63 itself, so a value at or past it does
// not fit. The lower bound is exact.
const (
	intUpperExclusiveAsFloat = 9223372036854775808.0  // 2^63
	intLowerInclusiveAsFloat = -9223372036854775808.0 // -2^63
)

// roundForDialect rounds f the way d's cast does and answers what d answers for
// a value the integer type has no room for.
//
// Every dialect here rounds a fractional value where SQLite truncates:
// PostgreSQL rounds halves to even, MySQL and GoogleSQL away from zero. Past the
// range the three part company. Converting out of range in Go is
// implementation-defined and gave the most negative integer for a large positive
// value, which is not any dialect's answer and not even a stable wrong one, so
// the range is checked here: MySQL clamps to the bound of the type, which is
// what it answers with a warning, and the dialects that raise for a value they
// cannot represent raise. NaN is neither in range nor out of it and is not an
// integer at all, so it takes the same two answers.
func roundForDialect(d Dialect, f float64, strict bool) (driver.Value, error) {
	rounded := math.Round(f)
	if d == PostgreSQL {
		rounded = math.RoundToEven(f)
	}
	switch {
	case math.IsNaN(rounded):
		if strict {
			return nil, fmt.Errorf("%w: NaN is not an integer", ErrInvalidCast)
		}
		// MySQL's answer for a value that names no number at all.
		return int64(0), nil
	case rounded >= intUpperExclusiveAsFloat:
		return outOfRangeInt(false, strict, fmt.Sprint(f))
	case rounded < intLowerInclusiveAsFloat:
		return outOfRangeInt(true, strict, fmt.Sprint(f))
	}
	return int64(rounded), nil
}

// outOfRangeInt is the answer for a value the integer type has no room for:
// MySQL clamps to the bound it passed, and the dialects that raise for a value
// they cannot represent raise. shown is the value as the error should name it.
func outOfRangeInt(negative, strict bool, shown string) (driver.Value, error) {
	if strict {
		return nil, fmt.Errorf("%w: %s is out of range for an integer", ErrInvalidCast, shown)
	}
	if negative {
		return int64(math.MinInt64), nil
	}
	return int64(math.MaxInt64), nil
}

// numericPrefix returns the value of the longest leading run of s that parses as
// a number, or 0 when there is none. It reproduces MySQL's coercion of a string
// to a number.
//
// The run is measured as it is scanned rather than taken as everything that
// looks numeric and parsed afterwards. Scanning greedily and parsing once read
// "1.2.3" as a five-character candidate, which no parser accepts, so a string
// with a perfectly good number in front of it coerced to 0 — the answer for a
// string with no number in it at all — where MySQL answers 1.2. The same
// greedy scan stopped at the "e" of "1e5" and answered 1 for a number MySQL
// reads whole as 100000.
//
// So the scanner follows the grammar: an optional sign, digits around at most
// one point, and an exponent only when a digit follows it. end trails one
// character past the last position that forms a number, which is what makes a
// broken tail ("1e", "1.2.3") end the number instead of voiding it.
func numericPrefix(s string) float64 {
	f, _ := numericPrefixFound(s)
	return f
}

// numericPrefixFound is numericPrefix with a report of whether a number was
// found at all, which the conversions that answer NULL for a value spelling no
// number need: MySQL's CAST('abc' AS TIME) and CAST('abc' AS YEAR) are both
// NULL, and the zero numericPrefix answers for them would be midnight and the
// year 2000.
func numericPrefixFound(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	end := 0
	for i < len(s) && isASCIIDigit(s[i]) {
		i++
		end = i
	}
	if i < len(s) && s[i] == '.' {
		i++
		// "1." is a number and ".5" is one; "." alone is not, so the point
		// extends the run only once a digit has been seen on one side of it.
		if end > 0 {
			end = i
		}
		for i < len(s) && isASCIIDigit(s[i]) {
			i++
			end = i
		}
	}
	if end > 0 && i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		if j < len(s) && isASCIIDigit(s[j]) {
			for j < len(s) && isASCIIDigit(s[j]) {
				j++
			}
			end = j
		}
	}
	if end == 0 {
		return 0, false
	}
	f, err := strconv.ParseFloat(s[:end], 64)
	// A run too large for a float64 is a number all the same, and ParseFloat
	// hands back the infinity along with the range error. MySQL answers the
	// bound of the type there rather than nothing, and the bound is also what
	// keeps an integer cast on the same path as a well-formed number too large
	// to hold, which castToInt already clamps.
	if errors.Is(err, strconv.ErrRange) {
		return math.Copysign(math.MaxFloat64, f), true
	}
	if err != nil {
		return 0, false
	}
	return f, true
}

// isASCIIDigit reports whether c is one of the digits a number is written with.
// unicode.IsDigit is wider than that: it accepts digits of other scripts, which
// no numeric parser here reads.
func isASCIIDigit(c byte) bool { return c >= '0' && c <= '9' }

func castToFloat(v driver.Value, strict bool) (driver.Value, error) {
	if f, ok := toFloat(v); ok {
		return f, nil
	}
	s, _ := toString(v)
	if strict {
		return nil, fmt.Errorf("%w: %q is not a number", ErrInvalidCast, s)
	}
	return numericPrefix(s), nil
}

// castToDecimal honors the scale that SQLite's REAL affinity discards.
// DECIMAL with no parameters means DECIMAL(10,0) in MySQL, so it rounds to a
// whole number there; PostgreSQL and GoogleSQL keep the full value.
func castToDecimal(d Dialect, v driver.Value, params []int, strict bool) (driver.Value, error) {
	f, err := castToFloat(v, strict)
	if err != nil {
		return nil, err
	}
	value, ok := f.(float64)
	if !ok {
		return f, nil
	}
	scale := -1
	if len(params) >= 2 {
		scale = params[1]
	} else if len(params) == 1 || d == MySQL {
		scale = 0
	}
	if scale < 0 {
		return value, nil
	}
	factor := math.Pow(10, float64(scale))
	rounded := math.Round(value*factor) / factor
	// The precision bounds the magnitude: DECIMAL(4,1) holds 999.9 at most, and
	// a value past it is clamped by MySQL and refused by the other two. Applying
	// the scale and ignoring the precision let CAST('2024-03-05' AS
	// DECIMAL(4,1)) answer 2024, which the type cannot hold.
	if len(params) == 0 {
		return rounded, nil
	}
	limit := decimalLimit(params[0], scale)
	if limit <= 0 || math.Abs(rounded) <= limit {
		return rounded, nil
	}
	if strict {
		return nil, fmt.Errorf("%w: %v does not fit a numeric with precision %d and scale %d",
			ErrInvalidCast, value, params[0], scale)
	}
	return math.Copysign(limit, rounded), nil
}

// decimalLimit is the largest magnitude a DECIMAL(precision, scale) holds, which
// is every one of its digits set to nine. A precision that cannot hold the scale
// bounds nothing and answers zero, so the caller leaves the value alone.
func decimalLimit(precision, scale int) float64 {
	if precision <= 0 || scale < 0 || scale > precision {
		return 0
	}
	return math.Pow(10, float64(precision-scale)) - math.Pow(10, float64(-scale))
}

// castToTimeValue converts to a TIME. The dialects part company over a value
// carrying no time of day at all: MySQL reads it as a number and spells it
// right to left as seconds, minutes and hours, so CAST('2024-03-05' AS TIME) is
// 00:20:24, while PostgreSQL refuses it. Formatting a date as a time answered
// 00:00:00 for both, which is a value a caller cannot tell from a real midnight.
func castToTimeValue(d Dialect, v driver.Value, strict bool) (driver.Value, error) {
	if tm, ok := toStringTime(v); ok && hasTimeOfDay(v) {
		return tm.Format(layoutTimeOnly), nil
	}
	if strict {
		s, _ := toString(v)
		return nil, fmt.Errorf("%w: %q is not a valid time value", ErrInvalidCast, s)
	}
	return mysqlTimeFromNumber(v), nil
}

// hasTimeOfDay reports whether the value carries a time rather than only a date.
// A time.Time from the driver always does; a string has to hold a ":" for one of
// the layouts with a clock in it to have matched.
func hasTimeOfDay(v driver.Value) bool {
	if _, ok := v.(time.Time); ok {
		return true
	}
	s, ok := toString(v)
	return ok && strings.Contains(s, ":")
}

// mysqlTimeFromNumber is MySQL's reading of a value with no clock in it: the
// number at the front of it, rounded, taken right to left as seconds, then
// minutes, then hours. CAST(123456 AS TIME) is 12:34:56 and CAST('12abc' AS
// TIME) is 00:00:12. A magnitude past the TIME range answers NULL, as MySQL
// does.
func mysqlTimeFromNumber(v driver.Value) driver.Value {
	s, ok := toString(v)
	if !ok {
		return nil
	}
	f, found := numericPrefixFound(s)
	if !found {
		// A value with no number in it is not a time at all, and MySQL answers
		// NULL rather than the midnight a zero would spell.
		return nil
	}
	n := int64(math.Round(f))
	negative := n < 0
	if negative {
		n = -n
	}
	seconds, minutes, hours := n%100, (n/100)%100, n/10000
	if seconds > 59 || minutes > 59 || hours > 838 {
		return nil
	}
	total := (hours*3600 + minutes*60 + seconds) * microsPerSecond
	if negative {
		total = -total
	}
	return formatMySQLTime(total)
}

// castToYear implements MySQL's YEAR type: a date or datetime gives its year, a
// number in 1..69 is 2001..2069 and one in 70..99 is 1970..1999, a four-digit
// year has to fall in the type's own range of 1901..2155, and zero is zero. A
// value outside all of that is not a year and answers NULL, which is what MySQL
// answers. The name used to map onto the text conversion, so the cast handed its
// argument straight back.
func castToYear(v driver.Value) (driver.Value, error) {
	if tm, ok := toStringTime(v); ok {
		return yearInRange(int64(tm.Year())), nil
	}
	s, ok := toString(v)
	if !ok {
		return nil, nil
	}
	f, found := numericPrefixFound(s)
	if !found {
		return nil, nil
	}
	n := int64(math.Round(f))
	switch {
	case n == 0:
		return int64(0), nil
	case n >= 1 && n <= 69:
		return 2000 + n, nil
	case n >= 70 && n <= 99:
		return 1900 + n, nil
	default:
		return yearInRange(n), nil
	}
}

// yearInRange is the year itself when the YEAR type can hold it, and NULL when
// it cannot. MySQL stores 1901 through 2155 and nothing else.
func yearInRange(n int64) driver.Value {
	if n < 1901 || n > 2155 {
		return nil
	}
	return n
}

// castToText applies the length of CHAR(n) / VARCHAR(n), which every dialect
// enforces by truncating and SQLite's TEXT affinity ignores.
func castToText(v driver.Value, params []int) (driver.Value, error) {
	s, ok := toString(v)
	if !ok {
		return nil, nil
	}
	if len(params) == 0 {
		return s, nil
	}
	runes := []rune(s)
	if params[0] < 0 || params[0] >= len(runes) {
		return s, nil
	}
	return string(runes[:params[0]]), nil
}

// boolLiterals are the spellings PostgreSQL accepts for a boolean; GoogleSQL
// accepts the "true"/"false" subset. SQLite has no boolean type, so the result
// is the integer 1 or 0.
var boolLiterals = map[string]int64{
	"true": 1, "t": 1, "yes": 1, "y": 1, "on": 1, "1": 1,
	"false": 0, "f": 0, "no": 0, "n": 0, "off": 0, "0": 0,
}

func castToBool(_ Dialect, v driver.Value, strict bool) (driver.Value, error) {
	switch x := v.(type) {
	case bool:
		return boolToInt(x), nil
	case int64:
		return boolToInt(x != 0), nil
	case float64:
		return boolToInt(x != 0), nil
	}
	s, _ := toString(v)
	if b, ok := boolLiterals[strings.ToLower(strings.TrimSpace(s))]; ok {
		return b, nil
	}
	if strict {
		return nil, fmt.Errorf("%w: %q is not a boolean", ErrInvalidCast, s)
	}
	return boolToInt(isTruthy(v)), nil
}

// castToTimeString validates a date or time value instead of letting an
// unparseable string through unchanged, which is what SQLite's TEXT affinity
// does and what makes a cast useless for checking input.
func castToTimeString(v driver.Value, layout string, strict bool) (driver.Value, error) {
	if tm, ok := toStringTime(v); ok {
		return tm.Format(layout), nil
	}
	s, _ := toString(v)
	if strict {
		return nil, fmt.Errorf("%w: %q is not a valid %s value", ErrInvalidCast, s, layoutName(layout))
	}
	// MySQL answers NULL for a value it cannot read as a date.
	return nil, nil
}

func layoutName(layout string) string {
	switch layout {
	case layoutDateOnly:
		return "date"
	case layoutTimeOnly:
		return "time"
	default:
		return "timestamp"
	}
}

// castToUUID validates the canonical 8-4-4-4-12 hexadecimal form.
func castToUUID(v driver.Value) (driver.Value, error) {
	s, ok := toString(v)
	if !ok {
		return nil, nil
	}
	trimmed := strings.ToLower(strings.TrimSpace(s))
	groups := strings.Split(trimmed, "-")
	want := []int{8, 4, 4, 4, 12}
	valid := len(groups) == len(want)
	for i := 0; valid && i < len(groups); i++ {
		if len(groups[i]) != want[i] {
			valid = false
			break
		}
		for _, r := range groups[i] {
			if !strings.ContainsRune("0123456789abcdef", r) {
				valid = false
				break
			}
		}
	}
	if !valid {
		return nil, fmt.Errorf("%w: %q is not a UUID", ErrInvalidCast, s)
	}
	return trimmed, nil
}

// castToJSON validates the document so a malformed one cannot flow into the
// json operators downstream.
func castToJSON(v driver.Value) (driver.Value, error) {
	s, ok := toString(v)
	if !ok {
		return nil, nil
	}
	if !json.Valid([]byte(s)) {
		return nil, fmt.Errorf("%w: %q is not valid JSON", ErrInvalidCast, s)
	}
	return s, nil
}

func castToBlob(v driver.Value) (driver.Value, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case time.Time:
		return []byte(x.Format(layoutDateTime)), nil
	}
	s, ok := toString(v)
	if !ok {
		return nil, nil
	}
	return []byte(s), nil
}

// dialectCast builds the UDF a dialect's rewrite pass calls: cast(value, type).
// When safe is set, an invalid cast yields NULL rather than an error, which is
// GoogleSQL's SAFE_CAST.
func dialectCast(d Dialect, safe bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("dialect: cast helper expects 2 arguments, got %d", len(args))
		}
		target, ok := toString(args[1])
		if !ok {
			return nil, errors.New("dialect: cast helper is missing a target type")
		}
		out, err := castValue(d, target, args[0])
		if err != nil {
			if safe && errors.Is(err, ErrInvalidCast) {
				return nil, nil
			}
			return nil, err
		}
		return out, nil
	}
}
