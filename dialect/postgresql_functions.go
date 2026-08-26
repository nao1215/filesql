package dialect

import (
	"crypto/sha256"
	"crypto/sha512"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// This file holds the PostgreSQL-only scalar functions, the ones with no SQLite
// spelling. They are registered under the names PostgreSQL gives them rather
// than rewritten, so the call text the query wrote is what runs and the result
// column keeps its name; the few whose name SQLite already means something else
// by are rewritten in postgresql.go instead.
//
// Every answer below was read from PostgreSQL 17.10 rather than derived.

// postgresqlScalarFunctions are the deterministic PostgreSQL helpers.
func postgresqlScalarFunctions() map[string]scalarSpec {
	return map[string]scalarSpec{
		// Quoting, for building SQL out of data.
		"quote_ident":    {1, fnQuoteIdent},
		"quote_literal":  {1, fnQuoteLiteral},
		"quote_nullable": {1, fnQuoteNullable},

		// Regular expressions.
		"regexp_count": {-1, fnRegexpCount},

		// SUBSTRING(s FROM x) where x is neither a string literal nor a number
		// literal: the rewrite cannot see what it is, so the reading is chosen
		// here from the value.
		"postgresql_substring_from": {2, fnPostgresSubstringFrom},

		// Arithmetic SQLite has no operator or function for.
		"cbrt":      {1, fnCbrt},
		"factorial": {1, fnFactorial},
		"gcd":       {2, fnGCD},
		"lcm":       {2, fnLCM},

		// The trigonometric functions that take and answer degrees. They exist
		// so the quadrant angles are exact -- sind(30) is 0.5 and not
		// 0.49999999999999994 -- which is what converting through radians
		// costs. Away from those angles the answer is the conversion, and it
		// can differ from PostgreSQL's in the last place: PostgreSQL reduces
		// the angle to the first quadrant before converting, and the C library
		// underneath is not the same one either.
		"sind":     {1, degreeTrig(degreeSin)},
		"cosd":     {1, degreeTrig(degreeCos)},
		"tand":     {1, degreeTrig(degreeTan)},
		"cotd":     {1, degreeTrig(degreeCot)},
		"asind":    {1, inverseDegreeTrig(math.Asin)},
		"acosd":    {1, inverseDegreeTrig(math.Acos)},
		"atand":    {1, inverseDegreeTrig(math.Atan)},
		"atan2d":   {2, fnAtan2d},
		"isfinite": {1, fnIsFinite},

		// Conversion.
		"to_number": {2, fnToNumber},

		// Building a date or a time out of its fields, and binning a timestamp.
		"make_date": {3, fnPostgresMakeDate},
		"make_time": {3, fnPostgresMakeTime},
		"date_bin":  {3, fnDateBin},

		// Counting the NULLs in an argument list, which no aggregate can do
		// because the values are columns of one row rather than rows of one
		// column.
		"num_nulls":    {-1, countNulls(true)},
		"num_nonnulls": {-1, countNulls(false)},

		// Bytes. The sha family answers bytes, so the spelling a query uses is
		// encode(sha256(x), 'hex'), and encode and decode are the pair that
		// makes it readable.
		"sha224":   {1, hashBytes(func(b []byte) []byte { s := sha256.Sum224(b); return s[:] })},
		"sha256":   {1, hashBytes(func(b []byte) []byte { s := sha256.Sum256(b); return s[:] })},
		"sha384":   {1, hashBytes(func(b []byte) []byte { s := sha512.Sum384(b); return s[:] })},
		"sha512":   {1, hashBytes(func(b []byte) []byte { s := sha512.Sum512(b); return s[:] })},
		"encode":   {2, fnEncode},
		"decode":   {2, fnDecode},
		"get_byte": {2, fnGetByte},
		"set_byte": {3, fnSetByte},
	}
}

// postgresqlNonDeterministicFunctions read the clock, so they must not be
// registered as deterministic: SQLite would fold them to one value.
func postgresqlNonDeterministicFunctions() map[string]scalarSpec {
	return map[string]scalarSpec{
		// PostgreSQL distinguishes the transaction's start from the statement's
		// from the moment of the call. filesql runs each statement on its own,
		// so the three answer the same clock; they are separate names because a
		// query that names one of them should not fail.
		"clock_timestamp":       {0, fnNow},
		"statement_timestamp":   {0, fnNow},
		"transaction_timestamp": {0, fnNow},
		"timeofday":             {0, fnTimeOfDay},
		"gen_random_uuid":       {0, fnGenerateUUID},
		"to_timestamp":          {-1, fnToTimestamp},
	}
}

// --- quoting ---

// fnQuoteIdent quotes a string as a SQL identifier, doubling any quote inside
// it. A name that needs no quoting -- one that starts with a lower-case letter
// or an underscore and holds nothing but lower-case letters, digits, underscores
// and dollar signs -- is returned as it is, which is what PostgreSQL does.
func fnQuoteIdent(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	if !identifierNeedsQuoting(s) {
		return s, nil
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`, nil
}

func identifierNeedsQuoting(s string) bool {
	if s == "" {
		return true
	}
	for i, r := range s {
		switch {
		case r == '_' || unicode.IsLower(r):
		case r == '$' || (r >= '0' && r <= '9'):
			if i == 0 {
				return true
			}
		default:
			return true
		}
	}
	return sqlReservedWords[strings.ToUpper(s)]
}

// sqlReservedWords are the words PostgreSQL quotes even when they are spelled
// in lower case, because unquoted they would be read as syntax. They are
// written as one string and split rather than listed as map keys, so the
// spelling of each appears once in this package.
var sqlReservedWords = keywordSet( //nolint:gochecknoglobals // a fixed table read by quote_ident
	"ALL AND AS ASC BETWEEN BY CASE CAST CHECK COLUMN CREATE DEFAULT DESC " +
		"DISTINCT DO ELSE END EXCEPT FALSE FOR FROM GROUP HAVING IN INTERSECT " +
		"INTO IS LIMIT NOT " + nullText + " OFFSET ON OR ORDER PRIMARY REFERENCES SELECT " +
		"TABLE THEN TRUE UNION UNIQUE USING WHEN WHERE WITH")

// keywordSet splits a space-separated list of keywords into a set.
func keywordSet(words string) map[string]bool {
	set := make(map[string]bool)
	for _, w := range strings.Fields(words) {
		set[w] = true
	}
	return set
}

// fnQuoteLiteral quotes a value as a SQL string literal, doubling the quotes
// and the backslashes inside it.
func fnQuoteLiteral(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	return quoteSQLLiteral(s), nil
}

// fnQuoteNullable is quote_literal except that NULL becomes the word NULL
// rather than NULL, which is the whole reason the two functions differ.
func fnQuoteNullable(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nullText, nil
	}
	return quoteSQLLiteral(s), nil
}

func quoteSQLLiteral(s string) string {
	quoted := "'" + strings.ReplaceAll(s, "'", "''") + "'"
	if !strings.Contains(s, `\`) {
		return quoted
	}
	// A backslash means the literal has to be an escape string, or the reader
	// takes it as itself or as an escape depending on their
	// standard_conforming_strings; the E prefix says which.
	return "E" + strings.ReplaceAll(quoted, `\`, `\\`)
}

// --- regular expressions ---

// fnRegexpCount implements REGEXP_COUNT(subject, pattern[, start[, flags]]):
// how many non-overlapping matches the pattern has from start on.
func fnRegexpCount(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 4 {
		return nil, fmt.Errorf("dialect: REGEXP_COUNT expects 2 to 4 arguments, got %d", len(args))
	}
	subject, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	start := int64(1)
	if len(args) >= 3 {
		n, ok := toInt(args[2])
		if !ok {
			return nil, nil
		}
		if n < 1 {
			return nil, errors.New("dialect: REGEXP_COUNT: start position must be at least 1")
		}
		start = n
	}
	if len(args) == 4 {
		flags, ok := toString(args[3])
		if !ok {
			return nil, nil
		}
		if strings.Contains(flags, "i") {
			pattern = "(?i)" + pattern
		}
	}
	runes := []rune(subject)
	if int(start) > len(runes) {
		return int64(0), nil
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return nil, err
	}
	return int64(len(re.FindAllString(string(runes[start-1:]), -1))), nil
}

// fnPostgresSubstringFrom implements SUBSTRING(s FROM x) for an operand whose
// kind the translation could not see. PostgreSQL chooses between a position and
// a pattern on the operand's declared type; SQLite has no declared type, so the
// choice is made from the value: a number is a position and anything else is a
// pattern. That is PostgreSQL's answer for every integer column and for every
// text column that does not hold digits, and differs from it for a text column
// that does. A string literal or a number literal in the query never reaches
// here -- the rewrite reads those from the query text, where PostgreSQL's own
// rule applies exactly.
func fnPostgresSubstringFrom(args []driver.Value) (driver.Value, error) {
	if _, isNumber := toFloat(args[1]); isNumber {
		return fnPostgreSQLSubstr(args)
	}
	return fnRegexpExtract(args)
}

// --- arithmetic ---

func fnCbrt(args []driver.Value) (driver.Value, error) {
	x, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	return math.Cbrt(x), nil
}

// fnFactorial answers in 64 bits, which runs out at 20. PostgreSQL computes it
// in arbitrary precision and does not, so a larger argument is refused here
// rather than answered with a number that has wrapped.
func fnFactorial(args []driver.Value) (driver.Value, error) {
	n, ok := toInt(args[0])
	if !ok {
		return nil, nil
	}
	if n < 0 {
		return nil, errors.New("dialect: FACTORIAL: argument must not be negative")
	}
	if n > 20 {
		return nil, fmt.Errorf("dialect: FACTORIAL(%d) does not fit a 64-bit integer", n)
	}
	result := int64(1)
	for i := int64(2); i <= n; i++ {
		result *= i
	}
	return result, nil
}

// fnGCD and fnLCM read their arguments unsigned, as PostgreSQL does: the sign
// of either argument does not change the answer.
func fnGCD(args []driver.Value) (driver.Value, error) {
	a, ok1 := toInt(args[0])
	b, ok2 := toInt(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	divisor := greatestCommonDivisor(magnitude(a), magnitude(b))
	if divisor > math.MaxInt64 {
		// Only the magnitude of the smallest int64 reaches here, and it is not
		// an int64. Answering it as one would be a negative divisor, which a
		// greatest common divisor never is.
		return nil, errors.New("dialect: GCD: the result does not fit a 64-bit integer")
	}
	return int64(divisor), nil
}

func fnLCM(args []driver.Value) (driver.Value, error) {
	a, ok1 := toInt(args[0])
	b, ok2 := toInt(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	if a == 0 || b == 0 {
		return int64(0), nil
	}
	magA, magB := magnitude(a), magnitude(b)
	quotient := magA / greatestCommonDivisor(magA, magB)
	if quotient != 0 && magB > math.MaxInt64/quotient {
		return nil, errors.New("dialect: LCM: the result does not fit a 64-bit integer")
	}
	return int64(quotient * magB), nil //nolint:gosec // the bound above keeps the product inside an int64
}

// greatestCommonDivisor runs Euclid over magnitudes, which are unsigned because
// the magnitude of the smallest int64 is not an int64: negating it overflows
// back to itself, and every remainder after that carries the sign.
func greatestCommonDivisor(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// magnitude is the absolute value of an int64, in a type that can hold it. The
// smallest int64 is why it is not an int64: negating that one overflows back to
// itself, so the "absolute value" would still be negative.
func magnitude(v int64) uint64 {
	if v < 0 {
		// -(v+1) is at most the largest int64, so the conversion is exact and
		// adding one back reaches the magnitude without ever leaving uint64.
		return uint64(-(v + 1)) + 1 //nolint:gosec // the expression is inside the int64 range by construction
	}
	return uint64(v)
}

// degreeKindValue names which trigonometric function a degree helper computes.
// It is carried rather than inferred, so the exact table below is chosen by the
// caller instead of by probing the function, which cannot tell tangent from
// sine.
type degreeKindValue int

const (
	degreeSin degreeKindValue = iota
	degreeCos
	degreeTan
	degreeCot
)

// exactDegreeValues are the angles PostgreSQL answers exactly. Its
// implementation reduces an angle to the first quadrant and answers the
// quadrant angles from a table rather than from the conversion to radians,
// which is the whole reason these functions exist beside the radian ones:
// SIND(30) is 0.5 and not 0.49999999999999994. Every value was read from
// PostgreSQL 17.10.
var exactDegreeValues = map[degreeKindValue]map[int64]float64{ //nolint:gochecknoglobals // a fixed table read by the degree helpers
	degreeSin: {0: 0, 30: 0.5, 90: 1, 150: 0.5, 180: 0, 210: -0.5, 270: -1, 330: -0.5},
	degreeCos: {0: 1, 60: 0.5, 90: 0, 120: -0.5, 180: -1, 240: -0.5, 270: 0, 300: 0.5},
	degreeTan: {0: 0, 45: 1, 90: math.Inf(1), 135: -1, 180: 0, 225: 1, 270: math.Inf(-1), 315: -1},
	degreeCot: {0: math.Inf(1), 45: 1, 90: 0, 135: -1, 180: math.Inf(-1), 225: 1, 270: 0, 315: -1},
}

// degreeTrig computes one of the four functions of an angle in degrees.
func degreeTrig(kind degreeKindValue) scalarFn {
	radian := map[degreeKindValue]func(float64) float64{
		degreeSin: math.Sin,
		degreeCos: math.Cos,
		degreeTan: math.Tan,
		degreeCot: func(x float64) float64 { return 1 / math.Tan(x) },
	}[kind]
	return func(args []driver.Value) (driver.Value, error) {
		deg, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		if exact, found := exactDegreeValue(kind, deg); found {
			return exact, nil
		}
		return radian(deg * math.Pi / 180), nil
	}
}

// exactDegreeValue looks an angle up in the table above, reducing it to one
// turn first.
func exactDegreeValue(kind degreeKindValue, deg float64) (float64, bool) {
	if deg != math.Trunc(deg) {
		return 0, false
	}
	d := int64(math.Mod(deg, 360))
	if d < 0 {
		d += 360
	}
	v, ok := exactDegreeValues[kind][d]
	return v, ok
}

// inverseDegreeTrig turns a function answering radians into one answering
// degrees.
func inverseDegreeTrig(fn func(float64) float64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		x, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		return radiansToDegrees(fn(x)), nil
	}
}

func fnAtan2d(args []driver.Value) (driver.Value, error) {
	y, ok1 := toFloat(args[0])
	x, ok2 := toFloat(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return radiansToDegrees(math.Atan2(y, x)), nil
}

// radiansToDegrees rounds a result that is within a rounding error of a whole
// number of degrees onto it, so ASIND(0.5) is 30 rather than 30.000000000000004.
func radiansToDegrees(rad float64) float64 {
	deg := rad * 180 / math.Pi
	if rounded := math.Round(deg); math.Abs(deg-rounded) < 1e-9 {
		return rounded
	}
	return deg
}

// fnIsFinite reports whether a date or timestamp is a real point in time.
// PostgreSQL has "infinity" as a value of both types and filesql has no way to
// spell it, so everything that parses is finite.
func fnIsFinite(args []driver.Value) (driver.Value, error) {
	if _, ok := toStringTime(args[0]); !ok {
		return nil, nil
	}
	return int64(1), nil
}

// --- conversion ---

// fnToNumber reads a string against the numeric template TO_CHAR writes.
func fnToNumber(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	format, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	items, _ := scanPGTemplate(format)
	t := parseNumericTemplate(items)
	var digits strings.Builder
	negative := false
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			digits.WriteRune(r)
		case r == '.':
			digits.WriteRune(r)
		case r == '-' || r == '<':
			negative = true
		}
	}
	value, parsed := parseFloatOrNothing(digits.String())
	if !parsed {
		// A string the template cannot read is NULL rather than an error,
		// which is how the other conversion helpers in this package answer
		// data they cannot make sense of.
		return nil, nil
	}
	for range t.shift {
		value /= 10
	}
	if negative {
		value = -value
	}
	return value, nil
}

// fnToTimestamp implements both spellings: one argument is a Unix epoch in
// seconds and two are a string read against a template.
func fnToTimestamp(args []driver.Value) (driver.Value, error) {
	switch len(args) {
	case 1:
		secs, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		whole, frac := math.Modf(secs)
		return time.Unix(int64(whole), int64(frac*1e9)).UTC().Format(layoutDateTime), nil
	case 2:
		s, ok1 := toString(args[0])
		format, ok2 := toString(args[1])
		if !ok1 || !ok2 {
			return nil, nil
		}
		tm, ok := parseLayout(pgParseLayout(format), s)
		if !ok {
			return nil, nil
		}
		return tm.Format(layoutDateTime), nil
	default:
		return nil, fmt.Errorf("dialect: TO_TIMESTAMP expects 1 or 2 arguments, got %d", len(args))
	}
}

// --- building dates and times ---

// fnMakeDate builds a date out of its fields, refusing a field outside its
// range rather than carrying it into the next one, which is what PostgreSQL
// does and what makes a bad computed field visible.
func fnPostgresMakeDate(args []driver.Value) (driver.Value, error) {
	year, ok1 := toInt(args[0])
	month, ok2 := toInt(args[1])
	day, ok3 := toInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if month < 1 || month > 12 {
		return nil, fmt.Errorf("dialect: MAKE_DATE: month %d is out of range", month)
	}
	if day < 1 || day > 31 {
		return nil, fmt.Errorf("dialect: MAKE_DATE: day %d is out of range", day)
	}
	tm := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
	if int(tm.Month()) != int(month) || tm.Day() != int(day) {
		return nil, fmt.Errorf("dialect: MAKE_DATE: %04d-%02d-%02d is not a date", year, month, day)
	}
	return tm.Format(layoutDateOnly), nil
}

// fnMakeTime builds a time of day out of its fields, refusing a minute or a
// second outside 0..59.
func fnPostgresMakeTime(args []driver.Value) (driver.Value, error) {
	hour, ok1 := toInt(args[0])
	minute, ok2 := toInt(args[1])
	second, ok3 := toFloat(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if hour < 0 || hour > 23 {
		return nil, fmt.Errorf("dialect: MAKE_TIME: hour %d is out of range", hour)
	}
	if minute < 0 || minute > 59 {
		return nil, fmt.Errorf("dialect: MAKE_TIME: minute %d is out of range", minute)
	}
	if second < 0 || second >= 60 {
		return nil, fmt.Errorf("dialect: MAKE_TIME: second %v is out of range", second)
	}
	out := fmt.Sprintf("%02d:%02d:%02d", hour, minute, int(second))
	// The seconds field takes a fraction, and PostgreSQL keeps it:
	// MAKE_TIME(10, 11, 12.5) is 10:11:12.5. Its trailing zeros go, as they do
	// everywhere else a fraction of a second is printed here.
	if frac := second - math.Trunc(second); frac != 0 {
		out += strings.TrimRight(strconv.FormatFloat(frac, 'f', 6, 64)[1:], "0")
	}
	return out, nil
}

// fnDateBin rounds a timestamp down to the start of the stride-wide interval it
// falls in, counted from origin.
func fnDateBin(args []driver.Value) (driver.Value, error) {
	stride, ok1 := toString(args[0])
	source, ok2 := toStringTime(args[1])
	origin, ok3 := toStringTime(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	width, err := intervalTextDuration(stride)
	if err != nil {
		return nil, err
	}
	if width <= 0 {
		return nil, errors.New("dialect: DATE_BIN: the stride must be greater than zero")
	}
	return binStart(source, origin, width).Format(layoutDateTime), nil
}

// binStart is the beginning of the stride-wide interval that source falls in,
// counted from origin. The distance is measured in whole days plus the time of
// day rather than as a time.Duration, whose int64 of nanoseconds saturates
// about 292 years out and would bin every far date onto the same instant.
func binStart(source, origin time.Time, width time.Duration) time.Time {
	const day = 24 * time.Hour
	days := dayNumber(source) - dayNumber(origin)
	within := timeOfDay(source) - timeOfDay(origin)
	// Carry the time of day into the day count so what is left is inside one
	// day, which is what keeps both branches below free of a multiplication
	// that could overflow.
	for within < 0 {
		within += day
		days--
	}
	for within >= day {
		within -= day
		days++
	}
	if width < day {
		// Every sub-day stride divides a day evenly, so the day count converts
		// exactly into the stride's own unit.
		unitsPerDay := int64(day / width)
		bins := days*unitsPerDay + int64(within/width)
		return origin.AddDate(0, 0, int(bins/unitsPerDay)).Add(time.Duration(bins%unitsPerDay) * width)
	}
	// A stride of a day or more is a whole number of days, and the time of day
	// left over is smaller than the stride, so the day count alone decides the
	// bin.
	strideDays := int64(width / day)
	bins := days / strideDays
	if days < 0 && days%strideDays != 0 {
		bins--
	}
	return origin.AddDate(0, 0, int(bins*strideDays))
}

// intervalTextDuration reads a DATE_BIN stride as a fixed length of time. It
// reuses the interval reader the "+ INTERVAL 'text'" rewrite uses, and refuses
// the units that are not a fixed length: a month is 28 to 31 days and a year is
// 365 or 366, so neither divides a timeline into bins of one size.
func intervalTextDuration(text string) (time.Duration, error) {
	terms, err := parseIntervalText(text)
	if err != nil {
		return 0, err
	}
	scale := map[string]time.Duration{
		unitSecond: time.Second,
		unitMinute: time.Minute,
		unitHour:   time.Hour,
		unitDay:    24 * time.Hour,
		unitWeek:   7 * 24 * time.Hour,
	}
	total := time.Duration(0)
	for _, term := range terms {
		unit, ok := scale[term.unit]
		if !ok {
			return 0, fmt.Errorf("dialect: DATE_BIN: %q is not a stride of a fixed length", text)
		}
		total += time.Duration(term.amount) * unit
	}
	return total, nil
}

// --- counting NULLs ---

// countNulls implements NUM_NULLS and NUM_NONNULLS, which count over the
// arguments of one row where an aggregate counts over rows.
func countNulls(wantNull bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		if len(args) == 0 {
			return nil, errors.New("dialect: NUM_NULLS/NUM_NONNULLS expects at least one argument")
		}
		n := int64(0)
		for _, a := range args {
			if (a == nil) == wantNull {
				n++
			}
		}
		return n, nil
	}
}

// --- bytes ---

// hashBytes wraps a hash so it reads its argument as bytes and answers bytes,
// which is what PostgreSQL's sha family does; a query prints one with
// encode(..., 'hex').
func hashBytes(sum func([]byte) []byte) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		b, ok := toBytes(args[0])
		if !ok {
			return nil, nil
		}
		return sum(b), nil
	}
}

// toBytes reads a value as the bytes PostgreSQL's bytea holds.
func toBytes(v driver.Value) ([]byte, bool) {
	switch x := v.(type) {
	case nil:
		return nil, false
	case []byte:
		return x, true
	case string:
		return []byte(x), true
	default:
		s, ok := toString(v)
		return []byte(s), ok
	}
}

// fnEncode spells bytes as text in one of the three encodings PostgreSQL has.
func fnEncode(args []driver.Value) (driver.Value, error) {
	b, ok1 := toBytes(args[0])
	format, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	switch strings.ToLower(format) {
	case "hex":
		return hex.EncodeToString(b), nil
	case "base64":
		return base64.StdEncoding.EncodeToString(b), nil
	case "escape":
		return escapeBytes(b), nil
	default:
		return nil, fmt.Errorf("dialect: ENCODE: unrecognized encoding %q", format)
	}
}

// fnDecode reads text back into bytes.
func fnDecode(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	format, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	switch strings.ToLower(format) {
	case "hex":
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("dialect: DECODE: %w", err)
		}
		return b, nil
	case "base64":
		// PostgreSQL ignores the whitespace its own encoder can insert.
		b, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(s), ""))
		if err != nil {
			return nil, fmt.Errorf("dialect: DECODE: %w", err)
		}
		return b, nil
	case "escape":
		return []byte(unescapeBytes(s)), nil
	default:
		return nil, fmt.Errorf("dialect: DECODE: unrecognized encoding %q", format)
	}
}

// escapeBytes writes the printable bytes as themselves and the rest as an octal
// escape, which is PostgreSQL's "escape" encoding.
func escapeBytes(b []byte) string {
	var out strings.Builder
	for _, c := range b {
		switch {
		case c == '\\':
			out.WriteString(`\\`)
		case c < 0x20 || c > 0x7e:
			fmt.Fprintf(&out, `\%03o`, c)
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

func unescapeBytes(s string) string {
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			out.WriteByte(s[i])
			continue
		}
		if s[i+1] == '\\' {
			out.WriteByte('\\')
			i++
			continue
		}
		if i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				out.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		out.WriteByte(s[i])
	}
	return out.String()
}

// fnGetByte answers the byte at a zero-based position.
func fnGetByte(args []driver.Value) (driver.Value, error) {
	b, ok1 := toBytes(args[0])
	n, ok2 := toInt(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	if n < 0 || n >= int64(len(b)) {
		return nil, fmt.Errorf("dialect: GET_BYTE: index %d is out of range", n)
	}
	return int64(b[n]), nil
}

// fnSetByte answers a copy with one byte replaced, leaving the argument alone.
func fnSetByte(args []driver.Value) (driver.Value, error) {
	b, ok1 := toBytes(args[0])
	n, ok2 := toInt(args[1])
	value, ok3 := toInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if n < 0 || n >= int64(len(b)) {
		return nil, fmt.Errorf("dialect: SET_BYTE: index %d is out of range", n)
	}
	out := make([]byte, len(b))
	copy(out, b)
	// PostgreSQL takes the low byte of whatever it is given rather than
	// refusing a value outside 0..255.
	out[n] = uint8(value) //nolint:gosec // the conversion is the truncation PostgreSQL performs
	return out, nil
}

// parseFloatOrNothing reads a number, reporting whether the text held one.
func parseFloatOrNothing(s string) (float64, bool) {
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}

// fnTimeOfDay is PostgreSQL's textual clock reading.
func fnTimeOfDay(_ []driver.Value) (driver.Value, error) {
	return time.Now().Format("Mon Jan 02 15:04:05.000000 2006 MST"), nil
}
