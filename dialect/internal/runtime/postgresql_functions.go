package runtime

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
)

// This file holds the dialects.PostgreSQL-only scalar functions, the ones with no dialects.SQLite
// spelling. They are registered under the names dialects.PostgreSQL gives them rather
// than rewritten, so the call text the query wrote is what runs and the result
// column keeps its name; the few whose name dialects.SQLite already means something else
// by are rewritten in postgresql.go instead.
//
// Every answer below was read from dialects.PostgreSQL 17.10 rather than derived.

// postgresqlScalarFunctions are the deterministic dialects.PostgreSQL helpers.
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

		// The functions PostgreSQL refuses outside their domain, where
		// SQLite's own answer NULL or an infinity.
		"postgresql_sqrt":  {1, pgFloatFn(fnPostgresSqrt)},
		"postgresql_ln":    {1, pgFloatFn(pgLogarithm(math.Log))},
		"postgresql_log":   {-1, fnPostgresLog},
		"postgresql_exp":   {1, pgFloatFn(fnPostgresExp)},
		"postgresql_power": {2, fnPostgresPower},
		"postgresql_acos":  {1, pgFloatFn(pgBounded(-1, 1, math.Acos))},
		"postgresql_asin":  {1, pgFloatFn(pgBounded(-1, 1, math.Asin))},
		"postgresql_acosh": {1, pgFloatFn(func(x float64) (float64, error) {
			if x < 1 {
				return 0, errOutOfRange
			}
			return math.Acosh(x), nil
		})},
		"postgresql_atanh": {1, pgFloatFn(func(x float64) (float64, error) {
			if x < -1 || x > 1 {
				return 0, errOutOfRange
			}
			return math.Atanh(x), nil
		})},
		"postgresql_cot": {1, fnPostgresCot},

		// Arithmetic dialects.SQLite has no operator or function for.
		"cbrt": {1, fnCbrt},
		"erf":  {1, floatFn(math.Erf)},
		"erfc": {1, floatFn(math.Erfc)},
		// TO_TIMESTAMP reads no clock: it turns an epoch second or a formatted
		// string into a timestamp, so the same arguments always give the same
		// answer.
		"to_timestamp": {-1, fnToTimestamp},
		// dialects.PostgreSQL fixes these two when the statement begins, which is what
		// separates them from clock_timestamp. filesql runs each statement on
		// its own, so the transaction's start and the statement's are the same
		// moment; they are separate names because a query naming either should
		// not fail.
		"statement_timestamp":   {0, fnNow},
		"transaction_timestamp": {0, fnNow},
		"factorial":             {1, fnFactorial},
		"gcd":                   {2, fnGCD},
		"lcm":                   {2, fnLCM},

		// The trigonometric functions that take and answer degrees. They exist
		// so the quadrant angles are exact -- sind(30) is 0.5 and not
		// 0.49999999999999994 -- which is what converting through radians
		// costs. Away from those angles the answer is the conversion, and it
		// can differ from dialects.PostgreSQL's in the last place: dialects.PostgreSQL reduces
		// the angle to the first quadrant before converting, and the C library
		// underneath is not the same one either.
		"sind":     {1, degreeTrig(degreeSin)},
		"cosd":     {1, degreeTrig(degreeCos)},
		"tand":     {1, degreeTrig(degreeTan)},
		"cotd":     {1, degreeTrig(degreeCot)},
		"asind":    {1, inverseDegreeTrig(math.Asin, -1, 1)},
		"acosd":    {1, inverseDegreeTrig(math.Acos, -1, 1)},
		"atand":    {1, inverseDegreeTrig(math.Atan, math.Inf(-1), math.Inf(1))},
		"atan2d":   {2, fnAtan2d},
		"isfinite": {1, fnIsFinite},

		// Conversion.
		"to_number": {2, fnToNumber},

		// Building a date or a time out of its fields, and binning a timestamp.
		"make_date": {3, fnPostgresMakeDate},
		"make_time": {3, fnPostgresMakeTime},

		"make_timestamp": {6, fnPostgresMakeTimestamp},
		"make_interval":  {-1, fnPostgresMakeInterval},
		"date_bin":       {3, fnDateBin},

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

// postgresqlNonDeterministicFunctions must be called again for every row.
//
// dialects.PostgreSQL separates the moment of the call from the start of the statement:
// clock_timestamp and timeofday advance while a statement runs, which is the
// only reason either exists, while now, statement_timestamp and
// transaction_timestamp are fixed when the statement begins. That second group
// is registered as deterministic with the rest of the fixed clock, so it is not
// here.
func postgresqlNonDeterministicFunctions() map[string]scalarSpec {
	return map[string]scalarSpec{
		"clock_timestamp":   {0, fnClockTimestamp},
		"timeofday":         {0, fnTimeOfDay},
		"gen_random_uuid":   {0, fnGenerateUUID},
		"postgresql_random": {0, fnPostgresRandom},
	}
}

// --- quoting ---

// fnPostgresFormat implements dialects.PostgreSQL's format(fmt, ...), whose verbs are
// not printf's: %s writes the value, %I quotes it as an identifier, %L quotes it
// as a literal, %% is a percent sign, and a verb may name its argument by
// position as %n$s. dialects.SQLite's own format() is printf and answered NULL for the
// whole call whenever the string held a verb it did not know, so %I, %L and the
// positional form all came back as a NULL indistinguishable from a NULL
// argument.
func fnPostgresFormat(args []driver.Value) (driver.Value, error) {
	if len(args) == 0 {
		return nil, errors.New("dialect: format expects a format string")
	}
	spec, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	rest := args[1:]
	var b strings.Builder
	next := 0
	runes := []rune(spec)
	for i := 0; i < len(runes); i++ {
		if runes[i] != '%' {
			b.WriteRune(runes[i])
			continue
		}
		i++
		if i >= len(runes) {
			return nil, errors.New("dialect: format: the format string ends in an unfinished verb")
		}
		// An argument may name its position, as "%1$s".
		position := -1
		start := i
		for i < len(runes) && runes[i] >= '0' && runes[i] <= '9' {
			i++
		}
		if i > start && i < len(runes) && runes[i] == '$' {
			n, err := strconv.Atoi(string(runes[start:i]))
			if err != nil || n < 1 {
				return nil, fmt.Errorf("dialect: format: %q is not an argument position", string(runes[start:i]))
			}
			position = n - 1
			i++
		} else {
			i = start
		}
		if i >= len(runes) {
			return nil, errors.New("dialect: format: the format string ends in an unfinished verb")
		}
		verb := runes[i]
		if verb == '%' {
			b.WriteByte('%')
			continue
		}
		index := position
		if index < 0 {
			index = next
			next++
		}
		if index >= len(rest) {
			return nil, fmt.Errorf("dialect: format: too few arguments for %%%c", verb)
		}
		text, hasValue := toString(rest[index])
		switch verb {
		case 's':
			b.WriteString(text)
		case 'I':
			if !hasValue {
				return nil, errors.New("dialect: format: NULL cannot be an identifier")
			}
			quoted, err := fnQuoteIdent([]driver.Value{rest[index]})
			if err != nil {
				return nil, err
			}
			out, _ := toString(quoted)
			b.WriteString(out)
		case 'L':
			if !hasValue {
				b.WriteString("NULL")
				continue
			}
			b.WriteString("'" + strings.ReplaceAll(text, "'", "''") + "'")
		default:
			return nil, fmt.Errorf("dialect: format: unrecognized verb %%%c", verb)
		}
	}
	return b.String(), nil
}

// fnPostgresScale implements scale(numeric): the number of digits after the
// decimal point. dialects.SQLite has no numeric type to read a declared scale from, so
// this reads the value as it is written, which is what a column loaded from a
// file holds. A decimal literal in the query has already lost its trailing
// zeros by the time it arrives -- scale(1.230) answers 2 here and 3 in
// dialects.PostgreSQL -- which is the same ceiling the README states for column types.
func fnPostgresScale(args []driver.Value) (driver.Value, error) {
	s, ok := decimalText(args[0])
	if !ok {
		return nil, nil
	}
	_, fraction, found := strings.Cut(s, ".")
	if !found {
		return int64(0), nil
	}
	return int64(len(fraction)), nil
}

// fnPostgresMinScale implements min_scale(numeric): the scale left after the
// trailing zeros are dropped.
func fnPostgresMinScale(args []driver.Value) (driver.Value, error) {
	s, ok := decimalText(args[0])
	if !ok {
		return nil, nil
	}
	_, fraction, found := strings.Cut(s, ".")
	if !found {
		return int64(0), nil
	}
	return int64(len(strings.TrimRight(fraction, "0"))), nil
}

// fnPostgresTrimScale implements trim_scale(numeric): the value with its
// trailing zeros dropped.
func fnPostgresTrimScale(args []driver.Value) (driver.Value, error) {
	s, ok := decimalText(args[0])
	if !ok {
		return nil, nil
	}
	if !strings.Contains(s, ".") {
		return s, nil
	}
	trimmed := strings.TrimRight(s, "0")
	return strings.TrimSuffix(trimmed, "."), nil
}

// decimalText is the value written as a decimal, which is what the scale
// functions measure. A float is written without an exponent so its digits can
// be counted; a value that is not a number at all reports false.
func decimalText(v driver.Value) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case int64:
		return strconv.FormatInt(x, 10), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	}
	s, ok := toString(v)
	if !ok {
		return "", false
	}
	if _, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err != nil {
		return "", false
	}
	return strings.TrimSpace(s), true
}

// fnPostgresMakeTimestamp builds a timestamp out of its fields, which is
// fnPostgresMakeDate and fnPostgresMakeTime joined: the two guards are the ones
// that already refuse a field out of range, so the three cannot disagree about
// what a field may hold.
func fnPostgresMakeTimestamp(args []driver.Value) (driver.Value, error) {
	if len(args) != 6 {
		return nil, fmt.Errorf("dialect: MAKE_TIMESTAMP expects 6 arguments, got %d", len(args))
	}
	date, err := fnPostgresMakeDate(args[:3])
	if err != nil || date == nil {
		return date, err
	}
	clock, err := fnPostgresMakeTime(args[3:])
	if err != nil || clock == nil {
		return clock, err
	}
	tm, ok := parseTime(fmt.Sprintf("%s 00:00:00", date))
	if !ok {
		return nil, nil
	}
	// MAKE_TIME reaches 24:00:00, which as a timestamp is the start of the next
	// day rather than a twenty-fifth hour: PostgreSQL answers
	// 2020-03-01 00:00:00 for MAKE_TIMESTAMP(2020, 2, 29, 24, 0, 0).
	offset, ok := toMySQLTime(clock)
	if !ok {
		return nil, nil
	}
	tm = tm.Add(time.Duration(offset) * time.Microsecond)
	if !withinDateRange(tm) {
		return nil, nil
	}
	return formatDateTimeValue(tm), nil
}

// fnPostgresMakeInterval builds an interval out of its seven fields, written the
// way formatPostgresInterval writes the one AGE answers. A week is seven days,
// which is how PostgreSQL folds the field it has no unit for.
func fnPostgresMakeInterval(args []driver.Value) (driver.Value, error) {
	if len(args) > 7 {
		return nil, fmt.Errorf("dialect: MAKE_INTERVAL expects at most 7 arguments, got %d", len(args))
	}
	fields := make([]float64, 7)
	for i, arg := range args {
		n, ok := toFloat(arg)
		if !ok {
			return nil, nil
		}
		fields[i] = n
	}
	years, months, weeks, days := int(fields[0]), int(fields[1]), int(fields[2]), int(fields[3])
	seconds := fields[4]*3600 + fields[5]*60 + fields[6]
	months += years * 12
	days += weeks * 7
	// The clock is held as microseconds, so a seconds field whose product does
	// not fit is refused rather than converted: int64(float64) is
	// implementation-defined outside the range and answered MinInt64 here,
	// which printed as a nonsense span. PostgreSQL raises "interval out of
	// range" for the same input.
	scaled := math.Round(seconds * 1e6)
	if math.IsNaN(scaled) || math.Abs(scaled) > math.MaxInt64 {
		return nil, errors.New("dialect: MAKE_INTERVAL: the interval is out of range")
	}
	return formatPostgresInterval(months/12, months%12, days, int64(scaled)), nil
}

// fnPostgresAge implements age(a, b): the interval between two timestamps,
// written the way dialects.PostgreSQL writes one -- whole years, months and days, with a
// time of day when there is one.
func fnPostgresAge(args []driver.Value) (driver.Value, error) {
	later, ok1 := toStringTime(args[0])
	earlier, ok2 := toStringTime(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	negative := later.Before(earlier)
	if negative {
		later, earlier = earlier, later
	}
	years, months, days, micros := calendarDifference(earlier, later)
	if negative {
		years, months, days, micros = -years, -months, -days, -micros
	}
	return formatPostgresInterval(years, months, days, micros), nil
}

// calendarDifference is the years, months, days and nanoseconds from earlier to
// later, borrowing from the next field up the way a calendar subtraction does.
//
// The clock part is counted in microseconds rather than in whole seconds because
// a fraction that is dropped is not a small error: from 00:00:00.75 to the next
// day's 00:00:00.25 is 23:59:59.5, and rounding the two ends down made it a
// whole day.
func calendarDifference(earlier, later time.Time) (years, months, days int, micros int64) {
	years = later.Year() - earlier.Year()
	months = int(later.Month()) - int(earlier.Month())
	days = later.Day() - earlier.Day()
	micros = (clockNanos(later) - clockNanos(earlier)) / int64(time.Microsecond)
	if micros < 0 {
		micros += int64(24 * time.Hour / time.Microsecond)
		days--
	}
	for days < 0 {
		months--
		// The borrowed days come from the earlier timestamp's own month, not
		// from the month before the later one. dialects.PostgreSQL borrows that way, and
		// the two differ whenever the months have different lengths: from
		// 2024-01-31 to 2024-03-01 it is a month and a day, because January has
		// 31 days, while borrowing February's 29 leaves a negative day count.
		days += daysInMonth(earlier.Year(), earlier.Month())
	}
	if months < 0 {
		months += 12
		years--
	}
	return years, months, days, micros
}

// formatPostgresInterval writes an interval the way dialects.PostgreSQL prints
// one. Each field carries its own sign, which is what lets a mixed-sign
// interval read as "-1 mons +33 days" the way PostgreSQL writes it, and what
// keeps a negative one from being signed twice.
func formatPostgresInterval(years, months, days int, micros int64) string {
	parts := make([]string, 0, 4)
	if years != 0 {
		parts = append(parts, fmt.Sprintf("%d year%s", years, plural(years)))
	}
	if months != 0 {
		parts = append(parts, fmt.Sprintf("%d mon%s", months, plural(months)))
	}
	if days != 0 {
		parts = append(parts, fmt.Sprintf("%d day%s", days, plural(days)))
	}
	if micros != 0 {
		parts = append(parts, formatPostgresClock(micros))
	}
	if len(parts) == 0 {
		return "00:00:00"
	}
	return strings.Join(parts, " ")
}

// formatPostgresClock writes the clock part of an interval, with the fraction
// of a second PostgreSQL keeps and with its trailing zeros trimmed the way
// PostgreSQL trims them. A negative span carries the sign on the whole field
// rather than on each of its numbers.
func formatPostgresClock(micros int64) string {
	// The seconds and the fraction are split before the sign is taken, because
	// negating the smallest int64 overflows back to itself and printed every
	// field of the clock negative; neither half of it is that large.
	sign := ""
	seconds, fraction := micros/1e6, micros%1e6
	if micros < 0 {
		sign = "-"
		seconds, fraction = -seconds, -fraction
	}
	out := fmt.Sprintf("%s%02d:%02d:%02d", sign, seconds/3600, seconds%3600/60, seconds%60)
	if fraction == 0 {
		return out
	}
	return out + "." + strings.TrimRight(fmt.Sprintf("%06d", fraction), "0")
}

// plural follows dialects.PostgreSQL, which pluralizes on the signed value: a lone year
// prints as "1 year" and its negation as "-1 years".
func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// fnPostgresTypeOf implements pg_typeof(x) over the storage classes dialects.SQLite has,
// named the way dialects.PostgreSQL names the type it would have used.
func fnPostgresTypeOf(args []driver.Value) (driver.Value, error) {
	switch args[0].(type) {
	case nil:
		return "unknown", nil
	case int64:
		return "bigint", nil
	case float64:
		return "double precision", nil
	case []byte:
		return "bytea", nil
	default:
		return "text", nil
	}
}

// fnPostgresRandom implements dialects.PostgreSQL's random(), which answers a double in
// [0, 1). dialects.SQLite's own random() answers a pseudo-random signed 64-bit integer,
// so every idiom built on the dialects.PostgreSQL meaning broke silently: "WHERE
// random() < 0.1" selected about half the rows rather than a tenth.
func fnPostgresRandom(_ []driver.Value) (driver.Value, error) {
	return randFloat(), nil
}

// fnPostgresDateAdd implements dialects.PostgreSQL's "date + integer": the date moved by
// that many days. A timestamp keeps its time of day, which is what dialects.PostgreSQL
// answers for a timestamp on the left.
func fnPostgresDateAdd(args []driver.Value) (driver.Value, error) {
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	days, ok := toFloat(args[1])
	if !ok {
		return nil, nil
	}
	moved := tm.AddDate(0, 0, int(days))
	// A fractional day is hours, which is what dialects.PostgreSQL answers for a
	// timestamp plus a non-integer count; the whole part is already in AddDate.
	if frac := days - math.Trunc(days); frac != 0 {
		moved = moved.Add(time.Duration(frac * float64(24*time.Hour)))
	}
	if hasTimePart(args[0]) || moved.Hour() != 0 || moved.Minute() != 0 || moved.Second() != 0 {
		return formatDateTimeValue(moved), nil
	}
	return moved.Format(layoutDateOnly), nil
}

// fnPostgresDateDiff implements dialects.PostgreSQL's "date - date": the whole days
// between them, which is an integer rather than an interval.
func fnPostgresDateDiff(args []driver.Value) (driver.Value, error) {
	left, ok1 := toStringTime(args[0])
	right, ok2 := toStringTime(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return int64(left.Sub(right) / (24 * time.Hour)), nil
}

// fnQuoteIdent quotes a string as a SQL identifier, doubling any quote inside
// it. A name that needs no quoting -- one that starts with a lower-case letter
// or an underscore and holds nothing but lower-case letters, digits, underscores
// and dollar signs -- is returned as it is, which is what dialects.PostgreSQL does.
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
		// Only ASCII lowercase may stand unquoted. unicode.IsLower is wider
		// than that and let a non-ASCII letter through unquoted, so
		// quote_ident('éèê') answered the name itself where dialects.PostgreSQL quotes
		// it -- and an unquoted name is the one answer that is not safe to
		// paste back into SQL, which is the whole point of the function.
		case r == '_' || (r >= 'a' && r <= 'z'):
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

// sqlReservedWords are the words dialects.PostgreSQL quotes even when they are spelled
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
// kind the translation could not see. dialects.PostgreSQL chooses between a position and
// a pattern on the operand's declared type; dialects.SQLite has no declared type, so the
// choice is made from the value: a number is a position and anything else is a
// pattern. That is dialects.PostgreSQL's answer for every integer column and for every
// text column that does not hold digits, and differs from it for a text column
// that does. A string literal or a number literal in the query never reaches
// here -- the rewrite reads those from the query text, where dialects.PostgreSQL's own
// rule applies exactly.
func fnPostgresSubstringFrom(args []driver.Value) (driver.Value, error) {
	if _, isNumber := toFloat(args[1]); isNumber {
		return fnPostgreSQLSubstr(args)
	}
	return fnRegexpExtract(args)
}

// --- arithmetic ---

// floatFn builds a helper from a function of one float, for the ones whose
// whole definition is that function: a NULL argument answers NULL and anything
// that is not a number does too.
func floatFn(f func(float64) float64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		x, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		return f(x), nil
	}
}

// PostgreSQL raises where these functions leave their domain, and SQLite's own
// answer NULL or an infinity there. A NULL reads as missing data rather than as
// arithmetic the engine refused, so it survives into a report; an infinity
// survives further still. The messages are PostgreSQL 17's own.
var (
	errSqrtNegative = errors.New("dialect: cannot take square root of a negative number")
	errLogZero      = errors.New("dialect: cannot take logarithm of zero")
	errLogNegative  = errors.New("dialect: cannot take logarithm of a negative number")
	errZeroNegPow   = errors.New("dialect: zero raised to a negative power is undefined")
	errOverflow     = errors.New("dialect: value out of range: overflow")
	errUnderflow    = errors.New("dialect: value out of range: underflow")
	errOutOfRange   = errors.New("dialect: input is out of range")
	errComplexPower = errors.New("dialect: a negative number raised to a non-integer power yields a complex result")
)

// pgFloatFn builds a helper from a function of one float that may refuse its
// argument. A NULL argument stays NULL, which is what PostgreSQL answers for
// one.
func pgFloatFn(f func(float64) (float64, error)) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		x, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		return f(x)
	}
}

// pgLogarithm is the domain PostgreSQL's logarithms share: zero and every
// negative number are refused, and each by its own message.
func pgLogarithm(f func(float64) float64) func(float64) (float64, error) {
	return func(x float64) (float64, error) {
		switch {
		case x == 0:
			return 0, errLogZero
		case x < 0:
			return 0, errLogNegative
		}
		return f(x), nil
	}
}

// pgBounded refuses an argument outside [low, high], which is how PostgreSQL
// answers for the inverse trigonometric functions.
func pgBounded(low, high float64, f func(float64) float64) func(float64) (float64, error) {
	return func(x float64) (float64, error) {
		if x < low || x > high {
			return 0, errOutOfRange
		}
		return f(x), nil
	}
}

func fnPostgresSqrt(x float64) (float64, error) {
	if x < 0 {
		return 0, errSqrtNegative
	}
	return math.Sqrt(x), nil
}

func fnPostgresExp(x float64) (float64, error) {
	out := math.Exp(x)
	if math.IsInf(out, 0) {
		return 0, errOverflow
	}
	// The exponential of a finite number is never zero, so a zero here is a
	// result too small for a double. PostgreSQL 17 says so rather than
	// answering the zero: EXP(-1000) is "value out of range: underflow".
	if out == 0 && !math.IsInf(x, 0) {
		return 0, errUnderflow
	}
	return out, nil
}

// fnPostgresLog is LOG, which takes the base ten logarithm of one argument and
// the logarithm of the second in the base of the first.
func fnPostgresLog(args []driver.Value) (driver.Value, error) {
	log10 := pgLogarithm(math.Log10)
	// PostgreSQL has LOG(x) and LOG(base, x) and nothing else, and answers
	// that it has no such function for any other count.
	if len(args) == 0 || len(args) > 2 {
		return nil, fmt.Errorf("dialect: LOG takes one argument or two, got %d", len(args))
	}
	if len(args) == 1 {
		x, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		return log10(x)
	}
	base, ok1 := toFloat(args[0])
	x, ok2 := toFloat(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	// The base is a logarithm of its own, so a base of zero or a negative one
	// is refused by the same rule the value is.
	lb, err := log10(base)
	if err != nil {
		return nil, err
	}
	lx, err := log10(x)
	if err != nil {
		return nil, err
	}
	if lb == 0 {
		return nil, errLogZero
	}
	return lx / lb, nil
}

// fnPostgresPower is POWER, whose one refusal is a zero raised to a negative
// power. PostgreSQL computes the rest in arbitrary precision and this does not,
// so a result past the range of a float is reported rather than answered as an
// infinity.
func fnPostgresPower(args []driver.Value) (driver.Value, error) {
	base, ok1 := toFloat(args[0])
	exponent, ok2 := toFloat(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	if base == 0 && exponent < 0 {
		return nil, errZeroNegPow
	}
	// A negative base raised to a fraction has no real answer, and PostgreSQL
	// says that rather than answering the NaN math.Pow gives.
	if base < 0 && exponent != math.Trunc(exponent) {
		return nil, errComplexPower
	}
	out := math.Pow(base, exponent)
	if math.IsInf(out, 0) {
		return nil, errOverflow
	}
	// A zero from a base that is not zero is a result too small for a double.
	// PostgreSQL 17 refuses that as it refuses the overflow: POWER(1e-300, 2)
	// is "value out of range: underflow" there and 0 in MySQL.
	if out == 0 && base != 0 {
		return nil, errUnderflow
	}
	return out, nil
}

// fnPostgresCot is COT, which PostgreSQL answers with an infinity at zero where
// MySQL refuses it. The helper registered under the bare name is MySQL's, and
// every dialect resolved to it. The reciprocal of the tangent can disagree with
// PostgreSQL in the last bit, as MySQL's does, because Go's tangent and the C
// library's round differently.
func fnPostgresCot(args []driver.Value) (driver.Value, error) {
	x, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	return 1 / math.Tan(x), nil
}

func fnCbrt(args []driver.Value) (driver.Value, error) {
	x, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	return math.Cbrt(x), nil
}

// fnFactorial answers in 64 bits, which runs out at 20. dialects.PostgreSQL computes it
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

// fnGCD and fnLCM read their arguments unsigned, as dialects.PostgreSQL does: the sign
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

// exactDegreeValues are the angles dialects.PostgreSQL answers exactly. Its
// implementation reduces an angle to the first quadrant and answers the
// quadrant angles from a table rather than from the conversion to radians,
// which is the whole reason these functions exist beside the radian ones:
// SIND(30) is 0.5 and not 0.49999999999999994. Every value was read from
// dialects.PostgreSQL 17.10.
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
func inverseDegreeTrig(fn func(float64) float64, low, high float64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		x, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		if x < low || x > high {
			return nil, errOutOfRange
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
// dialects.PostgreSQL has "infinity" as a value of both types and filesql has no way to
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
	if t.digitCount() == 0 {
		// An empty template names no digit positions, so PostgreSQL reads
		// nothing and answers NULL. Scraping the digits out of the value
		// instead answered a number that is not in it in any recognizable
		// form: to_number('2024-02-29', '') was -20240229, the digits of a
		// date run together with the hyphens read as a sign.
		return nil, nil
	}
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
		// PostgreSQL raises for a value it cannot read rather than answering
		// NULL, and NULL is also its answer for a value that is absent, so the
		// two were indistinguishable: a query written to reject a bad number
		// reported success on exactly the rows it was written to reject.
		return nil, fmt.Errorf("dialect: invalid input syntax for type numeric: %q", s)
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
		tm, err := pgReadTemplate(format, s)
		if err != nil {
			return nil, err
		}
		return formatDateTimeValue(tm), nil
	default:
		return nil, fmt.Errorf("dialect: TO_TIMESTAMP expects 1 or 2 arguments, got %d", len(args))
	}
}

// --- building dates and times ---

// fnMakeDate builds a date out of its fields, refusing a field outside its
// range rather than carrying it into the next one, which is what dialects.PostgreSQL
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
	// PostgreSQL's time reaches 24:00:00 exactly, which is the end of the day
	// rather than the start of the next one; anything past it is refused.
	endOfDay := hour == 24 && minute == 0 && second == 0
	if (hour < 0 || hour > 23) && !endOfDay {
		return nil, fmt.Errorf("dialect: MAKE_TIME: hour %d is out of range", hour)
	}
	if minute < 0 || minute > 59 {
		return nil, fmt.Errorf("dialect: MAKE_TIME: minute %d is out of range", minute)
	}
	if second < 0 || second >= 60 {
		return nil, fmt.Errorf("dialect: MAKE_TIME: second %v is out of range", second)
	}
	out := fmt.Sprintf("%02d:%02d:%02d", hour, minute, int(second))
	// The seconds field takes a fraction, and dialects.PostgreSQL keeps it:
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
// which is what dialects.PostgreSQL's sha family does; a query prints one with
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

// toBytes reads a value as the bytes dialects.PostgreSQL's bytea holds.
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

// fnEncode spells bytes as text in one of the three encodings dialects.PostgreSQL has.
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
		// dialects.PostgreSQL ignores the whitespace its own encoder can insert.
		b, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(s), ""))
		if err != nil {
			return nil, fmt.Errorf("dialect: DECODE: %w", err)
		}
		return b, nil
	case "escape":
		b, err := unescapeBytes(s)
		if err != nil {
			return nil, fmt.Errorf("dialect: DECODE: %w", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("dialect: DECODE: unrecognized encoding %q", format)
	}
}

// decodeBytea reads the text of a bytea value in either of the two input
// formats dialects.PostgreSQL defines: the hexadecimal one, which the whole string
// carries after a leading backslash-x and which may hold whitespace between
// digit pairs, and the escape one, where a byte outside the printable range is
// written as a backslash and three octal digits. A string holding neither is
// its own bytes, which is what the escape format says of a string with no
// backslash in it.
func decodeBytea(s string) ([]byte, error) {
	if rest, hexForm := strings.CutPrefix(s, `\x`); hexForm {
		digits, ok := hexDigitPairs(rest)
		if !ok {
			return nil, fmt.Errorf("%w: whitespace in bytea is allowed only between hexadecimal pairs", ErrInvalidCast)
		}
		b, err := hex.DecodeString(digits)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid hexadecimal data for bytea: %w", ErrInvalidCast, err)
		}
		return b, nil
	}
	return unescapeBytes(s)
}

// hexDigitPairs joins the runs of hexadecimal digits whitespace separates, and
// reports false when a run holds half a pair. dialects.PostgreSQL allows whitespace
// "between digit pairs" and nowhere else, so it reads '\x41 42' and refuses
// '\x4 142', where dropping every space alike would have taken both.
func hexDigitPairs(s string) (string, bool) {
	var out strings.Builder
	out.Grow(len(s))
	for _, run := range strings.Fields(s) {
		if len(run)%2 == 1 {
			return "", false
		}
		out.WriteString(run)
	}
	return out.String(), true
}

// unescapeBytes reads dialects.PostgreSQL's escape format for bytea. An escape it has
// no meaning for is an error rather than a backslash written through: the
// backslash would come back as data the caller never wrote, and dialects.PostgreSQL
// refuses the same input.
func unescapeBytes(s string) ([]byte, error) {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			out = append(out, s[i])
			continue
		}
		if i+1 < len(s) && s[i+1] == '\\' {
			out = append(out, '\\')
			i++
			continue
		}
		if i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				out = append(out, byte(n))
				i += 3
				continue
			}
		}
		return nil, fmt.Errorf("%w: invalid input syntax for bytea at offset %d", ErrInvalidCast, i)
	}
	return out, nil
}

// escapeBytes writes the printable bytes as themselves and the rest as an octal
// escape, which is dialects.PostgreSQL's "escape" encoding.
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
	// dialects.PostgreSQL takes the low byte of whatever it is given rather than
	// refusing a value outside 0..255.
	out[n] = uint8(value) //nolint:gosec // the conversion is the truncation dialects.PostgreSQL performs
	return out, nil
}

// parseFloatOrNothing reads a number, reporting whether the text held one.
func parseFloatOrNothing(s string) (float64, bool) {
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}

// fnTimeOfDay is dialects.PostgreSQL's textual clock reading.
// fnClockTimestamp reads the clock itself rather than the reading the fixed
// clock shares. Moving while a statement runs is the whole of what separates
// this function from now, so it must not be given a value held still for one.
func fnClockTimestamp(_ []driver.Value) (driver.Value, error) {
	return time.Now().UTC().Format(layoutDateTime), nil
}

func fnTimeOfDay(_ []driver.Value) (driver.Value, error) {
	return time.Now().UTC().Format("Mon Jan 02 15:04:05.000000 2006 MST"), nil
}
