package runtime

import (
	"database/sql/driver"
	"math"
	"strconv"
	"strings"
	"time"
)

// This file holds the reading of a time value out of text, which every dialect
// in this package needs and none of them owns: a cell loaded from a file is
// text, and a date function has to make a time of it before it can answer.

// timeLayouts are tried in order by parseTime, covering the common SQL date and
// datetime shapes. The Z07:00 pair reads a timezone suffix — a trailing Z or an
// offset like +09:00 — which BigQuery's TIMESTAMP literals carry.
// The unpadded and compact spellings are here because a file carries them:
// MySQL reads 2020-1-2 and 20200229 as dates, and a helper that refused them
// answered NULL for a column the caller had loaded and could see.
var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02 15:04",
	"2006-01-02",
	"2006/01/02 15:04:05",
	"2006/01/02",
	"2006-1-2 15:04:05.999999999",
	"2006-1-2 15:04:05",
	"2006-1-2 15:04",
	"2006-1-2",
	"2006/1/2 15:04:05",
	"2006/1/2",
	"20060102150405",
	"20060102",
	"15:04:05",
	"15:04:05.999999999",
}

// parseTime parses a date/time string against the supported layouts. A value
// with a timezone suffix is normalized to UTC, which is the timezone BigQuery
// extracts fields in by default.
func parseTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range timeLayouts {
		if tm, err := time.Parse(layout, s); err == nil {
			return tm.UTC(), true
		}
	}
	return time.Time{}, false
}

// formatFloatText renders a REAL as text the way the engines do. Go's shortest
// 'g' switches to exponent notation once the decimal exponent reaches 6, so a
// plain 1234567.5 came out of every function that reads its argument as text as
// "1.2345675e+06", where dialects.SQLite's own concatenation, dialects.MySQL and dialects.PostgreSQL all
// write 1234567.5. Between 1e-4 and 1e15 the three agree on plain notation and
// the value is written plainly; outside that band they disagree with each other
// and the shortest form is kept.
func formatFloatText(f float64) string {
	if abs := math.Abs(f); abs != 0 && (abs < 1e-4 || abs >= 1e15) {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// formatFloatTextMySQL renders a REAL the way MySQL writes one in a string
// context, which is not how formatFloatText writes it: MySQL has no plus sign
// and no padding in an exponent, and it keeps the plain spelling out to 1e14
// and down to 1e-15, so 1e308 is "1e308" rather than "1e+308" and 1e-5 is
// "0.00001" rather than "1e-05".
//
// Which of the two a value takes turns on where the decimal point sits relative
// to the digits rather than on the length alone, which is why 1234567890123456
// is written with an exponent and 1234567890123456.8, one digit longer, is
// written plainly. TestMySQLWritesARealTheWayTheEngineDoes holds the values
// this was read from.
func formatFloatTextMySQL(f float64) string {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		// Neither is a value SQLite can hold in a column loaded from a file,
		// and MySQL has no spelling for them; the shared one applies.
		return formatFloatText(f)
	}
	digits, point := shortestDecimal(f)
	if point <= -15 || (point >= 16 && point >= len(digits)) || plainTextWidth(len(digits), point) > 24 {
		return exponentTextMySQL(f, digits, point)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// shortestDecimal is the shortest run of digits that reads back as f, and where
// the decimal point sits in it: f is 0.<digits> times ten to the point.
func shortestDecimal(f float64) (digits string, point int) {
	written := strconv.FormatFloat(math.Abs(f), 'e', -1, 64)
	mantissa, exponent, _ := strings.Cut(written, "e")
	e, err := strconv.Atoi(exponent)
	if err != nil {
		// FormatFloat with 'e' always writes an exponent, so this cannot
		// happen; answering the mantissa alone keeps a caller from a panic.
		return strings.Replace(mantissa, ".", "", 1), 1
	}
	return strings.Replace(mantissa, ".", "", 1), e + 1
}

// plainTextWidth is how many characters the plain spelling of a value takes,
// counting the "0." a value below one opens with and the point a fraction needs.
func plainTextWidth(length, point int) int {
	if point <= 0 {
		return length - point + 2
	}
	if point < length {
		return length + 1
	}
	return point
}

// exponentTextMySQL writes a value in MySQL's exponent spelling: one digit, the
// rest after a point, then "e" and the exponent with a sign only when it is
// negative.
func exponentTextMySQL(f float64, digits string, point int) string {
	var b strings.Builder
	if math.Signbit(f) {
		b.WriteByte('-')
	}
	b.WriteByte(digits[0])
	if len(digits) > 1 {
		b.WriteByte('.')
		b.WriteString(digits[1:])
	}
	b.WriteByte('e')
	b.WriteString(strconv.Itoa(point - 1))
	return b.String()
}

// fnMySQLText writes a value the way MySQL writes one a string function reads.
// Only a REAL is rewritten, so a blob stays a blob and an integer stays an
// integer for the call it is handed to.
func fnMySQLText(args []driver.Value) (driver.Value, error) {
	if f, ok := args[0].(float64); ok {
		return formatFloatTextMySQL(f), nil
	}
	return args[0], nil
}

// mysqlTextAll is mysqlTextArgs over every argument.
func mysqlTextAll(fn scalarFn) scalarFn { return mysqlTextArgs(fn) }

// mysqlTextFrom is mysqlTextArgs over every argument from first onward, for the
// helpers whose text arguments are a tail of any length.
func mysqlTextFrom(fn scalarFn, first int) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		for i := first; i < len(args); i++ {
			if f, ok := args[i].(float64); ok {
				args[i] = formatFloatTextMySQL(f)
			}
		}
		return fn(args)
	}
}

// mysqlTextArgs returns fn with the arguments at the given positions -- every
// argument when none are named -- written the way MySQL writes a value a string
// function reads. Only a REAL is rewritten: MySQL spells an integer, a string
// and a blob the way this package already does.
//
// The helper it wraps stays shared rather than gaining a MySQL twin, because
// PostgreSQL and GoogleSQL refuse a float to these functions outright: there is
// no second answer to keep.
func mysqlTextArgs(fn scalarFn, positions ...int) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		if len(positions) == 0 {
			for i, arg := range args {
				if f, ok := arg.(float64); ok {
					args[i] = formatFloatTextMySQL(f)
				}
			}
			return fn(args)
		}
		for _, i := range positions {
			if i < 0 || i >= len(args) {
				continue
			}
			if f, ok := args[i].(float64); ok {
				args[i] = formatFloatTextMySQL(f)
			}
		}
		return fn(args)
	}
}

// toStringTime coerces a value to a time, accepting both strings and time.Time.
func toStringTime(v driver.Value) (time.Time, bool) {
	if tm, ok := v.(time.Time); ok {
		return tm, true
	}
	s, ok := toString(v)
	if !ok {
		return time.Time{}, false
	}
	return parseTime(s)
}
