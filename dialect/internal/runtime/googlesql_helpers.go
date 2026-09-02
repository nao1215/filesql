package runtime

import (
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// This file holds the dialects.GoogleSQL functions the lowering layer reaches under a
// rewritten name, and the ones shared with no other dialect that grew up here.
// googlesql_functions.go holds the ones registered under BigQuery's own name.

// fnSafeDivide implements dialects.GoogleSQL SAFE_DIVIDE(x, y): x/y, or NULL when y is 0
// or either argument is NULL.
func fnSafeDivide(args []driver.Value) (driver.Value, error) {
	x, ok1 := toFloat(args[0])
	y, ok2 := toFloat(args[1])
	if !ok1 || !ok2 || y == 0 {
		return nil, nil
	}
	return x / y, nil
}

// fnStartsWith implements dialects.GoogleSQL STARTS_WITH(value, prefix).
func fnStartsWith(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	prefix, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return boolToInt(strings.HasPrefix(s, prefix)), nil
}

// fnEndsWith implements dialects.GoogleSQL ENDS_WITH(value, suffix).
func fnEndsWith(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	suffix, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return boolToInt(strings.HasSuffix(s, suffix)), nil
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// fnRegexpContains implements dialects.GoogleSQL REGEXP_CONTAINS(value, pattern). Note the
// argument order is (value, pattern), the reverse of the REGEXP operator.
func fnRegexpContains(args []driver.Value) (driver.Value, error) {
	value, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return nil, err
	}
	return boolToInt(re.MatchString(value)), nil
}

// fnRegexpExtract implements dialects.GoogleSQL REGEXP_EXTRACT(value, pattern): the first
// capturing group when the pattern has one, otherwise the whole match, or NULL
// when there is no match.
func fnRegexpExtract(args []driver.Value) (driver.Value, error) {
	value, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return nil, err
	}
	m := re.FindStringSubmatch(value)
	if m == nil {
		return nil, nil
	}
	if len(m) > 1 {
		return m[1], nil
	}
	return m[0], nil
}

// fnDateDiff3 implements dialects.GoogleSQL DATE_DIFF/TIMESTAMP_DIFF(a, b, unit): the
// signed count of unit boundaries from b to a.
func fnDateDiff3(args []driver.Value) (driver.Value, error) {
	a, ok1 := toStringTime(args[0])
	b, ok2 := toStringTime(args[1])
	unit, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	switch part := strings.ToLower(strings.TrimSpace(unit)); part {
	case unitYear:
		return int64(a.Year() - b.Year()), nil
	case unitISOYear:
		yearA, _ := a.ISOWeek()
		yearB, _ := b.ISOWeek()
		return int64(yearA - yearB), nil
	case unitQuarter:
		return int64((a.Year()*4 + (int(a.Month())-1)/3) - (b.Year()*4 + (int(b.Month())-1)/3)), nil
	case unitMonth:
		return int64((a.Year()*12 + int(a.Month())) - (b.Year()*12 + int(b.Month()))), nil
	case unitWeek, unitISOWeek:
		// A week difference counts the week boundaries crossed rather than the
		// seven-day spans between the two dates, so a Saturday and the Sunday
		// after it are one week apart. WEEK begins on Sunday and ISOWEEK on
		// Monday.
		start := time.Sunday
		if part == unitISOWeek {
			start = time.Monday
		}
		return int64(weekBoundariesBetween(a, b, start)), nil
	case unitDay:
		return dayNumber(a) - dayNumber(b), nil
	case unitHour:
		return secondsBetween(a, b) / 3600, nil
	case unitMinute:
		return secondsBetween(a, b) / 60, nil
	case unitSecond:
		return secondsBetween(a, b), nil
	case unitMillisecond:
		return a.Sub(b).Milliseconds(), nil
	case unitMicrosecond:
		return a.Sub(b).Microseconds(), nil
	default:
		if start, ok := weekStartDay(part); ok {
			return int64(weekBoundariesBetween(a, b, start)), nil
		}
		return nil, fmt.Errorf("dialect: unsupported DATE_DIFF unit %q", unit)
	}
}

// weekBoundariesBetween counts the weeks that begin on start between two dates,
// signed, which is what DATE_DIFF counts for every week spelling.
func weekBoundariesBetween(a, b time.Time, start time.Weekday) int {
	weekStartDayNumber := func(t time.Time) int64 {
		return dayNumber(t) - int64((int(t.Weekday())-int(start)+7)%7)
	}
	return int((weekStartDayNumber(a) - weekStartDayNumber(b)) / 7)
}

// dayNumber is the day a time falls on, counted from 1970-01-01 and computed
// from the civil date rather than from a duration. A time.Duration is an int64
// of nanoseconds, so subtracting two times saturates about 292 years out and
// answers the bound rather than the distance -- which made the days to a
// 9999-12-31 sentinel 106751 instead of 2913109.
func dayNumber(t time.Time) int64 {
	y, m, d := t.Date()
	return civilDayNumber(int64(y), int64(m), int64(d))
}

// civilDayNumber is the day number of a proleptic Gregorian date, counted from
// 1970-01-01. It shifts the year to start in March so a leap day falls at the
// end of a four-century cycle, which is what lets the whole thing be integer
// arithmetic with no table and no bound short of the int64 the year is in.
func civilDayNumber(year, month, day int64) int64 {
	if month <= 2 {
		year--
	}
	era := year / 400
	if year < 0 {
		era = (year - 399) / 400
	}
	yearOfEra := year - era*400 // [0, 399]
	monthShift := int64(9)
	if month > 2 {
		monthShift = -3
	}
	dayOfYear := (153*(month+monthShift)+2)/5 + day - 1                 // [0, 365]
	dayOfEra := yearOfEra*365 + yearOfEra/4 - yearOfEra/100 + dayOfYear // [0, 146096]
	return era*146097 + dayOfEra - 719468
}

// secondsBetween is the signed distance between two times in seconds, built
// from the day number and the time of day so it is exact for every date the
// calendar holds.
func secondsBetween(a, b time.Time) int64 {
	const secondsPerDay = 24 * 60 * 60
	days := dayNumber(a) - dayNumber(b)
	return days*secondsPerDay + secondsOfDay(a) - secondsOfDay(b)
}

// secondsOfDay is how far into its day a time is, in whole seconds.
func secondsOfDay(t time.Time) int64 {
	return int64(t.Hour())*3600 + int64(t.Minute())*60 + int64(t.Second())
}

// fnMySQLDateDiff implements dialects.MySQL TIMESTAMPDIFF(unit, start, end), called as
// mysql_date_diff(end, start, 'unit'): the signed number of complete units from
// start to end, where fnDateDiff3 counts the boundaries BigQuery counts. DAY
// and WEEK are complete 24-hour and 7-day periods of the actual span, so 23
// hours is zero days; MONTH, QUARTER and YEAR are complete calendar months.
func fnMySQLDateDiff(args []driver.Value) (driver.Value, error) {
	end, ok1 := toStringTime(args[0])
	start, ok2 := toStringTime(args[1])
	unit, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case unitMicrosecond:
		return end.Sub(start).Microseconds(), nil
	case unitSecond:
		return wholeUnits(end, start, 1), nil
	case unitMinute:
		return wholeUnits(end, start, 60), nil
	case unitHour:
		return wholeUnits(end, start, 3600), nil
	case unitDay:
		return wholeUnits(end, start, 24*3600), nil
	case unitWeek:
		return wholeUnits(end, start, daysPerWeek*24*3600), nil
	case unitMonth:
		return completeMonths(end, start), nil
	case unitQuarter:
		return completeMonths(end, start) / monthsPerQuartr, nil
	case unitYear:
		return completeMonths(end, start) / 12, nil
	default:
		return nil, fmt.Errorf("dialect: unsupported TIMESTAMPDIFF unit %q", unit)
	}
}

// wholeUnits is the signed number of complete unitSeconds-second units from
// start to end, truncated toward zero. It counts in whole seconds rather than
// through a time.Duration, which saturates at about ±292 years while dialects.MySQL's
// DATETIME range spans nine millennia.
func wholeUnits(end, start time.Time, unitSeconds int64) int64 {
	seconds := end.Unix() - start.Unix()
	// The nanosecond fields decide whether the span's last second is complete.
	if seconds > 0 && end.Nanosecond() < start.Nanosecond() {
		seconds--
	} else if seconds < 0 && end.Nanosecond() > start.Nanosecond() {
		seconds++
	}
	return seconds / unitSeconds
}

// completeMonths is the signed number of complete calendar months from start to
// end: the month delta, minus one when the end's day and time of day fall short
// of the start's. dialects.MySQL has no month-end special case, so January 31 to
// February 29 is zero complete months even though February 29 ends its month.
func completeMonths(end, start time.Time) int64 {
	sign := int64(1)
	if end.Before(start) {
		end, start = start, end
		sign = -1
	}
	months := int64((end.Year()-start.Year())*12 + int(end.Month()) - int(start.Month()))
	if end.Day() < start.Day() || (end.Day() == start.Day() && clockNanos(end) < clockNanos(start)) {
		months--
	}
	return sign * months
}

// clockNanos is the time of day as nanoseconds since midnight.
func clockNanos(tm time.Time) int64 {
	return int64(tm.Hour())*int64(time.Hour) + int64(tm.Minute())*int64(time.Minute) +
		int64(tm.Second())*int64(time.Second) + int64(tm.Nanosecond())
}

// mysqlTimeMaxSeconds is the ceiling of dialects.MySQL's TIME type, 838:59:59, which
// TIMEDIFF clamps to on both sides.
const mysqlTimeMaxSeconds = 838*3600 + 59*60 + 59

// fnMySQLTimeDiff implements dialects.MySQL TIMEDIFF(a, b): a minus b rendered as a
// dialects.MySQL TIME, whose hours keep counting past 24 and clamp at ±838:59:59.
// Two datetimes and two bare TIME values both work; mixing the two shapes is
// NULL, as it is in dialects.MySQL, and so is a value that does not parse.
func fnMySQLTimeDiff(args []driver.Value) (driver.Value, error) {
	s1, ok1 := toString(args[0])
	s2, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	if hasCalendarDate(s1) != hasCalendarDate(s2) {
		return nil, nil
	}
	if !hasCalendarDate(s1) {
		// Bare TIME values, whose hours may pass 23 and carry a sign, which
		// the calendar parser cannot read.
		n1, ok1 := mysqlClockNanos(s1)
		n2, ok2 := mysqlClockNanos(s2)
		if !ok1 || !ok2 {
			return nil, nil
		}
		return renderMySQLTime(n1 - n2), nil
	}
	t1, ok1 := parseTime(s1)
	t2, ok2 := parseTime(s2)
	if !ok1 || !ok2 {
		return nil, nil
	}
	// Sub saturates at ±292 years, far past the ±838:59:59 clamp, so the
	// saturated value renders the same clamped TIME.
	return renderMySQLTime(int64(t1.Sub(t2))), nil
}

// renderMySQLTime renders signed nanoseconds as a dialects.MySQL TIME, clamped to
// ±838:59:59, with a six-digit fraction only when the value has one.
func renderMySQLTime(nanos int64) string {
	sign := ""
	if nanos < 0 {
		sign = "-"
		nanos = -nanos
	}
	if nanos > mysqlTimeMaxSeconds*int64(time.Second) {
		nanos = mysqlTimeMaxSeconds * int64(time.Second)
	}
	secs := nanos / int64(time.Second)
	out := fmt.Sprintf("%s%02d:%02d:%02d", sign, secs/3600, secs/60%60, secs%60)
	if frac := nanos % int64(time.Second); frac != 0 {
		out += fmt.Sprintf(".%06d", frac/int64(time.Microsecond))
	}
	return out
}

// mysqlClockNanos reads a dialects.MySQL TIME value — an optional sign, hours that may
// pass 23, minutes, seconds and an optional fraction — as signed nanoseconds
// from 00:00:00. ok is false when s is not that shape.
func mysqlClockNanos(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	rest, negative := strings.CutPrefix(s, "-")
	clock, frac, hasFrac := strings.Cut(rest, ".")
	parts := strings.Split(clock, ":")
	if len(parts) != 3 {
		return 0, false
	}
	fields := make([]int64, 3)
	for i, part := range parts {
		if part == "" || (i > 0 && len(part) > 2) {
			return 0, false
		}
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return 0, false
		}
		fields[i] = n
	}
	if fields[1] > 59 || fields[2] > 59 {
		return 0, false
	}
	// dialects.MySQL clamps each TIME argument to its range before subtracting, so
	// TIMEDIFF('2000:00:00', '1000:00:00') is zero. Clamping here also keeps
	// an absurd hour count from overflowing the multiplication.
	if fields[0] > mysqlTimeMaxSeconds/3600 {
		nanos := int64(mysqlTimeMaxSeconds) * int64(time.Second)
		if negative {
			nanos = -nanos
		}
		return nanos, true
	}
	nanos := (fields[0]*3600 + fields[1]*60 + fields[2]) * int64(time.Second)
	if hasFrac {
		if len(frac) > 9 {
			frac = frac[:9]
		}
		n, err := strconv.ParseInt(frac+strings.Repeat("0", 9-len(frac)), 10, 64)
		if err != nil {
			return 0, false
		}
		nanos += n
	}
	if negative {
		nanos = -nanos
	}
	return nanos, true
}

// hasCalendarDate reports whether a datetime string carries a calendar date,
// which is what separates '2024-01-01 01:00:00' from a bare '01:00:00'. A
// leading minus is a TIME's sign, not a date separator.
func hasCalendarDate(s string) bool {
	return strings.ContainsAny(strings.TrimPrefix(strings.TrimSpace(s), "-"), "-/")
}

// strftimeToGoLayout maps the strftime-style specifiers dialects.GoogleSQL uses in
// FORMAT_DATE and PARSE_DATE to Go reference-time layout fragments. They differ
// from the dialects.MySQL DATE_FORMAT set: %M is the minute here but the month name
// there, and the month and weekday names are %B and %A.
var strftimeToGoLayout = map[byte]string{
	'Y': "2006",
	'y': "06",
	'm': "01",
	'd': "02",
	'e': "_2",
	'H': "15",
	'I': "03",
	'M': "04",
	'S': "05",
	'p': "PM",
	'B': layoutMonthLong,
	'b': layoutMonthShort,
	'A': layoutWeekdayLong,
	'a': layoutWeekdayShort,
	'F': layoutDateOnly,
	'T': layoutTimeOnly,
	'R': "15:04",
	'h': layoutMonthShort,
	'P': "pm",
	'D': "01/02/06",
	'x': "01/02/06",
	'X': layoutTimeOnly,
	'c': "Mon Jan _2 15:04:05 2006",
}

// strftimeLayout converts a dialects.GoogleSQL format string into a Go layout. An
// unknown specifier contributes its own letter, mirroring how DATE_FORMAT
// handles the same case.
func strftimeLayout(format string) string {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] == '%' && i+1 < len(format) {
			if layout, ok := strftimeToGoLayout[format[i+1]]; ok {
				b.WriteString(layout)
			} else if format[i+1] == '%' {
				b.WriteByte('%')
			} else {
				b.WriteByte(format[i+1])
			}
			i++
			continue
		}
		b.WriteByte(format[i])
	}
	return b.String()
}

// strftimeRender writes tm according to a dialects.GoogleSQL format string.
//
// Rendering is separate from strftimeLayout, which builds a Go layout for
// PARSE_DATE, because a layout can only carry what Go has a reference-time
// fragment for. A day of the year, a week number and an epoch second are
// computed rather than spelled, so twenty specifiers had nowhere to go and were
// written as their own letter -- which is what BigQuery does for a specifier it
// does not know, so a format asking for a time came back holding an "X" and
// looked like it had worked.
//
// Building the answer rather than a layout also stops the text around the
// specifiers being read as one. A literal "2006" or "1" in a format string used
// to reach time.Format, which rendered it as the year or the month; the
// characters between specifiers are copied verbatim now.
func strftimeRender(tm time.Time, format string) string {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			b.WriteByte(format[i])
			continue
		}
		spec := format[i+1]
		i++
		if spec == '%' {
			b.WriteByte('%')
			continue
		}
		if layout, ok := strftimeToGoLayout[spec]; ok {
			b.WriteString(tm.Format(layout))
			continue
		}
		if computed, ok := strftimeComputed(tm, spec); ok {
			b.WriteString(computed)
			continue
		}
		// An unknown specifier is its own letter, which is what BigQuery does.
		b.WriteByte(spec)
	}
	return b.String()
}

// strftimeComputed handles the specifiers with no Go layout fragment: the ones
// whose value is calculated from the date rather than spelled out of it.
//
// The week numbers are the four strftime defines. %V and %G are the ISO pair Go
// answers directly; %U counts weeks from the year's first Sunday and %W from its
// first Monday, both numbering the days before that week 0, which is the
// calculation the C library does and is written out here rather than derived
// from the ISO pair, since the three disagree at every turn of the year.
func strftimeComputed(tm time.Time, spec byte) (string, bool) {
	switch spec {
	case 'j':
		return fmt.Sprintf("%03d", tm.YearDay()), true
	case 's':
		return strconv.FormatInt(tm.Unix(), 10), true
	case 'C':
		return fmt.Sprintf("%02d", tm.Year()/100), true
	case 'Q':
		return strconv.Itoa((int(tm.Month())-1)/3 + 1), true
	case 'k':
		return fmt.Sprintf("%2d", tm.Hour()), true
	case 'l':
		hour := tm.Hour() % 12
		if hour == 0 {
			hour = 12
		}
		return fmt.Sprintf("%2d", hour), true
	case 'u':
		return strconv.Itoa(weekdayIndex(tm, true) + 1), true
	case 'w':
		return strconv.Itoa(int(tm.Weekday())), true
	case 'G':
		year, _ := tm.ISOWeek()
		return strconv.Itoa(year), true
	case 'V':
		_, week := tm.ISOWeek()
		return fmt.Sprintf("%02d", week), true
	case 'U':
		return fmt.Sprintf("%02d", (tm.YearDay()-1+7-int(tm.Weekday()))/7), true
	case 'W':
		return fmt.Sprintf("%02d", (tm.YearDay()-1+7-weekdayIndex(tm, true))/7), true
	case 'n':
		return "\n", true
	case 't':
		return "\t", true
	default:
		return "", false
	}
}

// fnFormatDate implements dialects.GoogleSQL FORMAT_DATE/FORMAT_DATETIME/
// FORMAT_TIMESTAMP(format, value). The format comes first, the reverse of dialects.MySQL
// DATE_FORMAT.
func fnFormatDate(args []driver.Value) (driver.Value, error) {
	format, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	tm, ok := toStringTime(args[1])
	if !ok {
		return nil, nil
	}
	return strftimeRender(tm, format), nil
}

// fnParseDate implements dialects.GoogleSQL PARSE_DATE(format, text) and fnParseTimestamp
// its datetime counterparts, returning NULL when the text does not match.
func fnParseDate(args []driver.Value) (driver.Value, error) {
	return parseWithStrftime(args, layoutDateOnly)
}

func fnParseTimestamp(args []driver.Value) (driver.Value, error) {
	return parseWithStrftime(args, layoutDateTime)
}

func parseWithStrftime(args []driver.Value, out string) (driver.Value, error) {
	format, ok1 := toString(args[0])
	s, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	tm, ok := parseLayout(strftimeLayout(format), s)
	if !ok {
		return nil, nil
	}
	return tm.Format(out), nil
}

// unixScale builds UNIX_SECONDS/UNIX_MILLIS/UNIX_MICROS, which differ only in
// how many sub-second units they report.
func unixScale(perSecond int64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		tm, ok := toStringTime(args[0])
		if !ok {
			return nil, nil
		}
		return tm.Unix()*perSecond + int64(tm.Nanosecond())/(1000000000/perSecond), nil
	}
}

// fromUnixScale builds TIMESTAMP_SECONDS/TIMESTAMP_MILLIS/TIMESTAMP_MICROS, the
// inverses of unixScale.
func fromUnixScale(perSecond int64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		n, ok := toInt(args[0])
		if !ok {
			return nil, nil
		}
		nanos := (n % perSecond) * (1000000000 / perSecond)
		return time.Unix(n/perSecond, nanos).UTC().Format(layoutDateTime), nil
	}
}

// fnToHex implements dialects.GoogleSQL TO_HEX(bytes): the lowercase hexadecimal form of
// the value's bytes.
func fnToHex(args []driver.Value) (driver.Value, error) {
	switch x := args[0].(type) {
	case nil:
		return nil, nil
	case []byte:
		return hex.EncodeToString(x), nil
	default:
		s, ok := toString(args[0])
		if !ok {
			return nil, nil
		}
		return hex.EncodeToString([]byte(s)), nil
	}
}

// fnPostgresToHex implements dialects.PostgreSQL to_hex(n): the lowercase hexadecimal
// digits of an integer, with a negative read as a 64-bit two's complement value
// the way dialects.PostgreSQL reads it. dialects.PostgreSQL has no string form of the function, so
// a value that names no integer is refused rather than hexed as text, which is
// what dialects.GoogleSQL's TO_HEX does with its bytes.
func fnPostgresToHex(args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	n, ok, err := postgresHexArgument(args[0])
	if err != nil || !ok {
		return nil, err
	}
	// The unsigned reading is the point: dialects.PostgreSQL answers the digits of the
	// 64-bit two's complement value, so to_hex(-1) is sixteen f's.
	return strconv.FormatUint(uint64(n), 16), nil //nolint:gosec // the two's complement reading is what dialects.PostgreSQL prints for a negative
}

// postgresHexArgument reads the integer to_hex converts. ok is false when the
// value carries no value at all; err is set when it names something that is not
// an integer, which dialects.PostgreSQL has no to_hex for.
func postgresHexArgument(v driver.Value) (int64, bool, error) {
	switch x := v.(type) {
	case int64:
		return x, true, nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) || x != math.Trunc(x) {
			return 0, false, fmt.Errorf("%w: to_hex expects an integer, got %v", ErrInvalidCast, x)
		}
		return int64(x), true, nil
	}
	s, ok := toString(v)
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("%w: to_hex expects an integer, got %q", ErrInvalidCast, s)
	}
	return n, true, nil
}

// fnMySQLFormat implements dialects.MySQL FORMAT(x, d): x rounded to d decimal places and
// written with a comma every three digits. dialects.SQLite has a format() of its own, an
// alias of printf, so an untranslated call answered the first argument expanded
// as a format string instead.
func fnMySQLFormat(args []driver.Value) (driver.Value, error) {
	d, ok2 := toInt(args[1])
	if !ok2 {
		return nil, nil
	}
	// dialects.MySQL reads a negative number of decimal places as none, and caps the
	// count at the 30 its DECIMAL type holds.
	if d < 0 {
		d = 0
	}
	if d > mysqlFormatMaxDecimals {
		d = mysqlFormatMaxDecimals
	}
	// An integer is grouped from its own digits. Widening it to a float64 first
	// lost the last digits of anything past 2^53, so FORMAT on a large id column
	// printed a number one or two away from the one stored.
	if n, isInt := args[0].(int64); isInt {
		return formatGrouped(strconv.FormatInt(n, 10), int(d)), nil
	}
	x, ok1 := mysqlNumericArgument(args[0])
	if !ok1 {
		return nil, nil
	}
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return nil, nil
	}
	text := mysqlFormatDecimal(x, int(d))
	sign := ""
	if strings.HasPrefix(text, "-") {
		sign, text = "-", text[1:]
	}
	whole, fraction, hasFraction := strings.Cut(text, ".")
	out := sign + groupThousands(whole)
	if hasFraction {
		out += "." + fraction
	}
	return out, nil
}

// formatGrouped writes an integer already spelled in decimal the way dialects.MySQL's
// FORMAT writes it: grouped in threes, with decimals decimal places of zeros
// after it.
func formatGrouped(digits string, decimals int) string {
	sign := ""
	if strings.HasPrefix(digits, "-") {
		sign, digits = "-", digits[1:]
	}
	out := sign + groupThousands(digits)
	if decimals > 0 {
		out += "." + strings.Repeat("0", decimals)
	}
	return out
}

// mysqlFormatMaxDecimals is the number of decimal places dialects.MySQL FORMAT keeps at
// most, which is the scale of its DECIMAL type.
// mysqlFormatDecimal is the number MySQL's FORMAT groups: the shortest decimal
// that reads back as the same double, rounded to decimals places with a half
// going to the even neighbor.
//
// Both halves of that are the engine's. Formatting the double itself printed
// the exact value of the binary number, so FORMAT(1e100, 2) came out as
// 10000000000000000159028911097599180468360808563945281389781327557747838772170381060813469985856815104.00
// where MySQL prints 1 followed by 100 zeros: the shortest decimal is the one
// the caller wrote, so what fills the places past it is zeros rather than the
// double's own digits. And a half goes to the even neighbor for a
// floating-point argument, the rule ROUND is on in MySQL and PostgreSQL, so
// FORMAT(2.5e0, 0) is 2 and FORMAT(3.5e0, 0) is 4.
//
// The rounding runs on the digits rather than on the value, because scaling a
// double by a power of ten is what loses the tie: 2.675 is a shade below itself
// in binary, so 2.675 * 100 is 267.49999999999997 and rounds down, where MySQL
// answers 2.68.
func mysqlFormatDecimal(x float64, decimals int) string {
	text := strconv.FormatFloat(x, 'f', -1, 64)
	sign := ""
	if strings.HasPrefix(text, "-") {
		sign, text = "-", text[1:]
	}
	whole, fraction, _ := strings.Cut(text, ".")
	if len(fraction) < decimals {
		fraction += strings.Repeat("0", decimals-len(fraction))
	}
	digits := whole + fraction[:decimals]
	if stepsUpHalfToEven(digits, fraction[decimals:]) {
		digits = incrementDecimal(digits)
	}
	if len(digits) <= decimals {
		digits = strings.Repeat("0", decimals+1-len(digits)) + digits
	}
	out := sign + digits[:len(digits)-decimals]
	if decimals > 0 {
		out += "." + digits[len(digits)-decimals:]
	}
	return out
}

// stepsUpHalfToEven reports whether the digits being cut off ask the kept ones
// to be stepped up: more than a half does, and exactly a half does only when
// the last kept digit is odd.
func stepsUpHalfToEven(kept, cut string) bool {
	if cut == "" || cut[0] < '5' {
		return false
	}
	if cut[0] > '5' {
		return true
	}
	for i := 1; i < len(cut); i++ {
		if cut[i] != '0' {
			return true
		}
	}
	last := byte('0')
	if kept != "" {
		last = kept[len(kept)-1]
	}
	return (last-'0')%2 == 1
}

// incrementDecimal adds one to a run of decimal digits, growing it by a place
// when every digit was a nine.
func incrementDecimal(digits string) string {
	out := []byte(digits)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != '9' {
			out[i]++
			return string(out)
		}
		out[i] = '0'
	}
	return "1" + string(out)
}

const mysqlFormatMaxDecimals = 30

// fnGoogleSQLFormat implements dialects.GoogleSQL FORMAT(format, ...). The verbs it
// shares with printf are handed to Sprintf, so they answer what dialects.SQLite's printf
// answered before; %t and %T are BigQuery's own and are printed here. Left to
// dialects.SQLite they made the whole call NULL, since printf answers NULL for a format
// string holding a verb it does not know.
func fnGoogleSQLFormat(args []driver.Value) (driver.Value, error) {
	if len(args) == 0 {
		return nil, errors.New("dialect: FORMAT expects a format string")
	}
	format, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	rest, next := args[1:], 0
	take := func() driver.Value {
		if next >= len(rest) {
			return nil
		}
		v := rest[next]
		next++
		return v
	}
	// The result is built in a byte slice rather than a strings.Builder so the
	// printf verbs can be appended in place: Appendf writes into it, where
	// Sprintf would allocate a string per verb only to copy it in.
	out := make([]byte, 0, len(format)+16)
	for i := 0; i < len(format); {
		if format[i] != '%' {
			out = append(out, format[i])
			i++
			continue
		}
		spec, verb, end := scanFormatSpec(format, i)
		switch {
		case end < 0:
			// A trailing "%" with no verb after it stands for itself.
			out = append(out, format[i:]...)
			i = len(format)
		case verb == '%':
			out = append(out, '%')
			i = end
		case verb == 't' || verb == 'T':
			out = append(out, googlesqlPrintValue(take(), verb == 'T')...)
			i = end
		default:
			operands := make([]any, 0, strings.Count(spec, "*")+1)
			for range strings.Count(spec, "*") {
				operands = append(operands, formatOperand('d', take()))
			}
			operands = append(operands, formatOperand(verb, take()))
			out = fmt.Appendf(out, goFormatSpec(spec, verb), operands...)
			i = end
		}
	}
	return string(out), nil
}

// scanFormatSpec reads the conversion specification that starts at the "%" at
// index start, returning the specification text, its verb, and the index just
// past it. end is -1 when the string ends before a verb.
func scanFormatSpec(format string, start int) (string, byte, int) {
	i := start + 1
	for i < len(format) && strings.IndexByte("+-# 0123456789.*'", format[i]) >= 0 {
		i++
	}
	if i >= len(format) {
		return format[start:], 0, -1
	}
	return format[start : i+1], format[i], i + 1
}

// goFormatSpec adapts a printf specification to the one Go understands: the
// apostrophe flag, which asks C for digit grouping, has no Go equivalent and is
// dropped, and %i is the spelling of %d that C printf accepts.
func goFormatSpec(spec string, verb byte) string {
	spec = strings.ReplaceAll(spec, "'", "")
	if verb == 'i' {
		spec = spec[:len(spec)-1] + "d"
	}
	return spec
}

// formatOperand coerces a value to the Go type the verb prints, the way dialects.SQLite's
// printf coerces its own arguments: a missing or unreadable value prints as the
// zero of the verb's type rather than as a Go error string.
func formatOperand(verb byte, v driver.Value) any {
	switch verb {
	case 'd', 'i', 'o', 'b', 'x', 'X', 'c', 'U':
		n, _ := toInt(v)
		return n
	case 'e', 'E', 'f', 'F', 'g', 'G':
		f, _ := toFloat(v)
		return f
	default:
		s, _ := toString(v)
		return s
	}
}

// nullText is how a value that has none is spelled where SQL spells it out:
// dialects.GoogleSQL's FORMAT prints it, dialects.MySQL's QUOTE answers it, dialects.PostgreSQL's
// quote_nullable answers it, and the keyword tables in this package hold it.
const nullText = "NULL"

// googlesqlPrintValue implements dialects.GoogleSQL's %t and %T: the printable form of a
// value, and the literal that would produce it. A boolean reaches here as the
// integer dialects.SQLite stores, so it prints as 0 or 1 rather than as false or true.
func googlesqlPrintValue(v driver.Value, literal bool) string {
	switch x := v.(type) {
	case nil:
		return nullText
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	}
	s, ok := toString(v)
	if !ok {
		return nullText
	}
	if literal {
		return strconv.Quote(s)
	}
	return s
}

// fnIsNaN implements dialects.GoogleSQL IS_NAN(x).
func fnIsNaN(args []driver.Value) (driver.Value, error) {
	f, ok := toFloat(args[0])
	if !ok {
		// A NULL argument is NULL; a non-numeric one is simply not NaN.
		if _, present := toString(args[0]); !present {
			return nil, nil
		}
		return int64(0), nil
	}
	return boolToInt(math.IsNaN(f)), nil
}

// safeArith builds the dialects.GoogleSQL SAFE_ADD/SAFE_SUBTRACT/SAFE_MULTIPLY family:
// the operation, or NULL where the source dialect would raise an overflow. Two
// integer arguments stay in int64 so overflow is detectable; anything else falls
// back to float64, where the result is already an infinity rather than an error.
func safeArith(intOp func(a, b int64) (int64, bool), floatOp func(a, b float64) float64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		if ai, ok := args[0].(int64); ok {
			if bi, ok := args[1].(int64); ok {
				res, fine := intOp(ai, bi)
				if !fine {
					return nil, nil
				}
				return res, nil
			}
		}
		a, ok1 := toFloat(args[0])
		b, ok2 := toFloat(args[1])
		if !ok1 || !ok2 {
			return nil, nil
		}
		res := floatOp(a, b)
		if math.IsInf(res, 0) || math.IsNaN(res) {
			return nil, nil
		}
		return res, nil
	}
}

func safeAddInt(a, b int64) (int64, bool) {
	sum := a + b
	// The sum overflowed when both operands share a sign that the result lost.
	if (a > 0 && b > 0 && sum < 0) || (a < 0 && b < 0 && sum >= 0) {
		return 0, false
	}
	return sum, true
}

func safeSubInt(a, b int64) (int64, bool) {
	if b == math.MinInt64 {
		// Negating the minimum has no int64 representation, so handle it as an
		// addition that cannot be expressed either.
		if a >= 0 {
			return 0, false
		}
		return a - b, true
	}
	return safeAddInt(a, -b)
}

func safeMulInt(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	product := a * b
	if product/b != a || (a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) {
		return 0, false
	}
	return product, true
}

// fnSafeNegate implements dialects.GoogleSQL SAFE_NEGATE(x).
func fnSafeNegate(args []driver.Value) (driver.Value, error) {
	if n, ok := args[0].(int64); ok {
		if n == math.MinInt64 {
			return nil, nil
		}
		return -n, nil
	}
	f, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	return -f, nil
}

// fnGenerateUUID implements dialects.GoogleSQL GENERATE_UUID: a random RFC 4122 v4 UUID.
func fnGenerateUUID(_ []driver.Value) (driver.Value, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("dialect: generate_uuid: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
