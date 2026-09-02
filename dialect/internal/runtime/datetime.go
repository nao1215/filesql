package runtime

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// This file implements date arithmetic with the source dialect's semantics.
//
// dialects.SQLite's datetime() modifier normalizes an out-of-range day forward, so
// '2026-01-31' plus one month became 2026-03-03 where dialects.MySQL, dialects.PostgreSQL, and
// dialects.GoogleSQL all clamp to 2026-02-28. It also always renders a full datetime,
// which is right for dialects.PostgreSQL, where a date plus an interval is a timestamp,
// and wrong for dialects.MySQL and dialects.GoogleSQL, where adding a day to a date answers a
// date. Both are silent: the query succeeds and the date is simply wrong.
//
// Interval arithmetic therefore goes through the helpers here rather than
// through datetime().

// intervalUnitDays and intervalUnitMonths express the compound units in terms of
// the ones the helper implements directly.
const (
	daysPerWeek     = 7
	monthsPerQuartr = 3
)

// errDateOutOfRange marks arithmetic whose result is not a date this package can
// write. A caller answers NULL for it the way MySQL does, rather than reporting
// it, so it is a sentinel rather than a message.
var errDateOutOfRange = errors.New("dialect: the result is outside the range of a date")

// addInterval applies n units to tm. Month and year arithmetic clamps to the
// last day of the target month, which is what all three dialects do and what
// Go's AddDate does not.
//
// An amount whose conversion to months would not fit in an int64 is
// errDateOutOfRange rather than the wrapped-around date the multiplication
// would otherwise give: a MaxInt64 of centuries came out as 1200 months back.
func addInterval(tm time.Time, n int64, unit string) (time.Time, error) {
	switch unit {
	case unitMillennium:
		return addMonthsChecked(tm, n, 12*1000)
	case unitCentury:
		return addMonthsChecked(tm, n, 12*100)
	case unitDecade:
		return addMonthsChecked(tm, n, 12*10)
	case unitYear:
		return addMonthsChecked(tm, n, 12)
	case unitQuarter:
		return addMonthsChecked(tm, n, monthsPerQuartr)
	case unitMonth:
		return addMonthsChecked(tm, n, 1)
	case unitWeek:
		return tm.AddDate(0, 0, int(n*daysPerWeek)), nil
	case unitDay:
		return tm.AddDate(0, 0, int(n)), nil
	case unitHour:
		return tm.Add(time.Duration(n) * time.Hour), nil
	case unitMinute:
		return tm.Add(time.Duration(n) * time.Minute), nil
	case unitSecond:
		return tm.Add(time.Duration(n) * time.Second), nil
	case unitMillisecond:
		return tm.Add(time.Duration(n) * time.Millisecond), nil
	case unitMicrosecond:
		return tm.Add(time.Duration(n) * time.Microsecond), nil
	default:
		return time.Time{}, fmt.Errorf("%w: unsupported interval unit %q", sqlerr.ErrUnsupportedSyntax, unit)
	}
}

// addMonthsChecked adds n periods of perPeriod months each, refusing an amount
// whose product does not fit rather than letting it wrap.
func addMonthsChecked(tm time.Time, n, perPeriod int64) (time.Time, error) {
	months, ok := mulNoOverflow(n, perPeriod)
	if !ok {
		return time.Time{}, errDateOutOfRange
	}
	return addMonths(tm, months)
}

// mulNoOverflow multiplies two int64 values, reporting whether the product fits.
func mulNoOverflow(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	product := a * b
	if product/b != a {
		return 0, false
	}
	return product, true
}

// addMonths adds months to tm, clamping the day to the last day of the target
// month. time.AddDate instead rolls the overflow forward, turning
// "January 31 plus one month" into March 3.
func addMonths(tm time.Time, months int64) (time.Time, error) {
	year, month, day := tm.Date()
	base := int64(year)*12 + int64(month) - 1
	total := base + months
	if (months > 0 && total < base) || (months < 0 && total > base) {
		return time.Time{}, errDateOutOfRange
	}
	targetYear := total / 12
	targetMonth := total % 12
	if targetMonth < 0 {
		targetMonth += 12
		targetYear--
	}
	targetMonth++
	// time.Date normalizes a year it cannot hold rather than refusing it, so
	// the range is checked here where the year is still a plain number.
	if targetYear < minDateYear || targetYear > maxDateYear {
		return time.Time{}, errDateOutOfRange
	}
	if last := daysInMonth(int(targetYear), time.Month(targetMonth)); day > last {
		day = last
	}
	return time.Date(int(targetYear), time.Month(targetMonth), day, tm.Hour(), tm.Minute(), tm.Second(), tm.Nanosecond(), tm.Location()), nil
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, -1).Day()
}

// dateGrainedUnits are the units that leave a date a date. Adding one of these
// to a value written without a time keeps the result a plain date, the way the
// source dialects type it; adding an hour or a minute promotes it to a datetime.
var dateGrainedUnits = map[string]bool{
	unitMillennium: true,
	unitCentury:    true,
	unitDecade:     true,
	unitYear:       true,
	unitISOYear:    true,
	unitQuarter:    true,
	unitMonth:      true,
	unitWeek:       true,
	unitISOWeek:    true,
	unitDay:        true,
}

// hasTimePart reports whether a value was written with a time of day, so the
// result can keep the same shape.
func hasTimePart(v driver.Value) bool {
	if _, ok := v.(time.Time); ok {
		return true
	}
	s, ok := toString(v)
	if !ok {
		return false
	}
	return strings.ContainsAny(strings.TrimSpace(s), " T:")
}

// formatInterval renders the result of interval arithmetic, keeping a date a
// date when nothing in the operation introduced a time.
func formatInterval(tm time.Time, sourceHadTime bool, unit string, format func(time.Time) string) string {
	if !sourceHadTime && dateGrainedUnits[unit] {
		return tm.Format(layoutDateOnly)
	}
	return format(tm)
}

// fnDateIntervalAdd implements the helper behind dialects.GoogleSQL's
// DATE_ADD/DATE_SUB/TIMESTAMP_ADD/TIMESTAMP_SUB: interval_add(value, n, 'unit').
func fnDateIntervalAdd(args []driver.Value) (driver.Value, error) {
	return intervalAddWith(args, "interval_add", formatDateTimeValue)
}

// fnMySQLDateIntervalAdd is fnDateIntervalAdd for dialects.MySQL, which writes
// all six digits of a fraction where the shared spelling writes the significant
// ones. The two are one helper with two names rather than one name reading the
// dialect, because the dialect is known where the call is built and not where
// it runs.
func fnMySQLDateIntervalAdd(args []driver.Value) (driver.Value, error) {
	return intervalAddWith(args, "mysql_interval_add", formatDateTimeValueMySQL)
}

func intervalAddWith(args []driver.Value, name string, format func(time.Time) string) (driver.Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("dialect: %s expects 3 arguments, got %d", name, len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	n, ok := toInt(args[1])
	if !ok {
		return nil, nil
	}
	unit, ok := toString(args[2])
	if !ok {
		return nil, nil
	}
	unit = strings.ToLower(strings.TrimSpace(unit))
	out, err := addInterval(tm, n, unit)
	if errors.Is(err, errDateOutOfRange) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !withinDateRange(out) {
		return nil, nil
	}
	return formatInterval(out, hasTimePart(args[0]), unit, format), nil
}

// The years a date can hold here, which is what MySQL and GoogleSQL hold and
// what the reader in timeparse.go can read back.
const (
	minDateYear = 1
	maxDateYear = 9999
)

// withinDateRange reports whether tm is a date this package can write and read
// back. Arithmetic that leaves the range used to answer a string with a
// five-digit or negative year, which every helper here then read as NULL: the
// row disappeared at the next function rather than at the one that could still
// say why. Both ends are what MySQL and GoogleSQL hold.
func withinDateRange(tm time.Time) bool {
	return tm.Year() >= minDateYear && tm.Year() <= maxDateYear
}

// intervalTextUnits maps the unit words a dialects.PostgreSQL interval literal may use,
// singular or plural, to this package's unit names.
var intervalTextUnits = map[string]string{
	unitMillennium: unitMillennium, "millenniums": unitMillennium, "millennia": unitMillennium,
	"mil": unitMillennium, "mils": unitMillennium,
	unitCentury: unitCentury, "centuries": unitCentury, "c": unitCentury, "cent": unitCentury,
	unitDecade: unitDecade, "decades": unitDecade, "dec": unitDecade, "decs": unitDecade,
	"year": unitYear, "years": unitYear, "y": unitYear,
	"quarter": unitQuarter, "quarters": unitQuarter,
	"month": unitMonth, "months": unitMonth, "mon": unitMonth, "mons": unitMonth,
	"week": unitWeek, "weeks": unitWeek, "w": unitWeek,
	"day": unitDay, "days": unitDay, "d": unitDay,
	"hour": unitHour, "hours": unitHour, "h": unitHour,
	"minute": unitMinute, "minutes": unitMinute, "min": unitMinute, "mins": unitMinute,
	"second": unitSecond, "seconds": unitSecond, "sec": unitSecond, "secs": unitSecond, "s": unitSecond,
	unitMillisecond: unitMillisecond, unitMillisecondsPlural: unitMillisecond, "msec": unitMillisecond, "msecs": unitMillisecond, "ms": unitMillisecond,
	unitMicrosecond: unitMicrosecond, unitMicrosecondsPlural: unitMicrosecond, "usec": unitMicrosecond, "usecs": unitMicrosecond, "us": unitMicrosecond,
}

// intervalTerm is one "amount unit" pair of a dialects.PostgreSQL interval literal.
type intervalTerm struct {
	amount int64
	unit   string
}

// parseIntervalText reads a dialects.PostgreSQL interval literal such as
// "1 day" or "1 year 6 months" into its terms.
func parseIntervalText(text string) ([]intervalTerm, error) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 || len(fields)%2 != 0 {
		return nil, fmt.Errorf("%w: cannot read interval %q", sqlerr.ErrUnsupportedSyntax, text)
	}
	terms := make([]intervalTerm, 0, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		unit, ok := intervalTextUnits[strings.ToLower(fields[i+1])]
		if !ok {
			return nil, fmt.Errorf("%w: unsupported interval unit %q", sqlerr.ErrUnsupportedSyntax, fields[i+1])
		}
		amount, err := strconv.ParseInt(fields[i], 10, 64)
		if err == nil {
			terms = append(terms, intervalTerm{amount: amount, unit: unit})
			continue
		}
		split, err := splitFractionalTerm(fields[i], unit)
		if err != nil {
			return nil, err
		}
		terms = append(terms, split...)
	}
	return terms, nil
}

// monthsPerUnit is how many months one of a unit lasts, for the units a
// calendar counts rather than a clock.
var monthsPerUnit = map[string]int64{
	unitMillennium: 12 * 1000,
	unitCentury:    12 * 100,
	unitDecade:     12 * 10,
	unitYear:       12,
	unitQuarter:    monthsPerQuartr,
	unitMonth:      1,
}

// intervalTotals folds an interval's terms into the three fields PostgreSQL
// holds one in. A week is seven days, which is how PostgreSQL stores it.
func intervalTotals(terms []intervalTerm) (months, days, micros int64, err error) {
	for _, term := range terms {
		switch {
		case monthsPerUnit[term.unit] != 0:
			n, ok := mulNoOverflow(term.amount, monthsPerUnit[term.unit])
			if !ok {
				return 0, 0, 0, errDateOutOfRange
			}
			months += n
		case term.unit == unitWeek:
			days += term.amount * daysPerWeek
		case term.unit == unitDay:
			days += term.amount
		default:
			n, ok := mulNoOverflow(term.amount, microsPerUnit[term.unit])
			if !ok {
				return 0, 0, 0, errDateOutOfRange
			}
			micros += n
		}
	}
	return months, days, micros, nil
}

// microsPerUnit is how many microseconds one of a unit lasts, for the units
// whose length does not depend on where in the calendar they fall. A unit that
// is not here has no fixed length, which is why a fraction of one is refused.
var microsPerUnit = map[string]int64{
	unitWeek:        7 * 24 * 60 * 60 * 1000000,
	unitDay:         24 * 60 * 60 * 1000000,
	unitHour:        60 * 60 * 1000000,
	unitMinute:      60 * 1000000,
	unitSecond:      1000000,
	unitMillisecond: 1000,
	unitMicrosecond: 1,
}

// splitFractionalTerm reads an amount written with a decimal point into a whole
// number of its own unit plus the remainder in microseconds, which is how
// PostgreSQL reads "1.5 hours". A fraction of a month or of anything longer is
// refused rather than guessed at: PostgreSQL spends it as thirty-day months,
// a length no other part of this package assumes.
func splitFractionalTerm(written, unit string) ([]intervalTerm, error) {
	perUnit, fixed := microsPerUnit[unit]
	if !fixed {
		return nil, fmt.Errorf("%w: interval amount %q is not a whole number of %ss", sqlerr.ErrUnsupportedSyntax, written, unit)
	}
	amount, err := strconv.ParseFloat(written, 64)
	if err != nil || math.IsInf(amount, 0) || math.IsNaN(amount) {
		return nil, fmt.Errorf("%w: interval amount %q is not a number", sqlerr.ErrUnsupportedSyntax, written)
	}
	micros := math.Round(amount * float64(perUnit))
	if math.Abs(micros) > math.MaxInt64 {
		return nil, fmt.Errorf("%w: interval amount %q is too large", sqlerr.ErrUnsupportedSyntax, written)
	}
	return []intervalTerm{{amount: int64(micros), unit: unitMicrosecond}}, nil
}

// fnIntervalTextAdd implements dialects.PostgreSQL's "value + INTERVAL 'text'" (and the
// "-" form, via sign). dialects.PostgreSQL has no DATE_ADD, so this operator is the only
// way to do date arithmetic in that dialect, and its result is always a
// timestamp.
func fnIntervalTextAdd(args []driver.Value) (driver.Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("dialect: interval_text_add expects 3 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	text, ok := toString(args[1])
	if !ok {
		return nil, nil
	}
	sign, ok := toInt(args[2])
	if !ok {
		return nil, nil
	}
	terms, err := parseIntervalText(text)
	if err != nil {
		return nil, err
	}
	// PostgreSQL holds an interval as months, days and microseconds and applies
	// the three in that order, whatever order the literal wrote them in. Adding
	// each term as it was written gives a different day whenever a month lands
	// on a month end: from 2021-01-30, "2 days 1 month" is 2021-03-02 there and
	// was 2021-03-01 here, because the two days moved before the month clamped.
	months, days, micros, err := intervalTotals(terms)
	if err != nil {
		return nil, nil //nolint:nilerr // an amount past any date is NULL
	}
	if months != 0 {
		if tm, err = addMonths(tm, sign*months); err != nil {
			return nil, nil //nolint:nilerr // an amount past any date is NULL
		}
	}
	tm = tm.AddDate(0, 0, int(sign*days))
	tm = tm.Add(time.Duration(sign*micros) * time.Microsecond)
	if !withinDateRange(tm) {
		return nil, nil
	}
	// A date plus an interval is a timestamp in dialects.PostgreSQL, whatever the
	// interval was made of: pg_typeof on it says "timestamp without time zone"
	// and the value carries the 00:00:00. That is the opposite of dialects.MySQL and
	// dialects.GoogleSQL, whose DATE_ADD on a date answers a date, which is why the two
	// helpers render their results differently rather than sharing one rule.
	return formatDateTimeValue(tm), nil
}

// fnDateTruncPart implements dialects.GoogleSQL's DATE_TRUNC(value, PART) argument order,
// the reverse of dialects.PostgreSQL's DATE_TRUNC('part', value), and its TIMESTAMP_TRUNC
// and DATETIME_TRUNC aliases. A value written without a time stays a date.
func fnDateTruncPart(args []driver.Value) (driver.Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("dialect: trunc helper expects 2 arguments, got %d", len(args))
	}
	unit, _ := toString(args[1])
	unit = strings.ToLower(strings.TrimSpace(unit))
	// BigQuery's WEEK begins on Sunday, its ISOWEEK on Monday, and
	// WEEK(<WEEKDAY>) on whichever day it names; the shared dialects.PostgreSQL helper
	// below knows only the ISO week, so all of them land here.
	if weekStart, isWeek := weekStartDay(unit); isWeek {
		tm, ok := toStringTime(args[0])
		if !ok {
			return nil, nil
		}
		offset := (int(tm.Weekday()) - int(weekStart) + 7) % 7
		y, mo, d := tm.Date()
		day := time.Date(y, mo, d, 0, 0, 0, 0, tm.Location()).AddDate(0, 0, -offset)
		if hasTimePart(args[0]) {
			return day.Format(layoutDateTime), nil
		}
		return day.Format(layoutDateOnly), nil
	}
	out, err := fnDateTrunc([]driver.Value{args[1], args[0]})
	if err != nil || out == nil {
		return out, err
	}
	if !hasTimePart(args[0]) && dateGrainedUnits[unit] {
		s, _ := toString(out)
		if tm, ok := parseTime(s); ok {
			return tm.Format(layoutDateOnly), nil
		}
	}
	return out, nil
}
