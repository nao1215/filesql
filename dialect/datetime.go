package dialect

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// This file implements date arithmetic with the source dialect's semantics.
//
// SQLite's datetime() modifier normalizes an out-of-range day forward, so
// '2026-01-31' plus one month became 2026-03-03 where MySQL, PostgreSQL, and
// GoogleSQL all clamp to 2026-02-28. It also always renders a full datetime, so
// adding a day to a date grew a "00:00:00" the source dialect would not have
// produced. Both are silent: the query succeeds and the date is simply wrong.
//
// Interval arithmetic therefore goes through the helpers here rather than
// through datetime().

// intervalUnitDays and intervalUnitMonths express the compound units in terms of
// the ones the helper implements directly.
const (
	daysPerWeek     = 7
	monthsPerQuartr = 3
)

// addInterval applies n units to tm. Month and year arithmetic clamps to the
// last day of the target month, which is what all three dialects do and what
// Go's AddDate does not.
func addInterval(tm time.Time, n int64, unit string) (time.Time, error) {
	switch unit {
	case unitYear:
		return addMonths(tm, n*12), nil
	case unitQuarter:
		return addMonths(tm, n*monthsPerQuartr), nil
	case unitMonth:
		return addMonths(tm, n), nil
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
	default:
		return time.Time{}, fmt.Errorf("%w: unsupported interval unit %q", ErrUnsupportedSyntax, unit)
	}
}

// addMonths adds months to tm, clamping the day to the last day of the target
// month. time.AddDate instead rolls the overflow forward, turning
// "January 31 plus one month" into March 3.
func addMonths(tm time.Time, months int64) time.Time {
	year, month, day := tm.Date()
	total := int64(year)*12 + int64(month) - 1 + months
	targetYear := int(total / 12)
	targetMonth := int(total % 12)
	if targetMonth < 0 {
		targetMonth += 12
		targetYear--
	}
	targetMonth++
	if last := daysInMonth(targetYear, time.Month(targetMonth)); day > last {
		day = last
	}
	return time.Date(targetYear, time.Month(targetMonth), day, tm.Hour(), tm.Minute(), tm.Second(), tm.Nanosecond(), tm.Location())
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, -1).Day()
}

// dateGrainedUnits are the units that leave a date a date. Adding one of these
// to a value written without a time keeps the result a plain date, the way the
// source dialects type it; adding an hour or a minute promotes it to a datetime.
var dateGrainedUnits = map[string]bool{
	unitYear:    true,
	unitQuarter: true,
	unitMonth:   true,
	unitWeek:    true,
	unitDay:     true,
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
func formatInterval(tm time.Time, sourceHadTime bool, unit string) string {
	if !sourceHadTime && dateGrainedUnits[unit] {
		return tm.Format(layoutDateOnly)
	}
	return tm.Format(layoutDateTime)
}

// fnDateIntervalAdd implements the helper behind MySQL's and GoogleSQL's
// DATE_ADD/DATE_SUB/TIMESTAMP_ADD/TIMESTAMP_SUB: interval_add(value, n, 'unit').
func fnDateIntervalAdd(args []driver.Value) (driver.Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("dialect: interval_add expects 3 arguments, got %d", len(args))
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
	if err != nil {
		return nil, err
	}
	return formatInterval(out, hasTimePart(args[0]), unit), nil
}

// intervalTextUnits maps the unit words a PostgreSQL interval literal may use,
// singular or plural, to this package's unit names.
var intervalTextUnits = map[string]string{
	"year": unitYear, "years": unitYear, "y": unitYear,
	"quarter": unitQuarter, "quarters": unitQuarter,
	"month": unitMonth, "months": unitMonth, "mon": unitMonth, "mons": unitMonth,
	"week": unitWeek, "weeks": unitWeek, "w": unitWeek,
	"day": unitDay, "days": unitDay, "d": unitDay,
	"hour": unitHour, "hours": unitHour, "h": unitHour,
	"minute": unitMinute, "minutes": unitMinute, "min": unitMinute, "mins": unitMinute,
	"second": unitSecond, "seconds": unitSecond, "sec": unitSecond, "secs": unitSecond, "s": unitSecond,
}

// intervalTerm is one "amount unit" pair of a PostgreSQL interval literal.
type intervalTerm struct {
	amount int64
	unit   string
}

// parseIntervalText reads a PostgreSQL interval literal such as
// "1 day" or "1 year 6 months" into its terms.
func parseIntervalText(text string) ([]intervalTerm, error) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 || len(fields)%2 != 0 {
		return nil, fmt.Errorf("%w: cannot read interval %q", ErrUnsupportedSyntax, text)
	}
	terms := make([]intervalTerm, 0, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		amount, err := strconv.ParseInt(fields[i], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: interval amount %q is not a whole number", ErrUnsupportedSyntax, fields[i])
		}
		unit, ok := intervalTextUnits[strings.ToLower(fields[i+1])]
		if !ok {
			return nil, fmt.Errorf("%w: unsupported interval unit %q", ErrUnsupportedSyntax, fields[i+1])
		}
		terms = append(terms, intervalTerm{amount: amount, unit: unit})
	}
	return terms, nil
}

// fnIntervalTextAdd implements PostgreSQL's "value + INTERVAL 'text'" (and the
// "-" form, via sign). PostgreSQL has no DATE_ADD, so this operator is the only
// way to do date arithmetic in that dialect.
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
	onlyDateGrained := true
	for _, term := range terms {
		tm, err = addInterval(tm, sign*term.amount, term.unit)
		if err != nil {
			return nil, err
		}
		if !dateGrainedUnits[term.unit] {
			onlyDateGrained = false
		}
	}
	if !hasTimePart(args[0]) && onlyDateGrained {
		return tm.Format(layoutDateOnly), nil
	}
	return tm.Format(layoutDateTime), nil
}

// fnDateTruncPart implements GoogleSQL's DATE_TRUNC(value, PART) argument order,
// the reverse of PostgreSQL's DATE_TRUNC('part', value), and its TIMESTAMP_TRUNC
// and DATETIME_TRUNC aliases. A value written without a time stays a date.
func fnDateTruncPart(args []driver.Value) (driver.Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("dialect: trunc helper expects 2 arguments, got %d", len(args))
	}
	out, err := fnDateTrunc([]driver.Value{args[1], args[0]})
	if err != nil || out == nil {
		return out, err
	}
	unit, _ := toString(args[1])
	if !hasTimePart(args[0]) && dateGrainedUnits[strings.ToLower(strings.TrimSpace(unit))] {
		s, _ := toString(out)
		if tm, ok := parseTime(s); ok {
			return tm.Format(layoutDateOnly), nil
		}
	}
	return out, nil
}
