package runtime

import (
	"crypto/md5"  //nolint:gosec // BigQuery's MD5() is the function being implemented, not a security choice
	"crypto/sha1" //nolint:gosec // BigQuery's SHA1() is the function being implemented, not a security choice
	"database/sql/driver"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"golang.org/x/text/unicode/norm"
)

// This file holds the dialects.GoogleSQL-only scalar functions: the ones with no dialects.SQLite
// spelling, and the ones whose name another dialect in this package already
// means something else by.
//
// Every answer below was read from BigQuery rather than derived.

// googlesqlScalarFunctions are the deterministic dialects.GoogleSQL helpers.
func googlesqlScalarFunctions() map[string]scalarSpec {
	return map[string]scalarSpec{
		// BigQuery's digests answer bytes where dialects.PostgreSQL's MD5 and dialects.MySQL's
		// SHA1 answer hexadecimal text, so TO_HEX(MD5(x)) -- the spelling
		// BigQuery's own documentation uses -- hexed the hex.
		"googlesql_md5":  {1, hashBytes(func(b []byte) []byte { s := md5.Sum(b); return s[:] })},  //nolint:gosec // see the file comment
		"googlesql_sha1": {1, hashBytes(func(b []byte) []byte { s := sha1.Sum(b); return s[:] })}, //nolint:gosec // see the file comment

		// The constructors. dialects.SQLite has date(), time() and datetime() of its own,
		// which read a time value and modifiers and answer NULL for three
		// integers, so every one of these is rewritten onto a helper rather
		// than left to the name dialects.SQLite already has.
		"googlesql_date":      {-1, fnGoogleSQLDate},
		"googlesql_datetime":  {-1, fnGoogleSQLDatetime},
		"googlesql_time":      {-1, fnGoogleSQLTime},
		"googlesql_timestamp": {-1, fnGoogleSQLTimestamp},
		"googlesql_string":    {1, fnGoogleSQLString},

		// Days since the epoch, which is how BigQuery spells a date as a
		// number.
		"unix_date":           {1, fnUnixDate},
		"date_from_unix_date": {1, fnDateFromUnixDate},

		// LAST_DAY takes a date part, where the shared last_day() takes a date
		// alone.
		"googlesql_last_day": {-1, fnGoogleSQLLastDay},

		// Strings.
		"googlesql_instr":        {-1, fnGoogleSQLInstr},
		"contains_substr":        {2, fnContainsSubstr},
		"normalize":              {-1, fnNormalize(false)},
		"normalize_and_casefold": {-1, fnNormalize(true)},
		"edit_distance":          {-1, fnEditDistance},

		// Bytes.
		"from_hex":       {1, fnFromHex},
		"to_base32":      {1, fnToBase32},
		"from_base32":    {1, fnFromBase32},
		"to_json_string": {1, fnToJSONString},

		// Arithmetic.
		"ieee_divide": {2, fnIEEEDivide},
		"is_inf":      {1, fnIsInf},
		"csc":         {1, reciprocalTrig(math.Sin)},
		"sec":         {1, reciprocalTrig(math.Cos)},
		"csch":        {1, reciprocalTrig(math.Sinh)},
		"sech":        {1, reciprocalTrig(math.Cosh)},
		"coth":        {1, reciprocalTrig(math.Tanh)},

		// The TIME family: a BigQuery TIME is a time of day, so its arithmetic
		// wraps around midnight rather than moving to another day.
		"time_add":   {3, fnTimeAdd},
		"time_trunc": {2, fnTimeTrunc},

		// A query that fails a row deliberately.
		"error": {1, fnError},
	}
}

// --- constructors ---

// fnGoogleSQLDate implements DATE(y, m, d), DATE(timestamp) and DATE(datetime).
func fnGoogleSQLDate(args []driver.Value) (driver.Value, error) {
	switch len(args) {
	case 1:
		tm, ok := toStringTime(args[0])
		if !ok {
			return nil, nil
		}
		return tm.Format(layoutDateOnly), nil
	case 3:
		tm, err := dateFromFields(args)
		if err != nil || tm == nil {
			return nil, err
		}
		return tm.Format(layoutDateOnly), nil
	default:
		return nil, fmt.Errorf("dialect: DATE expects 1 or 3 arguments, got %d", len(args))
	}
}

// fnGoogleSQLDatetime implements DATETIME(y, m, d, h, mi, s), DATETIME(date,
// time) and DATETIME(timestamp).
func fnGoogleSQLDatetime(args []driver.Value) (driver.Value, error) {
	switch len(args) {
	case 1:
		tm, ok := toStringTime(args[0])
		if !ok {
			return nil, nil
		}
		return tm.Format(layoutDateTime), nil
	case 2:
		date, ok1 := toString(args[0])
		clock, ok2 := toString(args[1])
		if !ok1 || !ok2 {
			return nil, nil
		}
		// The other two-argument form is DATETIME(timestamp, time_zone), and a
		// zone is something filesql does not carry. Answering the timestamp
		// unshifted would be a different instant, so it is refused.
		if _, ok := parseTime(strings.TrimSpace(clock)); !ok {
			return nil, errUnsupportedTimeZone("DATETIME")
		}
		tm, ok := toStringTime(strings.TrimSpace(date) + " " + strings.TrimSpace(clock))
		if !ok {
			return nil, nil
		}
		return tm.Format(layoutDateTime), nil
	case 6:
		tm, err := dateFromFields(args[:3])
		if err != nil || tm == nil {
			return nil, err
		}
		clock, err := timeFromFields(args[3:])
		if err != nil || clock == "" {
			return nil, err
		}
		return tm.Format(layoutDateOnly) + " " + clock, nil
	default:
		return nil, fmt.Errorf("dialect: DATETIME expects 1, 2 or 6 arguments, got %d", len(args))
	}
}

// fnGoogleSQLTime implements TIME(h, mi, s), TIME(timestamp) and TIME(datetime).
func fnGoogleSQLTime(args []driver.Value) (driver.Value, error) {
	switch len(args) {
	case 1:
		tm, ok := toStringTime(args[0])
		if !ok {
			return nil, nil
		}
		return tm.Format(layoutTimeOnly), nil
	case 3:
		clock, err := timeFromFields(args)
		if err != nil || clock == "" {
			return nil, err
		}
		return clock, nil
	default:
		return nil, fmt.Errorf("dialect: TIME expects 1 or 3 arguments, got %d", len(args))
	}
}

// fnGoogleSQLTimestamp implements TIMESTAMP(x), which reads a date or a
// datetime. BigQuery answers a value carrying a zone; filesql has none to
// carry, so the answer is the same datetime this package spells everywhere.
func fnGoogleSQLTimestamp(args []driver.Value) (driver.Value, error) {
	switch len(args) {
	case 1:
	case 2:
		return nil, errUnsupportedTimeZone(typeTimestamp)
	default:
		return nil, fmt.Errorf("dialect: TIMESTAMP expects 1 or 2 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	return tm.Format(layoutDateTime), nil
}

// errUnsupportedTimeZone reports the time-zone argument this package cannot
// honor. Shifting an instant by a zone it does not carry would answer a
// different point in time, which is worse than saying so.
func errUnsupportedTimeZone(name string) error {
	return fmt.Errorf("dialect: %s: a time zone argument is not supported; filesql carries no time zone", name)
}

// fnGoogleSQLString implements STRING(timestamp), which spells a timestamp.
func fnGoogleSQLString(args []driver.Value) (driver.Value, error) {
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	return tm.Format(layoutDateTime), nil
}

// dateFromFields builds a date out of three integer arguments, refusing a field
// outside its range rather than carrying it into the next one -- which is what
// BigQuery does, and what makes a bad computed field visible instead of shifting
// a date by a month.
func dateFromFields(args []driver.Value) (*time.Time, error) {
	year, ok1 := toInt(args[0])
	month, ok2 := toInt(args[1])
	day, ok3 := toInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil //nolint:nilnil // a NULL argument makes the call NULL
	}
	if month < 1 || month > 12 {
		return nil, fmt.Errorf("dialect: DATE: month %d is out of range", month)
	}
	if day < 1 || day > 31 {
		return nil, fmt.Errorf("dialect: DATE: day %d is out of range", day)
	}
	tm := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
	if int(tm.Month()) != int(month) || tm.Day() != int(day) {
		return nil, fmt.Errorf("dialect: DATE: %04d-%02d-%02d is not a date", year, month, day)
	}
	return &tm, nil
}

// timeFromFields builds a time of day out of three arguments. An empty string
// with a nil error means one of them was NULL.
func timeFromFields(args []driver.Value) (string, error) {
	hour, ok1 := toInt(args[0])
	minute, ok2 := toInt(args[1])
	second, ok3 := toInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return "", nil
	}
	if hour < 0 || hour > 23 {
		return "", fmt.Errorf("dialect: TIME: hour %d is out of range", hour)
	}
	if minute < 0 || minute > 59 {
		return "", fmt.Errorf("dialect: TIME: minute %d is out of range", minute)
	}
	if second < 0 || second > 59 {
		return "", fmt.Errorf("dialect: TIME: second %d is out of range", second)
	}
	return fmt.Sprintf("%02d:%02d:%02d", hour, minute, second), nil
}

// fnCurrentDatetime reads the clock. BigQuery's takes an optional time zone,
// which filesql does not carry: answering the local instant under another
// zone's name would be a different reading, so a zone is refused.
func fnCurrentDatetime(args []driver.Value) (driver.Value, error) {
	if len(args) > 0 {
		return nil, errUnsupportedTimeZone("CURRENT_DATETIME")
	}
	return clockUTC().Format(layoutDateTime), nil
}

// --- days since the epoch ---

// unixEpochDay is the day UNIX_DATE counts from.
var unixEpochDay = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // a constant date

func fnUnixDate(args []driver.Value) (driver.Value, error) {
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	return dayNumber(tm), nil
}

func fnDateFromUnixDate(args []driver.Value) (driver.Value, error) {
	n, ok := toInt(args[0])
	if !ok {
		return nil, nil
	}
	return unixEpochDay.AddDate(0, 0, int(n)).Format(layoutDateOnly), nil
}

// fnGoogleSQLLastDay implements LAST_DAY(date[, part]): the last day of the
// period the date falls in. The default is MONTH. WEEK begins on Sunday and so
// ends on Saturday, while ISOWEEK begins on Monday and ends on Sunday, which is
// the pair this function invites getting backwards.
func fnGoogleSQLLastDay(args []driver.Value) (driver.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("dialect: LAST_DAY expects 1 or 2 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	part := unitMonth
	if len(args) == 2 {
		p, partOK := toString(args[1])
		if !partOK {
			return nil, nil
		}
		part = strings.ToLower(strings.TrimSpace(p))
	}
	y, m, d := tm.Date()
	day := time.Date(y, m, d, 0, 0, 0, 0, tm.Location())
	switch part {
	case unitMonth:
		return day.AddDate(0, 1, -day.Day()).Format(layoutDateOnly), nil
	case unitQuarter:
		endMonth := time.Month((int(m)-1)/3*3 + 3)
		end := time.Date(y, endMonth, 1, 0, 0, 0, 0, day.Location())
		return end.AddDate(0, 1, -1).Format(layoutDateOnly), nil
	case unitYear:
		return time.Date(y, 12, 31, 0, 0, 0, 0, day.Location()).Format(layoutDateOnly), nil
	case unitISOYear:
		isoYear, _ := day.ISOWeek()
		return lastDayOfISOYear(isoYear, day.Location()).Format(layoutDateOnly), nil
	default:
		start, ok := weekStartDay(part)
		if !ok {
			return nil, fmt.Errorf("dialect: LAST_DAY: unsupported date part %q", part)
		}
		offset := (int(day.Weekday()) - int(start) + 7) % 7
		return day.AddDate(0, 0, 6-offset).Format(layoutDateOnly), nil
	}
}

// lastDayOfISOYear is the Sunday that closes an ISO year.
func lastDayOfISOYear(isoYear int, loc *time.Location) time.Time {
	day := time.Date(isoYear+1, 1, 4, 0, 0, 0, 0, loc)
	return day.AddDate(0, 0, -isoWeekday(day))
}

// weekStartDay maps a week date part to the weekday its week begins on: "week"
// on Sunday, "isoweek" on Monday, and "week_<weekday>" on the day it names.
func weekStartDay(part string) (time.Weekday, bool) {
	switch part {
	case unitWeek:
		return time.Sunday, true
	case unitISOWeek:
		return time.Monday, true
	}
	name, found := strings.CutPrefix(part, "week_")
	if !found {
		return 0, false
	}
	day, ok := weekdayNames[name]
	return day, ok
}

// weekdayNames are the weekday names BigQuery takes inside WEEK(<WEEKDAY>).
var weekdayNames = map[string]time.Weekday{ //nolint:gochecknoglobals // a fixed table read by the week parts
	"sunday": time.Sunday, "monday": time.Monday, "tuesday": time.Tuesday,
	"wednesday": time.Wednesday, "thursday": time.Thursday,
	"friday": time.Friday, "saturday": time.Saturday,
}

// --- strings ---

// fnGoogleSQLInstr implements INSTR(source, search[, position[, occurrence]]).
// A negative position searches backwards from the end, and an occurrence past
// the number of matches is 0. Positions are counted in characters.
func fnGoogleSQLInstr(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 4 {
		return nil, fmt.Errorf("dialect: INSTR expects 2 to 4 arguments, got %d", len(args))
	}
	source, ok1 := toString(args[0])
	search, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	position := int64(1)
	if len(args) >= 3 {
		n, ok := toInt(args[2])
		if !ok {
			return nil, nil
		}
		if n == 0 {
			return nil, errors.New("dialect: INSTR: position must not be zero")
		}
		position = n
	}
	occurrence := int64(1)
	if len(args) == 4 {
		n, ok := toInt(args[3])
		if !ok {
			return nil, nil
		}
		if n < 1 {
			return nil, errors.New("dialect: INSTR: occurrence must be at least 1")
		}
		occurrence = n
	}
	return int64(instrAt([]rune(source), []rune(search), position, occurrence)), nil
}

// instrAt is the character position INSTR answers, or 0 when there is no such
// occurrence. A positive position starts the search there and runs forward; a
// negative one starts that far from the end and runs backward.
func instrAt(source, search []rune, position, occurrence int64) int {
	if len(search) == 0 {
		if position > 0 {
			return int(position)
		}
		return len(source) + int(position) + 1
	}
	forward := position > 0
	start := int(position) - 1
	if !forward {
		start = len(source) + int(position)
	}
	if start < 0 || start > len(source) {
		return 0
	}
	found := int64(0)
	if forward {
		for i := start; i+len(search) <= len(source); i++ {
			if string(source[i:i+len(search)]) == string(search) {
				found++
				if found == occurrence {
					return i + 1
				}
			}
		}
		return 0
	}
	for i := min(start, len(source)-len(search)); i >= 0; i-- {
		if string(source[i:i+len(search)]) == string(search) {
			found++
			if found == occurrence {
				return i + 1
			}
		}
	}
	return 0
}

// fnContainsSubstr implements CONTAINS_SUBSTR(value, search), which normalizes
// both sides to NFKC and casefolds them before looking. So it matches across
// case and not across accents: 'café' does not contain 'cafe'.
func fnContainsSubstr(args []driver.Value) (driver.Value, error) {
	search, ok := toString(args[1])
	if !ok {
		// BigQuery raises rather than answering NULL for a NULL search value,
		// since there is no substring to look for.
		return nil, errors.New("dialect: CONTAINS_SUBSTR: the search value must not be NULL")
	}
	value, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	return boolToInt(strings.Contains(casefoldNFKC(value), casefoldNFKC(search))), nil
}

// casefoldNFKC is the normalization CONTAINS_SUBSTR and NORMALIZE_AND_CASEFOLD
// share.
func casefoldNFKC(s string) string {
	return strings.ToLower(norm.NFKC.String(s))
}

// fnNormalize implements NORMALIZE(value[, mode]) and, with casefold set,
// NORMALIZE_AND_CASEFOLD. The mode is written as a bare keyword in BigQuery and
// reaches here as a string, since the rewrite turns the word into an argument.
func fnNormalize(casefold bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		if len(args) < 1 || len(args) > 2 {
			return nil, fmt.Errorf("dialect: NORMALIZE expects 1 or 2 arguments, got %d", len(args))
		}
		s, ok := toString(args[0])
		if !ok {
			return nil, nil
		}
		mode := "NFC"
		if len(args) == 2 {
			m, modeOK := toString(args[1])
			if !modeOK {
				return nil, nil
			}
			mode = strings.ToUpper(strings.TrimSpace(m))
		}
		form, ok := normalizationForms[mode]
		if !ok {
			return nil, fmt.Errorf("dialect: NORMALIZE: unsupported mode %q", mode)
		}
		out := form.String(s)
		if casefold {
			out = strings.ToLower(out)
		}
		return out, nil
	}
}

var normalizationForms = map[string]norm.Form{ //nolint:gochecknoglobals // a fixed table read by NORMALIZE
	"NFC": norm.NFC, "NFD": norm.NFD, "NFKC": norm.NFKC, "NFKD": norm.NFKD,
}

// fnEditDistance implements EDIT_DISTANCE(a, b[, max_distance]): the Levenshtein
// distance in characters, capped at max_distance when one is given.
func fnEditDistance(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("dialect: EDIT_DISTANCE expects 2 or 3 arguments, got %d", len(args))
	}
	a, ok1 := toString(args[0])
	b, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	distance := int64(levenshtein([]rune(a), []rune(b)))
	if len(args) == 3 {
		limit, ok := toInt(args[2])
		if !ok {
			return nil, nil
		}
		if limit < 0 {
			return nil, errors.New("dialect: EDIT_DISTANCE: max_distance must not be negative")
		}
		distance = min(distance, limit)
	}
	return distance, nil
}

// levenshtein counts the single-character edits between two strings, over one
// row of the matrix rather than all of it.
func levenshtein(a, b []rune) int {
	if len(a) < len(b) {
		a, b = b, a
	}
	previous := make([]int, len(b)+1)
	for j := range previous {
		previous[j] = j
	}
	current := make([]int, len(b)+1)
	for i := 1; i <= len(a); i++ {
		current[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			current[j] = min(previous[j]+1, current[j-1]+1, previous[j-1]+cost)
		}
		previous, current = current, previous
	}
	return previous[len(b)]
}

// --- bytes ---

func fnFromHex(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("dialect: FROM_HEX: %w", err)
	}
	return b, nil
}

func fnToBase32(args []driver.Value) (driver.Value, error) {
	b, ok := toBytes(args[0])
	if !ok {
		return nil, nil
	}
	return base32.StdEncoding.EncodeToString(b), nil
}

func fnFromBase32(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	b, err := base32.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("dialect: FROM_BASE32: %w", err)
	}
	return b, nil
}

// fnToJSONString spells a value as JSON. dialects.SQLite has no boolean, so a boolean
// column reaches here as the integer it is stored as and prints as a number.
func fnToJSONString(args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return "null", nil
	}
	var value any
	switch x := args[0].(type) {
	case []byte:
		value = string(x)
	default:
		value = x
	}
	out, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("dialect: TO_JSON_STRING: %w", err)
	}
	return string(out), nil
}

// --- arithmetic ---

// fnIEEEDivide divides without raising: a non-zero numerator over zero is an
// infinity and zero over zero is NaN, which is what the "/" operator refuses to
// answer under this dialect.
func fnIEEEDivide(args []driver.Value) (driver.Value, error) {
	a, ok1 := toFloat(args[0])
	b, ok2 := toFloat(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	out := a / b
	if math.IsNaN(out) {
		// dialects.SQLite has no NaN, and NULL is what this package answers in its
		// place everywhere else.
		return nil, nil
	}
	return out, nil
}

func fnIsInf(args []driver.Value) (driver.Value, error) {
	x, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	return boolToInt(math.IsInf(x, 0)), nil
}

// reciprocalTrig builds CSC, SEC and their hyperbolic siblings out of the
// function they are the reciprocal of.
func reciprocalTrig(fn func(float64) float64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		x, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		return 1 / fn(x), nil
	}
}

// fnError raises with the message it is given, which is how a BigQuery query
// fails a row deliberately.
func fnError(args []driver.Value) (driver.Value, error) {
	message, ok := toString(args[0])
	if !ok {
		return nil, errors.New("dialect: ERROR: NULL")
	}
	return nil, errors.New("dialect: " + message)
}

// --- the TIME family ---

// fnTimeAdd implements TIME_ADD and TIME_SUB, called as time_add(t, ±n, 'unit').
// A BigQuery TIME is a time of day rather than a point in time, so the result
// wraps around midnight: TIME_ADD(TIME '23:04:05', INTERVAL 2 HOUR) is
// 01:04:05 rather than a value on the next day.
func fnTimeAdd(args []driver.Value) (driver.Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("dialect: time_add expects 3 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	amount, ok := toInt(args[1])
	if !ok {
		return nil, nil
	}
	unit, ok := toString(args[2])
	if !ok {
		return nil, nil
	}
	step, err := timeUnitDuration(strings.ToLower(strings.TrimSpace(unit)))
	if err != nil {
		return nil, err
	}
	const day = 24 * time.Hour
	// A whole number of days added to a time of day changes nothing, so the
	// amount is reduced to less than one day before it is multiplied out --
	// otherwise a large interval overflows the int64 of nanoseconds a Duration
	// is and wraps to some other time. Every unit the TIME family takes
	// divides a day evenly, so the reduction loses nothing.
	amount %= int64(day / step)
	offset := (timeOfDay(tm) + time.Duration(amount)*step) % day
	if offset < 0 {
		offset += day
	}
	return timeOfDayString(offset), nil
}

// fnTimeDiff implements TIME_DIFF(a, b, unit): the units between two times of
// day, which is a plain subtraction rather than the boundary count DATE_DIFF
// does, since a time of day has no calendar to cross.
func fnTimeDiff(args []driver.Value) (driver.Value, error) {
	a, ok1 := toStringTime(args[0])
	b, ok2 := toStringTime(args[1])
	unit, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	step, err := timeUnitDuration(strings.ToLower(strings.TrimSpace(unit)))
	if err != nil {
		return nil, err
	}
	return int64((timeOfDay(a) - timeOfDay(b)) / step), nil
}

// fnTimeTrunc implements TIME_TRUNC(t, unit), truncating within the day.
func fnTimeTrunc(args []driver.Value) (driver.Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("dialect: TIME_TRUNC expects 2 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	unit, ok := toString(args[1])
	if !ok {
		return nil, nil
	}
	step, err := timeUnitDuration(strings.ToLower(strings.TrimSpace(unit)))
	if err != nil {
		return nil, err
	}
	return timeOfDayString(timeOfDay(tm).Truncate(step)), nil
}

// timeOfDay is how far into its day a time is.
func timeOfDay(tm time.Time) time.Duration {
	return time.Duration(tm.Hour())*time.Hour +
		time.Duration(tm.Minute())*time.Minute +
		time.Duration(tm.Second())*time.Second +
		time.Duration(tm.Nanosecond())
}

// timeOfDayString spells a duration as a time of day, with its fraction of a
// second when it has one -- so a TIME truncated to a millisecond keeps the
// millisecond rather than reading as the second below it.
func timeOfDayString(d time.Duration) string {
	out := fmt.Sprintf("%02d:%02d:%02d", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
	frac := d % time.Second
	if frac == 0 {
		return out
	}
	return out + "." + strings.TrimRight(fmt.Sprintf("%09d", frac), "0")
}

// timeUnitDuration is the length of a unit the TIME family takes. The calendar
// units are not among them: a time of day has no month to move by.
func timeUnitDuration(unit string) (time.Duration, error) {
	switch unit {
	case unitHour:
		return time.Hour, nil
	case unitMinute:
		return time.Minute, nil
	case unitSecond:
		return time.Second, nil
	case unitMillisecond:
		return time.Millisecond, nil
	case unitMicrosecond:
		return time.Microsecond, nil
	default:
		return 0, fmt.Errorf("dialect: %q is not a unit of a time of day", unit)
	}
}
