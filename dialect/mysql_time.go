package dialect

import (
	"database/sql/driver"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// This file holds MySQL's TIME functions, which need a value of their own
// rather than the datetime helpers beside them. A MySQL TIME is a signed span
// running from -838:59:59 to 838:59:59, not a point on a clock: SEC_TO_TIME
// answers 100:00:00 for 360000, ADDTIME adds two of them into a third, and
// TIME_FORMAT prints an hour field that can be three digits and carry a sign.
// A time.Time can hold none of that.
//
// Every expected value in the tests beside this file was read from MySQL 8.4
// rather than derived.

// microsPerSecond and its siblings convert between the units a TIME is written
// in and the microseconds it is carried in.
const (
	microsPerSecond = int64(1_000_000)
	microsPerMinute = 60 * microsPerSecond
	microsPerHour   = 60 * microsPerMinute
)

// mysqlTimeMaxMicros is the largest magnitude a MySQL TIME holds, 838:59:59.
// A value past it is clamped rather than refused, which is what MySQL does.
const mysqlTimeMaxMicros = 838*microsPerHour + 59*microsPerMinute + 59*microsPerSecond

// mysqlTimeFunctions returns the TIME helpers, keyed by the name a query calls
// them by. None of these names is a SQLite keyword or built-in.
func mysqlTimeFunctions() map[string]scalarSpec {
	return map[string]scalarSpec{
		"sec_to_time": {1, fnSecToTime},
		"time_to_sec": {1, fnTimeToSec},
		"maketime":    {3, fnMakeTime},
		"time_format": {2, fnTimeFormat},
		"addtime":     {2, addTime(1)},
		"subtime":     {2, addTime(-1)},
		"microsecond": {1, fnMicrosecond},
		"to_days":     {1, fnToDays},
		"from_days":   {1, fnFromDays},
		"makedate":    {2, fnMakeDate},
		"period_add":  {2, fnPeriodAdd},
		"period_diff": {2, fnPeriodDiff},
	}
}

// clampTime holds a TIME to the range MySQL stores one in.
func clampTime(micros int64) int64 {
	return max(min(micros, mysqlTimeMaxMicros), -mysqlTimeMaxMicros)
}

// parseMySQLTime reads a MySQL TIME from a string, accepting the hour-first
// forms a TIME is written in and the time half of a datetime. The hour field is
// not limited to 24, so "100:00:00" is six days and four hours.
func parseMySQLTime(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	// A datetime carries its date in front; the TIME is what follows it. The
	// "T" form is read as well as the space form, which MySQL does not do -- it
	// coerces the whole string and answers a number from its digits -- because
	// an ISO timestamp is what a file this package reads is likely to hold.
	if _, after, found := strings.Cut(s, " "); found {
		return parseMySQLTime(after)
	}
	if _, after, found := strings.Cut(s, "T"); found {
		return parseMySQLTime(after)
	}
	negative := false
	if s[0] == '-' || s[0] == '+' {
		negative = s[0] == '-'
		s = s[1:]
	}
	if !strings.Contains(s, ":") {
		return parsePackedTime(s, negative)
	}
	fields := strings.Split(s, ":")
	if len(fields) < 2 || len(fields) > 3 {
		return 0, false
	}
	hours, ok := parseTimeField(fields[0])
	if !ok {
		return 0, false
	}
	minutes, ok := parseTimeField(fields[1])
	if !ok || minutes > 59 {
		return 0, false
	}
	micros := hours*microsPerHour + minutes*microsPerMinute
	if len(fields) == 3 {
		seconds, frac, ok := parseSecondsField(fields[2])
		if !ok || seconds > 59 {
			return 0, false
		}
		micros += seconds*microsPerSecond + frac
	}
	if negative {
		micros = -micros
	}
	return clampTime(micros), true
}

// packedTimeDigits is the most digits the colonless form of a TIME holds:
// HHHMMSS is one more than a TIME's hours field can be, so six is the limit.
const packedTimeDigits = 6

// parsePackedTime reads the colonless form of a TIME, where the last two digits
// are the seconds, the two before them the minutes and whatever is left the
// hours: "1234" is 12 minutes 34 seconds and "12" is 12 seconds.
func parsePackedTime(s string, negative bool) (int64, bool) {
	digits, fraction, found := strings.Cut(s, ".")
	if len(digits) == 0 || len(digits) > packedTimeDigits {
		return 0, false
	}
	var frac int64
	if found {
		var ok bool
		if frac, ok = parseFractionDigits(fraction); !ok {
			return 0, false
		}
	}
	fields := [3]int64{}
	for i := range 3 {
		cut := max(len(digits)-2, 0)
		field, ok := parseTimeField(digits[cut:])
		if !ok {
			return 0, false
		}
		fields[i] = field
		digits = digits[:cut]
		if digits == "" {
			break
		}
	}
	seconds, minutes, hours := fields[0], fields[1], fields[2]
	if seconds > 59 || minutes > 59 {
		return 0, false
	}
	micros := hours*microsPerHour + minutes*microsPerMinute + seconds*microsPerSecond + frac
	if negative {
		micros = -micros
	}
	return clampTime(micros), true
}

// parseTimeField reads one unsigned field of a TIME.
func parseTimeField(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// parseSecondsField reads the seconds field of a TIME, which can carry a
// fraction, returning the whole seconds and the fraction in microseconds.
func parseSecondsField(s string) (int64, int64, bool) {
	whole, fraction, found := strings.Cut(s, ".")
	seconds, ok := parseTimeField(whole)
	if !ok {
		return 0, 0, false
	}
	if !found {
		return seconds, 0, true
	}
	micros, ok := parseFractionDigits(fraction)
	if !ok {
		return 0, 0, false
	}
	return seconds, micros, true
}

// microsecondDigits is how many digits of a second MySQL keeps.
const microsecondDigits = 6

// parseFractionDigits reads a decimal fraction of a second as microseconds,
// padding or truncating to the six digits MySQL keeps.
func parseFractionDigits(fraction string) (int64, bool) {
	if fraction == "" {
		return 0, true
	}
	if len(fraction) > microsecondDigits {
		fraction = fraction[:microsecondDigits]
	}
	n, ok := parseTimeField(fraction)
	if !ok {
		return 0, false
	}
	for i := len(fraction); i < microsecondDigits; i++ {
		n *= 10
	}
	return n, true
}

// fnMySQLIntervalCompound implements DATE_ADD and DATE_SUB with one of MySQL's
// compound INTERVAL units, whose value carries several fields in one string:
// INTERVAL '1:30' HOUR_MINUTE is an hour and a half, and INTERVAL '2-3'
// YEAR_MONTH is two years and three months.
//
// MySQL separates the fields on any run of punctuation, and a value shorter
// than the unit names is read from the right, so INTERVAL '1:10' DAY_SECOND is
// a minute and ten seconds rather than a day and ten hours.
func fnMySQLIntervalCompound(args []driver.Value) (driver.Value, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("dialect: compound interval helper expects 4 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	value, ok := toString(args[1])
	if !ok {
		return nil, nil
	}
	unit, ok := toString(args[2])
	if !ok {
		return nil, nil
	}
	sign, ok := toInt(args[3])
	if !ok {
		return nil, nil
	}
	fields, ok := mysqlCompositeParts[strings.ToLower(strings.TrimSpace(unit))]
	if !ok {
		return nil, fmt.Errorf("%w: unsupported INTERVAL unit %q", ErrUnsupportedSyntax, unit)
	}
	components := splitIntervalComponents(value)
	if len(components) == 0 {
		return nil, nil
	}
	if len(components) > len(fields) {
		components = components[len(components)-len(fields):]
	}
	// A short value names the rightmost fields, which is MySQL's rule.
	fields = fields[len(fields)-len(components):]
	dateGrained := true
	var err error
	for i, field := range fields {
		if !dateGrainedUnits[field] {
			dateGrained = false
		}
		amount := components[i]
		if field == unitMicrosecond {
			// The fraction is written after a point, so "5.1" is a tenth of a
			// second rather than one microsecond.
			amount = padMicroseconds(amount)
		}
		n, convErr := strconv.ParseInt(amount, 10, 64)
		if convErr != nil {
			return nil, nil //nolint:nilerr // MySQL answers NULL for a value it cannot read
		}
		if tm, err = addInterval(tm, sign*n, field); err != nil {
			return nil, err
		}
	}
	// A date moved only by date-grained fields is still a date, which is the
	// same rule the single-unit arithmetic follows.
	if dateGrained && !hasTimePart(args[0]) {
		return tm.Format(layoutDateOnly), nil
	}
	return formatDateTimeValue(tm), nil
}

// splitIntervalComponents cuts an interval value on every run of characters that
// is not a digit, which is the delimiter rule MySQL states: any punctuation may
// separate the fields.
func splitIntervalComponents(value string) []string {
	var out []string
	current := strings.Builder{}
	for _, r := range strings.TrimSpace(value) {
		if r >= '0' && r <= '9' {
			current.WriteRune(r)
			continue
		}
		if current.Len() > 0 {
			out = append(out, current.String())
			current.Reset()
		}
	}
	if current.Len() > 0 {
		out = append(out, current.String())
	}
	return out
}

// padMicroseconds reads a fractional-second field written with fewer than six
// digits as the fraction it spells, so ".1" is 100000 microseconds.
func padMicroseconds(digits string) string {
	if len(digits) >= microsecondDigits {
		return digits[:microsecondDigits]
	}
	return digits + strings.Repeat("0", microsecondDigits-len(digits))
}

// fnMySQLToSeconds implements TO_SECONDS(d): the seconds from year 0 to the
// given datetime, which is the seconds counterpart of TO_DAYS.
func fnMySQLToSeconds(args []driver.Value) (driver.Value, error) {
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	// MySQL counts from the start of year 0, which puts the Unix epoch at day
	// 719528 -- the number TO_DAYS answers for it. The two disagree by one day
	// for a date in year 0 itself, where MySQL's own documentation says
	// TO_DAYS is unreliable: it warns against any date before 1582, when the
	// Gregorian calendar began.
	const daysToUnixEpoch = 719528
	days := dayNumber(tm) + daysToUnixEpoch
	return days*86400 + int64(tm.Hour())*3600 + int64(tm.Minute())*60 + int64(tm.Second()), nil
}

// fnMySQLTimestamp implements TIMESTAMP(expr[, time]): the value read as a
// datetime, with the second argument added to it as a time of day when there is
// one. SQLite has no function of that name, so the call failed by name.
func fnMySQLTimestamp(args []driver.Value) (driver.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("dialect: TIMESTAMP expects 1 or 2 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	if len(args) == 1 {
		return formatDateTimeValue(tm), nil
	}
	micros, ok := toMySQLTime(args[1])
	if !ok {
		return nil, nil
	}
	return formatDateTimeValue(tm.Add(time.Duration(micros) * time.Microsecond)), nil
}

// fnMySQLConvertTZ implements CONVERT_TZ(dt, from, to) for the fixed offsets
// this package can read. A named zone needs a zone database, which nothing here
// carries, and answers NULL as MySQL itself does when its zone tables are not
// loaded.
func fnMySQLConvertTZ(args []driver.Value) (driver.Value, error) {
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	from, ok1 := toString(args[1])
	to, ok2 := toString(args[2])
	if !ok1 || !ok2 {
		return nil, nil
	}
	fromOffset, ok1 := fixedZoneOffset(from)
	toOffset, ok2 := fixedZoneOffset(to)
	if !ok1 || !ok2 {
		return nil, nil
	}
	return formatDateTimeValue(tm.Add(time.Duration(toOffset-fromOffset) * time.Second)), nil
}

// fixedZoneOffset reads a "+09:00" or "-05:30" zone as seconds east of UTC,
// reporting false for a named zone.
func fixedZoneOffset(zone string) (int, bool) {
	zone = strings.TrimSpace(zone)
	// The one named zone this package can answer for, since every value it
	// holds is read as UTC already.
	switch strings.ToUpper(zone) {
	case "UTC", "GMT", "Z", "+00:00", "-00:00":
		return 0, true
	}
	if len(zone) != 6 || (zone[0] != '+' && zone[0] != '-') || zone[3] != ':' {
		return 0, false
	}
	hours, err1 := strconv.Atoi(zone[1:3])
	minutes, err2 := strconv.Atoi(zone[4:6])
	if err1 != nil || err2 != nil || minutes > 59 {
		return 0, false
	}
	seconds := hours*3600 + minutes*60
	if zone[0] == '-' {
		seconds = -seconds
	}
	return seconds, true
}

// mysqlCompositeParts maps each of MySQL's compound date-part names to the
// single fields it runs together, most significant first. EXTRACT of one
// answers those fields concatenated as a number: EXTRACT(HOUR_MINUTE FROM
// '13:45:56') is 1345 and EXTRACT(YEAR_MONTH FROM '2024-03-05') is 202403.
//
//nolint:gochecknoglobals // a fixed table of MySQL's own part names
var mysqlCompositeParts = map[string][]string{
	"second_microsecond": {unitSecond, unitMicrosecond},
	"minute_microsecond": {unitMinute, unitSecond, unitMicrosecond},
	"minute_second":      {unitMinute, unitSecond},
	"hour_microsecond":   {unitHour, unitMinute, unitSecond, unitMicrosecond},
	"hour_second":        {unitHour, unitMinute, unitSecond},
	"hour_minute":        {unitHour, unitMinute},
	"day_microsecond":    {unitDay, unitHour, unitMinute, unitSecond, unitMicrosecond},
	"day_second":         {unitDay, unitHour, unitMinute, unitSecond},
	"day_minute":         {unitDay, unitHour, unitMinute},
	"day_hour":           {unitDay, unitHour},
	"year_month":         {unitYear, unitMonth},
}

// mysqlCompositeWidths is how many digits each field takes when it is not the
// leading one, so the fields concatenate the way MySQL concatenates them.
//
//nolint:gochecknoglobals // a fixed table beside the one above
var mysqlCompositeWidths = map[string]int{
	unitMonth: 2, unitDay: 2, unitHour: 2, unitMinute: 2, unitSecond: 2, unitMicrosecond: 6,
}

// mysqlCompositePart answers EXTRACT for one of the compound part names, and
// reports whether the name is one of them.
func mysqlCompositePart(part string, tm time.Time) (driver.Value, bool, error) {
	fields, ok := mysqlCompositeParts[part]
	if !ok {
		return nil, false, nil
	}
	var out int64
	for i, field := range fields {
		value, err := mysqlSinglePart(field, tm)
		if err != nil {
			return nil, true, err
		}
		if i == 0 {
			out = value
			continue
		}
		width := mysqlCompositeWidths[field]
		out = out*int64(math.Pow10(width)) + value
	}
	return out, true, nil
}

// mysqlSinglePart is the value of one plain date part, which the compound names
// are built from.
func mysqlSinglePart(field string, tm time.Time) (int64, error) {
	if field == unitMicrosecond {
		return int64(tm.Nanosecond() / 1000), nil
	}
	value, err := datePartValue(field, tm)
	if err != nil {
		return 0, err
	}
	n, ok := toInt(value)
	if !ok {
		return 0, fmt.Errorf("dialect: date part %q is not a number", field)
	}
	return n, nil
}

// formatMySQLTime writes a TIME the way MySQL prints one: a sign when it is
// negative, at least two digits of hours, and a fraction only when there is
// one, with its trailing zeros removed.
func formatMySQLTime(micros int64) string {
	sign := ""
	if micros < 0 {
		sign = "-"
		micros = -micros
	}
	hours := micros / microsPerHour
	minutes := micros % microsPerHour / microsPerMinute
	seconds := micros % microsPerMinute / microsPerSecond
	frac := micros % microsPerSecond
	out := fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
	if frac == 0 {
		return out
	}
	return out + "." + strings.TrimRight(fmt.Sprintf("%06d", frac), "0")
}

// toMySQLTime coerces a value to a TIME.
func toMySQLTime(v driver.Value) (int64, bool) {
	s, ok := toString(v)
	if !ok {
		return 0, false
	}
	return parseMySQLTime(s)
}

// fnSecToTime implements SEC_TO_TIME(n): the seconds as a TIME, with the
// fraction MySQL keeps six digits of.
func fnSecToTime(args []driver.Value) (driver.Value, error) {
	v, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	if math.IsNaN(v) {
		return nil, nil
	}
	micros := clampTime(roundToInt64(v * float64(microsPerSecond)))
	return formatMySQLTime(micros), nil
}

// fnTimeToSec implements TIME_TO_SEC(t): the TIME as a count of seconds, with
// the fraction dropped rather than rounded, which is what MySQL does.
func fnTimeToSec(args []driver.Value) (driver.Value, error) {
	micros, ok := toMySQLTime(args[0])
	if !ok {
		return nil, nil
	}
	return micros / microsPerSecond, nil
}

// fnMakeTime implements MAKETIME(hour, minute, second). A minute or second
// outside 0..59 is NULL rather than a carry, and an hour past the range is
// clamped, both of which are MySQL's answers. The sign comes from the hour.
func fnMakeTime(args []driver.Value) (driver.Value, error) {
	hour, ok1 := toInt(args[0])
	minute, ok2 := toInt(args[1])
	second, ok3 := toFloat(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if minute < 0 || minute > 59 || second < 0 || second >= 60 {
		return nil, nil
	}
	negative := hour < 0
	if negative {
		hour = -hour
	}
	micros := hour*microsPerHour + minute*microsPerMinute + roundToInt64(second*float64(microsPerSecond))
	if negative {
		micros = -micros
	}
	return formatMySQLTime(clampTime(micros)), nil
}

// fnMicrosecond implements MICROSECOND(t): the fraction of a second, in
// microseconds.
func fnMicrosecond(args []driver.Value) (driver.Value, error) {
	micros, ok := toMySQLTime(args[0])
	if !ok {
		// A date with no time of day is midnight, which has no fraction, and a
		// value that is neither is NULL.
		if _, isDate := toStringTime(args[0]); isDate {
			return int64(0), nil
		}
		return nil, nil
	}
	if micros < 0 {
		micros = -micros
	}
	return micros % microsPerSecond, nil
}

// addTime backs ADDTIME and SUBTIME, which differ only in the sign they give
// the second operand. When the first is a datetime the answer is a datetime;
// when it is a TIME the answer is a TIME, which can run past a day.
func addTime(sign int64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		// The second operand is a TIME and not a datetime: MySQL answers NULL
		// for one carrying a date rather than taking its time of day.
		if s, isString := toString(args[1]); isString && hasDatePart(s) {
			return nil, nil
		}
		delta, ok := toMySQLTime(args[1])
		if !ok {
			return nil, nil
		}
		delta *= sign
		if s, isString := toString(args[0]); isString && hasDatePart(s) {
			base, parsed := toStringTime(args[0])
			if !parsed {
				return nil, nil
			}
			return formatDateTimeValue(base.Add(time.Duration(delta) * time.Microsecond)), nil
		}
		base, parsed := toMySQLTime(args[0])
		if !parsed {
			return nil, nil
		}
		return formatMySQLTime(clampTime(base + delta)), nil
	}
}

// hasDatePart reports whether a value written as a string carries a date as
// well as a time of day, which decides whether ADDTIME answers a datetime.
func hasDatePart(s string) bool {
	s = strings.TrimSpace(s)
	// A leading sign belongs to a TIME, not to a date: "-01:00:00" is a
	// negative span and the hyphen in it is not a date separator.
	s = strings.TrimLeft(s, "+-")
	before, _, found := strings.Cut(s, " ")
	if !found {
		before = s
	}
	return strings.Contains(before, "-") || strings.Contains(before, "/")
}

// timeFormatZeroes are the date specifiers TIME_FORMAT answers with zeros
// rather than refusing, because a TIME has no date and MySQL prints the field
// as if it were empty.
var timeFormatZeroes = map[byte]string{ //nolint:gochecknoglobals // a fixed table read by TIME_FORMAT
	'c': "0", 'd': "00", 'e': "0", 'm': "00", 'y': "00", 'Y': "0000",
}

// timeFormatRefused are the date specifiers TIME_FORMAT answers NULL for: a
// month name or a week number cannot be printed from a value that has no date,
// and MySQL refuses the whole call rather than inventing one.
var timeFormatRefused = map[byte]bool{ //nolint:gochecknoglobals // a fixed table read by TIME_FORMAT
	'a': true, 'b': true, 'j': true, 'u': true, 'v': true, 'w': true,
	'x': true, 'D': true, 'M': true, 'U': true, 'V': true, 'W': true, 'X': true,
}

// fnTimeFormat implements TIME_FORMAT(t, format).
func fnTimeFormat(args []driver.Value) (driver.Value, error) {
	micros, ok := toMySQLTime(args[0])
	if !ok {
		return nil, nil
	}
	format, ok := toString(args[1])
	if !ok || format == "" {
		// MySQL answers NULL for an empty format rather than an empty string.
		return nil, nil
	}
	var b strings.Builder
	// The sign stands in front of the whole result rather than on each field,
	// which is what MySQL prints: TIME_FORMAT('-01:30:45', '%k %p') is "-1 AM".
	if micros < 0 {
		b.WriteByte('-')
		micros = -micros
	}
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			b.WriteByte(format[i])
			continue
		}
		i++
		if timeFormatRefused[format[i]] {
			return nil, nil
		}
		if zero, isZero := timeFormatZeroes[format[i]]; isZero {
			b.WriteString(zero)
			continue
		}
		b.WriteString(timeSpecifier(format[i], micros))
	}
	return b.String(), nil
}

// timeSpecifier renders one TIME_FORMAT specifier. A specifier this does not
// know is written as the letter itself, which is what MySQL does.
func timeSpecifier(spec byte, micros int64) string {
	value := micros
	hours := value / microsPerHour
	minutes := value % microsPerHour / microsPerMinute
	seconds := value % microsPerMinute / microsPerSecond
	switch spec {
	case 'H':
		return fmt.Sprintf("%02d", hours)
	case 'k':
		return strconv.FormatInt(hours, 10)
	case 'h', 'I':
		return fmt.Sprintf("%02d", twelveHour(hours))
	case 'l':
		return strconv.FormatInt(twelveHour(hours), 10)
	case 'i':
		return fmt.Sprintf("%02d", minutes)
	case 's', 'S':
		return fmt.Sprintf("%02d", seconds)
	case 'f':
		return fmt.Sprintf("%06d", value%microsPerSecond)
	case 'p':
		return meridiem(hours)
	case 'r':
		return fmt.Sprintf("%02d:%02d:%02d %s", twelveHour(hours), minutes, seconds, meridiem(hours))
	case 'T':
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	case '%':
		return "%"
	default:
		return string(spec)
	}
}

// hoursPerHalfDay is the width of the 12-hour clock TIME_FORMAT's %h and %p
// read the hour field against.
const hoursPerHalfDay = 12

// twelveHour is the hour on a 12-hour clock, where zero reads as twelve.
func twelveHour(hours int64) int64 {
	h := hours % (2 * hoursPerHalfDay) % hoursPerHalfDay
	if h == 0 {
		return hoursPerHalfDay
	}
	return h
}

// meridiem is AM or PM for an hour field.
func meridiem(hours int64) string {
	if hours%(2*hoursPerHalfDay) < hoursPerHalfDay {
		return "AM"
	}
	return "PM"
}

// toDaysEpoch is what TO_DAYS answers for 1970-01-01, which is where its day
// count and the Unix day count meet.
const toDaysEpoch = 719528

// minToDays and maxToDays bound the day numbers FROM_DAYS answers a date for:
// below the first is the zero date MySQL prints, and above the last is past the
// year 9999 it stores.
const (
	minToDays = 366
	maxToDays = 3652424
)

// zeroDate is what MySQL prints for a day number before the year one.
const zeroDate = "0000-00-00"

// fnToDays implements TO_DAYS(d): the day number of a date, counting from the
// year zero the way MySQL does.
func fnToDays(args []driver.Value) (driver.Value, error) {
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	return dayNumber(tm) + toDaysEpoch, nil
}

// fnFromDays implements FROM_DAYS(n): the date a day number names.
func fnFromDays(args []driver.Value) (driver.Value, error) {
	n, ok := toInt(args[0])
	if !ok {
		return nil, nil
	}
	if n < minToDays {
		return zeroDate, nil
	}
	if n > maxToDays {
		return nil, nil
	}
	return civilFromDays(n - toDaysEpoch).Format(layoutDateOnly), nil
}

// civilFromDays is the inverse of dayNumber.
func civilFromDays(days int64) time.Time {
	return time.Unix(days*24*60*60, 0).UTC()
}

// twoDigitYearPivot is where MySQL splits a two-digit year: 69 and below are in
// the twenty-first century and 70 and above in the twentieth.
const twoDigitYearPivot = 69

// expandTwoDigitYear applies that rule. A year already written in full is left
// alone.
func expandTwoDigitYear(year int64) int64 {
	switch {
	case year > 99:
		return year
	case year <= twoDigitYearPivot:
		return 2000 + year
	default:
		return 1900 + year
	}
}

// maxYear is the last year MySQL stores a date in.
const maxYear = 9999

// fnMakeDate implements MAKEDATE(year, dayofyear): the date that many days into
// the year, counting the first of January as day one. A day number below one is
// NULL and one past the end of the year rolls into the next.
func fnMakeDate(args []driver.Value) (driver.Value, error) {
	year, ok1 := toInt(args[0])
	day, ok2 := toInt(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	if day < 1 || year < 0 || year > maxYear {
		return nil, nil
	}
	tm := time.Date(int(expandTwoDigitYear(year)), time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(day-1))
	if tm.Year() > maxYear {
		return nil, nil
	}
	return tm.Format(layoutDateOnly), nil
}

// monthsPerYear is how many months a period covers in a year.
const monthsPerYear = 12

// fnPeriodAdd implements PERIOD_ADD(p, n): the period n months after p. A
// period is YYMM or YYYYMM, and one whose month is not a month is refused, the
// way MySQL refuses it rather than answering.
func fnPeriodAdd(args []driver.Value) (driver.Value, error) {
	period, ok1 := toInt(args[0])
	months, ok2 := toInt(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	total, err := periodToMonths(period, "PERIOD_ADD")
	if err != nil {
		return nil, err
	}
	return monthsToPeriod(total + months), nil
}

// fnPeriodDiff implements PERIOD_DIFF(p1, p2): the months between two periods.
func fnPeriodDiff(args []driver.Value) (driver.Value, error) {
	first, ok1 := toInt(args[0])
	second, ok2 := toInt(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	a, err := periodToMonths(first, "PERIOD_DIFF")
	if err != nil {
		return nil, err
	}
	b, err := periodToMonths(second, "PERIOD_DIFF")
	if err != nil {
		return nil, err
	}
	return a - b, nil
}

// periodToMonths converts a YYMM or YYYYMM period into a count of months.
func periodToMonths(period int64, name string) (int64, error) {
	if period < 0 {
		return 0, fmt.Errorf("dialect: %s: %d is not a period", name, period)
	}
	month := period % 100
	if month < 1 || month > monthsPerYear {
		return 0, fmt.Errorf("dialect: %s: %d has no month", name, period)
	}
	return expandTwoDigitYear(period/100)*monthsPerYear + month - 1, nil
}

// monthsToPeriod is the inverse of periodToMonths, written the way MySQL writes
// a period back: four digits of year and two of month.
func monthsToPeriod(months int64) int64 {
	return months/monthsPerYear*100 + months%monthsPerYear + 1
}
