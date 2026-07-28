package dialect

import (
	"crypto/rand"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlite "modernc.org/sqlite"
)

// randFloat returns a uniform float64 in [0, 1) sourced from crypto/rand, so no
// package-global PRNG state is seeded or shared.
func randFloat() float64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	return float64(binary.BigEndian.Uint64(b[:])>>11) / float64(uint64(1)<<53)
}

// RegisterFunctions registers the dialect helper functions (REGEXP, NOW,
// DATE_FORMAT, DATE_PART, ...) into the modernc.org/sqlite driver so translated
// queries that call them succeed. Registration is process-global and applies to
// connections opened afterward, so callers should invoke it during startup,
// before opening the database that runs dialect queries.
//
// It is idempotent and safe to call multiple times and from multiple
// goroutines; the functions are registered only once. The functions are always
// registered as a set regardless of which dialect is in use, so a helper such as
// SPLIT_PART is available even under the SQLite dialect.
func RegisterFunctions() error {
	registerOnce.Do(func() {
		errRegister = registerAll()
	})
	return errRegister
}

var (
	registerOnce sync.Once
	errRegister  error
)

// layoutDateTime is the canonical datetime string the helpers emit.
const layoutDateTime = "2006-01-02 15:04:05"

// Date-part / interval unit names shared by the date helpers and the interval
// rewrite so the same spelling is used for a function name, an INTERVAL unit,
// and a DATE_PART argument.
const (
	unitYear      = "year"
	unitMonth     = "month"
	unitDay       = "day"
	unitHour      = "hour"
	unitMinute    = "minute"
	unitSecond    = "second"
	unitDayOfWeek = "dayofweek"
	unitDayOfYear = "dayofyear"
	unitWeekday   = "weekday"
)

// scalarFn adapts a simpler signature (no context) to the driver's scalar
// function shape.
type scalarFn func(args []driver.Value) (driver.Value, error)

func registerAll() error {
	// deterministic functions: same output for the same inputs.
	det := map[string]struct {
		nArg int32
		fn   scalarFn
	}{
		"regexp":          {2, fnRegexp}, // REGEXP(pattern, s); also the "x REGEXP y" operator
		"regexp_contains": {2, fnRegexp}, // GoogleSQL alias used by later dialects
		"if":              {3, fnIf},     // IF(cond, a, b)
		"date_format":     {2, fnDateFormat},
		"str_to_date":     {2, fnStrToDate},
		"datediff":        {2, fnDateDiff},
		"date_part":       {2, fnDatePart},
		unitYear:          {1, unaryDatePart(unitYear)},
		unitMonth:         {1, unaryDatePart(unitMonth)},
		unitDay:           {1, unaryDatePart(unitDay)},
		unitHour:          {1, unaryDatePart(unitHour)},
		unitMinute:        {1, unaryDatePart(unitMinute)},
		unitSecond:        {1, unaryDatePart(unitSecond)},
		unitDayOfWeek:     {1, unaryDatePart(unitDayOfWeek)},
		unitDayOfYear:     {1, unaryDatePart(unitDayOfYear)},
		unitWeekday:       {1, unaryDatePart(unitWeekday)},
		"locate":          {-1, fnLocate},
		"lpad":            {3, fnLpad},
		"rpad":            {3, fnRpad},
		"substring_index": {3, fnSubstringIndex},
		"repeat":          {2, fnRepeat},
		"space":           {1, fnSpace},
		"truncate":        {2, fnTruncate},
	}
	for name, spec := range det {
		if err := sqlite.RegisterDeterministicScalarFunction(name, spec.nArg, wrapScalar(spec.fn)); err != nil {
			return fmt.Errorf("dialect: register %s: %w", name, err)
		}
	}

	// Non-deterministic functions must not be registered as deterministic.
	nondet := map[string]struct {
		nArg int32
		fn   scalarFn
	}{
		"now":     {0, fnNow},
		"curdate": {0, fnCurdate},
		"curtime": {0, fnCurtime},
		"rand":    {0, fnRand},
	}
	for name, spec := range nondet {
		if err := sqlite.RegisterScalarFunction(name, spec.nArg, wrapScalar(spec.fn)); err != nil {
			return fmt.Errorf("dialect: register %s: %w", name, err)
		}
	}
	return nil
}

func wrapScalar(fn scalarFn) func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
	return func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		return fn(args)
	}
}

// --- value coercion helpers ---

// toString converts a driver.Value to its string form, reporting whether the
// value was non-NULL.
func toString(v driver.Value) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case string:
		return x, true
	case []byte:
		return string(x), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), true
	case bool:
		if x {
			return "1", true
		}
		return "0", true
	case time.Time:
		return x.Format(layoutDateTime), true
	default:
		return fmt.Sprintf("%v", x), true
	}
}

// toInt converts a driver.Value to an int64, reporting whether it was non-NULL
// and numeric.
func toInt(v driver.Value) (int64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case int64:
		return x, true
	case float64:
		return int64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n, err == nil
	case []byte:
		n, err := strconv.ParseInt(strings.TrimSpace(string(x)), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

// toFloat converts a driver.Value to a float64, reporting success.
func toFloat(v driver.Value) (float64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	case []byte:
		f, err := strconv.ParseFloat(strings.TrimSpace(string(x)), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// --- common / MySQL scalar functions ---

var (
	regexpCacheMu sync.RWMutex
	regexpCache   = map[string]*regexp.Regexp{}
)

// compileRegexp compiles pattern, caching the result. Compilation errors are
// returned so the caller can surface them.
func compileRegexp(pattern string) (*regexp.Regexp, error) {
	regexpCacheMu.RLock()
	re, ok := regexpCache[pattern]
	regexpCacheMu.RUnlock()
	if ok {
		return re, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	regexpCacheMu.Lock()
	regexpCache[pattern] = re
	regexpCacheMu.Unlock()
	return re, nil
}

// fnRegexp implements REGEXP(pattern, subject), the function SQLite invokes for
// the "subject REGEXP pattern" operator. It returns 1 on match, 0 otherwise, and
// NULL when either argument is NULL.
func fnRegexp(args []driver.Value) (driver.Value, error) {
	pattern, ok1 := toString(args[0])
	subject, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return nil, err
	}
	if re.MatchString(subject) {
		return int64(1), nil
	}
	return int64(0), nil
}

// fnIf implements IF(cond, a, b): a when cond is truthy, else b.
func fnIf(args []driver.Value) (driver.Value, error) {
	if isTruthy(args[0]) {
		return args[1], nil
	}
	return args[2], nil
}

// isTruthy applies SQLite-like truthiness: NULL and zero are false.
func isTruthy(v driver.Value) bool {
	switch x := v.(type) {
	case nil:
		return false
	case int64:
		return x != 0
	case float64:
		return x != 0
	case bool:
		return x
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return x != ""
		}
		return f != 0
	case []byte:
		return len(x) != 0
	default:
		return true
	}
}

func fnNow(_ []driver.Value) (driver.Value, error) {
	return time.Now().Format(layoutDateTime), nil
}

func fnCurdate(_ []driver.Value) (driver.Value, error) {
	return time.Now().Format("2006-01-02"), nil
}

func fnCurtime(_ []driver.Value) (driver.Value, error) {
	return time.Now().Format("15:04:05"), nil
}

func fnRand(_ []driver.Value) (driver.Value, error) {
	return randFloat(), nil
}

// mysqlToGoLayout maps the MySQL DATE_FORMAT specifiers to Go reference-time
// layout fragments. Unlisted specifiers are passed through without the percent.
var mysqlToGoLayout = map[byte]string{
	'Y': "2006",
	'y': "06",
	'm': "01",
	'c': "1",
	'd': "02",
	'e': "2",
	'H': "15",
	'h': "03",
	'I': "03",
	'i': "04",
	's': "05",
	'S': "05",
	'p': "PM",
	'M': "January",
	'b': "Jan",
	'W': "Monday",
	'a': "Mon",
}

// fnDateFormat implements MySQL DATE_FORMAT(date, format).
func fnDateFormat(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	format, ok2 := toString(args[1])
	if !ok || !ok2 {
		return nil, nil
	}
	tm, ok := parseTime(s)
	if !ok {
		return nil, nil
	}
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] == '%' && i+1 < len(format) {
			if layout, found := mysqlToGoLayout[format[i+1]]; found {
				b.WriteString(tm.Format(layout))
			} else if extra, found := dateFormatSpecial(tm, format[i+1]); found {
				b.WriteString(extra)
			} else {
				b.WriteByte(format[i+1])
			}
			i++
			continue
		}
		b.WriteByte(format[i])
	}
	return b.String(), nil
}

// dateFormatSpecial handles DATE_FORMAT specifiers that have no direct Go layout
// (day of year, weekday index).
func dateFormatSpecial(tm time.Time, spec byte) (string, bool) {
	switch spec {
	case 'j':
		return fmt.Sprintf("%03d", tm.YearDay()), true
	case 'w':
		return strconv.Itoa(int(tm.Weekday())), true
	case '%':
		return "%", true
	default:
		return "", false
	}
}

// fnStrToDate implements a pragmatic MySQL STR_TO_DATE(str, format): it parses
// str against a Go layout derived from the MySQL format and returns a canonical
// datetime string.
func fnStrToDate(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	format, ok2 := toString(args[1])
	if !ok || !ok2 {
		return nil, nil
	}
	var layout strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] == '%' && i+1 < len(format) {
			if l, found := mysqlToGoLayout[format[i+1]]; found {
				layout.WriteString(l)
			} else {
				layout.WriteByte(format[i+1])
			}
			i++
			continue
		}
		layout.WriteByte(format[i])
	}
	tm, ok3 := parseLayout(layout.String(), s)
	if !ok3 {
		return nil, nil
	}
	return tm.Format(layoutDateTime), nil
}

// parseLayout parses s against a single Go layout, reporting success.
func parseLayout(layout, s string) (time.Time, bool) {
	tm, err := time.Parse(layout, s)
	return tm, err == nil
}

// fnDateDiff implements MySQL DATEDIFF(a, b) = whole days from b to a.
func fnDateDiff(args []driver.Value) (driver.Value, error) {
	a, ok1 := toStringTime(args[0])
	b, ok2 := toStringTime(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	da := time.Date(a.Year(), a.Month(), a.Day(), 0, 0, 0, 0, time.UTC)
	db := time.Date(b.Year(), b.Month(), b.Day(), 0, 0, 0, 0, time.UTC)
	return int64(da.Sub(db).Hours() / 24), nil
}

// fnDatePart implements DATE_PART(unit, date), returning the requested field as
// an integer. It also backs the MySQL YEAR/MONTH/... helpers and the EXTRACT
// rewrite.
func fnDatePart(args []driver.Value) (driver.Value, error) {
	unit, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	tm, ok := toStringTime(args[1])
	if !ok {
		return nil, nil
	}
	return datePartValue(strings.ToLower(strings.TrimSpace(unit)), tm)
}

func datePartValue(unit string, tm time.Time) (driver.Value, error) {
	switch unit {
	case unitYear:
		return int64(tm.Year()), nil
	case unitMonth:
		return int64(tm.Month()), nil
	case unitDay:
		return int64(tm.Day()), nil
	case unitHour:
		return int64(tm.Hour()), nil
	case unitMinute:
		return int64(tm.Minute()), nil
	case unitSecond:
		return int64(tm.Second()), nil
	case "dow", unitDayOfWeek:
		// MySQL DAYOFWEEK: Sunday=1..Saturday=7.
		return int64(tm.Weekday()) + 1, nil
	case unitWeekday:
		// MySQL WEEKDAY: Monday=0..Sunday=6.
		return int64((int(tm.Weekday()) + 6) % 7), nil
	case "doy", unitDayOfYear:
		return int64(tm.YearDay()), nil
	case "quarter":
		return int64((int(tm.Month())-1)/3 + 1), nil
	case "week":
		_, wk := tm.ISOWeek()
		return int64(wk), nil
	case "epoch":
		return tm.Unix(), nil
	default:
		return nil, fmt.Errorf("dialect: unsupported date part %q", unit)
	}
}

// unaryDatePart builds a one-argument function (YEAR, MONTH, ...) from a fixed
// date part.
func unaryDatePart(unit string) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		tm, ok := toStringTime(args[0])
		if !ok {
			return nil, nil
		}
		return datePartValue(unit, tm)
	}
}

// fnLocate implements MySQL LOCATE(substr, str[, pos]) returning a 1-based
// index, or 0 when not found.
func fnLocate(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("dialect: LOCATE expects 2 or 3 arguments, got %d", len(args))
	}
	substr, ok1 := toString(args[0])
	str, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	start := 0
	if len(args) == 3 {
		pos, ok := toInt(args[2])
		if !ok || pos < 1 {
			return int64(0), nil
		}
		start = int(pos) - 1
		if start > len(str) {
			return int64(0), nil
		}
	}
	idx := strings.Index(str[start:], substr)
	if idx < 0 {
		return int64(0), nil
	}
	return int64(start + idx + 1), nil
}

func fnLpad(args []driver.Value) (driver.Value, error) { return pad(args, true) }
func fnRpad(args []driver.Value) (driver.Value, error) { return pad(args, false) }

func pad(args []driver.Value, left bool) (driver.Value, error) {
	s, ok1 := toString(args[0])
	n, ok2 := toInt(args[1])
	padStr, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if n < 0 {
		return nil, nil
	}
	length := int(n)
	if len(s) >= length {
		return s[:length], nil
	}
	if padStr == "" {
		return s, nil
	}
	needed := length - len(s)
	var fill strings.Builder
	for fill.Len() < needed {
		fill.WriteString(padStr)
	}
	filler := fill.String()[:needed]
	if left {
		return filler + s, nil
	}
	return s + filler, nil
}

// fnSubstringIndex implements MySQL SUBSTRING_INDEX(str, delim, count).
func fnSubstringIndex(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	delim, ok2 := toString(args[1])
	count, ok3 := toInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if delim == "" || count == 0 {
		return "", nil
	}
	parts := strings.Split(s, delim)
	if count > 0 {
		if int(count) >= len(parts) {
			return s, nil
		}
		return strings.Join(parts[:count], delim), nil
	}
	c := int(-count)
	if c >= len(parts) {
		return s, nil
	}
	return strings.Join(parts[len(parts)-c:], delim), nil
}

// fnRepeat implements MySQL REPEAT(str, count).
func fnRepeat(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	count, ok2 := toInt(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	if count <= 0 {
		return "", nil
	}
	return strings.Repeat(s, int(count)), nil
}

// fnSpace implements MySQL SPACE(n).
func fnSpace(args []driver.Value) (driver.Value, error) {
	n, ok := toInt(args[0])
	if !ok {
		return nil, nil
	}
	if n <= 0 {
		return "", nil
	}
	return strings.Repeat(" ", int(n)), nil
}

// fnTruncate implements MySQL TRUNCATE(x, d): truncate x to d decimal places
// toward zero.
func fnTruncate(args []driver.Value) (driver.Value, error) {
	x, ok1 := toFloat(args[0])
	d, ok2 := toInt(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	factor := math.Pow(10, float64(d))
	return math.Trunc(x*factor) / factor, nil
}

// --- time parsing ---

// timeLayouts are tried in order by parseTime, covering the common SQL date and
// datetime shapes.
var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02",
	"2006/01/02 15:04:05",
	"2006/01/02",
	"15:04:05",
}

// parseTime parses a date/time string against the supported layouts.
func parseTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range timeLayouts {
		if tm, err := time.Parse(layout, s); err == nil {
			return tm, true
		}
	}
	return time.Time{}, false
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
