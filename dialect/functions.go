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

// Go reference-time fragments for month and weekday names, shared by the MySQL
// and PostgreSQL format mappings.
const (
	layoutMonthLong    = "January"
	layoutMonthShort   = "Jan"
	layoutWeekdayLong  = "Monday"
	layoutWeekdayShort = "Mon"
)

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
	unitQuarter   = "quarter"
	unitWeek      = "week"
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

		// PostgreSQL helpers.
		"to_char":        {2, fnToChar},
		"to_date":        {2, fnToDate},
		"date_trunc":     {2, fnDateTrunc},
		"split_part":     {3, fnSplitPart},
		"initcap":        {1, fnInitcap},
		"strpos":         {2, fnStrpos},
		"left":           {2, fnLeft},
		"right":          {2, fnRight},
		"regexp_replace": {3, fnRegexpReplace},

		// GoogleSQL helpers.
		"safe_divide":     {2, fnSafeDivide},
		"starts_with":     {2, fnStartsWith},
		"ends_with":       {2, fnEndsWith},
		"regexp_contains": {2, fnRegexpContains},
		"regexp_extract":  {2, fnRegexpExtract},
		"date_diff":       {3, fnDateDiff3},
		"timestamp_diff":  {3, fnDateDiff3},
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
		"now":           {0, fnNow},
		"curdate":       {0, fnCurdate},
		"curtime":       {0, fnCurtime},
		"rand":          {0, fnRand},
		"generate_uuid": {0, fnGenerateUUID},
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
	'M': layoutMonthLong,
	'b': layoutMonthShort,
	'W': layoutWeekdayLong,
	'a': layoutWeekdayShort,
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
	case unitQuarter:
		return int64((int(tm.Month())-1)/3 + 1), nil
	case unitWeek:
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

// --- PostgreSQL scalar functions ---

// pgToCharTokens maps TO_CHAR template patterns to Go reference-time layout
// fragments, longest first so the scanner prefers the longest match.
var pgToCharTokens = []struct{ pat, layout string }{
	{"YYYY", "2006"},
	{"YY", "06"},
	{"MONTH", layoutMonthLong},
	{"Month", layoutMonthLong},
	{"MON", layoutMonthShort},
	{"Mon", layoutMonthShort},
	{"MM", "01"},
	{"DAY", layoutWeekdayLong},
	{"Day", layoutWeekdayLong},
	{"DY", layoutWeekdayShort},
	{"Dy", layoutWeekdayShort},
	{"DD", "02"},
	{"HH24", "15"},
	{"HH12", "03"},
	{"HH", "03"},
	{"MI", "04"},
	{"SS", "05"},
	{"AM", "PM"},
	{"PM", "PM"},
	{"am", "pm"},
	{"pm", "pm"},
}

// toCharLayout converts a PostgreSQL TO_CHAR/TO_DATE template into a Go layout,
// dropping the "FM" fill-mode prefix and passing literal characters through.
func toCharLayout(format string) string {
	var b strings.Builder
	for i := 0; i < len(format); {
		if strings.HasPrefix(format[i:], "FM") {
			i += 2
			continue
		}
		matched := false
		for _, tok := range pgToCharTokens {
			if strings.HasPrefix(format[i:], tok.pat) {
				b.WriteString(tok.layout)
				i += len(tok.pat)
				matched = true
				break
			}
		}
		if !matched {
			b.WriteByte(format[i])
			i++
		}
	}
	return b.String()
}

// fnToChar implements PostgreSQL TO_CHAR(value, format) for date/time values.
func fnToChar(args []driver.Value) (driver.Value, error) {
	format, ok := toString(args[1])
	if !ok {
		return nil, nil
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	return tm.Format(toCharLayout(format)), nil
}

// fnToDate implements PostgreSQL TO_DATE(str, format).
func fnToDate(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	format, ok2 := toString(args[1])
	if !ok || !ok2 {
		return nil, nil
	}
	tm, ok := parseLayout(toCharLayout(format), s)
	if !ok {
		return nil, nil
	}
	return tm.Format("2006-01-02"), nil
}

// fnDateTrunc implements PostgreSQL DATE_TRUNC(unit, timestamp).
func fnDateTrunc(args []driver.Value) (driver.Value, error) {
	unit, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	tm, ok := toStringTime(args[1])
	if !ok {
		return nil, nil
	}
	y, mo, d := tm.Date()
	loc := tm.Location()
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case unitYear:
		return time.Date(y, 1, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitQuarter:
		q := (int(mo)-1)/3*3 + 1
		return time.Date(y, time.Month(q), 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitMonth:
		return time.Date(y, mo, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitWeek:
		offset := (int(tm.Weekday()) + 6) % 7 // days since Monday
		monday := time.Date(y, mo, d, 0, 0, 0, 0, loc).AddDate(0, 0, -offset)
		return monday.Format(layoutDateTime), nil
	case unitDay:
		return time.Date(y, mo, d, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitHour:
		return time.Date(y, mo, d, tm.Hour(), 0, 0, 0, loc).Format(layoutDateTime), nil
	case unitMinute:
		return time.Date(y, mo, d, tm.Hour(), tm.Minute(), 0, 0, loc).Format(layoutDateTime), nil
	case unitSecond:
		return time.Date(y, mo, d, tm.Hour(), tm.Minute(), tm.Second(), 0, loc).Format(layoutDateTime), nil
	default:
		return nil, fmt.Errorf("dialect: unsupported DATE_TRUNC unit %q", unit)
	}
}

// fnSplitPart implements PostgreSQL SPLIT_PART(string, delimiter, n) with a
// 1-based field index.
func fnSplitPart(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	delim, ok2 := toString(args[1])
	n, ok3 := toInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if delim == "" {
		if n == 1 || n == -1 {
			return s, nil
		}
		return "", nil
	}
	parts := strings.Split(s, delim)
	idx := int(n)
	if idx < 0 {
		idx = len(parts) + idx + 1
	}
	if idx < 1 || idx > len(parts) {
		return "", nil
	}
	return parts[idx-1], nil
}

// fnInitcap implements PostgreSQL INITCAP: uppercase the first letter of each
// alphanumeric run, lowercase the rest.
func fnInitcap(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	var b strings.Builder
	prevAlnum := false
	for _, r := range s {
		alnum := isAlnumRune(r)
		switch {
		case alnum && !prevAlnum:
			b.WriteString(strings.ToUpper(string(r)))
		case alnum:
			b.WriteString(strings.ToLower(string(r)))
		default:
			b.WriteRune(r)
		}
		prevAlnum = alnum
	}
	return b.String(), nil
}

func isAlnumRune(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// fnStrpos implements PostgreSQL STRPOS(string, substring): 1-based index or 0.
func fnStrpos(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	sub, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	idx := strings.Index(s, sub)
	if idx < 0 {
		return int64(0), nil
	}
	return int64(idx + 1), nil
}

func fnLeft(args []driver.Value) (driver.Value, error)  { return leftRight(args, true) }
func fnRight(args []driver.Value) (driver.Value, error) { return leftRight(args, false) }

// leftRight implements LEFT/RIGHT with PostgreSQL's negative-count semantics: a
// negative n removes |n| characters from the far end.
func leftRight(args []driver.Value, left bool) (driver.Value, error) {
	s, ok1 := toString(args[0])
	n, ok2 := toInt(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	runes := []rune(s)
	count := int(n)
	if count < 0 {
		count = len(runes) + count
	}
	if count <= 0 {
		return "", nil
	}
	if count > len(runes) {
		count = len(runes)
	}
	if left {
		return string(runes[:count]), nil
	}
	return string(runes[len(runes)-count:]), nil
}

// fnRegexpReplace implements REGEXP_REPLACE(source, pattern, replacement),
// replacing every match. PostgreSQL back-references (\1) are translated to Go's
// ${1} expansion form.
func fnRegexpReplace(args []driver.Value) (driver.Value, error) {
	src, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	repl, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return nil, err
	}
	return re.ReplaceAllString(src, pgReplacement(repl)), nil
}

// pgReplacement translates PostgreSQL replacement back-references (\1..\9, \&) to
// the ${n} form Go's regexp expansion understands.
func pgReplacement(repl string) string {
	var b strings.Builder
	for i := 0; i < len(repl); i++ {
		if repl[i] == '\\' && i+1 < len(repl) {
			c := repl[i+1]
			switch {
			case c >= '0' && c <= '9':
				b.WriteString("${")
				b.WriteByte(c)
				b.WriteByte('}')
			case c == '&':
				b.WriteString("${0}")
			default:
				b.WriteByte(c)
			}
			i++
			continue
		}
		if repl[i] == '$' {
			// Escape a literal '$' so Go does not treat it as an expansion.
			b.WriteString("$$")
			continue
		}
		b.WriteByte(repl[i])
	}
	return b.String()
}

// --- GoogleSQL scalar functions ---

// fnSafeDivide implements GoogleSQL SAFE_DIVIDE(x, y): x/y, or NULL when y is 0
// or either argument is NULL.
func fnSafeDivide(args []driver.Value) (driver.Value, error) {
	x, ok1 := toFloat(args[0])
	y, ok2 := toFloat(args[1])
	if !ok1 || !ok2 || y == 0 {
		return nil, nil
	}
	return x / y, nil
}

// fnStartsWith implements GoogleSQL STARTS_WITH(value, prefix).
func fnStartsWith(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	prefix, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return boolToInt(strings.HasPrefix(s, prefix)), nil
}

// fnEndsWith implements GoogleSQL ENDS_WITH(value, suffix).
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

// fnRegexpContains implements GoogleSQL REGEXP_CONTAINS(value, pattern). Note the
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

// fnRegexpExtract implements GoogleSQL REGEXP_EXTRACT(value, pattern): the first
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

// fnDateDiff3 implements GoogleSQL DATE_DIFF/TIMESTAMP_DIFF(a, b, unit): the
// signed count of unit boundaries from b to a.
func fnDateDiff3(args []driver.Value) (driver.Value, error) {
	a, ok1 := toStringTime(args[0])
	b, ok2 := toStringTime(args[1])
	unit, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case unitYear:
		return int64(a.Year() - b.Year()), nil
	case unitQuarter:
		return int64((a.Year()*4 + (int(a.Month())-1)/3) - (b.Year()*4 + (int(b.Month())-1)/3)), nil
	case unitMonth:
		return int64((a.Year()*12 + int(a.Month())) - (b.Year()*12 + int(b.Month()))), nil
	case unitWeek:
		return int64(truncDay(a).Sub(truncDay(b)).Hours() / 24 / 7), nil
	case unitDay:
		return int64(truncDay(a).Sub(truncDay(b)).Hours() / 24), nil
	case unitHour:
		return int64(a.Sub(b).Hours()), nil
	case unitMinute:
		return int64(a.Sub(b).Minutes()), nil
	case unitSecond:
		return int64(a.Sub(b).Seconds()), nil
	default:
		return nil, fmt.Errorf("dialect: unsupported DATE_DIFF unit %q", unit)
	}
}

func truncDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// fnGenerateUUID implements GoogleSQL GENERATE_UUID: a random RFC 4122 v4 UUID.
func fnGenerateUUID(_ []driver.Value) (driver.Value, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("dialect: generate_uuid: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
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
