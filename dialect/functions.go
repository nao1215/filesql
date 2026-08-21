package dialect

import (
	"crypto/md5" //nolint:gosec // MD5 backs PostgreSQL's MD5() function, not a security control
	"crypto/rand"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

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

// init registers the helper functions while the package is still initializing,
// before any goroutine can open a connection. The driver exposes functions only
// to connections opened after registration, and its registry is not safe to
// write while another goroutine opens a connection, so doing this once at
// startup is both the documented usage and the only race-free ordering.
// RegisterFunctions stays exported and idempotent; it reports the same stored
// error a caller would have seen here.
//
//nolint:gochecknoinits // the driver's function registry is not safe to write while another goroutine opens a connection, so registration has to finish before any connection can exist
func init() {
	// The error is intentionally dropped: it is stored and returned by
	// RegisterFunctions, which callers already invoke.
	_ = RegisterFunctions() //nolint:errcheck // reported by RegisterFunctions instead
}

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
		"reverse":         {1, fnReverse},
		"find_in_set":     {2, fnFindInSet},
		"field":           {-1, fnField},
		"elt":             {-1, fnElt},
		"monthname":       {1, fnMonthName},
		"dayname":         {1, fnDayName},
		"last_day":        {1, fnLastDay},
		"from_unixtime":   {-1, fnFromUnixtime},

		// Shared by every dialect; SQLite spells them min/max with several
		// arguments, which collides with the aggregate forms.
		"least":    {-1, fnLeast},
		"greatest": {-1, fnGreatest},

		// Cast helpers. Each dialect's rewrite pass routes CAST through its own
		// helper so the conversion follows that dialect's rules rather than
		// SQLite's affinity; see cast.go.
		"mysql_cast":          {2, dialectCast(MySQL, false)},
		"mysql_divide":        {2, divideFloat(false)},
		"mysql_bit_xor":       {2, fnBitXor},
		"interval_add":        {3, fnDateIntervalAdd},
		"interval_text_add":   {3, fnIntervalTextAdd},
		"date_trunc_part":     {2, fnDateTruncPart},
		"mysql_hex":           {1, fnMySQLHex},
		"mysql_unhex":         {1, fnMySQLUnhex},
		"like_sensitive":      {2, likeCompare(true)},
		"like_insensitive":    {2, likeCompare(false)},
		"similar_to":          {2, fnSimilarTo},
		"mysql_ord":           {1, fnMySQLOrd},
		"json_unquote":        {1, fnJSONUnquote},
		"overlay":             {-1, fnOverlay},
		"strict_concat":       {-1, fnStrictConcat},
		"postgresql_cast":     {2, dialectCast(PostgreSQL, false)},
		"googlesql_cast":      {2, dialectCast(GoogleSQL, false)},
		"googlesql_divide":    {2, divideFloat(true)},
		"googlesql_safe_cast": {2, dialectCast(GoogleSQL, true)},

		// PostgreSQL helpers.
		"to_char":    {2, fnToChar},
		"to_date":    {2, fnToDate},
		"date_trunc": {2, fnDateTrunc},
		"split_part": {3, fnSplitPart},
		"initcap":    {1, fnInitcap},
		// SQLite's own upper() and lower() fold ASCII alone, which is not what
		// any of these dialects does; their calls are rewritten onto these.
		"unicode_upper":  {1, fnUnicodeUpper},
		"unicode_lower":  {1, fnUnicodeLower},
		"strpos":         {2, fnStrpos},
		"left":           {2, fnLeft},
		"right":          {2, fnRight},
		"regexp_replace": {-1, fnRegexpReplace},
		"md5":            {1, fnMD5},
		"ascii":          {1, fnASCII},
		"chr":            {1, fnChr},
		"translate":      {3, fnTranslate},

		// GoogleSQL helpers.
		"safe_divide":       {2, fnSafeDivide},
		"starts_with":       {2, fnStartsWith},
		"ends_with":         {2, fnEndsWith},
		"regexp_contains":   {2, fnRegexpContains},
		"regexp_extract":    {2, fnRegexpExtract},
		"date_diff":         {3, fnDateDiff3},
		"timestamp_diff":    {3, fnDateDiff3},
		"format_date":       {2, fnFormatDate},
		"format_datetime":   {2, fnFormatDate},
		"format_timestamp":  {2, fnFormatDate},
		"parse_date":        {2, fnParseDate},
		"parse_datetime":    {2, fnParseTimestamp},
		"parse_timestamp":   {2, fnParseTimestamp},
		"unix_seconds":      {1, unixScale(1)},
		"unix_millis":       {1, unixScale(1000)},
		"unix_micros":       {1, unixScale(1000000)},
		"timestamp_seconds": {1, fromUnixScale(1)},
		"timestamp_millis":  {1, fromUnixScale(1000)},
		"timestamp_micros":  {1, fromUnixScale(1000000)},
		"to_hex":            {1, fnToHex},
		"is_nan":            {1, fnIsNaN},
		"safe_add":          {2, safeArith(safeAddInt, func(a, b float64) float64 { return a + b })},
		"safe_subtract":     {2, safeArith(safeSubInt, func(a, b float64) float64 { return a - b })},
		"safe_multiply":     {2, safeArith(safeMulInt, func(a, b float64) float64 { return a * b })},
		"safe_negate":       {1, fnSafeNegate},
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
		// UNIX_TIMESTAMP() with no argument reads the clock, so the whole
		// function has to be registered as non-deterministic even though the
		// one-argument form is pure.
		"unix_timestamp": {-1, fnUnixTimestamp},
		"generate_uuid":  {0, fnGenerateUUID},
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
	return time.Now().Format(layoutDateOnly), nil
}

func fnCurtime(_ []driver.Value) (driver.Value, error) {
	return time.Now().Format(layoutTimeOnly), nil
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
	case "date":
		// GoogleSQL allows the date and time parts of a timestamp to be
		// extracted, not just its numeric fields.
		return tm.Format(layoutDateOnly), nil
	case "time":
		return tm.Format(layoutTimeOnly), nil
	case "datetime":
		return tm.Format(layoutDateTime), nil
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
		if args[2] == nil {
			// MySQL answers NULL when any argument is NULL, which the other two
			// already do by way of toString.
			return nil, nil
		}
		pos, ok := toInt(args[2])
		if !ok || pos < 1 {
			return int64(0), nil
		}
		// The start is a character position too, which is what makes it usable
		// with the position this returns.
		start = int(pos) - 1
	}
	return int64(characterIndex(str, substr, start)), nil
}

func fnLpad(args []driver.Value) (driver.Value, error) { return pad(args, true) }
func fnRpad(args []driver.Value) (driver.Value, error) { return pad(args, false) }

// pad implements LPAD and RPAD. The length is a count of characters in all
// three dialects, so the arithmetic is over runes: measuring in bytes cut a
// multibyte character in half and returned bytes that are not UTF-8.
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
	runes := []rune(s)
	if len(runes) >= length {
		return string(runes[:length]), nil
	}
	if padStr == "" {
		return s, nil
	}
	padRunes := []rune(padStr)
	filler := make([]rune, 0, length-len(runes))
	for len(filler) < length-len(runes) {
		filler = append(filler, padRunes...)
	}
	filler = filler[:length-len(runes)]
	if left {
		return string(filler) + s, nil
	}
	return s + string(filler), nil
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

// fnLeast implements LEAST(a, b, ...) and fnGreatest GREATEST(a, b, ...).
// Values are compared numerically when every argument parses as a number and
// lexicographically otherwise, matching how the source dialects coerce a mixed
// list. A NULL argument makes the whole call NULL, which is MySQL and GoogleSQL
// behavior; PostgreSQL instead skips NULLs, a difference callers should know
// about.
func fnLeast(args []driver.Value) (driver.Value, error) { return extremum(args, true) }

func fnGreatest(args []driver.Value) (driver.Value, error) { return extremum(args, false) }

func extremum(args []driver.Value, wantSmaller bool) (driver.Value, error) {
	if len(args) == 0 {
		return nil, errors.New("dialect: LEAST/GREATEST expects at least one argument")
	}
	strs := make([]string, len(args))
	nums := make([]float64, len(args))
	allNumeric := true
	for i, a := range args {
		s, ok := toString(a)
		if !ok {
			return nil, nil
		}
		strs[i] = s
		if f, isNum := toFloat(a); isNum {
			nums[i] = f
		} else {
			allNumeric = false
		}
	}
	best := 0
	for i := 1; i < len(args); i++ {
		var smaller bool
		if allNumeric {
			smaller = nums[i] < nums[best]
		} else {
			smaller = strs[i] < strs[best]
		}
		if smaller == wantSmaller {
			best = i
		}
	}
	return args[best], nil
}

// fnReverse implements MySQL REVERSE, reversing runes rather than bytes so
// multi-byte text survives.
func fnReverse(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
}

// fnFindInSet implements MySQL FIND_IN_SET(needle, "a,b,c"): the 1-based
// position of needle in the comma-separated list, or 0 when it is absent.
func fnFindInSet(args []driver.Value) (driver.Value, error) {
	needle, ok1 := toString(args[0])
	set, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	for i, part := range strings.Split(set, ",") {
		if part == needle {
			return int64(i + 1), nil
		}
	}
	return int64(0), nil
}

// fnField implements MySQL FIELD(x, a, b, ...): the 1-based position of the
// first argument that equals x, or 0 when none does.
func fnField(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("dialect: FIELD expects at least 2 arguments, got %d", len(args))
	}
	needle, ok := toString(args[0])
	if !ok {
		return int64(0), nil
	}
	for i, a := range args[1:] {
		if s, ok := toString(a); ok && s == needle {
			return int64(i + 1), nil
		}
	}
	return int64(0), nil
}

// fnElt implements MySQL ELT(n, a, b, ...): the nth argument, or NULL when n is
// out of range.
func fnElt(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("dialect: ELT expects at least 2 arguments, got %d", len(args))
	}
	n, ok := toInt(args[0])
	if !ok || n < 1 || n > int64(len(args)-1) {
		return nil, nil
	}
	return args[n], nil
}

// fnMonthName and fnDayName implement the MySQL MONTHNAME/DAYNAME helpers.
func fnMonthName(args []driver.Value) (driver.Value, error) {
	return namedTimePart(args[0], layoutMonthLong)
}

func fnDayName(args []driver.Value) (driver.Value, error) {
	return namedTimePart(args[0], layoutWeekdayLong)
}

func namedTimePart(v driver.Value, layout string) (driver.Value, error) {
	tm, ok := toStringTime(v)
	if !ok {
		return nil, nil
	}
	return tm.Format(layout), nil
}

// fnLastDay implements MySQL LAST_DAY(date): the last day of that month. It is
// computed as "the day before the first of the next month" so leap years need
// no special case.
func fnLastDay(args []driver.Value) (driver.Value, error) {
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	firstOfNext := time.Date(tm.Year(), tm.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
	return firstOfNext.AddDate(0, 0, -1).Format(layoutDateOnly), nil
}

// fnUnixTimestamp implements MySQL UNIX_TIMESTAMP([date]): the current epoch
// second with no argument, or the epoch second of the given datetime. Values are
// read as UTC, as everywhere else in this package.
func fnUnixTimestamp(args []driver.Value) (driver.Value, error) {
	if len(args) == 0 {
		return time.Now().Unix(), nil
	}
	if len(args) > 1 {
		return nil, fmt.Errorf("dialect: UNIX_TIMESTAMP expects 0 or 1 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	return tm.Unix(), nil
}

// fnFromUnixtime implements MySQL FROM_UNIXTIME(seconds[, format]), the inverse
// of UNIX_TIMESTAMP.
func fnFromUnixtime(args []driver.Value) (driver.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("dialect: FROM_UNIXTIME expects 1 or 2 arguments, got %d", len(args))
	}
	sec, ok := toInt(args[0])
	if !ok {
		return nil, nil
	}
	tm := time.Unix(sec, 0).UTC()
	if len(args) == 1 {
		return tm.Format(layoutDateTime), nil
	}
	return fnDateFormat([]driver.Value{tm.Format(layoutDateTime), args[1]})
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

// fnToChar implements PostgreSQL TO_CHAR(value, format) for date/time values and
// for numbers. The two are told apart by the template: a numeric one is built
// from digit positions, which no date template contains.
func fnToChar(args []driver.Value) (driver.Value, error) {
	format, ok := toString(args[1])
	if !ok {
		return nil, nil
	}
	if isNumericTemplate(format) {
		return numericToChar(args[0], format)
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	return tm.Format(toCharLayout(format)), nil
}

// isNumericTemplate reports whether a TO_CHAR template describes a number.
// "9" and "0" are digit positions and appear in no date template.
func isNumericTemplate(format string) bool {
	return strings.ContainsAny(format, "90")
}

// numericToChar formats a number against a PostgreSQL numeric template. It
// supports the common pieces: "9" and "0" digit positions, "." for the decimal
// point, "," for a group separator, and the leading space PostgreSQL reserves
// for the sign. Anything else is passed through as a literal.
func numericToChar(v driver.Value, format string) (driver.Value, error) {
	value, ok := toFloat(v)
	if !ok {
		return nil, nil
	}
	intPart, fracPart, hasPoint := splitTemplate(format)
	decimals := strings.Count(fracPart, "9") + strings.Count(fracPart, "0")
	digits := strconv.FormatFloat(math.Abs(value), 'f', decimals, 64)

	whole, frac, _ := strings.Cut(digits, ".")
	if strings.Contains(intPart, ",") {
		whole = groupThousands(whole)
	}
	if value < 0 {
		whole = "-" + whole
	}
	// PostgreSQL pads to the template width and reserves one more column for the
	// sign, so a positive number keeps a leading space.
	width := strings.Count(intPart, "9") + strings.Count(intPart, "0") + strings.Count(intPart, ",") + 1
	for len([]rune(whole)) < width {
		whole = " " + whole
	}
	if hasPoint && decimals > 0 {
		whole += "." + frac
	}
	return whole, nil
}

// splitTemplate divides a numeric template at its decimal point.
func splitTemplate(format string) (intPart, fracPart string, hasPoint bool) {
	if before, after, found := strings.Cut(format, "."); found {
		return before, after, true
	}
	return format, "", false
}

// groupThousands inserts a comma every three digits from the right.
func groupThousands(digits string) string {
	var b strings.Builder
	for i, r := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String()
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
	return tm.Format(layoutDateOnly), nil
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

// isAlnumRune reports whether r is part of a word, which is what decides where
// INITCAP capitalizes. A letter is a letter whatever its script: testing the
// ASCII range read an accented letter as a separator, so "école" came back
// "éCole".
// fnUnicodeUpper and fnUnicodeLower fold case over the whole of Unicode, which
// is what MySQL, PostgreSQL and GoogleSQL do and what SQLite's own upper() and
// lower() do not: theirs stop at ASCII, so UPPER('école') came back 'éCOLE'.
func fnUnicodeUpper(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	return strings.ToUpper(s), nil
}

func fnUnicodeLower(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	return strings.ToLower(s), nil
}

func isAlnumRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// fnStrpos implements PostgreSQL STRPOS(string, substring): 1-based index or 0.
func fnStrpos(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	sub, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return int64(characterIndex(s, sub, 0)), nil
}

// characterIndex is the 1-based position of sub in s counted in characters, or
// 0 when it is not there. from is a character offset to start at, which is what
// LOCATE's third argument means. Every dialect here counts a position in
// characters; strings.Index answers in bytes, and returning that answered a
// number that indexes nothing in text outside ASCII.
func characterIndex(s, sub string, from int) int {
	runes := []rune(s)
	if from < 0 || from > len(runes) {
		return 0
	}
	tail := string(runes[from:])
	idx := strings.Index(tail, sub)
	if idx < 0 {
		return 0
	}
	return from + utf8.RuneCountInString(tail[:idx]) + 1
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

// fnRegexpReplace implements REGEXP_REPLACE(source, pattern, replacement
// [, flags]). PostgreSQL back-references (\1) are translated to Go's ${1}
// expansion form.
//
// The three-argument form replaces every match, which is GoogleSQL semantics.
// PostgreSQL's three-argument form replaces only the first match and needs the
// "g" flag for the rest; that difference is left in place because the same
// function is shared by all dialects and replace-all is the behavior callers
// expect. With an explicit flags argument the flags win: "g" replaces every
// match and its absence replaces only the first, and "i" matches case
// insensitively.
func fnRegexpReplace(args []driver.Value) (driver.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return nil, fmt.Errorf("dialect: REGEXP_REPLACE expects 3 or 4 arguments, got %d", len(args))
	}
	src, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	repl, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	global := true
	if len(args) == 4 {
		flags, ok := toString(args[3])
		if !ok {
			return nil, nil
		}
		global = strings.Contains(flags, "g")
		if strings.Contains(flags, "i") {
			pattern = "(?i)" + pattern
		}
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return nil, err
	}
	expansion := pgReplacement(repl)
	if global {
		return re.ReplaceAllString(src, expansion), nil
	}
	// Expand against the source rather than the matched text on its own: a
	// boundary-dependent pattern such as `\Bb` matches inside "ab" but not in
	// the isolated "b", which would leave ExpandString without submatch indices.
	loc := re.FindStringSubmatchIndex(src)
	if loc == nil {
		return src, nil
	}
	out := re.ExpandString([]byte(src[:loc[0]]), expansion, src, loc)
	return string(out) + src[loc[1]:], nil
}

// fnMD5 implements PostgreSQL MD5(text). MD5 is used here as a content
// fingerprint compatible with the source dialect, never for security.
func fnMD5(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	sum := md5.Sum([]byte(s)) //nolint:gosec // required for PostgreSQL MD5() compatibility, not a security control
	return hex.EncodeToString(sum[:]), nil
}

// fnASCII implements ASCII(text): the code point of the first character, or 0
// for an empty string.
func fnASCII(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	for _, r := range s {
		return int64(r), nil
	}
	return int64(0), nil
}

// fnChr implements CHR(code): the character for a code point.
func fnChr(args []driver.Value) (driver.Value, error) {
	n, ok := toInt(args[0])
	if !ok {
		return nil, nil
	}
	if n < 0 || n > utf8.MaxRune {
		return nil, fmt.Errorf("dialect: CHR: code point %d is out of range", n)
	}
	return string(rune(n)), nil
}

// fnTranslate implements PostgreSQL TRANSLATE(string, from, to): each character
// of from is replaced by the character at the same position in to, and a
// character whose position has no counterpart in to is dropped.
func fnTranslate(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	from, ok2 := toString(args[1])
	to, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	fromRunes := []rune(from)
	toRunes := []rune(to)
	var b strings.Builder
	for _, r := range s {
		idx := -1
		for i, f := range fromRunes {
			if f == r {
				idx = i
				break
			}
		}
		switch {
		case idx < 0:
			b.WriteRune(r)
		case idx < len(toRunes):
			b.WriteRune(toRunes[idx])
		}
	}
	return b.String(), nil
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

// strftimeToGoLayout maps the strftime-style specifiers GoogleSQL uses in
// FORMAT_DATE and PARSE_DATE to Go reference-time layout fragments. They differ
// from the MySQL DATE_FORMAT set: %M is the minute here but the month name
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
}

// strftimeLayout converts a GoogleSQL format string into a Go layout. An
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

// fnFormatDate implements GoogleSQL FORMAT_DATE/FORMAT_DATETIME/
// FORMAT_TIMESTAMP(format, value). The format comes first, the reverse of MySQL
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
	return tm.Format(strftimeLayout(format)), nil
}

// fnParseDate implements GoogleSQL PARSE_DATE(format, text) and fnParseTimestamp
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

// fnToHex implements GoogleSQL TO_HEX(bytes): the lowercase hexadecimal form of
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

// fnIsNaN implements GoogleSQL IS_NAN(x).
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

// safeArith builds the GoogleSQL SAFE_ADD/SAFE_SUBTRACT/SAFE_MULTIPLY family:
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

// fnSafeNegate implements GoogleSQL SAFE_NEGATE(x).
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
