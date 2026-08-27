package dialect

import (
	"crypto/md5" //nolint:gosec // MD5 backs PostgreSQL's MD5() function, not a security control
	"crypto/rand"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
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

// layoutDateTimeFraction is layoutDateTime with the fractional seconds a value
// carries. The nines drop the trailing zeros, so a value with none is written
// exactly as layoutDateTime writes it.
const layoutDateTimeFraction = "2006-01-02 15:04:05.999999"

// formatDateTimeValue writes a datetime that came from the caller's own data,
// keeping whatever fraction of a second it carried. A clock reading is written
// with layoutDateTime instead, since a fraction there is this package's own
// invention rather than something the caller wrote.
//
// Formatting every value at second resolution dropped a fraction that was in
// the input: TIME('2024-03-05 01:02:03.123456') answered 01:02:03 and
// DATE_ADD on the same value moved the date and lost the microseconds with it.
func formatDateTimeValue(tm time.Time) string {
	if tm.Nanosecond() == 0 {
		return tm.Format(layoutDateTime)
	}
	return tm.Format(layoutDateTimeFraction)
}

// formatTimeOfDayValue is formatDateTimeValue for a value written as a time of
// day alone.
func formatTimeOfDayValue(tm time.Time) string {
	if tm.Nanosecond() == 0 {
		return tm.Format(layoutTimeOnly)
	}
	return tm.Format(layoutTimeOnly + ".999999")
}

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
	unitISOWeek   = "isoweek"
	unitISOYear   = "isoyear"
)

// scalarFn adapts a simpler signature (no context) to the driver's scalar
// function shape.
type scalarFn func(args []driver.Value) (driver.Value, error)

// scalarSpec is a scalar function's argument count and implementation, in the
// shape the driver's registry takes them. An nArg of -1 accepts any count.
type scalarSpec struct {
	nArg int32
	fn   scalarFn
}

func registerAll() error {
	// deterministic functions: same output for the same inputs.
	det := map[string]scalarSpec{
		"regexp":      {2, fnRegexp}, // REGEXP(pattern, s); also the "x REGEXP y" operator
		"if":          {3, fnIf},     // IF(cond, a, b)
		"date_format": {2, fnDateFormat},
		"str_to_date": {2, fnStrToDate},
		"datediff":    {2, fnDateDiff},
		// TIMESTAMPDIFF and TIMEDIFF need MySQL's own helpers: the former
		// counts complete units where BigQuery's date_diff counts boundaries,
		// and the latter would otherwise fall through to SQLite's timediff(),
		// which answers in SQLite's interval spelling.
		"mysql_date_diff": {3, fnMySQLDateDiff},
		"mysql_timediff":  {2, fnMySQLTimeDiff},
		"date_part":       {2, fnDatePart},
		// EXTRACT names each dialect's own helper, because the dialects
		// disagree on WEEK: PostgreSQL's is the ISO week, MySQL's and
		// BigQuery's begin on Sunday, and BigQuery alone has ISOWEEK/ISOYEAR.
		"mysql_date_part":     {2, fnMySQLDatePart},
		"googlesql_date_part": {2, fnGoogleSQLDatePart},
		unitYear:              {1, unaryDatePart(unitYear)},
		unitMonth:             {1, unaryDatePart(unitMonth)},
		unitDay:               {1, unaryDatePart(unitDay)},
		unitHour:              {1, unaryDatePart(unitHour)},
		unitMinute:            {1, unaryDatePart(unitMinute)},
		unitSecond:            {1, unaryDatePart(unitSecond)},
		unitDayOfWeek:         {1, unaryDatePart(unitDayOfWeek)},
		unitDayOfYear:         {1, unaryDatePart(unitDayOfYear)},
		unitWeekday:           {1, unaryDatePart(unitWeekday)},
		unitQuarter:           {1, unaryDatePart(unitQuarter)},

		// The week functions carry MySQL's name because MySQL's week is not
		// everyone's: it starts on Sunday or Monday by mode, where PostgreSQL
		// has only the ISO week and BigQuery has both under separate names.
		"mysql_week":       {-1, fnMySQLWeek},
		"mysql_weekofyear": {1, fnMySQLWeekOfYear},
		"mysql_yearweek":   {-1, fnMySQLYearWeek},
		"locate":           {-1, fnLocate},
		"lpad":             {-1, fnLpad},
		"rpad":             {-1, fnRpad},

		// LPAD and RPAD answer a negative length and an empty pad differently
		// per dialect, so each dialect's rewrite names its own helper; see
		// padRules.
		"mysql_lpad":      {-1, padFor(mysqlPadRules, true)},
		"mysql_rpad":      {-1, padFor(mysqlPadRules, false)},
		"postgresql_lpad": {-1, padFor(postgresqlPadRules, true)},
		"postgresql_rpad": {-1, padFor(postgresqlPadRules, false)},
		"substring_index": {3, fnSubstringIndex},

		// SUBSTRING at position 0 and at a negative position is answered
		// differently by each dialect, and by SQLite's own substr(), so each
		// dialect's rewrite names its own helper.
		"mysql_substr":       {-1, fnMySQLSubstr},
		"postgresql_substr":  {-1, fnPostgreSQLSubstr},
		"googlesql_substr":   {-1, fnGoogleSQLSubstr},
		"dialect_round":      {2, fnDialectRound},
		"dialect_round_even": {-1, fnDialectRoundEven},
		"repeat":             {2, fnRepeat},
		"googlesql_repeat":   {2, fnGoogleSQLRepeat},
		"googlesql_lpad":     {-1, padFor(googlesqlPadRules, true)},
		"googlesql_rpad":     {-1, padFor(googlesqlPadRules, false)},
		"space":              {1, fnSpace},
		"truncate":           {2, fnTruncate},
		"reverse":            {1, fnReverse},
		"find_in_set":        {2, fnFindInSet},
		"field":              {-1, fnField},
		"elt":                {-1, fnElt},
		"monthname":          {1, fnMonthName},
		"dayname":            {1, fnDayName},
		"last_day":           {1, fnLastDay},
		"from_unixtime":      {-1, fnFromUnixtime},

		// Shared by every dialect; SQLite spells them min/max with several
		// arguments, which collides with the aggregate forms.
		"least":    {-1, fnLeast},
		"greatest": {-1, fnGreatest},

		// PostgreSQL's pair skips NULL arguments where the two above answer
		// NULL for the whole call.
		"postgresql_least":    {-1, fnPostgresLeast},
		"postgresql_greatest": {-1, fnPostgresGreatest},

		// Cast helpers. Each dialect's rewrite pass routes CAST through its own
		// helper so the conversion follows that dialect's rules rather than
		// SQLite's affinity; see cast.go.
		"mysql_cast":           {2, dialectCast(MySQL, false)},
		"mysql_format":         {2, fnMySQLFormat},
		"mysql_left":           {2, fnMySQLLeft},
		"mysql_right":          {2, fnMySQLRight},
		"mysql_regexp_replace": {-1, fnMySQLRegexpReplace},
		"mysql_divide":         {2, divideFloat(false)},
		"mysql_mod":            {2, moduloDialect(false)},
		"mysql_bit_xor":        {2, fnBitXor},
		"interval_add":         {3, fnDateIntervalAdd},
		"interval_text_add":    {3, fnIntervalTextAdd},
		"date_trunc_part":      {2, fnDateTruncPart},
		"mysql_hex":            {1, fnMySQLHex},
		"mysql_unhex":          {1, fnMySQLUnhex},
		"mysql_soundex":        {1, fnMySQLSoundex},
		"googlesql_soundex":    {1, fnGoogleSQLSoundex},
		"dialect_replace":      {3, fnDialectReplace},
		"mysql_char":           {-1, fnMySQLChar},
		"mysql_quote":          {1, fnMySQLQuote},
		"mysql_ascii":          {1, fnMySQLASCII},
		"mysql_shift_left":     {2, mysqlShift(true)},
		"mysql_shift_right":    {2, mysqlShift(false)},
		// MySQL matches a regular expression under the collation of its
		// operands, and its default collation folds case; the shared regexp()
		// is right for PostgreSQL and BigQuery, which do not.
		"mysql_regexp":              {2, fnMySQLRegexp},
		"like_sensitive":            {2, likeCompare(true)},
		"like_insensitive":          {2, likeCompare(false)},
		"similar_to":                {2, fnSimilarTo},
		"mysql_ord":                 {1, fnMySQLOrd},
		"json_unquote":              {1, fnJSONUnquote},
		"overlay":                   {-1, fnOverlay},
		"strict_concat":             {-1, fnStrictConcat},
		"div":                       {2, integerDivide},
		"trunc_scale":               {2, truncateScale},
		"width_bucket":              {4, widthBucket},
		"postgresql_cast":           {2, dialectCast(PostgreSQL, false)},
		"postgresql_to_hex":         {1, fnPostgresToHex},
		"postgresql_regexp_replace": {-1, fnPostgresRegexpReplace},
		"postgresql_divide":         {2, divideSQLite},
		"postgresql_mod":            {2, moduloDialect(true)},
		fnNamePostgresDateAdd:       {2, fnPostgresDateAdd},
		"postgresql_date_diff":      {2, fnPostgresDateDiff},
		"googlesql_cast":            {2, dialectCast(GoogleSQL, false)},
		"googlesql_divide":          {2, divideFloat(true)},
		"googlesql_mod":             {2, moduloDialect(true)},
		"googlesql_safe_cast":       {2, dialectCast(GoogleSQL, true)},

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
		"datetime_diff":     {3, fnDateDiff3},
		"time_diff":         {3, fnTimeDiff},
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
		"googlesql_format":  {-1, fnGoogleSQLFormat},
		"googlesql_left":    {2, fnGoogleSQLLeft},
		"googlesql_right":   {2, fnGoogleSQLRight},
		"is_nan":            {1, fnIsNaN},
		"safe_add":          {2, safeArith(safeAddInt, func(a, b float64) float64 { return a + b })},
		"safe_subtract":     {2, safeArith(safeSubInt, func(a, b float64) float64 { return a - b })},
		"safe_multiply":     {2, safeArith(safeMulInt, func(a, b float64) float64 { return a * b })},
		"safe_negate":       {1, fnSafeNegate},
		// The clock helpers every engine fixes at the start of the statement.
		// Registering them as deterministic is what fixes them: SQLite computes
		// a deterministic function whose arguments are constant once per
		// execution and reuses the answer for every row, and computes it again
		// the next time the statement runs. UNIX_TIMESTAMP is variadic and only
		// its no-argument form reads the clock; the form that takes a datetime
		// is pure, so both belong here.
		"now":                     {0, fnNow},
		"curdate":                 {0, fnCurdate},
		"curtime":                 {0, fnCurtime},
		"unix_timestamp":          {-1, fnUnixTimestamp},
		"mysql_time_of_day":       {1, fnMySQLTimeOfDay},
		"mysql_interval_compound": {4, fnMySQLIntervalCompound},
		"current_datetime":        {-1, fnCurrentDatetime},
	}
	// The MySQL-only helpers live in their own file, because there are enough of
	// them that listing them here would bury the ones every dialect shares.
	maps.Copy(det, mysqlScalarFunctions())
	maps.Copy(det, mysqlTimeFunctions())
	maps.Copy(det, postgresqlScalarFunctions())
	maps.Copy(det, googlesqlScalarFunctions())
	for name, spec := range det {
		if err := sqlite.RegisterDeterministicScalarFunction(name, spec.nArg, wrapScalar(spec.fn)); err != nil {
			return fmt.Errorf("dialect: register %s: %w", name, err)
		}
	}
	registeredFunctions = det

	nondet := nonDeterministicFunctions()
	// safe_call runs one of the helpers above and swallows its error, which is
	// what BigQuery's SAFE. prefix asks for. It is registered here rather than
	// in that table because it has to see the finished one, and it stays
	// non-deterministic because the helper it is given may be clock_timestamp.
	nondet["safe_call"] = scalarSpec{nArg: -1, fn: fnSafeCall}
	for name, spec := range nondet {
		if err := sqlite.RegisterScalarFunction(name, spec.nArg, wrapScalar(spec.fn)); err != nil {
			return fmt.Errorf("dialect: register %s: %w", name, err)
		}
	}
	maps.Copy(registeredFunctions, nondet)
	return nil
}

// nonDeterministicFunctions is every helper SQLite must call again for each row.
// Two kinds belong here and nothing else does: the ones that are meant to give
// a different answer every time they are asked -- a random number, a fresh UUID
// -- and PostgreSQL's changing clock, which is the whole of what separates
// clock_timestamp and timeofday from now and statement_timestamp. Everything
// that reads the clock once at the start of the statement is registered as
// deterministic instead, which is what makes one statement see one reading.
func nonDeterministicFunctions() map[string]scalarSpec {
	nondet := map[string]scalarSpec{
		"rand":          {0, fnRand},
		"generate_uuid": {0, fnGenerateUUID},
	}
	// GoogleSQL has nothing to add: BigQuery fixes CURRENT_DATETIME at the
	// start of the statement, like the rest of its CURRENT_ family.
	maps.Copy(nondet, postgresqlNonDeterministicFunctions())
	return nondet
}

// registeredFunctions is every scalar function this package computes itself,
// which is what the SAFE. prefix can promise a NULL for: a function SQLite
// computes is out of reach from here. It is written once, during registration,
// before any connection can exist.
var registeredFunctions map[string]scalarSpec //nolint:gochecknoglobals // the table registerAll built, read by safe_call

// isRegisteredFunction reports whether this package computes name itself.
func isRegisteredFunction(name string) bool {
	_, ok := registeredFunctions[name]
	return ok
}

// fnSafeCall runs the helper its first argument names over the rest, answering
// NULL where that helper would have raised. It is BigQuery's SAFE. prefix,
// which turns an error into a NULL for one call.
func fnSafeCall(args []driver.Value) (driver.Value, error) {
	if len(args) == 0 {
		return nil, errors.New("dialect: safe_call expects a function name")
	}
	name, ok := toString(args[0])
	if !ok {
		return nil, errors.New("dialect: safe_call expects a function name")
	}
	spec, found := registeredFunctions[name]
	if !found {
		return nil, fmt.Errorf("dialect: safe_call: no such function %q", name)
	}
	rest := args[1:]
	if spec.nArg >= 0 && len(rest) != int(spec.nArg) {
		// An arity the function does not have is the caller's mistake rather
		// than a value it could not compute, so it is reported.
		return nil, fmt.Errorf("dialect: %s expects %d arguments, got %d", name, spec.nArg, len(rest))
	}
	out, err := spec.fn(rest)
	if err != nil {
		return nil, nil //nolint:nilerr,nilnil // swallowing the error is what SAFE. asks for
	}
	return out, nil
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

// toCount converts a driver.Value to the whole number a dialect reads it as
// where the argument counts things rather than being a numeric result: how many
// times to repeat, how many spaces, which element of a list. MySQL rounds such a
// value half away from zero, so REPEAT('ab', 2.7) repeats three times, where
// truncating toward zero left it one short.
//
// A string takes the number its leading run spells and truncates it, which is
// the other rule MySQL has here: REPEAT('ab', '2.5') repeats twice where
// REPEAT('ab', 2.5) repeats three times, and a string spelling no number counts
// zero. Requiring the whole string to parse as an integer made every count read
// from a text column -- which is what a cell loaded from a CSV file is -- answer
// NULL for the whole call.
func toCount(v driver.Value) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return int64(math.Round(x)), true
	case string:
		return int64(leadingNumber(x)), true
	case []byte:
		return int64(leadingNumber(string(x))), true
	}
	return toInt(v)
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

// fnMySQLRegexp implements the MySQL "subject REGEXP pattern" operator. It is
// fnRegexp with MySQL's default collation applied, which folds case over the
// whole of Unicode rather than matching the pattern as written.
func fnMySQLRegexp(args []driver.Value) (driver.Value, error) {
	pattern, ok1 := toString(args[0])
	subject, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	folded, err := mysqlRegexpPattern(pattern, "")
	if err != nil {
		return nil, err
	}
	re, err := compileRegexp(folded)
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
		return leadingNumber(x) != 0
	case []byte:
		return leadingNumber(string(x)) != 0
	default:
		return true
	}
}

// leadingNumber reads a string the way MySQL and SQLite both read one in a
// numeric context: skip leading space, take the longest prefix that spells a
// number, and answer zero when there is none. Requiring the whole string to
// parse and falling back to "not empty" made every non-numeric string true, so
// IF('abc', a, b) took the a branch where both engines take b.
func leadingNumber(s string) float64 {
	t := strings.TrimLeft(s, " \t\n\v\f\r")
	i := 0
	if i < len(t) && (t[i] == '+' || t[i] == '-') {
		i++
	}
	digits := false
	for i < len(t) && t[i] >= '0' && t[i] <= '9' {
		i++
		digits = true
	}
	if i < len(t) && t[i] == '.' {
		i++
		for i < len(t) && t[i] >= '0' && t[i] <= '9' {
			i++
			digits = true
		}
	}
	if !digits {
		return 0
	}
	end := i
	// An exponent counts only when digits follow it, so "1e" is the number 1
	// with a letter after it rather than a malformed number.
	if i < len(t) && (t[i] == 'e' || t[i] == 'E') {
		j := i + 1
		if j < len(t) && (t[j] == '+' || t[j] == '-') {
			j++
		}
		for j < len(t) && t[j] >= '0' && t[j] <= '9' {
			j++
			end = j
		}
	}
	f, err := strconv.ParseFloat(t[:end], 64)
	if err != nil {
		// A prefix this built is a number Go can read, save for one too large
		// for a float64, which comes back as an infinity and is not zero.
		if errors.Is(err, strconv.ErrRange) {
			return f
		}
		return 0
	}
	return f
}

// statementClockWindow is how long one reading of the clock is reused.
//
// Registering the fixed clock as deterministic takes it off the row, but not
// quite onto the statement: SQLite folds each occurrence of the call once, so a
// query naming NOW twice reads the clock twice, microseconds apart, and the two
// answers differ whenever those microseconds straddle a second. Every engine
// this package translates gives one answer to every occurrence in a statement.
// Sharing a reading for a window wider than the gap between the occurrences and
// far narrower than the second these functions are formatted to is what closes
// that gap.
//
// A statement that begins within the window of the one before it therefore sees
// that statement's reading, which is at most a window old and never earlier
// than what an earlier statement was given.
const statementClockWindow = time.Millisecond

//nolint:gochecknoglobals // one reading, shared by every helper that must not move within a statement
var (
	clockMu   sync.Mutex
	lastClock time.Time
)

// clockUTC is the reading the fixed clock functions answer from. It is UTC
// because this package carries no time zone -- no column has one, no cast
// produces one, and a zone argument is refused -- so UTC is the reading that is
// the same on every machine, and it is the one SQLite's own CURRENT_TIMESTAMP
// answers.
func clockUTC() time.Time {
	clockMu.Lock()
	defer clockMu.Unlock()

	if now := time.Now(); now.Sub(lastClock) >= statementClockWindow {
		lastClock = now
	}
	return lastClock.UTC()
}

func fnNow(_ []driver.Value) (driver.Value, error) {
	return clockUTC().Format(layoutDateTime), nil
}

func fnCurdate(_ []driver.Value) (driver.Value, error) {
	return clockUTC().Format(layoutDateOnly), nil
}

func fnCurtime(_ []driver.Value) (driver.Value, error) {
	return clockUTC().Format(layoutTimeOnly), nil
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
	'l': "3",
	'r': "03:04:05 PM",
	'T': "15:04:05",
	'M': layoutMonthLong,
	'b': layoutMonthShort,
	'W': layoutWeekdayLong,
	'a': layoutWeekdayShort,
}

// mysqlParseLayout overrides mysqlToGoLayout for the specifiers whose parsing
// width differs from their formatting width. MySQL pads a number on output and
// reads one digit or two on input, while a Go layout element means both at once:
// "01" formats a month as two digits and refuses one, and "1" accepts either and
// formats without the padding. So the two directions need two maps, and only the
// numeric specifiers appear here.
var mysqlParseLayout = map[byte]string{
	'm': "1",
	'd': "2",
	'h': "3",
	'I': "3",
	'i': "4",
	's': "5",
	'S': "5",
	'r': "3:4:5 PM",
	'T': "15:4:5",
}

// mysqlLayoutFor is the Go layout fragment a MySQL specifier means, in the
// direction asked for.
func mysqlLayoutFor(spec byte, parsing bool) (string, bool) {
	if parsing {
		if l, found := mysqlParseLayout[spec]; found {
			return l, true
		}
	}
	l, found := mysqlToGoLayout[spec]
	return l, found
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

// dateFormatSpecial handles DATE_FORMAT specifiers that have no direct Go
// layout: the day of the year, the weekday index, the microseconds, the two
// unpadded hours, the day with its English suffix, and the four week numberings
// with the two years that go with them.
func dateFormatSpecial(tm time.Time, spec byte) (string, bool) {
	switch spec {
	case 'j':
		return fmt.Sprintf("%03d", tm.YearDay()), true
	case 'w':
		return strconv.Itoa(int(tm.Weekday())), true
	case 'f':
		return fmt.Sprintf("%06d", tm.Nanosecond()/1000), true
	case 'k':
		return strconv.Itoa(tm.Hour()), true
	case 'D':
		return strconv.Itoa(tm.Day()) + ordinalSuffix(tm.Day()), true
	case 'u':
		week, _ := mysqlWeek(tm, 1)
		return fmt.Sprintf("%02d", week), true
	case 'U':
		week, _ := mysqlWeek(tm, 0)
		return fmt.Sprintf("%02d", week), true
	case 'v':
		week, _ := mysqlWeek(tm, 3)
		return fmt.Sprintf("%02d", week), true
	case 'V':
		week, _ := mysqlWeek(tm, 2)
		return fmt.Sprintf("%02d", week), true
	case 'x':
		_, year := mysqlWeek(tm, 3)
		return strconv.Itoa(year), true
	case 'X':
		_, year := mysqlWeek(tm, 2)
		return strconv.Itoa(year), true
	case '%':
		return "%", true
	default:
		return "", false
	}
}

// ordinalSuffix is the English suffix DATE_FORMAT's %D puts after a day and
// TO_CHAR's TH after a number. The teens are the exception: 11, 12 and 13 take
// "th" where 1, 2 and 3 take "st", "nd" and "rd", and so does every hundred
// above them, which is why the test is on the last two digits rather than on
// the value.
func ordinalSuffix(n int) string {
	if n < 0 {
		n = -n
	}
	if n%100 >= 11 && n%100 <= 13 {
		return "th"
	}
	switch n % 10 {
	case 1:
		return "st"
	case 2:
		return "nd"
	case 3:
		return "rd"
	default:
		return "th"
	}
}

// weekModeYear is the MySQL week-mode flag that makes the count belong to a
// year rather than start over at week 0, which is the difference between WEEK
// and YEARWEEK at the turn of a year.
const weekModeYear = 2

// mysqlWeek is the week number of tm under one of MySQL's week modes, and the
// year that week belongs to.
//
// MySQL numbers weeks four ways and DATE_FORMAT reaches all four: %U is mode 0,
// %u mode 1, %V mode 2 and %v mode 3. Two things vary. A week may start on
// Sunday (modes 0 and 2) or on Monday (1 and 3), and week 1 may be the first
// week holding a day of the new year's first weekday (0 and 2) or the first
// week holding four or more days of the new year (1 and 3, the ISO rule).
// Modes 0 and 1 number from zero, so the first days of January can be week 0;
// modes 2 and 3 have no week 0 and lend those days to the previous year's last
// week, which is why %X and %x exist to say which year the number belongs to.
//
// This follows MySQL's own calculation rather than deriving one, because the
// years where the four disagree are exactly the ones a derivation gets wrong:
// 2024-12-31 is week 53 by %u and week 1 of 2025 by %v.
func mysqlWeek(tm time.Time, mode int) (week, year int) {
	// MySQL turns a mode into three flags, inverting the "four or more days"
	// rule for the Sunday-first modes.
	const (
		mondayFirst  = 1
		weekYear     = weekModeYear
		firstWeekday = 4
	)
	flags := mode & 7
	if flags&mondayFirst == 0 {
		flags ^= firstWeekday
	}

	year = tm.Year()
	firstOfYear := time.Date(year, time.January, 1, 0, 0, 0, 0, tm.Location())
	// weekday is how far the year's first day is into its own week, counted
	// from whichever day the mode starts a week on.
	weekday := weekdayIndex(firstOfYear, flags&mondayFirst != 0)
	// startsWeekOne says whether the week holding January 1 is week 1 already.
	startsWeekOne := func(weekday int) bool {
		if flags&firstWeekday != 0 {
			return weekday == 0
		}
		return weekday < 4
	}

	// borrowed says the count runs from the previous year's first day, which is
	// so for the modes that have no week 0 and becomes so for the modes that do
	// once a January day turns out to belong to the previous year's last week.
	borrowed := flags&weekYear != 0

	days := tm.YearDay() - 1
	if tm.Month() == time.January && tm.Day() <= 7-weekday {
		if !borrowed && !startsWeekOne(weekday) {
			return 0, year
		}
		borrowed = true
		year--
		inPreviousYear := daysInYear(year)
		days += inPreviousYear
		weekday = (weekday + 53*7 - inPreviousYear) % 7
	}

	if startsWeekOne(weekday) {
		days += weekday
	} else {
		days -= 7 - weekday
	}

	if borrowed && days >= 52*7 {
		// A 53rd week only exists when the following year's first week starts
		// late enough to hold this one; otherwise these days are week 1 of it.
		weekday = (weekday + daysInYear(year)) % 7
		if startsWeekOne(weekday) {
			return 1, year + 1
		}
	}
	return days/7 + 1, year
}

// weekdayIndex is how far into its week a day falls, counting from Monday or
// from Sunday.
func weekdayIndex(tm time.Time, mondayFirst bool) int {
	if mondayFirst {
		return (int(tm.Weekday()) + 6) % 7
	}
	return int(tm.Weekday())
}

// daysInYear is 366 in a leap year and 365 otherwise.
func daysInYear(year int) int {
	if (year%4 == 0 && year%100 != 0) || year%400 == 0 {
		return 366
	}
	return 365
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
	var sawYear, sawMonth, sawDay, sawWeekday, sawTime bool
	for i := 0; i < len(format); i++ {
		if format[i] == '%' && i+1 < len(format) {
			spec := format[i+1]
			if l, found := mysqlLayoutFor(spec, true); found {
				layout.WriteString(l)
				switch spec {
				case 'Y', 'y':
					sawYear = true
				case 'm', 'c', 'M', 'b':
					sawMonth = true
				case 'd', 'e':
					sawDay = true
				case 'W', 'a':
					sawWeekday = true
				default:
					sawTime = true
				}
			} else {
				layout.WriteByte(spec)
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
	// The shape follows the format: a DATE for date specifiers, a TIME for
	// time specifiers, a DATETIME for both. A format that names part of a date
	// without completing it is NULL, which is MySQL's answer under its default
	// sql_mode rather than a default-filled date.
	hasDate := sawYear || sawMonth || sawDay || sawWeekday
	if hasDate && (!sawYear || !sawMonth || !sawDay) {
		return nil, nil
	}
	switch {
	case hasDate && sawTime:
		return tm.Format(layoutDateTime), nil
	case hasDate:
		return tm.Format(layoutDateOnly), nil
	case sawTime:
		return tm.Format(layoutTimeOnly), nil
	default:
		return nil, nil
	}
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
	return dayNumber(da) - dayNumber(db), nil
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
	unit = strings.ToLower(strings.TrimSpace(unit))
	// PostgreSQL's second carries the fraction of a second with it, so
	// DATE_PART('second', '10:11:12.5') is 12.5. MySQL's SECOND() and
	// BigQuery's EXTRACT(SECOND) both answer the whole number, which is what
	// the shared helper below gives them.
	if unit == unitSecond {
		return secondsWithFraction(tm), nil
	}
	return datePartValue(unit, tm)
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
	case "dow":
		// PostgreSQL DOW: Sunday=0..Saturday=6, which is Go's own numbering.
		// This spelling is PostgreSQL's; dayofweek below is MySQL's and
		// GoogleSQL's, and the two number the week differently.
		return int64(tm.Weekday()), nil
	case "isodow":
		// PostgreSQL ISODOW: Monday=1..Sunday=7.
		return int64(weekdayIndex(tm, true)) + 1, nil
	case unitDayOfWeek:
		// MySQL DAYOFWEEK: Sunday=1..Saturday=7.
		return int64(tm.Weekday()) + 1, nil
	case unitWeekday:
		// MySQL WEEKDAY: Monday=0..Sunday=6.
		return int64(weekdayIndex(tm, true)), nil
	case "doy", unitDayOfYear:
		return int64(tm.YearDay()), nil
	case unitQuarter:
		return int64((int(tm.Month())-1)/3 + 1), nil
	case unitWeek:
		_, wk := tm.ISOWeek()
		return int64(wk), nil
	case unitISOYear:
		// The year the ISO week belongs to, which is the companion of the ISO
		// week above: without it the last week of December and the first week
		// of January group together.
		year, _ := tm.ISOWeek()
		return int64(year), nil
	case "decade":
		return int64(decadeOf(tm.Year())), nil
	case "century":
		return int64(centuryOf(tm.Year())), nil
	case "millennium":
		return int64(millenniumOf(tm.Year())), nil
	case "milliseconds", unitMillisecond:
		return secondsWithFraction(tm) * 1000, nil
	case unitMicrosecondsPlural, unitMicrosecond:
		// A microsecond is the finest a PostgreSQL timestamp holds, so the
		// count is always whole; answering it as an integer also keeps SQLite
		// from spelling a large REAL in exponent form.
		return int64(math.Round(secondsWithFraction(tm) * 1000000)), nil
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

// secondsWithFraction is the seconds field of a time together with whatever
// fraction of a second it carries. PostgreSQL's second, milliseconds and
// microseconds parts are all built from it, which is why 12.5 seconds is 12500
// milliseconds rather than 500.
func secondsWithFraction(tm time.Time) float64 {
	return float64(tm.Second()) + float64(tm.Nanosecond())/1e9
}

// decadeOf, centuryOf and millenniumOf number the year the way PostgreSQL does.
// A century counts from 1, so the year 2000 is in century 20 and 2001 is the
// first year of century 21; the same rule one order of magnitude up gives the
// millennium. A decade is the plain division, since there is no year zero to
// shift it. Each has its own arm for a year before the common era, where the
// division has to round the other way.
func decadeOf(year int) int {
	if year >= 0 {
		return year / 10
	}
	return -((8 - (year - 1)) / 10)
}

func centuryOf(year int) int {
	if year > 0 {
		return (year + 99) / 100
	}
	return -((99 - (year - 1)) / 100)
}

func millenniumOf(year int) int {
	if year > 0 {
		return (year + 999) / 1000
	}
	return -((999 - (year - 1)) / 1000)
}

// fnGoogleSQLDatePart implements EXTRACT under GoogleSQL. BigQuery's WEEK
// begins on Sunday and numbers the days before the year's first Sunday as week
// 0 — MySQL's week mode 0 — and ISOWEEK and ISOYEAR are the ISO pair whose week
// the shared helper spells "week". A value that does not parse is refused by name, the way
// BigQuery refuses an invalid literal, rather than answered with NULL, which
// would read as a statement about the data.
func fnGoogleSQLDatePart(args []driver.Value) (driver.Value, error) {
	unit, ok := toString(args[0])
	if !ok || args[1] == nil {
		return nil, nil
	}
	tm, ok := toStringTime(args[1])
	if !ok {
		s, _ := toString(args[1])
		return nil, fmt.Errorf("dialect: not a date or timestamp: %q", s)
	}
	switch unit = strings.ToLower(strings.TrimSpace(unit)); unit {
	case unitWeek:
		week, _ := mysqlWeek(tm, 0)
		return int64(week), nil
	case unitISOWeek:
		_, week := tm.ISOWeek()
		return int64(week), nil
	// BigQuery's MILLISECOND and MICROSECOND are the fraction of a second
	// alone, where PostgreSQL's are the seconds field with the fraction scaled
	// into it, and PostgreSQL answers to both spellings. So 13:04:05.123 is
	// 123 milliseconds here and 5123 there.
	case unitMillisecond:
		return int64(tm.Nanosecond() / 1e6), nil
	case unitMicrosecond:
		return int64(tm.Nanosecond() / 1e3), nil
	default:
		// BigQuery writes a week numbered from another weekday as
		// WEEK(<WEEKDAY>), which reaches here as "week_monday" and the rest.
		if start, ok := weekStartDay(unit); ok {
			return int64(weekNumberFrom(tm, start)), nil
		}
		return datePartValue(unit, tm)
	}
}

// weekNumberFrom counts the weeks that begin on start, with the days before the
// year's first such weekday in week 0 -- which is the rule BigQuery's plain
// WEEK already follows for Sunday, generalized to the weekday WEEK(<WEEKDAY>)
// names.
func weekNumberFrom(tm time.Time, start time.Weekday) int {
	jan1 := time.Date(tm.Year(), time.January, 1, 0, 0, 0, 0, tm.Location())
	firstStart := jan1.AddDate(0, 0, (int(start)-int(jan1.Weekday())+7)%7)
	day := time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, tm.Location())
	if day.Before(firstStart) {
		return 0
	}
	return int((dayNumber(day)-dayNumber(firstStart))/7) + 1
}

// fnMySQLDatePart implements EXTRACT under MySQL, whose WEEK is WEEK(x) with
// the session's default_week_format — 0 by default, the Sunday-first numbering
// mysqlWeek computes. Everything else follows the shared helper, including NULL
// for a value that does not parse, which is MySQL's own answer.
func fnMySQLDatePart(args []driver.Value) (driver.Value, error) {
	unit, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	tm, ok := toStringTime(args[1])
	if !ok {
		return nil, nil
	}
	unit = strings.ToLower(strings.TrimSpace(unit))
	switch unit {
	case unitWeek:
		week, _ := mysqlWeek(tm, 0)
		return int64(week), nil
	case unitMicrosecondsPlural, unitMicrosecond:
		// MySQL's MICROSECOND is the fractional part alone, where PostgreSQL's
		// MICROSECONDS is the seconds field multiplied out and so carries the
		// whole seconds with it. The shared helper answers PostgreSQL's, which
		// made EXTRACT(MICROSECOND FROM ...) answer MySQL's SECOND_MICROSECOND
		// value -- a plausible number a million times too large.
		return int64(tm.Nanosecond() / 1000), nil
	}
	if value, composite, err := mysqlCompositePart(unit, tm); composite {
		return value, err
	}
	return datePartValue(unit, tm)
}

// weekMode reads the optional mode argument MySQL's week functions take,
// answering the default when there is none.
func weekMode(args []driver.Value, defaultMode int) (int, bool) {
	if len(args) < 2 {
		return defaultMode, true
	}
	n, ok := toInt(args[1])
	if !ok {
		return 0, false
	}
	return int(n), true
}

// fnMySQLWeek implements MySQL WEEK(date[, mode]). Mode 0, the default, starts
// the week on Sunday and numbers from zero, so the first days of January can be
// week 0.
func fnMySQLWeek(args []driver.Value) (driver.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("dialect: WEEK expects 1 or 2 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	mode, ok := weekMode(args, 0)
	if !ok {
		return nil, nil
	}
	week, _ := mysqlWeek(tm, mode)
	return int64(week), nil
}

// fnMySQLWeekOfYear implements MySQL WEEKOFYEAR(date), which is WEEK(date, 3):
// the ISO week, starting on Monday and numbering from one.
func fnMySQLWeekOfYear(args []driver.Value) (driver.Value, error) {
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	week, _ := mysqlWeek(tm, 3)
	return int64(week), nil
}

// fnMySQLYearWeek implements MySQL YEARWEEK(date[, mode]) as year*100 + week.
// The year is the one the week belongs to rather than the one the date is in,
// so the first days of January can answer with the previous year: YEARWEEK
// forces on the mode flag that lends them to the previous year's last week,
// which is why it has no week 0 where WEEK does.
func fnMySQLYearWeek(args []driver.Value) (driver.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("dialect: YEARWEEK expects 1 or 2 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	mode, ok := weekMode(args, 0)
	if !ok {
		return nil, nil
	}
	week, year := mysqlWeek(tm, mode|weekModeYear)
	return int64(year)*100 + int64(week), nil
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

func fnLpad(args []driver.Value) (driver.Value, error) { return pad(args, true, padRules{}) }
func fnRpad(args []driver.Value) (driver.Value, error) { return pad(args, false, padRules{}) }

// padRules holds the two boundary answers LPAD and RPAD differ on between the
// dialects, checked against MySQL 8.4 and PostgreSQL 17.
//
//   - A negative length: MySQL answers NULL, PostgreSQL an empty string, and
//     BigQuery refuses the call.
//   - An empty pad with a length past the input: MySQL cannot reach the length
//     with nothing to pad with and answers an empty string, PostgreSQL returns
//     the input unpadded.
//
// The zero value is the answer given before either dialect had its own, and is
// what the dialects without a verified rule keep.
type padRules struct {
	emptyOnNegativeLength bool
	raiseOnNegativeLength bool
	emptyOnEmptyPad       bool
}

var (
	mysqlPadRules      = padRules{emptyOnEmptyPad: true}       //nolint:gochecknoglobals // constant-like
	postgresqlPadRules = padRules{emptyOnNegativeLength: true} //nolint:gochecknoglobals // constant-like
	googlesqlPadRules  = padRules{raiseOnNegativeLength: true} //nolint:gochecknoglobals // constant-like
)

func padFor(rules padRules, left bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) { return pad(args, left, rules) }
}

// pad implements LPAD and RPAD. The length is a count of characters in all
// three dialects, so the arithmetic is over runes: measuring in bytes cut a
// multibyte character in half and returned bytes that are not UTF-8. A call with
// two arguments pads with spaces, which is PostgreSQL's short form.
func pad(args []driver.Value, left bool, rules padRules) (driver.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("dialect: LPAD/RPAD expects 2 or 3 arguments, got %d", len(args))
	}
	s, ok1 := toString(args[0])
	n, ok2 := toCount(args[1])
	padStr, ok3 := " ", true
	if len(args) == 3 {
		padStr, ok3 = toString(args[2])
	}
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if n < 0 {
		switch {
		case rules.emptyOnNegativeLength:
			return "", nil
		case rules.raiseOnNegativeLength:
			return nil, fmt.Errorf("dialect: LPAD/RPAD length must not be negative, got %d", n)
		}
		return nil, nil
	}
	length := int(n)
	runes := []rune(s)
	if len(runes) >= length {
		return string(runes[:length]), nil
	}
	if padStr == "" {
		// Nothing to pad with and a length still to reach.
		if rules.emptyOnEmptyPad {
			return "", nil
		}
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

// fnMySQLSubstr implements MySQL SUBSTRING(s, pos[, len]), which SQLite's own
// substr() does not match at position 0: MySQL reads 0 as no position at all and
// answers the empty string, where SQLite reads it as the place before the first
// character and answers the whole string. That matters because 0 is what LOCATE
// returns when it finds nothing, so SUBSTRING(s, LOCATE('x', s)) is empty in
// MySQL for a row without an x and the whole of s under SQLite's rule.
//
// A negative position counts from the end, which SQLite does agree on, and the
// arithmetic is over characters rather than bytes.
func fnMySQLSubstr(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("dialect: SUBSTRING expects 2 or 3 arguments, got %d", len(args))
	}
	s, ok1 := toString(args[0])
	pos, ok2 := toCount(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	// Every argument is read before the range is worked out, so a NULL length
	// makes the call NULL even when the position already put the range outside
	// the string. Answering the empty string there hid the NULL from a caller
	// filtering on IS NULL.
	var count int64
	if len(args) == 3 {
		var ok bool
		if count, ok = toCount(args[2]); !ok {
			return nil, nil
		}
	}
	runes := []rune(s)
	length := int64(len(runes))
	var start int64
	switch {
	case pos == 0:
		return "", nil
	case pos < 0:
		start = length + pos
		if start < 0 {
			return "", nil
		}
	default:
		start = pos - 1
		if start >= length {
			return "", nil
		}
	}
	end := length
	if len(args) == 3 {
		if count <= 0 {
			return "", nil
		}
		if count < length-start {
			end = start + count
		}
	}
	return string(runes[start:end]), nil
}

// fnPostgreSQLSubstr implements PostgreSQL substr(s, start[, count]). Positions
// are counted from 1 and a start below 1 is not an offset from the end: the
// result is the characters at positions start through start+count-1 that the
// string actually has, so substr('abcdef', -1, 3) covers positions -1, 0 and 1
// and yields "a". SQLite's substr() reads a negative start from the end instead,
// which is MySQL's rule, and answered "f".
func fnPostgreSQLSubstr(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("dialect: SUBSTRING expects 2 or 3 arguments, got %d", len(args))
	}
	s, ok1 := toString(args[0])
	start, ok2 := toCount(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	// The length is read before the range is worked out, so a NULL there makes
	// the call NULL rather than being reached only for a range that survives.
	var count int64
	if len(args) == 3 {
		var ok bool
		if count, ok = toCount(args[2]); !ok {
			return nil, nil
		}
	}
	runes := []rune(s)
	length := int64(len(runes))
	// Positions are 1-based and the end is exclusive, so the whole string is
	// positions 1 through length+1.
	end := length + 1
	if len(args) == 3 {
		if count < 0 {
			return nil, errors.New("dialect: SUBSTRING: negative substring length not allowed")
		}
		// start+count is where the result ends, and both are whatever integer
		// literal the query held, so the sum is guarded rather than clamped:
		// subtracting a start near math.MinInt64 would wrap the comparison and
		// widen the result to the whole string, and adding a count near
		// math.MaxInt64 to a positive start would wrap it the other way. A sum
		// that would overflow upward ends past the string, which end already is.
		if start <= 0 || count <= math.MaxInt64-start {
			if sum := start + count; sum < end {
				end = sum
			}
		}
	}
	from := max(start, 1)
	to := min(end, length+1)
	if to <= from {
		return "", nil
	}
	return string(runes[from-1 : to-1]), nil
}

// fnGoogleSQLSubstr implements BigQuery SUBSTR(s, position[, length]), which is
// neither of the other two dialects' rules.
//
// Position 0 means position 1, a negative position counts back from the end
// where -1 is the last character, and a position that lands before the string
// clamps to its start with the length measured from there rather than consumed
// by the part that fell outside. That last clause is what separates it from
// PostgreSQL, where the out-of-range prefix eats the length, and the first is
// what separates it from MySQL, where position 0 is no position at all.
//
// Positions count characters, not bytes.
func fnGoogleSQLSubstr(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("dialect: SUBSTR expects 2 or 3 arguments, got %d", len(args))
	}
	s, ok1 := toString(args[0])
	pos, ok2 := toCount(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	// Every argument is read before the range is worked out, so a NULL length
	// makes the call NULL even when the position already put the range outside
	// the string.
	var count int64
	if len(args) == 3 {
		var ok bool
		if count, ok = toCount(args[2]); !ok {
			return nil, nil
		}
		if count < 0 {
			return nil, errors.New("dialect: SUBSTR: length must not be negative")
		}
	}
	runes := []rune(s)
	length := int64(len(runes))

	start := pos - 1
	if pos == 0 {
		start = 0
	} else if pos < 0 {
		start = length + pos
	}
	// A position before the string starts the result at its first character.
	start = max(start, 0)
	if start >= length {
		return "", nil
	}

	end := length
	if len(args) == 3 {
		if count == 0 {
			return "", nil
		}
		if count < length-start {
			end = start + count
		}
	}
	return string(runes[start:end]), nil
}

// fnDialectRound implements ROUND(x, n) for the digit counts SQLite's own
// round() will not take.
//
// A negative n rounds to a power of ten -- ROUND(12345, -2) is 12300 -- which is
// how MySQL, PostgreSQL and BigQuery all spell "round to the nearest hundred".
// SQLite reads the second argument as digits after the decimal point and ignores
// a negative one, so the call succeeded and returned its input. All three
// engines agree on every answer, half away from zero, so one helper serves them;
// dialect.SQLite is not rewritten onto it, because ignoring a negative count is
// SQLite's documented behavior and is what a caller who named no dialect asked
// for.
func fnDialectRound(args []driver.Value) (driver.Value, error) {
	value, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	digits, ok := toCount(args[1])
	if !ok {
		return nil, nil
	}
	if digits >= 0 {
		// What SQLite already does, kept here so one function answers the whole
		// call rather than the rewrite having to decide which one to emit.
		return roundHalfAwayFromZero(value, digits), nil
	}
	// Below the smallest power of ten a float64 holds, the whole value is under
	// the rounding unit: ROUND(12345, -400) is 0 in MySQL and in BigQuery. The
	// comparison comes before the negation because negating math.MinInt64 wraps.
	if digits < -float64MaxDecimalExponent {
		return int64(0), nil
	}
	scale := math.Pow(10, float64(-digits))
	rounded := math.Round(value/scale) * scale
	if math.IsInf(rounded, 0) {
		// Rounding a value near the largest float64 up to the next unit lands
		// past what a float64 holds. BigQuery raises on the overflow rather
		// than answering, and an infinity here would flow into the rest of the
		// query as a number.
		return nil, fmt.Errorf("dialect: ROUND: rounding %v to %d digits overflows", value, digits)
	}
	// A whole result is returned as an integer so a rounded count does not
	// arrive with a decimal point the engines do not put there.
	if rounded == math.Trunc(rounded) && math.Abs(rounded) < 1e15 {
		return int64(rounded), nil
	}
	return rounded, nil
}

// float64MaxDecimalExponent is the largest power of ten a float64 holds. Ten to
// anything beyond it is infinite, and an infinite scale turns a finite value
// into a NaN rather than into the answer every engine gives.
const float64MaxDecimalExponent = 308

// fnDialectRoundEven is fnDialectRound with MySQL's and PostgreSQL's tie rule.
// Both round a floating-point argument to the even neighbor: ROUND(2.5) is 2
// and ROUND(3.5) is 4, where SQLite's own round() and BigQuery both answer 3
// and 4. Every non-integer value SQLite holds is a float, so this is the rule
// that matches what a REAL column loaded from a file does in either engine.
//
// The one case it cannot match is a decimal literal written in the query:
// MySQL reads 2.5 as an exact decimal and answers 3, and SQLite has no type to
// tell that from the double 2.5e0, which MySQL answers 2 for. The column is the
// case worth getting right.
func fnDialectRoundEven(args []driver.Value) (driver.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("dialect: ROUND expects 1 or 2 arguments, got %d", len(args))
	}
	value, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	digits := int64(0)
	if len(args) > 1 {
		if digits, ok = toCount(args[1]); !ok {
			return nil, nil
		}
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return value, nil
	}
	if digits < -float64MaxDecimalExponent {
		return int64(0), nil
	}
	if digits > float64MaxDecimalExponent {
		return value, nil
	}
	rounded, ok := roundHalfToEven(value, digits)
	if !ok {
		return value, nil
	}
	if math.IsInf(rounded, 0) {
		return nil, fmt.Errorf("dialect: ROUND: rounding %v to %d digits overflows", value, digits)
	}
	if rounded == math.Trunc(rounded) && math.Abs(rounded) < 1e15 {
		return int64(rounded), nil
	}
	return rounded, nil
}

// roundHalfToEven rounds value to digits decimal places, breaking an exact tie
// toward the even neighbor.
//
// It rounds the shortest decimal that reads back as value rather than the
// binary value itself, which is what makes 2.675 round to 2.68 the way MySQL
// answers: the nearest double to 2.675 is a shade below it, so scaling by 100
// in binary lands on 267.49999999999997 and rounds down. Reading the value as
// the decimal a person wrote is the only way the tie is a tie at all.
func roundHalfToEven(value float64, digits int64) (float64, bool) {
	exact, ok := new(big.Rat).SetString(strconv.FormatFloat(value, 'f', -1, 64))
	if !ok {
		return 0, false
	}
	shift := digits
	if shift < 0 {
		shift = -shift
	}
	pow := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(shift), nil))
	scaled := new(big.Rat)
	if digits >= 0 {
		scaled.Mul(exact, pow)
	} else {
		scaled.Quo(exact, pow)
	}
	whole := ratRoundHalfToEven(scaled)
	result := new(big.Rat).SetInt(whole)
	if digits >= 0 {
		result.Quo(result, pow)
	} else {
		result.Mul(result, pow)
	}
	f, _ := result.Float64()
	return f, true
}

// ratRoundHalfToEven is r rounded to an integer, with an exact half going to
// whichever neighbor is even.
func ratRoundHalfToEven(r *big.Rat) *big.Int {
	quo, rem := new(big.Int).QuoRem(r.Num(), r.Denom(), new(big.Int))
	twice := new(big.Int).Abs(rem)
	twice.Lsh(twice, 1)
	switch cmp := twice.Cmp(r.Denom()); {
	case cmp < 0:
		return quo
	case cmp == 0 && quo.Bit(0) == 0:
		return quo
	}
	if r.Sign() < 0 {
		return quo.Sub(quo, big.NewInt(1))
	}
	return quo.Add(quo, big.NewInt(1))
}

// roundHalfAwayFromZero is value rounded to digits places after the decimal
// point, with a half rounded away from zero, which is what all three engines do
// and what SQLite's round() does.
//
// A digit count so large that the scaling cannot be represented leaves the value
// alone rather than answering NaN. The test is on what the scaling produces
// rather than on the count, because the two are not the same question: at 18
// digits the scale is finite and 5e-19 does round, to 1e-18, while at 400 the
// scale is infinite and nothing can.
func roundHalfAwayFromZero(value float64, digits int64) float64 {
	if digits == 0 {
		return math.Round(value)
	}
	scale := math.Pow(10, float64(digits))
	scaled := value * scale
	if math.IsInf(scale, 0) || math.IsInf(scaled, 0) {
		return value
	}
	return math.Round(scaled) / scale
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

// fnRepeat implements MySQL REPEAT(str, count), which answers the empty string
// for a count of zero or less. fnGoogleSQLRepeat is the same call under
// BigQuery's rule, where a negative count is refused rather than answered.
func fnRepeat(args []driver.Value) (driver.Value, error) { return repeatWith(args, false) }

func fnGoogleSQLRepeat(args []driver.Value) (driver.Value, error) { return repeatWith(args, true) }

func repeatWith(args []driver.Value, raiseOnNegative bool) (driver.Value, error) {
	s, ok1 := toString(args[0])
	count, ok2 := toCount(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	if count < 0 && raiseOnNegative {
		return nil, fmt.Errorf("dialect: REPEAT count must not be negative, got %d", count)
	}
	if count <= 0 {
		return "", nil
	}
	return strings.Repeat(s, int(count)), nil
}

// soundexRules are the three things the dialects differ on, all of them read
// off mysql:8.4 and the BigQuery emulator rather than assumed.
//
//   - MySQL emits one digit per coded consonant however many there are, so
//     SOUNDEX('Hello World') is H4643; BigQuery stops at three, giving H464.
//   - MySQL upper-cases an ASCII first letter, so SOUNDEX('hello') is H400;
//     BigQuery keeps it as written and answers h400.
//   - MySQL treats any Unicode letter as the first letter and writes it back
//     unchanged, so SOUNDEX('éèê') is é000; BigQuery sees no letter at all
//     there and answers the empty string.
//
// What they agree on is the coding rule, which is not the textbook one: no
// letter resets the run, so SOUNDEX('Tymczak') is T520 rather than the T522 a
// vowel reset would give, and the first letter's own code counts as the
// previous one, so SOUNDEX('Pfister') is P236 rather than P1236.
type soundexRules struct {
	maxDigits  int
	upperFirst bool
	asciiOnly  bool
}

var (
	mysqlSoundexRules     = soundexRules{upperFirst: true}              //nolint:gochecknoglobals // constant-like
	googlesqlSoundexRules = soundexRules{maxDigits: 3, asciiOnly: true} //nolint:gochecknoglobals // constant-like
)

func fnMySQLSoundex(args []driver.Value) (driver.Value, error) {
	return soundexWith(args, mysqlSoundexRules)
}

func fnGoogleSQLSoundex(args []driver.Value) (driver.Value, error) {
	return soundexWith(args, googlesqlSoundexRules)
}

// soundexWith implements SOUNDEX under one dialect's rules. SQLite's own
// soundex() matches neither: it answers "?000" for NULL and for a value holding
// no letter at all, and it cuts the code to four characters whatever the dialect
// asks for.
func soundexWith(args []driver.Value, rules soundexRules) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	runes := []rune(s)
	isLetter := func(r rune) bool {
		if rules.asciiOnly {
			return r < utf8.RuneSelf && unicode.IsLetter(r)
		}
		return unicode.IsLetter(r)
	}
	first := -1
	for i, r := range runes {
		if isLetter(r) {
			first = i
			break
		}
	}
	if first < 0 {
		// No letter to build a code from. Both dialects answer the empty
		// string, where SQLite answers its "?000" placeholder.
		return "", nil
	}
	var b strings.Builder
	if lead := runes[first]; rules.upperFirst && lead < utf8.RuneSelf {
		b.WriteRune(unicode.ToUpper(lead))
	} else {
		b.WriteRune(lead)
	}
	previous := soundexCode(runes[first])
	digits := 0
	for _, r := range runes[first+1:] {
		if !isLetter(r) {
			continue
		}
		if rules.maxDigits > 0 && digits >= rules.maxDigits {
			break
		}
		code := soundexCode(r)
		if code == 0 || code == previous {
			// A letter with no code is passed over without resetting the run,
			// which is what makes SOUNDEX('Honeyman') H500 rather than H555.
			continue
		}
		b.WriteByte(byte('0' + code)) //nolint:gosec // code is 1..6 from soundexCode
		digits++
		previous = code
	}
	for ; digits < 3; digits++ {
		b.WriteByte('0')
	}
	return b.String(), nil
}

// soundexCode is the digit MySQL gives a letter, or zero for a letter it does
// not code and for any letter outside ASCII.
func soundexCode(r rune) int {
	switch unicode.ToUpper(r) {
	case 'B', 'F', 'P', 'V':
		return 1
	case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z':
		return 2
	case 'D', 'T':
		return 3
	case 'L':
		return 4
	case 'M', 'N':
		return 5
	case 'R':
		return 6
	default:
		return 0
	}
}

// fnDialectReplace implements REPLACE(subject, search, replacement) with the
// NULL rule every dialect here has and SQLite does not: SQLite short-circuits on
// an empty search string and answers the subject without looking at the
// replacement, so REPLACE('hello', ”, NULL) answered 'hello' where MySQL and
// PostgreSQL answer NULL. A NULL that should have traveled through the
// expression disappeared and the row read as unchanged rather than as unknown.
func fnDialectReplace(args []driver.Value) (driver.Value, error) {
	subject, ok1 := toString(args[0])
	search, ok2 := toString(args[1])
	replacement, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if search == "" {
		return subject, nil
	}
	return strings.ReplaceAll(subject, search, replacement), nil
}

// fnMySQLChar implements MySQL CHAR(n, ...), which builds bytes where SQLite's
// char() builds code points: MySQL answers the single zero byte for CHAR(0),
// which SQLite drops entirely, and the two bytes 0x01 0x00 for CHAR(256), which
// SQLite encodes as the UTF-8 of U+0100. A NULL argument is skipped rather than
// making the call NULL, which is MySQL's rule for this one function.
func fnMySQLChar(args []driver.Value) (driver.Value, error) {
	out := make([]byte, 0, len(args)*4)
	for _, arg := range args {
		if arg == nil {
			continue
		}
		n, ok := toCount(arg)
		if !ok {
			continue
		}
		// MySQL takes each argument modulo 2^32 and writes its bytes
		// big-endian, dropping the leading zero bytes but keeping one byte for
		// an argument of zero.
		u := uint32(n) //nolint:gosec // reinterpreting the bits is MySQL's rule here
		var word [4]byte
		binary.BigEndian.PutUint32(word[:], u)
		first := 0
		for first < 3 && word[first] == 0 {
			first++
		}
		out = append(out, word[first:]...)
	}
	// The result is a blob so a zero byte in it survives into whatever reads it
	// next; a text value carrying one is cut there.
	return out, nil
}

// fnSpace implements MySQL SPACE(n).
func fnSpace(args []driver.Value) (driver.Value, error) {
	n, ok := toCount(args[0])
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
// list. A NULL argument makes the whole call NULL, which is what MySQL and
// GoogleSQL answer; PostgreSQL skips its NULLs and reaches the pair below
// instead.
func fnLeast(args []driver.Value) (driver.Value, error) { return extremum(args, true, false) }

func fnGreatest(args []driver.Value) (driver.Value, error) { return extremum(args, false, false) }

// fnPostgresLeast and fnPostgresGreatest implement PostgreSQL's LEAST and
// GREATEST, which ignore their NULL arguments and answer NULL only when every
// argument is NULL. An empty cell loads as NULL, so under the other rule a row
// missing one of the columns being compared reports no extreme at all.
func fnPostgresLeast(args []driver.Value) (driver.Value, error) { return extremum(args, true, true) }

func fnPostgresGreatest(args []driver.Value) (driver.Value, error) {
	return extremum(args, false, true)
}

func extremum(args []driver.Value, wantSmaller, skipNulls bool) (driver.Value, error) {
	if len(args) == 0 {
		return nil, errors.New("dialect: LEAST/GREATEST expects at least one argument")
	}
	// The NULLs are dropped before anything else is decided, so the numeric or
	// lexicographic choice below is made from the values that are really
	// compared rather than from a list a NULL is still standing in.
	if skipNulls {
		kept := make([]driver.Value, 0, len(args))
		for _, a := range args {
			if a != nil {
				kept = append(kept, a)
			}
		}
		if len(kept) == 0 {
			return nil, nil
		}
		args = kept
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
	// An empty set holds nothing, including the empty string. Splitting it
	// yields one empty element, which found an empty needle at position 1 where
	// MySQL answers 0. An empty element inside a non-empty set is a real
	// element and keeps its position.
	if set == "" {
		return int64(0), nil
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
	n, ok := toCount(args[0])
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

// fnMySQLTimeOfDay implements MySQL's one-argument TIME(x): the time part of a
// datetime, with whatever fraction of a second the value carried. SQLite has a
// time() of its own that takes modifiers and answers at second resolution, so
// the fraction written in the value was dropped.
func fnMySQLTimeOfDay(args []driver.Value) (driver.Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("dialect: TIME expects 1 argument, got %d", len(args))
	}
	if tm, ok := toStringTime(args[0]); ok && hasTimeOfDay(args[0]) {
		return formatTimeOfDayValue(tm), nil
	}
	// A value with no clock in it is read as a number the way the cast to TIME
	// reads one, so TIME('2024-03-05') is 00:20:24 as MySQL answers rather than
	// the midnight a date formatted as a time would give.
	return mysqlTimeFromNumber(args[0]), nil
}

// fnUnixTimestamp implements MySQL UNIX_TIMESTAMP([date]): the current epoch
// second with no argument, or the epoch second of the given datetime. Values are
// read as UTC, as everywhere else in this package.
func fnUnixTimestamp(args []driver.Value) (driver.Value, error) {
	if len(args) == 0 {
		return clockUTC().Unix(), nil
	}
	if len(args) > 1 {
		return nil, fmt.Errorf("dialect: UNIX_TIMESTAMP expects 0 or 1 arguments, got %d", len(args))
	}
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	if nanos := tm.Nanosecond(); nanos != 0 {
		// MySQL answers a real when the value carries a fraction, so a
		// microsecond-resolution timestamp does not lose it here.
		return float64(tm.Unix()) + float64(nanos)/1e9, nil
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
	// MySQL answers NULL outside the range of its TIMESTAMP type, 1970-01-01
	// 00:00:00 UTC through 3001-01-18 23:59:59 (32536771199 seconds).
	if sec < 0 || sec > 32536771199 {
		return nil, nil
	}
	tm := time.Unix(sec, 0).UTC()
	if len(args) == 1 {
		return tm.Format(layoutDateTime), nil
	}
	return fnDateFormat([]driver.Value{tm.Format(layoutDateTime), args[1]})
}

// --- PostgreSQL scalar functions ---

// fnToChar implements PostgreSQL TO_CHAR(value, format) for date/time values and
// for numbers. The two are told apart by the template: a numeric one is built
// from digit positions, which no date template contains.
func fnToChar(args []driver.Value) (driver.Value, error) {
	format, ok := toString(args[1])
	if !ok {
		return nil, nil
	}
	// The argument decides which template language this is, because it is the
	// only thing that carries the answer: a digit in a date template is
	// literal text and a letter in a numeric one is too, so PostgreSQL reads
	// the argument's type and never the template. A number is a number; text
	// that parses as a date is a date. Text that is neither has no type to
	// read, and there the template is the only signal left.
	switch value := args[0].(type) {
	case int64:
		return pgFormatNumber(float64(value), format), nil
	case float64:
		return pgFormatNumber(value, format), nil
	}
	if tm, ok := toStringTime(args[0]); ok {
		return pgFormatTime(format, tm), nil
	}
	if !isDateTemplate(format) {
		value, ok := toFloat(args[0])
		if !ok {
			return nil, nil
		}
		return pgFormatNumber(value, format), nil
	}
	return nil, nil
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
	tm, ok := parseLayout(pgParseLayout(format), s)
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
	case "decade":
		return time.Date(decadeOf(y)*10, 1, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case "century":
		// A century starts in its first year, which is the year ending in 01:
		// truncating 2024 to a century gives 2001 rather than 2000.
		return time.Date((centuryOf(y)-1)*100+1, 1, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case "millennium":
		return time.Date((millenniumOf(y)-1)*1000+1, 1, 1, 0, 0, 0, 0, loc).Format(layoutDateTime), nil
	case "milliseconds", unitMillisecond:
		return truncatedFraction(tm, time.Millisecond), nil
	case unitMicrosecondsPlural, unitMicrosecond:
		return truncatedFraction(tm, time.Microsecond), nil
	case unitISOYear:
		// The ISO year begins on the Monday of the week holding its first
		// Thursday, which is not January 1 in most years.
		isoYear, _ := tm.ISOWeek()
		jan4 := time.Date(isoYear, time.January, 4, 0, 0, 0, 0, loc)
		offset := (int(jan4.Weekday()) + 6) % 7
		return jan4.AddDate(0, 0, -offset).Format(layoutDateTime), nil
	default:
		return nil, fmt.Errorf("dialect: unsupported DATE_TRUNC unit %q", unit)
	}
}

// truncatedFraction rounds a time down to a multiple of unit and spells it with
// however much of the fraction survives, trailing zeros removed, which is how
// PostgreSQL prints a timestamp: DATE_TRUNC('millisecond', '10:11:12.123456')
// is 10:11:12.123 and a whole second keeps no decimal point at all.
func truncatedFraction(tm time.Time, unit time.Duration) string {
	out := tm.Truncate(unit)
	if out.Nanosecond() == 0 {
		return out.Format(layoutDateTime)
	}
	return strings.TrimRight(out.Format(layoutDateTime+".000000000"), "0")
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
	// PostgreSQL refuses a zero field position rather than answering with an
	// empty string, which is what makes an off-by-one in a computed position
	// visible instead of reading as an empty field.
	if n == 0 {
		return nil, errors.New("dialect: SPLIT_PART: field position must not be zero")
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

func fnMySQLLeft(args []driver.Value) (driver.Value, error)  { return mysqlLeftRight(args, true) }
func fnMySQLRight(args []driver.Value) (driver.Value, error) { return mysqlLeftRight(args, false) }

func fnGoogleSQLLeft(args []driver.Value) (driver.Value, error) {
	return googlesqlLeftRight(args, true)
}

func fnGoogleSQLRight(args []driver.Value) (driver.Value, error) {
	return googlesqlLeftRight(args, false)
}

// leftRight implements LEFT/RIGHT with PostgreSQL's negative-count semantics: a
// negative n removes |n| characters from the far end.
func leftRight(args []driver.Value, left bool) (driver.Value, error) {
	s, n, ok := leftRightArgs(args)
	if !ok {
		return nil, nil
	}
	count := int(n)
	if count < 0 {
		count = len([]rune(s)) + count
	}
	return takeRunes(s, count, left), nil
}

// mysqlLeftRight implements LEFT/RIGHT with MySQL's negative-count semantics: a
// negative n answers the empty string rather than trimming the far end.
func mysqlLeftRight(args []driver.Value, left bool) (driver.Value, error) {
	s, n, ok := leftRightArgs(args)
	if !ok {
		return nil, nil
	}
	return takeRunes(s, int(n), left), nil
}

// googlesqlLeftRight implements LEFT/RIGHT with GoogleSQL's rule for a negative
// count, which is to raise: BigQuery has no meaning for a negative length and
// answering PostgreSQL's trimmed string for one would hide the mistake.
func googlesqlLeftRight(args []driver.Value, left bool) (driver.Value, error) {
	s, n, ok := leftRightArgs(args)
	if !ok {
		return nil, nil
	}
	if n < 0 {
		return nil, fmt.Errorf("dialect: LEFT/RIGHT length must not be negative, got %d", n)
	}
	return takeRunes(s, int(n), left), nil
}

// leftRightArgs coerces the shared (string, count) arguments of LEFT and RIGHT.
func leftRightArgs(args []driver.Value) (string, int64, bool) {
	s, ok1 := toString(args[0])
	n, ok2 := toCount(args[1])
	return s, n, ok1 && ok2
}

// takeRunes returns the first or last count characters of s, the whole of s when
// it is shorter than that, and the empty string when count is not positive.
func takeRunes(s string, count int, fromLeft bool) string {
	if count <= 0 {
		return ""
	}
	runes := []rune(s)
	if count > len(runes) {
		count = len(runes)
	}
	if fromLeft {
		return string(runes[:count])
	}
	return string(runes[len(runes)-count:])
}

// fnRegexpReplace implements GoogleSQL REGEXP_REPLACE(source, pattern,
// replacement), which replaces every match, and is also what a query written in
// the SQLite dialect reaches. PostgreSQL back-references (\1) are translated to
// Go's ${1} expansion form. A flags argument is accepted here for the callers
// that already pass one: "g" replaces every match and its absence replaces only
// the first, and "i" matches case insensitively.
func fnRegexpReplace(args []driver.Value) (driver.Value, error) {
	return regexpReplace(args, true)
}

// fnPostgresRegexpReplace implements PostgreSQL regexp_replace(source, pattern,
// replacement[, flags]), whose three-argument form replaces the first match
// alone; the rest need the "g" flag.
func fnPostgresRegexpReplace(args []driver.Value) (driver.Value, error) {
	return regexpReplace(args, false)
}

// regexpReplace is the shared body of the flag-taking REGEXP_REPLACE forms.
// defaultGlobal is what the three-argument form means, which is every match for
// GoogleSQL and the first alone for PostgreSQL.
func regexpReplace(args []driver.Value, defaultGlobal bool) (driver.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return nil, fmt.Errorf("dialect: REGEXP_REPLACE expects 3 or 4 arguments, got %d", len(args))
	}
	src, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	repl, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	global := defaultGlobal
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

// fnMySQLRegexpReplace implements MySQL REGEXP_REPLACE(subject, pattern,
// replacement[, pos[, occurrence[, match_type]]]). MySQL's fourth argument is a
// 1-based character position to start at rather than PostgreSQL's flag string,
// and its fifth selects one match to replace, with 0 meaning every match from
// the position onward.
func fnMySQLRegexpReplace(args []driver.Value) (driver.Value, error) {
	if len(args) < 3 || len(args) > 6 {
		return nil, fmt.Errorf("dialect: REGEXP_REPLACE expects 3 to 6 arguments, got %d", len(args))
	}
	src, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	repl, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	pos, occurrence := int64(1), int64(0)
	if len(args) >= 4 {
		n, ok := toInt(args[3])
		if !ok {
			return nil, nil
		}
		pos = n
	}
	if len(args) >= 5 {
		n, ok := toInt(args[4])
		if !ok {
			return nil, nil
		}
		occurrence = n
	}
	matchType := ""
	if len(args) == 6 {
		m, ok := toString(args[5])
		if !ok {
			return nil, nil
		}
		matchType = m
	}
	// The pattern goes through the match-type mapping even when no match type
	// was given, because MySQL's default is not Go's: its collation folds case.
	pattern, err := mysqlRegexpPattern(pattern, matchType)
	if err != nil {
		return nil, err
	}
	runes := []rune(src)
	if pos < 1 || int(pos) > len(runes)+1 {
		return nil, fmt.Errorf("dialect: REGEXP_REPLACE position %d is out of bounds", pos)
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return nil, err
	}
	head, tail := string(runes[:pos-1]), string(runes[pos-1:])
	expansion := mysqlReplacement(repl)
	if occurrence == 0 {
		return head + re.ReplaceAllString(tail, expansion), nil
	}
	// A negative occurrence is not a form MySQL documents; it answers the first
	// match there, so the count below starts at one for anything under it.
	wanted := int(occurrence)
	if wanted < 1 {
		wanted = 1
	}
	matches := re.FindAllStringSubmatchIndex(tail, wanted)
	if len(matches) < wanted {
		return src, nil
	}
	loc := matches[wanted-1]
	out := re.ExpandString([]byte(tail[:loc[0]]), expansion, tail, loc)
	return head + string(out) + tail[loc[1]:], nil
}

// applyMySQLMatchType folds a MySQL match_type string into the pattern as Go
// regexp flags. MySQL spells them c (case sensitive), i (case insensitive), m
// (multi-line) and n (a dot matches a newline); u, which selects Unix line
// endings, has no Go equivalent and is refused rather than ignored.
func mysqlRegexpPattern(pattern, matchType string) (string, error) {
	fold := true
	var flags string
	for _, c := range matchType {
		switch c {
		case 'c':
			fold = false
		case 'i':
			fold = true
		case 'm':
			flags += "m"
		case 'n':
			flags += "s"
		default:
			return "", fmt.Errorf("dialect: regular expression match type %q is not supported", matchType)
		}
	}
	if fold {
		flags = "i" + flags
	} else {
		flags += "-i"
	}
	// The flag group leads the pattern, so a group the caller wrote themselves
	// stands after it and wins for the rest of the pattern.
	return "(?" + flags + ")" + pattern, nil
}

// mysqlReplacement translates MySQL replacement references ($1..$9) to the ${n}
// form Go's regexp expansion understands. MySQL writes a literal "$" as "\$",
// and a backslash before anything else stands for that character.
func mysqlReplacement(repl string) string {
	var b strings.Builder
	for i := 0; i < len(repl); i++ {
		switch {
		case repl[i] == '\\' && i+1 < len(repl):
			if repl[i+1] == '$' {
				b.WriteString("$$")
			} else {
				b.WriteByte(repl[i+1])
			}
			i++
		case repl[i] == '$' && i+1 < len(repl) && repl[i+1] >= '0' && repl[i+1] <= '9':
			b.WriteString("${")
			b.WriteByte(repl[i+1])
			b.WriteByte('}')
			i++
		case repl[i] == '$':
			b.WriteString("$$")
		default:
			b.WriteByte(repl[i])
		}
	}
	return b.String()
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

// fnMySQLDateDiff implements MySQL TIMESTAMPDIFF(unit, start, end), called as
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
// through a time.Duration, which saturates at about ±292 years while MySQL's
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
// of the start's. MySQL has no month-end special case, so January 31 to
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

// mysqlTimeMaxSeconds is the ceiling of MySQL's TIME type, 838:59:59, which
// TIMEDIFF clamps to on both sides.
const mysqlTimeMaxSeconds = 838*3600 + 59*60 + 59

// fnMySQLTimeDiff implements MySQL TIMEDIFF(a, b): a minus b rendered as a
// MySQL TIME, whose hours keep counting past 24 and clamp at ±838:59:59.
// Two datetimes and two bare TIME values both work; mixing the two shapes is
// NULL, as it is in MySQL, and so is a value that does not parse.
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

// renderMySQLTime renders signed nanoseconds as a MySQL TIME, clamped to
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

// mysqlClockNanos reads a MySQL TIME value — an optional sign, hours that may
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
	// MySQL clamps each TIME argument to its range before subtracting, so
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
	'h': layoutMonthShort,
	'P': "pm",
	'D': "01/02/06",
	'x': "01/02/06",
	'X': layoutTimeOnly,
	'c': "Mon Jan _2 15:04:05 2006",
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

// strftimeRender writes tm according to a GoogleSQL format string.
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
	return strftimeRender(tm, format), nil
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

// fnPostgresToHex implements PostgreSQL to_hex(n): the lowercase hexadecimal
// digits of an integer, with a negative read as a 64-bit two's complement value
// the way PostgreSQL reads it. PostgreSQL has no string form of the function, so
// a value that names no integer is refused rather than hexed as text, which is
// what GoogleSQL's TO_HEX does with its bytes.
func fnPostgresToHex(args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	n, ok, err := postgresHexArgument(args[0])
	if err != nil || !ok {
		return nil, err
	}
	// The unsigned reading is the point: PostgreSQL answers the digits of the
	// 64-bit two's complement value, so to_hex(-1) is sixteen f's.
	return strconv.FormatUint(uint64(n), 16), nil //nolint:gosec // the two's complement reading is what PostgreSQL prints for a negative
}

// postgresHexArgument reads the integer to_hex converts. ok is false when the
// value carries no value at all; err is set when it names something that is not
// an integer, which PostgreSQL has no to_hex for.
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

// fnMySQLFormat implements MySQL FORMAT(x, d): x rounded to d decimal places and
// written with a comma every three digits. SQLite has a format() of its own, an
// alias of printf, so an untranslated call answered the first argument expanded
// as a format string instead.
func fnMySQLFormat(args []driver.Value) (driver.Value, error) {
	d, ok2 := toInt(args[1])
	if !ok2 {
		return nil, nil
	}
	// MySQL reads a negative number of decimal places as none, and caps the
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
	x, ok1 := toFloat(args[0])
	if !ok1 {
		return nil, nil
	}
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return nil, nil
	}
	text := strconv.FormatFloat(roundHalfAwayFromZero(x, d), 'f', int(d), 64)
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

// formatGrouped writes an integer already spelled in decimal the way MySQL's
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

// mysqlFormatMaxDecimals is the number of decimal places MySQL FORMAT keeps at
// most, which is the scale of its DECIMAL type.
const mysqlFormatMaxDecimals = 30

// fnGoogleSQLFormat implements GoogleSQL FORMAT(format, ...). The verbs it
// shares with printf are handed to Sprintf, so they answer what SQLite's printf
// answered before; %t and %T are BigQuery's own and are printed here. Left to
// SQLite they made the whole call NULL, since printf answers NULL for a format
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

// formatOperand coerces a value to the Go type the verb prints, the way SQLite's
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
// GoogleSQL's FORMAT prints it, MySQL's QUOTE answers it, PostgreSQL's
// quote_nullable answers it, and the keyword tables in this package hold it.
const nullText = "NULL"

// googlesqlPrintValue implements GoogleSQL's %t and %T: the printable form of a
// value, and the literal that would produce it. A boolean reaches here as the
// integer SQLite stores, so it prints as 0 or 1 rather than as false or true.
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
// datetime shapes. The Z07:00 pair reads a timezone suffix — a trailing Z or an
// offset like +09:00 — which BigQuery's TIMESTAMP literals carry.
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
