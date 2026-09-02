// Package runtime holds the scalar functions this module registers with the
// SQLite driver, for the semantics SQL alone cannot express: a division that
// answers what MySQL answers, a cast that converts the way PostgreSQL converts,
// a date part BigQuery names. Nothing here knows about parsing or lowering; a
// helper is reached by name, and the name is all the lowering layer has of it.
package runtime

import (
	"crypto/rand"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	"github.com/nao1215/filesql/dialect/internal/dialects"

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
// SPLIT_PART is available even under the dialects.SQLite dialect.
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

// mysqlFractionScale is how many digits of a second MySQL holds.
const mysqlFractionScale = 6

// formatDateTimeValueMySQL writes a datetime the way MySQL writes the result of
// date arithmetic: a value with a fraction gets all six digits, so a value that
// arrived as 13:45:59.100000 leaves as 13:45:59.100000 rather than as
// 13:45:59.1, and DATE_ADD with a zero interval is the identity it reads as.
//
// PostgreSQL trims the trailing zeros of a fraction and formatDateTimeValue
// keeps doing that for it, which is why the two exist side by side.
func formatDateTimeValueMySQL(tm time.Time) string {
	if tm.Nanosecond() == 0 {
		return tm.Format(layoutDateTime)
	}
	return formatDateTimeValueScaled(tm, mysqlFractionScale)
}

// formatDateTimeValueScaled writes a datetime with exactly scale digits of
// fraction, or none when scale is zero. It is what a helper that converts a
// value rather than moving it needs: MySQL takes the width from the value it
// was given, so TIMESTAMP('...59.1') keeps one digit and TIMESTAMP('...59.100000')
// keeps six.
func formatDateTimeValueScaled(tm time.Time, scale int) string {
	if scale <= 0 {
		return tm.Format(layoutDateTime)
	}
	if scale > mysqlFractionScale {
		scale = mysqlFractionScale
	}
	return tm.Format(layoutDateTime + "." + strings.Repeat("0", scale))
}

// mysqlFractionDigits is how many digits of a fraction of a second a value was
// written with, capped at the six MySQL holds. A value with no fraction, or one
// that is not text, is zero.
func mysqlFractionDigits(v driver.Value) int {
	s, ok := toString(v)
	if !ok {
		return 0
	}
	_, fraction, found := strings.Cut(strings.TrimSpace(s), ".")
	if !found {
		return 0
	}
	digits := 0
	for _, r := range fraction {
		if r < '0' || r > '9' {
			break
		}
		digits++
	}
	if digits > mysqlFractionScale {
		return mysqlFractionScale
	}
	return digits
}

// formatTimeOfDayValue is formatDateTimeValue for a value written as a time of
// day alone.
func formatTimeOfDayValue(tm time.Time) string {
	if tm.Nanosecond() == 0 {
		return tm.Format(layoutTimeOnly)
	}
	return tm.Format(layoutTimeOnly + ".999999")
}

// formatTimeOfDayValueScaled writes a time of day with exactly scale digits of
// fraction. MySQL takes that width from the value it was given, so
// TIME('13:45:59.100000') keeps six digits and TIME('13:45:59.1') keeps one.
func formatTimeOfDayValueScaled(tm time.Time, scale int) string {
	if scale <= 0 {
		return tm.Format(layoutTimeOnly)
	}
	if scale > mysqlFractionScale {
		scale = mysqlFractionScale
	}
	return tm.Format(layoutTimeOnly + "." + strings.Repeat("0", scale))
}

// Go reference-time fragments for month and weekday names, shared by the dialects.MySQL
// and dialects.PostgreSQL format mappings.
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
		// TIMESTAMPDIFF and TIMEDIFF need dialects.MySQL's own helpers: the former
		// counts complete units where BigQuery's date_diff counts boundaries,
		// and the latter would otherwise fall through to dialects.SQLite's timediff(),
		// which answers in dialects.SQLite's interval spelling.
		"mysql_date_diff": {3, fnMySQLDateDiff},
		"mysql_timediff":  {2, fnMySQLTimeDiff},
		"date_part":       {2, fnDatePart},
		// EXTRACT names each dialect's own helper, because the dialects
		// disagree on WEEK: dialects.PostgreSQL's is the ISO week, dialects.MySQL's and
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

		// The week functions carry dialects.MySQL's name because dialects.MySQL's week is not
		// everyone's: it starts on Sunday or Monday by mode, where dialects.PostgreSQL
		// has only the ISO week and BigQuery has both under separate names.
		"mysql_week":       {-1, fnMySQLWeek},
		"mysql_weekofyear": {1, fnMySQLWeekOfYear},
		"mysql_yearweek":   {-1, fnMySQLYearWeek},
		"locate":           {-1, mysqlTextArgs(fnLocate, 0, 1)},
		"lpad":             {-1, fnLpad},
		"rpad":             {-1, fnRpad},

		// LPAD and RPAD answer a negative length and an empty pad differently
		// per dialect, so each dialect's rewrite names its own helper; see
		// padRules.
		"mysql_lpad":      {-1, mysqlTextArgs(padFor(mysqlPadRules, true), 0, 2)},
		"mysql_rpad":      {-1, mysqlTextArgs(padFor(mysqlPadRules, false), 0, 2)},
		"postgresql_lpad": {-1, padFor(postgresqlPadRules, true)},
		"postgresql_rpad": {-1, padFor(postgresqlPadRules, false)},
		"substring_index": {3, mysqlTextArgs(fnSubstringIndex, 0, 1)},

		// SUBSTRING at position 0 and at a negative position is answered
		// differently by each dialect, and by dialects.SQLite's own substr(), so each
		// dialect's rewrite names its own helper.
		"mysql_substr":       {-1, mysqlTextArgs(fnMySQLSubstr, 0)},
		"postgresql_substr":  {-1, fnPostgreSQLSubstr},
		"googlesql_substr":   {-1, fnGoogleSQLSubstr},
		"dialect_round":      {2, fnDialectRound},
		"dialect_round_even": {-1, fnDialectRoundEven},
		"repeat":             {2, mysqlTextArgs(fnRepeat, 0)},
		"googlesql_repeat":   {2, fnGoogleSQLRepeat},
		"googlesql_lpad":     {-1, padFor(googlesqlPadRules, true)},
		"googlesql_rpad":     {-1, padFor(googlesqlPadRules, false)},
		"space":              {1, fnSpace},
		"truncate":           {2, fnTruncate},
		"reverse":            {1, mysqlTextArgs(fnReverse, 0)},
		"find_in_set":        {2, mysqlTextArgs(fnFindInSet, 0, 1)},
		"field":              {-1, fnField},
		"elt":                {-1, mysqlTextFrom(fnElt, 1)},
		"monthname":          {1, fnMonthName},
		"dayname":            {1, fnDayName},
		"last_day":           {1, fnLastDay},
		"from_unixtime":      {-1, fnFromUnixtime},

		// Shared by every dialect; dialects.SQLite spells them min/max with several
		// arguments, which collides with the aggregate forms.
		"least":          {-1, fnLeast},
		"greatest":       {-1, fnGreatest},
		"mysql_least":    {-1, fnMySQLLeast},
		"mysql_greatest": {-1, fnMySQLGreatest},

		// dialects.PostgreSQL's pair skips NULL arguments where the two above answer
		// NULL for the whole call.
		"postgresql_least":    {-1, fnPostgresLeast},
		"postgresql_greatest": {-1, fnPostgresGreatest},

		// Cast helpers. Each dialect's rewrite pass routes CAST through its own
		// helper so the conversion follows that dialect's rules rather than
		// dialects.SQLite's affinity; see cast.go.
		"mysql_cast":           {2, dialectCast(dialects.MySQL, false)},
		"mysql_format":         {2, fnMySQLFormat},
		"mysql_left":           {2, mysqlTextArgs(fnMySQLLeft, 0)},
		"mysql_right":          {2, mysqlTextArgs(fnMySQLRight, 0)},
		"mysql_regexp_replace": {-1, mysqlTextArgs(fnMySQLRegexpReplace, 0, 1)},
		"mysql_divide":         {2, divideFloat(false)},
		"mysql_mod":            {2, moduloDialect(false)},
		"mysql_bit_xor":        {2, dialectBitOp(dialects.MySQL, bitXor)},
		"mysql_bit_and":        {2, dialectBitOp(dialects.MySQL, bitAnd)},
		"mysql_bit_or":         {2, dialectBitOp(dialects.MySQL, bitOr)},
		"mysql_bit_not":        {1, dialectBitNot},
		"postgresql_bit_xor":   {2, fnBitXor},
		"googlesql_bit_xor":    {2, dialectBitOp(dialects.GoogleSQL, bitXor)},
		"googlesql_bit_and":    {2, dialectBitOp(dialects.GoogleSQL, bitAnd)},
		"googlesql_bit_or":     {2, dialectBitOp(dialects.GoogleSQL, bitOr)},
		"googlesql_bit_not":    {1, dialectBitNot},
		"interval_add":         {3, fnDateIntervalAdd},
		"interval_text_add":    {3, fnIntervalTextAdd},
		"date_trunc_part":      {2, fnDateTruncPart},
		"mysql_hex":            {1, fnMySQLHex},
		"mysql_unhex":          {1, fnMySQLUnhex},
		"mysql_soundex":        {1, mysqlTextArgs(fnMySQLSoundex, 0)},
		"googlesql_soundex":    {1, fnGoogleSQLSoundex},
		"dialect_replace":      {3, mysqlTextAll(fnDialectReplace)},
		"mysql_char":           {-1, fnMySQLChar},
		"mysql_json_type":      {1, fnMySQLJSONType},
		"mysql_quote":          {1, mysqlTextArgs(fnMySQLQuote, 0)},
		"mysql_ascii":          {1, mysqlTextArgs(fnMySQLASCII, 0)},
		"mysql_shift_left":     {2, mysqlShift(true)},
		"mysql_shift_right":    {2, mysqlShift(false)},
		// dialects.MySQL matches a regular expression under the collation of its
		// operands, and its default collation folds case; the shared regexp()
		// is right for dialects.PostgreSQL and BigQuery, which do not.
		"mysql_regexp":      {2, mysqlTextArgs(fnMySQLRegexp, 0, 1)},
		"like_sensitive":    {-1, mysqlTextArgs(likeCompare(true, true), 0, 1)},
		"like_insensitive":  {-1, mysqlTextArgs(likeCompare(false, false), 0, 1)},
		"similar_to":        {2, fnSimilarTo},
		"similar_substring": {3, fnSimilarSubstring},
		"mysql_ord":         {1, mysqlTextArgs(fnMySQLOrd, 0)},
		"mysql_nullif":      {2, fnMySQLNullif},
		"json_unquote":      {1, mysqlTextArgs(fnJSONUnquote, 0)},
		// mysql_text is the conversion on its own, for the calls the MySQL
		// lowering leaves on a function SQLite answers itself. There is nothing
		// to rename there -- SQLite trims and joins the way MySQL does -- so
		// only the argument is wrapped.
		"mysql_text":                {1, fnMySQLText},
		"overlay":                   {-1, fnOverlay},
		"strict_concat":             {-1, mysqlTextAll(fnStrictConcat)},
		"div":                       {2, integerDivide},
		"trunc_scale":               {2, truncateScale},
		"width_bucket":              {4, widthBucket},
		"postgresql_cast":           {2, dialectCast(dialects.PostgreSQL, false)},
		"postgresql_to_hex":         {1, fnPostgresToHex},
		"postgresql_regexp_replace": {-1, fnPostgresRegexpReplace},
		"postgresql_divide":         {2, divideSQLite},
		"postgresql_mod":            {2, moduloDialect(true)},
		"postgresql_format":         {-1, fnPostgresFormat},
		"scale":                     {1, fnPostgresScale},
		"min_scale":                 {1, fnPostgresMinScale},
		"trim_scale":                {1, fnPostgresTrimScale},
		"age":                       {2, fnPostgresAge},
		"pg_typeof":                 {1, fnPostgresTypeOf},
		"postgresql_json_typeof":    {1, fnPostgresJSONTypeOf},
		fnNamePostgresDateAdd:       {2, fnPostgresDateAdd},
		"postgresql_date_diff":      {2, fnPostgresDateDiff},
		"googlesql_cast":            {2, dialectCast(dialects.GoogleSQL, false)},
		"googlesql_divide":          {2, divideFloat(true)},
		"googlesql_mod":             {2, moduloDialect(true)},
		"googlesql_safe_cast":       {2, dialectCast(dialects.GoogleSQL, true)},

		// dialects.PostgreSQL helpers.
		"to_char":    {2, fnToChar},
		"to_date":    {2, fnToDate},
		"date_trunc": {2, fnDateTrunc},
		"split_part": {3, fnSplitPart},
		"initcap":    {1, fnInitcap},
		// dialects.SQLite's own upper() and lower() fold ASCII alone, which is not what
		// any of these dialects does; their calls are rewritten onto these.
		"unicode_upper":  {1, mysqlTextArgs(fnUnicodeUpper, 0)},
		"unicode_lower":  {1, mysqlTextArgs(fnUnicodeLower, 0)},
		"strpos":         {2, fnStrpos},
		"left":           {2, fnLeft},
		"right":          {2, fnRight},
		"regexp_replace": {-1, fnRegexpReplace},
		"md5":            {1, mysqlTextArgs(fnMD5, 0)},
		"ascii":          {1, fnASCII},
		"chr":            {1, fnChr},
		"translate":      {3, fnTranslate},

		// dialects.GoogleSQL helpers.
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
		// Registering them as deterministic is what fixes them: dialects.SQLite computes
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
		"to_seconds":              {1, fnMySQLToSeconds},
		"mysql_timestamp":         {-1, fnMySQLTimestamp},
		"convert_tz":              {3, fnMySQLConvertTZ},
		"mysql_interval_compound": {4, fnMySQLIntervalCompound},
		"mysql_interval_add":      {3, fnMySQLDateIntervalAdd},
		"mysql_date":              {1, fnMySQLDate},
		"current_datetime":        {-1, fnCurrentDatetime},
	}
	// The dialects.MySQL-only helpers live in their own file, because there are enough of
	// them that listing them here would bury the ones every dialect shares.
	maps.Copy(det, mysqlScalarFunctions())
	maps.Copy(det, mysqlTimeFunctions())
	maps.Copy(det, postgresqlScalarFunctions())
	maps.Copy(det, googlesqlScalarFunctions())
	for name, spec := range det {
		if err := registerScalar(name, spec, true); err != nil {
			return err
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
		if err := registerScalar(name, spec, false); err != nil {
			return err
		}
	}
	maps.Copy(registeredFunctions, nondet)
	return nil
}

// nonDeterministicFunctions is every helper dialects.SQLite must call again for each row.
// Two kinds belong here and nothing else does: the ones that are meant to give
// a different answer every time they are asked -- a random number, a fresh UUID
// -- and dialects.PostgreSQL's changing clock, which is the whole of what separates
// clock_timestamp and timeofday from now and statement_timestamp. Everything
// that reads the clock once at the start of the statement is registered as
// deterministic instead, which is what makes one statement see one reading.
func nonDeterministicFunctions() map[string]scalarSpec {
	nondet := map[string]scalarSpec{
		"rand":          {0, fnRand},
		"generate_uuid": {0, fnGenerateUUID},
	}
	// dialects.GoogleSQL has nothing to add: BigQuery fixes CURRENT_DATETIME at the
	// start of the statement, like the rest of its CURRENT_ family.
	maps.Copy(nondet, postgresqlNonDeterministicFunctions())
	return nondet
}

// RegisteredNames lists every helper this package registers with the driver.
// The lowering layer keeps its own copy of the list, because the dependency
// runs one way; a test holds the two together.
func RegisteredNames() []string {
	if err := RegisterFunctions(); err != nil {
		return nil
	}
	names := make([]string, 0, len(registeredFunctions))
	for name := range registeredFunctions {
		names = append(names, name)
	}
	return names
}

// RegisteredArity is how many arguments a helper takes, with -1 for one that
// takes any number. It is what the driver refuses a call by, so the lowering
// layer reads it to refuse the same call under the name the caller wrote.
func RegisteredArity(name string) (int, bool) {
	if err := RegisterFunctions(); err != nil {
		return 0, false
	}
	spec, found := registeredFunctions[name]
	if !found {
		return 0, false
	}
	return int(spec.nArg), true
}

// registeredFunctions is every scalar function this package computes itself,
// which is what the SAFE prefix can promise a NULL for: a function SQLite
// computes is out of reach from here. It is written once, during registration,
// before any connection can exist.
var registeredFunctions map[string]scalarSpec //nolint:gochecknoglobals // the table registerAll built, read by safe_call

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
		return fn(copyArgs(args))
	}
}

// registerScalar registers one helper with the driver.
//
// The registration asks for volatile arguments, which is the driver's only way
// to hand over a text value whole: with them off it reads a TEXT argument as a
// C string, which ends at the first zero byte, so a value SQLite holds as
// "a\x00b" reached a helper as "a". A zero byte is a character MySQL,
// PostgreSQL and SQLite all store inside a string and a CSV cell can carry, and
// a helper that answered about a shorter value than the one in the database
// disagreed with SQLite's own built-ins about the same cell.
//
// The driver's own contract for a volatile argument is that it is a view into
// memory SQLite reuses after the call, so copyArgs takes a copy of every string
// and byte slice before the helper sees it. That is the allocation the driver
// would have made itself with volatile arguments off, so the cost is the same
// and the bytes are all there.
func registerScalar(name string, spec scalarSpec, deterministic bool) error {
	impl := &sqlite.FunctionImpl{
		NArgs:         spec.nArg,
		Deterministic: deterministic,
		Scalar:        wrapScalar(spec.fn),
		VolatileArgs:  true,
	}
	if err := sqlite.RegisterFunction(name, impl); err != nil {
		return fmt.Errorf("dialect: register %s: %w", name, err)
	}
	return nil
}

// copyArgs moves a call's text and blob arguments into memory this package
// owns. See registerScalar for why the driver hands over memory it does not.
func copyArgs(args []driver.Value) []driver.Value {
	for i, arg := range args {
		switch v := arg.(type) {
		case string:
			args[i] = string(append([]byte(nil), v...))
		case []byte:
			args[i] = append([]byte(nil), v...)
		}
	}
	return args
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
		return formatFloatText(x), true
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
		return int64FromFloat(x), true
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
// times to repeat, how many spaces, which element of a list. dialects.MySQL rounds such a
// value half away from zero, so REPEAT('ab', 2.7) repeats three times, where
// truncating toward zero left it one short.
//
// A string takes the number its leading run spells and truncates it, which is
// the other rule dialects.MySQL has here: REPEAT('ab', '2.5') repeats twice where
// REPEAT('ab', 2.5) repeats three times, and a string spelling no number counts
// zero. Requiring the whole string to parse as an integer made every count read
// from a text column -- which is what a cell loaded from a CSV file is -- answer
// NULL for the whole call.
func toCount(v driver.Value) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return int64FromFloat(math.Round(x)), true
	case string:
		return int64(leadingNumber(x)), true
	case []byte:
		return int64(leadingNumber(string(x))), true
	}
	return toInt(v)
}

// int64FromFloat is x as an int64, with an answer for the values a conversion
// does not have one for. Go leaves int64(x) implementation-defined when x is
// outside the range and for NaN, which on amd64 is the minimum -- so a helper
// given a count of 1e308 read it as the most negative number there is and
// indexed a slice with it. A magnitude past the range is the bound it passed,
// which is what a caller writing such a number means, and NaN is no number at
// all.
func int64FromFloat(x float64) int64 {
	switch {
	case math.IsNaN(x):
		return 0
	case x >= math.MaxInt64:
		return math.MaxInt64
	case x <= math.MinInt64:
		return math.MinInt64
	default:
		return int64(x)
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

// --- common / dialects.MySQL scalar functions ---

// regexpCache remembers compiled patterns. It is bounded because a pattern can
// be a column: SELECT ... WHERE v REGEXP p compiles one per distinct value of
// p, and an unbounded map keyed by data kept them for the life of the process,
// long after the database they came from was closed.
//
//nolint:gochecknoglobals // a process-wide cache, bounded; see boundedCache
var regexpCache boundedCache[*regexp.Regexp]

// compileRegexp compiles pattern, caching the result up to the cache's bound.
// Compilation errors are returned so the caller can surface them.
func compileRegexp(pattern string) (*regexp.Regexp, error) {
	return regexpCache.get(pattern, func() (*regexp.Regexp, error) {
		return regexp.Compile(pattern)
	})
}

// fnRegexp implements REGEXP(pattern, subject), the function dialects.SQLite invokes for
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

// fnMySQLRegexp implements the dialects.MySQL "subject REGEXP pattern" operator. It is
// fnRegexp with dialects.MySQL's default collation applied, which folds case over the
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

// isTruthy applies dialects.SQLite-like truthiness: NULL and zero are false.
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

// leadingNumber reads a string the way dialects.MySQL and dialects.SQLite both read one in a
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
// quite onto the statement: dialects.SQLite folds each occurrence of the call once, so a
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
// the same on every machine, and it is the one dialects.SQLite's own CURRENT_TIMESTAMP
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

// mysqlToGoLayout maps the dialects.MySQL DATE_FORMAT specifiers to Go reference-time
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
// width differs from their formatting width. dialects.MySQL pads a number on output and
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

// mysqlLayoutFor is the Go layout fragment a dialects.MySQL specifier means, in the
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

// fnDateFormat implements dialects.MySQL DATE_FORMAT(date, format).
func fnDateFormat(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	format, ok2 := toString(args[1])
	if !ok || !ok2 {
		return nil, nil
	}
	if format == "" {
		// MySQL answers NULL for an empty format rather than the empty string.
		// Nothing it can write is empty, so the two are distinguishable, and
		// the empty string here was indistinguishable from a format that
		// produced one.
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

// mysqlShortYear moves a two-digit year onto MySQL's pivot. Go reads 69 as 1969
// and MySQL reads it as 2069: MySQL puts 00 to 69 in the 2000s and 70 to 99 in
// the 1900s, so the two rules differ on that one value and agree on every other.
func mysqlShortYear(tm time.Time, fromTwoDigits bool) time.Time {
	if !fromTwoDigits || tm.Year() != 1969 {
		return tm
	}
	return tm.AddDate(100, 0, 0)
}

// padDayOfYear zero-pads the day of year in s to the three digits Go's layout
// element for it requires. MySQL reads one to three digits there, so
// STR_TO_DATE('2020 60', '%Y %j') is the sixtieth day and not a NULL.
//
// before is the part of the format that precedes the specifier; its trailing
// run of literal characters is what locates the number in s. A specifier with
// no literal in front of it is left alone, since nothing anchors it.
func padDayOfYear(before, s string) string {
	anchor := before
	if at := strings.LastIndexByte(anchor, '%'); at >= 0 {
		anchor = anchor[at+2:]
	}
	start := 0
	if anchor != "" {
		at := strings.Index(s, anchor)
		if at < 0 {
			return s
		}
		start = at + len(anchor)
	} else if before != "" {
		// A specifier straight after another one: nothing separates the two
		// numbers, so padding could only guess where the day of year begins.
		return s
	}
	end := start
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	if end == start || end-start >= 3 {
		return s
	}
	return s[:start] + strings.Repeat("0", 3-(end-start)) + s[start:]
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

// weekModeYear is the dialects.MySQL week-mode flag that makes the count belong to a
// year rather than start over at week 0, which is the difference between WEEK
// and YEARWEEK at the turn of a year.
const weekModeYear = 2

// mysqlWeek is the week number of tm under one of dialects.MySQL's week modes, and the
// year that week belongs to.
//
// dialects.MySQL numbers weeks four ways and DATE_FORMAT reaches all four: %U is mode 0,
// %u mode 1, %V mode 2 and %v mode 3. Two things vary. A week may start on
// Sunday (modes 0 and 2) or on Monday (1 and 3), and week 1 may be the first
// week holding a day of the new year's first weekday (0 and 2) or the first
// week holding four or more days of the new year (1 and 3, the ISO rule).
// Modes 0 and 1 number from zero, so the first days of January can be week 0;
// modes 2 and 3 have no week 0 and lend those days to the previous year's last
// week, which is why %X and %x exist to say which year the number belongs to.
//
// This follows dialects.MySQL's own calculation rather than deriving one, because the
// years where the four disagree are exactly the ones a derivation gets wrong:
// 2024-12-31 is week 53 by %u and week 1 of 2025 by %v.
func mysqlWeek(tm time.Time, mode int) (week, year int) {
	// dialects.MySQL turns a mode into three flags, inverting the "four or more days"
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

// fnStrToDate implements a pragmatic dialects.MySQL STR_TO_DATE(str, format): it parses
// str against a Go layout derived from the dialects.MySQL format and returns a canonical
// datetime string.
func fnStrToDate(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	format, ok2 := toString(args[1])
	if !ok || !ok2 {
		return nil, nil
	}
	var layout strings.Builder
	var sawYear, sawMonth, sawDay, sawWeekday, sawTime, sawFraction, sawShortYear bool
	for i := 0; i < len(format); i++ {
		if format[i] == '%' && i+1 < len(format) {
			spec := format[i+1]
			if spec == 'y' {
				sawShortYear = true
			}
			switch {
			case spec == 'j':
				s = padDayOfYear(format[:i], s)
				// The day of the year, which Go's layout spells 002 and
				// no MySQL-to-Go table entry could, because the formatting
				// direction writes it from a computed field. Together with a
				// year it is a whole date, so it stands for the month and the
				// day the completeness check below asks for.
				layout.WriteString("002")
				sawMonth, sawDay = true, true
			case spec == 'f' && strings.HasSuffix(layout.String(), "."):
				// The microseconds, which Go reads as part of the seconds and
				// so takes the point with them. MySQL pads a short fraction on
				// the right, and so does Go: ".1" is a tenth of a second.
				current := layout.String()
				layout.Reset()
				layout.WriteString(current[:len(current)-1])
				layout.WriteString(".999999")
				sawTime, sawFraction = true, true
			default:
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
	tm = mysqlShortYear(tm, sawShortYear)
	// The shape follows the format: a DATE for date specifiers, a TIME for
	// time specifiers, a DATETIME for both. A format that names part of a date
	// without completing it is NULL, which is dialects.MySQL's answer under its default
	// sql_mode rather than a default-filled date.
	hasDate := sawYear || sawMonth || sawDay || sawWeekday
	if hasDate && (!sawYear || !sawMonth || !sawDay) {
		return nil, nil
	}
	clock := layoutTimeOnly
	if sawFraction {
		// MySQL writes the six digits, so a fraction of a tenth is .100000.
		clock += ".000000"
	}
	switch {
	case hasDate && sawTime:
		return tm.Format(layoutDateOnly + " " + clock), nil
	case hasDate:
		return tm.Format(layoutDateOnly), nil
	case sawTime:
		return tm.Format(clock), nil
	default:
		return nil, nil
	}
}

// parseLayout parses s against a single Go layout, reporting success.
func parseLayout(layout, s string) (time.Time, bool) {
	tm, err := time.Parse(layout, s)
	return tm, err == nil
}

// fnDateDiff implements dialects.MySQL DATEDIFF(a, b) = whole days from b to a.
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
// an integer. It also backs the dialects.MySQL YEAR/MONTH/... helpers and the EXTRACT
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
	// dialects.PostgreSQL's second carries the fraction of a second with it, so
	// DATE_PART('second', '10:11:12.5') is 12.5. dialects.MySQL's SECOND() and
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
		// dialects.PostgreSQL DOW: Sunday=0..Saturday=6, which is Go's own numbering.
		// This spelling is dialects.PostgreSQL's; dayofweek below is dialects.MySQL's and
		// dialects.GoogleSQL's, and the two number the week differently.
		return int64(tm.Weekday()), nil
	case "isodow":
		// dialects.PostgreSQL ISODOW: Monday=1..Sunday=7.
		return int64(weekdayIndex(tm, true)) + 1, nil
	case unitDayOfWeek:
		// dialects.MySQL DAYOFWEEK: Sunday=1..Saturday=7.
		return int64(tm.Weekday()) + 1, nil
	case unitWeekday:
		// dialects.MySQL WEEKDAY: Monday=0..Sunday=6.
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
	case unitDecade:
		return int64(decadeOf(tm.Year())), nil
	case unitCentury:
		return int64(centuryOf(tm.Year())), nil
	case unitMillennium:
		return int64(millenniumOf(tm.Year())), nil
	case unitMillisecondsPlural, unitMillisecond:
		return secondsWithFraction(tm) * 1000, nil
	case unitMicrosecondsPlural, unitMicrosecond:
		// A microsecond is the finest a dialects.PostgreSQL timestamp holds, so the
		// count is always whole; answering it as an integer also keeps dialects.SQLite
		// from spelling a large REAL in exponent form.
		return int64(math.Round(secondsWithFraction(tm) * 1000000)), nil
	case "julian":
		// The Julian date, which is a fixed offset from the day count dialects.SQLite
		// already computes: 2440588 is 1970-01-01. A timestamp away from
		// midnight carries the fraction of the day with it, so noon on a day is
		// that day's number and a half.
		day := dayNumber(tm) + 2440588
		if fraction := secondsWithFraction(tm) + float64(tm.Hour()*3600+tm.Minute()*60); fraction != 0 {
			return float64(day) + fraction/86400, nil
		}
		return day, nil
	case "epoch":
		return tm.Unix(), nil
	case "date":
		// dialects.GoogleSQL allows the date and time parts of a timestamp to be
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
// fraction of a second it carries. dialects.PostgreSQL's second, milliseconds and
// microseconds parts are all built from it, which is why 12.5 seconds is 12500
// milliseconds rather than 500.
func secondsWithFraction(tm time.Time) float64 {
	return float64(tm.Second()) + float64(tm.Nanosecond())/1e9
}

// decadeOf, centuryOf and millenniumOf number the year the way dialects.PostgreSQL does.
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

// fnGoogleSQLDatePart implements EXTRACT under dialects.GoogleSQL. BigQuery's WEEK
// begins on Sunday and numbers the days before the year's first Sunday as week
// 0 — dialects.MySQL's week mode 0 — and ISOWEEK and ISOYEAR are the ISO pair whose week
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
	// alone, where dialects.PostgreSQL's are the seconds field with the fraction scaled
	// into it, and dialects.PostgreSQL answers to both spellings. So 13:04:05.123 is
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

// fnMySQLDatePart implements EXTRACT under dialects.MySQL, whose WEEK is WEEK(x) with
// the session's default_week_format — 0 by default, the Sunday-first numbering
// mysqlWeek computes. Everything else follows the shared helper, including NULL
// for a value that does not parse, which is dialects.MySQL's own answer.
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
		// dialects.MySQL's MICROSECOND is the fractional part alone, where dialects.PostgreSQL's
		// MICROSECONDS is the seconds field multiplied out and so carries the
		// whole seconds with it. The shared helper answers dialects.PostgreSQL's, which
		// made EXTRACT(MICROSECOND FROM ...) answer dialects.MySQL's SECOND_MICROSECOND
		// value -- a plausible number a million times too large.
		return int64(tm.Nanosecond() / 1000), nil
	}
	if value, composite, err := mysqlCompositePart(unit, tm); composite {
		return value, err
	}
	return datePartValue(unit, tm)
}

// weekMode reads the optional mode argument dialects.MySQL's week functions take,
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

// fnMySQLWeek implements dialects.MySQL WEEK(date[, mode]). Mode 0, the default, starts
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

// fnMySQLWeekOfYear implements dialects.MySQL WEEKOFYEAR(date), which is WEEK(date, 3):
// the ISO week, starting on Monday and numbering from one.
func fnMySQLWeekOfYear(args []driver.Value) (driver.Value, error) {
	tm, ok := toStringTime(args[0])
	if !ok {
		return nil, nil
	}
	week, _ := mysqlWeek(tm, 3)
	return int64(week), nil
}

// fnMySQLYearWeek implements dialects.MySQL YEARWEEK(date[, mode]) as year*100 + week.
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

// fnLocate implements dialects.MySQL LOCATE(substr, str[, pos]) returning a 1-based
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
			// dialects.MySQL answers NULL when any argument is NULL, which the other two
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
	// MySQL's default collation folds case, so a needle finds a letter in the
	// other case. The folding maps one rune to one rune, so the position the
	// search reports is a position in the value the caller passed.
	return int64(characterIndex(foldCase(str), foldCase(substr), start)), nil
}

func fnLpad(args []driver.Value) (driver.Value, error) { return pad(args, true, padRules{}) }
func fnRpad(args []driver.Value) (driver.Value, error) { return pad(args, false, padRules{}) }

// padRules holds the two boundary answers LPAD and RPAD differ on between the
// dialects, checked against dialects.MySQL 8.4 and dialects.PostgreSQL 17.
//
//   - A negative length: dialects.MySQL answers NULL, dialects.PostgreSQL an empty string, and
//     BigQuery refuses the call.
//   - An empty pad with a length past the input: dialects.MySQL cannot reach the length
//     with nothing to pad with and answers an empty string, dialects.PostgreSQL returns
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
// two arguments pads with spaces, which is dialects.PostgreSQL's short form.
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
	name := fnNameRpad
	if left {
		name = fnNameLpad
	}
	if err := checkStringLength(name, 1, n); err != nil {
		return nil, err
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

// fnMySQLSubstr implements dialects.MySQL SUBSTRING(s, pos[, len]), which dialects.SQLite's own
// substr() does not match at position 0: dialects.MySQL reads 0 as no position at all and
// answers the empty string, where dialects.SQLite reads it as the place before the first
// character and answers the whole string. That matters because 0 is what LOCATE
// returns when it finds nothing, so SUBSTRING(s, LOCATE('x', s)) is empty in
// dialects.MySQL for a row without an x and the whole of s under dialects.SQLite's rule.
//
// A negative position counts from the end, which dialects.SQLite does agree on, and the
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

// fnPostgreSQLSubstr implements dialects.PostgreSQL substr(s, start[, count]). Positions
// are counted from 1 and a start below 1 is not an offset from the end: the
// result is the characters at positions start through start+count-1 that the
// string actually has, so substr('abcdef', -1, 3) covers positions -1, 0 and 1
// and yields "a". dialects.SQLite's substr() reads a negative start from the end instead,
// which is dialects.MySQL's rule, and answered "f".
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
// dialects.PostgreSQL, where the out-of-range prefix eats the length, and the first is
// what separates it from dialects.MySQL, where position 0 is no position at all.
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

// fnDialectRound implements ROUND(x, n) for the digit counts dialects.SQLite's own
// round() will not take.
//
// A negative n rounds to a power of ten -- ROUND(12345, -2) is 12300 -- which is
// how dialects.MySQL, dialects.PostgreSQL and BigQuery all spell "round to the nearest hundred".
// dialects.SQLite reads the second argument as digits after the decimal point and ignores
// a negative one, so the call succeeded and returned its input. All three
// engines agree on every answer, half away from zero, so one helper serves them;
// dialect.SQLite is not rewritten onto it, because ignoring a negative count is
// dialects.SQLite's documented behavior and is what a caller who named no dialect asked
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
		// What dialects.SQLite already does, kept here so one function answers the whole
		// call rather than the rewrite having to decide which one to emit.
		return roundHalfAwayFromZero(value, digits), nil
	}
	// Below the smallest power of ten a float64 holds, the whole value is under
	// the rounding unit: ROUND(12345, -400) is 0 in dialects.MySQL and in BigQuery. The
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

// fnDialectRoundEven is fnDialectRound with dialects.MySQL's and dialects.PostgreSQL's tie rule.
// Both round a floating-point argument to the even neighbor: ROUND(2.5) is 2
// and ROUND(3.5) is 4, where dialects.SQLite's own round() and BigQuery both answer 3
// and 4. Every non-integer value dialects.SQLite holds is a float, so this is the rule
// that matches what a REAL column loaded from a file does in either engine.
//
// The one case it cannot match is a decimal literal written in the query:
// dialects.MySQL reads 2.5 as an exact decimal and answers 3, and dialects.SQLite has no type to
// tell that from the double 2.5e0, which dialects.MySQL answers 2 for. The column is the
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
// binary value itself, which is what makes 2.675 round to 2.68 the way dialects.MySQL
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
// and what dialects.SQLite's round() does.
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

// fnSubstringIndex implements dialects.MySQL SUBSTRING_INDEX(str, delim, count).
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
	// The counts are compared as int64 and the negative one is answered before
	// it is negated: negating the smallest int64 leaves it where it was, so
	// "the last -9223372036854775808 parts" indexed the slice from far below
	// zero and panicked. A magnitude that reaches the number of parts already
	// answers with the whole string, and the smallest count is such a one.
	if count >= int64(len(parts)) || count <= -int64(len(parts)) {
		return s, nil
	}
	if count > 0 {
		return strings.Join(parts[:count], delim), nil
	}
	return strings.Join(parts[int64(len(parts))+count:], delim), nil
}

// fnRepeat implements dialects.MySQL REPEAT(str, count), which answers the empty string
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
	if err := checkStringLength(fnNameRepeat, int64(len(s)), count); err != nil {
		return nil, err
	}
	return strings.Repeat(s, int(count)), nil
}

// maxStringLength is the longest string a helper will build. It is SQLite's own
// SQLITE_MAX_LENGTH default, which is the length past which the engine refuses a
// string with "string or blob too big", so a helper's answer and SQLite's agree
// about what can exist.
const maxStringLength = 1_000_000_000

// checkStringLength refuses a result too long to hold, before it is built. The
// product is not computed: it is what overflows, and strings.Repeat answers an
// overflow with a panic, which inside a SQLite user function goes up through the
// driver and ends the caller's process.
func checkStringLength(name string, unit, count int64) error {
	if unit == 0 || count <= 0 {
		return nil
	}
	if count > int64(maxStringLength)/unit {
		return fmt.Errorf("dialect: %s would build a string of %d x %d bytes, past the %d a string can hold",
			name, unit, count, maxStringLength)
	}
	return nil
}

// soundexRules are the three things the dialects differ on, all of them read
// off mysql:8.4 and the BigQuery emulator rather than assumed.
//
//   - dialects.MySQL emits one digit per coded consonant however many there are, so
//     SOUNDEX('Hello World') is H4643; BigQuery stops at three, giving H464.
//   - dialects.MySQL upper-cases an ASCII first letter, so SOUNDEX('hello') is H400;
//     BigQuery keeps it as written and answers h400.
//   - dialects.MySQL treats any Unicode letter as the first letter and writes it back
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

// soundexWith implements SOUNDEX under one dialect's rules. dialects.SQLite's own
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
		// string, where dialects.SQLite answers its "?000" placeholder.
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

// soundexCode is the digit dialects.MySQL gives a letter, or zero for a letter it does
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
// NULL rule every dialect here has and dialects.SQLite does not: dialects.SQLite short-circuits on
// an empty search string and answers the subject without looking at the
// replacement, so REPLACE('hello', ”, NULL) answered 'hello' where dialects.MySQL and
// dialects.PostgreSQL answer NULL. A NULL that should have traveled through the
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

// fnMySQLChar implements dialects.MySQL CHAR(n, ...), which builds bytes where dialects.SQLite's
// char() builds code points: dialects.MySQL answers the single zero byte for CHAR(0),
// which dialects.SQLite drops entirely, and the two bytes 0x01 0x00 for CHAR(256), which
// dialects.SQLite encodes as the UTF-8 of U+0100. A NULL argument is skipped rather than
// making the call NULL, which is dialects.MySQL's rule for this one function.
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
		// dialects.MySQL takes each argument modulo 2^32 and writes its bytes
		// big-endian, dropping the leading zero bytes but keeping one byte for
		// an argument of zero.
		u := uint32(n) //nolint:gosec // reinterpreting the bits is dialects.MySQL's rule here
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

// fnPostgresJSONTypeOf implements json_typeof and jsonb_typeof. dialects.SQLite's
// json_type is the same walk over the document, but it answers with dialects.SQLite's
// own type names -- text, integer, real, true, false -- where dialects.PostgreSQL
// answers with the names JSON itself defines.
func fnPostgresJSONTypeOf(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil //nolint:nilnil // a NULL document has no type
	}
	value, err := decodeWholeJSON(s)
	if err != nil {
		return nil, err
	}
	switch value.(type) {
	case nil:
		return "null", nil
	case bool:
		return "boolean", nil
	case json.Number:
		return "number", nil
	case string:
		return "string", nil
	case []any:
		return "array", nil
	default:
		return "object", nil
	}
}

// fnMySQLJSONType implements dialects.MySQL's JSON_TYPE, which names the type in upper
// case where dialects.SQLite's json_type answers lower case, so a query comparing the
// result against the name dialects.MySQL's documentation prints matched nothing.
func fnMySQLJSONType(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	value, err := decodeWholeJSON(s)
	if err != nil {
		return nil, err
	}
	switch v := value.(type) {
	case nil:
		return "NULL", nil
	case bool:
		return "BOOLEAN", nil
	case string:
		return typeNameString, nil
	case []any:
		return "ARRAY", nil
	case map[string]any:
		return "OBJECT", nil
	case json.Number:
		if _, err := v.Int64(); err == nil {
			return "INTEGER", nil
		}
		return "DOUBLE", nil
	default:
		return nil, fmt.Errorf("dialect: %q is not valid JSON", s)
	}
}

// decodeWholeJSON decodes one JSON document, with the whole of the text having
// to be that document: "1 2" is not the integer 1 with something ignored after
// it. Numbers stay json.Number so the caller can tell an integer from a double.
func decodeWholeJSON(text string) (any, error) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("dialect: %q is not valid JSON", text)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("dialect: %q is not valid JSON", text)
	}
	return value, nil
}

// fnSpace implements dialects.MySQL SPACE(n).
func fnSpace(args []driver.Value) (driver.Value, error) {
	n, ok := toCount(args[0])
	if !ok {
		return nil, nil
	}
	if n <= 0 {
		return "", nil
	}
	if err := checkStringLength(fnNameSpace, 1, n); err != nil {
		return nil, err
	}
	return strings.Repeat(" ", int(n)), nil
}

// fnTruncate implements dialects.MySQL TRUNCATE(x, d): truncate x to d decimal places
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
// list. A NULL argument makes the whole call NULL, which is what dialects.MySQL and
// dialects.GoogleSQL answer; dialects.PostgreSQL skips its NULLs and reaches the pair below
// instead.
func fnLeast(args []driver.Value) (driver.Value, error) { return extremum(args, true, false) }

func fnMySQLLeast(args []driver.Value) (driver.Value, error) {
	return extremumWith(args, true, false, true)
}

func fnMySQLGreatest(args []driver.Value) (driver.Value, error) {
	return extremumWith(args, false, false, true)
}

func fnGreatest(args []driver.Value) (driver.Value, error) { return extremum(args, false, false) }

// fnPostgresLeast and fnPostgresGreatest implement dialects.PostgreSQL's LEAST and
// GREATEST, which ignore their NULL arguments and answer NULL only when every
// argument is NULL. An empty cell loads as NULL, so under the other rule a row
// missing one of the columns being compared reports no extreme at all.
func fnPostgresLeast(args []driver.Value) (driver.Value, error) { return extremum(args, true, true) }

func fnPostgresGreatest(args []driver.Value) (driver.Value, error) {
	return extremum(args, false, true)
}

func extremum(args []driver.Value, wantSmaller, skipNulls bool) (driver.Value, error) {
	return extremumWith(args, wantSmaller, skipNulls, false)
}

// extremumWith is extremum with the collation named: dialects.MySQL's default collation
// folds case, so GREATEST('a', 'B') is 'B' there and 'a' under a byte-order
// comparison. STRCMP and the LIKE and REGEXP helpers already fold for the same
// reason, so leaving these two alone had one dialect disagreeing with itself
// about which of two strings is larger.
func extremumWith(args []driver.Value, wantSmaller, skipNulls, foldCaseCompare bool) (driver.Value, error) {
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
		switch {
		case allNumeric:
			smaller = nums[i] < nums[best]
		case foldCaseCompare:
			smaller = foldCase(strs[i]) < foldCase(strs[best])
		default:
			smaller = strs[i] < strs[best]
		}
		if smaller == wantSmaller {
			best = i
		}
	}
	return args[best], nil
}

// fnReverse implements dialects.MySQL REVERSE, reversing runes rather than bytes so
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

// fnFindInSet implements dialects.MySQL FIND_IN_SET(needle, "a,b,c"): the 1-based
// position of needle in the comma-separated list, or 0 when it is absent.
func fnFindInSet(args []driver.Value) (driver.Value, error) {
	needle, ok1 := toString(args[0])
	set, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	// An empty set holds nothing, including the empty string. Splitting it
	// yields one empty element, which found an empty needle at position 1 where
	// dialects.MySQL answers 0. An empty element inside a non-empty set is a real
	// element and keeps its position.
	if set == "" {
		return int64(0), nil
	}
	// The comparison folds case, which is what MySQL's default collation does
	// for this call as it does for LIKE.
	needle = foldCase(needle)
	for i, part := range strings.Split(set, ",") {
		if foldCase(part) == needle {
			return int64(i + 1), nil
		}
	}
	return int64(0), nil
}

// fnField implements dialects.MySQL FIELD(x, a, b, ...): the 1-based position of the
// first argument that equals x, or 0 when none does.
func fnField(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("dialect: FIELD expects at least 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return int64(0), nil
	}
	asText := fieldComparesAsText(args)
	for i, a := range args[1:] {
		if a != nil && fieldEqual(args[0], a, asText) {
			return int64(i + 1), nil
		}
	}
	return int64(0), nil
}

// fieldComparesAsText reports whether FIELD compares its arguments as strings,
// which MySQL does when every argument it was given is one. With a number among
// them the comparison is numeric, and a string that does not spell a number
// reads as zero there, which is what makes FIELD(0, 'x') answer 1. A NULL never
// matches and does not decide which comparison the others get.
func fieldComparesAsText(args []driver.Value) bool {
	for _, a := range args {
		switch a.(type) {
		case nil, string, []byte:
		default:
			return false
		}
	}
	return true
}

// fieldEqual is one comparison of a FIELD call, under whichever of the two
// readings the arguments asked for. Two integers are compared as integers, so a
// pair past what a float64 holds exactly does not match itself into the wrong
// position.
func fieldEqual(needle, candidate driver.Value, asText bool) bool {
	if asText {
		a, aok := toString(needle)
		b, bok := toString(candidate)
		return aok && bok && fnFoldCase(a) == fnFoldCase(b)
	}
	if a, ok := needle.(int64); ok {
		if b, ok := candidate.(int64); ok {
			return a == b
		}
	}
	a, aok := mysqlNumericArgument(needle)
	b, bok := mysqlNumericArgument(candidate)
	return aok && bok && a == b
}

// fnElt implements dialects.MySQL ELT(n, a, b, ...): the nth argument, or NULL when n is
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

// fnMonthName and fnDayName implement the dialects.MySQL MONTHNAME/DAYNAME helpers.
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

// fnLastDay implements dialects.MySQL LAST_DAY(date): the last day of that month. It is
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

// fnMySQLTimeOfDay implements dialects.MySQL's one-argument TIME(x): the time part of a
// datetime, with whatever fraction of a second the value carried. dialects.SQLite has a
// time() of its own that takes modifiers and answers at second resolution, so
// the fraction written in the value was dropped.
func fnMySQLTimeOfDay(args []driver.Value) (driver.Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("dialect: TIME expects 1 argument, got %d", len(args))
	}
	if tm, ok := toStringTime(args[0]); ok && hasTimeOfDay(args[0]) {
		return formatTimeOfDayValueScaled(tm, mysqlFractionDigits(args[0])), nil
	}
	// A value with no clock in it is read as a number the way the cast to TIME
	// reads one, so TIME('2024-03-05') is 00:20:24 as dialects.MySQL answers rather than
	// the midnight a date formatted as a time would give.
	return mysqlTimeFromNumber(args[0]), nil
}

// fnUnixTimestamp implements dialects.MySQL UNIX_TIMESTAMP([date]): the current epoch
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
		// dialects.MySQL answers a real when the value carries a fraction, so a
		// microsecond-resolution timestamp does not lose it here.
		return float64(tm.Unix()) + float64(nanos)/1e9, nil
	}
	return tm.Unix(), nil
}

// fnFromUnixtime implements dialects.MySQL FROM_UNIXTIME(seconds[, format]), the inverse
// of UNIX_TIMESTAMP.
func fnFromUnixtime(args []driver.Value) (driver.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("dialect: FROM_UNIXTIME expects 1 or 2 arguments, got %d", len(args))
	}
	seconds, ok := toFloat(args[0])
	if !ok || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return nil, nil
	}
	// dialects.MySQL answers NULL outside the range of its TIMESTAMP type, 1970-01-01
	// 00:00:00 UTC through 3001-01-18 23:59:59 (32536771199 seconds).
	if seconds < 0 || seconds > 32536771199 {
		return nil, nil
	}
	// The fraction is part of the answer: MySQL reads FROM_UNIXTIME(1.5) as a
	// second and a half, and truncating it to a whole second lost it.
	sec := int64(seconds)
	micros := int64(math.Round((seconds - float64(sec)) * 1e6))
	tm := time.Unix(sec, micros*int64(time.Microsecond)).UTC()
	if len(args) == 1 {
		return formatDateTimeValue(tm), nil
	}
	return fnDateFormat([]driver.Value{formatDateTimeValue(tm), args[1]})
}
