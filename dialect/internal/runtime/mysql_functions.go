package runtime

import (
	"crypto/sha1" //nolint:gosec // SHA-1 backs dialects.MySQL's SHA1() function, not a security control
	"crypto/sha256"
	"crypto/sha512"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// This file holds the dialects.MySQL functions that have no dialects.SQLite spelling and no
// equivalent among the helpers the other dialects share. Each is registered
// under the name dialects.MySQL gives it, because none of these names is a dialects.SQLite
// keyword or built-in and none of the other dialects means something else by
// one; the calls that do collide are rewritten in mysql.go instead.
//
// Every expected value in the tests beside this file was read from dialects.MySQL 8.4
// rather than derived, because these are the functions whose edge cases are
// easiest to guess wrong: CONV stops at the first digit its base does not
// have, INSERT leaves the string alone for a position outside it, and a dialects.MySQL
// TIME is not a clock time.

// mysqlScalarFunctions returns the dialects.MySQL-only deterministic helpers, keyed by
// the name a query calls them by.
func mysqlScalarFunctions() map[string]scalarSpec {
	return map[string]scalarSpec{
		// Spellings of functions this package already has. They are registered
		// rather than rewritten so the result column keeps the name the query
		// wrote, which a rewrite would have to put back as an alias.
		"lcase":      {1, mysqlTextArgs(fnUnicodeLower, 0)},
		"ucase":      {1, mysqlTextArgs(fnUnicodeUpper, 0)},
		"mid":        {-1, mysqlTextArgs(fnMySQLSubstr, 0)},
		"dayofmonth": {1, unaryDatePart(unitDay)},

		// Strings and bytes.
		"strcmp":       {2, mysqlTextArgs(fnMySQLStrcmp, 0, 1)},
		"bit_length":   {1, mysqlTextArgs(fnMySQLBitLength, 0)},
		"mysql_insert": {4, mysqlTextArgs(fnMySQLInsert, 0, 3)},
		"to_base64":    {1, mysqlTextArgs(fnMySQLToBase64, 0)},
		"from_base64":  {1, fnMySQLFromBase64},

		// Numbers. Each reads a string the way dialects.MySQL reads one in a numeric
		// context -- the number at the front of it, or zero -- where dialects.SQLite's
		// own answers NULL, and each refuses a result dialects.MySQL refuses rather than
		// answering an infinity that no file format can carry.
		"mysql_ceil":  {1, mysqlMath(math.Ceil)},
		"mysql_floor": {1, mysqlMath(math.Floor)},
		"mysql_sign":  {1, mysqlMath(func(f float64) float64 { return float64(sign(f)) })},
		"mysql_sqrt":  {1, mysqlMath(math.Sqrt)},
		"mysql_exp":   {1, mysqlMath(math.Exp)},
		"mysql_ln":    {1, mysqlLogarithm(math.Log)},
		"mysql_log2":  {1, mysqlLogarithm(math.Log2)},
		"mysql_log10": {1, mysqlLogarithm(math.Log10)},

		// Numbers and bits.
		"conv":      {3, fnMySQLConv},
		"bin":       {1, convTo(2)},
		"oct":       {1, convTo(8)},
		"bit_count": {1, fnMySQLBitCount},
		"cot":       {1, fnCot},
		"crc32":     {1, fnMySQLCRC32},

		// Regular expressions. They fold case by default for the same reason
		// the REGEXP operator does, and take the same match_type argument.
		"regexp_like":   {-1, mysqlTextArgs(fnMySQLRegexpLike, 0, 1)},
		"regexp_substr": {-1, mysqlTextArgs(fnMySQLRegexpSubstr, 0, 1)},
		"regexp_instr":  {-1, mysqlTextArgs(fnMySQLRegexpInstr, 0, 1)},

		// Digests. dialects.MySQL answers the hexadecimal text where BigQuery answers
		// bytes, so if dialects.GoogleSQL ever gains these they will need their own.
		"sha1": {1, mysqlTextArgs(fnMySQLSHA1, 0)},
		"sha2": {2, mysqlTextArgs(fnMySQLSHA2, 0)},

		// Addresses, for the log files this package exists to query.
		// Sets and JSON.
		"make_set":       {-1, mysqlTextFrom(fnMySQLMakeSet, 1)},
		"export_set":     {-1, mysqlTextArgs(fnMySQLExportSet, 1, 2, 3)},
		"mysql_interval": {-1, fnMySQLIntervalPosition},
		"mysql_number":   {1, fnMySQLNumber},
		"json_length":    {-1, fnMySQLJSONLength},
		"json_contains":  {-1, fnMySQLJSONContains},

		// Addresses, for the log files this package exists to query.
		"inet_aton": {1, fnInetAton},
		"inet_ntoa": {1, fnInetNtoa},
		"is_ipv4":   {1, fnIsIPv4},
		"is_ipv6":   {1, fnIsIPv6},
	}
}

// mysqlMath wraps a one-argument computation with dialects.MySQL's two rules for it: a
// string argument is the number at the front of it rather than nothing, and a
// result the type cannot hold is an error rather than an infinity.
//
// dialects.SQLite's own ceil, floor, sign, sqrt, exp, ln, log2 and log10 answer NULL for
// a string, which made every one of them answer NULL for a column loaded from a
// file -- where every cell is text. They also answer an infinity for an
// overflow, and an infinity is a value no dump format here can write.
func mysqlMath(compute func(float64) float64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil
		}
		x, ok := mysqlNumericArgument(args[0])
		if !ok {
			return nil, nil
		}
		out := compute(x)
		if math.IsInf(out, 0) {
			return nil, fmt.Errorf("dialect: the result of this operation on %v is out of range", x)
		}
		if math.IsNaN(out) {
			// dialects.MySQL answers NULL where the operation is undefined, as SQRT(-1)
			// and LN(0) are.
			return nil, nil
		}
		return out, nil
	}
}

// mysqlLogarithm is mysqlMath for a logarithm, where an argument of zero or
// less is not an overflow but a value the function is undefined for: dialects.MySQL
// answers NULL for LN(0) and LN(-1) rather than reporting a range error.
func mysqlLogarithm(compute func(float64) float64) scalarFn {
	inner := mysqlMath(compute)
	return func(args []driver.Value) (driver.Value, error) {
		if x, ok := mysqlNumericArgument(args[0]); ok && x <= 0 {
			return nil, nil
		}
		return inner(args)
	}
}

// mysqlNumericArgument reads a value the way dialects.MySQL reads one where a number is
// wanted: a number is itself, and a string is the number its leading run
// spells, or zero.
func mysqlNumericArgument(v driver.Value) (float64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case string:
		return numericPrefix(x), true
	case []byte:
		return numericPrefix(string(x)), true
	}
	return toFloat(v)
}

// fnMySQLNumber implements mysql_number(x): the value a call reads where a
// number is wanted. It is the numeric counterpart of mysql_text, and is needed
// where a call stays on a function SQLite or this package answers for every
// dialect: those read a string that spells no number as no number at all and
// answer NULL, where MySQL reads it as zero and answers a number.
func fnMySQLNumber(args []driver.Value) (driver.Value, error) {
	x, ok := mysqlNumericArgument(args[0])
	if !ok {
		return nil, nil
	}
	return x, nil
}

// sign is the -1, 0 or 1 every dialect answers for SIGN.
func sign(f float64) int {
	switch {
	case f < 0:
		return -1
	case f > 0:
		return 1
	default:
		return 0
	}
}

// fnMySQLMakeSet implements MAKE_SET(bits, s1, s2, ...): the strings whose
// position matches a set bit, joined by commas. A NULL string is skipped and a
// NULL bit mask makes the whole call NULL.
func fnMySQLMakeSet(args []driver.Value) (driver.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("dialect: MAKE_SET expects at least one argument")
	}
	bits, ok := toCount(args[0])
	if !ok {
		return nil, nil
	}
	parts := make([]string, 0, len(args)-1)
	for i, arg := range args[1:] {
		if i >= 64 || bits&(1<<uint(i)) == 0 {
			continue
		}
		s, ok := toString(arg)
		if !ok {
			// dialects.MySQL leaves a NULL member out rather than answering NULL.
			continue
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ","), nil
}

// fnMySQLExportSet implements EXPORT_SET(bits, on, off[, separator[, count]]):
// one string per bit, least significant first, joined by the separator. The
// separator defaults to a comma and the count to 64, which is the width dialects.MySQL
// uses when it is not told one.
func fnMySQLExportSet(args []driver.Value) (driver.Value, error) {
	if len(args) < 3 || len(args) > 5 {
		return nil, fmt.Errorf("dialect: EXPORT_SET expects 3 to 5 arguments, got %d", len(args))
	}
	bits, ok1 := toCount(args[0])
	on, ok2 := toString(args[1])
	off, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	separator := ","
	if len(args) >= 4 {
		if separator, ok1 = toString(args[3]); !ok1 {
			return nil, nil
		}
	}
	count := int64(64)
	if len(args) == 5 {
		if count, ok1 = toCount(args[4]); !ok1 {
			return nil, nil
		}
	}
	if count < 0 {
		count = 0
	}
	if count > 64 {
		count = 64
	}
	parts := make([]string, 0, count)
	for i := range count {
		if bits&(1<<uint(i)) != 0 {
			parts = append(parts, on)
		} else {
			parts = append(parts, off)
		}
	}
	return strings.Join(parts, separator), nil
}

// fnMySQLIntervalPosition implements INTERVAL(n, v1, v2, ...): the index of the
// last value not greater than n, counting from 1, with 0 when n comes before
// them all and -1 when n is NULL. The values are assumed to be in order, which
// is what dialects.MySQL documents and what makes the function a bucket lookup.
func fnMySQLIntervalPosition(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("dialect: INTERVAL expects at least two arguments")
	}
	n, ok := mysqlNumericArgument(args[0])
	if !ok {
		// dialects.MySQL answers -1 for a NULL first argument rather than NULL.
		return int64(-1), nil
	}
	position := int64(0)
	for i, arg := range args[1:] {
		bound, ok := mysqlNumericArgument(arg)
		if !ok || n < bound {
			break
		}
		position = int64(i) + 1
	}
	return position, nil
}

// fnMySQLJSONLength implements JSON_LENGTH(doc[, path]): the number of members
// of an object, the number of elements of an array, and 1 for a scalar.
func fnMySQLJSONLength(args []driver.Value) (driver.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("dialect: JSON_LENGTH expects 1 or 2 arguments, got %d", len(args))
	}
	value, ok, err := mysqlJSONAt(args)
	if err != nil || !ok {
		return nil, err
	}
	switch v := value.(type) {
	case []any:
		return int64(len(v)), nil
	case map[string]any:
		return int64(len(v)), nil
	default:
		return int64(1), nil
	}
}

// fnMySQLJSONContains implements JSON_CONTAINS(doc, candidate[, path]): whether
// the candidate document is contained in the target, which for an array means
// membership and for an object means every member matching.
func fnMySQLJSONContains(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("dialect: JSON_CONTAINS expects 2 or 3 arguments, got %d", len(args))
	}
	target := []driver.Value{args[0]}
	if len(args) == 3 {
		target = append(target, args[2])
	}
	haystack, ok, err := mysqlJSONAt(target)
	if err != nil || !ok {
		return nil, err
	}
	needle, ok, err := mysqlJSONAt([]driver.Value{args[1]})
	if err != nil || !ok {
		return nil, err
	}
	return boolToInt(jsonContains(haystack, needle)), nil
}

// mysqlJSONAt decodes the document in args[0] and, when a path is given in
// args[1], follows it. It reports false when either is NULL.
func mysqlJSONAt(args []driver.Value) (any, bool, error) {
	text, ok := toString(args[0])
	if !ok {
		return nil, false, nil
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, false, fmt.Errorf("dialect: %q is not valid JSON", text)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, false, fmt.Errorf("dialect: %q is not valid JSON", text)
	}
	if len(args) < 2 {
		return value, true, nil
	}
	path, ok := toString(args[1])
	if !ok {
		return nil, false, nil
	}
	found, ok := jsonAtPath(value, path)
	return found, ok, nil
}

// jsonAtPath follows a dialects.MySQL JSON path of the plain "$.a.b[0]" shape, which is
// what the length and containment functions take. A path this does not
// understand finds nothing, which is the NULL dialects.MySQL answers for a path that
// matches no value.
func jsonAtPath(value any, path string) (any, bool) {
	rest, found := strings.CutPrefix(strings.TrimSpace(path), "$")
	if !found {
		return nil, false
	}
	for rest != "" {
		switch {
		case strings.HasPrefix(rest, "."):
			rest = rest[1:]
			end := strings.IndexAny(rest, ".[")
			if end < 0 {
				end = len(rest)
			}
			object, isObject := value.(map[string]any)
			if !isObject {
				return nil, false
			}
			member, has := object[rest[:end]]
			if !has {
				return nil, false
			}
			value, rest = member, rest[end:]
		case strings.HasPrefix(rest, "["):
			end := strings.Index(rest, "]")
			if end < 0 {
				return nil, false
			}
			index, err := strconv.Atoi(rest[1:end])
			array, isArray := value.([]any)
			if err != nil || !isArray || index < 0 || index >= len(array) {
				return nil, false
			}
			value, rest = array[index], rest[end+1:]
		default:
			return nil, false
		}
	}
	return value, true
}

// jsonContains reports whether needle is contained in haystack the way dialects.MySQL's
// JSON_CONTAINS defines it: a scalar equals a scalar, an array holds each
// element of an array candidate, and an object holds every member of an object
// candidate.
func jsonContains(haystack, needle any) bool {
	switch want := needle.(type) {
	case []any:
		for _, item := range want {
			if !jsonContains(haystack, item) {
				return false
			}
		}
		return true
	case map[string]any:
		have, ok := haystack.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range want {
			member, has := have[k]
			if !has || !jsonEqual(member, v) {
				return false
			}
		}
		return true
	}
	if items, ok := haystack.([]any); ok {
		for _, item := range items {
			if jsonEqual(item, needle) {
				return true
			}
		}
		return false
	}
	return jsonEqual(haystack, needle)
}

// jsonEqual compares two decoded documents by their written form, which is
// enough for the containment question and keeps a number's spelling out of it.
func jsonEqual(a, b any) bool {
	left, err1 := json.Marshal(a)
	right, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(left) == string(right)
}

// fnMySQLStrcmp implements STRCMP(a, b): -1, 0 or 1 as a sorts before, with or
// after b, and NULL when either is NULL.
//
// The comparison folds case, which is what dialects.MySQL's default collation does. It
// does not fold accents, which that collation also does -- dialects.MySQL answers 0 for
// STRCMP('é', 'e') and this answers a difference. Modeling that would mean
// carrying the collation's weight tables, and the case rule is the one a query
// over a file is written against.
func fnMySQLStrcmp(args []driver.Value) (driver.Value, error) {
	a, ok1 := toString(args[0])
	b, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return int64(strings.Compare(fnFoldCase(a), fnFoldCase(b))), nil
}

// fnMySQLNullif implements NULLIF(a, b): NULL when the two are equal under
// MySQL's "=", and a otherwise.
//
// SQLite's own nullif() compares by storage class, so a string never equals a
// number there and NULLIF('abc', 0) answered 'abc'. MySQL reads both sides as
// numbers as soon as either is one, where a string that spells no number reads
// as zero, which makes that call NULL. Two strings compare under a collation
// that folds case, the same reading FIELD is on.
func fnMySQLNullif(args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	if args[1] == nil {
		return args[0], nil
	}
	if fieldEqual(args[0], args[1], fieldComparesAsText(args)) {
		return nil, nil
	}
	return args[0], nil
}

// fnFoldCase folds a string for a case-insensitive comparison.
func fnFoldCase(s string) string {
	return strings.ToLower(s)
}

// fnMySQLBitLength implements BIT_LENGTH(x): the length of the value's string
// form in bits.
func fnMySQLBitLength(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	return 8 * int64(len(s)), nil
}

// fnMySQLInsert implements INSERT(str, pos, len, newstr): str with len
// characters from pos replaced by newstr. A position outside the string leaves
// it alone, and a length that reaches past the end, or a negative one, replaces
// everything from pos onwards. Positions and lengths count characters, and a
// fraction is rounded rather than truncated.
func fnMySQLInsert(args []driver.Value) (driver.Value, error) {
	src, ok1 := toString(args[0])
	pos, ok2 := toCount(args[1])
	length, ok3 := toCount(args[2])
	repl, ok4 := toString(args[3])
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return nil, nil
	}
	runes := []rune(src)
	if pos < 1 || pos > int64(len(runes)) {
		return src, nil
	}
	head := runes[:pos-1]
	tail := len(runes)
	// The length is bounded before it is added to the position, because a
	// length near the top of the int64 range wraps the sum negative and would
	// then pass for a position inside the string.
	if length >= 0 && length < int64(len(runes)) && pos-1+length < int64(len(runes)) {
		tail = int(pos - 1 + length)
	}
	return string(head) + repl + string(runes[tail:]), nil
}

// base64LineLength is where dialects.MySQL's TO_BASE64 breaks a line.
const base64LineLength = 76

// fnMySQLToBase64 implements TO_BASE64(x). dialects.MySQL breaks the output into lines
// of 76 characters, which a caller comparing against dialects.MySQL's own output will
// see.
func fnMySQLToBase64(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(s))
	if len(encoded) <= base64LineLength {
		return encoded, nil
	}
	var b strings.Builder
	b.Grow(len(encoded) + len(encoded)/base64LineLength)
	for i := 0; i < len(encoded); i += base64LineLength {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(encoded[i:min(i+base64LineLength, len(encoded))])
	}
	return b.String(), nil
}

// fnMySQLFromBase64 implements FROM_BASE64(x): the inverse of TO_BASE64, with
// NULL for an argument that is not base-64. Whitespace is ignored, so the lines
// TO_BASE64 writes decode again.
func fnMySQLFromBase64(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(strings.Map(dropSpace, s))
	if err != nil {
		return nil, nil //nolint:nilerr // NULL is dialects.MySQL's documented result here
	}
	return string(raw), nil
}

// dropSpace removes a whitespace rune from a string being mapped.
func dropSpace(r rune) rune {
	switch r {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return -1
	default:
		return r
	}
}

// convMinBase and convMaxBase bound the bases CONV accepts.
const (
	convMinBase = 2
	convMaxBase = 36
)

// fnMySQLConv implements CONV(n, from_base, to_base): n read in one base and
// written in another.
//
// The reading is unsigned and stops at the first character the base does not
// have, so CONV('12abc', 10, 10) is 12 and CONV('xyz', 16, 10) is 0; only an
// empty argument is NULL. A value past the range saturates rather than
// wrapping. A negative base means the number on that side is signed, which is
// visible only when the sign bit is set: CONV(-15, 10, -2) is -1111 and
// CONV(-15, 10, 16) is the same bits written as an unsigned FFFFFFFFFFFFFFF1.
func fnMySQLConv(args []driver.Value) (driver.Value, error) {
	s, ok1 := toString(args[0])
	from, ok2 := toInt(args[1])
	to, ok3 := toInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if s == "" {
		return nil, nil
	}
	fromBase, signedIn := absBase(from)
	toBase, signedOut := absBase(to)
	if fromBase < convMinBase || fromBase > convMaxBase || toBase < convMinBase || toBase > convMaxBase {
		return nil, nil
	}
	value := parseInBase(s, fromBase)
	_ = signedIn // the reading is the same either way; only the writing differs
	if signedOut {
		return strings.ToUpper(strconv.FormatInt(int64(value), int(toBase))), nil //nolint:gosec // a negative base is dialects.MySQL's way of asking for the signed reading
	}
	return strings.ToUpper(strconv.FormatUint(value, int(toBase))), nil
}

// convTo is CONV with a fixed base and base ten input, which is what BIN and
// OCT are.
func convTo(base int64) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		return fnMySQLConv([]driver.Value{args[0], int64(10), base})
	}
}

// absBase splits a CONV base into its magnitude and whether it was negative,
// which is how dialects.MySQL asks for the signed reading.
func absBase(base int64) (int64, bool) {
	if base < 0 {
		return -base, true
	}
	return base, false
}

// parseInBase reads the longest prefix of s that spells a number in the given
// base, after a leading sign and any leading space. A prefix too large for 64
// bits saturates, which is what dialects.MySQL answers.
func parseInBase(s string, base int64) uint64 {
	t := strings.TrimLeft(s, " \t\n\v\f\r")
	negative := false
	if t != "" && (t[0] == '-' || t[0] == '+') {
		negative = t[0] == '-'
		t = t[1:]
	}
	var value uint64
	for i := range len(t) {
		digit := digitValue(t[i])
		if digit < 0 || int64(digit) >= base {
			break
		}
		next := value*uint64(base) + uint64(digit)               //nolint:gosec // digit is non-negative and below base
		if next < value || value > math.MaxUint64/uint64(base) { //nolint:gosec // base is between 2 and 36
			value = math.MaxUint64
			break
		}
		value = next
	}
	if negative {
		return -value
	}
	return value
}

// digitValue is the value of a base-36 digit, or -1 for a character that is not
// one.
func digitValue(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'z':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'Z':
		return int(c-'A') + 10
	default:
		return -1
	}
}

// fnMySQLBitCount implements BIT_COUNT(n): the number of one bits in the
// value's 64-bit form, so BIT_COUNT(-1) is 64.
func fnMySQLBitCount(args []driver.Value) (driver.Value, error) {
	n, ok := toInt(args[0])
	if !ok {
		return nil, nil
	}
	return int64(bits.OnesCount64(uint64(n))), nil //nolint:gosec // counting bits is what the reinterpretation is for
}

// fnCot implements COT(x). dialects.MySQL refuses the value at zero rather than
// answering an infinity, so this does too: an infinity here would flow into the
// rest of the query as a number.
func fnCot(args []driver.Value) (driver.Value, error) {
	x, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	// The reciprocal of the tangent, which is how dialects.MySQL computes it. The two
	// can disagree in the last bit, because Go's tangent and the C library's
	// round differently: dialects.MySQL answers 0.6420926159343306 for COT(1) and this
	// answers 0.6420926159343308.
	tan := math.Tan(x)
	if tan == 0 {
		return nil, fmt.Errorf("dialect: COT: the cotangent of %v is out of range", x)
	}
	return 1 / tan, nil
}

// fnMySQLCRC32 implements CRC32(x): the IEEE checksum of the value's string
// form, as an unsigned 32-bit number.
func fnMySQLCRC32(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	return int64(crc32.ChecksumIEEE([]byte(s))), nil
}

// fnMySQLRegexpLike implements REGEXP_LIKE(subject, pattern[, match_type]).
func fnMySQLRegexpLike(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("dialect: REGEXP_LIKE expects 2 or 3 arguments, got %d", len(args))
	}
	subject, pattern, matchType, ok := regexpArgs(args, 2)
	if !ok {
		return nil, nil
	}
	re, err := compileMySQLRegexp(pattern, matchType)
	if err != nil {
		return nil, err
	}
	return boolToInt(re.MatchString(subject)), nil
}

// fnMySQLRegexpSubstr implements REGEXP_SUBSTR(subject, pattern[, pos[,
// occurrence[, match_type]]]): the text of the occurrence, or NULL when there
// is none.
func fnMySQLRegexpSubstr(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 5 {
		return nil, fmt.Errorf("dialect: REGEXP_SUBSTR expects 2 to 5 arguments, got %d", len(args))
	}
	subject, pattern, matchType, ok := regexpArgs(args, 4)
	if !ok {
		return nil, nil
	}
	loc, ok, err := regexpOccurrence(args, subject, pattern, matchType, "REGEXP_SUBSTR")
	if err != nil || !ok {
		return nil, err
	}
	return subject[loc[0]:loc[1]], nil
}

// fnMySQLRegexpInstr implements REGEXP_INSTR(subject, pattern[, pos[,
// occurrence[, return_option[, match_type]]]]): the character position the
// occurrence starts at, or the one after it ends when return_option is 1, and 0
// when there is no occurrence.
func fnMySQLRegexpInstr(args []driver.Value) (driver.Value, error) {
	if len(args) < 2 || len(args) > 6 {
		return nil, fmt.Errorf("dialect: REGEXP_INSTR expects 2 to 6 arguments, got %d", len(args))
	}
	subject, pattern, matchType, ok := regexpArgs(args, 5)
	if !ok {
		return nil, nil
	}
	returnEnd := int64(0)
	if len(args) >= 5 {
		n, argOK := toInt(args[4])
		if !argOK {
			return nil, nil
		}
		if n != 0 && n != 1 {
			return nil, fmt.Errorf("dialect: REGEXP_INSTR return option %d is not 0 or 1", n)
		}
		returnEnd = n
	}
	loc, ok, err := regexpOccurrence(args, subject, pattern, matchType, "REGEXP_INSTR")
	if err != nil {
		return nil, err
	}
	if !ok {
		return int64(0), nil
	}
	offset := loc[0]
	if returnEnd == 1 {
		offset = loc[1]
	}
	// dialects.MySQL counts characters, not bytes.
	return int64(len([]rune(subject[:offset])) + 1), nil
}

// regexpArgs reads the subject, pattern and optional match type shared by the
// REGEXP_ family. matchTypeIdx is the argument the match type sits at for the
// calling function. It reports false when an argument is NULL, which is the
// answer all of them give.
func regexpArgs(args []driver.Value, matchTypeIdx int) (subject, pattern, matchType string, ok bool) {
	subject, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return "", "", "", false
	}
	if len(args) > matchTypeIdx {
		m, ok3 := toString(args[matchTypeIdx])
		if !ok3 {
			return "", "", "", false
		}
		matchType = m
	}
	return subject, pattern, matchType, true
}

// regexpOccurrence finds the occurrence the pos and occurrence arguments name,
// reporting false when the subject holds no such occurrence.
func regexpOccurrence(args []driver.Value, subject, pattern, matchType, name string) (loc []int, ok bool, err error) {
	pos, occurrence := int64(1), int64(1)
	if len(args) >= 3 {
		n, argOK := toInt(args[2])
		if !argOK {
			return nil, false, nil
		}
		pos = n
	}
	if len(args) >= 4 {
		n, argOK := toInt(args[3])
		if !argOK {
			return nil, false, nil
		}
		// dialects.MySQL reads an occurrence below one as the first.
		occurrence = max(n, 1)
	}
	runes := []rune(subject)
	if pos < 1 || pos > int64(len(runes))+1 {
		return nil, false, fmt.Errorf("dialect: %s position %d is out of bounds", name, pos)
	}
	// Non-overlapping matches cannot outnumber the characters, so an occurrence
	// past that has none. The check also keeps the count below what an int holds
	// on a 32-bit build, where a truncated negative would mean "every match".
	if occurrence > int64(len(runes))+1 {
		return nil, false, nil
	}
	re, err := compileMySQLRegexp(pattern, matchType)
	if err != nil {
		return nil, false, err
	}
	start := len(string(runes[:pos-1]))
	matches := re.FindAllStringIndex(subject[start:], int(occurrence))
	if int64(len(matches)) < occurrence {
		return nil, false, nil
	}
	found := matches[occurrence-1]
	return []int{start + found[0], start + found[1]}, true, nil
}

// compileMySQLRegexp compiles a pattern under dialects.MySQL's rules, which fold case
// unless the match type says otherwise.
func compileMySQLRegexp(pattern, matchType string) (*regexp.Regexp, error) {
	folded, err := mysqlRegexpPattern(pattern, matchType)
	if err != nil {
		return nil, err
	}
	return compileRegexp(folded)
}

// fnMySQLSHA1 implements SHA1(x): the digest as lowercase hexadecimal text,
// which is what dialects.MySQL answers where BigQuery answers bytes.
func fnMySQLSHA1(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	sum := sha1.Sum([]byte(s)) //nolint:gosec // SHA-1 is the function dialects.MySQL defines, not a security choice
	return hex.EncodeToString(sum[:]), nil
}

// fnMySQLSHA2 implements SHA2(x, n): the SHA-2 digest of the requested width as
// lowercase hexadecimal text. A width dialects.MySQL does not offer answers NULL, and 0
// means 256.
func fnMySQLSHA2(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	width, ok := toInt(args[1])
	if !ok {
		return nil, nil
	}
	switch width {
	case 0, 256:
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:]), nil
	case 224:
		sum := sha256.Sum224([]byte(s))
		return hex.EncodeToString(sum[:]), nil
	case 384:
		sum := sha512.Sum384([]byte(s))
		return hex.EncodeToString(sum[:]), nil
	case 512:
		sum := sha512.Sum512([]byte(s))
		return hex.EncodeToString(sum[:]), nil
	default:
		return nil, nil
	}
}

// ipv4Parts is the number of parts a dotted-quad address has.
const ipv4Parts = 4

// fnInetAton implements INET_ATON(addr): the numeric form of a dotted address.
// A short form fills the top bytes from the left and puts the last part in the
// lowest byte, so '1.2' is 1.0.0.2, which is what dialects.MySQL does.
func fnInetAton(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	parts := strings.Split(s, ".")
	if len(parts) < 1 || len(parts) > ipv4Parts || s == "" {
		return nil, nil
	}
	var value uint32
	for i, part := range parts {
		octet, digits := parseOctet(part)
		if !digits {
			return nil, nil
		}
		shift := 0
		if i < len(parts)-1 {
			shift = 8 * (3 - i)
		}
		value |= octet << shift
	}
	return int64(value), nil
}

// fnInetNtoa implements INET_NTOA(n): the dotted form of a numeric address, and
// NULL for a number outside the 32-bit range.
func fnInetNtoa(args []driver.Value) (driver.Value, error) {
	n, ok := toInt(args[0])
	if !ok {
		return nil, nil
	}
	if n < 0 || n > math.MaxUint32 {
		return nil, nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", n>>24&0xff, n>>16&0xff, n>>8&0xff, n&0xff), nil
}

// fnIsIPv4 implements IS_IPV4(addr): whether the argument is a dotted quad.
// Leading zeros are accepted, which is what dialects.MySQL does, and anything shorter
// than four parts is not.
func fnIsIPv4(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	parts := strings.Split(s, ".")
	if len(parts) != ipv4Parts {
		return int64(0), nil
	}
	for _, part := range parts {
		if _, ok := parseOctet(part); !ok {
			return int64(0), nil
		}
	}
	return int64(1), nil
}

// fnIsIPv6 implements IS_IPV6(addr): whether the argument is an IPv6 address.
// A dotted quad on its own is not one, and neither is an address carrying a
// zone, both of which dialects.MySQL refuses.
func fnIsIPv6(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	if !strings.Contains(s, ":") {
		return int64(0), nil
	}
	return boolToInt(net.ParseIP(s) != nil), nil
}

// parseOctet reads one part of a dotted address: a non-empty run of decimal
// digits whose value is at most 255. Leading zeros are accepted, which is what
// dialects.MySQL does, and a sign is not, which strconv on its own would take.
func parseOctet(part string) (uint32, bool) {
	if part == "" || len(part) > 3 {
		return 0, false
	}
	var value uint32
	for i := range len(part) {
		if part[i] < '0' || part[i] > '9' {
			return 0, false
		}
		value = value*10 + uint32(part[i]-'0')
	}
	if value > 255 {
		return 0, false
	}
	return value, true
}
