package runtime

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// This file implements the operators whose meaning changes on the way to
// dialects.SQLite. Like the cast helpers in cast.go, each is a silent divergence: the
// query runs and returns a plausible answer that the source dialect would never
// give.
//
//   - "/" is integer division in dialects.SQLite when both operands are integers, but
//     floating-point division in dialects.MySQL and dialects.GoogleSQL. An average or a ratio came
//     out truncated.
//   - A zero divisor answers NULL in dialects.SQLite and in dialects.MySQL, and raises in
//     dialects.PostgreSQL and dialects.GoogleSQL. The NULL reads as missing data rather than as
//     arithmetic the engine refused, so it survives into a report.
//   - LIKE folds ASCII case in dialects.SQLite. It is case-sensitive in dialects.PostgreSQL and
//     dialects.GoogleSQL, so a filter matched rows it should not have, and dialects.PostgreSQL's
//     ILIKE became indistinguishable from LIKE.
//   - dialects.MySQL reads "||" as a logical OR under its default sql_mode, where dialects.SQLite
//     concatenates.
//
// The operators are rewritten into helper calls rather than left to dialects.SQLite,
// since a pragma such as case_sensitive_like is connection-wide and would change
// the dialects.SQLite dialect's behavior too.

// ErrDivideByZero reports a division by zero in a dialect that raises for it.
// It stands on its own rather than wrapping ErrInvalidCast, which is about a
// value a target type cannot represent: a division by zero converts nothing,
// and an error reading "invalid cast: division by zero" sent the reader looking
// at CAST expressions their query does not contain.
// ErrDivideByZero reports a division by zero in a dialect that raises for it.
// It stands on its own rather than wrapping ErrInvalidCast, which is about a
// value a target type cannot represent: a division by zero converts nothing,
// and an error reading "invalid cast: division by zero" sent the reader looking
// at CAST expressions their query does not contain.
var ErrDivideByZero = errors.New("dialect: division by zero")

// divideFloat implements the "/" operator for the dialects whose division is
// always floating point. dialects.MySQL answers NULL when the divisor is zero;
// dialects.GoogleSQL raises, and offers SAFE_DIVIDE for callers who want the NULL.
func divideFloat(raiseOnZero bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		a, ok1 := toFloat(args[0])
		b, ok2 := toFloat(args[1])
		if !ok1 || !ok2 {
			return nil, nil
		}
		if b == 0 {
			if raiseOnZero {
				return nil, ErrDivideByZero
			}
			return nil, nil
		}
		return a / b, nil
	}
}

// divideSQLite implements the "/" operator for a dialect that divides the way
// dialects.SQLite does — two integers give an integer — but raises on a zero divisor
// where dialects.SQLite answers NULL. dialects.PostgreSQL is that dialect: 7 / 2 is 3 in both,
// and 7 / 0 stops the query in dialects.PostgreSQL.
//
// Both operands are read the way dialects.SQLite reads them, so only the zero divisor
// moves: a pair of integers divides as integers, anything else as floats, and
// text takes the number it spells, which is why "'7' / 2" is 3 rather than 3.5.
// A NULL operand is nothing to divide with and stays NULL.
func divideSQLite(args []driver.Value) (driver.Value, error) {
	a, aok := sqliteOperand(args[0])
	b, bok := sqliteOperand(args[1])
	if !aok || !bok {
		return nil, nil
	}
	if a.isInt && b.isInt {
		if b.i == 0 {
			return nil, ErrDivideByZero
		}
		return a.i / b.i, nil
	}
	if b.float() == 0 {
		return nil, ErrDivideByZero
	}
	return a.float() / b.float(), nil
}

// remainder is the remainder every dialect here computes: an integer one when
// both operands are integers, and math.Mod otherwise. dialects.SQLite's own "%"
// truncates both operands to integers first, so it answers 1 for 7.5 % 2 where
// every dialect this package translates answers 1.5, and 1 for 7 % 2.5 where
// they answer 2.0.
//
// The sign follows the dividend in all of them, which is what both Go's "%" and
// math.Mod already do.
func remainder(a, b numOperand) driver.Value {
	if a.isInt && b.isInt {
		return a.i % b.i
	}
	return math.Mod(a.float(), b.float())
}

// moduloDialect implements "%" and mod() with the dialect's arithmetic rather
// than dialects.SQLite's. raiseOnZero says what a zero divisor does: dialects.PostgreSQL and
// dialects.GoogleSQL raise, dialects.MySQL answers NULL, and dialects.SQLite answers NULL, which is why
// only the raising pair used to need a helper at all.
func moduloDialect(raiseOnZero bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		a, aok := sqliteOperand(args[0])
		b, bok := sqliteOperand(args[1])
		if !aok || !bok {
			return nil, nil
		}
		// A divisor is zero as a value, not as the integer it truncates to:
		// 7 % 0.5 divides evenly in every dialect here and must not be refused.
		if b.float() == 0 {
			if raiseOnZero {
				return nil, ErrDivideByZero
			}
			return nil, nil
		}
		return remainder(a, b), nil
	}
}

// integerDivide implements dialects.PostgreSQL's div(x, y) and dialects.GoogleSQL's DIV(x, y):
// the quotient truncated toward zero, which is what both engines answer for a
// negative operand — div(-7, 2) is -3 rather than the -4 a floor would give.
// A zero divisor raises, as it does for the operators.
func integerDivide(args []driver.Value) (driver.Value, error) {
	a, aok := sqliteOperand(args[0])
	b, bok := sqliteOperand(args[1])
	if !aok || !bok {
		return nil, nil
	}
	if b.float() == 0 {
		return nil, ErrDivideByZero
	}
	if a.isInt && b.isInt {
		return a.i / b.i, nil
	}
	// The quotient is truncated, not the operands: div(7, 2.5) is 2 because
	// 2.8 truncates, where truncating 2.5 to 2 first would answer 3.
	return numOperand{f: math.Trunc(a.float() / b.float())}.integer(), nil
}

// truncateScale implements dialects.PostgreSQL's trunc(x, n) and dialects.GoogleSQL's
// TRUNC(x, n): x with everything past n decimal places cut off, toward zero, so
// trunc(-12.345, 2) is -12.34 rather than -12.35. A negative scale truncates to
// a power of ten, which is what dialects.PostgreSQL answers for trunc(12345.6, -2) with
// 12300.
func truncateScale(args []driver.Value) (driver.Value, error) {
	x, ok := toFloat(args[0])
	if !ok {
		return nil, nil
	}
	scale, ok := sqliteOperand(args[1])
	if !ok {
		return nil, nil
	}
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return x, nil
	}
	factor := math.Pow(10, float64(scale.integer()))
	switch {
	case factor == 0:
		// A scale so negative that the power of ten underflows truncates every
		// finite value to nothing, which is the 0 dialects.PostgreSQL answers for
		// trunc(12.345, -400).
		return float64(0), nil
	case math.IsInf(factor, 0):
		// A scale past every decimal the value has keeps the value, which is
		// what dialects.PostgreSQL answers for trunc(12.345, 400).
		return x, nil
	}
	return math.Trunc(x*factor) / factor, nil
}

// widthBucket implements dialects.PostgreSQL's width_bucket(x, lo, hi, count): which of
// count equal-width buckets spanning lo..hi the value falls in, numbered from
// 1, with 0 for a value below the range and count+1 for one above it. The
// bounds may be given in either order, which is how a descending scale is
// bucketed, and a range of no width is refused the way dialects.PostgreSQL refuses it.
func widthBucket(args []driver.Value) (driver.Value, error) {
	x, ok1 := toFloat(args[0])
	low, ok2 := toFloat(args[1])
	high, ok3 := toFloat(args[2])
	count, ok4 := sqliteOperand(args[3])
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return nil, nil
	}
	buckets := count.integer()
	if buckets <= 0 {
		return nil, fmt.Errorf("%w: width_bucket count must be greater than zero", ErrInvalidCast)
	}
	if low == high {
		return nil, fmt.Errorf("%w: width_bucket lower bound cannot equal upper bound", ErrInvalidCast)
	}
	if low > high {
		// A descending range is the ascending one counted from the other end.
		low, high, x = high, low, high+low-x
	}
	if x < low {
		return int64(0), nil
	}
	if x >= high {
		return buckets + 1, nil
	}
	// The buckets are numbered from 1, so the value's share of the range names
	// the bucket below it and the one it is in is the next.
	return int64(float64(buckets)*(x-low)/(high-low)) + 1, nil
}

// numOperand is one arithmetic operand as dialects.SQLite reads it: an integer, or a
// float when it is not one.
type numOperand struct {
	isInt bool
	i     int64
	f     float64
}

func (n numOperand) float() float64 {
	if n.isInt {
		return float64(n.i)
	}
	return n.f
}

// integer is the operand as dialects.SQLite's "%" takes it, truncating toward zero and
// stopping at the ends of the int64 range. The clamp is the whole reason this
// is not a bare conversion: Go leaves int64(1e300) to the implementation, which
// on amd64 is the most negative int64, so "1e300 % 7" answered -1 where dialects.SQLite
// answers the 0 that clamping to the largest int64 gives.
func (n numOperand) integer() int64 {
	if n.isInt {
		return n.i
	}
	switch {
	case math.IsNaN(n.f):
		return 0
	case n.f >= math.MaxInt64:
		return math.MaxInt64
	case n.f <= math.MinInt64:
		return math.MinInt64
	default:
		return int64(n.f)
	}
}

// sqliteOperand applies the numeric affinity dialects.SQLite applies to an arithmetic
// operand, so a helper standing in for an operator computes what the operator
// computed: an integer stays one, a float stays one, and text takes the number
// it spells — zero when it spells none, which is why "'abc' / 2" is 0 rather
// than an error. NULL is nothing to compute with and reports false, which is
// how the operator's NULL propagation is kept.
func sqliteOperand(v driver.Value) (numOperand, bool) {
	switch x := v.(type) {
	case nil:
		return numOperand{}, false
	case int64:
		return numOperand{isInt: true, i: x}, true
	case float64:
		return numOperand{f: x}, true
	case string:
		return textOperand(x), true
	case []byte:
		return textOperand(string(x)), true
	default:
		return numOperand{}, false
	}
}

// textOperand is the number a text operand spells, or zero.
func textOperand(s string) numOperand {
	s = strings.TrimSpace(s)
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return numOperand{isInt: true, i: i}
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return numOperand{f: f}
	}
	return numOperand{isInt: true}
}

// likeEscape is the character that makes the next one literal in a LIKE
// pattern. dialects.PostgreSQL and dialects.GoogleSQL both use a backslash when no ESCAPE clause
// says otherwise, which is how a caller searches for a value containing "%" or
// "_". dialects.SQLite has no default escape at all, so this has to be honored here
// rather than left to the engine — a pattern that arrived without an ESCAPE
// clause has already lost the chance to be given one.
const likeEscape = '\\'

// noEscape is the escape character of a pattern that has none, which is what an
// empty ESCAPE clause asks for. It is a code point no text can hold.
const noEscape = rune(-1)

// likeCompare implements SQL LIKE, where "%" matches any run of characters and
// "_" matches exactly one. dialects.SQLite's own LIKE folds ASCII case; this one folds
// only when asked, so dialects.PostgreSQL and dialects.GoogleSQL keep their case-sensitive LIKE
// and dialects.PostgreSQL's ILIKE folds every character rather than just the ASCII ones.
// likeCompare implements one dialect's LIKE. caseSensitive follows the
// dialect's collation, and strictEscape follows its reading of a pattern that
// ends in the escape character: PostgreSQL raises for one, and MySQL reads the
// character as itself.
func likeCompare(caseSensitive, strictEscape bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		if len(args) < 2 || len(args) > 3 {
			return nil, fmt.Errorf("dialect: LIKE takes a pattern, a subject and an optional escape character, got %d arguments", len(args))
		}
		pattern, ok1 := toString(args[0])
		subject, ok2 := toString(args[1])
		if !ok1 || !ok2 {
			return nil, nil
		}
		escape := likeEscape
		if len(args) == 3 {
			text, ok := toString(args[2])
			if !ok {
				return nil, nil
			}
			chars := []rune(text)
			switch len(chars) {
			case 0:
				// An empty ESCAPE clause turns escaping off, which is what
				// PostgreSQL means by it.
				escape = noEscape
			case 1:
				escape = chars[0]
			default:
				return nil, fmt.Errorf("dialect: the ESCAPE clause takes one character, got %q", text)
			}
		}
		if !caseSensitive {
			pattern = foldCase(pattern)
			subject = foldCase(subject)
			if escape != noEscape {
				escape = unicode.ToLower(escape)
			}
		}
		matched, err := likeMatch([]rune(pattern), []rune(subject), escape, strictEscape)
		if err != nil {
			return nil, err
		}
		return boolToInt(matched), nil
	}
}

func foldCase(s string) string {
	return strings.Map(unicode.ToLower, s)
}

// likeMatch reports whether subject matches pattern. It walks both strings once,
// remembering the last "%" so a failed branch can resume there, which keeps a
// pattern full of wildcards from backtracking exponentially.
//
// An escape makes the character after it literal, so "a\\%b" matches the three
// characters "a%b" and nothing else. An escape at the very end of the pattern
// escapes nothing, and the dialects read it differently: MySQL takes it for
// itself, so "!" LIKE "!" ESCAPE "!" is true there, and PostgreSQL raises. The
// error is reported where the walk reaches it, which is where PostgreSQL
// reports it too: "ab" LIKE "ab!" runs out of subject first and answers false
// rather than raising.
func likeMatch(pattern, subject []rune, escape rune, strictEscape bool) (bool, error) {
	var (
		p, s          int
		star          = -1
		afterStarSubj int
	)
	for s < len(subject) {
		if strictEscape && p == len(pattern)-1 && pattern[p] == escape {
			return false, errors.New("dialect: the LIKE pattern ends with its escape character, which escapes nothing")
		}
		switch {
		case p < len(pattern) && pattern[p] == escape && literalAt(pattern, p, escape) == subject[s]:
			p += escapedWidth(pattern, p)
			s++
		case p < len(pattern) && pattern[p] != escape && (pattern[p] == '_' || pattern[p] == subject[s]):
			p++
			s++
		case p < len(pattern) && pattern[p] == '%':
			star = p
			afterStarSubj = s
			p++
		case star >= 0:
			// Backtrack: let the last "%" swallow one more character.
			p = star + 1
			afterStarSubj++
			s = afterStarSubj
		default:
			return false, nil
		}
	}
	for p < len(pattern) && pattern[p] == '%' {
		p++
	}
	return p == len(pattern), nil
}

// literalAt is the character an escape at p stands for: the one after it, or
// the escape itself when it ends the pattern.
func literalAt(pattern []rune, p int, escape rune) rune {
	if p+1 < len(pattern) {
		return pattern[p+1]
	}
	return escape
}

// escapedWidth is how much of the pattern an escape at p consumes.
func escapedWidth(pattern []rune, p int) int {
	if p+1 < len(pattern) {
		return 2
	}
	return 1
}

// fnMySQLHex implements dialects.MySQL HEX(x): the hexadecimal digits of a number, or of
// a string's bytes. dialects.SQLite's own HEX only does the latter, so HEX(255) answered
// "323535" (the bytes of "255") instead of "FF".
//
// A number is read as an unsigned 64-bit value and a fraction is rounded before
// it is hexed, which is what makes UNHEX(HEX(n)) round-trip: formatting a
// negative value as a signed number wrote "-1", which holds no hexadecimal
// digits and which UNHEX answers NULL for. Which of the two forms applies is
// decided by the argument's type and not by its contents, so HEX('255') hexes
// three bytes where HEX(255) hexes one.
func fnMySQLHex(args []driver.Value) (driver.Value, error) {
	switch x := args[0].(type) {
	case nil:
		return nil, nil
	case int64:
		return hexUnsigned(uint64(x)), nil //nolint:gosec // reinterpreting the bits is what dialects.MySQL's unsigned reading is
	case float64:
		return hexUnsigned(uint64(roundToInt64(x))), nil //nolint:gosec // same reinterpretation, after rounding
	case []byte:
		return strings.ToUpper(hex.EncodeToString(x)), nil
	}
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	return strings.ToUpper(hex.EncodeToString([]byte(s))), nil
}

// hexUnsigned prints u as dialects.MySQL writes it: uppercase, without a sign and
// without leading zeros.
func hexUnsigned(u uint64) string {
	return strings.ToUpper(strconv.FormatUint(u, 16))
}

// roundToInt64 rounds v half away from zero, the way dialects.MySQL converts a fraction
// to an integer, and clamps rather than converting a value no int64 holds, since
// that conversion is undefined in Go.
func roundToInt64(v float64) int64 {
	r := math.Round(v)
	switch {
	case math.IsNaN(r):
		return 0
	case r >= math.MaxInt64:
		return math.MaxInt64
	case r <= math.MinInt64:
		return math.MinInt64
	default:
		return int64(r)
	}
}

// mysqlShift implements dialects.MySQL's "<<" and ">>", which shift an unsigned 64-bit
// value. dialects.SQLite's own shifts are signed: ">>" copies the sign bit rather than
// bringing in zeros, so -1 >> 1 stayed -1 where dialects.MySQL answers the 63 one bits
// below the sign, and a shift by 64 or more left a negative value untouched
// where dialects.MySQL clears it. dialects.SQLite also reads a negative count as a shift the other
// way, where dialects.MySQL reads the count as unsigned too and so shifts past the width.
//
// A result whose top bit is set comes back as the negative integer holding those
// bits, because dialects.SQLite has no unsigned 64-bit integer to return it in: ~0 >> 0
// is 18446744073709551615 in dialects.MySQL and -1 here, carrying the same bits under the
// only reading dialects.SQLite has for them.
func mysqlShift(left bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		binary, isBinary := bytesOperand(args[0])
		var v int64
		if !isBinary {
			var ok bool
			v, ok = toInt(args[0])
			if !ok {
				return nil, nil
			}
		}
		n, ok := toInt(args[1])
		if !ok {
			return nil, nil
		}
		count := uint64(n) //nolint:gosec // dialects.MySQL reads the count as unsigned, so a negative one is a count past the width
		if isBinary {
			// A binary string keeps its length, and the count is unsigned here
			// too, so a negative one shifts past the width and clears it.
			return bytesShift(binary, count, left), nil
		}
		if count >= 64 {
			return int64(0), nil
		}
		u := uint64(v)
		if left {
			u <<= count
		} else {
			u >>= count
		}
		return int64(u), nil //nolint:gosec // dialects.SQLite has no unsigned integer to answer with; the bits are the answer
	}
}

// fnMySQLQuote implements dialects.MySQL QUOTE(x): the value as a literal a dialects.MySQL
// statement can hold. dialects.SQLite's own quote() escapes by doubling the single quote
// and leaves a number unquoted, which is right for dialects.SQLite and is neither the
// escape nor the shape dialects.MySQL reads back.
func fnMySQLQuote(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		// dialects.MySQL answers the word rather than NULL, so the result can be pasted
		// into a statement whatever the value was.
		return nullText, nil
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	// Escaping runs over bytes rather than runes: every byte dialects.MySQL escapes is
	// ASCII, and no byte of a multi-byte UTF-8 sequence is.
	for i := range len(s) {
		switch s[i] {
		case '\'', '\\':
			b.WriteByte('\\')
			b.WriteByte(s[i])
		case 0:
			b.WriteString(`\0`)
		case 26:
			b.WriteString(`\Z`)
		default:
			b.WriteByte(s[i])
		}
	}
	b.WriteByte('\'')
	return b.String(), nil
}

// fnMySQLASCII implements dialects.MySQL ASCII(x): the leftmost byte of the value's
// string form, which is a number in 0..255. The shared ascii() helper answers
// the code point, which is what dialects.PostgreSQL means by the name and what makes
// ASCII indistinguishable from ORD for a dialects.MySQL caller.
func fnMySQLASCII(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	if s == "" {
		return int64(0), nil
	}
	return int64(s[0]), nil
}

// fnMySQLUnhex implements dialects.MySQL UNHEX(s), the inverse of HEX for a string.
func fnMySQLUnhex(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	if len(s)%2 == 1 {
		// dialects.MySQL reads an odd digit count as having a leading zero, so
		// UNHEX('ABC') decodes '0ABC' and UNHEX('0') decodes '00'. Refusing it
		// dropped every value whose digit count happened to be odd.
		s = "0" + s
	}
	raw, decodeErr := hex.DecodeString(s)
	if decodeErr != nil {
		// dialects.MySQL answers NULL rather than raising for a non-hexadecimal argument.
		return nil, nil //nolint:nilerr // NULL is dialects.MySQL's documented result here
	}
	// The bytes are handed back as a blob rather than as text, which is what
	// dialects.MySQL's UNHEX answers and what keeps a zero byte in them: a text value
	// carrying one is cut there on its way into the next function's arguments.
	return raw, nil
}

// fnBitXor implements dialects.MySQL's "^", a bitwise exclusive OR. dialects.SQLite has &, | and ~
// but no XOR operator, and the expression built from them would evaluate each
// operand twice.
//
// The arithmetic is unsigned, as dialects.MySQL's bitwise operators are: an operand with
// the high bit set is 2^64-1 rather than -1 there, and an operand written past
// int64 is an ordinary literal. The result comes back as the same 64 bits in
// dialects.SQLite's only integer, which is signed, so a result with the high bit set
// reads as a negative number where dialects.MySQL would print it unsigned.
//
// A NULL operand gives NULL, as every arithmetic operator in SQL does. An
// operand that is not a number gives NULL rather than an error, as the other
// operator helpers here do.
func fnBitXor(args []driver.Value) (driver.Value, error) {
	a, ok1 := toUint64Bits(args[0])
	b, ok2 := toUint64Bits(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return int64(a ^ b), nil //nolint:gosec // the bits are the value; dialects.SQLite has no unsigned integer
}

// toUint64Bits reads an operand as the 64 bits dialects.MySQL's bitwise operators work
// on. A negative number is its two's complement, and a text operand past int64
// is read as the unsigned literal dialects.MySQL would have taken it for.
func toUint64Bits(v driver.Value) (uint64, bool) {
	if s, ok := v.(string); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64); err == nil {
			return n, true
		}
	}
	if b, ok := v.([]byte); ok {
		if n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil {
			return n, true
		}
	}
	n, ok := toInt(v)
	if !ok {
		return 0, false
	}
	return uint64(n), true //nolint:gosec // a negative operand is its two's complement, which is what dialects.MySQL uses
}

// similarToRegexp converts a SQL SIMILAR TO pattern into a Go regular
// expression. SIMILAR TO is regex-like already: it keeps |, *, +, ?, {}, (), and
// [] but spells "any run" as "%" and "any character" as "_", and it matches the
// whole string.
func similarToRegexp(pattern string) string {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; c {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		case '.', '^', '$':
			b.WriteString("\\")
			b.WriteByte(c)
		case '\\':
			if i+1 < len(pattern) {
				b.WriteByte(c)
				i++
				b.WriteByte(pattern[i])
				break
			}
			// A pattern that ends in an escape has nothing to escape. Written
			// through, the backslash would escape the anchor this appends instead,
			// so "a\" became ^a\$ — a regex matching a literal "$" rather than the
			// backslash the pattern ends with.
			b.WriteString(`\\`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteString("$")
	return b.String()
}

// fnSimilarSubstring implements the SQL-standard SUBSTRING(x SIMILAR p ESCAPE
// e) and its older spelling SUBSTRING(x FROM p FOR e): the pattern is a SIMILAR
// TO pattern in which two occurrences of the escape character followed by a
// double quote delimit the part to return.
//
// It used to be refused in one spelling and read as a position and a length in
// the other, which answered NULL for every row.
func fnSimilarSubstring(args []driver.Value) (driver.Value, error) {
	subject, ok1 := toString(args[0])
	pattern, ok2 := toString(args[1])
	escape, ok3 := toString(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	if len([]rune(escape)) != 1 {
		return nil, fmt.Errorf("%w: the SUBSTRING escape must be one character, got %q", ErrInvalidCast, escape)
	}
	marker := escape + `"`
	before, rest, found := strings.Cut(pattern, marker)
	if !found {
		// No marker pair at all: the whole match is the result, which is what
		// dialects.PostgreSQL answers there.
		re, err := compileRegexp(similarToRegexp(unescapeSimilar(pattern, escape)))
		if err != nil {
			return nil, err
		}
		if m := re.FindString(subject); m != "" || re.MatchString(subject) {
			return m, nil
		}
		return nil, nil
	}
	middle, after, closed := strings.Cut(rest, marker)
	if !closed {
		return nil, fmt.Errorf("%w: the SUBSTRING pattern has one %s marker and needs two", ErrInvalidCast, marker)
	}
	// Each portion is wrapped in its own group before the three are joined, so
	// an alternation inside one of them cannot reach across into the next.
	re, err := compileRegexp(
		"^(?:" + trimAnchors(similarToRegexp(unescapeSimilar(before, escape)), true, true) + ")" +
			"(" + trimAnchors(similarToRegexp(unescapeSimilar(middle, escape)), true, true) + ")" +
			"(?:" + trimAnchors(similarToRegexp(unescapeSimilar(after, escape)), true, true) + ")$")
	if err != nil {
		return nil, err
	}
	groups := re.FindStringSubmatch(subject)
	if groups == nil {
		return nil, nil
	}
	return groups[1], nil
}

// unescapeSimilar drops the escape character in front of a character it made
// literal, leaving the pattern in the form similarToRegexp reads. The marker
// pair has already been taken out by the caller.
func unescapeSimilar(pattern, escape string) string {
	if escape == `\` {
		return pattern
	}
	var b strings.Builder
	runes := []rune(pattern)
	esc := []rune(escape)[0]
	for i := 0; i < len(runes); i++ {
		if runes[i] != esc || i+1 >= len(runes) {
			b.WriteRune(runes[i])
			continue
		}
		i++
		if strings.ContainsRune(`%_|*+?{}()[].^$\`, runes[i]) {
			b.WriteRune('\\')
		}
		b.WriteRune(runes[i])
	}
	return b.String()
}

// trimAnchors removes the ^ and $ similarToRegexp puts around a whole pattern,
// so the three parts of a marked pattern can be stitched into one expression
// that is anchored only at its own ends.
func trimAnchors(expr string, dropStart, dropEnd bool) string {
	if dropEnd {
		expr = strings.TrimSuffix(expr, "$")
	}
	if dropStart {
		expr = strings.TrimPrefix(expr, "^")
	}
	return expr
}

// fnSimilarTo implements dialects.PostgreSQL's "x SIMILAR TO p".
func fnSimilarTo(args []driver.Value) (driver.Value, error) {
	pattern, ok1 := toString(args[0])
	subject, ok2 := toString(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	re, err := compileRegexp(similarToRegexp(pattern))
	if err != nil {
		return nil, err
	}
	return boolToInt(re.MatchString(subject)), nil
}

// fnMySQLOrd implements dialects.MySQL ORD(s): the code of the first character, read as
// its UTF-8 bytes in big-endian order. A single-byte character therefore gives
// the same answer as ASCII(), and a multi-byte one does not.
func fnMySQLOrd(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	if s == "" {
		return int64(0), nil
	}
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && size <= 1 {
		return int64(s[0]), nil
	}
	var code int64
	for _, b := range []byte(s[:size]) {
		code = code<<8 | int64(b)
	}
	return code, nil
}

// fnJSONUnquote implements dialects.MySQL JSON_UNQUOTE(s): a JSON string literal becomes
// its value, and anything else is returned unchanged.
func fnJSONUnquote(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return s, nil
	}
	var out string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return s, nil //nolint:nilerr // dialects.MySQL leaves a value it cannot unquote alone
	}
	return out, nil
}

// fnOverlay implements dialects.PostgreSQL OVERLAY(target PLACING replacement FROM start
// [FOR count]): replacement is written over count characters of target starting
// at the 1-based start, defaulting to the length of replacement.
func fnOverlay(args []driver.Value) (driver.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return nil, fmt.Errorf("dialect: OVERLAY expects 3 or 4 arguments, got %d", len(args))
	}
	target, ok1 := toString(args[0])
	replacement, ok2 := toString(args[1])
	start, ok3 := toInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}
	runes := []rune(target)
	count := int64(len([]rune(replacement)))
	if len(args) == 4 {
		n, ok := toInt(args[3])
		if !ok {
			return nil, nil
		}
		count = n
	}
	// dialects.PostgreSQL defines the call as
	// substring(s from 1 for start-1) || replacement || substring(s from start+count),
	// so following the definition settles the boundaries rather than clamping
	// them one at a time. A start below 1 makes the first substring's length
	// negative, which is the error dialects.PostgreSQL raises; a negative count makes the
	// tail begin before the overlaid position, so part of the string is repeated,
	// which is the answer it gives.
	if start < 1 {
		return nil, errors.New("dialect: OVERLAY: negative substring length not allowed")
	}
	// Clamp in int64 before narrowing: a FOR count near math.MaxInt64 is a
	// perfectly ordinary dialects.SQLite integer literal, and start+count would wrap to a
	// negative slice bound.
	length := int64(len(runes))
	head := min(start-1, length)
	tailStart := int64(1)
	if count > length-start {
		tailStart = length + 1
	} else if start+count > 1 {
		tailStart = start + count
	}
	tail := min(max(tailStart-1, 0), length)
	return string(runes[:head]) + replacement + string(runes[tail:]), nil
}

// fnStrictConcat concatenates its arguments the way dialects.MySQL and dialects.GoogleSQL CONCAT
// do: a NULL anywhere makes the whole result NULL. dialects.SQLite's own concat() treats
// a NULL as an empty string, so a call passed through to it answered a plausible
// non-NULL string where the source dialect answers NULL — a wrong answer with no
// error to notice. dialects.PostgreSQL's concat() does ignore NULLs, so it keeps using
// dialects.SQLite's and never reaches this helper.
func fnStrictConcat(args []driver.Value) (driver.Value, error) {
	var b strings.Builder
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
		s, ok := toString(arg)
		if !ok {
			return nil, nil
		}
		b.WriteString(s)
	}
	return b.String(), nil
}
