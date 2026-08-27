package dialect

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
// SQLite. Like the cast helpers in cast.go, each is a silent divergence: the
// query runs and returns a plausible answer that the source dialect would never
// give.
//
//   - "/" is integer division in SQLite when both operands are integers, but
//     floating-point division in MySQL and GoogleSQL. An average or a ratio came
//     out truncated.
//   - A zero divisor answers NULL in SQLite and in MySQL, and raises in
//     PostgreSQL and GoogleSQL. The NULL reads as missing data rather than as
//     arithmetic the engine refused, so it survives into a report.
//   - LIKE folds ASCII case in SQLite. It is case-sensitive in PostgreSQL and
//     GoogleSQL, so a filter matched rows it should not have, and PostgreSQL's
//     ILIKE became indistinguishable from LIKE.
//   - MySQL reads "||" as a logical OR under its default sql_mode, where SQLite
//     concatenates.
//
// The operators are rewritten into helper calls rather than left to SQLite,
// since a pragma such as case_sensitive_like is connection-wide and would change
// the SQLite dialect's behavior too.

// ErrDivideByZero reports a division by zero in a dialect that raises for it.
// It stands on its own rather than wrapping ErrInvalidCast, which is about a
// value a target type cannot represent: a division by zero converts nothing,
// and an error reading "invalid cast: division by zero" sent the reader looking
// at CAST expressions their query does not contain.
var ErrDivideByZero = errors.New("dialect: division by zero")

// divideFloat implements the "/" operator for the dialects whose division is
// always floating point. MySQL answers NULL when the divisor is zero;
// GoogleSQL raises, and offers SAFE_DIVIDE for callers who want the NULL.
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
// SQLite does — two integers give an integer — but raises on a zero divisor
// where SQLite answers NULL. PostgreSQL is that dialect: 7 / 2 is 3 in both,
// and 7 / 0 stops the query in PostgreSQL.
//
// Both operands are read the way SQLite reads them, so only the zero divisor
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
// both operands are integers, and math.Mod otherwise. SQLite's own "%"
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
// than SQLite's. raiseOnZero says what a zero divisor does: PostgreSQL and
// GoogleSQL raise, MySQL answers NULL, and SQLite answers NULL, which is why
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

// integerDivide implements PostgreSQL's div(x, y) and GoogleSQL's DIV(x, y):
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

// truncateScale implements PostgreSQL's trunc(x, n) and GoogleSQL's
// TRUNC(x, n): x with everything past n decimal places cut off, toward zero, so
// trunc(-12.345, 2) is -12.34 rather than -12.35. A negative scale truncates to
// a power of ten, which is what PostgreSQL answers for trunc(12345.6, -2) with
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
		// finite value to nothing, which is the 0 PostgreSQL answers for
		// trunc(12.345, -400).
		return float64(0), nil
	case math.IsInf(factor, 0):
		// A scale past every decimal the value has keeps the value, which is
		// what PostgreSQL answers for trunc(12.345, 400).
		return x, nil
	}
	return math.Trunc(x*factor) / factor, nil
}

// widthBucket implements PostgreSQL's width_bucket(x, lo, hi, count): which of
// count equal-width buckets spanning lo..hi the value falls in, numbered from
// 1, with 0 for a value below the range and count+1 for one above it. The
// bounds may be given in either order, which is how a descending scale is
// bucketed, and a range of no width is refused the way PostgreSQL refuses it.
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

// numOperand is one arithmetic operand as SQLite reads it: an integer, or a
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

// integer is the operand as SQLite's "%" takes it, truncating toward zero and
// stopping at the ends of the int64 range. The clamp is the whole reason this
// is not a bare conversion: Go leaves int64(1e300) to the implementation, which
// on amd64 is the most negative int64, so "1e300 % 7" answered -1 where SQLite
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

// sqliteOperand applies the numeric affinity SQLite applies to an arithmetic
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
// pattern. PostgreSQL and GoogleSQL both use a backslash when no ESCAPE clause
// says otherwise, which is how a caller searches for a value containing "%" or
// "_". SQLite has no default escape at all, so this has to be honored here
// rather than left to the engine — a pattern that arrived without an ESCAPE
// clause has already lost the chance to be given one.
const likeEscape = '\\'

// likeCompare implements SQL LIKE, where "%" matches any run of characters and
// "_" matches exactly one. SQLite's own LIKE folds ASCII case; this one folds
// only when asked, so PostgreSQL and GoogleSQL keep their case-sensitive LIKE
// and PostgreSQL's ILIKE folds every character rather than just the ASCII ones.
func likeCompare(caseSensitive bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		pattern, ok1 := toString(args[0])
		subject, ok2 := toString(args[1])
		if !ok1 || !ok2 {
			return nil, nil
		}
		if !caseSensitive {
			pattern = foldCase(pattern)
			subject = foldCase(subject)
		}
		return boolToInt(likeMatch([]rune(pattern), []rune(subject))), nil
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
// characters "a%b" and nothing else. A trailing escape stands for itself rather
// than being an error, which is the same answer the SIMILAR TO translation
// gives; erroring would turn a questionable pattern into a failed query.
func likeMatch(pattern, subject []rune) bool {
	var (
		p, s          int
		star          = -1
		afterStarSubj int
	)
	for s < len(subject) {
		switch {
		case p < len(pattern) && pattern[p] == likeEscape && literalAt(pattern, p) == subject[s]:
			p += escapedWidth(pattern, p)
			s++
		case p < len(pattern) && pattern[p] != likeEscape && (pattern[p] == '_' || pattern[p] == subject[s]):
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
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '%' {
		p++
	}
	return p == len(pattern)
}

// literalAt is the character an escape at p stands for: the one after it, or
// the escape itself when it ends the pattern.
func literalAt(pattern []rune, p int) rune {
	if p+1 < len(pattern) {
		return pattern[p+1]
	}
	return likeEscape
}

// escapedWidth is how much of the pattern an escape at p consumes.
func escapedWidth(pattern []rune, p int) int {
	if p+1 < len(pattern) {
		return 2
	}
	return 1
}

// fnMySQLHex implements MySQL HEX(x): the hexadecimal digits of a number, or of
// a string's bytes. SQLite's own HEX only does the latter, so HEX(255) answered
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
		return hexUnsigned(uint64(x)), nil //nolint:gosec // reinterpreting the bits is what MySQL's unsigned reading is
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

// hexUnsigned prints u as MySQL writes it: uppercase, without a sign and
// without leading zeros.
func hexUnsigned(u uint64) string {
	return strings.ToUpper(strconv.FormatUint(u, 16))
}

// roundToInt64 rounds v half away from zero, the way MySQL converts a fraction
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

// mysqlShift implements MySQL's "<<" and ">>", which shift an unsigned 64-bit
// value. SQLite's own shifts are signed: ">>" copies the sign bit rather than
// bringing in zeros, so -1 >> 1 stayed -1 where MySQL answers the 63 one bits
// below the sign, and a shift by 64 or more left a negative value untouched
// where MySQL clears it. SQLite also reads a negative count as a shift the other
// way, where MySQL reads the count as unsigned too and so shifts past the width.
//
// A result whose top bit is set comes back as the negative integer holding those
// bits, because SQLite has no unsigned 64-bit integer to return it in: ~0 >> 0
// is 18446744073709551615 in MySQL and -1 here, carrying the same bits under the
// only reading SQLite has for them.
func mysqlShift(left bool) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		v, ok := toInt(args[0])
		if !ok {
			return nil, nil
		}
		n, ok := toInt(args[1])
		if !ok {
			return nil, nil
		}
		count := uint64(n) //nolint:gosec // MySQL reads the count as unsigned, so a negative one is a count past the width
		if count >= 64 {
			return int64(0), nil
		}
		u := uint64(v) //nolint:gosec // the shift is defined on the bits, which is what the reinterpretation keeps
		if left {
			u <<= count
		} else {
			u >>= count
		}
		return int64(u), nil //nolint:gosec // SQLite has no unsigned integer to answer with; the bits are the answer
	}
}

// fnMySQLQuote implements MySQL QUOTE(x): the value as a literal a MySQL
// statement can hold. SQLite's own quote() escapes by doubling the single quote
// and leaves a number unquoted, which is right for SQLite and is neither the
// escape nor the shape MySQL reads back.
func fnMySQLQuote(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		// MySQL answers the word rather than NULL, so the result can be pasted
		// into a statement whatever the value was.
		return nullText, nil
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	// Escaping runs over bytes rather than runes: every byte MySQL escapes is
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

// fnMySQLASCII implements MySQL ASCII(x): the leftmost byte of the value's
// string form, which is a number in 0..255. The shared ascii() helper answers
// the code point, which is what PostgreSQL means by the name and what makes
// ASCII indistinguishable from ORD for a MySQL caller.
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

// fnMySQLUnhex implements MySQL UNHEX(s), the inverse of HEX for a string.
func fnMySQLUnhex(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	if len(s)%2 == 1 {
		// MySQL reads an odd digit count as having a leading zero, so
		// UNHEX('ABC') decodes '0ABC' and UNHEX('0') decodes '00'. Refusing it
		// dropped every value whose digit count happened to be odd.
		s = "0" + s
	}
	raw, decodeErr := hex.DecodeString(s)
	if decodeErr != nil {
		// MySQL answers NULL rather than raising for a non-hexadecimal argument.
		return nil, nil //nolint:nilerr // NULL is MySQL's documented result here
	}
	// The bytes are handed back as a blob rather than as text, which is what
	// MySQL's UNHEX answers and what keeps a zero byte in them: a text value
	// carrying one is cut there on its way into the next function's arguments.
	return raw, nil
}

// binaryOperatorPass rewrites "a <op> b" into helper(a, b) for every operator
// token whose text is op. Both operands must be primary expressions, the same
// requirement MySQL's DIV rewrite has.
//
// This is the pass for an operator that binds tighter than everything around it,
// where one primary is the whole operand: MySQL's bitwise "^", and PostgreSQL's
// exponentiation "^". An operator that shares its precedence level with others
// takes binaryChainOperatorPass instead.
func binaryOperatorPass(tokens []token, op, helper string) ([]token, error) {
	return binaryOperatorPassWith(tokens, op, helper, operandBack)
}

// binaryChainOperatorPass is binaryOperatorPass for an operator that shares a
// precedence level with "*", "/" and "%", where the left operand is the whole
// chain to its left rather than the primary beside the operator: MySQL reads
// "7 % 4 / 2" as "(7 % 4) / 2", and taking only the "4" answered 1 where MySQL
// answers 1.5.
func binaryChainOperatorPass(tokens []token, op, helper string) ([]token, error) {
	return binaryOperatorPassWith(tokens, op, helper, chainOperandBack)
}

// binaryOperatorPassWith is the pass above, with left deciding how much of what
// stands before the operator the operand takes.
func binaryOperatorPassWith(tokens []token, op, helper string, operandOf func([]token) ([]token, int, bool)) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if isOpEq(tokens[i], op) {
			left, start, ok := operandOf(out)
			if !ok {
				return nil, fmt.Errorf("%w: left operand of %s is not a primary expression", ErrUnsupportedSyntax, op)
			}
			rightStart := nextSig(tokens, i+1)
			rightEnd, ok := primaryEndForward(tokens, i+1)
			if !ok {
				return nil, fmt.Errorf("%w: right operand of %s is not a primary expression", ErrUnsupportedSyntax, op)
			}
			out = append(out[:start], callTokens(helper, left, tokens[rightStart:rightEnd+1])...)
			i = rightEnd + 1
			continue
		}
		out = append(out, tokens[i])
		i++
	}
	return out, nil
}

// likePass rewrites "a LIKE b" (and the ILIKE and NOT forms) into a call to the
// matching helper, so the comparison uses the source dialect's case rules rather
// than SQLite's. A pattern with an ESCAPE clause is left alone: SQLite handles
// it natively and the helpers do not model a custom escape character.
func likePass(tokens []token, keyword, helper string) ([]token, error) {
	return infixMatchPass(tokens, keyword, helper, escapeClauseFollows)
}

// regexpPass rewrites "a REGEXP b" into a call, so the match runs under MySQL's
// collation rather than Go's default. RLIKE is renamed to REGEXP before this
// runs, so both spellings arrive here as one.
func regexpPass(tokens []token, helper string) ([]token, error) {
	return infixMatchPass(tokens, "REGEXP", helper, callFormFollows)
}

// escapeClauseFollows reports whether an ESCAPE clause follows the pattern,
// which is the LIKE form this package leaves to SQLite.
func escapeClauseFollows(tokens []token, _, rightEnd int) bool {
	esc := nextSig(tokens, rightEnd+1)
	return esc >= 0 && isWordEq(tokens[esc], "ESCAPE")
}

// callFormFollows reports whether the keyword at i names a function rather than
// standing between two operands. A parenthesis immediately after the name with
// nothing between is how MySQL's own parser tells a call from an operator, which
// is the same rule the MOD pass uses.
func callFormFollows(tokens []token, i, _ int) bool {
	return i+1 < len(tokens) && isOpEq(tokens[i+1], "(")
}

// infixMatchPass rewrites "a KEYWORD b" (and its NOT form) into helper(b, a),
// skipping the occurrences skip reports. The pattern comes first because that is
// the argument order SQLite's own like() and regexp() take.
func infixMatchPass(tokens []token, keyword, helper string, skip func(tokens []token, i, rightEnd int) bool) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if !isWordEq(tokens[i], keyword) {
			out = append(out, tokens[i])
			i++
			continue
		}
		rightStart := nextSig(tokens, i+1)
		rightEnd, ok := primaryEndForward(tokens, i+1)
		if !ok {
			return nil, fmt.Errorf("%w: right operand of %s is not a primary expression", ErrUnsupportedSyntax, keyword)
		}
		if skip(tokens, i, rightEnd) {
			out = append(out, tokens[i])
			i++
			continue
		}

		// "a NOT LIKE b" puts NOT between the operand and the keyword; it belongs
		// to the comparison, not to the left operand.
		negated := false
		if n := lastSig(out); n >= 0 && isWordEq(out[n], "NOT") {
			negated = true
			out = out[:n]
		}
		left, start, ok := operandBack(out)
		if !ok {
			return nil, fmt.Errorf("%w: left operand of %s is not a primary expression", ErrUnsupportedSyntax, keyword)
		}
		out = out[:start]
		if negated {
			out = append(out, wordToken("NOT"), spaceToken())
		}
		// The helper takes the pattern first, matching SQLite's own like().
		out = append(out, callTokens(helper, tokens[rightStart:rightEnd+1], left)...)
		i = rightEnd + 1
	}
	return out, nil
}

// shiftPass rewrites "a << b" and "a >> b" into calls to the helpers above.
//
// The operands are not single primaries. MySQL and SQLite agree that "*", "/",
// "%", "+" and "-" all bind tighter than a shift, so "1 + 2 >> 1" is "(1 + 2) >>
// 1" to both of them and taking only the primary beside the operator would have
// answered 2 where both engines answer 1. They disagree about "&" and "|", which
// MySQL puts below the shifts and SQLite puts on the same level; taking the
// operand no further than the arithmetic gives MySQL's grouping for those too.
func shiftPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		var op string
		switch {
		case isOpEq(tokens[i], "<<"):
			op = "mysql_shift_left"
		case isOpEq(tokens[i], ">>"):
			op = "mysql_shift_right"
		default:
			out = append(out, tokens[i])
			i++
			continue
		}
		left, start, ok := shiftOperandBack(out)
		if !ok {
			return nil, fmt.Errorf("%w: left operand of %s is not an expression", ErrUnsupportedSyntax, tokens[i].text)
		}
		rightStart := nextSig(tokens, i+1)
		rightEnd, ok := shiftOperandForward(tokens, i+1)
		if !ok {
			return nil, fmt.Errorf("%w: right operand of %s is not an expression", ErrUnsupportedSyntax, tokens[i].text)
		}
		out = append(out[:start], callTokens(op, left, tokens[rightStart:rightEnd+1])...)
		i = rightEnd + 1
	}
	return out, nil
}

// shiftOperandBack extracts the expression that ends the tokens emitted so far,
// walking back across the arithmetic that binds tighter than a shift.
func shiftOperandBack(out []token) ([]token, int, bool) {
	start, ok := primaryStartBack(out)
	if !ok {
		return nil, 0, false
	}
	start = extendUnaryBack(out, start)
	for {
		prev := prevSig(out, start)
		if prev < 0 || !isShiftTighterOperator(out[prev]) || isUnarySign(out, prev) {
			break
		}
		next, ok := primaryStartBack(out[:prev])
		if !ok {
			break
		}
		start = extendUnaryBack(out, next)
	}
	return append([]token{}, trimSpaceTokens(out[start:])...), start, true
}

// extendUnaryBack walks start back over the prefix operators that bind tighter
// than a shift: MySQL's bitwise complement, and a sign that is not the binary
// operator of the same spelling.
func extendUnaryBack(tokens []token, start int) int {
	for {
		prev := prevSig(tokens, start)
		if prev < 0 || (!isOpEq(tokens[prev], "~") && !isUnarySign(tokens, prev)) {
			return start
		}
		start = prev
	}
}

// shiftOperandForward returns the index of the last token of the expression
// beginning at or after from, walking forward across the same arithmetic.
func shiftOperandForward(tokens []token, from int) (int, bool) {
	// A complement stands in front of its operand, and the caller slices from
	// the first significant token, so skipping it here keeps it in the operand.
	for {
		s := nextSig(tokens, from)
		if s < 0 || !isOpEq(tokens[s], "~") {
			break
		}
		from = s + 1
	}
	end, ok := primaryEndForward(tokens, from)
	if !ok {
		return 0, false
	}
	for {
		op := nextSig(tokens, end+1)
		if op < 0 || !isShiftTighterOperator(tokens[op]) {
			return end, true
		}
		next, ok := primaryEndForward(tokens, op+1)
		if !ok {
			return end, true
		}
		end = next
	}
}

// isShiftTighterOperator reports whether t binds tighter than "<<" and ">>" in
// MySQL. DIV, MOD and "/" are already calls by the time the shift pass runs, so
// only the four that stay operators are listed.
func isShiftTighterOperator(t token) bool {
	return isOpEq(t, "*") || isOpEq(t, "%") || isOpEq(t, "+") || isOpEq(t, "-")
}

// isUnarySign reports whether the "+" or "-" at i is a sign on the operand after
// it rather than the binary operator of the same spelling. A sign follows
// nothing, an operator other than a closing parenthesis, or one of the words in
// operandExpectingKeywords and clauseKeywords; anything else before it ends an
// operand, which makes the token binary. An unquoted word neither table lists is
// read as a column name, and a keyword missing from them turns into a SQLite
// syntax error rather than into a different answer, because the operand would
// then start at the keyword.
func isUnarySign(tokens []token, i int) bool {
	if !isOpEq(tokens[i], "+") && !isOpEq(tokens[i], "-") {
		return false
	}
	prev := prevSig(tokens, i)
	if prev < 0 {
		return true
	}
	switch {
	case isOpEq(tokens[prev], ")"):
		return false
	case tokens[prev].kind == tokOp:
		return true
	case tokens[prev].kind == tokWord:
		word := strings.ToUpper(tokens[prev].text)
		return operandExpectingKeywords[word] || clauseKeywords[word]
	default:
		return false
	}
}

// clauseKeywords are the unquoted words that introduce a clause and so stand in
// front of an expression. Together with operandExpectingKeywords, which lists
// the words that demand an operand inside an expression, they are the words a
// "+" or "-" can follow as a sign rather than as the binary operator.
var clauseKeywords = map[string]bool{ //nolint:gochecknoglobals // a fixed table read by the sign rule
	"SELECT": true, kwWhere: true, kwHaving: true, "ON": true, "BY": true,
	kwLimit: true, "OFFSET": true, "VALUES": true, "SET": true,
	"RETURNING": true, "USING": true, "XOR": true,
}

// operandBack extracts the primary expression that ends the tokens emitted so
// far, returning it and the index it started at.
func operandBack(out []token) ([]token, int, bool) {
	start, ok := primaryStartBack(out)
	if !ok {
		return nil, 0, false
	}
	return append([]token{}, trimSpaceTokens(out[start:])...), start, true
}

// chainOperandBack is operandBack extended over the operators that share the
// caller's precedence level, and parenthesized when it took more than one
// primary so a later pass reading the same tokens cannot regroup them.
func chainOperandBack(out []token) ([]token, int, bool) {
	start, ok := primaryStartBack(out)
	if !ok {
		return nil, 0, false
	}
	start, extended := operandChainStartBack(out, start)
	operand := append([]token{}, trimSpaceTokens(out[start:])...)
	if extended {
		operand = append([]token{opToken("(")}, append(operand, opToken(")"))...)
	}
	return operand, start, true
}

// callTokens builds "name(a, b)".
func callTokens(name string, a, b []token) []token {
	repl := make([]token, 0, len(a)+len(b)+5)
	repl = append(repl, wordToken(name), opToken("("))
	repl = append(repl, a...)
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, b...)
	repl = append(repl, opToken(")"))
	return repl
}

// fnBitXor implements MySQL's "^", a bitwise exclusive OR. SQLite has &, | and ~
// but no XOR operator, and the expression built from them would evaluate each
// operand twice.
//
// The arithmetic is unsigned, as MySQL's bitwise operators are: an operand with
// the high bit set is 2^64-1 rather than -1 there, and an operand written past
// int64 is an ordinary literal. The result comes back as the same 64 bits in
// SQLite's only integer, which is signed, so a result with the high bit set
// reads as a negative number where MySQL would print it unsigned.
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
	return int64(a ^ b), nil //nolint:gosec // the bits are the value; SQLite has no unsigned integer
}

// toUint64Bits reads an operand as the 64 bits MySQL's bitwise operators work
// on. A negative number is its two's complement, and a text operand past int64
// is read as the unsigned literal MySQL would have taken it for.
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
	return uint64(n), true //nolint:gosec // a negative operand is its two's complement, which is what MySQL uses
}

// unaryNotPass rewrites MySQL's "!" into a parenthesized NOT over the primary it
// applies to.
//
// MySQL's "!" binds tighter than a comparison while SQLite's NOT binds looser
// than one, so "!a = b" written as "NOT a = b" changes from negating a to
// negating the comparison. Wrapping the result keeps it a primary, which is what
// the operator was.
//
// A run of them is one operand each way round: "!!a" is a double negation in
// MySQL, and the operand of the last one is what the primary rule applies to.
// The operand is rewritten too, so a "!" inside a parenthesized one is not left
// for SQLite.
//
// "!=" is one token to the tokenizer, so a not-equal comparison never reaches
// here.
func unaryNotPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if !isOpEq(tokens[i], "!") {
			out = append(out, tokens[i])
			i++
			continue
		}

		// Every "!" up to the operand, so "!!a" negates twice rather than failing
		// on an operand that is another "!".
		depth := 0
		j := i
		for j >= 0 && j < len(tokens) && isOpEq(tokens[j], "!") {
			depth++
			j = nextSig(tokens, j+1)
		}
		if j < 0 {
			return nil, fmt.Errorf("%w: operand of ! is not a primary expression", ErrUnsupportedSyntax)
		}
		start := j
		end, ok := primaryEndForward(tokens, j)
		if !ok {
			return nil, fmt.Errorf("%w: operand of ! is not a primary expression", ErrUnsupportedSyntax)
		}
		operand, err := unaryNotPass(tokens[start : end+1])
		if err != nil {
			return nil, err
		}

		for range depth {
			out = append(out, opToken("("), wordToken("NOT"), spaceToken())
		}
		out = append(out, operand...)
		for range depth {
			out = append(out, opToken(")"))
		}
		i = end + 1
	}
	return out, nil
}

// rejectWordPass refuses a construct that has no faithful SQLite spelling,
// naming the construct rather than letting SQLite answer with a syntax error
// near a keyword it does not know.
func rejectWordPass(tokens []token, keyword, because string) error {
	for _, t := range tokens {
		if isWordEq(t, keyword) {
			return fmt.Errorf("%w: %s is not supported; %s", ErrUnsupportedSyntax, keyword, because)
		}
	}
	return nil
}

// replaceOperatorWithWord swaps an operator token for a keyword, spacing it so
// "a||b" renders as "a OR b" rather than "aORb".
func replaceOperatorWithWord(tokens []token, op, keyword string) []token {
	out := make([]token, 0, len(tokens))
	for i, t := range tokens {
		if !isOpEq(t, op) {
			out = append(out, t)
			continue
		}
		if n := len(out); n > 0 && out[n-1].kind != tokWhitespace {
			out = append(out, spaceToken())
		}
		out = append(out, wordToken(keyword))
		if i+1 < len(tokens) && tokens[i+1].kind != tokWhitespace {
			out = append(out, spaceToken())
		}
	}
	return out
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

// fnSimilarTo implements PostgreSQL's "x SIMILAR TO p".
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

// similarToPass rewrites "a SIMILAR TO b" into similar_to(b, a). SIMILAR TO is
// two keywords, so it cannot go through the single-keyword LIKE pass.
func similarToPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if !isWordEq(tokens[i], "SIMILAR") {
			out = append(out, tokens[i])
			i++
			continue
		}
		to := nextSig(tokens, i+1)
		if to < 0 || !isWordEq(tokens[to], "TO") {
			out = append(out, tokens[i])
			i++
			continue
		}
		rightStart := nextSig(tokens, to+1)
		rightEnd, ok := primaryEndForward(tokens, to+1)
		if !ok {
			return nil, fmt.Errorf("%w: right operand of SIMILAR TO is not a primary expression", ErrUnsupportedSyntax)
		}
		negated := false
		if n := lastSig(out); n >= 0 && isWordEq(out[n], "NOT") {
			negated = true
			out = out[:n]
		}
		left, start, ok := operandBack(out)
		if !ok {
			return nil, fmt.Errorf("%w: left operand of SIMILAR TO is not a primary expression", ErrUnsupportedSyntax)
		}
		out = out[:start]
		if negated {
			out = append(out, wordToken("NOT"), spaceToken())
		}
		out = append(out, callTokens("similar_to", tokens[rightStart:rightEnd+1], left)...)
		i = rightEnd + 1
	}
	return out, nil
}

// fnMySQLOrd implements MySQL ORD(s): the code of the first character, read as
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

// fnJSONUnquote implements MySQL JSON_UNQUOTE(s): a JSON string literal becomes
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
		return s, nil //nolint:nilerr // MySQL leaves a value it cannot unquote alone
	}
	return out, nil
}

// fnOverlay implements PostgreSQL OVERLAY(target PLACING replacement FROM start
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
	// PostgreSQL defines the call as
	// substring(s from 1 for start-1) || replacement || substring(s from start+count),
	// so following the definition settles the boundaries rather than clamping
	// them one at a time. A start below 1 makes the first substring's length
	// negative, which is the error PostgreSQL raises; a negative count makes the
	// tail begin before the overlaid position, so part of the string is repeated,
	// which is the answer it gives.
	if start < 1 {
		return nil, errors.New("dialect: OVERLAY: negative substring length not allowed")
	}
	// Clamp in int64 before narrowing: a FOR count near math.MaxInt64 is a
	// perfectly ordinary SQLite integer literal, and start+count would wrap to a
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

// trimKeywords are the SQL-standard TRIM specifications, mapped to the SQLite
// function that trims that end.
var trimKeywords = map[string]string{
	"BOTH":     fnNameTrim,
	"LEADING":  "LTRIM",
	"TRAILING": "RTRIM",
}

// rewriteTrim implements the SQL-standard TRIM(BOTH 'x' FROM s) form that MySQL
// and PostgreSQL accept and SQLite does not, mapping it onto SQLite's
// trim/ltrim/rtrim with a character-set argument. The specification is optional
// and defaults to BOTH. The comma form, TRIM(s, 'x'), is already SQLite's own
// and is left alone.
func rewriteTrim(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	from := topLevelWord(tokens, open, closeIdx, "FROM")
	if from < 0 {
		return nil, false, nil
	}
	fn := fnNameTrim
	charsStart := open + 1
	if spec := nextSig(tokens, open+1); spec >= 0 && spec < from {
		if mapped, ok := trimKeywords[strings.ToUpper(tokens[spec].text)]; ok {
			fn = mapped
			charsStart = spec + 1
		}
	}
	subject, err := recurse(tokens[from+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	chars, err := recurse(tokens[charsStart:from])
	if err != nil {
		return nil, false, err
	}
	subject = trimSpaceTokens(subject)
	chars = trimSpaceTokens(chars)

	repl := make([]token, 0, len(subject)+len(chars)+5)
	repl = append(repl, wordToken(fn), opToken("("))
	repl = append(repl, subject...)
	if len(chars) > 0 {
		repl = append(repl, opToken(","), spaceToken())
		repl = append(repl, chars...)
	}
	return append(repl, opToken(")")), true, nil
}

// rewriteOverlay implements PostgreSQL OVERLAY(x PLACING y FROM n [FOR m]),
// turning the keyword-separated arguments into a call to the helper.
func rewriteOverlay(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	placing := topLevelWord(tokens, open, closeIdx, "PLACING")
	from := topLevelWord(tokens, open, closeIdx, "FROM")
	if placing < 0 || from < 0 {
		return nil, false, nil
	}
	forKw := topLevelWord(tokens, open, closeIdx, "FOR")
	startEnd := closeIdx
	if forKw > from {
		startEnd = forKw
	}
	parts := [][2]int{{open + 1, placing}, {placing + 1, from}, {from + 1, startEnd}}
	if forKw > from {
		parts = append(parts, [2]int{forKw + 1, closeIdx})
	}

	repl := make([]token, 0, closeIdx-open+8)
	repl = append(repl, wordToken("overlay"), opToken("("))
	for i, part := range parts {
		arg, err := recurse(tokens[part[0]:part[1]])
		if err != nil {
			return nil, false, err
		}
		if i > 0 {
			repl = append(repl, opToken(","), spaceToken())
		}
		repl = append(repl, trimSpaceTokens(arg)...)
	}
	return append(repl, opToken(")")), true, nil
}

// unionDistinctPass drops the DISTINCT that MySQL and GoogleSQL allow after
// UNION. SQLite rejects the keyword, and its plain UNION already deduplicates.
func unionDistinctPass(tokens []token) []token {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		out = append(out, tokens[i])
		if isWordEq(tokens[i], "UNION") {
			if d := nextSig(tokens, i+1); d >= 0 && isWordEq(tokens[d], "DISTINCT") {
				i = d + 1
				continue
			}
		}
		i++
	}
	return out
}

// fnStrictConcat concatenates its arguments the way MySQL and GoogleSQL CONCAT
// do: a NULL anywhere makes the whole result NULL. SQLite's own concat() treats
// a NULL as an empty string, so a call passed through to it answered a plausible
// non-NULL string where the source dialect answers NULL — a wrong answer with no
// error to notice. PostgreSQL's concat() does ignore NULLs, so it keeps using
// SQLite's and never reaches this helper.
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
