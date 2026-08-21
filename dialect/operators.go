package dialect

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
var ErrDivideByZero = fmt.Errorf("%w: division by zero", ErrInvalidCast)

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
func fnMySQLHex(args []driver.Value) (driver.Value, error) {
	switch x := args[0].(type) {
	case nil:
		return nil, nil
	case int64:
		return strings.ToUpper(strconv.FormatInt(x, 16)), nil
	case float64:
		return strings.ToUpper(strconv.FormatInt(int64(x), 16)), nil
	case []byte:
		return strings.ToUpper(hex.EncodeToString(x)), nil
	}
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	// A numeric string is a number to MySQL, which hexes its value rather than
	// its digits.
	if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
		return strings.ToUpper(strconv.FormatInt(n, 16)), nil
	}
	return strings.ToUpper(hex.EncodeToString([]byte(s))), nil
}

// fnMySQLUnhex implements MySQL UNHEX(s), the inverse of HEX for a string.
func fnMySQLUnhex(args []driver.Value) (driver.Value, error) {
	s, ok := toString(args[0])
	if !ok {
		return nil, nil
	}
	raw, decodeErr := hex.DecodeString(s)
	if decodeErr != nil {
		// MySQL answers NULL rather than raising for a non-hexadecimal argument.
		return nil, nil //nolint:nilerr // NULL is MySQL's documented result here
	}
	return string(raw), nil
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
		if esc := nextSig(tokens, rightEnd+1); esc >= 0 && isWordEq(tokens[esc], "ESCAPE") {
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
