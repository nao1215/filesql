package dialect

import (
	"fmt"
	"strings"
)

// This file holds the shared machinery the per-dialect rewrite passes use to
// recognize and rewrite patterns in a token stream: token constructors,
// significance helpers, parenthesis matching, and "primary expression"
// boundary detection. The stream keeps whitespace and comment tokens so
// rendering preserves the original adjacency; rewrite rules therefore work in
// terms of "significant" tokens (everything except whitespace and comments).

// Function-name keywords recognized by more than one dialect's call pass.
const (
	fnNameCast    = "CAST"
	fnNameExtract = "EXTRACT"
)

// callRecurser rewrites a slice of argument tokens with a dialect's call pass so
// nested recognized calls inside a rewritten call are handled too.
type callRecurser func([]token) ([]token, error)

// rewriteExtractCall implements the C-1 rule shared by every dialect:
// EXTRACT(part FROM x) -> DATE_PART('part', x). It returns handled=false when the
// call is not the "part FROM x" form so the caller leaves it untouched.
func rewriteExtractCall(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	part := nextSig(tokens, open+1)
	if part < 0 || tokens[part].kind != tokWord {
		return nil, false, nil
	}
	from := nextSig(tokens, part+1)
	if from < 0 || !isWordEq(tokens[from], "FROM") {
		return nil, false, nil
	}
	expr, err := recurse(tokens[from+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	expr = trimSpaceTokens(expr)
	repl := make([]token, 0, len(expr)+6)
	repl = append(repl, wordToken("DATE_PART"), opToken("("))
	repl = append(repl, stringToken(strings.ToLower(tokens[part].text)))
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, expr...)
	repl = append(repl, opToken(")"))
	return repl, true, nil
}

// rewriteCastCall rewrites CAST(x AS type) into a call to the dialect's cast
// helper (M-8, P-8, G-4), so the conversion follows the source dialect's rules
// rather than SQLite's type affinity; see cast.go. An unmapped type leaves the
// call unchanged (handled=false) and is left to SQLite.
func rewriteCastCall(tokens []token, open, closeIdx int, d Dialect, helper string, recurse callRecurser) ([]token, bool, error) {
	as := topLevelWord(tokens, open, closeIdx, "AS")
	if as < 0 {
		return nil, false, nil
	}
	typeName := nextSig(tokens, as+1)
	if typeName < 0 || tokens[typeName].kind != tokWord {
		return nil, false, nil
	}
	if _, ok := lookupCastKind(d, tokens[typeName].text); !ok {
		return nil, false, nil
	}
	target, _, err := castTargetText(tokens, typeName)
	if err != nil {
		return nil, false, err
	}
	expr, err := recurse(tokens[open+1 : as])
	if err != nil {
		return nil, false, err
	}
	return castHelperCall(helper, trimSpaceTokens(expr), target), true, nil
}

// castTargetText renders the target type that starts at typeName, keeping any
// parameter list so CHAR(3) and DECIMAL(10,2) reach the helper intact. It also
// returns the index of the type's last token.
func castTargetText(tokens []token, typeName int) (string, int, error) {
	end := typeName
	if e, ok := adjacentCallEnd(tokens, typeName); ok {
		if e < 0 {
			return "", 0, fmt.Errorf("%w: unbalanced type parameters in a cast", ErrInvalidSyntax)
		}
		end = e
	}
	return render(tokens[typeName : end+1]), end, nil
}

// castHelperCall builds "helper(expr, 'target')".
func castHelperCall(helper string, expr []token, target string) []token {
	repl := make([]token, 0, len(expr)+6)
	repl = append(repl, wordToken(helper), opToken("("))
	repl = append(repl, expr...)
	repl = append(repl, opToken(","), spaceToken(), stringToken(target), opToken(")"))
	return repl
}

// intervalUnits maps the INTERVAL units that map cleanly onto a SQLite datetime
// modifier. Units outside this set (WEEK, QUARTER, compound units) are rejected.
var intervalUnits = map[string]string{
	"SECOND": unitSecond,
	"MINUTE": unitMinute,
	"HOUR":   unitHour,
	"DAY":    unitDay,
	"MONTH":  unitMonth,
	"YEAR":   unitYear,
}

// rewriteDateArith implements the shared "date +/- INTERVAL n unit" rewrite used
// by MySQL M-5 (DATE_ADD/DATE_SUB) and GoogleSQL G-7 (DATE_ADD/DATE_SUB/
// TIMESTAMP_ADD/TIMESTAMP_SUB): f(x, INTERVAL n unit) -> datetime(x, '±n unit').
// sign is "+" for the ADD forms and "-" for the SUB forms.
func rewriteDateArith(tokens []token, open, closeIdx int, sign string, recurse callRecurser) ([]token, bool, error) {
	comma := topLevelComma(tokens, open, closeIdx)
	if comma < 0 {
		return nil, false, nil
	}
	interval := nextSig(tokens, comma+1)
	if interval < 0 || !isWordEq(tokens[interval], "INTERVAL") {
		return nil, false, nil
	}
	value := nextSig(tokens, interval+1)
	if value < 0 || tokens[value].kind != tokNumber {
		return nil, false, fmt.Errorf("%w: INTERVAL value must be a numeric literal", ErrUnsupportedSyntax)
	}
	unitTok := nextSig(tokens, value+1)
	if unitTok < 0 || tokens[unitTok].kind != tokWord {
		return nil, false, fmt.Errorf("%w: INTERVAL is missing a unit", ErrUnsupportedSyntax)
	}
	unit, ok := intervalUnits[strings.ToUpper(tokens[unitTok].text)]
	if !ok {
		return nil, false, fmt.Errorf("%w: unsupported INTERVAL unit %q", ErrUnsupportedSyntax, tokens[unitTok].text)
	}
	if after := nextSig(tokens, unitTok+1); after != closeIdx {
		return nil, false, fmt.Errorf("%w: unsupported INTERVAL expression", ErrUnsupportedSyntax)
	}

	expr, err := recurse(tokens[open+1 : comma])
	if err != nil {
		return nil, false, err
	}
	expr = trimSpaceTokens(expr)
	modifier := sign + tokens[value].text + " " + unit
	repl := make([]token, 0, len(expr)+6)
	repl = append(repl, wordToken("datetime"), opToken("("))
	repl = append(repl, expr...)
	repl = append(repl, opToken(","), spaceToken(), stringToken(modifier), opToken(")"))
	return repl, true, nil
}

// rewriteRenameCall rewrites a call to use newName, recursing into its arguments.
// It backs simple function renames such as GoogleSQL FORMAT -> printf.
func rewriteRenameCall(tokens []token, open, closeIdx int, newName string, recurse callRecurser) ([]token, bool, error) {
	inner, err := recurse(tokens[open+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	repl := make([]token, 0, len(inner)+3)
	repl = append(repl, wordToken(newName), opToken("("))
	repl = append(repl, inner...)
	repl = append(repl, opToken(")"))
	return repl, true, nil
}

// trimSpaceTokens returns toks without leading or trailing whitespace tokens.
func trimSpaceTokens(toks []token) []token {
	lo, hi := 0, len(toks)
	for lo < hi && toks[lo].kind == tokWhitespace {
		lo++
	}
	for hi > lo && toks[hi-1].kind == tokWhitespace {
		hi--
	}
	return toks[lo:hi]
}

func wordToken(s string) token   { return token{kind: tokWord, text: s} }
func opToken(s string) token     { return token{kind: tokOp, text: s} }
func stringToken(s string) token { return token{kind: tokString, text: s} }
func spaceToken() token          { return token{kind: tokWhitespace, text: " "} }

// isSignificant reports whether t participates in the grammar (i.e. is not
// whitespace or a comment).
func isSignificant(t token) bool {
	switch t.kind {
	case tokWhitespace, tokLineComment, tokBlockComment:
		return false
	default:
		return true
	}
}

// isWordEq reports whether t is an unquoted word equal to kw, ignoring case.
func isWordEq(t token, kw string) bool {
	return t.kind == tokWord && strings.EqualFold(t.text, kw)
}

// isOpEq reports whether t is the operator/punctuation op.
func isOpEq(t token, op string) bool {
	return t.kind == tokOp && t.text == op
}

// isName reports whether t can name a column or table (an unquoted word or a
// quoted identifier).
func isName(t token) bool {
	return t.kind == tokWord || t.kind == tokQuotedIdent
}

// isLiteral reports whether t is a value literal.
func isLiteral(t token) bool {
	switch t.kind {
	case tokNumber, tokString, tokBlob, tokPlaceholder:
		return true
	default:
		return false
	}
}

// nextSig returns the index of the first significant token at or after from, or
// -1 if there is none.
func nextSig(toks []token, from int) int {
	for j := from; j < len(toks); j++ {
		if isSignificant(toks[j]) {
			return j
		}
	}
	return -1
}

// prevSig returns the index of the last significant token before before, or -1.
func prevSig(toks []token, before int) int {
	for j := before - 1; j >= 0; j-- {
		if isSignificant(toks[j]) {
			return j
		}
	}
	return -1
}

// lastSig returns the index of the last significant token in toks, or -1.
func lastSig(toks []token) int {
	return prevSig(toks, len(toks))
}

// matchParen returns the index of the ")" that closes the "(" at open, or -1 if
// the parentheses are unbalanced.
func matchParen(toks []token, open int) int {
	depth := 0
	for j := open; j < len(toks); j++ {
		if !isSignificant(toks[j]) {
			continue
		}
		if toks[j].kind == tokOp {
			switch toks[j].text {
			case "(":
				depth++
			case ")":
				depth--
				if depth == 0 {
					return j
				}
			}
		}
	}
	return -1
}

// matchOpenParen returns the index of the "(" that the ")" at closeIdx closes,
// scanning backward, or -1 if unbalanced.
func matchOpenParen(toks []token, closeIdx int) int {
	depth := 0
	for j := closeIdx; j >= 0; j-- {
		if !isSignificant(toks[j]) {
			continue
		}
		if toks[j].kind == tokOp {
			switch toks[j].text {
			case ")":
				depth++
			case "(":
				depth--
				if depth == 0 {
					return j
				}
			}
		}
	}
	return -1
}

// topLevelComma returns the index of the first "," at depth 1 inside the call
// whose parentheses are open..close, or -1 if there is none.
func topLevelComma(toks []token, open, closeIdx int) int {
	depth := 0
	for j := open; j < closeIdx; j++ {
		if !isSignificant(toks[j]) {
			continue
		}
		if toks[j].kind != tokOp {
			continue
		}
		switch toks[j].text {
		case "(":
			depth++
		case ")":
			depth--
		case ",":
			if depth == 1 {
				return j
			}
		}
	}
	return -1
}

// topLevelCommas returns the indices of every "," at depth 1 inside the call
// whose parentheses are open..closeIdx, in order.
func topLevelCommas(toks []token, open, closeIdx int) []int {
	depth := 0
	var res []int
	for j := open; j < closeIdx; j++ {
		if !isSignificant(toks[j]) {
			continue
		}
		if toks[j].kind != tokOp {
			continue
		}
		switch toks[j].text {
		case "(":
			depth++
		case ")":
			depth--
		case ",":
			if depth == 1 {
				res = append(res, j)
			}
		}
	}
	return res
}

// topLevelWord returns the index of the first word equal to kw at depth 1 inside
// the call whose parentheses are open..close, or -1 if there is none.
func topLevelWord(toks []token, open, closeIdx int, kw string) int {
	depth := 0
	for j := open; j < closeIdx; j++ {
		if !isSignificant(toks[j]) {
			continue
		}
		if toks[j].kind == tokOp {
			switch toks[j].text {
			case "(":
				depth++
			case ")":
				depth--
			}
			continue
		}
		if depth == 1 && isWordEq(toks[j], kw) {
			return j
		}
	}
	return -1
}

// chainStartBack extends idx backward across "name . name" chains and returns the
// index where the chain begins. idx must already be the last name of the chain.
func chainStartBack(toks []token, idx int) int {
	for {
		dot := prevSig(toks, idx)
		if dot < 0 || !isOpEq(toks[dot], ".") {
			return idx
		}
		name := prevSig(toks, dot)
		if name < 0 || !isName(toks[name]) {
			return idx
		}
		idx = name
	}
}

// primaryStartBack returns the start index of the primary expression that ends
// at the last significant token of toks. A primary is a literal, an identifier
// chain (a, a.b.c), a function call, or a parenthesized expression. It reports
// false when the trailing tokens are not a primary expression.
func primaryStartBack(toks []token) (int, bool) {
	end := lastSig(toks)
	if end < 0 {
		return 0, false
	}
	switch {
	case isOpEq(toks[end], ")"):
		open := matchOpenParen(toks, end)
		if open < 0 {
			return 0, false
		}
		// A "(" is a function call only when a name sits immediately before it
		// (as in count(*)); a name separated by whitespace (as in the keyword of
		// "SELECT (a + b)") is not part of the parenthesized primary.
		if open > 0 && isName(toks[open-1]) {
			return chainStartBack(toks, open-1), true
		}
		return open, true
	case isName(toks[end]) || isLiteral(toks[end]):
		return chainStartBack(toks, end), true
	default:
		return 0, false
	}
}

// primaryEndForward returns the index of the last token of the primary
// expression that begins at or after from. It reports false when there is no
// primary expression there.
func primaryEndForward(toks []token, from int) (int, bool) {
	s0 := nextSig(toks, from)
	if s0 < 0 {
		return 0, false
	}
	var end int
	switch {
	case isOpEq(toks[s0], "("):
		e := matchParen(toks, s0)
		if e < 0 {
			return 0, false
		}
		end = e
	case isName(toks[s0]):
		end = s0
		if e, ok := adjacentCallEnd(toks, end); ok {
			if e < 0 {
				return 0, false
			}
			end = e
		}
	case isLiteral(toks[s0]):
		end = s0
	default:
		return 0, false
	}
	// Extend over ".name" chains and any call parentheses that follow them.
	for {
		dot := nextSig(toks, end+1)
		if dot < 0 || !isOpEq(toks[dot], ".") {
			return end, true
		}
		name := nextSig(toks, dot+1)
		if name < 0 || !isName(toks[name]) {
			return end, true
		}
		end = name
		if e, ok := adjacentCallEnd(toks, end); ok {
			if e < 0 {
				return 0, false
			}
			end = e
		}
	}
}

// adjacentCallEnd reports whether the name at nameIdx is immediately followed by
// a "(" (a function call, as in count(*), not a name separated from a
// parenthesized expression by whitespace) and, if so, returns the index of the
// matching ")". A returned index of -1 means the parentheses are unbalanced.
func adjacentCallEnd(toks []token, nameIdx int) (int, bool) {
	if nameIdx+1 < len(toks) && isOpEq(toks[nameIdx+1], "(") {
		return matchParen(toks, nameIdx+1), true
	}
	return 0, false
}
