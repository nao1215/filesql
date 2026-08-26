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
	fnNameCast      = "CAST"
	fnNameExtract   = "EXTRACT"
	fnNameTrim      = "TRIM"
	fnNameCharLen   = "CHAR_LENGTH"
	fnNameCharLen2  = "CHARACTER_LENGTH"
	fnNameStringAgg = "STRING_AGG"
	fnNameRound     = "ROUND"
	fnNameSubstring = "SUBSTRING"
	fnNameSubstr    = "SUBSTR"
	fnNameMod       = "MOD"
	fnNameTrunc     = "TRUNC"
)

// callRecurser rewrites a slice of argument tokens with a dialect's call pass so
// nested recognized calls inside a rewritten call are handled too.
type callRecurser func([]token) ([]token, error)

// callRewriter is a dialect's answer for one function call: the tokens that
// replace it, whether it recognized the call at all, and the error that ends
// the translation. It is given the whole stream with the call's name and
// parenthesis positions, since a rule may look at the arguments.
type callRewriter func(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error)

// walkCalls hands every "name(" in the stream to the dialect's rewriter and
// keeps whatever it does not recognize. The walk is the same for all three
// dialects, only the rewriter differs, and an unbalanced parenthesis after a
// name ends the translation rather than being rendered back out.
func walkCalls(tokens []token, rewrite callRewriter) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		t := tokens[i]
		if t.kind == tokWord {
			open := nextSig(tokens, i+1)
			if open >= 0 && isOpEq(tokens[open], "(") {
				closeIdx := matchParen(tokens, open)
				if closeIdx < 0 {
					return nil, fmt.Errorf("%w: unbalanced parentheses after %s", ErrInvalidSyntax, t.text)
				}
				repl, handled, err := rewrite(tokens, i, open, closeIdx)
				if err != nil {
					return nil, err
				}
				if handled {
					out = append(out, repl...)
					i = closeIdx + 1
					continue
				}
			}
		}
		out = append(out, t)
		i++
	}
	return out, nil
}

// rewriteExtractCall implements the C-1 rule shared by every dialect:
// EXTRACT(part FROM x) -> helper('part', x). The helper is the dialect's own
// date-part function, because the dialects disagree on what a part means —
// PostgreSQL's WEEK is the ISO week where MySQL's and BigQuery's begin on
// Sunday. It returns handled=false when the call is not the "part FROM x" form
// so the caller leaves it untouched.
func rewriteExtractCall(tokens []token, open, closeIdx int, helper string, recurse callRecurser) ([]token, bool, error) {
	part, last, ok := datePartAt(tokens, nextSig(tokens, open+1))
	if !ok {
		return nil, false, nil
	}
	from := nextSig(tokens, last+1)
	if from < 0 || !isWordEq(tokens[from], "FROM") {
		return nil, false, nil
	}
	expr, err := recurse(tokens[from+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	expr = trimSpaceTokens(expr)
	repl := make([]token, 0, len(expr)+6)
	repl = append(repl, wordToken(helper), opToken("("))
	repl = append(repl, stringToken(part))
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, expr...)
	repl = append(repl, opToken(")"))
	return repl, true, nil
}

// datePartAt reads the date part written at i and returns it in the spelling
// the helpers take, together with the index of its last token. A part is
// usually one word; BigQuery also writes the week ones as a call naming the
// weekday a week begins on -- WEEK(MONDAY) -- which reaches the helpers as
// "week_monday", so one string still carries the whole part.
func datePartAt(tokens []token, i int) (string, int, bool) {
	if i < 0 || i >= len(tokens) || tokens[i].kind != tokWord {
		return "", 0, false
	}
	name := strings.ToLower(tokens[i].text)
	open := nextSig(tokens, i+1)
	if name != unitWeek || open < 0 || !isOpEq(tokens[open], "(") {
		return name, i, true
	}
	day := nextSig(tokens, open+1)
	if day < 0 || tokens[day].kind != tokWord {
		return "", 0, false
	}
	closeIdx := nextSig(tokens, day+1)
	if closeIdx < 0 || !isOpEq(tokens[closeIdx], ")") {
		return "", 0, false
	}
	return unitWeek + "_" + strings.ToLower(tokens[day].text), closeIdx, true
}

// rewriteDatePartArgCall rewrites a call whose last argument is a bare date
// part into one taking that part as a string, under the given name. The part is
// optional: a call with one argument is renamed and left alone.
func rewriteDatePartArgCall(tokens []token, open, closeIdx int, target string, recurse callRecurser) ([]token, bool, error) {
	comma := topLevelComma(tokens, open, closeIdx)
	if comma < 0 {
		return rewriteRenameCall(tokens, open, closeIdx, target, recurse)
	}
	part, last, ok := datePartAt(tokens, nextSig(tokens, comma+1))
	if !ok {
		return nil, false, nil
	}
	if after := nextSig(tokens, last+1); after != closeIdx {
		return nil, false, nil
	}
	value, err := recurse(tokens[open+1 : comma])
	if err != nil {
		return nil, false, err
	}
	value = trimSpaceTokens(value)
	repl := make([]token, 0, len(value)+6)
	repl = append(repl, wordToken(target), opToken("("))
	repl = append(repl, value...)
	repl = append(repl, opToken(","), spaceToken(), stringToken(part), opToken(")"))
	return repl, true, nil
}

// rewriteKeywordArgCall rewrites a call whose argument at index keywordArg is a
// bare keyword into one taking that keyword as a string. BigQuery's NORMALIZE
// writes its normalization form that way, and a bare word would reach SQLite as
// a column name.
func rewriteKeywordArgCall(tokens []token, open, closeIdx int, target string, keywordArg int, recurse callRecurser) ([]token, bool, error) {
	commas := topLevelCommas(tokens, open, closeIdx)
	if len(commas) < keywordArg {
		return rewriteRenameCall(tokens, open, closeIdx, target, recurse)
	}
	bounds := append([]int{open}, commas...)
	bounds = append(bounds, closeIdx)
	repl := make([]token, 0, closeIdx-open+8)
	repl = append(repl, wordToken(target), opToken("("))
	for i := range len(bounds) - 1 {
		if i > 0 {
			repl = append(repl, opToken(","), spaceToken())
		}
		if i == keywordArg {
			word := nextSig(tokens, bounds[i]+1)
			if word < 0 || tokens[word].kind != tokWord || nextSig(tokens, word+1) != bounds[i+1] {
				return nil, false, nil
			}
			repl = append(repl, stringToken(tokens[word].text))
			continue
		}
		arg, err := recurse(tokens[bounds[i]+1 : bounds[i+1]])
		if err != nil {
			return nil, false, err
		}
		repl = append(repl, trimSpaceTokens(arg)...)
	}
	return append(repl, opToken(")")), true, nil
}

// rewriteEditDistance rewrites EDIT_DISTANCE, whose third argument is written
// as a named one -- "max_distance => 2" -- which SQLite has no syntax for. The
// name is dropped and the value kept, since there is only one named argument to
// drop it in favor of.
func rewriteEditDistance(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	commas := topLevelCommas(tokens, open, closeIdx)
	bounds := append([]int{open}, commas...)
	bounds = append(bounds, closeIdx)
	repl := make([]token, 0, closeIdx-open+8)
	repl = append(repl, wordToken("edit_distance"), opToken("("))
	for i := range len(bounds) - 1 {
		if i > 0 {
			repl = append(repl, opToken(","), spaceToken())
		}
		from := bounds[i] + 1
		if arrow := topLevelOperator(tokens, bounds[i], bounds[i+1], "=>"); arrow >= 0 {
			from = arrow + 1
		}
		arg, err := recurse(tokens[from:bounds[i+1]])
		if err != nil {
			return nil, false, err
		}
		repl = append(repl, trimSpaceTokens(arg)...)
	}
	return append(repl, opToken(")")), true, nil
}

// topLevelOperator finds an operator between open and closeIdx that is not
// inside nested parentheses.
func topLevelOperator(tokens []token, open, closeIdx int, op string) int {
	depth := 0
	for i := open + 1; i < closeIdx; i++ {
		switch {
		case isOpEq(tokens[i], "("):
			depth++
		case isOpEq(tokens[i], ")"):
			depth--
		case depth == 0 && isOpEq(tokens[i], op):
			return i
		}
	}
	return -1
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

// intervalUnits maps the INTERVAL units the interval helper implements. Compound
// units (DAY_HOUR and friends) are rejected.
var intervalUnits = map[string]string{
	"SECOND":  unitSecond,
	"MINUTE":  unitMinute,
	"HOUR":    unitHour,
	"DAY":     unitDay,
	"WEEK":    unitWeek,
	"MONTH":   unitMonth,
	"QUARTER": unitQuarter,
	"YEAR":    unitYear,
}

// intervalAddCall builds the interval_add(expr, amount, 'unit') call the date
// arithmetic of every dialect goes through.
func intervalAddCall(expr, amount []token, sign, unit string) []token {
	repl := make([]token, 0, len(expr)+len(amount)+10)
	repl = append(repl, wordToken("interval_add"), opToken("("))
	repl = append(repl, expr...)
	repl = append(repl, opToken(","), spaceToken())
	if sign == "-" {
		// Negate the whole amount, which may be an expression rather than a
		// literal: MySQL accepts DATE_SUB(d, INTERVAL n DAY) with a column n.
		repl = append(repl, opToken("-"), opToken("("))
		repl = append(repl, amount...)
		repl = append(repl, opToken(")"))
	} else {
		repl = append(repl, amount...)
	}
	repl = append(repl, opToken(","), spaceToken(), stringToken(unit), opToken(")"))
	return repl
}

// rewriteAddDate rewrites MySQL's ADDDATE and SUBDATE, which are DATE_ADD and
// DATE_SUB under another name plus one shorthand: a second argument that is not
// an interval counts days, so ADDDATE(d, 1) is DATE_ADD(d, INTERVAL 1 DAY).
func rewriteAddDate(tokens []token, open, closeIdx int, sign string, recurse callRecurser) ([]token, bool, error) {
	repl, handled, err := rewriteDateArith(tokens, open, closeIdx, sign, recurse)
	if handled || err != nil {
		return repl, handled, err
	}
	comma := topLevelComma(tokens, open, closeIdx)
	if comma < 0 {
		return nil, false, nil
	}
	expr, err := recurse(tokens[open+1 : comma])
	if err != nil {
		return nil, false, err
	}
	amount, err := recurse(tokens[comma+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	expr = trimSpaceTokens(expr)
	amount = trimSpaceTokens(amount)
	if len(expr) == 0 || len(amount) == 0 {
		return nil, false, nil
	}
	return intervalAddCall(expr, amount, sign, unitDay), true, nil
}

// rewriteDateArith implements the shared "date +/- INTERVAL n unit" rewrite used
// by MySQL M-5 (DATE_ADD/DATE_SUB) and GoogleSQL G-7 (DATE_ADD/DATE_SUB/
// TIMESTAMP_ADD/TIMESTAMP_SUB): f(x, INTERVAL n unit) -> interval_add(x, ±n,
// 'unit'). sign is "+" for the ADD forms and "-" for the SUB forms.
//
// The amount is any expression up to the trailing unit keyword, so a signed
// literal and a column both work; MySQL accepts either.
//
// It goes through the helper rather than SQLite's datetime() modifier because
// datetime() rolls a month-end overflow forward where every source dialect
// clamps it, and always renders a time of day.
func rewriteDateArith(tokens []token, open, closeIdx int, sign string, recurse callRecurser) ([]token, bool, error) {
	return rewriteDateArithNamed(tokens, open, closeIdx, sign, "interval_add", recurse)
}

// rewriteDateArithNamed is rewriteDateArith under a helper the caller names. A
// BigQuery TIME is a time of day rather than a point in time, so TIME_ADD wraps
// around midnight and reaches a helper of its own.
func rewriteDateArithNamed(tokens []token, open, closeIdx int, sign, helper string, recurse callRecurser) ([]token, bool, error) {
	comma := topLevelComma(tokens, open, closeIdx)
	if comma < 0 {
		return nil, false, nil
	}
	interval := nextSig(tokens, comma+1)
	if interval < 0 || !isWordEq(tokens[interval], "INTERVAL") {
		return nil, false, nil
	}
	unitTok := prevSig(tokens, closeIdx)
	if unitTok < 0 || unitTok <= interval || tokens[unitTok].kind != tokWord {
		return nil, false, fmt.Errorf("%w: INTERVAL is missing a unit", ErrUnsupportedSyntax)
	}
	unit, ok := intervalUnits[strings.ToUpper(tokens[unitTok].text)]
	if !ok {
		return nil, false, fmt.Errorf("%w: unsupported INTERVAL unit %q", ErrUnsupportedSyntax, tokens[unitTok].text)
	}
	amount, err := recurse(tokens[interval+1 : unitTok])
	if err != nil {
		return nil, false, err
	}
	amount = trimSpaceTokens(amount)
	if len(amount) == 0 {
		return nil, false, fmt.Errorf("%w: INTERVAL is missing a value", ErrUnsupportedSyntax)
	}

	expr, err := recurse(tokens[open+1 : comma])
	if err != nil {
		return nil, false, err
	}
	expr = trimSpaceTokens(expr)
	repl := make([]token, 0, len(expr)+len(amount)+10)
	repl = append(repl, wordToken(helper), opToken("("))
	repl = append(repl, expr...)
	repl = append(repl, opToken(","), spaceToken())
	if sign == "-" {
		// Negate the whole amount, which may be an expression rather than a
		// literal: MySQL accepts DATE_SUB(d, INTERVAL n DAY) with a column n.
		repl = append(repl, opToken("-"), opToken("("))
		repl = append(repl, amount...)
		repl = append(repl, opToken(")"))
	} else {
		repl = append(repl, amount...)
	}
	repl = append(repl, opToken(","), spaceToken(), stringToken(unit), opToken(")"))
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

// The case-folding call names, and the helpers they are rewritten onto. SQLite
// folds ASCII alone; MySQL, PostgreSQL and GoogleSQL all fold the whole of
// Unicode, so the call goes to a helper of this package's own for those three
// and is left to SQLite for the SQLite dialect, whose callers want SQLite's
// answers.
const (
	fnNameUpper = "UPPER"
	fnNameLower = "LOWER"
)

// unicodeCaseHelper is the helper name for a call to UPPER or LOWER.
func unicodeCaseHelper(name string) string {
	if strings.EqualFold(name, fnNameLower) {
		return "unicode_lower"
	}
	return "unicode_upper"
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
	commas := topLevelCommas(toks, open, closeIdx)
	if len(commas) == 0 {
		return -1
	}
	return commas[0]
}

// rewriteRoundCall routes the two-argument ROUND onto the helper that honors a
// negative digit count, which SQLite's own round() ignores: ROUND(12345, -2) is
// 12300 in MySQL, PostgreSQL and BigQuery alike, and was 12345 here. The
// one-argument form is left alone, since rounding to a whole number is what
// SQLite already does and there is nothing for a dialect to disagree about.
func rewriteRoundCall(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	if len(topLevelCommas(tokens, open, closeIdx)) != 1 {
		return nil, false, nil
	}
	return rewriteRenameCall(tokens, open, closeIdx, "dialect_round", recurse)
}

// rewriteTruncScaleCall renames the two-argument TRUNC, which truncates at a
// scale the way PostgreSQL's trunc(x, n) and GoogleSQL's TRUNC(x, n) do. The
// one-argument form is SQLite's own trunc and is left alone, so the two live
// under one name the way ROUND's two forms do.
func rewriteTruncScaleCall(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	if len(topLevelCommas(tokens, open, closeIdx)) != 1 {
		return nil, false, nil
	}
	return rewriteRenameCall(tokens, open, closeIdx, "trunc_scale", recurse)
}

// topLevelCommas returns the indices of every "," at depth 1 inside the call
// whose parentheses are open..closeIdx, in order.
// callArity returns how many top-level arguments the call spanning open..close
// takes, so a rewrite can tell the forms of an overloaded name apart. An empty
// argument list is zero.
func callArity(toks []token, open, closeIdx int) int {
	if nextSig(toks, open+1) == closeIdx {
		return 0
	}
	return len(topLevelCommas(toks, open, closeIdx)) + 1
}

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
		// A FILTER or OVER clause modifies the call in front of it, so the
		// primary is the whole "f(...) FILTER (...) OVER (...)" and not just the
		// clause's own parentheses.
		if prev := prevSig(toks, open); prev >= 0 && isWindowClauseKeyword(toks[prev]) {
			return primaryStartBack(toks[:prev])
		}
		// A "(" is a function call only when a name sits immediately before it
		// (as in count(*)); a name separated by whitespace (as in the keyword of
		// "SELECT (a + b)") is not part of the parenthesized primary.
		if open > 0 && isName(toks[open-1]) {
			return chainStartBack(toks, open-1), true
		}
		return open, true
	case isName(toks[end]) || isLiteral(toks[end]):
		start := chainStartBack(toks, end)
		// "f(...) OVER w" names a window defined in a WINDOW clause; the name
		// belongs to the call, not to whatever precedes it.
		if prev := prevSig(toks, start); prev >= 0 && isWordEq(toks[prev], "OVER") {
			return primaryStartBack(toks[:prev])
		}
		return start, true
	default:
		return 0, false
	}
}

// operandChainStartBack walks start back over the operators that share DIV's
// precedence, so the operand is the whole chain rather than the primary beside
// the operator. It reports whether it moved.
func operandChainStartBack(toks []token, start int) (int, bool) {
	moved := false
	for {
		prev := prevSig(toks, start)
		if prev < 0 || !isEqualPrecedenceOperator(toks[prev]) {
			return start, moved
		}
		next, ok := primaryStartBack(toks[:prev])
		if !ok {
			return start, moved
		}
		start = next
		moved = true
	}
}

// isEqualPrecedenceOperator reports whether t is one of the operators MySQL puts
// on DIV's precedence level. MOD is not among them because it has already been
// written as "%" by the time this runs.
func isEqualPrecedenceOperator(t token) bool {
	return isOpEq(t, "*") || isOpEq(t, "/") || isOpEq(t, "%")
}

// isWindowClauseKeyword reports whether t introduces a postfix clause that binds
// to the aggregate call before it.
func isWindowClauseKeyword(t token) bool {
	return isWordEq(t, "OVER") || isWordEq(t, "FILTER")
}

// extendWindowClauses walks forward across the FILTER and OVER clauses that
// follow the call ending at end, so a binary-operator rewrite treats
// "SUM(x) OVER (...)" as one operand rather than splitting it at the clause.
func extendWindowClauses(toks []token, end int) int {
	for {
		next := nextSig(toks, end+1)
		if next < 0 || !isWindowClauseKeyword(toks[next]) {
			return end
		}
		after := nextSig(toks, next+1)
		if after < 0 {
			return end
		}
		switch {
		case isOpEq(toks[after], "("):
			closeIdx := matchParen(toks, after)
			if closeIdx < 0 {
				return end
			}
			end = closeIdx
		case isWordEq(toks[next], "OVER") && isName(toks[after]):
			end = after
		default:
			return end
		}
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
	// A unary sign binds tighter than the binary operators this backs, so
	// "a / -1" takes "-1" as the right operand rather than failing.
	for s0 >= 0 && (isOpEq(toks[s0], "-") || isOpEq(toks[s0], "+")) {
		s0 = nextSig(toks, s0+1)
	}
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
	end = extendWindowClauses(toks, end)
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
		end = extendWindowClauses(toks, end)
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

// datePrefixTypes are the type keywords that can prefix a string literal, as in
// DATE '2026-01-01'. All three dialects accept them; SQLite parses none of them.
var datePrefixTypes = map[string]bool{
	"DATE":      true,
	"DATETIME":  true,
	"TIMESTAMP": true,
	"TIME":      true,
}

// typePrefixedLiteralPass drops a DATE/DATETIME/TIMESTAMP/TIME keyword that sits
// immediately before a string literal and keeps the literal, since SQLite stores
// these values as text (G-3, M-14, P-13).
func typePrefixedLiteralPass(tokens []token) []token {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		t := tokens[i]
		if t.kind == tokWord && datePrefixTypes[strings.ToUpper(t.text)] {
			if lit := nextSig(tokens, i+1); lit >= 0 && tokens[lit].kind == tokString {
				out = append(out, tokens[lit])
				i = lit + 1
				continue
			}
		}
		out = append(out, t)
		i++
	}
	return out
}

// currentValueKeywords are the no-argument datetime keywords SQLite accepts only
// without parentheses, while MySQL and GoogleSQL write them as calls.
var currentValueKeywords = map[string]bool{
	"CURRENT_DATE":      true,
	"CURRENT_TIME":      true,
	"CURRENT_TIMESTAMP": true,
}

// currentValueParenPass drops the empty parentheses from CURRENT_DATE() and its
// siblings, which SQLite rejects (M-15, G-14).
func currentValueParenPass(tokens []token) []token {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		t := tokens[i]
		if t.kind == tokWord && currentValueKeywords[strings.ToUpper(t.text)] {
			if open := nextSig(tokens, i+1); open >= 0 && isOpEq(tokens[open], "(") {
				if closeIdx := nextSig(tokens, open+1); closeIdx >= 0 && isOpEq(tokens[closeIdx], ")") {
					out = append(out, t)
					i = closeIdx + 1
					continue
				}
			}
		}
		out = append(out, t)
		i++
	}
	return out
}

// rewriteTruncCall implements GoogleSQL's DATE_TRUNC(value, PART) argument
// order, where the part is a bare keyword. The PostgreSQL spelling,
// DATE_TRUNC('part', value), is left alone so both work under either dialect.
func rewriteTruncCall(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	comma := topLevelComma(tokens, open, closeIdx)
	if comma < 0 {
		return nil, false, nil
	}
	part, last, ok := datePartAt(tokens, nextSig(tokens, comma+1))
	if !ok {
		return nil, false, nil
	}
	if after := nextSig(tokens, last+1); after != closeIdx {
		return nil, false, nil
	}
	value, err := recurse(tokens[open+1 : comma])
	if err != nil {
		return nil, false, err
	}
	value = trimSpaceTokens(value)
	repl := make([]token, 0, len(value)+6)
	repl = append(repl, wordToken("date_trunc_part"), opToken("("))
	repl = append(repl, value...)
	repl = append(repl, opToken(","), spaceToken(), stringToken(part), opToken(")"))
	return repl, true, nil
}

// rewriteTimestampDiff implements MySQL TIMESTAMPDIFF(unit, start, end) ->
// mysql_date_diff(end, start, 'unit'). MySQL counts forward from start, the
// reverse of the helper's argument order, and counts complete units where
// BigQuery's DATE_DIFF counts boundaries, so the helper is MySQL's own.
func rewriteTimestampDiff(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	unit, args, ok, err := unitFirstCallArgs(tokens, open, closeIdx, 3, recurse)
	if !ok || err != nil {
		return nil, false, err
	}
	repl := make([]token, 0, len(args[0])+len(args[1])+8)
	repl = append(repl, wordToken("mysql_date_diff"), opToken("("))
	repl = append(repl, args[1]...)
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, args[0]...)
	repl = append(repl, opToken(","), spaceToken(), stringToken(unit), opToken(")"))
	return repl, true, nil
}

// rewriteTimestampAdd implements MySQL TIMESTAMPADD(unit, n, x) ->
// interval_add(x, n, 'unit').
func rewriteTimestampAdd(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	unit, args, ok, err := unitFirstCallArgs(tokens, open, closeIdx, 3, recurse)
	if !ok || err != nil {
		return nil, false, err
	}
	repl := make([]token, 0, len(args[0])+len(args[1])+8)
	repl = append(repl, wordToken("interval_add"), opToken("("))
	repl = append(repl, args[1]...)
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, args[0]...)
	repl = append(repl, opToken(","), spaceToken(), stringToken(unit), opToken(")"))
	return repl, true, nil
}

// unitFirstCallArgs splits a call whose first argument is a bare unit keyword,
// returning the lowercased unit and the remaining arguments. It reports ok=false
// when the call does not have that shape, so the caller leaves it untouched.
func unitFirstCallArgs(tokens []token, open, closeIdx, want int, recurse callRecurser) (string, [][]token, bool, error) {
	commas := topLevelCommas(tokens, open, closeIdx)
	if len(commas) != want-1 {
		return "", nil, false, nil
	}
	unitTok := nextSig(tokens, open+1)
	if unitTok < 0 || tokens[unitTok].kind != tokWord || nextSig(tokens, unitTok+1) != commas[0] {
		return "", nil, false, nil
	}
	unit, ok := intervalUnits[strings.ToUpper(tokens[unitTok].text)]
	if !ok {
		return "", nil, false, fmt.Errorf("%w: unsupported unit %q", ErrUnsupportedSyntax, tokens[unitTok].text)
	}
	bounds := append(commas, closeIdx)
	args := make([][]token, 0, want-1)
	for i := range commas {
		arg, err := recurse(tokens[bounds[i]+1 : bounds[i+1]])
		if err != nil {
			return "", nil, false, err
		}
		args = append(args, trimSpaceTokens(arg))
	}
	return unit, args, true, nil
}

// pgIntervalPass implements P-12: "x + INTERVAL 'text'" and "x - INTERVAL 'text'"
// become interval_text_add(x, 'text', ±1). PostgreSQL has no DATE_ADD, so this
// operator form is the only date arithmetic that dialect offers.
func pgIntervalPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if !isWordEq(tokens[i], "INTERVAL") {
			out = append(out, tokens[i])
			i++
			continue
		}
		lit := nextSig(tokens, i+1)
		if lit < 0 || tokens[lit].kind != tokString {
			out = append(out, tokens[i])
			i++
			continue
		}
		op := lastSig(out)
		if op < 0 || (!isOpEq(out[op], "+") && !isOpEq(out[op], "-")) {
			out = append(out, tokens[i])
			i++
			continue
		}
		sign := "1"
		if out[op].text == "-" {
			sign = "-1"
		}
		out = out[:op]
		left, start, ok := operandBack(out)
		if !ok {
			return nil, fmt.Errorf("%w: left operand of INTERVAL arithmetic is not a primary expression", ErrUnsupportedSyntax)
		}
		out = out[:start]
		out = append(out, wordToken("interval_text_add"), opToken("("))
		out = append(out, left...)
		out = append(out, opToken(","), spaceToken(), tokens[lit])
		out = append(out, opToken(","), spaceToken(), wordToken(sign), opToken(")"))
		i = lit + 1
	}
	return out, nil
}

// betweenSymmetricPass implements P-23: "x BETWEEN SYMMETRIC a AND b" is true
// when x lies between a and b in whichever order the two are, and SQLite has no
// such form -- passed through, its parser reports a syntax error naming a token
// from the middle of the caller's own query. The rewrite sorts the two bounds
// with least() and greatest() so each operand is still written once, which
// matters when one of them is a function call the caller does not expect to be
// evaluated twice.
//
// The two shared helpers are named rather than PostgreSQL's own pair, because a
// NULL bound makes the whole comparison NULL in PostgreSQL, and the shared ones
// answer NULL for the whole call while PostgreSQL's LEAST and GREATEST skip
// their NULLs and would leave a bound behind. ASYMMETRIC is PostgreSQL's
// explicit spelling of the ordinary reading, so the keyword is simply dropped.
func betweenSymmetricPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		symmetric := isWordEq(tokens[i], "SYMMETRIC")
		if !symmetric && !isWordEq(tokens[i], "ASYMMETRIC") {
			out = append(out, tokens[i])
			i++
			continue
		}
		if prev := lastSig(out); prev < 0 || !isWordEq(out[prev], "BETWEEN") {
			out = append(out, tokens[i])
			i++
			continue
		}
		// The whitespace that stood before the keyword is already in out, so
		// the keyword and the run of whitespace after it go together; leaving
		// the latter behind would double the space.
		if !symmetric {
			i = skipInsignificant(tokens, i+1)
			continue
		}
		lo, hi, next, err := betweenBounds(tokens, i+1)
		if err != nil {
			return nil, err
		}
		out = append(out, callTokens("least", lo, hi)...)
		out = append(out, spaceToken(), wordToken("AND"), spaceToken())
		out = append(out, callTokens("greatest", lo, hi)...)
		i = next
	}
	return out, nil
}

// skipInsignificant returns the index of the first significant token at or
// after from, or len(tokens) when there is none.
func skipInsignificant(tokens []token, from int) int {
	for i := from; i < len(tokens); i++ {
		if isSignificant(tokens[i]) {
			return i
		}
	}
	return len(tokens)
}

// betweenBounds reads the two operands of a BETWEEN range starting at from,
// returning them and the index just past the second.
func betweenBounds(tokens []token, from int) (lo, hi []token, next int, err error) {
	fail := fmt.Errorf("%w: BETWEEN SYMMETRIC needs two bounds this translation can read", ErrUnsupportedSyntax)
	loEnd, ok := betweenOperandForward(tokens, from)
	if !ok {
		return nil, nil, 0, fail
	}
	and := nextSig(tokens, loEnd+1)
	if and < 0 || !isWordEq(tokens[and], "AND") {
		return nil, nil, 0, fail
	}
	hiEnd, ok := betweenOperandForward(tokens, and+1)
	if !ok {
		return nil, nil, 0, fail
	}
	loStart := nextSig(tokens, from)
	hiStart := nextSig(tokens, and+1)
	lo = append([]token{}, trimSpaceTokens(tokens[loStart:loEnd+1])...)
	hi = append([]token{}, trimSpaceTokens(tokens[hiStart:hiEnd+1])...)
	return lo, hi, hiEnd + 1, nil
}

// betweenOperandForward returns the index of the last token of a BETWEEN bound
// beginning at or after from. A bound binds tighter than the AND that separates
// the two, so it runs across the arithmetic and concatenation operators and
// stops at the first word, which is that AND or whatever follows the range.
func betweenOperandForward(tokens []token, from int) (int, bool) {
	end, ok := primaryEndForward(tokens, from)
	if !ok {
		return 0, false
	}
	for {
		op := nextSig(tokens, end+1)
		if op < 0 || tokens[op].kind != tokOp || !betweenTighterOperators[tokens[op].text] {
			return end, true
		}
		next, ok := primaryEndForward(tokens, op+1)
		if !ok {
			return end, true
		}
		end = next
	}
}

// betweenTighterOperators are the operators that bind tighter than the AND of a
// BETWEEN range, so a bound extends across them.
var betweenTighterOperators = map[string]bool{ //nolint:gochecknoglobals // a fixed table read by the bound scan
	"*": true, "/": true, "%": true, "+": true, "-": true, "||": true,
	"&": true, "|": true, "<<": true, ">>": true,
}

// unitIntervalPass rewrites "x + INTERVAL n unit" and "x - INTERVAL n unit"
// into interval_add(x, ±n, 'unit'), the call DATE_ADD and DATE_SUB already
// produce. MySQL and BigQuery both write the arithmetic this way as well as
// through the functions, and the two spellings have to answer the same thing.
//
// A "(" directly after the word is not an interval literal: MySQL's
// INTERVAL(n, a, b, ...) is an ordinary function that returns the index of the
// first argument larger than n, so it is left for the call pass to see.
func unitIntervalPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if !isWordEq(tokens[i], "INTERVAL") {
			out = append(out, tokens[i])
			i++
			continue
		}
		op := lastSig(out)
		if op < 0 || (!isOpEq(out[op], "+") && !isOpEq(out[op], "-")) {
			out = append(out, tokens[i])
			i++
			continue
		}
		amountStart := nextSig(tokens, i+1)
		unitTok := intervalUnitAfter(tokens, amountStart)
		if amountStart < 0 || unitTok < 0 {
			out = append(out, tokens[i])
			i++
			continue
		}
		unit, ok := intervalUnits[strings.ToUpper(tokens[unitTok].text)]
		if !ok {
			return nil, fmt.Errorf("%w: unsupported INTERVAL unit %q", ErrUnsupportedSyntax, tokens[unitTok].text)
		}
		sign := out[op].text
		out = out[:op]
		left, start, ok := operandBack(out)
		if !ok {
			return nil, fmt.Errorf("%w: left operand of INTERVAL arithmetic is not a primary expression", ErrUnsupportedSyntax)
		}
		out = out[:start]
		amount := trimSpaceTokens(tokens[amountStart:unitTok])
		out = append(out, intervalAddCall(left, amount, sign, unit)...)
		i = unitTok + 1
	}
	return out, nil
}

// intervalUnitAfter finds the unit keyword that closes an interval literal
// beginning at start, or -1 when there is none. MySQL writes the literal as an
// expression followed by a unit keyword, and the expression can name columns,
// so the scan runs forward to the first word that is a unit rather than
// assuming the amount is one token. It stops at a word that ends the expression
// instead, and at a comma or a closing parenthesis outside any nesting, so a
// unit-shaped word further along the statement is not mistaken for this
// literal's unit.
func intervalUnitAfter(tokens []token, start int) int {
	if start < 0 {
		return -1
	}
	depth, caseDepth := 0, 0
	for i := start; i < len(tokens); i++ {
		t := tokens[i]
		if !isSignificant(t) {
			continue
		}
		switch {
		case isOpEq(t, "("):
			depth++
		case isOpEq(t, ")"):
			if depth == 0 {
				return -1
			}
			depth--
		case depth > 0:
		case isOpEq(t, ","):
			return -1
		case isWordEq(t, "CASE"):
			// A CASE is one expression and its WHEN, THEN and ELSE are inside
			// it, so the stop words below do not apply until its END.
			caseDepth++
		case isWordEq(t, "END"):
			if caseDepth == 0 {
				return -1
			}
			caseDepth--
		case caseDepth > 0:
		case t.kind == tokWord:
			if _, ok := intervalUnits[strings.ToUpper(t.text)]; ok {
				return i
			}
			if intervalScanStopWords[strings.ToUpper(t.text)] {
				return -1
			}
		}
	}
	return -1
}

// kwHaving and kwLimit are spelled in more than one of the keyword tables in
// this package, which is what makes them constants rather than literals.
const (
	// The date and time type names, and the sub-second units, are each spelled
	// in more than one table in this package.
	typeDate      = "DATE"
	typeDatetime  = "DATETIME"
	typeTime      = "TIME"
	typeTimestamp = "TIMESTAMP"

	unitMillisecond = "millisecond"
	unitMicrosecond = "microsecond"

	kwHaving = "HAVING"
	kwLimit  = "LIMIT"
	kwWhere  = "WHERE"
)

// intervalScanStopWords end the expression that carries an interval's amount.
// They are the words that can follow a complete expression, so a unit keyword
// standing past one of them belongs to something else.
var intervalScanStopWords = map[string]bool{ //nolint:gochecknoglobals // a fixed table read by the interval scan
	"FROM": true, kwWhere: true, "GROUP": true, "ORDER": true, kwHaving: true,
	kwLimit: true, "OFFSET": true, "AS": true, "AND": true, "OR": true,
	"NOT": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"ON": true, "USING": true, "JOIN": true, "UNION": true, "INTO": true,
	"SELECT": true, "IS": true, "IN": true, "LIKE": true, "BETWEEN": true,
}

// checkLeftoverInterval reports an INTERVAL literal that no arithmetic rewrite
// consumed. SQLite has no interval type, so such a literal reaches its parser
// as a syntax error naming a token from the middle of the caller's own query --
// "near \"1\"" for INTERVAL 1 DAY -- which reads as a mistake in the SQL
// rather than as a construct the translation does not cover.
func checkLeftoverInterval(tokens []token) error {
	for i, t := range tokens {
		if !isWordEq(t, "INTERVAL") {
			continue
		}
		next := nextSig(tokens, i+1)
		if next < 0 || isOpEq(tokens[next], "(") {
			continue
		}
		if tokens[next].kind != tokString && tokens[next].kind != tokNumber && tokens[next].kind != tokWord {
			continue
		}
		return fmt.Errorf("%w: an INTERVAL literal is only supported in date arithmetic; SQLite has no interval type", ErrUnsupportedSyntax)
	}
	return nil
}

// rewriteJSONQuery implements GoogleSQL JSON_QUERY(json, path) as SQLite's "->"
// operator. Unlike JSON_VALUE, JSON_QUERY keeps its result in JSON text, so a
// string value stays quoted; "->" does that where json_extract does not.
func rewriteJSONQuery(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	comma := topLevelComma(tokens, open, closeIdx)
	if comma < 0 {
		return nil, false, nil
	}
	doc, err := recurse(tokens[open+1 : comma])
	if err != nil {
		return nil, false, err
	}
	path, err := recurse(tokens[comma+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	repl := make([]token, 0, len(doc)+len(path)+6)
	repl = append(repl, opToken("("))
	repl = append(repl, trimSpaceTokens(doc)...)
	repl = append(repl, spaceToken(), opToken("->"), spaceToken())
	repl = append(repl, trimSpaceTokens(path)...)
	return append(repl, opToken(")")), true, nil
}
