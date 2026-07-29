package dialect

import (
	"fmt"
	"strings"
)

// rewritePostgreSQL applies the PostgreSQL token rewrite rules. The lexical
// rules (P-7 E'...'/$$...$$ strings) are handled by the tokenizer; TRUE/FALSE
// (P-9) are native to SQLite. This pass handles the structural rules:
//
//	C-1  EXTRACT(part FROM x)      -> DATE_PART('part', x)
//	P-1  expr::type                -> postgresql_cast(expr, 'type')
//	P-2  x LIKE p / x ILIKE p      -> like_sensitive / like_insensitive
//	P-3  x ~ p / !~ / ~* / !~*     -> x REGEXP p / NOT REGEXP / case-insensitive
//	P-4  POSITION(x IN y)          -> INSTR(y, x)
//	P-5  SUBSTRING(x FROM n FOR m) -> SUBSTR(x, n, m)
//	P-6  STRING_AGG(x, s)          -> group_concat(x, s)
//	P-8  CAST(x AS pg_type)        -> postgresql_cast(x, 'pg_type')
//	P-10 DISTINCT ON (...), LATERAL -> ErrUnsupportedSyntax
//	P-11 a ^ b                     -> power(a, b)
//	P-12 x +/- INTERVAL 'text'     -> interval_text_add(x, 'text', ±1)
//	P-13 DATE 'lit' / TIMESTAMP 'lit' -> 'lit'
func rewritePostgreSQL(tokens []token) ([]token, error) {
	if err := checkUnsupportedPostgreSQL(tokens); err != nil {
		return nil, err
	}
	out, err := pgCallPass(tokens)
	if err != nil {
		return nil, err
	}
	out, err = pgCastOperatorPass(out)
	if err != nil {
		return nil, err
	}
	out, err = pgRegexOperatorPass(out)
	if err != nil {
		return nil, err
	}
	// P-2: SQLite's LIKE folds ASCII case, so it answers PostgreSQL's LIKE
	// wrongly and makes ILIKE indistinguishable from it. Route both through
	// helpers that decide case folding explicitly.
	out, err = likePass(out, "ILIKE", "like_insensitive")
	if err != nil {
		return nil, err
	}
	out, err = likePass(out, "LIKE", "like_sensitive")
	if err != nil {
		return nil, err
	}
	// P-11: "^" is exponentiation in PostgreSQL and not an operator at all in
	// SQLite.
	out, err = binaryOperatorPass(out, "^", "power")
	if err != nil {
		return nil, err
	}
	// P-13 runs before P-12 so that "DATE '2026-01-01' + INTERVAL '1 day'" has
	// already lost its DATE keyword and the interval rewrite sees a plain
	// literal as the left operand.
	out = typePrefixedLiteralPass(out)
	// P-12: "x + INTERVAL '1 day'" is the only way to do date arithmetic in
	// PostgreSQL; SQLite has no interval type at all.
	out, err = pgIntervalPass(out)
	if err != nil {
		return nil, err
	}
	out = renameWordPass(out, "STRING_AGG", "group_concat")
	return out, nil
}

// checkUnsupportedPostgreSQL rejects P-10 constructs that have no SQLite
// equivalent: "DISTINCT ON (...)" and the LATERAL keyword.
func checkUnsupportedPostgreSQL(tokens []token) error {
	for i, t := range tokens {
		if !isSignificant(t) {
			continue
		}
		if isWordEq(t, "LATERAL") {
			return fmt.Errorf("%w: LATERAL is not supported", ErrUnsupportedSyntax)
		}
		if isWordEq(t, "DISTINCT") {
			if on := nextSig(tokens, i+1); on >= 0 && isWordEq(tokens[on], "ON") {
				return fmt.Errorf("%w: DISTINCT ON is not supported", ErrUnsupportedSyntax)
			}
		}
	}
	return nil
}

// pgCallPass rewrites the PostgreSQL function-call rules (C-1, P-4, P-5, P-8),
// recursing into the arguments of recognized calls.
func pgCallPass(tokens []token) ([]token, error) {
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
				repl, handled, err := pgRewriteCall(tokens, i, open, closeIdx)
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

func pgRewriteCall(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error) {
	switch strings.ToUpper(tokens[nameIdx].text) {
	case fnNameExtract:
		return rewriteExtractCall(tokens, open, closeIdx, pgCallPass)
	case "POSITION":
		return rewritePosition(tokens, open, closeIdx)
	case "SUBSTRING":
		return rewriteSubstring(tokens, open, closeIdx)
	case fnNameCast:
		return rewriteCastCall(tokens, open, closeIdx, PostgreSQL, "postgresql_cast", pgCallPass)
	default:
		return nil, false, nil
	}
}

// rewritePosition implements P-4: POSITION(x IN y) -> INSTR(y, x).
func rewritePosition(tokens []token, open, closeIdx int) ([]token, bool, error) {
	in := topLevelWord(tokens, open, closeIdx, "IN")
	if in < 0 {
		return nil, false, nil
	}
	needle, err := pgCallPass(tokens[open+1 : in])
	if err != nil {
		return nil, false, err
	}
	haystack, err := pgCallPass(tokens[in+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	needle = trimSpaceTokens(needle)
	haystack = trimSpaceTokens(haystack)
	repl := make([]token, 0, len(needle)+len(haystack)+5)
	repl = append(repl, wordToken("INSTR"), opToken("("))
	repl = append(repl, haystack...)
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, needle...)
	repl = append(repl, opToken(")"))
	return repl, true, nil
}

// rewriteSubstring implements P-5: SUBSTRING(x FROM n FOR m) -> SUBSTR(x, n, m).
// The FROM and FOR parts are each optional; a missing FROM defaults the start to
// 1. The comma-argument form SUBSTRING(x, n, m) is left unchanged (SQLite accepts
// it natively).
func rewriteSubstring(tokens []token, open, closeIdx int) ([]token, bool, error) {
	from := topLevelWord(tokens, open, closeIdx, "FROM")
	forKw := topLevelWord(tokens, open, closeIdx, "FOR")
	if from < 0 && forKw < 0 {
		return nil, false, nil
	}

	// The subject ends at whichever of FROM/FOR is present (FROM first when both
	// are). At least one exists here, since the no-keyword case returned above.
	subjectEnd := forKw
	if from >= 0 {
		subjectEnd = from
	}
	subject, err := pgCallPass(tokens[open+1 : subjectEnd])
	if err != nil {
		return nil, false, err
	}
	subject = trimSpaceTokens(subject)

	var start, length []token
	if from >= 0 {
		startEnd := closeIdx
		if forKw > from {
			startEnd = forKw
		}
		start, err = pgCallPass(tokens[from+1 : startEnd])
		if err != nil {
			return nil, false, err
		}
		start = trimSpaceTokens(start)
	}
	if forKw >= 0 {
		length, err = pgCallPass(tokens[forKw+1 : closeIdx])
		if err != nil {
			return nil, false, err
		}
		length = trimSpaceTokens(length)
	}

	repl := make([]token, 0, len(subject)+len(start)+len(length)+8)
	repl = append(repl, wordToken("SUBSTR"), opToken("("))
	repl = append(repl, subject...)
	repl = append(repl, opToken(","), spaceToken())
	if len(start) == 0 {
		repl = append(repl, wordToken("1"))
	} else {
		repl = append(repl, start...)
	}
	if len(length) > 0 {
		repl = append(repl, opToken(","), spaceToken())
		repl = append(repl, length...)
	}
	repl = append(repl, opToken(")"))
	return repl, true, nil
}

// pgCastOperatorPass implements P-1: expr::type -> postgresql_cast(expr,
// 'type'). It processes "::" left to right, so a chain such as a::int::text
// nests into postgresql_cast(postgresql_cast(a, 'int'), 'text').
func pgCastOperatorPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if isOpEq(tokens[i], "::") {
			start, ok := primaryStartBack(out)
			if !ok {
				return nil, fmt.Errorf("%w: left operand of :: is not a primary expression", ErrUnsupportedSyntax)
			}
			typeName := nextSig(tokens, i+1)
			if typeName < 0 || tokens[typeName].kind != tokWord {
				return nil, fmt.Errorf("%w: :: is missing a type name", ErrUnsupportedSyntax)
			}
			target, typeEnd, err := castTargetText(tokens, typeName)
			if err != nil {
				return nil, err
			}

			left := append([]token{}, trimSpaceTokens(out[start:])...)
			out = out[:start]
			if _, known := lookupCastKind(PostgreSQL, tokens[typeName].text); known {
				out = append(out, castHelperCall("postgresql_cast", left, target)...)
			} else {
				// An unrecognized type is left to SQLite, which at least keeps a
				// query using a domain or extension type running.
				out = append(out, wordToken("CAST"), opToken("("))
				out = append(out, left...)
				out = append(out, spaceToken(), wordToken("AS"), spaceToken())
				out = append(out, tokens[typeName:typeEnd+1]...)
				out = append(out, opToken(")"))
			}
			i = typeEnd + 1
			continue
		}
		out = append(out, tokens[i])
		i++
	}
	return out, nil
}

// pgRegexOperatorPass implements P-3: the "~" family of regex-match operators.
//
//	x ~ p    -> x REGEXP p
//	x !~ p   -> x NOT REGEXP p
//	x ~* p   -> x REGEXP '(?i)p'    (p must be a string literal)
//	x !~* p  -> x NOT REGEXP '(?i)p'
func pgRegexOperatorPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	caseInsensitivePending := false
	for i := range tokens {
		t := tokens[i]
		if caseInsensitivePending && isSignificant(t) {
			if t.kind != tokString {
				return nil, fmt.Errorf("%w: case-insensitive regex operator requires a string-literal pattern", ErrUnsupportedSyntax)
			}
			out = append(out, stringToken("(?i)"+t.text))
			caseInsensitivePending = false
			continue
		}
		if t.kind == tokOp {
			switch t.text {
			case "~":
				out = appendRegexOperator(out, tokens, i, false)
				continue
			case "!~":
				out = appendRegexOperator(out, tokens, i, true)
				continue
			case "~*":
				out = appendRegexOperator(out, tokens, i, false)
				caseInsensitivePending = true
				continue
			case "!~*":
				out = appendRegexOperator(out, tokens, i, true)
				caseInsensitivePending = true
				continue
			}
		}
		out = append(out, t)
	}
	if caseInsensitivePending {
		return nil, fmt.Errorf("%w: case-insensitive regex operator requires a string-literal pattern", ErrUnsupportedSyntax)
	}
	return out, nil
}

// appendRegexOperator appends "REGEXP" (or "NOT REGEXP" when negated) in place of
// a regex operator token at index i, inserting single spaces only where the
// original text lacked whitespace so the result reads "x REGEXP p".
func appendRegexOperator(out, tokens []token, i int, negated bool) []token {
	if n := len(out); n > 0 && out[n-1].kind != tokWhitespace {
		out = append(out, spaceToken())
	}
	if negated {
		out = append(out, wordToken("NOT"), spaceToken())
	}
	out = append(out, wordToken("REGEXP"))
	if i+1 < len(tokens) && tokens[i+1].kind != tokWhitespace {
		out = append(out, spaceToken())
	}
	return out
}
