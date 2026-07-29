package dialect

import (
	"fmt"
	"strings"
)

// rewriteGoogleSQL applies the GoogleSQL (BigQuery / Cloud Spanner) token
// rewrite rules. Lexical rules are handled elsewhere: G-1 (backtick compound
// identifiers), G-5 (# comments), and G-10 (r'...'/b'...' literals) by the
// tokenizer. This pass handles the structural rules:
//
//	C-1  EXTRACT(part FROM x)                 -> DATE_PART('part', x)
//	G-2  SAFE_CAST(x AS t)                    -> googlesql_safe_cast(x, 't')
//	G-3  DATE/DATETIME/TIMESTAMP/TIME 'lit'   -> 'lit'
//	G-4  CAST(x AS googlesql_type)            -> googlesql_cast(x, 'type')
//	G-6  FORMAT(fmt, ...)                     -> printf(fmt, ...)
//	G-7  DATE_ADD/DATE_SUB/TIMESTAMP_ADD/SUB  -> datetime(x, '±n unit')
//	G-8  DATE_DIFF/TIMESTAMP_DIFF(a, b, UNIT) -> DATE_DIFF(a, b, 'unit')
//	G-9  QUALIFY / UNNEST / ARRAY<> / STRUCT<> / SELECT * EXCEPT / REPLACE
//	     -> ErrUnsupportedSyntax
//	G-11 a / b                                -> googlesql_divide(a, b)
//	G-12 x LIKE p                             -> like_sensitive(p, x)
//	G-13 DATE_TRUNC(x, PART)                  -> date_trunc_part(x, 'part')
//	G-14 CURRENT_DATE() and friends           -> CURRENT_DATE
func rewriteGoogleSQL(tokens []token) ([]token, error) {
	if err := checkUnsupportedGoogleSQL(tokens); err != nil {
		return nil, err
	}
	out, err := googlesqlCallPass(tokens)
	if err != nil {
		return nil, err
	}
	// G-11: GoogleSQL division always yields FLOAT64, so 5/2 is 2.5 rather than
	// the 2 SQLite gives for two integers.
	out, err = binaryOperatorPass(out, "/", "googlesql_divide")
	if err != nil {
		return nil, err
	}
	// G-12: GoogleSQL LIKE is case-sensitive; SQLite's folds ASCII case.
	out, err = likePass(out, "LIKE", "like_sensitive")
	if err != nil {
		return nil, err
	}
	return currentValueParenPass(typePrefixedLiteralPass(out)), nil
}

// checkUnsupportedGoogleSQL rejects the G-9 constructs that have no SQLite
// equivalent.
func checkUnsupportedGoogleSQL(tokens []token) error {
	for i, t := range tokens {
		if !isSignificant(t) {
			continue
		}
		if isWordEq(t, "QUALIFY") {
			return fmt.Errorf("%w: QUALIFY is not supported", ErrUnsupportedSyntax)
		}
		if isWordEq(t, "UNNEST") {
			return fmt.Errorf("%w: UNNEST is not supported", ErrUnsupportedSyntax)
		}
		if isWordEq(t, "ARRAY") || isWordEq(t, "STRUCT") {
			if lt := nextSig(tokens, i+1); lt >= 0 && isOpEq(tokens[lt], "<") {
				return fmt.Errorf("%w: %s type is not supported", ErrUnsupportedSyntax, strings.ToUpper(t.text))
			}
		}
		// SELECT * EXCEPT(...) / SELECT * REPLACE(...)
		if isOpEq(t, "*") {
			if w := nextSig(tokens, i+1); w >= 0 && (isWordEq(tokens[w], "EXCEPT") || isWordEq(tokens[w], "REPLACE")) {
				if p := nextSig(tokens, w+1); p >= 0 && isOpEq(tokens[p], "(") {
					return fmt.Errorf("%w: SELECT * %s is not supported", ErrUnsupportedSyntax, strings.ToUpper(tokens[w].text))
				}
			}
		}
	}
	return nil
}

// googlesqlCallPass rewrites the GoogleSQL function-call rules (C-1, G-2, G-4,
// G-6, G-7, G-8), recursing into the arguments of recognized calls.
func googlesqlCallPass(tokens []token) ([]token, error) {
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
				repl, handled, err := googlesqlRewriteCall(tokens, i, open, closeIdx)
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

func googlesqlRewriteCall(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error) {
	switch strings.ToUpper(tokens[nameIdx].text) {
	case fnNameExtract:
		return rewriteExtractCall(tokens, open, closeIdx, googlesqlCallPass)
	case fnNameCast:
		return rewriteCastCall(tokens, open, closeIdx, GoogleSQL, "googlesql_cast", googlesqlCallPass)
	case "SAFE_CAST":
		return rewriteSafeCast(tokens, open, closeIdx)
	case "FORMAT":
		return rewriteRenameCall(tokens, open, closeIdx, "printf", googlesqlCallPass)
	case "DATE_ADD", "TIMESTAMP_ADD":
		return rewriteDateArith(tokens, open, closeIdx, "+", googlesqlCallPass)
	case "DATE_SUB", "TIMESTAMP_SUB":
		return rewriteDateArith(tokens, open, closeIdx, "-", googlesqlCallPass)
	case "DATE_DIFF", "TIMESTAMP_DIFF":
		return rewriteDateDiff(tokens, nameIdx, open, closeIdx)
	case "DATE_TRUNC", "TIMESTAMP_TRUNC", "DATETIME_TRUNC":
		return rewriteTruncCall(tokens, open, closeIdx, googlesqlCallPass)
	default:
		return nil, false, nil
	}
}

// rewriteSafeCast implements G-2: SAFE_CAST(x AS type) -> a call to the
// GoogleSQL cast helper that answers NULL instead of raising, which is what
// SAFE_CAST means. Unlike CAST it always rewrites (SAFE_CAST is not a SQLite
// function); an unmapped type keeps its original name and falls back to a plain
// SQLite CAST.
func rewriteSafeCast(tokens []token, open, closeIdx int) ([]token, bool, error) {
	as := topLevelWord(tokens, open, closeIdx, "AS")
	if as < 0 {
		return nil, false, fmt.Errorf("%w: SAFE_CAST is missing AS type", ErrUnsupportedSyntax)
	}
	typeName := nextSig(tokens, as+1)
	if typeName < 0 || tokens[typeName].kind != tokWord {
		return nil, false, fmt.Errorf("%w: SAFE_CAST is missing a type name", ErrUnsupportedSyntax)
	}
	target, typeEnd, err := castTargetText(tokens, typeName)
	if err != nil {
		return nil, false, err
	}
	expr, err := googlesqlCallPass(tokens[open+1 : as])
	if err != nil {
		return nil, false, err
	}
	expr = trimSpaceTokens(expr)
	if _, known := lookupCastKind(GoogleSQL, tokens[typeName].text); known {
		return castHelperCall("googlesql_safe_cast", expr, target), true, nil
	}
	repl := make([]token, 0, len(expr)+6)
	repl = append(repl, wordToken("CAST"), opToken("("))
	repl = append(repl, expr...)
	repl = append(repl, spaceToken(), wordToken("AS"), spaceToken())
	repl = append(repl, tokens[typeName:typeEnd+1]...)
	repl = append(repl, opToken(")"))
	return repl, true, nil
}

// rewriteDateDiff implements G-8: DATE_DIFF/TIMESTAMP_DIFF(a, b, UNIT) ->
// name(a, b, 'unit'), turning the bare unit keyword into a string argument for
// the helper UDF. The function name is preserved.
func rewriteDateDiff(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error) {
	commas := topLevelCommas(tokens, open, closeIdx)
	if len(commas) != 2 {
		return nil, false, nil
	}
	firstComma, secondComma := commas[0], commas[1]
	unitTok := nextSig(tokens, secondComma+1)
	if unitTok < 0 || tokens[unitTok].kind != tokWord {
		return nil, false, nil
	}
	if after := nextSig(tokens, unitTok+1); after != closeIdx {
		return nil, false, nil
	}

	argA, err := googlesqlCallPass(tokens[open+1 : firstComma])
	if err != nil {
		return nil, false, err
	}
	argB, err := googlesqlCallPass(tokens[firstComma+1 : secondComma])
	if err != nil {
		return nil, false, err
	}
	argA = trimSpaceTokens(argA)
	argB = trimSpaceTokens(argB)
	repl := make([]token, 0, len(argA)+len(argB)+8)
	repl = append(repl, tokens[nameIdx], opToken("("))
	repl = append(repl, argA...)
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, argB...)
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, stringToken(strings.ToLower(tokens[unitTok].text)))
	repl = append(repl, opToken(")"))
	return repl, true, nil
}
