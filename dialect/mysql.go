package dialect

import (
	"fmt"
	"strings"
)

// rewriteMySQL applies the MySQL token rewrite rules. The lexical rules (M-1
// backtick identifiers, M-2 double-quoted strings, M-3 # comments, M-4
// backslash escapes) are handled by the tokenizer and renderer; this pass
// handles the structural rules:
//
//	C-1  EXTRACT(part FROM x)            -> DATE_PART('part', x)
//	M-5  DATE_ADD/DATE_SUB(x, INTERVAL n unit) -> datetime(x, '±n unit')
//	M-6  GROUP_CONCAT(x SEPARATOR s)     -> group_concat(x, s)
//	M-7  a DIV b                         -> CAST(a / b AS INTEGER)
//	M-8  CAST(x AS mysql_type)           -> CAST(x AS sqlite_type)
//	M-9  x RLIKE p                       -> x REGEXP p
//
// M-10 (LIMIT n, m) needs no rewrite: SQLite accepts it natively.
func rewriteMySQL(tokens []token) ([]token, error) {
	out, err := mysqlCallPass(tokens)
	if err != nil {
		return nil, err
	}
	out, err = divPass(out)
	if err != nil {
		return nil, err
	}
	return renameWordPass(out, "RLIKE", "REGEXP"), nil
}

// mysqlCallPass rewrites the MySQL function-call rules (C-1, M-5, M-6, M-8),
// recursing into the arguments of recognized calls so nested calls are handled
// in one pass.
func mysqlCallPass(tokens []token) ([]token, error) {
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
				repl, handled, err := mysqlRewriteCall(tokens, i, open, closeIdx)
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

// mysqlRewriteCall rewrites the recognized MySQL call that starts with the word
// at nameIdx and spans open..close. It returns the replacement tokens and
// whether the call was rewritten. When it is not (handled is false), the caller
// keeps scanning into the call's tokens, so nested recognized calls are still
// rewritten by the main pass.
func mysqlRewriteCall(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error) {
	switch strings.ToUpper(tokens[nameIdx].text) {
	case "EXTRACT":
		return rewriteExtractCall(tokens, open, closeIdx, mysqlCallPass)
	case "DATE_ADD":
		return rewriteDateArith(tokens, open, closeIdx, "+")
	case "DATE_SUB":
		return rewriteDateArith(tokens, open, closeIdx, "-")
	case "GROUP_CONCAT":
		return rewriteGroupConcat(tokens, nameIdx, open, closeIdx)
	case "CAST":
		return rewriteCastCall(tokens, nameIdx, open, closeIdx, mysqlCastTypes, mysqlCallPass)
	default:
		return nil, false, nil
	}
}

// mysqlIntervalUnits maps the INTERVAL units supported by M-5 to the SQLite
// datetime modifier unit.
var mysqlIntervalUnits = map[string]string{
	"SECOND": unitSecond,
	"MINUTE": unitMinute,
	"HOUR":   unitHour,
	"DAY":    unitDay,
	"MONTH":  unitMonth,
	"YEAR":   unitYear,
}

// rewriteDateArith implements M-5: DATE_ADD/DATE_SUB(x, INTERVAL n unit) ->
// datetime(x, '±n unit'). sign is "+" for DATE_ADD and "-" for DATE_SUB.
func rewriteDateArith(tokens []token, open, closeIdx int, sign string) ([]token, bool, error) {
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
		return nil, false, fmt.Errorf("%w: DATE_ADD/DATE_SUB INTERVAL value must be a numeric literal", ErrUnsupportedSyntax)
	}
	unitTok := nextSig(tokens, value+1)
	if unitTok < 0 || tokens[unitTok].kind != tokWord {
		return nil, false, fmt.Errorf("%w: DATE_ADD/DATE_SUB INTERVAL is missing a unit", ErrUnsupportedSyntax)
	}
	unit, ok := mysqlIntervalUnits[strings.ToUpper(tokens[unitTok].text)]
	if !ok {
		return nil, false, fmt.Errorf("%w: unsupported INTERVAL unit %q", ErrUnsupportedSyntax, tokens[unitTok].text)
	}
	if after := nextSig(tokens, unitTok+1); after != closeIdx {
		return nil, false, fmt.Errorf("%w: unsupported INTERVAL expression", ErrUnsupportedSyntax)
	}

	expr, err := mysqlCallPass(tokens[open+1 : comma])
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

// rewriteGroupConcat implements M-6: GROUP_CONCAT(x SEPARATOR s) ->
// group_concat(x, s). A leading DISTINCT is preserved.
func rewriteGroupConcat(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error) {
	sep := topLevelWord(tokens, open, closeIdx, "SEPARATOR")
	inner, err := mysqlCallPass(tokens[open+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	repl := []token{tokens[nameIdx], opToken("(")}
	if sep < 0 {
		repl = append(repl, inner...)
		repl = append(repl, opToken(")"))
		return repl, true, nil
	}
	// Replace the SEPARATOR keyword (offset-shifted into inner) with a comma,
	// dropping any whitespace that immediately precedes it so the result reads
	// "x, s" rather than "x , s".
	sepInner := sep - (open + 1)
	for j := range inner {
		if j == sepInner {
			for len(repl) > 2 && repl[len(repl)-1].kind == tokWhitespace {
				repl = repl[:len(repl)-1]
			}
			repl = append(repl, opToken(","))
			continue
		}
		repl = append(repl, inner[j])
	}
	repl = append(repl, opToken(")"))
	return repl, true, nil
}

// divPass implements M-7: a DIV b -> CAST(a / b AS INTEGER). The operands must
// be primary expressions.
func divPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if isWordEq(tokens[i], "DIV") {
			start, ok := primaryStartBack(out)
			if !ok {
				return nil, fmt.Errorf("%w: left operand of DIV is not a primary expression", ErrUnsupportedSyntax)
			}
			rightStart := nextSig(tokens, i+1)
			rightEnd, ok := primaryEndForward(tokens, i+1)
			if !ok {
				return nil, fmt.Errorf("%w: right operand of DIV is not a primary expression", ErrUnsupportedSyntax)
			}
			left := append([]token{}, trimSpaceTokens(out[start:])...)
			out = out[:start]
			out = append(out, wordToken("CAST"), opToken("("))
			out = append(out, left...)
			out = append(out, spaceToken(), opToken("/"), spaceToken())
			out = append(out, tokens[rightStart:rightEnd+1]...)
			out = append(out, spaceToken(), wordToken("AS"), spaceToken(), wordToken(sqliteInteger), opToken(")"))
			i = rightEnd + 1
			continue
		}
		out = append(out, tokens[i])
		i++
	}
	return out, nil
}

// renameWordPass renames every unquoted word equal to from (case-insensitive)
// to the given replacement, preserving all other tokens. It implements the
// operator-keyword renames such as M-9 (RLIKE -> REGEXP).
func renameWordPass(tokens []token, from, to string) []token {
	out := make([]token, len(tokens))
	copy(out, tokens)
	for i := range out {
		if isWordEq(out[i], from) {
			out[i] = wordToken(to)
		}
	}
	return out
}
