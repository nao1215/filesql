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
//	M-8  CAST(x AS mysql_type)           -> mysql_cast(x, 'mysql_type')
//	M-9  x RLIKE p                       -> x REGEXP p
//	M-11 a / b                           -> mysql_divide(a, b)
//	M-12 a || b                          -> a OR b
//	M-13 a <=> b                         -> a IS b
//	M-14 DATE 'lit' / TIMESTAMP 'lit'    -> 'lit'
//	M-15 CURRENT_DATE() and friends      -> CURRENT_DATE
//	M-16 TIMESTAMPDIFF/TIMESTAMPADD      -> DATE_DIFF / interval_add
//	M-17 POSITION(x IN y), SUBSTRING FROM -> INSTR / SUBSTR
//	M-18 ANY_VALUE / STD / VARIANCE      -> SQLite aggregate expressions
//	M-19 UNION DISTINCT                  -> UNION
//	M-20 LENGTH / CHAR_LENGTH / ORD / TRIM(BOTH x FROM s)
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
	// M-11: MySQL's "/" is floating-point division, so 5/2 is 2.5 rather than
	// the 2 SQLite gives for two integers.
	out, err = binaryOperatorPass(out, "/", "mysql_divide")
	if err != nil {
		return nil, err
	}
	// M-12/M-13: MySQL reads "||" as a logical OR under its default sql_mode,
	// and "<=>" as null-safe equality, which SQLite spells IS.
	out = replaceOperatorWithWord(out, "||", "OR")
	out = replaceOperatorWithWord(out, "<=>", "IS")
	// M-14/M-15: MySQL accepts typed date literals and the parenthesized
	// CURRENT_DATE() spelling, neither of which SQLite parses.
	// M-19: SQLite rejects "UNION DISTINCT"; its plain UNION already
	// deduplicates.
	out = unionDistinctPass(currentValueParenPass(typePrefixedLiteralPass(out)))
	// M-18: ANY_VALUE and the variance family have no SQLite aggregate.
	return aggregatePass(renameWordPass(out, "RLIKE", "REGEXP"), MySQL)
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
	case fnNameExtract:
		return rewriteExtractCall(tokens, open, closeIdx, mysqlCallPass)
	case "DATE_ADD":
		return rewriteDateArith(tokens, open, closeIdx, "+", mysqlCallPass)
	case "DATE_SUB":
		return rewriteDateArith(tokens, open, closeIdx, "-", mysqlCallPass)
	case "GROUP_CONCAT":
		return rewriteGroupConcat(tokens, nameIdx, open, closeIdx)
	case "TIMESTAMPDIFF":
		return rewriteTimestampDiff(tokens, open, closeIdx, mysqlCallPass)
	case "TIMESTAMPADD":
		return rewriteTimestampAdd(tokens, open, closeIdx, mysqlCallPass)
	case "POSITION":
		return rewritePosition(tokens, open, closeIdx)
	case "SUBSTRING":
		return rewriteSubstring(tokens, open, closeIdx)
	case "LENGTH", "OCTET_LENGTH":
		// MySQL LENGTH counts bytes; SQLite's counts characters.
		return rewriteRenameCall(tokens, open, closeIdx, "octet_length", mysqlCallPass)
	case fnNameCharLen, fnNameCharLen2:
		return rewriteRenameCall(tokens, open, closeIdx, "length", mysqlCallPass)
	case "ORD":
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_ord", mysqlCallPass)
	case fnNameTrim:
		return rewriteTrim(tokens, open, closeIdx, mysqlCallPass)
	case "HEX":
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_hex", mysqlCallPass)
	case "UNHEX":
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_unhex", mysqlCallPass)
	case fnNameCast:
		return rewriteCastCall(tokens, open, closeIdx, MySQL, "mysql_cast", mysqlCallPass)
	default:
		return nil, false, nil
	}
}

// rewriteGroupConcat implements M-6: GROUP_CONCAT(x SEPARATOR s) ->
// group_concat(x, s). A leading DISTINCT is preserved.
func rewriteGroupConcat(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error) {
	sep := topLevelWord(tokens, open, closeIdx, "SEPARATOR")
	if sep < 0 {
		inner, err := mysqlCallPass(tokens[open+1 : closeIdx])
		if err != nil {
			return nil, false, err
		}
		repl := []token{tokens[nameIdx], opToken("(")}
		repl = append(repl, inner...)
		return append(repl, opToken(")")), true, nil
	}
	// SQLite takes the separator as a second argument, and a second argument is
	// incompatible with its DISTINCT aggregates. Say so rather than letting the
	// engine report "DISTINCT aggregates must have exactly one argument", which
	// says nothing about the query the caller wrote.
	if distinct := nextSig(tokens, open+1); distinct >= 0 && isWordEq(tokens[distinct], "DISTINCT") {
		return nil, false, fmt.Errorf("%w: GROUP_CONCAT cannot combine DISTINCT with SEPARATOR", ErrUnsupportedSyntax)
	}

	// The value ends at ORDER BY when there is one. SQLite spells the whole
	// thing group_concat(value, separator ORDER BY ...), so the separator has to
	// move ahead of the ORDER BY rather than replace the SEPARATOR keyword in
	// place: left where it was, SQLite would read it as another sort term and
	// silently fall back to a comma.
	valueEnd := sep
	orderBy := -1
	if order := topLevelWord(tokens, open, sep, "ORDER"); order >= 0 {
		orderBy = order
		valueEnd = order
	}
	value, err := mysqlCallPass(tokens[open+1 : valueEnd])
	if err != nil {
		return nil, false, err
	}
	separator, err := mysqlCallPass(tokens[sep+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}

	repl := []token{tokens[nameIdx], opToken("(")}
	repl = append(repl, trimSpaceTokens(value)...)
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, trimSpaceTokens(separator)...)
	if orderBy >= 0 {
		order, err := mysqlCallPass(tokens[orderBy:sep])
		if err != nil {
			return nil, false, err
		}
		repl = append(repl, spaceToken())
		repl = append(repl, trimSpaceTokens(order)...)
	}
	return append(repl, opToken(")")), true, nil
}

// divPass implements M-7: a DIV b -> CAST(a / b AS INTEGER). The operands must
// be primary expressions. This keeps SQLite's own truncating CAST rather than
// the MySQL cast helper, since DIV truncates toward zero while MySQL's CAST
// rounds.
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
