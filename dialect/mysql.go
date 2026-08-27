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
//	M-24 a MOD b                         -> a % b
//	M-8  CAST(x AS mysql_type)           -> mysql_cast(x, 'mysql_type')
//	M-9  x RLIKE p                       -> x REGEXP p
//	M-11 a / b                           -> mysql_divide(a, b)
//	M-12 CONCAT(a, b)                   -> strict_concat(a, b)
//	M-12 a || b                          -> a OR b
//	M-13 a <=> b                         -> a IS b
//	M-14 DATE 'lit' / TIMESTAMP 'lit'    -> 'lit'
//	M-15 CURRENT_DATE() and friends      -> CURRENT_DATE
//	M-16 TIMESTAMPDIFF/TIMESTAMPADD      -> DATE_DIFF / interval_add
//	M-17 POSITION(x IN y), SUBSTRING FROM -> INSTR / mysql_substr, and the
//	                                     comma form of SUBSTRING with it
//	M-18 ANY_VALUE / STD / VARIANCE      -> SQLite aggregate expressions
//	M-19 UNION DISTINCT                  -> UNION
//	M-20 LENGTH / CHAR_LENGTH / ORD / TRIM(BOTH x FROM s)
//	M-21 a && b                          -> a AND b
//	M-21 !a                              -> (NOT a)
//	M-21 a ^ b                           -> mysql_bit_xor(a, b)
//	M-21 a XOR b                         -> ErrUnsupportedSyntax
//	M-25 UPPER(x) / LOWER(x)             -> unicode_upper / unicode_lower
//	M-26 LPAD(x, n, p) / RPAD(x, n, p)   -> mysql_lpad / mysql_rpad
//	M-27 ADDDATE / SUBDATE               -> interval_add, interval or day form
//	M-28 WEEK / WEEKOFYEAR / YEARWEEK    -> mysql_week / mysql_weekofyear /
//	                                     mysql_yearweek
//	M-29 a << b / a >> b                 -> mysql_shift_left / mysql_shift_right
//	M-30 x REGEXP p, QUOTE, ASCII        -> mysql_regexp / mysql_quote /
//	                                     mysql_ascii
//	M-31 INSERT(...)                     -> mysql_insert(...)
//	M-31 ISNULL(x)                       -> (x IS NULL)
//	M-31 ATAN(y, x)                      -> atan2(y, x)
//
// M-10 (LIMIT n, m) needs no rewrite: SQLite accepts it natively.
func rewriteMySQL(tokens []token) ([]token, error) {
	out, err := mysqlCallPass(tokens)
	if err != nil {
		return nil, err
	}
	// M-24: MOD the operator is the same operation as "%" at the same
	// precedence, so it becomes that token here and the "%" pass below turns
	// both into the helper. It runs before DIV so that a MOD standing to the
	// left of a DIV is already an operator token when DIV goes looking for its
	// left operand.
	out = modPass(out)
	out, err = divPass(out)
	if err != nil {
		return nil, err
	}
	// M-8: BINARY is a cast written as a prefix operator. It runs after the call
	// pass so the BINARY that names a type inside CAST or CONVERT is already
	// part of a helper call and cannot be read as the operator.
	out, err = prefixCastPass(out, "BINARY", "mysql_cast")
	if err != nil {
		return nil, err
	}
	// M-21: "!" is MySQL's NOT, and it binds tighter than every operator the
	// passes below rewrite, so it is resolved before them: "!a ^ b" is
	// "(!a) ^ b", and a "^" pass that ran first would take "a" for its left
	// operand and leave the negation outside the call.
	out, err = unaryNotPass(out)
	if err != nil {
		return nil, err
	}
	// M-11: MySQL's "/" is floating-point division, so 5/2 is 2.5 rather than
	// the 2 SQLite gives for two integers.
	out, err = binaryChainOperatorPass(out, "/", "mysql_divide")
	if err != nil {
		return nil, err
	}
	// M-24: SQLite's "%" truncates both operands to integers before taking the
	// remainder, so "7 % 2.5" answered 1 where MySQL answers 2.0. The helper
	// keeps the operands as written, and MOD the operator and MOD the call both
	// reach it, so the three spellings cannot disagree.
	out, err = binaryChainOperatorPass(out, "%", "mysql_mod")
	if err != nil {
		return nil, err
	}
	// M-21: "^" is a bitwise XOR in MySQL, which SQLite has no operator for. It
	// binds tightly enough that both operands are primaries, so the rewrite is a
	// helper call rather than the (a|b)&~(a&b) expansion, which would evaluate
	// each operand twice.
	out, err = binaryOperatorPass(out, "^", "mysql_bit_xor")
	if err != nil {
		return nil, err
	}
	// M-29: MySQL shifts an unsigned 64-bit value where SQLite shifts a signed
	// one, so ">>" brought the sign bit down instead of zeros. This runs after
	// the passes above so their operators are already calls, leaving only the
	// arithmetic that an operand of a shift has to reach across.
	out, err = shiftPass(out)
	if err != nil {
		return nil, err
	}
	// M-21: XOR has no SQLite spelling that keeps its precedence. It sits between
	// OR and AND, so its operands are whole AND-expressions rather than the
	// primaries a rewrite can pick out: translating it as if they were primaries
	// would reassociate "a AND b XOR c" into something MySQL never meant.
	if err := rejectWordPass(out, "XOR",
		"SQLite has no operator with its precedence; write (a AND NOT b) OR (NOT a AND b)"); err != nil {
		return nil, err
	}
	// M-12/M-13/M-21: MySQL reads "||" as a logical OR under its default
	// sql_mode, "&&" as AND, and "<=>" as null-safe equality, which SQLite
	// spells IS.
	out = replaceOperatorWithWord(out, "||", "OR")
	out = replaceOperatorWithWord(out, "&&", "AND")
	out = replaceOperatorWithWord(out, "<=>", "IS")
	// M-14/M-15: MySQL accepts typed date literals and the parenthesized
	// CURRENT_DATE() spelling, neither of which SQLite parses.
	// M-19: SQLite rejects "UNION DISTINCT"; its plain UNION already
	// deduplicates.
	out = unionDistinctPass(currentValueParenPass(typePrefixedLiteralPass(out)))
	// M-5: "x + INTERVAL n unit" is the operator spelling of DATE_ADD, and the
	// two have to answer the same thing. It runs after the typed literal above
	// so that "DATE '2026-01-01' + INTERVAL 1 DAY" has already lost its DATE
	// keyword and the left operand is a plain literal.
	out, err = unitIntervalPass(out)
	if err != nil {
		return nil, err
	}
	if err := checkLeftoverInterval(out); err != nil {
		return nil, err
	}
	// M-22: MySQL's LIKE escapes with a backslash unless an ESCAPE clause says
	// otherwise, and its default collation folds case beyond ASCII. Appending
	// SQLite's own ESCAPE clause covered the escape but left two gaps that the
	// helper the other dialects already route through does not have: SQLite's
	// LIKE matches nothing at all for a pattern ending in the escape character,
	// where MySQL reads that character as itself, and its folding stops at
	// ASCII, so "É" did not match "é".
	out, err = likePass(out, "LIKE", "like_insensitive")
	if err != nil {
		return nil, err
	}
	// M-9/M-30: RLIKE is a second spelling of REGEXP, and both match under the
	// collation of their operands, which folds case by default the way LIKE
	// above does. Left as an operator, the match ran with Go's own rules and
	// answered no for a letter MySQL matches.
	out, err = regexpPass(renameWordPass(out, "RLIKE", "REGEXP"), "mysql_regexp")
	if err != nil {
		return nil, err
	}
	// M-23: a hexadecimal literal means one thing or the other depending on where
	// it sits, and the translation cannot see which.
	if err := rejectHexLiteralPass(out); err != nil {
		return nil, err
	}
	// M-18: ANY_VALUE and the variance family have no SQLite aggregate.
	return aggregatePass(out, MySQL)
}

// mysqlCallPass rewrites the MySQL function-call rules (C-1, M-5, M-6, M-8),
// recursing into the arguments of recognized calls so nested calls are handled
// in one pass.
func mysqlCallPass(tokens []token) ([]token, error) {
	return walkCalls(tokens, mysqlRewriteCall)
}

// mysqlRewriteCall rewrites the recognized MySQL call that starts with the word
// at nameIdx and spans open..close. It returns the replacement tokens and
// whether the call was rewritten. When it is not (handled is false), the caller
// keeps scanning into the call's tokens, so nested recognized calls are still
// rewritten by the main pass.
func mysqlRewriteCall(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error) {
	switch strings.ToUpper(tokens[nameIdx].text) {
	case fnNameExtract:
		return rewriteExtractCall(tokens, open, closeIdx, "mysql_date_part", mysqlCallPass)
	case "DATE_ADD":
		return rewriteDateArith(tokens, open, closeIdx, "+", mysqlCallPass)
	case "DATE_SUB":
		return rewriteDateArith(tokens, open, closeIdx, "-", mysqlCallPass)
	case "GROUP_CONCAT":
		return rewriteGroupConcat(tokens, nameIdx, open, closeIdx)
	case "TIMESTAMPDIFF":
		return rewriteTimestampDiff(tokens, open, closeIdx, mysqlCallPass)
	case "TIMEDIFF":
		// SQLite has its own timediff() since 3.43, which answers in SQLite's
		// interval spelling; MySQL's answers a TIME. Renamed so the call cannot
		// fall through to SQLite's.
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_timediff", mysqlCallPass)
	case "TIMESTAMPADD":
		return rewriteTimestampAdd(tokens, open, closeIdx, mysqlCallPass)
	case "ADDDATE":
		return rewriteAddDate(tokens, open, closeIdx, "+", mysqlCallPass)
	case "SUBDATE":
		return rewriteAddDate(tokens, open, closeIdx, "-", mysqlCallPass)
	case "WEEK", "WEEKOFYEAR", "YEARWEEK":
		// MySQL numbers weeks its own way, by a mode that decides which day
		// starts a week and which week is week 1, so the call carries MySQL's
		// name rather than a shared one.
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_"+strings.ToLower(tokens[nameIdx].text), mysqlCallPass)
	case "POSITION":
		return rewritePosition(tokens, open, closeIdx, mysqlCallPass)
	case fnNameSubstring, fnNameSubstr:
		return rewriteSubstringCall(tokens, open, closeIdx, "mysql_substr", mysqlCallPass)
	case fnNameReplace:
		// SQLite answers the subject for an empty search string without looking
		// at the replacement, so a NULL replacement did not reach the result.
		return rewriteRenameCall(tokens, open, closeIdx, "dialect_replace", mysqlCallPass)
	case "CHAR":
		// MySQL CHAR builds bytes where SQLite's char() builds code points, and
		// takes a charset clause SQLite cannot parse.
		if repl, handled, err := rewriteCharUsingCall(tokens, open, closeIdx, "mysql_char", mysqlCallPass); handled || err != nil {
			return repl, handled, err
		}
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_char", mysqlCallPass)
	case "SOUNDEX":
		// SQLite has a soundex() of its own that answers a placeholder for a
		// value holding no letter and cuts the code to four characters.
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_soundex", mysqlCallPass)
	case fnNameRound:
		return rewriteRoundEvenCall(tokens, open, closeIdx, mysqlCallPass)
	case fnNameMod:
		// SQLite's own mod() happens to answer what MySQL answers, but the
		// operator spellings reach mysql_mod and the three have to agree.
		//
		// Only the call is renamed. MySQL tells MOD the function from MOD the
		// operator by whether the parenthesis follows the name with nothing
		// between, so "a MOD (b + 1)" is the operator and belongs to modPass,
		// which runs later; renaming it here would take "(b + 1)" for the whole
		// argument list and leave "a" stranded beside the call.
		if open != nameIdx+1 {
			return nil, false, nil
		}
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_mod", mysqlCallPass)
	case "LENGTH", "OCTET_LENGTH":
		// MySQL LENGTH counts bytes; SQLite's counts characters.
		return rewriteRenameCall(tokens, open, closeIdx, "octet_length", mysqlCallPass)
	case fnNameCharLen, fnNameCharLen2:
		return rewriteRenameCall(tokens, open, closeIdx, "length", mysqlCallPass)
	case "LOG":
		// MySQL LOG(x) is the natural logarithm where SQLite's log(x) is the
		// base-ten one. The two-argument form already writes its base first, the
		// way SQLite's does, so it is left alone.
		if callArity(tokens, open, closeIdx) == 1 {
			return rewriteRenameCall(tokens, open, closeIdx, "ln", mysqlCallPass)
		}
		return nil, false, nil
	case "FORMAT":
		// MySQL FORMAT(x, d) rounds and groups a number. SQLite's format() is an
		// alias of printf, which reads the first argument as a format string and
		// answered the number unchanged.
		if callArity(tokens, open, closeIdx) != 2 {
			return nil, false, fmt.Errorf("%w: FORMAT takes a value and a number of decimal places; its locale argument is not supported", ErrUnsupportedSyntax)
		}
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_format", mysqlCallPass)
	case "LEFT", "RIGHT":
		// A negative length answers the empty string in MySQL and trims the far
		// end in PostgreSQL, so each dialect names its own helper.
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_"+strings.ToLower(tokens[nameIdx].text), mysqlCallPass)
	case "REGEXP_REPLACE":
		// MySQL's fourth argument is a start position and its fifth an
		// occurrence, where PostgreSQL's fourth is a flag string.
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_regexp_replace", mysqlCallPass)
	case "ORD":
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_ord", mysqlCallPass)
	case "INSERT":
		// INSERT begins a statement in SQLite, so the call form is a syntax
		// error there whatever this package registers. It is renamed rather
		// than registered.
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_insert", mysqlCallPass)
	case "ISNULL":
		// SQLite spells this as a postfix operator, so ISNULL(x) is a syntax
		// error to it and the call becomes the operator instead.
		return rewriteIsNull(tokens, open, closeIdx, mysqlCallPass)
	case "ATAN":
		// SQLite's atan takes one argument; MySQL's two-argument form is its
		// atan2, with the same argument order.
		if callArity(tokens, open, closeIdx) == 2 {
			return rewriteRenameCall(tokens, open, closeIdx, "atan2", mysqlCallPass)
		}
		return nil, false, nil
	case "CONCAT":
		// MySQL CONCAT returns NULL when any argument is NULL. SQLite's own
		// concat() treats a NULL as an empty string, so passing the call through
		// answered a plausible non-NULL string where MySQL answers NULL.
		return rewriteRenameCall(tokens, open, closeIdx, "strict_concat", mysqlCallPass)
	case fnNameTrim:
		return rewriteTrim(tokens, open, closeIdx, mysqlCallPass)
	case fnNameUpper, fnNameLower:
		// SQLite's own upper() and lower() fold ASCII alone, where every dialect
		// here folds the whole of Unicode: UPPER('école') came back 'éCOLE'.
		return rewriteRenameCall(tokens, open, closeIdx, unicodeCaseHelper(tokens[nameIdx].text), mysqlCallPass)
	case "HEX":
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_hex", mysqlCallPass)
	case "QUOTE":
		// SQLite's own quote() doubles the single quote and leaves a number
		// unquoted, neither of which MySQL reads back.
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_quote", mysqlCallPass)
	case "ASCII":
		// MySQL's ASCII answers a byte and PostgreSQL's a code point, and the
		// shared helper answers the latter.
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_ascii", mysqlCallPass)
	case "UNHEX":
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_unhex", mysqlCallPass)
	case fnNameLpad, fnNameRpad:
		// A negative length and an empty pad are answered differently by each
		// dialect, so each names its own helper rather than sharing one.
		return rewriteRenameCall(tokens, open, closeIdx, "mysql_"+strings.ToLower(tokens[nameIdx].text), mysqlCallPass)
	case "CONVERT":
		// MySQL's other cast spelling, and its charset clause.
		return rewriteConvertCall(tokens, open, closeIdx, MySQL, "mysql_cast", mysqlCallPass)
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

// modPass implements M-24: a MOD b -> a % b.
//
// MySQL gives MOD, DIV, "%" and "*" one precedence level, and SQLite gives "%"
// the level of "*" and "/", so replacing the word with the operator cannot
// change how the expression groups; both also take the sign of the dividend, so
// it cannot change the answer either. Left as it was, the word reached SQLite,
// which has no such operator, and the caller was told their own query had a
// syntax error near a token they did write.
//
// The word is only an operator when operands stand on both sides of it: MOD(a,
// b) is a function call, which already works, and a name spelled "mod" that a
// query quotes is an identifier token rather than a word by the time this runs.
// A call is told from an operator the way MySQL's own parser tells them apart,
// by whether the parenthesis follows the name with nothing between: "MOD(7, 2)"
// is the function and "a MOD (b + 1)" is the operator.
func modPass(tokens []token) []token {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if isWordEq(tokens[i], "MOD") && isModOperator(out, tokens, i) {
			out = append(out, opToken("%"))
			i++
			continue
		}
		out = append(out, tokens[i])
		i++
	}
	return out
}

// isModOperator reports whether the MOD at index i stands between two operands
// rather than naming a function or a column.
func isModOperator(out []token, tokens []token, i int) bool {
	if i+1 < len(tokens) && isOpEq(tokens[i+1], "(") {
		return false
	}
	if _, ok := primaryStartBack(out); !ok {
		return false
	}
	_, ok := primaryEndForward(tokens, i+1)
	return ok
}

// divPass implements M-7: a DIV b -> CAST(a / b AS INTEGER). This keeps SQLite's
// own truncating CAST rather than the MySQL cast helper, since DIV truncates
// toward zero while MySQL's CAST rounds.
//
// The right operand is one primary expression, which is what left-to-right
// association gives it. The left operand is the whole chain of equal-precedence
// operators before it: MySQL puts "*", "/", "%", DIV and MOD on one level, so
// "8 * 5 DIV 2" is "(8 * 5) DIV 2" and reading only the primary beside the
// operator answered 16 where MySQL answers 20. An extended operand is
// parenthesized on the way out, so the "/" pass that runs later sees one
// primary and cannot regroup it in turn.
func divPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if isWordEq(tokens[i], "DIV") {
			start, ok := primaryStartBack(out)
			if !ok {
				return nil, fmt.Errorf("%w: left operand of DIV is not a primary expression", ErrUnsupportedSyntax)
			}
			start, extended := operandChainStartBack(out, start)
			rightStart := nextSig(tokens, i+1)
			rightEnd, ok := primaryEndForward(tokens, i+1)
			if !ok {
				return nil, fmt.Errorf("%w: right operand of DIV is not a primary expression", ErrUnsupportedSyntax)
			}
			left := append([]token{}, trimSpaceTokens(out[start:])...)
			if extended {
				left = append([]token{opToken("(")}, append(left, opToken(")"))...)
			}
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

// rejectHexLiteralPass refuses MySQL's 0x hexadecimal literal.
//
// MySQL calls it a binary string: SELECT 0x41 prints "A", and comparing a
// column against 0x616263 compares it against "abc". In numeric context the
// same literal is the number: 0x10 + 1 is 17. SQLite has only the second
// reading, so the literal reached it as a number and a comparison against a
// text column quietly became a numeric one, matching different rows with
// nothing to say so.
//
// Rewriting it to the string it stands for would fix that case and break the
// other one just as quietly, because which reading applies depends on where the
// literal sits — something a token rewrite cannot see. A literal with two
// meanings and no way to tell them apart is what ErrUnsupportedSyntax is for:
// the caller writes the one they meant, x'41' for the string or 65 for the
// number, and gets what they asked for either way.
func rejectHexLiteralPass(tokens []token) error {
	for _, t := range tokens {
		if t.kind != tokNumber || len(t.text) < 3 {
			continue
		}
		if t.text[0] == '0' && (t.text[1] == 'x' || t.text[1] == 'X') {
			return fmt.Errorf("%w: MySQL reads %s as a string in one place and as a number in another, and SQLite has only the number; write x'%s' for the string or the decimal for the number",
				ErrUnsupportedSyntax, t.text, t.text[2:])
		}
	}
	return nil
}

// rewriteIsNull implements the ISNULL part of M-31: ISNULL(x) -> (x IS NULL).
// SQLite reads ISNULL as a postfix operator, so the call form is a syntax error
// there and a rename would not help.
func rewriteIsNull(tokens []token, open, closeIdx int, pass func([]token) ([]token, error)) ([]token, bool, error) {
	if callArity(tokens, open, closeIdx) != 1 {
		return nil, false, fmt.Errorf("%w: ISNULL takes one argument", ErrUnsupportedSyntax)
	}
	inner, err := pass(tokens[open+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	repl := make([]token, 0, len(inner)+6)
	repl = append(repl, opToken("("))
	repl = append(repl, trimSpaceTokens(inner)...)
	repl = append(repl, spaceToken(), wordToken("IS"), spaceToken(), wordToken("NULL"), opToken(")"))
	return repl, true, nil
}
