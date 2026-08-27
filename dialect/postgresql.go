package dialect

import (
	"fmt"
	"strconv"
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
//	P-5  SUBSTRING(x FROM n FOR m) -> postgresql_substr(x, n, m), and the
//	                                  comma form with it
//	P-6  STRING_AGG(x, s)          -> group_concat(x, s), and the DISTINCT form
//	                                  -> group_concat(DISTINCT x) for s = ','
//	P-8  CAST(x AS pg_type)        -> postgresql_cast(x, 'pg_type')
//	P-10 DISTINCT ON (...), LATERAL -> ErrUnsupportedSyntax
//	P-11 a ^ b                     -> power(a, b)
//	P-27 a # b                     -> postgresql_bit_xor(a, b)
//	P-12 x +/- INTERVAL 'text'     -> interval_text_add(x, 'text', ±1)
//	P-13 DATE 'lit' / TIMESTAMP 'lit' -> 'lit'
//	P-28 ORDER BY x [DESC]          -> ORDER BY x [DESC] NULLS LAST|FIRST
//	P-25 date + n / date - n / d1 - d2 -> postgresql_date_add / postgresql_date_diff
//	P-14 x SIMILAR TO p            -> similar_to(p, x)
//	P-15 BOOL_AND / STDDEV / ...   -> SQLite aggregate expressions
//	P-16 TRIM(BOTH x FROM s), OVERLAY, BTRIM, JSONB_ARRAY_LENGTH
//	P-17 ARRAY[...]                -> ErrUnsupportedSyntax
//	P-18 generate_series(...) etc. -> ErrUnsupportedSyntax
//	P-19 UPPER(x) / LOWER(x)       -> unicode_upper / unicode_lower
//	P-20 LPAD(x, n, p) / RPAD      -> postgresql_lpad / postgresql_rpad
//	P-21 a / b, a % b, MOD(a, b)   -> postgresql_divide / postgresql_mod, which
//	                                  raise on a zero divisor
//	P-22 TRUNC(x, n)               -> trunc_scale(x, n); div and width_bucket
//	                                  are helpers of their own names
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
	// P-25: "date + 1" moves the date by a day and "date - date" counts the days
	// between them. Neither reaches SQLite as anything but arithmetic on the
	// number the date text spells, so '2024-03-05'::date + 1 answered 2025. The
	// pass runs after the cast and typed-literal passes so a date operand is
	// visible as the helper call or the plain literal they leave behind.
	out = pgDateArithmeticPass(out)
	// Whatever INTERVAL the pass above did not consume has no SQLite form at
	// all, so it is refused here rather than left to SQLite's parser.
	if err := checkLeftoverInterval(out); err != nil {
		return nil, err
	}
	// P-14: SIMILAR TO is SQL-standard pattern matching that SQLite lacks.
	out, err = similarToPass(out)
	if err != nil {
		return nil, err
	}
	// P-21: PostgreSQL divides two integers the way SQLite does, so the helper
	// keeps that arithmetic and moves only the zero divisor, which PostgreSQL
	// raises on where SQLite answers NULL — a NULL in a numeric column reads as
	// missing data rather than as arithmetic the engine refused. The remainder
	// goes the same way, since mod(7, 0) raises in PostgreSQL too.
	out, err = binaryChainOperatorPass(out, "/", "postgresql_divide")
	if err != nil {
		return nil, err
	}
	out, err = binaryChainOperatorPass(out, "%", "postgresql_mod")
	if err != nil {
		return nil, err
	}
	// P-27: "#" is PostgreSQL's bitwise XOR, which SQLite has no operator for.
	// It reached SQLite's lexer as an unknown token.
	out, err = binaryOperatorPass(out, "#", "postgresql_bit_xor")
	if err != nil {
		return nil, err
	}
	// P-24: LOCALTIMESTAMP and LOCALTIME are keywords rather than calls in
	// PostgreSQL, so SQLite read them as column names and reported no such
	// column. They name the same clock the corresponding functions read.
	// The SQL-standard truth-value predicate and the quantified comparisons,
	// neither of which SQLite spells: IS UNKNOWN reached its parser as a column
	// name and "= ANY (...)" as a syntax error naming the subquery's SELECT.
	// P-26: the Unicode escape literal, whose leading U reached SQLite as a
	// column name.
	out, err = pgUnicodeEscapeStringPass(out)
	if err != nil {
		return nil, err
	}
	// A COLLATE clause names an ordering SQLite does not have, so it is mapped
	// onto the SQLite collation that means the same or refused by name.
	out, err = collatePass(out)
	if err != nil {
		return nil, err
	}
	// P-28: PostgreSQL sorts NULLs at the opposite end from SQLite, which
	// changes the rows a window function reads as well as the order they come
	// back in.
	out = pgNullsOrderPass(out)
	// COALESCE(x) is x. SQLite's own needs two arguments and refused the call.
	out = singleArgumentCoalescePass(out)
	out = isUnknownPass(out)
	out, err = quantifiedComparisonPass(out)
	if err != nil {
		return nil, err
	}
	out = wordToCallPass(out, "LOCALTIMESTAMP", "now")
	out = wordToCallPass(out, "LOCALTIME", "curtime")
	// P-23: BETWEEN SYMMETRIC has no SQLite form. It runs after the call pass
	// so the least() and greatest() it emits are the shared helpers rather than
	// PostgreSQL's NULL-skipping pair, which would lose a NULL bound.
	out, err = betweenSymmetricPass(out)
	if err != nil {
		return nil, err
	}
	out = renameWordPass(out, "STRING_AGG", "group_concat")
	return aggregatePass(out, PostgreSQL)
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
		if isWordEq(t, "ARRAY") {
			if b := nextSig(tokens, i+1); b >= 0 && isOpEq(tokens[b], "[") {
				return fmt.Errorf("%w: array literals are not supported", ErrUnsupportedSyntax)
			}
		}
		if isWordEq(t, "DISTINCT") {
			if on := nextSig(tokens, i+1); on >= 0 && isWordEq(tokens[on], "ON") {
				return fmt.Errorf("%w: DISTINCT ON is not supported", ErrUnsupportedSyntax)
			}
		}
		// P-18: a set-returning function is a row source, and SQLite has no form
		// for one. Passed through, the error was "no such table: generate_series",
		// which reads as a missing input file rather than as a construct the
		// translation cannot express.
		if name, ok := setReturningFunction(t); ok {
			if open := nextSig(tokens, i+1); open >= 0 && isOpEq(tokens[open], "(") {
				return fmt.Errorf("%w: %s is not supported; SQLite has no set-returning functions", ErrUnsupportedSyntax, name)
			}
		}
	}
	return nil
}

// pgSetReturningFunctions are the PostgreSQL functions that return a set of
// rows. They are named rather than detected, because nothing in the syntax says
// a call returns a set.
//
// It lists only the functions with no SQLite counterpart. The json ones are left
// out on purpose: SQLite's json1 extension has json_each and json_tree, so a
// query using them runs, and refusing it would take away something that works.
var pgSetReturningFunctions = map[string]struct{}{
	"generate_series":       {},
	"generate_subscripts":   {},
	"unnest":                {},
	"regexp_split_to_table": {},
	"string_to_table":       {},
	"regexp_matches":        {},
}

// setReturningFunction reports whether t names a set-returning function,
// returning the name PostgreSQL spells it with.
func setReturningFunction(t token) (string, bool) {
	if t.kind != tokWord {
		return "", false
	}
	name := strings.ToLower(t.text)
	if _, ok := pgSetReturningFunctions[name]; !ok {
		return "", false
	}
	return name, true
}

// pgCallPass rewrites the PostgreSQL function-call rules (C-1, P-4, P-5, P-8),
// recursing into the arguments of recognized calls.
func pgCallPass(tokens []token) ([]token, error) {
	return walkCalls(tokens, pgRewriteCall)
}

func pgRewriteCall(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error) {
	switch strings.ToUpper(tokens[nameIdx].text) {
	case fnNameExtract:
		return rewriteExtractCall(tokens, open, closeIdx, "DATE_PART", pgCallPass)
	case "POSITION":
		return rewritePosition(tokens, open, closeIdx, pgCallPass)
	case fnNameSubstring, fnNameSubstr:
		return rewritePostgresSubstringCall(tokens, open, closeIdx, pgCallPass)
	case fnNameFormat:
		// PostgreSQL's format() has its own verbs; SQLite's is printf and
		// answered NULL for the whole call when it met one it did not know.
		return rewriteRenameCall(tokens, open, closeIdx, "postgresql_format", pgCallPass)
	case "RANDOM":
		// SQLite's random() answers a pseudo-random 64-bit integer where
		// PostgreSQL's answers a double in [0, 1).
		return rewriteRenameCall(tokens, open, closeIdx, "postgresql_random", pgCallPass)
	case "JSONB_TYPEOF", "JSON_TYPEOF":
		// SQLite's json_type answers with SQLite's own type names -- text,
		// integer, real, true, false -- where PostgreSQL answers with the names
		// JSON itself defines: string, number, boolean.
		return rewriteRenameCall(tokens, open, closeIdx, "postgresql_json_typeof", pgCallPass)
	case fnNameReplace:
		// SQLite answers the subject for an empty search string without looking
		// at the replacement, so a NULL replacement did not reach the result.
		return rewriteRenameCall(tokens, open, closeIdx, "dialect_replace", pgCallPass)
	case fnNameRound:
		return rewriteRoundEvenCall(tokens, open, closeIdx, pgCallPass)
	case fnNameTrim:
		return rewriteTrim(tokens, open, closeIdx, pgCallPass)
	case "OVERLAY":
		return rewriteOverlay(tokens, open, closeIdx, pgCallPass)
	case fnNameUpper, fnNameLower:
		return rewriteRenameCall(tokens, open, closeIdx, unicodeCaseHelper(tokens[nameIdx].text), pgCallPass)
	case "TO_HEX":
		// PostgreSQL to_hex(n) converts an integer; GoogleSQL TO_HEX hexes the
		// bytes of its argument, and the helper of that name is written for it.
		return rewriteRenameCall(tokens, open, closeIdx, "postgresql_to_hex", pgCallPass)
	case "REGEXP_REPLACE":
		// PostgreSQL replaces the first match alone unless the flags say "g".
		return rewriteRenameCall(tokens, open, closeIdx, "postgresql_regexp_replace", pgCallPass)
	case "BTRIM":
		return rewriteRenameCall(tokens, open, closeIdx, "trim", pgCallPass)
	case "GREATEST", "LEAST":
		// PostgreSQL ignores a NULL argument; MySQL and BigQuery answer NULL
		// for the whole call, which is what the shared helpers do.
		return rewriteRenameCall(tokens, open, closeIdx, "postgresql_"+strings.ToLower(tokens[nameIdx].text), pgCallPass)
	case fnNameMod:
		return rewriteRenameCall(tokens, open, closeIdx, "postgresql_mod", pgCallPass)
	case fnNameTrunc:
		return rewriteTruncScaleCall(tokens, open, closeIdx, pgCallPass)
	case "JSONB_ARRAY_LENGTH", "JSON_ARRAY_LENGTH":
		return rewriteRenameCall(tokens, open, closeIdx, "json_array_length", pgCallPass)
	case fnNameCharLen, fnNameCharLen2:
		return rewriteRenameCall(tokens, open, closeIdx, "length", pgCallPass)
	case fnNameLpad, fnNameRpad:
		// A negative length and an empty pad are answered differently by each
		// dialect, so each names its own helper rather than sharing one.
		return rewriteRenameCall(tokens, open, closeIdx, "postgresql_"+strings.ToLower(tokens[nameIdx].text), pgCallPass)
	case fnNameCast:
		return rewriteCastCall(tokens, open, closeIdx, PostgreSQL, "postgresql_cast", pgCallPass)
	case fnNameStringAgg:
		// Only the DISTINCT form is handled here; the plain one is renamed by the
		// word pass below, which keeps the separator SQLite accepts there.
		return rewriteStringAggDistinct(tokens, open, closeIdx, pgCallPass)
	default:
		return nil, false, nil
	}
}

// rewritePosition implements P-4: POSITION(x IN y) -> INSTR(y, x).
func rewritePosition(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	in := topLevelWord(tokens, open, closeIdx, "IN")
	if in < 0 {
		return nil, false, nil
	}
	needle, err := recurse(tokens[open+1 : in])
	if err != nil {
		return nil, false, err
	}
	haystack, err := recurse(tokens[in+1 : closeIdx])
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

// wordToCallPass replaces a bare keyword with a call to name, for the
// PostgreSQL words that read a value without parentheses. A word already
// followed by "(" is left alone, since it is a call the caller wrote.
func wordToCallPass(tokens []token, word, name string) []token {
	out := make([]token, 0, len(tokens))
	for i, t := range tokens {
		if !isWordEq(t, word) {
			out = append(out, t)
			continue
		}
		if next := nextSig(tokens, i+1); next >= 0 && isOpEq(tokens[next], "(") {
			out = append(out, t)
			continue
		}
		out = append(out, wordToken(name), opToken("("), opToken(")"))
	}
	return out
}

// rewriteSubstringCall routes both spellings of SUBSTRING onto the dialect's own
// helper. The keyword form is converted to arguments by rewriteSubstring; the
// comma form is renamed, since SQLite's own substr() answers position 0 and a
// negative position by rules that are neither dialect's.
func rewriteSubstringCall(tokens []token, open, closeIdx int, target string, recurse callRecurser) ([]token, bool, error) {
	repl, ok, err := rewriteSubstring(tokens, open, closeIdx, target, recurse)
	if ok || err != nil {
		return repl, ok, err
	}
	return rewriteRenameCall(tokens, open, closeIdx, target, recurse)
}

// rewritePostgresSubstringCall is rewriteSubstringCall plus the two forms only
// PostgreSQL has. SUBSTRING(s FROM p) extracts the text matching p when p is a
// pattern rather than a position, and SUBSTRING(s SIMILAR p ESCAPE e) is the
// SQL-standard regular-expression form, which has no SQLite equivalent.
//
// PostgreSQL decides between a position and a pattern on the static type of the
// operand, so SUBSTRING('abc123' FROM '2') is the match of the pattern 2 rather
// than the substring starting at position 2. SQLite has no declared type to
// consult once the query runs, and the token stream is the one place the same
// information exists: a string literal is what PostgreSQL would type as text.
// An operand that is anything else -- a number, a column, an expression --
// keeps the positional reading.
func rewritePostgresSubstringCall(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	// SUBSTRING(x SIMILAR p ESCAPE e) and the older SUBSTRING(x FROM p FOR e)
	// are the SQL-standard escaped-pattern form, where the pattern's two escaped
	// double quotes mark the part to return. The FROM/FOR spelling is told from
	// the position-and-length one by the kind of its first operand.
	if similar := topLevelWord(tokens, open, closeIdx, "SIMILAR"); similar >= 0 {
		escape := topLevelWord(tokens, open, closeIdx, "ESCAPE")
		if escape <= similar {
			// topLevelWord finds each keyword on its own, so an ESCAPE written
			// before the SIMILAR would leave the pattern slice inverted and
			// panic rather than report the syntax.
			return nil, false, fmt.Errorf("%w: SUBSTRING(x SIMILAR p ESCAPE e) needs an ESCAPE clause after the pattern", ErrUnsupportedSyntax)
		}
		return rewriteSimilarSubstring(tokens, open, closeIdx, similar, escape, recurse)
	}
	from := topLevelWord(tokens, open, closeIdx, "FROM")
	forKw := topLevelWord(tokens, open, closeIdx, "FOR")
	if from >= 0 && forKw > from &&
		loneLiteralKind(tokens, from+1, forKw) == tokString &&
		loneLiteralKind(tokens, forKw+1, closeIdx) == tokString {
		// Both operands are strings, so the second is an escape character and
		// the first a pattern. A numeric FOR is a length however the FROM
		// operand is written: PostgreSQL answers "ell" for
		// substring('hello' from '2' for 3).
		return rewriteSimilarSubstring(tokens, open, closeIdx, from, forKw, recurse)
	}
	if from < 0 || forKw >= 0 {
		return rewriteSubstringCall(tokens, open, closeIdx, "postgresql_substr", recurse)
	}
	subject, err := recurse(tokens[open+1 : from])
	if err != nil {
		return nil, false, err
	}
	operand, err := recurse(tokens[from+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	subject, operand = trimSpaceTokens(subject), trimSpaceTokens(operand)
	switch kind := loneLiteralKind(tokens, from+1, closeIdx); kind {
	case tokString:
		return callTokens("regexp_extract", subject, operand), true, nil
	case tokNumber:
		return callTokens("postgresql_substr", subject, operand), true, nil
	default:
		// A column, a placeholder or an expression: the kind of the operand is
		// not in the query text, so the reading has to be chosen from the value
		// at run time. It is the reading PostgreSQL would have chosen whenever
		// the operand's type matches what its value looks like, which is every
		// integer column and every text column that does not hold digits.
		return callTokens("postgresql_substring_from", subject, operand), true, nil
	}
}

// rewriteSimilarSubstring builds the three-argument call behind the
// SQL-standard SUBSTRING: the subject, the pattern and the escape character,
// which stand before patternKw, between patternKw and escapeKw, and after
// escapeKw.
func rewriteSimilarSubstring(tokens []token, open, closeIdx, patternKw, escapeKw int, recurse callRecurser) ([]token, bool, error) {
	subject, err := recurse(tokens[open+1 : patternKw])
	if err != nil {
		return nil, false, err
	}
	pattern, err := recurse(tokens[patternKw+1 : escapeKw])
	if err != nil {
		return nil, false, err
	}
	escape, err := recurse(tokens[escapeKw+1 : closeIdx])
	if err != nil {
		return nil, false, err
	}
	repl := make([]token, 0, len(subject)+len(pattern)+len(escape)+8)
	repl = append(repl, wordToken("similar_substring"), opToken("("))
	repl = append(repl, trimSpaceTokens(subject)...)
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, trimSpaceTokens(pattern)...)
	repl = append(repl, opToken(","), spaceToken())
	repl = append(repl, trimSpaceTokens(escape)...)
	return append(repl, opToken(")")), true, nil
}

// loneLiteralKind reports the kind of the single literal that fills the tokens
// between from and end, or tokWord when what is there is anything else.
func loneLiteralKind(tokens []token, from, end int) tokenKind {
	kind := tokWord
	seen := false
	for i := from; i < end; i++ {
		if !isSignificant(tokens[i]) {
			continue
		}
		if seen || (tokens[i].kind != tokString && tokens[i].kind != tokNumber) {
			return tokWord
		}
		kind, seen = tokens[i].kind, true
	}
	if !seen {
		return tokWord
	}
	return kind
}

// rewriteSubstring implements P-5: SUBSTRING(x FROM n FOR m) -> target(x, n, m).
// The FROM and FOR parts are each optional; a missing FROM defaults the start to
// 1. The comma-argument form is left to the caller, which renames it.
func rewriteSubstring(tokens []token, open, closeIdx int, target string, recurse callRecurser) ([]token, bool, error) {
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
	subject, err := recurse(tokens[open+1 : subjectEnd])
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
		start, err = recurse(tokens[from+1 : startEnd])
		if err != nil {
			return nil, false, err
		}
		start = trimSpaceTokens(start)
	}
	if forKw >= 0 {
		length, err = recurse(tokens[forKw+1 : closeIdx])
		if err != nil {
			return nil, false, err
		}
		length = trimSpaceTokens(length)
	}

	repl := make([]token, 0, len(subject)+len(start)+len(length)+8)
	repl = append(repl, wordToken(target), opToken("("))
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

// fnNamePostgresDateAdd is the helper both directions of "date + n" reach.
const fnNamePostgresDateAdd = "postgresql_date_add"

// pgDateArithmeticPass rewrites PostgreSQL's date arithmetic with a plain
// number and its date difference, neither of which SQLite has: it reads both
// sides as numbers, so "'2024-03-05'::date + 1" answered 2025 and
// "'2024-03-05'::date - '2024-01-01'::date" answered 0 where PostgreSQL answers
// 2024-03-06 and 64.
//
// Only an operand this pass can see is a date is rewritten: a cast whose target
// is a date or a timestamp, or a string literal spelling one. A column has no
// type here to read, and PostgreSQL itself needs the column to be a date for the
// expression to compile at all, so the cast a caller already has to write is the
// signal. Leaving every other "+" alone keeps ordinary arithmetic out of a
// helper call.
func pgDateArithmeticPass(tokens []token) []token {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if !isOpEq(tokens[i], "+") && !isOpEq(tokens[i], "-") {
			out = append(out, tokens[i])
			i++
			continue
		}
		start, ok := primaryStartBack(out)
		if !ok {
			out = append(out, tokens[i])
			i++
			continue
		}
		rightStart := nextSig(tokens, i+1)
		rightEnd, rightOK := primaryEndForward(tokens, i+1)
		if rightStart < 0 || !rightOK {
			out = append(out, tokens[i])
			i++
			continue
		}
		left := trimSpaceTokens(out[start:])
		right := trimSpaceTokens(tokens[rightStart : rightEnd+1])
		leftIsDate, rightIsDate := isDateOperand(left), isDateOperand(right)
		helper := ""
		switch {
		case isOpEq(tokens[i], "-") && leftIsDate && rightIsDate:
			helper = "postgresql_date_diff"
		case leftIsDate && !rightIsDate:
			helper = fnNamePostgresDateAdd
		case isOpEq(tokens[i], "+") && rightIsDate && !leftIsDate:
			// "1 + date" is the same sum written the other way round.
			left, right = right, left
			helper = fnNamePostgresDateAdd
		}
		if helper == "" {
			out = append(out, tokens[i])
			i++
			continue
		}
		if isOpEq(tokens[i], "-") && helper == fnNamePostgresDateAdd {
			right = append([]token{opToken("-"), opToken("(")}, append(right, opToken(")"))...)
		}
		out = append(out[:start], callTokens(helper, left, right)...)
		i = rightEnd + 1
	}
	return out
}

// isDateOperand reports whether the tokens spell something this package can see
// is a date: a cast to a date or timestamp type, or a string literal in one of
// the date layouts. A bare column reference is not one, because there is no type
// to read it from.
func isDateOperand(operand []token) bool {
	sig := make([]token, 0, len(operand))
	for _, t := range operand {
		if isSignificant(t) {
			sig = append(sig, t)
		}
	}
	if len(sig) == 0 {
		return false
	}
	if len(sig) == 1 && sig[0].kind == tokString {
		_, ok := parseTime(sig[0].text)
		return ok
	}
	if !isWordEq(sig[0], "postgresql_cast") || len(sig) < 4 {
		return false
	}
	target := sig[len(sig)-2]
	if target.kind != tokString {
		return false
	}
	name, _ := parseCastTarget(target.text)
	kind, ok := lookupCastKind(PostgreSQL, name)
	return ok && (kind == castDate || kind == castTimestamp)
}

// pgNullsOrderPass appends the NULLS keyword PostgreSQL defaults to, which is
// the opposite of SQLite's: PostgreSQL sorts NULLs last for an ascending order
// and first for a descending one, and SQLite does the reverse.
//
// The difference is not only the order of the rows a caller reads. A window
// function reads position, so first_value, last_value, nth_value, lag and lead
// all answer about a different row, and those answers travel into the rest of
// the query where nothing marks them as ordering-dependent.
//
// A term that already names NULLS FIRST or NULLS LAST is left alone, and so is
// an ORDER BY inside a string literal or a quoted identifier, which never
// reaches this pass as words.
func pgNullsOrderPass(tokens []token) []token {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if !isWordEq(tokens[i], "ORDER") {
			out = append(out, tokens[i])
			i++
			continue
		}
		by := nextSig(tokens, i+1)
		if by < 0 || !isWordEq(tokens[by], "BY") {
			out = append(out, tokens[i])
			i++
			continue
		}
		out = append(out, tokens[i:by+1]...)
		i = by + 1
		end := orderByEnd(tokens, i)
		out = append(out, orderTermsWithNulls(tokens[i:end])...)
		i = end
	}
	return out
}

// orderByEnd is the index just past the ordering terms that begin at start: the
// list runs to the end of the statement or to the first word that begins the
// clause after it, at parenthesis depth zero.
func orderByEnd(tokens []token, start int) int {
	depth := 0
	for i := start; i < len(tokens); i++ {
		switch {
		case isOpEq(tokens[i], "("):
			depth++
		case isOpEq(tokens[i], ")"):
			if depth == 0 {
				return i
			}
			depth--
		case depth == 0 && isOpEq(tokens[i], ";"):
			// The statement terminator ends the ordering list; a NULLS keyword
			// appended after it would not parse.
			return i
		case depth == 0 && tokens[i].kind == tokWord && orderByEndKeywords[strings.ToUpper(tokens[i].text)]:
			return i
		}
	}
	return len(tokens)
}

// orderByEndKeywords are the words that can follow an ORDER BY list.
var orderByEndKeywords = map[string]bool{ //nolint:gochecknoglobals // a fixed table read by the pass above
	kwLimit: true, kwOffset: true, "FETCH": true, "FOR": true,
	kwUnion: true, kwIntersect: true, kwExcept: true, "WINDOW": true, "ROWS": true, "RANGE": true, "GROUPS": true,
}

// orderTermsWithNulls appends the NULLS keyword to each ordering term of the
// list that does not already carry one.
func orderTermsWithNulls(terms []token) []token {
	out := make([]token, 0, len(terms)+4)
	start := 0
	depth := 0
	flush := func(term []token) {
		if len(term) == 0 || termNamesNulls(term) {
			out = append(out, term...)
			return
		}
		// The keyword goes before the term's trailing whitespace rather than
		// after it, so the rendered clause has one space in each place.
		body := term
		for len(body) > 0 && !isSignificant(body[len(body)-1]) {
			body = body[:len(body)-1]
		}
		keyword := "LAST"
		if termIsDescending(term) {
			keyword = "FIRST"
		}
		out = append(out, body...)
		out = append(out, spaceToken(), wordToken("NULLS"), spaceToken(), wordToken(keyword))
		out = append(out, term[len(body):]...)
	}
	for i := range terms {
		switch {
		case isOpEq(terms[i], "("):
			depth++
		case isOpEq(terms[i], ")"):
			depth--
		case depth == 0 && isOpEq(terms[i], ","):
			flush(terms[start:i])
			out = append(out, terms[i])
			start = i + 1
		}
	}
	flush(terms[start:])
	return out
}

// termNamesNulls reports whether an ordering term already says where its NULLs
// go, in which case the caller has decided and nothing is appended.
func termNamesNulls(term []token) bool {
	for _, t := range term {
		if isWordEq(t, "NULLS") {
			return true
		}
	}
	return false
}

// termIsDescending reports whether an ordering term sorts downward, which is
// where PostgreSQL puts its NULLs first.
func termIsDescending(term []token) bool {
	for i := len(term) - 1; i >= 0; i-- {
		if !isSignificant(term[i]) {
			continue
		}
		return isWordEq(term[i], "DESC")
	}
	return false
}

// pgUnicodeEscapeStringPass decodes PostgreSQL's U&'...' literal, whose escape
// sequences name a code point, and its U&"..." identifier form. Neither is
// SQLite syntax, so the "U" arrived as a bare word and the caller read "no such
// column: U" about a literal they had written. An optional UESCAPE clause names
// a different escape character.
//
// The literal is three tokens here -- the word U, the "&" operator and the
// string -- because the lexer has no rule joining them; the pass reads that
// shape rather than the lexer being taught a fourth string form.
func pgUnicodeEscapeStringPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		amp := i + 1
		lit := i + 2
		if !isWordEq(tokens[i], "U") || lit >= len(tokens) || !isOpEq(tokens[amp], "&") ||
			(tokens[lit].kind != tokString && tokens[lit].kind != tokQuotedIdent) {
			out = append(out, tokens[i])
			i++
			continue
		}
		escape := '\\'
		next := lit
		if uescape := nextSig(tokens, lit+1); uescape >= 0 && isWordEq(tokens[uescape], "UESCAPE") {
			spec := nextSig(tokens, uescape+1)
			if spec < 0 || tokens[spec].kind != tokString || len([]rune(tokens[spec].text)) != 1 {
				return nil, fmt.Errorf("%w: UESCAPE takes a one-character string", ErrUnsupportedSyntax)
			}
			escape = []rune(tokens[spec].text)[0]
			next = spec
		}
		decoded, err := decodeUnicodeEscapes(tokens[lit].text, escape)
		if err != nil {
			return nil, err
		}
		out = append(out, token{kind: tokens[lit].kind, text: decoded})
		i = next + 1
	}
	return out, nil
}

// decodeUnicodeEscapes reads the code-point escapes of a U&'...' literal, with
// escape standing in for the backslash. Two escape characters in a row are the
// character itself.
func decodeUnicodeEscapes(s string, escape rune) (string, error) {
	runes := []rune(s)
	var b strings.Builder
	for i := 0; i < len(runes); i++ {
		if runes[i] != escape {
			b.WriteRune(runes[i])
			continue
		}
		switch {
		case i+1 < len(runes) && runes[i+1] == escape:
			b.WriteRune(escape)
			i++
		case i+1 < len(runes) && runes[i+1] == '+' && i+7 < len(runes):
			r, err := strconv.ParseUint(string(runes[i+2:i+8]), 16, 21)
			if err != nil {
				return "", fmt.Errorf("%w: %s is not a Unicode escape", ErrUnsupportedSyntax, string(runes[i:i+8]))
			}
			b.WriteRune(rune(r))
			i += 7
		case i+4 < len(runes):
			r, err := strconv.ParseUint(string(runes[i+1:i+5]), 16, 21)
			if err != nil {
				return "", fmt.Errorf("%w: %s is not a Unicode escape", ErrUnsupportedSyntax, string(runes[i:i+5]))
			}
			b.WriteRune(rune(r))
			i += 4
		default:
			return "", fmt.Errorf("%w: a Unicode escape needs four hexadecimal digits", ErrUnsupportedSyntax)
		}
	}
	return b.String(), nil
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
			if t.text == "~" && operandPositionBack(out) {
				// A "~" with no left operand is PostgreSQL's bitwise NOT, which
				// SQLite spells the same way. Reading it as the regex match
				// operator made "~5" a comparison missing its left side.
				out = append(out, t)
				continue
			}
			switch t.text {
			case "~~", "!~~", "~~*", "!~~*":
				// PostgreSQL's operator spellings of LIKE, NOT LIKE, ILIKE and
				// NOT ILIKE. They are not the regex operators they start with:
				// the pattern is written with LIKE wildcards, and reading it as
				// a regular expression matched something else.
				out = appendLikeOperator(out, tokens, i, t.text)
				continue
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

// appendLikeOperator appends the keyword spelling of one of PostgreSQL's LIKE
// operator aliases, so the later LIKE pass sees the form it already handles.
func appendLikeOperator(out, tokens []token, i int, op string) []token {
	if n := len(out); n > 0 && out[n-1].kind != tokWhitespace {
		out = append(out, spaceToken())
	}
	if strings.HasPrefix(op, "!") {
		out = append(out, wordToken("NOT"), spaceToken())
	}
	if strings.HasSuffix(op, "*") {
		out = append(out, wordToken("ILIKE"))
	} else {
		out = append(out, wordToken("LIKE"))
	}
	if i+1 < len(tokens) && tokens[i+1].kind != tokWhitespace {
		out = append(out, spaceToken())
	}
	return out
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
