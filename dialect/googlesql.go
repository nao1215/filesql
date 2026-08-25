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
//	G-6  FORMAT(fmt, ...)                     -> googlesql_format(fmt, ...)
//	G-7  DATE_ADD/DATE_SUB/TIMESTAMP_ADD/SUB  -> datetime(x, '±n unit')
//	G-8  DATE_DIFF/TIMESTAMP_DIFF(a, b, UNIT) -> DATE_DIFF(a, b, 'unit')
//	G-9  QUALIFY / UNNEST / ARRAY / STRUCT / SELECT * EXCEPT / REPLACE
//	     -> ErrUnsupportedSyntax
//	G-11 a / b                                -> googlesql_divide(a, b)
//	G-12 x LIKE p                             -> like_sensitive(p, x)
//	G-13 DATE_TRUNC(x, PART)                  -> date_trunc_part(x, 'part')
//	G-14 CURRENT_DATE() and friends           -> CURRENT_DATE
//	G-15 COUNTIF / LOGICAL_AND / STDDEV       -> SQLite aggregate expressions
//	G-16 UNION DISTINCT                       -> UNION
//	G-17 JSON_VALUE / BYTE_LENGTH / CHAR_LENGTH
//	G-18 STRING_AGG(DISTINCT x, ',')           -> group_concat(DISTINCT x)
//	G-19 [1,2,3] / x[OFFSET(n)]               -> ErrUnsupportedSyntax
//	G-20 SAFE.f(args)                         -> safe_f(args)
//	G-21 UPPER(x) / LOWER(x)                  -> unicode_upper / unicode_lower
//	G-22 a % b, MOD(a, b)                     -> googlesql_mod, which raises on a
//	                                             zero divisor
//	G-23 TRUNC(x, n)                          -> trunc_scale(x, n); DIV is a
//	                                             helper of its own name
//	G-24 LEFT(x, n) / RIGHT(x, n)             -> googlesql_left / googlesql_right
//	G-25 LOG(x) / LOG(x, base)                -> ln(x) / log(base, x)
func rewriteGoogleSQL(tokens []token) ([]token, error) {
	if err := checkUnsupportedGoogleSQL(tokens); err != nil {
		return nil, err
	}
	// G-20: fold BigQuery's "SAFE." call prefix into the underscore name the
	// rest of the pass already knows, before any call is looked at.
	tokens, err := safePrefixPass(tokens)
	if err != nil {
		return nil, err
	}
	out, err := googlesqlCallPass(tokens)
	if err != nil {
		return nil, err
	}
	// G-11: GoogleSQL division always yields FLOAT64, so 5/2 is 2.5 rather than
	// the 2 SQLite gives for two integers.
	out, err = binaryChainOperatorPass(out, "/", "googlesql_divide")
	if err != nil {
		return nil, err
	}
	// G-22: GoogleSQL spells the remainder MOD() and has no "%" of its own, so
	// the operator is this package's extension and follows MOD, which raises on
	// a zero divisor where SQLite answers NULL.
	out, err = binaryChainOperatorPass(out, "%", "googlesql_mod")
	if err != nil {
		return nil, err
	}
	// G-12: GoogleSQL LIKE is case-sensitive; SQLite's folds ASCII case.
	out, err = likePass(out, "LIKE", "like_sensitive")
	if err != nil {
		return nil, err
	}
	// G-15: COUNTIF, LOGICAL_AND/OR, and the variance family have no SQLite
	// aggregate.
	// G-16: SQLite rejects "UNION DISTINCT"; its plain UNION already
	// deduplicates.
	out = unionDistinctPass(currentValueParenPass(typePrefixedLiteralPass(out)))
	return aggregatePass(out, GoogleSQL)
}

// safeFunctions are the functions a "SAFE." prefix can be honored for: those
// with a safe_ helper registered for them, named the same lowercased. The prefix
// means "return NULL rather than raise", and only a helper written for that can
// promise it. The order is the message's, so it does not change between runs.
var safeFunctions = []string{"ADD", "DIVIDE", "MULTIPLY", "NEGATE", "SUBTRACT"}

// safeHelperFor returns the helper a "SAFE." prefix on name maps to, and
// whether there is one.
func safeHelperFor(name string) (string, bool) {
	upper := strings.ToUpper(name)
	for _, fn := range safeFunctions {
		if fn == upper {
			return "safe_" + strings.ToLower(fn), true
		}
	}
	return "", false
}

// safePrefixPass implements G-20: SAFE.f(args) -> safe_f(args).
//
// BigQuery's own documentation writes the safe functions this way, and only a
// few of them have an underscore name at all, so the prefix is the general
// form. Left alone it reaches SQLite as a qualified name, which reports on the
// "(" that follows rather than on the prefix, and says nothing about the
// dialect.
//
// A prefix on a function with no safe form is refused rather than dropped:
// dropping it would answer the query with the plain function, which raises
// where the caller asked for a NULL.
func safePrefixPass(tokens []token) ([]token, error) {
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		name, end, ok := matchSafePrefix(tokens, i)
		if !ok {
			out = append(out, tokens[i])
			i++
			continue
		}
		helper, supported := safeHelperFor(name)
		if !supported {
			return nil, fmt.Errorf("%w: SAFE.%s is not supported; the SAFE. prefix works with %s",
				ErrUnsupportedSyntax, strings.ToUpper(name), strings.Join(safeFunctions, ", "))
		}
		out = append(out, wordToken(helper))
		i = end + 1
	}
	return out, nil
}

// matchSafePrefix reports whether tokens[i] starts a "SAFE . name (" sequence,
// returning the function name and the index of the name token.
func matchSafePrefix(tokens []token, i int) (string, int, bool) {
	if !isWordEq(tokens[i], "SAFE") {
		return "", 0, false
	}
	dot := nextSig(tokens, i+1)
	if dot < 0 || !isOpEq(tokens[dot], ".") {
		return "", 0, false
	}
	name := nextSig(tokens, dot+1)
	if name < 0 || tokens[name].kind != tokWord {
		return "", 0, false
	}
	// The "(" is what makes this a call rather than a qualified column.
	if open := nextSig(tokens, name+1); open < 0 || !isOpEq(tokens[open], "(") {
		return "", 0, false
	}
	return tokens[name].text, name, true
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
		// The type parameters spell these out — ARRAY<INT64>, STRUCT<a INT64> —
		// and the parenthesis spells the same thing without them: STRUCT(1 AS a)
		// is the ordinary way to write a struct, and ARRAY(SELECT ...) the
		// ordinary way to build an array. Only the typed form was refused, so the
		// spelling a caller is most likely to write reached SQLite as a call and
		// failed on the AS inside it, or on a function name SQLite does not have.
		if isWordEq(t, "ARRAY") || isWordEq(t, "STRUCT") {
			if lt := nextSig(tokens, i+1); lt >= 0 && (isOpEq(tokens[lt], "<") || isOpEq(tokens[lt], "(")) {
				return fmt.Errorf("%w: %s is not supported; SQLite has no %s type",
					ErrUnsupportedSyntax, strings.ToUpper(t.text), strings.ToLower(t.text))
			}
		}
		// G-19: SQLite has no array type, and "[" is its identifier quoting, so an
		// array literal or subscript reaches the planner as a name: the error was
		// "no such column: 1,2,3", about a column the query never wrote. Under
		// GoogleSQL a "[" is never quoting — backticks are — so every one of them
		// is an array.
		if isOpEq(t, "[") {
			return fmt.Errorf("%w: arrays are not supported; SQLite has no array type", ErrUnsupportedSyntax)
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
	return walkCalls(tokens, googlesqlRewriteCall)
}

func googlesqlRewriteCall(tokens []token, nameIdx, open, closeIdx int) ([]token, bool, error) {
	switch strings.ToUpper(tokens[nameIdx].text) {
	case fnNameExtract:
		return rewriteExtractCall(tokens, open, closeIdx, "googlesql_date_part", googlesqlCallPass)
	case fnNameCast:
		return rewriteCastCall(tokens, open, closeIdx, GoogleSQL, "googlesql_cast", googlesqlCallPass)
	case "SAFE_CAST":
		return rewriteSafeCast(tokens, open, closeIdx)
	case fnNameUpper, fnNameLower:
		return rewriteRenameCall(tokens, open, closeIdx, unicodeCaseHelper(tokens[nameIdx].text), googlesqlCallPass)
	case "FORMAT":
		// The verbs GoogleSQL shares with printf are printed the same way, but %t
		// and %T are its own and SQLite's printf answers NULL for a format string
		// holding a verb it does not know.
		return rewriteRenameCall(tokens, open, closeIdx, "googlesql_format", googlesqlCallPass)
	case "LOG":
		return rewriteGoogleSQLLog(tokens, open, closeIdx)
	case "LEFT", "RIGHT":
		// GoogleSQL raises for a negative length where PostgreSQL, whose helper
		// carries the shared name, trims the far end.
		return rewriteRenameCall(tokens, open, closeIdx, "googlesql_"+strings.ToLower(tokens[nameIdx].text), googlesqlCallPass)
	case "CONCAT":
		// GoogleSQL CONCAT returns NULL when any argument is NULL, like MySQL and
		// unlike SQLite's own concat(), which treats a NULL as an empty string.
		return rewriteRenameCall(tokens, open, closeIdx, "strict_concat", googlesqlCallPass)
	case fnNameMod:
		// GoogleSQL raises on a zero divisor where SQLite answers NULL.
		return rewriteRenameCall(tokens, open, closeIdx, "googlesql_mod", googlesqlCallPass)
	case fnNameTrunc:
		return rewriteTruncScaleCall(tokens, open, closeIdx, googlesqlCallPass)
	case "DATE_ADD", "TIMESTAMP_ADD":
		return rewriteDateArith(tokens, open, closeIdx, "+", googlesqlCallPass)
	case "DATE_SUB", "TIMESTAMP_SUB":
		return rewriteDateArith(tokens, open, closeIdx, "-", googlesqlCallPass)
	case "DATE_DIFF", "TIMESTAMP_DIFF":
		return rewriteDateDiff(tokens, nameIdx, open, closeIdx)
	case "JSON_VALUE", "JSON_EXTRACT_SCALAR":
		return rewriteRenameCall(tokens, open, closeIdx, "json_extract", googlesqlCallPass)
	case "JSON_QUERY":
		return rewriteJSONQuery(tokens, open, closeIdx, googlesqlCallPass)
	case fnNameSubstring, fnNameSubstr:
		return rewriteSubstringCall(tokens, open, closeIdx, "googlesql_substr", googlesqlCallPass)
	case fnNameRound:
		return rewriteRoundCall(tokens, open, closeIdx, googlesqlCallPass)
	case "BYTE_LENGTH":
		return rewriteRenameCall(tokens, open, closeIdx, "octet_length", googlesqlCallPass)
	case fnNameCharLen, fnNameCharLen2:
		return rewriteRenameCall(tokens, open, closeIdx, "length", googlesqlCallPass)
	case "DATE_TRUNC", "TIMESTAMP_TRUNC", "DATETIME_TRUNC":
		return rewriteTruncCall(tokens, open, closeIdx, googlesqlCallPass)
	case fnNameStringAgg:
		// SQLite has string_agg as an alias of group_concat, so the plain form
		// runs as written and only the DISTINCT one needs rewriting.
		return rewriteStringAggDistinct(tokens, open, closeIdx, googlesqlCallPass)
	default:
		return nil, false, nil
	}
}

// rewriteGoogleSQLLog implements G-25: GoogleSQL LOG(x) is a synonym of LN(x),
// where SQLite's log(x) is the base-ten logarithm, and GoogleSQL's LOG(x, base)
// writes its base second where SQLite's log(base, x) writes it first.
func rewriteGoogleSQLLog(tokens []token, open, closeIdx int) ([]token, bool, error) {
	commas := topLevelCommas(tokens, open, closeIdx)
	switch len(commas) {
	case 0:
		if callArity(tokens, open, closeIdx) != 1 {
			return nil, false, nil
		}
		return rewriteRenameCall(tokens, open, closeIdx, "ln", googlesqlCallPass)
	case 1:
		value, err := googlesqlCallPass(tokens[open+1 : commas[0]])
		if err != nil {
			return nil, false, err
		}
		base, err := googlesqlCallPass(tokens[commas[0]+1 : closeIdx])
		if err != nil {
			return nil, false, err
		}
		value, base = trimSpaceTokens(value), trimSpaceTokens(base)
		repl := make([]token, 0, len(value)+len(base)+5)
		repl = append(repl, wordToken("log"), opToken("("))
		repl = append(repl, base...)
		repl = append(repl, opToken(","), spaceToken())
		repl = append(repl, value...)
		repl = append(repl, opToken(")"))
		return repl, true, nil
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
