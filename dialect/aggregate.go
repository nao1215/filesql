package dialect

import (
	"fmt"
	"strings"
)

// This file translates the aggregates the source dialects have and SQLite does
// not. They cannot be filled in with a user-defined function the way the scalars
// are, because the driver has no aggregate registration hook, so each is
// rewritten into an equivalent SQLite expression instead.

// aggregateRule describes how one source-dialect aggregate is expressed in
// SQLite.
type aggregateRule struct {
	// rename is the SQLite aggregate to call instead, when a rename is enough.
	// Booleans are stored as 0 and 1, so MIN is a logical AND over them and MAX
	// a logical OR, and both skip NULLs the way the source aggregates do.
	rename string
	// countif marks COUNTIF, which counts the rows where its argument is true.
	countif bool
	// stat marks the variance and standard-deviation family.
	stat bool
	// sample selects the sample estimator (divide by n-1) over the population
	// one (divide by n).
	sample bool
	// root marks a standard deviation, which is the square root of the variance.
	root bool
}

// The aggregate names shared by more than one dialect's table.
const (
	aggStddev     = "STDDEV"
	aggStddevPop  = "STDDEV_POP"
	aggStddevSamp = "STDDEV_SAMP"
	aggVariance   = "VARIANCE"
	aggVarPop     = "VAR_POP"
	aggVarSamp    = "VAR_SAMP"
	aggAnyValue   = "ANY_VALUE"

	// SQLite spellings the rename rules target. Booleans are stored as 0 and 1,
	// so MIN is a logical AND over them and MAX a logical OR.
	sqliteMin = "MIN"
	sqliteMax = "MAX"
)

// aggregateRules maps every spelling to its rule. The defaults differ by
// dialect: a bare STDDEV or VARIANCE is the sample estimator in PostgreSQL and
// GoogleSQL but the population one in MySQL, where STD is the usual spelling.
var aggregateRules = map[Dialect]map[string]aggregateRule{
	MySQL: {
		aggAnyValue:   {rename: sqliteMin},
		"STD":         {stat: true, root: true},
		aggStddev:     {stat: true, root: true},
		aggStddevPop:  {stat: true, root: true},
		aggStddevSamp: {stat: true, root: true, sample: true},
		aggVariance:   {stat: true},
		aggVarPop:     {stat: true},
		aggVarSamp:    {stat: true, sample: true},
	},
	PostgreSQL: {
		"BOOL_AND":    {rename: sqliteMin},
		"BOOL_OR":     {rename: sqliteMax},
		"EVERY":       {rename: sqliteMin},
		aggStddev:     {stat: true, root: true, sample: true},
		aggStddevPop:  {stat: true, root: true},
		aggStddevSamp: {stat: true, root: true, sample: true},
		aggVariance:   {stat: true, sample: true},
		aggVarPop:     {stat: true},
		aggVarSamp:    {stat: true, sample: true},
	},
	GoogleSQL: {
		"LOGICAL_AND": {rename: sqliteMin},
		"LOGICAL_OR":  {rename: sqliteMax},
		aggAnyValue:   {rename: sqliteMin},
		"COUNTIF":     {countif: true},
		aggStddev:     {stat: true, root: true, sample: true},
		aggStddevPop:  {stat: true, root: true},
		aggStddevSamp: {stat: true, root: true, sample: true},
		aggVariance:   {stat: true, sample: true},
		aggVarPop:     {stat: true},
		aggVarSamp:    {stat: true, sample: true},
	},
}

// aggregatePass rewrites the aggregates listed for d. It runs after the operator
// passes so the SQL it emits is already in SQLite terms and is not rewritten
// again; the arguments it copies have been translated by the earlier passes.
func aggregatePass(tokens []token, d Dialect) ([]token, error) {
	rules := aggregateRules[d]
	if rules == nil {
		return tokens, nil
	}
	out := make([]token, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		t := tokens[i]
		if t.kind != tokWord {
			out = append(out, t)
			i++
			continue
		}
		rule, ok := rules[strings.ToUpper(t.text)]
		open := nextSig(tokens, i+1)
		if !ok || open < 0 || !isOpEq(tokens[open], "(") {
			out = append(out, t)
			i++
			continue
		}
		closeIdx := matchParen(tokens, open)
		if closeIdx < 0 {
			return nil, fmt.Errorf("%w: unbalanced parentheses after %s", ErrInvalidSyntax, t.text)
		}
		// Rewrite the argument first so a nested aggregate is handled too.
		arg, err := aggregatePass(tokens[open+1:closeIdx], d)
		if err != nil {
			return nil, err
		}
		repl, err := applyAggregateRule(rule, trimSpaceTokens(arg))
		if err != nil {
			return nil, err
		}
		out = append(out, repl...)
		i = closeIdx + 1
	}
	return out, nil
}

func applyAggregateRule(rule aggregateRule, arg []token) ([]token, error) {
	switch {
	case rule.rename != "":
		repl := make([]token, 0, len(arg)+3)
		repl = append(repl, wordToken(rule.rename), opToken("("))
		repl = append(repl, arg...)
		return append(repl, opToken(")")), nil
	case rule.countif:
		// COUNTIF counts rows, so it is 0 rather than NULL over no rows.
		return sqliteExpr(fmt.Sprintf("IFNULL(SUM(CASE WHEN (%s) THEN 1 ELSE 0 END), 0)", render(arg)))
	case rule.stat:
		return sqliteExpr(varianceExpr(render(arg), rule.sample, rule.root))
	default:
		return nil, fmt.Errorf("%w: no rewrite for this aggregate", ErrUnsupportedSyntax)
	}
}

// sqliteDefaultSeparator is what SQLite joins with when group_concat is given no
// separator, which is what makes dropping an explicit "," a translation rather
// than a change of answer.
const sqliteDefaultSeparator = ","

// rewriteStringAggDistinct rewrites STRING_AGG(DISTINCT x, s), which PostgreSQL
// and GoogleSQL both accept and SQLite cannot express as written: its DISTINCT
// aggregates take exactly one argument, so the separator has nowhere to go.
//
// Left alone the call reached the engine and failed with "DISTINCT aggregates
// must have exactly one argument", which describes SQLite's parser rather than
// the query the caller wrote — and STRING_AGG was neither translated, rejected,
// nor runnable, so it belonged to none of the three classes the dialect contract
// defines.
//
// A separator of "," is dropped, because that is already what group_concat joins
// with, so the answer is unchanged. Any other separator is refused by name.
// Joining with a comma regardless would answer a question nobody asked, which is
// the one thing a translation must not do.
//
// It reports false for a call it does not handle — no DISTINCT, or no separator —
// leaving the caller's own rewrite to run.
func rewriteStringAggDistinct(tokens []token, open, closeIdx int, recurse callRecurser) ([]token, bool, error) {
	distinct := nextSig(tokens, open+1)
	if distinct < 0 || !isWordEq(tokens[distinct], "DISTINCT") {
		return nil, false, nil
	}
	comma := topLevelComma(tokens, open, closeIdx)
	if comma < 0 {
		return nil, false, nil
	}

	separator := trimSpaceTokens(tokens[comma+1 : closeIdx])
	if len(separator) != 1 || separator[0].kind != tokString || separator[0].text != sqliteDefaultSeparator {
		return nil, false, fmt.Errorf(
			"%w: STRING_AGG cannot combine DISTINCT with a separator other than ',' on the SQLite backend; drop DISTINCT, or use ','",
			ErrUnsupportedSyntax)
	}

	value, err := recurse(tokens[distinct+1 : comma])
	if err != nil {
		return nil, false, err
	}
	repl := []token{wordToken("group_concat"), opToken("("), wordToken("DISTINCT"), spaceToken()}
	repl = append(repl, trimSpaceTokens(value)...)
	return append(repl, opToken(")")), true, nil
}

// varianceExpr builds the variance of expr from the sums SQLite does have. The
// 1.0 factors keep the arithmetic in floating point, and dividing by the
// (COUNT - 1) of a single row yields NULL, which is what the source dialects
// return for a sample estimator over one value.
func varianceExpr(expr string, sample, root bool) string {
	divisor := fmt.Sprintf("COUNT(%s)", expr)
	if sample {
		divisor = fmt.Sprintf("(COUNT(%s) - 1.0)", expr)
	}
	variance := fmt.Sprintf(
		"((SUM(1.0*(%[1]s)*(%[1]s)) - SUM(1.0*(%[1]s))*SUM(1.0*(%[1]s))/COUNT(%[1]s)) / %[2]s)",
		expr, divisor,
	)
	if root {
		return "sqrt(" + variance + ")"
	}
	return variance
}

// sqliteExpr tokenizes SQL this package generates. It reads the text with
// SQLite's own lexical rules, which is what render emits: double-quoted
// identifiers and single-quoted strings. The input is built here, not supplied
// by a caller, so a lexing failure means a bug in the generator.
func sqliteExpr(sql string) ([]token, error) {
	tokens, err := tokenize(sql, lexConfig{identDoubleQuote: true})
	if err != nil {
		return nil, fmt.Errorf("dialect: generated expression failed to tokenize: %w", err)
	}
	return tokens, nil
}
