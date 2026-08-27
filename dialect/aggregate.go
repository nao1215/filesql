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
	// pair marks an aggregate over two columns: the covariances and the
	// correlation.
	pair bool
	// correlation selects CORR over the covariance the pair flag otherwise
	// computes.
	correlation bool
	// distinctCount marks an approximate distinct count, which SQLite answers
	// exactly.
	distinctCount bool
	// reject names why the aggregate has no SQLite form. A rule that carries
	// one refuses the call rather than rewriting it, so the caller reads about
	// the aggregate they wrote instead of about a missing function.
	reject string
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

// The reasons the aggregates without a SQLite form are refused. Each says what
// SQLite would do differently rather than only that it cannot, so a caller can
// see whether the difference matters to them.
const (
	objectAggReject = "SQLite's json_group_object keeps a repeated key where this dialect keeps the last value for it; " +
		"write json_group_object with a grouped subquery if the repetition is not possible in your data"
	bitAggregateReject = "SQLite has no bitwise aggregate; " +
		"write the fold with a recursive CTE, or compute it outside the query"
	regressionReject = "SQLite has no regression aggregate; " +
		"write it from sum(x), sum(y), sum(x*y), sum(x*x) and count(*), which it does have"
	orderedSetReject = "an ordered-set aggregate takes a WITHIN GROUP clause, which SQLite has no form for; " +
		"a median can be written with LIMIT and OFFSET over an ordered subquery"
)

// aggregateRules maps every spelling to its rule. The defaults differ by
// dialect: a bare STDDEV or VARIANCE is the sample estimator in PostgreSQL and
// GoogleSQL but the population one in MySQL, where STD is the usual spelling.
var aggregateRules = map[Dialect]map[string]aggregateRule{
	MySQL: {
		aggAnyValue: {rename: sqliteMin},
		// SQLite builds the same documents under its own names.
		"JSON_ARRAYAGG":  {rename: sqliteJSONArray},
		"JSON_OBJECTAGG": {reject: objectAggReject},
		"BIT_AND":        {reject: bitAggregateReject},
		"BIT_OR":         {reject: bitAggregateReject},
		"BIT_XOR":        {reject: bitAggregateReject},
		"STD":            {stat: true, root: true},
		aggStddev:        {stat: true, root: true},
		aggStddevPop:     {stat: true, root: true},
		aggStddevSamp:    {stat: true, root: true, sample: true},
		aggVariance:      {stat: true},
		aggVarPop:        {stat: true},
		aggVarSamp:       {stat: true, sample: true},
	},
	PostgreSQL: {
		"BOOL_AND": {rename: sqliteMin},
		"BOOL_OR":  {rename: sqliteMax},
		"EVERY":    {rename: sqliteMin},
		// SQLite builds the same documents under its own names.
		"JSON_AGG":         {rename: sqliteJSONArray},
		"JSONB_AGG":        {rename: sqliteJSONArray},
		"JSON_OBJECT_AGG":  {reject: objectAggReject},
		"JSONB_OBJECT_AGG": {reject: objectAggReject},
		"ARRAY_AGG":        {reject: "its result is an array and SQLite has no array type"},
		"BIT_AND":          {reject: bitAggregateReject},
		"BIT_OR":           {reject: bitAggregateReject},
		"BIT_XOR":          {reject: bitAggregateReject},
		// The correlation family, which GoogleSQL already reaches by the same
		// rules.
		// The regression family and the ordered-set aggregates, neither of
		// which SQLite can express.
		"REGR_SLOPE":      {reject: regressionReject},
		"REGR_INTERCEPT":  {reject: regressionReject},
		"REGR_R2":         {reject: regressionReject},
		"REGR_COUNT":      {reject: regressionReject},
		"REGR_AVGX":       {reject: regressionReject},
		"REGR_AVGY":       {reject: regressionReject},
		"REGR_SXX":        {reject: regressionReject},
		"REGR_SYY":        {reject: regressionReject},
		"REGR_SXY":        {reject: regressionReject},
		"PERCENTILE_CONT": {reject: orderedSetReject},
		"PERCENTILE_DISC": {reject: orderedSetReject},
		"MODE":            {reject: orderedSetReject},
		"CORR":            {pair: true, correlation: true},
		"COVAR_POP":       {pair: true},
		"COVAR_SAMP":      {pair: true, sample: true},
		aggStddev:         {stat: true, root: true, sample: true},
		aggStddevPop:      {stat: true, root: true},
		aggStddevSamp:     {stat: true, root: true, sample: true},
		aggVariance:       {stat: true, sample: true},
		aggVarPop:         {stat: true},
		aggVarSamp:        {stat: true, sample: true},
	},
	GoogleSQL: {
		"LOGICAL_AND": {rename: sqliteMin},
		"LOGICAL_OR":  {rename: sqliteMax},
		aggAnyValue:   {rename: sqliteMin},
		"COUNTIF":     {countif: true},
		// BigQuery's count is approximate and SQLite's is exact, which is a
		// correct answer to the question the approximation estimates.
		"APPROX_COUNT_DISTINCT": {distinctCount: true},
		"CORR":                  {pair: true, correlation: true},
		"COVAR_POP":             {pair: true},
		"COVAR_SAMP":            {pair: true, sample: true},
		aggStddev:               {stat: true, root: true, sample: true},
		aggStddevPop:            {stat: true, root: true},
		aggStddevSamp:           {stat: true, root: true, sample: true},
		aggVariance:             {stat: true, sample: true},
		aggVarPop:               {stat: true},
		aggVarSamp:              {stat: true, sample: true},
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
		if rule.reject != "" {
			return nil, fmt.Errorf("%w: %s is not supported: %s", ErrUnsupportedSyntax, strings.ToUpper(t.text), rule.reject)
		}
		// A rule that expands into an expression rather than a rename cannot
		// carry a window: the result is several aggregates inside arithmetic,
		// and an OVER after it belongs to none of them. Left alone, SQLite
		// reported on whichever generated function it happened to reach --
		// "sqrt() may not be used as a window function" for a standard
		// deviation -- naming a function the query does not contain.
		if rule.rename == "" {
			if over := nextSig(tokens, closeIdx+1); over >= 0 && isWordEq(tokens[over], "OVER") {
				return nil, fmt.Errorf("%w: %s cannot be used as a window function; SQLite has no aggregate to attach the window to",
					ErrUnsupportedSyntax, strings.ToUpper(t.text))
			}
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
	case rule.distinctCount:
		repl := make([]token, 0, len(arg)+5)
		repl = append(repl, wordToken("COUNT"), opToken("("), wordToken("DISTINCT"), spaceToken())
		repl = append(repl, arg...)
		return append(repl, opToken(")")), nil
	case rule.pair:
		commas := argumentCommas(arg)
		if len(commas) != 1 {
			return nil, fmt.Errorf("%w: this aggregate takes two arguments", ErrUnsupportedSyntax)
		}
		return sqliteExpr(pairStatExpr(
			render(trimSpaceTokens(arg[:commas[0]])),
			render(trimSpaceTokens(arg[commas[0]+1:])),
			rule.sample, rule.correlation,
		))
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
// A trailing ORDER BY belongs to the aggregate rather than to the separator, and
// SQLite accepts it inside group_concat, so it is carried over rather than read
// as part of the separator and refused.
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

	// The separator ends where the aggregate's own ORDER BY begins, when there is
	// one. Everything to the end of the call is the separator otherwise.
	separatorEnd := closeIdx
	orderBy := topLevelWord(tokens, open, closeIdx, "ORDER")
	if orderBy > comma {
		separatorEnd = orderBy
	}

	separator := trimSpaceTokens(tokens[comma+1 : separatorEnd])
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
	if separatorEnd != closeIdx {
		order, err := recurse(tokens[orderBy:closeIdx])
		if err != nil {
			return nil, false, err
		}
		repl = append(repl, spaceToken())
		repl = append(repl, trimSpaceTokens(order)...)
	}
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

// argumentCommas finds the commas that separate the arguments in a token slice
// that is the inside of a call, with its own parentheses already stripped --
// where topLevelCommas expects the opening parenthesis to still be there.
func argumentCommas(arg []token) []int {
	depth := 0
	var res []int
	for i, t := range arg {
		if t.kind != tokOp {
			continue
		}
		switch t.text {
		case "(":
			depth++
		case ")":
			depth--
		case ",":
			if depth == 0 {
				res = append(res, i)
			}
		}
	}
	return res
}

// pairStatExpr is the covariance or the correlation of two expressions. A row
// where either is NULL takes no part in either, so both are read through a CASE
// that answers NULL unless the pair is complete -- otherwise the sums and the
// count would be taken over different sets of rows.
func pairStatExpr(x, y string, sample, correlation bool) string {
	both := func(expr string) string {
		return fmt.Sprintf("CASE WHEN (%s) IS NOT NULL AND (%s) IS NOT NULL THEN 1.0*(%s) END", x, y, expr)
	}
	fx, fy := both(x), both(y)
	n := fmt.Sprintf("COUNT(%s)", fx)
	// The centered sum of products, which both answers are built from.
	covariance := fmt.Sprintf("(SUM((%[1]s)*(%[2]s)) - SUM(%[1]s)*SUM(%[2]s)/%[3]s)", fx, fy, n)
	if correlation {
		spread := func(f string) string {
			return fmt.Sprintf("(SUM((%[1]s)*(%[1]s)) - SUM(%[1]s)*SUM(%[1]s)/%[2]s)", f, n)
		}
		return fmt.Sprintf("(%s / sqrt((%s)*(%s)))", covariance, spread(fx), spread(fy))
	}
	divisor := n
	if sample {
		divisor = fmt.Sprintf("(%s - 1.0)", n)
	}
	return fmt.Sprintf("(%s / %s)", covariance, divisor)
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
