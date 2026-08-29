package lower

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// This file lowers the aggregates the source dialects have and SQLite does not.
// They cannot be filled in with a user-defined function the way the scalars
// are, because the driver has no aggregate registration hook, so each becomes
// an equivalent SQLite expression instead.

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

	sqliteMin       = "MIN"
	sqliteMax       = "MAX"
	sqliteJSONArray = "json_group_array"
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
	arrayAggReject = "its result is an array and SQLite has no array type"
)

// aggregateRules maps every spelling to its rule. The defaults differ by
// dialect: a bare STDDEV or VARIANCE is the sample estimator in PostgreSQL and
// GoogleSQL but the population one in MySQL, where STD is the usual spelling.
var aggregateRules = map[dialects.Dialect]map[string]aggregateRule{ //nolint:gochecknoglobals // a fixed table
	dialects.MySQL: {
		aggAnyValue:      {rename: sqliteMin},
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
	dialects.PostgreSQL: {
		"BOOL_AND":         {rename: sqliteMin},
		"BOOL_OR":          {rename: sqliteMax},
		"EVERY":            {rename: sqliteMin},
		"JSON_AGG":         {rename: sqliteJSONArray},
		"JSONB_AGG":        {rename: sqliteJSONArray},
		"JSON_OBJECT_AGG":  {reject: objectAggReject},
		"JSONB_OBJECT_AGG": {reject: objectAggReject},
		"ARRAY_AGG":        {reject: arrayAggReject},
		"BIT_AND":          {reject: bitAggregateReject},
		"BIT_OR":           {reject: bitAggregateReject},
		"BIT_XOR":          {reject: bitAggregateReject},
		"REGR_SLOPE":       {reject: regressionReject},
		"REGR_INTERCEPT":   {reject: regressionReject},
		"REGR_R2":          {reject: regressionReject},
		"REGR_COUNT":       {reject: regressionReject},
		"REGR_AVGX":        {reject: regressionReject},
		"REGR_AVGY":        {reject: regressionReject},
		"REGR_SXX":         {reject: regressionReject},
		"REGR_SYY":         {reject: regressionReject},
		"REGR_SXY":         {reject: regressionReject},
		"PERCENTILE_CONT":  {reject: orderedSetReject},
		"PERCENTILE_DISC":  {reject: orderedSetReject},
		"MODE":             {reject: orderedSetReject},
		"CORR":             {pair: true, correlation: true},
		"COVAR_POP":        {pair: true},
		"COVAR_SAMP":       {pair: true, sample: true},
		aggStddev:          {stat: true, root: true, sample: true},
		aggStddevPop:       {stat: true, root: true},
		aggStddevSamp:      {stat: true, root: true, sample: true},
		aggVariance:        {stat: true, sample: true},
		aggVarPop:          {stat: true},
		aggVarSamp:         {stat: true, sample: true},
	},
	dialects.GoogleSQL: {
		"LOGICAL_AND":           {rename: sqliteMin},
		"LOGICAL_OR":            {rename: sqliteMax},
		aggAnyValue:             {rename: sqliteMin},
		"COUNTIF":               {countif: true},
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

// aggregate lowers a call that names one of the dialect's aggregates. It
// reports whether it handled the call.
func (l *lowerer) aggregate(call *ast.FuncCall) (ast.Expr, bool, error) {
	rules := aggregateRules[l.rules.Dialect()]
	if rules == nil || len(call.Name) != 1 {
		return nil, false, nil
	}
	name := strings.ToUpper(call.Name[0].Name)
	rule, ok := rules[name]
	if !ok {
		return nil, false, nil
	}
	if rule.reject != "" {
		return nil, false, unsupported(call.Span, "%s is not supported: %s", name, rule.reject)
	}
	if rule.rename != "" {
		return rename(call, rule.rename), true, nil
	}
	// A rule that expands into an expression rather than a rename cannot carry
	// a window: the result is several aggregates inside arithmetic, and an OVER
	// after it belongs to none of them. Left alone, SQLite reported on
	// whichever generated function it happened to reach -- "sqrt() may not be
	// used as a window function" for a standard deviation -- naming a function
	// the query does not contain.
	if call.Over != nil {
		return nil, false, unsupported(call.Span,
			"%s cannot be used as a window function; SQLite has no aggregate to attach the window to", name)
	}
	// The same reasoning holds for the clauses that narrow which rows an
	// aggregate sees. The expansion is several aggregates inside arithmetic,
	// and a FILTER or a DISTINCT written once would have to be repeated on each
	// of them to mean the same thing; dropped, it answers over rows the caller
	// excluded and says nothing about it.
	if call.Filter != nil {
		return nil, false, unsupported(call.Span,
			"%s cannot take a FILTER clause; it becomes several aggregates, and the filter belongs to none of them", name)
	}
	if call.Distinct {
		return nil, false, unsupported(call.Span,
			"%s cannot take DISTINCT; it becomes several aggregates, and the distinctness belongs to none of them", name)
	}
	switch {
	case rule.distinctCount:
		call.Distinct = true
		return rename(call, "COUNT"), true, nil
	case rule.countif:
		if len(call.Args) != 1 {
			return nil, false, unsupported(call.Span, "COUNTIF takes one argument")
		}
		return countifExpr(call.Args[0], call.Span), true, nil
	case rule.stat:
		if len(call.Args) != 1 {
			return nil, false, unsupported(call.Span, "%s takes one argument", name)
		}
		return varianceExpr(call.Args[0], rule.sample, rule.root, call.Span), true, nil
	case rule.pair:
		if len(call.Args) != 2 {
			return nil, false, unsupported(call.Span, "this aggregate takes two arguments")
		}
		return pairStatExpr(call.Args[0], call.Args[1], rule.sample, rule.correlation, call.Span), true, nil
	default:
		return nil, false, unsupported(call.Span, "no rewrite for this aggregate")
	}
}

// The expansions below build trees rather than SQL text, and reuse the argument
// node in more than one place. Nothing rewrites a tree after lowering, so the
// sharing is only a saving; a copy would be identical.

// countifExpr counts the rows where the argument is true. It is 0 rather than
// NULL over no rows, because it counts rather than sums.
func countifExpr(arg ast.Expr, span ast.Span) ast.Expr {
	when := &ast.CaseExpr{
		Whens: []ast.WhenClause{{
			Cond:   paren(arg),
			Result: number(1, span),
			Span:   span,
		}},
		Else: number(0, span),
		Span: span,
	}
	return helper("IFNULL", span, helper("SUM", span, when), number(0, span))
}

// varianceExpr builds the variance of an expression from the sums SQLite does
// have. The 1.0 factors keep the arithmetic in floating point, and dividing by
// the (COUNT - 1) of a single row yields NULL, which is what the source
// dialects return for a sample estimator over one value.
func varianceExpr(arg ast.Expr, sample, root bool, span ast.Span) ast.Expr {
	f := floatOf(arg, span)
	count := helper("COUNT", span, arg)
	divisor := ast.Expr(count)
	if sample {
		divisor = paren(binary(count, ast.Sub, floatLiteral("1.0", span), span))
	}
	sumSquares := helper("SUM", span, binary(f, ast.Mul, paren(arg), span))
	sumValues := helper("SUM", span, f)
	centered := paren(binary(
		sumSquares,
		ast.Sub,
		binary(binary(sumValues, ast.Mul, sumValues, span), ast.Div, count, span),
		span,
	))
	variance := paren(binary(centered, ast.Div, divisor, span))
	if root {
		return helper("sqrt", span, variance)
	}
	return variance
}

// pairStatExpr is the covariance or the correlation of two expressions. A row
// where either is NULL takes no part in either, so both are read through a CASE
// that answers NULL unless the pair is complete -- otherwise the sums and the
// count would be taken over different sets of rows.
func pairStatExpr(x, y ast.Expr, sample, correlation bool, span ast.Span) ast.Expr {
	both := func(e ast.Expr) ast.Expr {
		return &ast.CaseExpr{
			Whens: []ast.WhenClause{{
				Cond: binary(
					isNull(paren(x), true, span),
					ast.And,
					isNull(paren(y), true, span),
					span,
				),
				Result: floatOf(e, span),
				Span:   span,
			}},
			Span: span,
		}
	}
	fx, fy := both(x), both(y)
	n := helper("COUNT", span, fx)
	covariance := paren(binary(
		helper("SUM", span, binary(paren(fx), ast.Mul, paren(fy), span)),
		ast.Sub,
		binary(binary(helper("SUM", span, fx), ast.Mul, helper("SUM", span, fy), span), ast.Div, n, span),
		span,
	))
	if correlation {
		spread := func(f ast.Expr) ast.Expr {
			return paren(binary(
				helper("SUM", span, binary(paren(f), ast.Mul, paren(f), span)),
				ast.Sub,
				binary(binary(helper("SUM", span, f), ast.Mul, helper("SUM", span, f), span), ast.Div, n, span),
				span,
			))
		}
		return paren(binary(
			covariance,
			ast.Div,
			helper("sqrt", span, paren(binary(spread(fx), ast.Mul, spread(fy), span))),
			span,
		))
	}
	divisor := ast.Expr(n)
	if sample {
		divisor = paren(binary(n, ast.Sub, floatLiteral("1.0", span), span))
	}
	return paren(binary(covariance, ast.Div, divisor, span))
}

// floatOf multiplies an expression by 1.0, which is how the arithmetic above
// stays in floating point when the column holds integers.
func floatOf(e ast.Expr, span ast.Span) ast.Expr {
	return binary(floatLiteral("1.0", span), ast.Mul, paren(e), span)
}

// floatLiteral builds a real constant.
func floatLiteral(value string, span ast.Span) ast.Expr {
	return &ast.Literal{Kind: ast.LitNumber, Value: value, Span: span}
}
