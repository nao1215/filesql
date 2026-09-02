package lower

import "github.com/nao1215/filesql/dialect/internal/ast"

// baseRules holds the clause rules every dialect shares. A dialect embeds it
// and overrides what it has an opinion about, so a rule that is the same in
// three places is written once. The expression rules are not here: each dialect
// answers for every one of them, and a default would only hide a rule someone
// forgot to write.
type baseRules struct{}

func (baseRules) Pre(e ast.Expr) (ast.Expr, bool, error) { return e, false, nil }

func (baseRules) Core(core *ast.SelectCore) error { return coreCommon(core) }

func (baseRules) Order(term *ast.OrderTerm) error { return orderCommon(term) }

// coreCommon refuses the SELECT clauses no dialect can lower, and drops the
// ones that ask for nothing SQLite can give.
func coreCommon(core *ast.SelectCore) error {
	if len(core.DistinctOn) > 0 {
		return unsupported(core.Span,
			"DISTINCT ON is not supported; write the first row of each group with a window function")
	}
	if core.GroupByAll {
		return unsupported(core.Span,
			"GROUP BY ALL is not supported; name the grouping columns")
	}
	if core.Grouping != nil {
		return unsupported(core.Grouping.Span,
			"a grouping set is not supported; write the groupings as separate queries joined by UNION ALL")
	}
	if core.Qualify != nil {
		return unsupported(core.Span,
			"QUALIFY is not supported; wrap the query and filter the window's result in the outer WHERE")
	}
	// The modifiers say how the engine should run the query rather than what it
	// should answer, and the answer is the same without them.
	core.Modifiers = nil
	core.All = false
	return nil
}

// orderCommon refuses the ordering clauses SQLite has no form for.
func orderCommon(term *ast.OrderTerm) error {
	if term.Using != "" {
		return unsupported(term.Span,
			"ORDER BY ... USING is not supported; write ASC or DESC")
	}
	if term.Collation != "" {
		name, ok := sqliteCollation(term.Collation)
		if !ok {
			return unsupported(term.Span,
				"the collation %s is not supported; SQLite has BINARY, NOCASE and RTRIM", term.Collation)
		}
		term.Collation = name
	}
	return nil
}
