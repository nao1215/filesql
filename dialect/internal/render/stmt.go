package render

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
)

// stmt writes any statement.
func (w *writer) stmt(s ast.Stmt) error {
	switch n := s.(type) {
	case *ast.SelectStmt:
		return w.selectStmt(n)
	case *ast.InsertStmt:
		return w.insert(n)
	case *ast.UpdateStmt:
		return w.update(n)
	case *ast.DeleteStmt:
		return w.deleteStmt(n)
	case *ast.CreateTableStmt:
		return w.createTable(n)
	case *ast.CreateViewStmt:
		return w.createView(n)
	case *ast.CreateIndexStmt:
		return w.createIndex(n)
	case *ast.DropStmt:
		return w.drop(n)
	case *ast.AlterTableStmt:
		return w.alter(n)
	case *ast.TransactionStmt:
		return w.transaction(n)
	case *ast.ExplainStmt:
		w.word("EXPLAIN")
		if n.QueryPlan {
			w.word("QUERY PLAN")
		}
		return w.stmt(n.Stmt)
	case *ast.PragmaStmt:
		return w.pragma(n)
	case *ast.AnalyzeStmt:
		w.word("ANALYZE")
		if n.Name != nil {
			return w.tableName(n.Name)
		}
		return nil
	default:
		return unsupported(s.At(), "this statement")
	}
}

func (w *writer) selectStmt(n *ast.SelectStmt) error {
	if n.With != nil {
		if err := w.with(n.With); err != nil {
			return err
		}
	}
	if err := w.queryBody(n.Body); err != nil {
		return err
	}
	if len(n.OrderBy) > 0 {
		w.word("ORDER BY")
		if err := w.orderTerms(n.OrderBy); err != nil {
			return err
		}
	}
	return w.limit(n.Limit)
}

func (w *writer) with(n *ast.WithClause) error {
	w.word("WITH")
	if n.Recursive {
		w.word("RECURSIVE")
	}
	for i, cte := range n.CTEs {
		if i > 0 {
			w.word(",")
		}
		w.word(quoteIfNeeded(cte.Name))
		if len(cte.Columns) > 0 {
			w.word("(")
			for j, col := range cte.Columns {
				if j > 0 {
					w.word(",")
				}
				w.word(quoteIfNeeded(col))
			}
			w.word(")")
		}
		w.word("AS")
		w.word("(")
		if err := w.selectStmt(cte.Stmt); err != nil {
			return err
		}
		w.word(")")
	}
	return nil
}

func (w *writer) queryBody(body ast.QueryBody) error {
	switch n := body.(type) {
	case *ast.SelectCore:
		return w.selectCore(n)
	case *ast.SetOp:
		return w.setOp(n)
	case *ast.ValuesBody:
		return w.values(n.Rows)
	default:
		return unsupported(body.At(), "this query")
	}
}

func (w *writer) setOp(n *ast.SetOp) error {
	if err := w.setOperand(n.Left, false); err != nil {
		return err
	}
	switch n.Op {
	case ast.Union:
		w.word("UNION")
	case ast.Intersect:
		w.word("INTERSECT")
	case ast.Except:
		w.word("EXCEPT")
	}
	if n.All {
		w.word("ALL")
	}
	return w.setOperand(n.Right, true)
}

// setOperand writes one side of a set operation. SQLite evaluates a compound
// SELECT left to right and has no precedence among the operators, where the
// source dialects bind INTERSECT tighter than UNION and EXCEPT. A set operation
// on the right therefore has to be written as a subquery, or SQLite would
// regroup it: "1 UNION (2 INTERSECT 3)" would become "(1 UNION 2) INTERSECT 3".
func (w *writer) setOperand(body ast.QueryBody, right bool) error {
	if _, nested := body.(*ast.SetOp); !nested || !right {
		return w.queryBody(body)
	}
	w.word("SELECT * FROM")
	w.word("(")
	if err := w.queryBody(body); err != nil {
		return err
	}
	w.word(")")
	return nil
}

func (w *writer) selectCore(n *ast.SelectCore) error {
	if len(n.DistinctOn) > 0 {
		return unsupported(n.Span, "DISTINCT ON")
	}
	if n.GroupByAll {
		return unsupported(n.Span, "GROUP BY ALL")
	}
	if n.Grouping != nil {
		return unsupported(n.Grouping.Span, "a grouping set")
	}
	if n.Qualify != nil {
		return unsupported(n.Span, "QUALIFY")
	}
	w.word("SELECT")
	if n.Distinct {
		w.word("DISTINCT")
	}
	for i, item := range n.Items {
		if i > 0 {
			w.word(",")
		}
		mark := w.b.Len()
		if err := w.expr(item.Expr, precLowest); err != nil {
			return err
		}
		alias, quoted := item.Alias, item.AliasQuoted
		if alias == "" {
			if label := preservedLabel(item, w.b.String()[mark:]); label != "" {
				alias, quoted = label, true
			}
		}
		switch {
		case alias != "" && quoted:
			w.word("AS")
			w.word(QuoteIdent(alias))
		case alias != "":
			w.word("AS")
			w.word(quoteIfNeeded(alias))
		}
	}
	if len(n.From) > 0 {
		w.word("FROM")
		for i, table := range n.From {
			if i > 0 {
				w.word(",")
			}
			if err := w.tableExpr(table); err != nil {
				return err
			}
		}
	}
	if n.Where != nil {
		w.word("WHERE")
		if err := w.expr(n.Where, precLowest); err != nil {
			return err
		}
	}
	if len(n.GroupBy) > 0 {
		w.word("GROUP BY")
		for i, e := range n.GroupBy {
			if i > 0 {
				w.word(",")
			}
			if err := w.expr(e, precLowest); err != nil {
				return err
			}
		}
	}
	if n.Having != nil {
		w.word("HAVING")
		if err := w.expr(n.Having, precLowest); err != nil {
			return err
		}
	}
	if len(n.Windows) > 0 {
		w.word("WINDOW")
		for i, win := range n.Windows {
			if i > 0 {
				w.word(",")
			}
			w.word(quoteIfNeeded(win.Name))
			w.word("AS")
			if err := w.windowSpec(win.Spec); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *writer) windowSpec(n *ast.WindowSpec) error {
	if n.Name != "" {
		w.word(quoteIfNeeded(n.Name))
		return nil
	}
	w.word("(")
	if n.Base != "" {
		w.word(quoteIfNeeded(n.Base))
	}
	if len(n.PartitionBy) > 0 {
		w.word("PARTITION BY")
		for i, e := range n.PartitionBy {
			if i > 0 {
				w.word(",")
			}
			if err := w.expr(e, precLowest); err != nil {
				return err
			}
		}
	}
	if len(n.OrderBy) > 0 {
		w.word("ORDER BY")
		if err := w.orderTerms(n.OrderBy); err != nil {
			return err
		}
	}
	if n.Frame != nil {
		if err := w.frame(n.Frame); err != nil {
			return err
		}
	}
	w.word(")")
	return nil
}

func (w *writer) frame(n *ast.WindowFrame) error {
	switch n.Unit {
	case ast.FrameRows:
		w.word("ROWS")
	case ast.FrameRange:
		w.word("RANGE")
	case ast.FrameGroups:
		w.word("GROUPS")
	}
	if n.End != nil {
		w.word("BETWEEN")
		if err := w.frameBound(n.Start); err != nil {
			return err
		}
		w.word("AND")
		if err := w.frameBound(*n.End); err != nil {
			return err
		}
	} else if err := w.frameBound(n.Start); err != nil {
		return err
	}
	switch n.Exclude {
	case ast.ExcludeNone:
	case ast.ExcludeCurrentRow:
		w.word("EXCLUDE CURRENT ROW")
	case ast.ExcludeGroup:
		w.word("EXCLUDE GROUP")
	case ast.ExcludeTies:
		w.word("EXCLUDE TIES")
	case ast.ExcludeNoOthers:
		w.word("EXCLUDE NO OTHERS")
	}
	return nil
}

func (w *writer) frameBound(b ast.FrameBound) error {
	switch b.Kind {
	case ast.BoundUnboundedPreceding:
		w.word("UNBOUNDED PRECEDING")
	case ast.BoundUnboundedFollowing:
		w.word("UNBOUNDED FOLLOWING")
	case ast.BoundCurrentRow:
		w.word("CURRENT ROW")
	case ast.BoundPreceding:
		if err := w.expr(b.Offset, precUnary); err != nil {
			return err
		}
		w.word("PRECEDING")
	case ast.BoundFollowing:
		if err := w.expr(b.Offset, precUnary); err != nil {
			return err
		}
		w.word("FOLLOWING")
	}
	return nil
}

func (w *writer) orderTerms(terms []ast.OrderTerm) error {
	for i, term := range terms {
		if i > 0 {
			w.word(",")
		}
		if term.Using != "" {
			return unsupported(term.Span, "ORDER BY ... USING")
		}
		if err := w.expr(term.Expr, precLowest); err != nil {
			return err
		}
		if term.Collation != "" {
			w.word("COLLATE")
			w.word(term.Collation)
		}
		if term.Desc {
			w.word("DESC")
		}
		switch term.Nulls {
		case ast.NullsDefault:
		case ast.NullsFirst:
			w.word("NULLS FIRST")
		case ast.NullsLast:
			w.word("NULLS LAST")
		}
	}
	return nil
}

func (w *writer) limit(n *ast.LimitClause) error {
	if n == nil {
		return nil
	}
	if n.WithTies {
		return unsupported(n.Span, "FETCH ... WITH TIES")
	}
	if n.Count == nil && n.Offset == nil {
		return nil
	}
	w.word("LIMIT")
	if n.Count == nil {
		// SQLite has no LIMIT ALL, and an OFFSET needs a LIMIT in front of it.
		// -1 is the count SQLite documents for "every row".
		w.word("-1")
	} else if err := w.expr(n.Count, precLowest); err != nil {
		return err
	}
	if n.Offset != nil {
		w.word("OFFSET")
		if err := w.expr(n.Offset, precLowest); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) values(rows [][]ast.Expr) error {
	w.word("VALUES")
	for i, row := range rows {
		if i > 0 {
			w.word(",")
		}
		w.word("(")
		for j, e := range row {
			if j > 0 {
				w.word(",")
			}
			if err := w.expr(e, precLowest); err != nil {
				return err
			}
		}
		w.word(")")
	}
	return nil
}

func (w *writer) tableExpr(t ast.TableExpr) error {
	switch n := t.(type) {
	case *ast.TableName:
		if err := w.tableName(n); err != nil {
			return err
		}
		if n.Alias != "" {
			w.word("AS")
			w.word(quoteIfNeeded(n.Alias))
		}
		return nil
	case *ast.SubqueryTable:
		w.word("(")
		if err := w.selectStmt(n.Sub); err != nil {
			return err
		}
		w.word(")")
		if n.Alias != "" {
			w.word("AS")
			w.word(quoteIfNeeded(n.Alias))
		}
		return nil
	case *ast.FuncTable:
		if err := w.call(n.Call); err != nil {
			return err
		}
		if n.Alias != "" {
			w.word("AS")
			w.word(quoteIfNeeded(n.Alias))
		}
		return nil
	case *ast.ParenTable:
		w.word("(")
		if err := w.tableExpr(n.Inner); err != nil {
			return err
		}
		w.word(")")
		return nil
	case *ast.JoinTable:
		return w.join(n)
	default:
		return unsupported(t.At(), "this table reference")
	}
}

func (w *writer) join(n *ast.JoinTable) error {
	if err := w.tableExpr(n.Left); err != nil {
		return err
	}
	if n.Natural {
		w.word("NATURAL")
	}
	switch n.Type {
	case ast.JoinInner:
		w.word("JOIN")
	case ast.JoinLeft:
		w.word("LEFT JOIN")
	case ast.JoinRight:
		w.word("RIGHT JOIN")
	case ast.JoinFull:
		w.word("FULL JOIN")
	case ast.JoinCross:
		w.word("CROSS JOIN")
	}
	if err := w.tableExpr(n.Right); err != nil {
		return err
	}
	switch {
	case n.On != nil:
		w.word("ON")
		return w.expr(n.On, precLowest)
	case len(n.Using) > 0:
		w.word("USING")
		w.word("(")
		for i, name := range n.Using {
			if i > 0 {
				w.word(",")
			}
			w.word(quoteIfNeeded(name))
		}
		w.word(")")
	}
	return nil
}

func (w *writer) tableName(n *ast.TableName) error {
	for i, part := range n.Parts {
		if i > 0 {
			w.dot()
		}
		w.name(part)
	}
	return nil
}
