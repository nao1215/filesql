package lower

import (
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// stmt lowers a statement and everything under it.
func (l *lowerer) stmt(s ast.Stmt) (ast.Stmt, error) {
	switch n := s.(type) {
	case *ast.SelectStmt:
		return l.selectStmt(n)
	case *ast.InsertStmt:
		return l.insert(n)
	case *ast.UpdateStmt:
		return l.update(n)
	case *ast.DeleteStmt:
		return l.deleteStmt(n)
	case *ast.CreateTableStmt:
		return l.createTable(n)
	case *ast.CreateViewStmt:
		if n.Select != nil {
			sub, err := l.selectStmt(n.Select)
			if err != nil {
				return nil, err
			}
			n.Select = sub
		}
		return n, nil
	case *ast.CreateIndexStmt:
		// An index's columns are lowered as expressions rather than as ordering
		// terms: the NULLS clause a dialect's sort order implies belongs to a
		// query's ORDER BY, and writing it into an index would build a
		// different index.
		for i := range n.Columns {
			e, err := l.expr(n.Columns[i].Expr)
			if err != nil {
				return nil, err
			}
			n.Columns[i].Expr = e
		}
		if n.Where != nil {
			where, err := l.expr(n.Where)
			if err != nil {
				return nil, err
			}
			n.Where = where
		}
		return n, nil
	case *ast.ExplainStmt:
		inner, err := l.stmt(n.Stmt)
		if err != nil {
			return nil, err
		}
		n.Stmt = inner
		return n, nil
	case *ast.AlterTableStmt:
		if n.Column != nil {
			if err := l.columnDef(n.Column); err != nil {
				return nil, err
			}
		}
		return n, nil

	case *ast.DropStmt, *ast.TransactionStmt, *ast.PragmaStmt, *ast.AnalyzeStmt:
		return s, nil
	default:
		return s, nil
	}
}

func (l *lowerer) selectStmt(n *ast.SelectStmt) (*ast.SelectStmt, error) {
	if n.With != nil {
		for i := range n.With.CTEs {
			if n.With.CTEs[i].Stmt == nil {
				continue
			}
			sub, err := l.selectStmt(n.With.CTEs[i].Stmt)
			if err != nil {
				return nil, err
			}
			n.With.CTEs[i].Stmt = sub
		}
	}
	body, err := l.queryBody(n.Body)
	if err != nil {
		return nil, err
	}
	n.Body = body
	for i := range n.OrderBy {
		if err := l.orderTerm(&n.OrderBy[i]); err != nil {
			return nil, err
		}
	}
	if n.Limit != nil {
		if err := l.limit(n.Limit); err != nil {
			return nil, err
		}
	}
	return n, nil
}

func (l *lowerer) limit(n *ast.LimitClause) error {
	if n.Count != nil {
		count, err := l.expr(n.Count)
		if err != nil {
			return err
		}
		n.Count = count
	}
	if n.Offset != nil {
		offset, err := l.expr(n.Offset)
		if err != nil {
			return err
		}
		n.Offset = offset
	}
	return nil
}

func (l *lowerer) queryBody(body ast.QueryBody) (ast.QueryBody, error) {
	switch n := body.(type) {
	case *ast.SelectCore:
		return l.selectCore(n)
	case *ast.SetOp:
		left, err := l.queryBody(n.Left)
		if err != nil {
			return nil, err
		}
		right, err := l.queryBody(n.Right)
		if err != nil {
			return nil, err
		}
		n.Left, n.Right = left, right
		return n, nil
	case *ast.ValuesBody:
		for i := range n.Rows {
			row, err := l.exprList(n.Rows[i])
			if err != nil {
				return nil, err
			}
			n.Rows[i] = row
		}
		return n, nil
	default:
		return body, nil
	}
}

func (l *lowerer) selectCore(n *ast.SelectCore) (ast.QueryBody, error) {
	// Asked before the items are lowered, so the refusal names the column the
	// caller wrote rather than whatever a rewrite left in its place.
	if err := checkGroupedSelect(l.rules.Dialect(), n); err != nil {
		return nil, err
	}
	for i := range n.Items {
		item, err := l.expr(n.Items[i].Expr)
		if err != nil {
			return nil, err
		}
		n.Items[i].Expr = item
	}
	for i := range n.From {
		table, err := l.tableExpr(n.From[i])
		if err != nil {
			return nil, err
		}
		n.From[i] = table
	}
	if n.Where != nil {
		where, err := l.expr(n.Where)
		if err != nil {
			return nil, err
		}
		n.Where = where
	}
	group, err := l.exprList(n.GroupBy)
	if err != nil {
		return nil, err
	}
	n.GroupBy = group
	if n.Having != nil {
		having, err := l.expr(n.Having)
		if err != nil {
			return nil, err
		}
		n.Having = having
	}
	for i := range n.Windows {
		if err := l.windowSpec(n.Windows[i].Spec); err != nil {
			return nil, err
		}
	}
	if n.Qualify != nil {
		qualify, err := l.expr(n.Qualify)
		if err != nil {
			return nil, err
		}
		n.Qualify = qualify
	}
	if err := l.rules.Core(n); err != nil {
		return nil, err
	}
	return n, nil
}

func (l *lowerer) windowSpec(n *ast.WindowSpec) error {
	if n == nil {
		return nil
	}
	partition, err := l.exprList(n.PartitionBy)
	if err != nil {
		return err
	}
	n.PartitionBy = partition
	for i := range n.OrderBy {
		if err := l.orderTerm(&n.OrderBy[i]); err != nil {
			return err
		}
	}
	if n.Frame != nil {
		if n.Frame.Start.Offset != nil {
			offset, err := l.expr(n.Frame.Start.Offset)
			if err != nil {
				return err
			}
			n.Frame.Start.Offset = offset
		}
		if n.Frame.End != nil && n.Frame.End.Offset != nil {
			offset, err := l.expr(n.Frame.End.Offset)
			if err != nil {
				return err
			}
			n.Frame.End.Offset = offset
		}
	}
	return nil
}

func (l *lowerer) orderTerm(term *ast.OrderTerm) error {
	e, err := l.expr(term.Expr)
	if err != nil {
		return err
	}
	term.Expr = e
	return l.rules.Order(term)
}

func (l *lowerer) tableExpr(t ast.TableExpr) (ast.TableExpr, error) {
	switch n := t.(type) {
	case *ast.TableName:
		if len(n.Columns) > 0 {
			return nil, unsupported(n.Span, renameTableColumns)
		}
		return n, nil
	case *ast.SubqueryTable:
		if n.Lateral {
			return nil, unsupported(n.Span,
				"LATERAL is not supported; SQLite has no way for a FROM item to see the columns of an earlier one")
		}
		sub, err := l.selectStmt(n.Sub)
		if err != nil {
			return nil, err
		}
		n.Sub = sub
		if err := renameSubqueryColumns(n); err != nil {
			return nil, err
		}
		return n, nil
	case *ast.FuncTable:
		if n.Lateral {
			return nil, unsupported(n.Span,
				"LATERAL is not supported; SQLite has no way for a FROM item to see the columns of an earlier one")
		}
		if len(n.Columns) > 0 {
			return nil, unsupported(n.Span, renameTableColumns)
		}
		call, err := l.expr(n.Call)
		if err != nil {
			return nil, err
		}
		fn, ok := call.(*ast.FuncCall)
		if !ok {
			return nil, unsupported(n.Span, "this table reference has no SQLite form")
		}
		n.Call = fn
		return n, nil
	case *ast.JoinTable:
		left, err := l.tableExpr(n.Left)
		if err != nil {
			return nil, err
		}
		right, err := l.tableExpr(n.Right)
		if err != nil {
			return nil, err
		}
		n.Left, n.Right = left, right
		if n.On != nil {
			on, err := l.expr(n.On)
			if err != nil {
				return nil, err
			}
			n.On = on
		}
		return n, nil
	case *ast.ParenTable:
		inner, err := l.tableExpr(n.Inner)
		if err != nil {
			return nil, err
		}
		n.Inner = inner
		return n, nil
	default:
		return t, nil
	}
}

func (l *lowerer) exprList(list []ast.Expr) ([]ast.Expr, error) {
	for i := range list {
		e, err := l.expr(list[i])
		if err != nil {
			return nil, err
		}
		list[i] = e
	}
	return list, nil
}

// operand lowers an expression that stands beside an operator or inside a call,
// where an interval is part of what the parent means rather than a value of its
// own. Lowering it here would refuse it before the parent could read it, so
// only its amount is lowered and the interval reaches the parent whole.
func (l *lowerer) operand(e ast.Expr) (ast.Expr, error) {
	switch n := e.(type) {
	case *ast.IntervalExpr:
		value, err := l.expr(n.Value)
		if err != nil {
			return nil, err
		}
		n.Value = value
		return n, nil
	default:
		return l.expr(e)
	}
}

// expr lowers an expression. Children are lowered first, so a rule always sees
// operands that already mean what they will mean to SQLite; that is what makes
// the rules independent of each other's order.
func (l *lowerer) expr(e ast.Expr) (ast.Expr, error) {
	replaced, handled, err := l.rules.Pre(e)
	if err != nil {
		return nil, err
	}
	if handled {
		// What a Pre rule built is lowered without being offered to Pre again.
		return l.lowerNode(replaced)
	}
	return l.lowerNode(e)
}

// lowerNode lowers one node and everything under it.
func (l *lowerer) lowerNode(e ast.Expr) (ast.Expr, error) {
	switch n := e.(type) {
	case *ast.Literal:
		return l.rules.Literal(n)

	case *ast.ColumnRef:
		value, ok, err := bareValue(l.rules.Dialect(), n)
		if err != nil {
			return nil, err
		}
		if ok {
			return value, nil
		}
		return e, nil

	case *ast.Ident, *ast.Star, *ast.Placeholder, *ast.Keyword:
		return e, nil

	case *ast.ParenExpr:
		inner, err := l.expr(n.Expr)
		if err != nil {
			return nil, err
		}
		n.Expr = inner
		return n, nil

	case *ast.UnaryExpr:
		operand, err := l.expr(n.Expr)
		if err != nil {
			return nil, err
		}
		n.Expr = operand
		return l.rules.Unary(n)

	case *ast.BinaryExpr:
		left, err := l.operand(n.Left)
		if err != nil {
			return nil, err
		}
		right, err := l.operand(n.Right)
		if err != nil {
			return nil, err
		}
		n.Left, n.Right = left, right
		if n.Escape != nil {
			escape, err := l.expr(n.Escape)
			if err != nil {
				return nil, err
			}
			n.Escape = escape
		}
		return l.rules.Binary(n)

	case *ast.IsExpr:
		return l.isExpr(n)

	case *ast.BetweenExpr:
		return l.between(n)

	case *ast.InExpr:
		return l.inExpr(n)

	case *ast.QuantifiedExpr:
		return l.quantified(n)

	case *ast.ExistsExpr:
		sub, err := l.selectStmt(n.Sub)
		if err != nil {
			return nil, err
		}
		n.Sub = sub
		return n, nil

	case *ast.SubqueryExpr:
		sub, err := l.selectStmt(n.Sub)
		if err != nil {
			return nil, err
		}
		n.Sub = sub
		return n, nil

	case *ast.RowExpr:
		list, err := l.exprList(n.Exprs)
		if err != nil {
			return nil, err
		}
		n.Exprs = list
		return n, nil

	case *ast.CaseExpr:
		return l.caseExpr(n)

	case *ast.CastExpr:
		inner, err := l.expr(n.Expr)
		if err != nil {
			return nil, err
		}
		n.Expr = inner
		return l.rules.Cast(n)

	case *ast.CollateExpr:
		return l.collate(n)

	case *ast.IntervalExpr:
		// An interval that reaches here stands where a value belongs: the date
		// arithmetic that would have consumed it is not around it. SQLite has
		// no type to hold one, so there is nothing to answer with.
		return nil, unsupported(n.Span,
			"an INTERVAL value is only supported beside a date; SQLite has no interval type")

	case *ast.TypedLiteral:
		return l.rules.TypedLiteral(n)

	case *ast.FuncCall:
		return l.call(n)

	case *ast.ArrayExpr:
		return nil, unsupported(n.Span, "arrays are not supported; SQLite has no array type")

	case *ast.SubscriptExpr:
		return nil, unsupported(n.Span, "a subscript is not supported; SQLite has no array type")

	case *ast.StructExpr:
		return nil, unsupported(n.Span, "structs are not supported; SQLite has no struct type")

	default:
		return e, nil
	}
}

func (l *lowerer) isExpr(n *ast.IsExpr) (ast.Expr, error) {
	left, err := l.expr(n.Expr)
	if err != nil {
		return nil, err
	}
	right, err := l.expr(n.Right)
	if err != nil {
		return nil, err
	}
	n.Expr, n.Right = left, right
	if n.Distinct {
		// IS DISTINCT FROM is null-safe inequality, which SQLite spells IS NOT.
		n.Distinct = false
		n.Negated = !n.Negated
	}
	return n, nil
}

func (l *lowerer) between(n *ast.BetweenExpr) (ast.Expr, error) {
	value, err := l.expr(n.Expr)
	if err != nil {
		return nil, err
	}
	low, err := l.expr(n.Low)
	if err != nil {
		return nil, err
	}
	high, err := l.expr(n.High)
	if err != nil {
		return nil, err
	}
	n.Expr, n.Low, n.High = value, low, high
	if n.Symmetric {
		// BETWEEN SYMMETRIC holds when the value lies between the two bounds in
		// either order, which is the same as lying between the smaller and the
		// larger of them. Each bound is evaluated twice, as PostgreSQL's own
		// expansion does.
		n.Symmetric = false
		n.Low = helper("least", n.Span, low, high)
		n.High = helper("greatest", n.Span, low, high)
	}
	return n, nil
}

func (l *lowerer) inExpr(n *ast.InExpr) (ast.Expr, error) {
	value, err := l.expr(n.Expr)
	if err != nil {
		return nil, err
	}
	n.Expr = value
	if n.Sub != nil {
		sub, err := l.selectStmt(n.Sub)
		if err != nil {
			return nil, err
		}
		n.Sub = sub
		return n, nil
	}
	list, err := l.exprList(n.List)
	if err != nil {
		return nil, err
	}
	n.List = list
	return n, nil
}

func (l *lowerer) caseExpr(n *ast.CaseExpr) (ast.Expr, error) {
	if n.Operand != nil {
		operand, err := l.expr(n.Operand)
		if err != nil {
			return nil, err
		}
		n.Operand = operand
	}
	for i := range n.Whens {
		cond, err := l.expr(n.Whens[i].Cond)
		if err != nil {
			return nil, err
		}
		result, err := l.expr(n.Whens[i].Result)
		if err != nil {
			return nil, err
		}
		n.Whens[i].Cond, n.Whens[i].Result = cond, result
	}
	if n.Else != nil {
		otherwise, err := l.expr(n.Else)
		if err != nil {
			return nil, err
		}
		n.Else = otherwise
	}
	return n, nil
}

func (l *lowerer) call(n *ast.FuncCall) (ast.Expr, error) {
	for i := range n.Args {
		arg, err := l.operand(n.Args[i])
		if err != nil {
			return nil, err
		}
		n.Args[i] = arg
	}
	if n.Filter != nil {
		filter, err := l.expr(n.Filter)
		if err != nil {
			return nil, err
		}
		n.Filter = filter
	}
	if n.Separator != nil {
		sep, err := l.expr(n.Separator)
		if err != nil {
			return nil, err
		}
		n.Separator = sep
	}
	if n.Limit != nil {
		limit, err := l.expr(n.Limit)
		if err != nil {
			return nil, err
		}
		n.Limit = limit
	}
	for i := range n.OrderBy {
		if err := l.orderTerm(&n.OrderBy[i]); err != nil {
			return nil, err
		}
	}
	for i := range n.WithinGroup {
		if err := l.orderTerm(&n.WithinGroup[i]); err != nil {
			return nil, err
		}
	}
	if err := l.windowSpec(n.Over); err != nil {
		return nil, err
	}
	if lowered, ok, err := l.aggregate(n); err != nil || ok {
		return lowered, err
	}
	// The name as the caller wrote it, taken before the rules rename the call:
	// the refusal below uses this name and not the helper's.
	written := callName(n)
	span := n.Span
	if err := checkSourceArity(l.rules.Dialect(), written, len(n.Args), span); err != nil {
		return nil, err
	}
	lowered, err := l.rules.Call(n)
	if err != nil {
		return nil, err
	}
	if err := checkHelperArity(lowered, written, span); err != nil {
		return nil, err
	}
	return lowered, nil
}

// checkHelperArity refuses a lowered call whose helper does not take that many
// arguments.
//
// A lowering renames a function to the helper that computes it without counting
// the arguments, so the count is checked here, where the caller's own spelling
// is still at hand.
func checkHelperArity(e ast.Expr, written string, span ast.Span) error {
	call, ok := e.(*ast.FuncCall)
	if !ok || len(call.Name) != 1 {
		return nil
	}
	// The helper is named in lower case in the table and a call carries whatever
	// case it was written in, the more so for the helpers registered under the
	// source function's own name.
	// A lowering that claimed the call gave it the target's own spelling; one
	// that left it alone kept the caller's, whatever case they wrote it in. So
	// a call the lowering renamed onto length is told apart from a LOG the
	// caller wrote and no rule claimed.
	renamed := call.Name[0].Name != written
	name := strings.ToLower(call.Name[0].Name)
	if helperTakesArgumentCount(name, len(call.Args), renamed) {
		return nil
	}
	return unsupported(span, "%s takes %s and the call has %d",
		written, arityDescription(name), len(call.Args))
}

// sourceArity is how many arguments a dialect's own function takes, for the
// names this package leaves on SQLite's function of the same name where SQLite
// accepts a count the source dialect does not.
//
// The helper arity check covers every name this package rewrites, because a
// rewriting has to know how many arguments it is rewriting. A name left alone
// is checked by SQLite instead, and SQLite answering a call the source dialect
// would have refused is the one outcome worth catching: MySQL's LTRIM takes the
// string alone, SQLite's takes an optional set of characters to strip, so
// LTRIM('xxab','x') answered "ab" here and "Incorrect parameter count" in MySQL.
var sourceArity = map[dialects.Dialect]map[string]int{ //nolint:gochecknoglobals // a fixed table
	dialects.MySQL: {
		"LTRIM": 1,
		"RTRIM": 1,
	},
}

// checkSourceArity refuses a call whose argument count the dialect that wrote
// it does not accept.
func checkSourceArity(d dialects.Dialect, written string, args int, span ast.Span) error {
	names, ok := sourceArity[d]
	if !ok {
		return nil
	}
	want, ok := names[strings.ToUpper(written)]
	if !ok || args == want {
		return nil
	}
	return unsupported(span, "%s takes %d argument and the call has %d",
		written, want, args)
}

func (l *lowerer) insert(n *ast.InsertStmt) (ast.Stmt, error) {
	for i := range n.Rows {
		row, err := l.exprList(n.Rows[i])
		if err != nil {
			return nil, err
		}
		n.Rows[i] = row
	}
	if n.Query != nil {
		query, err := l.selectStmt(n.Query)
		if err != nil {
			return nil, err
		}
		n.Query = query
	}
	if n.OnConflict != nil {
		if err := l.assignments(n.OnConflict.Set); err != nil {
			return nil, err
		}
		if n.OnConflict.TargetWhere != nil {
			where, err := l.expr(n.OnConflict.TargetWhere)
			if err != nil {
				return nil, err
			}
			n.OnConflict.TargetWhere = where
		}
		if n.OnConflict.Where != nil {
			where, err := l.expr(n.OnConflict.Where)
			if err != nil {
				return nil, err
			}
			n.OnConflict.Where = where
		}
	}
	return n, l.selectItems(n.Returning)
}

func (l *lowerer) update(n *ast.UpdateStmt) (ast.Stmt, error) {
	if err := rowLimitedWrite("UPDATE", n.OrderBy, n.Limit, n.Span); err != nil {
		return nil, err
	}
	if err := l.assignments(n.Set); err != nil {
		return nil, err
	}
	for i := range n.From {
		table, err := l.tableExpr(n.From[i])
		if err != nil {
			return nil, err
		}
		n.From[i] = table
	}
	if n.Where != nil {
		where, err := l.expr(n.Where)
		if err != nil {
			return nil, err
		}
		n.Where = where
	}
	return n, l.selectItems(n.Returning)
}

func (l *lowerer) deleteStmt(n *ast.DeleteStmt) (ast.Stmt, error) {
	if err := rowLimitedWrite("DELETE", n.OrderBy, n.Limit, n.Span); err != nil {
		return nil, err
	}
	if len(n.Using) > 0 {
		return nil, unsupported(n.Span,
			"DELETE ... USING is not supported; write the other tables as a subquery in WHERE")
	}
	if n.Where != nil {
		where, err := l.expr(n.Where)
		if err != nil {
			return nil, err
		}
		n.Where = where
	}
	return n, l.selectItems(n.Returning)
}

// rowLimitedWrite refuses an UPDATE or a DELETE that says which rows it touches
// by order and count. SQLite takes ORDER BY and LIMIT on those statements only
// in a build compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT, and the build
// behind this package is not one, so the statement can never run however it is
// written. Passed through it reached the driver as a syntax error naming a
// keyword that is valid where the caller wrote it, and the clause had been
// reshaped on the way -- MySQL's "LIMIT 2, 1" rewritten into "LIMIT 1 OFFSET 2"
// on a statement with nothing to run it.
func rowLimitedWrite(statement string, order []ast.OrderTerm, limit *ast.LimitClause, span ast.Span) error {
	var clause string
	switch {
	case len(order) > 0:
		clause = "ORDER BY"
		// The statement's own span is where UPDATE or DELETE stands, which is
		// not where the refused clause is; a caller reading the message goes to
		// the clause it names.
		if at := order[0].Span; at != (ast.Span{}) {
			span = at
		}
	case limit != nil:
		clause = "LIMIT"
		if at := limit.Span; at != (ast.Span{}) {
			span = at
		}
	default:
		return nil
	}
	return unsupported(span,
		"%s on %s is not supported: SQLite takes it only in a build compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT, "+
			"and this one is not; name the rows in WHERE, with a subquery that orders and limits them if that is how they are chosen",
		clause, statement)
}

func (l *lowerer) assignments(assigns []ast.Assignment) error {
	for i := range assigns {
		value, err := l.expr(assigns[i].Value)
		if err != nil {
			return err
		}
		assigns[i].Value = value
	}
	return nil
}

func (l *lowerer) selectItems(items []ast.SelectItem) error {
	for i := range items {
		e, err := l.expr(items[i].Expr)
		if err != nil {
			return err
		}
		items[i].Expr = e
	}
	return nil
}

func (l *lowerer) createTable(n *ast.CreateTableStmt) (ast.Stmt, error) {
	if n.AsSelect != nil {
		query, err := l.selectStmt(n.AsSelect)
		if err != nil {
			return nil, err
		}
		n.AsSelect = query
	}
	for i := range n.Columns {
		if err := l.columnDef(&n.Columns[i]); err != nil {
			return nil, err
		}
	}
	for i := range n.Constraints {
		if n.Constraints[i].Expr == nil {
			continue
		}
		e, err := l.expr(n.Constraints[i].Expr)
		if err != nil {
			return nil, err
		}
		n.Constraints[i].Expr = e
	}
	return n, nil
}

func (l *lowerer) columnDef(col *ast.ColumnDef) error {
	// The numbering has to be read before the type is mapped: SERIAL becomes
	// INTEGER, and by then nothing says the column was one.
	if err := promoteSerial(col); err != nil {
		return err
	}
	if col.Type != nil {
		name, err := columnTypeFor(l.rules.Dialect(), *col.Type)
		if err != nil {
			return err
		}
		col.Type = &name
	}
	for i := range col.Constraints {
		if col.Constraints[i].Kind == ast.ConstraintCollate {
			name, ok := sqliteCollation(col.Constraints[i].Text)
			if !ok {
				return unsupported(col.Constraints[i].Span,
					"the collation %s is not supported; SQLite has BINARY, NOCASE and RTRIM",
					col.Constraints[i].Text)
			}
			col.Constraints[i].Text = name
			continue
		}
		if col.Constraints[i].Expr == nil {
			continue
		}
		e, err := l.expr(col.Constraints[i].Expr)
		if err != nil {
			return err
		}
		col.Constraints[i].Expr = e
	}
	return nil
}

// renameTableColumns is the refusal for a column list SQLite cannot be given.
// A FROM item takes only a name in SQLite, so the rename has to be carried by
// the columns themselves, which is possible only when translation can see them.
const renameTableColumns = "a column list on a table reference is not supported; " +
	"give the columns their names in a select list, or write WITH name (columns) AS (...)"

// renameSubqueryColumns moves a derived table's column list onto the select
// list it renames, which is where SQLite takes a result column's name from. A
// compound query takes its names from the first SELECT, so that is the one the
// names go on. What the list cannot be moved onto -- a VALUES body, a select
// list that ends in a star, a count that does not match -- is refused rather
// than dropped, since dropping it answers the old names in silence.
func renameSubqueryColumns(n *ast.SubqueryTable) error {
	if len(n.Columns) == 0 {
		return nil
	}
	items := firstSelectItems(n.Sub.Body)
	if items == nil || len(*items) != len(n.Columns) {
		return unsupported(n.Span, renameTableColumns)
	}
	for i := range *items {
		if _, ok := (*items)[i].Expr.(*ast.Star); ok {
			return unsupported(n.Span, renameTableColumns)
		}
	}
	for i, name := range n.Columns {
		(*items)[i].Alias, (*items)[i].AliasQuoted = name, true
	}
	n.Columns = nil
	return nil
}

// firstSelectItems reports the select list a query body's result columns are
// named after, or nil when the body has none to name.
func firstSelectItems(body ast.QueryBody) *[]ast.SelectItem {
	switch b := body.(type) {
	case *ast.SelectCore:
		return &b.Items
	case *ast.SetOp:
		return firstSelectItems(b.Left)
	default:
		return nil
	}
}
