package ast

// WalkSelectCores calls fn for every SELECT in a statement, including the ones
// inside subqueries and common table expressions. It exists for the passes that
// have something to say about a select list and nothing to say about the rest
// of the tree.
func WalkSelectCores(node Node, fn func(*SelectCore)) {
	switch n := node.(type) {
	case nil:
		return
	case *SelectStmt:
		if n.With != nil {
			for i := range n.With.CTEs {
				if n.With.CTEs[i].Stmt != nil {
					WalkSelectCores(n.With.CTEs[i].Stmt, fn)
				}
			}
		}
		WalkSelectCores(n.Body, fn)
		for i := range n.OrderBy {
			WalkSelectCores(n.OrderBy[i].Expr, fn)
		}
	case *SelectCore:
		fn(n)
		for i := range n.Items {
			WalkSelectCores(n.Items[i].Expr, fn)
		}
		for _, table := range n.From {
			WalkSelectCores(table, fn)
		}
		WalkSelectCores(n.Where, fn)
		for _, e := range n.GroupBy {
			WalkSelectCores(e, fn)
		}
		WalkSelectCores(n.Having, fn)
	case *SetOp:
		WalkSelectCores(n.Left, fn)
		WalkSelectCores(n.Right, fn)
	case *SubqueryTable:
		WalkSelectCores(n.Sub, fn)
	case *JoinTable:
		WalkSelectCores(n.Left, fn)
		WalkSelectCores(n.Right, fn)
		WalkSelectCores(n.On, fn)
	case *ParenTable:
		WalkSelectCores(n.Inner, fn)
	case *SubqueryExpr:
		WalkSelectCores(n.Sub, fn)
	case *ExistsExpr:
		WalkSelectCores(n.Sub, fn)
	case *InExpr:
		WalkSelectCores(n.Expr, fn)
		if n.Sub != nil {
			WalkSelectCores(n.Sub, fn)
		}
		for _, e := range n.List {
			WalkSelectCores(e, fn)
		}
	case *ParenExpr:
		WalkSelectCores(n.Expr, fn)
	case *UnaryExpr:
		WalkSelectCores(n.Expr, fn)
	case *BinaryExpr:
		WalkSelectCores(n.Left, fn)
		WalkSelectCores(n.Right, fn)
	case *IsExpr:
		WalkSelectCores(n.Expr, fn)
		WalkSelectCores(n.Right, fn)
	case *BetweenExpr:
		WalkSelectCores(n.Expr, fn)
		WalkSelectCores(n.Low, fn)
		WalkSelectCores(n.High, fn)
	case *CaseExpr:
		WalkSelectCores(n.Operand, fn)
		for i := range n.Whens {
			WalkSelectCores(n.Whens[i].Cond, fn)
			WalkSelectCores(n.Whens[i].Result, fn)
		}
		WalkSelectCores(n.Else, fn)
	case *CastExpr:
		WalkSelectCores(n.Expr, fn)
	case *FuncCall:
		for _, arg := range n.Args {
			WalkSelectCores(arg, fn)
		}
		WalkSelectCores(n.Filter, fn)
	case *InsertStmt:
		if n.Query != nil {
			WalkSelectCores(n.Query, fn)
		}
	case *UpdateStmt:
		WalkSelectCores(n.Where, fn)
		for i := range n.Set {
			WalkSelectCores(n.Set[i].Value, fn)
		}
	case *DeleteStmt:
		WalkSelectCores(n.Where, fn)
	case *CreateViewStmt:
		WalkSelectCores(n.Select, fn)
	case *CreateTableStmt:
		if n.AsSelect != nil {
			WalkSelectCores(n.AsSelect, fn)
		}
	case *ExplainStmt:
		WalkSelectCores(n.Stmt, fn)
	}
}
