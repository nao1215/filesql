package render

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
)

// Precedence levels for rendering. They are SQLite's, because SQLite is what
// reads the output; the parser's table is the source dialect's and the two are
// deliberately separate.
const (
	precLowest = iota
	precOr
	precAnd
	precNot
	precCompare
	precBitwise
	precAddSub
	precMulDiv
	precConcat
	precUnary
	precPostfix
)

// sqlitePrec is the binding power SQLite gives an operator.
func sqlitePrec(op ast.BinaryOp) int {
	switch op {
	case ast.Or:
		return precOr
	case ast.And:
		return precAnd
	case ast.Eq, ast.NotEq, ast.Lt, ast.Lte, ast.Gt, ast.Gte, ast.Like, ast.NotLike, ast.Regexp, ast.NotRegexp:
		return precCompare
	case ast.BitAnd, ast.BitOr, ast.ShiftLeft, ast.ShiftRight:
		return precBitwise
	case ast.Add, ast.Sub:
		return precAddSub
	case ast.Mul, ast.Div, ast.Mod:
		return precMulDiv
	case ast.Concat, ast.JSONGet, ast.JSONGetText:
		// SQLite binds these three tighter than anything else, so an operand
		// that is a sum needs its parentheses back.
		return precConcat
	default:
		return precCompare
	}
}

// sqliteOperator spells an operator the way SQLite does. An operator with no
// SQLite spelling reports false; lowering is supposed to have removed it.
func sqliteOperator(op ast.BinaryOp) (string, bool) {
	switch op {
	case ast.Add:
		return "+", true
	case ast.Sub:
		return "-", true
	case ast.Mul:
		return "*", true
	case ast.Div:
		return "/", true
	case ast.Mod:
		return "%", true
	case ast.Eq:
		return "=", true
	case ast.NotEq:
		return "<>", true
	case ast.Lt:
		return "<", true
	case ast.Lte:
		return "<=", true
	case ast.Gt:
		return ">", true
	case ast.Gte:
		return ">=", true
	case ast.And:
		return "AND", true
	case ast.Or:
		return "OR", true
	case ast.BitAnd:
		return "&", true
	case ast.BitOr:
		return "|", true
	case ast.ShiftLeft:
		return "<<", true
	case ast.ShiftRight:
		return ">>", true
	case ast.Concat:
		return "||", true
	case ast.JSONGet:
		return "->", true
	case ast.JSONGetText:
		return "->>", true
	case ast.Like:
		return "LIKE", true
	case ast.NotLike:
		return "NOT LIKE", true
	case ast.Regexp:
		return "REGEXP", true
	case ast.NotRegexp:
		return "NOT REGEXP", true
	default:
		return "", false
	}
}

// sqliteAlsoSpells reports whether SQLite accepts the caller's spelling of an
// operator beside the one this package would write.
func sqliteAlsoSpells(op ast.BinaryOp, spelling string) bool {
	switch op {
	case ast.NotEq:
		return spelling == "!=" || spelling == "<>"
	case ast.Eq:
		return spelling == "==" || spelling == "="
	default:
		return false
	}
}

// operatorName spells an operator for a diagnostic about it.
func operatorName(op ast.BinaryOp) string {
	if name, ok := sqliteOperator(op); ok {
		return "the " + name + " operator"
	}
	switch op {
	case ast.IntDiv:
		return "the DIV operator"
	case ast.Xor:
		return "the XOR operator"
	case ast.NullSafeEq:
		return "the <=> operator"
	case ast.BitXor:
		return "the bitwise XOR operator"
	case ast.ILike, ast.NotILike:
		return "the ILIKE operator"
	case ast.SimilarTo, ast.NotSimilarTo:
		return "the SIMILAR TO operator"
	case ast.RegexpCI, ast.NotRegexpCI:
		return "the case-insensitive regular-expression operator"
	case ast.SoundsLike:
		return "the SOUNDS LIKE operator"
	case ast.MemberOf:
		return "the MEMBER OF operator"
	default:
		return "this operator"
	}
}

// expr writes an expression, parenthesizing it when the context binds tighter
// than the expression's own operator.
func (w *writer) expr(e ast.Expr, minPrec int) error {
	switch n := e.(type) {
	case *ast.Literal:
		return w.literal(n)
	case *ast.Ident:
		w.name(*n)
		return nil
	case *ast.ColumnRef:
		return w.columnRef(n)
	case *ast.Star:
		return w.star(n)
	case *ast.Keyword:
		w.word(n.Name)
		return nil
	case *ast.Placeholder:
		w.word(n.Text)
		return nil
	case *ast.ParenExpr:
		w.word("(")
		if err := w.expr(n.Expr, precLowest); err != nil {
			return err
		}
		w.word(")")
		return nil
	case *ast.UnaryExpr:
		return w.unary(n, minPrec)
	case *ast.BinaryExpr:
		return w.binary(n, minPrec)
	case *ast.IsExpr:
		return w.isExpr(n, minPrec)
	case *ast.BetweenExpr:
		return w.between(n, minPrec)
	case *ast.InExpr:
		return w.inExpr(n, minPrec)
	case *ast.ExistsExpr:
		return w.existsExpr(n)
	case *ast.SubqueryExpr:
		w.word("(")
		if err := w.selectStmt(n.Sub); err != nil {
			return err
		}
		w.word(")")
		return nil
	case *ast.CaseExpr:
		return w.caseExpr(n)
	case *ast.CastExpr:
		return w.cast(n)
	case *ast.CollateExpr:
		if err := w.expr(n.Expr, precPostfix); err != nil {
			return err
		}
		w.word("COLLATE")
		w.word(n.Collation)
		return nil
	case *ast.FuncCall:
		return w.call(n)
	case *ast.RowExpr:
		return w.rowExpr(n)
	case *ast.QuantifiedExpr:
		return unsupported(n.Span, "a quantified comparison")
	case *ast.IntervalExpr:
		return unsupported(n.Span, "an INTERVAL value")
	case *ast.TypedLiteral:
		return unsupported(n.Span, "a typed literal")
	case *ast.ArrayExpr:
		return unsupported(n.Span, "an array")
	case *ast.SubscriptExpr:
		return unsupported(n.Span, "a subscript")
	case *ast.StructExpr:
		return unsupported(n.Span, "a struct")
	default:
		return unsupported(e.At(), "this expression")
	}
}

func (w *writer) literal(n *ast.Literal) error {
	switch n.Kind {
	case ast.LitNumber:
		// A literal SQLite's own lexer would read differently -- a hexadecimal
		// or a bit string -- never reaches here, because lowering turns it into
		// a decimal.
		w.word(n.Value)
	case ast.LitString:
		w.word(QuoteString(n.Value))
	case ast.LitBlob:
		w.word("x'" + n.Value + "'")
	case ast.LitNull:
		w.word("NULL")
	case ast.LitBool:
		w.word(n.Value)
	case ast.LitHex:
		// SQLite reads 0x... as an integer, which is what the dialects that
		// write it mean by it.
		w.word(n.Value)
	case ast.LitBit:
		// A bit string is written as the text of its digits, which is what
		// PostgreSQL compares and concatenates. MySQL and GoogleSQL refuse the
		// literal while lowering -- each reads it as a number in one place and
		// as bytes in another -- so one that reaches here is PostgreSQL's.
		w.word(QuoteString(n.Value))
	}
	return nil
}

func (w *writer) columnRef(n *ast.ColumnRef) error {
	for i, part := range n.Parts {
		if i > 0 {
			w.dot()
		}
		w.name(part)
	}
	return nil
}

func (w *writer) star(n *ast.Star) error {
	for _, part := range n.Qualifier {
		w.name(part)
		w.dot()
	}
	w.word("*")
	return nil
}

func (w *writer) unary(n *ast.UnaryExpr, minPrec int) error {
	prec := precUnary
	if n.Op == ast.UnaryNot {
		prec = precNot
	}
	open := prec < minPrec
	if open {
		w.word("(")
	}
	switch n.Op {
	case ast.UnaryPlus:
		w.word("+")
	case ast.UnaryMinus:
		w.word("-")
	case ast.UnaryNot:
		w.word("NOT")
	case ast.UnaryBitNot:
		w.word("~")
	case ast.UnaryRegexpMatch:
		return unsupported(n.Span, "a prefix ~ operator")
	}
	if n.Op != ast.UnaryNot {
		// A sign belongs to its operand.
		w.tight = true
	}
	if err := w.expr(n.Expr, prec); err != nil {
		return err
	}
	if open {
		w.word(")")
	}
	return nil
}

func (w *writer) binary(n *ast.BinaryExpr, minPrec int) error {
	text, ok := sqliteOperator(n.Op)
	if !ok {
		return unsupported(n.Span, operatorName(n.Op))
	}
	if n.Spelling != "" && sqliteAlsoSpells(n.Op, n.Spelling) {
		text = n.Spelling
	}
	prec := sqlitePrec(n.Op)
	open := prec < minPrec
	if open {
		w.word("(")
	}
	if err := w.expr(n.Left, prec); err != nil {
		return err
	}
	w.word(text)
	// The right operand is written one level tighter, which is what keeps
	// "a - (b - c)" from losing its parentheses.
	if err := w.expr(n.Right, prec+1); err != nil {
		return err
	}
	if n.Escape != nil {
		w.word("ESCAPE")
		if err := w.expr(n.Escape, precUnary); err != nil {
			return err
		}
	}
	if open {
		w.word(")")
	}
	return nil
}

func (w *writer) isExpr(n *ast.IsExpr, minPrec int) error {
	if n.Distinct {
		return unsupported(n.Span, "IS DISTINCT FROM")
	}
	open := precCompare < minPrec
	if open {
		w.word("(")
	}
	if err := w.expr(n.Expr, precCompare); err != nil {
		return err
	}
	w.word("IS")
	if n.Negated {
		w.word("NOT")
	}
	if err := w.expr(n.Right, precCompare+1); err != nil {
		return err
	}
	if open {
		w.word(")")
	}
	return nil
}

func (w *writer) between(n *ast.BetweenExpr, minPrec int) error {
	if n.Symmetric {
		return unsupported(n.Span, "BETWEEN SYMMETRIC")
	}
	open := precCompare < minPrec
	if open {
		w.word("(")
	}
	if err := w.expr(n.Expr, precCompare); err != nil {
		return err
	}
	if n.Negated {
		w.word("NOT")
	}
	w.word("BETWEEN")
	if err := w.expr(n.Low, precCompare+1); err != nil {
		return err
	}
	w.word("AND")
	if err := w.expr(n.High, precCompare+1); err != nil {
		return err
	}
	if open {
		w.word(")")
	}
	return nil
}

func (w *writer) inExpr(n *ast.InExpr, minPrec int) error {
	open := precCompare < minPrec
	if open {
		w.word("(")
	}
	if err := w.expr(n.Expr, precCompare); err != nil {
		return err
	}
	if n.Negated {
		w.word("NOT")
	}
	w.word("IN")
	w.word("(")
	switch {
	case n.Sub != nil:
		if err := w.selectStmt(n.Sub); err != nil {
			return err
		}
	default:
		for i, e := range n.List {
			if i > 0 {
				w.word(",")
			}
			if err := w.expr(e, precLowest); err != nil {
				return err
			}
		}
	}
	w.word(")")
	if open {
		w.word(")")
	}
	return nil
}

func (w *writer) existsExpr(n *ast.ExistsExpr) error {
	if n.Negated {
		w.word("NOT")
	}
	w.word("EXISTS")
	w.word("(")
	if err := w.selectStmt(n.Sub); err != nil {
		return err
	}
	w.word(")")
	return nil
}

func (w *writer) caseExpr(n *ast.CaseExpr) error {
	w.word("CASE")
	if n.Operand != nil {
		if err := w.expr(n.Operand, precLowest); err != nil {
			return err
		}
	}
	for _, when := range n.Whens {
		w.word("WHEN")
		if err := w.expr(when.Cond, precLowest); err != nil {
			return err
		}
		w.word("THEN")
		if err := w.expr(when.Result, precLowest); err != nil {
			return err
		}
	}
	if n.Else != nil {
		w.word("ELSE")
		if err := w.expr(n.Else, precLowest); err != nil {
			return err
		}
	}
	w.word("END")
	return nil
}

func (w *writer) cast(n *ast.CastExpr) error {
	if n.TryCast {
		return unsupported(n.Span, "a cast that answers NULL rather than raising")
	}
	w.word("CAST")
	w.b.WriteByte('(')
	if err := w.expr(n.Expr, precLowest); err != nil {
		return err
	}
	w.word("AS")
	if n.Type.Written != "" {
		w.word(n.Type.Written)
		w.word(")")
		return nil
	}
	w.word(n.Type.Name)
	if len(n.Type.Params) > 0 {
		w.b.WriteByte('(')
		for i, param := range n.Type.Params {
			if i > 0 {
				w.b.WriteByte(',')
			}
			w.b.WriteString(param)
		}
		w.b.WriteByte(')')
	}
	w.word(")")
	return nil
}

func (w *writer) rowExpr(n *ast.RowExpr) error {
	w.word("(")
	for i, e := range n.Exprs {
		if i > 0 {
			w.word(",")
		}
		if err := w.expr(e, precLowest); err != nil {
			return err
		}
	}
	w.word(")")
	return nil
}

func (w *writer) call(n *ast.FuncCall) error {
	if len(n.Name) != 1 {
		return unsupported(n.Span, "a qualified function name")
	}
	if n.Separator != nil || n.Limit != nil || len(n.WithinGroup) > 0 {
		return unsupported(n.Span, "this aggregate clause")
	}
	// A call name is quoted by the same rule every other name is: SQLite
	// keeps words like SELECT for its own grammar, and one written bare made
	// SQL that answered with a syntax error about text this package rendered
	// rather than with the missing function the caller asked for.
	w.word(quoteIfNeeded(n.Name[0].Name))
	w.b.WriteByte('(')
	switch {
	case n.Star:
		w.b.WriteByte('*')
	default:
		if n.Distinct {
			w.word("DISTINCT")
		}
		for i, arg := range n.Args {
			if i > 0 {
				w.word(",")
			}
			if err := w.expr(arg, precLowest); err != nil {
				return err
			}
		}
		if len(n.OrderBy) > 0 {
			w.word("ORDER BY")
			if err := w.orderTerms(n.OrderBy); err != nil {
				return err
			}
		}
	}
	w.word(")")
	if n.Filter != nil {
		w.word("FILTER")
		w.word("(WHERE")
		if err := w.expr(n.Filter, precLowest); err != nil {
			return err
		}
		w.word(")")
	}
	if n.Over != nil {
		w.word("OVER")
		if err := w.windowSpec(n.Over); err != nil {
			return err
		}
	}
	return nil
}
