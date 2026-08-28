// Package lower turns a syntax tree written in a source dialect into one that
// means the same thing to SQLite. It works on the tree: a rule reads a node and
// its children, never a token or a position, so no rule depends on another
// having run first.
//
// A construct becomes one of three things. What SQLite spells is rewritten into
// SQLite's spelling. What SQLite cannot spell but a function can compute
// becomes a call to a helper this package's runtime registers with the driver.
// What is neither is refused, with the place it was written.
package lower

import (
	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// Rules is what one dialect does to a tree. Every method is given a node whose
// children are already lowered, and answers the node that replaces it.
//
// A method that has nothing to say about a node returns it unchanged; the
// shared rules in this package then apply, and what neither handles is refused
// by the renderer, which is the only place that knows what SQLite can write.
type Rules interface {
	// Dialect names the dialect these rules are for.
	Dialect() dialects.Dialect
	// Pre is given a node before its children are lowered, for the rules that
	// have to see the source form: a date literal is a date here and a string
	// once it is lowered, and an interval is a duration here and nothing SQLite
	// can hold afterwards. A rule that answers true has its result lowered in
	// place of the node.
	Pre(ast.Expr) (ast.Expr, bool, error)
	// Call lowers a function or aggregate call.
	Call(*ast.FuncCall) (ast.Expr, error)
	// Binary lowers an infix operator.
	Binary(*ast.BinaryExpr) (ast.Expr, error)
	// Unary lowers a prefix operator.
	Unary(*ast.UnaryExpr) (ast.Expr, error)
	// Cast lowers a cast.
	Cast(*ast.CastExpr) (ast.Expr, error)
	// Literal lowers a constant.
	Literal(*ast.Literal) (ast.Expr, error)
	// TypedLiteral lowers a literal introduced by its type.
	TypedLiteral(*ast.TypedLiteral) (ast.Expr, error)
	// Core lowers a SELECT's clauses.
	Core(*ast.SelectCore) error
	// Order lowers one ORDER BY term.
	Order(*ast.OrderTerm) error
}

// Lower rewrites a statement and returns the statement that replaces it. The
// tree it answers holds only what SQLite can express.
func Lower(d dialects.Dialect, stmt ast.Stmt) (ast.Stmt, error) {
	rules, err := rulesFor(d)
	if err != nil {
		return nil, err
	}
	l := &lowerer{rules: rules}
	return l.stmt(stmt)
}

// rulesFor selects the rules of a dialect.
func rulesFor(d dialects.Dialect) (Rules, error) {
	switch d {
	case dialects.MySQL:
		return &mysqlRules{}, nil
	case dialects.PostgreSQL:
		return &postgresRules{}, nil
	case dialects.GoogleSQL:
		return &googleRules{}, nil
	case dialects.SQLite:
		// SQLite is the identity translation and never reaches here.
		return nil, sqlerr.At(dialects.ErrUnknownDialect, 0, 0, "%q", string(d))
	default:
		return nil, sqlerr.At(dialects.ErrUnknownDialect, 0, 0, "%q", string(d))
	}
}

type lowerer struct {
	rules Rules
}

// unsupported reports a construct SQLite cannot express, at the node.
func unsupported(span ast.Span, format string, args ...any) error {
	return sqlerr.At(sqlerr.ErrUnsupportedSyntax, span.Line, span.Col, format, args...)
}
