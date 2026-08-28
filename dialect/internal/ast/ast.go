// Package ast is the typed syntax tree the parser builds and the lowering layer
// rewrites. A node holds what the query says and where it says it; it never
// holds rendered SQL, and it never holds a decision that belongs to a dialect.
package ast

import "github.com/nao1215/filesql/dialect/internal/token"

// Span is the source range a node covers, as a line and column of its first
// token. It exists so a diagnostic can name the place a construct was written
// rather than the whole query.
type Span struct {
	Line int
	Col  int
}

// SpanOf reads the span of a token.
func SpanOf(t token.Token) Span { return Span{Line: t.Line, Col: t.Col} }

// Node is any syntax tree node.
type Node interface {
	// At reports where the node starts.
	At() Span
}

// Expr is an expression: something that has a value.
type Expr interface {
	Node
	exprNode()
}

// Stmt is a statement: something that can stand alone in a query.
type Stmt interface {
	Node
	stmtNode()
}
