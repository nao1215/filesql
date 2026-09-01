package lower

import (
	"strconv"
	"strings"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// callName is the function's own name, upper-cased and without its
// qualification.
func callName(call *ast.FuncCall) string {
	if len(call.Name) == 0 {
		return ""
	}
	return strings.ToUpper(call.Name[len(call.Name)-1].Name)
}

// rename replaces a call's name, dropping any qualification. It is the commonest
// lowering: the function computes the same thing under another name.
func rename(call *ast.FuncCall, name string) ast.Expr {
	call.Name = []ast.Ident{{Name: name, Span: call.Span}}
	return call
}

// plural spells an argument count, in the number the count calls for.
func plural(n int) string {
	if n == 1 {
		return "1 argument"
	}
	return strconv.Itoa(n) + " arguments"
}

// helper builds a call to a runtime helper.
func helper(name string, span ast.Span, args ...ast.Expr) *ast.FuncCall {
	return &ast.FuncCall{
		Name: []ast.Ident{{Name: name, Span: span}},
		Args: args,
		Span: span,
	}
}

// text builds a string literal.
func text(value string, span ast.Span) *ast.Literal {
	return &ast.Literal{Kind: ast.LitString, Value: value, Span: span}
}

// number builds a numeric literal.
func number(value int64, span ast.Span) *ast.Literal {
	return &ast.Literal{Kind: ast.LitNumber, Value: strconv.FormatInt(value, 10), Span: span}
}

// typeText spells a type name the way the cast helpers take it: the name with
// its parameters, which is what the helper parses to decide precision and
// scale.
func typeText(t ast.TypeName) string {
	if t.Written != "" {
		return t.Written
	}
	if len(t.Params) == 0 {
		return t.Name
	}
	return t.Name + "(" + strings.Join(t.Params, ",") + ")"
}

// columnTypeFor maps a declared column type onto SQLite's. A type SQLite does
// not know is not left as written: SQLite takes any run of words as a type and
// derives an affinity from the letters in it, so an unmapped name silently
// decides how the column stores its values.
func columnTypeFor(d dialects.Dialect, t ast.TypeName) (ast.TypeName, error) {
	if t.Array {
		return ast.TypeName{}, unsupported(t.Span, "an array column is not supported; SQLite has no array type")
	}
	name := strings.ToUpper(t.Name)
	// The qualifiers a dialect writes into the type, which SQLite has no second
	// of: an unsigned integer is an integer here, and its range is not enforced.
	for _, suffix := range []string{" UNSIGNED", " SIGNED", " ZEROFILL", " PRECISION UNSIGNED"} {
		name = strings.TrimSuffix(name, suffix)
	}
	mapped, ok := sqliteTypeNames[name]
	if !ok {
		return ast.TypeName{}, unsupported(t.Span,
			"the column type %s is not supported; SQLite stores values as INTEGER, REAL, TEXT, BLOB or NUMERIC", t.Name)
	}
	_ = d
	// The parameters of a text or integer type are a width SQLite does not
	// enforce, and keeping them changes nothing but the declared type; the two
	// numeric types keep theirs because a caller reads them back.
	out := ast.TypeName{Name: mapped, Span: t.Span}
	if mapped == typeNameNumeric {
		out.Params = t.Params
	}
	return out, nil
}

// promoteSerial turns PostgreSQL's SERIAL and the identity spelling into the
// column SQLite numbers by itself. SQLite numbers only an INTEGER PRIMARY KEY,
// so a serial column that is not the primary key is refused rather than created
// with its numbering silently dropped.
func promoteSerial(col *ast.ColumnDef) error {
	serial := false
	if col.Type != nil {
		switch strings.ToUpper(col.Type.Name) {
		case typeNameSerial, typeNameBigserial, "SMALLSERIAL":
			serial = true
		}
	}
	identity := false
	for i, c := range col.Constraints {
		if c.Kind == ast.ConstraintAutoIncrement {
			identity = true
			col.Constraints = append(col.Constraints[:i], col.Constraints[i+1:]...)
			break
		}
	}
	if !serial && !identity {
		return nil
	}
	for i := range col.Constraints {
		if col.Constraints[i].Kind == ast.ConstraintPrimaryKey {
			col.Constraints[i].AutoIncrement = true
			return nil
		}
	}
	return unsupported(col.Span,
		"a column numbered by the database that is not the primary key is not supported; "+
			"SQLite numbers only an INTEGER PRIMARY KEY")
}

// notExpr wraps an expression in a logical negation.
func notExpr(e ast.Expr, span ast.Span) ast.Expr {
	return &ast.UnaryExpr{Op: ast.UnaryNot, Expr: e, Span: span}
}

// isNull builds "e IS NULL", or its negation.
func isNull(e ast.Expr, negated bool, span ast.Span) ast.Expr {
	return &ast.IsExpr{
		Expr:    e,
		Right:   &ast.Literal{Kind: ast.LitNull, Span: span},
		Negated: negated,
		Span:    span,
	}
}

// binary builds an infix expression.
func binary(left ast.Expr, op ast.BinaryOp, right ast.Expr, span ast.Span) ast.Expr {
	return &ast.BinaryExpr{Left: left, Op: op, Right: right, Span: span}
}

// paren wraps an expression so its own operators cannot bind with the ones
// around it. The renderer parenthesizes by precedence, so this is only needed
// where a rule builds a node whose shape the caller cannot see.
func paren(e ast.Expr) ast.Expr {
	switch e.(type) {
	case *ast.Literal, *ast.ColumnRef, *ast.FuncCall, *ast.ParenExpr, *ast.Placeholder:
		return e
	default:
		return &ast.ParenExpr{Expr: e, Span: e.At()}
	}
}

// literalText reads the text of a string literal argument, if the expression is
// one.
func literalText(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.Literal)
	if !ok || lit.Kind != ast.LitString {
		return "", false
	}
	return lit.Value, true
}
