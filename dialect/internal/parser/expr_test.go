package parser

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// sketch draws the shape of an expression tree, so a test can assert what the
// parser built rather than what it would be written back as. Writing it back
// would test the renderer, and a renderer that adds a parenthesis for every
// node would make a wrong tree look right.
func sketch(e ast.Expr) string {
	switch n := e.(type) {
	case nil:
		return "<nil>"
	case *ast.Literal:
		switch n.Kind {
		case ast.LitNull:
			return "NULL"
		case ast.LitString:
			return "'" + n.Value + "'"
		default:
			return n.Value
		}
	case *ast.ColumnRef:
		parts := make([]string, 0, len(n.Parts))
		for _, p := range n.Parts {
			parts = append(parts, p.Name)
		}
		return strings.Join(parts, ".")
	case *ast.Star:
		return "*"
	case *ast.Placeholder:
		return n.Text
	case *ast.Keyword:
		return n.Name
	case *ast.ParenExpr:
		return "paren(" + sketch(n.Expr) + ")"
	case *ast.UnaryExpr:
		return fmt.Sprintf("%s(%s)", unaryName(n.Op), sketch(n.Expr))
	case *ast.BinaryExpr:
		return fmt.Sprintf("%s(%s, %s)", binaryName(n.Op), sketch(n.Left), sketch(n.Right))
	case *ast.IsExpr:
		name := "is"
		if n.Negated {
			name = "isnot"
		}
		return fmt.Sprintf("%s(%s, %s)", name, sketch(n.Expr), sketch(n.Right))
	case *ast.BetweenExpr:
		return fmt.Sprintf("between(%s, %s, %s)", sketch(n.Expr), sketch(n.Low), sketch(n.High))
	case *ast.InExpr:
		parts := make([]string, 0, len(n.List))
		for _, e := range n.List {
			parts = append(parts, sketch(e))
		}
		return fmt.Sprintf("in(%s, [%s])", sketch(n.Expr), strings.Join(parts, " "))
	case *ast.CaseExpr:
		var b strings.Builder
		b.WriteString("case(")
		if n.Operand != nil {
			b.WriteString(sketch(n.Operand) + " ")
		}
		for _, w := range n.Whens {
			b.WriteString("when(" + sketch(w.Cond) + ", " + sketch(w.Result) + ")")
		}
		if n.Else != nil {
			b.WriteString(" else(" + sketch(n.Else) + ")")
		}
		b.WriteString(")")
		return b.String()
	case *ast.CastExpr:
		return fmt.Sprintf("cast(%s, %s)", sketch(n.Expr), n.Type.Name)
	case *ast.CollateExpr:
		return fmt.Sprintf("collate(%s, %s)", sketch(n.Expr), n.Collation)
	case *ast.IntervalExpr:
		return fmt.Sprintf("interval(%s, %s)", sketch(n.Value), n.Unit)
	case *ast.TypedLiteral:
		return fmt.Sprintf("typed(%s, '%s')", n.Type, n.Value)
	case *ast.FuncCall:
		parts := make([]string, 0, len(n.Args))
		for _, a := range n.Args {
			parts = append(parts, sketch(a))
		}
		name := make([]string, 0, len(n.Name))
		for _, id := range n.Name {
			name = append(name, id.Name)
		}
		return fmt.Sprintf("call(%s, [%s])", strings.Join(name, "."), strings.Join(parts, " "))
	case *ast.SubqueryExpr:
		return "subquery"
	case *ast.ExistsExpr:
		return "exists"
	case *ast.RowExpr:
		parts := make([]string, 0, len(n.Exprs))
		for _, e := range n.Exprs {
			parts = append(parts, sketch(e))
		}
		return "row(" + strings.Join(parts, " ") + ")"
	case *ast.QuantifiedExpr:
		return fmt.Sprintf("quantified(%s, %s)", binaryName(n.Op), sketch(n.Left))
	default:
		return fmt.Sprintf("%T", e)
	}
}

func unaryName(op ast.UnaryOp) string {
	switch op {
	case ast.UnaryPlus:
		return "pos"
	case ast.UnaryMinus:
		return "neg"
	case ast.UnaryNot:
		return "not"
	case ast.UnaryBitNot:
		return "bitnot"
	default:
		return "unary"
	}
}

func binaryName(op ast.BinaryOp) string {
	names := map[ast.BinaryOp]string{
		ast.Add: "add", ast.Sub: "sub", ast.Mul: "mul", ast.Div: "div", ast.Mod: "mod",
		ast.IntDiv: "intdiv", ast.Power: "pow", ast.Eq: "eq", ast.NotEq: "ne",
		ast.Lt: "lt", ast.Lte: "le", ast.Gt: "gt", ast.Gte: "ge",
		ast.NullSafeEq: "nulleq", ast.And: "and", ast.Or: "or", ast.Xor: "xor",
		ast.BitAnd: "bitand", ast.BitOr: "bitor", ast.BitXor: "bitxor",
		ast.ShiftLeft: "shl", ast.ShiftRight: "shr", ast.Concat: "concat",
		ast.Like: "like", ast.NotLike: "notlike", ast.ILike: "ilike",
		ast.SimilarTo: "similar", ast.Regexp: "regexp", ast.NotRegexp: "notregexp",
		ast.RegexpCI: "regexpci", ast.SoundsLike: "sounds", ast.MemberOf: "memberof",
		ast.JSONGet: "jsonget", ast.JSONGetText: "jsongettext",
	}
	if name, ok := names[op]; ok {
		return name
	}
	return "op"
}

// TestPrecedenceAndAssociativity asserts the shape of the tree for the
// expressions whose meaning depends on what binds tighter than what. The whole
// of that decision is one table in this package, and these are the cases that
// table exists for.
func TestPrecedenceAndAssociativity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dialect dialects.Dialect
		input   string
		want    string
	}{
		{dialects.PostgreSQL, "a + b * c", "add(a, mul(b, c))"},
		{dialects.PostgreSQL, "(a + b) * c", "mul(paren(add(a, b)), c)"},
		{dialects.PostgreSQL, "a - b - c", "sub(sub(a, b), c)"},
		{dialects.PostgreSQL, "a - (b - c)", "sub(a, paren(sub(b, c)))"},
		{dialects.PostgreSQL, "a OR b AND c", "or(a, and(b, c))"},
		{dialects.PostgreSQL, "a AND b OR c", "or(and(a, b), c)"},
		{dialects.PostgreSQL, "NOT a = b", "not(eq(a, b))"},
		{dialects.PostgreSQL, "NOT a AND b", "and(not(a), b)"},
		{dialects.PostgreSQL, "a = b AND c = d", "and(eq(a, b), eq(c, d))"},
		{dialects.PostgreSQL, "-a + b", "add(neg(a), b)"},
		{dialects.PostgreSQL, "a || b || c", "concat(concat(a, b), c)"},
		{dialects.PostgreSQL, "a < b + 1", "lt(a, add(b, 1))"},
		{dialects.PostgreSQL, "a::int + b", "add(cast(a, INT), b)"},
		{dialects.PostgreSQL, "f(a + b, g(c))", "call(f, [add(a, b) call(g, [c])])"},
		{dialects.PostgreSQL, "CASE WHEN a > 1 THEN b ELSE c END", "case(when(gt(a, 1), b) else(c))"},
		{dialects.PostgreSQL, "a BETWEEN 1 AND 2 AND b", "and(between(a, 1, 2), b)"},
		{dialects.PostgreSQL, "a IN (1, 2) OR b", "or(in(a, [1 2]), b)"},
		{dialects.PostgreSQL, "a IS NULL AND b", "and(is(a, NULL), b)"},
		{dialects.PostgreSQL, "a IS NOT NULL", "isnot(a, NULL)"},
		{dialects.PostgreSQL, "a LIKE 'x%' AND b", "and(like(a, 'x%'), b)"},

		// PostgreSQL raises to a power with "^", above multiplication; MySQL
		// and GoogleSQL take a bitwise XOR with the same character, and MySQL
		// binds it tighter still.
		{dialects.PostgreSQL, "a ^ b * c", "mul(pow(a, b), c)"},
		{dialects.MySQL, "a ^ b * c", "mul(bitxor(a, b), c)"},
		{dialects.GoogleSQL, "a ^ b | c", "bitor(bitxor(a, b), c)"},

		// MySQL's XOR sits between OR and AND, which no other dialect has a
		// level for.
		{dialects.MySQL, "a OR b XOR c", "or(a, xor(b, c))"},
		{dialects.MySQL, "a XOR b AND c", "xor(a, and(b, c))"},

		// MySQL's "!" is NOT bound tighter than any arithmetic, so it takes
		// only the operand next to it.
		{dialects.MySQL, "!a ^ b", "bitxor(not(a), b)"},
		{dialects.MySQL, "a DIV b + c", "add(intdiv(a, b), c)"},
		{dialects.MySQL, "a + b DIV c", "add(a, intdiv(b, c))"},

		// The shifts bind looser than addition and tighter than the bitwise
		// operators.
		{dialects.MySQL, "a << b + c", "shl(a, add(b, c))"},
		{dialects.MySQL, "a & b << c", "bitand(a, shl(b, c))"},

		// An interval takes a whole expression for its amount and stops at the
		// unit, which is a bare word rather than an operator.
		{dialects.MySQL, "d + INTERVAL 1 + 1 DAY", "add(d, interval(add(1, 1), DAY))"},
	}
	for _, tt := range tests {
		t.Run(tt.dialect.DisplayName()+" "+tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := ParseExpr(tt.dialect, tt.input)
			if err != nil {
				t.Fatalf("ParseExpr(%s, %q): %v", tt.dialect, tt.input, err)
			}
			if sketch(got) != tt.want {
				t.Errorf("ParseExpr(%s, %q) = %s, want %s", tt.dialect, tt.input, sketch(got), tt.want)
			}
		})
	}
}

// TestExpressionsThatCannotBeRead covers the inputs that stop in the middle,
// which are refused with a position rather than accepted with a guess.
func TestExpressionsThatCannotBeRead(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		"a +", "a AND", "(a", "a)", "CASE a END", "CASE WHEN a THEN", "f(", "f(a,",
		"a BETWEEN 1", "a IN", "a IS", "1 +* 2", "a ~", ".", "",
	} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			if _, err := ParseExpr(dialects.PostgreSQL, input); !errors.Is(err, sqlerr.ErrInvalidSyntax) {
				t.Errorf("ParseExpr(%q) error = %v, want ErrInvalidSyntax", input, err)
			}
		})
	}
}

// TestDiagnosticsCarryTheirPosition holds the parser to reporting where a
// construct was written. A translation runs on text a person typed, and a
// message with no position sends them looking through the whole query.
func TestDiagnosticsCarryTheirPosition(t *testing.T) {
	t.Parallel()

	_, err := Parse(dialects.PostgreSQL, "SELECT a,\n       b +\nFROM t")
	if err == nil {
		t.Fatal("a query that stops in the middle should be refused")
	}
	if !strings.Contains(err.Error(), "line 3") {
		t.Errorf("error = %v, want it to name the line the construct is on", err)
	}
}

// TestNestingIsBounded keeps a query that nests without end from exhausting the
// stack. SQL can arrive from anywhere, so the depth is a refusal rather than a
// crash.
func TestNestingIsBounded(t *testing.T) {
	t.Parallel()

	deep := strings.Repeat("(", 5000) + "1" + strings.Repeat(")", 5000)
	if _, err := ParseExpr(dialects.PostgreSQL, deep); !errors.Is(err, sqlerr.ErrInvalidSyntax) {
		t.Errorf("a deeply nested expression error = %v, want ErrInvalidSyntax", err)
	}
}

// TestPostgresOtherOperatorPrecedence pins the level PostgreSQL's grammar calls
// "any other operator": below addition, above the pattern predicates, and one
// level for all of them. Read at the level the other two dialects give these
// operators, "a || b * c" would have concatenated first and multiplied the
// result.
func TestPostgresOtherOperatorPrecedence(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ input, want string }{
		{"a || b * c", "concat(a, mul(b, c))"},
		{"a || b + c", "concat(a, add(b, c))"},
		{"a # b + c", "bitxor(a, add(b, c))"},
		{"a | b & c", "bitand(bitor(a, b), c)"},
		{"a << b + c", "shl(a, add(b, c))"},
		{"a -> b || c", "concat(jsonget(a, b), c)"},
		{"a || b = c", "eq(concat(a, b), c)"},
		{"a || b LIKE c", "like(concat(a, b), c)"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := ParseExpr(dialects.PostgreSQL, tt.input)
			if err != nil {
				t.Fatalf("ParseExpr(%q): %v", tt.input, err)
			}
			if sketch(got) != tt.want {
				t.Errorf("ParseExpr(%q) = %s, want %s", tt.input, sketch(got), tt.want)
			}
		})
	}

	// BigQuery concatenates at the level it multiplies at, so the two group
	// left to right.
	got, err := ParseExpr(dialects.GoogleSQL, "a || b * c")
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	if sketch(got) != "mul(concat(a, b), c)" {
		t.Errorf("ParseExpr(googlesql, %q) = %s, want mul(concat(a, b), c)", "a || b * c", sketch(got))
	}
}

// TestAShortUnicodeEscapeIsRefused covers the literal that used to read past the
// end of the string and crash the parser. A query is untrusted text, so a
// malformed escape has to be an error rather than a panic.
func TestAShortUnicodeEscapeIsRefused(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		`SELECT U&'\+41424'`, `SELECT U&'\+4142'`, `SELECT U&'\+'`, `SELECT U&'\41'`, `SELECT U&'\'`,
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()

			if _, err := Parse(dialects.PostgreSQL, query); err == nil {
				t.Errorf("Parse(%q) succeeded, want a refusal", query)
			}
		})
	}

	// The well-formed spellings still read.
	for _, tt := range []struct{ query, want string }{
		{`SELECT U&'\0041'`, "'A'"},
		{`SELECT U&'\+000041'`, "'A'"},
	} {
		stmt := mustParse(t, tt.query)
		sel, ok := stmt.(*ast.SelectStmt)
		if !ok {
			t.Fatalf("statement is %T, want a SELECT", stmt)
		}
		core, ok := sel.Body.(*ast.SelectCore)
		if !ok {
			t.Fatalf("body is %T, want a SELECT core", sel.Body)
		}
		if got := sketch(core.Items[0].Expr); got != tt.want {
			t.Errorf("Parse(%q) = %s, want %s", tt.query, got, tt.want)
		}
	}
}

// TestRecursionIsBoundedEverywhere covers the two recursive paths that had no
// depth guard. A stack overflow is fatal and cannot be recovered from, so every
// path that recurses once per token has to stop.
func TestRecursionIsBoundedEverywhere(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ name, query string }{
		{"nested parenthesized queries", strings.Repeat("(", 5000) + "SELECT 1" + strings.Repeat(")", 5000)},
		{"repeated EXPLAIN", strings.Repeat("EXPLAIN ", 5000) + "SELECT 1"},
		{"nested subqueries", strings.Repeat("SELECT (", 2000) + "1" + strings.Repeat(")", 2000)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := Parse(dialects.PostgreSQL, tt.query); err == nil {
				t.Error("a query that nests without end should be refused")
			}
		})
	}
}

// TestTheUnicodeEscapeFormsPostgresReads covers the U&'...' literal: a code
// point in the basic plane, one outside it, a surrogate pair written as its two
// halves, the doubled escape, and a UESCAPE clause naming another character.
// The halves have to be read together, since each on its own is not a character.
func TestTheUnicodeEscapeFormsPostgresReads(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ query, want string }{
		{`SELECT U&'\0041'`, "'A'"},
		{`SELECT U&'\+000041'`, "'A'"},
		{`SELECT U&'\0041\0042'`, "'AB'"},
		{`SELECT U&'x\0041y'`, "'xAy'"},
		{`SELECT U&'\\'`, `'\'`},
		{`SELECT U&'\+01F600'`, "'😀'"},
		// The same character written as the two halves of a surrogate pair.
		{`SELECT U&'\D83D\DE00'`, "'😀'"},
		{`SELECT U&'d!0061t!+000061' UESCAPE '!'`, "'data'"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			stmt, err := Parse(dialects.PostgreSQL, tt.query)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.query, err)
			}
			sel, ok := stmt.(*ast.SelectStmt)
			if !ok {
				t.Fatalf("statement is %T, want a SELECT", stmt)
			}
			core, ok := sel.Body.(*ast.SelectCore)
			if !ok {
				t.Fatalf("body is %T, want a SELECT core", sel.Body)
			}
			if got := sketch(core.Items[0].Expr); got != tt.want {
				t.Errorf("Parse(%q) = %s, want %s", tt.query, got, tt.want)
			}
		})
	}

	// A half with no partner names no character, and the zero character is not
	// one a string can hold; PostgreSQL refuses both.
	for _, query := range []string{
		`SELECT U&'\D83D'`, `SELECT U&'\DE00'`, `SELECT U&'\D83D\0041'`,
		`SELECT U&'\0000'`, `SELECT U&'\+000000'`, `SELECT U&'x' UESCAPE 'ab'`,
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()

			if _, err := Parse(dialects.PostgreSQL, query); err == nil {
				t.Errorf("Parse(%q) succeeded, want a refusal", query)
			}
		})
	}
}
