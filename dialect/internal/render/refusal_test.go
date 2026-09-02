package render

import (
	"errors"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// TestTheRendererRefusesWhatLoweringShouldHaveRemoved builds the nodes SQLite
// has no spelling for and holds the renderer to refusing them. Reaching one of
// these means the lowering layer left something behind, and the renderer is the
// last place that can tell: written out on a guess, the result would be SQL
// that either does not parse or means something else.
func TestTheRendererRefusesWhatLoweringShouldHaveRemoved(t *testing.T) {
	t.Parallel()

	span := ast.Span{Line: 1, Col: 1}
	column := &ast.ColumnRef{Parts: []ast.Ident{{Name: "a", Span: span}}, Span: span}
	other := &ast.ColumnRef{Parts: []ast.Ident{{Name: "b", Span: span}}, Span: span}

	tests := []struct {
		name string
		expr ast.Expr
	}{
		{"the DIV operator", &ast.BinaryExpr{Left: column, Op: ast.IntDiv, Right: other, Span: span}},
		{"XOR", &ast.BinaryExpr{Left: column, Op: ast.Xor, Right: other, Span: span}},
		{"the null-safe comparison", &ast.BinaryExpr{Left: column, Op: ast.NullSafeEq, Right: other, Span: span}},
		{"a bitwise XOR", &ast.BinaryExpr{Left: column, Op: ast.BitXor, Right: other, Span: span}},
		{"ILIKE", &ast.BinaryExpr{Left: column, Op: ast.ILike, Right: other, Span: span}},
		{"SIMILAR TO", &ast.BinaryExpr{Left: column, Op: ast.SimilarTo, Right: other, Span: span}},
		{"a case-insensitive match", &ast.BinaryExpr{Left: column, Op: ast.RegexpCI, Right: other, Span: span}},
		{"SOUNDS LIKE", &ast.BinaryExpr{Left: column, Op: ast.SoundsLike, Right: other, Span: span}},
		{"MEMBER OF", &ast.BinaryExpr{Left: column, Op: ast.MemberOf, Right: other, Span: span}},
		{"raising to a power", &ast.BinaryExpr{Left: column, Op: ast.Power, Right: other, Span: span}},
		{"a JSON containment test", &ast.BinaryExpr{Left: column, Op: ast.JSONContains, Right: other, Span: span}},
		{"a prefix regular-expression operator", &ast.UnaryExpr{Op: ast.UnaryRegexpMatch, Expr: column, Span: span}},
		{"IS DISTINCT FROM", &ast.IsExpr{Expr: column, Right: other, Distinct: true, Span: span}},
		{"BETWEEN SYMMETRIC", &ast.BetweenExpr{Expr: column, Low: other, High: other, Symmetric: true, Span: span}},
		{"a quantified comparison", &ast.QuantifiedExpr{Left: column, Op: ast.Eq, Quant: ast.QuantAll, Span: span}},
		{"an interval", &ast.IntervalExpr{Value: column, Unit: "DAY", Span: span}},
		{"a typed literal", &ast.TypedLiteral{Type: "DATE", Value: "2024-01-01", Span: span}},
		{"an array", &ast.ArrayExpr{Span: span}},
		{"a subscript", &ast.SubscriptExpr{Expr: column, Index: other, Span: span}},
		{"a struct", &ast.StructExpr{Span: span}},
		{"a cast that answers NULL", &ast.CastExpr{
			Expr: column, Type: ast.TypeName{Name: "INTEGER", Span: span}, TryCast: true, Span: span,
		}},
		{"a qualified function name", &ast.FuncCall{
			Name: []ast.Ident{{Name: "safe", Span: span}, {Name: "f", Span: span}}, Span: span,
		}},
		{"an aggregate separator", &ast.FuncCall{
			Name: []ast.Ident{{Name: "group_concat", Span: span}}, Args: []ast.Expr{column},
			Separator: other, Span: span,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &writer{}
			err := w.expr(tt.expr, precLowest)
			if !errors.Is(err, sqlerr.ErrUnsupportedSyntax) {
				t.Errorf("rendering %s: error = %v, want ErrUnsupportedSyntax", tt.name, err)
			}
		})
	}
}

// TestTheRendererRefusesUnrenderableStatements does the same for the statement
// clauses that have no SQLite form.
func TestTheRendererRefusesUnrenderableStatements(t *testing.T) {
	t.Parallel()

	span := ast.Span{Line: 1, Col: 1}
	name := &ast.TableName{Parts: []ast.Ident{{Name: "t", Span: span}}, Span: span}
	column := &ast.ColumnRef{Parts: []ast.Ident{{Name: "a", Span: span}}, Span: span}
	core := func(mutate func(*ast.SelectCore)) *ast.SelectStmt {
		body := &ast.SelectCore{Items: []ast.SelectItem{{Expr: column, Span: span}}, Span: span}
		mutate(body)
		return &ast.SelectStmt{Body: body, Span: span}
	}

	tests := []struct {
		name string
		stmt ast.Stmt
	}{
		{"DISTINCT ON", core(func(c *ast.SelectCore) { c.DistinctOn = []ast.Expr{column} })},
		{"GROUP BY ALL", core(func(c *ast.SelectCore) { c.GroupByAll = true })},
		{"a grouping set", core(func(c *ast.SelectCore) {
			c.Grouping = &ast.GroupingClause{Kind: ast.GroupingRollup, Span: span}
		})},
		{"QUALIFY", core(func(c *ast.SelectCore) { c.Qualify = column })},
		{"a DROP naming two objects", &ast.DropStmt{Kind: ast.DropTable, Names: []*ast.TableName{name, name}, Span: span}},
		{"DELETE ... USING", &ast.DeleteStmt{Table: name, Using: []ast.TableExpr{name}, Span: span}},
		{"FETCH WITH TIES", &ast.SelectStmt{
			Body: &ast.SelectCore{Items: []ast.SelectItem{{Expr: column, Span: span}}, Span: span},
			Limit: &ast.LimitClause{
				Count: &ast.Literal{Kind: ast.LitNumber, Value: "1", Span: span}, WithTies: true, Span: span,
			},
			Span: span,
		}},
		{"ORDER BY ... USING", &ast.SelectStmt{
			Body:    &ast.SelectCore{Items: []ast.SelectItem{{Expr: column, Span: span}}, Span: span},
			OrderBy: []ast.OrderTerm{{Expr: column, Using: "<", Span: span}},
			Span:    span,
		}},
		{"an auto-numbered column that is not the key", &ast.CreateTableStmt{
			Name: name,
			Columns: []ast.ColumnDef{{
				Name:        "a",
				Constraints: []ast.ColumnConstraint{{Kind: ast.ConstraintAutoIncrement, Span: span}},
				Span:        span,
			}},
			Span: span,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := Render(tt.stmt); !errors.Is(err, sqlerr.ErrUnsupportedSyntax) {
				t.Errorf("rendering %s: error = %v, want ErrUnsupportedSyntax", tt.name, err)
			}
		})
	}
}

// TestRenderingTheSpellingsEachNodeHas covers the branches a translation test
// reaches only one side of: every frame bound and exclusion, every join, the
// literal kinds, and the names that need quoting.
func TestRenderingTheSpellingsEachNodeHas(t *testing.T) {
	t.Parallel()

	span := ast.Span{Line: 1, Col: 1}
	one := &ast.Literal{Kind: ast.LitNumber, Value: "1", Span: span}

	t.Run("every frame bound", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			bound ast.FrameBound
			want  string
		}{
			{ast.FrameBound{Kind: ast.BoundUnboundedPreceding, Span: span}, "UNBOUNDED PRECEDING"},
			{ast.FrameBound{Kind: ast.BoundUnboundedFollowing, Span: span}, "UNBOUNDED FOLLOWING"},
			{ast.FrameBound{Kind: ast.BoundCurrentRow, Span: span}, "CURRENT ROW"},
			{ast.FrameBound{Kind: ast.BoundPreceding, Offset: one, Span: span}, "1 PRECEDING"},
			{ast.FrameBound{Kind: ast.BoundFollowing, Offset: one, Span: span}, "1 FOLLOWING"},
		} {
			w := &writer{}
			if err := w.frameBound(tt.bound); err != nil {
				t.Fatalf("frameBound: %v", err)
			}
			if w.b.String() != tt.want {
				t.Errorf("frameBound = %q, want %q", w.b.String(), tt.want)
			}
		}
	})

	t.Run("every frame exclusion", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			exclude ast.FrameExclusion
			want    string
		}{
			{ast.ExcludeNone, "ROWS CURRENT ROW"},
			{ast.ExcludeCurrentRow, "ROWS CURRENT ROW EXCLUDE CURRENT ROW"},
			{ast.ExcludeGroup, "ROWS CURRENT ROW EXCLUDE GROUP"},
			{ast.ExcludeTies, "ROWS CURRENT ROW EXCLUDE TIES"},
			{ast.ExcludeNoOthers, "ROWS CURRENT ROW EXCLUDE NO OTHERS"},
		} {
			w := &writer{}
			frame := &ast.WindowFrame{
				Unit:    ast.FrameRows,
				Start:   ast.FrameBound{Kind: ast.BoundCurrentRow, Span: span},
				Exclude: tt.exclude,
				Span:    span,
			}
			if err := w.frame(frame); err != nil {
				t.Fatalf("frame: %v", err)
			}
			if w.b.String() != tt.want {
				t.Errorf("frame = %q, want %q", w.b.String(), tt.want)
			}
		}
	})

	t.Run("every literal kind", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			lit  *ast.Literal
			want string
		}{
			{&ast.Literal{Kind: ast.LitNumber, Value: "1.5", Span: span}, "1.5"},
			{&ast.Literal{Kind: ast.LitString, Value: "it's", Span: span}, "'it''s'"},
			{&ast.Literal{Kind: ast.LitBlob, Value: "4142", Span: span}, "x'4142'"},
			{&ast.Literal{Kind: ast.LitNull, Span: span}, "NULL"},
			{&ast.Literal{Kind: ast.LitBool, Value: "TRUE", Span: span}, "TRUE"},
			{&ast.Literal{Kind: ast.LitHex, Value: "0x41", Span: span}, "0x41"},
		} {
			w := &writer{}
			if err := w.expr(tt.lit, precLowest); err != nil {
				t.Fatalf("literal: %v", err)
			}
			if w.b.String() != tt.want {
				t.Errorf("literal = %q, want %q", w.b.String(), tt.want)
			}
		}
	})

	t.Run("a name is quoted only when it has to be", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct{ name, want string }{
			{"a", "a"},
			{"_a1", "_a1"},
			{"select", `"select"`},
			{"a b", `"a b"`},
			{"1a", `"1a"`},
			{"", `""`},
			{`a"b`, `"a""b"`},
		} {
			if got := quoteIfNeeded(tt.name); got != tt.want {
				t.Errorf("quoteIfNeeded(%q) = %q, want %q", tt.name, got, tt.want)
			}
		}
	})

	t.Run("a star and a qualified star", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			star *ast.Star
			want string
		}{
			{&ast.Star{Span: span}, "*"},
			{&ast.Star{Qualifier: []ast.Ident{{Name: "t", Span: span}}, Span: span}, "t.*"},
		} {
			w := &writer{}
			if err := w.expr(tt.star, precLowest); err != nil {
				t.Fatalf("star: %v", err)
			}
			if w.b.String() != tt.want {
				t.Errorf("star = %q, want %q", w.b.String(), tt.want)
			}
		}
	})
}

// TestTheRendererRefusesTextSQLiteCannotSpell holds the renderer to refusing a
// NUL byte inside a string literal or a name. SQLite reads a statement as a C
// string and ends it at the first NUL, so no SQL text can carry one; the MySQL
// and GoogleSQL escape \0 decodes to exactly that byte, and writing it out
// produced SQL that reported success here and failed later with an error about
// the opening quote. Every other control character those escapes produce goes
// through SQLite's tokenizer unchanged, which is the boundary the last two
// cases hold.
func TestTheRendererRefusesTextSQLiteCannotSpell(t *testing.T) {
	t.Parallel()

	span := ast.Span{Line: 1, Col: 1}
	item := func(e ast.Expr) *ast.SelectStmt {
		return &ast.SelectStmt{
			Body: &ast.SelectCore{Items: []ast.SelectItem{{Expr: e, Span: span}}, Span: span},
			Span: span,
		}
	}
	str := func(v string) ast.Expr { return &ast.Literal{Kind: ast.LitString, Value: v, Span: span} }
	ident := func(v string) ast.Expr {
		return &ast.ColumnRef{Parts: []ast.Ident{{Name: v, Span: span}}, Span: span}
	}

	for _, tt := range []struct {
		name    string
		stmt    ast.Stmt
		refused bool
	}{
		{"a string literal holding a NUL", item(str("a\x00b")), true},
		{"a string literal that is only a NUL", item(str("\x00")), true},
		{"a name holding a NUL", item(ident("a\x00b")), true},
		{"a string literal holding the substitute character", item(str("a\x1ab")), false},
		{"a string literal holding a backspace", item(str("a\bb")), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := Render(tt.stmt)
			if tt.refused {
				if !errors.Is(err, sqlerr.ErrUnsupportedSyntax) {
					t.Errorf("rendering %s: error = %v, want ErrUnsupportedSyntax", tt.name, err)
				}
				return
			}
			if err != nil {
				t.Errorf("rendering %s: error = %v, want it to render", tt.name, err)
			}
		})
	}
}

// TestRenderWritesABitStringAsItsDigits covers the one literal kind whose
// rendered form is not the form it was written in: PostgreSQL compares and
// concatenates a bit string as the text of its digits, and that is what the
// value has to reach SQLite as. The other two dialects refuse the literal while
// lowering, so one that reaches the renderer is PostgreSQL's.
func TestRenderWritesABitStringAsItsDigits(t *testing.T) {
	t.Parallel()

	span := ast.Span{}
	for _, tt := range []struct{ value, want string }{
		{"1010", "'1010'"},
		{"01000001", "'01000001'"},
		{"", "''"},
	} {
		w := &writer{}
		if err := w.expr(&ast.Literal{Kind: ast.LitBit, Value: tt.value, Span: span}, precLowest); err != nil {
			t.Fatalf("rendering B'%s': %v", tt.value, err)
		}
		if got := w.b.String(); got != tt.want {
			t.Errorf("rendering B'%s' = %q, want %q", tt.value, got, tt.want)
		}
	}
}
