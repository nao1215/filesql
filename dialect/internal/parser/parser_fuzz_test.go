package parser

import (
	"errors"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// FuzzParse checks the properties that have to hold for any input at all, since
// a query can arrive from anywhere: parsing never panics, never runs away, is
// deterministic, and either fails with one of this package's errors or answers
// a tree whose invariants hold.
func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		"SELECT * FROM t",
		"SELECT `a`, \"b\" FROM `t` WHERE x = 'v'",
		"SELECT a::int FROM t -- c",
		"SELECT $$d$$, E'e\\n', $1",
		"SELECT r'raw', b'AB', `p.d.t`.x # h",
		"SELECT count(*) /* mid */ FROM t;",
		"SELECT a + b * c, (a + b) * c, a OR b AND c, NOT a = b",
		"SELECT CASE WHEN a > 1 THEN b ELSE c END",
		"WITH x AS (SELECT 1 AS n) SELECT n FROM x JOIN y USING (n)",
		"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2",
		"UPDATE t SET a = 1 WHERE b IN (SELECT c FROM d)",
		"CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT NOT NULL)",
		"SELECT a FROM t ORDER BY a NULLS FIRST LIMIT 1 OFFSET 2",
		"SELECT sum(a) OVER (PARTITION BY b ORDER BY c ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
		"'unterminated",
		"/* unterminated",
		"((((((((((1))))))))))",
		"((((((((((SELECT 1))))))))))",
		"EXPLAIN EXPLAIN EXPLAIN SELECT 1",
		"SELECT U&'\\+41424'",
		"SELECT 1 UNION SELECT 2 INTERSECT SELECT 3 EXCEPT SELECT 4",
		"CREATE TABLE t (a INT REFERENCES u (id) ON DELETE SET NULL)",
		"SELECT a FROM t WHERE a LIKE 'x!%' ESCAPE '!'",
		"",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, query string) {
		for _, d := range dialects.All() {
			if d == dialects.SQLite {
				continue
			}
			stmt, err := Parse(d, query)
			if err != nil {
				if !knownError(err) {
					t.Fatalf("Parse(%s, %q) failed with an error of an unknown kind: %v", d, query, err)
				}
				continue
			}
			again, err := Parse(d, query)
			if err != nil {
				t.Fatalf("Parse(%s, %q) succeeded then failed: %v", d, query, err)
			}
			if stmt == nil || again == nil {
				t.Fatalf("Parse(%s, %q) answered a nil statement and no error", d, query)
			}
			checkInvariants(t, d, query, stmt)
		}
	})
}

// knownError reports whether an error is one this package promises to return.
// Anything else means a failure reached the caller unlabeled.
func knownError(err error) bool {
	return errors.Is(err, sqlerr.ErrInvalidSyntax) ||
		errors.Is(err, sqlerr.ErrUnsupportedSyntax) ||
		errors.Is(err, sqlerr.ErrUnsupportedFeature) ||
		errors.Is(err, dialects.ErrUnknownDialect)
}

// checkInvariants holds the tree to what the rest of the pipeline may assume: a
// node is never nil where a node is required, and a span points somewhere.
func checkInvariants(t *testing.T, d dialects.Dialect, query string, stmt ast.Stmt) {
	t.Helper()
	fail := func(what string) {
		t.Fatalf("Parse(%s, %q) answered a tree where %s", d, query, what)
	}
	ast.WalkSelectCores(stmt, func(core *ast.SelectCore) {
		if len(core.Items) == 0 {
			fail("a SELECT has no result columns")
		}
		for _, item := range core.Items {
			if item.Expr == nil {
				fail("a result column has no expression")
			}
			if item.Span.Line <= 0 {
				fail("a result column has no source position")
			}
			// The text is not trimmed: a name made of a Unicode space is a
			// name, since the lexer separates tokens on ASCII whitespace the
			// way the source engines do.
			if item.Source == "" {
				fail("a result column has no source text")
			}
		}
	})
}

// FuzzLex checks the lexer's own properties through the parser's entry point:
// an input that lexes must produce tokens whose offsets do not run backwards,
// which is what every position in a diagnostic depends on.
func FuzzLex(f *testing.F) {
	for _, seed := range []string{
		"SELECT 1", "'a''b'", "x'41'", "$tag$body$tag$", "-- c\nSELECT 1",
		"/* a /* b */ c */ SELECT 1", "0x41 0b1010 1e5 .5", "@name :name ?1 $1",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, query string) {
		for _, d := range dialects.All() {
			if d == dialects.SQLite {
				continue
			}
			p, err := newForTest(d, query)
			if err != nil {
				continue
			}
			last := -1
			for _, tok := range p.toks {
				if tok.Offset < last {
					t.Fatalf("Lex(%s, %q) produced tokens out of order at offset %d", d, query, tok.Offset)
				}
				if tok.Line <= 0 || tok.Col <= 0 {
					t.Fatalf("Lex(%s, %q) produced a token with no position", d, query)
				}
				last = tok.Offset
			}
		}
	})
}
