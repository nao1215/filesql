package render_test

import (
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/parser"
	"github.com/nao1215/filesql/dialect/internal/render"
)

// TestRenderReparsesToTheSameShape holds the renderer to the property that
// makes it safe: what it writes reads back as the same query. A renderer that
// drops a parenthesis or fuses two tokens produces SQL that parses into
// something else, and nothing downstream would notice.
func TestRenderReparsesToTheSameShape(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		"SELECT 1",
		"SELECT a + b * c FROM t",
		"SELECT (a + b) * c FROM t",
		"SELECT a - (b - c) FROM t",
		"SELECT a FROM t WHERE a > 1 AND (b < 2 OR c = 3)",
		"SELECT NOT (a AND b) FROM t",
		"SELECT CASE WHEN a THEN b ELSE c END FROM t",
		"SELECT count(*) FROM t GROUP BY a HAVING count(*) > 1",
		"SELECT a FROM t ORDER BY a DESC NULLS LAST LIMIT 1 OFFSET 2",
		"SELECT sum(a) OVER (PARTITION BY b ORDER BY c ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
		"WITH w AS (SELECT 1 AS n) SELECT n FROM w",
		"SELECT a FROM t JOIN u ON t.id = u.id LEFT JOIN v USING (id)",
		"SELECT 1 UNION ALL SELECT 2",
		"INSERT INTO t (a, b) VALUES (1, 2)",
		"UPDATE t SET a = 1 WHERE b = 2",
		"DELETE FROM t WHERE a = 1",
		"CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT NOT NULL)",
		"CREATE INDEX i ON t (a DESC)",
		`SELECT "select" FROM "from"`,
		"SELECT 'it''s' FROM t",
		"SELECT -1, +2, 1e5, .5 FROM t",
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()

			first := mustRender(t, query)
			second := mustRender(t, first)
			if first != second {
				t.Errorf("rendering is not stable:\n  once  %q\n  twice %q", first, second)
			}
		})
	}
}

// mustRender reads a query as SQLite and writes it back.
func mustRender(t *testing.T, query string) string {
	t.Helper()
	// SQLite is the identity translation, so its own lexical rules are read
	// with the PostgreSQL configuration, which spells identifiers and strings
	// the same way SQLite does.
	stmt, err := parser.Parse(dialects.PostgreSQL, query)
	if err != nil {
		t.Fatalf("Parse(%q): %v", query, err)
	}
	out, err := render.Render(stmt)
	if err != nil {
		t.Fatalf("Render(%q): %v", query, err)
	}
	return out
}

// TestRenderSeparatesTokensThatWouldFuse covers the pairs that read as one
// token when written together: a name against a quoted name, two operator
// characters, and the two that open a comment.
func TestRenderSeparatesTokensThatWouldFuse(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ query, want string }{
		{"SELECT a - -1 FROM t", "- -1"},
		// "a--1" is a column and a comment, which is what SQLite reads too.
		{"SELECT 1 - -2", "- -2"},
		// A number may end in its decimal point, and the word after it is a
		// word: "1.FROM" is one token to SQLite and not one it knows.
		{"SELECT 1. FROM t", "1. FROM"},
		{"SELECT a FROM t WHERE c > 1. AND a = 1", "1. AND"},
		{"SELECT 1. AS n", "1. AS"},
		// The dot that qualifies a name still binds to what follows it, which
		// is the rule the one above must not be fixed by loosening.
		{"SELECT t.a FROM t", "t.a"},
		{"SELECT t.* FROM t", "t.*"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			got := mustRender(t, tt.query)
			if !strings.Contains(got, tt.want) {
				t.Errorf("Render(%q) = %q, want it to contain %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestRenderNeverLabelsAStar holds the label rule to what it is for. An
// unaliased select item carries its source text as an alias when lowering
// rewrote it, so the caller's column keeps its name; a star names no single
// column and cannot take an alias at all, so writing one produces SQL no
// engine reads. Whitespace around the qualifying dot is what makes the source
// text differ from the rendering, and it is legal everywhere.
func TestRenderNeverLabelsAStar(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ query, want string }{
		{"SELECT t. * FROM t", "SELECT t.* FROM t"},
		{"SELECT t .* FROM t", "SELECT t.* FROM t"},
		{"SELECT t.\n* FROM t", "SELECT t.* FROM t"},
		{"SELECT  * FROM t", "SELECT * FROM t"},
		{"SELECT t. *, a FROM t", "SELECT t.*, a FROM t"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			if got := mustRender(t, tt.query); got != tt.want {
				t.Errorf("Render(%q) = %q, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestRenderLabelsAnItemItRewrote is the other half of the rule above: an
// item the renderer spells differently from the caller keeps its original
// text as a label, so a star can be exempted without exempting everything. A
// qualified column written with a space around its dot is the same shape as
// the star cases and does name one column, so it keeps the label.
func TestRenderLabelsAnItemItRewrote(t *testing.T) {
	t.Parallel()

	got := mustRender(t, "SELECT t. a FROM t")
	if !strings.Contains(got, `AS "t. a"`) {
		t.Errorf("Render() = %q, want it to label the item with its own text", got)
	}
}
