package filesql

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect"
	_ "modernc.org/sqlite"
)

// TestTranslatedQueriesStillExecute is the property the label pass has to keep:
// a query SQLite accepts must still be accepted after translation. Checking the
// translated text alone cannot see this — the text looked reasonable, and only
// running it showed the statement no longer parsed.
//
// The case that got through: an implicit alias after a rewritten call.
// "SELECT CONCAT(a,b) z" became "strict_concat(a,b) z AS \"CONCAT(a,b) z\"",
// two names for one item, because a closing paren was read as an operator and
// the alias after it as part of the expression.
func TestTranslatedQueriesStillExecute(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "f.csv")
	if err := os.WriteFile(path, []byte("a,b,c\n1,2,x\n3,4,y\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	plain, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	// t.Cleanup, not defer: the subtests below are parallel, so they run after
	// this function returns. A deferred Close would shut the database before the
	// first one queried it, and every assertion would pass on "database is
	// closed" instead of on what it meant to check.
	t.Cleanup(func() { _ = plain.Close() })

	// Expression shapes crossed with the ways a select item can be named. Each
	// atom that a dialect rewrites is what makes the naming matter.
	atoms := []string{
		"a", "a / b", "a + b", "CONCAT(c,c)", "ABS(a)", "a DIV b", "COUNT(*)",
		"MAX(a)", "CASE WHEN a>1 THEN a ELSE b END", "(SELECT MAX(a) FROM f)",
		"a IS NULL", "c LIKE 'x'", "a * 2", "-a", "(a)", "(a / b)", "LENGTH(c)",
		"CAST(a AS CHAR)",
	}
	suffixes := []string{"", " AS z", " z", " COLLATE NOCASE"}
	tails := []string{"", " FROM f", " FROM f WHERE a > 0", " FROM f GROUP BY c ORDER BY c"}

	for _, atom := range atoms {
		for _, suffix := range suffixes {
			for _, tail := range tails {
				query := "SELECT " + atom + suffix + tail
				t.Run(strings.NewReplacer(" ", "_", "/", "div", "*", "star").Replace(query), func(t *testing.T) {
					t.Parallel()
					// The property is that translation never turns SQL that parses
					// into SQL that does not. A query holding syntax only the source
					// dialect has — MySQL's DIV, say — does not parse to begin with,
					// so a dialect that leaves it alone has broken nothing.
					if runErr := execErr(plain, query); runErr != nil && isSyntaxError(runErr) {
						t.Skipf("SQLite cannot parse %q on its own", query)
					}
					// A syntax error is the only failure in scope. The translated
					// query may still name a helper this connection has not
					// registered, or a function SQLite lacks — those are the
					// caller's contract with the dialect, not malformed SQL. What
					// translation must never do is produce something that does not
					// parse, and asserting on the text alone cannot see that.
					for _, d := range []dialect.Dialect{dialect.MySQL, dialect.PostgreSQL, dialect.GoogleSQL} {
						translated, err := dialect.Translate(d, query)
						if err != nil {
							continue // a construct the dialect refuses by name is a different contract
						}
						if runErr := execErr(plain, translated); runErr != nil && isSyntaxError(runErr) {
							t.Errorf("%s translated a query into one that does not parse\n  in:  %s\n  out: %s\n  err: %v",
								d, query, translated, runErr)
						}
					}
				})
			}
		}
	}
}

// execErr runs the statement and returns whatever stopped it, or nil.
func execErr(db *sql.DB, query string) error {
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	return rows.Err()
}

// isSyntaxError reports whether SQLite refused to parse the statement, as
// opposed to refusing to run one it understood.
func isSyntaxError(err error) bool {
	return strings.Contains(err.Error(), "syntax error")
}

// TestLikeEscapesWildcard pins that a pattern escaping a wildcard matches the
// literal character, in every dialect that says a backslash does that.
//
// It did not, and the two failures pointed opposite ways: MySQL and GoogleSQL
// returned every row, because the escape was dropped and the "%" left behind
// went on being a wildcard, and PostgreSQL returned none, because the backslash
// survived into a matcher that had no notion of escaping and looked for it
// literally. An over-match is the worse of the two: it hands the caller rows
// they filtered out, and nothing about the result looks wrong.
func TestLikeEscapesWildcard(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "codes.csv")
	if err := os.WriteFile(path, []byte("id,code\n1,a%b\n2,axxb\n3,a_b\n4,azb\n5,a\\zb\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "an escaped percent matches only a literal percent",
			query: `SELECT code FROM codes WHERE code LIKE 'a\%b' ORDER BY id`,
			want:  []string{"a%b"},
		},
		{
			name:  "an escaped underscore matches only a literal underscore",
			query: `SELECT code FROM codes WHERE code LIKE 'a\_b' ORDER BY id`,
			want:  []string{"a_b"},
		},
		{
			name:  "an unescaped wildcard still matches everything",
			query: `SELECT code FROM codes WHERE code LIKE 'a%b' ORDER BY id`,
			want:  []string{"a%b", "axxb", "a_b", "azb", "a\\zb"},
		},
		{
			name:  "an unescaped underscore still matches one character",
			query: `SELECT code FROM codes WHERE code LIKE 'a_b' ORDER BY id`,
			want:  []string{"a%b", "a_b", "azb"},
		},
		{
			name:  "NOT LIKE excludes exactly what LIKE selects",
			query: `SELECT code FROM codes WHERE code NOT LIKE 'a\%b' ORDER BY id`,
			want:  []string{"axxb", "a_b", "azb", "a\\zb"},
		},
		{
			name:  "an explicit ESCAPE clause is honored",
			query: `SELECT code FROM codes WHERE code LIKE 'a!%b' ESCAPE '!' ORDER BY id`,
			want:  []string{"a%b"},
		},
	}

	for _, d := range []dialect.Dialect{dialect.MySQL, dialect.PostgreSQL, dialect.GoogleSQL} {
		t.Run(string(d), func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			validated, err := NewBuilder().AddPath(path).WithDialect(d).Build(ctx)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			db, err := validated.Open(ctx)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					got := queryStrings(t, db, tt.query)
					if len(got) != len(tt.want) {
						t.Fatalf("%s\n got  %q\n want %q", tt.query, got, tt.want)
					}
					for i := range got {
						if got[i] != tt.want[i] {
							t.Fatalf("%s\n got  %q\n want %q", tt.query, got, tt.want)
						}
					}
				})
			}
		})
	}
}

func queryStrings(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}
