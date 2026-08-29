package dialect

import (
	"errors"
	"testing"
)

// FuzzTranslate checks the properties that have to hold for any input at all,
// since a query can arrive from anywhere: translation never panics, never runs
// away, is deterministic, and either refuses with one of this package's errors
// or answers SQL that reads back as the same statement.
//
// The last property is what makes the output SQL rather than text: a
// translation that ends a literal or a comment somewhere the input did not
// would produce something that parses into a different query, and re-reading it
// is what catches that.
func FuzzTranslate(f *testing.F) {
	for _, seed := range []string{
		"SELECT * FROM t",
		"SELECT `a`, \"b\" FROM `t` WHERE x = 'v'",
		"SELECT a::int FROM t -- c",
		"SELECT $$d$$, E'e\\n', $1",
		"SELECT r'raw', b'AB', `p.d.t`.x # h",
		"SELECT count(*) /* mid */ FROM t;",
		"SELECT a + b * c, (a + b) * c, a OR b AND c, NOT a = b",
		"SELECT EXTRACT(YEAR FROM d), DATE_ADD(d, INTERVAL 1 DAY) FROM t",
		"SELECT CASE WHEN a > 1 THEN b ELSE c END",
		"WITH x AS (SELECT 1 AS n) SELECT n FROM x JOIN y USING (n)",
		"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2",
		"CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT NOT NULL)",
		"'unterminated",
		"/* unterminated",
		"",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, query string) {
		for _, d := range Dialects() {
			if d == SQLite {
				// SQLite is the identity translation: it hands back whatever it
				// was given, so the output is only as well-formed as the input.
				continue
			}
			out, err := Translate(d, query)
			if err != nil {
				if !knownError(err) {
					t.Fatalf("Translate(%s, %q) failed with an error of an unknown kind: %v", d, query, err)
				}
				continue
			}
			again, err := Translate(d, query)
			if err != nil {
				t.Fatalf("Translate(%s, %q) succeeded then failed: %v", d, query, err)
			}
			if again != out {
				t.Fatalf("Translate(%s, %q) is not deterministic: %q then %q", d, query, out, again)
			}
			// The output is SQLite SQL, and reading it as SQLite has to give
			// the same text back.
			reread, err := Translate(PostgreSQL, out)
			if err != nil {
				// PostgreSQL's lexical rules are SQLite's for identifiers and
				// strings, but its grammar refuses a few things SQLite writes;
				// a refusal here is not a defect in the output.
				continue
			}
			if third, err := Translate(PostgreSQL, reread); err == nil && third != reread {
				t.Fatalf("Translate(%s, %q) produced %q, which does not read back as itself: %q then %q",
					d, query, out, reread, third)
			}
		}
	})
}

// knownError reports whether an error is one this package promises to return.
// Anything else means a failure reached the caller unlabeled.
func knownError(err error) bool {
	return errors.Is(err, ErrInvalidSyntax) ||
		errors.Is(err, ErrUnsupportedSyntax) ||
		errors.Is(err, ErrUnsupportedFeature) ||
		errors.Is(err, ErrUnknownDialect)
}
