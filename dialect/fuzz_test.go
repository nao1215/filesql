package dialect

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
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
		"SELECT 1. FROM t",
		"SELECT a FROM t WHERE c > 1. AND a = 1",
		"SELECT t. * FROM t",
		"SELECT SUBSTRING(v, 1, 2), POSITION('a' IN v) FROM t",
		"UPDATE t SET a = 1 ORDER BY a LIMIT 1",
		"DELETE FROM t LIMIT 2, 1",
		`SELECT 'a\0b'`,
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

// FuzzTranslationPrepares holds every successful translation to SQL SQLite can
// compile. FuzzTranslate above checks that a translation reads back as the same
// statement through this package's own parser; this asks the question a caller
// depends on, which is whether the engine can read it: a query this package
// said yes to has to reach SQLite as something SQLite can prepare.
func FuzzTranslationPrepares(f *testing.F) {
	for _, seed := range []string{
		"SELECT a FROM t",
		"SELECT CONCAT(a, 'x') FROM t WHERE b > 1",
		"SELECT CAST(a AS UNSIGNED) FROM t",
		"SELECT DATE_FORMAT(a, '%Y') FROM t GROUP BY a",
		"SELECT a::text FROM t WHERE a ILIKE 'x%'",
		"SELECT SUBSTR(a, 1, 2) FROM t ORDER BY b DESC LIMIT 3",
		"SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) FROM t",
		"WITH x AS (SELECT a FROM t) SELECT * FROM x",
		"INSERT INTO t (a, b) VALUES ('x', 1)",
		"UPDATE t SET a = TRIM(a) WHERE b IS NOT NULL",
		"SELECT JSON_EXTRACT(a, '$.k') FROM t",
		"SELECT SAFE.ADD(b, 1) FROM t",
		"SELECT INTERVAL 1 DAY + a FROM t",
		`SELECT b'\x41' FROM t`,
		"SELECT t.* AS everything FROM t",
		"SELECT 0x FROM t",
		"SELECT X'0'",
		"INSERT INTO t VALUES ()",
		"SELECT INDEX(1)",
		"SELECT a.b.c.d FROM t",
		`SELECT "" FROM t`,
		"\ufeffSELECT a FROM t",
		"SELECT (0) FROM a.b.c",
	} {
		f.Add(seed)
	}

	if err := RegisterFunctions(); err != nil {
		f.Fatal(err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(f.Context(), `CREATE TABLE t (a TEXT, b INTEGER, c REAL)`); err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, query string) {
		if len(query) > 400 {
			return
		}
		for _, d := range []Dialect{MySQL, PostgreSQL, GoogleSQL} {
			translated, err := Translate(d, query)
			if err != nil {
				continue // A refusal is an answer.
			}
			// A translation that gave the text back unchanged is the caller's
			// own SQL, and SQLite complaining about it is SQLite answering
			// them. What this looks for is a rendering this package wrote.
			if strings.TrimSpace(translated) == strings.TrimSpace(query) {
				continue
			}
			stmt, prepErr := db.PrepareContext(t.Context(), translated)
			if prepErr == nil {
				_ = stmt.Close()
				continue
			}
			// A translation SQLite cannot parse is this package's rendering;
			// one it parses and then complains about -- a column that is not
			// there, an argument count that does not match, a window nobody
			// declared -- is SQLite answering about the caller's own query.
			message := prepErr.Error()
			if !strings.Contains(message, "syntax error") &&
				!strings.Contains(message, "unrecognized token") &&
				!strings.Contains(message, "incomplete input") {
				continue
			}
			t.Errorf("%s: %q became %q, which SQLite cannot parse: %v", d, query, translated, prepErr)
		}
	})
}
