package dialect

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// TestOperatorSemantics locks in the operators whose meaning changes on the way
// to SQLite: division that truncates, a LIKE that folds case, and the MySQL
// spellings of OR and null-safe equality.
func TestOperatorSemantics(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
		null    bool
	}{
		// Division is floating point in MySQL and GoogleSQL, integer division
		// in SQLite when both operands are integers.
		{"mysql divides as float", MySQL, `SELECT 5/2`, "2.5", false},
		{"googlesql divides as float", GoogleSQL, `SELECT 5/2`, "2.5", false},
		{"mysql divides exactly", MySQL, `SELECT 4/2`, "2", false},
		{"mysql divides by zero to null", MySQL, `SELECT 1/0`, "", true},
		{"mysql chains division left to right", MySQL, `SELECT 100/4/5`, "5", false},
		{"mysql divides a call result", MySQL, `SELECT ABS(-5)/2`, "2.5", false},
		{"mysql divides a parenthesized operand", MySQL, `SELECT (3+7)/4`, "2.5", false},
		{"mysql divides by a negative literal", MySQL, `SELECT 5/-2`, "-2.5", false},
		{"mysql mixes multiplication and division", MySQL, `SELECT 2*3/4`, "1.5", false},
		{"mysql DIV still truncates", MySQL, `SELECT 7 DIV 2`, "3", false},
		{"mysql DIV truncates toward zero", MySQL, `SELECT -7 DIV 2`, "-3", false},
		{"postgresql keeps integer division", PostgreSQL, `SELECT 5/2`, "2", false},

		// The operators MySQL spells with punctuation, executed rather than only
		// rewritten: the rewrite is only right if the answer is.
		{"mysql && is AND", MySQL, `SELECT 1 && 0`, "0", false},
		{"mysql && is true when both are", MySQL, `SELECT 1 && 2`, "1", false},
		{"mysql ! negates", MySQL, `SELECT !0`, "1", false},
		{"mysql ! negates a nonzero", MySQL, `SELECT !5`, "0", false},
		{"mysql ! binds tighter than a comparison", MySQL, `SELECT !0 = 1`, "1", false},
		{"mysql ! twice is the value's truth", MySQL, `SELECT !!5`, "1", false},
		{"mysql ^ is a bitwise xor", MySQL, `SELECT 5 ^ 3`, "6", false},
		{"mysql ^ of equal operands is zero", MySQL, `SELECT 7 ^ 7`, "0", false},
		// The bits are what the operator works on, and SQLite's only integer is
		// signed, so a result with the high bit set reads as a negative number
		// where MySQL prints it unsigned.
		{"mysql ^ works on the high bit", MySQL, `SELECT -1 ^ 0`, "-1", false},
		// A text operand past int64 is read as the unsigned number it spells. As a
		// literal it never reaches the helper unchanged: SQLite's own parser turns
		// an integer past int64 into a float first.
		{"mysql ^ of a high-bit text operand", MySQL, `SELECT '18446744073709551615' ^ 0`, "-1", false},
		{"mysql ^ propagates null", MySQL, `SELECT 1 ^ NULL`, "", true},

		// LIKE is case-sensitive outside MySQL; SQLite's folds ASCII case.
		{"postgresql LIKE is case-sensitive", PostgreSQL, `SELECT 'ABC' LIKE 'abc'`, "0", false},
		{"postgresql LIKE matches exactly", PostgreSQL, `SELECT 'abc' LIKE 'a%'`, "1", false},
		{"postgresql NOT LIKE", PostgreSQL, `SELECT 'ABC' NOT LIKE 'abc'`, "1", false},
		{"postgresql ILIKE folds case", PostgreSQL, `SELECT 'ABC' ILIKE 'abc'`, "1", false},
		{"postgresql ILIKE folds beyond ascii", PostgreSQL, `SELECT 'ÄBC' ILIKE 'äbc'`, "1", false},
		{"postgresql NOT ILIKE", PostgreSQL, `SELECT 'ABC' NOT ILIKE 'abc'`, "0", false},
		{"postgresql underscore wildcard", PostgreSQL, `SELECT 'abc' LIKE 'a_c'`, "1", false},
		{"postgresql trailing percent", PostgreSQL, `SELECT 'abc' LIKE '%c'`, "1", false},
		{"postgresql anchored percent", PostgreSQL, `SELECT 'abc' LIKE '%b%'`, "1", false},
		{"postgresql empty pattern", PostgreSQL, `SELECT '' LIKE ''`, "1", false},
		{"postgresql percent matches empty", PostgreSQL, `SELECT '' LIKE '%'`, "1", false},
		{"googlesql LIKE is case-sensitive", GoogleSQL, `SELECT 'ABC' LIKE 'abc'`, "0", false},
		{"mysql LIKE stays case-insensitive", MySQL, `SELECT 'ABC' LIKE 'abc'`, "1", false},

		// PostgreSQL "^" is exponentiation, not a SQLite operator at all.
		{"postgresql power", PostgreSQL, `SELECT 2 ^ 3`, "8", false},

		// MySQL reads "||" as OR and "<=>" as null-safe equality.
		{"mysql pipes are OR", MySQL, `SELECT 1 || 0`, "1", false},
		{"mysql pipes are OR when false", MySQL, `SELECT 0 || 0`, "0", false},
		{"mysql null-safe equality", MySQL, `SELECT NULL <=> NULL`, "1", false},
		{"mysql null-safe inequality", MySQL, `SELECT 1 <=> NULL`, "0", false},

		// MySQL HEX takes the value of a number, not the bytes of its digits.
		{"mysql hex of a number", MySQL, `SELECT HEX(255)`, "FF", false},
		{"mysql hex of a string", MySQL, `SELECT HEX('ab')`, "6162", false},
		{"mysql unhex", MySQL, `SELECT UNHEX('6162')`, "ab", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if tt.null {
				if got.Valid {
					t.Fatalf("%s = %q, want NULL", tt.query, got.String)
				}
				return
			}
			if !got.Valid || got.String != tt.want {
				t.Fatalf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestGoogleSQLDivideByZeroRaises keeps the one place the two float-division
// dialects disagree: GoogleSQL raises where MySQL answers NULL, and offers
// SAFE_DIVIDE to callers who want the NULL.
func TestGoogleSQLDivideByZeroRaises(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	if _, err := runDialect(t, db, GoogleSQL, `SELECT 1/0`); err == nil {
		t.Fatal("GoogleSQL division by zero should fail")
	}
	got, err := runDialect(t, db, GoogleSQL, `SELECT SAFE_DIVIDE(1, 0)`)
	if err != nil {
		t.Fatalf("SAFE_DIVIDE: %v", err)
	}
	if got.Valid {
		t.Fatalf("SAFE_DIVIDE(1, 0) = %q, want NULL", got.String)
	}
}

// TestLikeEscapeClauseIsLeftToSQLite keeps a pattern with a custom escape
// character on SQLite's own LIKE, which the helpers do not model.
func TestLikeEscapeClauseIsLeftToSQLite(t *testing.T) {
	t.Parallel()

	got, err := Translate(PostgreSQL, `SELECT * FROM t WHERE a LIKE 'x!%' ESCAPE '!'`)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !strings.Contains(got, "LIKE") || strings.Contains(got, "like_sensitive") {
		t.Fatalf("Translate = %q, want the native LIKE kept", got)
	}
}

func TestLikeMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern, subject string
		want             bool
	}{
		{"", "", true},
		{"", "a", false},
		{"%", "", true},
		{"%", "anything", true},
		{"a%", "abc", true},
		{"a%", "bbc", false},
		{"%c", "abc", true},
		{"a_c", "abc", true},
		{"a_c", "ac", false},
		{"a%c", "abbbc", true},
		{"a%b%c", "axxbyyc", true},
		{"a%b%c", "axxbyy", false},
		{"%%%%%%b", "aaaaaaaaab", true},
		{"%%%%%%b", "aaaaaaaaac", false},
	}
	for _, tt := range tests {
		if got := likeMatch([]rune(tt.pattern), []rune(tt.subject)); got != tt.want {
			t.Fatalf("likeMatch(%q, %q) = %v, want %v", tt.pattern, tt.subject, got, tt.want)
		}
	}
}

// TestOperatorNullHandling verifies that a NULL operand propagates rather than
// being coerced by the helpers.
func TestOperatorNullHandling(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, q := range []string{
		`SELECT like_sensitive(NULL, 'a')`,
		`SELECT like_insensitive('a', NULL)`,
		`SELECT mysql_divide(NULL, 1)`,
		`SELECT mysql_divide(1, NULL)`,
		`SELECT mysql_hex(NULL)`,
		`SELECT mysql_unhex(NULL)`,
		`SELECT mysql_unhex('not hex')`,
	} {
		var got sql.NullString
		if err := db.QueryRowContext(context.Background(), q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got.Valid {
			t.Fatalf("%s = %q, want NULL", q, got.String)
		}
	}
}

// TestOperandMustBePrimary reports a missing operand instead of emitting SQL
// that would mean something else.
func TestOperandMustBePrimary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dialect Dialect
		query   string
	}{
		{MySQL, "SELECT 2 /"},
		{PostgreSQL, "SELECT a LIKE"},
		{PostgreSQL, "SELECT 2 ^"},
	}
	for _, tt := range tests {
		if _, err := Translate(tt.dialect, tt.query); err == nil {
			t.Fatalf("Translate(%s, %q) should fail", tt.dialect, tt.query)
		}
	}
}

// TestWindowFunctionOperands keeps a window or filter clause attached to the
// aggregate it modifies. The operand scanners walk back from an operator to the
// primary expression beside it, and a bare "SUM(x) OVER (...)" ends in the
// clause's own parentheses, which is easy to mistake for the whole operand.
func TestWindowFunctionOperands(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	if _, err := db.ExecContext(context.Background(), `CREATE TABLE w (id INTEGER, score INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO w VALUES (1, 10), (2, 20), (3, 30)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
	}{
		{"mysql divides a windowed sum", MySQL, `SELECT SUM(score) OVER (ORDER BY id) / 2 FROM w ORDER BY id LIMIT 1`, "5"},
		{"mysql divides by a windowed count", MySQL, `SELECT score / COUNT(*) OVER () FROM w ORDER BY id LIMIT 1`, "3.3333333333333335"},
		{"googlesql divides a windowed sum", GoogleSQL, `SELECT SUM(score) OVER (ORDER BY id) / 2 FROM w ORDER BY id LIMIT 1`, "5"},
		{"mysql divides a named window", MySQL, `SELECT SUM(score) OVER win / 2 FROM w WINDOW win AS (ORDER BY id) ORDER BY id LIMIT 1`, "5"},
		{"mysql divides a filtered aggregate", MySQL, `SELECT COUNT(*) FILTER (WHERE id > 1) / 2 FROM w`, "1"},
		{"postgresql matches a windowed value", PostgreSQL, `SELECT CAST(SUM(score) OVER (ORDER BY id) AS TEXT) LIKE '1%' FROM w ORDER BY id LIMIT 1`, "1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if !got.Valid || got.String != tt.want {
				t.Fatalf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestStrictConcatSemantics runs CONCAT through the real driver for every
// dialect. MySQL and GoogleSQL propagate a NULL argument to a NULL result;
// PostgreSQL's concat() ignores NULLs, and SQLite's does too, so only the first
// two are routed to the helper. Passing all of them through to SQLite answered a
// plausible non-NULL string where MySQL and BigQuery answer NULL, with nothing
// to notice.
func TestStrictConcatSemantics(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
		null    bool
	}{
		{name: "mysql concat joins", dialect: MySQL, query: `SELECT CONCAT('a', 'b')`, want: "ab"},
		{name: "mysql concat with null is null", dialect: MySQL, query: `SELECT CONCAT('a', NULL)`, null: true},
		{name: "mysql concat null first", dialect: MySQL, query: `SELECT CONCAT(NULL, 'b')`, null: true},
		{name: "mysql concat single argument", dialect: MySQL, query: `SELECT CONCAT('a')`, want: "a"},
		{name: "mysql concat numbers", dialect: MySQL, query: `SELECT CONCAT(1, 2)`, want: "12"},
		{name: "mysql concat nested null", dialect: MySQL, query: `SELECT CONCAT('a', CONCAT('b', NULL))`, null: true},
		{name: "mysql concat_ws still skips null", dialect: MySQL, query: `SELECT CONCAT_WS(',', 'a', NULL, 'b')`, want: "a,b"},

		{name: "googlesql concat joins", dialect: GoogleSQL, query: `SELECT CONCAT('a', 'b')`, want: "ab"},
		{name: "googlesql concat with null is null", dialect: GoogleSQL, query: `SELECT CONCAT('a', NULL)`, null: true},

		// PostgreSQL's concat() ignores NULLs, so it must NOT be rewritten.
		{name: "postgresql concat ignores null", dialect: PostgreSQL, query: `SELECT CONCAT('a', NULL)`, want: "a"},
		{name: "sqlite concat ignores null", dialect: SQLite, query: `SELECT CONCAT('a', NULL)`, want: "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if tt.null {
				if got.Valid {
					t.Fatalf("%s = %q, want NULL", tt.query, got.String)
				}
				return
			}
			if !got.Valid || got.String != tt.want {
				t.Fatalf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}
