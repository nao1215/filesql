package dialect

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestParse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    Dialect
		wantErr bool
	}{
		{"sqlite", "sqlite", SQLite, false},
		{"sqlite3 alias", "sqlite3", SQLite, false},
		{"mysql", "mysql", MySQL, false},
		{"postgresql", "postgresql", PostgreSQL, false},
		{"postgres alias", "postgres", PostgreSQL, false},
		{"pg alias", "pg", PostgreSQL, false},
		{"googlesql", "googlesql", GoogleSQL, false},
		{"bigquery alias", "bigquery", GoogleSQL, false},
		{"spanner alias", "spanner", GoogleSQL, false},
		{"zetasql alias", "zetasql", GoogleSQL, false},
		{"case insensitive", "MySQL", MySQL, false},
		{"trims whitespace", "  postgres  ", PostgreSQL, false},
		{"unknown", "oracle", "", true},
		{"empty", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Parse(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrUnknownDialect) {
					t.Fatalf("Parse(%q) error = %v, want ErrUnknownDialect", tt.input, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("Parse(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDialects(t *testing.T) {
	t.Parallel()
	got := Dialects()
	want := []Dialect{SQLite, MySQL, PostgreSQL, GoogleSQL}
	if len(got) != len(want) {
		t.Fatalf("Dialects() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Dialects()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestDialectDisplayName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input Dialect
		want  string
	}{
		{"sqlite", SQLite, "SQLite"},
		{"mysql", MySQL, "MySQL"},
		{"postgresql", PostgreSQL, "PostgreSQL"},
		{"googlesql", GoogleSQL, "GoogleSQL"},
		{"unregistered dialect reads back as its wire value", Dialect("oracle"), "oracle"},
		{"empty dialect", Dialect(""), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.input.DisplayName(); got != tt.want {
				t.Fatalf("Dialect(%q).DisplayName() = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestEveryBuiltInDialectHasADisplayName(t *testing.T) {
	t.Parallel()
	// A dialect added to Dialects() without a spelling would read back as its
	// lowercase wire value in a sentence, which is the drift this pins.
	for _, d := range Dialects() {
		name := d.DisplayName()
		if name == "" {
			t.Fatalf("Dialect(%q).DisplayName() is empty", d)
		}
		if name == string(d) {
			t.Fatalf("Dialect(%q).DisplayName() = %q, want a spelled-out name", d, name)
		}
	}
}

func TestTranslateSQLiteIsIdentity(t *testing.T) {
	t.Parallel()
	// The SQLite dialect must never alter the query, including constructs the
	// other dialects would rewrite (backticks, double-quoted strings, # comments).
	inputs := []string{
		"SELECT * FROM users WHERE age > 25",
		"SELECT `weird` FROM t -- comment\n",
		`SELECT "a" ` + "`b`" + ` FROM t`,
		"SELECT 1; SELECT 2;",
		"",
	}
	for _, in := range inputs {
		got, err := Translate(SQLite, in)
		if err != nil {
			t.Fatalf("Translate(SQLite, %q) unexpected error: %v", in, err)
		}
		if got != in {
			t.Fatalf("Translate(SQLite, %q) = %q, want identity", in, got)
		}
	}
}

func TestTranslateUnknownDialect(t *testing.T) {
	t.Parallel()
	_, err := Translate(Dialect("oracle"), "SELECT 1")
	if !errors.Is(err, ErrUnknownDialect) {
		t.Fatalf("Translate(oracle) error = %v, want ErrUnknownDialect", err)
	}
}

// TestTranslateIdempotent verifies that feeding a translated statement back
// through the SQLite (identity) dialect leaves it unchanged, as guaranteed by
// the package's idempotency invariant.
func TestTranslateIdempotent(t *testing.T) {
	t.Parallel()
	inputs := []struct {
		dialect Dialect
		query   string
	}{
		{MySQL, "SELECT `id`, \"name\" FROM `users` # trailing\n"},
		{PostgreSQL, `SELECT "col" FROM t WHERE x = E'a\nb'`},
		{GoogleSQL, "SELECT `p.d.t`.x FROM `p.d.t` WHERE s = r'raw\\n'"},
	}
	for _, tc := range inputs {
		out, err := Translate(tc.dialect, tc.query)
		if err != nil {
			t.Fatalf("Translate(%s, %q) error: %v", tc.dialect, tc.query, err)
		}
		again, err := Translate(SQLite, out)
		if err != nil {
			t.Fatalf("Translate(SQLite, %q) error: %v", out, err)
		}
		if again != out {
			t.Fatalf("not idempotent: Translate(SQLite, %q) = %q", out, again)
		}
	}
}

// TestSQLiteDialectKeepsItsOwnCaseFolding pins the boundary of the case-folding
// rewrite: a caller writing SQLite gets SQLite's functions, whose folding stops
// at ASCII, because that is what SQLite answers and what they asked for.
func TestSQLiteDialectKeepsItsOwnCaseFolding(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		`SELECT UPPER(name) FROM t`,
		`SELECT LOWER(name) FROM t`,
	} {
		got, err := Translate(SQLite, query)
		if err != nil {
			t.Fatalf("Translate(SQLite, %q) error: %v", query, err)
		}
		if got != query {
			t.Errorf("Translate(SQLite, %q) = %q, want it unchanged", query, got)
		}
	}
}

// TestTranslateLexical exercises the lexical normalization that translation
// performs for the non-SQLite dialects: identifier quoting, string quoting,
// comment style, string escapes, blob/byte literals, and passthrough of
// operators, numbers, and placeholders. The dialect-specific rules are
// covered by the per-dialect rule tests added alongside those rules.
func TestTranslateLexical(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dialect Dialect
		input   string
		want    string
	}{
		// MySQL lexical rules.
		{"mysql backtick identifier", MySQL, "SELECT `id` FROM `users`", `SELECT "id" FROM "users"`},
		{"mysql double-quoted string", MySQL, `SELECT "hello" AS x`, `SELECT 'hello' AS x`},
		{"mysql hash comment", MySQL, "SELECT 1 # note", "SELECT 1"},
		{"mysql backslash escape", MySQL, `SELECT 'It\'s'`, `SELECT 'It''s'`},
		{"mysql doubled quote", MySQL, `SELECT 'a''b'`, `SELECT 'a''b'`},
		{"mysql backslash escapes", MySQL, `SELECT 'a\nb\tc\\d\Ze'`, "SELECT 'a\nb\tc\\d\x1ae'"},
		{"mysql backslash unknown", MySQL, `SELECT 'a\qb'`, `SELECT 'aqb'`},
		{"mysql blob passthrough", MySQL, `SELECT x'4142'`, `SELECT x'4142'`},
		{"mysql empty blob", MySQL, `SELECT x''`, `SELECT x''`},
		// A name written straight against a quoted one is an alias in every
		// dialect that allows it, and the translation writes the AS the caller
		// left out. Written back without it, X and "41" would read as the blob
		// literal x'41', which is not what the caller wrote and raises no error.
		{"word before string is an alias", MySQL, `SELECT X"41"`, `SELECT X AS "41"`},
		{"number before string is an alias", MySQL, `SELECT 1"b"`, `SELECT 1 AS "b"`},
		{"word before identifier is an alias", MySQL, "SELECT a`b`", `SELECT a AS "b"`},

		// PostgreSQL lexical rules.
		{"postgres double-quoted identifier", PostgreSQL, `SELECT "col" FROM t`, `SELECT "col" FROM t`},
		{"postgres escape string", PostgreSQL, `SELECT E'a\tb'`, "SELECT 'a\tb'"},
		{"postgres dollar quote", PostgreSQL, `SELECT $$hi$$`, `SELECT 'hi'`},
		{"postgres tagged dollar quote", PostgreSQL, `SELECT $q$a'b$q$`, `SELECT 'a''b'`},
		{"postgres numbered placeholder", PostgreSQL, `SELECT $1`, `SELECT $1`},
		{"postgres arithmetic passthrough", PostgreSQL, `SELECT a + b * c`, `SELECT a + b * c`},
		// A PostgreSQL block comment nests: the whole thing is one comment, and
		// the "+ 1" between the inner close and the outer one is commented out.
		// Ending at the first close let that text execute.
		// The rendered comment carries the inner delimiters with a space in them:
		// SQLite comments do not nest, so an inner "*/" left as it was would end
		// the comment there and leave the rest of the body as statement text.
		{"postgres nested block comment", PostgreSQL, "SELECT 1 /* /* inner */ + 1 */", "SELECT 1"},
		{"postgres nested block comment three deep", PostgreSQL, "SELECT /* a /* b /* c */ */ d */ 1", "SELECT 1"},
		{"mysql block comment body is untouched", MySQL, "SELECT 1", "SELECT 1"},
		// The escapes that name a character by its number. They used to come out
		// as the letter followed by the digits.
		{"postgres escape string hex", PostgreSQL, `SELECT E'\x41'`, `SELECT 'A'`},
		{"postgres escape string octal", PostgreSQL, `SELECT E'\101'`, `SELECT 'A'`},
		{"postgres escape string code point", PostgreSQL, `SELECT E'\u0041'`, `SELECT 'A'`},
		{"postgres plain string keeps its backslash", PostgreSQL, `SELECT '\x41'`, `SELECT '\x41'`},

		// GoogleSQL lexical rules.
		{"googlesql backtick path", GoogleSQL, "SELECT `p.d.t`", `SELECT "p.d.t"`},
		{"googlesql double-quoted string", GoogleSQL, `SELECT "str"`, `SELECT 'str'`},
		{"googlesql raw string", GoogleSQL, `SELECT r'a\nb'`, `SELECT 'a\nb'`},
		{"googlesql byte string", GoogleSQL, `SELECT b'AB'`, `SELECT x'4142'`},
		{"googlesql hash comment", GoogleSQL, "SELECT 1 # c", "SELECT 1"},
		// A triple-quoted string holds its content bare. Read as an ordinary
		// string, the doubled quotes looked like escaped quotes and the literal
		// kept one quote on each end.
		{"googlesql triple-quoted string", GoogleSQL, "SELECT '''abc'''", `SELECT 'abc'`},
		{"googlesql triple-quoted double", GoogleSQL, `SELECT """abc"""`, `SELECT 'abc'`},
		{"googlesql triple-quoted holds a quote", GoogleSQL, "SELECT '''a'b'''", `SELECT 'a''b'`},
		{"googlesql triple-quoted holds a line break", GoogleSQL, "SELECT '''a\nb'''", "SELECT 'a\nb'"},
		{"googlesql doubled quote is still an escape", GoogleSQL, `SELECT 'a''b'`, `SELECT 'a''b'`},
		// A prefix and a triple quote combine: r, b, and both at once, in either
		// order. The prefixed cases used to read the doubled quotes as an empty
		// string and leave the rest as stray tokens.
		{"googlesql raw triple-quoted string", GoogleSQL, "SELECT r'''a'''", `SELECT 'a'`},
		{"googlesql byte triple-quoted string", GoogleSQL, `SELECT b"a"`, `SELECT x'61'`},
		{"googlesql raw byte triple-quoted string", GoogleSQL, "SELECT rb'''a'''", `SELECT x'61'`},
		{"googlesql byte raw prefix", GoogleSQL, `SELECT br'a'`, `SELECT x'61'`},
		{"googlesql raw string keeps its backslash", GoogleSQL, "SELECT r'''a\\nb'''", `SELECT 'a\nb'`},
		// The escapes that name a character by its number.
		{"googlesql hex escape", GoogleSQL, `SELECT '\x41'`, `SELECT 'A'`},
		{"googlesql code point escape", GoogleSQL, `SELECT '\u0041'`, `SELECT 'A'`},
		{"mysql keeps the lenient reading of a hex escape", MySQL, `SELECT '\x41'`, `SELECT 'x41'`},

		// Common passthrough and whitespace normalization.
		{"whitespace collapses", MySQL, "SELECT    *   FROM t", "SELECT * FROM t"},
		{"operators preserved", PostgreSQL, "SELECT a <> b, c || d", "SELECT a <> b, c || d"},
		{"numbers preserved", PostgreSQL, "SELECT 3.5e+10, 0xFF, .25", "SELECT 3.5e+10, 0xFF, .25"},
		{"placeholders preserved", MySQL, "SELECT ?, ?1, @v, :name", "SELECT ?, ?1, @v, :name"},
		{"block comment preserved", MySQL, "SELECT 1", "SELECT 1"},
		{"function call no spacing", MySQL, "SELECT count(*) FROM t", "SELECT count(*) FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Translate(tt.dialect, tt.input)
			if err != nil {
				t.Fatalf("Translate(%s, %q) unexpected error: %v", tt.dialect, tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("Translate(%s, %q) = %q, want %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}

// TestTranslateInvalidSyntax covers the inputs that cannot be read at all.
func TestTranslateInvalidSyntax(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dialect Dialect
		input   string
	}{
		{"unterminated string", MySQL, "SELECT 'abc"},
		{"unterminated block comment", MySQL, "SELECT 1 /* x"},
		{"unterminated backtick identifier", MySQL, "SELECT `abc"},
		{"unterminated double-quoted identifier", PostgreSQL, `SELECT "abc`},
		{"unterminated dollar quote", PostgreSQL, "SELECT $$abc"},
		{"unterminated blob", MySQL, "SELECT x'41"},
		{"unterminated nested block comment", PostgreSQL, "SELECT 1 /* /* inner */"},
		{"unterminated triple-quoted string", GoogleSQL, "SELECT '''abc"},
		// A blob literal holds hexadecimal digits and nothing else. These were
		// accepted and rendered back as SQL that no longer parses.
		{"blob literal holding a quote", MySQL, `SELECT X''''`},
		{"blob literal holding a quote among digits", MySQL, `SELECT x'41''42'`},
		{"blob literal holding letters that are not hex", MySQL, `SELECT x'zz'`},
		// A repeated prefix letter is not a prefix, so "rr" is a name with a
		// string standing beside it and nothing joining the two. A string is an
		// alias in MySQL and is not one here.
		{"googlesql repeated prefix letter is not a prefix", GoogleSQL, `SELECT rr'a'`},
		// A query with no statement in it.
		{"empty query", MySQL, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Translate(tt.dialect, tt.input)
			if !errors.Is(err, ErrInvalidSyntax) {
				t.Fatalf("Translate(%s, %q) error = %v, want ErrInvalidSyntax", tt.dialect, tt.input, err)
			}
		})
	}
}

// TestLiteralFormsAreOneToken pins the literal spellings the lexer used to
// split, each of which reached SQLite as a bare word and failed naming
// something the caller had not written. The values were read from mysql:8.4 and
// postgres:17-alpine.
func TestLiteralFormsAreOneToken(t *testing.T) {
	t.Parallel()

	// A bit literal has MySQL's two readings, the same as the hexadecimal one,
	// so both spellings are refused rather than answered. Before "0b1010" was
	// scanned as one token it split into a zero and an alias and answered 0.
	refused := []struct {
		dialect Dialect
		query   string
	}{
		{MySQL, "SELECT 0b1010"},
		{MySQL, "SELECT 0B1010"},
		{MySQL, "SELECT 0b1010 + 0"},
		{MySQL, "SELECT b'1010'"},
		{MySQL, "SELECT B'1010'"},
		{MySQL, "SELECT _latin1'abc'"},
	}
	for _, tt := range refused {
		if _, err := Translate(tt.dialect, tt.query); !errors.Is(err, ErrUnsupportedSyntax) {
			t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
		}
	}

	// A charset introducer naming UTF-8 is dropped, _binary becomes the
	// literal's own bytes, and the near-misses are left alone.
	translated := []struct {
		dialect Dialect
		query   string
		want    string
	}{
		// The label keeps a space between the introducer and the literal,
		// because render puts one between two atoms that a quote used to
		// separate; the value is what matters and it is the literal's.
		{MySQL, `SELECT _utf8mb4'abc'`, `SELECT 'abc' AS "_utf8mb4'abc'"`},
		{MySQL, "SELECT N'abc'", `SELECT 'abc' AS "N'abc'"`},
		{MySQL, "SELECT _binary'abc'", `SELECT x'616263' AS "_binary'abc'"`},
		{MySQL, "SELECT 0 b1010 FROM t", "SELECT 0 AS b1010 FROM t"},
		{PostgreSQL, `SELECT U&'\0041'`, `SELECT 'A' AS "U&'\0041'"`},
		{PostgreSQL, `SELECT U&'\+000041'`, `SELECT 'A' AS "U&'\+000041'"`},
		{PostgreSQL, `SELECT U&'d!0061t!+000061' UESCAPE '!'`, `SELECT 'data' AS "U&'d!0061t!+000061' UESCAPE '!'"`},
		// A column named u, and an ordinary bitwise AND, are not the literal.
		{PostgreSQL, "SELECT u & 1 FROM t", "SELECT u & 1 FROM t"},
		// GoogleSQL's raw and bytes prefixes run into the quote, so a column
		// named after one of their letters is a column and not the start of a
		// literal, whatever operator stands between it and a string.
		{GoogleSQL, "SELECT a FROM t WHERE b='x'", "SELECT a FROM t WHERE b = 'x'"},
		{GoogleSQL, "SELECT b,'x' FROM t", "SELECT b, 'x' FROM t"},
		{GoogleSQL, "SELECT a,b,'x' FROM t", "SELECT a, b, 'x' FROM t"},
		{GoogleSQL, "SELECT r,'x' FROM t", "SELECT r, 'x' FROM t"},
		{GoogleSQL, "SELECT CONCAT(b,'#') FROM t", `SELECT strict_concat(b, '#') AS "CONCAT(b,'#')" FROM t`},
		// The prefixes themselves, which touching the quote is what makes.
		{GoogleSQL, `SELECT b'x'`, `SELECT x'78'`},
		{GoogleSQL, `SELECT rb'x\n'`, `SELECT x'785c6e'`},
		{GoogleSQL, `SELECT r'x\n'`, `SELECT 'x\n'`},
	}
	for _, tt := range translated {
		got, err := Translate(tt.dialect, tt.query)
		if err != nil {
			t.Errorf("Translate(%v, %q): %v", tt.dialect, tt.query, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Translate(%v, %q)\n  = %q\nwant %q", tt.dialect, tt.query, got, tt.want)
		}
	}
}

// TestPlaceholderNamesReachSQLiteWhole covers the bound-parameter names the
// translation used to split. SQLite reads a dollar sign and a leading digit as
// name characters, so the space the renderer puts between two tokens made SQL
// that no longer parses, and only under a dialect: the same query with no
// dialect ran.
func TestPlaceholderNamesReachSQLiteWhole(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect Dialect
		input   string
		want    string
	}{
		{MySQL, "SELECT @0$0", "SELECT @0$0"},
		{MySQL, "SELECT @a$b + 1", "SELECT @a$b + 1"},
		{MySQL, "SELECT :1abc", "SELECT :1abc"},
		{PostgreSQL, "SELECT $1 + $2", "SELECT $1 + $2"},
		{PostgreSQL, "SELECT :name", "SELECT :name"},
		{GoogleSQL, "SELECT @param", "SELECT @param"},
		{MySQL, "SELECT ?1, ?", "SELECT ?1, ?"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := Translate(tt.dialect, tt.input)
			if err != nil {
				t.Fatalf("Translate(%s, %q): %v", tt.dialect, tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Translate(%s, %q) = %q, want %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}

// TestATranslatedQueryIsOneSQLiteCanPrepare holds translation to the property
// that makes its output SQL rather than text: SQLite has to be able to read
// what comes back. Reporting success and answering a statement SQLite refuses
// moves the failure to the caller's next Query, where the message is about a
// token the caller never wrote.
//
// The queries are ones every dialect here spells the same way, apart from the
// quoting each one owns, so the only thing that varies is the translation. The
// shapes that motivated this are a number ending in its decimal point, which
// used to fuse with the word after it, and a qualified star written with
// whitespace around the dot, which used to come back with an alias.
func TestATranslatedQueryIsOneSQLiteCanPrepare(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Every connection to ":memory:" opens its own database, so the schema and
	// the prepares have to share one.
	db.SetMaxOpenConns(1)
	for _, ddl := range []string{
		"CREATE TABLE t (a INTEGER, b TEXT, c REAL)",
		"CREATE TABLE y (n INTEGER, m TEXT)",
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("exec %q: %v", ddl, err)
		}
	}

	shared := []string{
		"SELECT 1. FROM t",
		"SELECT c * 1. FROM t",
		"SELECT a FROM t WHERE c > 1. AND a = 1",
		"SELECT 1. AS n FROM t",
		"SELECT 1., 2. FROM t",
		"SELECT t. * FROM t",
		"SELECT t .* FROM t",
		"SELECT t.\n* FROM t",
		"SELECT t. *, a FROM t",
		"SELECT t.* FROM t",
		"SELECT  * FROM t",
		"SELECT t.a FROM t",
		"SELECT a FROM t JOIN y ON t.a = y.n",
		"SELECT count(*) FROM t GROUP BY b HAVING count(*) > 0",
		"SELECT a FROM t ORDER BY a DESC LIMIT 1 OFFSET 1",
		"SELECT a FROM t UNION SELECT n FROM y",
	}
	type translation struct {
		dialect Dialect
		query   string
	}
	dialectsUnderTest := []Dialect{MySQL, PostgreSQL, GoogleSQL}
	cases := make([]translation, 0, len(shared)*len(dialectsUnderTest)+1)
	for _, d := range dialectsUnderTest {
		for _, q := range shared {
			cases = append(cases, translation{d, q})
		}
	}
	cases = append(cases, translation{MySQL, "SELECT `t` . * FROM `t`"})

	// The calls whose helper this package names after the spelling the caller
	// used. A helper the runtime does not register reaches the driver as an
	// unknown function naming something the caller never wrote, which is what
	// happened to one spelling of SUBSTRING while the other worked, so every
	// name that derivation can produce is prepared here.
	named := map[Dialect][]string{
		MySQL: {
			"SELECT WEEK(b), WEEKOFYEAR(b), YEARWEEK(b) FROM t",
			"SELECT GREATEST(a, 1), LEAST(a, 1) FROM t",
			"SELECT LEFT(b, 1), RIGHT(b, 1), LPAD(b, 3, 'x'), RPAD(b, 3, 'x') FROM t",
			"SELECT ORD(b), HEX(b), QUOTE(b), ASCII(b), UNHEX(b), INSERT(b, 1, 1, 'x') FROM t",
			"SELECT SUBSTR(b, 1, 2), SUBSTRING(b, 1, 2), MID(b, 1, 2) FROM t",
			"SELECT POSITION('a' IN b), LOCATE('a', b), INSTR(b, 'a') FROM t",
			"SELECT CEIL(c), CEILING(c), FLOOR(c), SIGN(c), SQRT(c), EXP(c), LN(c), LOG2(c), LOG10(c) FROM t",
		},
		PostgreSQL: {
			"SELECT GREATEST(a, 1), LEAST(a, 1), MOD(a, 2) FROM t",
			"SELECT LPAD(b, 3, 'x'), RPAD(b, 3, 'x') FROM t",
			"SELECT SUBSTR(b, 1, 2), SUBSTRING(b, 1, 2) FROM t",
			"SELECT POSITION('a' IN b), STRPOS(b, 'a') FROM t",
		},
		GoogleSQL: {
			"SELECT LEFT(b, 1), RIGHT(b, 1), REPEAT(b, 2) FROM t",
			"SELECT LPAD(b, 3, 'x'), RPAD(b, 3, 'x'), MOD(a, 2) FROM t",
			"SELECT SUBSTR(b, 1, 2), SUBSTRING(b, 1, 2) FROM t",
			"SELECT POSITION('a' IN b), INSTR(b, 'a') FROM t",
			"SELECT SOUNDEX(b), MD5(b), SHA1(b) FROM t",
			"SELECT DATE(b), DATETIME(b), TIME(b), TIMESTAMP(b) FROM t",
		},
	}
	for _, d := range dialectsUnderTest {
		for _, q := range named[d] {
			cases = append(cases, translation{d, q})
		}
	}

	for _, tt := range cases {
		t.Run(tt.dialect.DisplayName()+" "+tt.query, func(t *testing.T) {
			t.Parallel()

			out, err := Translate(tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("Translate(%v, %q): %v", tt.dialect, tt.query, err)
			}
			stmt, err := db.PrepareContext(t.Context(), out)
			if err != nil {
				t.Fatalf("Translate(%v, %q) = %q, which SQLite refuses: %v", tt.dialect, tt.query, out, err)
			}
			_ = stmt.Close()
		})
	}
}

// TestTranslateRefusesACallTheRenameCannotTake holds the other half of the
// property above. A lowering renames a call onto a function that takes a
// different number of arguments, and the count has to be answered here, where
// the caller's own spelling is at hand: reaching SQLite means failing under a
// name the caller never wrote, so they cannot tell a mistake in their query
// from a defect in the translation.
//
// The names below are the ones a rename lands on that are SQLite's own rather
// than a helper this package registers, which is what put them outside the
// check that already covers the helpers.
func TestTranslateRefusesACallTheRenameCannotTake(t *testing.T) {
	t.Parallel()

	refused := []struct {
		dialect Dialect
		query   string
		names   string
	}{
		{PostgreSQL, "SELECT BTRIM(b, ' ', ' ') FROM t", "BTRIM"},
		{PostgreSQL, "SELECT BTRIM() FROM t", "BTRIM"},
		{GoogleSQL, "SELECT BYTE_LENGTH(b, b) FROM t", "BYTE_LENGTH"},
		{MySQL, "SELECT LENGTH(b, b) FROM t", "LENGTH"},
		{MySQL, "SELECT OCTET_LENGTH(b, b, b) FROM t", "OCTET_LENGTH"},
		{PostgreSQL, "SELECT JSONB_ARRAY_LENGTH(b, b, b) FROM t", "JSONB_ARRAY_LENGTH"},
		{MySQL, "SELECT ANY_VALUE(*) FROM t", "ANY_VALUE"},
		{PostgreSQL, "SELECT BOOL_AND(*) FROM t", "BOOL_AND"},
		{PostgreSQL, "SELECT BOOL_OR(*) FROM t", "BOOL_OR"},
		{PostgreSQL, "SELECT EVERY(*) FROM t", "EVERY"},
		{GoogleSQL, "SELECT LOGICAL_AND(*) FROM t", "LOGICAL_AND"},
		{GoogleSQL, "SELECT LOGICAL_OR(*) FROM t", "LOGICAL_OR"},
		{PostgreSQL, "SELECT JSON_AGG(a, c) FROM t", "JSON_AGG"},
		{PostgreSQL, "SELECT JSONB_AGG(a, c) FROM t", "JSONB_AGG"},
		{MySQL, "SELECT JSON_ARRAYAGG(a, c) FROM t", "JSON_ARRAYAGG"},
		{GoogleSQL, "SELECT APPROX_COUNT_DISTINCT(a, c) FROM t", "APPROX_COUNT_DISTINCT"},
	}
	for _, tt := range refused {
		t.Run(tt.dialect.DisplayName()+" "+tt.query, func(t *testing.T) {
			t.Parallel()

			out, err := Translate(tt.dialect, tt.query)
			if err == nil {
				t.Fatalf("Translate(%v, %q) = %q, want a refusal", tt.dialect, tt.query, out)
			}
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
			}
			if !strings.Contains(err.Error(), tt.names) {
				t.Errorf("Translate(%v, %q) error = %v, which does not name %s",
					tt.dialect, tt.query, err, tt.names)
			}
		})
	}

	// The ordinary calls beside them. A fix that refuses by counting is the
	// kind that refuses one call too many.
	accepted := []struct {
		dialect Dialect
		query   string
	}{
		{PostgreSQL, "SELECT BTRIM(b) FROM t"},
		{PostgreSQL, "SELECT BTRIM(b, ' ') FROM t"},
		{GoogleSQL, "SELECT BYTE_LENGTH(b) FROM t"},
		{MySQL, "SELECT LENGTH(b) FROM t"},
		{MySQL, "SELECT OCTET_LENGTH(b) FROM t"},
		{PostgreSQL, "SELECT JSONB_ARRAY_LENGTH(b) FROM t"},
		{MySQL, "SELECT ANY_VALUE(a) FROM t"},
		{PostgreSQL, "SELECT BOOL_AND(a) FROM t"},
		{GoogleSQL, "SELECT LOGICAL_OR(a) FROM t"},
		{PostgreSQL, "SELECT JSON_AGG(a) FROM t"},
		{MySQL, "SELECT JSON_ARRAYAGG(a) FROM t"},
		{GoogleSQL, "SELECT APPROX_COUNT_DISTINCT(a) FROM t"},
	}
	for _, tt := range accepted {
		t.Run("accepted "+tt.dialect.DisplayName()+" "+tt.query, func(t *testing.T) {
			t.Parallel()

			if _, err := Translate(tt.dialect, tt.query); err != nil {
				t.Errorf("Translate(%v, %q): %v", tt.dialect, tt.query, err)
			}
		})
	}
}

// TestTranslateRefusesTextSQLiteCannotSpell pins the refusal for a NUL byte.
// The MySQL and GoogleSQL escape \0 decodes to one, and SQLite reads a
// statement up to the first NUL, so the byte cannot be written into SQL at all.
// Answering it produced a statement that failed later with an error about the
// opening quote; rendering it as a cast from a blob would parse and then answer
// a different length, since SQLite's length() stops at a NUL where MySQL's does
// not, so a refusal is what this returns.
func TestTranslateRefusesTextSQLiteCannotSpell(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect Dialect
		query   string
	}{
		{MySQL, `SELECT '\0'`},
		{MySQL, `SELECT 'a\0b'`},
		{MySQL, `SELECT '\0' = b FROM t`},
		{MySQL, "SELECT `a\x00b` FROM t"},
		{GoogleSQL, `SELECT '\0'`},
	} {
		if _, err := Translate(tt.dialect, tt.query); !errors.Is(err, ErrUnsupportedSyntax) {
			t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
		}
	}

	// Every other control character those escapes produce goes through
	// SQLite's tokenizer unchanged, which is the boundary that keeps the
	// refusal from widening.
	for _, query := range []string{`SELECT '\Z'`, `SELECT '\b'`} {
		if _, err := Translate(MySQL, query); err != nil {
			t.Errorf("Translate(MySQL, %q) error = %v, want it to translate", query, err)
		}
	}
}

// TestTranslateRefusesACallUnderTheCallersOwnName pins that a call with an
// argument count no form of the function accepts is refused here, naming the
// function as the caller wrote it.
//
// A lowering renames a function to the helper that computes it without counting
// the arguments, so such a call used to translate, reach the driver, and fail
// there with "wrong number of arguments to function mysql_time_of_day" -- a
// name the caller never wrote, arriving from their next Query rather than from
// Translate. Every helper's count matches the source dialect's own, so no
// correct call is refused; the second half of this holds that.
func TestTranslateRefusesACallUnderTheCallersOwnName(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect Dialect
		query   string
		names   string
	}{
		{MySQL, "SELECT TIME()", "TIME"},
		{MySQL, "SELECT LEFT('abc')", "LEFT"},
		{MySQL, "SELECT TIMEDIFF('1')", "TIMEDIFF"},
		{MySQL, "SELECT WEEKOFYEAR()", "WEEKOFYEAR"},
		{MySQL, "SELECT CONVERT_TZ('a')", "CONVERT_TZ"},
		{PostgreSQL, "SELECT split_part('a', ',')", "SPLIT_PART"},
		{GoogleSQL, "SELECT IEEE_DIVIDE(1)", "IEEE_DIVIDE"},
	} {
		_, err := Translate(tt.dialect, tt.query)
		if !errors.Is(err, ErrUnsupportedSyntax) {
			t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
			continue
		}
		if !strings.Contains(err.Error(), tt.names) {
			t.Errorf("Translate(%v, %q) error = %q, want it to name %s", tt.dialect, tt.query, err, tt.names)
		}
		// The helper's own name is this package's business and not the
		// caller's, so it does not appear in what they are told.
		if strings.Contains(err.Error(), "mysql_") || strings.Contains(err.Error(), "googlesql_") {
			t.Errorf("Translate(%v, %q) error = %q, want it to name no helper", tt.dialect, tt.query, err)
		}
	}

	// The arities the same functions do accept still translate.
	for _, tt := range []struct {
		dialect Dialect
		query   string
	}{
		{MySQL, "SELECT TIME('12:00:00')"},
		{MySQL, "SELECT LEFT('abc', 2)"},
		{MySQL, "SELECT TIMEDIFF('1', '2')"},
		{MySQL, "SELECT WEEKOFYEAR('2024-01-01')"},
		{MySQL, "SELECT CONVERT_TZ('2024-01-01', '+00:00', '+09:00')"},
		{MySQL, "SELECT GREATEST(1, 2, 3)"},
		{MySQL, "SELECT GREATEST(1, 2)"},
		{PostgreSQL, "SELECT split_part('a,b', ',', 1)"},
		{GoogleSQL, "SELECT IEEE_DIVIDE(1, 2)"},
		{GoogleSQL, "SELECT EDIT_DISTANCE('a', 'b', 3)"},
		// A function whose count varies is registered as taking any number, so
		// every arity it accepts still translates. These are the ones a caller
		// meets most.
		{MySQL, "SELECT ROUND(1.5)"},
		{MySQL, "SELECT ROUND(1.55, 1)"},
		{MySQL, "SELECT SUBSTRING('abc', 2)"},
		{MySQL, "SELECT SUBSTRING('abc', 2, 1)"},
		{MySQL, "SELECT LOG(2)"},
		{MySQL, "SELECT LOG(2, 8)"},
		{MySQL, "SELECT LOCATE('a', 'abc')"},
		{MySQL, "SELECT LOCATE('a', 'abc', 2)"},
		{MySQL, "SELECT WEEK('2024-01-01')"},
		{MySQL, "SELECT WEEK('2024-01-01', 1)"},
		{MySQL, "SELECT CHAR(65)"},
		{MySQL, "SELECT CHAR(65, 66)"},
		{MySQL, "SELECT FORMAT(1234.5, 2)"},
	} {
		if _, err := Translate(tt.dialect, tt.query); err != nil {
			t.Errorf("Translate(%v, %q) error = %v, want it to translate", tt.dialect, tt.query, err)
		}
	}
}

// TestARewrittenItemKeepsItsNameWhereverItStands holds every list of result
// columns to one rule. SQLite names an unaliased column after the text of the
// expression that produced it, so an item lowering rewrote would answer under
// the helper's name; the select list carries the caller's text back as an alias
// to stop that, and RETURNING answers the same columns from the same items.
func TestARewrittenItemKeepsItsNameWhereverItStands(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "CREATE TABLE t (a INTEGER, b TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	for _, tt := range []struct {
		dialect Dialect
		item    string
	}{
		{PostgreSQL, "a::text"},
		{PostgreSQL, "a / 2"},
		{PostgreSQL, "a % 2"},
		{PostgreSQL, "a::text || 'y'"},
		{MySQL, "a / 2"},
		{MySQL, "a DIV 2"},
		{MySQL, "a MOD 2"},
		{MySQL, "CONCAT(b, 'x')"},
		{GoogleSQL, "a / 2"},
		{GoogleSQL, "CAST(a AS STRING)"},
	} {
		t.Run(tt.dialect.DisplayName()+" "+tt.item, func(t *testing.T) {
			t.Parallel()

			selected := columnNames(t, db, tt.dialect, "SELECT "+tt.item+" FROM t")
			for _, statement := range []string{
				"INSERT INTO t (a) VALUES (1) RETURNING " + tt.item,
				"UPDATE t SET b = b WHERE a = 0 RETURNING " + tt.item,
				"DELETE FROM t WHERE a = 0 RETURNING " + tt.item,
			} {
				if got := columnNames(t, db, tt.dialect, statement); !slices.Equal(got, selected) {
					t.Errorf("Translate(%v, %q) answers columns %q, want %q as the select list does",
						tt.dialect, statement, got, selected)
				}
			}
		})
	}
}

// TestAnExplicitAliasStillWinsOverThePreservedName pins the other half: the
// label is what an item falls back to, not something written over the name the
// caller chose, and an item lowering left alone takes no label at all.
func TestAnExplicitAliasStillWinsOverThePreservedName(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		query string
		want  string
	}{
		{"INSERT INTO t (a) VALUES (1) RETURNING a::text AS x", `RETURNING postgresql_cast(a, 'text') AS x`},
		{"INSERT INTO t (a) VALUES (1) RETURNING a", "RETURNING a"},
		{"SELECT a::text AS x FROM t", `SELECT postgresql_cast(a, 'text') AS x FROM t`},
	} {
		got, err := Translate(PostgreSQL, tt.query)
		if err != nil {
			t.Fatalf("Translate(%v, %q): %v", PostgreSQL, tt.query, err)
		}
		if !strings.Contains(got, tt.want) {
			t.Errorf("Translate(%v, %q) = %q, want it to contain %q", PostgreSQL, tt.query, got, tt.want)
		}
	}
}

// columnNames translates a statement, runs it, and reports the names of the
// columns it answers.
func columnNames(t *testing.T, db *sql.DB, d Dialect, query string) []string {
	t.Helper()
	out, err := Translate(d, query)
	if err != nil {
		t.Fatalf("Translate(%v, %q): %v", d, query, err)
	}
	rows, err := db.QueryContext(t.Context(), out)
	if err != nil {
		t.Fatalf("Translate(%v, %q) = %q, which SQLite refuses: %v", d, query, out, err)
	}
	defer func() { _ = rows.Close() }()
	names, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns of %q: %v", out, err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows of %q: %v", out, err)
	}
	return names
}

// TestATableAliasColumnListRenamesOrIsRefused holds the alias list on a table
// reference to the same rule the rest of translation follows: what SQLite
// spells differently is rewritten, and what it cannot spell is refused. A
// derived table's names can be moved onto its select list; a base table's
// cannot, since translation does not know what columns the table has.
func TestATableAliasColumnListRenamesOrIsRefused(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "CREATE TABLE t (a INTEGER, b TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	t.Run("renamed", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
			want    []string
		}{
			{PostgreSQL, "SELECT * FROM (SELECT a, b FROM t) s(x, y)", []string{"x", "y"}},
			{PostgreSQL, "SELECT s.x FROM (SELECT a FROM t) s(x)", []string{"x"}},
			{PostgreSQL, "SELECT * FROM (SELECT a FROM t UNION ALL SELECT a FROM t) s(x)", []string{"x"}},
			{PostgreSQL, "SELECT * FROM t JOIN (SELECT a FROM t) s(x) ON s.x = t.a", []string{"a", "b", "x"}},
			{PostgreSQL, "WITH s(x, y) AS (SELECT a, b FROM t) SELECT * FROM s", []string{"x", "y"}},
			{MySQL, "SELECT * FROM (SELECT a, b FROM t) s(x, y)", []string{"x", "y"}},
			{GoogleSQL, "SELECT * FROM (SELECT a, b FROM t) s(x, y)", []string{"x", "y"}},
		} {
			if got := columnNames(t, db, tt.dialect, tt.query); !slices.Equal(got, tt.want) {
				t.Errorf("Translate(%v, %q) answers columns %q, want %q", tt.dialect, tt.query, got, tt.want)
			}
		}
	})

	t.Run("refused", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
		}{
			{PostgreSQL, "SELECT * FROM t AS s(x, y)"},
			{PostgreSQL, "SELECT * FROM (SELECT * FROM t) s(x, y)"},
			// One name against one item is refused too when the item is a star,
			// which stands for however many columns the table has.
			{PostgreSQL, "SELECT * FROM (SELECT * FROM t) s(x)"},
			{PostgreSQL, "SELECT * FROM (SELECT a, b FROM t) s(x)"},
			{PostgreSQL, "SELECT * FROM (VALUES (1, 2)) v(x, y)"},
			{MySQL, "SELECT * FROM t AS s(x)"},
			{GoogleSQL, "SELECT * FROM t AS s(x)"},
		} {
			_, err := Translate(tt.dialect, tt.query)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
			}
			if err != nil && !strings.Contains(err.Error(), "column") {
				t.Errorf("Translate(%v, %q) error = %v, want it to name the column list", tt.dialect, tt.query, err)
			}
		}
	})
}

// TestAlterTableNamesTheConstraintItCannotAdd covers the statements that read
// as ADD COLUMN if nothing looks for the constraint keywords first. SQLite can
// only add, drop and rename columns, so these are refused; what matters is that
// the refusal names the constraint rather than reporting the constraint's name
// as a column type.
func TestAlterTableNamesTheConstraintItCannotAdd(t *testing.T) {
	t.Parallel()

	t.Run("refused", func(t *testing.T) {
		t.Parallel()

		for _, query := range []string{
			"ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0)",
			"ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES u(id)",
			"ALTER TABLE t ADD CONSTRAINT uq UNIQUE (a)",
			"ALTER TABLE t ADD PRIMARY KEY (a)",
			"ALTER TABLE t ADD UNIQUE (a)",
			"ALTER TABLE t ADD UNIQUE KEY uq (a)",
			"ALTER TABLE t ADD UNIQUE INDEX i (a)",
			"ALTER TABLE t ADD CHECK (a > 0)",
			"ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES u(id)",
			"ALTER TABLE t DROP CONSTRAINT ck",
		} {
			for _, d := range []Dialect{MySQL, PostgreSQL, GoogleSQL} {
				_, err := Translate(d, query)
				if !errors.Is(err, ErrUnsupportedSyntax) {
					t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", d, query, err)
					continue
				}
				if !strings.Contains(err.Error(), "constraint") {
					t.Errorf("Translate(%v, %q) error = %v, want it to name the constraint", d, query, err)
				}
			}
		}
	})

	t.Run("an index says which statement declares one", func(t *testing.T) {
		t.Parallel()

		for _, query := range []string{
			"ALTER TABLE t ADD INDEX i (a)",
			"ALTER TABLE t ADD KEY (a)",
			"ALTER TABLE t ADD FULLTEXT INDEX i (a)",
			"ALTER TABLE t DROP INDEX i",
		} {
			_, err := Translate(MySQL, query)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", MySQL, query, err)
				continue
			}
			if !strings.Contains(err.Error(), "INDEX") {
				t.Errorf("Translate(%v, %q) error = %v, want it to name the statement that declares an index", MySQL, query, err)
			}
		}
	})

	t.Run("still translated", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
			want    string
		}{
			{MySQL, "ALTER TABLE t ADD COLUMN a INT", "ALTER TABLE t ADD COLUMN a INTEGER"},
			{MySQL, "ALTER TABLE t ADD a INT", "ALTER TABLE t ADD COLUMN a INTEGER"},
			{MySQL, "ALTER TABLE t DROP a", "ALTER TABLE t DROP COLUMN a"},
			{MySQL, "ALTER TABLE t DROP COLUMN a", "ALTER TABLE t DROP COLUMN a"},
			{MySQL, "ALTER TABLE t RENAME TO u", "ALTER TABLE t RENAME TO u"},
			{MySQL, "ALTER TABLE t RENAME COLUMN a TO b", "ALTER TABLE t RENAME COLUMN a TO b"},
			// A column whose name is one of the keywords the refusal looks for
			// is still a column.
			{PostgreSQL, `ALTER TABLE t ADD COLUMN "constraint" INT`, `ALTER TABLE t ADD COLUMN "constraint" INTEGER`},
			{PostgreSQL, `ALTER TABLE t ADD COLUMN "check" INT`, `ALTER TABLE t ADD COLUMN "check" INTEGER`},
			{PostgreSQL, `ALTER TABLE t DROP COLUMN "unique"`, `ALTER TABLE t DROP COLUMN "unique"`},
			{MySQL, "ALTER TABLE t ADD COLUMN `key` INT", `ALTER TABLE t ADD COLUMN "key" INTEGER`},
			{MySQL, "ALTER TABLE t DROP COLUMN `index`", `ALTER TABLE t DROP COLUMN "index"`},
			// The constraint parser the refusal routes around still reads the
			// constraints a CREATE TABLE declares.
			{PostgreSQL, "CREATE TABLE t (a INT, CONSTRAINT uq UNIQUE (a))", "CREATE TABLE t (a INTEGER, CONSTRAINT uq UNIQUE (a))"},
			{PostgreSQL, "CREATE TABLE t (a INT, PRIMARY KEY (a))", "CREATE TABLE t (a INTEGER, PRIMARY KEY (a))"},
		} {
			got, err := Translate(tt.dialect, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", tt.dialect, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", tt.dialect, tt.query, got, tt.want)
			}
		}
	})
}

// TestATableReferenceNamesTheTableTheCallerWrote pins PostgreSQL's ONLY and its
// opposite spelling, the trailing star. Both say which of a table and the
// tables inheriting from it a statement reaches, and a database here has
// neither inheritance nor a table to inherit from, so both name the one table
// they stand beside. Reading ONLY as that name made the statement reach a table
// called ONLY: it answered nothing on a database without one, and on a database
// holding a file named only.csv it answered, updated or deleted the wrong one.
func TestATableReferenceNamesTheTableTheCallerWrote(t *testing.T) {
	t.Parallel()

	t.Run("postgresql reads ONLY as the keyword it is", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{"SELECT * FROM ONLY t", "SELECT * FROM t"},
			{"SELECT * FROM only t", "SELECT * FROM t"},
			{"SELECT * FROM ONLY t AS x", "SELECT * FROM t AS x"},
			{"SELECT * FROM ONLY t x", "SELECT * FROM t AS x"},
			{"SELECT * FROM ONLY (t)", "SELECT * FROM t"},
			{"SELECT * FROM ONLY (t) AS x", "SELECT * FROM t AS x"},
			{"SELECT * FROM t *", "SELECT * FROM t"},
			{"SELECT * FROM ONLY s.t", "SELECT * FROM s.t"},
			{"SELECT * FROM ONLY t JOIN ONLY u ON t.a = u.a", "SELECT * FROM t JOIN u ON t.a = u.a"},
			{"SELECT * FROM ONLY t, ONLY u", "SELECT * FROM t, u"},
			{"UPDATE ONLY t SET a = 1", "UPDATE t SET a = 1"},
			{"DELETE FROM ONLY t WHERE a = 1", "DELETE FROM t WHERE a = 1"},
			{"ALTER TABLE ONLY t ADD COLUMN a INT", "ALTER TABLE t ADD COLUMN a INTEGER"},
			{"ALTER TABLE ONLY t RENAME COLUMN a TO b", "ALTER TABLE t RENAME COLUMN a TO b"},
			{"ALTER TABLE ONLY t DROP COLUMN a", "ALTER TABLE t DROP COLUMN a"},
		} {
			got, err := Translate(PostgreSQL, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", PostgreSQL, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", PostgreSQL, tt.query, got, tt.want)
			}
		}
	})

	t.Run("a name that stops in the middle is still refused", func(t *testing.T) {
		t.Parallel()

		// A star straight after a dot is a qualified name missing its last
		// part, not the inheritance star, and reading it as one would answer
		// from the schema the caller named.
		for _, query := range []string{
			"SELECT * FROM s.*",
			"SELECT * FROM ONLY s.*",
			"UPDATE s.* SET a = 1",
			"DELETE FROM s.*",
			"SELECT * FROM t * *",
		} {
			if _, err := Translate(PostgreSQL, query); !errors.Is(err, ErrInvalidSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrInvalidSyntax", PostgreSQL, query, err)
			}
		}
	})

	t.Run("a quoted name is still a name", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{`SELECT * FROM "only"`, `SELECT * FROM "only"`},
			{`SELECT * FROM "only" AS x`, `SELECT * FROM "only" AS x`},
			{`SELECT * FROM ONLY "only"`, `SELECT * FROM "only"`},
			// A quoted identifier names a table whatever word it spells, so it
			// follows ONLY as any other name does.
			{`SELECT * FROM ONLY "from"`, `SELECT * FROM "from"`},
			{`SELECT * FROM ONLY "select" AS x`, `SELECT * FROM "select" AS x`},
			{`UPDATE ONLY "from" SET a = 1`, `UPDATE "from" SET a = 1`},
		} {
			got, err := Translate(PostgreSQL, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", PostgreSQL, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", PostgreSQL, tt.query, got, tt.want)
			}
		}
	})

	t.Run("the dialects without the keyword keep reading a name", func(t *testing.T) {
		t.Parallel()

		// MySQL answers "Table 'probe.ONLY' doesn't exist" for this query and
		// GoogleSQL has no ONLY either, so the word is the table's name there
		// and t is its alias.
		for _, d := range []Dialect{MySQL, GoogleSQL} {
			got, err := Translate(d, "SELECT * FROM ONLY t")
			if err != nil {
				t.Errorf("Translate(%v) error = %v, want it to translate", d, err)
				continue
			}
			if want := "SELECT * FROM ONLY AS t"; got != want {
				t.Errorf("Translate(%v, %q) = %q, want %q", d, "SELECT * FROM ONLY t", got, want)
			}
		}
	})

	t.Run("a query written with ONLY answers the rows the table holds", func(t *testing.T) {
		t.Parallel()

		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		db.SetMaxOpenConns(1)
		for _, ddl := range []string{
			"CREATE TABLE t (a INTEGER)",
			"INSERT INTO t VALUES (1), (2)",
		} {
			if _, err := db.ExecContext(t.Context(), ddl); err != nil {
				t.Fatalf("exec %q: %v", ddl, err)
			}
		}
		out, err := Translate(PostgreSQL, "SELECT sum(a) FROM ONLY t")
		if err != nil {
			t.Fatalf("Translate: %v", err)
		}
		var sum int
		if err := db.QueryRowContext(t.Context(), out).Scan(&sum); err != nil {
			t.Fatalf("query %q: %v", out, err)
		}
		if sum != 3 {
			t.Errorf("sum over ONLY t = %d, want 3", sum)
		}
	})
}

// TestAlterTableRefusesBySpellingWhatItCannotMake pins which sentinel a caller
// gets for an ALTER TABLE their own engine accepts. ErrInvalidSyntax says the
// query could not be read, and these read: they ask for changes SQLite does not
// make. A caller who reports a typing mistake on one sentinel and falls back on
// the other needs the two told apart.
func TestAlterTableRefusesBySpellingWhatItCannotMake(t *testing.T) {
	t.Parallel()

	t.Run("valid in its dialect and refused by name", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
			names   string
		}{
			{PostgreSQL, "ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b INT", "one change"},
			{MySQL, "ALTER TABLE t ADD COLUMN a INT, DROP COLUMN b", "one change"},
			{PostgreSQL, "ALTER TABLE t RENAME COLUMN a TO b, RENAME COLUMN c TO d", "one change"},
			{PostgreSQL, "ALTER TABLE t ADD COLUMN IF NOT EXISTS a INT", "IF NOT EXISTS"},
			{MySQL, "ALTER TABLE t ADD COLUMN IF NOT EXISTS a INT", "IF NOT EXISTS"},
			{GoogleSQL, "ALTER TABLE t ADD COLUMN IF NOT EXISTS a INT64", "IF NOT EXISTS"},
			{PostgreSQL, "ALTER TABLE t DROP COLUMN IF EXISTS a", "IF EXISTS"},
			{MySQL, "ALTER TABLE t DROP COLUMN IF EXISTS a", "IF EXISTS"},
			{GoogleSQL, "ALTER TABLE t DROP COLUMN IF EXISTS a", "IF EXISTS"},
			{PostgreSQL, "ALTER TABLE IF EXISTS t ADD COLUMN a INT", "IF EXISTS"},
			{MySQL, "ALTER TABLE t ADD COLUMN a INT FIRST", "FIRST"},
			{MySQL, "ALTER TABLE t ADD COLUMN a INT AFTER b", "AFTER"},
		} {
			_, err := Translate(tt.dialect, tt.query)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
				continue
			}
			if !strings.Contains(err.Error(), tt.names) {
				t.Errorf("Translate(%v, %q) error = %v, want it to name %q", tt.dialect, tt.query, err, tt.names)
			}
		}
	})

	t.Run("a word DROP TABLE already drops", func(t *testing.T) {
		t.Parallel()

		// DROP TABLE reads CASCADE and RESTRICT and drops them, since SQLite
		// has neither word; a dropped column deserves the same answer.
		for _, tt := range []struct {
			query string
			want  string
		}{
			{"ALTER TABLE t DROP COLUMN a CASCADE", "ALTER TABLE t DROP COLUMN a"},
			{"ALTER TABLE t DROP COLUMN a RESTRICT", "ALTER TABLE t DROP COLUMN a"},
			{"ALTER TABLE t DROP a CASCADE", "ALTER TABLE t DROP COLUMN a"},
			{"DROP TABLE t CASCADE", "DROP TABLE t"},
			{"DROP TABLE t RESTRICT", "DROP TABLE t"},
		} {
			got, err := Translate(PostgreSQL, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", PostgreSQL, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", PostgreSQL, tt.query, got, tt.want)
			}
		}
	})

	t.Run("a statement that stops in the middle is still unreadable", func(t *testing.T) {
		t.Parallel()

		for _, query := range []string{
			"ALTER TABLE t ADD COLUMN a INT,",
			"ALTER TABLE t ADD COLUMN",
			"ALTER TABLE t DROP COLUMN",
			"ALTER TABLE t RENAME COLUMN a TO",
			// One word says what depends on the dropped thing, or neither
			// does. Both together is a statement no engine takes.
			"ALTER TABLE t DROP COLUMN a CASCADE RESTRICT",
			"ALTER TABLE t DROP COLUMN a RESTRICT CASCADE",
			"DROP TABLE t CASCADE RESTRICT",
			"DROP TABLE t RESTRICT CASCADE",
		} {
			if _, err := Translate(PostgreSQL, query); !errors.Is(err, ErrInvalidSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrInvalidSyntax", PostgreSQL, query, err)
			}
		}
	})
}

// TestARowLockingClauseIsRefusedInEverySpelling pins that the clause is refused
// as unsupported wherever it stands. Two spellings were refused as unreadable
// SQL instead: MySQL's LOCK IN SHARE MODE when it followed a table name with no
// alias between them, because LOCK was read as the alias, and PostgreSQL's FOR
// KEY SHARE, which was missing from the words that open the clause.
func TestARowLockingClauseIsRefusedInEverySpelling(t *testing.T) {
	t.Parallel()

	t.Run("refused as unsupported", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
		}{
			{MySQL, "SELECT * FROM t LOCK IN SHARE MODE"},
			{MySQL, "SELECT * FROM t AS x LOCK IN SHARE MODE"},
			{MySQL, "SELECT * FROM t WHERE a = 1 LOCK IN SHARE MODE"},
			{PostgreSQL, "SELECT * FROM t FOR KEY SHARE"},
			{PostgreSQL, "SELECT * FROM t FOR NO KEY UPDATE"},
			{PostgreSQL, "SELECT * FROM t FOR SHARE"},
			{PostgreSQL, "SELECT * FROM t FOR UPDATE"},
		} {
			_, err := Translate(tt.dialect, tt.query)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
				continue
			}
			if !strings.Contains(err.Error(), "row-locking") {
				t.Errorf("Translate(%v, %q) error = %v, want it to name the row-locking clause", tt.dialect, tt.query, err)
			}
		}
	})

	t.Run("LOCK is still a name where MySQL allows one", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{"SELECT * FROM t lock", "SELECT * FROM t AS lock"},
			{"SELECT * FROM t AS lock", "SELECT * FROM t AS lock"},
			{"SELECT * FROM lock", "SELECT * FROM lock"},
		} {
			got, err := Translate(MySQL, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", MySQL, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", MySQL, tt.query, got, tt.want)
			}
		}
	})
}

// TestAValueKeywordIsNotAName pins DEFAULT written where a value goes. It was
// read as a column name and rendered as a quoted identifier, and SQLite reads a
// quoted name that matches no column as a string, so "SET a = DEFAULT" filled
// the column with the word DEFAULT and reported nothing.
func TestAValueKeywordIsNotAName(t *testing.T) {
	t.Parallel()

	t.Run("refused where a value goes", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
		}{
			{PostgreSQL, "UPDATE t SET a = DEFAULT"},
			{MySQL, "UPDATE t SET a = DEFAULT"},
			{PostgreSQL, "UPDATE t SET a = DEFAULT WHERE b = 1"},
			{PostgreSQL, "INSERT INTO t (a) VALUES (DEFAULT)"},
			{MySQL, "INSERT INTO t (a) VALUES (DEFAULT)"},
			{PostgreSQL, "INSERT INTO t (a, b) VALUES (DEFAULT, 1)"},
			{PostgreSQL, "SELECT DEFAULT"},
		} {
			_, err := Translate(tt.dialect, tt.query)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
				continue
			}
			if !strings.Contains(err.Error(), "DEFAULT") {
				t.Errorf("Translate(%v, %q) error = %v, want it to name DEFAULT", tt.dialect, tt.query, err)
			}
		}
	})

	t.Run("the spellings that are not a value", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
			want    string
		}{
			{PostgreSQL, "INSERT INTO t DEFAULT VALUES", "INSERT INTO t DEFAULT VALUES"},
			{PostgreSQL, "CREATE TABLE t (a INT DEFAULT 1)", "CREATE TABLE t (a INTEGER DEFAULT (1))"},
			{PostgreSQL, `UPDATE t SET a = "default"`, `UPDATE t SET a = "default"`},
		} {
			got, err := Translate(tt.dialect, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", tt.dialect, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", tt.dialect, tt.query, got, tt.want)
			}
		}
	})
}

// TestAReturningClauseIsReadInEverySpelling pins Cloud Spanner's THEN RETURN,
// which is the returning clause this package already translates written the way
// GoogleSQL writes it, and PostgreSQL's OVERRIDING clause, which says what to do
// about an identity column a table here does not have.
func TestAReturningClauseIsReadInEverySpelling(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect Dialect
		query   string
		want    string
	}{
		{GoogleSQL, "INSERT INTO t (a) VALUES (1) THEN RETURN a", "INSERT INTO t (a) VALUES (1) RETURNING a"},
		{GoogleSQL, "DELETE FROM t WHERE a = 1 THEN RETURN a", "DELETE FROM t WHERE a = 1 RETURNING a"},
		{GoogleSQL, "UPDATE t SET a = 1 WHERE b = 2 THEN RETURN *", "UPDATE t SET a = 1 WHERE b = 2 RETURNING *"},
		{PostgreSQL, "INSERT INTO t (a) OVERRIDING SYSTEM VALUE VALUES (1)", "INSERT INTO t (a) VALUES (1)"},
		{PostgreSQL, "INSERT INTO t (a) OVERRIDING USER VALUE VALUES (1)", "INSERT INTO t (a) VALUES (1)"},
		// A grouping element is an expression, not only a name.
		{PostgreSQL, "SELECT a FROM t GROUP BY DISTINCT 1", "SELECT a FROM t GROUP BY 1"},
		{PostgreSQL, "SELECT a FROM t GROUP BY ALL (a + b)", "SELECT a FROM t GROUP BY (a + b)"},
	} {
		got, err := Translate(tt.dialect, tt.query)
		if err != nil {
			t.Errorf("Translate(%v, %q) error = %v, want it to translate", tt.dialect, tt.query, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Translate(%v, %q) = %q, want %q", tt.dialect, tt.query, got, tt.want)
		}
	}
}

// TestStatementSpellingsAreAnsweredByName pins the statements that read in
// their dialect and ask for what SQLite does not do. Each was reported as a
// query that could not be read, and one was reported as a column type the
// caller never wrote.
func TestStatementSpellingsAreAnsweredByName(t *testing.T) {
	t.Parallel()

	t.Run("refused by name", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
			names   string
		}{
			{PostgreSQL, "COMMIT AND CHAIN", "CHAIN"},
			{PostgreSQL, "ROLLBACK AND CHAIN", "CHAIN"},
			{MySQL, "COMMIT AND CHAIN", "CHAIN"},
			{PostgreSQL, "CREATE TABLE t (a INT) INHERITS (u)", "INHERITS"},
			{PostgreSQL, "CREATE TABLE t (LIKE u)", "LIKE"},
			{MySQL, "CREATE TABLE t LIKE u", "LIKE"},
		} {
			_, err := Translate(tt.dialect, tt.query)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
				continue
			}
			if !strings.Contains(err.Error(), tt.names) {
				t.Errorf("Translate(%v, %q) error = %v, want it to name %q", tt.dialect, tt.query, err, tt.names)
			}
		}
	})

	t.Run("a word that belongs to one dialect stays there", func(t *testing.T) {
		t.Parallel()

		// OVERRIDING is PostgreSQL's, and a statement writing it elsewhere is
		// not one those dialects take.
		for _, d := range []Dialect{MySQL, GoogleSQL} {
			query := "INSERT INTO t (a) OVERRIDING SYSTEM VALUE VALUES (1)"
			if _, err := Translate(d, query); err == nil {
				t.Errorf("Translate(%v, %q) translated, want it refused", d, query)
			}
		}
	})

	t.Run("the two spellings of a copied table read alike", func(t *testing.T) {
		t.Parallel()

		_, pgErr := Translate(PostgreSQL, "CREATE TABLE t (LIKE u)")
		_, myErr := Translate(MySQL, "CREATE TABLE t LIKE u")
		if pgErr == nil || myErr == nil {
			t.Fatalf("both spellings must be refused: postgresql=%v mysql=%v", pgErr, myErr)
		}
		if !strings.Contains(pgErr.Error(), "copies a table") || !strings.Contains(myErr.Error(), "copies a table") {
			t.Errorf("the two spellings answer differently:\n  postgresql: %v\n  mysql:      %v", pgErr, myErr)
		}
	})

	t.Run("read and dropped, or moved where SQLite takes it", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
			want    string
		}{
			{PostgreSQL, "COMMIT AND NO CHAIN", "COMMIT"},
			{PostgreSQL, "ROLLBACK AND NO CHAIN", "ROLLBACK"},
			{PostgreSQL, "SELECT a FROM t GROUP BY DISTINCT a", "SELECT a FROM t GROUP BY a"},
			{PostgreSQL, "SELECT a FROM t GROUP BY ALL a", "SELECT a FROM t GROUP BY a"},
			{GoogleSQL, "CREATE TABLE t (a INT64) PRIMARY KEY (a)", "CREATE TABLE t (a INTEGER, PRIMARY KEY (a))"},
			{GoogleSQL, "CREATE TABLE t (a INT64, b BOOL) PRIMARY KEY (a, b)", "CREATE TABLE t (a INTEGER, b BOOLEAN, PRIMARY KEY (a, b))"},
		} {
			got, err := Translate(tt.dialect, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", tt.dialect, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", tt.dialect, tt.query, got, tt.want)
			}
		}
	})
}

// TestAJSONPredicateAsksWhatItSays pins PostgreSQL's IS JSON, which reached
// SQLite untranslated: SQLite reads IS as its null-safe comparison and JSON as a
// name, so the predicate became a comparison against a column called JSON.
func TestAJSONPredicateAsksWhatItSays(t *testing.T) {
	t.Parallel()

	t.Run("translated", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{"SELECT a FROM t WHERE a IS JSON", "SELECT a FROM t WHERE json_valid(a)"},
			{"SELECT a FROM t WHERE a IS NOT JSON", "SELECT a FROM t WHERE NOT json_valid(a)"},
			{"SELECT a FROM t WHERE a IS JSON VALUE", "SELECT a FROM t WHERE json_valid(a)"},
		} {
			got, err := Translate(PostgreSQL, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", PostgreSQL, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", PostgreSQL, tt.query, got, tt.want)
			}
		}
	})

	t.Run("the narrowed forms are refused by name", func(t *testing.T) {
		t.Parallel()

		for _, query := range []string{
			"SELECT a FROM t WHERE a IS JSON OBJECT",
			"SELECT a FROM t WHERE a IS JSON ARRAY",
			"SELECT a FROM t WHERE a IS JSON SCALAR",
			"SELECT a FROM t WHERE a IS JSON WITH UNIQUE KEYS",
		} {
			_, err := Translate(PostgreSQL, query)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", PostgreSQL, query, err)
				continue
			}
			if !strings.Contains(err.Error(), "JSON") {
				t.Errorf("Translate(%v, %q) error = %v, want it to name the predicate", PostgreSQL, query, err)
			}
		}
	})

	t.Run("a column named json is still a column", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{"SELECT json FROM t", "SELECT json FROM t"},
			{`SELECT a FROM t WHERE a IS "json"`, `SELECT a FROM t WHERE a IS "json"`},
		} {
			got, err := Translate(PostgreSQL, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", PostgreSQL, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", PostgreSQL, tt.query, got, tt.want)
			}
		}
	})

	t.Run("the translation answers the rows that hold JSON", func(t *testing.T) {
		t.Parallel()

		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		db.SetMaxOpenConns(1)
		for _, ddl := range []string{
			"CREATE TABLE t (a TEXT, json TEXT)",
			`INSERT INTO t VALUES ('{"a":1}', 'x'), ('nope', 'x')`,
		} {
			if _, err := db.ExecContext(t.Context(), ddl); err != nil {
				t.Fatalf("exec %q: %v", ddl, err)
			}
		}
		out, err := Translate(PostgreSQL, "SELECT count(*) FROM t WHERE a IS JSON")
		if err != nil {
			t.Fatalf("Translate: %v", err)
		}
		var n int
		if err := db.QueryRowContext(t.Context(), out).Scan(&n); err != nil {
			t.Fatalf("query %q: %v", out, err)
		}
		if n != 1 {
			t.Errorf("%q answered %d rows, want the one row that holds JSON", out, n)
		}
	})
}

// TestATimeZoneClauseIsAnsweredByName pins the time-zone spellings, which read
// in their dialect and were reported as queries that could not be read.
func TestATimeZoneClauseIsAnsweredByName(t *testing.T) {
	t.Parallel()

	t.Run("refused by name", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
			names   string
		}{
			{PostgreSQL, "SELECT a AT TIME ZONE 'UTC' FROM t", "time zone"},
			{PostgreSQL, "SELECT to_timestamp(0) AT TIME ZONE 'UTC'", "time zone"},
			{GoogleSQL, "SELECT EXTRACT(DAY FROM a AT TIME ZONE 'UTC') FROM t", "time zone"},
			{PostgreSQL, "SELECT TIMESTAMP WITH TIME ZONE '2024-01-01 00:00:00+00'", "time zone"},
			{PostgreSQL, "SELECT a OPERATOR(pg_catalog.+) b FROM t", "OPERATOR"},
		} {
			_, err := Translate(tt.dialect, tt.query)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
				continue
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.names)) {
				t.Errorf("Translate(%v, %q) error = %v, want it to name %q", tt.dialect, tt.query, err, tt.names)
			}
		}
	})

	t.Run("the timestamp without a zone still translates", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{
				"SELECT TIMESTAMP '2024-01-01 00:00:00'",
				`SELECT '2024-01-01 00:00:00' AS "TIMESTAMP '2024-01-01 00:00:00'"`,
			},
			{
				"SELECT TIMESTAMP WITHOUT TIME ZONE '2024-01-01 00:00:00'",
				`SELECT '2024-01-01 00:00:00' AS "TIMESTAMP WITHOUT TIME ZONE '2024-01-01 00:00:00'"`,
			},
		} {
			got, err := Translate(PostgreSQL, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", PostgreSQL, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", PostgreSQL, tt.query, got, tt.want)
			}
		}
	})
}

// TestAnalyzeNamesWhatTheCallerNamed pins the words each dialect writes between
// ANALYZE and the object's name. They were read as the name: "ANALYZE VERBOSE
// t" analyzed something called VERBOSE and dropped t, and MySQL's "ANALYZE
// TABLE t" analyzed one called TABLE.
func TestAnalyzeNamesWhatTheCallerNamed(t *testing.T) {
	t.Parallel()

	t.Run("the name survives the options", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
			want    string
		}{
			{PostgreSQL, "ANALYZE t", "ANALYZE t"},
			{PostgreSQL, "ANALYZE VERBOSE t", "ANALYZE t"},
			{PostgreSQL, "ANALYZE (VERBOSE) t", "ANALYZE t"},
			{PostgreSQL, "ANALYZE (VERBOSE, SKIP_LOCKED) t", "ANALYZE t"},
			{MySQL, "ANALYZE TABLE t", "ANALYZE t"},
			{MySQL, "ANALYZE NO_WRITE_TO_BINLOG TABLE t", "ANALYZE t"},
			{MySQL, "ANALYZE LOCAL TABLE t", "ANALYZE t"},
			{PostgreSQL, "ANALYZE", "ANALYZE"},
			{PostgreSQL, "ANALYZE VERBOSE", "ANALYZE"},
			{PostgreSQL, `ANALYZE "verbose"`, `ANALYZE "verbose"`},
		} {
			got, err := Translate(tt.dialect, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", tt.dialect, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", tt.dialect, tt.query, got, tt.want)
			}
		}
	})

	t.Run("what SQLite analyzes one of", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			dialect Dialect
			query   string
		}{
			{MySQL, "ANALYZE TABLE t, u"},
			{PostgreSQL, "ANALYZE t (a, b)"},
		} {
			if _, err := Translate(tt.dialect, tt.query); !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
			}
		}
	})
}

// TestAClauseAroundAStatementIsRead pins the clauses that stand around a
// statement this package already carries. Each was reported as a query that
// could not be read.
func TestAClauseAroundAStatementIsRead(t *testing.T) {
	t.Parallel()

	t.Run("read and dropped", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{"CREATE INDEX i ON t (a) INCLUDE (b)", "CREATE INDEX i ON t (a)"},
			{"CREATE INDEX i ON t (a) INCLUDE (b, c) WHERE a > 1", "CREATE INDEX i ON t (a) WHERE a > 1"},
			{"CREATE VIEW v AS SELECT 1 WITH CHECK OPTION", "CREATE VIEW v AS SELECT 1"},
			{"CREATE VIEW v AS SELECT 1 WITH CASCADED CHECK OPTION", "CREATE VIEW v AS SELECT 1"},
			{"CREATE VIEW v AS SELECT 1 WITH LOCAL CHECK OPTION", "CREATE VIEW v AS SELECT 1"},
		} {
			got, err := Translate(PostgreSQL, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", PostgreSQL, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", PostgreSQL, tt.query, got, tt.want)
			}
		}
	})

	t.Run("a select that makes a table", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{"SELECT a INTO newt FROM t", "CREATE TABLE newt AS SELECT a FROM t"},
			{"SELECT a INTO TABLE newt FROM t", "CREATE TABLE newt AS SELECT a FROM t"},
			{"SELECT a INTO TEMP newt FROM t WHERE a > 1", "CREATE TEMPORARY TABLE newt AS SELECT a FROM t WHERE a > 1"},
			// PostgreSQL takes the INTO of a compound query from its first
			// select, which is where its column names come from too.
			{"SELECT a INTO newt FROM t UNION SELECT b FROM u", "CREATE TABLE newt AS SELECT a FROM t UNION SELECT b FROM u"},
			// The query behind a WITH is the statement's own query.
			{"WITH c AS (SELECT 1 AS a) SELECT a INTO newt FROM c", "CREATE TABLE newt AS WITH c AS (SELECT 1 AS a) SELECT a FROM c"},
		} {
			got, err := Translate(PostgreSQL, tt.query)
			if err != nil {
				t.Errorf("Translate(%v, %q) error = %v, want it to translate", PostgreSQL, tt.query, err)
				continue
			}
			if got != tt.want {
				t.Errorf("Translate(%v, %q) = %q, want %q", PostgreSQL, tt.query, got, tt.want)
			}
		}
	})

	t.Run("a select that writes elsewhere is refused by name", func(t *testing.T) {
		t.Parallel()

		for _, query := range []string{
			"SELECT a INTO OUTFILE '/tmp/x' FROM t",
			"SELECT a INTO DUMPFILE '/tmp/x' FROM t",
			"SELECT a INTO @v FROM t",
			"SELECT a FROM t INTO @v",
		} {
			_, err := Translate(MySQL, query)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", MySQL, query, err)
				continue
			}
			if !strings.Contains(err.Error(), "INTO") {
				t.Errorf("Translate(%v, %q) error = %v, want it to name the clause", MySQL, query, err)
			}
		}
	})

	t.Run("an INTO inside a subquery is not a statement", func(t *testing.T) {
		t.Parallel()

		for _, query := range []string{
			"SELECT * FROM (SELECT a INTO newt FROM t) s",
			"SELECT * FROM t WHERE a IN (SELECT b INTO newt FROM u)",
			"WITH w AS (SELECT a INTO newt FROM t) SELECT * FROM w",
			"SELECT * FROM t WHERE EXISTS (WITH w AS (SELECT 1) SELECT a INTO newt FROM t)",
		} {
			if _, err := Translate(PostgreSQL, query); err == nil {
				t.Errorf("Translate(%v, %q) translated, want it refused", PostgreSQL, query)
			}
		}
	})
}

// TestAWriteCarryingOrderByOrLimitIsRefused pins the refusal for an UPDATE or a
// DELETE that names how many rows it touches. SQLite takes ORDER BY and LIMIT on
// those statements only in a build compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT,
// and the one behind this package is not, so passing them through gave the
// caller a syntax error naming a keyword that is valid where they wrote it --
// after the translation had reshaped the clause, rewriting MySQL's "LIMIT 2, 1"
// into "LIMIT 1 OFFSET 2" on a statement that could never run.
func TestAWriteCarryingOrderByOrLimitIsRefused(t *testing.T) {
	t.Parallel()

	t.Run("refused as unsupported", func(t *testing.T) {
		t.Parallel()

		for _, query := range []string{
			"UPDATE t SET b = 'x' LIMIT 1",
			"UPDATE t SET b = 'x' ORDER BY a",
			"UPDATE t SET b = 'x' ORDER BY a LIMIT 1",
			"UPDATE t SET b = 'x' LIMIT 1 OFFSET 2",
			"DELETE FROM t LIMIT 1",
			"DELETE FROM t ORDER BY a",
			"DELETE FROM t ORDER BY a LIMIT 1",
			"DELETE FROM t WHERE a = 1 ORDER BY a LIMIT 1",
		} {
			for _, d := range []Dialect{MySQL, PostgreSQL, GoogleSQL} {
				_, err := Translate(d, query)
				if !errors.Is(err, ErrUnsupportedSyntax) {
					t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", d, query, err)
					continue
				}
				want := "LIMIT"
				if strings.Contains(query, "ORDER BY") {
					want = "ORDER BY"
				}
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Translate(%v, %q) error = %q, want it to name %s", d, query, err, want)
				}
			}
		}
	})

	// MySQL writes LIMIT after a two-argument form as well, and the refusal has
	// to reach it before the rewrite that turns it into LIMIT and OFFSET.
	t.Run("the two-argument limit is refused too", func(t *testing.T) {
		t.Parallel()

		if _, err := Translate(MySQL, "DELETE FROM t LIMIT 2, 1"); !errors.Is(err, ErrUnsupportedSyntax) {
			t.Errorf("Translate(MySQL, %q) error = %v, want ErrUnsupportedSyntax", "DELETE FROM t LIMIT 2, 1", err)
		}
	})

	// The refusal points at the clause rather than at the statement, so a
	// caller reading the position goes to the words it names.
	t.Run("the position names the clause", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{"UPDATE t SET b = 'x'\n  ORDER BY a\n  LIMIT 1", "line 2"},
			{"DELETE FROM t\n  LIMIT 1", "line 2"},
			{"UPDATE t SET b = 'x'\n\n  LIMIT 1", "line 3"},
		} {
			_, err := Translate(MySQL, tt.query)
			if err == nil {
				t.Errorf("Translate(MySQL, %q) translated, want a refusal", tt.query)
				continue
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Translate(MySQL, %q) error = %q, want it to name %s", tt.query, err, tt.want)
			}
		}
	})

	// A SELECT is where SQLite does take both, so the refusal must not spread to
	// it -- including the SELECT inside an INSERT and the one a CTE holds.
	t.Run("a select still takes both", func(t *testing.T) {
		t.Parallel()

		for _, query := range []string{
			"SELECT a FROM t ORDER BY a LIMIT 1",
			"INSERT INTO y SELECT a, b FROM t ORDER BY a LIMIT 1",
			"WITH c AS (SELECT a FROM t ORDER BY a LIMIT 1) SELECT a FROM c",
			"UPDATE t SET b = (SELECT b FROM t ORDER BY a LIMIT 1)",
			"DELETE FROM t WHERE a IN (SELECT a FROM t ORDER BY a LIMIT 1)",
		} {
			if _, err := Translate(MySQL, query); err != nil {
				t.Errorf("Translate(MySQL, %q) error = %v, want it to translate", query, err)
			}
		}
	})
}

// TestDialectMirrorsTheInternalIdentifier holds the two declarations of a
// dialect's name together. The packages under this one cannot import it, so
// they carry the identifier themselves; a value that disagreed here would name
// one dialect to a caller and another to the parser.
func TestDialectMirrorsTheInternalIdentifier(t *testing.T) {
	t.Parallel()

	pairs := []struct {
		public   Dialect
		internal dialects.Dialect
	}{
		{SQLite, dialects.SQLite},
		{MySQL, dialects.MySQL},
		{PostgreSQL, dialects.PostgreSQL},
		{GoogleSQL, dialects.GoogleSQL},
	}
	for _, pair := range pairs {
		if string(pair.public) != string(pair.internal) {
			t.Errorf("public %q and internal %q name the same dialect differently", pair.public, pair.internal)
		}
		if pair.public.DisplayName() != pair.internal.DisplayName() {
			t.Errorf("%q: DisplayName %q != %q", pair.public, pair.public.DisplayName(), pair.internal.DisplayName())
		}
	}
	if got := len(Dialects()); got != len(dialects.All()) {
		t.Errorf("Dialects() has %d entries, the internal list has %d", got, len(dialects.All()))
	}
}

// TestBothSpellingsOfANiladicFunctionAgree holds the two spellings of one
// construct to one answer, and the three PostgreSQL reserves to a refusal that
// gives the reason.
func TestBothSpellingsOfANiladicFunctionAgree(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "CURRENT_CATALOG", "CURRENT_SCHEMA", "CURRENT_ROLE",
	} {
		for _, d := range []Dialect{MySQL, PostgreSQL, GoogleSQL} {
			t.Run(name+" under "+string(d), func(t *testing.T) {
				t.Parallel()

				_, bareErr := Translate(d, "SELECT "+name)
				_, calledErr := Translate(d, "SELECT "+name+"()")
				if (bareErr == nil) != (calledErr == nil) {
					t.Errorf("the two spellings of %s answer differently: bare=%v called=%v",
						name, bareErr, calledErr)
				}
			})
		}
	}

	// The three PostgreSQL reserves and answers from its catalog, which a
	// database made from files has none of. Both spellings are refused.
	for _, name := range []string{"CURRENT_USER", "SESSION_USER", "SYSTEM_USER"} {
		t.Run("postgresql refuses "+name, func(t *testing.T) {
			t.Parallel()

			for _, query := range []string{"SELECT " + name, "SELECT " + name + "()"} {
				translated, err := Translate(PostgreSQL, query)
				if err == nil {
					t.Errorf("%q was translated to %q rather than refused", query, translated)
					continue
				}
				if !strings.Contains(err.Error(), "a fact about the server") {
					t.Errorf("%q is refused without the reason: %v", query, err)
				}
			}
		})
	}

	t.Run("a quoted name is the column it spells", func(t *testing.T) {
		t.Parallel()

		// A column really named one of these is reachable by the spelling the
		// engine that reserved the word requires anyway.
		translated, err := Translate(PostgreSQL, `SELECT "current_user" FROM t`)
		if err != nil {
			t.Fatalf("a quoted name is a column: %v", err)
		}
		if !strings.Contains(translated, "current_user") {
			t.Errorf("the column was lost: %q", translated)
		}
	})

	t.Run("sqlite reads the word as the column it spells", func(t *testing.T) {
		t.Parallel()

		// The identity translation refuses nothing, so a caller writing SQLite
		// gets the column their file holds.
		if _, err := Translate(SQLite, "SELECT current_user FROM t"); err != nil {
			t.Errorf("the identity translation refuses nothing: %v", err)
		}
	})
}

// TestSourceArityIsCheckedForNamesLeftOnSQLite holds a call to the argument
// count the dialect that wrote it accepts, for a name this package leaves on
// SQLite's function of the same name. The sourceArity table says which names
// those are and why.
func TestSourceArityIsCheckedForNamesLeftOnSQLite(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect Dialect
		query   string
		refuse  bool
	}{
		{MySQL, "SELECT LTRIM('xxab','x')", true},
		{MySQL, "SELECT RTRIM('abxx','x')", true},
		{MySQL, "SELECT LTRIM('  ab')", false},
		{MySQL, "SELECT RTRIM('ab  ')", false},
		// Both take the second argument in these dialects.
		{PostgreSQL, "SELECT LTRIM('xxab','x')", false},
		{GoogleSQL, "SELECT LTRIM('xxab','x')", false},
	} {
		t.Run(string(tt.dialect)+" "+tt.query, func(t *testing.T) {
			t.Parallel()

			_, err := Translate(tt.dialect, tt.query)
			if tt.refuse && err == nil {
				t.Errorf("%s does not take that many arguments and the call was translated", tt.query)
			}
			if !tt.refuse && err != nil {
				t.Errorf("%s is a call the dialect accepts: %v", tt.query, err)
			}
		})
	}
}

// TestGoogleSQLBytesLiteralNamesBytes holds a BYTES literal to the bytes it
// names. Its escapes name bytes rather than code points, which is the
// difference between it and a string literal; decoding one into a Go string
// made b'\xff' the code point U+00FF and stored it as the two UTF-8 bytes
// c3 bf, so TO_HEX answered c3bf where BigQuery answers ff and a comparison
// against a hash matched nothing.
func TestGoogleSQLBytesLiteralNamesBytes(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		literal string
		hex     string
	}{
		{`b'\xff'`, "ff"},
		{`b'\x00'`, "00"},
		{`b'\x41'`, "41"},
		{`b'\x00\x7f\x80\xff'`, "007f80ff"},
		{`b'A'`, "41"},
		{`b'\377'`, "ff"},
		{`b'\101'`, "41"},
		{`b'\\'`, "5c"},
		{`b'\n'`, "0a"},
		{`b''`, ""},
	} {
		t.Run(tt.literal, func(t *testing.T) {
			t.Parallel()

			translated, err := Translate(GoogleSQL, "SELECT "+tt.literal)
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			want := "SELECT x'" + tt.hex + "'"
			if !strings.Contains(translated, want) {
				t.Errorf("%s became %q, want a blob spelled %q", tt.literal, translated, want)
			}
		})
	}

	t.Run("a string literal's escape names a code point", func(t *testing.T) {
		t.Parallel()

		// The same escape in a string literal is the character U+00FF, which
		// SQLite holds as the two bytes of its UTF-8, and that is the
		// distinction the two kinds of literal are for.
		translated, err := Translate(GoogleSQL, `SELECT '\xff'`)
		if err != nil {
			t.Fatalf("translate: %v", err)
		}
		if !strings.Contains(translated, "ÿ") {
			t.Errorf(`'\xff' became %q, want the character U+00FF`, translated)
		}
	})
}

// TestGoogleSQLBytesLiteralRefusesAnOctalPastAByte holds a BYTES literal to the
// range an octal escape can name. Three octal digits reach 511 and a byte stops
// at 255, so \400 names no byte; BigQuery refuses such a literal, and reading it
// as a code point would put two bytes where the caller wrote one escape.
func TestGoogleSQLBytesLiteralRefusesAnOctalPastAByte(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		literal string
		refuse  bool
	}{
		{`b'\377'`, false},
		{`b'\400'`, true},
		{`b'\777'`, true},
		{`b'\401\402'`, true},
		// A backslash written as an escape is a backslash, and the digits
		// after it are digits.
		{`b'\\400'`, false},
		// A string literal names code points, where the same escape is the
		// character that number names.
		{`'\400'`, false},
	} {
		t.Run(tt.literal, func(t *testing.T) {
			t.Parallel()

			translated, err := Translate(GoogleSQL, "SELECT "+tt.literal)
			if tt.refuse {
				if err == nil {
					t.Errorf("%s names no byte and became %q", tt.literal, translated)
					return
				}
				if !strings.Contains(err.Error(), "byte") {
					t.Errorf("%s is refused without saying why: %v", tt.literal, err)
				}
				return
			}
			if err != nil {
				t.Errorf("%s is a literal the dialect accepts: %v", tt.literal, err)
			}
		})
	}
}

// TestATranslationPreparesOrNamesWhatTheCallerWrote holds every translation to
// the one thing SQLite has to be able to do with it, and every failure to
// naming what the caller wrote.
//
// The refusal tables are swept for "translated or refused by name"; this asks
// the next question. A call renamed onto a SQLite builtin used to reach the
// engine unchecked, so CHAR_LENGTH() failed as "wrong number of arguments to
// function length()", about a function nowhere in the caller's query -- the
// outcome dialect/doc.go says the arity check exists to prevent.
func TestATranslationPreparesOrNamesWhatTheCallerWrote(t *testing.T) {
	t.Parallel()

	require.NoError(t, RegisterFunctions())
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(t.Context(), `CREATE TABLE t (a TEXT, b INTEGER, c REAL)`)
	require.NoError(t, err)

	// Argument counts a caller might write, including the wrong ones, which is
	// where the name in the refusal matters.
	argLists := []string{"", "'x'", "'x', 'y'", "'x', 'y', 'z'", "1", "1, 2", "1, 2, 3"}

	for _, tt := range []struct {
		dialect Dialect
		list    string
	}{
		{MySQL, filepath.Join("testdata", "engine_functions_mysql.txt")},
		{PostgreSQL, filepath.Join("testdata", "engine_functions_postgresql.txt")},
	} {
		names := engineFunctionNames(t, tt.list)
		require.NotEmpty(t, names, "the name list is what this runs over")
		for _, name := range names {
			for _, args := range argLists {
				query := fmt.Sprintf("SELECT %s(%s) FROM t", name, args)
				translated, translateErr := Translate(tt.dialect, query)
				if translateErr != nil {
					continue // Refused by name, which the sweep already covers.
				}
				stmt, prepErr := db.PrepareContext(t.Context(), translated)
				if prepErr == nil {
					_ = stmt.Close()
					continue
				}
				// A name with no answer is written down in the debt file, and
				// SQLite saying so is what that file records.
				if strings.Contains(prepErr.Error(), "no such function") {
					continue
				}
				// Anything else has to be about the caller's own spelling.
				assert.Containsf(t, strings.ToUpper(prepErr.Error()), strings.ToUpper(name),
					"%s: %s became %q and SQLite refused it without naming %s: %v",
					tt.dialect, query, translated, name, prepErr)
			}
		}
	}
}

// engineFunctionNames reads a list of an engine's function names out of the
// testdata the refusal sweep already keeps.
func engineFunctionNames(t *testing.T, path string) []string {
	t.Helper()

	body, err := os.ReadFile(path) //nolint:gosec // fixed, in-repo testdata path
	require.NoError(t, err)

	var names []string
	for line := range strings.Lines(string(body)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		names = append(names, line)
	}
	return names
}

// TestATranslationIsSQLSQLiteCanRead pins the shapes that once translated into
// text SQLite could not parse, which reached the caller as a syntax error about
// SQL this package had written rather than about the query they had. Each is
// either refused here, under the spelling the caller used, or translated into
// something SQLite reads.
func TestATranslationIsSQLSQLiteCanRead(t *testing.T) {
	t.Parallel()

	require.NoError(t, RegisterFunctions())
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	// Cleanup rather than defer: the subtests below run in parallel and resume
	// after this function returns, and a closed database answers every prepare
	// with the same error, which is not one of the ones this looks for.
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecContext(t.Context(), `CREATE TABLE t (a TEXT, b INTEGER, c REAL)`)
	require.NoError(t, err)

	for _, tt := range []struct {
		name    string
		dialect Dialect
		query   string
		refused bool
	}{
		{"an alias on a star", MySQL, "SELECT t.* AS everything FROM t", true},
		{"a hex prefix with no digits", MySQL, "SELECT 0x FROM t", true},
		{"a binary prefix with no digits", MySQL, "SELECT 0b FROM t", true},
		{"a hex literal PostgreSQL reads as bits", PostgreSQL, "SELECT X'0'", false},
		{"a blob literal of one digit", GoogleSQL, "SELECT X'0'", true},
		{"a VALUES row with no values", MySQL, "INSERT INTO t VALUES ()", true},
		{"a qualified name of four parts", MySQL, "SELECT a.b.c.d FROM t", true},
		{"a quoted name with nothing in it", PostgreSQL, `SELECT "" FROM t`, true},
		{"a table name of three parts", GoogleSQL, "SELECT b FROM a.b.c", true},
		{"a byte order mark inside a statement", MySQL, "SELECT a FROM t" + "\ufeff", true},
		{"a byte order mark ahead of a query", MySQL, "\ufeff" + "SELECT a FROM t", false},
		{"a call named for a word SQLite keeps", MySQL, "SELECT INDEX(1)", false},
		{"a call the caller quoted", MySQL, "SELECT `index`(1)", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			translated, translateErr := Translate(tt.dialect, tt.query)
			if tt.refused {
				require.Errorf(t, translateErr, "%s translated to %q", tt.query, translated)
				return
			}
			require.NoError(t, translateErr)

			stmt, prepErr := db.PrepareContext(t.Context(), translated)
			if prepErr == nil {
				_ = stmt.Close()
				return
			}
			// SQLite may still have nothing by that name, which is it
			// answering about the query; what it may not do is fail to read
			// the text this package wrote.
			for _, unparsable := range []string{"syntax error", "unrecognized token", "incomplete input"} {
				assert.NotContainsf(t, prepErr.Error(), unparsable,
					"%s became %q, which SQLite cannot parse: %v", tt.query, translated, prepErr)
			}
		})
	}
}

// TestTheMySQLUpsertSpelling translates the upsert MySQL callers write and runs
// it, since the rows it leaves are the answer that matters. They are the rows
// MySQL 8.4 leaves for the same statements.
func TestTheMySQLUpsertSpelling(t *testing.T) {
	t.Parallel()

	require.NoError(t, RegisterFunctions())
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecContext(t.Context(), `CREATE TABLE u (a INTEGER PRIMARY KEY, b TEXT)`)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO u VALUES (1, 'x')`)
	require.NoError(t, err)

	for _, query := range []string{
		"INSERT INTO u (a, b) VALUES (1, 'y') ON DUPLICATE KEY UPDATE b = VALUES(b)",
		"INSERT INTO u (a, b) VALUES (2, 'z') ON DUPLICATE KEY UPDATE b = VALUES(b)",
		"INSERT INTO u (a, b) VALUES (1, 'w') ON DUPLICATE KEY UPDATE b = CONCAT(VALUES(b), '!')",
	} {
		translated, err := Translate(MySQL, query)
		require.NoErrorf(t, err, "%s", query)
		_, err = db.ExecContext(t.Context(), translated)
		require.NoErrorf(t, err, "%s became %q", query, translated)
	}

	rows, err := db.QueryContext(t.Context(), `SELECT a, b FROM u ORDER BY a`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	got := map[int]string{}
	for rows.Next() {
		var a int
		var b string
		require.NoError(t, rows.Scan(&a, &b))
		got[a] = b
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[int]string{1: "w!", 2: "z"}, got)

	// VALUES names one column and nothing else, and outside an upsert there is
	// no row for it to name, which is what MySQL answers too.
	_, err = Translate(MySQL, "INSERT INTO u (a) VALUES (1) ON DUPLICATE KEY UPDATE b = VALUES(1 + 1)")
	assert.Error(t, err)
}

// TestPostgreSQLRaisesWhereItsDomainEnds holds the mathematical functions to
// PostgreSQL's answers at the edge of their domain. SQLite's own answer NULL or
// an infinity there, which reads as missing data rather than as arithmetic the
// engine refused. Every expectation was read from PostgreSQL 17.
func TestPostgreSQLRaisesWhereItsDomainEnds(t *testing.T) {
	t.Parallel()

	require.NoError(t, RegisterFunctions())
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	t.Run("refused", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			expr string
			want string
		}{
			{"sqrt(-1)", "cannot take square root of a negative number"},
			{"ln(0)", "cannot take logarithm of zero"},
			{"ln(-1)", "cannot take logarithm of a negative number"},
			{"log(0)", "cannot take logarithm of zero"},
			{"log(-1)", "cannot take logarithm of a negative number"},
			{"log(10, 0)", "cannot take logarithm of zero"},
			{"log(0, 10)", "cannot take logarithm of zero"},
			{"log(2, -1)", "cannot take logarithm of a negative number"},
			{"power(0, -1)", "zero raised to a negative power is undefined"},
			{"pow(0, -1)", "zero raised to a negative power is undefined"},
			{"exp(1000)", "value out of range: overflow"},
			{"acos(2)", "input is out of range"},
			{"asin(2)", "input is out of range"},
			{"acosd(2)", "input is out of range"},
			{"asind(2)", "input is out of range"},
			{"acosh(0)", "input is out of range"},
			{"atanh(2)", "input is out of range"},
		} {
			t.Run(tt.expr, func(t *testing.T) {
				t.Parallel()

				translated, err := Translate(PostgreSQL, "SELECT "+tt.expr)
				require.NoError(t, err)
				var v any
				err = db.QueryRowContext(t.Context(), translated).Scan(&v)
				require.Errorf(t, err, "%s answered %v", tt.expr, v)
				assert.Contains(t, err.Error(), tt.want)
			})
		}
	})

	t.Run("answered", func(t *testing.T) {
		t.Parallel()

		// The values PostgreSQL answers, including the two edges it does not
		// refuse: a helper that starts refusing those would fail here.
		for _, tt := range []struct {
			expr string
			want float64
		}{
			{"sqrt(4)", 2},
			{"sqrt(0)", 0},
			{"ln(1)", 0},
			{"log(100)", 2},
			{"log(2, 8)", 3},
			{"power(2, 10)", 1024},
			{"power(-2, 3)", -8},
			{"power(0, 0)", 1},
			{"exp(0)", 1},
			{"acos(1)", 0},
			{"acosd(1)", 0},
			{"asind(0.5)", 30},
			{"acosh(1)", 0},
			{"atanh(0)", 0},
			{"atanh(1)", math.Inf(1)},
			{"cot(0)", math.Inf(1)},
		} {
			t.Run(tt.expr, func(t *testing.T) {
				t.Parallel()

				translated, err := Translate(PostgreSQL, "SELECT "+tt.expr)
				require.NoError(t, err)
				var got float64
				require.NoError(t, db.QueryRowContext(t.Context(), translated).Scan(&got))
				assert.InDelta(t, tt.want, got, 1e-9)
			})
		}
	})

	// A NULL argument is nothing to compute with, and PostgreSQL answers NULL
	// for one rather than refusing it.
	for _, expr := range []string{"sqrt(NULL)", "ln(NULL)", "log(NULL, 2)", "power(NULL, -1)", "cot(NULL)"} {
		translated, err := Translate(PostgreSQL, "SELECT "+expr)
		require.NoError(t, err)
		var v any
		require.NoErrorf(t, db.QueryRowContext(t.Context(), translated).Scan(&v), "%s", expr)
		assert.Nilf(t, v, "%s", expr)
	}
}
