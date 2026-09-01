package dialect

import (
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

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
		{MySQL, "SELECT 0x41"},
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

	t.Run("a quoted name is still a name", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{`SELECT * FROM "only"`, `SELECT * FROM "only"`},
			{`SELECT * FROM "only" AS x`, `SELECT * FROM "only" AS x`},
			{`SELECT * FROM ONLY "only"`, `SELECT * FROM "only"`},
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
