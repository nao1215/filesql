package dialect

import (
	"errors"
	"testing"
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
		{"mysql backslash escapes", MySQL, `SELECT 'a\nb\tc\\d\0e'`, "SELECT 'a\nb\tc\\d\x00e'"},
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
		{MySQL, "SELECT _utf8mb4'abc'", `SELECT 'abc' AS "_utf8mb4'abc'"`},
		{MySQL, "SELECT N'abc'", `SELECT 'abc' AS "N'abc'"`},
		{MySQL, "SELECT _binary'abc'", `SELECT x'616263' AS "_binary'abc'"`},
		{MySQL, "SELECT 0 b1010 FROM t", "SELECT 0 AS b1010 FROM t"},
		{PostgreSQL, `SELECT U&'\0041'`, `SELECT 'A' AS "U&'\0041'"`},
		{PostgreSQL, `SELECT U&'\+000041'`, `SELECT 'A' AS "U&'\+000041'"`},
		{PostgreSQL, `SELECT U&'d!0061t!+000061' UESCAPE '!'`, `SELECT 'data' AS "U&'d!0061t!+000061' UESCAPE '!'"`},
		// A column named u, and an ordinary bitwise AND, are not the literal.
		{PostgreSQL, "SELECT u & 1 FROM t", "SELECT u & 1 FROM t"},
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
