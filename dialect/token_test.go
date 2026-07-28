package dialect

import (
	"errors"
	"testing"
)

// TestTranslateLexical exercises the lexical normalization that translation
// performs for the non-SQLite dialects: identifier quoting, string quoting,
// comment style, string escapes, blob/byte literals, and passthrough of
// operators, numbers, and placeholders. Dialect-specific token rewrites are
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
		{"mysql hash comment", MySQL, "SELECT 1 # note", "SELECT 1 -- note\n"},
		{"mysql backslash escape", MySQL, `SELECT 'It\'s'`, `SELECT 'It''s'`},
		{"mysql doubled quote", MySQL, `SELECT 'a''b'`, `SELECT 'a''b'`},
		{"mysql backslash escapes", MySQL, `SELECT 'a\nb\tc\\d\0e'`, "SELECT 'a\nb\tc\\d\x00e'"},
		{"mysql backslash unknown", MySQL, `SELECT 'a\qb'`, `SELECT 'aqb'`},
		{"mysql blob passthrough", MySQL, `SELECT x'4142'`, `SELECT x'4142'`},

		// PostgreSQL lexical rules.
		{"postgres double-quoted identifier", PostgreSQL, `SELECT "col" FROM t`, `SELECT "col" FROM t`},
		{"postgres escape string", PostgreSQL, `SELECT E'a\tb'`, "SELECT 'a\tb'"},
		{"postgres dollar quote", PostgreSQL, `SELECT $$hi$$`, `SELECT 'hi'`},
		{"postgres tagged dollar quote", PostgreSQL, `SELECT $q$a'b$q$`, `SELECT 'a''b'`},
		{"postgres numbered placeholder", PostgreSQL, `SELECT $1`, `SELECT $1`},
		{"postgres arithmetic passthrough", PostgreSQL, `SELECT a + b * c`, `SELECT a + b * c`},

		// GoogleSQL lexical rules.
		{"googlesql backtick path", GoogleSQL, "SELECT `p.d.t`", `SELECT "p.d.t"`},
		{"googlesql double-quoted string", GoogleSQL, `SELECT "str"`, `SELECT 'str'`},
		{"googlesql raw string", GoogleSQL, `SELECT r'a\nb'`, `SELECT 'a\nb'`},
		{"googlesql byte string", GoogleSQL, `SELECT b'AB'`, `SELECT x'4142'`},
		{"googlesql hash comment", GoogleSQL, "SELECT 1 # c", "SELECT 1 -- c\n"},

		// Common passthrough and whitespace normalization.
		{"whitespace collapses", MySQL, "SELECT    *   FROM t", "SELECT * FROM t"},
		{"operators preserved", PostgreSQL, "SELECT a <> b, c || d", "SELECT a <> b, c || d"},
		{"numbers preserved", MySQL, "SELECT 3.5e+10, 0xFF, .25", "SELECT 3.5e+10, 0xFF, .25"},
		{"placeholders preserved", MySQL, "SELECT ?, ?1, @v, :name", "SELECT ?, ?1, @v, :name"},
		{"block comment preserved", MySQL, "SELECT /* hi */ 1", "SELECT /* hi */ 1"},
		{"function call no spacing", MySQL, "SELECT count(*) FROM t", "SELECT count(*) FROM t"},
		{"empty query", MySQL, "", ""},
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

func TestDecodeBackslash(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want string
		adv  int
	}{
		{`\n`, "\n", 2},
		{`\t`, "\t", 2},
		{`\r`, "\r", 2},
		{`\0`, "\x00", 2},
		{`\b`, "\b", 2},
		{`\f`, "\f", 2},
		{`\v`, "\v", 2},
		{`\\`, "\\", 2},
		{`\'`, "'", 2},
		{`\"`, "\"", 2},
		{"\\`", "`", 2},
		{`\q`, "q", 2}, // unknown escape drops the backslash
		{`\`, "\\", 1}, // trailing backslash with nothing after it
	}
	for _, c := range cases {
		got, adv := decodeBackslash(c.in, 0)
		if got != c.want || adv != c.adv {
			t.Fatalf("decodeBackslash(%q) = (%q, %d), want (%q, %d)", c.in, got, adv, c.want, c.adv)
		}
	}
}

// TestTokenizeOffsets verifies that token offsets are monotonically
// non-decreasing and within the source, a property the Fuzz test also relies on.
func TestTokenizeOffsets(t *testing.T) {
	t.Parallel()
	const query = "SELECT `a`, 'b', 1 + 2 -- tail\nFROM t"
	cfg, ok := lexConfigFor(MySQL)
	if !ok {
		t.Fatal("lexConfigFor(MySQL) not ok")
	}
	tokens, err := tokenize(query, cfg)
	if err != nil {
		t.Fatalf("tokenize error: %v", err)
	}
	prev := -1
	for i, tok := range tokens {
		if tok.offset < 0 || tok.offset >= len(query) {
			t.Fatalf("token %d offset %d out of range [0,%d)", i, tok.offset, len(query))
		}
		if tok.offset <= prev {
			t.Fatalf("token %d offset %d not increasing (prev %d)", i, tok.offset, prev)
		}
		prev = tok.offset
	}
}
