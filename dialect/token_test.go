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
		{"mysql empty blob", MySQL, `SELECT x''`, `SELECT x''`},
		// A word and a string the input kept apart stay apart. Written adjacent,
		// X and "41" read back as the blob literal x'41', which is not what the
		// caller wrote and raises no error.
		{"word before string does not fuse", MySQL, `SELECT X"41"`, `SELECT X '41'`},
		{"number before string does not fuse", MySQL, `SELECT 1"b"`, `SELECT 1 'b'`},
		{"word before identifier does not fuse", MySQL, "SELECT a`b`", `SELECT a "b"`},

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
		{"postgres nested block comment", PostgreSQL, "SELECT 1 /* /* inner */ + 1 */", "SELECT 1 /* / * inner * / + 1 */"},
		{"postgres nested block comment three deep", PostgreSQL, "SELECT /* a /* b /* c */ */ d */ 1", "SELECT /* a / * b / * c * / * / d */ 1"},
		{"mysql block comment body is untouched", MySQL, "SELECT 1 /* plain */", "SELECT 1 /* plain */"},
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
		{"googlesql hash comment", GoogleSQL, "SELECT 1 # c", "SELECT 1 -- c\n"},
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
		{"googlesql repeated prefix letter is not a prefix", GoogleSQL, `SELECT rr'a'`, `SELECT rr 'a'`},
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
		{"unterminated nested block comment", PostgreSQL, "SELECT 1 /* /* inner */"},
		{"unterminated triple-quoted string", GoogleSQL, "SELECT '''abc"},
		// A blob literal holds hexadecimal digits and nothing else. These were
		// accepted and rendered back as SQL that no longer parses.
		{"blob literal holding a quote", MySQL, `SELECT X''''`},
		{"blob literal holding a quote among digits", MySQL, `SELECT x'41''42'`},
		{"blob literal holding letters that are not hex", MySQL, `SELECT x'zz'`},
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
		// The two that keep their backslash. MySQL documents them that way so a
		// LIKE pattern survives being written as a literal: dropping it left an
		// ordinary wildcard, and a pattern asking for one row matched every row.
		{`\%`, `\%`, 2},
		{`\_`, `\_`, 2},
		// MySQL's \Z is ASCII 26. It used to fall through to the default and
		// come out as a literal "Z".
		{`\Z`, "\x1a", 2},
	}
	for _, c := range cases {
		got, adv := decodeBackslash(c.in, 0, escapeRules{backslash: true})
		if got != c.want || adv != c.adv {
			t.Fatalf("decodeBackslash(%q) = (%q, %d), want (%q, %d)", c.in, got, adv, c.want, c.adv)
		}
	}
}

// TestDecodeNumericEscapes covers the escapes that name a character by its
// number, which only the dialects defining them decode.
//
// They used to fall through to the lenient default, which drops the backslash
// and keeps the digits: a hex escape came out as the letter x followed by its
// digits rather than as the character, and a comparison against such a literal
// matched different rows than the caller had asked for.
func TestDecodeNumericEscapes(t *testing.T) {
	t.Parallel()

	numeric := escapeRules{backslash: true, numeric: true}
	cases := []struct {
		in   string
		want string
		adv  int
	}{
		{`\x41`, "A", 4},
		{`\x7`, "\a", 3},        // one hex digit is enough
		{`\x41x`, "A", 4},       // stops after two digits
		{`\101`, "A", 4},        // octal
		{`\12`, "\n", 3},        // octal, two digits
		{`\0`, "\x00", 2},       // octal zero, the same answer as before
		{`\u0041`, "A", 6},      // code point
		{`\U00000041`, "A", 10}, // wide code point
		{`\u3042`, "\u3042", 6},
		{`\xzz`, "x", 2}, // no hex digits: the lenient default
		{`\u00`, "u", 2}, // too few digits: the lenient default
		{`\n`, "\n", 2},  // the letter escapes still work
		{`\%`, `\%`, 2},  // and the two that keep their backslash
		{`\8`, "8", 2},   // 8 is not an octal digit
	}
	for _, c := range cases {
		got, adv := decodeBackslash(c.in, 0, numeric)
		if got != c.want || adv != c.adv {
			t.Fatalf("decodeBackslash(%q, numeric) = (%q, %d), want (%q, %d)", c.in, got, adv, c.want, c.adv)
		}
	}

	// Without the numeric rules the same input keeps the lenient reading, which
	// is what MySQL does.
	plain := escapeRules{backslash: true}
	if got, adv := decodeBackslash(`\x41`, 0, plain); got != "x" || adv != 2 {
		t.Fatalf("decodeBackslash(%q, plain) = (%q, %d), want (%q, 2)", `\x41`, got, adv, "x")
	}
}

// TestTokenizeNestedBlockComment pins that a PostgreSQL block comment nests, at
// the level where it is decided: the text between the inner close and the outer
// one has to stay inside the comment rather than become tokens of its own.
//
// The rendered SQL looks the same either way, which is why this is asserted on
// the tokens: what changed was that the query executed the text it had
// commented out.
func TestTokenizeNestedBlockComment(t *testing.T) {
	t.Parallel()

	const query = "SELECT 1 /* /* inner */ + 1 */"

	pg, ok := lexConfigFor(PostgreSQL)
	if !ok {
		t.Fatal("lexConfigFor(PostgreSQL) not ok")
	}
	tokens, err := tokenize(query, pg)
	if err != nil {
		t.Fatalf("tokenize(%q, PostgreSQL) unexpected error: %v", query, err)
	}
	for _, tok := range tokens {
		if tok.kind == tokOp && tok.text == "+" {
			t.Fatalf("tokenize(%q, PostgreSQL) produced a + outside the comment: %v", query, tokens)
		}
	}
	if got := tokens[len(tokens)-1]; got.kind != tokBlockComment || got.text != " /* inner */ + 1 " {
		t.Fatalf("tokenize(%q, PostgreSQL) last token = %+v, want the whole comment", query, got)
	}

	// MySQL comments do not nest, so the same input keeps the reading it had:
	// the comment ends at the first close and the rest is the statement.
	my, ok := lexConfigFor(MySQL)
	if !ok {
		t.Fatal("lexConfigFor(MySQL) not ok")
	}
	tokens, err = tokenize(query, my)
	if err != nil {
		t.Fatalf("tokenize(%q, MySQL) unexpected error: %v", query, err)
	}
	found := false
	for _, tok := range tokens {
		if tok.kind == tokOp && tok.text == "+" {
			found = true
		}
	}
	if !found {
		t.Fatalf("tokenize(%q, MySQL) = %v, want a + outside the comment", query, tokens)
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
