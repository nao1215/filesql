package token

import (
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
)

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
		// The two that keep their backslash. dialects.MySQL documents them that way so a
		// LIKE pattern survives being written as a literal: dropping it left an
		// ordinary wildcard, and a pattern asking for one row matched every row.
		{`\%`, `\%`, 2},
		{`\_`, `\_`, 2},
		// dialects.MySQL's \Z is ASCII 26. It used to fall through to the default and
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
	// is what dialects.MySQL does.
	plain := escapeRules{backslash: true}
	if got, adv := decodeBackslash(`\x41`, 0, plain); got != "x" || adv != 2 {
		t.Fatalf("decodeBackslash(%q, plain) = (%q, %d), want (%q, 2)", `\x41`, got, adv, "x")
	}
}

// TestTokenizeNestedBlockComment pins that a dialects.PostgreSQL block comment nests, at
// the level where it is decided: the text between the inner close and the outer
// one has to stay inside the comment rather than become tokens of its own.
//
// The rendered SQL looks the same either way, which is why this is asserted on
// the tokens: what changed was that the query executed the text it had
// commented out.
func TestTokenizeNestedBlockComment(t *testing.T) {
	t.Parallel()

	const query = "SELECT 1 /* /* inner */ + 1 */"

	pg, ok := ConfigFor(dialects.PostgreSQL)
	if !ok {
		t.Fatal("ConfigFor(dialects.PostgreSQL) not ok")
	}
	tokens, err := Lex(query, pg)
	if err != nil {
		t.Fatalf("Lex(%q, dialects.PostgreSQL) unexpected error: %v", query, err)
	}
	for _, tok := range tokens {
		if tok.Kind == Op && tok.Text == "+" {
			t.Fatalf("Lex(%q, dialects.PostgreSQL) produced a + outside the comment: %v", query, tokens)
		}
	}
	if got := tokens[len(tokens)-1]; got.Kind != BlockComment || got.Text != " /* inner */ + 1 " {
		t.Fatalf("Lex(%q, dialects.PostgreSQL) last token = %+v, want the whole comment", query, got)
	}

	// dialects.MySQL comments do not nest, so the same input keeps the reading it had:
	// the comment ends at the first close and the rest is the statement.
	my, ok := ConfigFor(dialects.MySQL)
	if !ok {
		t.Fatal("ConfigFor(dialects.MySQL) not ok")
	}
	tokens, err = Lex(query, my)
	if err != nil {
		t.Fatalf("Lex(%q, dialects.MySQL) unexpected error: %v", query, err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Kind == Op && tok.Text == "+" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Lex(%q, dialects.MySQL) = %v, want a + outside the comment", query, tokens)
	}
}

// TestTokenizeOffsets verifies that token offsets are monotonically
// non-decreasing and within the source, a property the Fuzz test also relies on.
func TestTokenizeOffsets(t *testing.T) {
	t.Parallel()
	const query = "SELECT `a`, 'b', 1 + 2 -- tail\nFROM t"
	cfg, ok := ConfigFor(dialects.MySQL)
	if !ok {
		t.Fatal("ConfigFor(dialects.MySQL) not ok")
	}
	tokens, err := Lex(query, cfg)
	if err != nil {
		t.Fatalf("tokenize error: %v", err)
	}
	prev := -1
	for i, tok := range tokens {
		if tok.Offset < 0 || tok.Offset >= len(query) {
			t.Fatalf("token %d offset %d out of range [0,%d)", i, tok.Offset, len(query))
		}
		if tok.Offset <= prev {
			t.Fatalf("token %d offset %d not increasing (prev %d)", i, tok.Offset, prev)
		}
		prev = tok.Offset
	}
}

// TestOperatorsThatAreOneToken pins the multi-character operators to being read
// whole. Split, each half reaches the grammar as a different operator: "#-"
// became a bitwise XOR of a negation, which SQLite ran and answered a number
// for, and "|/" became a bitwise OR beside a division, whose error named a "/"
// the caller had not written.
func TestOperatorsThatAreOneToken(t *testing.T) {
	t.Parallel()

	cfg, _ := ConfigFor(dialects.PostgreSQL)
	for _, tt := range []struct {
		input string
		want  []string
	}{
		{"a #- b", []string{"a", "#-", "b"}},
		{"a #> b", []string{"a", "#>", "b"}},
		{"a #>> b", []string{"a", "#>>", "b"}},
		{"a # b", []string{"a", "#", "b"}},
		{"|/ a", []string{"|/", "a"}},
		{"||/ a", []string{"||/", "a"}},
		{"a || b", []string{"a", "||", "b"}},
		{"a | b", []string{"a", "|", "b"}},
		{"@ a", []string{"@", "a"}},
		{"a @> b", []string{"a", "@>", "b"}},
		{"a @? b", []string{"a", "@?", "b"}},
		{"a @@ b", []string{"a", "@@", "b"}},
		{"a <@ b", []string{"a", "<@", "b"}},
		{"1 / 2", []string{"1", "/", "2"}},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			toks, err := Lex(tt.input, cfg)
			if err != nil {
				t.Fatalf("Lex(%q): %v", tt.input, err)
			}
			var got []string
			for _, tok := range toks {
				if tok.Significant() {
					got = append(got, tok.Text)
				}
			}
			if len(got) != len(tt.want) {
				t.Fatalf("Lex(%q) = %v, want %v", tt.input, got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("Lex(%q) token %d = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestPlaceholderNamesFollowSQLite pins the characters a bound parameter's name
// may hold. SQLite reads a dollar sign and a digit as name characters, so
// splitting the name there produced two tokens where the caller wrote one, and
// the space between them made SQL that no longer parses.
func TestPlaceholderNamesFollowSQLite(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialects.Dialect
		input   string
		want    string
	}{
		{dialects.MySQL, "@a$b", "@a$b"},
		{dialects.MySQL, ":1abc", ":1abc"},
		{dialects.MySQL, "@0$0", "@0$0"},
		{dialects.PostgreSQL, "$1", "$1"},
		{dialects.PostgreSQL, ":name", ":name"},
		{dialects.MySQL, "?1", "?1"},
		{dialects.MySQL, "@name", "@name"},
	} {
		t.Run(tt.dialect.DisplayName()+" "+tt.input, func(t *testing.T) {
			t.Parallel()

			cfg, _ := ConfigFor(tt.dialect)
			toks, err := Lex(tt.input, cfg)
			if err != nil {
				t.Fatalf("Lex(%q): %v", tt.input, err)
			}
			var got []Token
			for _, tok := range toks {
				if tok.Significant() {
					got = append(got, tok)
				}
			}
			if len(got) != 1 {
				t.Fatalf("Lex(%q) produced %d tokens, want one", tt.input, len(got))
			}
			if got[0].Kind != Placeholder || got[0].Text != tt.want {
				t.Errorf("Lex(%q) = %v %q, want a placeholder %q", tt.input, got[0].Kind, got[0].Text, tt.want)
			}
		})
	}
}
