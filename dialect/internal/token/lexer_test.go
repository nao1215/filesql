package token

import (
	"errors"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// TestLexPositions holds every token to carrying the place it was written.
// Every diagnostic in the package is built from these, so a token with no
// position is a message with no position.
func TestLexPositions(t *testing.T) {
	t.Parallel()

	cfg, _ := ConfigFor(dialects.PostgreSQL)
	toks, err := Lex("SELECT a,\n  b\nFROM t", cfg)
	if err != nil {
		t.Fatalf("Lex: %v", err)
	}
	var lines []int
	for _, tok := range toks {
		if !tok.Significant() {
			continue
		}
		lines = append(lines, tok.Line)
		if tok.Col < 1 {
			t.Errorf("token %q has column %d", tok.Text, tok.Col)
		}
		if tok.End <= tok.Offset {
			t.Errorf("token %q spans %d..%d", tok.Text, tok.Offset, tok.End)
		}
	}
	want := []int{1, 1, 1, 2, 3, 3}
	if len(lines) != len(want) {
		t.Fatalf("read %d significant tokens, want %d", len(lines), len(want))
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("token %d is on line %d, want %d", i, lines[i], want[i])
		}
	}
}

// TestLexRefusesWhatItCannotFinish covers the literals that never end, which
// are refused with a position rather than read to the end of the query.
func TestLexRefusesWhatItCannotFinish(t *testing.T) {
	t.Parallel()

	cfg, _ := ConfigFor(dialects.PostgreSQL)
	for _, input := range []string{"SELECT 'abc", `SELECT "abc`, "SELECT 1 /* x", "SELECT $$abc"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			if _, err := Lex(input, cfg); !errors.Is(err, sqlerr.ErrInvalidSyntax) {
				t.Errorf("Lex(%q) error = %v, want ErrInvalidSyntax", input, err)
			}
		})
	}
}

// TestPostgreSQLRejectsNumericTrailingJunk pins the lexical rule PostgreSQL
// added for a number written directly against identifier characters: the text
// is one malformed numeric literal, not a number followed by an implicit alias.
// Reading it as two tokens made "SELECT 123abc" answer 1 under the alias abc,
// which is a query PostgreSQL never ran.
func TestPostgreSQLRejectsNumericTrailingJunk(t *testing.T) {
	t.Parallel()

	cfg, _ := ConfigFor(dialects.PostgreSQL)
	for _, input := range []string{
		"SELECT 123abc",
		"SELECT 1.5abc",
		"SELECT 1e2abc",
		"SELECT 1e",
		"SELECT 0xdeadbeefg",
	} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			if _, err := Lex(input, cfg); !errors.Is(err, sqlerr.ErrInvalidSyntax) {
				t.Errorf("Lex(%q) error = %v, want ErrInvalidSyntax", input, err)
			}
		})
	}

	for _, input := range []string{"SELECT 123 abc", "SELECT 0xdeadbeef"} {
		if _, err := Lex(input, cfg); err != nil {
			t.Errorf("Lex(%q) error = %v, want nil", input, err)
		}
	}
}

// TestIntegerPrefixesByDialect pins which base prefixes each dialect reads and
// how the literal reaches SQLite: PostgreSQL's 0b, 0o and 0x and GoogleSQL's 0x
// in decimal, MySQL's 0x and 0b as written for the parser to classify, and
// MySQL's digit-led names as one word.
func TestIntegerPrefixesByDialect(t *testing.T) {
	t.Parallel()

	pg, _ := ConfigFor(dialects.PostgreSQL)
	google, _ := ConfigFor(dialects.GoogleSQL)
	mysql, _ := ConfigFor(dialects.MySQL)
	for _, tt := range []struct {
		name  string
		cfg   Config
		input string
		kind  Kind
		text  string
	}{
		{"one binary digit", pg, "0b1", Number, "1"},
		{"an octal zero", pg, "0o10", Number, "8"},
		{"the largest octal digit", pg, "0o7", Number, "7"},
		{"hexadecimal", pg, "0xff", Number, "255"},
		{"past 64 bits", pg, "0x10000000000000000", Number, "18446744073709551616"},
		{"GoogleSQL hexadecimal", google, "0x1F", Number, "31"},
		{"GoogleSQL binary is left for the lowering to refuse", google, "0b1", Number, "0b1"},
		{"MySQL hexadecimal is left for the parser", mysql, "0x1F", Number, "0x1F"},
		{"MySQL digits run into a name", mysql, "1abc", Word, "1abc"},
		{"MySQL uppercase prefix", mysql, "0X41", Word, "0X41"},
		{"MySQL prefix with no digits", mysql, "0x", Word, "0x"},
		{"MySQL exponent without digits", mysql, "1e", Word, "1e"},
	} {
		toks, err := Lex(tt.input, tt.cfg)
		if err != nil || len(toks) != 1 || toks[0].Kind != tt.kind || toks[0].Text != tt.text {
			t.Errorf("%s: Lex(%q) = %+v, %v; want one token %q", tt.name, tt.input, toks, err, tt.text)
		}
	}

	// A prefix with no digit after it, and an octal prefix where the dialect
	// has none, are errors.
	for _, tt := range []struct {
		cfg   Config
		input string
	}{{pg, "0o"}, {pg, "0x"}, {pg, "0o8"}, {google, "0o7"}, {google, "0x"}} {
		if _, err := Lex(tt.input, tt.cfg); !errors.Is(err, sqlerr.ErrInvalidSyntax) {
			t.Errorf("Lex(%q) error = %v, want ErrInvalidSyntax", tt.input, err)
		}
	}
}

// TestPostgreSQLDigitSeparators follows PostgreSQL 16, which lets an
// underscore stand between two digits of a numeric literal, and right after
// the prefix of a hexadecimal one, for visual grouping. The literal is one
// token whose text is its digits without the separators: the value, which is
// what SQLite has to read. An underscore anywhere else is not a separator, so
// the literal ends before it and the rest is trailing junk.
func TestPostgreSQLDigitSeparators(t *testing.T) {
	t.Parallel()

	cfg, _ := ConfigFor(dialects.PostgreSQL)
	for input, want := range map[string]string{
		"1_000":         "1000",
		"1_500_000_000": "1500000000",
		"0xFFFF_FFFF":   "4294967295",
		"0x_FF":         "255",
		"0b1010_0101":   "165",
		"0o_7_7":        "63",
		"1_000.5":       "1000.5",
		"1.618_034":     "1.618034",
		".5_0":          ".50",
		"1e1_0":         "1e10",
		"1_0e-1_0":      "10e-10",
	} {
		toks, err := Lex(input, cfg)
		if err != nil {
			t.Errorf("Lex(%q) error = %v, want one number", input, err)
			continue
		}
		if len(toks) != 1 || toks[0].Kind != Number || toks[0].Text != want {
			t.Errorf("Lex(%q) = %+v, want one Number %q", input, toks, want)
			continue
		}
		if toks[0].End != len(input) {
			t.Errorf("Lex(%q) number ends at %d, want %d: the source spelling is lost", input, toks[0].End, len(input))
		}
	}

	for _, input := range []string{"SELECT 1_", "SELECT 1__0", "SELECT 1_.5", "SELECT 1._5", "SELECT 1e_1", "SELECT 0x__1", "SELECT 0x_"} {
		if _, err := Lex(input, cfg); !errors.Is(err, sqlerr.ErrInvalidSyntax) {
			t.Errorf("Lex(%q) error = %v, want ErrInvalidSyntax", input, err)
		}
	}

	// A leading underscore starts a name, as it always has.
	toks, err := Lex("_1", cfg)
	if err != nil || len(toks) != 1 || toks[0].Kind != Word {
		t.Errorf("Lex(%q) = %+v, %v; want one Word", "_1", toks, err)
	}

	// MySQL has no digit separator: there 1_000 is a name, as 1abc is.
	mysql, _ := ConfigFor(dialects.MySQL)
	toks, err = Lex("1_000", mysql)
	if err != nil || len(toks) != 1 || toks[0].Kind != Word || toks[0].Text != "1_000" {
		t.Errorf("Lex(MySQL, %q) = %+v, %v; want one Word", "1_000", toks, err)
	}
}

// TestUnknownDialectHasNoConfig keeps a dialect this package does not implement
// from being lexed with someone else's rules.
func TestUnknownDialectHasNoConfig(t *testing.T) {
	t.Parallel()

	if _, ok := ConfigFor(dialects.Dialect("oracle")); ok {
		t.Error("ConfigFor answered a configuration for a dialect this package does not implement")
	}
	if _, ok := ConfigFor(dialects.SQLite); ok {
		t.Error("ConfigFor answered a configuration for SQLite, which is not translated")
	}
}

// TestDoubleDashOpensACommentPerDialect pins which dialect reads a double dash
// as a comment and which reads it as two minus signs. MySQL asks for a blank or
// a control character after the dashes, so "1--1" there is one minus negative
// one; every other dialect opens a comment whatever follows. The arithmetic
// values were read from mysql:8.4 and postgres:17 rather than derived, since
// the point is that the two engines disagree.
func TestDoubleDashOpensACommentPerDialect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dialect dialects.Dialect
		query   string
		comment bool
	}{
		{dialects.MySQL, "SELECT 1--1", false},
		{dialects.MySQL, "SELECT 1--1+5", false},
		{dialects.MySQL, "SELECT 5---3", false},
		{dialects.MySQL, "SELECT 1-- 1", true},
		{dialects.MySQL, "SELECT 1--\t1", true},
		{dialects.MySQL, "SELECT 1--\n1", true},
		{dialects.MySQL, "SELECT 1--", true},
		{dialects.PostgreSQL, "SELECT 1--1", true},
		{dialects.PostgreSQL, "SELECT 1-- 1", true},
		{dialects.PostgreSQL, "SELECT 5---3", true},
		{dialects.GoogleSQL, "SELECT 1--1", true},
		{dialects.GoogleSQL, "SELECT 1-- 1", true},
	}

	for _, tt := range tests {
		t.Run(string(tt.dialect)+" "+tt.query, func(t *testing.T) {
			t.Parallel()

			cfg, ok := ConfigFor(tt.dialect)
			if !ok {
				t.Fatalf("no config for %s", tt.dialect)
			}
			toks, err := Lex(tt.query, cfg)
			if err != nil {
				t.Fatalf("Lex(%q): %v", tt.query, err)
			}
			var sawComment bool
			for _, tok := range toks {
				if tok.Kind == LineComment {
					sawComment = true
				}
			}
			if sawComment != tt.comment {
				t.Errorf("Lex(%s, %q) read a comment: %v, want %v",
					tt.dialect, tt.query, sawComment, tt.comment)
			}
		})
	}
}

// TestABacktickIdentifierTakesTheEscapesItsDialectGives pins the other lexical
// rule that was applied to every dialect at once. BigQuery lists the string
// escape sequences among what a backtick-quoted identifier accepts, an escaped
// backtick with them, so a name written with one closed early. MySQL has none
// there: a backtick is doubled, and a backslash is a backslash, which is why
// mysql:8.4 refuses the same text as a syntax error.
func TestABacktickIdentifierTakesTheEscapesItsDialectGives(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dialect dialects.Dialect
		query   string
		want    string
		wantErr bool
	}{
		{dialect: dialects.GoogleSQL, query: "SELECT `a\\`b`", want: "a`b"},
		{dialect: dialects.GoogleSQL, query: "SELECT `a\\nb`", want: "a\nb"},
		{dialect: dialects.GoogleSQL, query: "SELECT `ab`", want: "ab"},
		{dialect: dialects.MySQL, query: "SELECT `a``b`", want: "a`b"},
		{dialect: dialects.MySQL, query: "SELECT `a\\nb`", want: "a\\nb"},
		{dialect: dialects.MySQL, query: "SELECT `a\\`b`", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(string(tt.dialect)+" "+tt.query, func(t *testing.T) {
			t.Parallel()

			cfg, _ := ConfigFor(tt.dialect)
			toks, err := Lex(tt.query, cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Lex(%s, %q) read a name, want a refusal", tt.dialect, tt.query)
				}
				return
			}
			if err != nil {
				t.Fatalf("Lex(%q): %v", tt.query, err)
			}
			var got string
			for _, tok := range toks {
				if tok.Kind == QuotedIdent {
					got = tok.Text
				}
			}
			if got != tt.want {
				t.Errorf("Lex(%s, %q) read %q, want %q", tt.dialect, tt.query, got, tt.want)
			}
		})
	}
}
