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
