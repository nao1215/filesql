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
