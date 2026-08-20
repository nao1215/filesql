package dialect

import "testing"

// sqliteLex is how SQLite reads the SQL a translation produces: double quotes
// open an identifier, a backslash is an ordinary character, and comments do not
// nest.
var sqliteLex = lexConfig{identDoubleQuote: true}

// FuzzTranslate checks that translation never panics, that it is deterministic,
// and that what it produces is still SQL: the output has to lex as SQLite,
// which is the language it is now written in.
//
// The check used to feed the output through Translate(SQLite, ...), which
// returns its input untouched, so it compared a string with itself and passed
// whatever the translation had produced. Feeding the output back through its
// own dialect is not the property either — the output is SQLite, and reading
// SQLite as MySQL turns a double-quoted identifier into a string. Lexing it as
// SQLite is what is actually being claimed, and it catches an output that ends
// a literal or a comment somewhere the input did not.
func FuzzTranslate(f *testing.F) {
	seeds := []string{
		"SELECT * FROM t",
		"SELECT `a`, \"b\" FROM `t` WHERE x = 'v'",
		"SELECT a::int FROM t -- c",
		"SELECT $$d$$, E'e\\n', $1",
		"SELECT r'raw', b'AB', `p.d.t`.x # h",
		"SELECT count(*) /* mid */ FROM t;",
		"'unterminated",
		"/* unterminated",
		"",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, query string) {
		for _, d := range Dialects() {
			out, err := Translate(d, query)
			if err != nil {
				continue
			}
			again, err := Translate(d, query)
			if err != nil {
				t.Fatalf("Translate(%s, %q) succeeded then failed: %v", d, query, err)
			}
			if again != out {
				t.Fatalf("Translate(%s, %q) is not deterministic: %q then %q", d, query, out, again)
			}
			if d == SQLite {
				// SQLite is the identity translation: it hands back whatever it was
				// given, so the output is only as well-formed as the input was.
				continue
			}
			if _, err := tokenize(out, sqliteLex); err != nil {
				t.Fatalf("Translate(%s, %q) produced %q, which does not lex as SQLite: %v", d, query, out, err)
			}
		}
	})
}
