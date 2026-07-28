package dialect

import "testing"

// FuzzTranslate checks that translation never panics and that a successful
// translation is stable under the SQLite identity dialect (its output feeds
// back through Translate(SQLite, ...) unchanged), across every built-in dialect.
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
			again, err := Translate(SQLite, out)
			if err != nil {
				t.Fatalf("Translate(SQLite, %q) after Translate(%s): %v", out, d, err)
			}
			if again != out {
				t.Fatalf("not idempotent for dialect %s: %q -> %q -> %q", d, query, out, again)
			}
		}
	})
}
