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

func TestRegisterTranslator(t *testing.T) {
	// Not parallel: mutates the process-global translator registry.
	const custom = "CUSTOM OUTPUT"
	RegisterTranslator(MySQL, func(string) (string, error) {
		return custom, nil
	})
	t.Cleanup(func() { RegisterTranslator(MySQL, nil) })

	got, err := Translate(MySQL, "anything at all")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != custom {
		t.Fatalf("Translate(MySQL) = %q, want %q from custom translator", got, custom)
	}

	// Removing the custom translator restores the built-in behavior.
	RegisterTranslator(MySQL, nil)
	got, err = Translate(MySQL, "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error after removing translator: %v", err)
	}
	if got != "SELECT 1" {
		t.Fatalf("Translate(MySQL, %q) = %q, want built-in result", "SELECT 1", got)
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
