package dialect

import (
	"errors"
	"strings"
	"testing"
)

// TestTranslate_LeavesUnrecognizedFormsAlone covers the rewrite rules' "not this
// form" answers. A call that only looks like the one a rule handles is passed
// through unchanged: rewriting it on a guess would turn a query the backend
// understands into one it does not, and the caller never wrote the rewritten
// form.
func TestTranslate_LeavesUnrecognizedFormsAlone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect Dialect
		sql     string
	}{
		{name: "EXTRACT without a part", dialect: PostgreSQL, sql: `SELECT EXTRACT(x) FROM t`},
		{name: "EXTRACT without FROM", dialect: PostgreSQL, sql: `SELECT EXTRACT(year x) FROM t`},
		{name: "CAST without AS", dialect: PostgreSQL, sql: `SELECT CAST(x) FROM t`},
		{name: "CAST to something that is not a type name", dialect: PostgreSQL, sql: `SELECT CAST(x AS 3) FROM t`},
		{name: "CAST to a type this package does not know", dialect: PostgreSQL, sql: `SELECT CAST(x AS quux) FROM t`},
		{name: "DATE_ADD without a second argument", dialect: MySQL, sql: `SELECT DATE_ADD(d) FROM t`},
		{name: "DATE_ADD without INTERVAL", dialect: MySQL, sql: `SELECT DATE_ADD(d, 3) FROM t`},
		{name: "SIMILAR without TO", dialect: PostgreSQL, sql: `SELECT x SIMILAR t FROM t`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Translate(tt.dialect, tt.sql)
			if err != nil {
				t.Fatalf("Translate(%q) error: %v", tt.sql, err)
			}
			if got != tt.sql {
				t.Fatalf("Translate(%q) = %q, want it unchanged", tt.sql, got)
			}
		})
	}
}

// TestTranslate_RefusesIntervalsItCannotRepresent covers the INTERVAL forms that
// are recognized and cannot be carried out. Each is refused by name rather than
// dropped, because an interval silently left out of a query answers with rows
// from the wrong dates.
func TestTranslate_RefusesIntervalsItCannotRepresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "no unit",
			sql:  `SELECT DATE_ADD(d, INTERVAL 1) FROM t`,
			want: "missing a unit",
		},
		{
			name: "a unit no dialect defines",
			sql:  `SELECT DATE_ADD(d, INTERVAL 1 FORTNIGHT) FROM t`,
			want: "unsupported INTERVAL unit",
		},
		{
			name: "no value",
			sql:  `SELECT DATE_ADD(d, INTERVAL DAY) FROM t`,
			want: "missing a value",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := Translate(MySQL, tt.sql)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Fatalf("Translate(%q) error = %v, want ErrUnsupportedSyntax", tt.sql, err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Translate(%q) error = %q, want it to mention %q", tt.sql, err, tt.want)
			}
		})
	}
}

// TestTranslate_CastKeepsTypeParameters checks that a parameterized type reaches
// the cast helper whole. Dropping the parameters would turn CHAR(3) into CHAR
// and DECIMAL(10,2) into DECIMAL, so a value would be cast to a different type
// than the one the query names.
func TestTranslate_CastKeepsTypeParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "a length", sql: `SELECT CAST(x AS CHAR(3)) FROM t`, want: `'CHAR(3)'`},
		{name: "a precision and scale", sql: `SELECT CAST(x AS DECIMAL(10,2)) FROM t`, want: `'DECIMAL(10,2)'`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Translate(PostgreSQL, tt.sql)
			if err != nil {
				t.Fatalf("Translate(%q) error: %v", tt.sql, err)
			}
			if !strings.Contains(got, tt.want) {
				t.Fatalf("Translate(%q) = %q, want it to carry %s", tt.sql, got, tt.want)
			}
		})
	}
}

// TestTranslate_DateSubNegatesTheAmount pins that subtracting an interval is the
// same helper with the amount negated, so the month clamping is applied in both
// directions rather than only when adding.
func TestTranslate_DateSubNegatesTheAmount(t *testing.T) {
	t.Parallel()

	got, err := Translate(MySQL, `SELECT DATE_SUB(d, INTERVAL 1 MONTH) FROM t`)
	if err != nil {
		t.Fatalf("Translate error: %v", err)
	}
	if !strings.Contains(got, `interval_add(d, -(1), 'month')`) {
		t.Fatalf("Translate = %q, want the amount negated through interval_add", got)
	}
}
