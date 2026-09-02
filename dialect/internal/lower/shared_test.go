package lower_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect"
)

// TestTranslateRefusesFormsItCannotRead covers the calls that only look like
// the ones a rule handles. Each used to be handed to SQLite unchanged, on the
// theory that a rewrite on a guess is worse than no rewrite; what reached SQLite
// was then a query with a keyword form SQLite does not have, and the error named
// SQLite's parser rather than the caller's construct. The parser reads them now,
// so each is refused where it was written.
func TestTranslateRefusesFormsItCannotRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect dialect.Dialect
		sql     string
		want    error
	}{
		{name: "EXTRACT without a part", dialect: dialect.PostgreSQL, sql: `SELECT EXTRACT(x) FROM t`, want: dialect.ErrInvalidSyntax},
		{name: "EXTRACT without FROM", dialect: dialect.PostgreSQL, sql: `SELECT EXTRACT(year x) FROM t`, want: dialect.ErrInvalidSyntax},
		{name: "CAST without AS", dialect: dialect.PostgreSQL, sql: `SELECT CAST(x) FROM t`, want: dialect.ErrInvalidSyntax},
		{name: "CAST to something that is not a type name", dialect: dialect.PostgreSQL, sql: `SELECT CAST(x AS 3) FROM t`, want: dialect.ErrInvalidSyntax},
		{name: "DATE_ADD without a second argument", dialect: dialect.MySQL, sql: `SELECT DATE_ADD(d) FROM t`, want: dialect.ErrUnsupportedSyntax},
		{name: "DATE_ADD without INTERVAL", dialect: dialect.MySQL, sql: `SELECT DATE_ADD(d, 3) FROM t`, want: dialect.ErrUnsupportedSyntax},
		{name: "SIMILAR without TO", dialect: dialect.PostgreSQL, sql: `SELECT x SIMILAR t FROM t`, want: dialect.ErrInvalidSyntax},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := dialect.Translate(tt.dialect, tt.sql); !errors.Is(err, tt.want) {
				t.Fatalf("Translate(%q) error = %v, want %v", tt.sql, err, tt.want)
			}
		})
	}
}

// TestTranslateRefusesACastToATypeItDoesNotKnow covers the target no helper
// converts to. Leaving it on SQLite's own CAST was not leaving it alone:
// SQLite applies numeric affinity to a type it has never heard of, so a value
// came back as the number its leading digits spell.
func TestTranslateRefusesACastToATypeItDoesNotKnow(t *testing.T) {
	t.Parallel()

	const query = `SELECT CAST(x AS quux) FROM t`
	got, err := dialect.Translate(dialect.PostgreSQL, query)
	if !errors.Is(err, dialect.ErrUnsupportedSyntax) {
		t.Fatalf("Translate(%q) = %q, error = %v, want ErrUnsupportedSyntax", query, got, err)
	}
	if !strings.Contains(err.Error(), "quux") {
		t.Errorf("Translate(%q) error = %q, want it to name the type", query, err)
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
		kind error
		want string
	}{
		{
			// An interval with no unit and one with no value are queries that
			// cannot be read rather than constructs with no SQLite form.
			name: "no unit",
			sql:  `SELECT DATE_ADD(d, INTERVAL 1) FROM t`,
			kind: dialect.ErrInvalidSyntax,
			want: "expected an interval unit",
		},
		{
			name: "a unit no dialect defines",
			sql:  `SELECT DATE_ADD(d, INTERVAL 1 FORTNIGHT) FROM t`,
			kind: dialect.ErrUnsupportedSyntax,
			want: "FORTNIGHT is not an interval unit",
		},
		{
			name: "no value",
			sql:  `SELECT DATE_ADD(d, INTERVAL DAY) FROM t`,
			kind: dialect.ErrInvalidSyntax,
			want: "expected an interval unit",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := dialect.Translate(dialect.MySQL, tt.sql)
			if !errors.Is(err, tt.kind) {
				t.Fatalf("Translate(%q) error = %v, want %v", tt.sql, err, tt.kind)
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

			got, err := dialect.Translate(dialect.PostgreSQL, tt.sql)
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

	got, err := dialect.Translate(dialect.MySQL, `SELECT DATE_SUB(d, INTERVAL 1 MONTH) FROM t`)
	if err != nil {
		t.Fatalf("Translate error: %v", err)
	}
	if !strings.Contains(got, `interval_add(d, -1, 'month')`) {
		t.Fatalf("Translate = %q, want the amount negated through interval_add", got)
	}
}
