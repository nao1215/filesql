package dialect

import (
	"strings"
	"testing"
)

// TestAggregateTranslation covers the aggregates SQLite does not have. The
// driver has no aggregate registration hook, so each is rewritten into an
// equivalent SQLite expression rather than filled in by a helper function.
func TestAggregateTranslation(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	// Two-row and three-row sources, so a sample estimator has something to
	// divide by.
	const pair = `FROM (SELECT 1 AS x UNION ALL SELECT 3) t`
	const bools = `FROM (SELECT 1 AS x UNION ALL SELECT 0) t`

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
	}{
		{"googlesql countif", GoogleSQL, `SELECT COUNTIF(x > 1) ` + pair, "1"},
		{"googlesql countif none", GoogleSQL, `SELECT COUNTIF(x > 99) ` + pair, "0"},
		{"googlesql countif over no rows", GoogleSQL, `SELECT COUNTIF(x > 1) FROM (SELECT 1 AS x WHERE 0) t`, "0"},
		{"googlesql logical_and", GoogleSQL, `SELECT LOGICAL_AND(x) ` + bools, "0"},
		{"googlesql logical_or", GoogleSQL, `SELECT LOGICAL_OR(x) ` + bools, "1"},
		{"postgresql bool_and", PostgreSQL, `SELECT BOOL_AND(x) ` + bools, "0"},
		{"postgresql bool_or", PostgreSQL, `SELECT BOOL_OR(x) ` + bools, "1"},
		{"mysql any_value", MySQL, `SELECT ANY_VALUE(x) ` + pair, "1"},

		// PostgreSQL and GoogleSQL default to the sample estimator; MySQL's
		// STDDEV and VARIANCE are the population ones.
		{"postgresql stddev", PostgreSQL, `SELECT STDDEV(x) ` + pair, "1.4142135623730951"},
		{"postgresql variance", PostgreSQL, `SELECT VARIANCE(x) ` + pair, "2"},
		{"postgresql stddev_pop", PostgreSQL, `SELECT STDDEV_POP(x) ` + pair, "1"},
		{"postgresql var_pop", PostgreSQL, `SELECT VAR_POP(x) ` + pair, "1"},
		{"googlesql stddev", GoogleSQL, `SELECT STDDEV(x) ` + pair, "1.4142135623730951"},
		{"mysql std is population", MySQL, `SELECT STD(x) ` + pair, "1"},
		{"mysql variance is population", MySQL, `SELECT VARIANCE(x) ` + pair, "1"},
		{"mysql stddev_samp", MySQL, `SELECT STDDEV_SAMP(x) ` + pair, "1.4142135623730951"},

		// MySQL keeps its separator next to an ORDER BY, where SQLite would
		// otherwise read it as another sort term.
		{"mysql group_concat separator", MySQL, `SELECT GROUP_CONCAT(x SEPARATOR '|') ` + pair, "1|3"},
		{"mysql group_concat ordered", MySQL, `SELECT GROUP_CONCAT(x ORDER BY x DESC SEPARATOR '|') ` + pair, "3|1"},
		{"mysql group_concat distinct", MySQL, `SELECT GROUP_CONCAT(DISTINCT x) ` + pair, "1,3"},

		// SIMILAR TO is SQL-standard pattern matching SQLite does not have.
		{"postgresql similar to", PostgreSQL, `SELECT 'abc' SIMILAR TO 'a%'`, "1"},
		{"postgresql similar to anchors", PostgreSQL, `SELECT 'abc' SIMILAR TO 'b'`, "0"},
		{"postgresql similar to alternation", PostgreSQL, `SELECT 'abc' SIMILAR TO '(abc|def)'`, "1"},
		{"postgresql similar to underscore", PostgreSQL, `SELECT 'abc' SIMILAR TO 'a_c'`, "1"},
		{"postgresql not similar to", PostgreSQL, `SELECT 'abc' NOT SIMILAR TO 'b'`, "1"},

		// TO_CHAR formats numbers, not only dates.
		{"postgresql to_char number", PostgreSQL, `SELECT TRIM(TO_CHAR(1234.5, '9999.99'))`, "1234.50"},
		{"postgresql to_char rounds", PostgreSQL, `SELECT TRIM(TO_CHAR(1.567, '9.99'))`, "1.57"},
		{"postgresql to_char groups", PostgreSQL, `SELECT TRIM(TO_CHAR(1234567, '9,999,999'))`, "1,234,567"},
		{"postgresql to_char negative", PostgreSQL, `SELECT TRIM(TO_CHAR(-42, '999'))`, "-42"},
		{"postgresql to_char keeps dates", PostgreSQL, `SELECT TO_CHAR('2026-01-15', 'YYYY/MM/DD')`, "2026/01/15"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if !got.Valid || got.String != tt.want {
				t.Fatalf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestSampleEstimatorOverOneRow keeps the sample estimators NULL for a single
// value, which is what the source dialects return and what dividing by
// COUNT - 1 produces.
func TestSampleEstimatorOverOneRow(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	got, err := runDialect(t, db, PostgreSQL, `SELECT STDDEV(x) FROM (SELECT 1 AS x) t`)
	if err != nil {
		t.Fatalf("STDDEV: %v", err)
	}
	if got.Valid {
		t.Fatalf("STDDEV over one row = %q, want NULL", got.String)
	}
}

// TestGroupConcatDistinctWithSeparatorIsRejected reports the one MySQL
// GROUP_CONCAT shape SQLite cannot express, rather than letting the engine
// complain about an argument count the caller never wrote.
func TestGroupConcatDistinctWithSeparatorIsRejected(t *testing.T) {
	t.Parallel()

	_, err := Translate(MySQL, `SELECT GROUP_CONCAT(DISTINCT x SEPARATOR '|') FROM t`)
	if err == nil {
		t.Fatal("GROUP_CONCAT(DISTINCT ... SEPARATOR ...) should be rejected")
	}
	if !strings.Contains(err.Error(), "DISTINCT with SEPARATOR") {
		t.Fatalf("error = %v, want it to name the combination", err)
	}
}

// TestNestedAggregateRewrite covers an aggregate inside another one, which the
// pass handles by rewriting the argument first.
func TestNestedAggregateRewrite(t *testing.T) {
	t.Parallel()

	got, err := Translate(GoogleSQL, `SELECT COUNTIF(LOGICAL_AND(x)) FROM t`)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !strings.Contains(got, "MIN(x)") {
		t.Fatalf("Translate = %q, want the inner LOGICAL_AND rewritten", got)
	}
}

func TestSimilarToRegexp(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"a%", "^a.*$"},
		{"a_c", "^a.c$"},
		{"a.c", `^a\.c$`},
		{"(a|b)", "^(a|b)$"},
		{"a+", "^a+$"},
	}
	for _, tt := range tests {
		if got := similarToRegexp(tt.in); got != tt.want {
			t.Fatalf("similarToRegexp(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestGroupThousands(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"1", "1"},
		{"123", "123"},
		{"1234", "1,234"},
		{"1234567", "1,234,567"},
	}
	for _, tt := range tests {
		if got := groupThousands(tt.in); got != tt.want {
			t.Fatalf("groupThousands(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
