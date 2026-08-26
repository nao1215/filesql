package dialect

import (
	"database/sql/driver"
	"errors"
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
	const pairs = `FROM (SELECT 1 AS x, 2 AS y UNION ALL SELECT 2, 4 UNION ALL SELECT 3, 7) t`

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

		// BigQuery's APPROX_COUNT_DISTINCT estimates what SQLite counts
		// exactly, which is a correct answer to the question it asks.
		{"googlesql approx_count_distinct", GoogleSQL, `SELECT APPROX_COUNT_DISTINCT(x) FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t`, "2"},

		// The correlation and the covariances, over the three pairs BigQuery
		// was asked for the same answers with. A row where either side is NULL
		// takes no part in any of them.
		{"googlesql corr", GoogleSQL, `SELECT CORR(x, y) ` + pairs, "0.9933992677987828"},
		{"googlesql covar_pop", GoogleSQL, `SELECT COVAR_POP(x, y) ` + pairs, "1.6666666666666667"},
		{"googlesql covar_samp", GoogleSQL, `SELECT COVAR_SAMP(x, y) ` + pairs, "2.5"},
		{"googlesql corr of a column with itself", GoogleSQL, `SELECT CORR(x, x) ` + pairs, "1"},
		{"googlesql covar_pop over one row", GoogleSQL, `SELECT COVAR_POP(x, x) FROM (SELECT 1 AS x) t`, "0"},
		{"googlesql covar_pop skips an incomplete pair", GoogleSQL, `SELECT COVAR_POP(x, y) FROM (SELECT 1 AS x, 2 AS y UNION ALL SELECT 2, NULL UNION ALL SELECT 3, 7) t`, "2.5"},

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

// TestStringAndJSONGaps covers the string and JSON spellings each dialect has
// and SQLite does not, plus the two places SQLite's own function means something
// different from the dialect's.
func TestStringAndJSONGaps(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
	}{
		// MySQL LENGTH counts bytes, SQLite's counts characters. The query
		// succeeds either way, so the difference is silent.
		{"mysql length is bytes", MySQL, `SELECT LENGTH('あい')`, "6"},
		{"mysql char_length is characters", MySQL, `SELECT CHAR_LENGTH('あい')`, "2"},
		{"mysql ord ascii", MySQL, `SELECT ORD('A')`, "65"},
		{"mysql ord multibyte", MySQL, `SELECT ORD('あ')`, "14909826"},
		{"mysql json_unquote", MySQL, `SELECT JSON_UNQUOTE('"x"')`, "x"},
		{"mysql json_unquote passthrough", MySQL, `SELECT JSON_UNQUOTE('x')`, "x"},
		{"mysql trim both", MySQL, `SELECT TRIM(BOTH 'x' FROM 'xxaxx')`, "a"},
		{"mysql trim leading", MySQL, `SELECT TRIM(LEADING 'x' FROM 'xxaxx')`, "axx"},
		{"mysql trim trailing", MySQL, `SELECT TRIM(TRAILING 'x' FROM 'xxaxx')`, "xxa"},
		{"mysql trim from only", MySQL, `SELECT TRIM(FROM '  a  ')`, "a"},
		{"mysql union distinct", MySQL, `SELECT 1 UNION DISTINCT SELECT 1`, "1"},

		{"postgresql btrim", PostgreSQL, `SELECT BTRIM('xxaxx', 'x')`, "a"},
		{"postgresql trim both", PostgreSQL, `SELECT TRIM(BOTH 'x' FROM 'xxaxx')`, "a"},
		{"postgresql overlay", PostgreSQL, `SELECT OVERLAY('abcdef' PLACING 'XY' FROM 2)`, "aXYdef"},
		{"postgresql overlay with for", PostgreSQL, `SELECT OVERLAY('abcdef' PLACING 'XY' FROM 2 FOR 4)`, "aXYf"},
		{"postgresql jsonb_array_length", PostgreSQL, `SELECT JSONB_ARRAY_LENGTH('[1,2,3]'::jsonb)`, "3"},
		{"postgresql char_length", PostgreSQL, `SELECT CHAR_LENGTH('あい')`, "2"},

		{"googlesql json_value", GoogleSQL, `SELECT JSON_VALUE('{"a":"x"}', '$.a')`, "x"},
		// JSON_QUERY keeps its result in JSON text, so a string value stays
		// quoted; JSON_VALUE returns the value itself.
		{"googlesql json_query keeps quotes", GoogleSQL, `SELECT JSON_QUERY('{"a":"x"}', '$.a')`, `"x"`},
		{"googlesql json_query object", GoogleSQL, `SELECT JSON_QUERY('{"a":{"b":1}}', '$.a')`, `{"b":1}`},
		{"googlesql byte_length", GoogleSQL, `SELECT BYTE_LENGTH('あい')`, "6"},
		{"googlesql char_length", GoogleSQL, `SELECT CHAR_LENGTH('あい')`, "2"},
		{"googlesql union distinct", GoogleSQL, `SELECT 1 UNION DISTINCT SELECT 1`, "1"},
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

// TestPostgreSQLArrayLiteralIsRejected reports an array literal by name instead
// of letting SQLite fail on the bracket, which says nothing useful.
func TestPostgreSQLArrayLiteralIsRejected(t *testing.T) {
	t.Parallel()

	_, err := Translate(PostgreSQL, `SELECT name FROM t WHERE name LIKE ANY(ARRAY['a%'])`)
	if err == nil {
		t.Fatal("an array literal should be rejected")
	}
	if !strings.Contains(err.Error(), "array literals are not supported") {
		t.Fatalf("error = %v, want it to name array literals", err)
	}
}

// TestOverlayBoundaries covers the OVERLAY offsets that fall outside the target.
func TestOverlayBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		args []driver.Value
		want string
	}{
		{[]driver.Value{"abc", "X", int64(1)}, "Xbc"},
		{[]driver.Value{"abc", "X", int64(9)}, "abcX"},
		{[]driver.Value{"abc", "XY", int64(2), int64(0)}, "aXYbc"},
		// PostgreSQL defines the call as substring(s from 1 for start-1) ||
		// replacement || substring(s from start+count), so a negative count
		// makes the tail begin before the overlaid position and part of the
		// string is repeated. Checked against PostgreSQL 17.
		{[]driver.Value{"abc", "XY", int64(2), int64(-1)}, "aXYabc"},
		{[]driver.Value{"abc", "XY", int64(2), int64(99)}, "aXY"},
		// A count near math.MaxInt64 is an ordinary SQLite integer literal and
		// must not wrap into a negative slice bound.
		{[]driver.Value{"ab", "X", int64(2), int64(9223372036854775807)}, "aX"},
		{[]driver.Value{"ab", "X", int64(9223372036854775807)}, "abX"},
	}
	for _, tt := range tests {
		got, err := fnOverlay(tt.args)
		if err != nil {
			t.Fatalf("fnOverlay(%v): %v", tt.args, err)
		}
		if got != tt.want {
			t.Fatalf("fnOverlay(%v) = %v, want %q", tt.args, got, tt.want)
		}
	}
	if _, err := fnOverlay([]driver.Value{"abc", "X"}); err == nil {
		t.Fatal("fnOverlay with 2 arguments should fail")
	}
	// A start below 1 makes the first substring's length negative, which is the
	// error PostgreSQL raises rather than an answer it gives.
	if _, err := fnOverlay([]driver.Value{"abc", "X", int64(0)}); err == nil {
		t.Fatal("fnOverlay at position 0 should fail, as PostgreSQL does")
	}
}

// TestGoogleSQLPairAggregatesAtTheEdges covers the answers a covariance gives
// where there is not enough to compute one: the sample estimator needs two
// complete pairs and the population one needs one.
func TestGoogleSQLPairAggregatesAtTheEdges(t *testing.T) {
	db := castDB(t)

	got, err := runDialect(t, db, GoogleSQL, `SELECT COVAR_SAMP(x, x) FROM (SELECT 1 AS x) t`)
	if err != nil {
		t.Fatalf("COVAR_SAMP over one row: %v", err)
	}
	if got.Valid {
		t.Errorf("COVAR_SAMP over one row = %q, want NULL", got.String)
	}

	got, err = runDialect(t, db, GoogleSQL, `SELECT CORR(x, y) FROM (SELECT 1 AS x, 1 AS y UNION ALL SELECT 1, 2) t`)
	if err != nil {
		t.Fatalf("CORR with no spread: %v", err)
	}
	if got.Valid {
		t.Errorf("CORR with no spread in one column = %q, want NULL", got.String)
	}
}

// TestGoogleSQLArrayAggregatesAreRejected keeps the aggregates whose result is
// an array on the same footing as every other array-shaped construct: refused
// by name rather than reported as an unknown function.
func TestGoogleSQLArrayAggregatesAreRejected(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		"SELECT ARRAY_AGG(s) FROM t",
		"SELECT ARRAY_CONCAT_AGG(s) FROM t",
		"SELECT APPROX_QUANTILES(n, 2) FROM t",
		"SELECT APPROX_TOP_COUNT(s, 2) FROM t",
		"SELECT APPROX_TOP_SUM(s, n, 2) FROM t",
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()

			if _, err := Translate(GoogleSQL, query); !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(GoogleSQL, %q) error = %v, want ErrUnsupportedSyntax", query, err)
			}
		})
	}

	// A column of that name is not the aggregate: only a "(" after the word
	// makes it a call.
	if _, err := Translate(GoogleSQL, "SELECT array_agg FROM t"); err != nil {
		t.Errorf("a column named array_agg must translate: %v", err)
	}
}

// TestExpandedAggregatesRefuseAWindow keeps the aggregates that expand into an
// expression from reaching SQLite with an OVER after them. The result is
// several aggregates inside arithmetic, and a window belongs to none of them;
// left alone SQLite reported on whichever generated function it happened to
// reach -- "sqrt() may not be used as a window function" for a standard
// deviation -- naming a function the query does not contain.
func TestExpandedAggregatesRefuseAWindow(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect Dialect
		query   string
	}{
		{GoogleSQL, "SELECT CORR(x, y) OVER () FROM t"},
		{GoogleSQL, "SELECT COVAR_POP(x, y) OVER () FROM t"},
		{GoogleSQL, "SELECT COUNTIF(x > 1) OVER () FROM t"},
		{GoogleSQL, "SELECT APPROX_COUNT_DISTINCT(x) OVER () FROM t"},
		{GoogleSQL, "SELECT STDDEV(x) OVER () FROM t"},
		{PostgreSQL, "SELECT VAR_POP(x) OVER () FROM t"},
		{MySQL, "SELECT STD(x) OVER () FROM t"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			if _, err := Translate(tt.dialect, tt.query); !errors.Is(err, ErrUnsupportedSyntax) {
				t.Errorf("Translate(%s, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
			}
		})
	}

	// A rename carries a window fine, since what runs is one SQLite aggregate.
	for _, query := range []string{
		"SELECT LOGICAL_AND(x) OVER () FROM t",
		"SELECT ANY_VALUE(x) OVER () FROM t",
	} {
		if _, err := Translate(GoogleSQL, query); err != nil {
			t.Errorf("Translate(GoogleSQL, %q) must translate: %v", query, err)
		}
	}
}
