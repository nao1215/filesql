package lower_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect"
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
		dialect dialect.Dialect
		query   string
		want    string
	}{
		{"googlesql countif", dialect.GoogleSQL, `SELECT COUNTIF(x > 1) ` + pair, "1"},
		{"googlesql countif none", dialect.GoogleSQL, `SELECT COUNTIF(x > 99) ` + pair, "0"},
		{"googlesql countif over no rows", dialect.GoogleSQL, `SELECT COUNTIF(x > 1) FROM (SELECT 1 AS x WHERE 0) t`, "0"},
		{"googlesql logical_and", dialect.GoogleSQL, `SELECT LOGICAL_AND(x) ` + bools, "0"},
		{"googlesql logical_or", dialect.GoogleSQL, `SELECT LOGICAL_OR(x) ` + bools, "1"},
		{"postgresql bool_and", dialect.PostgreSQL, `SELECT BOOL_AND(x) ` + bools, "0"},
		{"postgresql bool_or", dialect.PostgreSQL, `SELECT BOOL_OR(x) ` + bools, "1"},
		{"mysql any_value", dialect.MySQL, `SELECT ANY_VALUE(x) ` + pair, "1"},
		{"postgresql any_value", dialect.PostgreSQL, `SELECT ANY_VALUE(x) ` + pair, "1"},
		{"googlesql any_value", dialect.GoogleSQL, `SELECT ANY_VALUE(x) ` + pair, "1"},

		// BigQuery's APPROX_COUNT_DISTINCT estimates what SQLite counts
		// exactly, which is a correct answer to the question it asks.
		{"googlesql approx_count_distinct", dialect.GoogleSQL, `SELECT APPROX_COUNT_DISTINCT(x) FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t`, "2"},

		// The correlation and the covariances, over the three pairs BigQuery
		// was asked for the same answers with. A row where either side is NULL
		// takes no part in any of them.
		{"googlesql corr", dialect.GoogleSQL, `SELECT CORR(x, y) ` + pairs, "0.9933992677987828"},
		{"googlesql covar_pop", dialect.GoogleSQL, `SELECT COVAR_POP(x, y) ` + pairs, "1.6666666666666667"},
		{"googlesql covar_samp", dialect.GoogleSQL, `SELECT COVAR_SAMP(x, y) ` + pairs, "2.5"},
		{"googlesql corr of a column with itself", dialect.GoogleSQL, `SELECT CORR(x, x) ` + pairs, "1"},
		{"googlesql covar_pop over one row", dialect.GoogleSQL, `SELECT COVAR_POP(x, x) FROM (SELECT 1 AS x) t`, "0"},
		{"googlesql covar_pop skips an incomplete pair", dialect.GoogleSQL, `SELECT COVAR_POP(x, y) FROM (SELECT 1 AS x, 2 AS y UNION ALL SELECT 2, NULL UNION ALL SELECT 3, 7) t`, "2.5"},

		// PostgreSQL and GoogleSQL default to the sample estimator; MySQL's
		// STDDEV and VARIANCE are the population ones.
		{"postgresql stddev", dialect.PostgreSQL, `SELECT STDDEV(x) ` + pair, "1.4142135623730951"},
		{"postgresql variance", dialect.PostgreSQL, `SELECT VARIANCE(x) ` + pair, "2"},
		{"postgresql stddev_pop", dialect.PostgreSQL, `SELECT STDDEV_POP(x) ` + pair, "1"},
		{"postgresql var_pop", dialect.PostgreSQL, `SELECT VAR_POP(x) ` + pair, "1"},
		{"googlesql stddev", dialect.GoogleSQL, `SELECT STDDEV(x) ` + pair, "1.4142135623730951"},
		{"mysql std is population", dialect.MySQL, `SELECT STD(x) ` + pair, "1"},
		{"mysql variance is population", dialect.MySQL, `SELECT VARIANCE(x) ` + pair, "1"},
		{"mysql stddev_samp", dialect.MySQL, `SELECT STDDEV_SAMP(x) ` + pair, "1.4142135623730951"},

		// MySQL keeps its separator next to an ORDER BY, where SQLite would
		// otherwise read it as another sort term.
		{"mysql group_concat separator", dialect.MySQL, `SELECT GROUP_CONCAT(x SEPARATOR '|') ` + pair, "1|3"},
		{"mysql group_concat ordered", dialect.MySQL, `SELECT GROUP_CONCAT(x ORDER BY x DESC SEPARATOR '|') ` + pair, "3|1"},
		{"mysql group_concat distinct", dialect.MySQL, `SELECT GROUP_CONCAT(DISTINCT x) ` + pair, "1,3"},

		// SIMILAR TO is SQL-standard pattern matching SQLite does not have.
		{"postgresql similar to", dialect.PostgreSQL, `SELECT 'abc' SIMILAR TO 'a%'`, "1"},
		{"postgresql similar to anchors", dialect.PostgreSQL, `SELECT 'abc' SIMILAR TO 'b'`, "0"},
		{"postgresql similar to alternation", dialect.PostgreSQL, `SELECT 'abc' SIMILAR TO '(abc|def)'`, "1"},
		{"postgresql similar to underscore", dialect.PostgreSQL, `SELECT 'abc' SIMILAR TO 'a_c'`, "1"},
		{"postgresql not similar to", dialect.PostgreSQL, `SELECT 'abc' NOT SIMILAR TO 'b'`, "1"},

		// TO_CHAR formats numbers, not only dates.
		{"postgresql to_char number", dialect.PostgreSQL, `SELECT TRIM(TO_CHAR(1234.5, '9999.99'))`, "1234.50"},
		{"postgresql to_char rounds", dialect.PostgreSQL, `SELECT TRIM(TO_CHAR(1.567, '9.99'))`, "1.57"},
		{"postgresql to_char groups", dialect.PostgreSQL, `SELECT TRIM(TO_CHAR(1234567, '9,999,999'))`, "1,234,567"},
		{"postgresql to_char negative", dialect.PostgreSQL, `SELECT TRIM(TO_CHAR(-42, '999'))`, "-42"},
		{"postgresql to_char keeps dates", dialect.PostgreSQL, `SELECT TO_CHAR('2026-01-15', 'YYYY/MM/DD')`, "2026/01/15"},
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
// TestAggregateArityRefusalNamesTheAggregate holds every refusal here to the
// standard the rest of the table sets: a message says which aggregate it is
// about. A query can name several in one select list, and one sentence that
// fits all of them leaves the caller to work out which.
func TestAggregateArityRefusalNamesTheAggregate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect dialect.Dialect
		query   string
		names   string
	}{
		{"postgresql corr", dialect.PostgreSQL, "SELECT CORR(a) FROM t", "CORR"},
		{"postgresql covar_pop", dialect.PostgreSQL, "SELECT COVAR_POP(a) FROM t", "COVAR_POP"},
		{"postgresql covar_samp", dialect.PostgreSQL, "SELECT COVAR_SAMP(a) FROM t", "COVAR_SAMP"},
		{"googlesql corr", dialect.GoogleSQL, "SELECT CORR(a) FROM t", "CORR"},
		{"googlesql covar_pop", dialect.GoogleSQL, "SELECT COVAR_POP(a) FROM t", "COVAR_POP"},
		{"googlesql covar_samp", dialect.GoogleSQL, "SELECT COVAR_SAMP(a) FROM t", "COVAR_SAMP"},
		{"googlesql countif", dialect.GoogleSQL, "SELECT COUNTIF(a, a) FROM t", "COUNTIF"},
		{"mysql stddev", dialect.MySQL, "SELECT STDDEV(a, a) FROM t", "STDDEV"},
		// The one that says which of two aggregates is wrong.
		{"the wrong one of two", dialect.PostgreSQL, "SELECT CORR(a, c), COVAR_SAMP(a) FROM t", "COVAR_SAMP"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := dialect.Translate(tt.dialect, tt.query)
			if err == nil {
				t.Fatalf("Translate(%v, %q) = nil error, want a refusal", tt.dialect, tt.query)
			}
			if !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Errorf("error = %v, want ErrUnsupportedSyntax", err)
			}
			if !strings.Contains(err.Error(), tt.names) {
				t.Errorf("error = %v, which does not name %s", err, tt.names)
			}
		})
	}

	// The calls that must keep translating.
	for _, tt := range []struct {
		dialect dialect.Dialect
		query   string
	}{
		{dialect.PostgreSQL, "SELECT CORR(a, c) FROM t"},
		{dialect.PostgreSQL, "SELECT COVAR_POP(a, c) FROM t"},
		{dialect.GoogleSQL, "SELECT COUNTIF(a) FROM t"},
		{dialect.MySQL, "SELECT STDDEV(a) FROM t"},
	} {
		t.Run("accepted "+tt.query, func(t *testing.T) {
			t.Parallel()

			if _, err := dialect.Translate(tt.dialect, tt.query); err != nil {
				t.Errorf("Translate(%v, %q): %v", tt.dialect, tt.query, err)
			}
		})
	}
}

func TestSampleEstimatorOverOneRow(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	got, err := runDialect(t, db, dialect.PostgreSQL, `SELECT STDDEV(x) FROM (SELECT 1 AS x) t`)
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

	_, err := dialect.Translate(dialect.MySQL, `SELECT GROUP_CONCAT(DISTINCT x SEPARATOR '|') FROM t`)
	if err == nil {
		t.Fatal("GROUP_CONCAT(DISTINCT ... SEPARATOR ...) should be rejected")
	}
	if !strings.Contains(err.Error(), "DISTINCT with a separator") {
		t.Fatalf("error = %v, want it to name the combination", err)
	}
}

// TestNestedAggregateRewrite covers an aggregate inside another one, which the
// pass handles by rewriting the argument first.
func TestNestedAggregateRewrite(t *testing.T) {
	t.Parallel()

	got, err := dialect.Translate(dialect.GoogleSQL, `SELECT COUNTIF(LOGICAL_AND(x)) FROM t`)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !strings.Contains(got, "MIN(x)") {
		t.Fatalf("Translate = %q, want the inner LOGICAL_AND rewritten", got)
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
		dialect dialect.Dialect
		query   string
		want    string
	}{
		// MySQL LENGTH counts bytes, SQLite's counts characters. The query
		// succeeds either way, so the difference is silent.
		{"mysql length is bytes", dialect.MySQL, `SELECT LENGTH('あい')`, "6"},
		{"mysql char_length is characters", dialect.MySQL, `SELECT CHAR_LENGTH('あい')`, "2"},
		{"mysql ord ascii", dialect.MySQL, `SELECT ORD('A')`, "65"},
		{"mysql ord multibyte", dialect.MySQL, `SELECT ORD('あ')`, "14909826"},
		{"mysql json_unquote", dialect.MySQL, `SELECT JSON_UNQUOTE('"x"')`, "x"},
		{"mysql json_unquote passthrough", dialect.MySQL, `SELECT JSON_UNQUOTE('x')`, "x"},
		{"mysql trim both", dialect.MySQL, `SELECT TRIM(BOTH 'x' FROM 'xxaxx')`, "a"},
		{"mysql trim leading", dialect.MySQL, `SELECT TRIM(LEADING 'x' FROM 'xxaxx')`, "axx"},
		{"mysql trim trailing", dialect.MySQL, `SELECT TRIM(TRAILING 'x' FROM 'xxaxx')`, "xxa"},
		{"mysql trim from only", dialect.MySQL, `SELECT TRIM(FROM '  a  ')`, "a"},
		{"mysql union distinct", dialect.MySQL, `SELECT 1 UNION DISTINCT SELECT 1`, "1"},

		{"postgresql btrim", dialect.PostgreSQL, `SELECT BTRIM('xxaxx', 'x')`, "a"},
		{"postgresql trim both", dialect.PostgreSQL, `SELECT TRIM(BOTH 'x' FROM 'xxaxx')`, "a"},
		{"postgresql overlay", dialect.PostgreSQL, `SELECT OVERLAY('abcdef' PLACING 'XY' FROM 2)`, "aXYdef"},
		{"postgresql overlay with for", dialect.PostgreSQL, `SELECT OVERLAY('abcdef' PLACING 'XY' FROM 2 FOR 4)`, "aXYf"},
		{"postgresql jsonb_array_length", dialect.PostgreSQL, `SELECT JSONB_ARRAY_LENGTH('[1,2,3]'::jsonb)`, "3"},
		{"postgresql char_length", dialect.PostgreSQL, `SELECT CHAR_LENGTH('あい')`, "2"},

		{"googlesql json_value", dialect.GoogleSQL, `SELECT JSON_VALUE('{"a":"x"}', '$.a')`, "x"},
		// JSON_QUERY keeps its result in JSON text, so a string value stays
		// quoted; JSON_VALUE returns the value itself.
		{"googlesql json_query keeps quotes", dialect.GoogleSQL, `SELECT JSON_QUERY('{"a":"x"}', '$.a')`, `"x"`},
		{"googlesql json_query object", dialect.GoogleSQL, `SELECT JSON_QUERY('{"a":{"b":1}}', '$.a')`, `{"b":1}`},
		{"googlesql byte_length", dialect.GoogleSQL, `SELECT BYTE_LENGTH('あい')`, "6"},
		{"googlesql char_length", dialect.GoogleSQL, `SELECT CHAR_LENGTH('あい')`, "2"},
		{"googlesql union distinct", dialect.GoogleSQL, `SELECT 1 UNION DISTINCT SELECT 1`, "1"},
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

	_, err := dialect.Translate(dialect.PostgreSQL, `SELECT name FROM t WHERE name LIKE ANY(ARRAY['a%'])`)
	if err == nil {
		t.Fatal("an array literal should be rejected")
	}
	if !strings.Contains(err.Error(), "arrays are not supported") {
		t.Fatalf("error = %v, want it to name array literals", err)
	}
}

// TestGoogleSQLPairAggregatesAtTheEdges covers the answers a covariance gives
// where there is not enough to compute one: the sample estimator needs two
// complete pairs and the population one needs one.
func TestGoogleSQLPairAggregatesAtTheEdges(t *testing.T) {
	db := castDB(t)

	got, err := runDialect(t, db, dialect.GoogleSQL, `SELECT COVAR_SAMP(x, x) FROM (SELECT 1 AS x) t`)
	if err != nil {
		t.Fatalf("COVAR_SAMP over one row: %v", err)
	}
	if got.Valid {
		t.Errorf("COVAR_SAMP over one row = %q, want NULL", got.String)
	}

	got, err = runDialect(t, db, dialect.GoogleSQL, `SELECT CORR(x, y) FROM (SELECT 1 AS x, 1 AS y UNION ALL SELECT 1, 2) t`)
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

			if _, err := dialect.Translate(dialect.GoogleSQL, query); !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Errorf("Translate(GoogleSQL, %q) error = %v, want ErrUnsupportedSyntax", query, err)
			}
		})
	}

	// A column of that name is not the aggregate: only a "(" after the word
	// makes it a call.
	if _, err := dialect.Translate(dialect.GoogleSQL, "SELECT array_agg FROM t"); err != nil {
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
		dialect dialect.Dialect
		query   string
	}{
		{dialect.GoogleSQL, "SELECT CORR(x, y) OVER () FROM t"},
		{dialect.GoogleSQL, "SELECT COVAR_POP(x, y) OVER () FROM t"},
		{dialect.GoogleSQL, "SELECT COUNTIF(x > 1) OVER () FROM t"},
		{dialect.GoogleSQL, "SELECT APPROX_COUNT_DISTINCT(x) OVER () FROM t"},
		{dialect.GoogleSQL, "SELECT STDDEV(x) OVER () FROM t"},
		{dialect.PostgreSQL, "SELECT VAR_POP(x) OVER () FROM t"},
		{dialect.MySQL, "SELECT STD(x) OVER () FROM t"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			if _, err := dialect.Translate(tt.dialect, tt.query); !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Errorf("Translate(%s, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
			}
		})
	}

	// A rename carries a window fine, since what runs is one SQLite aggregate.
	for _, query := range []string{
		"SELECT LOGICAL_AND(x) OVER () FROM t",
		"SELECT ANY_VALUE(x) OVER () FROM t",
	} {
		if _, err := dialect.Translate(dialect.GoogleSQL, query); err != nil {
			t.Errorf("Translate(GoogleSQL, %q) must translate: %v", query, err)
		}
	}
}

// TestPostgreSQLAggregatesAddedForTheEngine covers the aggregates that had no
// translation at all. The three numbers were read from PostgreSQL 17.10 over
// the same three rows.
func TestPostgreSQLAggregatesAddedForTheEngine(t *testing.T) {
	db := castDB(t)

	const rows = ` FROM (SELECT 1 AS a, 2 AS b UNION ALL SELECT 2, 4 UNION ALL SELECT 3, 7) t`

	for _, tt := range []struct{ query, want string }{
		{query: `SELECT corr(a, b)` + rows, want: "0.9933992677987828"},
		{query: `SELECT covar_pop(a, b)` + rows, want: "1.6666666666666667"},
		{query: `SELECT covar_samp(a, b)` + rows, want: "2.5"},
		// SQLite writes the same array without the spaces PostgreSQL puts
		// after its commas.
		{query: `SELECT json_agg(a)` + rows, want: "[1,2,3]"},
		{query: `SELECT jsonb_agg(a)` + rows, want: "[1,2,3]"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, dialect.PostgreSQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if !got.Valid || got.String != tt.want {
				t.Errorf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestAggregatesWithoutASQLiteFormAreRejected keeps the aggregates that SQLite
// cannot express refused by name. Reaching SQLite they were reported as unknown
// functions, which tells the caller a name they did write does not exist rather
// than that the aggregate has no SQLite form.
func TestAggregatesWithoutASQLiteFormAreRejected(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialect.Dialect
		query   string
	}{
		{dialect: dialect.MySQL, query: "SELECT JSON_OBJECTAGG(b, a) FROM t"},
		{dialect: dialect.MySQL, query: "SELECT BIT_AND(a) FROM t"},
		{dialect: dialect.MySQL, query: "SELECT BIT_OR(a) FROM t"},
		{dialect: dialect.MySQL, query: "SELECT BIT_XOR(a) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT json_object_agg(b, a) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT jsonb_object_agg(b, a) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT array_agg(a) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT bit_and(a) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT regr_slope(a, b) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT regr_r2(a, b) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY a) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT mode() WITHIN GROUP (ORDER BY a) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT bit_or(a) FROM t"},
		{dialect: dialect.PostgreSQL, query: "SELECT bit_xor(a) FROM t"},
		{dialect: dialect.GoogleSQL, query: "SELECT BIT_AND(a) FROM t"},
		{dialect: dialect.GoogleSQL, query: "SELECT BIT_OR(a) FROM t"},
		{dialect: dialect.GoogleSQL, query: "SELECT BIT_XOR(a) FROM t"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			if _, err := dialect.Translate(tt.dialect, tt.query); !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
			}
		})
	}

	// A column of one of those names is not the aggregate.
	if _, err := dialect.Translate(dialect.PostgreSQL, "SELECT mode FROM t"); err != nil {
		t.Errorf("a column named mode must translate: %v", err)
	}
}

// TestAnExpandedAggregateRefusesClausesItWouldDrop covers the aggregates that
// become an expression rather than a call. A FILTER or a DISTINCT written on
// one of those has nowhere to go: the result is several aggregates inside
// arithmetic, and a clause repeated on some of them and not others would answer
// over a different set of rows than the caller asked about.
func TestAnExpandedAggregateRefusesClausesItWouldDrop(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialect.Dialect
		query   string
		mention string
	}{
		{dialect.PostgreSQL, "SELECT VAR_POP(a) FILTER (WHERE b > 0) FROM t", "FILTER"},
		{dialect.PostgreSQL, "SELECT STDDEV(a) FILTER (WHERE b > 0) FROM t", "FILTER"},
		{dialect.PostgreSQL, "SELECT CORR(a, b) FILTER (WHERE b > 0) FROM t", "FILTER"},
		{dialect.GoogleSQL, "SELECT COUNTIF(a) FILTER (WHERE b > 0) FROM t", "FILTER"},
		{dialect.PostgreSQL, "SELECT VAR_POP(DISTINCT a) FROM t", "DISTINCT"},
		{dialect.MySQL, "SELECT STDDEV(DISTINCT a) FROM t", "DISTINCT"},
		{dialect.GoogleSQL, "SELECT COUNTIF(DISTINCT a) FROM t", "DISTINCT"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			_, err := dialect.Translate(tt.dialect, tt.query)
			if !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Fatalf("Translate(%s, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
			}
			if !strings.Contains(err.Error(), tt.mention) {
				t.Errorf("Translate(%s, %q) error = %q, want it to name %s", tt.dialect, tt.query, err, tt.mention)
			}
		})
	}

	// An aggregate that is only renamed keeps both clauses, because the call it
	// becomes is still one call.
	got, err := dialect.Translate(dialect.GoogleSQL, "SELECT LOGICAL_AND(a) FILTER (WHERE b > 0) FROM t")
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !strings.Contains(got, "FILTER (WHERE b > 0)") {
		t.Errorf("Translate = %q, want the filter kept on the renamed aggregate", got)
	}
}
