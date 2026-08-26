package dialect

import (
	"errors"
	"testing"
)

// TestGoogleSQLTranslate covers the GoogleSQL rewrite rules by rule ID.
func TestGoogleSQLTranslate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"C-1_extract", "SELECT EXTRACT(YEAR FROM d) FROM t", "SELECT googlesql_date_part('year', d) AS \"EXTRACT(YEAR FROM d)\" FROM t"},

		// G-1 backtick compound identifier is lexical (tokenizer).
		{"G-1_backtick_path", "SELECT x FROM `proj.dataset.table`", `SELECT x FROM "proj.dataset.table"`},

		{"G-2_safe_cast", "SELECT SAFE_CAST(x AS INT64) FROM t", "SELECT googlesql_safe_cast(x, 'INT64') AS \"SAFE_CAST(x AS INT64)\" FROM t"},
		{"G-2_safe_cast_unknown_type", "SELECT SAFE_CAST(x AS GEOGRAPHY)", "SELECT CAST(x AS GEOGRAPHY) AS \"SAFE_CAST(x AS GEOGRAPHY)\""},

		// G-20: BigQuery writes the safe functions with a "SAFE." call prefix, and
		// its own documentation uses that spelling. Only a few of them have an
		// underscore name, so the prefix is the general form.
		{"G-20_safe_divide", "SELECT SAFE.DIVIDE(1, 0)", `SELECT safe_divide(1, 0) AS "SAFE.DIVIDE(1, 0)"`},
		{"G-20_safe_divide_lowercase", "SELECT safe.divide(a, b) FROM t", `SELECT safe_divide(a, b) AS "safe.divide(a, b)" FROM t`},
		{"G-20_safe_add", "SELECT SAFE.ADD(a, b) FROM t", `SELECT safe_add(a, b) AS "SAFE.ADD(a, b)" FROM t`},
		{"G-20_safe_negate", "SELECT SAFE.NEGATE(a) FROM t", `SELECT safe_negate(a) AS "SAFE.NEGATE(a)" FROM t`},
		{"G-20_safe_multiply_nested", "SELECT SAFE.MULTIPLY(SAFE.ADD(a, b), c) FROM t", `SELECT safe_multiply(safe_add(a, b), c) AS "SAFE.MULTIPLY(SAFE.ADD(a, b), c)" FROM t`},
		{"G-20_safe_divide_in_where", "SELECT a FROM t WHERE SAFE.DIVIDE(a, b) > 1", "SELECT a FROM t WHERE safe_divide(a, b) > 1"},
		// A column named safe, and a qualified column on a table named safe, are
		// not calls and keep their meaning.
		{"G-20_safe_column_untouched", "SELECT safe FROM t", "SELECT safe FROM t"},
		{"G-20_safe_qualified_column_untouched", "SELECT safe.divide FROM t", "SELECT safe.divide FROM t"},

		{"G-3_date_literal", "SELECT DATE '2026-01-01'", "SELECT '2026-01-01' AS \"DATE '2026-01-01'\""},
		{"G-3_timestamp_literal", "SELECT TIMESTAMP '2026-01-01 00:00:00'", "SELECT '2026-01-01 00:00:00' AS \"TIMESTAMP '2026-01-01 00:00:00'\""},
		{"G-3_date_word_not_literal", "SELECT DATE FROM t", "SELECT DATE FROM t"},

		{"G-4_cast_int64", "SELECT CAST(x AS INT64)", "SELECT googlesql_cast(x, 'INT64') AS \"CAST(x AS INT64)\""},
		{"G-4_cast_float64", "SELECT CAST(x AS FLOAT64)", "SELECT googlesql_cast(x, 'FLOAT64') AS \"CAST(x AS FLOAT64)\""},
		{"G-4_cast_string", "SELECT CAST(x AS STRING)", "SELECT googlesql_cast(x, 'STRING') AS \"CAST(x AS STRING)\""},
		{"G-4_cast_bytes", "SELECT CAST(x AS BYTES)", "SELECT googlesql_cast(x, 'BYTES') AS \"CAST(x AS BYTES)\""},

		{"G-6_format", "SELECT FORMAT('%d', n) FROM t", "SELECT googlesql_format('%d', n) AS \"FORMAT('%d', n)\" FROM t"},

		{"G-24_left", "SELECT LEFT(name, 3) FROM t", `SELECT googlesql_left(name, 3) AS "LEFT(name, 3)" FROM t`},
		{"G-24_right", "SELECT RIGHT(name, 3) FROM t", `SELECT googlesql_right(name, 3) AS "RIGHT(name, 3)" FROM t`},

		// G-25: the one-argument form is the natural logarithm, and the
		// two-argument one writes its base second where SQLite writes it first.
		{"G-25_log", "SELECT LOG(x) FROM t", `SELECT ln(x) AS "LOG(x)" FROM t`},
		{"G-25_log_with_base", "SELECT LOG(x, 2) FROM t", `SELECT log(2, x) AS "LOG(x, 2)" FROM t`},
		{"G-25_log_nested", "SELECT LOG(LOG(x), 2) FROM t", `SELECT log(2, ln(x)) AS "LOG(LOG(x), 2)" FROM t`},
		{"G-25_log10_untouched", "SELECT LOG10(x) FROM t", "SELECT LOG10(x) FROM t"},
		{"G-25_log_three_arguments_untouched", "SELECT LOG(a, b, c) FROM t", "SELECT LOG(a, b, c) FROM t"},
		{"G-25_log_without_arguments_untouched", "SELECT LOG() FROM t", "SELECT LOG() FROM t"},

		// SQLite's DISTINCT aggregates take one argument, so the separator has to
		// go. Dropping it is only correct when it is the comma SQLite defaults to.
		{"G-18_string_agg", "SELECT STRING_AGG(name, ', ') FROM t", "SELECT STRING_AGG(name, ', ') FROM t"},
		{"G-18_string_agg_distinct_comma", "SELECT STRING_AGG(DISTINCT name, ',') FROM t", "SELECT group_concat(DISTINCT name) AS \"STRING_AGG(DISTINCT name, ',')\" FROM t"},
		{"G-21_upper", "SELECT UPPER(name) FROM t", `SELECT unicode_upper(name) AS "UPPER(name)" FROM t`},
		{"G-21_lower", "SELECT LOWER(name) FROM t", `SELECT unicode_lower(name) AS "LOWER(name)" FROM t`},

		{"G-18_string_agg_distinct_expression", "SELECT STRING_AGG(DISTINCT UPPER(name), ',') FROM t", "SELECT group_concat(DISTINCT unicode_upper(name)) AS \"STRING_AGG(DISTINCT UPPER(name), ',')\" FROM t"},
		// An ORDER BY belongs to the aggregate, not to the separator, and SQLite
		// takes it inside group_concat.
		{"G-18_string_agg_distinct_order_by", "SELECT STRING_AGG(DISTINCT name, ',' ORDER BY name) FROM t", "SELECT group_concat(DISTINCT name ORDER BY name) AS \"STRING_AGG(DISTINCT name, ',' ORDER BY name)\" FROM t"},

		{"G-7_date_add", "SELECT DATE_ADD(d, INTERVAL 3 DAY)", "SELECT interval_add(d, 3, 'day') AS \"DATE_ADD(d, INTERVAL 3 DAY)\""},
		{"G-7_timestamp_sub", "SELECT TIMESTAMP_SUB(ts, INTERVAL 2 HOUR)", "SELECT interval_add(ts, -(2), 'hour') AS \"TIMESTAMP_SUB(ts, INTERVAL 2 HOUR)\""},

		{"G-8_date_diff", "SELECT DATE_DIFF(a, b, DAY) FROM t", "SELECT date_diff(a, b, 'day') AS \"DATE_DIFF(a, b, DAY)\" FROM t"},
		{"G-8_timestamp_diff", "SELECT TIMESTAMP_DIFF(a, b, SECOND)", "SELECT timestamp_diff(a, b, 'second') AS \"TIMESTAMP_DIFF(a, b, SECOND)\""},

		// G-10 raw and byte strings are lexical.
		{"G-10_raw_string", `SELECT r'a\nb'`, `SELECT 'a\nb'`},
		{"G-10_byte_string", `SELECT b'AB'`, `SELECT x'4142'`},

		{"double_quoted_string", `SELECT "hello"`, `SELECT 'hello'`},
		{"nested_safe_cast_in_extract", "SELECT EXTRACT(YEAR FROM SAFE_CAST(s AS DATETIME))", "SELECT googlesql_date_part('year', googlesql_safe_cast(s, 'DATETIME')) AS \"EXTRACT(YEAR FROM SAFE_CAST(s AS DATETIME))\""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Translate(GoogleSQL, tt.input)
			if err != nil {
				t.Fatalf("Translate(GoogleSQL, %q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("Translate(GoogleSQL, %q)\n  = %q\nwant %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGoogleSQLTranslateUnsupported(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"G-9_qualify", "SELECT x FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY x) = 1"},
		{"G-9_unnest", "SELECT * FROM UNNEST([1, 2, 3])"},
		{"G-9_array_type", "SELECT ARRAY<INT64>[1, 2]"},
		{"G-9_struct_type", "SELECT STRUCT<a INT64>(1)"},
		// The untyped constructor is the ordinary spelling, and reached SQLite as
		// a call, which fails on the AS inside it.
		{"G-9_struct_constructor", "SELECT STRUCT(1 AS a)"},
		{"G-9_struct_constructor_two_fields", "SELECT STRUCT(1 AS a, 'x' AS b)"},
		{"G-9_struct_constructor_in_a_subquery", "SELECT s.a FROM (SELECT STRUCT(1 AS a) AS s)"},
		{"G-9_array_constructor", "SELECT ARRAY(SELECT 1)"},
		{"G-9_except", "SELECT * EXCEPT(col) FROM t"},
		{"G-9_replace", "SELECT * REPLACE(a AS b) FROM t"},
		// A separator SQLite cannot keep alongside DISTINCT. Answering with a
		// comma-joined string would be a different answer, not a translation.
		{"G-18_string_agg_distinct_other_separator", "SELECT STRING_AGG(DISTINCT name, '-') FROM t"},
		{"G-18_string_agg_distinct_separator_expression", "SELECT STRING_AGG(DISTINCT name, sep) FROM t"},

		// A SAFE. call whose function has no safe form here is refused by name.
		// Passed through, SQLite reads "SAFE.SUBSTR" as schema.table and reports on
		// the "(" instead.
		{"G-20_safe_unknown_function", "SELECT SAFE.SUBSTR(s, 1, 2) FROM t"},
		{"G-20_safe_cast_prefix", "SELECT SAFE.CAST(x AS INT64) FROM t"},

		// G-19: SQLite reads "[...]" as an identifier, so an array literal came
		// back as "no such column: 1,2,3", about a column the query never named.
		{"G-19_array_literal", "SELECT ARRAY_LENGTH([1,2,3]) AS r"},
		{"G-19_array_literal_bare", "SELECT [1, 2, 3]"},
		{"G-19_array_subscript", "SELECT x[OFFSET(0)] AS r FROM t"},
		{"G-19_array_subscript_ordinal", "SELECT x[ORDINAL(1)] FROM t"},
		{"G-19_array_literal_in_where", "SELECT a FROM t WHERE b IN UNNEST([1, 2])"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Translate(GoogleSQL, tt.input)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Fatalf("Translate(GoogleSQL, %q) error = %v, want ErrUnsupportedSyntax", tt.input, err)
			}
		})
	}
}
