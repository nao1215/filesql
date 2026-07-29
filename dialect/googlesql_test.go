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
		{"C-1_extract", "SELECT EXTRACT(YEAR FROM d) FROM t", "SELECT DATE_PART('year', d) FROM t"},

		// G-1 backtick compound identifier is lexical (tokenizer).
		{"G-1_backtick_path", "SELECT x FROM `proj.dataset.table`", `SELECT x FROM "proj.dataset.table"`},

		{"G-2_safe_cast", "SELECT SAFE_CAST(x AS INT64) FROM t", "SELECT googlesql_safe_cast(x, 'INT64') FROM t"},
		{"G-2_safe_cast_unknown_type", "SELECT SAFE_CAST(x AS GEOGRAPHY)", "SELECT CAST(x AS GEOGRAPHY)"},

		{"G-3_date_literal", "SELECT DATE '2026-01-01'", "SELECT '2026-01-01'"},
		{"G-3_timestamp_literal", "SELECT TIMESTAMP '2026-01-01 00:00:00'", "SELECT '2026-01-01 00:00:00'"},
		{"G-3_date_word_not_literal", "SELECT DATE FROM t", "SELECT DATE FROM t"},

		{"G-4_cast_int64", "SELECT CAST(x AS INT64)", "SELECT googlesql_cast(x, 'INT64')"},
		{"G-4_cast_float64", "SELECT CAST(x AS FLOAT64)", "SELECT googlesql_cast(x, 'FLOAT64')"},
		{"G-4_cast_string", "SELECT CAST(x AS STRING)", "SELECT googlesql_cast(x, 'STRING')"},
		{"G-4_cast_bytes", "SELECT CAST(x AS BYTES)", "SELECT googlesql_cast(x, 'BYTES')"},

		{"G-6_format", "SELECT FORMAT('%d', n) FROM t", "SELECT printf('%d', n) FROM t"},

		{"G-7_date_add", "SELECT DATE_ADD(d, INTERVAL 3 DAY)", "SELECT datetime(d, '+3 day')"},
		{"G-7_timestamp_sub", "SELECT TIMESTAMP_SUB(ts, INTERVAL 2 HOUR)", "SELECT datetime(ts, '-2 hour')"},

		{"G-8_date_diff", "SELECT DATE_DIFF(a, b, DAY) FROM t", "SELECT DATE_DIFF(a, b, 'day') FROM t"},
		{"G-8_timestamp_diff", "SELECT TIMESTAMP_DIFF(a, b, SECOND)", "SELECT TIMESTAMP_DIFF(a, b, 'second')"},

		// G-10 raw and byte strings are lexical.
		{"G-10_raw_string", `SELECT r'a\nb'`, `SELECT 'a\nb'`},
		{"G-10_byte_string", `SELECT b'AB'`, `SELECT x'4142'`},

		{"double_quoted_string", `SELECT "hello"`, `SELECT 'hello'`},
		{"nested_safe_cast_in_extract", "SELECT EXTRACT(YEAR FROM SAFE_CAST(s AS DATETIME))", "SELECT DATE_PART('year', googlesql_safe_cast(s, 'DATETIME'))"},
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
		{"G-9_except", "SELECT * EXCEPT(col) FROM t"},
		{"G-9_replace", "SELECT * REPLACE(a AS b) FROM t"},
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
