package dialect

import (
	"errors"
	"testing"
)

// TestMySQLTranslate covers the MySQL rewrite rules by rule ID. Lexical rules
// (M-1..M-4) are exercised in TestTranslateLexical; this file covers the
// structural rules and their error cases.
func TestMySQLTranslate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"C-1_extract", "SELECT EXTRACT(YEAR FROM d) FROM t", "SELECT DATE_PART('year', d) AS \"EXTRACT(YEAR FROM d)\" FROM t"},
		{"C-1_extract_expr", "SELECT EXTRACT(MONTH FROM o.created) FROM t", "SELECT DATE_PART('month', o.created) AS \"EXTRACT(MONTH FROM o.created)\" FROM t"},

		{"M-5_date_add", "SELECT DATE_ADD(d, INTERVAL 3 DAY) FROM t", "SELECT interval_add(d, 3, 'day') AS \"DATE_ADD(d, INTERVAL 3 DAY)\" FROM t"},
		{"M-5_date_sub", "SELECT DATE_SUB(d, INTERVAL 2 MONTH) FROM t", "SELECT interval_add(d, -(2), 'month') AS \"DATE_SUB(d, INTERVAL 2 MONTH)\" FROM t"},
		{"M-5_date_add_hour", "SELECT DATE_ADD(ts, INTERVAL 5 HOUR)", "SELECT interval_add(ts, 5, 'hour') AS \"DATE_ADD(ts, INTERVAL 5 HOUR)\""},
		{"M-5_date_add_string_arg", "SELECT DATE_ADD('2020-01-01', INTERVAL 1 YEAR)", "SELECT interval_add('2020-01-01', 1, 'year') AS \"DATE_ADD('2020-01-01', INTERVAL 1 YEAR)\""},

		// MySQL CONCAT is NULL-propagating: any NULL argument makes the whole
		// result NULL. SQLite's own concat() treats NULL as an empty string, so the
		// call has to be routed to a helper rather than passed through.
		{"M-x_concat", "SELECT CONCAT(a, b) FROM t", "SELECT strict_concat(a, b) AS \"CONCAT(a, b)\" FROM t"},
		{"M-x_concat_one_arg", "SELECT CONCAT(a)", "SELECT strict_concat(a) AS \"CONCAT(a)\""},
		{"M-x_concat_nested", "SELECT CONCAT(a, CONCAT(b, c))", "SELECT strict_concat(a, strict_concat(b, c)) AS \"CONCAT(a, CONCAT(b, c))\""},
		{"M-x_concat_ws_untouched", "SELECT CONCAT_WS(',', a, b)", "SELECT CONCAT_WS(',', a, b)"},
		{"M-x_group_concat_untouched", "SELECT GROUP_CONCAT(a) FROM t", "SELECT GROUP_CONCAT(a) FROM t"},

		{"M-6_group_concat_separator", "SELECT GROUP_CONCAT(name SEPARATOR ', ') FROM t", "SELECT GROUP_CONCAT(name, ', ') AS \"GROUP_CONCAT(name SEPARATOR ', ')\" FROM t"},
		{"M-6_group_concat_plain", "SELECT GROUP_CONCAT(name) FROM t", "SELECT GROUP_CONCAT(name) FROM t"},
		{"M-6_group_concat_distinct", "SELECT GROUP_CONCAT(DISTINCT name) FROM t", "SELECT GROUP_CONCAT(DISTINCT name) FROM t"},

		{"M-7_div", "SELECT a DIV b FROM t", `SELECT CAST(mysql_divide(a, b) AS INTEGER) AS "a DIV b" FROM t`},
		{"M-7_div_literals", "SELECT 7 DIV 2", "SELECT CAST(mysql_divide(7, 2) AS INTEGER) AS \"7 DIV 2\""},
		{"M-7_div_qualified", "SELECT t.a DIV t.b FROM t", "SELECT CAST(mysql_divide(t.a, t.b) AS INTEGER) AS \"t.a DIV t.b\" FROM t"},
		{"M-7_div_paren_left", "SELECT (a + b) DIV c", `SELECT CAST(mysql_divide((a + b), c) AS INTEGER) AS "(a + b) DIV c"`},
		{"M-7_div_call_right", "SELECT x DIV ABS(y)", "SELECT CAST(mysql_divide(x, ABS(y)) AS INTEGER) AS \"x DIV ABS(y)\""},
		{"M-7_div_call_left", "SELECT ABS(x) DIV y", `SELECT CAST(mysql_divide(ABS(x), y) AS INTEGER) AS "ABS(x) DIV y"`},

		{"M-8_cast_signed", "SELECT CAST(x AS SIGNED) FROM t", "SELECT mysql_cast(x, 'SIGNED') AS \"CAST(x AS SIGNED)\" FROM t"},
		{"M-8_cast_unsigned_integer", "SELECT CAST(x AS UNSIGNED INTEGER)", "SELECT mysql_cast(x, 'UNSIGNED') AS \"CAST(x AS UNSIGNED INTEGER)\""},
		{"M-8_cast_char", "SELECT CAST(x AS CHAR)", "SELECT mysql_cast(x, 'CHAR') AS \"CAST(x AS CHAR)\""},
		{"M-8_cast_decimal", "SELECT CAST(x AS DECIMAL(10,2))", "SELECT mysql_cast(x, 'DECIMAL(10,2)') AS \"CAST(x AS DECIMAL(10,2))\""},
		{"M-8_cast_datetime", "SELECT CAST(x AS DATETIME)", "SELECT mysql_cast(x, 'DATETIME') AS \"CAST(x AS DATETIME)\""},
		{"M-8_cast_binary", "SELECT CAST(x AS BINARY)", "SELECT mysql_cast(x, 'BINARY') AS \"CAST(x AS BINARY)\""},
		{"M-8_cast_unknown_passthrough", "SELECT CAST(x AS GEOMETRY)", "SELECT CAST(x AS GEOMETRY)"},

		{"M-9_rlike", "SELECT * FROM t WHERE name RLIKE '^a'", "SELECT * FROM t WHERE name REGEXP '^a'"},
		{"M-9_not_rlike", "SELECT * FROM t WHERE name NOT RLIKE '^a'", "SELECT * FROM t WHERE name NOT REGEXP '^a'"},

		{"M-10_limit_offset", "SELECT * FROM t LIMIT 5, 10", "SELECT * FROM t LIMIT 5, 10"},

		{"nested_date_add_in_extract", "SELECT EXTRACT(DAY FROM DATE_ADD(d, INTERVAL 1 DAY))", "SELECT DATE_PART('day', interval_add(d, 1, 'day')) AS \"EXTRACT(DAY FROM DATE_ADD(d, INTERVAL 1 DAY))\""},
		{"unrelated_function_untouched", "SELECT COALESCE(a, b), SUM(c) FROM t", "SELECT COALESCE(a, b), SUM(c) FROM t"},
		{"extract_without_from_passthrough", "SELECT EXTRACT(x) FROM t", "SELECT EXTRACT(x) FROM t"},
		{"cast_without_as_passthrough", "SELECT CAST(x) FROM t", "SELECT CAST(x) FROM t"},
		{"date_add_without_interval_passthrough", "SELECT DATE_ADD(a, b) FROM t", "SELECT DATE_ADD(a, b) FROM t"},
		{"nested_cast_in_unrecognized_call", "SELECT COALESCE(CAST(x AS SIGNED), 0) FROM t", "SELECT COALESCE(mysql_cast(x, 'SIGNED'), 0) AS \"COALESCE(CAST(x AS SIGNED), 0)\" FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Translate(MySQL, tt.input)
			if err != nil {
				t.Fatalf("Translate(MySQL, %q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("Translate(MySQL, %q)\n  = %q\nwant %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestMySQLTranslateUnsupported(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"M-5_unsupported_unit", "SELECT DATE_ADD(d, INTERVAL 1 DAY_HOUR)"},
		{"M-5_missing_interval_value", "SELECT DATE_ADD(d, INTERVAL DAY)"},
		{"M-5_compound_interval", "SELECT DATE_ADD(d, INTERVAL '1:1' MINUTE_SECOND)"},
		{"M-5_missing_unit", "SELECT DATE_ADD(d, INTERVAL 3)"},
		{"M-7_div_left_not_primary", "SELECT a, DIV b"},
		{"M-7_div_right_missing", "SELECT a DIV"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Translate(MySQL, tt.input)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Fatalf("Translate(MySQL, %q) error = %v, want ErrUnsupportedSyntax", tt.input, err)
			}
		})
	}
}
