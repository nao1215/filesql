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
		// The left operand is the whole chain of equal-precedence operators, not
		// the primary beside the DIV: MySQL reads "a * b DIV c" as "(a * b) DIV c".
		{"M-7_div_of_a_product", "SELECT a * b DIV c", `SELECT CAST(mysql_divide((a * b), c) AS INTEGER) AS "a * b DIV c"`},
		{"M-7_div_of_a_quotient", "SELECT a / b DIV c", `SELECT CAST(mysql_divide((mysql_divide(a, b)), c) AS INTEGER) AS "a / b DIV c"`},
		{"M-7_div_of_a_remainder", "SELECT a % b DIV c", `SELECT CAST(mysql_divide((a % b), c) AS INTEGER) AS "a % b DIV c"`},
		{"M-7_div_stops_at_lower_precedence", "SELECT a + b DIV c", `SELECT a + CAST(mysql_divide(b, c) AS INTEGER) AS "a + b DIV c"`},

		// M-11: "/" takes its left operand the same way, so a remainder to its
		// left is divided rather than dividing.
		{"M-11_divide_of_a_remainder", "SELECT a % b / c", `SELECT mysql_divide((a % b), c) AS "a % b / c"`},
		{"M-11_divide_of_a_product", "SELECT a * b / c", `SELECT mysql_divide((a * b), c) AS "a * b / c"`},
		{"M-11_divide_stops_at_lower_precedence", "SELECT a + b / c", `SELECT a + mysql_divide(b, c) AS "a + b / c"`},
		{"M-21_xor_operand_is_one_primary", "SELECT a * b ^ c", `SELECT a * mysql_bit_xor(b, c) AS "a * b ^ c"`},

		// M-24: MOD is MySQL's spelling of the remainder operator, and SQLite's
		// "%" is the same operation at the same precedence. The function
		// spelling already works and is left alone.
		{"M-24_mod", "SELECT a MOD b FROM t", `SELECT a % b AS "a MOD b" FROM t`},
		{"M-24_mod_literals", "SELECT 7 MOD 2", `SELECT 7 % 2 AS "7 MOD 2"`},
		{"M-24_mod_in_where", "SELECT * FROM t WHERE a MOD b = 1", "SELECT * FROM t WHERE a % b = 1"},
		{"M-24_mod_call_untouched", "SELECT MOD(7, 2)", "SELECT MOD(7, 2)"},

		// UPPER and LOWER go to helpers that fold the whole of Unicode; SQLite's
		// own fold only ASCII.
		{"M-25_upper", "SELECT UPPER(name) FROM t", `SELECT unicode_upper(name) AS "UPPER(name)" FROM t`},
		{"M-25_lower", "SELECT LOWER(name) FROM t", `SELECT unicode_lower(name) AS "LOWER(name)" FROM t`},
		{"M-25_upper_nested", "SELECT UPPER(TRIM(name))", `SELECT unicode_upper(TRIM(name)) AS "UPPER(TRIM(name))"`},
		{"M-24_mod_quoted_name_untouched", "SELECT `mod` FROM t", `SELECT "mod" FROM t`},
		{"M-24_mod_alias_untouched", "SELECT a AS `mod` FROM t", `SELECT a AS "mod" FROM t`},
		{"M-24_mod_parenthesized_right", "SELECT a MOD (b + 1) FROM t", `SELECT a % (b + 1) AS "a MOD (b + 1)" FROM t`},
		{"M-24_mod_parenthesized_left", "SELECT (a + 1) MOD b FROM t", `SELECT (a + 1) % b AS "(a + 1) MOD b" FROM t`},
		{"M-24_mod_call_arguments_untouched", "SELECT MOD(a MOD b, 2) FROM t", `SELECT MOD(a % b, 2) AS "MOD(a MOD b, 2)" FROM t`},
		{"M-24_mod_without_a_right_operand_is_left_alone", "SELECT a MOD", "SELECT a MOD"},

		{"M-8_cast_signed", "SELECT CAST(x AS SIGNED) FROM t", "SELECT mysql_cast(x, 'SIGNED') AS \"CAST(x AS SIGNED)\" FROM t"},
		{"M-8_cast_unsigned_integer", "SELECT CAST(x AS UNSIGNED INTEGER)", "SELECT mysql_cast(x, 'UNSIGNED') AS \"CAST(x AS UNSIGNED INTEGER)\""},
		{"M-8_cast_char", "SELECT CAST(x AS CHAR)", "SELECT mysql_cast(x, 'CHAR') AS \"CAST(x AS CHAR)\""},
		{"M-8_cast_decimal", "SELECT CAST(x AS DECIMAL(10,2))", "SELECT mysql_cast(x, 'DECIMAL(10,2)') AS \"CAST(x AS DECIMAL(10,2))\""},
		{"M-8_cast_datetime", "SELECT CAST(x AS DATETIME)", "SELECT mysql_cast(x, 'DATETIME') AS \"CAST(x AS DATETIME)\""},
		{"M-8_cast_binary", "SELECT CAST(x AS BINARY)", "SELECT mysql_cast(x, 'BINARY') AS \"CAST(x AS BINARY)\""},
		{"M-8_cast_unknown_passthrough", "SELECT CAST(x AS GEOMETRY)", "SELECT CAST(x AS GEOMETRY)"},

		{"M-9_rlike", "SELECT * FROM t WHERE name RLIKE '^a'", "SELECT * FROM t WHERE name REGEXP '^a'"},
		{"M-9_not_rlike", "SELECT * FROM t WHERE name NOT RLIKE '^a'", "SELECT * FROM t WHERE name NOT REGEXP '^a'"},
		// M-22: LIKE routes through the helper that folds case the way MySQL's
		// default collation does and reads a trailing escape as itself. SQLite's
		// own LIKE ... ESCAPE did neither.
		{"M-22_like", "SELECT * FROM t WHERE name LIKE 'a%'", "SELECT * FROM t WHERE like_insensitive('a%', name)"},
		{"M-22_like_with_escape_clause_is_left_alone", "SELECT * FROM t WHERE name LIKE 'a!%' ESCAPE '!'", "SELECT * FROM t WHERE name LIKE 'a!%' ESCAPE '!'"},

		{"M-10_limit_offset", "SELECT * FROM t LIMIT 5, 10", "SELECT * FROM t LIMIT 5, 10"},

		{"nested_date_add_in_extract", "SELECT EXTRACT(DAY FROM DATE_ADD(d, INTERVAL 1 DAY))", "SELECT DATE_PART('day', interval_add(d, 1, 'day')) AS \"EXTRACT(DAY FROM DATE_ADD(d, INTERVAL 1 DAY))\""},
		// M-21: the logical and bitwise operators MySQL spells with punctuation.
		// "||" was translated and its siblings were not, so they reached SQLite's
		// tokenizer as unrecognized tokens.
		{"M-21_andand", "SELECT a && b FROM t", `SELECT a AND b AS "a && b" FROM t`},
		{"M-21_andand_no_spaces", "SELECT a&&b FROM t", `SELECT a AND b AS "a&&b" FROM t`},
		{"M-21_bang", "SELECT !a FROM t", `SELECT (NOT a) AS "!a" FROM t`},
		{"M-21_bang_paren", "SELECT !(a AND b) FROM t", `SELECT (NOT (a AND b)) AS "!(a AND b)" FROM t`},
		{"M-21_bang_call", "SELECT !f(a) FROM t", `SELECT (NOT f(a)) AS "!f(a)" FROM t`},
		// "!" binds tighter than a comparison in MySQL, so the negation is of the
		// operand alone. Written as a bare NOT it would have swallowed the
		// comparison, because SQLite's NOT binds looser than "=".
		{"M-21_bang_before_comparison", "SELECT !a = b FROM t", `SELECT (NOT a) = b AS "!a = b" FROM t`},
		{"M-21_not_equal_untouched", "SELECT a != b FROM t", `SELECT a != b FROM t`},
		{"M-21_bitxor", "SELECT a ^ b FROM t", `SELECT mysql_bit_xor(a, b) AS "a ^ b" FROM t`},
		{"M-21_bitxor_literals", "SELECT 5 ^ 3", `SELECT mysql_bit_xor(5, 3) AS "5 ^ 3"`},
		{"M-21_bitand_untouched", "SELECT a & b FROM t", "SELECT a & b FROM t"},
		{"M-21_bitor_untouched", "SELECT a | b FROM t", "SELECT a | b FROM t"},
		{"M-21_shifts_untouched", "SELECT a << 1, b >> 2 FROM t", "SELECT a << 1, b >> 2 FROM t"},
		// The positions a rewrite has to survive: a WHERE clause, a CASE, a
		// window's PARTITION BY, and a GROUP BY, where an operand that stopped at
		// the wrong token has shown up before.
		{"M-21_andand_in_where", "SELECT a FROM t WHERE b && c", "SELECT a FROM t WHERE b AND c"},
		{"M-21_bitxor_in_where", "SELECT a FROM t WHERE b ^ c = 0", "SELECT a FROM t WHERE mysql_bit_xor(b, c) = 0"},
		{"M-21_bitxor_in_case", "SELECT CASE WHEN a ^ b THEN 1 ELSE 0 END FROM t", `SELECT CASE WHEN mysql_bit_xor(a, b) THEN 1 ELSE 0 END AS "CASE WHEN a ^ b THEN 1 ELSE 0 END" FROM t`},
		{"M-21_bitxor_in_group_by", "SELECT a FROM t GROUP BY a ^ b", "SELECT a FROM t GROUP BY mysql_bit_xor(a, b)"},
		{"M-21_bitxor_in_window", "SELECT SUM(a) OVER (PARTITION BY b ^ c) FROM t", `SELECT SUM(a) OVER (PARTITION BY mysql_bit_xor(b, c)) AS "SUM(a) OVER (PARTITION BY b ^ c)" FROM t`},
		{"M-21_bang_in_where", "SELECT a FROM t WHERE !b", "SELECT a FROM t WHERE (NOT b)"},
		// "!" binds tighter than every operator rewritten below it, so it has to be
		// resolved first: taken the other way round, the negation would end up
		// outside the call and apply to the whole thing.
		{"M-21_bang_left_of_bitxor", "SELECT !a ^ b FROM t", `SELECT mysql_bit_xor((NOT a), b) AS "!a ^ b" FROM t`},
		{"M-21_bang_right_of_bitxor", "SELECT a ^ !b FROM t", `SELECT mysql_bit_xor(a, (NOT b)) AS "a ^ !b" FROM t`},
		{"M-21_bang_left_of_divide", "SELECT !a / b FROM t", `SELECT mysql_divide((NOT a), b) AS "!a / b" FROM t`},
		// A run of them negates as many times, and one inside a parenthesized
		// operand is rewritten too rather than left for SQLite.
		{"M-21_bang_double", "SELECT !!a FROM t", `SELECT (NOT (NOT a)) AS "!!a" FROM t`},
		{"M-21_bang_nested_paren", "SELECT !(!a) FROM t", `SELECT (NOT ((NOT a))) AS "!(!a)" FROM t`},
		{"M-21_bang_inside_call", "SELECT f(!a) FROM t", `SELECT f((NOT a)) AS "f(!a)" FROM t`},

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
		// XOR sits between OR and AND in MySQL's precedence, so its operands are
		// whole AND-expressions rather than the primaries a rewrite can pick out.
		// It is refused by name instead of being translated wrongly.
		{"M-21_xor", "SELECT a XOR b FROM t"},
		{"M-21_xor_lowercase", "SELECT a xor b FROM t"},
		{"M-21_xor_in_where", "SELECT a FROM t WHERE b XOR c"},
		{"M-21_bitxor_left_not_primary", "SELECT a, ^ b"},
		{"M-21_bitxor_right_missing", "SELECT a ^"},
		// M-23: 0x41 is the string "A" where MySQL prints a value and the number
		// 65 where it does arithmetic. SQLite has only the number, and which
		// reading applies is not something a token rewrite can see, so the
		// literal is refused rather than translated into one of the two.
		{"M-23_hex_literal", "SELECT 0x41"},
		{"M-23_hex_literal_uppercase_prefix", "SELECT 0X41"},
		{"M-23_hex_literal_in_arithmetic", "SELECT 1 + 0x10"},
		{"M-23_hex_literal_in_comparison", "SELECT * FROM t WHERE s = 0x616263"},
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
