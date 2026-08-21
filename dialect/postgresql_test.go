package dialect

import (
	"errors"
	"testing"
)

// TestPostgreSQLTranslate covers the PostgreSQL rewrite rules by rule ID.
func TestPostgreSQLTranslate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"C-1_extract", "SELECT EXTRACT(YEAR FROM d) FROM t", "SELECT DATE_PART('year', d) AS \"EXTRACT(YEAR FROM d)\" FROM t"},

		{"P-1_cast", "SELECT a::int FROM t", "SELECT postgresql_cast(a, 'int') AS \"a::int\" FROM t"},
		{"P-1_cast_text", "SELECT a::text", "SELECT postgresql_cast(a, 'text') AS \"a::text\""},
		{"P-1_cast_chain", "SELECT a::int::text", "SELECT postgresql_cast(postgresql_cast(a, 'int'), 'text') AS \"a::int::text\""},
		{"P-1_cast_varchar_param", "SELECT a::varchar(50)", "SELECT postgresql_cast(a, 'varchar(50)') AS \"a::varchar(50)\""},
		{"P-1_cast_unknown_type", "SELECT a::inet", "SELECT CAST(a AS inet) AS \"a::inet\""},
		{"P-1_cast_paren_operand", "SELECT (a + b)::int", "SELECT postgresql_cast((a + b), 'int') AS \"(a + b)::int\""},

		{"P-2_ilike", "SELECT * FROM t WHERE name ILIKE 'a%'", `SELECT * FROM t WHERE like_insensitive('a%', name)`},
		{"P-2_not_ilike", "SELECT * FROM t WHERE name NOT ILIKE 'a%'", `SELECT * FROM t WHERE NOT like_insensitive('a%', name)`},

		{"P-3_match", "SELECT * FROM t WHERE name ~ '^a'", "SELECT * FROM t WHERE name REGEXP '^a'"},
		{"P-3_not_match", "SELECT * FROM t WHERE name !~ '^a'", "SELECT * FROM t WHERE name NOT REGEXP '^a'"},
		{"P-3_match_ci", "SELECT * FROM t WHERE name ~* '^a'", "SELECT * FROM t WHERE name REGEXP '(?i)^a'"},
		{"P-3_not_match_ci", "SELECT * FROM t WHERE name !~* '^a'", "SELECT * FROM t WHERE name NOT REGEXP '(?i)^a'"},

		{"P-4_position", "SELECT POSITION('b' IN name) FROM t", "SELECT INSTR(name, 'b') AS \"POSITION('b' IN name)\" FROM t"},
		{"P-4_position_expr", "SELECT POSITION(sub IN col) FROM t", "SELECT INSTR(col, sub) AS \"POSITION(sub IN col)\" FROM t"},

		{"P-5_substring_from_for", "SELECT SUBSTRING(name FROM 2 FOR 3) FROM t", "SELECT SUBSTR(name, 2, 3) AS \"SUBSTRING(name FROM 2 FOR 3)\" FROM t"},
		{"P-5_substring_from", "SELECT SUBSTRING(name FROM 2) FROM t", "SELECT SUBSTR(name, 2) AS \"SUBSTRING(name FROM 2)\" FROM t"},
		{"P-5_substring_for", "SELECT SUBSTRING(name FOR 3) FROM t", "SELECT SUBSTR(name, 1, 3) AS \"SUBSTRING(name FOR 3)\" FROM t"},
		{"P-5_substring_comma_passthrough", "SELECT SUBSTRING(name, 2, 3) FROM t", "SELECT SUBSTRING(name, 2, 3) FROM t"},

		{"P-6_string_agg", "SELECT STRING_AGG(name, ', ') FROM t", "SELECT group_concat(name, ', ') AS \"STRING_AGG(name, ', ')\" FROM t"},
		// SQLite's DISTINCT aggregates take one argument, so the separator has to
		// go. Dropping it is only correct when it is the comma SQLite defaults to.
		{"P-6_string_agg_distinct_comma", "SELECT STRING_AGG(DISTINCT name, ',') FROM t", "SELECT group_concat(DISTINCT name) AS \"STRING_AGG(DISTINCT name, ',')\" FROM t"},
		{"P-6_string_agg_distinct_comma_spaced", "SELECT STRING_AGG( DISTINCT name , ',' ) FROM t", "SELECT group_concat(DISTINCT name) AS \"STRING_AGG( DISTINCT name , ',' )\" FROM t"},
		{"P-x_upper", "SELECT UPPER(name) FROM t", `SELECT unicode_upper(name) AS "UPPER(name)" FROM t`},
		{"P-x_lower", "SELECT LOWER(name) FROM t", `SELECT unicode_lower(name) AS "LOWER(name)" FROM t`},

		{"P-6_string_agg_distinct_expression", "SELECT STRING_AGG(DISTINCT UPPER(name), ',') FROM t", "SELECT group_concat(DISTINCT unicode_upper(name)) AS \"STRING_AGG(DISTINCT UPPER(name), ',')\" FROM t"},
		// An ORDER BY belongs to the aggregate, not to the separator, and SQLite
		// takes it inside group_concat.
		{"P-6_string_agg_distinct_order_by", "SELECT STRING_AGG(DISTINCT name, ',' ORDER BY name) FROM t", "SELECT group_concat(DISTINCT name ORDER BY name) AS \"STRING_AGG(DISTINCT name, ',' ORDER BY name)\" FROM t"},
		{"P-6_string_agg_distinct_order_by_desc", "SELECT STRING_AGG(DISTINCT name, ',' ORDER BY name DESC) FROM t", "SELECT group_concat(DISTINCT name ORDER BY name DESC) AS \"STRING_AGG(DISTINCT name, ',' ORDER BY name DESC)\" FROM t"},

		{"P-8_cast_int4", "SELECT CAST(x AS int4) FROM t", "SELECT postgresql_cast(x, 'int4') AS \"CAST(x AS int4)\" FROM t"},
		{"P-8_cast_boolean", "SELECT CAST(x AS boolean)", "SELECT postgresql_cast(x, 'boolean') AS \"CAST(x AS boolean)\""},
		{"P-8_cast_bytea", "SELECT CAST(x AS bytea)", "SELECT postgresql_cast(x, 'bytea') AS \"CAST(x AS bytea)\""},

		{"P-9_true_false", "SELECT * FROM t WHERE active = TRUE OR done = FALSE", "SELECT * FROM t WHERE active = TRUE OR done = FALSE"},

		// Lexical rules P-7 handled by the tokenizer.
		{"P-7_escape_string", `SELECT E'a\tb'`, "SELECT 'a\tb'"},
		{"P-7_dollar_quote", "SELECT $$hi$$", "SELECT 'hi'"},

		{"double_quoted_identifier_preserved", `SELECT "col" FROM "My Table"`, `SELECT "col" FROM "My Table"`},
		{"nested_cast_in_position", "SELECT POSITION(x::text IN y) FROM t", "SELECT INSTR(y, postgresql_cast(x, 'text')) AS \"POSITION(x::text IN y)\" FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Translate(PostgreSQL, tt.input)
			if err != nil {
				t.Fatalf("Translate(PostgreSQL, %q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("Translate(PostgreSQL, %q)\n  = %q\nwant %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestPostgreSQLSetReturningNameIsNotACall checks that a name which merely looks
// like a set-returning function is not one: only a call is refused, so a column
// or a table of that name still translates.
func TestPostgreSQLSetReturningNameIsNotACall(t *testing.T) {
	t.Parallel()
	tests := []string{
		"SELECT generate_series FROM t",
		"SELECT a FROM generate_series",
		"SELECT t.unnest FROM t",
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got, err := Translate(PostgreSQL, input)
			if err != nil {
				t.Fatalf("Translate(PostgreSQL, %q) unexpected error: %v", input, err)
			}
			if got != input {
				t.Fatalf("Translate(PostgreSQL, %q) = %q, want it unchanged", input, got)
			}
		})
	}
}

func TestPostgreSQLTranslateUnsupported(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"P-10_distinct_on", "SELECT DISTINCT ON (a) a, b FROM t"},
		// P-18: passed through, this reported "no such table: generate_series",
		// which reads as a missing input file rather than as a construct the
		// translation cannot express.
		{"P-18_generate_series", "SELECT n FROM generate_series(1, 3) AS n"},
		{"P-18_generate_series_in_select", "SELECT generate_series(1, 3)"},
		{"P-18_unnest", "SELECT * FROM unnest(ARRAY[1, 2])"},
		{"P-18_regexp_split_to_table", "SELECT regexp_split_to_table('a,b', ',')"},
		{"P-10_lateral", "SELECT * FROM t, LATERAL (SELECT 1) s"},
		{"P-3_ci_non_literal", "SELECT * FROM t WHERE a ~* b"},
		// A separator SQLite cannot keep alongside DISTINCT. Answering with a
		// comma-joined string would be a different answer, not a translation.
		{"P-6_string_agg_distinct_other_separator", "SELECT STRING_AGG(DISTINCT name, '-') FROM t"},
		{"P-6_string_agg_distinct_separator_expression", "SELECT STRING_AGG(DISTINCT name, sep) FROM t"},
		{"P-1_missing_type", "SELECT a::"},
		{"P-1_type_not_word", "SELECT a:: , b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Translate(PostgreSQL, tt.input)
			if !errors.Is(err, ErrUnsupportedSyntax) {
				t.Fatalf("Translate(PostgreSQL, %q) error = %v, want ErrUnsupportedSyntax", tt.input, err)
			}
		})
	}
}
