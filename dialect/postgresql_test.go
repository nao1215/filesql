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

		// The calls another dialect's helper of the same name would have taken:
		// to_hex hexes bytes for GoogleSQL, and regexp_replace replaces every
		// match there where PostgreSQL replaces the first alone.
		{"P-14_to_hex", "SELECT to_hex(n) FROM t", `SELECT postgresql_to_hex(n) AS "to_hex(n)" FROM t`},
		{"P-14_regexp_replace", "SELECT regexp_replace(s, 'a', 'b') FROM t", `SELECT postgresql_regexp_replace(s, 'a', 'b') AS "regexp_replace(s, 'a', 'b')" FROM t`},
		{"P-14_regexp_replace_with_flags", "SELECT regexp_replace(s, 'a', 'b', 'g') FROM t", `SELECT postgresql_regexp_replace(s, 'a', 'b', 'g') AS "regexp_replace(s, 'a', 'b', 'g')" FROM t`},

		{"P-4_position", "SELECT POSITION('b' IN name) FROM t", "SELECT INSTR(name, 'b') AS \"POSITION('b' IN name)\" FROM t"},
		{"P-4_position_expr", "SELECT POSITION(sub IN col) FROM t", "SELECT INSTR(col, sub) AS \"POSITION(sub IN col)\" FROM t"},

		{"P-5_substring_from_for", "SELECT SUBSTRING(name FROM 2 FOR 3) FROM t", "SELECT postgresql_substr(name, 2, 3) AS \"SUBSTRING(name FROM 2 FOR 3)\" FROM t"},
		{"P-5_substring_from", "SELECT SUBSTRING(name FROM 2) FROM t", "SELECT postgresql_substr(name, 2) AS \"SUBSTRING(name FROM 2)\" FROM t"},
		{"P-5_substring_for", "SELECT SUBSTRING(name FOR 3) FROM t", "SELECT postgresql_substr(name, 1, 3) AS \"SUBSTRING(name FOR 3)\" FROM t"},
		{"P-5_substring_comma_form", "SELECT SUBSTRING(name, 2, 3) FROM t", "SELECT postgresql_substr(name, 2, 3) AS \"SUBSTRING(name, 2, 3)\" FROM t"},
		// A string literal after FROM is a pattern, not a position: PostgreSQL
		// tells the two readings apart on the operand's type, and the token
		// stream is where that information still exists.
		{"P-5_substring_pattern", "SELECT SUBSTRING(name FROM '[0-9]+') FROM t", `SELECT regexp_extract(name, '[0-9]+') AS "SUBSTRING(name FROM '[0-9]+')" FROM t`},
		{"P-5_substring_numeric_operand_is_a_position", "SELECT SUBSTRING(name FROM 2) FROM t", "SELECT postgresql_substr(name, 2) AS \"SUBSTRING(name FROM 2)\" FROM t"},
		// A column is neither literal, so which reading applies is not in the
		// query text and the helper decides it from the value.
		{"P-5_substring_column_operand_decides_at_run_time", "SELECT SUBSTRING(name FROM n) FROM t", "SELECT postgresql_substring_from(name, n) AS \"SUBSTRING(name FROM n)\" FROM t"},
		{"P-5_substring_expression_operand_decides_at_run_time", "SELECT SUBSTRING(name FROM n + 1) FROM t", "SELECT postgresql_substring_from(name, n + 1) AS \"SUBSTRING(name FROM n + 1)\" FROM t"},
		{"P-5_substring_pattern_with_a_length_is_a_position", "SELECT SUBSTRING(name FROM '2' FOR 3) FROM t", "SELECT postgresql_substr(name, '2', 3) AS \"SUBSTRING(name FROM '2' FOR 3)\" FROM t"},

		{"P-6_string_agg", "SELECT STRING_AGG(name, ', ') FROM t", "SELECT group_concat(name, ', ') AS \"STRING_AGG(name, ', ')\" FROM t"},
		// SQLite's DISTINCT aggregates take one argument, so the separator has to
		// go. Dropping it is only correct when it is the comma SQLite defaults to.
		{"P-6_string_agg_distinct_comma", "SELECT STRING_AGG(DISTINCT name, ',') FROM t", "SELECT group_concat(DISTINCT name) AS \"STRING_AGG(DISTINCT name, ',')\" FROM t"},
		{"P-6_string_agg_distinct_comma_spaced", "SELECT STRING_AGG( DISTINCT name , ',' ) FROM t", "SELECT group_concat(DISTINCT name) AS \"STRING_AGG( DISTINCT name , ',' )\" FROM t"},
		{"P-20_greatest", "SELECT GREATEST(a, b) FROM t", `SELECT postgresql_greatest(a, b) AS "GREATEST(a, b)" FROM t`},
		// P-23: the bounds are sorted with the shared helpers, which answer
		// NULL for the whole call, rather than with PostgreSQL's NULL-skipping
		// pair, which would drop a NULL bound and turn a NULL answer into false.
		{"P-23_between_symmetric", "SELECT * FROM t WHERE x BETWEEN SYMMETRIC a AND b", "SELECT * FROM t WHERE x BETWEEN least(a, b) AND greatest(a, b)"},
		{"P-23_not_between_symmetric", "SELECT * FROM t WHERE x NOT BETWEEN SYMMETRIC a AND b", "SELECT * FROM t WHERE x NOT BETWEEN least(a, b) AND greatest(a, b)"},
		{"P-23_between_asymmetric", "SELECT * FROM t WHERE x BETWEEN ASYMMETRIC a AND b", "SELECT * FROM t WHERE x BETWEEN a AND b"},
		{"P-23_symmetric_is_not_a_keyword_alone", "SELECT symmetric FROM t", "SELECT symmetric FROM t"},
		{"P-20_least", "SELECT LEAST(a, b) FROM t", `SELECT postgresql_least(a, b) AS "LEAST(a, b)" FROM t`},
		{"P-19_upper", "SELECT UPPER(name) FROM t", `SELECT unicode_upper(name) AS "UPPER(name)" FROM t`},
		{"P-19_lower", "SELECT LOWER(name) FROM t", `SELECT unicode_lower(name) AS "LOWER(name)" FROM t`},

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
		// P-12: an INTERVAL literal is only translatable as the right operand
		// of date arithmetic. Anywhere else it reached SQLite's parser, which
		// reported a syntax error naming a token from the caller's own query.
		{"P-12_bare_interval", "SELECT INTERVAL '3 days'"},
		{"P-12_interval_as_an_argument", "SELECT JUSTIFY_DAYS(INTERVAL '35 days')"},
		{"P-12_interval_on_the_left", "SELECT INTERVAL '1 day' + d FROM t"},
		// P-5: the SQL-standard regular-expression form, whose third operand is
		// an escape character rather than a length. Read positionally it would
		// answer something the query never asked for.
		{"P-5_substring_similar_escape", "SELECT SUBSTRING(s SIMILAR 'a#\"b#\"c' ESCAPE '#') FROM t"},
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
