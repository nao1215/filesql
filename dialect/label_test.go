package dialect

import "testing"

// A rewrite changes the text of an expression, and SQLite names an unaliased
// result column after that text. Left alone, translation renames the caller's
// columns to the helper functions this package rewrites into: a PostgreSQL
// "amt::text" came back as "postgresql_cast(amt, 'text')", which reached the
// caller as a CSV header and a JSON key. The label belongs to the query as it
// was written, so a rewritten select item carries its original text as an alias.

func TestTranslatePreservesResultColumnLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
	}{
		{
			name:    "postgresql cast operator",
			dialect: PostgreSQL,
			query:   "SELECT amt::text FROM t",
			want:    `SELECT postgresql_cast(amt, 'text') AS "amt::text" FROM t`,
		},
		{
			name:    "postgresql cast call",
			dialect: PostgreSQL,
			query:   "SELECT CAST(amt AS text) FROM t",
			want:    `SELECT postgresql_cast(amt, 'text') AS "CAST(amt AS text)" FROM t`,
		},
		{
			name:    "postgresql ilike",
			dialect: PostgreSQL,
			query:   "SELECT a ILIKE 'x' FROM t",
			want:    `SELECT like_insensitive('x', a) AS "a ILIKE 'x'" FROM t`,
		},
		{
			name:    "mysql cast",
			dialect: MySQL,
			query:   "SELECT CAST(a AS SIGNED) FROM t",
			want:    `SELECT mysql_cast(a, 'SIGNED') AS "CAST(a AS SIGNED)" FROM t`,
		},
		{
			name:    "mysql division",
			dialect: MySQL,
			query:   "SELECT a / b FROM t",
			want:    `SELECT mysql_divide(a, b) AS "a / b" FROM t`,
		},
		{
			name:    "mysql concat",
			dialect: MySQL,
			query:   "SELECT CONCAT(a,b) FROM t",
			want:    `SELECT strict_concat(a,b) AS "CONCAT(a,b)" FROM t`,
		},
		{
			name:    "googlesql safe cast",
			dialect: GoogleSQL,
			query:   "SELECT SAFE_CAST(a AS INT64) FROM t",
			want:    `SELECT googlesql_safe_cast(a, 'INT64') AS "SAFE_CAST(a AS INT64)" FROM t`,
		},
		{
			name:    "an explicit alias is left alone",
			dialect: PostgreSQL,
			query:   "SELECT amt::text AS label FROM t",
			want:    `SELECT postgresql_cast(amt, 'text') AS label FROM t`,
		},
		{
			name:    "an implicit alias is left alone",
			dialect: PostgreSQL,
			query:   "SELECT amt::text label FROM t",
			want:    `SELECT postgresql_cast(amt, 'text') label FROM t`,
		},
		{
			name:    "an item the rewrite did not touch keeps its own text",
			dialect: PostgreSQL,
			query:   "SELECT a + 1 FROM t",
			want:    "SELECT a + 1 FROM t",
		},
		{
			name:    "a bare column reference is not aliased",
			dialect: PostgreSQL,
			query:   "SELECT a, t.b FROM t",
			want:    "SELECT a, t.b FROM t",
		},
		{
			name:    "a star is not aliased",
			dialect: MySQL,
			query:   "SELECT *, a / b FROM t",
			want:    `SELECT *, mysql_divide(a, b) AS "a / b" FROM t`,
		},
		{
			name:    "only the rewritten item is aliased",
			dialect: MySQL,
			query:   "SELECT a, a / b, b FROM t",
			want:    `SELECT a, mysql_divide(a, b) AS "a / b", b FROM t`,
		},
		{
			name:    "a rewrite inside a WHERE clause is not aliased",
			dialect: MySQL,
			query:   "SELECT a FROM t WHERE a / b > 1",
			want:    "SELECT a FROM t WHERE mysql_divide(a, b) > 1",
		},
		{
			name:    "a subquery select list is aliased too",
			dialect: MySQL,
			query:   "SELECT x FROM (SELECT a / b FROM t)",
			want:    `SELECT x FROM (SELECT mysql_divide(a, b) AS "a / b" FROM t)`,
		},
		{
			name:    "a quoted label is escaped",
			dialect: PostgreSQL,
			query:   `SELECT "od"::text FROM t`,
			want:    `SELECT postgresql_cast("od", 'text') AS """od""::text" FROM t`,
		},
		{
			name:    "distinct is not mistaken for a select item",
			dialect: MySQL,
			query:   "SELECT DISTINCT a / b FROM t",
			want:    `SELECT DISTINCT mysql_divide(a, b) AS "a / b" FROM t`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Translate(tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("Translate(%s, %q) error: %v", tt.dialect, tt.query, err)
			}
			if got != tt.want {
				t.Errorf("Translate(%s, %q)\n got: %s\nwant: %s", tt.dialect, tt.query, got, tt.want)
			}
		})
	}
}

// TestTranslatePreservesLabelsInStatementsWithoutSelectList checks the pass
// leaves alone the statements that have no select list to label, so an UPDATE or
// an INSERT is translated exactly as before.
func TestTranslatePreservesLabelsInStatementsWithoutSelectList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect Dialect
		query   string
		want    string
	}{
		{
			name:    "update",
			dialect: MySQL,
			query:   "UPDATE t SET x = a / b",
			want:    "UPDATE t SET x = mysql_divide(a, b)",
		},
		{
			name:    "delete",
			dialect: MySQL,
			query:   "DELETE FROM t WHERE a / b > 1",
			want:    "DELETE FROM t WHERE mysql_divide(a, b) > 1",
		},
		{
			name:    "insert with a select list",
			dialect: MySQL,
			query:   "INSERT INTO t SELECT a / b FROM u",
			want:    `INSERT INTO t SELECT mysql_divide(a, b) AS "a / b" FROM u`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Translate(tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("Translate(%s, %q) error: %v", tt.dialect, tt.query, err)
			}
			if got != tt.want {
				t.Errorf("Translate(%s, %q)\n got: %s\nwant: %s", tt.dialect, tt.query, got, tt.want)
			}
		})
	}
}
