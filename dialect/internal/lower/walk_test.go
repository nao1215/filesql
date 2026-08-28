package lower_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect"
)

// TestStatementsTranslate runs every statement form the package supports
// through the whole pipeline. A statement that parses and does not render is
// not translated, and the two halves are only exercised together here.
func TestStatementsTranslate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect dialect.Dialect
		input   string
		want    string
	}{
		{
			name:    "insert with values",
			dialect: dialect.MySQL,
			input:   "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')",
			want:    "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')",
		},
		{
			name:    "insert from a query",
			dialect: dialect.PostgreSQL,
			input:   "INSERT INTO t (a) SELECT b FROM u",
			want:    "INSERT INTO t (a) SELECT b FROM u",
		},
		{
			name:    "insert with default values",
			dialect: dialect.PostgreSQL,
			input:   "INSERT INTO t DEFAULT VALUES",
			want:    "INSERT INTO t DEFAULT VALUES",
		},
		{
			// MySQL's IGNORE is SQLite's OR IGNORE, and its SET form is the
			// same insert written column by column.
			name:    "mysql insert ignore in the set form",
			dialect: dialect.MySQL,
			input:   "INSERT IGNORE INTO t SET a = 1, b = 2",
			want:    "INSERT OR IGNORE INTO t (a, b) VALUES (1, 2)",
		},
		{
			name:    "upsert",
			dialect: dialect.PostgreSQL,
			input:   "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2 RETURNING a",
			want:    "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2 RETURNING a",
		},
		{
			name:    "mysql upsert",
			dialect: dialect.MySQL,
			input:   "INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a = 2",
			want:    "INSERT INTO t (a) VALUES (1) ON CONFLICT DO UPDATE SET a = 2",
		},
		{
			name:    "update with order and limit",
			dialect: dialect.MySQL,
			input:   "UPDATE t SET a = a + 1 WHERE b = 2 ORDER BY a LIMIT 3",
			want:    "UPDATE t SET a = a + 1 WHERE b = 2 ORDER BY a LIMIT 3",
		},
		{
			name:    "delete",
			dialect: dialect.PostgreSQL,
			input:   "DELETE FROM t WHERE a = 1 RETURNING a",
			want:    "DELETE FROM t WHERE a = 1 RETURNING a",
		},
		{
			name:    "create table",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL DEFAULT 'x', CHECK (id > 0))",
			want:    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT ('x'), CHECK (id > 0))",
		},
		{
			// SERIAL is an integer with a sequence behind it, which SQLite
			// spells AUTOINCREMENT on the primary key.
			name:    "create table with a serial key",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)",
			want:    "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
		},
		{
			name:    "create table with a table constraint",
			dialect: dialect.MySQL,
			input:   "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a), UNIQUE (b))",
			want:    "CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a), UNIQUE (b))",
		},
		{
			name:    "create table as select",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t AS SELECT a FROM u",
			want:    "CREATE TABLE t AS SELECT a FROM u",
		},
		{
			name:    "create view",
			dialect: dialect.PostgreSQL,
			input:   "CREATE VIEW v (a) AS SELECT b FROM t",
			want:    "CREATE VIEW v (a) AS SELECT b FROM t",
		},
		{
			name:    "create index",
			dialect: dialect.PostgreSQL,
			input:   "CREATE UNIQUE INDEX IF NOT EXISTS i ON t (a DESC) WHERE a > 1",
			want:    "CREATE UNIQUE INDEX IF NOT EXISTS i ON t (a DESC) WHERE a > 1",
		},
		{
			name:    "drop",
			dialect: dialect.MySQL,
			input:   "DROP TABLE IF EXISTS t",
			want:    "DROP TABLE IF EXISTS t",
		},
		{
			name:    "alter table rename",
			dialect: dialect.PostgreSQL,
			input:   "ALTER TABLE t RENAME TO u",
			want:    "ALTER TABLE t RENAME TO u",
		},
		{
			name:    "alter table add column",
			dialect: dialect.MySQL,
			input:   "ALTER TABLE t ADD COLUMN a VARCHAR(10)",
			want:    "ALTER TABLE t ADD COLUMN a TEXT",
		},
		{
			name:    "alter table drop column",
			dialect: dialect.PostgreSQL,
			input:   "ALTER TABLE t DROP COLUMN a",
			want:    "ALTER TABLE t DROP COLUMN a",
		},
		{
			// START TRANSACTION is BEGIN under another name.
			name:    "transaction control",
			dialect: dialect.MySQL,
			input:   "START TRANSACTION",
			want:    "BEGIN",
		},
		{
			name:    "savepoint",
			dialect: dialect.PostgreSQL,
			input:   "SAVEPOINT s",
			want:    "SAVEPOINT s",
		},
		{
			name:    "explain",
			dialect: dialect.PostgreSQL,
			input:   "EXPLAIN SELECT 1",
			want:    "EXPLAIN SELECT 1",
		},
		{
			name:    "pragma",
			dialect: dialect.PostgreSQL,
			input:   "PRAGMA table_info(t)",
			want:    "PRAGMA table_info(t)",
		},
		{
			name:    "analyze",
			dialect: dialect.PostgreSQL,
			input:   "ANALYZE t",
			want:    "ANALYZE t",
		},
		{
			name:    "values as a query",
			dialect: dialect.PostgreSQL,
			input:   "VALUES (1, 2), (3, 4)",
			want:    "VALUES (1, 2), (3, 4)",
		},
		{
			name:    "a row constructor and a scalar subquery",
			dialect: dialect.PostgreSQL,
			input:   "SELECT (a, b) FROM t WHERE a = (SELECT max(x) FROM u)",
			want:    "SELECT (a, b) FROM t WHERE a = (SELECT max(x) FROM u)",
		},
		{
			name:    "not exists",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a FROM t WHERE NOT EXISTS (SELECT 1 FROM u)",
			want:    "SELECT a FROM t WHERE NOT EXISTS (SELECT 1 FROM u)",
		},
		{
			// FETCH FIRST is LIMIT written the standard way.
			name:    "fetch first",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a FROM t ORDER BY a OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY",
			want:    "SELECT a FROM t ORDER BY a NULLS LAST LIMIT 3 OFFSET 2",
		},
		{
			// LIMIT ALL asks for every row, which SQLite spells as a count of
			// -1 because an OFFSET needs a LIMIT in front of it.
			name:    "limit all with an offset",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a FROM t LIMIT ALL OFFSET 2",
			want:    "SELECT a FROM t LIMIT -1 OFFSET 2",
		},
		{
			name:    "a recursive common table expression",
			dialect: dialect.PostgreSQL,
			input:   "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
			want:    "WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
		},
		{
			name:    "every join",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a FROM t JOIN u ON t.id = u.id LEFT OUTER JOIN v USING (id) CROSS JOIN w",
			want:    "SELECT a FROM t JOIN u ON t.id = u.id LEFT JOIN v USING (id) CROSS JOIN w",
		},
		{
			name:    "a window frame with an exclusion",
			dialect: dialect.PostgreSQL,
			input:   "SELECT sum(a) OVER (ORDER BY b GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM t",
			want: "SELECT sum(a) OVER (ORDER BY b NULLS LAST GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES) " +
				`AS "sum(a) OVER (ORDER BY b GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES)" FROM t`,
		},
		{
			name:    "a filtered aggregate over a named window",
			dialect: dialect.PostgreSQL,
			input:   "SELECT count(*) FILTER (WHERE a > 1) OVER w FROM t WINDOW w AS (PARTITION BY b)",
			want:    "SELECT count(*) FILTER (WHERE a > 1) OVER w FROM t WINDOW w AS (PARTITION BY b)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := dialect.Translate(tt.dialect, tt.input)
			if err != nil {
				t.Fatalf("Translate(%s, %q): %v", tt.dialect, tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Translate(%s, %q)\n  = %q\nwant %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}

// TestStatementsWithNoSQLiteForm covers the statements that parse and cannot be
// carried out, each refused by name rather than left to fail in the engine.
func TestStatementsWithNoSQLiteForm(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialect.Dialect
		input   string
		mention string
	}{
		{dialect.PostgreSQL, "DELETE FROM t USING u WHERE t.id = u.id", "DELETE ... USING"},
		{dialect.PostgreSQL, "SELECT * FROM t, LATERAL (SELECT 1) s", "LATERAL"},
		{dialect.PostgreSQL, "SELECT DISTINCT ON (a) a FROM t", "DISTINCT ON"},
		{dialect.PostgreSQL, "SELECT a FROM t GROUP BY ROLLUP (a)", "grouping set"},
		{dialect.PostgreSQL, "SELECT generate_series(1, 3)", "set of rows"},
		{dialect.PostgreSQL, "UPDATE t, u SET a = 1", "more than one table"},
		{dialect.MySQL, "SELECT a FROM t FOR UPDATE", "row-locking"},
		{dialect.GoogleSQL, "SELECT a FROM t QUALIFY a > 1", "QUALIFY"},
		{dialect.GoogleSQL, "SELECT a FROM t GROUP BY ALL", "GROUP BY ALL"},
		{dialect.MySQL, "DROP TABLE t, u", "more than one object"},
		{dialect.PostgreSQL, "CREATE TABLE t (a INT[])", "array column"},
		{dialect.PostgreSQL, "CREATE TABLE t (a GEOMETRY)", "column type"},
		{dialect.PostgreSQL, "CREATE TABLE t (a SERIAL, b INT)", "not the primary key"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			_, err := dialect.Translate(tt.dialect, tt.input)
			if !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Fatalf("Translate(%s, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.input, err)
			}
			if !strings.Contains(err.Error(), tt.mention) {
				t.Errorf("Translate(%s, %q) error = %q, want it to mention %q", tt.dialect, tt.input, err, tt.mention)
			}
		})
	}
}

// TestConstraintsAndOptionsSurviveTheTranslation covers the column and table
// clauses that carry into SQLite unchanged, and the ones that do not. A
// constraint dropped in translation is a table that accepts rows the caller's
// schema forbids, which nothing later reports.
func TestConstraintsAndOptionsSurviveTheTranslation(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		dialect dialect.Dialect
		input   string
		want    string
	}{
		{
			name:    "a foreign key and its actions",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT REFERENCES u (id) ON DELETE CASCADE)",
			want:    "CREATE TABLE t (a INTEGER REFERENCES u (id) ON DELETE CASCADE)",
		},
		{
			name:    "a table-level foreign key",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT, FOREIGN KEY (a) REFERENCES u (id))",
			want:    "CREATE TABLE t (a INTEGER, FOREIGN KEY (a) REFERENCES u (id))",
		},
		{
			name:    "a named constraint",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT CONSTRAINT positive CHECK (a > 0))",
			want:    "CREATE TABLE t (a INTEGER CONSTRAINT positive CHECK (a > 0))",
		},
		{
			name:    "a collation on a column",
			dialect: dialect.MySQL,
			input:   "CREATE TABLE t (a VARCHAR(10) COLLATE utf8mb4_bin)",
			want:    "CREATE TABLE t (a TEXT COLLATE BINARY)",
		},
		{
			name:    "a generated column",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
			want:    "CREATE TABLE t (a INTEGER, b INTEGER AS (a + 1) STORED)",
		},
		{
			name:    "an identity column that is the key",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY)",
			want:    "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)",
		},
		{
			name:    "sqlite's own table options",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT) WITHOUT ROWID",
			want:    "CREATE TABLE t (a INTEGER) WITHOUT ROWID",
		},
		{
			name:    "a decimal keeps its precision and scale",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a NUMERIC(10,2))",
			want:    "CREATE TABLE t (a NUMERIC(10,2))",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := dialect.Translate(tt.dialect, tt.input)
			if err != nil {
				t.Fatalf("Translate(%s, %q): %v", tt.dialect, tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Translate(%s, %q)\n  = %q\nwant %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}

// TestAQualifiedFunctionNameIsRefused keeps a namespaced call from reaching
// SQLite, which has one namespace of functions and would report on a name the
// caller did not write.
func TestAQualifiedFunctionNameIsRefused(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialect.Dialect
		input   string
	}{
		{dialect.PostgreSQL, "SELECT pg_catalog.length('a')"},
		{dialect.MySQL, "SELECT mysql.f(1)"},
		{dialect.GoogleSQL, "SELECT project.dataset.f(1)"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			if _, err := dialect.Translate(tt.dialect, tt.input); !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Errorf("Translate(%s, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.input, err)
			}
		})
	}
}

// TestInListsAndSubqueriesTranslate covers the IN forms, whose list and
// subquery are lowered by different paths.
func TestInListsAndSubqueriesTranslate(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ input, want string }{
		{"SELECT a FROM t WHERE a IN (1, 2)", "SELECT a FROM t WHERE a IN (1, 2)"},
		{"SELECT a FROM t WHERE a NOT IN (SELECT b FROM u)", "SELECT a FROM t WHERE a NOT IN (SELECT b FROM u)"},
		{"SELECT a FROM t WHERE a IN ()", "SELECT a FROM t WHERE a IN ()"},
		{"SELECT a FROM t WHERE a = ANY (SELECT b FROM u)", "SELECT a FROM t WHERE a IN (SELECT b FROM u)"},
		{"SELECT a FROM t WHERE a <> ALL (1, 2)", "SELECT a FROM t WHERE a NOT IN (1, 2)"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := dialect.Translate(dialect.PostgreSQL, tt.input)
			if err != nil {
				t.Fatalf("Translate(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Translate(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestTheRestOfTheStatementForms covers the spellings that reach a branch the
// tables above do not: the transaction words each dialect writes, a
// parenthesized join, a qualified star, a multi-column assignment, and the
// ordering clauses.
func TestTheRestOfTheStatementForms(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialect.Dialect
		input   string
		want    string
	}{
		{dialect.PostgreSQL, "BEGIN", "BEGIN"},
		{dialect.PostgreSQL, "BEGIN TRANSACTION", "BEGIN"},
		{dialect.MySQL, "COMMIT WORK", "COMMIT"},
		{dialect.PostgreSQL, "END", "COMMIT"},
		{dialect.PostgreSQL, "ROLLBACK", "ROLLBACK"},
		{dialect.PostgreSQL, "ROLLBACK TO SAVEPOINT s", "ROLLBACK TO SAVEPOINT s"},
		{dialect.PostgreSQL, "RELEASE SAVEPOINT s", "RELEASE s"},
		{dialect.PostgreSQL, "EXPLAIN QUERY PLAN SELECT 1", "EXPLAIN QUERY PLAN SELECT 1"},
		{dialect.PostgreSQL, "PRAGMA foreign_keys = 1", "PRAGMA foreign_keys = 1"},
		{dialect.PostgreSQL, "ANALYZE", "ANALYZE"},

		{dialect.PostgreSQL, "SELECT t.* FROM t", "SELECT t.* FROM t"},
		{dialect.PostgreSQL, "SELECT * FROM (t JOIN u ON t.a = u.a)", "SELECT * FROM (t JOIN u ON t.a = u.a)"},
		{dialect.PostgreSQL, "SELECT * FROM t NATURAL JOIN u", "SELECT * FROM t NATURAL JOIN u"},
		{dialect.PostgreSQL, "SELECT * FROM t RIGHT JOIN u ON t.a = u.a", "SELECT * FROM t RIGHT JOIN u ON t.a = u.a"},
		{dialect.PostgreSQL, "SELECT * FROM t FULL OUTER JOIN u ON t.a = u.a", "SELECT * FROM t FULL JOIN u ON t.a = u.a"},
		{dialect.PostgreSQL, "SELECT * FROM generate(1) AS g", "SELECT * FROM generate(1) AS g"},
		{dialect.PostgreSQL, "SELECT * FROM (SELECT 1 AS n) AS s (m)", "SELECT * FROM (SELECT 1 AS n) AS s"},
		{dialect.PostgreSQL, "UPDATE t SET (a, b) = (1, 2)", "UPDATE t SET (a, b) = (1, 2)"},
		{dialect.PostgreSQL, "SELECT a FROM t ORDER BY a COLLATE \"C\"", "SELECT a FROM t ORDER BY a COLLATE BINARY NULLS LAST"},
		{dialect.PostgreSQL, "SELECT a FROM t ORDER BY a ASC NULLS FIRST", "SELECT a FROM t ORDER BY a NULLS FIRST"},
		{dialect.MySQL, "SELECT a FROM t ORDER BY a DESC", "SELECT a FROM t ORDER BY a DESC"},
		{dialect.PostgreSQL, "SELECT sum(a) OVER (ORDER BY b RANGE UNBOUNDED PRECEDING) FROM t",
			`SELECT sum(a) OVER (ORDER BY b NULLS LAST RANGE UNBOUNDED PRECEDING) AS "sum(a) OVER (ORDER BY b RANGE UNBOUNDED PRECEDING)" FROM t`},
		{dialect.PostgreSQL, "SELECT sum(a) OVER (w ORDER BY b) FROM t WINDOW w AS (PARTITION BY c)",
			`SELECT sum(a) OVER (w ORDER BY b NULLS LAST) AS "sum(a) OVER (w ORDER BY b)" FROM t WINDOW w AS (PARTITION BY c)`},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := dialect.Translate(tt.dialect, tt.input)
			if err != nil {
				t.Fatalf("Translate(%s, %q): %v", tt.dialect, tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Translate(%s, %q)\n  = %q\nwant %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}

// TestTheRemainingLoweringBranches covers the rules the tables above reach only
// on one side: the negated pattern operators, the frame bounds that follow, the
// two sides of TRIM, and the dates PostgreSQL can see the type of.
func TestTheRemainingLoweringBranches(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialect.Dialect
		input   string
		want    string
	}{
		{dialect.PostgreSQL, "SELECT a FROM t WHERE a NOT LIKE 'x%'",
			"SELECT a FROM t WHERE NOT like_sensitive('x%', a)"},
		{dialect.MySQL, "SELECT a FROM t WHERE a NOT REGEXP 'x'",
			"SELECT a FROM t WHERE NOT mysql_regexp('x', a)"},
		{dialect.PostgreSQL, "SELECT a FROM t WHERE a NOT SIMILAR TO 'x%'",
			"SELECT a FROM t WHERE NOT similar_to('x%', a)"},
		{dialect.PostgreSQL, "SELECT a FROM t WHERE a !~ 'x'",
			"SELECT a FROM t WHERE a NOT REGEXP 'x'"},
		{dialect.PostgreSQL, "SELECT TRIM(LEADING 'x' FROM a) FROM t",
			`SELECT ltrim(a, 'x') AS "TRIM(LEADING 'x' FROM a)" FROM t`},
		{dialect.PostgreSQL, "SELECT TRIM(TRAILING 'x' FROM a) FROM t",
			`SELECT rtrim(a, 'x') AS "TRIM(TRAILING 'x' FROM a)" FROM t`},
		{dialect.MySQL, "SELECT TRIM(BOTH 'x' FROM a) FROM t",
			`SELECT trim(a, 'x') AS "TRIM(BOTH 'x' FROM a)" FROM t`},
		{dialect.PostgreSQL, "SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
			`SELECT sum(a) OVER (ORDER BY b NULLS LAST ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) ` +
				`AS "sum(a) OVER (ORDER BY b ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)" FROM t`},
		{dialect.PostgreSQL, "SELECT sum(a) OVER (ORDER BY b ROWS 2 FOLLOWING) FROM t",
			`SELECT sum(a) OVER (ORDER BY b NULLS LAST ROWS 2 FOLLOWING) ` +
				`AS "sum(a) OVER (ORDER BY b ROWS 2 FOLLOWING)" FROM t`},
		{dialect.PostgreSQL, "SELECT (d::date) + 1 FROM t",
			`SELECT postgresql_date_add((postgresql_cast(d, 'date')), 1) AS "(d::date) + 1" FROM t`},
		{dialect.PostgreSQL, "SELECT TIMESTAMP '2024-01-01' - DATE '2023-01-01'",
			`SELECT postgresql_date_diff('2024-01-01', '2023-01-01') AS "TIMESTAMP '2024-01-01' - DATE '2023-01-01'"`},
		{dialect.MySQL, "DELETE FROM t WHERE a = 1 ORDER BY a LIMIT 2",
			"DELETE FROM t WHERE a = 1 ORDER BY a LIMIT 2"},
		{dialect.PostgreSQL, "UPDATE t SET a = 1 FROM u WHERE t.b = u.b",
			"UPDATE t SET a = 1 FROM u WHERE t.b = u.b"},
		{dialect.PostgreSQL, "ALTER TABLE t RENAME COLUMN a TO b", "ALTER TABLE t RENAME COLUMN a TO b"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := dialect.Translate(tt.dialect, tt.input)
			if err != nil {
				t.Fatalf("Translate(%s, %q): %v", tt.dialect, tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Translate(%s, %q)\n  = %q\nwant %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}

// TestQuantifiedComparisonsWithoutAForm names the quantifier in the refusal, so
// a caller reads about the comparison they wrote.
func TestQuantifiedComparisonsWithoutAForm(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ input, mention string }{
		{"SELECT a FROM t WHERE a > ANY (SELECT b FROM u)", "ANY"},
		{"SELECT a FROM t WHERE a < SOME (SELECT b FROM u)", "SOME"},
		{"SELECT a FROM t WHERE a = ALL (SELECT b FROM u)", "ALL"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			_, err := dialect.Translate(dialect.PostgreSQL, tt.input)
			if !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Fatalf("Translate(%q) error = %v, want ErrUnsupportedSyntax", tt.input, err)
			}
			if !strings.Contains(err.Error(), tt.mention) {
				t.Errorf("Translate(%q) error = %q, want it to name %s", tt.input, err, tt.mention)
			}
		})
	}
}
