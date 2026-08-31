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
		// A derived table's column list renames the columns it stands in front
		// of, so the names move onto the select list SQLite takes them from.
		{dialect.PostgreSQL, "SELECT * FROM (SELECT 1 AS n) AS s (m)", `SELECT * FROM (SELECT 1 AS "m") AS s`},
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
			`SELECT trim(mysql_text(a), 'x') AS "TRIM(BOTH 'x' FROM a)" FROM t`},
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

// TestTheFindingsFromReview covers the constructs a review of this rewrite
// found: each was translated into SQL that ran and answered something the
// caller had not asked for, or that crashed the parser.
func TestTheFindingsFromReview(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		dialect dialect.Dialect
		input   string
		want    string
	}{
		{
			// SQLite has no precedence among the compound operators and reads
			// them left to right, where every source dialect binds INTERSECT
			// tighter. Written flat, "1 UNION 2 INTERSECT 3" would have asked
			// for (1 UNION 2) INTERSECT 3.
			name:    "intersect binds tighter than union",
			dialect: dialect.PostgreSQL,
			input:   "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
			want:    "SELECT 1 UNION SELECT * FROM (SELECT 2 INTERSECT SELECT 3)",
		},
		{
			name:    "a left-nested set operation needs no subquery",
			dialect: dialect.PostgreSQL,
			input:   "SELECT 1 UNION SELECT 2 UNION SELECT 3",
			want:    "SELECT 1 UNION SELECT 2 UNION SELECT 3",
		},
		{
			// PostgreSQL reads concatenation below multiplication and SQLite
			// reads it above, so the product needs parentheses to stay the
			// thing being concatenated.
			name:    "concatenation binds looser than multiplication",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a || b * c FROM t",
			want:    `SELECT a || (b * c) AS "a || b * c" FROM t`,
		},
		{
			name:    "a referential action that ends in a keyword",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT REFERENCES u (id) ON DELETE SET NULL)",
			want:    "CREATE TABLE t (a INTEGER REFERENCES u (id) ON DELETE SET NULL)",
		},
		{
			name:    "a referential action of SET DEFAULT",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT REFERENCES u (id) ON UPDATE SET DEFAULT ON DELETE CASCADE)",
			want:    "CREATE TABLE t (a INTEGER REFERENCES u (id) ON UPDATE SET DEFAULT ON DELETE CASCADE)",
		},
		{
			name:    "a deferrable foreign key",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT REFERENCES u (id) DEFERRABLE INITIALLY DEFERRED)",
			want:    "CREATE TABLE t (a INTEGER REFERENCES u (id) DEFERRABLE INITIALLY DEFERRED)",
		},
		{
			// The standard spelling with no number asks for one row.
			name:    "fetch first row only",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a FROM t FETCH FIRST ROW ONLY",
			want:    "SELECT a FROM t LIMIT 1",
		},
		{
			// The predicate picks which partial unique index the upsert
			// resolves against, so dropping it changes what the statement does.
			name:    "an upsert against a partial index",
			dialect: dialect.PostgreSQL,
			input:   "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) WHERE a > 0 DO NOTHING",
			want:    "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) WHERE a > 0 DO NOTHING",
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

// TestTheCallsWithAUnitOrAPartWrittenAsAWord covers the calls whose argument is
// a keyword rather than a value, on both the paths that read one: the unit is
// refused when it is not a word, and normalized when it is.
func TestTheCallsWithAUnitOrAPartWrittenAsAWord(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialect.Dialect
		input   string
		want    string
	}{
		{dialect.MySQL, "SELECT TIMESTAMPADD(MINUTE, 5, d) FROM t",
			`SELECT interval_add(d, 5, 'minute') AS "TIMESTAMPADD(MINUTE, 5, d)" FROM t`},
		{dialect.MySQL, "SELECT TIMESTAMPDIFF(HOUR, a, b) FROM t",
			`SELECT mysql_date_diff(b, a, 'hour') AS "TIMESTAMPDIFF(HOUR, a, b)" FROM t`},
		{dialect.GoogleSQL, "SELECT TIME_TRUNC(t, HOUR) FROM u",
			`SELECT time_trunc(t, 'hour') AS "TIME_TRUNC(t, HOUR)" FROM u`},
		{dialect.GoogleSQL, "SELECT NORMALIZE(s, NFKC) FROM t",
			`SELECT normalize(s, 'NFKC') AS "NORMALIZE(s, NFKC)" FROM t`},
		{dialect.GoogleSQL, "SELECT LAST_DAY(d, WEEK(MONDAY)) FROM t",
			`SELECT googlesql_last_day(d, 'week_monday') AS "LAST_DAY(d, WEEK(MONDAY))" FROM t`},
		{dialect.GoogleSQL, "SELECT DATE_DIFF(a, b, ISOWEEK) FROM t",
			`SELECT date_diff(a, b, 'isoweek') AS "DATE_DIFF(a, b, ISOWEEK)" FROM t`},
		{dialect.GoogleSQL, "SELECT TIME_ADD(t, INTERVAL 5 MINUTE) FROM u",
			`SELECT time_add(t, 5, 'MINUTE') AS "TIME_ADD(t, INTERVAL 5 MINUTE)" FROM u`},
		{dialect.GoogleSQL, "SELECT TIME_SUB(t, INTERVAL 5 MINUTE) FROM u",
			`SELECT time_add(t, -5, 'MINUTE') AS "TIME_SUB(t, INTERVAL 5 MINUTE)" FROM u`},
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

	// A unit written as anything but a word, and a unit no helper knows, are
	// refused rather than passed on to answer NULL.
	for _, tt := range []struct {
		dialect dialect.Dialect
		input   string
	}{
		{dialect.MySQL, "SELECT TIMESTAMPADD(5, 5, d) FROM t"},
		{dialect.MySQL, "SELECT TIMESTAMPADD(FORTNIGHT, 5, d) FROM t"},
		{dialect.MySQL, "SELECT TIMESTAMPDIFF(5, a, b) FROM t"},
		{dialect.MySQL, "SELECT TIMESTAMPDIFF(FORTNIGHT, a, b) FROM t"},
		{dialect.MySQL, "SELECT TIMESTAMPADD(MINUTE, 5) FROM t"},
		{dialect.GoogleSQL, "SELECT TIME_TRUNC(t, 5) FROM u"},
		{dialect.GoogleSQL, "SELECT DATE_DIFF(a, b) FROM t"},
		{dialect.GoogleSQL, "SELECT TIME_ADD(t, 5) FROM u"},
		{dialect.GoogleSQL, "SELECT LAST_DAY(d, 5) FROM t"},
		{dialect.MySQL, "SELECT DATE_ADD(d) FROM t"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			if _, err := dialect.Translate(tt.dialect, tt.input); !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Errorf("Translate(%s, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.input, err)
			}
		})
	}
}

// TestTheLiteralFormsEachDialectRefuses covers the constants whose meaning
// depends on where they are written, which no translation can see.
func TestTheLiteralFormsEachDialectRefuses(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialect.Dialect
		input   string
		mention string
	}{
		{dialect.MySQL, "SELECT 0x41", "hexadecimal"},
		{dialect.MySQL, "SELECT 0b1010", "bit literal"},
		{dialect.MySQL, "SELECT b'1010'", "bit literal"},
		{dialect.GoogleSQL, "SELECT 0b1010", "binary literal"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			_, err := dialect.Translate(tt.dialect, tt.input)
			if !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Fatalf("Translate(%s, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.input, err)
			}
			if !strings.Contains(err.Error(), tt.mention) {
				t.Errorf("Translate(%s, %q) error = %q, want it to mention %s", tt.dialect, tt.input, err, tt.mention)
			}
		})
	}

	// The forms each dialect does read. GoogleSQL takes 0x as an integer, and
	// PostgreSQL's B'..' and X'..' are bit strings it compares as text.
	for _, tt := range []struct {
		dialect dialect.Dialect
		input   string
		want    string
	}{
		{dialect.GoogleSQL, "SELECT 0x41", "SELECT 0x41"},
		{dialect.PostgreSQL, "SELECT 0xFF", "SELECT 0xFF"},
		{dialect.PostgreSQL, "SELECT B'1010'", `SELECT '1010' AS "B'1010'"`},
		// The lexer reads X'..' as SQLite spells a blob, for every dialect, and
		// the bytes are the same ones PostgreSQL's bit string holds.
		{dialect.PostgreSQL, "SELECT X'41'", "SELECT x'41'"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := dialect.Translate(tt.dialect, tt.input)
			if err != nil {
				t.Fatalf("Translate(%s, %q): %v", tt.dialect, tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Translate(%s, %q) = %q, want %q", tt.dialect, tt.input, got, tt.want)
			}
		})
	}
}

// TestTheSpellingsEachClauseAccepts covers the alternatives inside a clause
// that the tables above take only one branch of: an identity column's sequence
// options, a constraint's conflict clause, a qualified table name, a default
// value written as an expression, and the several ways a name can be written.
func TestTheSpellingsEachClauseAccepts(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		dialect dialect.Dialect
		input   string
		want    string
	}{
		{
			name:    "an identity column with sequence options",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 2) PRIMARY KEY)",
			want:    "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)",
		},
		{
			name:    "a constraint with a conflict clause",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT NOT NULL ON CONFLICT ROLLBACK, b INT UNIQUE ON CONFLICT IGNORE)",
			want:    "CREATE TABLE t (a INTEGER NOT NULL, b INTEGER UNIQUE)",
		},
		{
			name:    "a default written as an expression",
			dialect: dialect.PostgreSQL,
			input:   "CREATE TABLE t (a INT DEFAULT (1 + 2), b TEXT DEFAULT 'x', c INT DEFAULT -1)",
			want:    "CREATE TABLE t (a INTEGER DEFAULT ((1 + 2)), b TEXT DEFAULT ('x'), c INTEGER DEFAULT (-1))",
		},
		{
			name:    "a qualified table name",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a FROM main.t",
			want:    "SELECT a FROM main.t",
		},
		{
			name:    "a name that collides with a keyword",
			dialect: dialect.PostgreSQL,
			input:   `SELECT "select", "order" FROM "table"`,
			want:    `SELECT "select", "order" FROM "table"`,
		},
		{
			name:    "a name that needs quoting for its characters",
			dialect: dialect.MySQL,
			input:   "SELECT `a b` FROM `t-1`",
			want:    `SELECT "a b" FROM "t-1"`,
		},
		{
			name:    "a comparison written the other way",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a FROM t WHERE a != 1 AND b <> 2",
			want:    "SELECT a FROM t WHERE a != 1 AND b <> 2",
		},
		{
			name:    "an insert with an alias and a returning list",
			dialect: dialect.PostgreSQL,
			input:   "INSERT INTO t AS x (a) VALUES (1) RETURNING a AS b, x.a",
			want:    "INSERT INTO t AS x (a) VALUES (1) RETURNING a AS b, x.a",
		},
		{
			name:    "an update through an alias",
			dialect: dialect.PostgreSQL,
			input:   "UPDATE t AS x SET a = 1 WHERE x.b = 2 RETURNING x.a",
			want:    "UPDATE t AS x SET a = 1 WHERE x.b = 2 RETURNING x.a",
		},
		{
			name:    "a delete through an alias",
			dialect: dialect.PostgreSQL,
			input:   "DELETE FROM t AS x WHERE x.a = 1 RETURNING x.a",
			want:    "DELETE FROM t AS x WHERE x.a = 1 RETURNING x.a",
		},
		{
			name:    "a pragma with a value and one without",
			dialect: dialect.PostgreSQL,
			input:   "PRAGMA main.journal_mode",
			want:    "PRAGMA main.journal_mode",
		},
		{
			name:    "a case with an operand",
			dialect: dialect.PostgreSQL,
			input:   "SELECT CASE a WHEN 1 THEN 'x' WHEN 2 THEN 'y' END FROM t",
			want:    `SELECT CASE a WHEN 1 THEN 'x' WHEN 2 THEN 'y' END FROM t`,
		},
		{
			name:    "the truth-value predicates",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a FROM t WHERE a IS TRUE AND b IS NOT FALSE AND c ISNULL AND d NOTNULL",
			want:    "SELECT a FROM t WHERE a IS TRUE AND b IS NOT FALSE AND c IS NULL AND d IS NOT NULL",
		},
		{
			name:    "is distinct from",
			dialect: dialect.PostgreSQL,
			input:   "SELECT a FROM t WHERE a IS DISTINCT FROM b AND c IS NOT DISTINCT FROM d",
			want:    "SELECT a FROM t WHERE a IS NOT b AND c IS d",
		},
		{
			name:    "a window over a frame counted in groups",
			dialect: dialect.MySQL,
			input:   "SELECT sum(a) OVER (ORDER BY b GROUPS UNBOUNDED PRECEDING) FROM t",
			want:    `SELECT sum(a) OVER (ORDER BY b GROUPS UNBOUNDED PRECEDING) FROM t`,
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

// TestEveryCallShapeThatIsRefused covers the calls whose arguments are wrong for
// what they name. Each is refused where it was written rather than passed on to
// a helper that would answer NULL, which a caller cannot tell from a NULL in
// their data.
func TestEveryCallShapeThatIsRefused(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialect.Dialect
		input   string
	}{
		{dialect.MySQL, "SELECT ROUND(a, 1, 2) FROM t"},
		{dialect.PostgreSQL, "SELECT ROUND() FROM t"},
		{dialect.MySQL, "SELECT FORMAT(a, 2, 'de_DE') FROM t"},
		{dialect.MySQL, "SELECT DATE_ADD(d, INTERVAL 1 DAY, 2) FROM t"},
		{dialect.GoogleSQL, "SELECT EDIT_DISTANCE('a', 'b', unknown_option => 1)"},
		{dialect.PostgreSQL, "SELECT STRING_AGG(DISTINCT a, '|') FROM t"},
		{dialect.MySQL, "SELECT GROUP_CONCAT(DISTINCT a SEPARATOR '|') FROM t"},
		{dialect.PostgreSQL, "SELECT VARIANCE(a, b) FROM t"},
		{dialect.PostgreSQL, "SELECT CORR(a) FROM t"},
		{dialect.GoogleSQL, "SELECT COUNTIF(a, b) FROM t"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			if _, err := dialect.Translate(tt.dialect, tt.input); !errors.Is(err, dialect.ErrUnsupportedSyntax) {
				t.Errorf("Translate(%s, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.input, err)
			}
		})
	}
}

// TestAggregateRenamesAndExpansions runs the aggregates each dialect has and
// SQLite does not, checking the answer rather than the text: an expansion built
// from the sums SQLite does have is only right if it computes the same number.
func TestAggregateRenamesAndExpansions(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	const rows = ` FROM (SELECT 1 AS a, 2 AS b UNION ALL SELECT 2, 4 UNION ALL SELECT 4, 7) t`

	for _, tt := range []struct {
		dialect dialect.Dialect
		query   string
		want    string
	}{
		{dialect.MySQL, "SELECT ANY_VALUE(a)" + rows, "1"},
		{dialect.PostgreSQL, "SELECT BOOL_AND(a > 0)" + rows, "1"},
		{dialect.PostgreSQL, "SELECT BOOL_OR(a > 3)" + rows, "1"},
		{dialect.PostgreSQL, "SELECT EVERY(a > 3)" + rows, "0"},
		{dialect.GoogleSQL, "SELECT LOGICAL_AND(a > 0)" + rows, "1"},
		{dialect.GoogleSQL, "SELECT LOGICAL_OR(a > 3)" + rows, "1"},
		{dialect.GoogleSQL, "SELECT COUNTIF(a > 1)" + rows, "2"},
		{dialect.GoogleSQL, "SELECT APPROX_COUNT_DISTINCT(a)" + rows, "3"},
		// The population variance of 1, 2 and 4 is 14/9; the sample variance is
		// 7/3. MySQL's bare VARIANCE is the population one and PostgreSQL's is
		// the sample one, which is the difference these two rows are for.
		{dialect.MySQL, "SELECT ROUND(VARIANCE(a), 4)" + rows, "1.5556"},
		{dialect.PostgreSQL, "SELECT ROUND(VARIANCE(a), 4)" + rows, "2.3333"},
		{dialect.MySQL, "SELECT ROUND(STDDEV(a), 4)" + rows, "1.2472"},
		{dialect.PostgreSQL, "SELECT ROUND(STDDEV_POP(a), 4)" + rows, "1.2472"},
		{dialect.GoogleSQL, "SELECT ROUND(VAR_SAMP(a), 4)" + rows, "2.3333"},
		// The covariance of the two columns, and their correlation.
		{dialect.PostgreSQL, "SELECT ROUND(COVAR_POP(a, b), 4)" + rows, "2.5556"},
		{dialect.PostgreSQL, "SELECT ROUND(COVAR_SAMP(a, b), 4)" + rows, "3.8333"},
		{dialect.PostgreSQL, "SELECT ROUND(CORR(a, b), 4)" + rows, "0.9972"},
		{dialect.PostgreSQL, "SELECT JSON_AGG(a)" + rows, "[1,2,4]"},
		{dialect.MySQL, "SELECT JSON_ARRAYAGG(a)" + rows, "[1,2,4]"},
		// A sample estimator over one row divides by zero rows and answers
		// NULL, which is what every source dialect answers.
		{dialect.PostgreSQL, "SELECT VAR_SAMP(a) FROM (SELECT 1 AS a) t", ""},
	} {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}
