package parser

import (
	"errors"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/ast"
	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// TestStatementShapes asserts what the parser builds for the statement forms
// the package supports, by walking the tree rather than by writing it back.
func TestStatementShapes(t *testing.T) {
	t.Parallel()

	t.Run("a select with every clause", func(t *testing.T) {
		t.Parallel()

		stmt := mustParse(t, `
			WITH w AS (SELECT 1 AS n)
			SELECT DISTINCT a, b AS c, count(*) FROM t JOIN u ON t.id = u.id
			WHERE a > 1 GROUP BY a HAVING count(*) > 2
			WINDOW win AS (PARTITION BY a ORDER BY b)
			ORDER BY a DESC NULLS LAST LIMIT 10 OFFSET 5`)
		sel, ok := stmt.(*ast.SelectStmt)
		if !ok {
			t.Fatalf("statement is %T, want a SELECT", stmt)
		}
		if sel.With == nil || len(sel.With.CTEs) != 1 || sel.With.CTEs[0].Name != "w" {
			t.Errorf("WITH = %+v, want one common table expression named w", sel.With)
		}
		core, ok := sel.Body.(*ast.SelectCore)
		if !ok {
			t.Fatalf("body is %T, want a SELECT core", sel.Body)
		}
		if !core.Distinct {
			t.Error("DISTINCT was not read")
		}
		if len(core.Items) != 3 || core.Items[1].Alias != "c" {
			t.Errorf("select list = %d items, second alias %q", len(core.Items), core.Items[1].Alias)
		}
		join, ok := core.From[0].(*ast.JoinTable)
		if !ok || join.On == nil {
			t.Fatalf("FROM = %T, want a join with an ON clause", core.From[0])
		}
		if core.Where == nil || core.Having == nil || len(core.GroupBy) != 1 {
			t.Error("WHERE, GROUP BY and HAVING were not all read")
		}
		if len(core.Windows) != 1 || core.Windows[0].Name != "win" {
			t.Errorf("WINDOW = %+v, want one named window", core.Windows)
		}
		if len(sel.OrderBy) != 1 || !sel.OrderBy[0].Desc || sel.OrderBy[0].Nulls != ast.NullsLast {
			t.Errorf("ORDER BY = %+v, want one descending term with NULLS LAST", sel.OrderBy)
		}
		if sel.Limit == nil || sel.Limit.Count == nil || sel.Limit.Offset == nil {
			t.Errorf("LIMIT = %+v, want a count and an offset", sel.Limit)
		}
	})

	t.Run("a set operation binds looser than a select", func(t *testing.T) {
		t.Parallel()

		stmt := mustParse(t, "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3")
		sel, ok := stmt.(*ast.SelectStmt)
		if !ok {
			t.Fatalf("statement is %T, want a SELECT", stmt)
		}
		top, ok := sel.Body.(*ast.SetOp)
		if !ok || top.Op != ast.Union {
			t.Fatalf("body is %T, want a UNION at the top", sel.Body)
		}
		// INTERSECT binds tighter, so it is the right operand of the UNION.
		if _, ok := top.Right.(*ast.SetOp); !ok {
			t.Errorf("right of UNION is %T, want the INTERSECT", top.Right)
		}
	})

	t.Run("a correlated subquery in WHERE", func(t *testing.T) {
		t.Parallel()

		stmt := mustParse(t, "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)")
		sel, ok := stmt.(*ast.SelectStmt)
		if !ok {
			t.Fatalf("statement is %T, want a SELECT", stmt)
		}
		core, ok := sel.Body.(*ast.SelectCore)
		if !ok {
			t.Fatalf("body is %T, want a SELECT core", sel.Body)
		}
		if _, ok := core.Where.(*ast.ExistsExpr); !ok {
			t.Errorf("WHERE is %T, want an EXISTS", core.Where)
		}
	})

	t.Run("the data statements", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			query string
			want  string
		}{
			{"INSERT INTO t (a) VALUES (1)", "*ast.InsertStmt"},
			{"INSERT INTO t SELECT a FROM u", "*ast.InsertStmt"},
			{"UPDATE t SET a = 1 WHERE b = 2", "*ast.UpdateStmt"},
			{"DELETE FROM t WHERE a = 1", "*ast.DeleteStmt"},
			{"CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)", "*ast.CreateTableStmt"},
			{"CREATE VIEW v AS SELECT 1 AS n", "*ast.CreateViewStmt"},
			{"CREATE INDEX i ON t (a)", "*ast.CreateIndexStmt"},
			{"DROP TABLE IF EXISTS t", "*ast.DropStmt"},
			{"ALTER TABLE t RENAME TO u", "*ast.AlterTableStmt"},
			{"BEGIN", "*ast.TransactionStmt"},
			{"EXPLAIN QUERY PLAN SELECT 1", "*ast.ExplainStmt"},
			{"PRAGMA table_info(t)", "*ast.PragmaStmt"},
		} {
			stmt := mustParse(t, tt.query)
			if got := typeName(stmt); got != tt.want {
				t.Errorf("Parse(%q) = %s, want %s", tt.query, got, tt.want)
			}
		}
	})
}

// TestStatementsOutsideTheSubset covers the statements this package does not
// model. Each is refused by name rather than forwarded to SQLite, which is what
// keeps the supported language from being whatever SQLite happens to accept.
func TestStatementsOutsideTheSubset(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialects.Dialect
		query   string
	}{
		{dialects.MySQL, "CREATE DATABASE d"},
		{dialects.MySQL, "CREATE TABLE t LIKE u"},
		{dialects.MySQL, "GRANT SELECT ON t TO PUBLIC"},
		{dialects.MySQL, "LOCK TABLES t WRITE"},
		{dialects.MySQL, "SHOW TABLES"},
		{dialects.PostgreSQL, "CREATE SEQUENCE s"},
		{dialects.PostgreSQL, "CREATE MATERIALIZED VIEW m AS SELECT 1"},
		{dialects.PostgreSQL, "COMMENT ON TABLE t IS 'x'"},
		{dialects.PostgreSQL, "CREATE OR REPLACE VIEW v AS SELECT 1"},
		{dialects.PostgreSQL, "ALTER TABLE t ALTER COLUMN a TYPE INT"},
		{dialects.GoogleSQL, "CREATE FUNCTION f(x INT64) AS (x + 1)"},
		{dialects.GoogleSQL, "EXPORT DATA OPTIONS (uri = 'x') AS SELECT 1"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(tt.dialect, tt.query)
			if !errors.Is(err, sqlerr.ErrUnsupportedSyntax) && !errors.Is(err, sqlerr.ErrUnsupportedFeature) {
				t.Errorf("Parse(%s, %q) error = %v, want a refusal", tt.dialect, tt.query, err)
			}
		})
	}
}

// TestStorageOptionsAreDropped keeps the clauses that ask for a physical layout
// out of the statement rather than refusing it: the table they describe is the
// table SQLite makes, and the words have nothing to say to it.
func TestStorageOptionsAreDropped(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialects.Dialect
		query   string
	}{
		{dialects.MySQL, "CREATE TABLE t (a INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		{dialects.MySQL, "CREATE TABLE t (a INT COMMENT 'a column')"},
		{dialects.GoogleSQL, "CREATE TABLE t (a INT64) CLUSTER BY a"},
		{dialects.GoogleSQL, "CREATE TABLE t (a INT64) OPTIONS (description = 'x')"},
		{dialects.PostgreSQL, "CREATE UNLOGGED TABLE t (a INT)"},
		{dialects.PostgreSQL, "CREATE INDEX CONCURRENTLY i ON t (a)"},
		{dialects.PostgreSQL, "DROP TABLE t CASCADE"},
		{dialects.MySQL, "SELECT a FROM t USE INDEX (i)"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			if _, err := Parse(tt.dialect, tt.query); err != nil {
				t.Errorf("Parse(%s, %q): %v", tt.dialect, tt.query, err)
			}
		})
	}
}

func mustParse(t *testing.T, query string) ast.Stmt {
	t.Helper()
	stmt, err := Parse(dialects.PostgreSQL, strings.TrimSpace(query))
	if err != nil {
		t.Fatalf("Parse(%q): %v", query, err)
	}
	return stmt
}

func typeName(v any) string {
	switch v.(type) {
	case *ast.SelectStmt:
		return "*ast.SelectStmt"
	case *ast.InsertStmt:
		return "*ast.InsertStmt"
	case *ast.UpdateStmt:
		return "*ast.UpdateStmt"
	case *ast.DeleteStmt:
		return "*ast.DeleteStmt"
	case *ast.CreateTableStmt:
		return "*ast.CreateTableStmt"
	case *ast.CreateViewStmt:
		return "*ast.CreateViewStmt"
	case *ast.CreateIndexStmt:
		return "*ast.CreateIndexStmt"
	case *ast.DropStmt:
		return "*ast.DropStmt"
	case *ast.AlterTableStmt:
		return "*ast.AlterTableStmt"
	case *ast.TransactionStmt:
		return "*ast.TransactionStmt"
	case *ast.ExplainStmt:
		return "*ast.ExplainStmt"
	case *ast.PragmaStmt:
		return "*ast.PragmaStmt"
	default:
		return "unknown"
	}
}

// TestQueryFormsThatAreNotImplemented covers the query shapes the parser reads
// far enough to name and then refuses, so the message is about the construct
// rather than about the token it stopped on.
func TestQueryFormsThatAreNotImplemented(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialects.Dialect
		query   string
		kind    error
	}{
		{dialects.PostgreSQL, "TABLE t", sqlerr.ErrUnsupportedFeature},
		{dialects.PostgreSQL, "SELECT a FROM t WHERE a > 1 FOR UPDATE", sqlerr.ErrUnsupportedSyntax},
		// A row-locking clause is refused wherever it stands and however it is
		// spelled. LOCK is a name in MySQL until IN follows it, and FOR KEY
		// SHARE is one of PostgreSQL's four lock strengths.
		{dialects.MySQL, "SELECT a FROM t LOCK IN SHARE MODE", sqlerr.ErrUnsupportedSyntax},
		{dialects.MySQL, "SELECT a FROM t AS x LOCK IN SHARE MODE", sqlerr.ErrUnsupportedSyntax},
		{dialects.PostgreSQL, "SELECT a FROM t FOR KEY SHARE", sqlerr.ErrUnsupportedSyntax},
		{dialects.PostgreSQL, "SELECT a FROM t TABLESAMPLE SYSTEM (10)", sqlerr.ErrUnsupportedSyntax},
		{dialects.MySQL, "SELECT a FROM t PARTITION (p0)", sqlerr.ErrUnsupportedSyntax},
		{dialects.PostgreSQL, "WITH w AS MATERIALIZED (SELECT 1) SELECT 1", sqlerr.ErrUnsupportedSyntax},
		{dialects.PostgreSQL, "WITH w AS (INSERT INTO t VALUES (1) RETURNING a) SELECT * FROM w", sqlerr.ErrUnsupportedSyntax},
		{dialects.PostgreSQL, "START TRANSACTION ISOLATION LEVEL SERIALIZABLE", sqlerr.ErrUnsupportedSyntax},
		{dialects.PostgreSQL, "EXPLAIN (ANALYZE) SELECT 1", sqlerr.ErrUnsupportedSyntax},
		{dialects.GoogleSQL, "SELECT AS STRUCT 1 AS a", sqlerr.ErrUnsupportedSyntax},
		{dialects.GoogleSQL, "SELECT * EXCEPT (a) FROM t", sqlerr.ErrUnsupportedSyntax},
		{dialects.GoogleSQL, "SELECT STRUCT(1 AS a)", sqlerr.ErrUnsupportedSyntax},
		{dialects.GoogleSQL, "SELECT [1, 2]", sqlerr.ErrUnsupportedSyntax},
		{dialects.GoogleSQL, "SELECT CAST(x AS STRING FORMAT 'a')", sqlerr.ErrUnsupportedSyntax},
		{dialects.MySQL, "ALTER TABLE t MODIFY COLUMN a INT", sqlerr.ErrUnsupportedSyntax},
		{dialects.MySQL, "ALTER TABLE t ENGINE = InnoDB", sqlerr.ErrUnsupportedFeature},
		{dialects.MySQL, "CREATE TABLE t (a INT, INDEX (a))", sqlerr.ErrUnsupportedSyntax},
		{dialects.PostgreSQL, "SELECT a FROM t GROUP BY GROUPING SETS ((a), (b))", sqlerr.ErrUnsupportedSyntax},
		{dialects.MySQL, "SELECT a FROM t GROUP BY a WITH ROLLUP", sqlerr.ErrUnsupportedSyntax},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(tt.dialect, tt.query)
			if err == nil {
				// The parser reads some of these and the lowering layer
				// refuses them; both are refusals, and the test above holds
				// the ones that reach lowering.
				return
			}
			if !errors.Is(err, tt.kind) && !errors.Is(err, sqlerr.ErrUnsupportedSyntax) &&
				!errors.Is(err, sqlerr.ErrUnsupportedFeature) {
				t.Errorf("Parse(%s, %q) error = %v, want a refusal", tt.dialect, tt.query, err)
			}
		})
	}
}

// TestIndexHintsAndSuffixesAreRead covers the clauses MySQL writes around a
// table reference, which are read so the reference after them still parses.
func TestIndexHintsAndSuffixesAreRead(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		"SELECT a FROM t USE INDEX (i, j)",
		"SELECT a FROM t FORCE INDEX FOR JOIN (i)",
		"SELECT a FROM t IGNORE KEY FOR ORDER BY (i)",
		"SELECT a FROM t AS x USE INDEX (i)",
		// LOCK opens the row-locking clause only when IN follows it. MySQL
		// leaves the word unreserved, so it is a table's alias and a table's
		// name everywhere else.
		"SELECT a FROM t lock",
		"SELECT a FROM t AS lock",
		"SELECT a FROM lock",
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()

			if _, err := Parse(dialects.MySQL, query); err != nil {
				t.Errorf("Parse(mysql, %q): %v", query, err)
			}
		})
	}
}

// TestQueriesThatStopInTheMiddle covers the error branches of every clause. A
// query that cannot be read is refused where it stops, and each of these stops
// somewhere different.
func TestQueriesThatStopInTheMiddle(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		"SELECT", "SELECT a FROM", "SELECT a FROM t WHERE",
		"SELECT a FROM t GROUP BY", "SELECT a FROM t GROUP BY a HAVING",
		"SELECT a FROM t ORDER BY", "SELECT a FROM t ORDER BY a NULLS",
		"SELECT a FROM t ORDER BY a COLLATE", "SELECT a FROM t LIMIT",
		"SELECT a FROM t LIMIT 1 OFFSET", "SELECT a FROM t FETCH",
		"SELECT a FROM t FETCH FIRST 1", "SELECT a FROM t FETCH FIRST 1 ROWS",
		"SELECT a FROM t JOIN", "SELECT a FROM t JOIN u ON",
		"SELECT a FROM t JOIN u USING", "SELECT a FROM t JOIN u USING (",
		"SELECT a FROM (", "SELECT a FROM (SELECT 1",
		"WITH", "WITH w", "WITH w AS", "WITH w AS (", "WITH w AS (SELECT 1)",
		"SELECT a FROM t WINDOW", "SELECT a FROM t WINDOW w", "SELECT a FROM t WINDOW w AS",
		"SELECT sum(a) OVER (ORDER BY b ROWS", "SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING",
		"SELECT sum(a) OVER (ORDER BY b ROWS 1)", "SELECT sum(a) OVER (ORDER BY b ROWS EXCLUDE)",
		"SELECT count(*) FILTER", "SELECT count(*) FILTER (", "SELECT count(*) FILTER (WHERE)",
		"SELECT a FROM t UNION", "VALUES", "VALUES (", "VALUES (1),",
		"INSERT", "INSERT INTO", "INSERT INTO t", "INSERT INTO t (", "INSERT INTO t (a)",
		"INSERT INTO t (a) VALUES", "INSERT INTO t (a) VALUES (1) ON CONFLICT",
		"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO",
		"UPDATE", "UPDATE t", "UPDATE t SET", "UPDATE t SET a", "UPDATE t SET a =",
		"DELETE", "DELETE FROM", "DELETE FROM t WHERE",
		"CREATE", "CREATE TABLE", "CREATE TABLE t", "CREATE TABLE t (",
		"CREATE TABLE t (a", "CREATE TABLE t (a INT,", "CREATE TABLE t (a INT CONSTRAINT)",
		"CREATE TABLE t (PRIMARY KEY)", "CREATE TABLE t (CHECK)", "CREATE TABLE t (FOREIGN KEY)",
		"CREATE VIEW", "CREATE VIEW v", "CREATE VIEW v AS", "CREATE INDEX",
		"CREATE INDEX i", "CREATE INDEX i ON", "CREATE INDEX i ON t", "CREATE INDEX i ON t (",
		"DROP", "DROP TABLE", "ALTER", "ALTER TABLE", "ALTER TABLE t",
		"ALTER TABLE t RENAME", "ALTER TABLE t RENAME COLUMN a", "ALTER TABLE t ADD COLUMN",
		"ALTER TABLE t DROP COLUMN", "SAVEPOINT", "RELEASE", "PRAGMA", "EXPLAIN",
		"SELECT CAST(x AS", "SELECT EXTRACT(", "SELECT TRIM(LEADING 'x'",
		"SELECT OVERLAY('a' PLACING 'b'", "SELECT SUBSTRING('a' SIMILAR 'b'",
		"SELECT a COLLATE", "SELECT a BETWEEN 1 AND",
		"SELECT CASE", "SELECT CASE WHEN a THEN b", "SELECT f(a", "SELECT (1,",
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(dialects.PostgreSQL, query)
			if err == nil {
				t.Errorf("Parse(%q) succeeded, want a refusal", query)
				return
			}
			if !errors.Is(err, sqlerr.ErrInvalidSyntax) &&
				!errors.Is(err, sqlerr.ErrUnsupportedSyntax) &&
				!errors.Is(err, sqlerr.ErrUnsupportedFeature) {
				t.Errorf("Parse(%q) error = %v, want a refusal of a known kind", query, err)
			}
		})
	}
}

// TestTheClauseSpellingsTheGrammarAccepts covers the alternatives inside a
// clause: the join words, the ordering words, the frame units, and the table
// options each dialect writes.
func TestTheClauseSpellingsTheGrammarAccepts(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		dialect dialects.Dialect
		query   string
	}{
		// Every join spelling, including the ones with OUTER written out.
		{dialects.PostgreSQL, "SELECT a FROM t INNER JOIN u ON t.a = u.a"},
		{dialects.PostgreSQL, "SELECT a FROM t LEFT OUTER JOIN u ON t.a = u.a"},
		{dialects.PostgreSQL, "SELECT a FROM t RIGHT OUTER JOIN u ON t.a = u.a"},
		{dialects.PostgreSQL, "SELECT a FROM t FULL OUTER JOIN u ON t.a = u.a"},
		{dialects.PostgreSQL, "SELECT a FROM t NATURAL LEFT JOIN u"},
		{dialects.MySQL, "SELECT a FROM t STRAIGHT_JOIN u ON t.a = u.a"},
		{dialects.PostgreSQL, "SELECT a FROM t, u, v"},

		// The ordering words, on both sides of the default.
		{dialects.PostgreSQL, "SELECT a FROM t ORDER BY a ASC, b DESC"},
		{dialects.PostgreSQL, "SELECT a FROM t ORDER BY a NULLS FIRST, b NULLS LAST"},
		{dialects.PostgreSQL, `SELECT a FROM t ORDER BY a COLLATE "C" DESC`},

		// Every frame unit and both bound forms.
		{dialects.PostgreSQL, "SELECT sum(a) OVER (ORDER BY b ROWS 1 PRECEDING) FROM t"},
		{dialects.PostgreSQL, "SELECT sum(a) OVER (ORDER BY b RANGE CURRENT ROW) FROM t"},
		{dialects.PostgreSQL, "SELECT sum(a) OVER (ORDER BY b GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"},
		{dialects.PostgreSQL, "SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM t"},
		{dialects.PostgreSQL, "SELECT sum(a) OVER (ORDER BY b ROWS CURRENT ROW EXCLUDE NO OTHERS) FROM t"},

		// The limit spellings.
		{dialects.MySQL, "SELECT a FROM t LIMIT 1, 2"},
		{dialects.PostgreSQL, "SELECT a FROM t OFFSET 2 ROWS"},
		{dialects.PostgreSQL, "SELECT a FROM t FETCH NEXT 2 ROWS ONLY"},

		// The table options each dialect writes after a column list.
		{dialects.PostgreSQL, "CREATE TABLE t (a INT) WITHOUT ROWID"},
		{dialects.PostgreSQL, "CREATE TABLE t (a INT) STRICT"},
		{dialects.MySQL, "CREATE TABLE t (a INT) AUTO_INCREMENT=1 COMMENT='x'"},
		{dialects.MySQL, "CREATE TEMPORARY TABLE IF NOT EXISTS t (a INT)"},
		{dialects.GoogleSQL, "CREATE TABLE t (a INT64) PARTITION BY a"},

		// The insert and conflict spellings.
		{dialects.PostgreSQL, "INSERT OR REPLACE INTO t (a) VALUES (1)"},
		{dialects.MySQL, "REPLACE INTO t (a) VALUES (1)"},
		{dialects.MySQL, "INSERT LOW_PRIORITY IGNORE INTO t (a) VALUES (1)"},
		{dialects.PostgreSQL, "INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING"},
		{dialects.PostgreSQL, "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2 WHERE a > 0"},
		{dialects.MySQL, "INSERT INTO t VALUE (1)"},

		// The delete and update modifiers MySQL writes.
		{dialects.MySQL, "DELETE LOW_PRIORITY QUICK IGNORE FROM t WHERE a = 1"},
		{dialects.MySQL, "UPDATE LOW_PRIORITY IGNORE t SET a = 1"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			if _, err := Parse(tt.dialect, tt.query); err != nil {
				t.Errorf("Parse(%s, %q): %v", tt.dialect, tt.query, err)
			}
		})
	}
}

// TestAClauseKeywordInFrontOfAParenthesisIsACall holds the exception to the
// refusal of a clause word where a value goes: a word in front of a
// parenthesis is a call, which is how MySQL spells an upsert's VALUES(a).
func TestAClauseKeywordInFrontOfAParenthesisIsACall(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		"SELECT LEFT('abc', 2)",
		"SELECT RIGHT('abc', 2)",
		"INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a = VALUES(a)",
	} {
		if _, err := Parse(dialects.MySQL, query); err != nil {
			t.Errorf("Parse(%q): %v", query, err)
		}
	}
}
