package dialect

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestErrorIsReportedFromEveryPosition places a broken expression in each
// position the parser and the rewrite walk into, and checks that the
// translation fails there and names that position. Both walks return the
// first error from a child at every level; a level that dropped it would
// render the statement without the expression, or with it unrewritten, and
// the caller would run a different query than the one written.
func TestErrorIsReportedFromEveryPosition(t *testing.T) {
	t.Parallel()

	for _, poison := range []struct {
		text string
		err  error
		at   int // the byte of text the error points at
	}{
		// SQLite has no form for an array, so the parser refuses it.
		{text: "ARRAY[1]", err: ErrUnsupportedSyntax, at: 0},
		// The rewrite refuses || between two documents at the operator, since
		// json_patch is not what it means.
		{text: "'{}'::jsonb || '{}'::jsonb", err: ErrUnsupportedSyntax, at: 12},
		// The parser expects an operand where the parenthesis closes.
		{text: "(1 +)", err: ErrInvalidSyntax, at: 4},
	} {
		for _, template := range positionTemplates {
			query := strings.Replace(template, "%s", poison.text, 1)
			t.Run(query, func(t *testing.T) {
				t.Parallel()

				out, err := Translate(PostgreSQL, query)
				if !errors.Is(err, poison.err) {
					t.Fatalf("Translate(%q) = %q, %v; want %v", query, out, err, poison.err)
				}
				want := fmt.Sprintf("line 1, column %d", strings.Index(query, poison.text)+poison.at+1)
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Translate(%q) error = %v; want it at %s", query, err, want)
				}
			})
		}
	}
}

// positionTemplates are PostgreSQL statements with one expression
// slot, %s, in each clause the rewrite descends into.
var positionTemplates = []string{
	"SELECT %s",
	"SELECT 1 FROM t WHERE %s",
	"SELECT 1 FROM t GROUP BY %s",
	"SELECT 1 FROM t GROUP BY a HAVING %s",
	"SELECT 1 FROM t ORDER BY %s",
	"SELECT 1 FROM t LIMIT %s",
	"SELECT 1 FROM t LIMIT 1 OFFSET %s",
	"WITH c AS (SELECT %s) SELECT 1 FROM c",
	"SELECT 1 UNION SELECT %s",
	"SELECT 1 UNION SELECT 2 ORDER BY %s",
	"VALUES (%s)",
	"SELECT 1 FROM t JOIN u ON %s",
	"SELECT 1 FROM (SELECT %s) AS s",
	"SELECT 1 FROM generate_series(1, %s)",
	"SELECT abs(%s)",
	"SELECT count(*) FILTER (WHERE %s) FROM t",
	"SELECT string_agg(a, ',' ORDER BY %s) FROM t",
	"SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY %s) FROM t",
	"SELECT row_number() OVER (PARTITION BY %s) FROM t",
	"SELECT row_number() OVER (ORDER BY %s) FROM t",
	"SELECT row_number() OVER w FROM t WINDOW w AS (ORDER BY %s)",
	"SELECT CASE WHEN %s THEN 1 END",
	"SELECT CASE WHEN true THEN %s END",
	"SELECT CASE WHEN true THEN 1 ELSE %s END",
	"SELECT CASE %s WHEN 1 THEN 1 END",
	"SELECT CAST(%s AS text)",
	"SELECT %s IS NULL",
	"SELECT NOT %s",
	"SELECT 1 + %s",
	"SELECT %s + 1",
	"SELECT 1 IN (%s)",
	"SELECT %s IN (1)",
	"SELECT 1 BETWEEN %s AND 2",
	"SELECT 1 BETWEEN 0 AND %s",
	"SELECT EXISTS (SELECT %s)",
	"SELECT (SELECT %s)",
	"SELECT 1 IN (SELECT %s)",
	"SELECT 'a' LIKE %s",
	"SELECT coalesce(%s, 1)",
	"INSERT INTO t VALUES (%s)",
	"INSERT INTO t SELECT %s",
	"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = %s",
	"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 1 WHERE %s",
	"INSERT INTO t VALUES (1) RETURNING %s",
	"UPDATE t SET a = %s",
	"UPDATE t SET a = 1 WHERE %s",
	"UPDATE t SET a = 1 FROM u WHERE %s",
	"UPDATE t SET a = 1 RETURNING %s",
	"DELETE FROM t WHERE %s",
	"DELETE FROM t RETURNING %s",
	"CREATE TABLE t (a int DEFAULT (%s))",
	"CREATE TABLE t (a int CHECK (%s))",
	"CREATE TABLE t (a int, CHECK (%s))",
	"CREATE TABLE t AS SELECT %s",
	"CREATE VIEW v AS SELECT %s",
	"CREATE INDEX i ON t (a) WHERE %s",
	"CREATE INDEX i ON t ((%s))",
	"ALTER TABLE t ADD COLUMN a int DEFAULT (%s)",
	"EXPLAIN SELECT %s",
}
