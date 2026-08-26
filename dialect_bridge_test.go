package filesql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// sample.csv has columns id,name,age,email with three rows (John/30, Jane/25,
// Bob/35), used by the dialect bridge tests below.

func TestOpenWithDialectMySQL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(dialect.MySQL).Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	db, err := builder.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Backtick identifiers (M-2) and the IF UDF.
	rows, err := db.QueryContext(ctx,
		"SELECT `name`, IF(`age` >= 30, 'senior', 'junior') AS tier FROM `sample` ORDER BY `id`")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name, tier string
		if err := rows.Scan(&name, &tier); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, name+":"+tier)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	want := []string{"John Doe:senior", "Jane Smith:junior", "Bob Johnson:senior"}
	assertStrings(t, got, want)
}

func TestOpenWithDialectPostgreSQL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(dialect.PostgreSQL).Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	db, err := builder.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// :: cast (P-1) and ILIKE (P-2).
	rows, err := db.QueryContext(ctx,
		"SELECT name, age::text AS a FROM sample WHERE name ILIKE 'j%' ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name, a string
		if err := rows.Scan(&name, &a); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, name+"="+a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	want := []string{"John Doe=30", "Jane Smith=25"}
	assertStrings(t, got, want)
}

func TestOpenWithDialectGoogleSQL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(dialect.GoogleSQL).Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	db, err := builder.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// SAFE_CAST (G-2) and SAFE_DIVIDE UDF.
	var half float64
	if err := db.QueryRowContext(ctx,
		"SELECT SAFE_DIVIDE(SAFE_CAST(age AS INT64), 2) FROM `sample` WHERE id = 1").Scan(&half); err != nil {
		t.Fatalf("query: %v", err)
	}
	if half != 15 {
		t.Fatalf("SAFE_DIVIDE = %v, want 15", half)
	}
}

// TestStringAggDistinctExecutes is the execution half of the STRING_AGG(DISTINCT)
// rewrite. The translated text alone proves nothing here: the whole point is that
// the old translation reached SQLite and failed there, so the check that matters
// is that the query now returns a row.
func TestStringAggDistinctExecutes(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		dialect dialect.Dialect
		query   string
	}{
		{
			name:    "postgresql",
			dialect: dialect.PostgreSQL,
			query:   "SELECT STRING_AGG(DISTINCT name, ',') AS names FROM sample",
		},
		{
			name:    "googlesql",
			dialect: dialect.GoogleSQL,
			query:   "SELECT STRING_AGG(DISTINCT name, ',') AS names FROM sample",
		},
		{
			name:    "postgresql_ordered",
			dialect: dialect.PostgreSQL,
			query:   "SELECT STRING_AGG(DISTINCT name, ',' ORDER BY name) AS names FROM sample",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(tt.dialect).Build(ctx)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			db, err := builder.Open(ctx)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()

			var names string
			if err := db.QueryRowContext(ctx, tt.query).Scan(&names); err != nil {
				t.Fatalf("query: %v", err)
			}
			// group_concat's default separator is the comma the query asked for,
			// which is what makes dropping the argument a translation.
			if !strings.Contains(names, ",") {
				t.Errorf("names = %q, want the values joined by a comma", names)
			}
		})
	}
}

func TestOpenWithDialectDefaultIsSQLite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Without WithDialect, the database is plain SQLite: a MySQL-only construct
	// like backtick strings is not translated, but standard SQL still works.
	builder, err := NewBuilder().AddPath("testdata/sample.csv").Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	db, err := builder.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sample").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
}

// TestWithDialectLoadIsSQLite verifies loading is unaffected by the dialect: the
// table is created and populated even though queries run as MySQL.
func TestWithDialectLoadIsSQLite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(dialect.MySQL).Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	db, err := builder.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `sample`").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
}

func TestOpenReadOnlyWithDialect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(dialect.PostgreSQL).Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	rodb, err := builder.OpenReadOnly(ctx)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer rodb.Close()

	// A translated read works.
	var n int
	if err := rodb.QueryRowContext(ctx, "SELECT COUNT(*) FROM sample WHERE name ILIKE 'j%'").Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}

	// A write is translated and then refused by SQLite, so the two settings
	// compose rather than one of them winning.
	if _, err := rodb.ExecContext(ctx, "DELETE FROM sample"); err == nil {
		t.Fatal("ExecContext(DELETE) succeeded on a read-only database")
	}
}

func TestWithDialectAutoSaveConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, err := NewBuilder().
		AddPath("testdata/sample.csv").
		WithDialect(dialect.MySQL).
		EnableAutoSave(t.TempDir()).
		Build(ctx)
	if err == nil {
		t.Fatal("Build should reject WithDialect + auto-save")
	}
	if !errors.Is(err, ErrDatabaseOperation) {
		t.Fatalf("error = %v, want ErrDatabaseOperation", err)
	}
}

func TestWithDialectUnknown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, err := NewBuilder().
		AddPath("testdata/sample.csv").
		WithDialect(dialect.Dialect("oracle")).
		Build(ctx)
	if err == nil {
		t.Fatal("Build should reject an unknown dialect")
	}
	if !errors.Is(err, ErrDatabaseOperation) {
		t.Fatalf("error = %v, want ErrDatabaseOperation", err)
	}
}

// TestWithDialectTranslationError verifies an unsupported construct surfaces as a
// query error rather than a silent wrong result.
func TestWithDialectTranslationError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(dialect.PostgreSQL).Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	db, err := builder.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var name string
	err = db.QueryRowContext(ctx, "SELECT DISTINCT ON (name) name FROM sample").Scan(&name)
	if err == nil {
		t.Fatal("query with DISTINCT ON should error")
	}
}

// TestWithDialectTransaction exercises the transaction path (BeginTx) through a
// dialect-translating database.
func TestWithDialectTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(dialect.MySQL).Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	db, err := builder.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var n int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM `sample`").Scan(&n); err != nil {
		t.Fatalf("query in tx: %v", err)
	}
	if n != 3 {
		t.Fatalf("count = %d, want 3", n)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// TestDialectConnectorMethods unit-tests the connector and connection wrapper
// directly, covering the legacy Prepare/Begin methods and the driver accessor
// that the database/sql fast paths do not exercise.
func TestDialectConnectorMethods(t *testing.T) {
	t.Parallel()
	connector := &dialectConnector{dsn: ":memory:", sqlDialect: dialect.MySQL}
	if connector.Driver() == nil {
		t.Fatal("Driver() returned nil")
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	dc, ok := conn.(*dialectConnection)
	if !ok {
		t.Fatalf("Connect returned %T, want *dialectConnection", conn)
	}

	// Prepare translates the backtick identifier to a double-quoted one before
	// preparing on the underlying connection.
	stmt, err := dc.Prepare("SELECT `x` FROM (SELECT 1 AS `x`)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("stmt close: %v", err)
	}

	// A translation failure surfaces from Prepare.
	if _, err := dc.Prepare("SELECT `x` DIV"); err == nil {
		t.Fatal("Prepare of an untranslatable query should error")
	}

	// Legacy Begin and BeginTx both start a transaction on the underlying conn.
	tx, err := dc.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	tx2, err := dc.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("rollback2: %v", err)
	}
}

func assertStrings(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
}

// TestDialectQueryKeepsResultColumnNames runs the translated SQL against SQLite
// and checks what the caller actually receives: the names of the result columns.
// A rewrite renames an unaliased column, because SQLite names it after the text
// of the expression, and that name is the CSV header or the JSON key the caller
// ends up with.
func TestDialectQueryKeepsResultColumnNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect dialect.Dialect
		query   string
		want    []string
	}{
		{
			name:    "postgresql cast operator",
			dialect: dialect.PostgreSQL,
			query:   `SELECT name, age::text FROM sample`,
			want:    []string{"name", "age::text"},
		},
		{
			name:    "mysql division",
			dialect: dialect.MySQL,
			query:   "SELECT age / 2 FROM sample",
			want:    []string{"age / 2"},
		},
		{
			// The label is the expression after lexical normalization, so a
			// backtick identifier comes back double-quoted: backticks are not
			// SQLite syntax and cannot appear in the statement that runs.
			name:    "mysql backtick identifiers are normalized in the label",
			dialect: dialect.MySQL,
			query:   "SELECT `age` / 2 FROM `sample`",
			want:    []string{`"age" / 2`},
		},
		{
			name:    "googlesql safe cast",
			dialect: dialect.GoogleSQL,
			query:   "SELECT SAFE_CAST(age AS STRING) FROM sample",
			want:    []string{"SAFE_CAST(age AS STRING)"},
		},
		{
			name:    "an explicit alias still wins",
			dialect: dialect.PostgreSQL,
			query:   `SELECT age::text AS shown FROM sample`,
			want:    []string{"shown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(tt.dialect).Build(ctx)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			db, err := builder.Open(ctx)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			rows, err := db.QueryContext(ctx, tt.query)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			defer func() { _ = rows.Close() }()

			got, err := rows.Columns()
			if err != nil {
				t.Fatalf("Columns: %v", err)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("Columns() = %q, want %q", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("column %d = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestDialectLiteralsMeanWhatTheySay runs a query in a dialect against a query
// written directly in SQLite that means the same thing, and requires the same
// answer. It is the differential form of the lexical fixes: a literal that
// translates without error but evaluates to something else is invisible in the
// translated SQL and shows up only here.
func TestDialectLiteralsMeanWhatTheySay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect dialect.Dialect
		query   string
		want    string
	}{
		{
			// A nested block comment is one comment, so the "|| 'x'" inside it
			// does not run. Ending the comment at the first close made it run.
			name:    "a nested block comment hides everything up to its own close",
			dialect: dialect.PostgreSQL,
			query:   "SELECT 'a' /* /* inner */ || 'x' */",
			want:    "a",
		},
		{
			name:    "a PostgreSQL escape string decodes a hex escape",
			dialect: dialect.PostgreSQL,
			query:   `SELECT E'\x41'`,
			want:    "A",
		},
		{
			name:    "a PostgreSQL escape string decodes an octal escape",
			dialect: dialect.PostgreSQL,
			query:   `SELECT E'\101'`,
			want:    "A",
		},
		{
			name:    "a GoogleSQL string decodes a hex escape",
			dialect: dialect.GoogleSQL,
			query:   `SELECT '\x41'`,
			want:    "A",
		},
		{
			// MySQL's default collation folds case beyond ASCII, where SQLite's
			// LIKE stops at it.
			name:    "a MySQL LIKE folds case beyond ASCII",
			dialect: dialect.MySQL,
			query:   `SELECT CASE WHEN 'É' LIKE 'é' THEN 'yes' ELSE 'no' END`,
			want:    "yes",
		},
		{
			// A pattern ending in the escape character reads it as itself. SQLite's
			// native LIKE ... ESCAPE matched nothing for such a pattern, so a row
			// holding exactly that text was dropped.
			name:    "a MySQL LIKE pattern ending in a backslash matches that text",
			dialect: dialect.MySQL,
			query:   `SELECT CASE WHEN 'A\\' LIKE 'A\\' THEN 'yes' ELSE 'no' END`,
			want:    "yes",
		},
		{
			name:    "a GoogleSQL triple-quoted string holds its content bare",
			dialect: dialect.GoogleSQL,
			query:   "SELECT '''abc'''",
			want:    "abc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			builder, err := NewBuilder().AddPath("testdata/sample.csv").WithDialect(tt.dialect).Build(ctx)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			db, err := builder.Open(ctx)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()

			var got string
			if err := db.QueryRowContext(ctx, tt.query).Scan(&got); err != nil {
				t.Fatalf("query %q: %v", tt.query, err)
			}
			if got != tt.want {
				t.Fatalf("%s: %q returned %q, want %q", tt.dialect, tt.query, got, tt.want)
			}
		})
	}
}

// TestTranslatedQueriesStillExecute is the property the label pass has to keep:
// a query SQLite accepts must still be accepted after translation. Checking the
// translated text alone cannot see this — the text looked reasonable, and only
// running it showed the statement no longer parsed.
//
// The case that got through: an implicit alias after a rewritten call.
// "SELECT CONCAT(a,b) z" became "strict_concat(a,b) z AS \"CONCAT(a,b) z\"",
// two names for one item, because a closing paren was read as an operator and
// the alias after it as part of the expression.
func TestTranslatedQueriesStillExecute(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "f.csv")
	if err := os.WriteFile(path, []byte("a,b,c\n1,2,x\n3,4,y\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	plain, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	// t.Cleanup, not defer: the subtests below are parallel, so they run after
	// this function returns. A deferred Close would shut the database before the
	// first one queried it, and every assertion would pass on "database is
	// closed" instead of on what it meant to check.
	t.Cleanup(func() { _ = plain.Close() })

	// Expression shapes crossed with the ways a select item can be named. Each
	// atom that a dialect rewrites is what makes the naming matter.
	atoms := []string{
		"a", "a / b", "a + b", "CONCAT(c,c)", "ABS(a)", "a DIV b", "COUNT(*)",
		"MAX(a)", "CASE WHEN a>1 THEN a ELSE b END", "(SELECT MAX(a) FROM f)",
		"a IS NULL", "c LIKE 'x'", "a * 2", "-a", "(a)", "(a / b)", "LENGTH(c)",
		"CAST(a AS CHAR)",
	}
	suffixes := []string{"", " AS z", " z", " COLLATE NOCASE"}
	tails := []string{"", " FROM f", " FROM f WHERE a > 0", " FROM f GROUP BY c ORDER BY c"}

	for _, atom := range atoms {
		for _, suffix := range suffixes {
			for _, tail := range tails {
				query := "SELECT " + atom + suffix + tail
				t.Run(strings.NewReplacer(" ", "_", "/", "div", "*", "star").Replace(query), func(t *testing.T) {
					t.Parallel()
					// The property is that translation never turns SQL that parses
					// into SQL that does not. A query holding syntax only the source
					// dialect has — MySQL's DIV, say — does not parse to begin with,
					// so a dialect that leaves it alone has broken nothing.
					if runErr := execErr(plain, query); runErr != nil && isSyntaxError(runErr) {
						t.Skipf("SQLite cannot parse %q on its own", query)
					}
					// A syntax error is the only failure in scope. The translated
					// query may still name a helper this connection has not
					// registered, or a function SQLite lacks — those are the
					// caller's contract with the dialect, not malformed SQL. What
					// translation must never do is produce something that does not
					// parse, and asserting on the text alone cannot see that.
					for _, d := range []dialect.Dialect{dialect.MySQL, dialect.PostgreSQL, dialect.GoogleSQL} {
						translated, err := dialect.Translate(d, query)
						if err != nil {
							continue // a construct the dialect refuses by name is a different contract
						}
						if runErr := execErr(plain, translated); runErr != nil && isSyntaxError(runErr) {
							t.Errorf("%s translated a query into one that does not parse\n  in:  %s\n  out: %s\n  err: %v",
								d, query, translated, runErr)
						}
					}
				})
			}
		}
	}
}

// execErr runs the statement and returns whatever stopped it, or nil.
func execErr(db *sql.DB, query string) error {
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	return rows.Err()
}

// isSyntaxError reports whether SQLite refused to parse the statement, as
// opposed to refusing to run one it understood.
func isSyntaxError(err error) bool {
	return strings.Contains(err.Error(), "syntax error")
}

// TestLikeEscapesWildcard pins that a pattern escaping a wildcard matches the
// literal character, in every dialect that says a backslash does that.
//
// It did not, and the two failures pointed opposite ways: MySQL and GoogleSQL
// returned every row, because the escape was dropped and the "%" left behind
// went on being a wildcard, and PostgreSQL returned none, because the backslash
// survived into a matcher that had no notion of escaping and looked for it
// literally. An over-match is the worse of the two: it hands the caller rows
// they filtered out, and nothing about the result looks wrong.
func TestLikeEscapesWildcard(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "codes.csv")
	if err := os.WriteFile(path, []byte("id,code\n1,a%b\n2,axxb\n3,a_b\n4,azb\n5,a\\zb\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "an escaped percent matches only a literal percent",
			query: `SELECT code FROM codes WHERE code LIKE 'a\%b' ORDER BY id`,
			want:  []string{"a%b"},
		},
		{
			name:  "an escaped underscore matches only a literal underscore",
			query: `SELECT code FROM codes WHERE code LIKE 'a\_b' ORDER BY id`,
			want:  []string{"a_b"},
		},
		{
			name:  "an unescaped wildcard still matches everything",
			query: `SELECT code FROM codes WHERE code LIKE 'a%b' ORDER BY id`,
			want:  []string{"a%b", "axxb", "a_b", "azb", "a\\zb"},
		},
		{
			name:  "an unescaped underscore still matches one character",
			query: `SELECT code FROM codes WHERE code LIKE 'a_b' ORDER BY id`,
			want:  []string{"a%b", "a_b", "azb"},
		},
		{
			name:  "NOT LIKE excludes exactly what LIKE selects",
			query: `SELECT code FROM codes WHERE code NOT LIKE 'a\%b' ORDER BY id`,
			want:  []string{"axxb", "a_b", "azb", "a\\zb"},
		},
		{
			name:  "an explicit ESCAPE clause is honored",
			query: `SELECT code FROM codes WHERE code LIKE 'a!%b' ESCAPE '!' ORDER BY id`,
			want:  []string{"a%b"},
		},
	}

	for _, d := range []dialect.Dialect{dialect.MySQL, dialect.PostgreSQL, dialect.GoogleSQL} {
		t.Run(string(d), func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			validated, err := NewBuilder().AddPath(path).WithDialect(d).Build(ctx)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			db, err := validated.Open(ctx)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					got := queryStrings(t, db, tt.query)
					if len(got) != len(tt.want) {
						t.Fatalf("%s\n got  %q\n want %q", tt.query, got, tt.want)
					}
					for i := range got {
						if got[i] != tt.want[i] {
							t.Fatalf("%s\n got  %q\n want %q", tt.query, got, tt.want)
						}
					}
				})
			}
		})
	}
}

func queryStrings(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// TestDialectConnector_UnusableDSN covers the connector that opens the
// translating connections. A DSN the driver refuses has to be reported when the
// connection is made rather than at the first query.
func TestDialectConnector_UnusableDSN(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	connector := &dialectConnector{
		drv:        db.Driver(),
		dsn:        "file:/nonexistent-directory/db.sqlite?mode=rw",
		sqlDialect: dialect.PostgreSQL,
	}

	_, err := connector.Connect(context.Background())
	assert.Error(t, err)
}

// TestDialectConnection_LegacyDriverFallbacks covers a wrapped connection that
// implements neither of the context-aware interfaces. Preparing and beginning
// still have to work, through the pre-context methods.
func TestDialectConnection_LegacyDriverFallbacks(t *testing.T) {
	t.Parallel()

	conn := &dialectConnection{conn: &plainConn{}, sqlDialect: dialect.PostgreSQL}

	t.Run("prepare", func(t *testing.T) {
		t.Parallel()

		_, err := conn.PrepareContext(context.Background(), "SELECT 1")
		assert.ErrorIs(t, err, errStub, "the legacy Prepare is what answers")
	})

	t.Run("begin", func(t *testing.T) {
		t.Parallel()

		tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
		require.NoError(t, err)
		assert.NotNil(t, tx)
	})
}
