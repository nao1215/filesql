package filesql

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/nao1215/filesql/dialect"
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

	// A write is rejected before translation.
	if _, err := rodb.ExecContext(ctx, "DELETE FROM sample"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ExecContext(DELETE) error = %v, want ErrReadOnly", err)
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
