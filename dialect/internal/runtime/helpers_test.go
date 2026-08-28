package runtime

import (
	"context"
	"database/sql"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/lower"
	"github.com/nao1215/filesql/dialect/internal/parser"
	"github.com/nao1215/filesql/dialect/internal/render"

	_ "modernc.org/sqlite"
)

// The tests in this package pin what the helpers answer, and a helper is
// reached by translating a query that calls it. Translate is the pipeline the
// public package runs, spelled out here because the public package imports this
// one and cannot be imported back.

// Translate reads a query in dialect d and answers the SQLite SQL it becomes.
func Translate(d dialects.Dialect, query string) (string, error) {
	if d == dialects.SQLite {
		return query, nil
	}
	stmt, err := parser.Parse(d, query)
	if err != nil {
		return "", err
	}
	lowered, err := lower.Lower(d, stmt)
	if err != nil {
		return "", err
	}
	return render.Render(lowered)
}

// castDB opens an in-memory SQLite database with the helper functions
// registered, so a translated call runs the way a caller's query would.
func castDB(t *testing.T) *sql.DB {
	t.Helper()
	if err := RegisterFunctions(); err != nil {
		t.Fatalf("RegisterFunctions() error: %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// runDialect translates query for d and runs it, returning the single scalar it
// selects.
func runDialect(t *testing.T, db *sql.DB, d dialects.Dialect, query string) (sql.NullString, error) {
	t.Helper()
	translated, err := Translate(d, query)
	if err != nil {
		return sql.NullString{}, err
	}
	var got sql.NullString
	err = db.QueryRowContext(context.Background(), translated).Scan(&got)
	return got, err
}
