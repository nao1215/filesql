package lower_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/nao1215/filesql/dialect"

	_ "modernc.org/sqlite"
)

// The tests in this package pin what a query becomes and what the result is,
// so they run the whole pipeline through the public entry point rather than
// calling the lowering layer directly: what a caller sees is the contract.

// castDB opens an in-memory SQLite database with the helper functions
// registered, so a translated query runs the way a caller's would.
func castDB(t *testing.T) *sql.DB {
	t.Helper()
	if err := dialect.RegisterFunctions(); err != nil {
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
func runDialect(t *testing.T, db *sql.DB, d dialect.Dialect, query string) (sql.NullString, error) {
	t.Helper()
	translated, err := dialect.Translate(d, query)
	if err != nil {
		return sql.NullString{}, err
	}
	var got sql.NullString
	err = db.QueryRowContext(context.Background(), translated).Scan(&got)
	return got, err
}
