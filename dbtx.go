package filesql

import (
	"context"
	"database/sql"
	"fmt"
)

// dbtx is the common execution surface implemented by *sql.DB and *sql.Tx.
// It lets a load run on a caller-owned transaction as readily as on the
// database, without the loader starting a nested one of its own.
type dbtx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

// tableExists reports whether db has a table named name, folding ASCII case the
// way SQLite does when it matches identifiers.
func tableExists(ctx context.Context, db dbtx, name string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE`, name,
	).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("%w: failed to check table existence: %w", ErrDatabaseOperation, err)
	}
	return n > 0, nil
}
