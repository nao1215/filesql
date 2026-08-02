package filesql

import (
	"context"
	"database/sql"
)

// DBTX is the common execution surface implemented by *sql.DB and *sql.Tx.
// It lets callers keep several file loads inside one transaction without
// forcing the loader to start a nested transaction.
type DBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}
